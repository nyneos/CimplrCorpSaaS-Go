package redemption

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Request/Response Types
// ---------------------------

type CreateRedemptionRequestSingle struct {
	UserID            string  `json:"user_id"`
	FolioID           string  `json:"folio_id,omitempty"`
	DematID           string  `json:"demat_id,omitempty"`
	SchemeID          string  `json:"scheme_id"`
	EntityName        string  `json:"entity_name,omitempty"`
	ByAmount          float64 `json:"by_amount,omitempty"`
	ByUnits           float64 `json:"by_units,omitempty"`
	Method            string  `json:"method,omitempty"`           // FIFO, LIFO, etc.
	TransactionDate   string  `json:"transaction_date,omitempty"` // YYYY-MM-DD (optional)
	EstimatedProceeds float64 `json:"estimated_proceeds,omitempty"`
	GainLoss          float64 `json:"gain_loss,omitempty"`
	Status            string  `json:"status,omitempty"`
}

type UpdateRedemptionRequest struct {
	UserID       string                 `json:"user_id"`
	RedemptionID string                 `json:"redemption_id"`
	Fields       map[string]interface{} `json:"fields"`
	Reason       string                 `json:"reason"`
}

type GetRedemptionDetailRequest struct {
	UserID       string `json:"user_id,omitempty"`
	EntityName   string `json:"entity_name,omitempty"`
	RedemptionID string `json:"redemption_id"`
}

// ---------------------------
// CreateRedemptionSingle
// ---------------------------

func CreateRedemptionSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateRedemptionRequestSingle
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		// Validate required fields
		if req.FolioID == "" && req.DematID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Either folio_id or demat_id is required")
			return
		}
		if strings.TrimSpace(req.SchemeID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "scheme_id is required")
			return
		}
		if req.ByAmount <= 0 && req.ByUnits <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Either by_amount or by_units must be greater than 0")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// status column removed from table; do not store status here
		method := "FIFO"
		if strings.TrimSpace(req.Method) != "" {
			method = req.Method
		}

		// Calculate units to block based on by_amount or by_units
		var unitsToBlock float64
		if req.ByUnits > 0 {
			// Direct units provided
			unitsToBlock = req.ByUnits
		} else if req.ByAmount > 0 {
			// Calculate units from amount using latest NAV
			var latestNAV float64
			navQuery := `
				SELECT ans.nav_value
				FROM investment.amfi_nav_staging ans
				LEFT JOIN investment.masterscheme ms ON (
					ms.internal_scheme_code = ans.scheme_code OR
					ms.isin = ans.isin_div_payout_growth OR
					ms.scheme_name = ans.scheme_name
				)
				WHERE (
					ms.scheme_id = $1 OR
					ms.scheme_name = $1 OR
					ms.internal_scheme_code = $1 OR
					ms.isin = $1
				)
				ORDER BY ans.nav_date DESC, ans.file_date DESC
				LIMIT 1
			`
			if err := tx.QueryRow(ctx, navQuery, req.SchemeID).Scan(&latestNAV); err != nil || latestNAV == 0 {
				api.RespondWithError(w, http.StatusInternalServerError, "Unable to fetch NAV for amount-based redemption: "+err.Error())
				return
			}
			unitsToBlock = req.ByAmount / latestNAV
		}

		// Update blocked_units in onboard_transaction for the holding
		if unitsToBlock > 0 {
			updateBlockedUnitsQuery := `
				WITH target_transactions AS (
					SELECT 
						ot.id,
						ot.units,
						COALESCE(ot.blocked_units, 0) AS current_blocked,
						ot.transaction_date
					FROM investment.onboard_transaction ot
					LEFT JOIN investment.masterscheme ms ON (
						ms.scheme_id = ot.scheme_id OR
						ms.internal_scheme_code = ot.scheme_internal_code OR
						ms.isin = ot.scheme_id
					)
					WHERE 
						LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
						AND (
							ms.scheme_id = $1 OR
							ms.scheme_name = $1 OR
							ms.internal_scheme_code = $1 OR
							ms.isin = $1 OR
							ot.scheme_id = $1 OR
							ot.scheme_internal_code = $1
						)
						AND (
							($2::text IS NOT NULL AND (ot.folio_id = $2 OR ot.folio_number = $2)) OR
							($3::text IS NOT NULL AND (ot.demat_id = $3 OR ot.demat_acc_number = $3))
						)
						AND (ot.units - COALESCE(ot.blocked_units, 0)) > 0
					ORDER BY 
						CASE WHEN $4 = 'FIFO' THEN ot.transaction_date END ASC,
						CASE WHEN $4 = 'LIFO' THEN ot.transaction_date END DESC
				),
				blocking_allocation AS (
					SELECT 
						id,
						units,
						current_blocked,
						LEAST(
							units - current_blocked,
							$5 - COALESCE(SUM(LEAST(units - current_blocked, $5)) OVER (
								ORDER BY transaction_date
								ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
							), 0)
						) AS units_to_block_here
					FROM target_transactions
				)
				UPDATE investment.onboard_transaction ot
				SET blocked_units = COALESCE(ot.blocked_units, 0) + ba.units_to_block_here
				FROM blocking_allocation ba
				WHERE ot.id = ba.id AND ba.units_to_block_here > 0
			`
			if _, err := tx.Exec(ctx, updateBlockedUnitsQuery,
				req.SchemeID,
				nullIfEmptyString(req.FolioID),
				nullIfEmptyString(req.DematID),
				method,
				unitsToBlock,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to block units: "+err.Error())
				return
			}
		}

		insertQ := `
		    INSERT INTO investment.redemption_initiation (
			    folio_id, demat_id, scheme_id, requested_by, requested_date, transaction_date,
			    by_amount, by_units, method, entity_name, old_entity_name, estimated_proceeds, gain_loss
		    ) VALUES ($1, $2, $3, $4, now()::date, $5, $6, $7, $8, $9, $10, $11, $12)
		    RETURNING redemption_id
		`
		var redemptionID string
		// requested_by is set from session email; requested_date uses current date
		if err := tx.QueryRow(ctx, insertQ,
			nullIfEmptyString(req.FolioID),
			nullIfEmptyString(req.DematID),
			req.SchemeID,
			userEmail,
			nullIfEmptyString(req.TransactionDate),
			nullIfZeroFloat(req.ByAmount),
			nullIfZeroFloat(req.ByUnits),
			method,
			nullIfEmptyString(req.EntityName),
			nil,
			nullIfZeroFloat(req.EstimatedProceeds),
			nullIfZeroFloat(req.GainLoss),
		).Scan(&redemptionID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// Create audit trail
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, redemptionID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"redemption_id": redemptionID,
			"requested":     userEmail,
		})
	}
}

// ---------------------------
// CreateRedemptionBulk
// ---------------------------

func CreateRedemptionBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				FolioID           string  `json:"folio_id,omitempty"`
				DematID           string  `json:"demat_id,omitempty"`
				SchemeID          string  `json:"scheme_id"`
				EntityName        string  `json:"entity_name,omitempty"`
				ByAmount          float64 `json:"by_amount,omitempty"`
				ByUnits           float64 `json:"by_units,omitempty"`
				Method            string  `json:"method,omitempty"`
				TransactionDate   string  `json:"transaction_date,omitempty"`
				EstimatedProceeds float64 `json:"estimated_proceeds,omitempty"`
				GainLoss          float64 `json:"gain_loss,omitempty"`
				Status            string  `json:"status,omitempty"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoRowsProvided)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			// Validate
			if row.FolioID == "" && row.DematID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Either folio_id or demat_id is required"})
				continue
			}
			if strings.TrimSpace(row.SchemeID) == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "scheme_id is required"})
				continue
			}
			if row.ByAmount <= 0 && row.ByUnits <= 0 {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Either by_amount or by_units must be > 0"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxBeginFailed + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// status column removed from table; do not store status here
			method := "FIFO"
			if strings.TrimSpace(row.Method) != "" {
				method = row.Method
			}

			// Calculate units to block
			var unitsToBlock float64
			if row.ByUnits > 0 {
				unitsToBlock = row.ByUnits
			} else if row.ByAmount > 0 {
				var latestNAV float64
				navQuery := `
					SELECT ans.nav_value
					FROM investment.amfi_nav_staging ans
					LEFT JOIN investment.masterscheme ms ON (
						ms.internal_scheme_code = ans.scheme_code OR
						ms.isin = ans.isin_div_payout_growth OR
						ms.scheme_name = ans.scheme_name
					)
					WHERE (
						ms.scheme_id = $1 OR
						ms.scheme_name = $1 OR
						ms.internal_scheme_code = $1 OR
						ms.isin = $1
					)
					ORDER BY ans.nav_date DESC, ans.file_date DESC
					LIMIT 1
				`
				if err := tx.QueryRow(ctx, navQuery, row.SchemeID).Scan(&latestNAV); err != nil || latestNAV == 0 {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Unable to fetch NAV: " + err.Error()})
					continue
				}
				unitsToBlock = row.ByAmount / latestNAV
			}

			// Update blocked_units
			if unitsToBlock > 0 {
				updateBlockedUnitsQuery := `
					WITH target_transactions AS (
						SELECT 
							ot.id,
							ot.units,
							COALESCE(ot.blocked_units, 0) AS current_blocked,
							ot.transaction_date
						FROM investment.onboard_transaction ot
						LEFT JOIN investment.masterscheme ms ON (
							ms.scheme_id = ot.scheme_id OR
							ms.internal_scheme_code = ot.scheme_internal_code OR
							ms.isin = ot.scheme_id
						)
						WHERE 
							LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
							AND (
								ms.scheme_id = $1 OR
								ms.scheme_name = $1 OR
								ms.internal_scheme_code = $1 OR
								ms.isin = $1 OR
								ot.scheme_id = $1 OR
								ot.scheme_internal_code = $1
							)
							AND (
								($2::text IS NOT NULL AND (ot.folio_id = $2 OR ot.folio_number = $2)) OR
								($3::text IS NOT NULL AND (ot.demat_id = $3 OR ot.demat_acc_number = $3))
							)
							AND (ot.units - COALESCE(ot.blocked_units, 0)) > 0
						ORDER BY 
							CASE WHEN $4 = 'FIFO' THEN ot.transaction_date END ASC,
							CASE WHEN $4 = 'LIFO' THEN ot.transaction_date END DESC
					),
					blocking_allocation AS (
						SELECT 
							id,
							units,
							current_blocked,
							LEAST(
								units - current_blocked,
								$5 - COALESCE(SUM(LEAST(units - current_blocked, $5)) OVER (
									ORDER BY transaction_date
									ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
								), 0)
							) AS units_to_block_here
						FROM target_transactions
					)
					UPDATE investment.onboard_transaction ot
					SET blocked_units = COALESCE(ot.blocked_units, 0) + ba.units_to_block_here
					FROM blocking_allocation ba
					WHERE ot.id = ba.id AND ba.units_to_block_here > 0
				`
				if _, err := tx.Exec(ctx, updateBlockedUnitsQuery,
					row.SchemeID,
					nullIfEmptyString(row.FolioID),
					nullIfEmptyString(row.DematID),
					method,
					unitsToBlock,
				); err != nil {
					results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Failed to block units: " + err.Error()})
					continue
				}
			}

			var redemptionID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.redemption_initiation (
						folio_id, demat_id, scheme_id, requested_by, requested_date, transaction_date,
						by_amount, by_units, method, entity_name, estimated_proceeds, gain_loss
				) VALUES ($1, $2, $3, $4, now()::date, $5, $6, $7, $8, $9, $10, $11)
				RETURNING redemption_id
			`, nullIfEmptyString(row.FolioID), nullIfEmptyString(row.DematID), row.SchemeID, userEmail,
				nullIfEmptyString(row.TransactionDate), nullIfZeroFloat(row.ByAmount), nullIfZeroFloat(row.ByUnits), method, nullIfEmptyString(row.EntityName), nullIfZeroFloat(row.EstimatedProceeds), nullIfZeroFloat(row.GainLoss)).Scan(&redemptionID); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Insert failed: " + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
			`, redemptionID, userEmail); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailedCapitalized + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{
				constants.ValueSuccess: true,
				"redemption_id":        redemptionID,
			})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// UpdateRedemption
// ---------------------------

func UpdateRedemption(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateRedemptionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.RedemptionID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFieldsToUpdateUser)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Fetch existing values (include transaction_date so scans align with fieldPairs)
		sel := `
			SELECT folio_id, demat_id, scheme_id, requested_by, requested_date, transaction_date,
				   by_amount, by_units, method, estimated_proceeds, gain_loss
			FROM investment.redemption_initiation WHERE redemption_id=$1 FOR UPDATE`
		var oldVals [11]interface{}
		if err := tx.QueryRow(ctx, sel, req.RedemptionID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5],
			&oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9], &oldVals[10],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"folio_id":           0,
			"demat_id":           1,
			"scheme_id":          2,
			"requested_by":       3,
			"requested_date":     4,
			"transaction_date":   5,
			"by_amount":          6,
			"by_units":           7,
			"method":             8,
			"estimated_proceeds": 9,
			"gain_loss":          10,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				oldField := "old_" + lk
				sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, lk, pos, oldField, pos+1))
				args = append(args, v, oldVals[idx])
				pos += 2
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields found")
			return
		}

		q := fmt.Sprintf("UPDATE investment.redemption_initiation SET %s, updated_at=now() WHERE redemption_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.RedemptionID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		// Audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
		`, req.RedemptionID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"redemption_id": req.RedemptionID,
			"requested":     userEmail,
		})
	}
}

// ---------------------------
// UpdateRedemptionBulk
// ---------------------------

func UpdateRedemptionBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				RedemptionID string                 `json:"redemption_id"`
				Fields       map[string]interface{} `json:"fields"`
				Reason       string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.RedemptionID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "redemption_id missing"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: constants.ErrTxBeginFailedCapitalized + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			sel := `
				SELECT folio_id, demat_id, scheme_id, requested_by, requested_date, transaction_date,
					   by_amount, by_units, method, estimated_proceeds, gain_loss
				FROM investment.redemption_initiation WHERE redemption_id=$1 FOR UPDATE`
			var oldVals [11]interface{}
			if err := tx.QueryRow(ctx, sel, row.RedemptionID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5],
				&oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9], &oldVals[10],
			); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"folio_id":           0,
				"demat_id":           1,
				"scheme_id":          2,
				"requested_by":       3,
				"requested_date":     4,
				"transaction_date":   5,
				"by_amount":          6,
				"by_units":           7,
				"method":             8,
				"estimated_proceeds": 9,
				"gain_loss":          10,
			}

			var sets []string
			var args []interface{}
			pos := 1

			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					oldField := "old_" + lk
					sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, lk, pos, oldField, pos+1))
					args = append(args, v, oldVals[idx])
					pos += 2
				}
			}

			if len(sets) == 0 {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: "No valid fields"})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.redemption_initiation SET %s, updated_at=now() WHERE redemption_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.RedemptionID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: constants.ErrUpdateFailed + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
			`, row.RedemptionID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: constants.ErrAuditInsertFailed + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: constants.ErrCommitFailed + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{constants.ValueSuccess: true, "redemption_id": row.RedemptionID, "requested": userEmail})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// DeleteRedemption
// ---------------------------

func DeleteRedemption(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			RedemptionIDs []string `json:"redemption_ids"`
			Reason        string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.RedemptionIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_ids required")
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.RedemptionIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionredemption (redemption_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now())
			`, id, req.Reason, requestedBy); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.RedemptionIDs})
	}
}

// ---------------------------
// BulkApproveRedemptionActions
// ---------------------------

func BulkApproveRedemptionActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			RedemptionIDs []string `json:"redemption_ids"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (redemption_id) action_id, redemption_id, actiontype, processing_status
			FROM investment.auditactionredemption
			WHERE redemption_id = ANY($1)
			ORDER BY redemption_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.RedemptionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var toApprove []string
		var toDeleteActionIDs []string
		var deleteMasterIDs []string

		for rows.Next() {
			var aid, rid, atype, pstatus string
			if err := rows.Scan(&aid, &rid, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == "APPROVED" {
				continue
			}
			if ps == "PENDING_DELETE_APPROVAL" {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, rid)
				continue
			}
			if ps == "PENDING_APPROVAL" || ps == "PENDING_EDIT_APPROVAL" {
				toApprove = append(toApprove, aid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrNoApprovableActions, map[string]any{
				"approved_action_ids": []string{},
				"deleted_redemptions": []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionredemption
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approve failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionredemption
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete action update failed: "+err.Error())
				return
			}

			// Release blocked units for deleted redemptions
			for _, rid := range deleteMasterIDs {
				// Fetch redemption details to determine which units to unblock
				var folioID, dematID, schemeID sql.NullString
				var byUnits sql.NullFloat64
				var method string

				if err := tx.QueryRow(ctx, `
					SELECT folio_id, demat_id, scheme_id, by_units, method
					FROM investment.redemption_initiation
					WHERE redemption_id = $1
				`, rid).Scan(&folioID, &dematID, &schemeID, &byUnits, &method); err != nil {
					continue // Skip if unable to fetch details
				}

				if !byUnits.Valid || byUnits.Float64 <= 0 {
					continue // Nothing to unblock
				}

				// Release blocked units using the same method (FIFO/LIFO) that was used to block them
				unblockQuery := `
					WITH target_transactions AS (
						SELECT 
							ot.id,
							COALESCE(ot.blocked_units, 0) AS current_blocked,
							ot.transaction_date
						FROM investment.onboard_transaction ot
						LEFT JOIN investment.masterscheme ms ON (
							ms.scheme_id = ot.scheme_id OR
							ms.internal_scheme_code = ot.scheme_internal_code OR
							ms.isin = ot.scheme_id
						)
						WHERE 
							LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
							AND (
								ms.scheme_id = $1 OR
								ms.scheme_name = $1 OR
								ms.internal_scheme_code = $1 OR
								ms.isin = $1 OR
								ot.scheme_id = $1 OR
								ot.scheme_internal_code = $1
							)
							AND (
								($2::text IS NOT NULL AND (ot.folio_id = $2 OR ot.folio_number = $2)) OR
								($3::text IS NOT NULL AND (ot.demat_id = $3 OR ot.demat_acc_number = $3))
							)
							AND COALESCE(ot.blocked_units, 0) > 0
						ORDER BY 
							CASE WHEN $4 = 'FIFO' THEN ot.transaction_date END ASC,
							CASE WHEN $4 = 'LIFO' THEN ot.transaction_date END DESC
					),
					unblocking_allocation AS (
						SELECT 
							id,
							current_blocked,
							LEAST(
								current_blocked,
								$5 - COALESCE(SUM(LEAST(current_blocked, $5)) OVER (
									ORDER BY transaction_date
									ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
								), 0)
							) AS units_to_unblock_here
						FROM target_transactions
					)
					UPDATE investment.onboard_transaction ot
					SET blocked_units = GREATEST(0, COALESCE(ot.blocked_units, 0) - ua.units_to_unblock_here)
					FROM unblocking_allocation ua
					WHERE ot.id = ua.id AND ua.units_to_unblock_here > 0
				`

				var folioIDStr, dematIDStr *string
				if folioID.Valid {
					folioIDStr = &folioID.String
				}
				if dematID.Valid {
					dematIDStr = &dematID.String
				}

				if _, err := tx.Exec(ctx, unblockQuery,
					schemeID.String,
					nullIfEmptyStringPtr(folioIDStr),
					nullIfEmptyStringPtr(dematIDStr),
					method,
					byUnits.Float64,
				); err != nil {
					// Log error but continue with deletion
					// You might want to log this for debugging
				}
			}

			if _, err := tx.Exec(ctx, `
				UPDATE investment.redemption_initiation
				SET is_deleted=true, updated_at=now()
				WHERE redemption_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete redemption failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"approved_action_ids": toApprove,
			"deleted_redemptions": deleteMasterIDs,
		})
	}
}

// ---------------------------
// BulkRejectRedemptionActions
// ---------------------------

func BulkRejectRedemptionActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			RedemptionIDs []string `json:"redemption_ids"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSession)
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTxBeginFailedCapitalized+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (redemption_id) action_id, redemption_id, processing_status
			FROM investment.auditactionredemption
			WHERE redemption_id = ANY($1)
			ORDER BY redemption_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.RedemptionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		cannotReject := []string{}
		found := map[string]bool{}
		for rows.Next() {
			var aid, rid, ps string
			if err := rows.Scan(&aid, &rid, &ps); err != nil {
				continue
			}
			found[rid] = true
			if strings.ToUpper(strings.TrimSpace(ps)) == "APPROVED" {
				cannotReject = append(cannotReject, rid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		missing := []string{}
		for _, id := range req.RedemptionIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for redemption_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved redemption_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactionredemption
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		// Release blocked units for rejected redemptions
		for _, rid := range req.RedemptionIDs {
			var folioID, dematID, schemeID sql.NullString
			var byUnits sql.NullFloat64
			var method string

			if err := tx.QueryRow(ctx, `
				SELECT folio_id, demat_id, scheme_id, by_units, method
				FROM investment.redemption_initiation
				WHERE redemption_id = $1
			`, rid).Scan(&folioID, &dematID, &schemeID, &byUnits, &method); err != nil {
				continue
			}

			if !byUnits.Valid || byUnits.Float64 <= 0 {
				continue
			}

			unblockQuery := `
				WITH target_transactions AS (
					SELECT 
						ot.id,
						COALESCE(ot.blocked_units, 0) AS current_blocked,
						ot.transaction_date
					FROM investment.onboard_transaction ot
					LEFT JOIN investment.masterscheme ms ON (
						ms.scheme_id = ot.scheme_id OR
						ms.internal_scheme_code = ot.scheme_internal_code OR
						ms.isin = ot.scheme_id
					)
					WHERE 
						LOWER(COALESCE(ot.transaction_type, '')) IN ('buy', 'purchase', 'subscription')
						AND (
							ms.scheme_id = $1 OR
							ms.scheme_name = $1 OR
							ms.internal_scheme_code = $1 OR
							ms.isin = $1 OR
							ot.scheme_id = $1 OR
							ot.scheme_internal_code = $1
						)
						AND (
							($2::text IS NOT NULL AND (ot.folio_id = $2 OR ot.folio_number = $2)) OR
							($3::text IS NOT NULL AND (ot.demat_id = $3 OR ot.demat_acc_number = $3))
						)
						AND COALESCE(ot.blocked_units, 0) > 0
					ORDER BY 
						CASE WHEN $4 = 'FIFO' THEN ot.transaction_date END ASC,
						CASE WHEN $4 = 'LIFO' THEN ot.transaction_date END DESC
				),
				unblocking_allocation AS (
					SELECT 
						id,
						current_blocked,
						LEAST(
							current_blocked,
							$5 - COALESCE(SUM(LEAST(current_blocked, $5)) OVER (
								ORDER BY transaction_date
								ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
							), 0)
						) AS units_to_unblock_here
					FROM target_transactions
				)
				UPDATE investment.onboard_transaction ot
				SET blocked_units = GREATEST(0, COALESCE(ot.blocked_units, 0) - ua.units_to_unblock_here)
				FROM unblocking_allocation ua
				WHERE ot.id = ua.id AND ua.units_to_unblock_here > 0
			`

			var folioIDStr, dematIDStr *string
			if folioID.Valid {
				folioIDStr = &folioID.String
			}
			if dematID.Valid {
				dematIDStr = &dematID.String
			}

			if _, err := tx.Exec(ctx, unblockQuery,
				schemeID.String,
				nullIfEmptyStringPtr(folioIDStr),
				nullIfEmptyStringPtr(dematIDStr),
				method,
				byUnits.Float64,
			); err != nil {
				// Log error but continue
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

// ---------------------------
// GetRedemptionsWithAudit
// ---------------------------

func GetRedemptionsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.redemption_id)
					a.redemption_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactionredemption a
				ORDER BY a.redemption_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					redemption_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactionredemption
				GROUP BY redemption_id
			),
			resolved_folio AS (
				SELECT DISTINCT ON (m.redemption_id)
					m.redemption_id,
					f.folio_number,
					f.folio_id::text AS folio_id_text
				FROM investment.redemption_initiation m
				LEFT JOIN investment.masterfolio f ON (
					(f.folio_id::text = m.folio_id) OR 
					(m.folio_id IS NOT NULL AND f.folio_number = m.folio_id)
				)
				ORDER BY m.redemption_id, f.folio_id
			),
			resolved_demat AS (
				SELECT DISTINCT ON (m.redemption_id)
					m.redemption_id,
					d.demat_account_number,
					d.demat_id::text AS demat_id_text
				FROM investment.redemption_initiation m
				LEFT JOIN investment.masterdemataccount d ON (
					(d.demat_id::text = m.demat_id) OR 
					(m.demat_id IS NOT NULL AND d.default_settlement_account = m.demat_id) OR 
					(m.demat_id IS NOT NULL AND d.demat_account_number = m.demat_id)
				)
				ORDER BY m.redemption_id, d.demat_id
			)
			SELECT
				m.redemption_id,
				m.folio_id,
				m.old_folio_id,
				m.demat_id,
				m.old_demat_id,
				m.scheme_id,
				m.old_scheme_id,
				m.requested_by,
				COALESCE(m.entity_name,'') AS entity_name,
				COALESCE(s.scheme_id::text, m.scheme_id::text) AS resolved_scheme_id,
				COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
				COALESCE(s.internal_scheme_code,'') AS scheme_code,
				COALESCE(s.isin,'') AS isin,
				COALESCE(s.amc_name,'') AS amc_name,
				COALESCE(rf.folio_number,'') AS folio_number,
				COALESCE(rf.folio_id_text,'') AS folio_id_text,
				COALESCE(rd.demat_account_number,'') AS demat_number,
				COALESCE(rd.demat_id_text,'') AS demat_id_text,
				m.old_requested_by,
				TO_CHAR(m.requested_date, 'YYYY-MM-DD') AS requested_date,
				TO_CHAR(m.old_requested_date, 'YYYY-MM-DD') AS old_requested_date,
				TO_CHAR(m.transaction_date, 'YYYY-MM-DD') AS transaction_date,
				TO_CHAR(m.old_transaction_date, 'YYYY-MM-DD') AS old_transaction_date,
				m.estimated_proceeds,
				m.old_estimated_proceeds,
				m.gain_loss,
				m.old_gain_loss,
				DATE_PART('day', now()::timestamp - COALESCE(m.transaction_date, m.requested_date)::timestamp)::int AS age_days,
				m.by_amount,
				m.old_by_amount,
				m.by_units,
				m.old_by_units,
				m.method,
				m.old_method,
				m.is_deleted,
				TO_CHAR(m.updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS audit_requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,
				
				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at
			FROM investment.redemption_initiation m
			LEFT JOIN latest_audit l ON l.redemption_id = m.redemption_id
			LEFT JOIN history h ON h.redemption_id = m.redemption_id
			LEFT JOIN investment.masterscheme s ON (
			    s.scheme_id::text = m.scheme_id
			 OR s.scheme_name = m.scheme_id
			 OR s.internal_scheme_code = m.scheme_id
			 OR s.isin = m.scheme_id
			)
			LEFT JOIN resolved_folio rf ON rf.redemption_id = m.redemption_id
			LEFT JOIN resolved_demat rd ON rd.redemption_id = m.redemption_id
			WHERE COALESCE(m.is_deleted, false) = false
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				rec[string(f.Name)] = vals[i]
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrRowsScanFailed+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// ---------------------------
// GetApprovedRedemptions
// ---------------------------

func GetApprovedRedemptions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
		WITH latest AS (
			SELECT DISTINCT ON (redemption_id)
				redemption_id,
				processing_status,
				requested_at,
				checker_at
			FROM investment.auditactionredemption
			ORDER BY redemption_id, requested_at DESC
		),
		resolved_folio AS (
			SELECT DISTINCT ON (m.redemption_id)
				m.redemption_id,
				f.folio_number
			FROM investment.redemption_initiation m
			LEFT JOIN investment.masterfolio f ON (
				(f.folio_id::text = m.folio_id) OR 
				(m.folio_id IS NOT NULL AND f.folio_number = m.folio_id)
			)
			ORDER BY m.redemption_id, f.folio_id
		),
		resolved_demat AS (
			SELECT DISTINCT ON (m.redemption_id)
				m.redemption_id,
				d.demat_account_number
			FROM investment.redemption_initiation m
			LEFT JOIN investment.masterdemataccount d ON (
				(d.demat_id::text = m.demat_id) OR 
				(m.demat_id IS NOT NULL AND d.default_settlement_account = m.demat_id) OR 
				(m.demat_id IS NOT NULL AND d.demat_account_number = m.demat_id)
			)
			ORDER BY m.redemption_id, d.demat_id
		)
		SELECT
			m.redemption_id,
			m.folio_id,
			m.demat_id,
			m.scheme_id,
			COALESCE(m.entity_name,'') AS entity_name,
			COALESCE(s.scheme_id::text, m.scheme_id::text) AS resolved_scheme_id,
			COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
			COALESCE(s.internal_scheme_code,'') AS scheme_code,
			COALESCE(s.isin,'') AS isin,
			COALESCE(s.amc_name,'') AS amc_name,
			COALESCE(rf.folio_number,'') AS folio_number,
			COALESCE(rd.demat_account_number,'') AS demat_number,
			m.requested_by,
			TO_CHAR(m.requested_date, 'YYYY-MM-DD') AS requested_date,
			TO_CHAR(m.transaction_date, 'YYYY-MM-DD') AS transaction_date,
			m.by_amount,
			m.by_units,
			m.method,
			m.estimated_proceeds,
			m.gain_loss,
			DATE_PART('day', now()::timestamp - COALESCE(m.transaction_date, m.requested_date)::timestamp)::int AS age_days
		FROM investment.redemption_initiation m
		JOIN latest l ON l.redemption_id = m.redemption_id
		LEFT JOIN investment.masterscheme s ON (
		    s.scheme_id::text = m.scheme_id
		 OR s.scheme_name = m.scheme_id
		 OR s.internal_scheme_code = m.scheme_id
		 OR s.isin = m.scheme_id
		)
		LEFT JOIN resolved_folio rf ON rf.redemption_id = m.redemption_id
		LEFT JOIN resolved_demat rd ON rd.redemption_id = m.redemption_id
		WHERE 
			UPPER(l.processing_status) = 'APPROVED'
			AND COALESCE(m.is_deleted,false)=false
			AND m.redemption_id NOT IN (
				SELECT redemption_id FROM investment.redemption_confirmation
			)
		ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC
	`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				rec[string(f.Name)] = vals[i]
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// ---------------------------
// GetRedemptionInitiationDetail
// Returns deep info for a single redemption_id: initiation + audit + holding + lots + confirmations.
// ---------------------------

func GetRedemptionInitiationDetail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req GetRedemptionDetailRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.RedemptionID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_id is required")
			return
		}

		ctx := r.Context()

		// Pull initiation + latest audit + resolved folio/demat/scheme in one go.
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.redemption_id)
					a.redemption_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactionredemption a
				ORDER BY a.redemption_id, a.requested_at DESC
			),
			resolved_folio AS (
				SELECT DISTINCT ON (m.redemption_id)
					m.redemption_id,
					f.folio_id::text AS folio_id,
					f.folio_number,
					f.entity_name AS folio_entity_name,
					COALESCE(f.default_subscription_account,'') AS default_subscription_account,
					COALESCE(f.default_redemption_account,'') AS default_redemption_account
				FROM investment.redemption_initiation m
				LEFT JOIN investment.masterfolio f ON (
					(f.folio_id::text = m.folio_id) OR
					(m.folio_id IS NOT NULL AND f.folio_number = m.folio_id)
				)
				WHERE m.redemption_id = $1
				ORDER BY m.redemption_id, f.folio_id
			),
			resolved_demat AS (
				SELECT DISTINCT ON (m.redemption_id)
					m.redemption_id,
					d.demat_id::text AS demat_id,
					d.demat_account_number,
					d.entity_name AS demat_entity_name,
					COALESCE(d.default_settlement_account,'') AS default_settlement_account
				FROM investment.redemption_initiation m
				LEFT JOIN investment.masterdemataccount d ON (
					(d.demat_id::text = m.demat_id) OR
					(m.demat_id IS NOT NULL AND d.default_settlement_account = m.demat_id) OR
					(m.demat_id IS NOT NULL AND d.demat_account_number = m.demat_id)
				)
				WHERE m.redemption_id = $1
				ORDER BY m.redemption_id, d.demat_id
			)
			SELECT
				m.redemption_id,
				COALESCE(m.entity_name,'') AS entity_name,
				m.folio_id,
				m.demat_id,
				m.scheme_id,
				m.requested_by,
				TO_CHAR(m.requested_date, 'YYYY-MM-DD') AS requested_date,
				TO_CHAR(m.transaction_date, 'YYYY-MM-DD') AS transaction_date,
				COALESCE(m.by_amount, 0) AS by_amount,
				COALESCE(m.by_units, 0) AS by_units,
				COALESCE(m.method, 'FIFO') AS method,
				COALESCE(m.estimated_proceeds, 0) AS estimated_proceeds,
				COALESCE(m.gain_loss, 0) AS gain_loss,
				COALESCE(m.is_deleted,false) AS is_deleted,
				COALESCE(s.scheme_id::text, m.scheme_id::text) AS resolved_scheme_id,
				COALESCE(s.scheme_name, m.scheme_id) AS scheme_name,
				COALESCE(s.internal_scheme_code,'') AS scheme_code,
				COALESCE(s.isin,'') AS isin,
				COALESCE(s.amc_name,'') AS amc_name,
				COALESCE(rf.folio_id,'') AS resolved_folio_id,
				COALESCE(rf.folio_number,'') AS folio_number,
				COALESCE(rf.folio_entity_name,'') AS folio_entity_name,
				COALESCE(rd.demat_id,'') AS resolved_demat_id,
				COALESCE(rd.demat_account_number,'') AS demat_account_number,
				COALESCE(rd.demat_entity_name,'') AS demat_entity_name,
				COALESCE(rf.default_subscription_account,'') AS default_subscription_account,
				COALESCE(rf.default_redemption_account,'') AS default_redemption_account,
				COALESCE(rd.default_settlement_account,'') AS default_settlement_account,
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS audit_requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS audit_requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason
			FROM investment.redemption_initiation m
			LEFT JOIN latest_audit l ON l.redemption_id = m.redemption_id
			LEFT JOIN investment.masterscheme s ON (
				s.scheme_id::text = m.scheme_id
			 OR s.scheme_name = m.scheme_id
			 OR s.internal_scheme_code = m.scheme_id
			 OR s.isin = m.scheme_id
			)
			LEFT JOIN resolved_folio rf ON rf.redemption_id = m.redemption_id
			LEFT JOIN resolved_demat rd ON rd.redemption_id = m.redemption_id
			WHERE m.redemption_id = $1
			LIMIT 1;
		`

		var (
			redemptionID         string
			entityName           string
			folioIDRaw           sql.NullString
			dematIDRaw           sql.NullString
			schemeIDRaw          string
			requestedBy          string
			requestedDate        string
			transactionDate      string
			byAmount             float64
			byUnits              float64
			method               string
			estimatedProceeds    float64
			gainLoss             float64
			isDeleted            bool
			resolvedSchemeID     string
			schemeName           string
			schemeCode           string
			isin                 string
			amcName              string
			resolvedFolioID      string
			folioNumber          string
			folioEntityName      string
			resolvedDematID      string
			dematAccountNumber   string
			dematEntityName      string
			defaultSubAcctRaw    string
			defaultRedAcctRaw    string
			defaultSettleAcctRaw string
			actionType           string
			processingStatus     string
			actionID             string
			auditRequestedBy     string
			auditRequestedAt     string
			checkerBy            string
			checkerAt            string
			checkerComment       string
			reason               string
		)

		err := pgxPool.QueryRow(ctx, q, req.RedemptionID).Scan(
			&redemptionID,
			&entityName,
			&folioIDRaw,
			&dematIDRaw,
			&schemeIDRaw,
			&requestedBy,
			&requestedDate,
			&transactionDate,
			&byAmount,
			&byUnits,
			&method,
			&estimatedProceeds,
			&gainLoss,
			&isDeleted,
			&resolvedSchemeID,
			&schemeName,
			&schemeCode,
			&isin,
			&amcName,
			&resolvedFolioID,
			&folioNumber,
			&folioEntityName,
			&resolvedDematID,
			&dematAccountNumber,
			&dematEntityName,
			&defaultSubAcctRaw,
			&defaultRedAcctRaw,
			&defaultSettleAcctRaw,
			&actionType,
			&processingStatus,
			&actionID,
			&auditRequestedBy,
			&auditRequestedAt,
			&checkerBy,
			&checkerAt,
			&checkerComment,
			&reason,
		)
		if err != nil {
			api.RespondWithError(w, http.StatusNotFound, "redemption_id not found")
			return
		}
		if isDeleted {
			api.RespondWithError(w, http.StatusNotFound, "redemption_id is deleted")
			return
		}

		// Resolve entity if missing on initiation.
		resolvedEntity := strings.TrimSpace(entityName)
		if resolvedEntity == "" {
			if strings.TrimSpace(folioEntityName) != "" {
				resolvedEntity = folioEntityName
			} else if strings.TrimSpace(dematEntityName) != "" {
				resolvedEntity = dematEntityName
			}
		}

		// Ensure we have an entity *name* for downstream SQL and account resolution.
		entityNameScoped := strings.TrimSpace(resolvedEntity)
		approvedNames := api.GetEntityNamesFromCtx(ctx)
		approvedIDs := api.GetEntityIDsFromCtx(ctx)
		idToName := map[string]string{}
		minLen := len(approvedIDs)
		if len(approvedNames) < minLen {
			minLen = len(approvedNames)
		}
		for i := 0; i < minLen; i++ {
			idToName[strings.ToUpper(strings.TrimSpace(approvedIDs[i]))] = strings.TrimSpace(approvedNames[i])
		}
		if mapped, ok := idToName[strings.ToUpper(strings.TrimSpace(entityNameScoped))]; ok && strings.TrimSpace(mapped) != "" {
			entityNameScoped = strings.TrimSpace(mapped)
		}
		if entityNameScoped == "" {
			entityNameScoped = strings.TrimSpace(req.EntityName)
		}
		if mapped, ok := idToName[strings.ToUpper(strings.TrimSpace(entityNameScoped))]; ok && strings.TrimSpace(mapped) != "" {
			entityNameScoped = strings.TrimSpace(mapped)
		}
		// Final fallback: look up entity_name by entity_id if needed.
		if entityNameScoped != "" {
			if _, ok := idToName[strings.ToUpper(strings.TrimSpace(entityNameScoped))]; ok {
				// already mapped
			} else {
				var lookedUpName string
				if err := pgxPool.QueryRow(ctx, `SELECT entity_name FROM masterentitycash WHERE entity_id = $1 LIMIT 1`, entityNameScoped).Scan(&lookedUpName); err == nil {
					if strings.TrimSpace(lookedUpName) != "" {
						entityNameScoped = strings.TrimSpace(lookedUpName)
					}
				} else if err := pgxPool.QueryRow(ctx, `SELECT entity_name FROM masterentity WHERE entity_id = $1 LIMIT 1`, entityNameScoped).Scan(&lookedUpName); err == nil {
					if strings.TrimSpace(lookedUpName) != "" {
						entityNameScoped = strings.TrimSpace(lookedUpName)
					}
				}
			}
		}

		// Scope check (critical): must be within entities granted by middleware.
		// NOTE: middleware provides BOTH entity names and entity IDs; some tables may store entity_id in `entity_name`.
		allowed := false
		if strings.TrimSpace(entityNameScoped) != "" && api.IsEntityAllowed(ctx, entityNameScoped) {
			allowed = true
		} else if strings.TrimSpace(resolvedEntity) != "" && api.IsEntityAllowed(ctx, resolvedEntity) {
			allowed = true
		} else if strings.TrimSpace(req.EntityName) != "" && api.IsEntityAllowed(ctx, req.EntityName) {
			allowed = true
		}
		if !allowed {
			api.RespondWithError(w, http.StatusForbidden, "not allowed for this entity")
			return
		}

		// Re-resolve folio/demat under the scoped entity to avoid collisions (e.g., folio_number reused).
		folioIdentifier := strings.TrimSpace(folioIDRaw.String)
		if folioIdentifier == "" {
			folioIdentifier = strings.TrimSpace(folioNumber)
		}
		if folioIdentifier != "" {
			var fID, fNum, fRed string
			if err := pgxPool.QueryRow(ctx, `
				SELECT folio_id::text, folio_number, COALESCE(default_redemption_account,'')
				FROM investment.masterfolio
				WHERE COALESCE(is_deleted,false)=false
					AND LOWER(TRIM(entity_name)) = LOWER(TRIM($1))
					AND (folio_id::text = $2 OR folio_number = $2 OR folio_number = $3)
				LIMIT 1
			`, entityNameScoped, folioIdentifier, strings.TrimSpace(folioNumber)).Scan(&fID, &fNum, &fRed); err == nil {
				if strings.TrimSpace(fID) != "" {
					resolvedFolioID = strings.TrimSpace(fID)
				}
				if strings.TrimSpace(fNum) != "" {
					folioNumber = strings.TrimSpace(fNum)
				}
				if strings.TrimSpace(fRed) != "" {
					defaultRedAcctRaw = strings.TrimSpace(fRed)
				}
			}
		}

		dematIdentifier := strings.TrimSpace(dematIDRaw.String)
		if dematIdentifier == "" {
			dematIdentifier = strings.TrimSpace(dematAccountNumber)
		}
		if dematIdentifier != "" {
			var dID, dNum, dSettle string
			if err := pgxPool.QueryRow(ctx, `
				SELECT demat_id::text, demat_account_number, COALESCE(default_settlement_account,'')
				FROM investment.masterdemataccount
				WHERE COALESCE(is_deleted,false)=false
					AND LOWER(TRIM(entity_name)) = LOWER(TRIM($1))
					AND (demat_id::text = $2 OR demat_account_number = $2)
				LIMIT 1
			`, entityNameScoped, dematIdentifier).Scan(&dID, &dNum, &dSettle); err == nil {
				if strings.TrimSpace(dID) != "" {
					resolvedDematID = strings.TrimSpace(dID)
				}
				if strings.TrimSpace(dNum) != "" {
					dematAccountNumber = strings.TrimSpace(dNum)
				}
				if strings.TrimSpace(dSettle) != "" {
					defaultSettleAcctRaw = strings.TrimSpace(dSettle)
				}
			}
		}

		// Resolve default accounts to *account numbers* (never ids)
		defaultRedAcct := resolveMasterBankAccountNumber(ctx, pgxPool, entityNameScoped, defaultRedAcctRaw)
		defaultSettleAcct := resolveMasterBankAccountNumber(ctx, pgxPool, entityNameScoped, defaultSettleAcctRaw)
		creditBankAccount := ""
		if strings.TrimSpace(folioNumber) != "" {
			creditBankAccount = defaultRedAcct
		} else if strings.TrimSpace(dematAccountNumber) != "" {
			creditBankAccount = defaultSettleAcct
		} else if strings.TrimSpace(defaultRedAcct) != "" {
			creditBankAccount = defaultRedAcct
		} else {
			creditBankAccount = defaultSettleAcct
		}

		// Holding snapshot (best-effort)
		var (
			snapTotalUnits    float64
			snapAvgNav        float64
			snapCurrentNav    float64
			snapCurrentValue  float64
			snapTotalInvested float64
			snapGainLoss      float64
			snapGainLossPct   float64
		)
		folioID := sql.NullString{String: resolvedFolioID, Valid: strings.TrimSpace(resolvedFolioID) != ""}
		dematID := sql.NullString{String: resolvedDematID, Valid: strings.TrimSpace(resolvedDematID) != ""}
		folioNumArg := nullIfEmptyString(folioNumber)
		dematNumArg := nullIfEmptyString(dematAccountNumber)
		_ = pgxPool.QueryRow(ctx, `
			SELECT
				COALESCE(total_units,0),
				COALESCE(avg_nav,0),
				COALESCE(current_nav,0),
				COALESCE(current_value,0),
				COALESCE(total_invested_amount,0),
				COALESCE(gain_loss,0),
				COALESCE(gain_losss_percent,0)
			FROM investment.portfolio_snapshot
			WHERE entity_name=$1 AND scheme_id=$2
				AND ((folio_id IS NOT NULL AND folio_id=$3) OR (demat_id IS NOT NULL AND demat_id=$4)
					OR (folio_number IS NOT NULL AND folio_number=$5) OR (demat_acc_number IS NOT NULL AND demat_acc_number=$6))
			ORDER BY created_at DESC
			LIMIT 1
		`, entityNameScoped, resolvedSchemeID, folioID, dematID, folioNumArg, dematNumArg).Scan(
			&snapTotalUnits, &snapAvgNav, &snapCurrentNav, &snapCurrentValue, &snapTotalInvested, &snapGainLoss, &snapGainLossPct,
		)

		// Blocked units: freeze units for all open (pending/approved) redemptions on the same holding.
		// This is derived from redemption_initiation (not onboard_transaction.blocked_units).
		var blockedByRedemptions float64
		_ = pgxPool.QueryRow(ctx, `
			WITH latest AS (
				SELECT DISTINCT ON (redemption_id)
					redemption_id,
					processing_status,
					requested_at
				FROM investment.auditactionredemption
				ORDER BY redemption_id, requested_at DESC
			)
			SELECT COALESCE(SUM(COALESCE(ri.by_units,0)),0)
			FROM investment.redemption_initiation ri
			JOIN latest l ON l.redemption_id = ri.redemption_id
			WHERE COALESCE(ri.is_deleted,false)=false
				AND UPPER(COALESCE(l.processing_status,'')) IN ('PENDING_APPROVAL','APPROVED')
				AND LOWER(TRIM(COALESCE(ri.entity_name,''))) = LOWER(TRIM($1))
				AND ri.scheme_id = $2
				AND (
					($3::text IS NOT NULL AND (ri.folio_id = $3 OR ri.folio_id = $5))
					OR ($4::text IS NOT NULL AND (ri.demat_id = $4 OR ri.demat_id = $6))
				)
		`, entityNameScoped, resolvedSchemeID,
			nullIfEmptyString(resolvedFolioID), nullIfEmptyString(resolvedDematID),
			nullIfEmptyString(folioNumber), nullIfEmptyString(dematAccountNumber)).Scan(&blockedByRedemptions)

		// Buy/Sell transaction breakdown for "from where" visibility.
		buyLots := make([]map[string]any, 0, 200)
		sellTxs := make([]map[string]any, 0, 100)
		var totalBlockedUnits float64
		var totalSellUnits float64

		buyQ := `
			SELECT
				ot.id,
				ot.batch_id,
				TO_CHAR(ot.transaction_date, 'YYYY-MM-DD') AS transaction_date,
				ot.transaction_type,
				COALESCE(ot.amount,0) AS amount,
				COALESCE(ot.units,0) AS units,
				COALESCE(ot.nav,0) AS nav,
				COALESCE(ot.blocked_units,0) AS blocked_units,
				COALESCE(ot.folio_number,'') AS folio_number,
				COALESCE(ot.demat_acc_number,'') AS demat_acc_number
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.portfolio_snapshot ps ON ps.batch_id = ot.batch_id
			LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id)
			WHERE (COALESCE(ot.entity_name,'') = $1 OR ps.entity_name = $1)
				AND LOWER(COALESCE(ot.transaction_type,'')) IN ('buy','purchase','subscription')
				AND (( $2::text IS NOT NULL AND ot.folio_number = $2) OR ($3::text IS NOT NULL AND ot.demat_acc_number = $3))
				AND ( ot.scheme_id = $4 OR ot.scheme_internal_code = $5 OR ms.isin = $6 OR ms.scheme_name = $7 )
			ORDER BY ot.transaction_date ASC, ot.id ASC
		`
		rows, err := pgxPool.Query(ctx, buyQ, entityNameScoped, nullIfEmptyString(folioNumber), nullIfEmptyString(dematAccountNumber), resolvedSchemeID, resolvedSchemeID, isin, schemeName)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var (
					id      int64
					batchID string
					txDate  string
					txType  string
					amount  float64
					units   float64
					nav     float64
					blocked float64
					fn      string
					dn      string
				)
				if err := rows.Scan(&id, &batchID, &txDate, &txType, &amount, &units, &nav, &blocked, &fn, &dn); err != nil {
					continue
				}
				// We'll allocate blocked units across lots (FIFO) after reading all lots.
				_ = blocked
				avail := units
				buyLots = append(buyLots, map[string]any{
					"id":               id,
					"batch_id":         batchID,
					"transaction_date": txDate,
					"transaction_type": txType,
					"folio_number":     fn,
					"demat_acc_number": dn,
					"amount":           amount,
					"units":            units,
					"nav":              nav,
					"blocked_units":    0.0,
					"available_units":  avail,
				})
			}
		}

		// Allocate blocked units across buy lots FIFO.
		blockedRemaining := blockedByRedemptions
		for i := range buyLots {
			if blockedRemaining <= 0 {
				break
			}
			u, _ := buyLots[i]["units"].(float64)
			if u <= 0 {
				continue
			}
			b := blockedRemaining
			if b > u {
				b = u
			}
			buyLots[i]["blocked_units"] = b
			buyLots[i]["available_units"] = u - b
			totalBlockedUnits += b
			blockedRemaining -= b
		}

		sellQ := `
			SELECT
				ot.id,
				ot.batch_id,
				TO_CHAR(ot.transaction_date, 'YYYY-MM-DD') AS transaction_date,
				ot.transaction_type,
				COALESCE(ot.amount,0) AS amount,
				COALESCE(ot.units,0) AS units,
				COALESCE(ot.nav,0) AS nav
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.portfolio_snapshot ps ON ps.batch_id = ot.batch_id
			LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id)
			WHERE (COALESCE(ot.entity_name,'') = $1 OR ps.entity_name = $1)
				AND LOWER(COALESCE(ot.transaction_type,'')) IN ('sell','redemption')
				AND (( $2::text IS NOT NULL AND ot.folio_number = $2) OR ($3::text IS NOT NULL AND ot.demat_acc_number = $3))
				AND ( ot.scheme_id = $4 OR ot.scheme_internal_code = $5 OR ms.isin = $6 OR ms.scheme_name = $7 )
			ORDER BY ot.transaction_date ASC, ot.id ASC
		`
		rows2, err := pgxPool.Query(ctx, sellQ, entityNameScoped, nullIfEmptyString(folioNumber), nullIfEmptyString(dematAccountNumber), resolvedSchemeID, resolvedSchemeID, isin, schemeName)
		if err == nil {
			defer rows2.Close()
			for rows2.Next() {
				var (
					id      int64
					batchID string
					txDate  string
					txType  string
					amount  float64
					units   float64
					nav     float64
				)
				if err := rows2.Scan(&id, &batchID, &txDate, &txType, &amount, &units, &nav); err != nil {
					continue
				}
				totalSellUnits += math.Abs(units)
				sellTxs = append(sellTxs, map[string]any{
					"id":               id,
					"batch_id":         batchID,
					"transaction_date": txDate,
					"transaction_type": txType,
					"amount":           amount,
					"units":            units,
					"nav":              nav,
				})
			}
		}

		// Confirmation progress
		confirmations := make([]map[string]any, 0, 50)
		var confirmedUnits float64
		confQ := `
			SELECT redemption_confirm_id, status, COALESCE(actual_units,0), COALESCE(actual_nav,0), COALESCE(gross_proceeds,0), COALESCE(net_credited,0)
			FROM investment.redemption_confirmation
			WHERE redemption_id = $1
			ORDER BY created_at DESC
		`
		cRows, err := pgxPool.Query(ctx, confQ, redemptionID)
		if err == nil {
			defer cRows.Close()
			for cRows.Next() {
				var rcID, st string
				var au, anav, gp, nc float64
				if err := cRows.Scan(&rcID, &st, &au, &anav, &gp, &nc); err != nil {
					continue
				}
				confirmedUnits += au
				confirmations = append(confirmations, map[string]any{
					"redemption_confirm_id": rcID,
					"status":                st,
					"actual_units":          au,
					"actual_nav":            anav,
					"gross_proceeds":        gp,
					"net_credited":          nc,
				})
			}
		}

		// Compose holding numbers (prefer snapshot, but always compute blocked/available from lots)
		holdingTotalUnits := snapTotalUnits
		if holdingTotalUnits == 0 {
			// Fallback to buy-sell net if snapshot missing
			var sumBuyUnits float64
			for _, lot := range buyLots {
				u, _ := lot["units"].(float64)
				sumBuyUnits += u
			}
			holdingTotalUnits = sumBuyUnits - totalSellUnits
			if holdingTotalUnits < 0 {
				holdingTotalUnits = 0
			}
		}
		availableUnits := holdingTotalUnits - totalBlockedUnits
		if availableUnits < 0 {
			availableUnits = 0
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"redemption": map[string]any{
				"redemption_id":              redemptionID,
				"entity_name":                entityNameScoped,
				"folio_id":                   strings.TrimSpace(folioIDRaw.String),
				"demat_id":                   strings.TrimSpace(dematIDRaw.String),
				"scheme_id":                  schemeIDRaw,
				"resolved_scheme_id":         resolvedSchemeID,
				"scheme_name":                schemeName,
				"scheme_code":                schemeCode,
				"isin":                       isin,
				"amc_name":                   amcName,
				"resolved_folio_id":          resolvedFolioID,
				"folio_number":               folioNumber,
				"resolved_demat_id":          resolvedDematID,
				"demat_account_number":       dematAccountNumber,
				"requested_by":               requestedBy,
				"requested_date":             requestedDate,
				"transaction_date":           transactionDate,
				"by_amount":                  byAmount,
				"by_units":                   byUnits,
				"method":                     method,
				"estimated_proceeds":         estimatedProceeds,
				"gain_loss":                  gainLoss,
				"default_redemption_account": defaultRedAcct,
				"default_settlement_account": defaultSettleAcct,
				"credit_bank_account":        creditBankAccount,
				"audit": map[string]any{
					"action_type":       actionType,
					"processing_status": processingStatus,
					"action_id":         actionID,
					"requested_by":      auditRequestedBy,
					"requested_at":      auditRequestedAt,
					"checker_by":        checkerBy,
					"checker_at":        checkerAt,
					"checker_comment":   checkerComment,
					"reason":            reason,
				},
			},
			"holding": map[string]any{
				"entity_name":           entityNameScoped,
				"folio_number":          folioNumber,
				"demat_acc_number":      dematAccountNumber,
				"scheme_id":             resolvedSchemeID,
				"scheme_name":           schemeName,
				"isin":                  isin,
				"total_units":           holdingTotalUnits,
				"blocked_units":         totalBlockedUnits,
				"available_units":       availableUnits,
				"avg_nav":               snapAvgNav,
				"current_nav":           snapCurrentNav,
				"current_value":         snapCurrentValue,
				"total_invested_amount": snapTotalInvested,
				"gain_loss":             snapGainLoss,
				"gain_loss_percent":     snapGainLossPct,
			},
			"buy_lots":      buyLots,
			"sell_txs":      sellTxs,
			"confirmations": confirmations,
			"summary": map[string]any{
				"requested_units":         byUnits,
				"requested_amount":        byAmount,
				"confirmed_units":         confirmedUnits,
				"already_redeemed_units":  totalSellUnits,
				"currently_blocked_units": totalBlockedUnits,
				"holding_total_units":     holdingTotalUnits,
				"holding_available_units": availableUnits,
			},
		})
	}
}

// ---------------------------
// Helper functions
// ---------------------------

func nullIfEmptyString(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func nullIfEmptyStringPtr(s *string) interface{} {
	if s == nil || strings.TrimSpace(*s) == "" {
		return nil
	}
	return *s
}

func nullIfZeroFloat(f float64) interface{} {
	if f == 0 {
		return nil
	}
	return f
}
