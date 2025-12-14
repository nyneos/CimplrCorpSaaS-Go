package redemption

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid or inactive session")
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
			api.RespondWithError(w, http.StatusInternalServerError, "rows scan failed: "+rows.Err().Error())
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
