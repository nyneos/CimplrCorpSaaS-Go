package redemption

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Request/Response Types for Redemption Confirmation
// ---------------------------

type CreateRedemptionConfirmationRequest struct {
	UserID                       string  `json:"user_id"`
	RedemptionID                 string  `json:"redemption_id"`
	ActualNAV                    float64 `json:"actual_nav"`
	ActualUnits                  float64 `json:"actual_units"`
	GrossProceeds                float64 `json:"gross_proceeds"`
	ExitLoad                     float64 `json:"exit_load,omitempty"`
	TDS                          float64 `json:"tds,omitempty"`
	NetCredited                  float64 `json:"net_credited"`
	STTCharges                   float64 `json:"stt_charges,omitempty"`
	ResolutionVariance           string  `json:"resolution_variance,omitempty"`
	ResolutionComment            string  `json:"resolution_comment,omitempty"`
	VarianceProceeds             float64 `json:"variance_proceeds,omitempty"`
	FinalRealisedCapitalGainLoss float64 `json:"final_realised_capital_gain_loss,omitempty"`
	Status                       string  `json:"status,omitempty"`
}

type UpdateRedemptionConfirmationRequest struct {
	UserID              string                 `json:"user_id"`
	RedemptionConfirmID string                 `json:"redemption_confirm_id"`
	Fields              map[string]interface{} `json:"fields"`
	Reason              string                 `json:"reason"`
}

// ---------------------------
// CreateRedemptionConfirmationSingle
// ---------------------------

func CreateRedemptionConfirmationSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateRedemptionConfirmationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		// Validate required fields
		if strings.TrimSpace(req.RedemptionID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_id is required")
			return
		}
		if req.ActualNAV <= 0 || req.ActualUnits <= 0 || req.GrossProceeds <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "actual_nav, actual_units, and gross_proceeds must be greater than 0")
			return
		}

		// Calculate net_credited if not provided
		if req.NetCredited <= 0 {
			req.NetCredited = req.GrossProceeds - req.ExitLoad - req.TDS - req.STTCharges
			if req.NetCredited < 0 {
				req.NetCredited = 0
			}
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

		// Validate: sum of confirmed units for this redemption_id shouldn't exceed initiation units
		var initiationUnits float64
		var existingConfirmedUnits float64
		if err := tx.QueryRow(ctx, `
			SELECT 
				COALESCE(ri.by_units, 0) AS initiation_units,
				COALESCE(SUM(rc.actual_units), 0) AS existing_confirmed_units
			FROM investment.redemption_initiation ri
			LEFT JOIN investment.redemption_confirmation rc ON rc.redemption_id = ri.redemption_id
				AND COALESCE(rc.is_deleted, false) = false
			WHERE ri.redemption_id = $1
			GROUP BY ri.redemption_id, ri.by_units
		`, req.RedemptionID).Scan(&initiationUnits, &existingConfirmedUnits); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to validate redemption: "+err.Error())
			return
		}

		if existingConfirmedUnits+req.ActualUnits > initiationUnits {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("Cannot confirm %.2f units. Initiation has %.2f units, already confirmed %.2f units. Only %.2f units available.",
				req.ActualUnits, initiationUnits, existingConfirmedUnits, initiationUnits-existingConfirmedUnits))
			return
		}

		status := "PENDING_CONFIRMATION"
		if strings.TrimSpace(req.Status) != "" {
			status = req.Status
		}

		insertQ := `
			INSERT INTO investment.redemption_confirmation (
				redemption_id, actual_nav, actual_units, gross_proceeds,
				exit_load, tds, net_credited, stt_charges, resolution_variance, resolution_comment,
				variance_proceeds, final_realised_capital_gain_loss, status
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			RETURNING redemption_confirm_id
		`
		var confirmID string
		if err := tx.QueryRow(ctx, insertQ,
			req.RedemptionID,
			req.ActualNAV,
			req.ActualUnits,
			req.GrossProceeds,
			req.ExitLoad,
			req.TDS,
			req.NetCredited,
			nullIfZeroFloat(req.STTCharges),
			req.ResolutionVariance,
			req.ResolutionComment,
			nullIfZeroFloat(req.VarianceProceeds),
			nullIfZeroFloat(req.FinalRealisedCapitalGainLoss),
			status,
		).Scan(&confirmID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// Create audit trail
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionredemptionconfirmation (redemption_confirm_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, confirmID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"redemption_confirm_id": confirmID,
			"redemption_id":         req.RedemptionID,
			"requested":             userEmail,
		})
	}
}

// ---------------------------
// CreateRedemptionConfirmationBulk
// ---------------------------

func CreateRedemptionConfirmationBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				RedemptionID                 string  `json:"redemption_id"`
				ActualNAV                    float64 `json:"actual_nav"`
				ActualUnits                  float64 `json:"actual_units"`
				GrossProceeds                float64 `json:"gross_proceeds"`
				ExitLoad                     float64 `json:"exit_load,omitempty"`
				TDS                          float64 `json:"tds,omitempty"`
				NetCredited                  float64 `json:"net_credited"`
				STTCharges                   float64 `json:"stt_charges,omitempty"`
				ResolutionVariance           string  `json:"resolution_variance,omitempty"`
				ResolutionComment            string  `json:"resolution_comment,omitempty"`
				VarianceProceeds             float64 `json:"variance_proceeds,omitempty"`
				FinalRealisedCapitalGainLoss float64 `json:"final_realised_capital_gain_loss,omitempty"`
				Status                       string  `json:"status,omitempty"`
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
			if strings.TrimSpace(row.RedemptionID) == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "redemption_id is required"})
				continue
			}
			if row.ActualNAV <= 0 || row.ActualUnits <= 0 || row.GrossProceeds <= 0 {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "actual_nav, actual_units, and gross_proceeds must be > 0"})
				continue
			}

			// Calculate net_credited if not provided
			if row.NetCredited <= 0 {
				row.NetCredited = row.GrossProceeds - row.ExitLoad - row.TDS - row.STTCharges
				if row.NetCredited < 0 {
					row.NetCredited = 0
				}
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxBeginFailed + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			// Validate: sum of confirmed units for this redemption_id shouldn't exceed initiation units
			var initiationUnits float64
			var existingConfirmedUnits float64
			if err := tx.QueryRow(ctx, `
				SELECT 
					COALESCE(ri.by_units, 0) AS initiation_units,
					COALESCE(SUM(rc.actual_units), 0) AS existing_confirmed_units
				FROM investment.redemption_initiation ri
				LEFT JOIN investment.redemption_confirmation rc ON rc.redemption_id = ri.redemption_id
					AND COALESCE(rc.is_deleted, false) = false
				WHERE ri.redemption_id = $1
				GROUP BY ri.redemption_id, ri.by_units
			`, row.RedemptionID).Scan(&initiationUnits, &existingConfirmedUnits); err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Failed to validate redemption: " + err.Error()})
				continue
			}

			if existingConfirmedUnits+row.ActualUnits > initiationUnits {
				results = append(results, map[string]interface{}{"success": false, "error": fmt.Sprintf("Cannot confirm %.2f units. Initiation has %.2f units, already confirmed %.2f units.",
					row.ActualUnits, initiationUnits, existingConfirmedUnits)})
				continue
			}

			status := "PENDING_CONFIRMATION"
			if strings.TrimSpace(row.Status) != "" {
				status = row.Status
			}

			var confirmID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.redemption_confirmation (
					redemption_id, actual_nav, actual_units, gross_proceeds,
					exit_load, tds, net_credited, stt_charges, resolution_variance, resolution_comment,
					variance_proceeds, final_realised_capital_gain_loss, status
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				RETURNING redemption_confirm_id
			`, row.RedemptionID, row.ActualNAV, row.ActualUnits, row.GrossProceeds,
				row.ExitLoad, row.TDS, row.NetCredited, nullIfZeroFloat(row.STTCharges), row.ResolutionVariance, row.ResolutionComment, nullIfZeroFloat(row.VarianceProceeds), nullIfZeroFloat(row.FinalRealisedCapitalGainLoss), status).Scan(&confirmID); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "Insert failed: " + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionredemptionconfirmation (redemption_confirm_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
			`, confirmID, userEmail); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrAuditInsertFailed + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrCommitFailedCapitalized + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{
				constants.ValueSuccess:  true,
				"redemption_confirm_id": confirmID,
				"redemption_id":         row.RedemptionID,
			})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// UpdateRedemptionConfirmation
// ---------------------------

func UpdateRedemptionConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateRedemptionConfirmationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.RedemptionConfirmID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_confirm_id required")
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

		// Fetch existing values
		sel := `
			SELECT redemption_id, actual_nav, actual_units, gross_proceeds,
				   exit_load, tds, net_credited, status, stt_charges, resolution_variance, resolution_comment,
				   variance_proceeds, final_realised_capital_gain_loss
			FROM investment.redemption_confirmation
			WHERE redemption_confirm_id=$1
			FOR UPDATE
		`
		var oldVals [13]interface{}
		if err := tx.QueryRow(ctx, sel, req.RedemptionConfirmID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9], &oldVals[10], &oldVals[11], &oldVals[12],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"redemption_id":                    0,
			"actual_nav":                       1,
			"actual_units":                     2,
			"gross_proceeds":                   3,
			"exit_load":                        4,
			"tds":                              5,
			"net_credited":                     6,
			constants.KeyStatus:                7,
			"stt_charges":                      8,
			"resolution_variance":              9,
			"resolution_comment":               10,
			"variance_proceeds":                11,
			"final_realised_capital_gain_loss": 12,
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

		q := fmt.Sprintf("UPDATE investment.redemption_confirmation SET %s, updated_at=now() WHERE redemption_confirm_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.RedemptionConfirmID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		// Audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactionredemptionconfirmation (redemption_confirm_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
		`, req.RedemptionConfirmID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"redemption_confirm_id": req.RedemptionConfirmID,
			"requested":             userEmail,
		})
	}
}

// ---------------------------
// UpdateRedemptionConfirmationBulk
// ---------------------------

func UpdateRedemptionConfirmationBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				RedemptionConfirmID string                 `json:"redemption_confirm_id"`
				Fields              map[string]interface{} `json:"fields"`
				Reason              string                 `json:"reason"`
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
			if row.RedemptionConfirmID == "" {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "redemption_confirm_id missing"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_confirm_id": row.RedemptionConfirmID, constants.ValueError: constants.ErrTxBeginFailedCapitalized + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			sel := `
				SELECT redemption_id, actual_nav, actual_units, gross_proceeds,
					   exit_load, tds, net_credited, status, stt_charges, resolution_variance, resolution_comment,
					   variance_proceeds, final_realised_capital_gain_loss
				FROM investment.redemption_confirmation WHERE redemption_confirm_id=$1 FOR UPDATE`
			var oldVals [13]interface{}
			if err := tx.QueryRow(ctx, sel, row.RedemptionConfirmID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
				&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9], &oldVals[10], &oldVals[11], &oldVals[12],
			); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_confirm_id": row.RedemptionConfirmID, constants.ValueError: "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"redemption_id":                    0,
				"actual_nav":                       1,
				"actual_units":                     2,
				"gross_proceeds":                   3,
				"exit_load":                        4,
				"tds":                              5,
				"net_credited":                     6,
				constants.KeyStatus:                7,
				"stt_charges":                      8,
				"resolution_variance":              9,
				"resolution_comment":               10,
				"variance_proceeds":                11,
				"final_realised_capital_gain_loss": 12,
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
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_confirm_id": row.RedemptionConfirmID, constants.ValueError: "No valid fields"})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.redemption_confirmation SET %s, updated_at=now() WHERE redemption_confirm_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.RedemptionConfirmID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_confirm_id": row.RedemptionConfirmID, constants.ValueError: constants.ErrUpdateFailed + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionredemptionconfirmation (redemption_confirm_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
			`, row.RedemptionConfirmID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_confirm_id": row.RedemptionConfirmID, constants.ValueError: constants.ErrAuditInsertFailed + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_confirm_id": row.RedemptionConfirmID, constants.ValueError: constants.ErrCommitFailed + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{constants.ValueSuccess: true, "redemption_confirm_id": row.RedemptionConfirmID, "requested": userEmail})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// DeleteRedemptionConfirmation
// ---------------------------

func DeleteRedemptionConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string   `json:"user_id"`
			RedemptionConfirmIDs []string `json:"redemption_confirm_ids"`
			Reason               string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.RedemptionConfirmIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_confirm_ids required")
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

		for _, id := range req.RedemptionConfirmIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactionredemptionconfirmation (redemption_confirm_id, actiontype, processing_status, reason, requested_by, requested_at)
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
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.RedemptionConfirmIDs})
	}
}

// ---------------------------
// BulkApproveRedemptionConfirmationActions
// ---------------------------

func BulkApproveRedemptionConfirmationActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string   `json:"user_id"`
			RedemptionConfirmIDs []string `json:"redemption_confirm_ids"`
			Comment              string   `json:"comment"`
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
			SELECT DISTINCT ON (redemption_confirm_id) action_id, redemption_confirm_id, actiontype, processing_status
			FROM investment.auditactionredemptionconfirmation
			WHERE redemption_confirm_id = ANY($1)
			ORDER BY redemption_confirm_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.RedemptionConfirmIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var toApprove []string
		var toApproveConfirmIDs []string
		var toDeleteActionIDs []string
		var deleteMasterIDs []string

		for rows.Next() {
			var aid, cid, atype, pstatus string
			if err := rows.Scan(&aid, &cid, &atype, &pstatus); err != nil {
				continue
			}
			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == "APPROVED" {
				continue
			}
			if ps == "PENDING_DELETE_APPROVAL" {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, cid)
				continue
			}
			if ps == "PENDING_APPROVAL" || ps == "PENDING_EDIT_APPROVAL" {
				toApprove = append(toApprove, aid)
				toApproveConfirmIDs = append(toApproveConfirmIDs, cid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrNoApprovableActions, map[string]any{
				"approved_action_ids":              []string{},
				"deleted_redemption_confirmations": []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionredemptionconfirmation
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approve failed: "+err.Error())
				return
			}

			// Update redemption_confirmation status to CONFIRMED
			if _, err := tx.Exec(ctx, `
				UPDATE investment.redemption_confirmation
				SET status='CONFIRMED'
				WHERE redemption_confirm_id = ANY($1)
			`, toApproveConfirmIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "update confirmation status failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactionredemptionconfirmation
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete action update failed: "+err.Error())
				return
			}

			// Release blocked units for deleted confirmations
			for _, confirmID := range deleteMasterIDs {
				// Fetch confirmation and initiation details
				var redemptionID string
				var actualUnits float64
				if err := tx.QueryRow(ctx, `
					SELECT redemption_id, actual_units
					FROM investment.redemption_confirmation
					WHERE redemption_confirm_id = $1
				`, confirmID).Scan(&redemptionID, &actualUnits); err != nil {
					continue
				}

				if actualUnits <= 0 {
					continue
				}

				// Get initiation details for unblocking
				var folioID, dematID, schemeID sql.NullString
				var method string
				if err := tx.QueryRow(ctx, `
					SELECT folio_id, demat_id, scheme_id, method
					FROM investment.redemption_initiation
					WHERE redemption_id = $1
				`, redemptionID).Scan(&folioID, &dematID, &schemeID, &method); err != nil {
					continue
				}

				// Release blocked units
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
					actualUnits,
				); err != nil {
					// Log error but continue
				}
			}

			if _, err := tx.Exec(ctx, `
				UPDATE investment.redemption_confirmation
				SET is_deleted=true, status='DELETED', updated_at=now()
				WHERE redemption_confirm_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete confirmation failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		// Automatically process confirmed redemptions (create SELL transactions and refresh snapshots)
		var confirmResult map[string]any
		if len(toApproveConfirmIDs) > 0 {
			confirmResult, err = processRedemptionConfirmations(pgxPool, ctx, req.UserID, checkerBy, toApproveConfirmIDs)
			if err != nil {
				// Log the error but don't fail the approval - confirmations can be reprocessed manually
				api.RespondWithPayload(w, true, "Approved but confirmation processing failed: "+err.Error(), map[string]any{
					"approved_action_ids":              toApprove,
					"deleted_redemption_confirmations": deleteMasterIDs,
					"confirmation_error":               err.Error(),
				})
				return
			}
		}

		response := map[string]any{
			"approved_action_ids":              toApprove,
			"deleted_redemption_confirmations": deleteMasterIDs,
		}
		if confirmResult != nil {
			response["confirmation_processing"] = confirmResult
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// ---------------------------
// BulkRejectRedemptionConfirmationActions
// ---------------------------

func BulkRejectRedemptionConfirmationActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID               string   `json:"user_id"`
			RedemptionConfirmIDs []string `json:"redemption_confirm_ids"`
			Comment              string   `json:"comment"`
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
			SELECT DISTINCT ON (redemption_confirm_id) action_id, redemption_confirm_id, processing_status
			FROM investment.auditactionredemptionconfirmation
			WHERE redemption_confirm_id = ANY($1)
			ORDER BY redemption_confirm_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.RedemptionConfirmIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := []string{}
		cannotReject := []string{}
		found := map[string]bool{}
		for rows.Next() {
			var aid, cid, ps string
			if err := rows.Scan(&aid, &cid, &ps); err != nil {
				continue
			}
			found[cid] = true
			if strings.ToUpper(strings.TrimSpace(ps)) == "APPROVED" {
				cannotReject = append(cannotReject, cid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		missing := []string{}
		for _, id := range req.RedemptionConfirmIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for redemption_confirm_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved redemption_confirm_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactionredemptionconfirmation
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

// ---------------------------
// GetRedemptionConfirmationsWithAudit
// ---------------------------

func GetRedemptionConfirmationsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.redemption_confirm_id)
					a.redemption_confirm_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactionredemptionconfirmation a
				ORDER BY a.redemption_confirm_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					redemption_confirm_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactionredemptionconfirmation
				GROUP BY redemption_confirm_id
			),
			resolved_folio AS (
				SELECT DISTINCT ON (i.redemption_id)
					i.redemption_id,
					f.folio_number,
					f.folio_id::text AS folio_id_text
				FROM investment.redemption_initiation i
				LEFT JOIN investment.masterfolio f ON (
					(f.folio_id::text = i.folio_id) OR 
					(i.folio_id IS NOT NULL AND f.folio_number = i.folio_id)
				)
				ORDER BY i.redemption_id, f.folio_id
			),
			resolved_demat AS (
				SELECT DISTINCT ON (i.redemption_id)
					i.redemption_id,
					d.demat_account_number,
					d.demat_id::text AS demat_id_text
				FROM investment.redemption_initiation i
				LEFT JOIN investment.masterdemataccount d ON (
					(d.demat_id::text = i.demat_id) OR 
					(i.demat_id IS NOT NULL AND d.default_settlement_account = i.demat_id) OR 
					(i.demat_id IS NOT NULL AND d.demat_account_number = i.demat_id)
				)
				ORDER BY i.redemption_id, d.demat_id
			)
			SELECT
				m.redemption_confirm_id,
				m.redemption_id,
				m.old_redemption_id,
				m.actual_nav,
				m.old_actual_nav,
				m.actual_units,
				m.old_actual_units,
				m.gross_proceeds,
				m.old_gross_proceeds,
				m.exit_load,
				m.old_exit_load,
				m.tds,
				m.old_tds,
				m.net_credited,
				m.old_net_credited,
				m.stt_charges,
				m.old_stt_charges,
				COALESCE(m.resolution_variance,'') AS resolution_variance,
				COALESCE(m.old_resolution_variance,'') AS old_resolution_variance,
				COALESCE(m.resolution_comment,'') AS resolution_comment,
				COALESCE(m.old_resolution_comment,'') AS old_resolution_comment,
				m.variance_proceeds,
				m.old_variance_proceeds,
				m.final_realised_capital_gain_loss,
				m.old_final_realised_capital_gain_loss,
				m.status,
				m.old_status,
				m.confirmed_by,
				TO_CHAR(m.confirmed_at, 'YYYY-MM-DD HH24:MI:SS') AS confirmed_at,
				m.is_deleted,
				TO_CHAR(m.updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
				
				-- initiation fields
				TO_CHAR(i.requested_date, 'YYYY-MM-DD') AS initiation_requested_date,
				i.entity_name AS initiation_entity_name,
				COALESCE(s.scheme_id::text, i.scheme_id::text) AS initiation_scheme_id,
				COALESCE(s.scheme_name, i.scheme_id) AS initiation_scheme_name,
				COALESCE(s.internal_scheme_code,'') AS initiation_scheme_code,
				COALESCE(s.isin,'') AS initiation_isin,
				COALESCE(s.amc_name,'') AS initiation_amc_name,
				COALESCE(rf.folio_number,'') AS initiation_folio_number,
				COALESCE(rf.folio_id_text,'') AS initiation_folio_id,
				COALESCE(rd.demat_account_number,'') AS initiation_demat_number,
				COALESCE(rd.demat_id_text,'') AS initiation_demat_id,
				COALESCE(i.by_amount,0) AS initiation_by_amount,
				COALESCE(i.by_units,0) AS initiation_by_units,
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
			FROM investment.redemption_confirmation m
			LEFT JOIN latest_audit l ON l.redemption_confirm_id = m.redemption_confirm_id
			LEFT JOIN history h ON h.redemption_confirm_id = m.redemption_confirm_id
			LEFT JOIN investment.redemption_initiation i ON i.redemption_id = m.redemption_id
			LEFT JOIN investment.masterscheme s ON (
			    s.scheme_id::text = i.scheme_id
			 OR s.scheme_name = i.scheme_id
			 OR s.internal_scheme_code = i.scheme_id
			 OR s.isin = i.scheme_id
			)
			LEFT JOIN resolved_folio rf ON rf.redemption_id = i.redemption_id
			LEFT JOIN resolved_demat rd ON rd.redemption_id = i.redemption_id
			WHERE COALESCE(m.is_deleted, false) = false
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC
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
// GetApprovedRedemptionConfirmations
// ---------------------------

func GetApprovedRedemptionConfirmations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest AS (
				SELECT DISTINCT ON (redemption_confirm_id)
					redemption_confirm_id,
					processing_status
				FROM investment.auditactionredemptionconfirmation
				ORDER BY redemption_confirm_id, requested_at DESC
			)
			SELECT
				m.redemption_confirm_id,
				m.redemption_id,
				m.actual_nav,
				m.actual_units,
				m.gross_proceeds,
				m.exit_load,
				m.tds,
				m.net_credited,
				m.stt_charges,
				COALESCE(m.resolution_variance, ''),
				COALESCE(m.resolution_comment, ''),
				m.variance_proceeds,
				m.final_realised_capital_gain_loss,
				m.status
			FROM investment.redemption_confirmation m
			JOIN latest l ON l.redemption_confirm_id = m.redemption_confirm_id
			WHERE 
				UPPER(l.processing_status) = 'APPROVED'
				AND COALESCE(m.is_deleted,false)=false
			ORDER BY m.updated_at DESC;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var confirmID, redemptionID, status string
			var actualNAV, actualUnits, grossProceeds, exitLoad, tds, netCredited float64
			var sttCharges, varianceProceeds, finalRealised float64
			var resolutionVariance, resolutionComment string
			_ = rows.Scan(&confirmID, &redemptionID, &actualNAV, &actualUnits, &grossProceeds,
				&exitLoad, &tds, &netCredited, &sttCharges, &resolutionVariance, &resolutionComment, &varianceProceeds, &finalRealised, &status)

			out = append(out, map[string]interface{}{
				"redemption_confirm_id":            confirmID,
				"redemption_id":                    redemptionID,
				"actual_nav":                       actualNAV,
				"actual_units":                     actualUnits,
				"gross_proceeds":                   grossProceeds,
				"exit_load":                        exitLoad,
				"tds":                              tds,
				"net_credited":                     netCredited,
				"stt_charges":                      sttCharges,
				"resolution_variance":              resolutionVariance,
				"resolution_comment":               resolutionComment,
				"variance_proceeds":                varianceProceeds,
				"final_realised_capital_gain_loss": finalRealised,
				constants.KeyStatus:                status,
			})
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// ---------------------------
// processRedemptionConfirmations - Internal helper to process confirmations
// ---------------------------

func processRedemptionConfirmations(pgxPool *pgxpool.Pool, ctx context.Context, userID string, confirmedBy string, redemptionConfirmationIDs []string) (map[string]any, error) {
	tx, err := pgxPool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("tx begin failed: %w", err)
	}
	defer tx.Rollback(ctx)

	// Use a transient batch ID (do not persist an `onboard_batch` row)
	batchID := uuid.New().String()

	// Fetch redemption confirmation details with related redemption initiation and scheme info
	// NOTE: redemption_initiation stores string identifiers in scheme_id/folio_id/demat_id, not UUIDs
	// We need to resolve these to actual master table IDs
	fetchQ := `
		SELECT 
			rc.redemption_confirm_id,
			rc.redemption_id,
			rc.actual_nav,
			rc.actual_units,
			rc.gross_proceeds,
			rc.exit_load,
			rc.tds,
			rc.net_credited,
			
			mr.folio_id AS folio_identifier,
			mr.demat_id AS demat_identifier,
			mr.scheme_id AS scheme_identifier,
			mr.requested_by,
			mr.requested_date,
			mr.by_amount,
			mr.by_units,
			mr.method,
			mr.entity_name,
			
			-- Resolve actual scheme details
			COALESCE(s.scheme_id::text, s2.scheme_id::text, s3.scheme_id::text, s4.scheme_id::text) AS resolved_scheme_id,
			COALESCE(s.scheme_name, s2.scheme_name, s3.scheme_name, s4.scheme_name) AS scheme_name,
			COALESCE(s.isin, s2.isin, s3.isin, s4.isin) AS isin,
			COALESCE(s.internal_scheme_code, s2.internal_scheme_code, s3.internal_scheme_code, s4.internal_scheme_code) AS internal_scheme_code,
			-- Resolve actual folio details
			f.folio_id::text AS resolved_folio_id,
			f.folio_number,
			f.entity_name AS folio_entity_name,
			-- Resolve actual demat details
			d.demat_id::text AS resolved_demat_id,
			d.demat_account_number,
			d.entity_name AS demat_entity_name
		FROM investment.redemption_confirmation rc
		JOIN investment.redemption_initiation mr ON rc.redemption_id = mr.redemption_id
		-- Resolve scheme by trying multiple match strategies
		LEFT JOIN investment.masterscheme s ON s.scheme_id::text = mr.scheme_id
		LEFT JOIN investment.masterscheme s2 ON s2.scheme_name = mr.scheme_id
		LEFT JOIN investment.masterscheme s3 ON s3.internal_scheme_code = mr.scheme_id
		LEFT JOIN investment.masterscheme s4 ON s4.isin = mr.scheme_id
		-- Resolve folio by trying folio_id or folio_number
		LEFT JOIN investment.masterfolio f ON (f.folio_id::text = mr.folio_id OR f.folio_number = mr.folio_id)
		-- Resolve demat by trying demat_id or demat_account_number
		LEFT JOIN investment.masterdemataccount d ON (
			d.demat_id::text = mr.demat_id OR 
			d.demat_account_number = mr.demat_id OR
			d.default_settlement_account = mr.demat_id
		)
		WHERE rc.redemption_confirm_id = ANY($1)
			AND rc.status = 'CONFIRMED'
	`

	rows, err := tx.Query(ctx, fetchQ, redemptionConfirmationIDs)
	if err != nil {
		return nil, fmt.Errorf("Fetch confirmations failed: %w", err)
	}
	defer rows.Close()

	type confirmationData struct {
		RedemptionConfirmID string
		RedemptionID        string
		ActualNAV           float64
		ActualUnits         float64
		GrossProceeds       float64
		ExitLoad            float64
		TDS                 float64
		NetCredited         float64
		FolioIdentifier     *string // String from initiation
		DematIdentifier     *string // String from initiation
		SchemeIdentifier    string  // String from initiation
		RequestedBy         string
		RequestedDate       time.Time
		ByAmount            *float64
		ByUnits             *float64
		Method              string
		EntityName          *string
		ResolvedSchemeID    *string // Actual scheme_id from masterscheme (string/varchar)
		SchemeName          *string
		ISIN                *string
		InternalSchemeCode  *string
		ResolvedFolioID     *string // Actual folio_id from masterfolio (string/varchar)
		FolioNumber         *string
		FolioEntityName     *string
		ResolvedDematID     *string // Actual demat_id from masterdemataccount (string/varchar)
		DematAccountNumber  *string
		DematEntityName     *string
	}

	confirmations := []confirmationData{}
	for rows.Next() {
		var cd confirmationData
		if err := rows.Scan(
			&cd.RedemptionConfirmID,
			&cd.RedemptionID,
			&cd.ActualNAV,
			&cd.ActualUnits,
			&cd.GrossProceeds,
			&cd.ExitLoad,
			&cd.TDS,
			&cd.NetCredited,
			&cd.FolioIdentifier,
			&cd.DematIdentifier,
			&cd.SchemeIdentifier,
			&cd.RequestedBy,
			&cd.RequestedDate,
			&cd.ByAmount,
			&cd.ByUnits,
			&cd.Method,
			&cd.EntityName,
			&cd.ResolvedSchemeID,
			&cd.SchemeName,
			&cd.ISIN,
			&cd.InternalSchemeCode,
			&cd.ResolvedFolioID,
			&cd.FolioNumber,
			&cd.FolioEntityName,
			&cd.ResolvedDematID,
			&cd.DematAccountNumber,
			&cd.DematEntityName,
		); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		confirmations = append(confirmations, cd)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("rows error: %w", rows.Err())
	}

	if len(confirmations) == 0 {
		return nil, fmt.Errorf("no confirmed redemptions found")
	}

	totalTransactions := 0

	// Process each confirmation
	for _, cd := range confirmations {
		// Use resolved folio_number and demat_account_number (already fetched from JOINs)
		var folioNumber, dematAccNumber *string
		if cd.FolioNumber != nil && *cd.FolioNumber != "" {
			folioNumber = cd.FolioNumber
		}
		if cd.DematAccountNumber != nil && *cd.DematAccountNumber != "" {
			dematAccNumber = cd.DematAccountNumber
		}

		// Validate we have essential data
		if cd.ResolvedSchemeID == nil {
			return nil, fmt.Errorf("failed to resolve scheme for redemption %s - scheme identifier: %s", cd.RedemptionConfirmID, cd.SchemeIdentifier)
		}

		// Create SELL transaction
		// Resolve entity_name: prefer folio/demat entity, fallback to initiation entity
		var entityName *string
		if cd.FolioEntityName != nil && *cd.FolioEntityName != "" {
			entityName = cd.FolioEntityName
		} else if cd.DematEntityName != nil && *cd.DematEntityName != "" {
			entityName = cd.DematEntityName
		} else if cd.EntityName != nil && *cd.EntityName != "" {
			entityName = cd.EntityName
		}

		txInsert := `
			INSERT INTO investment.onboard_transaction (
				batch_id, transaction_date, transaction_type, folio_number, demat_acc_number,
				amount, units, nav, scheme_id, folio_id, demat_id, scheme_internal_code, entity_name, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now())
		`

		// Insert SELL transaction with POSITIVE values
		// Transaction type 'SELL' indicates this reduces holdings
		if _, err := tx.Exec(ctx, txInsert,
			batchID,
			cd.RequestedDate,
			"SELL",
			folioNumber,
			dematAccNumber,
			cd.GrossProceeds, // positive amount (proceeds received)
			cd.ActualUnits,   // positive units (quantity redeemed)
			cd.ActualNAV,
			cd.ResolvedSchemeID, // Use actual scheme_id (string)
			cd.ResolvedFolioID,  // Use actual folio_id (string)
			cd.ResolvedDematID,  // Use actual demat_id (string)
			cd.InternalSchemeCode,
			entityName, // Entity from folio/demat or initiation
		); err != nil {
			return nil, fmt.Errorf("transaction insert failed: %w", err)
		}

		// Release blocked units since SELL transaction is now created
		// The SELL transaction will subtract from total units, so we need to remove from blocked_units too
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

		if _, err := tx.Exec(ctx, unblockQuery,
			cd.SchemeIdentifier,
			cd.FolioIdentifier,
			cd.DematIdentifier,
			cd.Method,
			cd.ActualUnits,
		); err != nil {
			// Log error but don't fail the transaction - SELL is already created
			// This prevents double subtraction from portfolio
		}

		totalTransactions++

		// Update redemption_confirmation confirmed details
		updateConfirm := `
			UPDATE investment.redemption_confirmation
			SET confirmed_by=$1, confirmed_at=now()
			WHERE redemption_confirm_id=$2
		`
		if _, err := tx.Exec(ctx, updateConfirm, confirmedBy, cd.RedemptionConfirmID); err != nil {
			return nil, fmt.Errorf("update confirmation failed: %w", err)
		}
	}

	// Refresh portfolio snapshot based on ALL transactions for affected folio_id/demat_id
	// Group by folio_id and demat_id (unique identifiers) instead of non-unique folio_number/demat_acc_number
	// Delete existing snapshots for affected folio_id/demat_id combinations
	deleteSnap := `
		DELETE FROM investment.portfolio_snapshot
		WHERE (folio_id IS NOT NULL AND folio_id IN (
			SELECT DISTINCT ot.folio_id
			FROM investment.onboard_transaction ot
			WHERE ot.batch_id = $1 AND ot.folio_id IS NOT NULL
		))
		OR (demat_id IS NOT NULL AND demat_id IN (
			SELECT DISTINCT ot.demat_id
			FROM investment.onboard_transaction ot
			WHERE ot.batch_id = $1 AND ot.demat_id IS NOT NULL
		))
	`
	if _, err := tx.Exec(ctx, deleteSnap, batchID); err != nil {
		return nil, fmt.Errorf("delete snapshot failed: %w", err)
	}

	// Rebuild snapshots for affected folio_id/demat_id using ALL their transactions (not just current batch)
	snapshotQ := `
WITH affected_folios_demats AS (
    SELECT DISTINCT 
        ot.folio_id,
        ot.demat_id
    FROM investment.onboard_transaction ot
    WHERE ot.batch_id = $1
        AND (ot.folio_id IS NOT NULL OR ot.demat_id IS NOT NULL)
),
scheme_resolved AS (
    SELECT
        ot.batch_id,
        ot.transaction_date,
        ot.transaction_type,
        ot.amount,
        ot.units,
        ot.nav,
        COALESCE(mf.entity_name, md.entity_name) AS entity_name,
        ot.folio_number,
        ot.demat_acc_number,
        ot.folio_id,
        ot.demat_id,
        COALESCE(ot.scheme_id, fsm.scheme_id, ms2.scheme_id, ms.scheme_id) AS scheme_id,
        COALESCE(ms2.scheme_name, ms.scheme_name) AS scheme_name,
        COALESCE(ms2.isin, ms.isin) AS isin
    FROM investment.onboard_transaction ot
    LEFT JOIN investment.masterfolio mf ON mf.folio_id = ot.folio_id
    LEFT JOIN investment.masterdemataccount md ON md.demat_id = ot.demat_id
    LEFT JOIN investment.folioschememapping fsm ON fsm.folio_id = ot.folio_id
    LEFT JOIN investment.masterscheme ms ON ms.scheme_id = fsm.scheme_id
    LEFT JOIN investment.masterscheme ms2 ON ms2.scheme_id::text = ot.scheme_id OR ms2.internal_scheme_code = ot.scheme_internal_code
    WHERE (ot.folio_id IN (SELECT folio_id FROM affected_folios_demats WHERE folio_id IS NOT NULL)
        OR ot.demat_id IN (SELECT demat_id FROM affected_folios_demats WHERE demat_id IS NOT NULL))
),
transaction_summary AS (
    SELECT
        entity_name,
        folio_number,
        demat_acc_number,
        folio_id,
        demat_id,
        scheme_id,
        scheme_name,
        isin,
        -- Net units: BUY positive, SELL negative
        SUM(CASE 
            WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
            WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN units
            ELSE units
        END) AS total_units,
        -- Total invested: sum of BUY amounts only
        SUM(CASE 
            WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN amount
            ELSE 0
        END) AS total_invested_amount,
        -- Avg NAV: weighted average of BUY transactions only
        CASE 
            WHEN SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
                ELSE 0
            END) = 0 THEN 0
            ELSE SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN nav * units
                ELSE 0
            END) / SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
                ELSE 0
            END)
        END AS avg_nav
    FROM scheme_resolved
    GROUP BY entity_name, folio_number, demat_acc_number, folio_id, demat_id, scheme_id, scheme_name, isin
),
latest_nav AS (
    SELECT DISTINCT ON (scheme_name)
        scheme_name,
        isin_div_payout_growth as isin,
        nav_value,
        nav_date
    FROM investment.amfi_nav_staging
    ORDER BY scheme_name, nav_date DESC
)
INSERT INTO investment.portfolio_snapshot (
  batch_id,
  entity_name,
  folio_number,
  demat_acc_number,
  folio_id,
  demat_id,
  scheme_id,
  scheme_name,
  isin,
  total_units,
  avg_nav,
  current_nav,
  current_value,
  total_invested_amount,
  gain_loss,
  gain_losss_percent,
  created_at
)
SELECT
  $1 AS batch_id,
  ts.entity_name,
  ts.folio_number,
  ts.demat_acc_number,
  ts.folio_id,
  ts.demat_id,
  ts.scheme_id,
  ts.scheme_name,
  ts.isin,
  ts.total_units,
  ts.avg_nav,
  COALESCE(ln.nav_value, 0) AS current_nav,
  ts.total_units * COALESCE(ln.nav_value, 0) AS current_value,
  ts.total_invested_amount,
  (ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount AS gain_loss,
  CASE 
    WHEN ts.total_invested_amount = 0 THEN 0
    ELSE (((ts.total_units * COALESCE(ln.nav_value, 0)) - ts.total_invested_amount) / ts.total_invested_amount) * 100
  END AS gain_losss_percent,
  NOW()
FROM transaction_summary ts
LEFT JOIN latest_nav ln ON (ln.scheme_name = ts.scheme_name OR ln.isin = ts.isin)
WHERE ts.total_units > 0;
`
	if _, err := tx.Exec(ctx, snapshotQ, batchID); err != nil {
		return nil, fmt.Errorf("rebuild snapshot failed: %w", err)
	}

	// Count snapshots created
	var snapshotCount int64
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM investment.portfolio_snapshot WHERE batch_id=$1`, batchID).Scan(&snapshotCount); err != nil {
		snapshotCount = 0
	}

	// We do not persist an onboard_batch row here; commit the tx and return transient batch info
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	return map[string]any{
		"batch_id":                         batchID,
		"total_redemption_transactions":    totalTransactions,
		"portfolio_snapshots_refreshed":    snapshotCount,
		"confirmed_by":                     confirmedBy,
		"processed_redemption_confirm_ids": redemptionConfirmationIDs,
	}, nil
}

// ---------------------------
// ConfirmRedemption - Process approved redemption confirmations (Manual endpoint for testing)
// ---------------------------

func ConfirmRedemption(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                    string   `json:"user_id"`
			RedemptionConfirmationIDs []string `json:"redemption_confirmation_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.RedemptionConfirmationIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "redemption_confirmation_ids required")
			return
		}

		confirmedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				confirmedBy = s.Name
				break
			}
		}
		if confirmedBy == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := context.Background()

		result, err := processRedemptionConfirmations(pgxPool, ctx, req.UserID, confirmedBy, req.RedemptionConfirmationIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", result)
	}
}
