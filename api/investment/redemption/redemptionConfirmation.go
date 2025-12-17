package redemption

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Request/Response Types for Redemption Confirmation
// ---------------------------

type CreateRedemptionConfirmationRequest struct {
	UserID        string  `json:"user_id"`
	RedemptionID  string  `json:"redemption_id"`
	ActualNAV     float64 `json:"actual_nav"`
	ActualUnits   float64 `json:"actual_units"`
	GrossProceeds float64 `json:"gross_proceeds"`
	ExitLoad      float64 `json:"exit_load,omitempty"`
	TDS           float64 `json:"tds,omitempty"`
	NetCredited   float64 `json:"net_credited"`
	Status        string  `json:"status,omitempty"`
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
		if req.ActualNAV <= 0 || req.ActualUnits <= 0 || req.GrossProceeds <= 0 || req.NetCredited <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "actual_nav, actual_units, gross_proceeds, and net_credited must be greater than 0")
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

		status := "PENDING_CONFIRMATION"
		if strings.TrimSpace(req.Status) != "" {
			status = req.Status
		}

		insertQ := `
			INSERT INTO investment.redemption_confirmation (
				redemption_id, actual_nav, actual_units, gross_proceeds,
				exit_load, tds, net_credited, status
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
				RedemptionID  string  `json:"redemption_id"`
				ActualNAV     float64 `json:"actual_nav"`
				ActualUnits   float64 `json:"actual_units"`
				GrossProceeds float64 `json:"gross_proceeds"`
				ExitLoad      float64 `json:"exit_load,omitempty"`
				TDS           float64 `json:"tds,omitempty"`
				NetCredited   float64 `json:"net_credited"`
				Status        string  `json:"status,omitempty"`
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
			if row.ActualNAV <= 0 || row.ActualUnits <= 0 || row.GrossProceeds <= 0 || row.NetCredited <= 0 {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "actual_nav, actual_units, gross_proceeds, net_credited must be > 0"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: constants.ErrTxBeginFailed + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			status := "PENDING_CONFIRMATION"
			if strings.TrimSpace(row.Status) != "" {
				status = row.Status
			}

			var confirmID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.redemption_confirmation (
					redemption_id, actual_nav, actual_units, gross_proceeds,
					exit_load, tds, net_credited, status
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				RETURNING redemption_confirm_id
			`, row.RedemptionID, row.ActualNAV, row.ActualUnits, row.GrossProceeds,
				row.ExitLoad, row.TDS, row.NetCredited, status).Scan(&confirmID); err != nil {
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
			       exit_load, tds, net_credited, status
			FROM investment.redemption_confirmation
			WHERE redemption_confirm_id=$1
			FOR UPDATE
		`
		var oldVals [8]interface{}
		if err := tx.QueryRow(ctx, sel, req.RedemptionConfirmID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"redemption_id":     0,
			"actual_nav":        1,
			"actual_units":      2,
			"gross_proceeds":    3,
			"exit_load":         4,
			"tds":               5,
			"net_credited":      6,
			constants.KeyStatus: 7,
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid or inactive session")
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
				       exit_load, tds, net_credited, status
				FROM investment.redemption_confirmation WHERE redemption_confirm_id=$1 FOR UPDATE`
			var oldVals [8]interface{}
			if err := tx.QueryRow(ctx, sel, row.RedemptionConfirmID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
				&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
			); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_confirm_id": row.RedemptionConfirmID, constants.ValueError: "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"redemption_id":     0,
				"actual_nav":        1,
				"actual_units":      2,
				"gross_proceeds":    3,
				"exit_load":         4,
				"tds":               5,
				"net_credited":      6,
				constants.KeyStatus: 7,
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
				m.status,
				m.old_status,
				m.confirmed_by,
				TO_CHAR(m.confirmed_at, 'YYYY-MM-DD HH24:MI:SS') AS confirmed_at,
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
			FROM investment.redemption_confirmation m
			LEFT JOIN latest_audit l ON l.redemption_confirm_id = m.redemption_confirm_id
			LEFT JOIN history h ON h.redemption_confirm_id = m.redemption_confirm_id
			WHERE COALESCE(m.is_deleted, false) = false
			ORDER BY m.updated_at DESC, m.redemption_confirm_id;
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
			_ = rows.Scan(&confirmID, &redemptionID, &actualNAV, &actualUnits, &grossProceeds,
				&exitLoad, &tds, &netCredited, &status)

			out = append(out, map[string]interface{}{
				"redemption_confirm_id": confirmID,
				"redemption_id":         redemptionID,
				"actual_nav":            actualNAV,
				"actual_units":          actualUnits,
				"gross_proceeds":        grossProceeds,
				"exit_load":             exitLoad,
				"tds":                   tds,
				"net_credited":          netCredited,
				constants.KeyStatus:     status,
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
		return nil, fmt.Errorf("TX begin failed: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create batch for these redemption transactions
	var batchID string
	batchInsert := `
		INSERT INTO investment.onboard_batch (user_id, user_email, source, total_records, status)
		VALUES ($1, $2, $3, 0, $4)
		RETURNING batch_id
	`
	if err := tx.QueryRow(ctx, batchInsert, userID, confirmedBy, "Redemption Confirmation Batch", "IN_PROGRESS").Scan(&batchID); err != nil {
		return nil, fmt.Errorf("Batch creation failed: %w", err)
	}

	// Fetch redemption confirmation details with related redemption initiation and scheme info
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
			
			mr.folio_id,
			mr.demat_id,
			mr.scheme_id,
			mr.requested_by,
			mr.requested_date,
			mr.by_amount,
			mr.by_units,
			mr.method,
			mr.entity_name
		FROM investment.redemption_confirmation rc
		JOIN investment.masterredemption mr ON rc.redemption_id = mr.redemption_id
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
		FolioID             *string
		DematID             *string
		SchemeID            string
		RequestedBy         string
		RequestedDate       string
		ByAmount            *float64
		ByUnits             *float64
		Method              string
		EntityName          *string
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
			&cd.FolioID,
			&cd.DematID,
			&cd.SchemeID,
			&cd.RequestedBy,
			&cd.RequestedDate,
			&cd.ByAmount,
			&cd.ByUnits,
			&cd.Method,
			&cd.EntityName,
		); err != nil {
			return nil, fmt.Errorf("Scan failed: %w", err)
		}
		confirmations = append(confirmations, cd)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("Rows error: %w", rows.Err())
	}

	if len(confirmations) == 0 {
		return nil, fmt.Errorf("No confirmed redemptions found")
	}

	totalTransactions := 0

	// Process each confirmation
	for _, cd := range confirmations {
		// Create SELL transaction
		txInsert := `
			INSERT INTO investment.onboard_transaction (
				batch_id, transaction_date, transaction_type, folio_number, demat_acc_number,
				amount, units, nav, scheme_id, folio_id, demat_id, entity_name
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		`

		// Get folio_number and demat_acc_number from master tables if needed
		var folioNumber, dematAccNumber *string
		if cd.FolioID != nil {
			var fn string
			_ = tx.QueryRow(ctx, `SELECT folio_number FROM investment.masterfolio WHERE folio_id=$1`, *cd.FolioID).Scan(&fn)
			if fn != "" {
				folioNumber = &fn
			}
		}
		if cd.DematID != nil {
			var dn string
			_ = tx.QueryRow(ctx, `SELECT demat_acc_number FROM investment.masterdemat WHERE demat_id=$1`, *cd.DematID).Scan(&dn)
			if dn != "" {
				dematAccNumber = &dn
			}
		}

		// Insert SELL transaction with negative units
		if _, err := tx.Exec(ctx, txInsert,
			batchID,
			cd.RequestedDate,
			"SELL",
			folioNumber,
			dematAccNumber,
			-cd.GrossProceeds, // negative amount for redemption
			-cd.ActualUnits,   // negative units for redemption
			cd.ActualNAV,
			cd.SchemeID,
			cd.FolioID,
			cd.DematID,
			cd.EntityName,
		); err != nil {
			return nil, fmt.Errorf("Transaction insert failed: %w", err)
		}

		totalTransactions++

		// Update redemption_confirmation confirmed details
		updateConfirm := `
			UPDATE investment.redemption_confirmation
			SET confirmed_by=$1, confirmed_at=now()
			WHERE redemption_confirm_id=$2
		`
		if _, err := tx.Exec(ctx, updateConfirm, confirmedBy, cd.RedemptionConfirmID); err != nil {
			return nil, fmt.Errorf("Update confirmation failed: %w", err)
		}
	}

	// Refresh portfolio snapshot based on the batch transactions
	// First delete any existing snapshots for entities in this batch
	deleteSnap := `
		DELETE FROM investment.portfolio_snapshot
		WHERE entity_name IN (
			SELECT DISTINCT entity_name
			FROM investment.onboard_transaction
			WHERE batch_id = $1
		)
	`
	if _, err := tx.Exec(ctx, deleteSnap, batchID); err != nil {
		return nil, fmt.Errorf("Delete snapshot failed: %w", err)
	}

	// Rebuild portfolio snapshot by aggregating all transactions
	rebuildSnap := `
		INSERT INTO investment.portfolio_snapshot (
			batch_id, entity_name, folio_number, demat_acc_number, scheme_id, scheme_name, isin,
			total_units, avg_nav, current_nav, current_value, total_invested_amount,
			gain_loss, gain_losss_percent
		)
		SELECT
			$1 AS batch_id,
			t.entity_name,
			t.folio_number,
			t.demat_acc_number,
			t.scheme_id,
			MAX(s.scheme_name) AS scheme_name,
			MAX(s.isin) AS isin,
			SUM(t.units) AS total_units,
			CASE
				WHEN SUM(t.units) > 0 THEN SUM(t.amount) / SUM(t.units)
				ELSE 0
			END AS avg_nav,
			MAX(s.latest_nav) AS current_nav,
			SUM(t.units) * MAX(s.latest_nav) AS current_value,
			SUM(t.amount) AS total_invested_amount,
			(SUM(t.units) * MAX(s.latest_nav)) - SUM(t.amount) AS gain_loss,
			CASE
				WHEN SUM(t.amount) != 0 THEN
					(((SUM(t.units) * MAX(s.latest_nav)) - SUM(t.amount)) / ABS(SUM(t.amount))) * 100
				ELSE 0
			END AS gain_losss_percent
		FROM investment.onboard_transaction t
		LEFT JOIN investment.amfi_schemes s ON t.scheme_id = s.scheme_id
		WHERE t.entity_name IN (
			SELECT DISTINCT entity_name
			FROM investment.onboard_transaction
			WHERE batch_id = $1
		)
		GROUP BY t.entity_name, t.folio_number, t.demat_acc_number, t.scheme_id
		HAVING SUM(t.units) > 0
	`
	if _, err := tx.Exec(ctx, rebuildSnap, batchID); err != nil {
		return nil, fmt.Errorf("Rebuild snapshot failed: %w", err)
	}

	// Count snapshots created
	var snapshotCount int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM investment.portfolio_snapshot WHERE batch_id=$1
	`, batchID).Scan(&snapshotCount); err != nil {
		snapshotCount = 0
	}

	// Mark batch as completed and set total_records
	updateBatch := `
		UPDATE investment.onboard_batch
		SET total_records=$1, status='COMPLETED', completed_at=now()
		WHERE batch_id=$2
	`
	if _, err := tx.Exec(ctx, updateBatch, totalTransactions, batchID); err != nil {
		return nil, fmt.Errorf("Update batch failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("Commit failed: %w", err)
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
