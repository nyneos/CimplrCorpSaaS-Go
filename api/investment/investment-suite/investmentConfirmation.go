package investmentsuite

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------
// Request/Response Types
// ---------------------------

type CreateConfirmationRequest struct {
	UserID        string  `json:"user_id"`
	InitiationID  string  `json:"initiation_id"`
	NAVDate       string  `json:"nav_date"` // YYYY-MM-DD
	NAV           float64 `json:"nav"`
	AllottedUnits float64 `json:"allotted_units"`
	StampDuty     float64 `json:"stamp_duty,omitempty"`
	NetAmount     float64 `json:"net_amount"`
	ActualNAV     float64 `json:"actual_nav,omitempty"`
	ActualUnits   float64 `json:"actual_allotted_units,omitempty"`
	Status        string  `json:"status,omitempty"`
}

type UpdateConfirmationRequest struct {
	UserID         string                 `json:"user_id"`
	ConfirmationID string                 `json:"confirmation_id"`
	Fields         map[string]interface{} `json:"fields"`
	Reason         string                 `json:"reason"`
}

// ---------------------------
// CreateConfirmationSingle
// ---------------------------

func CreateConfirmationSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateConfirmationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// validate required fields
		if strings.TrimSpace(req.InitiationID) == "" ||
			strings.TrimSpace(req.NAVDate) == "" ||
			req.NAV <= 0 ||
			req.AllottedUnits <= 0 ||
			req.NetAmount <= 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Missing required fields: initiation_id, nav_date, nav, allotted_units, net_amount")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		status := "PENDING_CONFIRMATION"
		if strings.TrimSpace(req.Status) != "" {
			status = req.Status
		}

		// Calculate variances if actual values provided
		var varianceNAV, varianceUnits *float64
		if req.ActualNAV > 0 {
			diff := req.ActualNAV - req.NAV
			varianceNAV = &diff
		}
		if req.ActualUnits > 0 {
			diff := req.ActualUnits - req.AllottedUnits
			varianceUnits = &diff
		}

		insertQ := `
			INSERT INTO investment.investment_confirmation (
				initiation_id, nav_date, nav, allotted_units, stamp_duty, net_amount,
				actual_nav, actual_allotted_units, variance_nav, variance_units, status
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			RETURNING confirmation_id
		`
		var confirmationID string
		if err := tx.QueryRow(ctx, insertQ,
			req.InitiationID,
			req.NAVDate,
			req.NAV,
			req.AllottedUnits,
			req.StampDuty,
			req.NetAmount,
			nullIfZero(req.ActualNAV),
			nullIfZero(req.ActualUnits),
			varianceNAV,
			varianceUnits,
			status,
		).Scan(&confirmationID); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Insert failed: "+err.Error())
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactioninvestmentconfirmation (confirmation_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, confirmationID, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"confirmation_id": confirmationID,
			"initiation_id":   req.InitiationID,
			"requested":       userEmail,
		})
	}
}

// ---------------------------
// CreateConfirmationBulk
// ---------------------------

func CreateConfirmationBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				InitiationID  string  `json:"initiation_id"`
				NAVDate       string  `json:"nav_date"`
				NAV           float64 `json:"nav"`
				AllottedUnits float64 `json:"allotted_units"`
				StampDuty     float64 `json:"stamp_duty,omitempty"`
				NetAmount     float64 `json:"net_amount"`
				ActualNAV     float64 `json:"actual_nav,omitempty"`
				ActualUnits   float64 `json:"actual_allotted_units,omitempty"`
				Status        string  `json:"status,omitempty"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid user session")
			return
		}

		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			initiationID := strings.TrimSpace(row.InitiationID)
			navDate := strings.TrimSpace(row.NAVDate)

			if initiationID == "" || navDate == "" || row.NAV <= 0 || row.AllottedUnits <= 0 || row.NetAmount <= 0 {
				results = append(results, map[string]interface{}{
					"success": false, "error": "Missing required fields: initiation_id, nav_date, nav, allotted_units, net_amount",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "TX begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			status := "PENDING_CONFIRMATION"
			if strings.TrimSpace(row.Status) != "" {
				status = row.Status
			}

			var varianceNAV, varianceUnits *float64
			if row.ActualNAV > 0 {
				diff := row.ActualNAV - row.NAV
				varianceNAV = &diff
			}
			if row.ActualUnits > 0 {
				diff := row.ActualUnits - row.AllottedUnits
				varianceUnits = &diff
			}

			var confirmationID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.investment_confirmation (
					initiation_id, nav_date, nav, allotted_units, stamp_duty, net_amount,
					actual_nav, actual_allotted_units, variance_nav, variance_units, status
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
				RETURNING confirmation_id
			`, initiationID, navDate, row.NAV, row.AllottedUnits, row.StampDuty, row.NetAmount,
				nullIfZero(row.ActualNAV), nullIfZero(row.ActualUnits), varianceNAV, varianceUnits, status).Scan(&confirmationID); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "initiation_id": initiationID, "error": "Insert failed: " + err.Error(),
				})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioninvestmentconfirmation (confirmation_id, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
			`, confirmationID, userEmail); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "confirmation_id": confirmationID, "error": "Audit insert failed: " + err.Error(),
				})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{
					"success": false, "confirmation_id": confirmationID, "error": "Commit failed: " + err.Error(),
				})
				continue
			}

			results = append(results, map[string]interface{}{
				"success":         true,
				"confirmation_id": confirmationID,
				"initiation_id":   initiationID,
			})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// UpdateConfirmation
// ---------------------------

func UpdateConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateConfirmationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if strings.TrimSpace(req.ConfirmationID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no fields to update")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// fetch existing values
		sel := `
			SELECT initiation_id, nav_date, nav, allotted_units, stamp_duty, net_amount, 
			       actual_nav, actual_allotted_units, variance_nav, variance_units, status
			FROM investment.investment_confirmation
			WHERE confirmation_id=$1
			FOR UPDATE
		`
		var oldVals [11]interface{}
		if err := tx.QueryRow(ctx, sel, req.ConfirmationID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4],
			&oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9], &oldVals[10],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"initiation_id":         0,
			"nav_date":              1,
			"nav":                   2,
			"allotted_units":        3,
			"stamp_duty":            4,
			"net_amount":            5,
			"actual_nav":            6,
			"actual_allotted_units": 7,
			"variance_nav":          8,
			"variance_units":        9,
			"status":                10,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			lk := strings.ToLower(k)
			if idx, ok := fieldPairs[lk]; ok {
				oldField := "old_" + lk
				sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", lk, pos, oldField, pos+1))
				args = append(args, v, oldVals[idx])
				pos += 2
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no valid updatable fields found")
			return
		}

		q := fmt.Sprintf("UPDATE investment.investment_confirmation SET %s, updated_at=now() WHERE confirmation_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.ConfirmationID)
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		// audit
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.auditactioninvestmentconfirmation (confirmation_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
		`, req.ConfirmationID, req.Reason, userEmail); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"confirmation_id": req.ConfirmationID,
			"requested":       userEmail,
		})
	}
}

// ---------------------------
// UpdateConfirmationBulk
// ---------------------------

func UpdateConfirmationBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				ConfirmationID string                 `json:"confirmation_id"`
				Fields         map[string]interface{} `json:"fields"`
				Reason         string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON body")
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
			if row.ConfirmationID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "confirmation_id missing"})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "confirmation_id": row.ConfirmationID, "error": "tx begin failed: " + err.Error()})
				continue
			}
			defer tx.Rollback(ctx)

			sel := `
				SELECT initiation_id, nav_date, nav, allotted_units, stamp_duty, net_amount,
				       actual_nav, actual_allotted_units, variance_nav, variance_units, status
				FROM investment.investment_confirmation WHERE confirmation_id=$1 FOR UPDATE`
			var oldVals [11]interface{}
			if err := tx.QueryRow(ctx, sel, row.ConfirmationID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4],
				&oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9], &oldVals[10],
			); err != nil {
				results = append(results, map[string]interface{}{"success": false, "confirmation_id": row.ConfirmationID, "error": "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"initiation_id":         0,
				"nav_date":              1,
				"nav":                   2,
				"allotted_units":        3,
				"stamp_duty":            4,
				"net_amount":            5,
				"actual_nav":            6,
				"actual_allotted_units": 7,
				"variance_nav":          8,
				"variance_units":        9,
				"status":                10,
			}

			var sets []string
			var args []interface{}
			pos := 1

			for k, v := range row.Fields {
				lk := strings.ToLower(k)
				if idx, ok := fieldPairs[lk]; ok {
					oldField := "old_" + lk
					sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", lk, pos, oldField, pos+1))
					args = append(args, v, oldVals[idx])
					pos += 2
				}
			}

			if len(sets) == 0 {
				results = append(results, map[string]interface{}{"success": false, "confirmation_id": row.ConfirmationID, "error": "No valid fields"})
				continue
			}

			q := fmt.Sprintf("UPDATE investment.investment_confirmation SET %s, updated_at=now() WHERE confirmation_id=$%d", strings.Join(sets, ", "), pos)
			args = append(args, row.ConfirmationID)

			if _, err := tx.Exec(ctx, q, args...); err != nil {
				results = append(results, map[string]interface{}{"success": false, "confirmation_id": row.ConfirmationID, "error": "update failed: " + err.Error()})
				continue
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioninvestmentconfirmation (confirmation_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now())
			`, row.ConfirmationID, row.Reason, userEmail); err != nil {
				results = append(results, map[string]interface{}{"success": false, "confirmation_id": row.ConfirmationID, "error": "audit insert failed: " + err.Error()})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				results = append(results, map[string]interface{}{"success": false, "confirmation_id": row.ConfirmationID, "error": "commit failed: " + err.Error()})
				continue
			}

			results = append(results, map[string]interface{}{"success": true, "confirmation_id": row.ConfirmationID, "requested": userEmail})
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// ---------------------------
// DeleteConfirmation
// ---------------------------

func DeleteConfirmation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			ConfirmationIDs []string `json:"confirmation_ids"`
			Reason          string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if len(req.ConfirmationIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_ids required")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.ConfirmationIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioninvestmentconfirmation (confirmation_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now())
			`, id, req.Reason, requestedBy); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "audit insert failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.ConfirmationIDs})
	}
}

// ---------------------------
// BulkApproveConfirmationActions
// ---------------------------

func BulkApproveConfirmationActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			ConfirmationIDs []string `json:"confirmation_ids"`
			Comment         string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (confirmation_id) action_id, confirmation_id, actiontype, processing_status
			FROM investment.auditactioninvestmentconfirmation
			WHERE confirmation_id = ANY($1)
			ORDER BY confirmation_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.ConfirmationIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		var toApprove []string
		var toApproveConfirmations []string
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
				toApproveConfirmations = append(toApproveConfirmations, cid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, "No approvable actions found", map[string]any{
				"approved_confirmation_ids": []string{},
				"deleted_confirmations":     []string{},
			})
			return
		}

		if len(toApprove) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactioninvestmentconfirmation
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "approve failed: "+err.Error())
				return
			}

			// Update investment_confirmation status to CONFIRMED
			if _, err := tx.Exec(ctx, `
				UPDATE investment.investment_confirmation
				SET status='CONFIRMED'
				WHERE confirmation_id = ANY($1)
			`, toApproveConfirmations); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "update confirmation status failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE investment.auditactioninvestmentconfirmation
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete action update failed: "+err.Error())
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE investment.investment_confirmation
				SET is_deleted=true, status='DELETED', updated_at=now()
				WHERE confirmation_id = ANY($1)
			`, deleteMasterIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "delete confirmation failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		// Automatically process confirmed investments (create BUY transactions and refresh snapshots)
		var confirmResult map[string]any
		if len(toApproveConfirmations) > 0 {
			confirmResult, err = processInvestmentConfirmations(pgxPool, ctx, req.UserID, checkerBy, toApproveConfirmations)
			if err != nil {
				// Log the error but don't fail the approval - confirmations can be reprocessed manually
				api.RespondWithPayload(w, true, "Approved but confirmation processing failed: "+err.Error(), map[string]any{
					"approved_confirmation_ids": toApproveConfirmations,
					"deleted_confirmations":     deleteMasterIDs,
					"confirmation_error":        err.Error(),
				})
				return
			}
		}

		// ensure non-nil slices so JSON marshals [] instead of null
		if toApproveConfirmations == nil {
			toApproveConfirmations = []string{}
		}
		if deleteMasterIDs == nil {
			deleteMasterIDs = []string{}
		}

		response := map[string]any{
			"approved_confirmation_ids": toApproveConfirmations,
			"deleted_confirmations":     deleteMasterIDs,
		}
		if confirmResult != nil {
			response["confirmation_processing"] = confirmResult
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// ---------------------------
// BulkRejectConfirmationActions
// ---------------------------

func BulkRejectConfirmationActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			ConfirmationIDs []string `json:"confirmation_ids"`
			Comment         string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
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
			api.RespondWithError(w, http.StatusUnauthorized, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (confirmation_id) action_id, confirmation_id, processing_status
			FROM investment.auditactioninvestmentconfirmation
			WHERE confirmation_id = ANY($1)
			ORDER BY confirmation_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.ConfirmationIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
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
		for _, id := range req.ConfirmationIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for confirmation_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved confirmation_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		if _, err := tx.Exec(ctx, `
			UPDATE investment.auditactioninvestmentconfirmation
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "update failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

// ---------------------------
// GetConfirmationsWithAudit
// ---------------------------

func GetConfirmationsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.confirmation_id)
					a.confirmation_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactioninvestmentconfirmation a
				ORDER BY a.confirmation_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					confirmation_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactioninvestmentconfirmation
				GROUP BY confirmation_id
			)
			SELECT
				m.confirmation_id,
				m.initiation_id,
				m.old_initiation_id,
				TO_CHAR(m.nav_date, 'YYYY-MM-DD') AS nav_date,
				TO_CHAR(m.old_nav_date, 'YYYY-MM-DD') AS old_nav_date,
				m.nav,
				m.old_nav,
				m.allotted_units,
				m.old_allotted_units,
				m.stamp_duty,
				m.old_stamp_duty,
				m.net_amount,
				m.old_net_amount,
				m.actual_nav,
				m.old_actual_nav,
				m.actual_allotted_units,
				m.old_actual_allotted_units,
				m.variance_nav,
				m.old_variance_nav,
				m.variance_units,
				m.old_variance_units,
				m.status,
				m.old_status,
				m.confirmed_by,
				TO_CHAR(m.confirmed_at, 'YYYY-MM-DD HH24:MI:SS') AS confirmed_at,
				m.is_deleted,
				TO_CHAR(m.updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,

				-- initiation fields
				TO_CHAR(i.transaction_date, 'YYYY-MM-DD') AS initiation_transaction_date,
				i.entity_name AS initiation_entity_name,
				COALESCE(s.scheme_id::text, i.scheme_id::text) AS initiation_scheme_id,
				COALESCE(s.scheme_name, i.scheme_id) AS initiation_scheme_name,
				COALESCE(s.amc_name, '') AS initiation_amc_name,
				COALESCE(f.folio_number, '') AS initiation_folio_number,
				COALESCE(f.folio_id::text, '') AS initiation_folio_id,
				COALESCE(d.demat_account_number, '') AS initiation_demat_number,
				COALESCE(d.demat_id::text, '') AS initiation_demat_id,
				COALESCE(i.amount, 0) AS initiation_amount,
				
				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS requested_by,
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
			FROM investment.investment_confirmation m
			LEFT JOIN latest_audit l ON l.confirmation_id = m.confirmation_id
			LEFT JOIN history h ON h.confirmation_id = m.confirmation_id
			LEFT JOIN investment.investment_initiation i ON i.initiation_id = m.initiation_id
			LEFT JOIN investment.masterscheme s ON (
				s.scheme_id::text = i.scheme_id OR
				s.scheme_name = i.scheme_id OR
				s.internal_scheme_code = i.scheme_id OR
				s.isin = i.scheme_id
			)
			LEFT JOIN investment.masterfolio f ON (f.folio_id::text = i.folio_id OR f.folio_number = i.folio_id)
			LEFT JOIN investment.masterdemataccount d ON (
				d.demat_id::text = i.demat_id OR
				d.default_settlement_account = i.demat_id OR
				d.demat_account_number = i.demat_id
			)
			WHERE COALESCE(m.is_deleted, false) = false
			ORDER BY m.nav_date DESC, m.initiation_id;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)
		// columns that should default to numeric zero when NULL
		numericCols := map[string]bool{
			"nav": true, "old_nav": true,
			"allotted_units": true, "old_allotted_units": true,
			"stamp_duty": true, "old_stamp_duty": true,
			"net_amount": true, "old_net_amount": true,
			"actual_nav": true, "old_actual_nav": true,
			"actual_allotted_units": true, "old_actual_allotted_units": true,
			"variance_nav": true, "old_variance_nav": true,
			"variance_units": true, "old_variance_units": true,
		}
		// boolean cols default to false
		boolCols := map[string]bool{"is_deleted": true}

		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				colName := string(f.Name)
				if vals[i] == nil {
					// postpone defaulting until after type maps
					rec[colName] = nil
					continue
				}
				switch v := vals[i].(type) {
				case time.Time:
					rec[colName] = v.Format("2006-01-02 15:04:05")
				default:
					rec[colName] = v
				}
			}

			// Normalize nils: numbers -> 0, bools -> false, others -> ""
			for _, f := range fields {
				colName := string(f.Name)
				if rec[colName] == nil {
					if numericCols[colName] {
						rec[colName] = 0
					} else if boolCols[colName] {
						rec[colName] = false
					} else {
						rec[colName] = ""
					}
				}
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

// GetAllConfirmationsWithAudit returns all rows from investment_confirmation along with
// the latest audit action and a compact history summary. This endpoint accepts no body
// and returns every confirmation row (including those marked deleted).
func GetAllConfirmationsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.confirmation_id)
					a.confirmation_id,
					a.actiontype,
					a.processing_status,
					a.action_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason
				FROM investment.auditactioninvestmentconfirmation a
				ORDER BY a.confirmation_id, a.requested_at DESC
			),
			history AS (
				SELECT 
					confirmation_id,
					MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.auditactioninvestmentconfirmation
				GROUP BY confirmation_id
			)
			SELECT
				m.confirmation_id,
				m.initiation_id,
				m.old_initiation_id,
				TO_CHAR(m.nav_date, 'YYYY-MM-DD') AS nav_date,
				TO_CHAR(m.old_nav_date, 'YYYY-MM-DD') AS old_nav_date,
				m.nav,
				m.old_nav,
				m.allotted_units,
				m.old_allotted_units,
				m.stamp_duty,
				m.old_stamp_duty,
				m.net_amount,
				m.old_net_amount,
				m.actual_nav,
				m.old_actual_nav,
				m.actual_allotted_units,
				m.old_actual_allotted_units,
				m.variance_nav,
				m.old_variance_nav,
				m.variance_units,
				m.old_variance_units,
				m.status,
				m.old_status,
				m.confirmed_by,
				TO_CHAR(m.confirmed_at, 'YYYY-MM-DD HH24:MI:SS') AS confirmed_at,
				m.is_deleted,
				TO_CHAR(m.updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,

				-- initiation fields
				TO_CHAR(i.transaction_date, 'YYYY-MM-DD') AS initiation_transaction_date,
				i.entity_name AS initiation_entity_name,
				COALESCE(s.scheme_id::text, i.scheme_id::text) AS initiation_scheme_id,
				COALESCE(s.scheme_name, i.scheme_id) AS initiation_scheme_name,
				COALESCE(s.amc_name, '') AS initiation_amc_name,
				COALESCE(f.folio_number, '') AS initiation_folio_number,
				COALESCE(f.folio_id::text, '') AS initiation_folio_id,
				COALESCE(d.demat_account_number, '') AS initiation_demat_number,
				COALESCE(d.demat_id::text, '') AS initiation_demat_id,
				COALESCE(i.amount, 0) AS initiation_amount,

				COALESCE(l.actiontype,'') AS action_type,
				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_id::text,'') AS action_id,
				COALESCE(l.requested_by,'') AS requested_by,
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
			FROM investment.investment_confirmation m
			LEFT JOIN latest_audit l ON l.confirmation_id = m.confirmation_id
			LEFT JOIN history h ON h.confirmation_id = m.confirmation_id
			LEFT JOIN investment.investment_initiation i ON i.initiation_id = m.initiation_id
			LEFT JOIN investment.masterscheme s ON (
				s.scheme_id::text = i.scheme_id OR
				s.scheme_name = i.scheme_id OR
				s.internal_scheme_code = i.scheme_id OR
				s.isin = i.scheme_id
			)
			LEFT JOIN investment.masterfolio f ON (f.folio_id::text = i.folio_id OR f.folio_number = i.folio_id)
			LEFT JOIN investment.masterdemataccount d ON (
				d.demat_id::text = i.demat_id OR
				d.default_settlement_account = i.demat_id OR
				d.demat_account_number = i.demat_id
			)
			ORDER BY m.nav_date DESC, m.initiation_id;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)
		numericCols := map[string]bool{
			"nav": true, "old_nav": true,
			"allotted_units": true, "old_allotted_units": true,
			"stamp_duty": true, "old_stamp_duty": true,
			"net_amount": true, "old_net_amount": true,
			"actual_nav": true, "old_actual_nav": true,
			"actual_allotted_units": true, "old_actual_allotted_units": true,
			"variance_nav": true, "old_variance_nav": true,
			"variance_units": true, "old_variance_units": true,
		}
		boolCols := map[string]bool{"is_deleted": true}

		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				colName := string(f.Name)
				if vals[i] == nil {
					rec[colName] = nil
					continue
				}
				switch v := vals[i].(type) {
				case time.Time:
					rec[colName] = v.Format("2006-01-02 15:04:05")
				default:
					rec[colName] = v
				}
			}

			// Normalize nils
			for _, f := range fields {
				colName := string(f.Name)
				if rec[colName] == nil {
					if numericCols[colName] {
						rec[colName] = 0
					} else if boolCols[colName] {
						rec[colName] = false
					} else {
						rec[colName] = ""
					}
				}
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
// GetApprovedConfirmations
// ---------------------------

func GetApprovedConfirmations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest AS (
				SELECT DISTINCT ON (confirmation_id)
					confirmation_id,
					processing_status
				FROM investment.auditactioninvestmentconfirmation
				ORDER BY confirmation_id, requested_at DESC
			)
			SELECT
				m.confirmation_id,
				m.initiation_id,
				TO_CHAR(m.nav_date, 'YYYY-MM-DD') AS nav_date,
				m.nav,
				m.allotted_units,
				m.stamp_duty,
				m.net_amount,
				m.actual_nav,
				m.actual_allotted_units,
				m.variance_nav,
				m.variance_units,
				m.status,

				-- initiation fields
				TO_CHAR(i.transaction_date, 'YYYY-MM-DD') AS initiation_transaction_date,
				i.entity_name AS initiation_entity_name,
				COALESCE(s.scheme_id::text, i.scheme_id::text) AS initiation_scheme_id,
				COALESCE(s.scheme_name, i.scheme_id) AS initiation_scheme_name,
				COALESCE(s.amc_name, '') AS initiation_amc_name,
				COALESCE(f.folio_number, '') AS initiation_folio_number,
				COALESCE(f.folio_id::text, '') AS initiation_folio_id,
				COALESCE(d.demat_account_number, '') AS initiation_demat_number,
				COALESCE(d.demat_id::text, '') AS initiation_demat_id,
				COALESCE(i.amount, 0) AS initiation_amount

			FROM investment.investment_confirmation m
			JOIN latest l ON l.confirmation_id = m.confirmation_id
			LEFT JOIN investment.investment_initiation i ON i.initiation_id = m.initiation_id
			LEFT JOIN investment.masterscheme s ON (
				s.scheme_id::text = i.scheme_id OR
				s.scheme_name = i.scheme_id OR
				s.internal_scheme_code = i.scheme_id OR
				s.isin = i.scheme_id
			)
			LEFT JOIN investment.masterfolio f ON (f.folio_id::text = i.folio_id OR f.folio_number = i.folio_id)
			LEFT JOIN investment.masterdemataccount d ON (
				d.demat_id::text = i.demat_id OR
				d.default_settlement_account = i.demat_id OR
				d.demat_account_number = i.demat_id
			)
			WHERE 
				UPPER(l.processing_status) = 'APPROVED'
				AND COALESCE(m.is_deleted,false)=false
			ORDER BY m.nav_date DESC;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 100)
		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					rec[string(f.Name)] = ""
				} else {
					if t, ok := vals[i].(time.Time); ok {
						rec[string(f.Name)] = t.Format("2006-01-02 15:04:05")
					} else {
						rec[string(f.Name)] = vals[i]
					}
				}
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
// processInvestmentConfirmations - Internal helper to process confirmations
// ---------------------------

func processInvestmentConfirmations(pgxPool *pgxpool.Pool, ctx context.Context, userID string, confirmedBy string, confirmationIDs []string) (map[string]any, error) {
	tx, err := pgxPool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("tx begin failed: %w", err)
	}
	defer tx.Rollback(ctx)

	// Use a generated transient batch ID (do not create an entry in `onboard_batch`)
	batchID := fmt.Sprintf("confirm-%d", time.Now().UnixNano())

	// Fetch confirmation details with related initiation and scheme info
	query := `
		SELECT 
			c.confirmation_id,
			c.initiation_id,
			c.nav_date,
			c.nav,
			c.allotted_units,
			c.stamp_duty,
			c.net_amount,
			c.status,
			i.transaction_date,
			i.entity_name,
			i.scheme_id,
			i.folio_id,
			i.demat_id,
			i.amount AS initiation_amount,
			s.scheme_name,
			s.isin,
			s.internal_scheme_code,
			f.folio_number,
			d.demat_account_number
		FROM investment.investment_confirmation c
		JOIN investment.investment_initiation i ON i.initiation_id = c.initiation_id
		LEFT JOIN investment.masterscheme s ON s.scheme_id = i.scheme_id
		LEFT JOIN investment.masterfolio f ON f.folio_id = i.folio_id
		LEFT JOIN investment.masterdemataccount d ON d.demat_id = i.demat_id
		WHERE c.confirmation_id = ANY($1)
			AND c.status = 'CONFIRMED'
			AND COALESCE(c.is_deleted, false) = false
	`

	rows, err := tx.Query(ctx, query, confirmationIDs)
	if err != nil {
		return nil, fmt.Errorf("fetch confirmations failed: %w", err)
	}
	defer rows.Close()

	type ConfirmationData struct {
		ConfirmationID     string
		InitiationID       string
		NAVDate            time.Time
		NAV                float64
		AllottedUnits      float64
		StampDuty          float64
		NetAmount          float64
		Status             string
		TransactionDate    time.Time
		EntityName         string
		SchemeID           string
		FolioID            *string
		DematID            *string
		InitiationAmount   float64
		SchemeName         *string
		ISIN               *string
		InternalSchemeCode *string
		FolioNumber        *string
		DematAccountNumber *string
	}

	confirmations := []ConfirmationData{}
	for rows.Next() {
		var cd ConfirmationData
		if err := rows.Scan(
			&cd.ConfirmationID, &cd.InitiationID, &cd.NAVDate, &cd.NAV, &cd.AllottedUnits,
			&cd.StampDuty, &cd.NetAmount, &cd.Status, &cd.TransactionDate, &cd.EntityName,
			&cd.SchemeID, &cd.FolioID, &cd.DematID, &cd.InitiationAmount, &cd.SchemeName,
			&cd.ISIN, &cd.InternalSchemeCode, &cd.FolioNumber, &cd.DematAccountNumber,
		); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		confirmations = append(confirmations, cd)
	}

	if len(confirmations) == 0 {
		return nil, fmt.Errorf("no confirmed investments found")
	}

	processedIDs := []string{}

	for _, cd := range confirmations {
		// Insert transaction into onboard_transaction
		txInsert := `
			INSERT INTO investment.onboard_transaction (
				batch_id, transaction_date, transaction_type, folio_number, demat_acc_number,
				amount, units, nav, scheme_id, scheme_name, isin, folio_id, demat_id,
				scheme_internal_code, entity_name
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		`

		// Resolve folio_number or demat_account_number from IDs
		var folioNumber, dematAccNumber *string
		if cd.FolioNumber != nil {
			folioNumber = cd.FolioNumber
		} else if cd.FolioID != nil {
			var fn string
			_ = tx.QueryRow(ctx, `SELECT folio_number FROM investment.masterfolio WHERE folio_id=$1`, *cd.FolioID).Scan(&fn)
			if fn != "" {
				folioNumber = &fn
			}
		}

		if cd.DematAccountNumber != nil {
			dematAccNumber = cd.DematAccountNumber
		} else if cd.DematID != nil {
			var da string
			_ = tx.QueryRow(ctx, `SELECT demat_account_number FROM investment.masterdemataccount WHERE demat_id=$1`, *cd.DematID).Scan(&da)
			if da != "" {
				dematAccNumber = &da
			}
		}

		if _, err := tx.Exec(ctx, txInsert,
			batchID,
			cd.NAVDate,
			"BUY",
			folioNumber,
			dematAccNumber,
			cd.NetAmount,
			cd.AllottedUnits,
			cd.NAV,
			cd.SchemeID,
			cd.SchemeName,
			cd.ISIN,
			cd.FolioID,
			cd.DematID,
			cd.InternalSchemeCode,
			cd.EntityName,
		); err != nil {
			return nil, fmt.Errorf("transaction insert failed: %w", err)
		}

		// Update confirmation
		updateConfirm := `
			UPDATE investment.investment_confirmation
			SET confirmed_by=$1, confirmed_at=now()
			WHERE confirmation_id=$2
		`
		if _, err := tx.Exec(ctx, updateConfirm, confirmedBy, cd.ConfirmationID); err != nil {
			return nil, fmt.Errorf("update confirmation failed: %w", err)
		}

		processedIDs = append(processedIDs, cd.ConfirmationID)
	}

	// Refresh portfolio snapshot
	deleteSnap := `
		DELETE FROM investment.portfolio_snapshot
		WHERE batch_id IN (
			SELECT DISTINCT batch_id FROM investment.onboard_transaction WHERE batch_id=$1
		)
	`
	if _, err := tx.Exec(ctx, deleteSnap, batchID); err != nil {
		return nil, fmt.Errorf("delete snapshot failed: %w", err)
	}

	snapshotQ := `
WITH scheme_resolved AS (
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
        COALESCE(fsm.scheme_id, ms2.scheme_id) AS scheme_id,
        COALESCE(ms.scheme_name, ms2.scheme_name) AS scheme_name,
        COALESCE(ms.isin, ms2.isin) AS isin
    FROM investment.onboard_transaction ot
    LEFT JOIN investment.masterfolio mf 
        ON mf.folio_number = ot.folio_number
    LEFT JOIN investment.masterdemataccount md
        ON md.demat_account_number = ot.demat_acc_number
    LEFT JOIN investment.folioschememapping fsm 
        ON fsm.folio_id = mf.folio_id
    LEFT JOIN investment.masterscheme ms 
        ON ms.scheme_id = fsm.scheme_id
    LEFT JOIN investment.masterscheme ms2
        ON ms2.internal_scheme_code = ot.scheme_internal_code
    WHERE ot.batch_id = $1
),
transaction_summary AS (
    SELECT
        entity_name,
        folio_number,
        demat_acc_number,
        scheme_id,
        scheme_name,
        isin,
        SUM(CASE 
            WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
            WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -units
            ELSE units
        END) AS total_units,
        SUM(CASE 
            WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN amount
            WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -amount
            ELSE amount
        END) AS total_invested_amount,
        CASE 
            WHEN SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
                WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -units
                ELSE units
            END) = 0 THEN 0
            ELSE SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN nav * units
                WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -nav * units
                ELSE nav * units
            END) / SUM(CASE 
                WHEN LOWER(transaction_type) IN ('purchase', 'buy', 'subscription') THEN units
                WHEN LOWER(transaction_type) IN ('sell', 'redemption') THEN -units
                ELSE units
            END)
        END AS avg_nav
    FROM scheme_resolved
    GROUP BY entity_name, folio_number, demat_acc_number, scheme_id, scheme_name, isin
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

	// NOTE: We no longer create or update an `onboard_batch` row.
	// The transient `batchID` is used only to group inserted transactions and snapshots,
	// but no entry is persisted in `investment.onboard_batch`.

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	return map[string]any{
		"batch_id":           batchID,
		"confirmed_ids":      processedIDs,
		"transactions_count": len(processedIDs),
		"snapshot_count":     snapshotCount,
		"confirmed_by":       confirmedBy,
	}, nil
}

// ---------------------------
// ConfirmInvestment - Process approved confirmations (Manual endpoint for testing)
// ---------------------------

func ConfirmInvestment(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			ConfirmationIDs []string `json:"confirmation_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		if len(req.ConfirmationIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "confirmation_ids required")
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
			api.RespondWithError(w, http.StatusUnauthorized, "Invalid session")
			return
		}

		ctx := context.Background()

		result, err := processInvestmentConfirmations(pgxPool, ctx, req.UserID, confirmedBy, req.ConfirmationIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", result)
	}
}

// ---------------------------
// Helper functions
// ---------------------------

func nullIfZero(val float64) interface{} {
	if val == 0 {
		return nil
	}
	return val
}

func stringOrNull(s *string) interface{} {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}
