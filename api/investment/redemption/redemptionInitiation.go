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
// Request/Response Types
// ---------------------------

type CreateRedemptionRequestSingle struct {
	UserID     string  `json:"user_id"`
	FolioID    string  `json:"folio_id,omitempty"`
	DematID    string  `json:"demat_id,omitempty"`
	SchemeID   string  `json:"scheme_id"`
	EntityName string  `json:"entity_name,omitempty"`
	ByAmount   float64 `json:"by_amount,omitempty"`
	ByUnits    float64 `json:"by_units,omitempty"`
	Method     string  `json:"method,omitempty"` // FIFO, LIFO, etc.
	Status     string  `json:"status,omitempty"`
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

		status := "Active"
		if strings.TrimSpace(req.Status) != "" {
			status = req.Status
		}

		method := "FIFO"
		if strings.TrimSpace(req.Method) != "" {
			method = req.Method
		}

		insertQ := `
			INSERT INTO investment.masterredemption (
				folio_id, demat_id, scheme_id, requested_by, requested_date,
				by_amount, by_units, method, status, entity_name, old_entity_name
			) VALUES ($1, $2, $3, $4, now()::date, $5, $6, $7, $8, $9, $10)
			RETURNING redemption_id
		`
		var redemptionID string
		// requested_by is set from session email; requested_date uses current date
		if err := tx.QueryRow(ctx, insertQ,
			nullIfEmptyString(req.FolioID),
			nullIfEmptyString(req.DematID),
			req.SchemeID,
			userEmail,
			nullIfZeroFloat(req.ByAmount),
			nullIfZeroFloat(req.ByUnits),
			method,
			status,
			nullIfEmptyString(req.EntityName),
			nil,
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
				FolioID    string  `json:"folio_id,omitempty"`
				DematID    string  `json:"demat_id,omitempty"`
				SchemeID   string  `json:"scheme_id"`
				EntityName string  `json:"entity_name,omitempty"`
				ByAmount   float64 `json:"by_amount,omitempty"`
				ByUnits    float64 `json:"by_units,omitempty"`
				Method     string  `json:"method,omitempty"`
				Status     string  `json:"status,omitempty"`
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

			status := "Active"
			if strings.TrimSpace(row.Status) != "" {
				status = row.Status
			}
			method := "FIFO"
			if strings.TrimSpace(row.Method) != "" {
				method = row.Method
			}

			var redemptionID string
			if err := tx.QueryRow(ctx, `
				INSERT INTO investment.masterredemption (
					folio_id, demat_id, scheme_id, requested_by, requested_date,
					by_amount, by_units, method, status, entity_name
				) VALUES ($1, $2, $3, $4, now()::date, $5, $6, $7, $8, $9)
				RETURNING redemption_id
			`, nullIfEmptyString(row.FolioID), nullIfEmptyString(row.DematID), row.SchemeID, userEmail,
				nullIfZeroFloat(row.ByAmount), nullIfZeroFloat(row.ByUnits), method, status, nullIfEmptyString(row.EntityName)).Scan(&redemptionID); err != nil {
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

		// Fetch existing values
		sel := `
			SELECT folio_id, demat_id, scheme_id, requested_by, requested_date,
			       by_amount, by_units, method, status
			FROM investment.masterredemption
			WHERE redemption_id=$1
			FOR UPDATE
		`
		var oldVals [9]interface{}
		if err := tx.QueryRow(ctx, sel, req.RedemptionID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4],
			&oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
		); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "fetch failed: "+err.Error())
			return
		}

		fieldPairs := map[string]int{
			"folio_id":          0,
			"demat_id":          1,
			"scheme_id":         2,
			"requested_by":      3,
			"requested_date":    4,
			"by_amount":         5,
			"by_units":          6,
			"method":            7,
			constants.KeyStatus: 8,
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

		q := fmt.Sprintf("UPDATE investment.masterredemption SET %s, updated_at=now() WHERE redemption_id=$%d", strings.Join(sets, ", "), pos)
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
				SELECT folio_id, demat_id, scheme_id, requested_by, requested_date,
				       by_amount, by_units, method, status
				FROM investment.masterredemption WHERE redemption_id=$1 FOR UPDATE`
			var oldVals [9]interface{}
			if err := tx.QueryRow(ctx, sel, row.RedemptionID).Scan(
				&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4],
				&oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8],
			); err != nil {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "redemption_id": row.RedemptionID, constants.ValueError: "fetch failed: " + err.Error()})
				continue
			}

			fieldPairs := map[string]int{
				"folio_id":          0,
				"demat_id":          1,
				"scheme_id":         2,
				"requested_by":      3,
				"requested_date":    4,
				"by_amount":         5,
				"by_units":          6,
				"method":            7,
				constants.KeyStatus: 8,
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

			q := fmt.Sprintf("UPDATE investment.masterredemption SET %s, updated_at=now() WHERE redemption_id=$%d", strings.Join(sets, ", "), pos)
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
			if _, err := tx.Exec(ctx, `
				UPDATE investment.masterredemption
				SET is_deleted=true, status='DELETED', updated_at=now()
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
				m.old_requested_by,
				TO_CHAR(m.requested_date, 'YYYY-MM-DD') AS requested_date,
				TO_CHAR(m.old_requested_date, 'YYYY-MM-DD') AS old_requested_date,
				m.by_amount,
				m.old_by_amount,
				m.by_units,
				m.old_by_units,
				m.method,
				m.old_method,
				m.status,
				m.old_status,
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
			FROM investment.masterredemption m
			LEFT JOIN latest_audit l ON l.redemption_id = m.redemption_id
			LEFT JOIN history h ON h.redemption_id = m.redemption_id
			WHERE COALESCE(m.is_deleted, false) = false
			ORDER BY m.requested_date DESC, m.redemption_id;
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
					processing_status
				FROM investment.auditactionredemption
				ORDER BY redemption_id, requested_at DESC
			)
			SELECT
				m.redemption_id,
				m.folio_id,
				m.demat_id,
				m.scheme_id,
				m.requested_by,
				TO_CHAR(m.requested_date, 'YYYY-MM-DD') AS requested_date,
				m.by_amount,
				m.by_units,
				m.method,
				m.status
			FROM investment.masterredemption m
			JOIN latest l ON l.redemption_id = m.redemption_id
			WHERE 
				UPPER(l.processing_status) = 'APPROVED'
				AND COALESCE(m.is_deleted,false)=false
			ORDER BY m.requested_date DESC;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var redemptionID string
			var folioID, dematID, schemeID, requestedBy, requestedDate, method, status *string
			var byAmount, byUnits *float64

			if err := rows.Scan(&redemptionID, &folioID, &dematID, &schemeID, &requestedBy, &requestedDate,
				&byAmount, &byUnits, &method, &status); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "row scan failed: "+err.Error())
				return
			}

			rec := map[string]interface{}{"redemption_id": redemptionID}
			if folioID != nil {
				rec["folio_id"] = *folioID
			} else {
				rec["folio_id"] = ""
			}
			if dematID != nil {
				rec["demat_id"] = *dematID
			} else {
				rec["demat_id"] = ""
			}
			if schemeID != nil {
				rec["scheme_id"] = *schemeID
			} else {
				rec["scheme_id"] = ""
			}
			if requestedBy != nil {
				rec["requested_by"] = *requestedBy
			} else {
				rec["requested_by"] = ""
			}
			if requestedDate != nil {
				rec["requested_date"] = *requestedDate
			} else {
				rec["requested_date"] = ""
			}
			if method != nil {
				rec["method"] = *method
			} else {
				rec["method"] = ""
			}
			if status != nil {
				rec[constants.KeyStatus] = *status
			} else {
				rec[constants.KeyStatus] = ""
			}

			if byAmount != nil {
				rec["by_amount"] = *byAmount
			}
			if byUnits != nil {
				rec["by_units"] = *byUnits
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

func nullIfZeroFloat(f float64) interface{} {
	if f == 0 {
		return nil
	}
	return f
}
