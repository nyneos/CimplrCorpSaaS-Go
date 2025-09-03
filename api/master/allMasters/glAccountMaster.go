package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type GLAccountRequest struct {
	GLAccountCode        string `json:"gl_account_code"`
	GLAccountName        string `json:"gl_account_name"`
	GLAccountType        string `json:"gl_account_type"`
	Status               string `json:"status"`
	Source               string `json:"source"`
	ErpRef               string `json:"erp_ref"`
	CashflowCategoryName string `json:"cashflow_category_name"`
}

// CreateGLAccounts inserts rows into masterglaccount and creates audit entries
func CreateGLAccounts(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string             `json:"user_id"`
			Rows   []GLAccountRequest `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		createdBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()
		created := make([]map[string]interface{}, 0)
		for _, rrow := range req.Rows {
			if rrow.GLAccountCode == "" || rrow.GLAccountName == "" || rrow.GLAccountType == "" || rrow.Source == "" {
				created = append(created, map[string]interface{}{"success": false, "error": "missing required fields", "gl_account_code": rrow.GLAccountCode})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				created = append(created, map[string]interface{}{"success": false, "error": "failed to start tx: " + err.Error()})
				continue
			}

			var id string
			q := `INSERT INTO masterglaccount (gl_account_code, gl_account_name, gl_account_type, status, source, erp_ref, cashflow_category_name) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING gl_account_id`
			if err := tx.QueryRow(ctx, q, rrow.GLAccountCode, rrow.GLAccountName, rrow.GLAccountType, rrow.Status, rrow.Source, rrow.ErpRef, rrow.CashflowCategoryName).Scan(&id); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": err.Error(), "gl_account_code": rrow.GLAccountCode})
				continue
			}

			auditQ := `INSERT INTO auditactionglaccount (gl_account_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
			if _, err := tx.Exec(ctx, auditQ, id, nil, createdBy); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": "audit insert failed: " + err.Error(), "id": id})
				continue
			}

			if err := tx.Commit(ctx); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "id": id})
				continue
			}
			created = append(created, map[string]interface{}{"success": true, "gl_account_id": id, "gl_account_code": rrow.GLAccountCode})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": created})
	}
}

type glaAuditInfo struct{ CreatedBy, CreatedAt, EditedBy, EditedAt, DeletedBy, DeletedAt string }

// GetGLAccountNamesWithID returns rows with latest audit and history
func GetGLAccountNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()
		mainQ := `
            WITH latest AS (
                SELECT DISTINCT ON (gl_account_id) gl_account_id, processing_status, requested_by, requested_at, actiontype, action_id, checker_by, checker_at, checker_comment, reason
                FROM auditactionglaccount
                ORDER BY gl_account_id, requested_at DESC
            )
            SELECT m.gl_account_id, m.gl_account_code, m.gl_account_name, m.gl_account_type, m.status, m.source, m.erp_ref, m.cashflow_category_name, m.old_cashflow_category_name, m.old_gl_account_code, m.old_gl_account_name, m.old_gl_account_type, m.old_status, m.old_source, m.old_erp_ref,
                   l.processing_status, l.requested_by, l.requested_at, l.actiontype, l.action_id, l.checker_by, l.checker_at, l.checker_comment, l.reason
            FROM masterglaccount m
            LEFT JOIN latest l ON l.gl_account_id = m.gl_account_id
        `

		rows, err := pgxPool.Query(ctx, mainQ)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		order := []string{}
		outMap := map[string]map[string]interface{}{}
		for rows.Next() {
			var (
				id                                                              string
				code, name, typ, status, source, erp, cat                       interface{}
				oldCat, oldCode, oldName, oldType, oldStatus, oldSource, oldErp interface{}
				processingStatus, requestedBy, requestedAt, actionType          interface{}
				actionID                                                        string
				checkerBy, checkerAt, checkerComment, reason                    interface{}
			)
			if err := rows.Scan(&id, &code, &name, &typ, &status, &source, &erp, &cat, &oldCat, &oldCode, &oldName, &oldType, &oldStatus, &oldSource, &oldErp, &processingStatus, &requestedBy, &requestedAt, &actionType, &actionID, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
				continue
			}
			order = append(order, id)
			outMap[id] = map[string]interface{}{
				"gl_account_id":              id,
				"gl_account_code":            ifaceToString(code),
				"gl_account_name":            ifaceToString(name),
				"gl_account_type":            ifaceToString(typ),
				"status":                     ifaceToString(status),
				"source":                     ifaceToString(source),
				"erp_ref":                    ifaceToString(erp),
				"cashflow_category_name":     ifaceToString(cat),
				"old_cashflow_category_name": ifaceToString(oldCat),
				"old_gl_account_code":        ifaceToString(oldCode),
				"old_gl_account_name":        ifaceToString(oldName),
				"old_gl_account_type":        ifaceToString(oldType),
				"old_status":                 ifaceToString(oldStatus),
				"old_source":                 ifaceToString(oldSource),
				"old_erp_ref":                ifaceToString(oldErp),
				"processing_status":          ifaceToString(processingStatus),
				"requested_by":               ifaceToString(requestedBy),
				"requested_at":               ifaceToTimeString(requestedAt),
				"action_type":                ifaceToString(actionType),
				"action_id":                  actionID,
				"checker_by":                 ifaceToString(checkerBy),
				"checker_at":                 ifaceToTimeString(checkerAt),
				"checker_comment":            ifaceToString(checkerComment),
				"reason":                     ifaceToString(reason),
				"created_by":                 "", "created_at": "", "edited_by": "", "edited_at": "", "deleted_by": "", "deleted_at": "",
			}
		}

		if len(order) > 0 {
			auditQ := `SELECT gl_account_id, actiontype, requested_by, requested_at FROM auditactionglaccount WHERE gl_account_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY gl_account_id, requested_at DESC`
			arows, aerr := pgxPool.Query(ctx, auditQ, order)
			if aerr == nil {
				defer arows.Close()
				auditMap := map[string]*glaAuditInfo{}
				for arows.Next() {
					var tid, atype string
					var rby, rat interface{}
					if err := arows.Scan(&tid, &atype, &rby, &rat); err != nil {
						continue
					}
					if auditMap[tid] == nil {
						auditMap[tid] = &glaAuditInfo{}
					}
					switch strings.ToUpper(strings.TrimSpace(atype)) {
					case "CREATE":
						if auditMap[tid].CreatedBy == "" {
							auditMap[tid].CreatedBy = ifaceToString(rby)
							auditMap[tid].CreatedAt = ifaceToTimeString(rat)
						}
					case "EDIT":
						if auditMap[tid].EditedBy == "" {
							auditMap[tid].EditedBy = ifaceToString(rby)
							auditMap[tid].EditedAt = ifaceToTimeString(rat)
						}
					case "DELETE":
						if auditMap[tid].DeletedBy == "" {
							auditMap[tid].DeletedBy = ifaceToString(rby)
							auditMap[tid].DeletedAt = ifaceToTimeString(rat)
						}
					}
				}
				for _, tid := range order {
					if a, ok := auditMap[tid]; ok {
						if rec, ok2 := outMap[tid]; ok2 {
							rec["created_by"] = a.CreatedBy
							rec["created_at"] = a.CreatedAt
							rec["edited_by"] = a.EditedBy
							rec["edited_at"] = a.EditedAt
							rec["deleted_by"] = a.DeletedBy
							rec["deleted_at"] = a.DeletedAt
							outMap[tid] = rec
						}
					}
				}
			}
		}

		out := make([]map[string]interface{}, 0, len(order))
		for _, tid := range order {
			if rec, ok := outMap[tid]; ok {
				out = append(out, rec)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}

// GetApprovedActiveGLAccounts returns minimal rows where latest audit is APPROVED and status is ACTIVE
func GetApprovedActiveGLAccounts(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		ctx := r.Context()
		q := `
            WITH latest AS (
                SELECT DISTINCT ON (gl_account_id) gl_account_id, processing_status
                FROM auditactionglaccount
                ORDER BY gl_account_id, requested_at DESC
            )
            SELECT m.gl_account_id, m.gl_account_code, m.gl_account_name, m.gl_account_type
            FROM masterglaccount m
            JOIN latest l ON l.gl_account_id = m.gl_account_id
            WHERE UPPER(l.processing_status) = 'APPROVED' AND UPPER(m.status) = 'ACTIVE'
        `
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		out := []map[string]interface{}{}
		for rows.Next() {
			var id string
			var code, name, typ interface{}
			if err := rows.Scan(&id, &code, &name, &typ); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{"gl_account_id": id, "gl_account_code": ifaceToString(code), "gl_account_name": ifaceToString(name), "gl_account_type": ifaceToString(typ)})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}

// UpdateGLAccountBulk updates multiple GL accounts and creates edit audit actions
func UpdateGLAccountBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				GLAccountID string                 `json:"gl_account_id"`
				Fields      map[string]interface{} `json:"fields"`
				Reason      string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		updatedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				updatedBy = s.Email
				break
			}
		}
		if updatedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := r.Context()
		results := []map[string]interface{}{}
		for _, row := range req.Rows {
			if row.GLAccountID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "missing gl_account_id"})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "begin failed: " + err.Error()})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback(ctx)
					}
				}()
				sel := `SELECT gl_account_code, gl_account_name, gl_account_type, status, source, erp_ref, cashflow_category_name FROM masterglaccount WHERE gl_account_id=$1 FOR UPDATE`
				var existingCode, existingName, existingType, existingStatus, existingSource, existingErp, existingCat interface{}
				if err := tx.QueryRow(ctx, sel, row.GLAccountID).Scan(&existingCode, &existingName, &existingType, &existingStatus, &existingSource, &existingErp, &existingCat); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "fetch failed: " + err.Error(), "gl_account_id": row.GLAccountID})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range row.Fields {
					switch k {
					case "gl_account_code":
						sets = append(sets, fmt.Sprintf("gl_account_code=$%d, old_gl_account_code=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCode))
						pos += 2
					case "gl_account_name":
						sets = append(sets, fmt.Sprintf("gl_account_name=$%d, old_gl_account_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingName))
						pos += 2
					case "gl_account_type":
						sets = append(sets, fmt.Sprintf("gl_account_type=$%d, old_gl_account_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingType))
						pos += 2
					case "status":
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatus))
						pos += 2
					case "source":
						sets = append(sets, fmt.Sprintf("source=$%d, old_source=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingSource))
						pos += 2
					case "erp_ref":
						sets = append(sets, fmt.Sprintf("erp_ref=$%d, old_erp_ref=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingErp))
						pos += 2
					case "cashflow_category_name":
						sets = append(sets, fmt.Sprintf("cashflow_category_name=$%d, old_cashflow_category_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCat))
						pos += 2
					default:
						// ignore
					}
				}
				updatedID := row.GLAccountID
				if len(sets) > 0 {
					q := "UPDATE masterglaccount SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE gl_account_id=$%d RETURNING gl_account_id", pos)
					args = append(args, row.GLAccountID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedID); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "update failed: " + err.Error(), "gl_account_id": row.GLAccountID})
						return
					}
				}
				auditQ := `INSERT INTO auditactionglaccount (gl_account_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, updatedID, row.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "audit failed: " + err.Error(), "gl_account_id": updatedID})
					return
				}
				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "gl_account_id": updatedID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "gl_account_id": updatedID})
			}()
		}
		overall := true
		for _, r := range results {
			if ok, exists := r["success"]; exists {
				if b, okb := ok.(bool); okb {
					if !b {
						overall = false
						break
					}
				} else {
					overall = false
					break
				}
			} else {
				overall = false
				break
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": overall, "rows": results})
	}
}

// DeleteGLAccount inserts a DELETE audit action
func DeleteGLAccount(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			UserID      string `json:"user_id"`
			GLAccountID string `json:"gl_account_id"`
			Reason      string `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == body.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		ctx := r.Context()
		if body.GLAccountID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "gl_account_id required")
			return
		}
		q := `INSERT INTO auditactionglaccount (gl_account_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`
		if _, err := pgxPool.Exec(ctx, q, body.GLAccountID, body.Reason, requestedBy); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
	}
}

// BulkRejectGLAccountActions rejects latest audit actions for gl_account_ids
func BulkRejectGLAccountActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			GLAccountIDs []string `json:"gl_account_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to start transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()
		sel := `SELECT DISTINCT ON (gl_account_id) action_id, gl_account_id, processing_status FROM auditactionglaccount WHERE gl_account_id = ANY($1) ORDER BY gl_account_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.GLAccountIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()
		actionIDs := []string{}
		found := map[string]bool{}
		cannotReject := []string{}
		for rows.Next() {
			var aid, gid, pstatus string
			if err := rows.Scan(&aid, &gid, &pstatus); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to scan audit rows: "+err.Error())
				return
			}
			found[gid] = true
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotReject = append(cannotReject, gid)
			}
			actionIDs = append(actionIDs, aid)
		}
		missing := []string{}
		for _, gid := range req.GLAccountIDs {
			if !found[gid] {
				missing = append(missing, gid)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for gl_account_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved gl_account_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}
		upd := `UPDATE auditactionglaccount SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, gl_account_id`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to update audit rows: "+err.Error())
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		for urows.Next() {
			var aid, gid string
			if err := urows.Scan(&aid, &gid); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "gl_account_id": gid})
			}
		}
		if len(updated) != len(actionIDs) {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("updated %d of %d actions, aborting", len(updated), len(actionIDs)))
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		committed = true
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}

// BulkApproveGLAccountActions approves latest audit actions for gl_account_ids
func BulkApproveGLAccountActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			GLAccountIDs []string `json:"gl_account_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to start transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()
		sel := `SELECT DISTINCT ON (gl_account_id) action_id, gl_account_id, actiontype, processing_status FROM auditactionglaccount WHERE gl_account_id = ANY($1) ORDER BY gl_account_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.GLAccountIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()
		actionIDs := []string{}
		found := map[string]bool{}
		cannotApprove := []string{}
		actionTypeByID := map[string]string{}
		for rows.Next() {
			var aid, gid, atype, pstatus string
			if err := rows.Scan(&aid, &gid, &atype, &pstatus); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to scan audit rows: "+err.Error())
				return
			}
			found[gid] = true
			actionIDs = append(actionIDs, aid)
			actionTypeByID[gid] = atype
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotApprove = append(cannotApprove, gid)
			}
		}
		missing := []string{}
		for _, gid := range req.GLAccountIDs {
			if !found[gid] {
				missing = append(missing, gid)
			}
		}
		if len(missing) > 0 || len(cannotApprove) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for gl_account_ids: %v. ", missing)
			}
			if len(cannotApprove) > 0 {
				msg += fmt.Sprintf("cannot approve already approved gl_account_ids: %v", cannotApprove)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}
		upd := `UPDATE auditactionglaccount SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, gl_account_id, actiontype`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to update audit rows: "+err.Error())
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		deleteIDs := []string{}
		for urows.Next() {
			var aid, gid, atype string
			if err := urows.Scan(&aid, &gid, &atype); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "gl_account_id": gid, "action_type": atype})
				if strings.ToUpper(strings.TrimSpace(atype)) == "DELETE" {
					deleteIDs = append(deleteIDs, gid)
				}
			}
		}
		if len(updated) != len(actionIDs) {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("updated %d of %d actions, aborting", len(updated), len(actionIDs)))
			return
		}
		if len(deleteIDs) > 0 {
			delQ := `DELETE FROM masterglaccount WHERE gl_account_id = ANY($1)`
			if _, err := tx.Exec(ctx, delQ, deleteIDs); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to delete master rows: "+err.Error())
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		committed = true
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "updated": updated})
	}
}

// UploadGLAccount handles staging and mapping for masterglaccount
func UploadGLAccount(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get("Content-Type") == "application/json" {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				http.Error(w, "user_id required in body", http.StatusBadRequest)
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue("user_id")
			if userID == "" {
				http.Error(w, "user_id required in form", http.StatusBadRequest)
				return
			}
		}
		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			http.Error(w, "User not found in active sessions", http.StatusUnauthorized)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse multipart form")
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No files uploaded")
			return
		}
		batchIDs := make([]string, 0, len(files))
		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "Failed to open file: "+fh.Filename)
				return
			}
			ext := getFileExt(fh.Filename)
			records, err := parseCashFlowCategoryFile(f, ext)
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, http.StatusBadRequest, "Invalid or empty file: "+fh.Filename)
				return
			}
			headerRow := records[0]
			dataRows := records[1:]
			batchID := uuid.New().String()
			batchIDs = append(batchIDs, batchID)
			colCount := len(headerRow)
			copyRows := make([][]interface{}, len(dataRows))
			for i, row := range dataRows {
				vals := make([]interface{}, colCount+1)
				vals[0] = batchID
				for j := 0; j < colCount; j++ {
					if j < len(row) {
						cell := strings.TrimSpace(row[j])
						if cell == "" {
							vals[j+1] = nil
						} else {
							vals[j+1] = cell
						}
					} else {
						vals[j+1] = nil
					}
				}
				copyRows[i] = vals
			}
			headerNorm := make([]string, len(headerRow))
			for i, h := range headerRow {
				hn := strings.TrimSpace(h)
				// remove stray trailing commas or punctuation from CSV headers
				hn = strings.Trim(hn, ", ")
				hn = strings.ToLower(hn)
				hn = strings.ReplaceAll(hn, " ", "_")
				hn = strings.Trim(hn, "\"'`")
				headerNorm[i] = hn
			}
			columns := append([]string{"upload_batch_id"}, headerNorm...)
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to start transaction: "+err.Error())
				return
			}
			committed := false
			defer func() {
				if !committed {
					tx.Rollback(ctx)
				}
			}()
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"input_glaccount_table"}, columns, pgx.CopyFromRows(copyRows))
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_glaccount`)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Mapping error")
				return
			}
			mapping := map[string]string{}
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					key := strings.ToLower(strings.TrimSpace(src))
					// handle stray commas in mapping source too
					key = strings.Trim(key, ", ")
					key = strings.ReplaceAll(key, " ", "_")
					// sanitize target field name: trim spaces, trailing commas and quotes
					tt := strings.TrimSpace(tgt)
					tt = strings.Trim(tt, ", \"'`")
					tt = strings.ReplaceAll(tt, " ", "_")
					mapping[key] = tt
				}
			}
			mapRows.Close()
			var srcCols, tgtCols []string
			for i, h := range headerRow {
				key := headerNorm[i]
				if t, ok := mapping[key]; ok {
					srcCols = append(srcCols, key)
					tgtCols = append(tgtCols, t)
				} else {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("No mapping for source column: %s", h))
					return
				}
			}
			tgtColsStr := strings.Join(tgtCols, ", ")
			var selectExprs []string
			for i, src := range srcCols {
				tgt := tgtCols[i]
				selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", src, tgt))
			}
			srcColsStr := strings.Join(selectExprs, ", ")
			insertSQL := fmt.Sprintf(`INSERT INTO masterglaccount (%s) SELECT %s FROM input_glaccount_table s WHERE s.upload_batch_id = $1 RETURNING gl_account_id`, tgtColsStr, srcColsStr)
			rows, err := tx.Query(ctx, insertSQL, batchID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Final insert error: "+err.Error())
				return
			}
			var newIDs []string
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err == nil {
					newIDs = append(newIDs, id)
				}
			}
			rows.Close()
			if len(newIDs) > 0 {
				auditSQL := `INSERT INTO auditactionglaccount (gl_account_id, actiontype, processing_status, reason, requested_by, requested_at) SELECT gl_account_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() FROM masterglaccount WHERE gl_account_id = ANY($2)`
				if _, err := tx.Exec(ctx, auditSQL, userName, newIDs); err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "Failed to insert audit actions: "+err.Error())
					return
				}
			}
			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Commit failed: "+err.Error())
				return
			}
			committed = true
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "batch_ids": batchIDs})
	}
}

// helper nullableUUID reused if needed
// func nullableUUID(s string) interface{} {
//     s = strings.TrimSpace(s)
//     if s == "" { return nil }
//     if _, err := uuid.Parse(s); err == nil { return s }
//     return s
// }
// package allMaster
