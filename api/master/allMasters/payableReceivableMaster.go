package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"

	// "mime/multipart"
	"net/http"
	"strings"

	// "time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Minimal request shape for create/sync
type PayableReceivableRequest struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Category    string `json:"category"`
	Description string `json:"description"`
	Status      string `json:"status"`
}

// CreatePayableReceivableTypes inserts rows into masterpayablereceivabletype and creates audit entries
func CreatePayableReceivableTypes(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                     `json:"user_id"`
			Rows   []PayableReceivableRequest `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// validate session and get created_by
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		ctx := context.Background()
		created := make([]map[string]interface{}, 0)

		for _, rrow := range req.Rows {
			if rrow.Type == "" || rrow.Name == "" || rrow.Category == "" || rrow.Status == "" {
				created = append(created, map[string]interface{}{"success": false, "error": "missing required fields", "name": rrow.Name})
				continue
			}
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				created = append(created, map[string]interface{}{"success": false, "error": "failed to start tx: " + err.Error()})
				continue
			}

			var id string
			q := `INSERT INTO masterpayablereceivabletype (type, name, category, description, status) VALUES ($1,$2,$3,$4,$5) RETURNING type_id`
			if err := tx.QueryRow(ctx, q, rrow.Type, rrow.Name, rrow.Category, rrow.Description, rrow.Status).Scan(&id); err != nil {
				tx.Rollback(ctx)
				created = append(created, map[string]interface{}{"success": false, "error": err.Error(), "name": rrow.Name})
				continue
			}

			// insert audit
			auditQ := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL', $2, $3, now())`
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
			created = append(created, map[string]interface{}{"success": true, "type_id": id, "name": rrow.Name})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(created)
	}
}

type AuditInfo struct {
	CreatedBy string
	CreatedAt string
	EditedBy  string
	EditedAt  string
	DeletedBy string
	DeletedAt string
}

func GetPayableReceivableNamesWithID(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// validate session
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

		// Step 1: fetch base rows with latest audit info
		query := `
			SELECT m.type_id, m.type, m.name, m.category, m.description, m.status,
			       m.old_type, m.old_name, m.old_category, m.old_description, m.old_status,
			       a.processing_status, a.requested_by, a.requested_at, a.actiontype, a.action_id,
			       a.checker_by, a.checker_at, a.checker_comment, a.reason
			FROM masterpayablereceivabletype m
			LEFT JOIN LATERAL (
				SELECT processing_status, requested_by, requested_at, actiontype, action_id,
				       checker_by, checker_at, checker_comment, reason
				FROM auditactionpayablereceivable a
				WHERE a.type_id = m.type_id
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		type AuditInfo struct {
			CreatedBy string
			CreatedAt string
			EditedBy  string
			EditedAt  string
			DeletedBy string
			DeletedAt string
		}

		out := []map[string]interface{}{}
		typeIDs := []string{}

		for rows.Next() {
			var (
				typeID            string
				typeI             interface{}
				nameI             interface{}
				categoryI         interface{}
				descI             interface{}
				statusI           interface{}
				oldTypeI          interface{}
				oldNameI          interface{}
				oldCategoryI      interface{}
				oldDescI          interface{}
				oldStatusI        interface{}
				processingStatusI interface{}
				requestedByI      interface{}
				requestedAtI      interface{}
				actionTypeI       interface{}
				actionIDI         string
				checkerByI        interface{}
				checkerAtI        interface{}
				checkerCommentI   interface{}
				reasonI           interface{}
			)

			if err := rows.Scan(
				&typeID,
				&typeI, &nameI, &categoryI, &descI, &statusI,
				&oldTypeI, &oldNameI, &oldCategoryI, &oldDescI, &oldStatusI,
				&processingStatusI, &requestedByI, &requestedAtI, &actionTypeI, &actionIDI,
				&checkerByI, &checkerAtI, &checkerCommentI, &reasonI,
			); err != nil {
				continue
			}

			record := map[string]interface{}{
				"type_id":           typeID,
				"type":              ifaceToString(typeI),
				"name":              ifaceToString(nameI),
				"category":          ifaceToString(categoryI),
				"description":       ifaceToString(descI),
				"status":            ifaceToString(statusI),
				"old_type":          ifaceToString(oldTypeI),
				"old_name":          ifaceToString(oldNameI),
				"old_category":      ifaceToString(oldCategoryI),
				"old_description":   ifaceToString(oldDescI),
				"old_status":        ifaceToString(oldStatusI),
				"processing_status": ifaceToString(processingStatusI),
				"requested_by":      ifaceToString(requestedByI),
				"requested_at":      ifaceToTimeString(requestedAtI),
				"action_type":       ifaceToString(actionTypeI),
				"action_id":         actionIDI,
				"checker_by":        ifaceToString(checkerByI),
				"checker_at":        ifaceToTimeString(checkerAtI),
				"checker_comment":   ifaceToString(checkerCommentI),
				"reason":            ifaceToString(reasonI),
			}

			out = append(out, record)
			typeIDs = append(typeIDs, typeID)
		}

		// Step 2: bulk fetch audit history (CREATE/EDIT/DELETE)
		if len(typeIDs) > 0 {
			queryAudit := `
				SELECT type_id, actiontype, requested_by, requested_at
				FROM auditactionpayablereceivable
				WHERE type_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE')
				ORDER BY requested_at DESC
			`
			auditRows, err := pgxPool.Query(ctx, queryAudit, typeIDs)
			if err == nil {
				defer auditRows.Close()

				auditMap := make(map[string]*AuditInfo)
				for auditRows.Next() {
					var (
						tid   string
						atype string
						rby   interface{}
						rat   interface{}
					)
					if err := auditRows.Scan(&tid, &atype, &rby, &rat); err == nil {
						if auditMap[tid] == nil {
							auditMap[tid] = &AuditInfo{}
						}
						switch atype {
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
				}

				// Step 3: merge audit info back into output
				for i, rec := range out {
					tid := rec["type_id"].(string)
					if audit, ok := auditMap[tid]; ok {
						rec["created_by"] = audit.CreatedBy
						rec["created_at"] = audit.CreatedAt
						rec["edited_by"] = audit.EditedBy
						rec["edited_at"] = audit.EditedAt
						rec["deleted_by"] = audit.DeletedBy
						rec["deleted_at"] = audit.DeletedAt
						out[i] = rec
					}
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}

func GetApprovedActivePayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}

		// validate session
		valid := false
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
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
		// Use a single set-based query: pick latest audit row per type_id with DISTINCT ON
		// This avoids executing a correlated subquery per master row (N+1).
		q := `
			WITH latest AS (
				SELECT DISTINCT ON (type_id) type_id, processing_status
				FROM auditactionpayablereceivable
				ORDER BY type_id, requested_at DESC
			)
			SELECT m.type_id, m.type, m.name, m.category
			FROM masterpayablereceivabletype m
			JOIN latest l ON l.type_id = m.type_id
			WHERE UPPER(l.processing_status) = 'APPROVED' AND UPPER(m.status) = 'ACTIVE'
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var (
				typeID    string
				typeI     interface{}
				nameI     interface{}
				categoryI interface{}
			)
			if err := rows.Scan(&typeID, &typeI, &nameI, &categoryI); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{
				"type_id":  typeID,
				"type":     ifaceToString(typeI),
				"name":     ifaceToString(nameI),
				"category": ifaceToString(categoryI),
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}

// UpdatePayableReceivableBulk updates multiple rows with old_* preservation and audit
func UpdatePayableReceivableBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				TypeID string                 `json:"type_id"`
				Fields map[string]interface{} `json:"fields"`
				Reason string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		// get updated_by
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
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
			if row.TypeID == "" {
				results = append(results, map[string]interface{}{"success": false, "error": "missing type_id"})
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

				// fetch existing
				var existingType, existingName, existingCategory, existingDescription, existingStatus interface{}
				sel := `SELECT type, name, category, description, status FROM masterpayablereceivabletype WHERE type_id=$1 FOR UPDATE`
				if err := tx.QueryRow(ctx, sel, row.TypeID).Scan(&existingType, &existingName, &existingCategory, &existingDescription, &existingStatus); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "fetch failed: " + err.Error(), "type_id": row.TypeID})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range row.Fields {
					switch k {
					case "type":
						sets = append(sets, fmt.Sprintf("type=$%d, old_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingType))
						pos += 2
					case "name":
						sets = append(sets, fmt.Sprintf("name=$%d, old_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingName))
						pos += 2
					case "category":
						sets = append(sets, fmt.Sprintf("category=$%d, old_category=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingCategory))
						pos += 2
					case "description":
						sets = append(sets, fmt.Sprintf("description=$%d, old_description=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingDescription))
						pos += 2
					case "status":
						sets = append(sets, fmt.Sprintf("status=$%d, old_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), ifaceToString(existingStatus))
						pos += 2
					default:
						// ignore
					}
				}

				updatedID := row.TypeID
				if len(sets) > 0 {
					q := "UPDATE masterpayablereceivabletype SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE type_id=$%d RETURNING type_id", pos)
					args = append(args, row.TypeID)
					if err := tx.QueryRow(ctx, q, args...).Scan(&updatedID); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "update failed: " + err.Error(), "type_id": row.TypeID})
						return
					}
				}

				// audit
				auditQ := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL', $2, $3, now())`
				if _, err := tx.Exec(ctx, auditQ, updatedID, row.Reason, updatedBy); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "audit failed: " + err.Error(), "type_id": updatedID})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "commit failed: " + err.Error(), "type_id": updatedID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "type_id": updatedID})
			}()
		}

		// compute overall success for bulk response
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

// DeletePayableReceivable inserts a DELETE audit action (no hard delete)
// DeletePayableReceivable inserts DELETE audit actions for one or more type_ids (bulk, all-or-nothing)
func DeletePayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TypeIDs []string `json:"type_ids"`
			Reason  string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		// validate session / user
		requestedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}

		if len(req.TypeIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "type_ids required")
			return
		}

		ctx := r.Context()
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

		// Insert an audit action per type_id; all-or-nothing
		q := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`
		for _, tid := range req.TypeIDs {
			if strings.TrimSpace(tid) == "" {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusBadRequest, "empty type_id provided")
				return
			}
			if _, err := tx.Exec(ctx, q, tid, req.Reason, requestedBy); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			tx.Rollback(ctx)
			api.RespondWithError(w, http.StatusInternalServerError, "commit failed: "+err.Error())
			return
		}
		committed = true

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
	}
}

// BulkRejectPayableReceivableActions rejects audit actions for type_ids and returns updated rows
func BulkRejectPayableReceivableActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TypeIDs []string `json:"type_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
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
		// start transaction for all-or-nothing behavior
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

		// fetch the latest audit action per type_id
		// use DISTINCT ON to get latest requested_at per type_id
		sel := `SELECT DISTINCT ON (type_id) action_id, type_id, processing_status FROM auditactionpayablereceivable WHERE type_id = ANY($1) ORDER BY type_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.TypeIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		foundTypes := map[string]bool{}
		var cannotReject []string
		for rows.Next() {
			var aid, tid, pstatus string
			if err := rows.Scan(&aid, &tid, &pstatus); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to scan audit rows: "+err.Error())
				return
			}
			foundTypes[tid] = true
			// if already APPROVED, we consider this a blocker
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotReject = append(cannotReject, tid)
			}
			actionIDs = append(actionIDs, aid)
		}

		// check for missing audit rows
		missing := []string{}
		for _, tid := range req.TypeIDs {
			if !foundTypes[tid] {
				missing = append(missing, tid)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for type_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved type_ids: %v", cannotReject)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		// perform update on the selected action_ids
		upd := `UPDATE auditactionpayablereceivable SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, type_id`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to update audit rows: "+err.Error())
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		for urows.Next() {
			var aid, tid string
			if err := urows.Scan(&aid, &tid); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "type_id": tid})
			}
		}

		// ensure we updated all action IDs
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

// BulkApprovePayableReceivableActions approves audit actions for type_ids
func BulkApprovePayableReceivableActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TypeIDs []string `json:"type_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
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
		// Start a transaction for all-or-nothing behavior
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

		// Fetch latest audit row per type_id
		sel := `SELECT DISTINCT ON (type_id) action_id, type_id, actiontype, processing_status FROM auditactionpayablereceivable WHERE type_id = ANY($1) ORDER BY type_id, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.TypeIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch audit rows: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0)
		foundTypes := map[string]bool{}
		cannotApprove := []string{}
		actionTypeByType := map[string]string{}
		for rows.Next() {
			var aid, tid, atype, pstatus string
			if err := rows.Scan(&aid, &tid, &atype, &pstatus); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to scan audit rows: "+err.Error())
				return
			}
			foundTypes[tid] = true
			actionIDs = append(actionIDs, aid)
			actionTypeByType[tid] = atype
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotApprove = append(cannotApprove, tid)
			}
		}

		// check for missing audit rows
		missing := []string{}
		for _, tid := range req.TypeIDs {
			if !foundTypes[tid] {
				missing = append(missing, tid)
			}
		}
		if len(missing) > 0 || len(cannotApprove) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for type_ids: %v. ", missing)
			}
			if len(cannotApprove) > 0 {
				msg += fmt.Sprintf("cannot approve already approved type_ids: %v", cannotApprove)
			}
			api.RespondWithError(w, http.StatusBadRequest, msg)
			return
		}

		// Approve the selected audit rows
		upd := `UPDATE auditactionpayablereceivable SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, type_id, actiontype`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to update audit rows: "+err.Error())
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		deleteTypeIDs := make([]string, 0)
		for urows.Next() {
			var aid, tid, atype string
			if err := urows.Scan(&aid, &tid, &atype); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "type_id": tid, "action_type": atype})
				if strings.ToUpper(strings.TrimSpace(atype)) == "DELETE" {
					deleteTypeIDs = append(deleteTypeIDs, tid)
				}
			}
		}

		if len(updated) != len(actionIDs) {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("updated %d of %d actions, aborting", len(updated), len(actionIDs)))
			return
		}

		// If any actions are DELETE, remove the corresponding master rows
		if len(deleteTypeIDs) > 0 {
			delQ := `DELETE FROM masterpayablereceivabletype WHERE type_id = ANY($1)`
			if _, err := tx.Exec(ctx, delQ, deleteTypeIDs); err != nil {
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

// UploadPayableReceivable handles staging and mapping for masterpayablereceivabletype
func UploadPayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get("Content-Type") == "application/json" {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in body")
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue("user_id")
			if userID == "" {
				api.RespondWithError(w, http.StatusBadRequest, "user_id required in form")
				return
			}
		}
		// Fetch user name from active sessions
		userName := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusUnauthorized, "User not found in active sessions")
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
			// normalize header names to match staging table column names
			headerNorm := make([]string, len(headerRow))
			for i, h := range headerRow {
				hn := strings.TrimSpace(h)
				// remove stray trailing commas or punctuation from CSV headers
				hn = strings.Trim(hn, ", ")
				hn = strings.ToLower(hn)
				hn = strings.ReplaceAll(hn, " ", "_")
				// ensure no surrounding quotes
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

			// stage
			_, err = tx.CopyFrom(ctx, pgx.Identifier{"input_payablereceivable_table"}, columns, pgx.CopyFromRows(copyRows))
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to stage data: "+err.Error())
				return
			}

			// read mapping
			mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_payablereceivable`)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Mapping error")
				return
			}
			mapping := make(map[string]string)
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					key := strings.ToLower(strings.TrimSpace(src))
					key = strings.ReplaceAll(key, " ", "_")
					// sanitize target field name: trim spaces, trailing commas and quotes
					tt := strings.TrimSpace(tgt)
					tt = strings.Trim(tt, ", \"'`")
					tt = strings.ReplaceAll(tt, " ", "_")
					mapping[key] = tt
				}
			}
			mapRows.Close()
			var srcCols []string
			var tgtCols []string
			for i, h := range headerRow {
				// lookup mapping using normalized header
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

			insertSQL := fmt.Sprintf(`
                INSERT INTO masterpayablereceivabletype (%s)
                SELECT %s
                FROM input_payablereceivable_table s
                WHERE s.upload_batch_id = $1
                RETURNING type_id
            `, tgtColsStr, srcColsStr)
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
				auditSQL := `INSERT INTO auditactionpayablereceivable (type_id, actiontype, processing_status, reason, requested_by, requested_at) SELECT type_id, 'CREATE', 'PENDING_APPROVAL', NULL, $1, now() FROM masterpayablereceivabletype WHERE type_id = ANY($2)`
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
