package catalog

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"

	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// Helper: send JSON error response
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": errMsg})
}

func getRequesterEmail() string {
	for _, s := range auth.GetActiveSessions() {
		if s.Email != "" {
			return s.Email
		}
	}
	return ""
}

// CreateEventSingle creates a single event row and an audit record (PENDING_APPROVAL)
func CreateEventSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string `json:"user_id"`
			ModuleCode    string `json:"module_code"`
			SubModuleCode string `json:"sub_module_code"`
			EventCode     string `json:"event_code"`
			EventName     string `json:"event_display_name"`
			Description   string `json:"description"`
			SourceRoute   string `json:"source_route"`
			IsActive      *bool  `json:"is_active"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.ModuleCode == "" || req.SubModuleCode == "" || req.EventCode == "" || req.EventName == "" {
			respondWithError(w, http.StatusBadRequest, "module_code, sub_module_code, event_code and event_display_name are required")
			return
		}

		if req.IsActive == nil {
			defaultActive := false
			req.IsActive = &defaultActive
		}

		userEmail := getRequesterEmail()
		if userEmail == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `
			INSERT INTO notification_svc.event (
				module_code, sub_module_code, event_code, event_display_name, description, source_route, is_active
			) VALUES ($1,$2,$3,$4,$5,$6,$7)
			RETURNING event_id
		`
		var eventID string
		err = tx.QueryRow(ctx, insertQ,
			req.ModuleCode, req.SubModuleCode, req.EventCode, req.EventName, req.Description, req.SourceRoute, req.IsActive,
		).Scan(&eventID)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		auditQ := `
			INSERT INTO notification_svc.audit_event (
				event_id, action_type, processing_status, requested_by, requested_at
			) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`
		if _, err := tx.Exec(ctx, auditQ, eventID, userEmail); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Seed one notification_config row per channel for this new event
		seedNotifConfigForEvents(ctx, pgxPool, []string{eventID}, userEmail)

		resp := map[string]any{constants.ValueSuccess: true, "event_id": eventID, "requested": userEmail}
		api.RespondWithPayload(w, true, "", resp)
		api.LogInfo("Event created: ID=%s, Code=%s", eventID, req.EventCode)
	}
}

// CreateEvent handles bulk create (simple payload with rows)
func CreateEvent(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				ModuleCode    string `json:"module_code"`
				SubModuleCode string `json:"sub_module_code"`
				EventCode     string `json:"event_code"`
				EventName     string `json:"event_display_name"`
				Description   string `json:"description"`
				SourceRoute   string `json:"source_route"`
				IsActive      *bool  `json:"is_active"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if len(req.Rows) == 0 {
			respondWithError(w, http.StatusBadRequest, "rows required")
			return
		}
		userEmail := getRequesterEmail()
		if userEmail == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(ctx)

		valueStrings := make([]string, 0, len(req.Rows))
		valueArgs := make([]interface{}, 0, len(req.Rows)*7)
		for i, rrow := range req.Rows {
			if rrow.IsActive == nil {
				def := false
				rrow.IsActive = &def
			}
			pos := i*7 + 1
			valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", pos, pos+1, pos+2, pos+3, pos+4, pos+5, pos+6))
			valueArgs = append(valueArgs, rrow.ModuleCode, rrow.SubModuleCode, rrow.EventCode, rrow.EventName, rrow.Description, rrow.SourceRoute, rrow.IsActive)
		}
		batchQ := fmt.Sprintf(`
			INSERT INTO notification_svc.event (
				module_code, sub_module_code, event_code, event_display_name, description, source_route, is_active
			) VALUES %s
			RETURNING event_id, event_display_name
		`, strings.Join(valueStrings, ","))

		rows, err := tx.Query(ctx, batchQ, valueArgs...)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var inserted []map[string]interface{}
		var eventIDs []string
		for rows.Next() {
			var id, name string
			if err := rows.Scan(&id, &name); err == nil {
				inserted = append(inserted, map[string]interface{}{"event_id": id, "event_display_name": name})
				eventIDs = append(eventIDs, id)
			}
		}

		if len(eventIDs) > 0 {
			// batch audit insert
			auditVals := make([]string, 0, len(eventIDs))
			auditArgs := make([]interface{}, 0, len(eventIDs)+1)
			auditArgPos := 1
			for _, id := range eventIDs {
				auditVals = append(auditVals, fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", auditArgPos, auditArgPos+1))
				auditArgs = append(auditArgs, id, userEmail)
				auditArgPos += 2
			}
			auditQ := fmt.Sprintf("INSERT INTO notification_svc.audit_event (event_id, action_type, processing_status, requested_by, requested_at) VALUES %s", strings.Join(auditVals, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				respondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Seed notification_config rows for all newly created events
		seedNotifConfigForEvents(ctx, pgxPool, eventIDs, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{"inserted": inserted})
		api.LogInfo("Bulk events created: %d", len(inserted))
	}
}

// UpdateEvent creates an EDIT audit record and marks processing_status as pending edit approval
func UpdateEvent(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string                 `json:"user_id"`
			EventID string                 `json:"event_id"`
			Fields  map[string]interface{} `json:"fields"`
			Reason  string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.EventID == "" || len(req.Fields) == 0 {
			respondWithError(w, http.StatusBadRequest, "event_id and fields required")
			return
		}
		userEmail := getRequesterEmail()
		if userEmail == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// fetch old values for audit
		sel := `
			SELECT event_display_name, description, is_active
			FROM notification_svc.event
			WHERE event_id = $1 FOR UPDATE
		`
		var oldName, oldDesc string
		var oldActive bool
		if err := tx.QueryRow(ctx, sel, req.EventID).Scan(&oldName, &oldDesc, &oldActive); err != nil {
			respondWithError(w, http.StatusBadRequest, "event not found")
			return
		}

		// Build update
		var sets []string
		var args []interface{}
		pos := 1
		allowed := map[string]bool{"event_display_name": true, "description": true, "source_route": true, "is_active": true}
		for k, v := range req.Fields {
			if !allowed[k] {
				continue
			}
			sets = append(sets, fmt.Sprintf("%s = $%d", k, pos))
			args = append(args, v)
			pos++
		}
		if len(sets) > 0 {
			q := fmt.Sprintf("UPDATE notification_svc.event SET %s WHERE event_id = $%d", strings.Join(sets, ", "), pos)
			args = append(args, req.EventID)
			if _, err := tx.Exec(ctx, q, args...); err != nil {
				respondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		// insert audit record
		auditQ := `
			INSERT INTO notification_svc.audit_event (
				event_id, action_type, processing_status, reason, requested_by, requested_at,
				old_event_display_name, old_description, old_is_active
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6)`
		if _, err := tx.Exec(ctx, auditQ, req.EventID, req.Reason, userEmail, oldName, oldDesc, oldActive); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"event_id": req.EventID, "requested": userEmail})
		api.LogInfo("Event update requested: ID=%s by %s", req.EventID, userEmail)
	}
}

// BulkApproveEvent approves pending audit records for events
func BulkApproveEvent(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			EventIDs []string `json:"event_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if len(req.EventIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "event_ids required")
			return
		}
		userEmail := getRequesterEmail()
		if userEmail == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE notification_svc.audit_event
			SET processing_status = 'APPROVED', checker_by = $1, checker_at = now(), checker_comment = $2
			WHERE event_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.EventIDs)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Handle DELETE approvals: mark master as deleted
		_, err = tx.Exec(ctx, `
			UPDATE notification_svc.event
			SET is_deleted = true
			WHERE event_id IN (
				SELECT DISTINCT a.event_id FROM notification_svc.audit_event a
				WHERE a.event_id = ANY($1::text[]) AND a.action_type = 'DELETE' AND a.processing_status = 'APPROVED'
			)
		`, req.EventIDs)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"approved_count": len(req.EventIDs), "checker": userEmail})
		api.LogInfo("Bulk event approvals: %d by %s", len(req.EventIDs), userEmail)
	}
}

// BulkRejectEvent rejects pending audit records for events
func BulkRejectEvent(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			EventIDs []string `json:"event_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if len(req.EventIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "event_ids required")
			return
		}
		userEmail := getRequesterEmail()
		if userEmail == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE notification_svc.audit_event
			SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_comment = $2
			WHERE event_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.EventIDs)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"rejected_count": len(req.EventIDs), "checker": userEmail})
		api.LogInfo("Bulk event rejections: %d by %s", len(req.EventIDs), userEmail)
	}
}

// GetEventsApprovedActive returns active events with approved audit
func GetEventsApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			SELECT m.event_id, m.module_code, m.sub_module_code, m.event_code, m.event_display_name, m.description, m.source_route
			FROM notification_svc.event m
			INNER JOIN notification_svc.audit_event a ON a.event_id = m.event_id
			WHERE a.processing_status = 'APPROVED' AND m.is_active = true AND COALESCE(m.is_deleted,false) = false
			ORDER BY m.event_display_name
		`
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var id, module, sub, code, name, desc, route string
			if err := rows.Scan(&id, &module, &sub, &code, &name, &desc, &route); err == nil {
				out = append(out, map[string]interface{}{
					"event_id":           id,
					"module_code":        module,
					"sub_module_code":    sub,
					"event_code":         code,
					"event_display_name": name,
					"description":        desc,
					"source_route":       route,
				})
			}
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// GetEventAuditHistory returns audit rows for a given event_id
func GetEventAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EventID string `json:"event_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.EventID == "" {
			respondWithError(w, http.StatusBadRequest, "event_id required")
			return
		}
		ctx := r.Context()
		q := `SELECT audit_id, event_id, action_type, processing_status, reason, requested_by, requested_at, checker_by, checker_at, checker_comment, old_event_display_name, old_description, old_is_active FROM notification_svc.audit_event WHERE event_id = $1 ORDER BY requested_at DESC`
		rows, err := pgxPool.Query(ctx, q, req.EventID)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var auditID, eventID, actionType, procStatus string
			var reason, requestedBy, checkerBy, checkerComment *string
			var requestedAt, checkerAt interface{}
			var oldName, oldDesc *string
			var oldActive *bool
			if err := rows.Scan(&auditID, &eventID, &actionType, &procStatus, &reason, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &oldName, &oldDesc, &oldActive); err == nil {
				out = append(out, map[string]interface{}{
					"audit_id":               auditID,
					"event_id":               eventID,
					"action_type":            actionType,
					"processing_status":      procStatus,
					"reason":                 reason,
					"requested_by":           requestedBy,
					"requested_at":           requestedAt,
					"checker_by":             checkerBy,
					"checker_at":             checkerAt,
					"checker_comment":        checkerComment,
					"old_event_display_name": oldName,
					"old_description":        oldDesc,
					"old_is_active":          oldActive,
				})
			}
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// DeleteEvent marks events as delete-pending by inserting audit records
func DeleteEvent(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			EventIDs []string `json:"event_ids"`
			Reason   string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if len(req.EventIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, "event_ids required")
			return
		}
		userEmail := getRequesterEmail()
		if userEmail == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// verify ids exist and are not already deleted
		verifyQ := `SELECT event_id FROM notification_svc.event WHERE event_id = ANY($1::text[]) AND COALESCE(is_deleted,false)=false`
		rows, err := tx.Query(ctx, verifyQ, req.EventIDs)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var validIDs []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				validIDs = append(validIDs, id)
			}
		}

		if len(validIDs) == 0 {
			api.RespondWithPayload(w, false, "no valid event ids to delete", nil)
			return
		}

		// batch audit insert
		auditVals := make([]string, 0, len(validIDs))
		auditArgs := make([]interface{}, 0, len(validIDs)*2)
		pos := 1
		for _, id := range validIDs {
			auditVals = append(auditVals, fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,now(),$%d)", pos, pos+1, pos+2))
			// Using an extra param for reason (pos+2)
			auditArgs = append(auditArgs, id, userEmail, req.Reason)
			pos += 3
		}
		auditQ := fmt.Sprintf("INSERT INTO notification_svc.audit_event (event_id, action_type, processing_status, requested_by, requested_at, checker_comment) VALUES %s", strings.Join(auditVals, ","))
		if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"requested": userEmail, "requested_count": len(validIDs)})
		api.LogInfo("Delete requests created for events: %d", len(validIDs))
	}
}

// BulkUpdateEvent updates multiple events and inserts audit records (PENDING_EDIT_APPROVAL)
func BulkUpdateEvent(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				EventID string                 `json:"event_id"`
				Fields  map[string]interface{} `json:"fields"`
				Reason  string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if len(req.Rows) == 0 {
			respondWithError(w, http.StatusBadRequest, "rows required")
			return
		}
		userEmail := getRequesterEmail()
		if userEmail == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(ctx)

		var success []map[string]interface{}
		var errorsList []map[string]interface{}

		for _, row := range req.Rows {
			if row.EventID == "" || len(row.Fields) == 0 {
				errorsList = append(errorsList, map[string]interface{}{"event_id": row.EventID, "success": false, "error": "event_id and fields required"})
				continue
			}
			// fetch old values
			sel := `SELECT event_display_name, description, source_route, is_active FROM notification_svc.event WHERE event_id=$1 FOR UPDATE`
			var oldName, oldDesc, oldRoute string
			var oldActive bool
			if err := tx.QueryRow(ctx, sel, row.EventID).Scan(&oldName, &oldDesc, &oldRoute, &oldActive); err != nil {
				errorsList = append(errorsList, map[string]interface{}{"event_id": row.EventID, "success": false, "error": "not found"})
				continue
			}

			// Build update for allowed fields
			allowed := map[string]bool{"event_display_name": true, "description": true, "source_route": true, "is_active": true}
			var sets []string
			var args []interface{}
			pos := 1
			for k, v := range row.Fields {
				if !allowed[k] {
					continue
				}
				sets = append(sets, fmt.Sprintf("%s=$%d", k, pos))
				args = append(args, v)
				pos++
			}
			if len(sets) > 0 {
				q := fmt.Sprintf("UPDATE notification_svc.event SET %s WHERE event_id=$%d", strings.Join(sets, ", "), pos)
				args = append(args, row.EventID)
				if _, err := tx.Exec(ctx, q, args...); err != nil {
					errorsList = append(errorsList, map[string]interface{}{"event_id": row.EventID, "success": false, "error": err.Error()})
					continue
				}
			}

			// insert audit record with old values
			auditQ := `INSERT INTO notification_svc.audit_event (event_id, action_type, processing_status, reason, requested_by, requested_at, old_event_display_name, old_description, old_is_active) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6)`
			if _, err := tx.Exec(ctx, auditQ, row.EventID, row.Reason, userEmail, oldName, oldDesc, oldActive); err != nil {
				errorsList = append(errorsList, map[string]interface{}{"event_id": row.EventID, "success": false, "error": err.Error()})
				continue
			}

			success = append(success, map[string]interface{}{"event_id": row.EventID, "success": true})
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		allResults := append(success, errorsList...)
		api.RespondWithPayload(w, api.IsBulkSuccess(allResults), "", allResults)
	}
}

// GetEventsWithAudit returns all events with their audit history as a JSON array per row
func GetEventsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.event_id)
					a.event_id,
					a.processing_status,
					a.action_type,
					a.audit_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason,
					a.old_event_display_name,
					a.old_description,
					a.old_is_active
				FROM notification_svc.audit_event a
				ORDER BY a.event_id, GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT
					event_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM notification_svc.audit_event
				GROUP BY event_id
			)
			SELECT
				m.event_id,
				COALESCE(m.module_code,'') AS module_code,
				COALESCE(m.sub_module_code,'') AS sub_module_code,
				COALESCE(m.event_code,'') AS event_code,
				COALESCE(m.event_display_name,'') AS event_display_name,
				COALESCE(m.description,'') AS description,
				COALESCE(m.source_route,'') AS source_route,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,

				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_type,'') AS action_type,
				COALESCE(l.audit_id::text,'') AS audit_id,
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

			FROM notification_svc.event m
			LEFT JOIN latest_audit l ON l.event_id = m.event_id
			LEFT JOIN history h ON h.event_id = m.event_id
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC
		`
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.LogError("GetEventsWithAudit query failed: %v", err)
			respondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()
		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			vals, _ := rows.Values()
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			out = append(out, row)
		}

		if rows.Err() != nil {
			api.LogError("Row iteration error in GetEventsWithAudit: %v", rows.Err())
			respondWithError(w, http.StatusInternalServerError, "row scan error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("Retrieved %d events with audit data", len(out))
	}
}

// GetEvent returns a single event along with its audit history
func GetEvent(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EventID string `json:"event_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.EventID == "" {
			respondWithError(w, http.StatusBadRequest, "event_id required")
			return
		}
		ctx := r.Context()
		q := `
			SELECT m.event_id, m.module_code, m.sub_module_code, m.event_code, m.event_display_name, m.description, m.source_route, m.is_active, m.is_deleted,
				   COALESCE((SELECT json_agg(json_build_object(
					   'audit_id', a.audit_id, 'action_type', a.action_type, 'processing_status', a.processing_status,
					   'reason', a.reason, 'requested_by', a.requested_by, 'requested_at', a.requested_at,
					   'checker_by', a.checker_by, 'checker_at', a.checker_at, 'checker_comment', a.checker_comment,
					   'old_event_display_name', a.old_event_display_name, 'old_description', a.old_description, 'old_is_active', a.old_is_active
				   ) ORDER BY a.requested_at DESC) FROM notification_svc.audit_event a WHERE a.event_id = m.event_id), '[]'::json) AS audits
			FROM notification_svc.event m
			WHERE m.event_id = $1
			LIMIT 1
		`
		row := pgxPool.QueryRow(ctx, q, req.EventID)
		var id, module, sub, code, name, desc, route string
		var isActive, isDeleted bool
		var audits json.RawMessage
		if err := row.Scan(&id, &module, &sub, &code, &name, &desc, &route, &isActive, &isDeleted, &audits); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		var auditsVal interface{}
		_ = json.Unmarshal(audits, &auditsVal)
		out := map[string]interface{}{
			"event_id":           id,
			"module_code":        module,
			"sub_module_code":    sub,
			"event_code":         code,
			"event_display_name": name,
			"description":        desc,
			"source_route":       route,
			"is_active":          isActive,
			"is_deleted":         isDeleted,
			"audits":             auditsVal,
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// UploadEventSimple reuses the CreateEvent contract but accepts a JSON payload
func UploadEventSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Expect multipart form with `file` and `user_id`
		if err := r.ParseMultipartForm(64 << 20); err != nil {
			respondWithError(w, http.StatusBadRequest, "failed to parse multipart form: "+err.Error())
			return
		}
		userID := r.FormValue("user_id")
		if userID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		file, handler, err := r.FormFile("file")
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "file is required: "+err.Error())
			return
		}
		defer file.Close()

		ext := strings.ToLower(filepath.Ext(handler.Filename))
		var rows [][]string
		if ext == ".csv" {
			rows, err = parseCSVFile(file)
		} else if ext == ".xlsx" || ext == ".xls" {
			rows, err = parseXLSXFile(file)
		} else {
			respondWithError(w, http.StatusBadRequest, "unsupported file type")
			return
		}
		if err != nil {
			respondWithError(w, http.StatusBadRequest, "failed to parse file: "+err.Error())
			return
		}
		if len(rows) < 2 {
			respondWithError(w, http.StatusBadRequest, "no data rows found in file")
			return
		}

		header := rows[0]
		data := rows[1:]
		requiredCols := []string{"module_code", "sub_module_code", "event_code", "event_display_name"}
		colMap := make(map[string]int)
		for i, col := range header {
			colMap[strings.TrimSpace(strings.ToLower(col))] = i
		}
		for _, rc := range requiredCols {
			if _, ok := colMap[rc]; !ok {
				respondWithError(w, http.StatusBadRequest, "missing required column: "+rc)
				return
			}
		}

		userEmail := getRequesterEmail()
		if userEmail == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidSessionCapitalized)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Build batch insert
		valueStrings := make([]string, 0, len(data))
		valueArgs := make([]interface{}, 0, len(data)*7)
		for i, row := range data {
			// safe access using colMap
			module := safeCol(row, colMap, "module_code")
			sub := safeCol(row, colMap, "sub_module_code")
			code := safeCol(row, colMap, "event_code")
			name := safeCol(row, colMap, "event_display_name")
			desc := safeCol(row, colMap, "description")
			route := safeCol(row, colMap, "source_route")
			isActiveStr := safeCol(row, colMap, "is_active")
			isActive := parseBoolDefault(isActiveStr, false)

			if module == "" || sub == "" || code == "" || name == "" {
				// skip invalid row
				continue
			}

			pos := i*7 + 1
			valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", pos, pos+1, pos+2, pos+3, pos+4, pos+5, pos+6))
			valueArgs = append(valueArgs, module, sub, code, name, desc, route, isActive)
		}
		if len(valueStrings) == 0 {
			api.RespondWithPayload(w, false, "no valid rows to insert", nil)
			return
		}

		batchQ := fmt.Sprintf(`
			INSERT INTO notification_svc.event_master (
				module_code, sub_module_code, event_code, event_display_name, description, source_route, is_active
			) VALUES %s
			RETURNING event_id, event_display_name
		`, strings.Join(valueStrings, ","))

		rowsOut, err := tx.Query(ctx, batchQ, valueArgs...)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rowsOut.Close()

		var inserted []map[string]interface{}
		var eventIDs []string
		for rowsOut.Next() {
			var id, nm string
			if err := rowsOut.Scan(&id, &nm); err == nil {
				inserted = append(inserted, map[string]interface{}{"event_id": id, "event_display_name": nm})
				eventIDs = append(eventIDs, id)
			}
		}

		if len(eventIDs) > 0 {
			auditVals := make([]string, 0, len(eventIDs))
			auditArgs := make([]interface{}, 0, len(eventIDs)*2)
			pos := 1
			for _, id := range eventIDs {
				auditVals = append(auditVals, fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", pos, pos+1))
				auditArgs = append(auditArgs, id, userEmail)
				pos += 2
			}
			auditQ := fmt.Sprintf("INSERT INTO notification_svc.audit_event_master (event_id, action_type, processing_status, requested_by, requested_at) VALUES %s", strings.Join(auditVals, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				respondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Seed notification_config rows for all newly uploaded events
		seedNotifConfigForEvents(ctx, pgxPool, eventIDs, userEmail)

		api.RespondWithPayload(w, true, "", map[string]interface{}{"inserted": inserted})
		api.LogInfo("UploadEventSimple: inserted %d rows from file %s", len(inserted), handler.Filename)
	}
}

// safeCol returns trimmed column value if index present
func safeCol(row []string, colMap map[string]int, name string) string {
	if idx, ok := colMap[name]; ok {
		if idx >= 0 && idx < len(row) {
			return strings.TrimSpace(row[idx])
		}
	}
	return ""
}

func parseBoolDefault(s string, def bool) bool {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "true" || s == "1" || s == "yes" {
		return true
	}
	if s == "false" || s == "0" || s == "no" {
		return false
	}
	return def
}

// parseCSVFile reads CSV into rows
func parseCSVFile(file multipart.File) ([][]string, error) {
	r := csv.NewReader(file)
	r.FieldsPerRecord = -1
	return r.ReadAll()
}

// parseXLSXFile reads first sheet rows
func parseXLSXFile(file multipart.File) ([][]string, error) {
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, file); err != nil {
		return nil, err
	}
	f, err := excelize.OpenReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sheet := f.GetSheetName(0)
	return f.GetRows(sheet)
}
