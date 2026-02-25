package catalog

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// simple error responder
func respondWithErrorTemplate(w http.ResponseWriter, status int, errMsg string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": errMsg})
}

func getRequesterEmailTemplate() string {
	for _, s := range auth.GetActiveSessions() {
		if s.Email != "" {
			return s.Email
		}
	}
	return ""
}

// CreateTemplateSingle inserts a template master row and an audit_template CREATE row
func CreateTemplateSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string `json:"user_id"`
			EventID      string `json:"event_id"`
			Channel      string `json:"channel"`
			RoleScope    string `json:"role_scope"`
			TemplateName string `json:"template_name"`
			Description  string `json:"description"`
			Subject      string `json:"subject"`
			BodyText     string `json:"body_text"`
			BodyHTML     string `json:"body_html"`
			IsHTML       *bool  `json:"is_html_enabled"`
			Formula      any    `json:"formula_steps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if req.EventID == "" || req.Channel == "" || req.TemplateName == "" {
			api.RespondWithPayload(w, false, "event_id, channel and template_name are required", nil)
			return
		}
		if req.IsHTML == nil {
			defaultHTML := false
			req.IsHTML = &defaultHTML
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `INSERT INTO notification_svc.template (event_id, channel, role_scope, template_name, description, created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING template_id`
		var tplID string
		if err := tx.QueryRow(ctx, insertQ, req.EventID, strings.ToUpper(req.Channel), req.RoleScope, req.TemplateName, req.Description, userEmail).Scan(&tplID); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		// insert audit_template record with CREATE (compute version_label = 'v' || (COUNT(*)+1) for this template)
		// ensure formula_steps and is_html_enabled are non-null
		isHTMLVal := false
		if req.IsHTML != nil {
			isHTMLVal = *req.IsHTML
		}
		var formulaVal interface{} = []byte("[]")
		if req.Formula != nil {
			if jf, jerr := json.Marshal(req.Formula); jerr == nil {
				formulaVal = jf
			}
		}
		auditQ := `INSERT INTO notification_svc.audit_template (template_id, action_type, processing_status, subject, body_text, body_html, is_html_enabled, formula_steps, version_label, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,$4,$5,$6,(SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $8),$7,now())`
		if _, err := tx.Exec(ctx, auditQ, tplID, req.Subject, req.BodyText, req.BodyHTML, isHTMLVal, formulaVal, userEmail, tplID); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"template_id": tplID, "requested": userEmail})
	}
}

// CreateTemplate handles bulk create of templates
func CreateTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				EventID      string `json:"event_id"`
				Channel      string `json:"channel"`
				RoleScope    string `json:"role_scope"`
				TemplateName string `json:"template_name"`
				Description  string `json:"description"`
				Subject      string `json:"subject"`
				BodyText     string `json:"body_text"`
				BodyHTML     string `json:"body_html"`
				IsHTML       *bool  `json:"is_html_enabled"`
				Formula      any    `json:"formula_steps"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithPayload(w, false, "rows required", nil)
			return
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		// Batch insert templates (include created_by)
		valueStrings := make([]string, 0, len(req.Rows))
		valueArgs := make([]interface{}, 0, len(req.Rows)*6)
		for i, rrow := range req.Rows {
			pos := i*6 + 1
			valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", pos, pos+1, pos+2, pos+3, pos+4, pos+5))
			valueArgs = append(valueArgs, rrow.EventID, strings.ToUpper(rrow.Channel), rrow.RoleScope, rrow.TemplateName, rrow.Description, userEmail)
		}
		batchQ := fmt.Sprintf(`INSERT INTO notification_svc.template (event_id, channel, role_scope, template_name, description, created_by) VALUES %s RETURNING template_id, template_name`, strings.Join(valueStrings, ","))
		rows, err := tx.Query(ctx, batchQ, valueArgs...)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()
		var inserted []map[string]interface{}
		var templateIDs []string
		for rows.Next() {
			var id, name string
			if err := rows.Scan(&id, &name); err == nil {
				inserted = append(inserted, map[string]interface{}{"template_id": id, "template_name": name})
				templateIDs = append(templateIDs, id)
			}
		}

		// insert audit rows for created templates (compute version_label per template)
		if len(templateIDs) > 0 {
			for _, id := range templateIDs {
				aq := `INSERT INTO notification_svc.audit_template (template_id, action_type, processing_status, requested_by, version_label, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,(SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $3),now())`
				if _, err := tx.Exec(ctx, aq, id, userEmail, id); err != nil {
					api.RespondWithPayload(w, false, err.Error(), nil)
					return
				}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"inserted": inserted})
	}
}

// recipientSubquery resolves recipients for template alias t.
// NULLIF guards against empty-string → uuid cast (SQLSTATE 22P02).
const recipientSubquery = `
	COALESCE((
		SELECT json_agg(json_build_object(
			'recipient_type',    tr.recipient_type,
			'recipient_role',    COALESCE(tr.recipient_role,''),
			'recipient_user_id', COALESCE(tr.recipient_user_id,''),
			'emails', CASE
				WHEN tr.recipient_type = 'USER' AND NULLIF(TRIM(COALESCE(tr.recipient_user_id,'')), '') IS NOT NULL
					THEN COALESCE(
						(SELECT json_agg(u.email) FROM users u
						 WHERE u.id::text = NULLIF(TRIM(tr.recipient_user_id),'')),
						'[]'::json)
				WHEN tr.recipient_type = 'ROLE' AND NULLIF(TRIM(COALESCE(tr.recipient_role,'')), '') IS NOT NULL
					THEN COALESCE(
						(SELECT json_agg(u2.email)
						 FROM users u2
						 JOIN user_roles ur ON ur.user_id = u2.id
						 JOIN roles ro ON ro.id = ur.role_id
						 WHERE ro.name = tr.recipient_role OR ro.rolecode = tr.recipient_role),
						'[]'::json)
				ELSE '[]'::json END
		)) FROM notification_svc.template_recipient tr
		WHERE tr.template_id = t.template_id AND tr.is_active = true
	), '[]'::json)
`

// GetTemplatesWithAudit — /notification/template/all
// One row per template. Latest audit = DISTINCT ON requested_at DESC.
// Flat history columns: created_by/at, edited_by/at, deleted_by/at (like interestTypeMaster).
func GetTemplatesWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.template_id)
					a.template_id,
					a.audit_id,
					a.action_type,
					a.processing_status,
					a.version_label,
					a.subject,
					a.body_text,
					a.body_html,
					a.is_html_enabled,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.is_deleted
				FROM notification_svc.audit_template a
				WHERE COALESCE(a.is_deleted, false) = false
				ORDER BY a.template_id, a.requested_at DESC NULLS LAST
			),
			history AS (
				SELECT
					template_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by  END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by  END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by  END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM notification_svc.audit_template
				GROUP BY template_id
			),
			-- old_* = content of the last APPROVED row (the "before" snapshot)
			old_vals AS (
				SELECT DISTINCT ON (template_id)
					template_id,
					COALESCE(subject,'')                                        AS old_subject,
					COALESCE(body_text,'')                                      AS old_body_text,
					COALESCE(body_html,'')                                      AS old_body_html,
					COALESCE(is_html_enabled,false)                             AS old_is_html_enabled,
					COALESCE(version_label,'')                                  AS old_version_label
				FROM notification_svc.audit_template
				WHERE processing_status = 'APPROVED'
				ORDER BY template_id, requested_at DESC NULLS LAST
			)
			SELECT
				t.template_id,
				COALESCE(t.event_id,'')      AS event_id,
				COALESCE(t.channel,'')       AS channel,
				COALESCE(t.role_scope,'')    AS role_scope,
				COALESCE(t.template_name,'') AS template_name,
				COALESCE(t.description,'')   AS description,
				t.is_active,

				COALESCE(l.audit_id::text,'')                                 AS audit_id,
				COALESCE(l.action_type,'')                                    AS action_type,
				COALESCE(l.processing_status,'')                              AS processing_status,
				COALESCE(l.version_label,'')                                  AS version_label,
				COALESCE(l.subject,'')                                        AS subject,
				COALESCE(l.body_text,'')                                      AS body_text,
				COALESCE(l.body_html,'')                                      AS body_html,
				COALESCE(l.is_html_enabled,false)                             AS is_html_enabled,
				COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(l.requested_by,'')                                   AS requested_by,
				COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
				COALESCE(l.checker_by,'')                                     AS checker_by,
				COALESCE(l.checker_comment,'')                                AS checker_comment,
				COALESCE(l.is_deleted, false)                                 AS version_is_deleted,

				COALESCE(h.created_by,'')        AS created_by,
				COALESCE(h.created_at,'')        AS created_at,
				COALESCE(h.edited_by,'')         AS edited_by,
				COALESCE(h.edited_at,'')         AS edited_at,
				COALESCE(h.deleted_by,'')        AS deleted_by,
				COALESCE(h.deleted_at,'')        AS deleted_at,

				COALESCE(ov.old_subject,'')          AS old_subject,
				COALESCE(ov.old_body_text,'')        AS old_body_text,
				COALESCE(ov.old_body_html,'')        AS old_body_html,
				COALESCE(ov.old_is_html_enabled,false) AS old_is_html_enabled,
				COALESCE(ov.old_version_label,'')    AS old_version_label,

				` + recipientSubquery + ` AS recipients

			FROM notification_svc.template t
			LEFT JOIN latest_audit l ON l.template_id = t.template_id
			LEFT JOIN history h ON h.template_id = t.template_id
			LEFT JOIN old_vals ov ON ov.template_id = t.template_id
			ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamptz), COALESCE(l.checker_at,'1970-01-01'::timestamptz)) DESC
		`
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
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
			api.RespondWithPayload(w, false, "row scan error: "+rows.Err().Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// GetTemplateVersions — /notification/template/versions
// Flat: one row per audit_template entry (every version of every template).
// Same template_id appears multiple times — one row per version, newest first.
// history CTE gives created_by/at, edited_by/at, deleted_by/at on every row.
// Optional body filter: { "template_id": "TPL-xxx", "version_label": "v2" }
func GetTemplateVersions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID   string `json:"template_id"`
			VersionLabel string `json:"version_label"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Build dynamic filter clause with correct positional args
		var filterParts []string
		var args []interface{}
		if req.TemplateID != "" {
			args = append(args, req.TemplateID)
			filterParts = append(filterParts, fmt.Sprintf("AND a.template_id = $%d", len(args)))
		}
		if req.VersionLabel != "" {
			args = append(args, req.VersionLabel)
			filterParts = append(filterParts, fmt.Sprintf("AND a.version_label = $%d", len(args)))
		}
		filterClause := ""
		if len(filterParts) > 0 {
			filterClause = " " + strings.Join(filterParts, " ")
		}

		q := `
			WITH history AS (
				SELECT
					template_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by  END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by  END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by  END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM notification_svc.audit_template
				GROUP BY template_id
			),
			-- old_* = content of the last APPROVED row ("before" snapshot for diff view)
			old_vals AS (
				SELECT DISTINCT ON (template_id)
					template_id,
					COALESCE(subject,'')                                        AS old_subject,
					COALESCE(body_text,'')                                      AS old_body_text,
					COALESCE(body_html,'')                                      AS old_body_html,
					COALESCE(is_html_enabled,false)                             AS old_is_html_enabled,
					COALESCE(version_label,'')                                  AS old_version_label
				FROM notification_svc.audit_template
				WHERE processing_status = 'APPROVED'
				ORDER BY template_id, requested_at DESC NULLS LAST
			)
			SELECT
				t.template_id,
				COALESCE(t.event_id,'')      AS event_id,
				COALESCE(t.channel,'')       AS channel,
				COALESCE(t.role_scope,'')    AS role_scope,
				COALESCE(t.template_name,'') AS template_name,
				COALESCE(t.description,'')   AS description,
				t.is_active,

				COALESCE(a.audit_id::text,'')                                 AS audit_id,
				COALESCE(a.version_label,'')                                  AS version_label,
				COALESCE(a.action_type,'')                                    AS action_type,
				COALESCE(a.processing_status,'')                              AS processing_status,
				COALESCE(a.subject,'')                                        AS subject,
				COALESCE(a.body_text,'')                                      AS body_text,
				COALESCE(a.body_html,'')                                      AS body_html,
				COALESCE(a.is_html_enabled,false)                             AS is_html_enabled,
				COALESCE(TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS'),'') AS requested_at,
				COALESCE(a.requested_by,'')                                   AS requested_by,
				COALESCE(TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS'),'')   AS checker_at,
				COALESCE(a.checker_by,'')                                     AS checker_by,
				COALESCE(a.checker_comment,'')                                AS checker_comment,

				COALESCE(h.created_by,'')        AS created_by,
				COALESCE(h.created_at,'')        AS created_at,
				COALESCE(h.edited_by,'')         AS edited_by,
				COALESCE(h.edited_at,'')         AS edited_at,
				COALESCE(h.deleted_by,'')        AS deleted_by,
				COALESCE(h.deleted_at,'')        AS deleted_at,

				COALESCE(ov.old_subject,'')          AS old_subject,
				COALESCE(ov.old_body_text,'')        AS old_body_text,
				COALESCE(ov.old_body_html,'')        AS old_body_html,
				COALESCE(ov.old_is_html_enabled,false) AS old_is_html_enabled,
				COALESCE(ov.old_version_label,'')    AS old_version_label,
				COALESCE(a.is_deleted, false)         AS version_is_deleted,

				` + recipientSubquery + ` AS recipients

			FROM notification_svc.audit_template a
			JOIN notification_svc.template t ON t.template_id = a.template_id
			LEFT JOIN history h ON h.template_id = a.template_id
			LEFT JOIN old_vals ov ON ov.template_id = a.template_id
			WHERE 1=1
			  AND COALESCE(a.is_deleted, false) = false
			` + filterClause + `
			ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamptz), COALESCE(a.checker_at, '1970-01-01'::timestamptz)) DESC NULLS LAST
		`

		var rows pgx.Rows
		var err error
		if len(args) > 0 {
			rows, err = pgxPool.Query(ctx, q, args...)
		} else {
			rows, err = pgxPool.Query(ctx, q)
		}
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
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
			api.RespondWithPayload(w, false, "row scan error: "+rows.Err().Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// EditTemplateSingle — POST /notification/template/edit
//
// Creates a NEW audit_template row with action_type='EDIT' and
// processing_status='PENDING_APPROVAL'. The version_label is auto-incremented
// (COUNT(*)+1 across ALL audit rows for this template, regardless of status).
// The notification_svc.template master row is NOT touched until a checker
// approves the edit. Every call produces a new version — no overwrites.
func EditTemplateSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string `json:"template_id"`
			Subject    string `json:"subject"`
			BodyText   string `json:"body_text"`
			BodyHTML   string `json:"body_html"`
			IsHTML     *bool  `json:"is_html_enabled"`
			Formula    any    `json:"formula_steps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if req.TemplateID == "" {
			api.RespondWithPayload(w, false, "template_id is required", nil)
			return
		}
		if req.IsHTML == nil {
			def := false
			req.IsHTML = &def
		}

		editor := getRequesterEmailTemplate()
		if editor == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		var formulaVal interface{} = []byte("[]")
		if req.Formula != nil {
			if jf, jerr := json.Marshal(req.Formula); jerr == nil {
				formulaVal = jf
			}
		}

		ctx := r.Context()

		// Verify template exists
		var exists bool
		if err := pgxPool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM notification_svc.template WHERE template_id=$1)`, req.TemplateID).Scan(&exists); err != nil || !exists {
			api.RespondWithPayload(w, false, "template not found", nil)
			return
		}

		// Insert a new EDIT audit row. Version = COUNT(*)+1 across ALL audit rows
		// for this template (same formula used by CREATE), guaranteeing strictly
		// increasing version numbers regardless of action_type.
		auditQ := `
			INSERT INTO notification_svc.audit_template
				(template_id, action_type, processing_status,
				 subject, body_text, body_html, is_html_enabled, formula_steps,
				 version_label, requested_by, requested_at)
			VALUES
				($1, 'EDIT', 'PENDING_EDIT_APPROVAL',
				 $2, $3, $4, $5, $6,
				 (SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $7),
				 $8, now())
			RETURNING audit_id::text, version_label
		`
		var newAuditID, newVersion string
		if err := pgxPool.QueryRow(ctx, auditQ,
			req.TemplateID,
			req.Subject, req.BodyText, req.BodyHTML, *req.IsHTML, formulaVal,
			req.TemplateID, editor,
		).Scan(&newAuditID, &newVersion); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"template_id":   req.TemplateID,
			"audit_id":      newAuditID,
			"version_label": newVersion,
			"action_type":   "EDIT",
			"status":        "PENDING_EDIT_APPROVAL",
			"requested_by":  editor,
		})
	}
}

// GetTemplate returns a single template with its full audit history and resolved recipients
func GetTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string `json:"template_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TemplateID == "" {
			api.RespondWithPayload(w, false, "template_id required", nil)
			return
		}
		ctx := r.Context()
		q := fmt.Sprintf(`
			SELECT t.template_id, t.event_id, t.channel, t.role_scope, t.template_name, t.description, t.is_active,
				COALESCE((SELECT json_agg(json_build_object(
					'audit_id', a.audit_id, 'action_type', a.action_type, 'processing_status', a.processing_status,
					'subject', COALESCE(a.subject,''), 'body_text', COALESCE(a.body_text,''), 'body_html', COALESCE(a.body_html,''),
					'is_html_enabled', COALESCE(a.is_html_enabled,false),
					'formula_steps', a.formula_steps, 'version_label', COALESCE(a.version_label,''),
					'requested_by', COALESCE(a.requested_by,''), 'requested_at', TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS'),
					'checker_by', COALESCE(a.checker_by,''), 'checker_at', TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS'),
					'checker_comment', COALESCE(a.checker_comment,'')
				) ORDER BY a.requested_at DESC) FROM notification_svc.audit_template a WHERE a.template_id = t.template_id AND COALESCE(a.is_deleted, false) = false), '[]'::json) AS audits,
				%s AS recipients
			FROM notification_svc.template t WHERE t.template_id = $1 LIMIT 1`, recipientSubquery)
		row := pgxPool.QueryRow(ctx, q, req.TemplateID)
		var tplID, eventID, channel, roleScope, name, desc string
		var isActive bool
		var audits json.RawMessage
		var recipients json.RawMessage
		if err := row.Scan(&tplID, &eventID, &channel, &roleScope, &name, &desc, &isActive, &audits, &recipients); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		var auditsVal interface{}
		var recVal interface{}
		_ = json.Unmarshal(audits, &auditsVal)
		_ = json.Unmarshal(recipients, &recVal)
		out := map[string]interface{}{
			"template_id":   tplID,
			"event_id":      eventID,
			"channel":       channel,
			"role_scope":    roleScope,
			"template_name": name,
			"description":   desc,
			"is_active":     isActive,
			"audits":        auditsVal,
			"recipients":    recVal,
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// GetTemplatesApprovedActive returns active templates with latest approved audit content and resolved recipients
func GetTemplatesApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := fmt.Sprintf(`
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.template_id)
					a.template_id, a.processing_status, a.action_type, a.audit_id,
					a.requested_by, a.requested_at, a.checker_by, a.checker_at, a.checker_comment,
					a.version_label, a.subject, a.body_text, a.body_html, a.is_html_enabled, a.formula_steps
				FROM notification_svc.audit_template a
				WHERE a.processing_status = 'APPROVED' AND COALESCE(a.is_deleted, false) = false
				ORDER BY a.template_id, a.requested_at DESC NULLS LAST
			)
			SELECT
				t.template_id, t.event_id, t.channel, t.role_scope, t.template_name, t.description, t.is_active,
				COALESCE(l.subject,'') AS subject, COALESCE(l.body_text,'') AS body_text,
				COALESCE(l.body_html,'') AS body_html, COALESCE(l.is_html_enabled,false) AS is_html_enabled,
				COALESCE(l.version_label,'') AS version_label,
				%s AS recipients
			FROM notification_svc.template t
			LEFT JOIN latest_audit l ON l.template_id = t.template_id
			WHERE t.is_active = true
			ORDER BY t.template_name
		`, recipientSubquery)
		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var tplID, eventID, channel, roleScope, tplName, desc string
			var isActive bool
			var subject, bodyText, bodyHTML string
			var isHTML bool
			var versionLabel string
			var recipients json.RawMessage
			if err := rows.Scan(&tplID, &eventID, &channel, &roleScope, &tplName, &desc, &isActive, &subject, &bodyText, &bodyHTML, &isHTML, &versionLabel, &recipients); err == nil {
				var recVal interface{}
				_ = json.Unmarshal(recipients, &recVal)
				out = append(out, map[string]interface{}{
					"template_id":     tplID,
					"event_id":        eventID,
					"channel":         channel,
					"role_scope":      roleScope,
					"template_name":   tplName,
					"description":     desc,
					"is_active":       isActive,
					"subject":         subject,
					"body_text":       bodyText,
					"body_html":       bodyHTML,
					"is_html_enabled": isHTML,
					"version_label":   versionLabel,
					"recipients":      recVal,
				})
			}
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// GetTemplateAuditHistory returns audit rows for a given template_id
func GetTemplateAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string `json:"template_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TemplateID == "" {
			api.RespondWithPayload(w, false, "template_id required", nil)
			return
		}
		ctx := r.Context()
		q := `SELECT audit_id, template_id, action_type, processing_status, subject, body_text, body_html, is_html_enabled, formula_steps, version_label, requested_by, requested_at, checker_by, checker_at, checker_comment, old_subject, old_body_text, old_body_html, old_formula_steps, COALESCE(is_deleted,false) AS is_deleted FROM notification_svc.audit_template WHERE template_id = $1 ORDER BY requested_at DESC`
		rows, err := pgxPool.Query(ctx, q, req.TemplateID)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var auditID, templateID, action, proc string
			var subject, bodyText, bodyHTML, requestedBy, checkerBy, checkerComment *string
			var isHTML *bool
			var formula interface{}
			var versionLabel *string
			var requestedAt, checkerAt interface{}
			var oldSubject, oldBodyText, oldBodyHTML *string
			var oldFormula interface{}
			var isDeleted bool
			if err := rows.Scan(&auditID, &templateID, &action, &proc, &subject, &bodyText, &bodyHTML, &isHTML, &formula, &versionLabel, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &oldSubject, &oldBodyText, &oldBodyHTML, &oldFormula, &isDeleted); err == nil {
				out = append(out, map[string]interface{}{
					"audit_id":          auditID,
					"template_id":       templateID,
					"action_type":       action,
					"processing_status": proc,
					"subject":           subject,
					"body_text":         bodyText,
					"body_html":         bodyHTML,
					"is_html_enabled":   isHTML,
					"formula_steps":     formula,
					"version_label":     versionLabel,
					"requested_by":      requestedBy,
					"requested_at":      requestedAt,
					"checker_by":        checkerBy,
					"checker_at":        checkerAt,
					"checker_comment":   checkerComment,
					"old_subject":       oldSubject,
					"old_body_text":     oldBodyText,
					"old_body_html":     oldBodyHTML,
					"old_formula_steps": oldFormula,
					"is_deleted":        isDeleted,
				})
			}
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// BulkApproveTemplate approves specific audit_template versions by audit_id.
// Accepts { "audit_ids": ["uuid1","uuid2"], "comment": "" }
// For CREATE/EDIT approvals: activates the parent template row.
// For DELETE approvals: sets is_deleted=true on the audit_template row itself.
// Returns per-ID results so caller knows exactly which ones were skipped and why.
func BulkApproveTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			AuditIDs []string `json:"audit_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if len(req.AuditIDs) == 0 {
			api.RespondWithPayload(w, false, "audit_ids required", nil)
			return
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}
		ctx := r.Context()

		// Fetch current status of every supplied audit_id in one round-trip
		statusRows, err := pgxPool.Query(ctx,
			`SELECT audit_id::text, processing_status, COALESCE(is_deleted, false)
			 FROM notification_svc.audit_template
			 WHERE audit_id = ANY($1::uuid[])`,
			req.AuditIDs)
		if err != nil {
			api.RespondWithPayload(w, false, "lookup failed: "+err.Error(), nil)
			return
		}
		defer statusRows.Close()

		type rowInfo struct{ status string; deleted bool }
		current := make(map[string]rowInfo, len(req.AuditIDs))
		for statusRows.Next() {
			var id, status string
			var deleted bool
			if err := statusRows.Scan(&id, &status, &deleted); err != nil {
				api.RespondWithPayload(w, false, "scan failed: "+err.Error(), nil)
				return
			}
			current[id] = rowInfo{status, deleted}
		}
		if statusRows.Err() != nil {
			api.RespondWithPayload(w, false, statusRows.Err().Error(), nil)
			return
		}

		var eligible []string
		var results []map[string]interface{}
		for _, id := range req.AuditIDs {
			info, found := current[id]
			if !found {
				results = append(results, map[string]interface{}{
					"audit_id": id, "success": false, "reason": "not found",
				})
				continue
			}
			if info.deleted {
				results = append(results, map[string]interface{}{
					"audit_id": id, "success": false, "reason": "already deleted",
				})
				continue
			}
			if !strings.Contains(info.status, "PENDING") {
				results = append(results, map[string]interface{}{
					"audit_id": id, "success": false,
					"reason": fmt.Sprintf("cannot approve — current status is '%s'", info.status),
				})
				continue
			}
			eligible = append(eligible, id)
		}

		if len(eligible) == 0 {
			api.RespondWithPayload(w, false, "no eligible audit rows to approve", results)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		// 1. Stamp all eligible PENDING rows as APPROVED in one shot
		tag, err := tx.Exec(ctx,
			`UPDATE notification_svc.audit_template
			 SET processing_status = 'APPROVED', checker_by = $1, checker_at = now(), checker_comment = $2
			 WHERE audit_id = ANY($3::uuid[])
			   AND processing_status LIKE '%PENDING%'
			   AND COALESCE(is_deleted, false) = false`,
			userEmail, req.Comment, eligible)
		if err != nil {
			api.RespondWithPayload(w, false, "approve stamp failed: "+err.Error(), nil)
			return
		}

		// 2. For CREATE approvals → activate the parent template
		_, err = tx.Exec(ctx,
			`UPDATE notification_svc.template SET is_active = true
			 WHERE template_id IN (
				 SELECT DISTINCT template_id FROM notification_svc.audit_template
				 WHERE audit_id = ANY($1::uuid[]) AND action_type = 'CREATE' AND processing_status = 'APPROVED'
			 )`,
			eligible)
		if err != nil {
			api.RespondWithPayload(w, false, "activate template failed: "+err.Error(), nil)
			return
		}

		// 3. For DELETE approvals → set is_deleted=true on the audit_template rows
		//    (template table no longer carries is_deleted; the audit row itself is the soft-delete record)
		_, err = tx.Exec(ctx,
			`UPDATE notification_svc.audit_template SET is_deleted = true
			 WHERE audit_id = ANY($1::uuid[])
			   AND action_type = 'DELETE'
			   AND processing_status = 'APPROVED'`,
			eligible)
		if err != nil {
			api.RespondWithPayload(w, false, "delete flag failed: "+err.Error(), nil)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		for _, id := range eligible {
			results = append(results, map[string]interface{}{
				"audit_id": id, "success": true, "status": "APPROVED",
			})
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"approved_count": tag.RowsAffected(),
			"checker":        userEmail,
			"results":        results,
		})
	}
}

// BulkRejectTemplate rejects specific audit_template versions by audit_id.
// Accepts { "audit_ids": ["uuid1","uuid2"], "comment": "" }
// Returns per-ID result so caller knows exactly which ones were skipped and why.
func BulkRejectTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			AuditIDs []string `json:"audit_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if len(req.AuditIDs) == 0 {
			api.RespondWithPayload(w, false, "audit_ids required", nil)
			return
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}
		ctx := r.Context()

		// Fetch current status of every supplied audit_id in one round-trip
		statusRows, err := pgxPool.Query(ctx,
			`SELECT audit_id::text, processing_status, COALESCE(is_deleted, false)
			 FROM notification_svc.audit_template
			 WHERE audit_id = ANY($1::uuid[])`,
			req.AuditIDs)
		if err != nil {
			api.RespondWithPayload(w, false, "lookup failed: "+err.Error(), nil)
			return
		}
		defer statusRows.Close()

		type rowInfo struct{ status string; deleted bool }
		current := make(map[string]rowInfo, len(req.AuditIDs))
		for statusRows.Next() {
			var id, status string
			var deleted bool
			if err := statusRows.Scan(&id, &status, &deleted); err != nil {
				api.RespondWithPayload(w, false, "scan failed: "+err.Error(), nil)
				return
			}
			current[id] = rowInfo{status, deleted}
		}
		if statusRows.Err() != nil {
			api.RespondWithPayload(w, false, statusRows.Err().Error(), nil)
			return
		}

		var eligible []string
		var results []map[string]interface{}
		for _, id := range req.AuditIDs {
			info, found := current[id]
			if !found {
				results = append(results, map[string]interface{}{
					"audit_id": id, "success": false, "reason": "not found",
				})
				continue
			}
			if info.deleted {
				results = append(results, map[string]interface{}{
					"audit_id": id, "success": false, "reason": "already deleted",
				})
				continue
			}
			if !strings.Contains(info.status, "PENDING") {
				results = append(results, map[string]interface{}{
					"audit_id": id, "success": false,
					"reason": fmt.Sprintf("cannot reject — current status is '%s'", info.status),
				})
				continue
			}
			eligible = append(eligible, id)
		}

		if len(eligible) > 0 {
			tag, err := pgxPool.Exec(ctx,
				`UPDATE notification_svc.audit_template
				 SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_comment = $2
				 WHERE audit_id = ANY($3::uuid[])
				   AND processing_status LIKE '%PENDING%'
				   AND COALESCE(is_deleted, false) = false`,
				userEmail, req.Comment, eligible)
			if err != nil {
				api.RespondWithPayload(w, false, err.Error(), nil)
				return
			}
			for _, id := range eligible {
				results = append(results, map[string]interface{}{
					"audit_id": id, "success": true, "status": "REJECTED",
				})
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"rejected_count": tag.RowsAffected(),
				"checker":        userEmail,
				"results":        results,
			})
			return
		}

		// All IDs were ineligible — return detailed failures
		api.RespondWithPayload(w, false, "no eligible audit rows to reject", results)
	}
}

// BulkDeleteTemplateVersions raises a PENDING_DELETE_APPROVAL request for each audit_id.
// Accepts { "audit_ids": ["uuid1","uuid2"], "comment": "" }
// The APPROVED (live) version cannot be queued for deletion — reject it first.
// Actual soft-delete (is_deleted=true) is applied when a checker calls BulkApproveTemplate.
func DeleteTemplateVersion(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			AuditIDs []string `json:"audit_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if len(req.AuditIDs) == 0 {
			api.RespondWithPayload(w, false, "audit_ids required", nil)
			return
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}
		ctx := r.Context()

		// Verify all supplied audit_ids exist, are not already deleted/APPROVED,
		// and collect their template_id + version_label for the response.
		// Single query — fetch all in one round-trip.
		rows, err := pgxPool.Query(ctx,
			`SELECT audit_id::text, template_id, COALESCE(version_label,''), processing_status
			 FROM notification_svc.audit_template
			 WHERE audit_id = ANY($1::uuid[])
			   AND COALESCE(is_deleted, false) = false`,
			req.AuditIDs)
		if err != nil {
			api.RespondWithPayload(w, false, "lookup failed: "+err.Error(), nil)
			return
		}
		defer rows.Close()

		type auditRow struct {
			auditID, templateID, versionLabel, status string
		}
		var valid []auditRow
		var blocked []map[string]interface{}
		for rows.Next() {
			var ar auditRow
			if err := rows.Scan(&ar.auditID, &ar.templateID, &ar.versionLabel, &ar.status); err != nil {
				api.RespondWithPayload(w, false, "scan failed: "+err.Error(), nil)
				return
			}
			if ar.status == "APPROVED" {
				blocked = append(blocked, map[string]interface{}{
					"audit_id":      ar.auditID,
					"version_label": ar.versionLabel,
					"error":         "cannot delete live APPROVED version — reject it first",
				})
			} else {
				valid = append(valid, ar)
			}
		}
		if rows.Err() != nil {
			api.RespondWithPayload(w, false, rows.Err().Error(), nil)
			return
		}
		if len(valid) == 0 {
			api.RespondWithPayload(w, false, "no eligible audit rows found", blocked)
			return
		}

		// ULTRA FAST: single UPDATE — marks all eligible rows as PENDING_DELETE_APPROVAL in one query
		validIDs := make([]string, len(valid))
		for i, v := range valid {
			validIDs[i] = v.auditID
		}
		if _, err := pgxPool.Exec(ctx,
			`UPDATE notification_svc.audit_template
			 SET processing_status = 'PENDING_DELETE_APPROVAL',
			     requested_by      = $1,
			     requested_at      = now(),
			     checker_comment   = $2
			 WHERE audit_id = ANY($3::uuid[])
			   AND COALESCE(is_deleted, false) = false
			   AND processing_status != 'APPROVED'`,
			userEmail, req.Comment, validIDs); err != nil {
			api.RespondWithPayload(w, false, "update failed: "+err.Error(), nil)
			return
		}

		var results []map[string]interface{}
		for _, v := range valid {
			results = append(results, map[string]interface{}{
				"audit_id":      v.auditID,
				"template_id":   v.templateID,
				"version_label": v.versionLabel,
				"status":        "PENDING_DELETE_APPROVAL",
				"requested_by":  userEmail,
			})
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"queued":  results,
			"blocked": blocked,
		})
	}
}
// CreateTemplateRecipient inserts a recipient
func CreateTemplateRecipient(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID        string `json:"template_id"`
			RecipientType     string `json:"recipient_type"`
			RecipientUserID   string `json:"recipient_user_id"`
			RecipientRole     string `json:"recipient_role"`
			IsActive          *bool  `json:"is_active"`
			RecipientPriority *int   `json:"recipient_priority"` // 1=highest urgency, default 3
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if req.TemplateID == "" || req.RecipientType == "" {
			api.RespondWithPayload(w, false, "template_id and recipient_type required", nil)
			return
		}
		if strings.ToUpper(req.RecipientType) == "USER" && req.RecipientUserID == "" {
			api.RespondWithPayload(w, false, "recipient_user_id required for USER type", nil)
			return
		}
		if strings.ToUpper(req.RecipientType) == "ROLE" && req.RecipientRole == "" {
			api.RespondWithPayload(w, false, "recipient_role required for ROLE type", nil)
			return
		}
		if req.IsActive == nil {
			d := true
			req.IsActive = &d
		}
		priority := 3
		if req.RecipientPriority != nil && *req.RecipientPriority > 0 {
			priority = *req.RecipientPriority
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()
		q := `INSERT INTO notification_svc.template_recipient
			(template_id, recipient_type, recipient_user_id, recipient_role, is_active, created_by, recipient_priority)
			VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING recipient_id`
		var rid string
		if err := pgxPool.QueryRow(ctx, q,
			req.TemplateID, strings.ToUpper(req.RecipientType),
			nullableStr(req.RecipientUserID), nullableStr(req.RecipientRole),
			req.IsActive, userEmail, priority,
		).Scan(&rid); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"recipient_id":       rid,
			"recipient_priority": priority,
		})
	}
}

// GetRecipientsByTemplate returns recipients for a template
func GetRecipientsByTemplate(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string `json:"template_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TemplateID == "" {
			api.RespondWithPayload(w, false, "template_id required", nil)
			return
		}
		ctx := r.Context()
		q := `SELECT recipient_id, template_id, recipient_type,
			     COALESCE(recipient_user_id,'') AS recipient_user_id,
			     COALESCE(recipient_role,'')    AS recipient_role,
			     is_active, created_at, created_by,
			     COALESCE(recipient_priority,3) AS recipient_priority
			  FROM notification_svc.template_recipient WHERE template_id=$1
			  ORDER BY recipient_priority ASC, created_at ASC`
		rows, err := pgxPool.Query(ctx, q, req.TemplateID)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()
		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var rid, tid, rtype, ruser, rrole, createdBy string
			var isActive bool
			var recipPriority int
			var createdAt interface{}
			if err := rows.Scan(&rid, &tid, &rtype, &ruser, &rrole, &isActive, &createdAt, &createdBy, &recipPriority); err == nil {
				out = append(out, map[string]interface{}{
					"recipient_id":       rid,
					"template_id":        tid,
					"recipient_type":     rtype,
					"recipient_user_id":  ruser,
					"recipient_role":     rrole,
					"is_active":          isActive,
					"created_at":         createdAt,
					"created_by":         createdBy,
					"recipient_priority": recipPriority,
				})
			}
		}
		api.RespondWithPayload(w, true, "", out)
	}
}

// DeleteTemplateRecipient soft-deletes (sets is_active=false) for a recipient
func DeleteTemplateRecipient(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RecipientID string `json:"recipient_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RecipientID == "" {
			api.RespondWithPayload(w, false, "recipient_id required", nil)
			return
		}
		ctx := r.Context()
		q := `UPDATE notification_svc.template_recipient SET is_active=false WHERE recipient_id=$1`
		if _, err := pgxPool.Exec(ctx, q, req.RecipientID); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"recipient_id": req.RecipientID})
	}
}

// UpdateTemplateRecipient patches is_active and/or recipient_priority for a single recipient.
//
//	{ "recipient_id": "RCP-xxx", "is_active": true, "recipient_priority": 2 }
func UpdateTemplateRecipient(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RecipientID       string `json:"recipient_id"`
			IsActive          *bool  `json:"is_active"`
			RecipientPriority *int   `json:"recipient_priority"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RecipientID == "" {
			api.RespondWithPayload(w, false, "recipient_id required", nil)
			return
		}
		if req.IsActive == nil && req.RecipientPriority == nil {
			api.RespondWithPayload(w, false, "at least one of is_active or recipient_priority is required", nil)
			return
		}

		var sets []string
		var args []interface{}
		pos := 1
		if req.IsActive != nil {
			sets = append(sets, fmt.Sprintf("is_active = $%d", pos))
			args = append(args, *req.IsActive)
			pos++
		}
		if req.RecipientPriority != nil {
			sets = append(sets, fmt.Sprintf("recipient_priority = $%d", pos))
			args = append(args, *req.RecipientPriority)
			pos++
		}
		args = append(args, req.RecipientID)

		q := fmt.Sprintf(`UPDATE notification_svc.template_recipient SET %s WHERE recipient_id = $%d
			RETURNING recipient_id, is_active, recipient_priority`,
			strings.Join(sets, ", "), pos)

		ctx := r.Context()
		var rid string
		var isActiveOut bool
		var priorityOut int
		if err := pgxPool.QueryRow(ctx, q, args...).Scan(&rid, &isActiveOut, &priorityOut); err != nil {
			api.RespondWithPayload(w, false, "recipient not found or update failed: "+err.Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"recipient_id":       rid,
			"is_active":          isActiveOut,
			"recipient_priority": priorityOut,
		})
	}
}

// BulkCreateRecipients creates multiple template_recipient rows in a single call.
// Accepts an array of recipient objects, all sharing the same template_id.
//
//	{
//	  "template_id": "TPL-xxx",
//	  "recipients": [
//	    { "recipient_type": "USER", "recipient_user_id": "uid-1", "recipient_priority": 1 },
//	    { "recipient_type": "ROLE", "recipient_role": "TREASURY", "recipient_priority": 2 },
//	    { "recipient_type": "USER", "recipient_user_id": "uid-2" }
//	  ]
//	}
func BulkCreateRecipients(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TemplateID string `json:"template_id"`
			Recipients []struct {
				RecipientType     string `json:"recipient_type"`
				RecipientUserID   string `json:"recipient_user_id"`
				RecipientRole     string `json:"recipient_role"`
				IsActive          *bool  `json:"is_active"`
				RecipientPriority *int   `json:"recipient_priority"`
			} `json:"recipients"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if req.TemplateID == "" {
			api.RespondWithPayload(w, false, "template_id required", nil)
			return
		}
		if len(req.Recipients) == 0 {
			api.RespondWithPayload(w, false, "recipients array must not be empty", nil)
			return
		}
		userEmail := getRequesterEmailTemplate()
		if userEmail == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()
		// Validate and build rows
		rows := make([][]interface{}, 0, len(req.Recipients))
		var validationErrors []string
		for i, rec := range req.Recipients {
			rtype := strings.ToUpper(strings.TrimSpace(rec.RecipientType))
			if rtype == "" {
				validationErrors = append(validationErrors, fmt.Sprintf("recipients[%d]: recipient_type required", i))
				continue
			}
			if rtype == "USER" && rec.RecipientUserID == "" {
				validationErrors = append(validationErrors, fmt.Sprintf("recipients[%d]: recipient_user_id required for USER type", i))
				continue
			}
			if rtype == "ROLE" && rec.RecipientRole == "" {
				validationErrors = append(validationErrors, fmt.Sprintf("recipients[%d]: recipient_role required for ROLE type", i))
				continue
			}
			isActive := true
			if rec.IsActive != nil {
				isActive = *rec.IsActive
			}
			priority := 3
			if rec.RecipientPriority != nil && *rec.RecipientPriority > 0 {
				priority = *rec.RecipientPriority
			}
			rows = append(rows, []interface{}{
				req.TemplateID,
				rtype,
				nullableStr(rec.RecipientUserID),
				nullableStr(rec.RecipientRole),
				isActive,
				userEmail,
				priority,
			})
		}
		if len(validationErrors) > 0 {
			api.RespondWithPayload(w, false, strings.Join(validationErrors, "; "), nil)
			return
		}

		// Insert via transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, "db error: "+err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		if err := batchInsertRecipientsOnTx(ctx, tx, rows); err != nil {
			api.RespondWithPayload(w, false, "insert failed: "+err.Error(), nil)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, "commit failed: "+err.Error(), nil)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"template_id":    req.TemplateID,
			"inserted_count": len(rows),
		})
	}
}

// nullableStr returns nil for empty strings so they land as SQL NULL.
func nullableStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// batchInsertRecipientsOnTx inserts recipient rows using an already-open pgx transaction.
// All rows land in the same tx as the parent template insert, guaranteeing atomicity.
func batchInsertRecipientsOnTx(ctx context.Context, tx pgx.Tx, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	singleQ := `INSERT INTO notification_svc.template_recipient (template_id, recipient_type, recipient_user_id, recipient_role, is_active, created_by, recipient_priority) VALUES ($1::varchar,$2::varchar,$3::varchar,$4::varchar,$5::boolean,$6::varchar,$7::int)`
	for idx, r := range rows {
		args := make([]interface{}, 7)
		for i := 0; i < 7; i++ {
			if i < len(r) && r[i] != nil {
				args[i] = r[i]
			} else {
				switch i {
				case 4:
					args[i] = true
				case 2, 3:
					args[i] = nil
				case 6:
					args[i] = 3 // default recipient_priority
				default:
					args[i] = ""
				}
			}
		}
		if _, err := tx.Exec(ctx, singleQ, args...); err != nil {
			api.LogError("batchInsertRecipientsOnTx row %d failed: %v", idx, err)
			return err
		}
	}
	return nil
}

// helper: batch insert recipients
func batchInsertRecipients(ctx context.Context, pgxPool *pgxpool.Pool, templateID string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	// Try fast, type-safe bulk insert using pgx CopyFrom
	conn, err := pgxPool.Acquire(ctx)
	if err == nil {
		defer conn.Release()
		// prepare rows for CopyFrom
		cfRows := make([][]interface{}, 0, len(rows))
		for _, r := range rows {
			// ensure length 7 and normalize nils
			row := make([]interface{}, 7)
			for i := 0; i < 7; i++ {
				if i < len(r) {
					if r[i] == nil {
						// replace nils with appropriate typed zero-values or NULL
						if i == 4 { // is_active
							row[i] = true
						} else if i == 2 || i == 3 {
							// recipient_user_id and recipient_role should be NULL when absent
							row[i] = nil
						} else if i == 6 { // recipient_priority
							row[i] = 3
						} else {
							row[i] = ""
						}
					} else {
						row[i] = r[i]
					}
				} else {
					if i == 4 {
						row[i] = true
					} else if i == 2 || i == 3 {
						row[i] = nil
					} else if i == 6 {
						row[i] = 3
					} else {
						row[i] = ""
					}
				}
			}
			cfRows = append(cfRows, row)
		}
		_, err = conn.CopyFrom(ctx, pgx.Identifier{"notification_svc", "template_recipient"}, []string{"template_id", "recipient_type", "recipient_user_id", "recipient_role", "is_active", "created_by", "recipient_priority"}, pgx.CopyFromRows(cfRows))
		if err == nil {
			return nil
		}
		// log and fallthrough to Exec fallback
		api.LogError("batchInsertRecipients CopyFrom failed: err=%v", err)
	} else {
		api.LogError("batchInsertRecipients acquire failed: %v", err)
	}

	// Fallback: insert rows one-by-one inside a transaction to avoid Postgres
	// ambiguous type inference across multi-row parameter lists (SQLSTATE 42P08).
	singleQ := `INSERT INTO notification_svc.template_recipient (template_id, recipient_type, recipient_user_id, recipient_role, is_active, created_by, recipient_priority) VALUES ($1::varchar,$2::varchar,$3::varchar,$4::varchar,$5::boolean,$6::varchar,$7::int)`
	tx, tErr := pgxPool.Begin(ctx)
	if tErr != nil {
		api.LogError("batchInsertRecipients begin tx failed: %v", tErr)
		// fallback to executing each on pool (best-effort)
		for idx, r := range rows {
			args := make([]interface{}, 7)
			for i := 0; i < 7; i++ {
				if i < len(r) && r[i] != nil {
					args[i] = r[i]
				} else {
					if i == 4 {
						args[i] = true
					} else if i == 2 || i == 3 {
						args[i] = nil
					} else if i == 6 {
						args[i] = 3
					} else {
						args[i] = ""
					}
				}
			}
			if _, serr := pgxPool.Exec(ctx, singleQ, args...); serr != nil {
				api.LogError("batchInsertRecipients single row (pool) failed idx=%d args=%v err=%v", idx, args, serr)
				return serr
			}
		}
		return nil
	}
	defer func() {
		// If tx still open, rollback to be safe
		_ = tx.Rollback(ctx)
	}()

	for idx, r := range rows {
		args := make([]interface{}, 7)
		for i := 0; i < 7; i++ {
			if i < len(r) && r[i] != nil {
				args[i] = r[i]
			} else {
				if i == 4 {
					args[i] = true
				} else if i == 2 || i == 3 {
					args[i] = nil
				} else if i == 6 {
					args[i] = 3
				} else {
					args[i] = ""
				}
			}
		}
		if _, serr := tx.Exec(ctx, singleQ, args...); serr != nil {
			api.LogError("batchInsertRecipients tx single row failed idx=%d args=%v err=%v", idx, args, serr)
			_ = tx.Rollback(ctx)
			return serr
		}
	}
	if cerr := tx.Commit(ctx); cerr != nil {
		api.LogError("batchInsertRecipients tx commit failed: %v", cerr)
		return cerr
	}
	return nil
}

// populateRecipientsForTemplate populates recipients according to strategy
func populateRecipientsForTemplate(ctx context.Context, pgxPool *pgxpool.Pool, templateID string, strategy map[string]interface{}, createdBy string) (int, error) {
	api.LogError("[DEBUG] populateRecipientsForTemplate: templateID=%s strategy=%+v createdBy=%s", templateID, strategy, createdBy)
	modeRaw, _ := strategy["mode"].(string)
	mode := strings.ToUpper(strings.TrimSpace(modeRaw))
	api.LogError("[DEBUG] populateRecipientsForTemplate: mode=%s", mode)
	switch mode {
	case "EXPLICIT":
		// expect 'recipients' : [{recipient_type, recipient_user_id, recipient_role, is_active}]
		recs, ok := strategy["recipients"].([]interface{})
		if !ok || len(recs) == 0 {
			return 0, fmt.Errorf("no explicit recipients provided")
		}
		rows := make([][]interface{}, 0, len(recs))
		for _, ri := range recs {
			rmap, ok := ri.(map[string]interface{})
			if !ok {
				continue
			}
			rtype, _ := rmap["recipient_type"].(string)
			ruserRaw, _ := rmap["recipient_user_id"]
			rroleRaw, _ := rmap["recipient_role"]
			var ruser interface{}
			var rrole interface{}
			if s, ok := ruserRaw.(string); ok && s != "" {
				ruser = s
			} else {
				ruser = nil
			}
			if s, ok := rroleRaw.(string); ok && s != "" {
				rrole = s
			} else {
				rrole = nil
			}
			isActive := true
			if v, ok := rmap["is_active"].(bool); ok {
				isActive = v
			}
			recipPriority := 3
			if v, ok := rmap["recipient_priority"].(float64); ok && v > 0 {
				recipPriority = int(v)
			}
			rows = append(rows, []interface{}{templateID, strings.ToUpper(rtype), ruser, rrole, isActive, createdBy, recipPriority})
		}
		if err := batchInsertRecipients(ctx, pgxPool, templateID, rows); err != nil {
			return 0, err
		}
		return len(rows), nil
	case "ROLE":
		// Expand ROLE into USER recipients: find users having the role and insert USER rows
		roleName, _ := strategy["role"].(string)
		if roleName == "" {
			return 0, fmt.Errorf("role required for ROLE mode")
		}
		// find users for the role
		rowsU, err := pgxPool.Query(ctx, `SELECT u.id FROM users u JOIN user_roles ur ON ur.user_id=u.id JOIN roles ro ON ro.id=ur.role_id WHERE ro.name=$1 OR ro.rolecode=$1`, roleName)
		if err != nil {
			api.LogError("populateRecipientsForTemplate ROLE: failed to query users for role %s: %v", roleName, err)
			return 0, err
		}
		defer rowsU.Close()
		userRows := make([][]interface{}, 0)
		for rowsU.Next() {
			var uid string
			if err := rowsU.Scan(&uid); err == nil {
				userRows = append(userRows, []interface{}{templateID, "USER", uid, nil, true, createdBy})
			}
		}
		if len(userRows) > 0 {
			api.LogError("populateRecipientsForTemplate ROLE: inserting %d users for role %s", len(userRows), roleName)
			if err := batchInsertRecipients(ctx, pgxPool, templateID, userRows); err != nil {
				return 0, err
			}
			return len(userRows), nil
		}
		// fallback: insert a ROLE recipient row so the role is recorded
		q := `INSERT INTO notification_svc.template_recipient (template_id, recipient_type, recipient_role, is_active, created_by) VALUES ($1::varchar,'ROLE',$2::varchar,true,$3::varchar)`
		api.LogError("populateRecipientsForTemplate ROLE: no users found for role %s, inserting ROLE row", roleName)
		if _, err := pgxPool.Exec(ctx, q, templateID, roleName, createdBy); err != nil {
			api.LogError("populateRecipientsForTemplate ROLE: failed to insert ROLE row for %s: %v", roleName, err)
			return 0, err
		}
		return 1, nil
	case "USER":
		// expect 'user_ids' : ["u1","u2"]
		uids, ok := strategy["user_ids"].([]interface{})
		if !ok || len(uids) == 0 {
			return 0, fmt.Errorf("user_ids required for USER mode")
		}
		rows := make([][]interface{}, 0, len(uids))
		for _, ui := range uids {
			uid, _ := ui.(string)
			rows = append(rows, []interface{}{templateID, "USER", uid, nil, true, createdBy})
		}
		if err := batchInsertRecipients(ctx, pgxPool, templateID, rows); err != nil {
			return 0, err
		}
		return len(rows), nil
	case "ALL":
		// select approved users from users table
		rowsU, err := pgxPool.Query(ctx, `SELECT id FROM users WHERE COALESCE(status,'') IN ('approved','active')`)
		if err != nil {
			return 0, err
		}
		defer rowsU.Close()
		rows := make([][]interface{}, 0)
		for rowsU.Next() {
			var uid string
			if err := rowsU.Scan(&uid); err == nil {
				rows = append(rows, []interface{}{templateID, "USER", uid, nil, true, createdBy})
			}
		}
		if len(rows) == 0 {
			return 0, nil
		}
		if err := batchInsertRecipients(ctx, pgxPool, templateID, rows); err != nil {
			return 0, err
		}
		return len(rows), nil
	default:
		return 0, fmt.Errorf("unsupported recipient population mode: %s", mode)
	}
}

// populateRecipientsOnTx is the tx-bound version of populateRecipientsForTemplate.
// All writes go through tx; read-only lookups (e.g. role→user expansion) use pgxPool.
func populateRecipientsOnTx(ctx context.Context, tx pgx.Tx, pgxPool *pgxpool.Pool, templateID string, strategy map[string]interface{}, createdBy string) (int, error) {
	modeRaw, _ := strategy["mode"].(string)
	mode := strings.ToUpper(strings.TrimSpace(modeRaw))
	switch mode {
	case "EXPLICIT":
		recs, ok := strategy["recipients"].([]interface{})
		if !ok || len(recs) == 0 {
			return 0, fmt.Errorf("no explicit recipients provided")
		}
		var rows [][]interface{}
		for _, ri := range recs {
			rmap, ok := ri.(map[string]interface{})
			if !ok {
				continue
			}
			rtype, _ := rmap["recipient_type"].(string)
			var ruser, rrole interface{}
			if s, ok := rmap["recipient_user_id"].(string); ok && s != "" {
				ruser = s
			}
			if s, ok := rmap["recipient_role"].(string); ok && s != "" {
				rrole = s
			}
			isActive := true
			if v, ok := rmap["is_active"].(bool); ok {
				isActive = v
			}
			recipPriority := 3
			if v, ok := rmap["recipient_priority"].(float64); ok && v > 0 {
				recipPriority = int(v)
			}
			rows = append(rows, []interface{}{templateID, strings.ToUpper(rtype), ruser, rrole, isActive, createdBy, recipPriority})
		}
		return len(rows), batchInsertRecipientsOnTx(ctx, tx, rows)

	case "ROLE":
		roleName, _ := strategy["role"].(string)
		if roleName == "" {
			return 0, fmt.Errorf("role required for ROLE mode")
		}
		rollePriority := 3
		if v, ok := strategy["recipient_priority"].(float64); ok && v > 0 {
			rollePriority = int(v)
		}
		// Read-only lookup on pool (role→users) — this is fine, no write conflict
		rowsU, err := pgxPool.Query(ctx,
			`SELECT u.id FROM users u
			  JOIN user_roles ur ON ur.user_id = u.id
			  JOIN roles ro ON ro.id = ur.role_id
			 WHERE ro.name = $1 OR ro.rolecode = $1`, roleName)
		if err != nil {
			return 0, err
		}
		defer rowsU.Close()
		var userRows [][]interface{}
		for rowsU.Next() {
			var uid string
			if err := rowsU.Scan(&uid); err == nil {
				userRows = append(userRows, []interface{}{templateID, "USER", uid, nil, true, createdBy, rollePriority})
			}
		}
		rowsU.Close()
		if len(userRows) > 0 {
			return len(userRows), batchInsertRecipientsOnTx(ctx, tx, userRows)
		}
		// fallback: insert a ROLE sentinel row
		err = batchInsertRecipientsOnTx(ctx, tx, [][]interface{}{{templateID, "ROLE", nil, roleName, true, createdBy, rollePriority}})
		if err != nil {
			return 0, err
		}
		return 1, nil

	case "USER":
		uids, ok := strategy["user_ids"].([]interface{})
		if !ok || len(uids) == 0 {
			return 0, fmt.Errorf("user_ids required for USER mode")
		}
		userPriority := 3
		if v, ok := strategy["recipient_priority"].(float64); ok && v > 0 {
			userPriority = int(v)
		}
		var rows [][]interface{}
		for _, ui := range uids {
			if uid, ok := ui.(string); ok && uid != "" {
				rows = append(rows, []interface{}{templateID, "USER", uid, nil, true, createdBy, userPriority})
			}
		}
		return len(rows), batchInsertRecipientsOnTx(ctx, tx, rows)

	case "ALL":
		allPriority := 3
		if v, ok := strategy["recipient_priority"].(float64); ok && v > 0 {
			allPriority = int(v)
		}
		rowsU, err := pgxPool.Query(ctx, `SELECT id FROM users WHERE COALESCE(status,'') IN ('approved','active')`)
		if err != nil {
			return 0, err
		}
		defer rowsU.Close()
		var rows [][]interface{}
		for rowsU.Next() {
			var uid string
			if err := rowsU.Scan(&uid); err == nil {
				rows = append(rows, []interface{}{templateID, "USER", uid, nil, true, createdBy, allPriority})
			}
		}
		rowsU.Close()
		return len(rows), batchInsertRecipientsOnTx(ctx, tx, rows)

	default:
		return 0, fmt.Errorf("unsupported recipient population mode: %s", mode)
	}
}

// CreateTemplateWithRecipients creates a template and populates recipients per strategy
func CreateTemplateWithRecipients(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string                 `json:"user_id"`
			EventID      string                 `json:"event_id"`
			Channel      string                 `json:"channel"`
			RoleScope    string                 `json:"role_scope"`
			TemplateName string                 `json:"template_name"`
			Description  string                 `json:"description"`
			Subject      string                 `json:"subject"`
			BodyText     string                 `json:"body_text"`
			BodyHTML     string                 `json:"body_html"`
			IsHTML       *bool                  `json:"is_html_enabled"`
			Formula      any                    `json:"formula_steps"`
			Strategy     map[string]interface{} `json:"recipient_strategy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if req.EventID == "" || req.Channel == "" || req.TemplateName == "" {
			api.RespondWithPayload(w, false, "event_id, channel and template_name are required", nil)
			return
		}
		if req.IsHTML == nil {
			def := false
			req.IsHTML = &def
		}
		creator := getRequesterEmailTemplate()
		if creator == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()

		// ── single transaction: template + audit_template + recipients ────────
		// If recipients fail → whole tx rolls back → no dangling template row.
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx) // no-op after Commit

		// 1. Insert template master row
		insertQ := `INSERT INTO notification_svc.template (event_id, channel, role_scope, template_name, description, created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING template_id`
		var tplID string
		if err := tx.QueryRow(ctx, insertQ,
			req.EventID, strings.ToUpper(req.Channel), req.RoleScope,
			req.TemplateName, req.Description, creator,
		).Scan(&tplID); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		// 2. Insert audit_template row
		isHTMLVal := false
		if req.IsHTML != nil {
			isHTMLVal = *req.IsHTML
		}
		var formulaVal interface{} = []byte("[]")
		if req.Formula != nil {
			if jf, jerr := json.Marshal(req.Formula); jerr == nil {
				formulaVal = jf
			}
		}
		auditQ := `INSERT INTO notification_svc.audit_template
			(template_id, action_type, processing_status, subject, body_text, body_html,
			 is_html_enabled, formula_steps, version_label, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,$4,$5,$6,
			        (SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $8),
			        $7, now())`
		if _, err := tx.Exec(ctx, auditQ,
			tplID, req.Subject, req.BodyText, req.BodyHTML,
			isHTMLVal, formulaVal, creator, tplID,
		); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		// 3. Insert recipients inside the SAME tx (FK satisfied because template row
		//    is visible within the transaction — Postgres sees intra-tx uncommitted rows).
		//    populateRecipientsOnTx does read-only lookups on pgxPool and all writes via tx.
		added := 0
		if req.Strategy != nil {
			c, err := populateRecipientsOnTx(ctx, tx, pgxPool, tplID, req.Strategy, creator)
			if err != nil {
				// tx.Rollback fires via defer — nothing is committed
				api.RespondWithPayload(w, false, "recipients failed — rolled back: "+err.Error(), nil)
				return
			}
			added = c
		}

		// 4. Commit everything atomically
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"template_id": tplID, "recipients_added": added, "requested": creator})
	}
}

// CreateTemplateWithRecipientsBulk creates multiple templates and populates recipients per-row strategy
func CreateTemplateWithRecipientsBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				EventID      string                 `json:"event_id"`
				Channel      string                 `json:"channel"`
				RoleScope    string                 `json:"role_scope"`
				TemplateName string                 `json:"template_name"`
				Description  string                 `json:"description"`
				Subject      string                 `json:"subject"`
				BodyText     string                 `json:"body_text"`
				BodyHTML     string                 `json:"body_html"`
				IsHTML       *bool                  `json:"is_html_enabled"`
				Formula      any                    `json:"formula_steps"`
				Strategy     map[string]interface{} `json:"recipient_strategy"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid request body", nil)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithPayload(w, false, "rows required", nil)
			return
		}
		creator := getRequesterEmailTemplate()
		if creator == "" {
			api.RespondWithPayload(w, false, constants.ErrInvalidSessionCapitalized, nil)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer tx.Rollback(ctx)

		// Build batch insert for templates
		valueStrings := make([]string, 0, len(req.Rows))
		valueArgs := make([]interface{}, 0, len(req.Rows)*6)
		for i, rrow := range req.Rows {
			if rrow.IsHTML == nil {
				def := false
				rrow.IsHTML = &def
			}
			pos := i*6 + 1
			valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", pos, pos+1, pos+2, pos+3, pos+4, pos+5))
			valueArgs = append(valueArgs, rrow.EventID, strings.ToUpper(rrow.Channel), rrow.RoleScope, rrow.TemplateName, rrow.Description, creator)
		}

		batchQ := fmt.Sprintf(`INSERT INTO notification_svc.template (event_id, channel, role_scope, template_name, description, created_by) VALUES %s RETURNING template_id, template_name`, strings.Join(valueStrings, ","))
		api.LogError("[DEBUG] Template batch insert: sql=%s args=%v", batchQ, valueArgs)
		rowsOut, err := tx.Query(ctx, batchQ, valueArgs...)
		if err != nil {
			api.LogError("[DEBUG] Template batch insert FAILED: sql=%s args=%v err=%v", batchQ, valueArgs, err)
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rowsOut.Close()
		var inserted []map[string]interface{}
		var templateIDs []string
		for rowsOut.Next() {
			var id, name string
			if err := rowsOut.Scan(&id, &name); err == nil {
				inserted = append(inserted, map[string]interface{}{"template_id": id, "template_name": name})
				templateIDs = append(templateIDs, id)
			}
		}

		// insert audit rows for created templates (compute version_label per template)
		if len(templateIDs) > 0 {
			for idx, id := range templateIDs {
				// map back to request row at same index
				rrow := req.Rows[idx]
				isHTMLVal := false
				if rrow.IsHTML != nil {
					isHTMLVal = *rrow.IsHTML
				}
				var formulaVal interface{} = []byte("[]")
				if rrow.Formula != nil {
					if jf, jerr := json.Marshal(rrow.Formula); jerr == nil {
						formulaVal = jf
					}
				}
				aq := `INSERT INTO notification_svc.audit_template (template_id, action_type, processing_status, subject, body_text, body_html, is_html_enabled, formula_steps, version_label, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,$4,$5,$6,(SELECT 'v' || (COUNT(*)+1) FROM notification_svc.audit_template WHERE template_id = $8),$7,now())`
				api.LogError("[DEBUG] Audit template insert: template=%s subject=%v is_html=%v formula=%v", id, rrow.Subject, isHTMLVal, formulaVal)
				if _, err := tx.Exec(ctx, aq, id, rrow.Subject, rrow.BodyText, rrow.BodyHTML, isHTMLVal, formulaVal, creator, id); err != nil {
					api.LogError("[DEBUG] Audit template insert FAILED: sql=%s err=%v", aq, err)
					api.RespondWithPayload(w, false, err.Error(), nil)
					return
				}
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}

		// Now populate recipients per-row (outside tx — each uses its own pool conn)
		totalAdded := 0
		summary := make([]map[string]interface{}, 0, len(templateIDs))
		for i, tplID := range templateIDs {
			strat := req.Rows[i].Strategy
			api.LogError("[DEBUG] Bulk populate: template=%s strategy=%+v", tplID, strat)
			if strat == nil {
				summary = append(summary, map[string]interface{}{"template_id": tplID, "added": 0, "error": "no strategy"})
				continue
			}
			c, err := populateRecipientsForTemplate(ctx, pgxPool, tplID, strat, creator)
			if err != nil {
				api.LogError("populateRecipientsForTemplate failed for template %s: %v", tplID, err)
				summary = append(summary, map[string]interface{}{"template_id": tplID, "added": 0, "error": err.Error()})
				continue
			}
			totalAdded += c
			summary = append(summary, map[string]interface{}{"template_id": tplID, "added": c, "error": ""})
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"inserted": inserted, "recipients_added": totalAdded, "recipient_summary": summary})
	}
}
