package templates

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleAuditLog returns maker-checker audit rows for one template (newest first).
func HandleAuditLog(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req struct {
			TemplateID string `json:"template_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid JSON body", "BAD_REQUEST")
			return
		}
		templateID := strings.TrimSpace(req.TemplateID)
		if templateID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "template_id is required", "BAD_REQUEST")
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT
				audit_id::text,
				template_id::text,
				version_id::text,
				action_type,
				processing_status,
				COALESCE(reason, '') AS reason,
				COALESCE(requested_by, '') AS requested_by,
				requested_at,
				COALESCE(requested_ip, '') AS requested_ip,
				COALESCE(checker_by, '') AS checker_by,
				checker_at,
				COALESCE(checker_ip, '') AS checker_ip,
				COALESCE(checker_comment, '') AS checker_comment,
				old_name, new_name,
				old_module_code, new_module_code,
				old_sub_module_code, new_sub_module_code,
				old_status, new_status,
				old_is_deleted, new_is_deleted
			FROM dms_svc.template_audit
			WHERE template_id = $1::uuid
			ORDER BY GREATEST(
				COALESCE(requested_at, '1970-01-01'::timestamptz),
				COALESCE(checker_at, '1970-01-01'::timestamptz)
			) DESC`, templateID)
		if err != nil {
			api.LogErrorForResponse(w, "dms template audit-log: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load template audit", "DMS_TEMPLATE_AUDIT_FAILED")
			return
		}
		defer rows.Close()

		out, err := common.RowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "dms template audit-log scan: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load template audit", "DMS_TEMPLATE_AUDIT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Template audit fetched", out)
	}
}
