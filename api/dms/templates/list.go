package templates

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

type listReq struct {
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
	EntityID      string `json:"entity_id"`
	PendingOnly   bool   `json:"pending_only"`
}

// HandleList lists non-deleted templates, newest-edited first.
func HandleList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req listReq
		if r.ContentLength != 0 {
			if err := common.DecodeJSON(r, &req); err != nil {
				api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
				return
			}
		}
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.EntityID = strings.TrimSpace(req.EntityID)
		scope := ctxutil.FromContext(r.Context())
		if req.EntityID != "" && !common.RequireEntityAccess(w, scope, req.EntityID) {
			return
		}

		query := `
			SELECT t.template_id::text, t.name, t.description, t.template_type, t.module_code, t.sub_module_code,
			       t.entity_id, t.entity_name,
			       t.status, t.processing_status, t.current_version_id::text,
			       COALESCE(t.created_by,''), t.created_at, COALESCE(t.last_modified_by,''), t.last_modified_at,
			       upper(COALESCE(
			         (SELECT tv.content_json->>'kind'
			          FROM dms_svc.template_version tv
			          WHERE tv.version_id = t.current_version_id),
			         t.template_type, 'DOCUMENT'))
			FROM dms_svc.template t
			WHERE t.is_deleted = false`
		args := []interface{}{}
		if req.ModuleCode != "" {
			args = append(args, req.ModuleCode)
			query += " AND t.module_code = $" + strconv.Itoa(len(args))
		}
		if req.SubModuleCode != "" {
			args = append(args, req.SubModuleCode)
			query += " AND t.sub_module_code = $" + strconv.Itoa(len(args))
		}
		if req.EntityID != "" {
			args = append(args, req.EntityID)
			query += " AND (t.entity_id IS NULL OR t.entity_id = $" + strconv.Itoa(len(args)) + ")"
		}
		query, args = common.AppendEntityScopeFilter(query, args, "t.entity_id", scope)
		if req.PendingOnly {
			query += ` AND t.processing_status = ANY('{PENDING_APPROVAL,PENDING_EDIT_APPROVAL,PENDING_DELETE_APPROVAL}')`
		}
		query += " ORDER BY t.last_modified_at DESC"

		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			api.LogErrorForResponse(w, "dms template list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list templates", "DMS_TEMPLATE_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]templateListItem, 0)
		for rows.Next() {
			var it templateListItem
			var createdAt, lastModifiedAt time.Time
			var currentVersionID *string
			var kind string
			if err := rows.Scan(&it.TemplateID, &it.Name, &it.Description, &it.TemplateType, &it.ModuleCode, &it.SubModuleCode,
				&it.EntityID, &it.EntityName,
				&it.Status, &it.ProcessingStatus, &currentVersionID, &it.CreatedBy, &createdAt, &it.LastModifiedBy, &lastModifiedAt,
				&kind); err != nil {
				continue
			}
			it.CurrentVersionID = currentVersionID
			it.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			it.LastModifiedAt = lastModifiedAt.UTC().Format(time.RFC3339)
			if kind != "" {
				it.Kind = kind
			} else {
				it.Kind = "DOCUMENT"
			}
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Templates fetched", out)
	}
}
