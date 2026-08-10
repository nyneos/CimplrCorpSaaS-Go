package templates

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

type createReq struct {
	Name              string                `json:"name"`
	Description       string                `json:"description"`
	ModuleCode        string                `json:"module_code"`
	SubModuleCode     string                `json:"sub_module_code"`
	EntityID          string                `json:"entity_id"`
	EntityName        string                `json:"entity_name"`
	ContentJSON       json.RawMessage       `json:"content_json"`
	MergeFields       []mergeFieldReq       `json:"merge_fields"`
	ChartPlaceholders []chartPlaceholderReq `json:"chart_placeholders"`
	ActorID           string                `json:"actor_id"`
}

// HandleCreate raises a new template (+ its first content version) for
// approval. Stage-then-apply: the row is written as PENDING_APPROVAL from the
// start — there is no "live" state to protect until a checker approves it.
func HandleCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req createReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.EntityID = strings.TrimSpace(req.EntityID)
		req.EntityName = strings.TrimSpace(req.EntityName)
		if req.Name == "" || req.ModuleCode == "" || req.SubModuleCode == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "name, module_code and sub_module_code are required", "VALIDATION_ERROR")
			return
		}
		if (req.EntityID == "") != (req.EntityName == "") {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "entity_id and entity_name must both be set or both empty", "VALIDATION_ERROR")
			return
		}
		scope := ctxutil.FromContext(r.Context())
		if !common.RequireEntityAccess(w, scope, req.EntityID) {
			return
		}
		if len(req.ContentJSON) == 0 {
			req.ContentJSON = json.RawMessage(`{}`)
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms template create begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create template", "DMS_TEMPLATE_CREATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var templateID string
		if err := tx.QueryRow(r.Context(), `
			INSERT INTO dms_svc.template (name, description, module_code, sub_module_code, entity_id, entity_name, status, processing_status, created_by)
			VALUES ($1, $2, $3, $4, NULLIF($5,''), NULLIF($6,''), 'PendingApproval', 'PENDING_APPROVAL', $7)
			RETURNING template_id::text`,
			req.Name, req.Description, req.ModuleCode, req.SubModuleCode, req.EntityID, req.EntityName, actor,
		).Scan(&templateID); err != nil {
			api.LogErrorForResponse(w, "dms template create insert: %v", err)
			api.RespondEnvelopeError(w, http.StatusConflict, "failed to create template (duplicate name, or unknown module/sub_module?)", "DMS_TEMPLATE_CREATE_FAILED")
			return
		}

		var versionID string
		if err := tx.QueryRow(r.Context(), `
			INSERT INTO dms_svc.template_version (template_id, version_no, content_json, source, status, created_by)
			VALUES ($1::uuid, 1, $2::jsonb, 'EDITOR', 'PENDING_APPROVAL', $3)
			RETURNING version_id::text`,
			templateID, string(req.ContentJSON), actor,
		).Scan(&versionID); err != nil {
			api.LogErrorForResponse(w, "dms template create version: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create template version", "DMS_TEMPLATE_CREATE_FAILED")
			return
		}

		if err := insertVersionChildren(r.Context(), tx, versionID, req.MergeFields, req.ChartPlaceholders); err != nil {
			api.LogErrorForResponse(w, "dms template create children: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadRequest, "failed to attach merge fields / chart placeholders (unknown domain_catalog field?)", "DMS_TEMPLATE_CREATE_FAILED")
			return
		}

		a := &auditRow{}
		a.set("template_id", templateID)
		a.set("version_id", versionID)
		a.set("action_type", "CREATE")
		a.set("processing_status", "PENDING_APPROVAL")
		a.set("requested_by", actor)
		a.set("requested_ip", common.NullIfEmpty(ip))
		a.set("new_name", req.Name)
		a.set("new_module_code", req.ModuleCode)
		a.set("new_sub_module_code", req.SubModuleCode)
		a.set("new_entity_id", common.NullIfEmpty(req.EntityID))
		a.set("new_entity_name", common.NullIfEmpty(req.EntityName))
		a.set("new_status", "PendingApproval")
		a.set("new_is_deleted", false)
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms template create audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit template creation", "DMS_TEMPLATE_CREATE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms template create commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create template", "DMS_TEMPLATE_CREATE_FAILED")
			return
		}

		api.RespondEnvelopeSuccess(w, "Template submitted for approval", map[string]interface{}{
			"template_id": templateID,
			"version_id":  versionID,
		})
	}
}
