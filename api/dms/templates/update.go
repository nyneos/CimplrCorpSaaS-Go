package templates

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type updateReq struct {
	TemplateID    string `json:"template_id"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
	Reason        string `json:"reason"`
	ActorID       string `json:"actor_id"`
}

// HandleUpdate raises a header edit (name/description/module/sub_module) for
// approval. STAGE-THEN-APPLY (CLAUDE.md Database Modeling Rules #4): the
// master row's columns are NOT touched here — only the proposed new_* values
// are written to template_audit. They are applied on approve; on reject there
// is nothing to revert, because nothing was ever changed on the live row.
func HandleUpdate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req updateReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.TemplateID = strings.TrimSpace(req.TemplateID)
		req.Name = strings.TrimSpace(req.Name)
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		if req.TemplateID == "" || req.Name == "" || req.ModuleCode == "" || req.SubModuleCode == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "template_id, name, module_code and sub_module_code are required", "VALIDATION_ERROR")
			return
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms template update begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update template", "DMS_TEMPLATE_UPDATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var oldName, oldModuleCode, oldSubModuleCode, oldStatus string
		if err := tx.QueryRow(r.Context(), `
			SELECT name, module_code, sub_module_code, status FROM dms_svc.template
			WHERE template_id = $1::uuid AND is_deleted = false
			FOR UPDATE`, req.TemplateID,
		).Scan(&oldName, &oldModuleCode, &oldSubModuleCode, &oldStatus); err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "template not found", "NOT_FOUND")
			return
		}
		if err := requirePendingFree(r.Context(), tx, req.TemplateID); err != nil {
			api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_TEMPLATE_PENDING_EXISTS")
			return
		}

		if _, err := tx.Exec(r.Context(), `
			UPDATE dms_svc.template SET processing_status = 'PENDING_EDIT_APPROVAL'
			WHERE template_id = $1::uuid`, req.TemplateID); err != nil {
			api.LogErrorForResponse(w, "dms template update flag: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update template", "DMS_TEMPLATE_UPDATE_FAILED")
			return
		}

		a := &auditRow{}
		a.set("template_id", req.TemplateID)
		a.set("action_type", "EDIT")
		a.set("processing_status", "PENDING_EDIT_APPROVAL")
		a.set("reason", common.NullIfEmpty(req.Reason))
		a.set("requested_by", actor)
		a.set("requested_ip", common.NullIfEmpty(ip))
		a.set("old_name", oldName)
		a.set("new_name", req.Name)
		a.set("old_module_code", oldModuleCode)
		a.set("new_module_code", req.ModuleCode)
		a.set("old_sub_module_code", oldSubModuleCode)
		a.set("new_sub_module_code", req.SubModuleCode)
		a.set("old_status", oldStatus)
		a.set("new_status", oldStatus)
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms template update audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit template update", "DMS_TEMPLATE_UPDATE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms template update commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update template", "DMS_TEMPLATE_UPDATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Template edit submitted for approval", map[string]interface{}{"template_id": req.TemplateID})
	}
}
