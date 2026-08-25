package templates

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

const errFailedUpdateTemplate = "failed to update template"

type updateReq struct {
	TemplateID    string `json:"template_id"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
	// Status Active|Inactive — optional. When only status changes on an APPROVED
	// template, applied immediately (pause/resume). Otherwise staged with header edit.
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	ActorID string `json:"actor_id"`
}

// HandleUpdate raises a header edit for approval, or immediately toggles
// Active↔Inactive when that is the only change on an APPROVED template.
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
		req.Status = strings.TrimSpace(req.Status)
		req.Description = strings.TrimSpace(req.Description)
		if req.TemplateID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "template_id is required", "VALIDATION_ERROR")
			return
		}
		if req.Status != "" && req.Status != "Active" && req.Status != "Inactive" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "status must be Active or Inactive", "VALIDATION_ERROR")
			return
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms template update begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedUpdateTemplate, "DMS_TEMPLATE_UPDATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var oldName, oldModuleCode, oldSubModuleCode, oldStatus, processingStatus, oldDescription string
		if err := tx.QueryRow(r.Context(), `
			SELECT name, module_code, sub_module_code, status, processing_status, COALESCE(description, '')
			FROM dms_svc.template
			WHERE template_id = $1::uuid AND is_deleted = false
			FOR UPDATE`, req.TemplateID,
		).Scan(&oldName, &oldModuleCode, &oldSubModuleCode, &oldStatus, &processingStatus, &oldDescription); err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "template not found", "NOT_FOUND")
			return
		}
		if err := requirePendingFree(r.Context(), tx, req.TemplateID); err != nil {
			api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_TEMPLATE_PENDING_EXISTS")
			return
		}

		// Fill blanks from current so clients can send status-only toggles.
		if req.Name == "" {
			req.Name = oldName
		}
		if req.ModuleCode == "" {
			req.ModuleCode = oldModuleCode
		}
		if req.SubModuleCode == "" {
			req.SubModuleCode = oldSubModuleCode
		}
		if req.Description == "" {
			req.Description = oldDescription
		}

		headerUnchanged := req.Name == oldName && req.ModuleCode == oldModuleCode &&
			req.SubModuleCode == oldSubModuleCode && req.Description == oldDescription
		statusOnly := req.Status != "" && req.Status != oldStatus && headerUnchanged

		if statusOnly {
			if processingStatus != "APPROVED" {
				api.RespondEnvelopeError(w, http.StatusConflict,
					"only APPROVED templates can be paused/resumed via status", "DMS_TEMPLATE_STATUS_LOCKED")
				return
			}
			if _, err := tx.Exec(r.Context(), `
				UPDATE dms_svc.template
				SET status = $1, last_modified_by = $2, last_modified_at = now()
				WHERE template_id = $3::uuid`, req.Status, actor, req.TemplateID); err != nil {
				api.LogErrorForResponse(w, "dms template set status: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update template status", "DMS_TEMPLATE_UPDATE_FAILED")
				return
			}
			reason := strings.TrimSpace(req.Reason)
			if reason == "" {
				reason = "Set status to " + req.Status
			}
			a := &auditRow{}
			a.set("template_id", req.TemplateID)
			a.set("action_type", "SET_STATUS")
			a.set("processing_status", "APPROVED")
			a.set("reason", reason)
			a.set("requested_by", actor)
			a.set("requested_ip", common.NullIfEmpty(ip))
			a.set("checker_by", actor)
			a.set("checker_comment", "Status applied immediately")
			a.set("old_status", oldStatus)
			a.set("new_status", req.Status)
			a.set("old_name", oldName)
			a.set("new_name", oldName)
			a.set("old_module_code", oldModuleCode)
			a.set("new_module_code", oldModuleCode)
			a.set("old_sub_module_code", oldSubModuleCode)
			a.set("new_sub_module_code", oldSubModuleCode)
			a.set("old_description", oldDescription)
			a.set("new_description", oldDescription)
			if err := a.exec(r.Context(), tx); err != nil {
				api.LogErrorForResponse(w, "dms template set status audit: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit status change", "DMS_TEMPLATE_UPDATE_FAILED")
				return
			}
			if err := tx.Commit(r.Context()); err != nil {
				api.LogErrorForResponse(w, "dms template set status commit: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update template status", "DMS_TEMPLATE_UPDATE_FAILED")
				return
			}
			api.RespondEnvelopeSuccess(w, "Template status updated", map[string]interface{}{
				"template_id": req.TemplateID,
				"status":      req.Status,
			})
			return
		}

		if req.Name == "" || req.ModuleCode == "" || req.SubModuleCode == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "name, module_code and sub_module_code are required", "VALIDATION_ERROR")
			return
		}

		newStatus := oldStatus
		if req.Status != "" {
			newStatus = req.Status
		}

		if _, err := tx.Exec(r.Context(), `
			UPDATE dms_svc.template SET processing_status = 'PENDING_EDIT_APPROVAL'
			WHERE template_id = $1::uuid`, req.TemplateID); err != nil {
			api.LogErrorForResponse(w, "dms template update flag: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedUpdateTemplate, "DMS_TEMPLATE_UPDATE_FAILED")
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
		a.set("new_status", newStatus)
		a.set("old_description", oldDescription)
		a.set("new_description", req.Description)
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms template update audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit template update", "DMS_TEMPLATE_UPDATE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms template update commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedUpdateTemplate, "DMS_TEMPLATE_UPDATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Template edit submitted for approval", map[string]interface{}{"template_id": req.TemplateID})
	}
}
