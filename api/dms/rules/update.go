package rules

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type updateReq struct {
	RuleID        string `json:"rule_id"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
	// Status Active|Inactive — optional. Status-only on APPROVED → immediate.
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	ActorID string `json:"actor_id"`
}

// HandleUpdate raises a header edit for approval, or immediately toggles
// Active↔Inactive when that is the only change on an APPROVED rule.
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
		req.RuleID = strings.TrimSpace(req.RuleID)
		req.Name = strings.TrimSpace(req.Name)
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.Status = strings.TrimSpace(req.Status)
		if req.RuleID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "rule_id is required", "VALIDATION_ERROR")
			return
		}
		if req.Status != "" && req.Status != "Active" && req.Status != "Inactive" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "status must be Active or Inactive", "VALIDATION_ERROR")
			return
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms rule update begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update rule", "DMS_RULE_UPDATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var oldName, oldModuleCode, oldSubModuleCode, oldStatus, processingStatus string
		if err := tx.QueryRow(r.Context(), `
			SELECT name, module_code, sub_module_code, status, processing_status
			FROM dms_svc.generation_rule
			WHERE rule_id = $1::uuid AND is_deleted = false
			FOR UPDATE`, req.RuleID,
		).Scan(&oldName, &oldModuleCode, &oldSubModuleCode, &oldStatus, &processingStatus); err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "rule not found", "NOT_FOUND")
			return
		}
		if err := requirePendingFree(r.Context(), tx, req.RuleID); err != nil {
			api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_RULE_PENDING_EXISTS")
			return
		}

		if req.Name == "" {
			req.Name = oldName
		}
		if req.ModuleCode == "" {
			req.ModuleCode = oldModuleCode
		}
		if req.SubModuleCode == "" {
			req.SubModuleCode = oldSubModuleCode
		}

		headerUnchanged := req.Name == oldName && req.ModuleCode == oldModuleCode && req.SubModuleCode == oldSubModuleCode
		statusOnly := req.Status != "" && req.Status != oldStatus && headerUnchanged

		if statusOnly {
			if processingStatus != "APPROVED" {
				api.RespondEnvelopeError(w, http.StatusConflict,
					"only APPROVED rules can be paused/resumed via status", "DMS_RULE_STATUS_LOCKED")
				return
			}
			if _, err := tx.Exec(r.Context(), `
				UPDATE dms_svc.generation_rule
				SET status = $1, last_modified_by = $2, last_modified_at = now()
				WHERE rule_id = $3::uuid`, req.Status, actor, req.RuleID); err != nil {
				api.LogErrorForResponse(w, "dms rule set status: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update rule status", "DMS_RULE_UPDATE_FAILED")
				return
			}
			reason := strings.TrimSpace(req.Reason)
			if reason == "" {
				reason = "Set status to " + req.Status
			}
			a := &auditRow{}
			a.set("rule_id", req.RuleID)
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
			if err := a.exec(r.Context(), tx); err != nil {
				api.LogErrorForResponse(w, "dms rule set status audit: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit status change", "DMS_RULE_UPDATE_FAILED")
				return
			}
			if err := tx.Commit(r.Context()); err != nil {
				api.LogErrorForResponse(w, "dms rule set status commit: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update rule status", "DMS_RULE_UPDATE_FAILED")
				return
			}
			api.RespondEnvelopeSuccess(w, "Rule status updated", map[string]interface{}{
				"rule_id": req.RuleID,
				"status":  req.Status,
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
			UPDATE dms_svc.generation_rule SET processing_status = 'PENDING_EDIT_APPROVAL'
			WHERE rule_id = $1::uuid`, req.RuleID); err != nil {
			api.LogErrorForResponse(w, "dms rule update flag: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update rule", "DMS_RULE_UPDATE_FAILED")
			return
		}

		a := &auditRow{}
		a.set("rule_id", req.RuleID)
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
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms rule update audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit rule update", "DMS_RULE_UPDATE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms rule update commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update rule", "DMS_RULE_UPDATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Rule edit submitted for approval", map[string]interface{}{"rule_id": req.RuleID})
	}
}
