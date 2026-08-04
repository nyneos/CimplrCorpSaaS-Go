package rules

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type activateVersionReq struct {
	RuleID    string `json:"rule_id"`
	VersionID string `json:"version_id"`
	Reason    string `json:"reason"`
	ActorID   string `json:"actor_id"`
}

// HandleActivateVersion points generation_rule.current_version_id at another
// APPROVED version. Exactly one version is live for runs/schedules.
func HandleActivateVersion(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req activateVersionReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.RuleID = strings.TrimSpace(req.RuleID)
		req.VersionID = strings.TrimSpace(req.VersionID)
		if req.RuleID == "" || req.VersionID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "rule_id and version_id are required", "VALIDATION_ERROR")
			return
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms rule activate begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to activate version", "DMS_RULE_ACTIVATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		if err := requirePendingFree(r.Context(), tx, req.RuleID); err != nil {
			api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_RULE_PENDING")
			return
		}

		var versionStatus, versionRuleID string
		var versionNo int
		err = tx.QueryRow(r.Context(), `
			SELECT rule_id::text, status, version_no
			FROM dms_svc.generation_rule_version
			WHERE version_id = $1::uuid`, req.VersionID,
		).Scan(&versionRuleID, &versionStatus, &versionNo)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "version not found", "DMS_VERSION_NOT_FOUND")
			return
		}
		if versionRuleID != req.RuleID {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "version does not belong to this rule", "VALIDATION_ERROR")
			return
		}
		if versionStatus != "APPROVED" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "only APPROVED versions can be activated", "VALIDATION_ERROR")
			return
		}

		var prevVersionID *string
		err = tx.QueryRow(r.Context(), `
			SELECT current_version_id::text FROM dms_svc.generation_rule
			WHERE rule_id = $1::uuid AND is_deleted = false`, req.RuleID,
		).Scan(&prevVersionID)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "rule not found", "DMS_RULE_NOT_FOUND")
			return
		}
		if prevVersionID != nil && *prevVersionID == req.VersionID {
			api.RespondEnvelopeSuccess(w, "Version already current", map[string]interface{}{
				"rule_id":            req.RuleID,
				"current_version_id": req.VersionID,
				"version_no":         versionNo,
				"unchanged":          true,
			})
			return
		}

		if _, err := tx.Exec(r.Context(), `
			UPDATE dms_svc.generation_rule
			SET current_version_id = $1::uuid, last_modified_by = $2, last_modified_at = now()
			WHERE rule_id = $3::uuid`,
			req.VersionID, actor, req.RuleID,
		); err != nil {
			api.LogErrorForResponse(w, "dms rule activate update: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to activate version", "DMS_RULE_ACTIVATE_FAILED")
			return
		}

		reason := strings.TrimSpace(req.Reason)
		if reason == "" {
			reason = "Activated approved version"
		}
		a := &auditRow{}
		a.set("rule_id", req.RuleID)
		a.set("version_id", req.VersionID)
		a.set("action_type", "ACTIVATE_VERSION")
		a.set("processing_status", "APPROVED")
		a.set("reason", reason)
		a.set("requested_by", actor)
		a.set("requested_ip", common.NullIfEmpty(ip))
		a.set("checker_by", actor)
		a.set("checker_comment", "Activated as current")
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms rule activate audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit activation", "DMS_RULE_ACTIVATE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms rule activate commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to activate version", "DMS_RULE_ACTIVATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Version activated", map[string]interface{}{
			"rule_id":             req.RuleID,
			"previous_version_id": prevVersionID,
			"current_version_id":  req.VersionID,
			"version_no":          versionNo,
		})
	}
}
