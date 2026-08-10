package templates

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

const errFailedActivateVersion = "failed to activate version"

type activateVersionReq struct {
	TemplateID string `json:"template_id"`
	VersionID  string `json:"version_id"`
	Reason     string `json:"reason"`
	ActorID    string `json:"actor_id"`
}

// HandleActivateVersion points template.current_version_id at another APPROVED
// version of the same template. Exactly one version is live at a time (the
// master row holds a single FK). Does not rewrite version rows — append-only
// audit records ACTIVATE_VERSION.
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
		req.TemplateID = strings.TrimSpace(req.TemplateID)
		req.VersionID = strings.TrimSpace(req.VersionID)
		if req.TemplateID == "" || req.VersionID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "template_id and version_id are required", "VALIDATION_ERROR")
			return
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms template activate begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedActivateVersion, "DMS_TEMPLATE_ACTIVATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		if err := requirePendingFree(r.Context(), tx, req.TemplateID); err != nil {
			api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_TEMPLATE_PENDING")
			return
		}

		var versionStatus, versionTemplateID string
		var versionNo int
		err = tx.QueryRow(r.Context(), `
			SELECT template_id::text, status, version_no
			FROM dms_svc.template_version
			WHERE version_id = $1::uuid AND is_deleted = false`, req.VersionID,
		).Scan(&versionTemplateID, &versionStatus, &versionNo)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "version not found", "DMS_VERSION_NOT_FOUND")
			return
		}
		if versionTemplateID != req.TemplateID {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "version does not belong to this template", "VALIDATION_ERROR")
			return
		}
		if versionStatus != "APPROVED" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "only APPROVED versions can be activated", "VALIDATION_ERROR")
			return
		}

		var prevVersionID *string
		var masterStatus string
		err = tx.QueryRow(r.Context(), `
			SELECT current_version_id::text, status FROM dms_svc.template
			WHERE template_id = $1::uuid AND is_deleted = false`, req.TemplateID,
		).Scan(&prevVersionID, &masterStatus)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "template not found", "DMS_TEMPLATE_NOT_FOUND")
			return
		}
		if prevVersionID != nil && *prevVersionID == req.VersionID {
			api.RespondEnvelopeSuccess(w, "Version already current", map[string]interface{}{
				"template_id":        req.TemplateID,
				"current_version_id": req.VersionID,
				"version_no":         versionNo,
				"status":             masterStatus,
				"unchanged":          true,
			})
			return
		}

		// Switching live version also marks the master Active (entity on/off still
		// uses SET_STATUS separately; live version pick resumes the entity).
		if _, err := tx.Exec(r.Context(), `
			UPDATE dms_svc.template
			SET current_version_id = $1::uuid, status = 'Active',
			    last_modified_by = $2, last_modified_at = now()
			WHERE template_id = $3::uuid`,
			req.VersionID, actor, req.TemplateID,
		); err != nil {
			api.LogErrorForResponse(w, "dms template activate update: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedActivateVersion, "DMS_TEMPLATE_ACTIVATE_FAILED")
			return
		}

		reason := strings.TrimSpace(req.Reason)
		if reason == "" {
			reason = "Activated approved version as live"
		}
		a := &auditRow{}
		a.set("template_id", req.TemplateID)
		a.set("version_id", req.VersionID)
		a.set("action_type", "ACTIVATE_VERSION")
		a.set("processing_status", "APPROVED")
		a.set("reason", reason)
		a.set("requested_by", actor)
		a.set("requested_ip", common.NullIfEmpty(ip))
		a.set("checker_by", actor)
		a.set("checker_comment", "Activated as current")
		a.set("old_status", masterStatus)
		a.set("new_status", "Active")
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms template activate audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit activation", "DMS_TEMPLATE_ACTIVATE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms template activate commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedActivateVersion, "DMS_TEMPLATE_ACTIVATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Version activated", map[string]interface{}{
			"template_id":         req.TemplateID,
			"previous_version_id": prevVersionID,
			"current_version_id":  req.VersionID,
			"version_no":          versionNo,
			"status":              "Active",
		})
	}
}
