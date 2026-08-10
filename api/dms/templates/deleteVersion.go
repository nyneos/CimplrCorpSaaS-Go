package templates

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

const errFailedDeleteVersion = "failed to delete version"

type deleteVersionReq struct {
	TemplateID string `json:"template_id"`
	VersionID  string `json:"version_id"`
	Reason     string `json:"reason"`
	ActorID    string `json:"actor_id"`
}

// HandleDeleteVersion soft-deletes a non-current template version (is_deleted=true).
// Current live version cannot be deleted — activate another first.
func HandleDeleteVersion(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req deleteVersionReq
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
			api.LogErrorForResponse(w, "dms template version delete begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedDeleteVersion, "DMS_TEMPLATE_VERSION_DELETE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		if err := requirePendingFree(r.Context(), tx, req.TemplateID); err != nil {
			api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_TEMPLATE_PENDING")
			return
		}

		var versionTemplateID string
		var versionNo int
		var alreadyDeleted bool
		err = tx.QueryRow(r.Context(), `
			SELECT template_id::text, version_no, is_deleted
			FROM dms_svc.template_version
			WHERE version_id = $1::uuid
			FOR UPDATE`, req.VersionID,
		).Scan(&versionTemplateID, &versionNo, &alreadyDeleted)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "version not found", "DMS_VERSION_NOT_FOUND")
			return
		}
		if versionTemplateID != req.TemplateID {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "version does not belong to this template", "VALIDATION_ERROR")
			return
		}
		if alreadyDeleted {
			api.RespondEnvelopeSuccess(w, "Version already deleted", map[string]interface{}{
				"template_id": req.TemplateID,
				"version_id":  req.VersionID,
				"version_no":  versionNo,
			})
			return
		}

		var currentVersionID *string
		if err := tx.QueryRow(r.Context(), `
			SELECT current_version_id::text FROM dms_svc.template
			WHERE template_id = $1::uuid AND is_deleted = false
			FOR UPDATE`, req.TemplateID,
		).Scan(&currentVersionID); err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "template not found", "NOT_FOUND")
			return
		}
		if currentVersionID != nil && *currentVersionID == req.VersionID {
			api.RespondEnvelopeError(w, http.StatusConflict,
				"cannot delete the current (live) version — activate another approved version first",
				"DMS_VERSION_IS_CURRENT")
			return
		}

		if _, err := tx.Exec(r.Context(), `
			UPDATE dms_svc.template_version SET is_deleted = true
			WHERE version_id = $1::uuid`, req.VersionID); err != nil {
			api.LogErrorForResponse(w, "dms template version soft-delete: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedDeleteVersion, "DMS_TEMPLATE_VERSION_DELETE_FAILED")
			return
		}

		reason := strings.TrimSpace(req.Reason)
		if reason == "" {
			reason = "Soft-deleted template version"
		}
		a := &auditRow{}
		a.set("template_id", req.TemplateID)
		a.set("version_id", req.VersionID)
		a.set("action_type", "DELETE_VERSION")
		a.set("processing_status", "APPROVED")
		a.set("reason", reason)
		a.set("requested_by", actor)
		a.set("requested_ip", common.NullIfEmpty(ip))
		a.set("checker_by", actor)
		a.set("checker_comment", "Version soft-deleted")
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms template version delete audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit version delete", "DMS_TEMPLATE_VERSION_DELETE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms template version delete commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedDeleteVersion, "DMS_TEMPLATE_VERSION_DELETE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Template version deleted", map[string]interface{}{
			"template_id": req.TemplateID,
			"version_id":  req.VersionID,
			"version_no":  versionNo,
		})
	}
}
