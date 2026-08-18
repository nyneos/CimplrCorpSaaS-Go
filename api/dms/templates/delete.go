package templates

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type deleteReq struct {
	IDs     []string `json:"ids"`
	Reason  string   `json:"reason"`
	ActorID string   `json:"actor_id"`
}

// HandleDelete raises a delete request only — soft-delete (is_deleted = true)
// happens on approve, same PENDING_DELETE_APPROVAL pattern as
// policyengine/cdm's delete handler.
func HandleDelete(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req deleteReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		if len(req.IDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "ids is required", "VALIDATION_ERROR")
			return
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms template delete begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request template delete", "DMS_TEMPLATE_DELETE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		requested := make([]string, 0, len(req.IDs))
		skipped := make([]string, 0)
		errs := make([]string, 0)

		for _, raw := range req.IDs {
			id := strings.TrimSpace(raw)
			if id == "" {
				continue
			}
			if err := requireDeletable(r.Context(), tx, id, actor); err != nil {
				skipped = append(skipped, id)
				continue
			}
			if _, err := tx.Exec(r.Context(), `
				UPDATE dms_svc.template SET processing_status = 'PENDING_DELETE_APPROVAL',
					last_modified_by = $1, last_modified_at = now()
				WHERE template_id = $2::uuid`, actor, id); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}
			a := &auditRow{}
			a.set("template_id", id)
			a.set("action_type", "DELETE")
			a.set("processing_status", "PENDING_DELETE_APPROVAL")
			a.set("reason", common.NullIfEmpty(req.Reason))
			a.set("requested_by", actor)
			a.set("requested_ip", common.NullIfEmpty(ip))
			a.set("old_is_deleted", false)
			a.set("new_is_deleted", true)
			if err := a.exec(r.Context(), tx); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}
			requested = append(requested, id)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms template delete commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request template delete", "DMS_TEMPLATE_DELETE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Template delete submitted for approval", map[string]interface{}{
			"requested": requested,
			"skipped":   skipped,
			"errors":    errs,
		})
	}
}
