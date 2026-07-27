package policy

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type deleteReq struct {
	IDs     []string `json:"ids"`
	Reason  string   `json:"reason"`
	ActorID string   `json:"actor_id"`
}

// HandleDelete raises a delete request only — never soft-deletes and never
// rewrites older audit rows. Soft-delete happens on approve of DELETE.
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
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "policy delete begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request policy delete", "POLICY_DELETE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		requested := make([]string, 0, len(req.IDs))
		skipped := make([]string, 0)
		errs := make([]string, 0)

		for _, rawID := range req.IDs {
			id := strings.TrimSpace(rawID)
			if id == "" {
				continue
			}
			var processingStatus string
			err := tx.QueryRow(r.Context(), `
				SELECT processing_status
				FROM policyengine_svc.policy_master
				WHERE policy_id = $1::uuid AND is_deleted = false
				FOR UPDATE`, id,
			).Scan(&processingStatus)
			if err != nil {
				skipped = append(skipped, id)
				continue
			}
			if processingStatus == "PENDING_DELETE_APPROVAL" {
				skipped = append(skipped, id)
				continue
			}

			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.policy_master
				SET processing_status = 'PENDING_DELETE_APPROVAL',
				    last_modified_by = $1, last_modified_at = now()
				WHERE policy_id = $2::uuid`, actor, id); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO policyengine_svc.policy_master_audit (
					policy_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
					old_is_deleted, new_is_deleted
				) VALUES ($1::uuid, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now(), $4, false, true)`,
				id, common.NullIfEmpty(req.Reason), actor, common.NullIfEmpty(ip)); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}
			requested = append(requested, id)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "policy delete commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request policy delete", "POLICY_DELETE_FAILED")
			return
		}

		api.RespondEnvelopeSuccess(w, "Policy delete submitted for approval", map[string]interface{}{
			"requested": requested,
			"skipped":   skipped,
			"errors":    errs,
		})
	}
}
