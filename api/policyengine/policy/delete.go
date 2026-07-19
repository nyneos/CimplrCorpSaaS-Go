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

// HandleDelete raises a delete request (PENDING_DELETE_APPROVAL) — the policy
// is only soft-deleted once a checker approves it.
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

		rows, err := pool.Query(r.Context(), `
			UPDATE policyengine_svc.policy_master
			SET processing_status = 'PENDING_DELETE_APPROVAL', last_modified_by = $2, last_modified_at = now()
			WHERE policy_id = ANY($1::uuid[]) AND is_deleted = false AND processing_status = 'APPROVED'
			RETURNING policy_id::text`,
			req.IDs, actor)
		if err != nil {
			api.LogErrorForResponse(w, "policy delete: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request policy delete", "POLICY_DELETE_FAILED")
			return
		}
		affected := make([]string, 0, len(req.IDs))
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request policy delete", "POLICY_DELETE_FAILED")
				return
			}
			affected = append(affected, id)
		}
		rows.Close()

		for _, id := range affected {
			_, err = pool.Exec(r.Context(), `
				INSERT INTO policyengine_svc.policy_master_audit (
					policy_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
					old_is_deleted, new_is_deleted
				) VALUES ($1::uuid, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now(), $4, false, true)`,
				id, common.NullIfEmpty(req.Reason), actor, common.NullIfEmpty(ip))
			if err != nil {
				api.LogErrorForResponse(w, "policy delete audit: %v", err)
			}
		}

		skipped := make([]string, 0)
		for _, id := range req.IDs {
			id = strings.TrimSpace(id)
			found := false
			for _, ok := range affected {
				if ok == id {
					found = true
					break
				}
			}
			if !found {
				skipped = append(skipped, id)
			}
		}
		api.RespondEnvelopeSuccess(w, "Policy delete requested", map[string]interface{}{
			"requested": affected,
			"skipped":   skipped,
		})
	}
}
