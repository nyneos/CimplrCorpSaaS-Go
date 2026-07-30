package policy

import (
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type decisionReq struct {
	IDs            []string `json:"ids"`
	CheckerComment string   `json:"checker_comment"`
	ActorID        string   `json:"actor_id"`
}

// HandleApprove bulk-approves pending CREATE/EDIT/DELETE requests. Approving a
// CREATE activates the policy (Active, or Scheduled if effective_start is in
// the future); approving a DELETE finalises the soft delete.
func HandleApprove(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req decisionReq
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
		today := time.Now().UTC().Format("2006-01-02")

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "policy approve begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to approve policies", "POLICY_APPROVE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		approved := make([]string, 0, len(req.IDs))
		errs := make([]string, 0)
		for _, raw := range req.IDs {
			id := strings.TrimSpace(raw)
			if id == "" {
				continue
			}
			pa, err := findPendingAudit(r, tx, id)
			if err != nil {
				errs = append(errs, id+": no pending request")
				continue
			}
			// Settle every outstanding request for this policy, not just the newest.
			// A policy edited before its CREATE was approved has two pending audit
			// rows; closing only the newest leaves the CREATE pending forever.
			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.policy_master_audit
				SET processing_status = 'APPROVED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
				WHERE policy_id = $4::uuid AND processing_status = ANY($5::text[])`,
				actor, common.NullIfEmpty(ip), common.NullIfEmpty(req.CheckerComment), id, common.PendingProcessingStatuses,
			); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}

			switch pa.ActionType {
			case "DELETE":
				if _, err := tx.Exec(r.Context(), `
					UPDATE policyengine_svc.policy_master
					SET processing_status = 'APPROVED', is_deleted = true, last_modified_by = $1, last_modified_at = now()
					WHERE policy_id = $2::uuid`, actor, id,
				); err != nil {
					errs = append(errs, id+": "+err.Error())
					continue
				}
			case "CREATE":
				if _, err := tx.Exec(r.Context(), `
					UPDATE policyengine_svc.policy_master
					SET processing_status = 'APPROVED',
					    status = CASE WHEN effective_start::text > $1 THEN 'Scheduled' ELSE 'Active' END,
					    approved_by = $2, approved_at = now(), last_modified_by = $2, last_modified_at = now()
					WHERE policy_id = $3::uuid AND status = 'PendingApproval'`, today, actor, id,
				); err != nil {
					errs = append(errs, id+": "+err.Error())
					continue
				}
			default: // EDIT
				// If the CREATE was never approved, approving the edit settles it too —
				// the checker has signed off the current definition. Leaving status at
				// PendingApproval would strand the row: never enforced (not Active or
				// Scheduled) and never queued again (nothing left pending).
				if _, err := tx.Exec(r.Context(), `
					UPDATE policyengine_svc.policy_master
					SET processing_status = 'APPROVED', approved_by = $1, approved_at = now(),
					    last_modified_by = $1, last_modified_at = now(),
					    status = CASE
					        WHEN status = 'PendingApproval'
					        THEN CASE WHEN effective_start::text > $2 THEN 'Scheduled' ELSE 'Active' END
					        ELSE status END
					WHERE policy_id = $3::uuid`, actor, today, id,
				); err != nil {
					errs = append(errs, id+": "+err.Error())
					continue
				}
			}
			approved = append(approved, id)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "policy approve commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to approve policies", "POLICY_APPROVE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Policies approved", map[string]interface{}{
			"approved": approved,
			"errors":   errs,
		})
	}
}
