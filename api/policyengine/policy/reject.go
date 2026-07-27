package policy

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleReject bulk-rejects pending CREATE/EDIT/DELETE requests.
// EDIT reject does NOT revert master columns or children — only processing_status
// (and CREATE additionally sets status=Inactive). Applied edit data stays as written.
func HandleReject(pool *pgxpool.Pool) http.HandlerFunc {
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

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "policy reject begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject policies", "POLICY_REJECT_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		rejected := make([]string, 0, len(req.IDs))
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
			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.policy_master_audit
				SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
				WHERE audit_id = $4::uuid`,
				actor, common.NullIfEmpty(ip), common.NullIfEmpty(req.CheckerComment), pa.AuditID,
			); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}

			var execErr error
			switch pa.ActionType {
			case "CREATE":
				_, execErr = tx.Exec(r.Context(), `
					UPDATE policyengine_svc.policy_master
					SET processing_status = 'REJECTED', status = 'Inactive', last_modified_by = $1, last_modified_at = now()
					WHERE policy_id = $2::uuid`, actor, id)
			default: // EDIT, DELETE — status only; no column revert
				_, execErr = tx.Exec(r.Context(), `
					UPDATE policyengine_svc.policy_master
					SET processing_status = 'REJECTED', last_modified_by = $1, last_modified_at = now()
					WHERE policy_id = $2::uuid`, actor, id)
			}
			if execErr != nil {
				errs = append(errs, id+": "+execErr.Error())
				continue
			}
			rejected = append(rejected, id)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "policy reject commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject policies", "POLICY_REJECT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Policies rejected", map[string]interface{}{
			"rejected": rejected,
			"errors":   errs,
		})
	}
}
