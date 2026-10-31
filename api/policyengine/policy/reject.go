package policy

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/governance"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
			sp, err := tx.Begin(r.Context())
			if err != nil {
				errs = append(errs, id+": failed to reject")
				break
			}
			if msg := rejectPolicy(r, sp, id, actor, ip, req.CheckerComment); msg != "" {
				_ = sp.Rollback(r.Context())
				errs = append(errs, id+": "+msg)
				continue
			}
			if err := sp.Commit(r.Context()); err != nil {
				errs = append(errs, id+": failed to reject")
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

func rejectPolicy(r *http.Request, tx pgx.Tx, id, actor, ip, checkerComment string) string {
	pa, err := findPendingAudit(r, tx, id)
	if err != nil {
		return "no pending request"
	}
	if governance.IsSelfApprovalCtx(r.Context(), pa.RequestedBy) || governance.IsSelfApproval(actor, pa.RequestedBy) {
		return governance.ErrSelfApproval.Error()
	}
	if _, err := tx.Exec(r.Context(), `
		UPDATE policyengine_svc.policy_master_audit
		SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
		WHERE audit_id = $4::uuid`,
		actor, common.NullIfEmpty(ip), common.NullIfEmpty(checkerComment), pa.AuditID,
	); err != nil {
		api.LogError("policy reject %s: %v", id, err)
		return "failed to reject"
	}

	switch pa.ActionType {
	case "CREATE":
		_, err = tx.Exec(r.Context(), `
			UPDATE policyengine_svc.policy_master
			SET processing_status = 'REJECTED', status = 'Inactive', last_modified_by = $1, last_modified_at = now()
			WHERE policy_id = $2::uuid`, actor, id)
	case "EDIT":
		err = revertPolicyEdit(r, tx, pa.AuditID, id, actor)
	default:
		_, err = tx.Exec(r.Context(), `
			UPDATE policyengine_svc.policy_master
			SET processing_status = 'APPROVED', last_modified_by = $1, last_modified_at = now()
			WHERE policy_id = $2::uuid`, actor, id)
	}
	if err != nil {
		api.LogError("policy reject %s: %v", id, err)
		return "failed to reject"
	}
	if pa.ActionType != "CREATE" {
		if _, err := tx.Exec(r.Context(), `
			UPDATE policyengine_svc.policy_master m
			SET processing_status = p.processing_status
			FROM (
				SELECT processing_status FROM policyengine_svc.policy_master_audit
				WHERE policy_id = $1::uuid AND processing_status = ANY($2::text[])
				ORDER BY requested_at DESC LIMIT 1
			) p
			WHERE m.policy_id = $1::uuid`, id, common.PendingProcessingStatuses); err != nil {
			api.LogError("policy reject %s: %v", id, err)
			return "failed to reject"
		}
	}
	return ""
}
