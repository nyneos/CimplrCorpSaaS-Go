package rules

import (
	"context"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleReject bulk-rejects pending CREATE/EDIT/DELETE/CREATE_VERSION
// requests. Stage-then-apply (like templates) — reject never needs to revert
// master columns, since nothing was ever applied ahead of approval.
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
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms rule reject begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject rules", "DMS_RULE_REJECT_FAILED")
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
			if err := applyRejection(r.Context(), tx, id, actor, ip, req.CheckerComment); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}
			rejected = append(rejected, id)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms rule reject commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject rules", "DMS_RULE_REJECT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Rules rejected", map[string]interface{}{
			"rejected": rejected,
			"errors":   errs,
		})
	}
}

func applyRejection(ctx context.Context, tx pgx.Tx, ruleID, actor, ip, checkerComment string) error {
	pa, err := findPendingAudit(ctx, tx, ruleID)
	if err != nil {
		return err
	}

	switch pa.ActionType {
	case "CREATE":
		if pa.VersionID == nil {
			return errNoVersionOnAudit
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.generation_rule_version SET status = 'REJECTED'
			WHERE version_id = $1::uuid`, *pa.VersionID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.generation_rule SET processing_status = 'REJECTED', status = 'Inactive',
				last_modified_by = $1, last_modified_at = now()
			WHERE rule_id = $2::uuid`, actor, ruleID); err != nil {
			return err
		}
	case "EDIT", "DELETE":
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.generation_rule SET processing_status = 'APPROVED',
				last_modified_by = $1, last_modified_at = now()
			WHERE rule_id = $2::uuid`, actor, ruleID); err != nil {
			return err
		}
	case "CREATE_VERSION":
		if pa.VersionID == nil {
			return errNoVersionOnAudit
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.generation_rule_version SET status = 'REJECTED'
			WHERE version_id = $1::uuid`, *pa.VersionID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.generation_rule SET processing_status = 'APPROVED',
				last_modified_by = $1, last_modified_at = now()
			WHERE rule_id = $2::uuid`, actor, ruleID); err != nil {
			return err
		}
	default:
		return errUnknownActionType
	}

	_, err = tx.Exec(ctx, `
		UPDATE dms_svc.generation_rule_audit
		SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
		WHERE audit_id = $4::uuid`,
		actor, common.NullIfEmpty(ip), common.NullIfEmpty(checkerComment), pa.AuditID)
	return err
}
