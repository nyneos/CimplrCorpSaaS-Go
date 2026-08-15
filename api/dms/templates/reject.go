package templates

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
// requests. Because every request type stage-then-applies (see create.go,
// update.go, versions.go), rejecting one never needs to revert master columns
// or replace child rows the way Policy Engine's reject has to — nothing was
// ever written to the live row in the first place. This is the concrete
// payoff of CLAUDE.md's Database Modeling Rules #4 "stage-then-apply" pattern.
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
			api.LogErrorForResponse(w, "dms template reject begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject templates", "DMS_TEMPLATE_REJECT_FAILED")
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
			api.LogErrorForResponse(w, "dms template reject commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject templates", "DMS_TEMPLATE_REJECT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Templates rejected", map[string]interface{}{
			"rejected": rejected,
			"errors":   errs,
		})
	}
}

func applyRejection(ctx context.Context, tx pgx.Tx, templateID, actor, ip, checkerComment string) error {
	pa, err := findPendingAudit(ctx, tx, templateID)
	if err != nil {
		return err
	}

	switch pa.ActionType {
	case "CREATE":
		// Never went live — nothing to revert, just close out the template and
		// its version 1 (see create.go) as rejected so neither is left dangling
		// in PENDING_APPROVAL.
		if pa.VersionID == nil {
			return errNoVersionOnAudit
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template_version SET status = 'REJECTED'
			WHERE version_id = $1::uuid`, *pa.VersionID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template SET processing_status = 'REJECTED', status = 'Inactive',
				last_modified_by = $1, last_modified_at = now()
			WHERE template_id = $2::uuid`, actor, templateID); err != nil {
			return err
		}
	case "EDIT", "DELETE":
		// Header columns / is_deleted were never touched (stage-then-apply) —
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template SET processing_status = 'REJECTED',
				last_modified_by = $1, last_modified_at = now()
			WHERE template_id = $2::uuid`, actor, templateID); err != nil {
			return err
		}
	case "CREATE_VERSION":
		if pa.VersionID == nil {
			return errNoVersionOnAudit
		}
		// current_version_id was never advanced — the previously approved
		// version (if any) stays live exactly as it was.
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template_version SET status = 'REJECTED'
			WHERE version_id = $1::uuid`, *pa.VersionID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dms_svc.template SET processing_status = 'REJECTED',
				last_modified_by = $1, last_modified_at = now()
			WHERE template_id = $2::uuid`, actor, templateID); err != nil {
			return err
		}
	default:
		return errUnknownActionType
	}

	_, err = tx.Exec(ctx, `
		UPDATE dms_svc.template_audit
		SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
		WHERE audit_id = $4::uuid`,
		actor, common.NullIfEmpty(ip), common.NullIfEmpty(checkerComment), pa.AuditID)
	return err
}
