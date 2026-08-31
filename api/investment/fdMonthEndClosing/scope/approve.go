package scope

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ApproveScope handles both POST /investment/fd-closing/scope/approve
// (single) and POST /investment/fd-closing/scope/bulk-approve — same handler
// accepts one or many scope_ids, matching cycle/approve.go's shape.
//
// Gating is SYNCHRONOUS via approvalengine.ActOnPendingOrDiagnose, copied from
// fdInterestAndTdsWorkbench/tdsRegister.go's pattern and cycle/approve.go: the
// engine is asked to act BEFORE any status is stamped, and the direct/no-
// instance fallback is only reachable via the exact same guard
// (`!actionRes.CancelledStale && actionRes.Reason != ""` blocks with the
// Reason; anything else falls through to the direct path). Do not reorder.
//
// On final-eye approval of a CREATE (Add): finalizeRecord (inside
// RecordAction) already flipped the audit row's processing_status to APPROVED
// generically. The registered post-finalize hook (approvalHooks.go, this
// package) then flips selection_status='APPROVED' and seeds the 5 checklist
// rows via applyScopeAddApproval. On final-eye approval of a DELETE (Remove):
// finalizeRecord's generic DELETE branch already flips is_deleted=true itself
// (RecordTable's PK column IS scope_id, matching AuditIDColumn) — no hook
// needed for that half, same as cycle/delete.go's DELETE case.
func ApproveScope(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ScopeID  string   `json:"scope_id"`
			ScopeIDs []string `json:"scope_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := mergeIDs(req.ScopeID, req.ScopeIDs)
		if len(ids) == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "scope_id or scope_ids is required")
			return
		}

		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		engineActed := 0
		directActed := 0
		var errs []string

		for _, scopeID := range ids {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: moduleCode, RecordID: scopeID,
				UserID: actor.UserID, UserEmail: actor.Email, RoleID: "",
				Action: approvalengine.ActionApproved, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[FDClosingScope] ActOnPendingOrDiagnose approve failed for %s: %v", scopeID, actionErr)
				errs = append(errs, scopeID+": "+actionErr.Error())
				continue
			}

			if actionRes.Acted {
				engineActed++
				continue
			}

			if !actionRes.CancelledStale && actionRes.Reason != "" {
				errs = append(errs, scopeID+": "+actionRes.Reason)
				continue
			}

			// No matrix/instance applies at all — direct stamp (legacy/no-matrix
			// path), replicating what the engine + our own post-finalize hook
			// would have done together.
			if err := directApproveScope(ctx, pool, scopeID, actor.Email, req.Comment); err != nil {
				errs = append(errs, scopeID+": "+err.Error())
				continue
			}
			directActed++
		}

		totalActed := engineActed + directActed
		success := totalActed > 0 || len(errs) == 0
		payload := map[string]interface{}{
			"engine_acted": engineActed, "direct_acted": directActed,
			"errors": errs, "checker": actor.Email,
		}
		if !success {
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No scope rows were approved", payload)
		} else {
			fdclosingcommon.RespondSuccess(w, "Scope row(s) approved", payload)
		}
		api.LogInfo("[FDClosingScope] ApproveScope: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errs), actor.Email)
	}
}

// directApproveScope handles the no-approval-matrix-configured path: find the
// single pending audit row for scopeID and apply exactly what the engine's
// generic finalizer + our own ADD post-finalize hook would have done
// together, since neither of those runs when there is no engine instance at
// all to act on.
func directApproveScope(ctx context.Context, pool *pgxpool.Pool, scopeID, checkerEmail, comment string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("tx begin failed: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	pending, err := lookupPendingScopeAction(ctx, tx, scopeID)
	if err != nil {
		return fmt.Errorf("no pending action found (already actioned or not found)")
	}

	switch pending.ActionType {
	case "CREATE":
		if err := applyScopeAddApproval(ctx, tx, scopeID, api.SystemIfBlank(checkerEmail), comment, "PENDING_APPROVAL", true); err != nil {
			return err
		}
	case "DELETE":
		if _, err := tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle_fd_scope_audit
			SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(), checker_comment = $3
			WHERE audit_id = $1`,
			pending.AuditID, api.SystemIfBlank(checkerEmail), comment,
		); err != nil {
			return fmt.Errorf("audit flip failed: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle_fd_scope SET is_deleted = true WHERE scope_id = $1`,
			scopeID,
		); err != nil {
			return fmt.Errorf("is_deleted flip failed: %w", err)
		}
	default:
		return fmt.Errorf("unsupported pending action_type %q", pending.ActionType)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	return nil
}
