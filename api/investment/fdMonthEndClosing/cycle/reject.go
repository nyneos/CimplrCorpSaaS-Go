package cycle

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

// RejectCycle handles both POST /investment/fd-closing/cycle/reject (single)
// and POST /investment/fd-closing/cycle/bulk-reject.
//
// Reject is intentionally the simple half of stage-then-apply: for EDIT the
// master was never touched, so there is nothing to revert; for DELETE
// is_deleted was never flipped (that only happens on approve), so there is
// nothing to revert there either. Every reject — engine-mediated or direct —
// only flips the audit row's processing_status to REJECTED.
func RejectCycle(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID  string   `json:"cycle_id"`
			CycleIDs []string `json:"cycle_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := mergeCycleIDs(req.CycleID, req.CycleIDs)
		if len(ids) == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "cycle_id or cycle_ids is required")
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

		for _, cycleID := range ids {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: moduleCode, RecordID: cycleID,
				UserID: actor.UserID, UserEmail: actor.Email, RoleID: "",
				Action: approvalengine.ActionRejected, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[FDClosingCycle] ActOnPendingOrDiagnose reject failed for %s: %v", cycleID, actionErr)
				errs = append(errs, cycleID+": "+actionErr.Error())
				continue
			}

			if actionRes.Acted {
				// finalizeRecord already flipped the audit row to REJECTED. Nothing
				// else to revert — see doc comment above.
				engineActed++
				continue
			}

			if !actionRes.CancelledStale && actionRes.Reason != "" {
				errs = append(errs, cycleID+": "+actionRes.Reason)
				continue
			}

			if err := directRejectCycle(ctx, pool, cycleID, actor.Email, req.Comment); err != nil {
				errs = append(errs, cycleID+": "+err.Error())
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
			// Same accuracy fix as ApproveCycle: don't claim success:true when
			// every cycle_id was actually blocked/errored.
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No cycles were rejected", payload)
		} else {
			fdclosingcommon.RespondSuccess(w, "Cycle(s) rejected", payload)
		}
		api.LogInfo("[FDClosingCycle] RejectCycle: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errs), actor.Email)
	}
}

// directRejectCycle is the no-approval-matrix-configured fallback: flip the
// single pending audit row to REJECTED. For EDIT/DELETE there is no
// master-level revert (see the package doc comment on RejectCycle); for
// CREATE the master row was never actually live, so reject also soft-deletes
// it — a rejected creation must not remain usable.
func directRejectCycle(ctx context.Context, pool *pgxpool.Pool, cycleID, checkerEmail, comment string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("tx begin failed: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	pending, err := lookupPendingCycleAction(ctx, tx, cycleID)
	if err != nil {
		return fmt.Errorf("no pending action found (already actioned or not found)")
	}

	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle_audit
		SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(), checker_comment = $3
		WHERE audit_id = $1`,
		pending.AuditID, api.SystemIfBlank(checkerEmail), comment,
	); err != nil {
		return fmt.Errorf("audit flip failed: %w", err)
	}

	if pending.ActionType == "CREATE" {
		if _, err := tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle SET is_deleted = true WHERE cycle_id = $1`,
			cycleID,
		); err != nil {
			return fmt.Errorf("is_deleted flip failed: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	return nil
}
