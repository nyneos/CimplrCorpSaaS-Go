package cycle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ApproveCycle handles both POST /investment/fd-closing/cycle/approve
// (single) and POST /investment/fd-closing/cycle/bulk-approve — the same
// handler accepts one or many cycle_ids, matching fdBookingWorkbench's
// BulkApproveBooking shape.
//
// Gating is SYNCHRONOUS via approvalengine.ActOnPendingOrDiagnose, copied from
// fdInterestAndTdsWorkbench/tdsRegister.go's (previously-fixed) pattern — the
// engine is asked to act BEFORE any status is stamped, and the direct/legacy
// fallback UPDATE is only reachable via the exact same guard tdsRegister.go
// uses: `else if !actionRes.CancelledStale && actionRes.Reason != ""` blocks
// with the Reason; anything else (Reason=="" or a just-cancelled stale
// instance) falls through to the direct path. This mirrors the previously
// fixed cash-module bug where a blocked/"not your turn" Reason was silently
// bypassed by an ungated legacy stamp — do not reorder this check.
func ApproveCycle(pool *pgxpool.Pool) http.HandlerFunc {
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
				Action: approvalengine.ActionApproved, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[FDClosingCycle] ActOnPendingOrDiagnose approve failed for %s: %v", cycleID, actionErr)
				errs = append(errs, cycleID+": "+actionErr.Error())
				continue
			}

			if actionRes.Acted {
				// finalizeRecord (inside RecordAction) already flipped the audit row
				// to APPROVED and — for a DELETE action — already flipped is_deleted
				// generically, all inside the engine's own transaction. For an EDIT
				// action, the registered post-finalize hook (approvalHooks.go) copies
				// new_* onto the master once the instance is fully resolved. Nothing
				// further to do here either way.
				engineActed++
				continue
			}

			if !actionRes.CancelledStale && actionRes.Reason != "" {
				errs = append(errs, cycleID+": "+actionRes.Reason)
				continue
			}

			// No matrix/instance applies at all — direct stamp (legacy/no-matrix
			// path), replicating what the engine would have done generically.
			if err := directApproveCycle(ctx, pool, cycleID, actor.Email, req.Comment); err != nil {
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
			// Nothing was actually approved (every cycle_id was blocked/errored) —
			// the envelope must say so via success:false, not a lying 200/success:true
			// (the exact previously-fixed bug class from the cash module bulk-approve
			// handlers: a toast reading the top-level `success` field would otherwise
			// report "approved" when every row was actually blocked).
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No cycles were approved", payload)
		} else {
			fdclosingcommon.RespondSuccess(w, "Cycle(s) approved", payload)
		}
		api.LogInfo("[FDClosingCycle] ApproveCycle: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errs), actor.Email)
	}
}

// directApproveCycle handles the no-approval-matrix-configured path: find the
// single pending audit row for cycleID and apply exactly what the engine's
// generic finalizer + our own EDIT post-finalize hook would have done
// together, since neither of those runs when there is no engine instance at
// all to act on.
func directApproveCycle(ctx context.Context, pool *pgxpool.Pool, cycleID, checkerEmail, comment string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("tx begin failed: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	pending, err := lookupPendingCycleAction(ctx, tx, cycleID)
	if err != nil {
		return fmt.Errorf("no pending action found (already actioned or not found)")
	}

	switch pending.ActionType {
	case "CREATE":
		if _, err := tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle_audit
			SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(), checker_comment = $3
			WHERE audit_id = $1`,
			pending.AuditID, api.SystemIfBlank(checkerEmail), comment,
		); err != nil {
			return fmt.Errorf("audit flip failed: %w", err)
		}
	case "EDIT":
		if err := ApplyEditToMaster(ctx, tx, cycleID, api.SystemIfBlank(checkerEmail), comment, "PENDING_EDIT_APPROVAL", true); err != nil {
			return err
		}
	case "DELETE":
		if _, err := tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle_audit
			SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(), checker_comment = $3
			WHERE audit_id = $1`,
			pending.AuditID, api.SystemIfBlank(checkerEmail), comment,
		); err != nil {
			return fmt.Errorf("audit flip failed: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle SET is_deleted = true WHERE cycle_id = $1`,
			cycleID,
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

func mergeCycleIDs(single string, many []string) []string {
	seen := make(map[string]struct{}, len(many)+1)
	out := make([]string, 0, len(many)+1)
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	add(single)
	for _, id := range many {
		add(id)
	}
	return out
}
