package lock

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

// ApproveLock handles both POST /investment/fd-closing/lock/approve (single)
// and POST /investment/fd-closing/lock/bulk-approve.
//
// Gating is SYNCHRONOUS via approvalengine.ActOnPendingOrDiagnose, copied from
// fdInterestAndTdsWorkbench/tdsRegister.go's pattern (also used verbatim by
// cycle/approve.go) — the engine is asked to act BEFORE any status is
// stamped, and the direct fallback UPDATE is only reachable via the same
// guard: `else if !actionRes.CancelledStale && actionRes.Reason != ""` blocks
// with the Reason; anything else falls through to the direct path.
//
// Approve here only ever flips processing_status→APPROVED (+checker_by/at).
// It deliberately does NOT touch fd_closing_cycle.status — that transition is
// a distinct, later Apply call (see apply.go), mirroring the
// "activation is a separate final act from approval" pattern in
// api/investment/fdMaster/activation.go.
func ApproveLock(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RequestID  string   `json:"request_id"`
			RequestIDs []string `json:"request_ids"`
			Comment    string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		ids := mergeRequestIDs(req.RequestID, req.RequestIDs)
		if len(ids) == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "request_id or request_ids is required")
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

		for _, requestID := range ids {
			actionRes, actionErr := approvalengine.ActOnPendingOrDiagnose(ctx, pool, approvalengine.ActOnPendingRequest{
				ModuleCode: moduleCode, RecordID: requestID,
				UserID: actor.UserID, UserEmail: actor.Email, RoleID: "",
				Action: approvalengine.ActionApproved, Comment: req.Comment,
			})
			if actionErr != nil {
				api.LogError("[FDClosingLock] ActOnPendingOrDiagnose approve failed for %s: %v", requestID, actionErr)
				errs = append(errs, requestID+": "+actionErr.Error())
				continue
			}

			if actionRes.Acted {
				// finalizeRecord already flipped processing_status→APPROVED on
				// the request row itself (AuditTable==RecordTable). Nothing else
				// to do — Apply is a separate, explicit call.
				engineActed++
				continue
			}

			if !actionRes.CancelledStale && actionRes.Reason != "" {
				errs = append(errs, requestID+": "+actionRes.Reason)
				continue
			}

			// No matrix/instance applies at all (e.g. the background
			// CreateInstance call from request.go hasn't run yet, or no
			// approval matrix is configured) — direct stamp, replicating what
			// the engine would have done generically.
			if err := directApproveLockRequest(ctx, pool, requestID, actor.Email, req.Comment); err != nil {
				errs = append(errs, requestID+": "+err.Error())
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
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No lock requests were approved", payload)
		} else {
			fdclosingcommon.RespondSuccess(w, "Lock request(s) approved", payload)
		}
		api.LogInfo("[FDClosingLock] ApproveLock: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errs), actor.Email)
	}
}

// directApproveLockRequest is the no-approval-matrix-configured fallback:
// flip the request row's processing_status straight to APPROVED. There is no
// separate audit table to update (the request row IS the audit trail).
func directApproveLockRequest(ctx context.Context, pool *pgxpool.Pool, requestID, checkerEmail, comment string) error {
	tag, err := pool.Exec(ctx, `
		UPDATE investment.fd_closing_lock_request
		SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(), checker_comment = $3
		WHERE request_id = $1 AND processing_status = 'PENDING_APPROVAL'`,
		requestID, api.SystemIfBlank(checkerEmail), comment,
	)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no pending lock request found (already actioned or not found)")
	}
	return nil
}
