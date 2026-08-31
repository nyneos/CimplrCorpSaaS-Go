package reopen

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

// ApproveReopen handles both POST /investment/fd-closing/reopen/approve
// (single) and POST /investment/fd-closing/reopen/bulk-approve. Same shape
// as lock.ApproveLock: synchronous gating via
// approvalengine.ActOnPendingOrDiagnose, and Approve only ever flips
// processing_status→APPROVED — the actual fd_closing_cycle.status='REOPENED'
// transition happens in the separate, later Apply call (apply.go).
func ApproveReopen(pool *pgxpool.Pool) http.HandlerFunc {
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
				api.LogError("[FDClosingReopen] ActOnPendingOrDiagnose approve failed for %s: %v", requestID, actionErr)
				errs = append(errs, requestID+": "+actionErr.Error())
				continue
			}

			if actionRes.Acted {
				engineActed++
				continue
			}

			if !actionRes.CancelledStale && actionRes.Reason != "" {
				errs = append(errs, requestID+": "+actionRes.Reason)
				continue
			}

			if err := directApproveReopenRequest(ctx, pool, requestID, actor.Email, req.Comment); err != nil {
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
			fdclosingcommon.RespondFailureWithData(w, http.StatusConflict, "No reopen requests were approved", payload)
		} else {
			fdclosingcommon.RespondSuccess(w, "Reopen request(s) approved", payload)
		}
		api.LogInfo("[FDClosingReopen] ApproveReopen: engine=%d direct=%d errors=%d by=%s",
			engineActed, directActed, len(errs), actor.Email)
	}
}

// directApproveReopenRequest is the no-approval-matrix-configured fallback:
// flip the request row's processing_status straight to APPROVED. There is no
// separate audit table to update (the request row IS the audit trail).
func directApproveReopenRequest(ctx context.Context, pool *pgxpool.Pool, requestID, checkerEmail, comment string) error {
	tag, err := pool.Exec(ctx, `
		UPDATE investment.fd_closing_reopen_request
		SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(), checker_comment = $3
		WHERE request_id = $1 AND processing_status = 'PENDING_APPROVAL'`,
		requestID, api.SystemIfBlank(checkerEmail), comment,
	)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no pending reopen request found (already actioned or not found)")
	}
	return nil
}
