package reopen

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// relockTargetCycleStatus is the fd_closing_cycle.status value Relock
// transitions the cycle back to. Now that LOCKED is a distinct status
// (database/2026-08-27/03_fd_closing_cycle_locked_status.sql) shared by both
// lock types (lock/apply.go), relock symmetrically returns to LOCKED
// regardless of the cycle's original lock_type — no more AWAITING_APPROVAL
// vs. CLOSED asymmetry. This remains a direct one-call convenience shortcut
// rather than a fresh Lock Request → Approve → Apply cycle, per the handler
// spec's explicit instruction.
const relockTargetCycleStatus = "LOCKED"

// RelockCycle handles POST /investment/fd-closing/reopen/relock. Stamps the
// originating reopen request's relocked_at/relocked_by, writes a RELOCK event
// log row, and moves the cycle back to relockTargetCycleStatus. Callable only
// once the request has actually been reopened (reopened_at IS NOT NULL) and
// not yet relocked.
func RelockCycle(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RequestID string `json:"request_id"`
			Reason    string `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.RequestID = strings.TrimSpace(req.RequestID)
		if req.RequestID == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "request_id is required")
			return
		}

		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] RelockCycle begin tx: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var cycleID string
		var reopenedAt, relockedAt *string
		err = tx.QueryRow(ctx, `
			SELECT cycle_id,
			       TO_CHAR(reopened_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
			       TO_CHAR(relocked_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')
			FROM investment.fd_closing_reopen_request
			WHERE request_id = $1 AND is_deleted = false
			FOR UPDATE`,
			req.RequestID,
		).Scan(&cycleID, &reopenedAt, &relockedAt)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Reopen request not found")
			return
		}
		if reopenedAt == nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "Cycle has not been reopened yet — nothing to relock")
			return
		}
		if relockedAt != nil {
			fdclosingcommon.RespondError(w, http.StatusConflict, "This reopen request has already been relocked")
			return
		}

		var entityID string
		err = tx.QueryRow(ctx, `
			SELECT entity_id FROM investment.fd_closing_cycle WHERE cycle_id = $1 AND is_deleted = false FOR UPDATE`,
			cycleID,
		).Scan(&entityID)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Cycle not found")
			return
		}

		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(entityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+entityID+"' is not within your authorized access scope.")
			return
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle
			SET status = $2, updated_by = $3, updated_at = now()
			WHERE cycle_id = $1`,
			cycleID, relockTargetCycleStatus, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] RelockCycle cycle status update: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to update cycle status")
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_cycle_event_log (cycle_id, event_type, reason, performed_by)
			VALUES ($1,'RELOCK',$2,$3)`,
			cycleID, nullIfEmpty(req.Reason), api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] RelockCycle event log insert: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to write event log")
			return
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_reopen_request
			SET relocked_at = now(), relocked_by = $2
			WHERE request_id = $1`,
			req.RequestID, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] RelockCycle request stamp: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to stamp reopen request as relocked")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] RelockCycle commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Cycle relocked", map[string]interface{}{
			"request_id":   req.RequestID,
			"cycle_id":     cycleID,
			"cycle_status": relockTargetCycleStatus,
		})
		api.LogInfo("[FDClosingReopen] RelockCycle: request=%s cycle=%s by=%s", req.RequestID, cycleID, actor.Email)
	}
}
