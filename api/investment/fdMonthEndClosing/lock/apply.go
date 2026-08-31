package lock

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

// lockTypeTargetCycleStatus maps an approved lock's lock_type to the
// fd_closing_cycle.status value Apply transitions the cycle to. Both lock
// types land on LOCKED (matches the mock UI's periodLockCloseApproval.tsx,
// which transitions PeriodCloseInfo.status to "LOCKED" for both alike) —
// CLOSED is a distinct, later terminal step performed by cycle/close.go
// after the evidence pack + final sign-off. LOCKED was added to the
// fd_closing_cycle status CHECK constraint via
// database/2026-08-27/03_fd_closing_cycle_locked_status.sql specifically
// for this.
var lockTypeTargetCycleStatus = map[string]string{
	"SOFT_LOCK": "LOCKED",
	"HARD_LOCK": "LOCKED",
}

// ApplyLock handles POST /investment/fd-closing/lock/apply. Only callable
// once the request has been approved and not yet applied — mirrors
// api/investment/fdMaster/activation.go's "activation/apply is a distinct
// final act, separate from approval" pattern.
func ApplyLock(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RequestID string `json:"request_id"`
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
			api.LogErrorForResponse(w, "[FDClosingLock] ApplyLock begin tx: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var cycleID, lockType, processingStatus, remarks string
		var appliedAt *string
		err = tx.QueryRow(ctx, `
			SELECT cycle_id, lock_type, processing_status, COALESCE(remarks,''),
			       TO_CHAR(applied_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')
			FROM investment.fd_closing_lock_request
			WHERE request_id = $1 AND is_deleted = false
			FOR UPDATE`,
			req.RequestID,
		).Scan(&cycleID, &lockType, &processingStatus, &remarks, &appliedAt)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Lock request not found")
			return
		}
		if processingStatus != "APPROVED" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"Lock request must be APPROVED before it can be applied (current status: "+processingStatus+")")
			return
		}
		if appliedAt != nil {
			fdclosingcommon.RespondError(w, http.StatusConflict, "Lock request has already been applied")
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

		targetStatus, ok := lockTypeTargetCycleStatus[lockType]
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Unrecognized lock_type on request: "+lockType)
			return
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle
			SET status = $2, updated_by = $3, updated_at = now()
			WHERE cycle_id = $1`,
			cycleID, targetStatus, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingLock] ApplyLock cycle status update: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to update cycle status")
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_cycle_event_log (cycle_id, event_type, lock_type, reason, performed_by)
			VALUES ($1,'LOCK',$2,$3,$4)`,
			cycleID, lockType, nullIfEmpty(remarks), api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingLock] ApplyLock event log insert: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to write event log")
			return
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_lock_request
			SET applied_at = now(), applied_by = $2
			WHERE request_id = $1`,
			req.RequestID, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingLock] ApplyLock request stamp: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to stamp lock request as applied")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingLock] ApplyLock commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Lock applied", map[string]interface{}{
			"request_id":   req.RequestID,
			"cycle_id":     cycleID,
			"lock_type":    lockType,
			"cycle_status": targetStatus,
		})
		api.LogInfo("[FDClosingLock] ApplyLock: request=%s cycle=%s type=%s -> status=%s by=%s",
			req.RequestID, cycleID, lockType, targetStatus, actor.Email)
	}
}
