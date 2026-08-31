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

// ApplyReopen handles POST /investment/fd-closing/reopen/apply. Only
// callable once the request has been approved and not yet reopened — mirrors
// lock.ApplyLock's "apply is a distinct final act, separate from approval"
// pattern (itself modeled on api/investment/fdMaster/activation.go).
func ApplyReopen(pool *pgxpool.Pool) http.HandlerFunc {
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
			api.LogErrorForResponse(w, "[FDClosingReopen] ApplyReopen begin tx: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var cycleID, processingStatus, reason string
		var reopenedAt *string
		err = tx.QueryRow(ctx, `
			SELECT cycle_id, processing_status, reason,
			       TO_CHAR(reopened_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')
			FROM investment.fd_closing_reopen_request
			WHERE request_id = $1 AND is_deleted = false
			FOR UPDATE`,
			req.RequestID,
		).Scan(&cycleID, &processingStatus, &reason, &reopenedAt)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Reopen request not found")
			return
		}
		if processingStatus != "APPROVED" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"Reopen request must be APPROVED before it can be applied (current status: "+processingStatus+")")
			return
		}
		if reopenedAt != nil {
			fdclosingcommon.RespondError(w, http.StatusConflict, "Reopen request has already been applied")
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
			SET status = 'REOPENED', updated_by = $2, updated_at = now()
			WHERE cycle_id = $1`,
			cycleID, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] ApplyReopen cycle status update: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to update cycle status")
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_cycle_event_log (cycle_id, event_type, reason, performed_by)
			VALUES ($1,'REOPEN',$2,$3)`,
			cycleID, nullIfEmpty(reason), api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] ApplyReopen event log insert: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to write event log")
			return
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_reopen_request
			SET reopened_at = now(), reopened_by = $2
			WHERE request_id = $1`,
			req.RequestID, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] ApplyReopen request stamp: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to stamp reopen request as applied")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] ApplyReopen commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Cycle reopened", map[string]interface{}{
			"request_id":   req.RequestID,
			"cycle_id":     cycleID,
			"cycle_status": "REOPENED",
		})
		api.LogInfo("[FDClosingReopen] ApplyReopen: request=%s cycle=%s by=%s", req.RequestID, cycleID, actor.Email)
	}
}
