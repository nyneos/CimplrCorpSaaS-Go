package cycle

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

// CloseCycle handles POST /investment/fd-closing/cycle/close — the distinct,
// later terminal step (LOCKED -> CLOSED) that lock/apply.go itself does not
// perform, per the product decision resolving lock/apply.go's original open
// question: a cycle locks first (LOCKED, added to the status CHECK
// constraint via database/2026-08-27/03_fd_closing_cycle_locked_status.sql),
// then closes later after the evidence pack + final sign-off. This is a
// direct action (not a fresh maker-checker artifact) since everything it
// depends on — the lock itself, and the checklist that made the lock
// possible — has already been approved; it is gated only by (a) the cycle
// actually being LOCKED and (b) at least one evidence pack having been
// generated for it, operationalizing "after evidence pack + final sign-off."
func CloseCycle(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID string `json:"cycle_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.CycleID = strings.TrimSpace(req.CycleID)
		if req.CycleID == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "cycle_id is required")
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
			api.LogErrorForResponse(w, "[FDClosingCycle] CloseCycle begin tx: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var entityID, status string
		var isDeleted bool
		err = tx.QueryRow(ctx, `
			SELECT entity_id, status, is_deleted
			FROM investment.fd_closing_cycle
			WHERE cycle_id = $1
			FOR UPDATE`,
			req.CycleID,
		).Scan(&entityID, &status, &isDeleted)
		if err != nil || isDeleted {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Cycle not found")
			return
		}

		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(entityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+entityID+"' is not within your authorized access scope.")
			return
		}

		if status != "LOCKED" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"Only a LOCKED cycle can be closed (current status: "+status+")")
			return
		}

		var evidencePackCount int
		if err = tx.QueryRow(ctx, `
			SELECT COUNT(*) FROM investment.fd_closing_evidence_pack
			WHERE cycle_id = $1 AND is_deleted = false`,
			req.CycleID,
		).Scan(&evidencePackCount); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] CloseCycle evidence pack check: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to verify evidence pack")
			return
		}
		if evidencePackCount == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"Generate an evidence pack before closing this cycle")
			return
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle
			SET status = 'CLOSED', updated_by = $2, updated_at = now()
			WHERE cycle_id = $1`,
			req.CycleID, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] CloseCycle status update: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to close cycle")
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_cycle_event_log (cycle_id, event_type, performed_by)
			VALUES ($1,'CLOSE',$2)`,
			req.CycleID, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] CloseCycle event log insert: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to write event log")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] CloseCycle commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Cycle closed", map[string]interface{}{
			"cycle_id": req.CycleID,
			"status":   "CLOSED",
		})
		api.LogInfo("[FDClosingCycle] CloseCycle: cycle=%s -> CLOSED by=%s", req.CycleID, actor.Email)
	}
}
