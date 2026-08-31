package cycle

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DeleteCycle handles POST /investment/fd-closing/cycle/delete.
//
// This sequencing IS identical to FD Booking's DeleteBooking (copied
// verbatim per the handler spec): eligibility check (only DRAFT cycles may be
// deleted), then immediately insert a PENDING_DELETE_APPROVAL audit row —
// is_deleted stays false until approved. On approval, the generic
// approvalengine finalizer (finalizeRecord in api/approvalengine/finalizer.go)
// flips is_deleted=true itself because ActionType=="DELETE" and RecordTable/
// AuditIDColumn point at fd_closing_cycle/cycle_id — no per-table hook is
// needed for this part, unlike the EDIT case.
func DeleteCycle(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID string `json:"cycle_id"`
			Reason  string `json:"reason"`
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
			api.LogErrorForResponse(w, "[FDClosingCycle] DeleteCycle begin tx: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var currentStatus, entityID string
		err = tx.QueryRow(ctx, `
			SELECT status, entity_id
			FROM investment.fd_closing_cycle
			WHERE cycle_id = $1 AND is_deleted = false
			FOR UPDATE`,
			req.CycleID,
		).Scan(&currentStatus, &entityID)
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

		// Only a DRAFT cycle may be deleted — never an in-progress/closed one.
		if currentStatus != "DRAFT" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"Only a DRAFT cycle can be deleted (current status: "+currentStatus+")")
			return
		}

		// Supersede any earlier pending request (same reasoning as update.go).
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle_audit
			SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(),
			    checker_comment = 'Superseded by new request'
			WHERE cycle_id = $1 AND processing_status IN ('PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL')`,
			req.CycleID, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] DeleteCycle supersede prior pending: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to supersede prior pending request")
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_cycle_audit (
				cycle_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip
			) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now(),$4)`,
			req.CycleID, nullIfEmpty(req.Reason), api.SystemIfBlank(actor.Email), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] DeleteCycle audit insert: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] DeleteCycle commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Delete submitted for approval", map[string]interface{}{
			"cycle_id": req.CycleID,
			"status":   "PENDING_DELETE_APPROVAL",
		})
		api.LogInfo("[FDClosingCycle] Delete requested: cycle=%s by=%s", req.CycleID, actor.Email)

		cycleID, entity, actorEmail, actorUserID := req.CycleID, entityID, actor.Email, actor.UserID
		runEngineInBackground(func(bgCtx context.Context) {
			if err := approvalengine.CancelPendingInstances(bgCtx, pool, moduleCode, cycleID, actorEmail); err != nil {
				api.LogError("[FDClosingCycle] CancelPendingInstances(DELETE) failed for cycle %s: %v", cycleID, err)
				return
			}
			instID, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:          moduleCode,
				EntityCode:          entity,
				TransactionType:     TxDeleteCycle,
				RecordID:            cycleID,
				RecordTable:         cycleTable,
				AuditTable:          cycleAuditTable,
				AuditIDColumn:       "cycle_id",
				ActionType:          "DELETE",
				SubmittedBy:         actorUserID,
				SubmittedByEmail:    actorEmail,
				RequirePinnedMatrix: true,
				// No auto-apply, matrix or not — every delete waits for an
				// explicit /approve call (unpinned falls to approve.go's own
				// directApproveCycle fallback).
				AutoApplyIfUnpinned: false,
			})
			if err != nil {
				api.LogError("[FDClosingCycle] CreateInstance(DELETE) failed for cycle %s: %v", cycleID, err)
				return
			}
			if instID != "" {
				api.LogInfo("[FDClosingCycle] CreateInstance(DELETE) %s → cycle %s PENDING_DELETE_APPROVAL", instID, cycleID)
			}
		})
	}
}
