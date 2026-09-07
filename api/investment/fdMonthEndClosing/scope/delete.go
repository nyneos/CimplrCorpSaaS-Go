package scope

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DeleteScope handles both POST /investment/fd-closing/scope/delete (single)
// and POST /investment/fd-closing/scope/bulk-delete — removing an FD from a
// cycle's scope.
//
// Applied immediately (same as Scope ADD): soft-deletes the scope row and
// removes its checklist items (5 steps) in the same transaction, as long as
// no checklist step has moved past NOT_STARTED. Cycle must not be LOCKED /
// CLOSED. Writes an APPROVED DELETE audit row for the trail.
func DeleteScope(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ScopeID  string   `json:"scope_id"`
			ScopeIDs []string `json:"scope_ids"`
			Reason   string   `json:"reason"`
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
		scopeCtx := ctxutil.FromContext(ctx)
		actorEmail := api.SystemIfBlank(actor.Email)
		actorIP := api.SystemIfBlank(api.ClientIPFromContext(ctx))

		type removed struct{ scopeID, cycleID, fdID string }
		var okIDs []removed
		var errs []string

		for _, scopeID := range ids {
			tx, err := pool.Begin(ctx)
			if err != nil {
				api.LogErrorForResponse(w, "[FDClosingScope] DeleteScope begin tx for %s: %v", scopeID, err)
				errs = append(errs, scopeID+": transaction failed")
				continue
			}

			var cycleID, entityID, selectionStatus, cycleStatus, fdID string
			err = tx.QueryRow(ctx, `
				SELECT s.cycle_id, c.entity_id, s.selection_status, c.status, s.fd_id
				FROM investment.fd_closing_cycle_fd_scope s
				JOIN investment.fd_closing_cycle c ON c.cycle_id = s.cycle_id
				WHERE s.scope_id = $1 AND s.is_deleted = false
				FOR UPDATE OF s`,
				scopeID,
			).Scan(&cycleID, &entityID, &selectionStatus, &cycleStatus, &fdID)
			if err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				errs = append(errs, scopeID+": scope not found")
				continue
			}

			if !scopeCtx.HasEntityAccess(entityID) {
				tx.Rollback(ctx) //nolint:errcheck
				errs = append(errs, scopeID+": entity not within your authorized access scope")
				continue
			}

			if cycleStatus == "LOCKED" || cycleStatus == "CLOSED" {
				tx.Rollback(ctx) //nolint:errcheck
				errs = append(errs, scopeID+": cannot remove FD while cycle is "+cycleStatus)
				continue
			}

			if err := applyScopeRemoveImmediate(ctx, tx, scopeID, cycleID, selectionStatus, actorEmail, actorIP, req.Reason); err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				api.LogErrorForResponse(w, "[FDClosingScope] DeleteScope apply for %s: %v", scopeID, err)
				errs = append(errs, scopeID+": "+err.Error())
				continue
			}

			if err = tx.Commit(ctx); err != nil {
				api.LogErrorForResponse(w, "[FDClosingScope] DeleteScope commit for %s: %v", scopeID, err)
				errs = append(errs, scopeID+": commit failed")
				continue
			}

			okIDs = append(okIDs, removed{scopeID: scopeID, cycleID: cycleID, fdID: fdID})
		}

		results := make([]map[string]interface{}, 0, len(okIDs)+len(errs))
		for _, r := range okIDs {
			results = append(results, map[string]interface{}{
				"success":  true,
				"scope_id": r.scopeID,
				"fd_id":    r.fdID,
				"status":   "REMOVED",
			})
		}
		for _, e := range errs {
			results = append(results, map[string]interface{}{"success": false, "error": e})
		}
		msg := "FD(s) removed from scope; checklist cleared"
		if len(okIDs) == 0 {
			msg = "No scope rows were removed"
		}
		fdclosingcommon.RespondSuccess(w, msg, map[string]interface{}{"results": results})
		api.LogInfo("[FDClosingScope] DeleteScope: ok=%d errors=%d by=%s", len(okIDs), len(errs), actor.Email)
	}
}

// applyScopeRemoveImmediate soft-deletes the scope row, deletes its checklist
// items (and their audits/files), writes an APPROVED DELETE scope audit, and
// refreshes cycle fd_count / readiness. Fails if any checklist step for this
// scope has progressed past NOT_STARTED.
func applyScopeRemoveImmediate(
	ctx context.Context,
	tx pgx.Tx,
	scopeID, cycleID, selectionStatus, actorEmail, actorIP, reason string,
) error {
	var inProgressCount int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM investment.fd_closing_checklist_item
		WHERE scope_id = $1 AND status NOT IN ('NOT_STARTED')`,
		scopeID,
	).Scan(&inProgressCount); err != nil {
		return fmt.Errorf("eligibility check failed")
	}
	if inProgressCount > 0 {
		return fmt.Errorf("cannot remove — checklist progress already recorded for this FD")
	}

	// Supersede any earlier pending request for this scope.
	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle_fd_scope_audit
		SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(),
		    checker_comment = 'Superseded by immediate remove'
		WHERE scope_id = $1 AND processing_status IN ('PENDING_APPROVAL','PENDING_DELETE_APPROVAL')`,
		scopeID, actorEmail,
	); err != nil {
		return fmt.Errorf("failed to supersede prior pending request")
	}

	if err := purgeChecklistForScope(ctx, tx, scopeID, actorEmail, reason); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle_fd_scope
		SET is_deleted = true,
		    selection_status = 'REMOVED',
		    removed_by = $2,
		    removed_at = now()
		WHERE scope_id = $1`,
		scopeID, actorEmail,
	); err != nil {
		return fmt.Errorf("scope soft-delete failed: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO investment.fd_closing_cycle_fd_scope_audit (
			scope_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
			checker_by, checker_at, checker_comment, old_selection_status
		) VALUES (
			$1,'DELETE','APPROVED',$2,$3,now(),$4,
			$3,now(),'Applied immediately with checklist purge',$5
		)`,
		scopeID, nullIfEmpty(reason), actorEmail, actorIP, selectionStatus,
	); err != nil {
		return fmt.Errorf("audit insert failed: %w", err)
	}

	if err := fdclosingcommon.RefreshCycleFdCount(ctx, tx, cycleID); err != nil {
		return fmt.Errorf("fd_count refresh failed: %w", err)
	}
	if err := fdclosingcommon.RefreshCycleReadiness(ctx, tx, cycleID); err != nil {
		return fmt.Errorf("readiness refresh failed: %w", err)
	}
	return nil
}

// purgeChecklistForScope removes checklist items for a scope (audit rows
// first — FK has no ON DELETE CASCADE — then items; files cascade from items).
// Scope DELETE audit remains the durable trail for the remove.
func purgeChecklistForScope(ctx context.Context, tx pgx.Tx, scopeID, _actorEmail, _reason string) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM investment.fd_closing_checklist_item_audit
		WHERE item_id IN (
			SELECT item_id FROM investment.fd_closing_checklist_item WHERE scope_id = $1
		)`,
		scopeID,
	); err != nil {
		return fmt.Errorf("checklist audit purge failed: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM investment.fd_closing_checklist_item WHERE scope_id = $1`,
		scopeID,
	); err != nil {
		return fmt.Errorf("checklist purge failed: %w", err)
	}
	return nil
}

// applyScopeRemoveOnApprove is used by the legacy maker-checker approve path
// (pending DELETE still in flight) and the REMOVE post-finalize hook.
func applyScopeRemoveOnApprove(ctx context.Context, tx pgx.Tx, scopeID, actorEmail, comment string) error {
	var cycleID, selectionStatus string
	if err := tx.QueryRow(ctx, `
		SELECT cycle_id, selection_status
		FROM investment.fd_closing_cycle_fd_scope
		WHERE scope_id = $1
		FOR UPDATE`,
		scopeID,
	).Scan(&cycleID, &selectionStatus); err != nil {
		return fmt.Errorf("scope %s not found: %w", scopeID, err)
	}

	var inProgressCount int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM investment.fd_closing_checklist_item
		WHERE scope_id = $1 AND status NOT IN ('NOT_STARTED')`,
		scopeID,
	).Scan(&inProgressCount); err != nil {
		return fmt.Errorf("eligibility check failed: %w", err)
	}
	if inProgressCount > 0 {
		return fmt.Errorf("cannot remove — checklist progress already recorded for this FD")
	}

	if err := purgeChecklistForScope(ctx, tx, scopeID, actorEmail, comment); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle_fd_scope
		SET is_deleted = true,
		    selection_status = 'REMOVED',
		    removed_by = $2,
		    removed_at = now()
		WHERE scope_id = $1`,
		scopeID, actorEmail,
	); err != nil {
		return fmt.Errorf("is_deleted flip failed: %w", err)
	}

	if err := fdclosingcommon.RefreshCycleFdCount(ctx, tx, cycleID); err != nil {
		return fmt.Errorf("fd_count refresh: %w", err)
	}
	if err := fdclosingcommon.RefreshCycleReadiness(ctx, tx, cycleID); err != nil {
		return fmt.Errorf("readiness refresh: %w", err)
	}
	return nil
}
