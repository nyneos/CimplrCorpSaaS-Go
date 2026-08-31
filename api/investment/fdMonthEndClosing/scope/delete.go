package scope

import (
	"context"
	"encoding/json"
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DeleteScope handles both POST /investment/fd-closing/scope/delete (single)
// and POST /investment/fd-closing/scope/bulk-delete — removing an FD from a
// cycle's scope. Per the handler spec's Section 2: removable while SELECTED
// (not yet approved) OR — mirroring FD Booking's eligibility-check-before-
// delete pattern — an already-APPROVED FD may still be removed as long as its
// checklist hasn't started (no fd_closing_checklist_item row for this
// scope_id has moved past NOT_STARTED). Immediately inserts a
// PENDING_DELETE_APPROVAL audit row; is_deleted stays false until approved —
// identical sequencing to cycle/delete.go and FD Booking's DeleteBooking.
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

		type removed struct{ scopeID, cycleID, entityID string }
		var okIDs []removed
		var errs []string

		for _, scopeID := range ids {
			tx, err := pool.Begin(ctx)
			if err != nil {
				api.LogErrorForResponse(w, "[FDClosingScope] DeleteScope begin tx for %s: %v", scopeID, err)
				errs = append(errs, scopeID+": transaction failed")
				continue
			}

			var cycleID, entityID, selectionStatus string
			err = tx.QueryRow(ctx, `
				SELECT s.cycle_id, c.entity_id, s.selection_status
				FROM investment.fd_closing_cycle_fd_scope s
				JOIN investment.fd_closing_cycle c ON c.cycle_id = s.cycle_id
				WHERE s.scope_id = $1 AND s.is_deleted = false
				FOR UPDATE OF s`,
				scopeID,
			).Scan(&cycleID, &entityID, &selectionStatus)
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

			// Eligibility: no checklist progress may exist yet for this scope.
			var inProgressCount int
			if err := tx.QueryRow(ctx, `
				SELECT COUNT(*) FROM investment.fd_closing_checklist_item
				WHERE scope_id = $1 AND status NOT IN ('NOT_STARTED')`,
				scopeID,
			).Scan(&inProgressCount); err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				api.LogErrorForResponse(w, "[FDClosingScope] DeleteScope checklist check for %s: %v", scopeID, err)
				errs = append(errs, scopeID+": eligibility check failed")
				continue
			}
			if inProgressCount > 0 {
				tx.Rollback(ctx) //nolint:errcheck
				errs = append(errs, scopeID+": cannot remove — checklist progress already recorded for this FD")
				continue
			}

			// Supersede any earlier pending request for this scope (same
			// reasoning as cycle/update.go and cycle/delete.go).
			if _, err = tx.Exec(ctx, `
				UPDATE investment.fd_closing_cycle_fd_scope_audit
				SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(),
				    checker_comment = 'Superseded by new request'
				WHERE scope_id = $1 AND processing_status IN ('PENDING_APPROVAL','PENDING_DELETE_APPROVAL')`,
				scopeID, api.SystemIfBlank(actor.Email),
			); err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				api.LogErrorForResponse(w, "[FDClosingScope] DeleteScope supersede prior pending for %s: %v", scopeID, err)
				errs = append(errs, scopeID+": failed to supersede prior pending request")
				continue
			}

			if _, err = tx.Exec(ctx, `
				INSERT INTO investment.fd_closing_cycle_fd_scope_audit (
					scope_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
					old_selection_status
				) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now(),$4,$5)`,
				scopeID, nullIfEmpty(req.Reason), api.SystemIfBlank(actor.Email), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
				selectionStatus,
			); err != nil {
				tx.Rollback(ctx) //nolint:errcheck
				api.LogErrorForResponse(w, "[FDClosingScope] DeleteScope audit insert for %s: %v", scopeID, err)
				errs = append(errs, scopeID+": audit insert failed")
				continue
			}

			if err = tx.Commit(ctx); err != nil {
				api.LogErrorForResponse(w, "[FDClosingScope] DeleteScope commit for %s: %v", scopeID, err)
				errs = append(errs, scopeID+": commit failed")
				continue
			}

			okIDs = append(okIDs, removed{scopeID: scopeID, cycleID: cycleID, entityID: entityID})
		}

		results := make([]map[string]interface{}, 0, len(okIDs)+len(errs))
		for _, r := range okIDs {
			results = append(results, map[string]interface{}{
				"success": true, "scope_id": r.scopeID, "status": "PENDING_DELETE_APPROVAL",
			})
		}
		for _, e := range errs {
			results = append(results, map[string]interface{}{"success": false, "error": e})
		}
		msg := "Remove submitted for approval"
		if len(okIDs) == 0 {
			msg = "No scope rows were removed"
		}
		fdclosingcommon.RespondSuccess(w, msg, map[string]interface{}{"results": results})
		api.LogInfo("[FDClosingScope] DeleteScope: ok=%d errors=%d by=%s", len(okIDs), len(errs), actor.Email)

		actorEmail, actorUserID := actor.Email, actor.UserID
		for _, r := range okIDs {
			scopeID, entity := r.scopeID, r.entityID
			runEngineInBackground(func(bgCtx context.Context) {
				if err := approvalengine.CancelPendingInstances(bgCtx, pool, moduleCode, scopeID, actorEmail); err != nil {
					api.LogError("[FDClosingScope] CancelPendingInstances(REMOVE) failed for scope %s: %v", scopeID, err)
					return
				}
				instID, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
					ModuleCode:          moduleCode,
					EntityCode:          entity,
					TransactionType:     TxScopeRemove,
					RecordID:            scopeID,
					RecordTable:         scopeTable,
					AuditTable:          scopeAuditTable,
					AuditIDColumn:       "scope_id",
					ActionType:          "DELETE",
					SubmittedBy:         actorUserID,
					SubmittedByEmail:    actorEmail,
					RequirePinnedMatrix: true,
					// Safe to auto-apply generically here: finalizeRecord's DELETE
					// branch only flips the generic is_deleted column (RecordTable's
					// PK column IS scope_id, matching AuditIDColumn), so the engine's
					// built-in auto-apply is correct as-is — no custom columns
					// involved, unlike the ADD/CREATE path.
					AutoApplyIfUnpinned: true,
				})
				if err != nil {
					api.LogError("[FDClosingScope] CreateInstance(REMOVE) failed for scope %s: %v", scopeID, err)
					return
				}
				if instID != "" {
					api.LogInfo("[FDClosingScope] CreateInstance(REMOVE) %s → scope %s PENDING_DELETE_APPROVAL", instID, scopeID)
				} else {
					api.LogInfo("[FDClosingScope] Auto-applied REMOVE for scope %s — no approval matrix configured", scopeID)
				}
			})
		}
	}
}
