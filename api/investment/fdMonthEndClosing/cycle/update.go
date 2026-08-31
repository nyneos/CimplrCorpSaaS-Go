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
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

// UpdateCycle handles POST /investment/fd-closing/cycle/update.
//
// DEVIATION from FD Booking's UpdateBooking (deliberate, per CLAUDE.md's
// preferred pattern for new modules and the handler spec's Section 0):
// this is genuine STAGE-THEN-APPLY, not apply-immediately. The proposed
// bank_id/currency_code/include_matured values are written ONLY onto a new
// fd_closing_cycle_audit row (as new_bank_id/new_currency_code/
// new_include_matured — added by database/2026-08-27/02_fd_closing_cycle_audit_new_columns.sql)
// with processing_status='PENDING_EDIT_APPROVAL'. The master row is NEVER
// touched here. Approve copies new_* onto the master (see approve.go /
// approvalHooks.go); reject only flips the audit row — there is nothing to
// revert because nothing was ever applied.
func UpdateCycle(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID        string  `json:"cycle_id"`
			BankID         *string `json:"bank_id"`
			CurrencyCode   *string `json:"currency_code"`
			IncludeMatured *bool   `json:"include_matured"`
			Reason         string  `json:"reason"`
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
		if req.BankID == nil && req.CurrencyCode == nil && req.IncludeMatured == nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"at least one of bank_id, currency_code, include_matured must be provided")
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
			api.LogErrorForResponse(w, "[FDClosingCycle] UpdateCycle begin tx: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var currentStatus, entityID string
		var curBankID, curCurrencyCode *string
		var curIncludeMatured bool
		err = tx.QueryRow(ctx, `
			SELECT status, entity_id, bank_id, currency_code, include_matured
			FROM investment.fd_closing_cycle
			WHERE cycle_id = $1 AND is_deleted = false
			FOR UPDATE`,
			req.CycleID,
		).Scan(&currentStatus, &entityID, &curBankID, &curCurrencyCode, &curIncludeMatured)
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

		// Immutability guard: once LOCKED or CLOSED, no further header edits
		// (mirrors fdMaster's "no update route once terminal" pattern per the
		// handler spec's Section 7). REOPENED explicitly falls through as
		// editable again — that's the entire point of the reopen/ sub-module.
		if currentStatus == "LOCKED" || currentStatus == "CLOSED" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "Cannot edit a "+strings.ToLower(currentStatus)+" cycle")
			return
		}

		newBankID := curBankID
		if req.BankID != nil {
			newBankID = nullableTrim(*req.BankID)
		}
		newCurrencyCode := curCurrencyCode
		if req.CurrencyCode != nil {
			newCurrencyCode = nullableTrim(*req.CurrencyCode)
		}
		// Same bank/currency prevalidation as create.go — validate the
		// PROPOSED value being staged, not the current one, so a bad edit
		// request never even reaches the audit row.
		if newBankID != nil && *newBankID != "" && !scope.HasApprovedBank(*newBankID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				fmt.Sprintf(constants.ErrBankNotApproved1, *newBankID))
			return
		}
		if newCurrencyCode != nil && *newCurrencyCode != "" && !scope.HasApprovedCurrency(*newCurrencyCode) {
			fdclosingcommon.RespondError(w, http.StatusForbidden, constants.ErrCurrencyNotApproved)
			return
		}
		newIncludeMatured := curIncludeMatured
		if req.IncludeMatured != nil {
			newIncludeMatured = *req.IncludeMatured
		}
		if strPtrEqual(newBankID, curBankID) && strPtrEqual(newCurrencyCode, curCurrencyCode) && newIncludeMatured == curIncludeMatured {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "No changes to apply")
			return
		}

		// Supersede any earlier pending request for this cycle — a new edit
		// replaces whatever was pending before it (mirrors FD Booking's
		// cancel-then-recreate shape in UpdateBooking, adapted for
		// stage-then-apply: since nothing was ever applied for a PENDING_EDIT/
		// DELETE_APPROVAL row, "superseding" it is just flipping the audit row —
		// there is no master state to revert).
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle_audit
			SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(),
			    checker_comment = 'Superseded by new request'
			WHERE cycle_id = $1 AND processing_status IN ('PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL')`,
			req.CycleID, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] UpdateCycle supersede prior pending: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to supersede prior pending request")
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_cycle_audit (
				cycle_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
				old_bank_id, old_currency_code, old_include_matured,
				new_bank_id, new_currency_code, new_include_matured
			) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6,$7,$8,$9,$10)`,
			req.CycleID, nullIfEmpty(req.Reason), api.SystemIfBlank(actor.Email), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			curBankID, curCurrencyCode, curIncludeMatured,
			newBankID, newCurrencyCode, newIncludeMatured,
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] UpdateCycle audit insert: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] UpdateCycle commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Edit submitted for approval", map[string]interface{}{
			"cycle_id": req.CycleID,
			"status":   "PENDING_EDIT_APPROVAL",
		})
		api.LogInfo("[FDClosingCycle] Edit requested: cycle=%s by=%s", req.CycleID, actor.Email)

		cycleID, entity, actorEmail, actorUserID := req.CycleID, entityID, actor.Email, actor.UserID
		runEngineInBackground(func(bgCtx context.Context) {
			// Cancel any in-flight approval instance so the edit resets the chain
			// (mirrors booking.go's UpdateBooking).
			if err := approvalengine.CancelPendingInstances(bgCtx, pool, moduleCode, cycleID, actorEmail); err != nil {
				api.LogError("[FDClosingCycle] CancelPendingInstances(EDIT) failed for cycle %s: %v", cycleID, err)
				return
			}
			instID, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:          moduleCode,
				EntityCode:          entity,
				TransactionType:     TxEditCycle,
				RecordID:            cycleID,
				RecordTable:         cycleTable,
				AuditTable:          cycleAuditTable,
				AuditIDColumn:       "cycle_id",
				ActionType:          "EDIT",
				SubmittedBy:         actorUserID,
				SubmittedByEmail:    actorEmail,
				RequirePinnedMatrix: true,
				// Deliberately NOT AutoApplyIfUnpinned here: the engine's generic
				// auto-apply (policyPin.go's autoApplyUnpinned) only flips the
				// legacy audit row's processing_status — it has no idea about our
				// new_bank_id/new_currency_code/new_include_matured columns, so it
				// would silently mark the request APPROVED without ever copying the
				// staged values onto the master. When no matrix applies (instID==""
				// below) we apply the edit ourselves instead.
				AutoApplyIfUnpinned: false,
			})
			if err != nil {
				api.LogError("[FDClosingCycle] CreateInstance(EDIT) failed for cycle %s: %v", cycleID, err)
				return
			}
			if instID != "" {
				api.LogInfo("[FDClosingCycle] CreateInstance(EDIT) %s → cycle %s PENDING_EDIT_APPROVAL", instID, cycleID)
				return
			}
			// No approval matrix configured for FD_CLOSING_CYCLE_EDIT — apply
			// immediately, same as every other FD_* transaction type's
			// "policy did not trigger approval" auto-apply behavior.
			applyTx, err := pool.Begin(bgCtx)
			if err != nil {
				api.LogError("[FDClosingCycle] auto-apply(EDIT) begin tx failed for cycle %s: %v", cycleID, err)
				return
			}
			defer applyTx.Rollback(bgCtx) //nolint:errcheck
			if err := ApplyEditToMaster(bgCtx, applyTx, cycleID, api.SystemIfBlank(actorEmail),
				"Auto-applied: policy did not trigger approval", "PENDING_EDIT_APPROVAL", true); err != nil {
				api.LogError("[FDClosingCycle] auto-apply(EDIT) failed for cycle %s: %v", cycleID, err)
				return
			}
			if err := applyTx.Commit(bgCtx); err != nil {
				api.LogError("[FDClosingCycle] auto-apply(EDIT) commit failed for cycle %s: %v", cycleID, err)
				return
			}
			api.LogInfo("[FDClosingCycle] Auto-applied EDIT for cycle %s — no approval matrix configured", cycleID)
		})
	}
}

// nullableTrim trims s and returns nil for an empty result (so an explicit
// "" in the request clears the column rather than storing an empty string).
func nullableTrim(s string) *string {
	t := strings.TrimSpace(s)
	if t == "" {
		return nil
	}
	return &t
}

func strPtrEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
