// Package cycle implements the fd_closing_cycle handlers — Section 1 of
// database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md. Every
// handler follows fdBookingWorkbench/booking.go's conventions (pgxpool,
// ctx := r.Context(), api.LogErrorForResponse for server-side error logs,
// RespondEnvelope* — never legacy RespondWith*) with ONE deliberate
// deviation: the EDIT lifecycle is genuine stage-then-apply, not FD Booking's
// apply-immediately-then-(non-reverting)-reject pattern — see update.go,
// approve.go and reject.go for the reasoning.
package cycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// moduleCode is the approval-engine module every fd_closing_cycle instance is
// submitted under — the same shared FD approval-matrix module used by every
// other investment/fd* workbench (fdBookingWorkbench, fdInterestAndTdsWorkbench).
const moduleCode = "FIXED_DEPOSIT"

// Transaction types registered in api/approvalengine/moduleconfig.go's
// txTypeRegistry (AuditTable=investment.fd_closing_cycle_audit,
// AuditIDColumn=cycle_id). CREATE deliberately has no transaction type — cycle
// creation is never gated by the approval engine (matches the mock UI and the
// migration's own design comment).
const (
	TxEditCycle   = "FD_CLOSING_CYCLE_EDIT"
	TxDeleteCycle = "FD_CLOSING_CYCLE_DELETE"
)

const (
	cycleTable      = "investment.fd_closing_cycle"
	cycleAuditTable = "investment.fd_closing_cycle_audit"
)

// CreateCycle handles POST /investment/fd-closing/cycle/create.
// Inserts the cycle directly as DRAFT — no approval gate on CREATE (matches
// the mock UI's onCreateCycle, which never shows an approval step for the
// cycle itself) — and writes a self-approved CREATE audit row for a complete
// trail. No approvalengine.CreateInstance call for this action.
func CreateCycle(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EntityID        string `json:"entity_id"`
			EntityName      string `json:"entity_name"`
			CloseType       string `json:"close_type"`
			BankID          string `json:"bank_id"`
			BankName        string `json:"bank_name"`
			CurrencyCode    string `json:"currency_code"`
			FinancialPeriod string `json:"financial_period"`
			PeriodStart     string `json:"period_start"`
			PeriodEnd       string `json:"period_end"`
			IncludeMatured  *bool  `json:"include_matured"`
			Source          string `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		req.EntityID = strings.TrimSpace(req.EntityID)
		req.CloseType = strings.ToUpper(strings.TrimSpace(req.CloseType))
		req.FinancialPeriod = strings.TrimSpace(req.FinancialPeriod)
		req.PeriodStart = strings.TrimSpace(req.PeriodStart)
		req.PeriodEnd = strings.TrimSpace(req.PeriodEnd)
		if req.EntityID == "" || req.CloseType == "" || req.FinancialPeriod == "" || req.PeriodStart == "" || req.PeriodEnd == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"entity_id, close_type, financial_period, period_start and period_end are required")
			return
		}
		if req.CloseType != "MONTH_END" && req.CloseType != "QUARTER_END" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "close_type must be MONTH_END or QUARTER_END")
			return
		}

		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(req.EntityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				fmt.Sprintf(constants.ErrEntityIDNotAuthorized, req.EntityID))
			return
		}
		// Bank/currency prevalidation — same convention as fdAccrual/run.go,
		// fdReceipt/receipt.go, fdMaster/activation.go: reject a bank/currency
		// that isn't in this request's prevalidated approved scope (loaded by
		// GlobalIndependentMiddleware/GlobalDependentMiddleware into ctx),
		// rather than trusting whatever string the client sent.
		req.BankID = strings.TrimSpace(req.BankID)
		if req.BankID != "" && !scope.HasApprovedBank(req.BankID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				fmt.Sprintf(constants.ErrBankNotApproved1, req.BankID))
			return
		}
		req.CurrencyCode = strings.TrimSpace(req.CurrencyCode)
		if req.CurrencyCode != "" && !scope.HasApprovedCurrency(req.CurrencyCode) {
			fdclosingcommon.RespondError(w, http.StatusForbidden, constants.ErrCurrencyNotApproved)
			return
		}

		entityName := strings.TrimSpace(req.EntityName)
		if entityName == "" {
			entityName = resolveEntityName(ctx, pool, req.EntityID)
		}
		if entityName == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"entity_name is required (could not be resolved from entity_id)")
			return
		}

		includeMatured := true
		if req.IncludeMatured != nil {
			includeMatured = *req.IncludeMatured
		}
		source := strings.ToUpper(strings.TrimSpace(req.Source))
		if source == "" {
			source = "MANUAL"
		}
		if source != "MANUAL" && source != "SCHEDULED" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "source must be MANUAL or SCHEDULED")
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] CreateCycle begin tx: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var cycleID string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_closing_cycle (
				close_type, entity_id, entity_name, bank_id, bank_name, currency_code,
				financial_period, period_start, period_end, include_matured, source,
				status, initiated_by, created_by
			) VALUES (
				$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'DRAFT',$12,$12
			) RETURNING cycle_id`,
			req.CloseType, req.EntityID, entityName, nullIfEmpty(req.BankID), nullIfEmpty(req.BankName), nullIfEmpty(req.CurrencyCode),
			req.FinancialPeriod, req.PeriodStart, req.PeriodEnd, includeMatured, source,
			actor.Email,
		).Scan(&cycleID)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] CreateCycle insert: %v", err)
			msg, status := friendlyDBError(err)
			fdclosingcommon.RespondError(w, status, msg)
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_cycle_audit (
				cycle_id, action_type, processing_status, requested_by, requested_at, requested_ip,
				checker_by, checker_at
			) VALUES ($1,'CREATE','APPROVED',$2,now(),$3,$2,now())`,
			cycleID, api.SystemIfBlank(actor.Email), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] CreateCycle audit insert: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingCycle] CreateCycle commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		fdclosingcommon.RespondSuccess(w, "FD closing cycle created", map[string]interface{}{
			"cycle_id":  cycleID,
			"entity_id": req.EntityID,
			"status":    "DRAFT",
		})
		api.LogInfo("[FDClosingCycle] Cycle created: id=%s entity=%s by=%s", cycleID, req.EntityID, actor.Email)
	}
}

// ─── shared helpers (used by create.go/update.go/delete.go/approve.go) ──────

// resolveEntityName looks up entity_name the same way
// fdBookingWorkbench/helpers.go's getEntityName does — masterentitycash first,
// masterentity as fallback — so callers that only send entity_id still get a
// valid NOT NULL entity_name on the master row.
func resolveEntityName(ctx context.Context, pool *pgxpool.Pool, entityID string) string {
	var name string
	if err := pool.QueryRow(ctx, `SELECT entity_name FROM masterentitycash WHERE entity_id=$1`, entityID).Scan(&name); err == nil {
		return name
	}
	if err := pool.QueryRow(ctx, `SELECT entity_name FROM masterentity WHERE entity_id=$1`, entityID).Scan(&name); err == nil {
		return name
	}
	return ""
}

// nullIfEmpty returns nil for a blank string so optional text columns are
// stored as SQL NULL rather than "" (mirrors fdBookingWorkbench's helper of
// the same name).
func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// friendlyDBError maps common Postgres error codes to a user-facing message
// and HTTP status, mirroring (a trimmed version of) fdBookingWorkbench's
// getUserFriendlyFDError.
func friendlyDBError(err error) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			if strings.Contains(strings.ToLower(pgErr.ConstraintName), "uniq_fd_closing_cycle_scope") {
				return "An open cycle already exists for this entity, financial period and close type.", http.StatusConflict
			}
			return "Duplicate record.", http.StatusConflict
		case "23503":
			return "Referenced record was not found or is not valid.", http.StatusBadRequest
		case "23514":
			return "Invalid value for one of the submitted fields.", http.StatusBadRequest
		}
	}
	return "Database operation failed. Please contact support if this persists.", http.StatusInternalServerError
}

// pendingCycleAction is the shape shared by the direct (no-approval-matrix)
// fallback paths in approve.go/reject.go and applied here so callers can
// react per action_type without duplicating the audit-row lookup query.
type pendingCycleAction struct {
	AuditID    string
	ActionType string
}

// lookupPendingCycleAction locks and returns the single most-recent PENDING%
// audit row for cycleID. Callers must be inside an open transaction (this
// runs FOR UPDATE). Returns pgx.ErrNoRows when nothing is pending.
func lookupPendingCycleAction(ctx context.Context, tx pgx.Tx, cycleID string) (pendingCycleAction, error) {
	var p pendingCycleAction
	err := tx.QueryRow(ctx, `
		SELECT audit_id, action_type
		FROM investment.fd_closing_cycle_audit
		WHERE cycle_id = $1 AND processing_status LIKE 'PENDING%'
		ORDER BY requested_at DESC
		LIMIT 1
		FOR UPDATE`,
		cycleID,
	).Scan(&p.AuditID, &p.ActionType)
	return p, err
}

// ApplyEditToMaster copies the staged new_bank_id/new_currency_code/
// new_include_matured columns from the EDIT audit row matching cycleID +
// matchStatus onto the fd_closing_cycle master row. This is the one place
// that actually "applies" a staged edit — exported because it is called from
// three sites across two packages:
//   - update.go's own goroutine, when no approval matrix applies at all
//     (matchStatus="PENDING_EDIT_APPROVAL", flipAuditStatus=true — nothing
//     else will ever flip this row, so this call must do it itself);
//   - approve.go's direct/no-engine-instance fallback (same as above); and
//   - ../approvalHooks.go's post-finalize hook for the engine-mediated path
//     (matchStatus="APPROVED", flipAuditStatus=false — finalizeRecord already
//     flipped processing_status inside the engine's own transaction before
//     the hook runs, so this call only needs to copy values onto the master).
func ApplyEditToMaster(ctx context.Context, tx pgx.Tx, cycleID, checkerEmail, checkerComment, matchStatus string, flipAuditStatus bool) error {
	var auditID string
	var newBankID, newCurrencyCode *string
	var newIncludeMatured *bool
	err := tx.QueryRow(ctx, `
		SELECT audit_id, new_bank_id, new_currency_code, new_include_matured
		FROM investment.fd_closing_cycle_audit
		WHERE cycle_id = $1 AND action_type = 'EDIT' AND processing_status = $2
		ORDER BY requested_at DESC
		LIMIT 1
		FOR UPDATE`,
		cycleID, matchStatus,
	).Scan(&auditID, &newBankID, &newCurrencyCode, &newIncludeMatured)
	if err != nil {
		return fmt.Errorf("copyCycleEditToMaster: no %s EDIT audit row for cycle %s: %w", matchStatus, cycleID, err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle
		SET bank_id         = COALESCE($2, bank_id),
		    currency_code   = COALESCE($3, currency_code),
		    include_matured = COALESCE($4, include_matured),
		    updated_by      = $5,
		    updated_at      = now()
		WHERE cycle_id = $1`,
		cycleID, newBankID, newCurrencyCode, newIncludeMatured, checkerEmail,
	); err != nil {
		return fmt.Errorf("copyCycleEditToMaster master update: %w", err)
	}

	if flipAuditStatus {
		if _, err := tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle_audit
			SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(), checker_comment = $3
			WHERE audit_id = $1`,
			auditID, checkerEmail, checkerComment,
		); err != nil {
			return fmt.Errorf("copyCycleEditToMaster audit flip: %w", err)
		}
	}
	return nil
}

// runEngineInBackground mirrors booking.go's fire-and-forget engine call
// shape (2-minute timeout, panic-safe) used by update.go/delete.go after
// their own transaction commits.
func runEngineInBackground(fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDClosingCycle] engine goroutine panic: %v", rec)
			}
		}()
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		fn(bgCtx)
	}()
}
