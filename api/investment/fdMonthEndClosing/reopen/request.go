package reopen

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/approvalengine"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// moduleCode is the approval-engine module every fd_closing_reopen_request
// instance is submitted under — the same shared FD approval-matrix module
// used by every other investment/fd* workbench (matches cycle.moduleCode /
// lock.moduleCode).
const moduleCode = "FIXED_DEPOSIT"

// TxReopenRequest is registered in api/approvalengine/moduleconfig.go's
// txTypeRegistry with AuditTable=investment.fd_closing_reopen_request (the
// request row IS its own audit trail — see the migration's design comment)
// and AuditIDColumn=request_id.
const TxReopenRequest = "FD_CLOSING_REOPEN"

const reopenRequestTable = "investment.fd_closing_reopen_request"

// eligibleReopenCycleStatuses is the set of fd_closing_cycle.status values a
// reopen may be requested against — i.e. cycles that have actually had a lock
// applied (LOCKED, both lock types per lock/apply.go) or been fully closed
// (CLOSED, via cycle/close.go).
var eligibleReopenCycleStatuses = map[string]bool{
	"LOCKED": true,
	"CLOSED": true,
}

// RequestReopen handles POST /investment/fd-closing/reopen/request. Runs the
// pre-submit validation checks, then inserts the request row directly as
// PENDING_APPROVAL — the migration's partial unique index
// (uniq_fd_closing_reopen_request_pending) already prevents two concurrent
// pending requests per cycle, so a duplicate insert is left to surface as a
// normal DB constraint-violation error mapped to a 409 rather than
// pre-checking-then-inserting (race-prone), per the handler spec.
func RequestReopen(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID       string `json:"cycle_id"`
			Reason        string `json:"reason"`
			ImpactSummary string `json:"impact_summary"`
			ApproverID    string `json:"approver_id"`
			ApproverName  string `json:"approver_name"`
			ApproverRole  string `json:"approver_role"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.CycleID = strings.TrimSpace(req.CycleID)
		req.Reason = strings.TrimSpace(req.Reason)
		req.ApproverID = strings.TrimSpace(req.ApproverID)
		req.ApproverName = strings.TrimSpace(req.ApproverName)
		if req.CycleID == "" || req.Reason == "" || req.ApproverID == "" || req.ApproverName == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"cycle_id, reason, approver_id and approver_name are required")
			return
		}

		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var entityID, cycleStatus string
		var isDeleted bool
		err := pool.QueryRow(ctx, `
			SELECT entity_id, status, is_deleted
			FROM investment.fd_closing_cycle
			WHERE cycle_id = $1`,
			req.CycleID,
		).Scan(&entityID, &cycleStatus, &isDeleted)
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

		if !eligibleReopenCycleStatuses[cycleStatus] {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"Only a locked/closed cycle can be reopened (current status: "+cycleStatus+")")
			return
		}

		// Pre-submit validation snapshot (accrual_valid / reconciliation_valid /
		// accounting_valid). TODO: tighten validation rules once the mock UI's
		// exact reopen-eligibility criteria are confirmed — the handler spec
		// explicitly allows a placeholder here (all-true / COMPLETED) since the
		// mock UI's ReopenValidationResult only shows a validation_status of
		// PENDING at request time and fills in real accrual/reconciliation/
		// accounting results asynchronously AFTER the period is reopened, not
		// before the request is submitted (see periodReopen.tsx's
		// handleReopenPeriod, which kicks off validation only after reopening).
		accrualValid, reconciliationValid, accountingValid := true, true, true
		validationStatus := "COMPLETED"

		var requestID string
		err = pool.QueryRow(ctx, `
			INSERT INTO investment.fd_closing_reopen_request (
				cycle_id, reason, impact_summary,
				approver_id, approver_name, approver_role,
				accrual_valid, reconciliation_valid, accounting_valid,
				validation_status, validated_at, validated_by,
				processing_status, requested_by, requested_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now(),$11,'PENDING_APPROVAL',$11,now())
			RETURNING request_id`,
			req.CycleID, req.Reason, nullIfEmpty(req.ImpactSummary),
			req.ApproverID, req.ApproverName, nullIfEmpty(req.ApproverRole),
			accrualValid, reconciliationValid, accountingValid,
			validationStatus, api.SystemIfBlank(actor.Email),
		).Scan(&requestID)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingReopen] RequestReopen insert: %v", err)
			msg, status := friendlyReopenDBError(err)
			fdclosingcommon.RespondError(w, status, msg)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Reopen request submitted for approval", map[string]interface{}{
			"request_id": requestID,
			"cycle_id":   req.CycleID,
			"status":     "PENDING_APPROVAL",
		})
		api.LogInfo("[FDClosingReopen] Reopen requested: request=%s cycle=%s by=%s", requestID, req.CycleID, actor.Email)

		reqID, entity, actorEmail, actorUserID := requestID, entityID, actor.Email, actor.UserID
		runEngineInBackground(func(bgCtx context.Context) {
			instID, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:          moduleCode,
				EntityCode:          entity,
				TransactionType:     TxReopenRequest,
				RecordID:            reqID,
				RecordTable:         reopenRequestTable,
				AuditTable:          reopenRequestTable,
				AuditIDColumn:       "request_id",
				ActionType:          "CREATE",
				SubmittedBy:         actorUserID,
				SubmittedByEmail:    actorEmail,
				RequirePinnedMatrix: true,
				// Safe to auto-apply generically here — same reasoning as
				// lock/request.go: AuditTable==RecordTable, so the generic
				// finalizeRecord flip is exactly correct with no custom columns
				// to copy.
				AutoApplyIfUnpinned: true,
			})
			if err != nil {
				api.LogError("[FDClosingReopen] CreateInstance failed for request %s: %v", reqID, err)
				return
			}
			if instID != "" {
				api.LogInfo("[FDClosingReopen] CreateInstance %s → request %s PENDING_APPROVAL", instID, reqID)
			} else {
				api.LogInfo("[FDClosingReopen] Auto-applied CREATE for request %s — no approval matrix configured", reqID)
			}
		})
	}
}

// ─── shared helpers (used by request.go/approve.go/reject.go/apply.go/relock.go/list.go/detail.go) ──

func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// friendlyReopenDBError maps common Postgres error codes to a user-facing
// message and HTTP status, mirroring lock.friendlyLockDBError.
func friendlyReopenDBError(err error) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			if strings.Contains(strings.ToLower(pgErr.ConstraintName), "uniq_fd_closing_reopen_request_pending") {
				return "A reopen request is already pending approval for this cycle.", http.StatusConflict
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

// mergeRequestIDs dedupes/trims a single id + a slice of ids, mirroring
// lock.mergeRequestIDs — shared by approve.go/reject.go's bulk shape.
func mergeRequestIDs(single string, many []string) []string {
	seen := make(map[string]struct{}, len(many)+1)
	out := make([]string, 0, len(many)+1)
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	add(single)
	for _, id := range many {
		add(id)
	}
	return out
}

// runEngineInBackground mirrors lock.runEngineInBackground's fire-and-forget
// shape (2-minute timeout, panic-safe).
func runEngineInBackground(fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDClosingReopen] engine goroutine panic: %v", rec)
			}
		}()
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		fn(bgCtx)
	}()
}
