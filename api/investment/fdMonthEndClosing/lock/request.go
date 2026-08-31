package lock

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

// moduleCode is the approval-engine module every fd_closing_lock_request
// instance is submitted under — the same shared FD approval-matrix module
// used by every other investment/fd* workbench (matches cycle.moduleCode).
const moduleCode = "FIXED_DEPOSIT"

// TxLockRequest is registered in api/approvalengine/moduleconfig.go's
// txTypeRegistry with AuditTable=investment.fd_closing_lock_request (the
// request row IS its own audit trail — see the migration's design comment)
// and AuditIDColumn=request_id.
const TxLockRequest = "FD_CLOSING_LOCK"

const lockRequestTable = "investment.fd_closing_lock_request"

// RequestLock handles POST /investment/fd-closing/lock/request. Inserts the
// request row directly as PENDING_APPROVAL — the migration's partial unique
// index (uniq_fd_closing_lock_request_pending) already prevents two
// concurrent pending requests per cycle, so a duplicate insert is left to
// surface as a normal DB constraint-violation error mapped to a 409 rather
// than pre-checking-then-inserting (race-prone), per the handler spec.
func RequestLock(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID           string `json:"cycle_id"`
			LockType          string `json:"lock_type"`
			LockEffectiveDate string `json:"lock_effective_date"`
			Remarks           string `json:"remarks"`
			ApproverID        string `json:"approver_id"`
			ApproverName      string `json:"approver_name"`
			ApproverRole      string `json:"approver_role"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.CycleID = strings.TrimSpace(req.CycleID)
		req.LockType = strings.ToUpper(strings.TrimSpace(req.LockType))
		req.LockEffectiveDate = strings.TrimSpace(req.LockEffectiveDate)
		req.ApproverID = strings.TrimSpace(req.ApproverID)
		req.ApproverName = strings.TrimSpace(req.ApproverName)
		if req.CycleID == "" || req.LockType == "" || req.LockEffectiveDate == "" || req.ApproverID == "" || req.ApproverName == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"cycle_id, lock_type, lock_effective_date, approver_id and approver_name are required")
			return
		}
		if req.LockType != "SOFT_LOCK" && req.LockType != "HARD_LOCK" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "lock_type must be SOFT_LOCK or HARD_LOCK")
			return
		}

		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var entityID, cycleStatus, eligibility string
		var isDeleted bool
		err := pool.QueryRow(ctx, `
			SELECT entity_id, status, eligibility, is_deleted
			FROM investment.fd_closing_cycle
			WHERE cycle_id = $1`,
			req.CycleID,
		).Scan(&entityID, &cycleStatus, &eligibility, &isDeleted)
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

		// Handoff-point gate per the handler spec's Section 7.2: a lock can only
		// be requested once the checklist module (sibling package) has pushed
		// the cycle's cached eligibility to READY_TO_CLOSE. Read-only here —
		// eligibility is refreshed by checklist/list.go and checklist/statusUpdate.go,
		// not by this package. Mirrors fdBookingWorkbench's
		// enforceFDBookingPolicyInline pre-check shape.
		if eligibility != "READY_TO_CLOSE" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"Cycle is not ready to close (eligibility="+eligibility+"); complete the closing checklist first")
			return
		}
		// Only an in-progress cycle may have a lock requested — once a lock has
		// already been applied, cycle.status has moved past IN_PROGRESS
		// (AWAITING_APPROVAL/CLOSED) and a fresh lock request would be
		// meaningless; Reopen is the correct path from there.
		if cycleStatus != "IN_PROGRESS" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"Only a cycle in IN_PROGRESS status can have a lock requested (current status: "+cycleStatus+")")
			return
		}

		var requestID string
		err = pool.QueryRow(ctx, `
			INSERT INTO investment.fd_closing_lock_request (
				cycle_id, lock_type, lock_effective_date, remarks,
				approver_id, approver_name, approver_role,
				processing_status, requested_by, requested_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,'PENDING_APPROVAL',$8,now())
			RETURNING request_id`,
			req.CycleID, req.LockType, req.LockEffectiveDate, nullIfEmpty(req.Remarks),
			req.ApproverID, req.ApproverName, nullIfEmpty(req.ApproverRole),
			api.SystemIfBlank(actor.Email),
		).Scan(&requestID)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingLock] RequestLock insert: %v", err)
			msg, status := friendlyLockDBError(err)
			fdclosingcommon.RespondError(w, status, msg)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Lock request submitted for approval", map[string]interface{}{
			"request_id": requestID,
			"cycle_id":   req.CycleID,
			"status":     "PENDING_APPROVAL",
		})
		api.LogInfo("[FDClosingLock] Lock requested: request=%s cycle=%s by=%s", requestID, req.CycleID, actor.Email)

		reqID, entity, actorEmail, actorUserID := requestID, entityID, actor.Email, actor.UserID
		runEngineInBackground(func(bgCtx context.Context) {
			instID, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:          moduleCode,
				EntityCode:          entity,
				TransactionType:     TxLockRequest,
				RecordID:            reqID,
				RecordTable:         lockRequestTable,
				AuditTable:          lockRequestTable,
				AuditIDColumn:       "request_id",
				ActionType:          "CREATE",
				SubmittedBy:         actorUserID,
				SubmittedByEmail:    actorEmail,
				RequirePinnedMatrix: true,
				// No auto-apply, matrix or not — every lock request waits for an
				// explicit /approve call (unpinned falls to approve.go's own
				// directApproveLockRequest fallback).
				AutoApplyIfUnpinned: false,
			})
			if err != nil {
				api.LogError("[FDClosingLock] CreateInstance failed for request %s: %v", reqID, err)
				return
			}
			if instID != "" {
				api.LogInfo("[FDClosingLock] CreateInstance %s → request %s PENDING_APPROVAL", instID, reqID)
			}
		})
	}
}

// ─── shared helpers (used by request.go/approve.go/reject.go/apply.go/list.go/detail.go) ──

func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// friendlyLockDBError maps common Postgres error codes to a user-facing
// message and HTTP status, mirroring cycle.friendlyDBError.
func friendlyLockDBError(err error) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			if strings.Contains(strings.ToLower(pgErr.ConstraintName), "uniq_fd_closing_lock_request_pending") {
				return "A lock request is already pending approval for this cycle.", http.StatusConflict
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
// cycle.mergeCycleIDs — shared by approve.go/reject.go's bulk shape.
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

// runEngineInBackground mirrors cycle.runEngineInBackground's fire-and-forget
// shape (2-minute timeout, panic-safe) used after the caller's own
// transaction/statement has already committed.
func runEngineInBackground(fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDClosingLock] engine goroutine panic: %v", rec)
			}
		}()
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		fn(bgCtx)
	}()
}
