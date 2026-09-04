// Package scope implements the fd_closing_cycle_fd_scope handlers — Section 2
// of database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md.
// Scope ADD is applied immediately with checklist seed (same tx as create).
// Scope REMOVE remains maker-checker. Every handler follows the same conventions
// as ../cycle (pgxpool, ctx := r.Context(), api.LogErrorForResponse,
// RespondEnvelope* via fdclosingcommon — never legacy RespondWith*).
package scope

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

// moduleCode is the approval-engine module every fd_closing_cycle_fd_scope
// instance is submitted under — same shared FD approval-matrix module used by
// ../cycle and every other investment/fd* workbench.
const moduleCode = "FIXED_DEPOSIT"

// Transaction types this package needs registered in
// api/approvalengine/moduleconfig.go's txTypeRegistry (AuditTable=
// investment.fd_closing_cycle_fd_scope_audit, AuditIDColumn=scope_id) — see
// the report-back in this change's PR description; NOT registered by this
// package itself (moduleconfig.go is shared wiring, edited by a human after
// this change lands, per the task's isolation constraint).
const (
	TxScopeAdd    = "FD_CLOSING_SCOPE_ADD"
	TxScopeRemove = "FD_CLOSING_SCOPE_REMOVE"
)

const (
	scopeTable      = "investment.fd_closing_cycle_fd_scope"
	scopeAuditTable = "investment.fd_closing_cycle_fd_scope_audit"
)

// checklistStep is one of the 5 fixed rows seeded into
// investment.fd_closing_checklist_item once a scope Add is approved — copied
// verbatim from the handler spec's Section 2 approve behavior.
type checklistStep struct {
	StepCode      string
	StepName      string
	OwnerRole     string
	Sequence      int
	IsCritical    bool
	DependsOnStep *string
}

func strPtr(s string) *string { return &s }

// checklistSteps is the fixed 5-row template. Order matches the spec exactly.
var checklistSteps = []checklistStep{
	{StepCode: "ACCRUAL_RUN_COMPLETED", StepName: "Accrual Run Completed", OwnerRole: "TREASURY", Sequence: 1, IsCritical: true, DependsOnStep: nil},
	{StepCode: "ACCRUAL_RUN_APPROVED", StepName: "Accrual Run Approved", OwnerRole: "FINANCE", Sequence: 2, IsCritical: true, DependsOnStep: strPtr("ACCRUAL_RUN_COMPLETED")},
	{StepCode: "RECEIPTS_CAPTURED", StepName: "Interest Receipts Captured", OwnerRole: "BACK_OFFICE", Sequence: 3, IsCritical: true, DependsOnStep: strPtr("ACCRUAL_RUN_APPROVED")},
	{StepCode: "RECEIPTS_RECONCILED", StepName: "Receipts Reconciled", OwnerRole: "FINANCE", Sequence: 4, IsCritical: true, DependsOnStep: strPtr("RECEIPTS_CAPTURED")},
	{StepCode: "TDS_VALIDATED", StepName: "TDS Validated", OwnerRole: "FINANCE", Sequence: 5, IsCritical: false, DependsOnStep: strPtr("RECEIPTS_CAPTURED")},
}

// CreateScope handles both POST /investment/fd-closing/scope/create (single
// fd_id) and POST /investment/fd-closing/scope/bulk-create (fd_ids).
//
// Scope ADD is applied immediately with the cycle/checklist create flow
// (no separate maker-checker for ADD): inserts APPROVED scope rows, APPROVED
// audit rows, and seeds the 5 checklist items in the same transaction.
// REMOVE remains maker-checker via DeleteScope.
func CreateScope(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CycleID string   `json:"cycle_id"`
			FdID    string   `json:"fd_id"`
			FdIDs   []string `json:"fd_ids"`
			Reason  string   `json:"reason"`
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
		fdIDs := mergeIDs(req.FdID, req.FdIDs)
		if len(fdIDs) == 0 {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "fd_id or fd_ids is required")
			return
		}

		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var cycleEntityID, cycleStatus string
		if err := pool.QueryRow(ctx, `
			SELECT entity_id, status FROM investment.fd_closing_cycle
			WHERE cycle_id = $1 AND is_deleted = false`,
			req.CycleID,
		).Scan(&cycleEntityID, &cycleStatus); err != nil {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Cycle not found")
			return
		}

		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(cycleEntityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+cycleEntityID+"' is not within your authorized access scope.")
			return
		}

		if cycleStatus == "CLOSED" || cycleStatus == "LOCKED" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "Cannot add FDs to a "+strings.ToLower(cycleStatus)+" cycle")
			return
		}

		alreadyInScope := map[string]struct{}{}
		{
			rows, err := pool.Query(ctx, `
				SELECT fd_id FROM investment.fd_closing_cycle_fd_scope
				WHERE cycle_id = $1 AND fd_id = ANY($2::text[]) AND is_deleted = false`,
				req.CycleID, fdIDs,
			)
			if err != nil {
				api.LogErrorForResponse(w, "[FDClosingScope] CreateScope existing-scope query: %v", err)
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
				return
			}
			for rows.Next() {
				var fdID string
				if err := rows.Scan(&fdID); err != nil {
					rows.Close()
					api.LogErrorForResponse(w, "[FDClosingScope] CreateScope existing-scope scan: %v", err)
					fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
					return
				}
				alreadyInScope[fdID] = struct{}{}
			}
			rows.Close()
		}

		type fdInfo struct{ entityID, status string }
		fdMeta := map[string]fdInfo{}
		{
			rows, err := pool.Query(ctx, `
				SELECT fd_id, entity_id, COALESCE(fd_status,'') FROM investment.fd_master
				WHERE fd_id = ANY($1::text[]) AND COALESCE(is_deleted,false) = false`,
				fdIDs,
			)
			if err != nil {
				api.LogErrorForResponse(w, "[FDClosingScope] CreateScope fd_master query: %v", err)
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrQueryFailed)
				return
			}
			for rows.Next() {
				var fdID, entityID, status string
				if err := rows.Scan(&fdID, &entityID, &status); err != nil {
					rows.Close()
					api.LogErrorForResponse(w, "[FDClosingScope] CreateScope fd_master scan: %v", err)
					fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrRowError)
					return
				}
				fdMeta[fdID] = fdInfo{entityID: entityID, status: status}
			}
			rows.Close()
		}

		type eligibleFD struct{ fdID string }
		var eligible []eligibleFD
		var errs []string
		for _, fdID := range fdIDs {
			if _, dup := alreadyInScope[fdID]; dup {
				errs = append(errs, fdID+": already in scope for this cycle")
				continue
			}
			meta, found := fdMeta[fdID]
			if !found {
				errs = append(errs, fdID+": FD not found or inactive")
				continue
			}
			if meta.entityID != cycleEntityID {
				errs = append(errs, fdID+": FD entity does not match cycle entity")
				continue
			}
			if err := validateEligibleStatus(meta.status); err != nil {
				errs = append(errs, fdID+": "+err.Error())
				continue
			}
			eligible = append(eligible, eligibleFD{fdID: fdID})
		}

		if len(eligible) == 0 {
			fdclosingcommon.RespondFailureWithData(w, http.StatusUnprocessableEntity,
				"No eligible FDs to add", map[string]interface{}{"errors": errs})
			return
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] CreateScope begin tx: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		type createdScope struct{ scopeID, fdID string }
		created := make([]createdScope, 0, len(eligible))
		actorEmail := api.SystemIfBlank(actor.Email)
		actorIP := api.SystemIfBlank(api.ClientIPFromContext(ctx))
		for _, e := range eligible {
			var scopeID string
			err := tx.QueryRow(ctx, `
				INSERT INTO investment.fd_closing_cycle_fd_scope (
					cycle_id, fd_id, selection_status, added_by, approved_by, approved_at
				) VALUES ($1,$2,'APPROVED',$3,$3,now())
				RETURNING scope_id`,
				req.CycleID, e.fdID, actor.Email,
			).Scan(&scopeID)
			if err != nil {
				api.LogErrorForResponse(w, "[FDClosingScope] CreateScope insert scope for fd=%s: %v", e.fdID, err)
				msg, status := friendlyDBError(err)
				fdclosingcommon.RespondError(w, status, msg)
				return
			}
			if _, err = tx.Exec(ctx, `
				INSERT INTO investment.fd_closing_cycle_fd_scope_audit (
					scope_id, action_type, processing_status, reason,
					requested_by, requested_at, requested_ip,
					checker_by, checker_at, checker_comment
				) VALUES ($1,'CREATE','APPROVED',$2,$3,now(),$4,$3,now(),$5)`,
				scopeID, nullIfEmpty(req.Reason), actorEmail, actorIP,
				"Applied with cycle/checklist create (no separate scope ADD approval)",
			); err != nil {
				api.LogErrorForResponse(w, "[FDClosingScope] CreateScope audit insert for scope=%s: %v", scopeID, err)
				fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed)
				return
			}

			for _, step := range checklistSteps {
				if _, err = tx.Exec(ctx, `
					INSERT INTO investment.fd_closing_checklist_item (
						cycle_id, fd_id, scope_id, step_code, step_name, owner_role,
						sequence, is_critical, depends_on_step_code, status, created_at
					) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'NOT_STARTED',now())
					ON CONFLICT (cycle_id, fd_id, step_code) DO NOTHING`,
					req.CycleID, e.fdID, scopeID, step.StepCode, step.StepName, step.OwnerRole,
					step.Sequence, step.IsCritical, step.DependsOnStep,
				); err != nil {
					api.LogErrorForResponse(w, "[FDClosingScope] CreateScope checklist seed scope=%s step=%s: %v",
						scopeID, step.StepCode, err)
					fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to seed checklist items")
					return
				}
			}

			created = append(created, createdScope{scopeID: scopeID, fdID: e.fdID})
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle
			SET status = 'IN_PROGRESS'
			WHERE cycle_id = $1
			  AND status = 'DRAFT'
			  AND EXISTS (
				SELECT 1 FROM investment.fd_closing_cycle_audit ca
				WHERE ca.cycle_id = $1
				  AND ca.action_type = 'CREATE'
				  AND ca.processing_status = 'APPROVED'
			  )`,
			req.CycleID,
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] CreateScope cycle status transition: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to update cycle status")
			return
		}

		if err := fdclosingcommon.RefreshCycleFdCount(ctx, tx, req.CycleID); err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] CreateScope fd_count refresh: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to refresh FD count")
			return
		}
		if err := fdclosingcommon.RefreshCycleReadiness(ctx, tx, req.CycleID); err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] CreateScope readiness refresh: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to refresh readiness")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingScope] CreateScope commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		results := make([]map[string]interface{}, 0, len(created)+len(errs))
		for _, c := range created {
			results = append(results, map[string]interface{}{
				"success": true, "fd_id": c.fdID, "scope_id": c.scopeID, "status": "APPROVED",
			})
		}
		for _, e := range errs {
			results = append(results, map[string]interface{}{"success": false, "error": e})
		}
		fdclosingcommon.RespondSuccess(w, "FD(s) added to scope; checklist seeded", map[string]interface{}{
			"cycle_id": req.CycleID, "results": results,
		})
		api.LogInfo("[FDClosingScope] CreateScope (immediate): cycle=%s created=%d errors=%d by=%s",
			req.CycleID, len(created), len(errs), actor.Email)
	}
}

// ─── shared helpers (used by create.go/delete.go/approve.go/reject.go) ──────

// applyScopeAddApproval is the one place that actually "applies" an approved
// scope-Add request: flips selection_status to APPROVED on the scope master
// row and — in the SAME transaction — seeds the 5 fixed fd_closing_checklist_item
// rows for this (cycle_id, fd_id, scope_id), per the handler spec's Section 2
// approve behavior. ON CONFLICT (cycle_id, fd_id, step_code) DO NOTHING makes
// this idempotent in case the post-finalize hook and a direct fallback path
// both fire for the same scope (should not happen, but cheap to guard).
//
// Exported-shape (lowercase, package-private — only approve.go, create.go's
// auto-apply fallback, and approvalHooks.go's post-finalize hook call this)
// mirrors cycle.ApplyEditToMaster's three-call-site shape:
//   - create.go's own auto-apply fallback, when no approval matrix applies at
//     all (matchStatus="PENDING_APPROVAL", flipAuditStatus=true — nothing else
//     will ever flip this row, so this call must do it itself);
//   - approve.go's direct/no-engine-instance fallback (same as above); and
//   - approvalHooks.go's post-finalize hook for the engine-mediated path
//     (matchStatus="APPROVED", flipAuditStatus=false — finalizeRecord already
//     flipped processing_status inside the engine's own transaction before the
//     hook runs, so this call only needs to flip selection_status + seed rows).
func applyScopeAddApproval(ctx context.Context, tx pgx.Tx, scopeID, checkerEmail, checkerComment, matchStatus string, flipAuditStatus bool) error {
	var cycleID, fdID string
	if err := tx.QueryRow(ctx, `
		SELECT cycle_id, fd_id FROM investment.fd_closing_cycle_fd_scope
		WHERE scope_id = $1
		FOR UPDATE`,
		scopeID,
	).Scan(&cycleID, &fdID); err != nil {
		return fmt.Errorf("applyScopeAddApproval: scope %s not found: %w", scopeID, err)
	}

	var auditID string
	if err := tx.QueryRow(ctx, `
		SELECT audit_id FROM investment.fd_closing_cycle_fd_scope_audit
		WHERE scope_id = $1 AND action_type = 'CREATE' AND processing_status = $2
		ORDER BY requested_at DESC
		LIMIT 1
		FOR UPDATE`,
		scopeID, matchStatus,
	).Scan(&auditID); err != nil {
		return fmt.Errorf("applyScopeAddApproval: no %s CREATE audit row for scope %s: %w", matchStatus, scopeID, err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle_fd_scope
		SET selection_status = 'APPROVED', approved_by = $2, approved_at = now()
		WHERE scope_id = $1`,
		scopeID, checkerEmail,
	); err != nil {
		return fmt.Errorf("applyScopeAddApproval master update: %w", err)
	}

	for _, step := range checklistSteps {
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_checklist_item (
				cycle_id, fd_id, scope_id, step_code, step_name, owner_role,
				sequence, is_critical, depends_on_step_code, status, created_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'NOT_STARTED',now())
			ON CONFLICT (cycle_id, fd_id, step_code) DO NOTHING`,
			cycleID, fdID, scopeID, step.StepCode, step.StepName, step.OwnerRole,
			step.Sequence, step.IsCritical, step.DependsOnStep,
		); err != nil {
			return fmt.Errorf("applyScopeAddApproval checklist seed (%s): %w", step.StepCode, err)
		}
	}

	// Move DRAFT → IN_PROGRESS only after CREATE is approved AND first FD is
	// in scope. Do not promote a still-pending CREATE cycle into work screens.
	if _, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle
		SET status = 'IN_PROGRESS'
		WHERE cycle_id = $1
		  AND status = 'DRAFT'
		  AND EXISTS (
			SELECT 1 FROM investment.fd_closing_cycle_audit ca
			WHERE ca.cycle_id = $1
			  AND ca.action_type = 'CREATE'
			  AND ca.processing_status = 'APPROVED'
		  )`,
		cycleID,
	); err != nil {
		return fmt.Errorf("applyScopeAddApproval cycle status transition: %w", err)
	}

	// Keep cached fd_count + readiness in sync (list/detail also compute live
	// counts; this heals the master row for any other reader).
	if err := fdclosingcommon.RefreshCycleFdCount(ctx, tx, cycleID); err != nil {
		return fmt.Errorf("applyScopeAddApproval fd_count refresh: %w", err)
	}
	if err := fdclosingcommon.RefreshCycleReadiness(ctx, tx, cycleID); err != nil {
		return fmt.Errorf("applyScopeAddApproval readiness refresh: %w", err)
	}

	if flipAuditStatus {
		if _, err := tx.Exec(ctx, `
			UPDATE investment.fd_closing_cycle_fd_scope_audit
			SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(), checker_comment = $3
			WHERE audit_id = $1`,
			auditID, checkerEmail, checkerComment,
		); err != nil {
			return fmt.Errorf("applyScopeAddApproval audit flip: %w", err)
		}
	}
	return nil
}

// pendingScopeAction is lookupPendingScopeAction's return shape — mirrors
// cycle's pendingCycleAction exactly.
type pendingScopeAction struct {
	AuditID    string
	ActionType string
}

// lookupPendingScopeAction locks and returns the single most-recent PENDING%
// audit row for scopeID. Callers must be inside an open transaction (this
// runs FOR UPDATE). Returns pgx.ErrNoRows when nothing is pending.
func lookupPendingScopeAction(ctx context.Context, tx pgx.Tx, scopeID string) (pendingScopeAction, error) {
	var p pendingScopeAction
	err := tx.QueryRow(ctx, `
		SELECT audit_id, action_type
		FROM investment.fd_closing_cycle_fd_scope_audit
		WHERE scope_id = $1 AND processing_status LIKE 'PENDING%'
		ORDER BY requested_at DESC
		LIMIT 1
		FOR UPDATE`,
		scopeID,
	).Scan(&p.AuditID, &p.ActionType)
	return p, err
}

// mergeIDs dedupes+trims a single ID and an ID slice into one ordered list —
// same helper shape as cycle's mergeCycleIDs, duplicated locally so scope has
// no compile-time dependency on the cycle package.
func mergeIDs(single string, many []string) []string {
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

func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// friendlyDBError maps common Postgres error codes to a user-facing message
// and HTTP status — mirrors cycle/create.go's helper of the same name,
// adjusted for this table's unique constraint.
func friendlyDBError(err error) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			if strings.Contains(strings.ToLower(pgErr.ConstraintName), "uniq_fd_closing_cycle_fd_scope") {
				return "This FD is already in scope for this cycle.", http.StatusConflict
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

// runEngineInBackground mirrors cycle's helper of the same name (fire-and-
// forget, 2-minute timeout, panic-safe) — duplicated locally per package,
// same reasoning as mergeIDs above.
func runEngineInBackground(fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDClosingScope] engine goroutine panic: %v", rec)
			}
		}()
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		fn(bgCtx)
	}()
}

// scanRowsToMaps converts a pgx.Rows result into []map[string]interface{} —
// same shape as cycle's helper, duplicated locally (small, generic, no
// cross-package dependency worth adding for it).
func scanRowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	fields := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0, 50)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fields))
		for i, f := range fields {
			if vals[i] == nil {
				row[string(f.Name)] = ""
			} else {
				row[string(f.Name)] = vals[i]
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
