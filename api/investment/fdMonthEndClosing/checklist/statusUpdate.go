// Package checklist implements the fd_closing_checklist_item handlers —
// Section 3 of database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md.
// Rows already exist by the time any handler here runs (created by the
// scope-approval post-finalize hook in the sibling scope package) — this
// package only ever does status UPDATE, LIST, DETAIL, AUDIT and the file
// sub-handlers, never CREATE/DELETE.
package checklist

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	fdclosingcommon "CimplrCorpSaas/api/investment/fdMonthEndClosing/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var checklistItemStatuses = map[string]bool{
	"NOT_STARTED": true, "IN_PROGRESS": true, "COMPLETED": true, "BLOCKED": true,
}

var checklistEvidenceTypes = map[string]bool{
	"REPORT": true, "RUN_ID": true, "RECONCILIATION_BATCH": true,
}

// UpdateChecklistItem handles POST /investment/fd-closing/checklist/update.
//
// Apply-immediately, self-approved — there is no checker step for a checklist
// status flip (matches the mock UI's handleStatusChange, and the migration's
// own design comment). old_status/old_blocked_comment/old_exception_count are
// captured from the row read via SELECT ... FOR UPDATE, the update is applied
// directly, and the audit row is written already APPROVED (requested_by ==
// checker_by == the acting user) in the same transaction — no
// approvalengine.CreateInstance call at all, unlike every
// fd_closing_cycle/*_scope handler in the sibling packages. After the status
// update commits its own values, recomputeCycleReadiness refreshes the parent
// fd_closing_cycle's readiness_score/blocker_count/eligibility so the "N steps
// x N FDs" grid and the Lock Request eligibility gate both stay in sync.
func UpdateChecklistItem(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ItemID         string  `json:"item_id"`
			Status         string  `json:"status"`
			EvidenceRef    *string `json:"evidence_ref"`
			EvidenceType   *string `json:"evidence_type"`
			ExceptionCount *int    `json:"exception_count"`
			BlockedComment *string `json:"blocked_comment"`
			Reason         string  `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		req.ItemID = strings.TrimSpace(req.ItemID)
		req.Status = strings.ToUpper(strings.TrimSpace(req.Status))
		if req.ItemID == "" || req.Status == "" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "item_id and status are required")
			return
		}
		if !checklistItemStatuses[req.Status] {
			fdclosingcommon.RespondError(w, http.StatusBadRequest,
				"status must be one of NOT_STARTED, IN_PROGRESS, COMPLETED, BLOCKED")
			return
		}
		var evidenceType *string
		if req.EvidenceType != nil {
			trimmed := strings.ToUpper(strings.TrimSpace(*req.EvidenceType))
			if trimmed != "" {
				if !checklistEvidenceTypes[trimmed] {
					fdclosingcommon.RespondError(w, http.StatusBadRequest,
						"evidence_type must be one of REPORT, RUN_ID, RECONCILIATION_BATCH")
					return
				}
				evidenceType = &trimmed
			}
		}

		actor, ok := fdclosingcommon.ActorFromRequest(r)
		if !ok {
			fdclosingcommon.RespondError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pool.Begin(ctx)
		if err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] UpdateChecklistItem begin tx: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrTransactionFailed)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var cycleID, entityID, cycleStatus, oldStatus string
		var oldBlockedComment, oldEvidenceRef, oldEvidenceType *string
		var oldExceptionCount int
		err = tx.QueryRow(ctx, `
			SELECT i.cycle_id, c.entity_id, c.status, i.status, i.blocked_comment, i.exception_count,
			       i.evidence_ref, i.evidence_type
			FROM investment.fd_closing_checklist_item i
			JOIN investment.fd_closing_cycle c ON c.cycle_id = i.cycle_id
			WHERE i.item_id = $1
			FOR UPDATE OF i`,
			req.ItemID,
		).Scan(&cycleID, &entityID, &cycleStatus, &oldStatus, &oldBlockedComment, &oldExceptionCount,
			&oldEvidenceRef, &oldEvidenceType)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Checklist item not found")
			return
		}

		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(entityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+entityID+"' is not within your authorized access scope.")
			return
		}

		// Immutability guard: once the cycle is LOCKED or CLOSED there is
		// nothing left to check off (mirrors cycle/update.go's own defensive
		// guard and the handler spec's Section 7 points 3/4 — REOPENED
		// explicitly re-enables this handler again since it is not in the
		// blocked set below).
		if cycleStatus == "LOCKED" || cycleStatus == "CLOSED" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "Cannot update a checklist item on a "+strings.ToLower(cycleStatus)+" cycle")
			return
		}

		// Partial update: a field omitted from the request (nil pointer) keeps
		// its current value rather than being wiped to NULL/0. This matters
		// because multiple independent screens (closingChecklistDashboard,
		// accrualCompletionApproval, receiptReconciliationSummary,
		// closingAccountingConsolidation) all call this same endpoint, each
		// typically only caring about its own subset of fields — a full
		// overwrite would let one screen's call silently erase another
		// screen's previously-set evidence_ref/exception_count/blocked_comment
		// on the same item.
		exceptionCount := oldExceptionCount
		if req.ExceptionCount != nil {
			exceptionCount = *req.ExceptionCount
		}
		evidenceRef := oldEvidenceRef
		if req.EvidenceRef != nil {
			evidenceRef = nullableTrimPtr(req.EvidenceRef)
		}
		// evidenceType was already validated/normalized above from
		// req.EvidenceType when that field was sent; only fall back to the
		// existing value when the caller omitted it entirely.
		if req.EvidenceType == nil {
			evidenceType = oldEvidenceType
		}
		blockedComment := oldBlockedComment
		if req.BlockedComment != nil {
			blockedComment = nullableTrimPtr(req.BlockedComment)
		}

		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_checklist_item
			SET status = $1, evidence_ref = $2, evidence_type = $3, exception_count = $4,
			    blocked_comment = $5, last_updated_by = $6, last_updated_at = now()
			WHERE item_id = $7`,
			req.Status, evidenceRef, evidenceType, exceptionCount,
			blockedComment, actor.Email, req.ItemID,
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] UpdateChecklistItem update: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to update checklist item")
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_checklist_item_audit (
				item_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
				checker_by, checker_at,
				old_status, old_blocked_comment, old_exception_count
			) VALUES ($1,'EDIT','APPROVED',$2,$3,now(),$4,$3,now(),$5,$6,$7)`,
			req.ItemID, nullIfEmpty(req.Reason), api.SystemIfBlank(actor.Email), api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldStatus, oldBlockedComment, oldExceptionCount,
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] UpdateChecklistItem audit insert: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed)
			return
		}

		if err = recomputeCycleReadiness(ctx, tx, cycleID); err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] UpdateChecklistItem recompute readiness: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to recompute cycle readiness")
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] UpdateChecklistItem commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Checklist item updated", map[string]interface{}{
			"item_id": req.ItemID,
			"status":  req.Status,
		})
		api.LogInfo("[FDClosingChecklist] UpdateChecklistItem: item=%s cycle=%s status=%s by=%s",
			req.ItemID, cycleID, req.Status, actor.Email)
	}
}

// recomputeCycleReadiness recomputes fd_closing_cycle.readiness_score/
// blocker_count/eligibility from every checklist item currently attached to
// cycleID, and stamps readiness_checked_at. Pulled out as a standalone,
// reusable package-level helper (rather than being inlined into
// UpdateChecklistItem) per the handler spec's explicit instruction, so any
// future caller — e.g. a bulk re-check job — can call it the same way instead
// of duplicating the aggregate SQL. Must run inside the same transaction as
// the status update it follows.
//
// eligibility rule (handler spec Section 3):
//   - READY_TO_CLOSE        — every item for the cycle is COMPLETED
//   - CONDITIONALLY_READY   — not all items are COMPLETED, but every
//     is_critical=true item is (some non-critical items may still be
//     incomplete)
//   - NOT_READY             — otherwise (including zero items, defensively)
func recomputeCycleReadiness(ctx context.Context, tx pgx.Tx, cycleID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE investment.fd_closing_cycle c
		SET readiness_score      = agg.readiness_score,
		    blocker_count        = agg.blocker_count,
		    eligibility          = CASE
		        WHEN agg.total_count = 0 THEN 'NOT_READY'
		        WHEN agg.completed_count = agg.total_count THEN 'READY_TO_CLOSE'
		        WHEN agg.critical_incomplete = 0 THEN 'CONDITIONALLY_READY'
		        ELSE 'NOT_READY'
		    END,
		    readiness_checked_at = now()
		FROM (
			SELECT
				cycle_id,
				COUNT(*) AS total_count,
				COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_count,
				COUNT(*) FILTER (WHERE status = 'BLOCKED') AS blocker_count,
				CASE WHEN COUNT(*) = 0 THEN 0
				     ELSE ROUND(COUNT(*) FILTER (WHERE status = 'COMPLETED') * 100.0 / COUNT(*), 2)
				END AS readiness_score,
				COUNT(*) FILTER (WHERE is_critical = true AND status <> 'COMPLETED') AS critical_incomplete
			FROM investment.fd_closing_checklist_item
			WHERE cycle_id = $1
			GROUP BY cycle_id
		) agg
		WHERE c.cycle_id = $1 AND c.cycle_id = agg.cycle_id`,
		cycleID,
	)
	return err
}

// nullIfEmpty returns nil for a blank string so optional text columns are
// stored as SQL NULL rather than "" (mirrors cycle/create.go's helper of the
// same name — package-local by design, not shared, matching that package's
// own precedent).
func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// nullableTrim trims s and returns nil for an empty result (mirrors
// cycle/update.go's nullableTrim).
func nullableTrim(s string) *string {
	t := strings.TrimSpace(s)
	if t == "" {
		return nil
	}
	return &t
}

// nullableTrimPtr applies nullableTrim through an optional *string request
// field, so an omitted field stays nil and an explicit "" clears the column.
func nullableTrimPtr(s *string) *string {
	if s == nil {
		return nil
	}
	return nullableTrim(*s)
}
