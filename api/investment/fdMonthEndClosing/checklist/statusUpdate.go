// Package checklist implements the fd_closing_checklist_item handlers —
// Section 3 of database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md.
//
// Status EDIT is STAGE-THEN-APPLY (CLAUDE.md preferred): master untouched while
// PENDING_EDIT_APPROVAL; approve copies new_* onto the master; reject flips
// audit only. DELETE is the same with PENDING_DELETE_APPROVAL → soft-delete +
// reseed NOT_STARTED on approve.
package checklist

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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var checklistItemStatuses = map[string]bool{
	"NOT_STARTED": true, "IN_PROGRESS": true, "COMPLETED": true, "BLOCKED": true,
}

var checklistEvidenceTypes = map[string]bool{
	"REPORT": true, "RUN_ID": true, "RECONCILIATION_BATCH": true,
}

var accrualFinalRunSteps = map[string]bool{
	"ACCRUAL_RUN_COMPLETED": true, "ACCRUAL_RUN_APPROVED": true,
}

// UpdateChecklistItem handles POST /investment/fd-closing/checklist/update.
// Stages an EDIT on the audit row (PENDING_EDIT_APPROVAL). Does not mutate the
// master checklist item until approve (or no-matrix auto-apply).
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

		var cycleID, entityID, cycleStatus, oldStatus, stepCode string
		var oldBlockedComment, oldEvidenceRef, oldEvidenceType *string
		var oldExceptionCount int
		var isDeleted bool
		err = tx.QueryRow(ctx, `
			SELECT i.cycle_id, c.entity_id, c.status, i.status, i.blocked_comment, i.exception_count,
			       i.evidence_ref, i.evidence_type, i.is_deleted, i.step_code
			FROM investment.fd_closing_checklist_item i
			JOIN investment.fd_closing_cycle c ON c.cycle_id = i.cycle_id
			WHERE i.item_id = $1
			FOR UPDATE OF i`,
			req.ItemID,
		).Scan(&cycleID, &entityID, &cycleStatus, &oldStatus, &oldBlockedComment, &oldExceptionCount,
			&oldEvidenceRef, &oldEvidenceType, &isDeleted, &stepCode)
		if err != nil {
			fdclosingcommon.RespondError(w, http.StatusNotFound, "Checklist item not found")
			return
		}
		if isDeleted {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "Checklist item is deleted; use the recreated step")
			return
		}

		scope := ctxutil.FromContext(ctx)
		if !scope.HasEntityAccess(entityID) {
			fdclosingcommon.RespondError(w, http.StatusForbidden,
				"Entity ID '"+entityID+"' is not within your authorized access scope.")
			return
		}

		if cycleStatus == "LOCKED" || cycleStatus == "CLOSED" {
			fdclosingcommon.RespondError(w, http.StatusBadRequest, "Cannot update a checklist item on a "+strings.ToLower(cycleStatus)+" cycle")
			return
		}

		exceptionCount := oldExceptionCount
		if req.ExceptionCount != nil {
			exceptionCount = *req.ExceptionCount
		}
		evidenceRef := oldEvidenceRef
		if req.EvidenceRef != nil {
			evidenceRef = nullableTrimPtr(req.EvidenceRef)
		}
		if req.EvidenceType == nil {
			evidenceType = oldEvidenceType
		}
		blockedComment := oldBlockedComment
		if req.BlockedComment != nil {
			blockedComment = nullableTrimPtr(req.BlockedComment)
		}

		if req.Status == "COMPLETED" && accrualFinalRunSteps[stepCode] {
			if evidenceRef == nil {
				fdclosingcommon.RespondError(w, http.StatusBadRequest,
					"Step "+stepCode+" requires a FINAL accrual run reference before it can be completed")
				return
			}
			var runMode string
			if err = tx.QueryRow(ctx, `
				SELECT COALESCE(run_mode,'')
				FROM investment.fd_accrual_run
				WHERE run_id = $1 AND COALESCE(is_deleted,false) = false`,
				*evidenceRef,
			).Scan(&runMode); err != nil {
				fdclosingcommon.RespondError(w, http.StatusBadRequest,
					"Accrual run '"+*evidenceRef+"' not found; step "+stepCode+" requires a FINAL accrual run reference")
				return
			}
			if strings.ToUpper(runMode) != "FINAL" {
				fdclosingcommon.RespondError(w, http.StatusBadRequest,
					"Cannot complete "+stepCode+" — accrual run "+*evidenceRef+" has run mode "+runMode+"; closing requires FINAL")
				return
			}
		}

		// Supersede any earlier pending EDIT/DELETE for this item.
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_checklist_item_audit
			SET processing_status = 'REJECTED', checker_by = $2, checker_at = now(),
			    checker_comment = 'Superseded by new request'
			WHERE item_id = $1 AND processing_status IN ('PENDING_EDIT_APPROVAL','PENDING_DELETE_APPROVAL')`,
			req.ItemID, api.SystemIfBlank(actor.Email),
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] UpdateChecklistItem supersede: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, "Failed to supersede prior pending request")
			return
		}

		if _, err = tx.Exec(ctx, `
			INSERT INTO investment.fd_closing_checklist_item_audit (
				item_id, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
				old_status, old_blocked_comment, old_exception_count,
				old_evidence_ref, old_evidence_type,
				new_status, new_blocked_comment, new_exception_count,
				new_evidence_ref, new_evidence_type
			) VALUES (
				$1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,
				$5,$6,$7,
				$8,$9,
				$10,$11,$12,
				$13,$14
			)`,
			req.ItemID,
			nullIfEmpty(req.Reason),
			api.SystemIfBlank(actor.Email),
			api.SystemIfBlank(api.ClientIPFromContext(ctx)),
			oldStatus, oldBlockedComment, oldExceptionCount,
			oldEvidenceRef, oldEvidenceType,
			req.Status, blockedComment, exceptionCount,
			evidenceRef, evidenceType,
		); err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] UpdateChecklistItem audit insert: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrAuditInsertFailed)
			return
		}

		if err = tx.Commit(ctx); err != nil {
			api.LogErrorForResponse(w, "[FDClosingChecklist] UpdateChecklistItem commit: %v", err)
			fdclosingcommon.RespondError(w, http.StatusInternalServerError, constants.ErrCommitFailedCapitalized)
			return
		}

		fdclosingcommon.RespondSuccess(w, "Checklist change submitted for approval", map[string]interface{}{
			"item_id":            req.ItemID,
			"processing_status":  "PENDING_EDIT_APPROVAL",
			"proposed_status":    req.Status,
			"status":             oldStatus, // live master unchanged
		})
		api.LogInfo("[FDClosingChecklist] UpdateChecklistItem staged: item=%s cycle=%s proposed=%s by=%s",
			req.ItemID, cycleID, req.Status, actor.Email)

		itemID, entity, actorEmail, actorUserID := req.ItemID, entityID, actor.Email, actor.UserID
		runEngineInBackground(func(bgCtx context.Context) {
			if err := approvalengine.CancelPendingInstances(bgCtx, pool, moduleCode, itemID, actorEmail); err != nil {
				api.LogError("[FDClosingChecklist] CancelPendingInstances(EDIT) failed for item %s: %v", itemID, err)
				return
			}
			instID, err := approvalengine.CreateInstance(bgCtx, pool, approvalengine.InstanceRequest{
				ModuleCode:          moduleCode,
				EntityCode:          entity,
				TransactionType:     TxEditChecklist,
				RecordID:            itemID,
				RecordTable:         checklistTable,
				AuditTable:          checklistAuditTable,
				AuditIDColumn:       "item_id",
				ActionType:          "EDIT",
				SubmittedBy:         actorUserID,
				SubmittedByEmail:    actorEmail,
				RequirePinnedMatrix: true,
				AutoApplyIfUnpinned: false,
			})
			if err != nil {
				api.LogError("[FDClosingChecklist] CreateInstance(EDIT) failed for item %s: %v", itemID, err)
				return
			}
			if instID != "" {
				return
			}
			// No matrix — apply staged edit directly.
			tx2, err := pool.Begin(bgCtx)
			if err != nil {
				api.LogError("[FDClosingChecklist] no-matrix EDIT begin tx failed for item %s: %v", itemID, err)
				return
			}
			defer tx2.Rollback(bgCtx) //nolint:errcheck
			if err := ApplyEditToMaster(bgCtx, tx2, itemID, api.SystemIfBlank(actorEmail), "Auto-applied (no approval matrix)", "PENDING_EDIT_APPROVAL", true); err != nil {
				api.LogError("[FDClosingChecklist] no-matrix EDIT apply failed for item %s: %v", itemID, err)
				return
			}
			if err := tx2.Commit(bgCtx); err != nil {
				api.LogError("[FDClosingChecklist] no-matrix EDIT commit failed for item %s: %v", itemID, err)
			}
		})
	}
}

// recomputeCycleReadiness recomputes fd_closing_cycle readiness from active
// (non-deleted) checklist items.
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
				i.cycle_id,
				COUNT(*) AS total_count,
				COUNT(*) FILTER (WHERE i.status = 'COMPLETED') AS completed_count,
				COUNT(*) FILTER (WHERE i.status = 'BLOCKED') AS blocker_count,
				CASE WHEN COUNT(*) = 0 THEN 0
				     ELSE ROUND(COUNT(*) FILTER (WHERE i.status = 'COMPLETED') * 100.0 / COUNT(*), 2)
				END AS readiness_score,
				COUNT(*) FILTER (WHERE i.is_critical = true AND i.status <> 'COMPLETED') AS critical_incomplete
			FROM investment.fd_closing_checklist_item i
			JOIN investment.fd_closing_cycle_fd_scope s
			  ON s.scope_id = i.scope_id AND s.is_deleted = false
			WHERE i.cycle_id = $1 AND i.is_deleted = false
			GROUP BY i.cycle_id
		) agg
		WHERE c.cycle_id = $1 AND c.cycle_id = agg.cycle_id`,
		cycleID,
	)
	return err
}

func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func nullableTrim(s string) *string {
	t := strings.TrimSpace(s)
	if t == "" {
		return nil
	}
	return &t
}

func nullableTrimPtr(s *string) *string {
	if s == nil {
		return nil
	}
	return nullableTrim(*s)
}
