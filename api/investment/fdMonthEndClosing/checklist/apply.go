package checklist

import (
	"context"
	"strings"
	"time"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5"
)

// Approval-engine constants for checklist EDIT/DELETE (FIXED_DEPOSIT module).
const (
	moduleCode          = "FIXED_DEPOSIT"
	TxEditChecklist     = "FD_CLOSING_CHECKLIST_EDIT"
	TxDeleteChecklist   = "FD_CLOSING_CHECKLIST_DELETE"
	checklistTable      = "investment.fd_closing_checklist_item"
	checklistAuditTable = "investment.fd_closing_checklist_item_audit"
)

type pendingChecklistAction struct {
	AuditID    string
	ActionType string
}

func lookupPendingChecklistAction(ctx context.Context, tx pgx.Tx, itemID string) (pendingChecklistAction, error) {
	var p pendingChecklistAction
	err := tx.QueryRow(ctx, `
		SELECT audit_id, action_type
		FROM investment.fd_closing_checklist_item_audit
		WHERE item_id = $1 AND processing_status LIKE 'PENDING%'
		ORDER BY requested_at DESC
		LIMIT 1
		FOR UPDATE`,
		itemID,
	).Scan(&p.AuditID, &p.ActionType)
	return p, err
}

// ApplyEditToMaster copies staged new_* from the EDIT audit onto the checklist
// item master. Exported for the post-finalize hook and no-matrix approve path.
func ApplyEditToMaster(ctx context.Context, tx pgx.Tx, itemID, checkerEmail, checkerComment, matchStatus string, flipAuditStatus bool) error {
	var auditID string
	var newStatus string
	var newBlockedComment, newEvidenceRef, newEvidenceType *string
	var newExceptionCount *int
	err := tx.QueryRow(ctx, `
		SELECT audit_id, COALESCE(new_status, ''), new_blocked_comment, new_exception_count,
		       new_evidence_ref, new_evidence_type
		FROM investment.fd_closing_checklist_item_audit
		WHERE item_id = $1 AND action_type = 'EDIT' AND processing_status = $2
		ORDER BY requested_at DESC
		LIMIT 1
		FOR UPDATE`,
		itemID, matchStatus,
	).Scan(&auditID, &newStatus, &newBlockedComment, &newExceptionCount, &newEvidenceRef, &newEvidenceType)
	if err != nil {
		return err
	}

	if newStatus == "" {
		return nil
	}

	if _, err = tx.Exec(ctx, `
		UPDATE investment.fd_closing_checklist_item
		SET status = $2,
		    evidence_ref = $3,
		    evidence_type = $4,
		    exception_count = COALESCE($5, exception_count),
		    blocked_comment = $6,
		    last_updated_by = $7,
		    last_updated_at = now()
		WHERE item_id = $1 AND is_deleted = false`,
		itemID, newStatus, newEvidenceRef, newEvidenceType, newExceptionCount, newBlockedComment, checkerEmail,
	); err != nil {
		return err
	}

	if flipAuditStatus {
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_checklist_item_audit
			SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(), checker_comment = $3
			WHERE audit_id = $1`,
			auditID, checkerEmail, checkerComment,
		); err != nil {
			return err
		}
	}

	var cycleID string
	if err = tx.QueryRow(ctx, `SELECT cycle_id FROM investment.fd_closing_checklist_item WHERE item_id = $1`, itemID).Scan(&cycleID); err != nil {
		return err
	}
	return recomputeCycleReadiness(ctx, tx, cycleID)
}

// ApplyDeleteToMaster soft-deletes the item and reseeds a fresh NOT_STARTED
// row for the same cycle/fd/step so the step can be progressed again.
func ApplyDeleteToMaster(ctx context.Context, tx pgx.Tx, itemID, checkerEmail, checkerComment, matchStatus string, flipAuditStatus bool) error {
	var auditID, cycleID, fdID, scopeID, stepCode, stepName, ownerRole string
	var sequence int
	var isCritical bool
	var dependsOn *string

	err := tx.QueryRow(ctx, `
		SELECT a.audit_id, i.cycle_id, i.fd_id, i.scope_id, i.step_code, i.step_name,
		       i.owner_role, i.sequence, i.is_critical, i.depends_on_step_code
		FROM investment.fd_closing_checklist_item_audit a
		JOIN investment.fd_closing_checklist_item i ON i.item_id = a.item_id
		WHERE a.item_id = $1 AND a.action_type = 'DELETE' AND a.processing_status = $2
		ORDER BY a.requested_at DESC
		LIMIT 1
		FOR UPDATE OF a, i`,
		itemID, matchStatus,
	).Scan(&auditID, &cycleID, &fdID, &scopeID, &stepCode, &stepName, &ownerRole, &sequence, &isCritical, &dependsOn)
	if err != nil {
		return err
	}

	if _, err = tx.Exec(ctx, `
		UPDATE investment.fd_closing_checklist_item
		SET is_deleted = true, last_updated_by = $2, last_updated_at = now()
		WHERE item_id = $1`,
		itemID, checkerEmail,
	); err != nil {
		return err
	}

	if flipAuditStatus {
		if _, err = tx.Exec(ctx, `
			UPDATE investment.fd_closing_checklist_item_audit
			SET processing_status = 'APPROVED', checker_by = $2, checker_at = now(), checker_comment = $3
			WHERE audit_id = $1`,
			auditID, checkerEmail, checkerComment,
		); err != nil {
			return err
		}
	}

	// Reseed fresh NOT_STARTED so the FD keeps a live step row.
	var newItemID string
	err = tx.QueryRow(ctx, `
		INSERT INTO investment.fd_closing_checklist_item (
			cycle_id, fd_id, scope_id, step_code, step_name, owner_role,
			sequence, is_critical, depends_on_step_code, status, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'NOT_STARTED',now())
		RETURNING item_id`,
		cycleID, fdID, scopeID, stepCode, stepName, ownerRole, sequence, isCritical, dependsOn,
	).Scan(&newItemID)
	if err != nil {
		return err
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO investment.fd_closing_checklist_item_audit (
			item_id, action_type, processing_status, reason,
			requested_by, requested_at, requested_ip,
			checker_by, checker_at, checker_comment, new_status
		) VALUES (
			$1,'CREATE','APPROVED',$2,$3,now(),$4,$3,now(),$5,'NOT_STARTED'
		)`,
		newItemID,
		"Recreated after soft-delete of "+itemID,
		api.SystemIfBlank(checkerEmail),
		api.SystemIfBlank(""),
		checkerComment,
	); err != nil {
		return err
	}

	return recomputeCycleReadiness(ctx, tx, cycleID)
}

func runEngineInBackground(fn func(ctx context.Context)) {
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDClosingChecklist] engine goroutine panic: %v", rec)
			}
		}()
		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		fn(bgCtx)
	}()
}

func mergeItemIDs(one string, many []string) []string {
	seen := map[string]struct{}{}
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
	add(one)
	for _, id := range many {
		add(id)
	}
	return out
}
