package rules

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5"
)

type auditRow struct {
	cols []string
	vals []interface{}
}

func (a *auditRow) set(col string, val interface{}) {
	a.cols = append(a.cols, col)
	a.vals = append(a.vals, val)
}

func (a *auditRow) exec(ctx context.Context, tx pgx.Tx) error {
	placeholders := make([]string, len(a.vals))
	for i := range a.vals {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	query := fmt.Sprintf("INSERT INTO dms_svc.generation_rule_audit (%s) VALUES (%s)",
		strings.Join(a.cols, ", "), strings.Join(placeholders, ", "))
	_, err := tx.Exec(ctx, query, a.vals...)
	return err
}

// findPendingAudit returns the latest pending generation_rule_audit row for a
// rule — the row a checker's approve/reject decision acts on.
func findPendingAudit(ctx context.Context, tx pgx.Tx, ruleID string) (ruleAuditRow, error) {
	var row ruleAuditRow
	err := tx.QueryRow(ctx, `
		SELECT audit_id::text, action_type, version_id::text, new_name, new_module_code, new_sub_module_code, new_status
		FROM dms_svc.generation_rule_audit
		WHERE rule_id = $1::uuid AND processing_status = ANY($2::text[])
		ORDER BY requested_at DESC
		LIMIT 1
		FOR UPDATE`, ruleID, common.PendingProcessingStatuses,
	).Scan(&row.AuditID, &row.ActionType, &row.VersionID, &row.NewName, &row.NewModuleCode, &row.NewSubModuleCode, &row.NewStatus)
	return row, err
}

// requirePendingFree returns an error if the rule already has a pending
// maker-checker request in flight.
func requirePendingFree(ctx context.Context, tx pgx.Tx, ruleID string) error {
	var processingStatus string
	err := tx.QueryRow(ctx, `
		SELECT processing_status FROM dms_svc.generation_rule
		WHERE rule_id = $1::uuid AND is_deleted = false
		FOR UPDATE`, ruleID,
	).Scan(&processingStatus)
	if err != nil {
		return err
	}
	if common.IsPendingStatus(processingStatus) {
		return fmt.Errorf("rule already has a pending request (%s) — resolve it before raising a new one", processingStatus)
	}
	return nil
}

// requireDeletable allows a delete request from any state except APPROVED and a
// delete already awaiting approval. Any other pending request (create/edit/new
// version) is superseded — its audit row is closed out so the DELETE row becomes
// the one a checker's decision acts on.
func requireDeletable(ctx context.Context, tx pgx.Tx, ruleID, actor string) error {
	var processingStatus string
	err := tx.QueryRow(ctx, `
		SELECT processing_status FROM dms_svc.generation_rule
		WHERE rule_id = $1::uuid AND is_deleted = false
		FOR UPDATE`, ruleID,
	).Scan(&processingStatus)
	if err != nil {
		return err
	}
	if processingStatus == "APPROVED" {
		return fmt.Errorf("approved rule cannot be deleted")
	}
	if processingStatus == "PENDING_DELETE_APPROVAL" {
		return fmt.Errorf("rule is already awaiting delete approval")
	}
	if !common.IsPendingStatus(processingStatus) {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE dms_svc.generation_rule_version SET status = 'REJECTED'
		WHERE version_id IN (
			SELECT version_id FROM dms_svc.generation_rule_audit
			WHERE rule_id = $1::uuid AND processing_status = ANY($2::text[])
				AND version_id IS NOT NULL
		)`, ruleID, common.PendingProcessingStatuses); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE dms_svc.generation_rule_audit
		SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(),
			checker_comment = 'Superseded by delete request'
		WHERE rule_id = $2::uuid AND processing_status = ANY($3::text[])`,
		actor, ruleID, common.PendingProcessingStatuses)
	return err
}

// pendingAmendableAudit returns the pending create/edit audit row a new version
// should amend, or nil when the rule has no pending request in flight.
func pendingAmendableAudit(ctx context.Context, tx pgx.Tx, ruleID string) (*ruleAuditRow, error) {
	var processingStatus string
	err := tx.QueryRow(ctx, `
		SELECT processing_status FROM dms_svc.generation_rule
		WHERE rule_id = $1::uuid AND is_deleted = false
		FOR UPDATE`, ruleID,
	).Scan(&processingStatus)
	if err != nil {
		return nil, err
	}
	if !common.IsPendingStatus(processingStatus) {
		return nil, nil
	}
	if processingStatus == "PENDING_DELETE_APPROVAL" {
		return nil, fmt.Errorf("rule is awaiting delete approval — resolve it before editing")
	}
	row, err := findPendingAudit(ctx, tx, ruleID)
	if err != nil {
		return nil, err
	}
	if row.ActionType != "CREATE" && row.ActionType != "CREATE_VERSION" {
		return nil, fmt.Errorf("rule already has a pending request (%s) — resolve it before raising a new one", row.ActionType)
	}
	return &row, nil
}

func requestActorAndIP(r *http.Request, reqActorID string) (actor, ip string) {
	return common.RequestActor(r, reqActorID), common.RequestIP(r)
}
