package templates

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5"
)

// auditRow builds an INSERT's column list and $N placeholders from ordered
// (column, value) pairs — same helper shape as policyengine/policy's auditRow.
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
	query := fmt.Sprintf("INSERT INTO dms_svc.template_audit (%s) VALUES (%s)",
		strings.Join(a.cols, ", "), strings.Join(placeholders, ", "))
	_, err := tx.Exec(ctx, query, a.vals...)
	return err
}

// findPendingAudit returns the latest pending template_audit row for a
// template — the row a checker's approve/reject decision acts on.
func findPendingAudit(ctx context.Context, tx pgx.Tx, templateID string) (templateAuditRow, error) {
	var row templateAuditRow
	err := tx.QueryRow(ctx, `
		SELECT audit_id::text, action_type, version_id::text, new_name, new_module_code, new_sub_module_code
		FROM dms_svc.template_audit
		WHERE template_id = $1::uuid AND processing_status = ANY($2::text[])
		ORDER BY requested_at DESC
		LIMIT 1
		FOR UPDATE`, templateID, common.PendingProcessingStatuses,
	).Scan(&row.AuditID, &row.ActionType, &row.VersionID, &row.NewName, &row.NewModuleCode, &row.NewSubModuleCode)
	return row, err
}

// requirePendingFree returns an error if the template already has a pending
// maker-checker request in flight — only one request may be outstanding at a
// time (a new one can't be raised on top of an unresolved one).
func requirePendingFree(ctx context.Context, tx pgx.Tx, templateID string) error {
	var processingStatus string
	err := tx.QueryRow(ctx, `
		SELECT processing_status FROM dms_svc.template
		WHERE template_id = $1::uuid AND is_deleted = false
		FOR UPDATE`, templateID,
	).Scan(&processingStatus)
	if err != nil {
		return err
	}
	if common.IsPendingStatus(processingStatus) {
		return fmt.Errorf("template already has a pending request (%s) — resolve it before raising a new one", processingStatus)
	}
	return nil
}

func requestActorAndIP(r *http.Request, reqActorID string) (actor, ip string) {
	return common.RequestActor(r, reqActorID), common.RequestIP(r)
}
