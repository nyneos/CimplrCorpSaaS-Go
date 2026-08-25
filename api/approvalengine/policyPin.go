package approvalengine

import (
	"CimplrCorpSaas/api"
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PolicyPinned is true when a TriggerApproval policy actually selected a matrix.
func PolicyPinned(matrixID string) bool {
	return strings.TrimSpace(matrixID) != ""
}

// isAmountRoutedModule is the FD exception: two amount-slab matrices still
// ResolveMatrix when policy did not pin. Every other module stays policy-pin only.
func isAmountRoutedModule(moduleCode string) bool {
	return strings.EqualFold(strings.TrimSpace(moduleCode), "FIXED_DEPOSIT")
}

// AuditStatus returns the pending maker-checker status callers insert on the
// audit row. Policy breach only adds an approval_instance when a matrix is
// pinned; unpinned rows stay PENDING for the normal visibility-based checker flow.
func AuditStatus(matrixID, pendingStatus string) string {
	_ = matrixID // pin affects approval_instance creation, not audit status
	return pendingStatus
}

func autoApplyUnpinned(ctx context.Context, pool *pgxpool.Pool, req InstanceRequest) error {
	if strings.TrimSpace(req.RecordID) == "" {
		return nil
	}
	if strings.TrimSpace(req.AuditTable) == "" || strings.TrimSpace(req.AuditIDColumn) == "" {
		regTable, regCol := LookupTxTableConfig(req.TransactionType)
		if strings.TrimSpace(req.AuditTable) == "" {
			req.AuditTable = regTable
		}
		if strings.TrimSpace(req.AuditIDColumn) == "" {
			req.AuditIDColumn = regCol
		}
	}
	if strings.TrimSpace(req.AuditTable) == "" || strings.TrimSpace(req.AuditIDColumn) == "" {
		api.LogInfo("[ApprovalEngine] Auto-apply skipped for %s/%s — no audit table/column",
			req.ModuleCode, req.RecordID)
		return nil
	}
	if strings.TrimSpace(req.RecordTable) == "" {
		req.RecordTable = req.AuditTable
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	checker := strings.TrimSpace(req.SubmittedByEmail)
	if checker == "" {
		checker = strings.TrimSpace(req.SubmittedBy)
	}
	if checker == "" {
		checker = "SYSTEM"
	}

	if err := finalizeRecord(ctx, tx, FinalizeParams{
		RecordID:       req.RecordID,
		AuditTable:     req.AuditTable,
		AuditIDColumn:  req.AuditIDColumn,
		RecordTable:    req.RecordTable,
		ActionType:     req.ActionType,
		FinalStatus:    InstStatusApproved,
		CheckerEmail:   checker,
		CheckerComment: "Auto-applied: policy did not trigger approval",
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	api.LogInfo("[ApprovalEngine] Auto-applied %s/%s — policy did not pin a matrix",
		req.ModuleCode, req.RecordID)
	return nil
}
