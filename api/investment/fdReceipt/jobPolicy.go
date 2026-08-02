package fdReceipt

import (
	"context"
	"fmt"
	"time"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const JobPathReceiptReconcile = "job://investment/receipt-reconcile"

// ReceiptReconcileEntity groups the entity identity fields for
// EnforceReceiptReconcileRun so the function stays under the 7-parameter limit.
type ReceiptReconcileEntity struct {
	ID   string
	Name string
}

// EnforceReceiptReconcileRun gates auto receipt reconcile run creation.
func EnforceReceiptReconcileRun(ctx context.Context, pool *pgxpool.Pool, runID string, entity ReceiptReconcileEntity, periodStart, periodEnd time.Time, pendingCount int) (bool, string) {
	fields := map[string]interface{}{
		"entity_id":        entity.ID,
		"entity_name":      entity.Name,
		"reconcile_run_id": runID,
		"period_start":     periodStart.Format("2006-01-02"),
		"period_end":       periodEnd.Format("2006-01-02"),
		"pending_count":    pendingCount,
		"trigger_mode":     "AUTO",
		"execution_mode":   "CRON",
	}
	return runtime.EnforceJobInline(ctx, pool, runtime.JobEnforceInput{
		EventCode:           common.TriggerScheduledDaily,
		ModuleCode:          common.ModuleInvestmentFD,
		SubModule:           fdSubReceipt,
		EntityCode:          entity.ID,
		ActorUserID:         "SYSTEM:receipt-reconcile",
		HandlerName:         "runAutoReconcile",
		APIPath:             JobPathReceiptReconcile,
		CorrelationID:       fmt.Sprintf("FD-RECON-%s", runID),
		BusinessRecordID:    runID,
		BusinessRecordType:  "FD_RECEIPT_RECONCILE_RUN",
		Fields:              fields,
		DefaultBlockMessage: "Receipt reconcile run blocked by policy",
	})
}
