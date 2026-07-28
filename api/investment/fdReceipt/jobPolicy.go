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

// EnforceReceiptReconcileRun gates auto receipt reconcile run creation.
func EnforceReceiptReconcileRun(ctx context.Context, pool *pgxpool.Pool, runID, entityID, entityName string, periodStart, periodEnd time.Time, pendingCount int) (bool, string) {
	fields := map[string]interface{}{
		"entity_id":        entityID,
		"entity_name":      entityName,
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
		EntityCode:          entityID,
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
