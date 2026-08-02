package fdAccrual

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const JobPathAccrualFire = "job://investment/accrual-fire"

// EnforceScheduledAccrualFire gates scheduler-created accrual runs.
func EnforceScheduledAccrualFire(ctx context.Context, pool *pgxpool.Pool, p FireScheduledParams, periodStart, periodEnd string) (bool, string) {
	eventCode := common.TriggerScheduledDaily
	switch strings.ToUpper(strings.TrimSpace(p.ScheduleFreq)) {
	case "MONTHLY", "QUARTERLY", "HALF_YEARLY", "YEARLY":
		eventCode = common.TriggerScheduledMonthly
	}
	fields := map[string]interface{}{
		"entity_id":            p.EntityID,
		"entity_name":          p.EntityName,
		"run_type":             RunTypeForGranularity(p.Granularity),
		"run_mode":             p.RunMode,
		"accrual_period_start": periodStart,
		"accrual_period_end":   periodEnd,
		"schedule_config_id":   p.ConfigID,
		"schedule_frequency":   p.ScheduleFreq,
		"bank_id_filter":       p.BankFilter,
		"fd_status_filter":     p.FDStatus,
		"created_by":           "SCHEDULER",
		"execution_mode":       "CRON",
	}
	if schedRow, err := loadFDAccrualScheduleRow(ctx, pool, p.ConfigID); err == nil {
		for k, v := range buildFDAccrualSchedulePolicyFields(schedRow) {
			fields[k] = v
		}
	}
	return runtime.EnforceJobInline(ctx, pool, runtime.JobEnforceInput{
		EventCode:           eventCode,
		ModuleCode:          common.ModuleInvestmentFD,
		SubModule:           fdSubAccrual,
		EntityCode:          p.EntityID,
		ActorUserID:         "SYSTEM:accrual-scheduler",
		HandlerName:         "fireScheduledRun",
		APIPath:             JobPathAccrualFire,
		CorrelationID:       fmt.Sprintf("ACCRUAL-SCHED-%s", p.ConfigID),
		BusinessRecordID:    p.ConfigID,
		BusinessRecordType:  "FD_ACCRUAL_SCHED",
		Fields:              fields,
		DefaultBlockMessage: "Scheduled accrual run blocked by policy",
	})
}
