package jobs

import (
	"context"
	"fmt"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const jobPathSweepFire = "job://cash/sweep-fire"

func enforceScheduledSweepFire(ctx context.Context, pool *pgxpool.Pool, sweepID, initiationID, handlerName string) (bool, string) {
	var entityName, sourceBankName, sourceAcct, targetBankName, targetAcct, sweepType, frequency, effectiveDate, execTime string
	var bufferAmt, sweepAmt *float64
	var requiresInit bool
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_name,''), COALESCE(source_bank_name,''), COALESCE(source_bank_account,''),
		       COALESCE(target_bank_name,''), COALESCE(target_bank_account,''),
		       COALESCE(sweep_type,''), COALESCE(frequency,''),
		       COALESCE(effective_date::text,''), COALESCE(execution_time::text,''),
		       buffer_amount, sweep_amount, COALESCE(requires_initiation,true)
		FROM cimplrcorpsaas.sweepconfiguration WHERE sweep_id = $1`, sweepID,
	).Scan(&entityName, &sourceBankName, &sourceAcct, &targetBankName, &targetAcct,
		&sweepType, &frequency, &effectiveDate, &execTime, &bufferAmt, &sweepAmt, &requiresInit)
	if err != nil {
		return false, "policy check failed — could not load sweep configuration"
	}
	fields := map[string]interface{}{
		"sweep_id": sweepID, "entity_name": entityName,
		"source_bank_name": sourceBankName, "source_bank_account": sourceAcct,
		"target_bank_name": targetBankName, "target_bank_account": targetAcct,
		"sweep_type": sweepType, "frequency": frequency,
		"effective_date": effectiveDate, "execution_time": execTime,
		"buffer_amount": bufferAmt, "sweep_amount": sweepAmt,
		"requires_initiation": requiresInit, "execution_mode": "CRON",
	}
	if initiationID != "" {
		fields["initiation_id"] = initiationID
	}
	corr := fmt.Sprintf("SWEEP-%s", sweepID)
	if initiationID != "" {
		corr = fmt.Sprintf("SWEEP-INIT-%s", initiationID)
	}
	return runtime.EnforceJobInline(ctx, pool, runtime.JobEnforceInput{
		EventCode: common.TriggerScheduledIntraday, ModuleCode: common.ModuleCash,
		SubModule: "SWEEP_EXECUTION", EntityCode: entityName, ActorUserID: "SYSTEM:sweep-v2",
		HandlerName: handlerName, APIPath: jobPathSweepFire, CorrelationID: corr,
		BusinessRecordID: sweepID, BusinessRecordType: "SWEEP_CONFIG",
		Fields: fields, DefaultBlockMessage: "Scheduled sweep blocked by policy",
	})
}
