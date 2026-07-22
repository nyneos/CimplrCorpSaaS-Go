package fdAccrual

import (
	"context"
	"fmt"
	"net/http"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	fdSubAccrual     = "FD_ACCRUAL"
	fdSubAccrualSched = "FD_ACCRUAL_SCHED"
)

func fdAccrualEnforce(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	eventCode, handlerName, apiPath, entityCode, actor string,
	fields map[string]interface{},
) bool {
	return runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:        eventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubAccrual,
		EntityCode:       entityCode,
		ActorUserID:      actor,
		HandlerName:      handlerName,
		APIPath:          apiPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func fdAccrualEnforceInline(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	eventCode, handlerName, apiPath, entityCode, actor string,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
		EventCode:        eventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubAccrual,
		EntityCode:       entityCode,
		ActorUserID:      actor,
		HandlerName:      handlerName,
		APIPath:          apiPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func fdAccrualSchedEnforce(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	eventCode, handlerName, apiPath, entityCode, actor string,
	fields map[string]interface{},
) bool {
	return runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:        eventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubAccrualSched,
		EntityCode:       entityCode,
		ActorUserID:      actor,
		HandlerName:      handlerName,
		APIPath:          apiPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func fdAccrualSchedEnforceInline(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	eventCode, handlerName, apiPath, entityCode, actor string,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
		EventCode:        eventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubAccrualSched,
		EntityCode:       entityCode,
		ActorUserID:      actor,
		HandlerName:      handlerName,
		APIPath:          apiPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func accrualRunPolicyFields(ctx context.Context, pool *pgxpool.Pool, runID string) (map[string]interface{}, string, error) {
	var (
		entityID, runType, runMode, runStatus, financialPeriod string
		periodStart, periodEnd                                 interface{}
		totalNet                                               float64
	)
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_id,''),
		       COALESCE(run_type,''),
		       COALESCE(run_mode,''),
		       COALESCE(run_status,''),
		       accrual_period_start,
		       accrual_period_end,
		       COALESCE(financial_period,''),
		       COALESCE(total_interest_accrued,0)
		FROM investment.fd_accrual_run
		WHERE run_id=$1 AND COALESCE(is_deleted,false)=false`, runID,
	).Scan(&entityID, &runType, &runMode, &runStatus, &periodStart, &periodEnd, &financialPeriod, &totalNet)
	if err != nil {
		return nil, "", fmt.Errorf("load accrual run for policy: %w", err)
	}
	fields := map[string]interface{}{
		"run_id":                  runID,
		"entity_id":               entityID,
		"entity_code":             entityID,
		"run_type":                runType,
		"run_mode":                runMode,
		"run_status":              runStatus,
		"accrual_period_start":    periodStart,
		"accrual_period_end":      periodEnd,
		"financial_period":        financialPeriod,
		"total_interest_accrued":  totalNet,
	}
	return fields, entityID, nil
}

func accrualSchedulePolicyFields(ctx context.Context, pool *pgxpool.Pool, configID string) (map[string]interface{}, string, error) {
	var (
		entityID, scheduleFreq, periodCoverage, defaultRunMode string
		isActive                                               bool
	)
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_id,''),
		       COALESCE(schedule_frequency,''),
		       COALESCE(period_coverage,''),
		       COALESCE(default_run_mode,''),
		       COALESCE(is_active,false)
		FROM investment.fd_accrual_schedule_config
		WHERE config_id=$1 AND COALESCE(is_deleted,false)=false`, configID,
	).Scan(&entityID, &scheduleFreq, &periodCoverage, &defaultRunMode, &isActive)
	if err != nil {
		return nil, "", fmt.Errorf("load schedule config for policy: %w", err)
	}
	fields := map[string]interface{}{
		"config_id":          configID,
		"entity_id":          entityID,
		"entity_code":        entityID,
		"schedule_frequency": scheduleFreq,
		"period_coverage":    periodCoverage,
		"default_run_mode":   defaultRunMode,
		"is_active":          isActive,
	}
	return fields, entityID, nil
}
