package fdAccrual

import (
	"context"
	"net/http"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	fdSubAccrual      = "FD_ACCRUAL"
	fdSubAccrualSched = "FD_ACCRUAL_SCHED"
)

// enforceCtx groups the per-call identifiers shared by the fdAccrual
// Enforce/EnforceInline variants (event/actor/handler routing info).
type enforceCtx struct {
	EventCode, HandlerName, APIPath, EntityCode, Actor string
}

func fdAccrualEnforce(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) bool {
	return runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubAccrual,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func fdAccrualEnforceInline(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubAccrual,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func fdAccrualSchedEnforce(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) bool {
	return runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubAccrualSched,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func fdAccrualSchedEnforceInline(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubAccrualSched,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

// accrualRunPolicyFields loads the full canonical FD_ACCRUAL run row (see
// accrualPolicyFields.go) and returns it as a policy Fields map plus the
// entity_id for scoping — used by every run-level call site that only has a
// run_id (RunAccrual, BulkApprove/Reject, RecomputeAccrualRun,
// BulkGenerateMonthlyAccruals' execute step) instead of each hand-rolling
// its own thinner ad hoc SELECT.
func accrualRunPolicyFields(ctx context.Context, pool *pgxpool.Pool, runID string) (map[string]interface{}, string, error) {
	row, err := loadFDAccrualRunRow(ctx, pool, runID)
	if err != nil {
		return nil, "", err
	}
	return buildFDAccrualPolicyFields(row), row.EntityID, nil
}

// accrualSchedulePolicyFields loads the full canonical FD_ACCRUAL_SCHED row
// (see accrualSchedulePolicyFields.go) and returns it as a policy Fields map
// plus the entity_id for scoping — used by every call site that only has a
// config_id (DisableSchedule, EnableSchedule, Approve/RejectScheduleConfig).
func accrualSchedulePolicyFields(ctx context.Context, pool *pgxpool.Pool, configID string) (map[string]interface{}, string, error) {
	row, err := loadFDAccrualScheduleRow(ctx, pool, configID)
	if err != nil {
		return nil, "", err
	}
	return buildFDAccrualSchedulePolicyFields(row), row.EntityID, nil
}
