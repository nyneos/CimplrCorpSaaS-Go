package fdMaster

import (
	"context"
	"net/http"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	fdSubMaster   = "FD_MASTER"
	fdSubCashflow = "FD_CASHFLOW"
)

func fdEnforce(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	eventCode, handlerName, apiPath, subModule, entityCode, actor string,
	fields map[string]interface{},
) bool {
	return runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:        eventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        subModule,
		EntityCode:       entityCode,
		ActorUserID:      actor,
		HandlerName:      handlerName,
		APIPath:          apiPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func fdEnforceInline(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	eventCode, handlerName, apiPath, subModule, entityCode, actor string,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
		EventCode:        eventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        subModule,
		EntityCode:       entityCode,
		ActorUserID:      actor,
		HandlerName:      handlerName,
		APIPath:          apiPath,
		Fields:           fields,
		RequireVariables: false,
	})
}
