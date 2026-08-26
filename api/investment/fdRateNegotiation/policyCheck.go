package fdRateNegotiation

import (
	"context"
	"net/http"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const fdSubRateNeg = "FD_RATE_NEGOTIATION"

type enforceCtx struct {
	EventCode, HandlerName, APIPath, EntityCode, Actor string
}

func fdEnforceMatrix(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceWithMatrix(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubRateNeg,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func fdEnforceInlineWithMatrix(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) (bool, string, string) {
	return runtime.EnforceInlineWithMatrix(ctx, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubRateNeg,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}
