package counterpartyHub

import (
	"context"
	"net/http"

	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const hubSubCounterparty = "COUNTERPARTY"

type enforceCtx struct {
	EventCode, HandlerName, APIPath, EntityCode, Actor string
}

func hubEnforceMatrix(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceWithMatrix(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       "COUNTERPARTY_HUB",
		SubModule:        hubSubCounterparty,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func hubEnforceInlineWithMatrix(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) (bool, string, string) {
	return runtime.EnforceInlineWithMatrix(ctx, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       "COUNTERPARTY_HUB",
		SubModule:        hubSubCounterparty,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}
