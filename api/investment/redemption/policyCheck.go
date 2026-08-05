package redemption

import (
	"context"
	"net/http"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	mfSubRedemption     = "MF_REDEMPTION"
	mfSubRedemptionConf = "MF_REDEMPTION_CONF"
)

// enforceCtx groups the per-call policy-enforcement identifiers so the
// Enforce/EnforceInline helpers below stay under the 7-parameter limit.
type enforceCtx struct {
	EventCode, HandlerName, APIPath, SubModule, EntityCode, Actor string
}

func mfEnforce(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) bool {
	return runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentMF,
		SubModule:        cc.SubModule,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func mfEnforceBlock(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
	recordID string,
) bool {
	out := runtime.EnforceDetailed(ctx, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentMF,
		SubModule:        cc.SubModule,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
	return runtime.WriteBlockResponse(w, out, recordID)
}

func mfEnforceInline(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentMF,
		SubModule:        cc.SubModule,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}
