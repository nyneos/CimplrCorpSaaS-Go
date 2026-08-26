package fdMaturityAndRollover

import (
	"context"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const fdSubClosure = "FD_CLOSURE"

type enforceCtx struct {
	EventCode, HandlerName, APIPath, EntityCode, Actor string
}

func fdEnforce(
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
		SubModule:        fdSubClosure,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func fdEnforceInline(
	ctx context.Context,
	r *http.Request,
	pool *pgxpool.Pool,
	cc enforceCtx,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
		EventCode:        cc.EventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubClosure,
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
		SubModule:        fdSubClosure,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}

func cimplrClosureUploadEntityID(ctx context.Context, pool *pgxpool.Pool, initiateID, confirmID string) string {
	var entityID string
	if strings.TrimSpace(confirmID) != "" {
		_ = pool.QueryRow(ctx, `SELECT COALESCE(entity_id,'') FROM cimplr.fd_closure_confirm WHERE closure_confirm_id=$1`, confirmID).Scan(&entityID)
		if entityID != "" {
			return entityID
		}
	}
	if strings.TrimSpace(initiateID) != "" {
		_ = pool.QueryRow(ctx, `SELECT COALESCE(entity_id,'') FROM cimplr.fd_closure_initiate WHERE closure_initiate_id=$1`, initiateID).Scan(&entityID)
	}
	return entityID
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
		SubModule:        fdSubClosure,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}
