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

func fdEnforce(
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
		SubModule:        fdSubClosure,
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
	eventCode, handlerName, apiPath, entityCode, actor string,
	fields map[string]interface{},
) (bool, string) {
	return runtime.EnforceInline(ctx, r, pool, runtime.EnforceInput{
		EventCode:        eventCode,
		ModuleCode:       common.ModuleInvestmentFD,
		SubModule:        fdSubClosure,
		EntityCode:       entityCode,
		ActorUserID:      actor,
		HandlerName:      handlerName,
		APIPath:          apiPath,
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
