package fdReceipt

import (
	"context"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	fdSubReceipt   = "FD_RECEIPT"
	fdSubException = "FD_EXCEPTION"
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

func exceptionPolicyEntityID(ctx context.Context, pool *pgxpool.Pool, hdr *varianceCaseHeader) string {
	if hdr == nil {
		return ""
	}
	if strings.TrimSpace(hdr.FDID) != "" {
		var entityID string
		_ = pool.QueryRow(ctx, `SELECT COALESCE(entity_id,'') FROM investment.fd_master WHERE fd_id=$1`, hdr.FDID).Scan(&entityID)
		if entityID != "" {
			return entityID
		}
	}
	if strings.TrimSpace(hdr.ReceiptID) != "" {
		var entityID string
		_ = pool.QueryRow(ctx, `SELECT COALESCE(entity_id,'') FROM investment.fd_interest_receipt WHERE receipt_id=$1`, hdr.ReceiptID).Scan(&entityID)
		return entityID
	}
	return ""
}
