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

// enforceCtx groups the per-call policy-enforcement identifiers so the
// Enforce/EnforceInline helpers below stay under the 7-parameter limit.
type enforceCtx struct {
	EventCode, HandlerName, APIPath, SubModule, EntityCode, Actor string
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
		SubModule:        cc.SubModule,
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
		SubModule:        cc.SubModule,
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
		SubModule:        cc.SubModule,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
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

// fdEnforceMatrix behaves like fdEnforce but also returns the approval matrix
// pinned by a breached TriggerApproval policy ("" when none applies).
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
		SubModule:        cc.SubModule,
		EntityCode:       cc.EntityCode,
		ActorUserID:      cc.Actor,
		HandlerName:      cc.HandlerName,
		APIPath:          cc.APIPath,
		Fields:           fields,
		RequireVariables: false,
	})
}
