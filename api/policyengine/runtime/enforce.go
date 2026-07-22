package runtime

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnforceInput is the shared PRE_* hook for business handlers
// (mirror of notifcatalog.TriggerNotification, but before mutation).
type EnforceInput struct {
	EventCode     string
	ModuleCode    string
	SubModule     string
	EntityCode    string
	ActorUserID   string
	HandlerName   string
	APIPath       string
	CorrelationID string
	// Variables are CDM evaluation keys (already mapped). Prefer BuildVariablesFromCatalog.
	Variables map[string]string
	// Fields + SubModule: if Variables empty, map via domain_catalog.field.cdm_path.
	Fields map[string]interface{}
	// RequireVariables: if true, empty CDM map aborts (FD booking pilot).
	// Default false — modules without CDM seed yet still call RunCheck (no matching policies → pass).
	RequireVariables bool
	// DefaultBlockMessage used when HardBlock has no policy message.
	DefaultBlockMessage string
}

// Enforce writes HTTP error and returns false when the handler must abort.
func Enforce(ctx context.Context, w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, in EnforceInput) bool {
	ok, msg := EnforceInline(ctx, r, pool, in)
	if ok {
		return true
	}
	status := http.StatusUnprocessableEntity
	if msg == "" {
		msg = "Blocked by policy"
	}
	if strings.HasPrefix(msg, "policy check failed") {
		status = http.StatusBadGateway
	}
	api.RespondWithError(w, status, msg)
	return false
}

// EnforceInline returns (ok, errorMessage) without writing HTTP — for bulk loops.
func EnforceInline(ctx context.Context, r *http.Request, pool *pgxpool.Pool, in EnforceInput) (bool, string) {
	vars := in.Variables
	if len(vars) == 0 && len(in.Fields) > 0 && strings.TrimSpace(in.SubModule) != "" {
		mapped, err := BuildVariablesFromCatalog(ctx, pool, in.SubModule, in.Fields, nil)
		if err != nil {
			api.LogError("policy CDM map %s/%s: %v", in.ModuleCode, in.SubModule, err)
			if in.RequireVariables {
				return false, "policy check failed — could not map fields to CDM"
			}
		} else {
			vars = mapped
		}
	}
	if len(vars) == 0 && in.RequireVariables {
		return false, "policy check failed — no CDM variables mapped"
	}
	if vars == nil {
		vars = map[string]string{}
	}

	corr := in.CorrelationID
	if corr == "" && r != nil {
		corr = common.ResolveCorrelationID(r, "")
	}
	traceID := observability.TraceIDFromContext(ctx)

	result, err := RunCheck(ctx, pool, CheckRequest{
		EventCode:     in.EventCode,
		ModuleCode:    in.ModuleCode,
		SubModule:     in.SubModule,
		EntityCode:    in.EntityCode,
		ActorUserID:   in.ActorUserID,
		HandlerName:   in.HandlerName,
		APIPath:       in.APIPath,
		CorrelationID: corr,
		TraceID:       traceID,
		Variables:     vars,
	})
	if err != nil {
		api.LogError("policy check %s %s: %v", in.HandlerName, in.EventCode, err)
		return false, "policy check failed — please try again later"
	}
	if result.BlocksSubmit() {
		msg := result.FirstBreachMessage()
		if msg == "" {
			msg = in.DefaultBlockMessage
		}
		if msg == "" {
			msg = fmt.Sprintf("Blocked by policy (%s)", in.EventCode)
		}
		return false, msg
	}
	return true, ""
}
