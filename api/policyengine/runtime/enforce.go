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
	// Also used to build notification template keys (field_code / NOTIFICATION alias_key).
	Fields map[string]interface{}
	// RequireVariables: if true, empty CDM map aborts (FD booking pilot).
	// Default false — modules without CDM seed yet still call RunCheck (no matching policies → pass).
	RequireVariables bool
	// DefaultBlockMessage used when HardBlock has no policy message.
	DefaultBlockMessage string
	BusinessRecordID   string
	BusinessRecordType string
}

// EnforceOutcome is returned by EnforceDetailed / EnforceInlineDetailed so
// callers (and HTTP) can surface every related policy pass/fail.
type EnforceOutcome struct {
	OK      bool
	Message string
	Result  CheckResult
}

// Enforce writes HTTP error and returns false when the handler must abort.
// On HardBlock, the error envelope includes data.policy_results (pass + fail).
func Enforce(ctx context.Context, w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, in EnforceInput) bool {
	out := EnforceDetailed(ctx, r, pool, in)
	if out.OK {
		out.Result.WriteSummaryHeader(w)
		return true
	}
	status := http.StatusUnprocessableEntity
	msg := out.Message
	if msg == "" {
		msg = "Blocked by policy"
	}
	if strings.HasPrefix(msg, "policy check failed") {
		status = http.StatusBadGateway
		api.RespondEnvelopeError(w, status, msg, "POLICY_SERVICE_ERROR")
		return false
	}
	api.RespondEnvelopeFailureWithData(w, status, msg, "POLICY_BREACH", out.Result.BlockPayload())
	return false
}

// EnforceWithMatrix behaves exactly like Enforce, but also returns the approval
// matrix pinned by a breached TriggerApproval policy. The string is empty when
// nothing breached that way, in which case callers resolve their own matrix.
func EnforceWithMatrix(ctx context.Context, w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, in EnforceInput) (bool, string) {
	out := EnforceDetailed(ctx, r, pool, in)
	if out.OK {
		out.Result.WriteSummaryHeader(w)
		return true, out.Result.TriggerApprovalMatrixID
	}
	status := http.StatusUnprocessableEntity
	msg := out.Message
	if msg == "" {
		msg = "Blocked by policy"
	}
	if strings.HasPrefix(msg, "policy check failed") {
		api.RespondEnvelopeError(w, http.StatusBadGateway, msg, "POLICY_SERVICE_ERROR")
		return false, ""
	}
	api.RespondEnvelopeFailureWithData(w, status, msg, "POLICY_BREACH", out.Result.BlockPayload())
	return false, ""
}

func WriteBlockResponse(w http.ResponseWriter, out EnforceOutcome, recordID string) bool {
	if out.OK {
		out.Result.WriteSummaryHeader(w)
		return true
	}
	msg := out.Message
	if msg == "" {
		msg = "Blocked by policy"
	}
	if strings.HasPrefix(msg, "policy check failed") {
		api.RespondEnvelopeError(w, http.StatusBadGateway, msg, "POLICY_SERVICE_ERROR")
		return false
	}
	if id := strings.TrimSpace(recordID); id != "" {
		msg = id + ": " + msg
	}
	api.RespondEnvelopeFailureWithData(w, http.StatusUnprocessableEntity, msg, "POLICY_BREACH", out.Result.BlockPayload())
	return false
}

// EnforceInline returns (ok, errorMessage) without writing HTTP — for bulk loops.
func EnforceInline(ctx context.Context, r *http.Request, pool *pgxpool.Pool, in EnforceInput) (bool, string) {
	out := EnforceDetailed(ctx, r, pool, in)
	return out.OK, out.Message
}

// EnforceDetailed runs the check and returns the full CheckResult for UIs.
func EnforceDetailed(ctx context.Context, r *http.Request, pool *pgxpool.Pool, in EnforceInput) EnforceOutcome {
	if !PolicyChecksEnabled() {
		return EnforceOutcome{OK: true}
	}
	if strings.TrimSpace(in.ModuleCode) != "" && strings.TrimSpace(in.SubModule) == "" {
		api.LogError("policy Enforce missing SubModule handler=%s module=%s event=%s — refusing cross-sub-module load",
			in.HandlerName, in.ModuleCode, in.EventCode)
		return EnforceOutcome{OK: false, Message: "policy check failed — sub_module is required"}
	}
	vars := in.Variables
	if len(vars) == 0 && len(in.Fields) > 0 && strings.TrimSpace(in.SubModule) != "" {
		mapped, err := BuildVariablesFromCatalog(ctx, pool, in.SubModule, in.Fields, nil)
		if err != nil {
			api.LogError("policy CDM map %s/%s: %v", in.ModuleCode, in.SubModule, err)
			if in.RequireVariables {
				return EnforceOutcome{OK: false, Message: "policy check failed — could not map fields to CDM"}
			}
		} else {
			vars = mapped
		}
	}
	if len(vars) == 0 && in.RequireVariables {
		return EnforceOutcome{OK: false, Message: "policy check failed — no CDM variables mapped"}
	}
	if vars == nil {
		vars = map[string]string{}
	}

	corr := in.CorrelationID
	if corr == "" && r != nil {
		corr = common.ResolveCorrelationID(r, "")
	}
	traceID := observability.TraceIDFromContext(ctx)

	actor := strings.TrimSpace(in.ActorUserID)
	if actor == "" && r != nil {
		actor = common.RequestActor(r, "")
	}
	requestedIP := ""
	if r != nil {
		requestedIP = common.RequestIP(r)
	}

	result, err := RunCheck(ctx, pool, CheckRequest{
		EventCode:          in.EventCode,
		ModuleCode:         in.ModuleCode,
		SubModule:          in.SubModule,
		EntityCode:         in.EntityCode,
		ActorUserID:        actor,
		RequestedIP:        requestedIP,
		HandlerName:        in.HandlerName,
		APIPath:            in.APIPath,
		CorrelationID:      corr,
		TraceID:            traceID,
		BusinessRecordID:   in.BusinessRecordID,
		BusinessRecordType: in.BusinessRecordType,
		Variables:          vars,
		NotifyFields:       in.Fields,
	})
	if err != nil {
		api.LogError("policy check %s %s: %v", in.HandlerName, in.EventCode, err)
		return EnforceOutcome{OK: false, Message: "policy check failed — please try again later"}
	}
	if result.BlocksSubmit() {
		msg := result.ClientMessage(in.DefaultBlockMessage)
		if msg == "" {
			msg = fmt.Sprintf("Blocked by policy (%s)", in.EventCode)
		}
		return EnforceOutcome{OK: false, Message: msg, Result: result}
	}
	return EnforceOutcome{OK: true, Result: result}
}
