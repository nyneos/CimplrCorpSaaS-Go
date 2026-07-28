package runtime

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"
)

// JobEnforceInput is the async/cron equivalent of EnforceInput — no *http.Request.
// Call from background jobs only (SmartCat, Sweep V2, accrual fire, auto-maturity,
// FD receipt reconcile). HTTP handlers keep using Enforce / EnforceInline.
type JobEnforceInput struct {
	EventCode           string
	ModuleCode          string
	SubModule           string
	EntityCode          string
	ActorUserID         string // e.g. "SYSTEM:smart-cat"
	HandlerName         string
	APIPath             string // synthetic: "job://cash/smart-cat"
	CorrelationID       string
	BusinessRecordID    string
	BusinessRecordType  string
	Variables           map[string]string
	Fields              map[string]interface{}
	RequireVariables    bool
	DefaultBlockMessage string
}

// EnforceJob runs the same RunCheck path as HTTP Enforce, for workers without a request.
//
// BRD §6.3: Scheduled / async system events are non-blocking — HardBlock and lane
// conflicts are logged and evaluated but never abort the cron worker. Only
// NotifyOnly and TriggerApproval breach actions dispatch notifications.
func EnforceJob(ctx context.Context, pool *pgxpool.Pool, in JobEnforceInput) EnforceOutcome {
	return applyJobNonBlocking(enforceJobDetailed(ctx, pool, in), in)
}

// applyJobNonBlocking implements BRD §6.3 — cron/async never abort on HardBlock.
func applyJobNonBlocking(out EnforceOutcome, in JobEnforceInput) EnforceOutcome {
	if !out.Result.BlocksSubmit() {
		return out
	}
	msg := out.Result.ClientMessage(in.DefaultBlockMessage)
	if msg == "" {
		msg = fmt.Sprintf("policy breach on %s (non-blocking for async job)", in.EventCode)
	}
	api.LogError("EnforceJob non-blocking breach handler=%s event=%s module=%s/%s msg=%s",
		in.HandlerName, in.EventCode, in.ModuleCode, in.SubModule, msg)
	return EnforceOutcome{OK: true, Result: out.Result}
}

// enforceJobDetailed mirrors EnforceDetailed but is isolated so job semantics can diverge.
func enforceJobDetailed(ctx context.Context, pool *pgxpool.Pool, in JobEnforceInput) EnforceOutcome {
	return EnforceDetailed(ctx, nil, pool, EnforceInput{
		EventCode:           in.EventCode,
		ModuleCode:          in.ModuleCode,
		SubModule:           in.SubModule,
		EntityCode:          in.EntityCode,
		ActorUserID:         strings.TrimSpace(in.ActorUserID),
		HandlerName:         in.HandlerName,
		APIPath:             in.APIPath,
		CorrelationID:       in.CorrelationID,
		Variables:           in.Variables,
		Fields:              in.Fields,
		RequireVariables:    in.RequireVariables,
		DefaultBlockMessage: in.DefaultBlockMessage,
		BusinessRecordID:    in.BusinessRecordID,
		BusinessRecordType:  in.BusinessRecordType,
	})
}

// EnforceJobInline returns (ok, message) for job loops.
// Async/cron jobs never hard-block — ok is false only for infrastructure/CDM failures.
func EnforceJobInline(ctx context.Context, pool *pgxpool.Pool, in JobEnforceInput) (bool, string) {
	out := EnforceJob(ctx, pool, in)
	return out.OK, out.Message
}

// JobTraceID returns a stable trace id for jobs (prefer context, else correlation).
func JobTraceID(ctx context.Context, correlationID string) string {
	if tid := observability.TraceIDFromContext(ctx); tid != "" {
		return tid
	}
	return strings.TrimSpace(correlationID)
}
