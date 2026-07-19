package common

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/observability"

	"github.com/google/uuid"
)

const HeaderCorrelationID = "X-Correlation-Id"

// ResolveTraceID returns the trace ID for an execution_log row: explicit body value,
// response header (observability middleware), request context, or incoming header.
func ResolveTraceID(w http.ResponseWriter, r *http.Request, fromBody string) string {
	if s := strings.TrimSpace(fromBody); s != "" {
		return s
	}
	if s := api.TraceIDFromWriter(w); s != "" {
		return s
	}
	if s := observability.TraceIDFromContext(r.Context()); s != "" {
		return s
	}
	return strings.TrimSpace(r.Header.Get(api.HeaderTraceID))
}

// ResolveCorrelationID returns the business correlation ID: explicit body value,
// X-Correlation-Id header, or the current request trace ID as a single-request fallback.
func ResolveCorrelationID(r *http.Request, fromBody string) string {
	if s := strings.TrimSpace(fromBody); s != "" {
		return s
	}
	if s := strings.TrimSpace(r.Header.Get(HeaderCorrelationID)); s != "" {
		return s
	}
	if s := observability.TraceIDFromContext(r.Context()); s != "" {
		return s
	}
	return uuid.NewString()
}
