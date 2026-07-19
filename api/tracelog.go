package api

import (
	"context"
	"net/http"

	"CimplrCorpSaas/internal/logger"
)

// HeaderTraceID is the response header the observability middleware
// (internal/observability.WrapHTTP) stamps on every request before any
// handler runs. Every module server is wrapped with it, so by the time a
// handler calls a Respond* helper, w already carries this header.
const HeaderTraceID = "X-Trace-Id"

// TraceIDFromWriter reads the trace ID already stamped on the response by
// the observability middleware. Handlers/response helpers can use this to
// correlate a log line or an error body with the trace ID the client
// receives, without threading ctx through every RespondWith*/RespondEnvelope*
// call site.
func TraceIDFromWriter(w http.ResponseWriter) string {
	return w.Header().Get(HeaderTraceID)
}

// LogErrorForResponse logs the full error detail at ERROR level, tagged with
// the trace ID already on the response (if any), so that a trace ID a user
// reports (e.g. from an error toast or the X-Trace-Id response header) is
// enough to grep the server log for the underlying raw error — even though
// the client only ever sees a sanitized message for 5xx errors.
func LogErrorForResponse(w http.ResponseWriter, msg string, args ...interface{}) {
	ctx := context.Background()
	if traceID := TraceIDFromWriter(w); traceID != "" {
		ctx = logger.WithTraceID(ctx, traceID)
	}
	logger.LogErrorCtx(ctx, msg, args...)
}
