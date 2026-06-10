package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"CimplrCorpSaas/internal/logger"
	"github.com/google/uuid"
)

type contextKey string

const (
	requestIDKey      contextKey = "observability_request_id"
	traceIDKey        contextKey = "observability_trace_id"
	headerRequestID              = "X-Request-Id"
	headerTraceID                = "X-Trace-Id"
	headerTraceParent            = "traceparent"
)

type statusCapturingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	bytes      int64
}

func (rw *statusCapturingResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *statusCapturingResponseWriter) Write(b []byte) (int, error) {
	if rw.statusCode == 0 {
		rw.statusCode = http.StatusOK
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.bytes += int64(n)
	return n, err
}

func (rw *statusCapturingResponseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

type metricKey struct {
	Service string
	Method  string
	Path    string
	Status  int
}

type metricValue struct {
	Count         uint64 `json:"count"`
	Errors        uint64 `json:"errors"`
	TotalBytes    uint64 `json:"total_bytes"`
	TotalDuration uint64 `json:"total_duration_ms"`
}

var httpMetrics sync.Map

func WrapHTTP(service string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		requestID := firstNonEmpty(
			strings.TrimSpace(r.Header.Get(headerRequestID)),
			uuid.NewString(),
		)
		traceID := firstNonEmpty(
			traceIDFromRequest(r),
			requestID,
		)

		ctx := context.WithValue(r.Context(), requestIDKey, requestID)
		ctx = context.WithValue(ctx, traceIDKey, traceID)
		ctx = logger.WithTraceID(ctx, traceID)
		r = r.WithContext(ctx)

		r.Header.Set(headerRequestID, requestID)
		r.Header.Set(headerTraceID, traceID)
		w.Header().Set(headerRequestID, requestID)
		w.Header().Set(headerTraceID, traceID)

		rw := &statusCapturingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rw, r)

		duration := time.Since(start)
		recordMetric(service, r.Method, r.URL.Path, rw.statusCode, rw.bytes, duration)
		logger.LogInfoCtx(
			r.Context(),
			"[HTTP] service=%s method=%s path=%s status=%d duration_ms=%d bytes=%d request_id=%s remote=%s",
			service,
			r.Method,
			r.URL.Path,
			rw.statusCode,
			duration.Milliseconds(),
			rw.bytes,
			requestID,
			traceID,
			clientIPFromRequest(r),
		)
	})
}

func MetricsHandler(service string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		payload := map[string]any{
			"service": service,
			"metrics": snapshot(service),
		}
		_ = json.NewEncoder(w).Encode(payload)
	})
}

func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}

func recordMetric(service, method, path string, status int, bytes int64, duration time.Duration) {
	key := metricKey{
		Service: service,
		Method:  method,
		Path:    path,
		Status:  status,
	}
	entry, _ := httpMetrics.LoadOrStore(key, &metricValue{})
	value := entry.(*metricValue)

	atomic.AddUint64(&value.Count, 1)
	if status >= http.StatusInternalServerError {
		atomic.AddUint64(&value.Errors, 1)
	}
	if bytes > 0 {
		atomic.AddUint64(&value.TotalBytes, uint64(bytes))
	}
	atomic.AddUint64(&value.TotalDuration, uint64(duration.Milliseconds()))
}

func snapshot(service string) []map[string]any {
	rows := make([]map[string]any, 0)
	httpMetrics.Range(func(k, v any) bool {
		key := k.(metricKey)
		if key.Service != service {
			return true
		}

		value := v.(*metricValue)
		count := atomic.LoadUint64(&value.Count)
		totalDuration := atomic.LoadUint64(&value.TotalDuration)
		avgDuration := float64(0)
		if count > 0 {
			avgDuration = float64(totalDuration) / float64(count)
		}

		rows = append(rows, map[string]any{
			"method":            key.Method,
			"path":              key.Path,
			"status":            key.Status,
			"count":             count,
			"errors":            atomic.LoadUint64(&value.Errors),
			"total_bytes":       atomic.LoadUint64(&value.TotalBytes),
			"total_duration_ms": totalDuration,
			"avg_duration_ms":   fmt.Sprintf("%.2f", avgDuration),
		})
		return true
	})
	return rows
}

func traceIDFromRequest(r *http.Request) string {
	if traceID := strings.TrimSpace(r.Header.Get(headerTraceID)); traceID != "" {
		return traceID
	}
	if requestID := strings.TrimSpace(r.Header.Get(headerRequestID)); requestID != "" {
		return requestID
	}
	traceParent := strings.TrimSpace(r.Header.Get(headerTraceParent))
	if traceParent == "" {
		return ""
	}
	parts := strings.Split(traceParent, "-")
	if len(parts) >= 4 && parts[1] != "" {
		return parts[1]
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func clientIPFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}

	if forwardedFor := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwardedFor != "" {
		parts := strings.Split(forwardedFor, ",")
		if first := strings.TrimSpace(parts[0]); first != "" {
			return normalizeClientIP(first)
		}
	}

	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return normalizeClientIP(realIP)
	}

	return normalizeClientIP(r.RemoteAddr)
}

func normalizeClientIP(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(trimmed); err == nil {
		return strings.TrimSpace(host)
	}
	return trimmed
}
