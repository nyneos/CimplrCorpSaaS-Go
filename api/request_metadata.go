package api

import (
	"CimplrCorpSaas/api/auth"
	"context"
	"net"
	"net/http"
	"strings"
)

const ClientIPContextKey = "client_ip"

func ClientIPFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}

	if forwardedFor := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwardedFor != "" {
		parts := strings.Split(forwardedFor, ",")
		if first := strings.TrimSpace(parts[0]); first != "" {
			return NormalizeClientIP(first)
		}
	}

	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return NormalizeClientIP(realIP)
	}

	return NormalizeClientIP(r.RemoteAddr)
}

func ClientIPFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if ip, ok := ctx.Value(ClientIPContextKey).(string); ok {
		if normalized := NormalizeClientIP(ip); normalized != "" {
			return normalized
		}
	}
	if session, ok := ctx.Value("session").(*auth.UserSession); ok && session != nil {
		return NormalizeClientIP(session.ClientIP)
	}
	return ""
}

func NormalizeClientIP(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	if host, _, err := net.SplitHostPort(trimmed); err == nil {
		return strings.TrimSpace(host)
	}

	return trimmed
}
