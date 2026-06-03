package api

import (
	"net"
	"net/http"
	"strings"
)

func ClientIPFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}

	if forwardedFor := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwardedFor != "" {
		parts := strings.Split(forwardedFor, ",")
		if first := strings.TrimSpace(parts[0]); first != "" {
			return first
		}
	}

	if realIP := strings.TrimSpace(r.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}

	if host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr)); err == nil {
		return host
	}

	return strings.TrimSpace(r.RemoteAddr)
}
