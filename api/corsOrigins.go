package api

import (
	"net/http"
	"os"
	"strings"
	"sync"
)

var (
	corsOriginsMu   sync.RWMutex
	corsOriginsList []string // empty = allow all (*) for backward compat until configured
)

// ConfigureCORSOrigins sets the allowlist from services.yaml / env.
// Empty list keeps legacy "*" behaviour.
func ConfigureCORSOrigins(origins []string) {
	cleaned := make([]string, 0, len(origins))
	seen := map[string]bool{}
	for _, o := range origins {
		o = strings.TrimRight(strings.TrimSpace(o), "/")
		if o == "" || seen[o] {
			continue
		}
		seen[o] = true
		cleaned = append(cleaned, o)
	}
	corsOriginsMu.Lock()
	corsOriginsList = cleaned
	corsOriginsMu.Unlock()
}

// LoadCORSOriginsFromEnv reads CORS_ALLOWED_ORIGINS (comma-separated).
func LoadCORSOriginsFromEnv() []string {
	raw := strings.TrimSpace(os.Getenv("CORS_ALLOWED_ORIGINS"))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, p)
	}
	return out
}

func corsAllowOriginForRequest(r *http.Request) string {
	corsOriginsMu.RLock()
	list := corsOriginsList
	corsOriginsMu.RUnlock()
	if len(list) == 0 {
		return allowOriginAll
	}
	origin := strings.TrimRight(strings.TrimSpace(r.Header.Get("Origin")), "/")
	if origin == "" {
		// Non-browser or same-origin proxy — echo first allowlist entry is wrong;
		// omit reflective origin; browsers without Origin don't need CORS.
		return list[0]
	}
	for _, allowed := range list {
		if strings.EqualFold(origin, allowed) {
			return origin
		}
	}
	// Not in allowlist — do not reflect attacker Origin.
	return ""
}

func applyCORSHeaders(w http.ResponseWriter, r *http.Request) {
	origin := corsAllowOriginForRequest(r)
	if origin != "" {
		w.Header().Set(headerAccessControlAllowOrigin, origin)
		if origin != allowOriginAll {
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}
	}
	w.Header().Set(headerAccessControlAllowMethods, allowMethodsAll)
	w.Header().Set(headerAccessControlAllowHeaders, allowHeadersAll)
	w.Header().Set(headerAccessControlExposeHeaders, "X-Trace-Id, X-Response-Enc")
}
