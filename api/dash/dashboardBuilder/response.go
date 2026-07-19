package dashboardbuilder

import (
	"encoding/json"
	"net/http"

	"CimplrCorpSaas/internal/logger"
)

func respondSuccess(w http.ResponseWriter, status int, message string, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":    true,
		"statusCode": status,
		"message":    message,
		"data":       data,
	})
}

func respondError(w http.ResponseWriter, status int, message, code, details string) {
	if details == "" {
		details = message
	}
	logger.LogError("dashboard-builder: %s (%s)", message, details)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":    false,
		"statusCode": status,
		"message":    message,
		"error": map[string]string{
			"code":    code,
			"details": details,
		},
	})
}

func respondBuilderError(w http.ResponseWriter, status int, details string) {
	message, code := builderErrorMeta(status, details)
	respondError(w, status, message, code, details)
}

func builderErrorMeta(status int, details string) (message, code string) {
	switch status {
	case http.StatusUnauthorized:
		return "Authentication failed", "AUTH_SESSION_EXPIRED"
	case http.StatusBadRequest:
		return "Bad request", "BAD_REQUEST"
	case http.StatusNotFound:
		return "Resource not found", "NOT_FOUND"
	case http.StatusMethodNotAllowed:
		return "Method not allowed", "METHOD_NOT_ALLOWED"
	default:
		if status >= http.StatusInternalServerError {
			return "Internal server error", "INTERNAL_ERROR"
		}
		return "Request failed", "REQUEST_FAILED"
	}
}
