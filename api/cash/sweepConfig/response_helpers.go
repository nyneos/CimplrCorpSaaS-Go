package sweepconfig

import (
	"CimplrCorpSaas/api"
	"net/http"
	"strings"
)

func legacyStatus(errMsg string) int {
	msg := strings.ToLower(errMsg)
	switch {
	case strings.Contains(msg, "duplicate"), strings.Contains(msg, "invalid"), strings.Contains(msg, "required"), strings.Contains(msg, "missing"):
		return http.StatusBadRequest
	case strings.Contains(msg, "limit exceeded"), strings.Contains(msg, "validation"):
		return http.StatusUnprocessableEntity
	case strings.Contains(msg, "unauthorized"), strings.Contains(msg, "session"):
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

func ensureData(payload interface{}) interface{} {
	if payload == nil {
		return map[string]any{}
	}
	return payload
}


func respondWithResult(w http.ResponseWriter, success bool, result string) {
	if success {
		if strings.TrimSpace(result) == "" {
			api.Success(w, http.StatusOK, map[string]any{}, "")
			return
		}
		if strings.ContainsAny(result, " \t\r\n") {
			api.Success(w, http.StatusOK, map[string]any{}, result)
			return
		}
		api.Success(w, http.StatusOK, result, "")
		return
	}
	api.Error(w, legacyStatus(result), result)
}

func respondWithPayload(w http.ResponseWriter, success bool, message string, payload interface{}) {
	if success {
		api.Success(w, http.StatusOK, ensureData(payload), message)
		return
	}
	if strings.TrimSpace(message) == "" {
		message = "One or more items failed"
	}
	api.Error(w, legacyStatus(message), message)
}
