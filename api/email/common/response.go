package emailcommon

import (
	"encoding/json"
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
)

type responseEnvelope struct {
	Success    bool        `json:"success"`
	StatusCode int         `json:"statusCode"`
	Message    string      `json:"message"`
	Data       interface{} `json:"data,omitempty"`
	Error      interface{} `json:"error,omitempty"`
}

type responseError struct {
	Code    string `json:"code"`
	Details string `json:"details"`
}

func RespondMethodNotAllowed(w http.ResponseWriter) {
	respondError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", constants.ErrMethodNotAllowed)
}

func RespondBadRequest(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusBadRequest, "BAD_REQUEST", msg)
}

func RespondUnauthorized(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusUnauthorized, "UNAUTHORIZED", msg)
}

func RespondForbidden(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusForbidden, "FORBIDDEN", msg)
}

func RespondNotFound(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusNotFound, "NOT_FOUND", msg)
}

func RespondUnavailable(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", msg)
}

func RespondInternal(w http.ResponseWriter, msg string) {
	respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", msg)
}

func RespondPayload(w http.ResponseWriter, logLabel string, payload interface{}) {
	api.LogInfo("email %s: ok", logLabel)
	respondSuccess(w, http.StatusOK, "OK", payload)
}

func RespondList(w http.ResponseWriter, logLabel string, rows interface{}, count int) {
	api.LogInfo("email %s: rows=%d", logLabel, count)
	if rows == nil {
		rows = []interface{}{}
	}
	respondSuccess(w, http.StatusOK, "OK", map[string]interface{}{
		"rows":  rows,
		"count": count,
	})
}

func RespondBulk(w http.ResponseWriter, logLabel, resultKey string, ok []string, errs []string) {
	api.LogInfo("email %s: %s=%d errors=%d", logLabel, resultKey, len(ok), len(errs))
	respondSuccess(w, http.StatusOK, "OK", map[string]interface{}{
		resultKey: ok,
		"errors":  errs,
	})
}

func RespondFailPayload(w http.ResponseWriter, logLabel, errMsg string, payload interface{}) {
	api.LogError("email %s: %s", logLabel, errMsg)
	respondErrorWithData(w, http.StatusBadRequest, "EMAIL_OPERATION_FAILED", errMsg, payload)
}

func respondSuccess(w http.ResponseWriter, statusCode int, message string, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(responseEnvelope{
		Success:    true,
		StatusCode: statusCode,
		Message:    message,
		Data:       payload,
	})
}

func respondError(w http.ResponseWriter, statusCode int, code, details string) {
	respondErrorWithData(w, statusCode, code, details, nil)
}

func respondErrorWithData(w http.ResponseWriter, statusCode int, code, details string, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(responseEnvelope{
		Success:    false,
		StatusCode: statusCode,
		Message:    details,
		Data:       payload,
		Error: responseError{
			Code:    code,
			Details: details,
		},
	})
}
