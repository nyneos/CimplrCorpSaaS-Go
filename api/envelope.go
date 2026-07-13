package api

import (
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"net/http"
)

// RespondEnvelopeSuccess writes the CLAUDE.md standard success envelope.
func RespondEnvelopeSuccess(w http.ResponseWriter, message string, data interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	writeEnvelopeJSON(w, map[string]interface{}{
		constants.ValueSuccess: true,
		"statusCode":           http.StatusOK,
		"message":              message,
		"data":                 data,
	})
}

// RespondEnvelopeError writes the CLAUDE.md standard error envelope.
func RespondEnvelopeError(w http.ResponseWriter, status int, message, code string) {
	RespondEnvelopeFailureWithData(w, status, message, code, nil)
}

// RespondEnvelopeFailureWithData writes an error envelope with optional data (e.g. validation results).
func RespondEnvelopeFailureWithData(w http.ResponseWriter, status int, message, code string, data interface{}) {
	if code == "" {
		code = EnvelopeErrorCode(status)
	}
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	body := map[string]interface{}{
		constants.ValueSuccess: false,
		"statusCode":           status,
		"message":              message,
		constants.ValueError: map[string]string{
			"code":    code,
			"details": message,
		},
	}
	if data != nil {
		body["data"] = data
	}
	writeEnvelopeJSON(w, body)
}

// EnvelopeErrorCode maps HTTP status to a stable error code for the envelope.
func EnvelopeErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusUnauthorized:
		return "AUTH_SESSION_EXPIRED"
	case http.StatusForbidden:
		return "FORBIDDEN"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusUnprocessableEntity:
		return "VALIDATION_FAILED"
	default:
		if status >= http.StatusInternalServerError {
			return "INTERNAL_ERROR"
		}
		return "REQUEST_FAILED"
	}
}

func writeEnvelopeJSON(w http.ResponseWriter, v interface{}) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
