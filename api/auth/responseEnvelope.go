package auth

import (
	"encoding/json"
	"net/http"

	"CimplrCorpSaas/api/constants"
)

// This file mirrors the CLAUDE.md envelope shape implemented canonically in
// api/envelope.go (RespondEnvelopeSuccess/RespondEnvelopeSuccessCompat/
// RespondEnvelopeError/RespondEnvelopeFailureCompat).
//
// api/auth CANNOT import "CimplrCorpSaas/api" to call those directly: the
// root api package already imports api/auth (see api/gateway.go,
// api/cashMiddleware.go, api/middleware.go, api/contextHelpers.go,
// api/requestMetadata.go), so api/auth -> api would be a Go import cycle.
//
// These functions produce a byte-for-byte identical JSON shape to their
// api/envelope.go counterparts. If envelope.go's shape ever changes, update
// this file to match. The proper long-term fix is extracting envelope.go
// into a dependency-free leaf package (e.g. api/envelope) that both the root
// api package and api/auth can import without a cycle — out of scope for
// this change.

const authEnvelopeTraceIDHeader = "X-Trace-Id"

func authEnvelopeWriteJSON(w http.ResponseWriter, v interface{}) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// authEnvelopeSuccess writes the CLAUDE.md standard success envelope.
func authEnvelopeSuccess(w http.ResponseWriter, message string, data interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	authEnvelopeWriteJSON(w, map[string]interface{}{
		"success":    true,
		"statusCode": http.StatusOK,
		"message":    message,
		"data":       data,
	})
}

// authEnvelopeSuccessCompat writes the success envelope while also
// flattening fields onto the top level for backward compatibility, matching
// RespondEnvelopeSuccessCompat.
func authEnvelopeSuccessCompat(w http.ResponseWriter, message string, fields map[string]interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	body := map[string]interface{}{
		"success":    true,
		"statusCode": http.StatusOK,
		"message":    message,
		"data":       fields,
	}
	for k, v := range fields {
		if _, reserved := body[k]; !reserved {
			body[k] = v
		}
	}
	authEnvelopeWriteJSON(w, body)
}

func authEnvelopeErrorBody(w http.ResponseWriter, code, message string) map[string]string {
	body := map[string]string{
		"code":    code,
		"details": message,
	}
	if traceID := w.Header().Get(authEnvelopeTraceIDHeader); traceID != "" {
		body["trace_id"] = traceID
	}
	return body
}

func authEnvelopeErrorCode(status int) string {
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

// authEnvelopeError writes the CLAUDE.md standard error envelope.
func authEnvelopeError(w http.ResponseWriter, status int, message, code string) {
	authEnvelopeFailureCompat(w, status, message, code, nil)
}

// authEnvelopeFailureCompat is authEnvelopeSuccessCompat's error-path twin,
// matching RespondEnvelopeFailureCompat.
func authEnvelopeFailureCompat(w http.ResponseWriter, status int, message, code string, fields map[string]interface{}) {
	if code == "" {
		code = authEnvelopeErrorCode(status)
	}
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	body := map[string]interface{}{
		"success":    false,
		"statusCode": status,
		"message":    message,
		"error":      authEnvelopeErrorBody(w, code, message),
	}
	if fields != nil {
		body["data"] = fields
		for k, v := range fields {
			if _, reserved := body[k]; !reserved {
				body[k] = v
			}
		}
	}
	authEnvelopeWriteJSON(w, body)
}
