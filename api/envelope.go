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
		constants.ValueError:   errorBody(w, code, message),
	}
	if data != nil {
		body["data"] = data
	}
	writeEnvelopeJSON(w, body)
}

// errorBody builds the error{code,details} object, adding trace_id (already
// stamped on w by the observability middleware) when present so a user can
// hand the trace ID back for debugging without exposing raw error internals.
func errorBody(w http.ResponseWriter, code, message string) map[string]string {
	body := map[string]string{
		"code":    code,
		"details": message,
	}
	if traceID := TraceIDFromWriter(w); traceID != "" {
		body["trace_id"] = traceID
	}
	return body
}

// RespondEnvelopeSuccessCompat writes the CLAUDE.md standard success envelope
// (success/statusCode/message/data — data genuinely contains fields) while ALSO
// flattening fields's keys onto the top level of the body, so any existing
// frontend code reading a legacy flat field (e.g. response.data.summary from
// before this endpoint was migrated) keeps working unchanged alongside new code
// reading response.data.data.summary. Use this instead of building a raw
// map[string]interface{} by hand at a call site.
func RespondEnvelopeSuccessCompat(w http.ResponseWriter, message string, fields map[string]interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	body := map[string]interface{}{
		constants.ValueSuccess: true,
		"statusCode":           http.StatusOK,
		"message":              message,
		"data":                 fields,
	}
	for k, v := range fields {
		if _, reserved := body[k]; !reserved {
			body[k] = v
		}
	}
	writeEnvelopeJSON(w, body)
}

// RespondEnvelopeFailureCompat is RespondEnvelopeSuccessCompat's error-path twin:
// success/statusCode/message/error are set correctly, fields is nested under data
// AND flattened onto the top level for the same backward-compatibility reason.
func RespondEnvelopeFailureCompat(w http.ResponseWriter, status int, message, code string, fields map[string]interface{}) {
	if code == "" {
		code = EnvelopeErrorCode(status)
	}
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	body := map[string]interface{}{
		constants.ValueSuccess: false,
		"statusCode":           status,
		"message":              message,
		constants.ValueError:   errorBody(w, code, message),
	}
	if fields != nil {
		body["data"] = fields
		for k, v := range fields {
			if _, reserved := body[k]; !reserved {
				body[k] = v
			}
		}
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
