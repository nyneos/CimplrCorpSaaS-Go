package api

import (
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

var auditISTLocation = time.FixedZone("IST", 5*60*60+30*60)

// ActionAuditInfo holds audit info for a record
type ActionAuditInfo struct {
	CreatedBy string
	CreatedAt string
	EditedBy  string
	EditedAt  string
	DeletedBy string
	DeletedAt string
}

// GetAuditInfo parses audit action fields and returns audit info for create/edit
func GetAuditInfo(actionType string, requestedBy *string, requestedAt *time.Time) ActionAuditInfo {
	info := ActionAuditInfo{}
	switch actionType {
	case constants.AuditActionCreate:
		info.CreatedBy = getPtrString(requestedBy)
		info.CreatedAt = getPtrTime(requestedAt)
	case constants.AuditActionEdit:
		info.EditedBy = getPtrString(requestedBy)
		info.EditedAt = getPtrTime(requestedAt)
	case constants.AuditActionDelete:
		info.DeletedBy = getPtrString(requestedBy)
		info.DeletedAt = getPtrTime(requestedAt)
	}
	return info
}

// getPtrString returns empty string for nil pointer
func getPtrString(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}

// getPtrTime returns formatted time for non-nil pointer
func getPtrTime(t *time.Time) string {
	if t != nil {
		return t.Format(constants.DateTimeFormat)
	}
	return ""
}


func FormatAuditTimestampIST(t time.Time) string {
	return t.In(auditISTLocation).Format(constants.DateTimeFormat)
}


func FormatAuditTimestampPtrIST(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return FormatAuditTimestampIST(*t)
}


func FormatAuditTimestampNullIST(t sql.NullTime) interface{} {
	if !t.Valid {
		return nil
	}
	return FormatAuditTimestampIST(t.Time)
}

// Helper to determine overall success for bulk operations
func IsBulkSuccess(results []map[string]interface{}) bool {
	for _, r := range results {
		if success, ok := r[constants.ValueSuccess].(bool); !ok || !success {
			return false
		}
	}
	return true
}

// Error response helper
func RespondWithError(w http.ResponseWriter, status int, errMsg string) {
	logger.LogError("%s", errMsg)

	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: false,
		constants.ValueError:   errMsg,
	})
}

// RespondWithResult sends a consistent JSON response for success or error
func RespondWithResult(w http.ResponseWriter, success bool, errMsg string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)

	if success {
		w.WriteHeader(http.StatusOK) // 200
		logger.LogInfo("RespondWithResult success")
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true})
	} else {
		// Set appropriate error status code based on error type
		if strings.Contains(errMsg, "duplicate") || strings.Contains(errMsg, "invalid") || strings.Contains(errMsg, "required") {
			w.WriteHeader(http.StatusBadRequest) // 400
		} else if strings.Contains(errMsg, "limit exceeded") || strings.Contains(errMsg, "validation") {
			w.WriteHeader(http.StatusUnprocessableEntity) // 422
		} else if strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "session") {
			w.WriteHeader(http.StatusUnauthorized) // 401
		} else {
			w.WriteHeader(http.StatusInternalServerError) // 500
		}
		logger.LogError("RespondWithResult: %s", errMsg)
		json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: errMsg})
	}
}

// RespondWithPayload sends a consistent JSON response and includes an arbitrary payload
func RespondWithPayload(w http.ResponseWriter, success bool, errMsg string, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	resp := map[string]interface{}{constants.ValueSuccess: success}

	if !success {
		errMsg = promotePayloadErrors(errMsg, payload)
	}

	if !success && errMsg != "" {
		resp[constants.ValueError] = errMsg
		logger.LogError("RespondWithPayload: %s", errMsg)
		// Set appropriate error status code
		if strings.Contains(errMsg, "duplicate") || strings.Contains(errMsg, "invalid") || strings.Contains(errMsg, "required") {
			w.WriteHeader(http.StatusBadRequest) // 400
		} else if strings.Contains(errMsg, "limit exceeded") || strings.Contains(errMsg, "validation") {
			w.WriteHeader(http.StatusUnprocessableEntity) // 422
		} else if strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "session") {
			w.WriteHeader(http.StatusUnauthorized) // 401
		} else {
			w.WriteHeader(http.StatusInternalServerError) // 500
		}
	} else {
		w.WriteHeader(http.StatusOK) // 200
	}
	if payload != nil {
		// use a conventional key `rows` for list payloads
		resp["rows"] = payload
		logger.LogInfo("RespondWithPayload: payload included")
	}
	json.NewEncoder(w).Encode(resp)
}

func promotePayloadErrors(errMsg string, payload interface{}) string {
	details := payloadErrorStrings(payload)
	if len(details) == 0 {
		return errMsg
	}
	return BulkActionErrorMessage(errMsg, details)
}

func BulkActionErrorMessage(summary string, details []string) string {
	clean := compactErrorStrings(details)
	if len(clean) == 0 {
		return summary
	}
	detail := strings.Join(clean, "; ")
	if strings.TrimSpace(summary) == "" {
		return detail
	}
	if strings.Contains(summary, detail) || strings.Contains(summary, clean[0]) {
		return summary
	}
	return summary + ": " + detail
}

func payloadErrorStrings(payload interface{}) []string {
	switch vals := payload.(type) {
	case []string:
		return compactErrorStrings(vals)
	case []interface{}:
		return compactErrorStrings(errorStringsFromInterfaceSlice(vals))
	case []map[string]interface{}:
		return compactErrorStrings(errorStringsFromMapSlice(vals))
	}

	rowMap, ok := payload.(map[string]interface{})
	if !ok {
		return nil
	}
	if raw, ok := rowMap["errors"]; ok {
		switch vals := raw.(type) {
		case []string:
			return compactErrorStrings(vals)
		case []interface{}:
			return compactErrorStrings(errorStringsFromInterfaceSlice(vals))
		case []map[string]interface{}:
			return compactErrorStrings(errorStringsFromMapSlice(vals))
		default:
			return nil
		}
	}
	if raw, ok := rowMap["validation_errors"]; ok {
		switch vals := raw.(type) {
		case []interface{}:
			return compactErrorStrings(errorStringsFromInterfaceSlice(vals))
		case []map[string]interface{}:
			return compactErrorStrings(errorStringsFromMapSlice(vals))
		}
	}
	return nil
}

func errorStringsFromInterfaceSlice(vals []interface{}) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		switch item := v.(type) {
		case string:
			out = append(out, item)
		case map[string]interface{}:
			out = append(out, errorStringsFromMap(item)...)
		}
	}
	return out
}

func errorStringsFromMapSlice(vals []map[string]interface{}) []string {
	out := make([]string, 0, len(vals))
	for _, item := range vals {
		out = append(out, errorStringsFromMap(item)...)
	}
	return out
}

func errorStringsFromMap(row map[string]interface{}) []string {
	for _, key := range []string{constants.ValueError, "message", "reason"} {
		if raw, ok := row[key]; ok {
			if s, ok := raw.(string); ok {
				return []string{s}
			}
		}
	}
	return nil
}

func compactErrorStrings(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// LogInfo logs an informational message (wrapper for consistent logging)
func LogInfo(msg string, args ...interface{}) {
	logger.LogInfo(msg, args...)
}

// LogError logs an error message (wrapper for consistent logging)
func LogError(msg string, args ...interface{}) {
	logger.LogError(msg, args...)
}
