package api

import (
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

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
	case "CREATE":
		info.CreatedBy = getPtrString(requestedBy)
		info.CreatedAt = getPtrTime(requestedAt)
	case "EDIT":
		info.EditedBy = getPtrString(requestedBy)
		info.EditedAt = getPtrTime(requestedAt)
	case "DELETE":
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

// Helper to determine overall success for bulk operations
func IsBulkSuccess(results []map[string]interface{}) bool {
	for _, r := range results {
		if success, ok := r[constants.ValueSuccess].(bool); !ok || !success {
			return false
		}
	}
	return true
}

func stableErrorMessage(status int) string {
	switch status {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return "invalid request"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusNotFound:
		return "not found"
	default:
		if status >= http.StatusInternalServerError {
			return "internal server error"
		}
		return "request failed"
	}
}

// Error response helper
func RespondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Printf("[ERROR: %v] [API] [Response] request failed", errMsg)
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: false,
		constants.ValueError:   stableErrorMessage(status),
	}); err != nil {
		log.Printf("[ERROR: %v] [API] [Response] failed to encode error response", err)
	}
}

// RespondWithResult sends a consistent JSON response for success or error
func RespondWithResult(w http.ResponseWriter, success bool, errMsg string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	if success {
		w.WriteHeader(http.StatusOK) // 200
		log.Println("[INFO] RespondWithResult success")
		if err := json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: true}); err != nil {
			log.Printf("[ERROR: %v] [API] [Response] failed to encode success result", err)
		}
	} else {
		// Set appropriate error status code based on error type
		status := http.StatusInternalServerError
		if strings.Contains(errMsg, "duplicate") || strings.Contains(errMsg, "invalid") || strings.Contains(errMsg, "required") {
			status = http.StatusBadRequest
		} else if strings.Contains(errMsg, "limit exceeded") || strings.Contains(errMsg, "validation") {
			status = http.StatusUnprocessableEntity
		} else if strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "session") {
			status = http.StatusUnauthorized
		}
		w.WriteHeader(status)
		log.Printf("[ERROR: %v] [API] [Response] request failed", errMsg)
		if err := json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: stableErrorMessage(status)}); err != nil {
			log.Printf("[ERROR: %v] [API] [Response] failed to encode error result", err)
		}
	}
}

// RespondWithPayload sends a consistent JSON response and includes an arbitrary payload
func RespondWithPayload(w http.ResponseWriter, success bool, errMsg string, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	resp := map[string]interface{}{constants.ValueSuccess: success}
	if !success && errMsg != "" {
		status := http.StatusInternalServerError
		log.Printf("[ERROR: %v] [API] [Response] request failed", errMsg)
		// Set appropriate error status code
		if strings.Contains(errMsg, "duplicate") || strings.Contains(errMsg, "invalid") || strings.Contains(errMsg, "required") {
			status = http.StatusBadRequest
		} else if strings.Contains(errMsg, "limit exceeded") || strings.Contains(errMsg, "validation") {
			status = http.StatusUnprocessableEntity
		} else if strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "session") {
			status = http.StatusUnauthorized
		}
		resp[constants.ValueError] = stableErrorMessage(status)
		w.WriteHeader(status)
	} else {
		w.WriteHeader(http.StatusOK) // 200
	}
	if payload != nil {
		// use a conventional key `rows` for list payloads
		resp["rows"] = payload
		log.Println("[INFO] RespondWithPayload payload included")
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[ERROR: %v] [API] [Response] failed to encode payload response", err)
	}
}

// LogInfo logs an informational message (wrapper for consistent logging)
func LogInfo(msg string, args ...interface{}) {
	if len(args) > 0 {
		log.Printf("[INFO] "+msg, args...)
	} else {
		log.Println("[INFO]", msg)
	}
}

// LogError logs an error message (wrapper for consistent logging)
func LogError(msg string, args ...interface{}) {
	if len(args) > 0 {
		log.Printf("[ERROR] "+msg, args...)
	} else {
		log.Println("[ERROR]", msg)
	}
}
