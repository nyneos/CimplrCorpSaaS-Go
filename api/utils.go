package api

import (
	"CimplrCorpSaas/api/constants"
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

// Error response helper
func RespondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Println("[ERROR]", errMsg)
	Error(w, status, errMsg)
}

// RespondWithResult sends a consistent JSON response for success or error
func RespondWithResult(w http.ResponseWriter, success bool, errMsg string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	if success {
		log.Println("[INFO] RespondWithResult success")
		Success(w, http.StatusOK, map[string]interface{}{}, "")
	} else {
		// Set appropriate error status code based on error type
		status := http.StatusInternalServerError
		if strings.Contains(errMsg, "duplicate") || strings.Contains(errMsg, "invalid") || strings.Contains(errMsg, "required") {
			status = http.StatusBadRequest // 400
		} else if strings.Contains(errMsg, "limit exceeded") || strings.Contains(errMsg, "validation") {
			status = http.StatusUnprocessableEntity // 422
		} else if strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "session") {
			status = http.StatusUnauthorized // 401
		}
		log.Println("[ERROR] RespondWithResult", errMsg)
		Error(w, status, errMsg)
	}
}

// RespondWithPayload sends a consistent JSON response and includes an arbitrary payload
func RespondWithPayload(w http.ResponseWriter, success bool, errMsg string, payload interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	if !success {
		status := http.StatusOK
		if errMsg != "" {
			log.Println("[ERROR] RespondWithPayload", errMsg)
			// Set appropriate error status code
			status = http.StatusInternalServerError
			if strings.Contains(errMsg, "duplicate") || strings.Contains(errMsg, "invalid") || strings.Contains(errMsg, "required") {
				status = http.StatusBadRequest // 400
			} else if strings.Contains(errMsg, "limit exceeded") || strings.Contains(errMsg, "validation") {
				status = http.StatusUnprocessableEntity // 422
			} else if strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "session") {
				status = http.StatusUnauthorized // 401
			}
		}
		Error(w, status, errMsg)
		return
	}
	if payload == nil {
		payload = map[string]interface{}{}
	} else {
		log.Println("[INFO] RespondWithPayload payload included")
	}
	Success(w, http.StatusOK, payload, "")
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
