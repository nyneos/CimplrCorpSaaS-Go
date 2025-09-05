package api

import (
	"encoding/json"
	"log"
	"net/http"
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
		return t.Format("2006-01-02 15:04:05")
	}
	return ""
}

// Helper to determine overall success for bulk operations
func IsBulkSuccess(results []map[string]interface{}) bool {
	for _, r := range results {
		if success, ok := r["success"].(bool); !ok || !success {
			return false
		}
	}
	return true
}

// Error response helper
func RespondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Println("[ERROR]", errMsg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

// RespondWithResult sends a consistent JSON response for success or error
func RespondWithResult(w http.ResponseWriter, success bool, errMsg string) {
	w.Header().Set("Content-Type", "application/json")
	if success {
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": errMsg})
	}
}
