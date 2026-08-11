package common

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"CimplrCorpSaas/api"
)

func RespondMethodNotAllowed(w http.ResponseWriter) {
	api.RespondEnvelopeError(w, http.StatusMethodNotAllowed, "method not allowed — use POST", "METHOD_NOT_ALLOWED")
}

func DecodeJSON(r *http.Request, dst interface{}) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(dst)
}

func RequirePOST(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		RespondMethodNotAllowed(w)
		return false
	}
	return true
}

// IsDMSEnabled is the single master on/off switch for the whole DMS feature.
// Unset or any value other than true/1/yes/on means disabled — matches the
// existing DMS_ENABLED parsing in api/dms/control/health.go, kept here as the
// one shared implementation.
func IsDMSEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("DMS_ENABLED")))
	return v == "true" || v == "1" || v == "yes" || v == "on"
}

// RequireDMSEnabled blocks an HTTP handler with a 403 when DMS_ENABLED is off.
// Every DMS action-triggering endpoint (run, adhoc generate, dispatch) must
// call this first — this is enforcement, not just the status-light display.
func RequireDMSEnabled(w http.ResponseWriter) bool {
	if !IsDMSEnabled() {
		api.RespondEnvelopeError(w, http.StatusForbidden, "DMS is disabled at application level (DMS_ENABLED is not set to true)", "DMS_DISABLED")
		return false
	}
	return true
}

// NullIfEmpty returns nil for a blank string so it binds as SQL NULL instead of "".
func NullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// PendingProcessingStatuses lists the maker-checker states awaiting a checker decision.
var PendingProcessingStatuses = []string{"PENDING_APPROVAL", "PENDING_EDIT_APPROVAL", "PENDING_DELETE_APPROVAL"}

// IsPendingStatus reports whether a processing_status value is awaiting checker action.
func IsPendingStatus(status string) bool {
	for _, p := range PendingProcessingStatuses {
		if status == p {
			return true
		}
	}
	return false
}
