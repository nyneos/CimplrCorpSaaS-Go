package common

import (
	"encoding/json"
	"net/http"
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
