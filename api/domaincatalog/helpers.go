package domaincatalog

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
)

func requirePOST(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		api.RespondEnvelopeError(w, http.StatusMethodNotAllowed, "method not allowed — use POST", "METHOD_NOT_ALLOWED")
		return false
	}
	return true
}

func decodeJSON(r *http.Request, dst interface{}) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(dst)
}

func normalizeConsumer(s string) string {
	return strings.ToUpper(strings.TrimSpace(s))
}

func sanitizeNotifKey(s string) string {
	up := strings.ToUpper(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range up {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
