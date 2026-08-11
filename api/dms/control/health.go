package control

import (
	"net/http"

	"CimplrCorpSaas/api"
	dmscommon "CimplrCorpSaas/api/dms/common"
	"CimplrCorpSaas/internal/services/docsvc"
)

// HandleServiceHealth pings Document-Service's /v1/health for the DMS status
// light + sidebar visibility gate. Always 200 with data.available — the
// unavailable case is a normal poll result, not an HTTP error.
func HandleServiceHealth(w http.ResponseWriter, r *http.Request) {
	if !dmscommon.RequirePOST(w, r) {
		return
	}

	if !dmscommon.IsDMSEnabled() {
		api.RespondEnvelopeSuccess(w, "document service health", map[string]interface{}{
			"available": false,
			"message":   "DMS is disabled at application level (DMS_ENABLED is not set to true)",
		})
		return
	}

	err := docsvc.NewFromEnv().Health(r.Context())
	available := err == nil
	message := "document service reachable"
	if !available {
		message = err.Error()
	}
	api.RespondEnvelopeSuccess(w, "document service health", map[string]interface{}{
		"available": available,
		"message":   message,
	})
}
