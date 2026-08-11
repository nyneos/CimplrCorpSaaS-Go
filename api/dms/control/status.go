package control

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	dmscommon "CimplrCorpSaas/api/dms/common"
	"CimplrCorpSaas/internal/services/docsvc"
)

// HandleStatus proxies Document-Service quota/hard-stop for UI banners.
// Always live — DMS_GENERATION_CONTROL_UI used to make this optional, but
// DMS_ENABLED (api/dms/common.RequireDMSEnabled) is now the real master
// switch, so gating whether the UI even asks for quota status added nothing.
func HandleStatus(w http.ResponseWriter, r *http.Request) {
	if !dmscommon.RequirePOST(w, r) {
		return
	}
	if !dmscommon.IsDMSEnabled() {
		api.RespondEnvelopeSuccess(w, "DMS disabled", map[string]interface{}{
			"ui_enabled":         true,
			"allowed":            false,
			"generation_enabled": false,
			"error_code":         "DMS_DISABLED",
			"message":            "DMS is disabled at application level (DMS_ENABLED is not set to true)",
		})
		return
	}
	client := docsvc.NewFromEnv()
	q, err := client.QuotaCheck(r.Context())
	if err != nil {
		code := "DOCUMENT_SERVICE_UNAVAILABLE"
		status := http.StatusServiceUnavailable
		if strings.Contains(err.Error(), "DOCUMENT_SERVICE_UNAUTHORIZED") {
			code = "DOCUMENT_SERVICE_UNAUTHORIZED"
			status = http.StatusUnauthorized
		} else if strings.Contains(err.Error(), "not configured") {
			code = "DOCUMENT_SERVICE_MISCONFIGURED"
		}
		api.RespondEnvelopeError(w, status, err.Error(), code)
		return
	}
	api.RespondEnvelopeSuccess(w, "generation control status", map[string]interface{}{
		"ui_enabled":         true,
		"allowed":            q.Allowed,
		"generation_enabled": q.GenerationEnabled,
		"error_code":         q.ErrorCode,
		"message":            q.Message,
		"month_count":        q.MonthCount,
		"month_limit":        q.MonthLimit,
		"hour_count":         q.HourCount,
		"hour_limit":         q.HourLimit,
	})
}
