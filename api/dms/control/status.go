package control

import (
	"net/http"
	"os"
	"strings"

	"CimplrCorpSaas/api"
	dmscommon "CimplrCorpSaas/api/dms/common"
	"CimplrCorpSaas/internal/services/docsvc"
)

func controlUIEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("DMS_GENERATION_CONTROL_UI")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

// HandleStatus proxies Document-Service quota/hard-stop for UI banners.
func HandleStatus(w http.ResponseWriter, r *http.Request) {
	if !dmscommon.RequirePOST(w, r) {
		return
	}
	if !controlUIEnabled() {
		api.RespondEnvelopeSuccess(w, "generation control UI disabled", map[string]interface{}{
			"ui_enabled": false,
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
