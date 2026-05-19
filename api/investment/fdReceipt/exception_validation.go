package fdReceipt

import (
	"errors"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgconn"
)

// DB check fd_irex_resolution_check allows only these values (varchar 50).
var allowedProposedResolutions = map[string]bool{
	"ACCEPT":           true,
	"ADJUST_ACCRUAL":   true,
	"RAISE_BANK_CLAIM": true,
	"RECLASSIFY":       true,
}

// Legacy / UI aliases mapped to DB values before write.
var proposedResolutionAliases = map[string]string{
	"ACCEPT_VARIANCE": "ACCEPT",
	"BANK_CLAIM":      "RAISE_BANK_CLAIM",
	"RAISE_BANK":      "RAISE_BANK_CLAIM",
}

const proposedResolutionAllowedMsg = "proposed_resolution must be one of: ACCEPT, ADJUST_ACCRUAL, RAISE_BANK_CLAIM, RECLASSIFY"

func normalizeProposedResolution(raw string) (string, bool) {
	s := strings.ToUpper(strings.TrimSpace(raw))
	if s == "" {
		return "", false
	}
	if canon, ok := proposedResolutionAliases[s]; ok {
		s = canon
	}
	return s, allowedProposedResolutions[s]
}

func proposedResolutionFormOptions() []map[string]string {
	return []map[string]string{
		{"value": "ACCEPT", "label": "Accept variance (allows posting after close)"},
		{"value": "ADJUST_ACCRUAL", "label": "Adjust accrual / schedule"},
		{"value": "RAISE_BANK_CLAIM", "label": "Raise bank claim"},
		{"value": "RECLASSIFY", "label": "Reclassify"},
	}
}

func reasonCodeFormOptions() []map[string]string {
	return []map[string]string{
		{"value": "TIMING_DIFFERENCE", "label": "Timing difference"},
		{"value": "ROUNDING", "label": "Rounding"},
		{"value": "BANK_ERROR", "label": "Bank error"},
		{"value": "RATE_MISMATCH", "label": "Rate mismatch"},
		{"value": "MISSING_ACCRUAL", "label": "Missing accrual"},
		{"value": "OTHER", "label": "Other"},
	}
}

func exceptionResolveFormOptions() map[string]interface{} {
	return map[string]interface{}{
		"proposed_resolution": proposedResolutionFormOptions(),
		"reason_code":         reasonCodeFormOptions(),
		"notes": []string{
			"Use POST /exception/resolve only to update proposed_resolution, reason_code, resolution_remarks, attachment.",
			"Receipt amount/period edits use POST /receipt/update (separate approval flow).",
			proposedResolutionAllowedMsg,
		},
	}
}

func validateExceptionResolveInput(proposedResolution, reasonCode, resolutionRemarks string) (canonicalResolution string, errMsg string) {
	canonical, ok := normalizeProposedResolution(proposedResolution)
	if !ok {
		return "", proposedResolutionAllowedMsg + " (ACCEPT_VARIANCE is accepted as alias for ACCEPT)"
	}
	if strings.TrimSpace(reasonCode) == "" {
		return "", "reason_code is required"
	}
	if strings.TrimSpace(resolutionRemarks) == "" {
		return "", "resolution_remarks is required"
	}
	return canonical, ""
}

func respondExceptionWriteError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.ConstraintName {
		case "fd_irex_resolution_check":
			api.RespondWithError(w, http.StatusBadRequest, proposedResolutionAllowedMsg)
			return
		case "fd_irex_status_check":
			api.RespondWithError(w, http.StatusBadRequest, "invalid exception_status for this action")
			return
		case "fd_irex_type_check":
			api.RespondWithError(w, http.StatusBadRequest, "invalid exception_type on record")
			return
		case "fd_irex_severity_check":
			api.RespondWithError(w, http.StatusBadRequest, "invalid severity on record")
			return
		case "fd_irex_result_type_check":
			api.RespondWithError(w, http.StatusBadRequest, "invalid result_type on record")
			return
		}
		switch pgErr.Code {
		case "23514":
			api.RespondWithError(w, http.StatusBadRequest, "value violates business rules; check proposed_resolution and status")
			return
		case "23505":
			api.RespondWithError(w, http.StatusConflict, "an open exception already exists for this reconcile result")
			return
		}
	}
	api.RespondWithError(w, http.StatusInternalServerError, constants.ErrUpdateFailed+err.Error())
}
