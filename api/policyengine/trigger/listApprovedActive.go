package trigger

import (
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type approvedActiveItem struct {
	EventCode             string `json:"event_code"`
	TimingCategory        string `json:"timing_category"`
	Description           string `json:"description"`
	AllowsHardBlock       bool   `json:"allows_hard_block"`
	AllowsSoftWarning     bool   `json:"allows_soft_warning"`
	AllowsTriggerApproval bool   `json:"allows_trigger_approval"`
	AllowsNotifyOnly      bool   `json:"allows_notify_only"`
}

// HandleListApprovedActive returns checker-approved, non-deleted trigger events —
// this is the picker source for the policy builder's trigger-event multi-select.
func HandleListApprovedActive(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT event_code, timing_category, description,
			       allows_hard_block, allows_soft_warning, allows_trigger_approval, allows_notify_only
			FROM policyengine_svc.trigger_event
			WHERE is_deleted = false AND processing_status = 'APPROVED'
			ORDER BY event_code`)
		if err != nil {
			api.LogErrorForResponse(w, "trigger list-approved-active: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list trigger events", "TRIGGER_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]approvedActiveItem, 0)
		for rows.Next() {
			var it approvedActiveItem
			if err := rows.Scan(&it.EventCode, &it.TimingCategory, &it.Description,
				&it.AllowsHardBlock, &it.AllowsSoftWarning, &it.AllowsTriggerApproval, &it.AllowsNotifyOnly); err != nil {
				api.LogErrorForResponse(w, "trigger list-approved-active scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list trigger events", "TRIGGER_LIST_FAILED")
				return
			}
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Approved active trigger events fetched", out)
	}
}
