package trigger

import (
	"net/http"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Item is the full trigger-event view: canonical columns + processing_status +
// maker/checker audit-trail scalars.
type Item struct {
	EventCode             string `json:"event_code"`
	TimingCategory        string `json:"timing_category"`
	Description           string `json:"description"`
	AllowsHardBlock       bool   `json:"allows_hard_block"`
	AllowsSoftWarning     bool   `json:"allows_soft_warning"`
	AllowsTriggerApproval bool   `json:"allows_trigger_approval"`
	AllowsNotifyOnly      bool   `json:"allows_notify_only"`
	ProcessingStatus      string `json:"processing_status"`
	CreatedBy             string `json:"created_by"`
	CreatedAt             string `json:"created_at"`
	LastModifiedBy        string `json:"last_modified_by"`
	LastModifiedAt        string `json:"last_modified_at"`
}

func HandleList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT event_code, timing_category, description,
			       allows_hard_block, allows_soft_warning, allows_trigger_approval, allows_notify_only,
			       processing_status, COALESCE(created_by, ''), created_at, COALESCE(last_modified_by, ''), last_modified_at
			FROM policyengine_svc.trigger_event
			WHERE is_deleted = false
			` + common.TriggerListOrderBy)
		if err != nil {
			api.LogErrorForResponse(w, "trigger list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list trigger events", "TRIGGER_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]Item, 0)
		for rows.Next() {
			var it Item
			var createdAt, lastModifiedAt time.Time
			if err := rows.Scan(&it.EventCode, &it.TimingCategory, &it.Description,
				&it.AllowsHardBlock, &it.AllowsSoftWarning, &it.AllowsTriggerApproval, &it.AllowsNotifyOnly,
				&it.ProcessingStatus, &it.CreatedBy, &createdAt, &it.LastModifiedBy, &lastModifiedAt); err != nil {
				api.LogErrorForResponse(w, "trigger list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list trigger events", "TRIGGER_LIST_FAILED")
				return
			}
			it.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			it.LastModifiedAt = lastModifiedAt.UTC().Format(time.RFC3339)
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Trigger events fetched", out)
	}
}
