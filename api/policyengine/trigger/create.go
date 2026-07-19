package trigger

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type createReq struct {
	EventCode             string `json:"event_code"`
	TimingCategory        string `json:"timing_category"`
	Description           string `json:"description"`
	AllowsHardBlock       bool   `json:"allows_hard_block"`
	AllowsSoftWarning     bool   `json:"allows_soft_warning"`
	AllowsTriggerApproval bool   `json:"allows_trigger_approval"`
	AllowsNotifyOnly      bool   `json:"allows_notify_only"`
	ActorID               string `json:"actor_id"`
}

// HandleCreate submits a new trigger event for checker approval.
func HandleCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req createReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.EventCode = strings.ToUpper(strings.TrimSpace(req.EventCode))
		req.TimingCategory = strings.TrimSpace(req.TimingCategory)
		req.Description = strings.TrimSpace(req.Description)
		if req.EventCode == "" || req.TimingCategory == "" || req.Description == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "event_code, timing_category, description are required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)
		// Defaults for scheduled/async: only notify + approval.
		if req.TimingCategory == "Scheduled" || req.TimingCategory == "Async Event" || req.TimingCategory == "Asynchronous" {
			req.AllowsHardBlock = false
			req.AllowsSoftWarning = false
			if !req.AllowsTriggerApproval && !req.AllowsNotifyOnly {
				req.AllowsNotifyOnly = true
				req.AllowsTriggerApproval = true
			}
		}

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "trigger create begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create trigger", "TRIGGER_CREATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		_, err = tx.Exec(r.Context(), `
			INSERT INTO policyengine_svc.trigger_event (
				event_code, timing_category, description,
				allows_hard_block, allows_soft_warning, allows_trigger_approval, allows_notify_only,
				processing_status, created_by, last_modified_by
			) VALUES ($1,$2,$3,$4,$5,$6,$7,'PENDING_APPROVAL',$8,$8)`,
			req.EventCode, req.TimingCategory, req.Description,
			req.AllowsHardBlock, req.AllowsSoftWarning, req.AllowsTriggerApproval, req.AllowsNotifyOnly, actor,
		)
		if err != nil {
			api.LogErrorForResponse(w, "trigger create insert: %v", err)
			api.RespondEnvelopeError(w, http.StatusConflict, "failed to create trigger (duplicate code?)", "TRIGGER_CREATE_FAILED")
			return
		}
		_, err = tx.Exec(r.Context(), `
			INSERT INTO policyengine_svc.trigger_event_audit (
				event_code, action_type, processing_status, requested_by, requested_at, requested_ip,
				new_timing_category, new_description,
				new_allows_hard_block, new_allows_soft_warning, new_allows_trigger_approval, new_allows_notify_only,
				new_is_deleted
			) VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3, $4, $5, $6, $7, $8, $9, false)`,
			req.EventCode, actor, common.NullIfEmpty(ip),
			req.TimingCategory, req.Description,
			req.AllowsHardBlock, req.AllowsSoftWarning, req.AllowsTriggerApproval, req.AllowsNotifyOnly,
		)
		if err != nil {
			api.LogErrorForResponse(w, "trigger create audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit trigger create", "TRIGGER_CREATE_FAILED")
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "trigger create commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create trigger", "TRIGGER_CREATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Trigger event submitted for approval", map[string]string{"event_code": req.EventCode})
	}
}
