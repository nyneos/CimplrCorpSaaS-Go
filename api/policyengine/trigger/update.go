package trigger

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

const errMsgUpdateTrigger = "failed to update trigger"

type updateReq struct {
	EventCode             string `json:"event_code"`
	TimingCategory        string `json:"timing_category"`
	Description           string `json:"description"`
	AllowsHardBlock       bool   `json:"allows_hard_block"`
	AllowsSoftWarning     bool   `json:"allows_soft_warning"`
	AllowsTriggerApproval bool   `json:"allows_trigger_approval"`
	AllowsNotifyOnly      bool   `json:"allows_notify_only"`
	ActorID               string `json:"actor_id"`
	Reason                string `json:"reason"`
}

// HandleUpdate stages a new EDIT for breach-action flags (and description/timing if sent).
// Create is disabled for triggers — edit exists so checkers can change allowed breach actions.
// Pending is allowed: always inserts a fresh EDIT audit; older audits are not rewritten.
func HandleUpdate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req updateReq
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
		if req.AllowsTriggerApproval && common.ForbidsTriggerApproval(req.EventCode) {
			api.RespondEnvelopeError(w, http.StatusBadRequest,
				"TriggerApproval is not allowed on approve/reject events (that would loop approvals)",
				"VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "trigger update begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errMsgUpdateTrigger, "TRIGGER_UPDATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var old Item
		err = tx.QueryRow(r.Context(), `
			SELECT timing_category, description, allows_hard_block, allows_soft_warning,
			       allows_trigger_approval, allows_notify_only, processing_status
			FROM policyengine_svc.trigger_event
			WHERE event_code = $1 AND is_deleted = false
			FOR UPDATE`, req.EventCode,
		).Scan(&old.TimingCategory, &old.Description, &old.AllowsHardBlock, &old.AllowsSoftWarning,
			&old.AllowsTriggerApproval, &old.AllowsNotifyOnly, &old.ProcessingStatus)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "trigger event not found", "NOT_FOUND")
			return
		}

		_, err = tx.Exec(r.Context(), `
			UPDATE policyengine_svc.trigger_event
			SET timing_category = $1, description = $2, allows_hard_block = $3, allows_soft_warning = $4,
			    allows_trigger_approval = $5, allows_notify_only = $6,
			    processing_status = 'PENDING_EDIT_APPROVAL', last_modified_by = $7, last_modified_at = now()
			WHERE event_code = $8`,
			req.TimingCategory, req.Description, req.AllowsHardBlock, req.AllowsSoftWarning,
			req.AllowsTriggerApproval, req.AllowsNotifyOnly, actor, req.EventCode,
		)
		if err != nil {
			api.LogErrorForResponse(w, "trigger update exec: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errMsgUpdateTrigger, "TRIGGER_UPDATE_FAILED")
			return
		}

		_, err = tx.Exec(r.Context(), `
			INSERT INTO policyengine_svc.trigger_event_audit (
				event_code, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
				old_timing_category, new_timing_category, old_description, new_description,
				old_allows_hard_block, new_allows_hard_block, old_allows_soft_warning, new_allows_soft_warning,
				old_allows_trigger_approval, new_allows_trigger_approval, old_allows_notify_only, new_allows_notify_only,
				old_is_deleted, new_is_deleted
			) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, now(), $4,
				$5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, false, false)`,
			req.EventCode, common.NullIfEmpty(req.Reason), actor, common.NullIfEmpty(ip),
			old.TimingCategory, req.TimingCategory, old.Description, req.Description,
			old.AllowsHardBlock, req.AllowsHardBlock, old.AllowsSoftWarning, req.AllowsSoftWarning,
			old.AllowsTriggerApproval, req.AllowsTriggerApproval, old.AllowsNotifyOnly, req.AllowsNotifyOnly,
		)
		if err != nil {
			api.LogErrorForResponse(w, "trigger update audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit trigger update", "TRIGGER_UPDATE_FAILED")
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "trigger update commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errMsgUpdateTrigger, "TRIGGER_UPDATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Trigger event edit submitted for approval", map[string]string{"event_code": req.EventCode})
	}
}
