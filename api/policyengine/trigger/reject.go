package trigger

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleReject bulk-rejects pending CREATE/EDIT/DELETE requests. A rejected
// EDIT reverts the master row to the audit row's old_* values; a rejected
// DELETE simply leaves is_deleted false; a rejected CREATE keeps the row
// visible with processing_status=REJECTED for history.
func HandleReject(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req decisionReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		codes := decisionCodes(req)
		if len(codes) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "event_codes is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "trigger reject begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject trigger events", "TRIGGER_REJECT_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		rejected := make([]string, 0, len(codes))
		errs := make([]string, 0)
		for _, raw := range codes {
			code := strings.ToUpper(strings.TrimSpace(raw))
			if code == "" {
				continue
			}
			var auditID, actionType string
			var oldTiming, oldDescription *string
			var oldHardBlock, oldSoftWarning, oldTriggerApproval, oldNotifyOnly *bool
			err := tx.QueryRow(r.Context(), `
				SELECT audit_id::text, action_type, old_timing_category, old_description,
				       old_allows_hard_block, old_allows_soft_warning, old_allows_trigger_approval, old_allows_notify_only
				FROM policyengine_svc.trigger_event_audit
				WHERE event_code = $1 AND processing_status = ANY($2::text[])
				ORDER BY requested_at DESC
				LIMIT 1
				FOR UPDATE`, code, common.PendingProcessingStatuses,
			).Scan(&auditID, &actionType, &oldTiming, &oldDescription,
				&oldHardBlock, &oldSoftWarning, &oldTriggerApproval, &oldNotifyOnly)
			if err != nil {
				errs = append(errs, code+": no pending request")
				continue
			}
			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.trigger_event_audit
				SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
				WHERE audit_id = $4::uuid`,
				actor, common.NullIfEmpty(ip), common.NullIfEmpty(req.CheckerComment), auditID,
			); err != nil {
				errs = append(errs, code+": "+err.Error())
				continue
			}

			if actionType == "EDIT" && oldTiming != nil {
				if _, err := tx.Exec(r.Context(), `
					UPDATE policyengine_svc.trigger_event
					SET timing_category = $1, description = $2,
					    allows_hard_block = COALESCE($3,false), allows_soft_warning = COALESCE($4,false),
					    allows_trigger_approval = COALESCE($5,true), allows_notify_only = COALESCE($6,true),
					    processing_status = 'REJECTED', last_modified_by = $7, last_modified_at = now()
					WHERE event_code = $8`,
					oldTiming, oldDescription, oldHardBlock, oldSoftWarning, oldTriggerApproval, oldNotifyOnly, actor, code,
				); err != nil {
					errs = append(errs, code+": "+err.Error())
					continue
				}
			} else {
				if _, err := tx.Exec(r.Context(), `
					UPDATE policyengine_svc.trigger_event
					SET processing_status = 'REJECTED', last_modified_by = $1, last_modified_at = now()
					WHERE event_code = $2`,
					actor, code,
				); err != nil {
					errs = append(errs, code+": "+err.Error())
					continue
				}
			}
			rejected = append(rejected, code)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "trigger reject commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject trigger events", "TRIGGER_REJECT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Trigger events rejected", map[string]interface{}{
			"rejected": rejected,
			"errors":   errs,
		})
	}
}
