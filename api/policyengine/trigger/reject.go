package trigger

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleReject bulk-rejects pending CREATE/EDIT/DELETE requests.
// EDIT reject does NOT revert master columns — only processing_status.
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
			err := tx.QueryRow(r.Context(), `
				SELECT audit_id::text, action_type
				FROM policyengine_svc.trigger_event_audit
				WHERE event_code = $1 AND processing_status = ANY($2::text[])
				ORDER BY requested_at DESC
				LIMIT 1
				FOR UPDATE`, code, common.PendingProcessingStatuses,
			).Scan(&auditID, &actionType)
			if err != nil {
				errs = append(errs, code+": no pending request")
				continue
			}
			_ = actionType
			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.trigger_event_audit
				SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
				WHERE audit_id = $4::uuid`,
				actor, common.NullIfEmpty(ip), common.NullIfEmpty(req.CheckerComment), auditID,
			); err != nil {
				errs = append(errs, code+": "+err.Error())
				continue
			}

			if _, err := tx.Exec(r.Context(), `
					UPDATE policyengine_svc.trigger_event
					SET processing_status = 'REJECTED', last_modified_by = $1, last_modified_at = now()
					WHERE event_code = $2`,
				actor, code,
			); err != nil {
				errs = append(errs, code+": "+err.Error())
				continue
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
