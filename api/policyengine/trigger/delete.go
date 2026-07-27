package trigger

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type deleteReq struct {
	EventCodes []string `json:"event_codes"`
	IDs        []string `json:"ids"`
	Reason     string   `json:"reason"`
	ActorID    string   `json:"actor_id"`
}

// HandleDelete raises PENDING_DELETE_APPROVAL + DELETE audit only.
// Soft-delete happens on approve. Older audits are not rewritten.
func HandleDelete(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req deleteReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		codes := req.EventCodes
		if len(codes) == 0 {
			codes = req.IDs
		}
		if len(codes) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "event_codes is required", "VALIDATION_ERROR")
			return
		}
		for i := range codes {
			codes[i] = strings.ToUpper(strings.TrimSpace(codes[i]))
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "trigger delete begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request trigger delete", "TRIGGER_DELETE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		requested := make([]string, 0, len(codes))
		skipped := make([]string, 0)
		errs := make([]string, 0)

		for _, code := range codes {
			if code == "" {
				continue
			}
			var processingStatus string
			err := tx.QueryRow(r.Context(), `
				SELECT processing_status
				FROM policyengine_svc.trigger_event
				WHERE event_code = $1 AND is_deleted = false
				FOR UPDATE`, code,
			).Scan(&processingStatus)
			if err != nil {
				skipped = append(skipped, code)
				continue
			}
			if processingStatus == "PENDING_DELETE_APPROVAL" {
				skipped = append(skipped, code)
				continue
			}

			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.trigger_event
				SET processing_status = 'PENDING_DELETE_APPROVAL',
				    last_modified_by = $1, last_modified_at = now()
				WHERE event_code = $2`, actor, code); err != nil {
				errs = append(errs, code+": "+err.Error())
				continue
			}
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO policyengine_svc.trigger_event_audit (
					event_code, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
					old_is_deleted, new_is_deleted
				) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now(), $4, false, true)`,
				code, common.NullIfEmpty(req.Reason), actor, common.NullIfEmpty(ip)); err != nil {
				errs = append(errs, code+": "+err.Error())
				continue
			}
			requested = append(requested, code)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "trigger delete commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request trigger delete", "TRIGGER_DELETE_FAILED")
			return
		}

		api.RespondEnvelopeSuccess(w, "Trigger event delete submitted for approval", map[string]interface{}{
			"requested": requested,
			"skipped":   skipped,
			"errors":    errs,
		})
	}
}
