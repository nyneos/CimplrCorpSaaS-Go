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

// HandleDelete raises a delete request (PENDING_DELETE_APPROVAL) — the row is
// only soft-deleted once a checker approves it.
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

		rows, err := pool.Query(r.Context(), `
			UPDATE policyengine_svc.trigger_event
			SET processing_status = 'PENDING_DELETE_APPROVAL', last_modified_by = $2, last_modified_at = now()
			WHERE event_code = ANY($1::varchar[]) AND is_deleted = false AND processing_status = 'APPROVED'
			RETURNING event_code`,
			codes, actor)
		if err != nil {
			api.LogErrorForResponse(w, "trigger delete: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request trigger delete", "TRIGGER_DELETE_FAILED")
			return
		}
		affected := make([]string, 0, len(codes))
		for rows.Next() {
			var code string
			if err := rows.Scan(&code); err != nil {
				rows.Close()
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to request trigger delete", "TRIGGER_DELETE_FAILED")
				return
			}
			affected = append(affected, code)
		}
		rows.Close()

		for _, code := range affected {
			_, err = pool.Exec(r.Context(), `
				INSERT INTO policyengine_svc.trigger_event_audit (
					event_code, action_type, processing_status, reason, requested_by, requested_at, requested_ip,
					old_is_deleted, new_is_deleted
				) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now(), $4, false, true)`,
				code, common.NullIfEmpty(req.Reason), actor, common.NullIfEmpty(ip))
			if err != nil {
				api.LogErrorForResponse(w, "trigger delete audit: %v", err)
			}
		}

		skipped := make([]string, 0)
		for _, code := range codes {
			found := false
			for _, ok := range affected {
				if ok == code {
					found = true
					break
				}
			}
			if !found {
				skipped = append(skipped, code)
			}
		}
		api.RespondEnvelopeSuccess(w, "Trigger event delete requested", map[string]interface{}{
			"requested": affected,
			"skipped":   skipped,
		})
	}
}
