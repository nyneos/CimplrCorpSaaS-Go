package cdm

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type decisionReq struct {
	IDs            []string `json:"ids"`
	CheckerComment string   `json:"checker_comment"`
	ActorID        string   `json:"actor_id"`
}

// HandleApprove bulk-approves pending CREATE/EDIT/DELETE requests: the latest
// pending audit row per variable_id is closed out, and the master row's
// processing_status becomes APPROVED (is_deleted also flips true for a delete request).
func HandleApprove(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req decisionReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		if len(req.IDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "ids is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "cdm approve begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to approve CDM variables", "CDM_APPROVE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		approved := make([]string, 0, len(req.IDs))
		errs := make([]string, 0)
		for _, rawID := range req.IDs {
			id := strings.TrimSpace(rawID)
			if id == "" {
				continue
			}
			var auditID, actionType string
			err := tx.QueryRow(r.Context(), `
				SELECT audit_id::text, action_type
				FROM policyengine_svc.cdm_variable_audit
				WHERE variable_id = $1::uuid AND processing_status = ANY($2::text[])
				ORDER BY requested_at DESC
				LIMIT 1
				FOR UPDATE`, id, common.PendingProcessingStatuses,
			).Scan(&auditID, &actionType)
			if err != nil {
				errs = append(errs, id+": no pending request")
				continue
			}
			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.cdm_variable_audit
				SET processing_status = 'APPROVED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
				WHERE audit_id = $4::uuid`,
				actor, common.NullIfEmpty(ip), common.NullIfEmpty(req.CheckerComment), auditID,
			); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}
			isDeleted := actionType == "DELETE"
			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.cdm_variable
				SET processing_status = 'APPROVED', is_deleted = ($1 OR is_deleted), last_modified_by = $2, last_modified_at = now()
				WHERE variable_id = $3::uuid`,
				isDeleted, actor, id,
			); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}
			approved = append(approved, id)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "cdm approve commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to approve CDM variables", "CDM_APPROVE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "CDM variables approved", map[string]interface{}{
			"approved": approved,
			"errors":   errs,
		})
	}
}
