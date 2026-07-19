package cdm

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
		if len(req.IDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "ids is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "cdm reject begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject CDM variables", "CDM_REJECT_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		rejected := make([]string, 0, len(req.IDs))
		errs := make([]string, 0)
		for _, rawID := range req.IDs {
			id := strings.TrimSpace(rawID)
			if id == "" {
				continue
			}
			var auditID, actionType string
			var oldName, oldDataType, oldUnit, oldLabel, oldDescription, oldDomain, oldSourceSystem, oldStatus *string
			var oldNullable *bool
			err := tx.QueryRow(r.Context(), `
				SELECT audit_id::text, action_type, old_name, old_data_type, old_unit, old_label,
				       old_description, old_domain, old_source_system, old_nullable, old_status
				FROM policyengine_svc.cdm_variable_audit
				WHERE variable_id = $1::uuid AND processing_status = ANY($2::text[])
				ORDER BY requested_at DESC
				LIMIT 1
				FOR UPDATE`, id, common.PendingProcessingStatuses,
			).Scan(&auditID, &actionType, &oldName, &oldDataType, &oldUnit, &oldLabel,
				&oldDescription, &oldDomain, &oldSourceSystem, &oldNullable, &oldStatus)
			if err != nil {
				errs = append(errs, id+": no pending request")
				continue
			}
			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.cdm_variable_audit
				SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
				WHERE audit_id = $4::uuid`,
				actor, common.NullIfEmpty(ip), common.NullIfEmpty(req.CheckerComment), auditID,
			); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}

			if actionType == "EDIT" && oldName != nil {
				if _, err := tx.Exec(r.Context(), `
					UPDATE policyengine_svc.cdm_variable
					SET name = $1, data_type = $2, unit = COALESCE($3,''), label = $4, description = $5, domain = $6,
					    source_system = $7, nullable = COALESCE($8,false), status = COALESCE($9,status),
					    processing_status = 'REJECTED', last_modified_by = $10, last_modified_at = now()
					WHERE variable_id = $11::uuid`,
					oldName, oldDataType, oldUnit, oldLabel, oldDescription, oldDomain,
					oldSourceSystem, oldNullable, oldStatus, actor, id,
				); err != nil {
					errs = append(errs, id+": "+err.Error())
					continue
				}
			} else {
				if _, err := tx.Exec(r.Context(), `
					UPDATE policyengine_svc.cdm_variable
					SET processing_status = 'REJECTED', last_modified_by = $1, last_modified_at = now()
					WHERE variable_id = $2::uuid`,
					actor, id,
				); err != nil {
					errs = append(errs, id+": "+err.Error())
					continue
				}
			}
			rejected = append(rejected, id)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "cdm reject commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject CDM variables", "CDM_REJECT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "CDM variables rejected", map[string]interface{}{
			"rejected": rejected,
			"errors":   errs,
		})
	}
}
