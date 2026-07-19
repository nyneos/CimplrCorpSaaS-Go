package cdm

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type createReq struct {
	Name         string `json:"name"`
	DataType     string `json:"data_type"`
	Unit         string `json:"unit"`
	Label        string `json:"label"`
	Description  string `json:"description"`
	Domain       string `json:"domain"`
	SourceSystem string `json:"source_system"`
	Nullable     bool   `json:"nullable"`
	ActorID      string `json:"actor_id"`
}

// HandleCreate submits a new CDM variable for checker approval — it is never
// auto-approved. The row is visible immediately with processing_status=PENDING_APPROVAL.
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
		req.Name = strings.TrimSpace(req.Name)
		req.Label = strings.TrimSpace(req.Label)
		req.Domain = strings.TrimSpace(req.Domain)
		req.DataType = strings.TrimSpace(req.DataType)
		req.Description = strings.TrimSpace(req.Description)
		if req.Name == "" || req.Label == "" || req.Domain == "" || req.DataType == "" || req.Description == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "name, label, domain, data_type, description are required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "cdm create begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create CDM variable", "CDM_CREATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var id string
		err = tx.QueryRow(r.Context(), `
			INSERT INTO policyengine_svc.cdm_variable
				(name, data_type, unit, label, description, domain, source_system, nullable, status, processing_status, created_by, last_modified_by)
			VALUES ($1,$2,COALESCE($3,''),$4,$5,$6,NULLIF($7,''),$8,'Active','PENDING_APPROVAL',$9,$9)
			RETURNING variable_id::text`,
			req.Name, req.DataType, req.Unit, req.Label, req.Description, req.Domain, req.SourceSystem, req.Nullable, actor,
		).Scan(&id)
		if err != nil {
			api.LogErrorForResponse(w, "cdm create insert: %v", err)
			api.RespondEnvelopeError(w, http.StatusConflict, "failed to create CDM variable (duplicate name?)", "CDM_CREATE_FAILED")
			return
		}

		_, err = tx.Exec(r.Context(), `
			INSERT INTO policyengine_svc.cdm_variable_audit (
				variable_id, action_type, processing_status, requested_by, requested_at, requested_ip,
				new_name, new_data_type, new_unit, new_label, new_description, new_domain,
				new_source_system, new_nullable, new_status, new_is_deleted
			) VALUES ($1::uuid, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3,
				$4, $5, $6, $7, $8, $9, $10, $11, 'Active', false)`,
			id, actor, common.NullIfEmpty(ip),
			req.Name, req.DataType, req.Unit, req.Label, req.Description, req.Domain, req.SourceSystem, req.Nullable,
		)
		if err != nil {
			api.LogErrorForResponse(w, "cdm create audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit CDM create", "CDM_CREATE_FAILED")
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "cdm create commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create CDM variable", "CDM_CREATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "CDM variable submitted for approval", map[string]string{"variable_id": id, "name": req.Name})
	}
}
