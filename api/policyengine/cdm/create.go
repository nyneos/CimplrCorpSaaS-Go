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
	CanonicalRef string `json:"canonical_ref"`
	UserAlias    string `json:"user_alias"`
	Nullable     bool   `json:"nullable"`
	ActorID      string `json:"actor_id"`
}

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
		req.CanonicalRef = strings.TrimSpace(req.CanonicalRef)
		req.UserAlias = strings.TrimSpace(req.UserAlias)
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
				(name, data_type, unit, label, description, domain, source_system,
				 canonical_ref, user_alias, nullable, status, processing_status, created_by, last_modified_by)
			VALUES ($1,$2,COALESCE($3,''),$4,$5,$6,NULLIF($7,''),COALESCE($8,''),NULLIF($9,''),$10,'Active','PENDING_APPROVAL',$11,$11)
			RETURNING variable_id::text`,
			req.Name, req.DataType, req.Unit, req.Label, req.Description, req.Domain, req.SourceSystem,
			req.CanonicalRef, req.UserAlias, req.Nullable, actor,
		).Scan(&id)
		if err != nil {
			api.LogErrorForResponse(w, "cdm create insert: %v", err)
			api.RespondEnvelopeError(w, http.StatusConflict, "failed to create CDM variable (duplicate name or user_alias?)", "CDM_CREATE_FAILED")
			return
		}

		if req.UserAlias != "" {
			_, _ = tx.Exec(r.Context(), `
				INSERT INTO policyengine_svc.cdm_variable_alias (variable_id, alias_name, created_by)
				SELECT $1::uuid, $2, $3
				WHERE NOT EXISTS (
					SELECT 1 FROM policyengine_svc.cdm_variable_alias
					WHERE is_deleted = false AND alias_name = $2
				)`, id, req.UserAlias, actor)
		}

		_, err = tx.Exec(r.Context(), `
			INSERT INTO policyengine_svc.cdm_variable_audit (
				variable_id, action_type, processing_status, requested_by, requested_at, requested_ip,
				new_name, new_data_type, new_unit, new_label, new_description, new_domain,
				new_source_system, new_canonical_ref, new_user_alias, new_nullable, new_status, new_is_deleted
			) VALUES ($1::uuid, 'CREATE', 'PENDING_APPROVAL', $2, now(), $3,
				$4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'Active', false)`,
			id, actor, common.NullIfEmpty(ip),
			req.Name, req.DataType, req.Unit, req.Label, req.Description, req.Domain, req.SourceSystem,
			req.CanonicalRef, common.NullIfEmpty(req.UserAlias), req.Nullable,
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
		api.RespondEnvelopeSuccess(w, "CDM variable submitted for approval", map[string]string{"variable_id": id})
	}
}
