package cdm

import (
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type detailReq struct {
	VariableID string `json:"variable_id"`
}

func HandleDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req detailReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.VariableID = strings.TrimSpace(req.VariableID)
		if req.VariableID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "variable_id is required", "VALIDATION_ERROR")
			return
		}

		var it Item
		var createdAt, lastModifiedAt time.Time
		err := pool.QueryRow(r.Context(), `
			SELECT variable_id::text, name, data_type, unit, label, description, domain,
			       COALESCE(source_system, ''), nullable, status, processing_status,
			       COALESCE(created_by, ''), created_at, COALESCE(last_modified_by, ''), last_modified_at
			FROM policyengine_svc.cdm_variable
			WHERE variable_id = $1::uuid AND is_deleted = false`, req.VariableID,
		).Scan(&it.VariableID, &it.Name, &it.DataType, &it.Unit, &it.Label, &it.Description,
			&it.Domain, &it.SourceSystem, &it.Nullable, &it.Status, &it.ProcessingStatus,
			&it.CreatedBy, &createdAt, &it.LastModifiedBy, &lastModifiedAt)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "CDM variable not found", "NOT_FOUND")
			return
		}
		it.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		it.LastModifiedAt = lastModifiedAt.UTC().Format(time.RFC3339)
		api.RespondEnvelopeSuccess(w, "CDM variable fetched", it)
	}
}
