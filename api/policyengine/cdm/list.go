package cdm

import (
	"net/http"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Item is the full CDM variable view: canonical columns + processing_status +
// maker/checker audit-trail scalars (created_by/at, last_modified_by/at).
type Item struct {
	VariableID       string `json:"variable_id"`
	Name             string `json:"name"`
	DataType         string `json:"data_type"`
	Unit             string `json:"unit"`
	Label            string `json:"label"`
	Description      string `json:"description"`
	Domain           string `json:"domain"`
	SourceSystem     string `json:"source_system"`
	Nullable         bool   `json:"nullable"`
	Status           string `json:"status"`
	ProcessingStatus string `json:"processing_status"`
	CreatedBy        string `json:"created_by"`
	CreatedAt        string `json:"created_at"`
	LastModifiedBy   string `json:"last_modified_by"`
	LastModifiedAt   string `json:"last_modified_at"`
}

func HandleList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT variable_id::text, name, data_type, unit, label, description, domain,
			       COALESCE(source_system, ''), nullable, status, processing_status,
			       COALESCE(created_by, ''), created_at, COALESCE(last_modified_by, ''), last_modified_at
			FROM policyengine_svc.cdm_variable
			WHERE is_deleted = false
			ORDER BY domain, name`)
		if err != nil {
			api.LogErrorForResponse(w, "cdm list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list CDM variables", "CDM_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]Item, 0)
		for rows.Next() {
			var it Item
			var createdAt, lastModifiedAt time.Time
			if err := rows.Scan(&it.VariableID, &it.Name, &it.DataType, &it.Unit, &it.Label, &it.Description,
				&it.Domain, &it.SourceSystem, &it.Nullable, &it.Status, &it.ProcessingStatus,
				&it.CreatedBy, &createdAt, &it.LastModifiedBy, &lastModifiedAt); err != nil {
				api.LogErrorForResponse(w, "cdm list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list CDM variables", "CDM_LIST_FAILED")
				return
			}
			it.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			it.LastModifiedAt = lastModifiedAt.UTC().Format(time.RFC3339)
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "CDM variables fetched", out)
	}
}
