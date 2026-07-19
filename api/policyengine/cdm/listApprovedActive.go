package cdm

import (
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// approvedActiveItem is the lean shape other modules (e.g. the policy builder's
// variable picker) consume — only checker-approved, active, non-deleted rows.
type approvedActiveItem struct {
	VariableID   string `json:"variable_id"`
	Name         string `json:"name"`
	DataType     string `json:"data_type"`
	Unit         string `json:"unit"`
	Label        string `json:"label"`
	Description  string `json:"description"`
	Domain       string `json:"domain"`
	SourceSystem string `json:"source_system"`
	Nullable     bool   `json:"nullable"`
}

func HandleListApprovedActive(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT variable_id::text, name, data_type, unit, label, description, domain, COALESCE(source_system, ''), nullable
			FROM policyengine_svc.cdm_variable
			WHERE is_deleted = false AND status = 'Active' AND processing_status = 'APPROVED'
			ORDER BY domain, name`)
		if err != nil {
			api.LogErrorForResponse(w, "cdm list-approved-active: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list CDM variables", "CDM_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]approvedActiveItem, 0)
		for rows.Next() {
			var it approvedActiveItem
			if err := rows.Scan(&it.VariableID, &it.Name, &it.DataType, &it.Unit, &it.Label, &it.Description,
				&it.Domain, &it.SourceSystem, &it.Nullable); err != nil {
				api.LogErrorForResponse(w, "cdm list-approved-active scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list CDM variables", "CDM_LIST_FAILED")
				return
			}
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Approved active CDM variables fetched", out)
	}
}
