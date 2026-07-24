package cdm

import (
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// approvedActiveItem is the lean shape other modules (policy builder pickers) consume.
type approvedActiveItem struct {
	VariableID   string `json:"variable_id"`
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
	// ModuleCode/SubModuleCode are resolved via domain_catalog.field.cdm_path
	// (== cdm_variable.name) — see domaincatalog/upsert.go's sync. `domain`
	// alone is too coarse to tell FD and MF apart (both map to "investment"),
	// which let FD-scoped pickers show MF fields and vice versa.
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
}

func HandleListApprovedActive(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT c.variable_id::text, c.name, c.data_type, c.unit, c.label, c.description, c.domain,
			       COALESCE(c.source_system, ''), COALESCE(c.canonical_ref, ''), COALESCE(c.user_alias, ''), c.nullable,
			       COALESCE(sm.module_code, ''), COALESCE(f.sub_module_code, '')
			FROM policyengine_svc.cdm_variable c
			LEFT JOIN domain_catalog.field f ON f.cdm_path = c.name AND f.is_deleted = false
			LEFT JOIN domain_catalog.sub_module sm ON sm.sub_module_code = f.sub_module_code AND sm.is_deleted = false
			WHERE c.is_deleted = false AND c.status = 'Active' AND c.processing_status = 'APPROVED'
			` + common.CdmListOrderByAliased)
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
				&it.Domain, &it.SourceSystem, &it.CanonicalRef, &it.UserAlias, &it.Nullable,
				&it.ModuleCode, &it.SubModuleCode); err != nil {
				api.LogErrorForResponse(w, "cdm list-approved-active scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list CDM variables", "CDM_LIST_FAILED")
				return
			}
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Approved active CDM variables fetched", out)
	}
}
