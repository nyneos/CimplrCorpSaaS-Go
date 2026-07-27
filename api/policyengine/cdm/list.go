package cdm

import (
	"net/http"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Item is the full CDM variable view: stable name + friendly label + source mapping.
type Item struct {
	VariableID       string `json:"variable_id"`
	Name             string `json:"name"`
	DataType         string `json:"data_type"`
	Unit             string `json:"unit"`
	Label            string `json:"label"`
	Description      string `json:"description"`
	Domain           string `json:"domain"`
	SourceSystem     string `json:"source_system"`
	CanonicalRef     string `json:"canonical_ref"`
	UserAlias        string `json:"user_alias"`
	Nullable         bool   `json:"nullable"`
	Status           string `json:"status"`
	ProcessingStatus string `json:"processing_status"`
	CreatedBy        string `json:"created_by"`
	CreatedAt        string `json:"created_at"`
	LastModifiedBy   string `json:"last_modified_by"`
	LastModifiedAt   string `json:"last_modified_at"`
	// Module/sub-module from domain_catalog (field.cdm_path or source_system = sub_module_code).
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
}

// List/detail join catalog so Module + Sub-module dropdowns can populate on view/edit.
const itemSelectSQL = `
	SELECT c.variable_id::text, c.name, c.data_type, c.unit, c.label, c.description, c.domain,
	       COALESCE(c.source_system, ''), COALESCE(c.canonical_ref, ''), COALESCE(c.user_alias, ''),
	       c.nullable, c.status, c.processing_status,
	       COALESCE(c.created_by, ''), c.created_at, COALESCE(c.last_modified_by, ''), c.last_modified_at,
	       COALESCE(sm_field.module_code, sm_src.module_code, ''),
	       COALESCE(NULLIF(f.sub_module_code, ''), NULLIF(c.source_system, ''), '')
	FROM policyengine_svc.cdm_variable c
	LEFT JOIN domain_catalog.field f
	       ON f.cdm_path = c.name AND f.is_deleted = false
	LEFT JOIN domain_catalog.sub_module sm_field
	       ON sm_field.sub_module_code = f.sub_module_code AND sm_field.is_deleted = false
	LEFT JOIN domain_catalog.sub_module sm_src
	       ON sm_src.sub_module_code = c.source_system AND sm_src.is_deleted = false
`

func scanItem(it *Item, createdAt, lastModifiedAt *time.Time) []interface{} {
	return []interface{}{
		&it.VariableID, &it.Name, &it.DataType, &it.Unit, &it.Label, &it.Description, &it.Domain,
		&it.SourceSystem, &it.CanonicalRef, &it.UserAlias,
		&it.Nullable, &it.Status, &it.ProcessingStatus,
		&it.CreatedBy, createdAt, &it.LastModifiedBy, lastModifiedAt,
		&it.ModuleCode, &it.SubModuleCode,
	}
}

func HandleList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), itemSelectSQL+`
			WHERE c.is_deleted = false
			`+common.CdmListOrderByAliased)
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
			if err := rows.Scan(scanItem(&it, &createdAt, &lastModifiedAt)...); err != nil {
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
