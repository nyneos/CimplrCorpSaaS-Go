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
	// Audit metadata resolved per action type from cdm_variable_audit
	// (CREATE → created_*, EDIT → edited_*, DELETE → deleted_*), so a variable
	// that was never edited reports an empty edited_at instead of created_at.
	CreatedBy      string `json:"created_by"`
	CreatedAt      string `json:"created_at"`
	EditedBy       string `json:"edited_by"`
	EditedAt       string `json:"edited_at"`
	DeletedBy      string `json:"deleted_by"`
	DeletedAt      string `json:"deleted_at"`
	LastModifiedBy string `json:"last_modified_by"`
	LastModifiedAt string `json:"last_modified_at"`
	RequestedBy    string `json:"requested_by"`
	RequestedAt    string `json:"requested_at"`
	CheckerBy      string `json:"checker_by"`
	CheckerAt      string `json:"checker_at"`
	CheckerComment string `json:"checker_comment"`
	// Module/sub-module from domain_catalog (field.cdm_path or source_system = sub_module_code).
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
}

// itemTimes are the nullable audit timestamps scanned alongside an Item.
type itemTimes struct {
	created      *time.Time
	edited       *time.Time
	deleted      *time.Time
	lastModified *time.Time
	requested    *time.Time
	checker      *time.Time
}

// List/detail join catalog so Module + Sub-module dropdowns can populate on
// view/edit, and the latest audit row per action type so the expand panel can
// show Requested / Edited / Deleted as distinct events.
const itemSelectSQL = `
	SELECT c.variable_id::text, c.name, c.data_type, c.unit, c.label, c.description, c.domain,
	       COALESCE(c.source_system, ''), COALESCE(c.canonical_ref, ''), COALESCE(c.user_alias, ''),
	       c.nullable, c.status, c.processing_status,
	       COALESCE(ac.requested_by, COALESCE(c.created_by, '')), COALESCE(ac.requested_at, c.created_at),
	       COALESCE(ae.requested_by, ''), ae.requested_at,
	       COALESCE(ad.requested_by, ''), ad.requested_at,
	       COALESCE(c.last_modified_by, ''), c.last_modified_at,
	       COALESCE(ar.requested_by, COALESCE(c.created_by, '')),
	       COALESCE(ar.requested_at, c.created_at),
	       COALESCE(ar.checker_by, ''), ar.checker_at,
	       COALESCE(ar.checker_comment, ''),
	       COALESCE(sm_field.module_code, sm_src.module_code, ''),
	       COALESCE(NULLIF(f.sub_module_code, ''), NULLIF(c.source_system, ''), '')
	FROM policyengine_svc.cdm_variable c
	LEFT JOIN domain_catalog.field f
	       ON f.cdm_path = c.name AND f.is_deleted = false
	LEFT JOIN domain_catalog.sub_module sm_field
	       ON sm_field.sub_module_code = f.sub_module_code AND sm_field.is_deleted = false
	LEFT JOIN domain_catalog.sub_module sm_src
	       ON sm_src.sub_module_code = c.source_system AND sm_src.is_deleted = false
	LEFT JOIN LATERAL (
		SELECT a.requested_by, a.requested_at
		FROM policyengine_svc.cdm_variable_audit a
		WHERE a.variable_id = c.variable_id AND a.action_type = 'CREATE'
		ORDER BY a.requested_at DESC LIMIT 1
	) ac ON true
	LEFT JOIN LATERAL (
		SELECT a.requested_by, a.requested_at
		FROM policyengine_svc.cdm_variable_audit a
		WHERE a.variable_id = c.variable_id AND a.action_type = 'EDIT'
		ORDER BY a.requested_at DESC LIMIT 1
	) ae ON true
	LEFT JOIN LATERAL (
		SELECT a.requested_by, a.requested_at
		FROM policyengine_svc.cdm_variable_audit a
		WHERE a.variable_id = c.variable_id AND a.action_type = 'DELETE'
		ORDER BY a.requested_at DESC LIMIT 1
	) ad ON true
	LEFT JOIN LATERAL (
		SELECT a.requested_by, a.requested_at, a.checker_by, a.checker_at,
		       COALESCE(a.checker_comment, '') AS checker_comment
		FROM policyengine_svc.cdm_variable_audit a
		WHERE a.variable_id = c.variable_id
		  AND a.action_type IN ('CREATE', 'EDIT', 'DELETE', 'DEPRECATE')
		ORDER BY a.requested_at DESC
		LIMIT 1
	) ar ON true
`

func scanItem(it *Item, ts *itemTimes) []interface{} {
	return []interface{}{
		&it.VariableID, &it.Name, &it.DataType, &it.Unit, &it.Label, &it.Description, &it.Domain,
		&it.SourceSystem, &it.CanonicalRef, &it.UserAlias,
		&it.Nullable, &it.Status, &it.ProcessingStatus,
		&it.CreatedBy, &ts.created,
		&it.EditedBy, &ts.edited,
		&it.DeletedBy, &ts.deleted,
		&it.LastModifiedBy, &ts.lastModified,
		&it.RequestedBy, &ts.requested,
		&it.CheckerBy, &ts.checker, &it.CheckerComment,
		&it.ModuleCode, &it.SubModuleCode,
	}
}

func applyItemTimes(it *Item, ts itemTimes) {
	it.CreatedAt = common.FormatAuditTime(ts.created)
	it.EditedAt = common.FormatAuditTime(ts.edited)
	it.DeletedAt = common.FormatAuditTime(ts.deleted)
	it.LastModifiedAt = common.FormatAuditTime(ts.lastModified)
	it.RequestedAt = common.FormatAuditTime(ts.requested)
	it.CheckerAt = common.FormatAuditTime(ts.checker)
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
			var ts itemTimes
			if err := rows.Scan(scanItem(&it, &ts)...); err != nil {
				api.LogErrorForResponse(w, "cdm list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list CDM variables", "CDM_LIST_FAILED")
				return
			}
			applyItemTimes(&it, ts)
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "CDM variables fetched", out)
	}
}
