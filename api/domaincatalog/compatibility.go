package domaincatalog

import (
	"net/http"
	"strconv"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

type compatAlias struct {
	ConsumerSystem string `json:"consumer_system"`
	AliasCode      string `json:"alias_code"`
	Level          string `json:"level"` // module | sub_module | part
	PartCode       string `json:"part_code,omitempty"`
}

type compatField struct {
	FieldCode    string `json:"field_code"`
	CDMPath      string `json:"cdm_path,omitempty"`
	Label        string `json:"label"`
	DataType     string `json:"data_type"`
	AliasKey     string `json:"alias_key"`
	Container    string `json:"container_name,omitempty"`
	UsageKind    string `json:"usage_kind"`
}

type compatConsumer struct {
	System       string         `json:"system"`
	ModuleAlias  string         `json:"module_alias,omitempty"`
	SubAlias     string         `json:"sub_module_alias,omitempty"`
	PartAliases  []compatAlias  `json:"part_aliases"`
	FieldAliases []compatField  `json:"field_aliases"`
	// DMS binding when system=DMS
	StorageModuleKey string `json:"storage_module_key,omitempty"`
	S3Prefix         string `json:"s3_prefix,omitempty"`
	ParentField      string `json:"parent_field,omitempty"`
	FilesTable       string `json:"files_table,omitempty"`
	APIPath          string `json:"api_path,omitempty"`
	Compatible       bool   `json:"compatible"`
	Notes            string `json:"notes,omitempty"`
}

type compatReport struct {
	ModuleCode    string           `json:"module_code"`
	ModuleName    string           `json:"module_name"`
	SubModuleCode string           `json:"sub_module_code"`
	SubModuleName string           `json:"sub_module_name"`
	Parts         []partRow        `json:"parts"`
	Consumers     []compatConsumer `json:"consumers"`
	Summary       string           `json:"summary"`
}

// HandleCompatibility shows how one sub-module (default FD_BOOKING) maps across
// POLICY / NOTIFICATION / APPROVAL / DASHBOARD / DMS — for end-to-end checks.
func HandleCompatibility(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req struct {
			SubModuleCode string `json:"sub_module_code"`
		}
		_ = decodeJSON(r, &req)
		sm := strings.TrimSpace(req.SubModuleCode)
		if sm == "" {
			sm = "FD_BOOKING"
		}

		var report compatReport
		err := pool.QueryRow(r.Context(), `
			SELECT m.module_code, m.module_name, s.sub_module_code, s.sub_module_name
			FROM domain_catalog.sub_module s
			JOIN domain_catalog.module m ON m.module_code = s.module_code
			WHERE s.is_deleted = false AND m.is_deleted = false AND s.sub_module_code = $1`, sm,
		).Scan(&report.ModuleCode, &report.ModuleName, &report.SubModuleCode, &report.SubModuleName)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "sub-module not found in domain catalog", "CATALOG_COMPAT_NOT_FOUND")
			return
		}

		partRows, err := pool.Query(r.Context(), `
			SELECT part_id::text, sub_module_code, part_code, part_name, description,
			       api_path, handler_name, storage_module_key, s3_prefix, parent_field, files_table, sort_order
			FROM domain_catalog.part
			WHERE is_deleted = false AND sub_module_code = $1
			ORDER BY sort_order, part_code`, sm)
		if err != nil {
			api.LogErrorForResponse(w, "compat parts: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to build compatibility report", "CATALOG_COMPAT_FAILED")
			return
		}
		defer partRows.Close()
		report.Parts = make([]partRow, 0)
		for partRows.Next() {
			var p partRow
			if err := partRows.Scan(&p.PartID, &p.SubModuleCode, &p.PartCode, &p.PartName, &p.Description,
				&p.APIPath, &p.HandlerName, &p.StorageModuleKey, &p.S3Prefix, &p.ParentField, &p.FilesTable, &p.SortOrder); err != nil {
				api.LogErrorForResponse(w, "compat part scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to build compatibility report", "CATALOG_COMPAT_FAILED")
				return
			}
			report.Parts = append(report.Parts, p)
		}

		systems := []string{"POLICY", "NOTIFICATION", "APPROVAL", "DASHBOARD", "DMS"}
		for _, sys := range systems {
			c := compatConsumer{System: sys, PartAliases: []compatAlias{}, FieldAliases: []compatField{}, Compatible: true}

			_ = pool.QueryRow(r.Context(), `
				SELECT alias_code FROM domain_catalog.module_alias
				WHERE is_deleted = false AND module_code = $1 AND consumer_system = $2
				ORDER BY alias_code LIMIT 1`, report.ModuleCode, sys).Scan(&c.ModuleAlias)

			_ = pool.QueryRow(r.Context(), `
				SELECT alias_code FROM domain_catalog.sub_module_alias
				WHERE is_deleted = false AND sub_module_code = $1 AND consumer_system = $2
				ORDER BY alias_code LIMIT 1`, sm, sys).Scan(&c.SubAlias)

			paRows, _ := pool.Query(r.Context(), `
				SELECT a.alias_code, p.part_code
				FROM domain_catalog.part_alias a
				JOIN domain_catalog.part p ON p.part_id = a.part_id
				WHERE a.is_deleted = false AND p.is_deleted = false
				  AND p.sub_module_code = $1 AND a.consumer_system = $2
				ORDER BY p.sort_order, a.alias_code`, sm, sys)
			if paRows != nil {
				for paRows.Next() {
					var alias, partCode string
					if err := paRows.Scan(&alias, &partCode); err == nil {
						c.PartAliases = append(c.PartAliases, compatAlias{
							ConsumerSystem: sys, AliasCode: alias, Level: "part", PartCode: partCode,
						})
					}
				}
				paRows.Close()
			}

			faRows, _ := pool.Query(r.Context(), `
				SELECT f.field_code, COALESCE(f.cdm_path,''), f.label, f.data_type,
				       a.alias_key, a.container_name, a.usage_kind
				FROM domain_catalog.field_alias a
				JOIN domain_catalog.field f ON f.field_id = a.field_id
				WHERE a.is_deleted = false AND f.is_deleted = false
				  AND f.sub_module_code = $1 AND a.consumer_system = $2
				ORDER BY a.container_name, a.sort_order, a.alias_key`, sm, sys)
			if faRows != nil {
				for faRows.Next() {
					var f compatField
					if err := faRows.Scan(&f.FieldCode, &f.CDMPath, &f.Label, &f.DataType, &f.AliasKey, &f.Container, &f.UsageKind); err == nil {
						c.FieldAliases = append(c.FieldAliases, f)
					}
				}
				faRows.Close()
			}

			switch sys {
			case "DMS":
				for _, p := range report.Parts {
					if p.PartCode == "FILES" {
						c.StorageModuleKey = p.StorageModuleKey
						c.S3Prefix = p.S3Prefix
						c.ParentField = p.ParentField
						c.FilesTable = p.FilesTable
						c.APIPath = p.APIPath
						break
					}
				}
				c.Compatible = c.StorageModuleKey != "" && c.ParentField != ""
				c.Notes = "uploadWorkspace + s3storage use storage_module_key; parent_field binds files to booking"
			case "POLICY":
				c.Compatible = c.ModuleAlias != "" && len(c.FieldAliases) > 0
				c.Notes = "RunCheck ModuleCode=INVESTMENT_FD; CDM paths on field aliases; part CREATE ↔ FD_BOOKING_CREATE"
			case "NOTIFICATION":
				c.Compatible = c.SubAlias != "" && len(c.FieldAliases) > 0
				c.Notes = "Template editor loads Bookings[] / scalars via notification-schema (alias FD BOOKING)"
			case "APPROVAL":
				c.Compatible = c.ModuleAlias != "" && len(c.PartAliases) > 0
				c.Notes = "module_code FIXED_DEPOSIT; txn types from part aliases (FD_BOOKING, FD_BOOKING_CREATE, …)"
			case "DASHBOARD":
				c.Compatible = c.SubAlias != "" && len(c.FieldAliases) > 0
				c.Notes = "DataSourceKey fdBooking; columns = DASHBOARD field aliases"
			}
			report.Consumers = append(report.Consumers, c)
		}

		ok := 0
		for _, c := range report.Consumers {
			if c.Compatible {
				ok++
			}
		}
		report.Summary = strings.TrimSpace(report.ModuleName) + " / " + report.SubModuleName +
			": " + strconv.Itoa(ok) + "/" + strconv.Itoa(len(report.Consumers)) + " consumers mapped"

		api.RespondEnvelopeSuccess(w, "Compatibility report", report)
	}
}
