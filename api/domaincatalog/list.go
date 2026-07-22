package domaincatalog

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

type moduleRow struct {
	ModuleCode   string  `json:"module_code"`
	ModuleName   string  `json:"module_name"`
	ParentModule *string `json:"parent_module"`
	Description  string  `json:"description"`
}

func HandleModulesList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		rows, err := pool.Query(r.Context(), `
			SELECT module_code, module_name, parent_module, description
			FROM domain_catalog.module
			WHERE is_deleted = false
			ORDER BY module_code`)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog modules/list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list modules", "CATALOG_MODULES_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]moduleRow, 0)
		for rows.Next() {
			var m moduleRow
			if err := rows.Scan(&m.ModuleCode, &m.ModuleName, &m.ParentModule, &m.Description); err != nil {
				api.LogErrorForResponse(w, "domain-catalog modules/list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list modules", "CATALOG_MODULES_LIST_FAILED")
				return
			}
			out = append(out, m)
		}
		api.RespondEnvelopeSuccess(w, "Modules fetched", out)
	}
}

type subModuleRow struct {
	SubModuleCode string `json:"sub_module_code"`
	ModuleCode    string `json:"module_code"`
	SubModuleName string `json:"sub_module_name"`
	Description   string `json:"description"`
}

func HandleSubModulesList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req struct {
			ModuleCode string `json:"module_code"`
		}
		_ = decodeJSON(r, &req)

		q := `
			SELECT sub_module_code, module_code, sub_module_name, description
			FROM domain_catalog.sub_module
			WHERE is_deleted = false`
		args := []interface{}{}
		if mc := strings.TrimSpace(req.ModuleCode); mc != "" {
			q += ` AND module_code = $1`
			args = append(args, mc)
		}
		q += ` ORDER BY module_code, sub_module_code`

		rows, err := pool.Query(r.Context(), q, args...)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog sub-modules/list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list sub-modules", "CATALOG_SUB_MODULES_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]subModuleRow, 0)
		for rows.Next() {
			var m subModuleRow
			if err := rows.Scan(&m.SubModuleCode, &m.ModuleCode, &m.SubModuleName, &m.Description); err != nil {
				api.LogErrorForResponse(w, "domain-catalog sub-modules/list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list sub-modules", "CATALOG_SUB_MODULES_LIST_FAILED")
				return
			}
			out = append(out, m)
		}
		api.RespondEnvelopeSuccess(w, "Sub-modules fetched", out)
	}
}

type partRow struct {
	PartID           string `json:"part_id"`
	SubModuleCode    string `json:"sub_module_code"`
	PartCode         string `json:"part_code"`
	PartName         string `json:"part_name"`
	Description      string `json:"description"`
	APIPath          string `json:"api_path"`
	HandlerName      string `json:"handler_name"`
	StorageModuleKey string `json:"storage_module_key"`
	S3Prefix         string `json:"s3_prefix"`
	ParentField      string `json:"parent_field"`
	FilesTable       string `json:"files_table"`
	SortOrder        int    `json:"sort_order"`
}

func HandlePartsList(pool *pgxpool.Pool) http.HandlerFunc {
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
			api.RespondEnvelopeError(w, http.StatusBadRequest, "sub_module_code is required", "CATALOG_SUB_MODULE_REQUIRED")
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT part_id::text, sub_module_code, part_code, part_name, description,
			       api_path, handler_name, storage_module_key, s3_prefix, parent_field, files_table, sort_order
			FROM domain_catalog.part
			WHERE is_deleted = false AND sub_module_code = $1
			ORDER BY sort_order, part_code`, sm)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog parts/list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list parts", "CATALOG_PARTS_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]partRow, 0)
		for rows.Next() {
			var p partRow
			if err := rows.Scan(&p.PartID, &p.SubModuleCode, &p.PartCode, &p.PartName, &p.Description,
				&p.APIPath, &p.HandlerName, &p.StorageModuleKey, &p.S3Prefix, &p.ParentField, &p.FilesTable, &p.SortOrder); err != nil {
				api.LogErrorForResponse(w, "domain-catalog parts/list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list parts", "CATALOG_PARTS_LIST_FAILED")
				return
			}
			out = append(out, p)
		}
		api.RespondEnvelopeSuccess(w, "Parts fetched", out)
	}
}

type fieldRow struct {
	FieldID       string  `json:"field_id"`
	SubModuleCode string  `json:"sub_module_code"`
	FieldCode     string  `json:"field_code"`
	CDMPath       *string `json:"cdm_path"`
	DataType      string  `json:"data_type"`
	Unit          string  `json:"unit"`
	Label         string  `json:"label"`
	Description   string  `json:"description"`
	Nullable      bool    `json:"nullable"`
	SortOrder     int     `json:"sort_order"`
}

type fieldAliasRow struct {
	AliasKey       string `json:"alias_key"`
	ContainerName  string `json:"container_name"`
	UsageKind      string `json:"usage_kind"`
	ConsumerSystem string `json:"consumer_system"`
	SortOrder      int    `json:"sort_order"`
	FieldCode      string `json:"field_code"`
	DataType       string `json:"data_type"`
	Label          string `json:"label"`
}

func HandleFieldsList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req struct {
			SubModuleCode  string `json:"sub_module_code"`
			ConsumerSystem string `json:"consumer_system"`
		}
		_ = decodeJSON(r, &req)
		sm := strings.TrimSpace(req.SubModuleCode)
		if sm == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "sub_module_code is required", "CATALOG_SUB_MODULE_REQUIRED")
			return
		}
		consumer := normalizeConsumer(req.ConsumerSystem)

		if consumer != "" {
			rows, err := pool.Query(r.Context(), `
				SELECT a.alias_key, a.container_name, a.usage_kind, a.consumer_system, a.sort_order,
				       f.field_code, f.data_type, f.label
				FROM domain_catalog.field_alias a
				JOIN domain_catalog.field f ON f.field_id = a.field_id
				WHERE a.is_deleted = false AND f.is_deleted = false
				  AND f.sub_module_code = $1 AND a.consumer_system = $2
				ORDER BY a.container_name, a.sort_order, a.alias_key`, sm, consumer)
			if err != nil {
				api.LogErrorForResponse(w, "domain-catalog fields/list aliases: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list field aliases", "CATALOG_FIELDS_LIST_FAILED")
				return
			}
			defer rows.Close()
			out := make([]fieldAliasRow, 0)
			for rows.Next() {
				var a fieldAliasRow
				if err := rows.Scan(&a.AliasKey, &a.ContainerName, &a.UsageKind, &a.ConsumerSystem, &a.SortOrder,
					&a.FieldCode, &a.DataType, &a.Label); err != nil {
					api.LogErrorForResponse(w, "domain-catalog fields/list alias scan: %v", err)
					api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list field aliases", "CATALOG_FIELDS_LIST_FAILED")
					return
				}
				out = append(out, a)
			}
			api.RespondEnvelopeSuccess(w, "Field aliases fetched", out)
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT field_id::text, sub_module_code, field_code, cdm_path, data_type, unit, label, description, nullable, sort_order
			FROM domain_catalog.field
			WHERE is_deleted = false AND sub_module_code = $1
			ORDER BY sort_order, field_code`, sm)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog fields/list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list fields", "CATALOG_FIELDS_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]fieldRow, 0)
		for rows.Next() {
			var f fieldRow
			if err := rows.Scan(&f.FieldID, &f.SubModuleCode, &f.FieldCode, &f.CDMPath, &f.DataType, &f.Unit,
				&f.Label, &f.Description, &f.Nullable, &f.SortOrder); err != nil {
				api.LogErrorForResponse(w, "domain-catalog fields/list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list fields", "CATALOG_FIELDS_LIST_FAILED")
				return
			}
			out = append(out, f)
		}
		api.RespondEnvelopeSuccess(w, "Fields fetched", out)
	}
}
