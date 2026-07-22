package domaincatalog

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

type treeAlias struct {
	ConsumerSystem string `json:"consumer_system"`
	AliasCode      string `json:"alias_code"`
}

type treeField struct {
	FieldCode   string  `json:"field_code"`
	CDMPath     *string `json:"cdm_path"`
	DataType    string  `json:"data_type"`
	Unit        string  `json:"unit"`
	Label       string  `json:"label"`
	Description string  `json:"description"`
	Nullable    bool    `json:"nullable"`
	SortOrder   int     `json:"sort_order"`
}

type treePart struct {
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
	Aliases          []treeAlias `json:"aliases"`
}

type treeResponse struct {
	ModuleCode    string      `json:"module_code"`
	ModuleName    string      `json:"module_name"`
	ModuleAliases []treeAlias `json:"module_aliases"`
	SubModuleCode string      `json:"sub_module_code"`
	SubModuleName string      `json:"sub_module_name"`
	SubAliases    []treeAlias `json:"sub_module_aliases"`
	Parts         []treePart  `json:"parts"`
	Fields        []treeField `json:"fields"`
}

func HandleTree(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req struct {
			ModuleCode    string `json:"module_code"`
			SubModuleCode string `json:"sub_module_code"`
		}
		_ = decodeJSON(r, &req)

		sm := strings.TrimSpace(req.SubModuleCode)
		mc := strings.TrimSpace(req.ModuleCode)
		if sm == "" && mc == "" {
			sm = "FD_BOOKING"
		}

		var out treeResponse
		err := pool.QueryRow(r.Context(), `
			SELECT m.module_code, m.module_name, s.sub_module_code, s.sub_module_name
			FROM domain_catalog.sub_module s
			JOIN domain_catalog.module m ON m.module_code = s.module_code
			WHERE s.is_deleted = false AND m.is_deleted = false
			  AND ($1 = '' OR s.sub_module_code = $1)
			  AND ($2 = '' OR s.module_code = $2)
			ORDER BY s.sub_module_code
			LIMIT 1`, sm, mc).Scan(&out.ModuleCode, &out.ModuleName, &out.SubModuleCode, &out.SubModuleName)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog tree: %v", err)
			api.RespondEnvelopeError(w, http.StatusNotFound, "catalog tree not found", "CATALOG_TREE_NOT_FOUND")
			return
		}

		out.ModuleAliases = loadAliases(w, r, pool, `
			SELECT consumer_system, alias_code FROM domain_catalog.module_alias
			WHERE is_deleted = false AND module_code = $1 ORDER BY consumer_system, alias_code`, out.ModuleCode)
		out.SubAliases = loadAliases(w, r, pool, `
			SELECT consumer_system, alias_code FROM domain_catalog.sub_module_alias
			WHERE is_deleted = false AND sub_module_code = $1 ORDER BY consumer_system, alias_code`, out.SubModuleCode)

		partRows, err := pool.Query(r.Context(), `
			SELECT part_id::text, part_code, part_name, description, api_path, handler_name,
			       storage_module_key, s3_prefix, parent_field, files_table, sort_order
			FROM domain_catalog.part
			WHERE is_deleted = false AND sub_module_code = $1
			ORDER BY sort_order, part_code`, out.SubModuleCode)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog tree parts: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load catalog tree", "CATALOG_TREE_FAILED")
			return
		}
		defer partRows.Close()

		out.Parts = make([]treePart, 0)
		for partRows.Next() {
			var partID string
			var p treePart
			if err := partRows.Scan(&partID, &p.PartCode, &p.PartName, &p.Description, &p.APIPath, &p.HandlerName,
				&p.StorageModuleKey, &p.S3Prefix, &p.ParentField, &p.FilesTable, &p.SortOrder); err != nil {
				api.LogErrorForResponse(w, "domain-catalog tree part scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load catalog tree", "CATALOG_TREE_FAILED")
				return
			}
			p.Aliases = loadAliases(w, r, pool, `
				SELECT consumer_system, alias_code FROM domain_catalog.part_alias
				WHERE is_deleted = false AND part_id = $1::uuid ORDER BY consumer_system, alias_code`, partID)
			out.Parts = append(out.Parts, p)
		}

		fieldRows, err := pool.Query(r.Context(), `
			SELECT field_code, cdm_path, data_type, unit, label, description, nullable, sort_order
			FROM domain_catalog.field
			WHERE is_deleted = false AND sub_module_code = $1
			ORDER BY sort_order, field_code`, out.SubModuleCode)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog tree fields: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load catalog tree", "CATALOG_TREE_FAILED")
			return
		}
		defer fieldRows.Close()

		out.Fields = make([]treeField, 0)
		for fieldRows.Next() {
			var f treeField
			if err := fieldRows.Scan(&f.FieldCode, &f.CDMPath, &f.DataType, &f.Unit, &f.Label, &f.Description, &f.Nullable, &f.SortOrder); err != nil {
				api.LogErrorForResponse(w, "domain-catalog tree field scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load catalog tree", "CATALOG_TREE_FAILED")
				return
			}
			out.Fields = append(out.Fields, f)
		}

		api.RespondEnvelopeSuccess(w, "Catalog tree fetched", out)
	}
}

func loadAliases(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, q string, arg string) []treeAlias {
	rows, err := pool.Query(r.Context(), q, arg)
	if err != nil {
		api.LogErrorForResponse(w, "domain-catalog aliases: %v", err)
		return []treeAlias{}
	}
	defer rows.Close()
	out := make([]treeAlias, 0)
	for rows.Next() {
		var a treeAlias
		if err := rows.Scan(&a.ConsumerSystem, &a.AliasCode); err != nil {
			continue
		}
		out = append(out, a)
	}
	return out
}
