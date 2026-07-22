package domaincatalog

import (
	"context"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// IsApprovalModuleAllowed returns true if moduleCode is a legacy allowlist entry
// OR an APPROVAL alias (or canonical module_code) present in domain_catalog.
func IsApprovalModuleAllowed(ctx context.Context, pool *pgxpool.Pool, moduleCode string) bool {
	code := strings.TrimSpace(moduleCode)
	if code == "" {
		return false
	}
	legacy := map[string]bool{
		"FIXED_DEPOSIT": true, "PAYMENTS": true, "RECONCILIATION": true,
		"VENDOR": true, "GENERAL": true, "EMAIL_INBOX": true,
	}
	if legacy[code] {
		return true
	}
	if pool == nil {
		return false
	}
	var one int
	err := pool.QueryRow(ctx, `
		SELECT 1 FROM domain_catalog.module_alias
		WHERE is_deleted = false AND consumer_system = 'APPROVAL' AND alias_code = $1
		LIMIT 1`, code).Scan(&one)
	if err == nil {
		return true
	}
	err = pool.QueryRow(ctx, `
		SELECT 1 FROM domain_catalog.module
		WHERE is_deleted = false AND module_code = $1
		LIMIT 1`, code).Scan(&one)
	return err == nil
}

type dashField struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	DataType string `json:"data_type"`
	Kind     string `json:"kind"` // categorical | numeric
}

type dashSchema struct {
	DataSourceKey     string      `json:"data_source_key"`
	Label             string      `json:"label"`
	SubModuleCode     string      `json:"sub_module_code"`
	CategoricalFields []dashField `json:"categorical_fields"`
	NumericFields     []dashField `json:"numeric_fields"`
}

func HandleDashboardSchema(pool *pgxpool.Pool) http.HandlerFunc {
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

		var label, dashKey string
		_ = pool.QueryRow(r.Context(), `
			SELECT s.sub_module_name,
			       COALESCE((
			         SELECT alias_code FROM domain_catalog.sub_module_alias
			         WHERE is_deleted = false AND sub_module_code = s.sub_module_code
			           AND consumer_system = 'DASHBOARD' LIMIT 1
			       ), s.sub_module_code)
			FROM domain_catalog.sub_module s
			WHERE s.is_deleted = false AND s.sub_module_code = $1`, sm).Scan(&label, &dashKey)

		rows, err := pool.Query(r.Context(), `
			SELECT a.alias_key, f.label, f.data_type
			FROM domain_catalog.field_alias a
			JOIN domain_catalog.field f ON f.field_id = a.field_id
			WHERE a.is_deleted = false AND f.is_deleted = false
			  AND f.sub_module_code = $1 AND a.consumer_system = 'DASHBOARD'
			ORDER BY a.sort_order, a.alias_key`, sm)
		if err != nil {
			api.LogErrorForResponse(w, "dashboard-schema: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load dashboard schema", "CATALOG_DASH_SCHEMA_FAILED")
			return
		}
		defer rows.Close()

		out := dashSchema{
			DataSourceKey:     dashKey,
			Label:             label,
			SubModuleCode:     sm,
			CategoricalFields: make([]dashField, 0),
			NumericFields:     make([]dashField, 0),
		}
		for rows.Next() {
			var key, lbl, dt string
			if err := rows.Scan(&key, &lbl, &dt); err != nil {
				continue
			}
			f := dashField{Key: key, Label: lbl, DataType: dt}
			if dt == "number" {
				f.Kind = "numeric"
				out.NumericFields = append(out.NumericFields, f)
			} else {
				f.Kind = "categorical"
				out.CategoricalFields = append(out.CategoricalFields, f)
			}
		}
		api.RespondEnvelopeSuccess(w, "Dashboard schema fetched", out)
	}
}

type dmsBinding struct {
	SubModuleCode    string `json:"sub_module_code"`
	StorageModuleKey string `json:"storage_module_key"`
	S3Prefix         string `json:"s3_prefix"`
	ParentField      string `json:"parent_field"`
	FilesTable       string `json:"files_table"`
	APIPath          string `json:"api_path"`
	PartName         string `json:"part_name"`
}

func HandleDmsBinding(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req struct {
			SubModuleCode    string `json:"sub_module_code"`
			StorageModuleKey string `json:"storage_module_key"`
		}
		_ = decodeJSON(r, &req)
		sm := strings.TrimSpace(req.SubModuleCode)
		key := strings.TrimSpace(req.StorageModuleKey)

		var b dmsBinding
		var err error
		if key != "" {
			err = pool.QueryRow(r.Context(), `
				SELECT sub_module_code, storage_module_key, s3_prefix, parent_field, files_table, api_path, part_name
				FROM domain_catalog.part
				WHERE is_deleted = false AND part_code = 'FILES' AND storage_module_key = $1
				LIMIT 1`, key).Scan(
				&b.SubModuleCode, &b.StorageModuleKey, &b.S3Prefix, &b.ParentField, &b.FilesTable, &b.APIPath, &b.PartName)
		} else {
			if sm == "" {
				sm = "FD_BOOKING"
			}
			err = pool.QueryRow(r.Context(), `
				SELECT sub_module_code, storage_module_key, s3_prefix, parent_field, files_table, api_path, part_name
				FROM domain_catalog.part
				WHERE is_deleted = false AND part_code = 'FILES' AND sub_module_code = $1
				LIMIT 1`, sm).Scan(
				&b.SubModuleCode, &b.StorageModuleKey, &b.S3Prefix, &b.ParentField, &b.FilesTable, &b.APIPath, &b.PartName)
		}
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "DMS binding not found in domain catalog", "CATALOG_DMS_NOT_FOUND")
			return
		}
		api.RespondEnvelopeSuccess(w, "DMS binding fetched", b)
	}
}
