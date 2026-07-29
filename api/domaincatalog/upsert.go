package domaincatalog

import (
	"context"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// upsert.go — write side of the domain catalog. Every handler here is a plain
// authenticated upsert (no maker-checker workflow — that's the CDM Variables
// page's job for policyengine_svc.cdm_variable, a different table). These
// exist so seeding a module's module/sub_module/part/field/*_alias rows no
// longer requires hand-writing a SQL migration file the way FD Booking's did
// (database/2026-07-19/fd_booking_catalog_seed.sql) — call these instead.

func actor(r *http.Request) string {
	if name := api.GetUserNameFromCtx(r.Context()); name != "" {
		return name
	}
	return "domain_catalog_api"
}

// catalogCDMText fills description / canonical_ref / user_alias for CDM rows
// synced from domain_catalog (seeds often omit description; UI create fills these).
func catalogCDMText(moduleCode, subModuleCode, fieldCode, label, description string) (desc, canonicalRef, userAlias string) {
	fieldCode = strings.TrimSpace(fieldCode)
	label = strings.TrimSpace(label)
	desc = strings.TrimSpace(description)
	if desc == "" {
		if label != "" && fieldCode != "" {
			desc = label + " (source field: " + fieldCode + ")"
		} else if label != "" {
			desc = label
		} else {
			desc = fieldCode
		}
	}
	table := strings.ToLower(strings.TrimSpace(subModuleCode))
	if table != "" && fieldCode != "" {
		canonicalRef = table + "." + fieldCode
	} else {
		canonicalRef = fieldCode
	}
	mod := strings.ToLower(strings.TrimSpace(moduleCode))
	switch strings.ToUpper(strings.TrimSpace(moduleCode)) {
	case "CASH":
		mod = "cash"
	case "FX":
		mod = "fx"
	case "INVESTMENT_FD", "INVESTMENT_MF":
		mod = "investment"
	}
	aliasPrefix := mod + "." + strings.ReplaceAll(table, "_", "")
	if fieldCode != "" {
		userAlias = aliasPrefix + "." + strings.ReplaceAll(fieldCode, "_", "")
	}
	return desc, canonicalRef, userAlias
}

// syncCDMVariableFromCatalog upserts policyengine_svc.cdm_variable for a catalog field.
// source_system is the sub_module_code (matches CDM UI create) so View form can resolve Module/Sub-module.
func syncCDMVariableFromCatalog(ctx context.Context, tx pgx.Tx, subModuleCode, fieldCode, cdmPath, dataType, unit, label, description string, nullable bool, who string) error {
	cdmDomain, err := cdmDomainForSubModule(ctx, tx, subModuleCode)
	if err != nil {
		return err
	}
	var moduleCode string
	_ = tx.QueryRow(ctx, `
		SELECT module_code FROM domain_catalog.sub_module
		WHERE sub_module_code = $1 AND is_deleted = false`, subModuleCode,
	).Scan(&moduleCode)

	desc, canonicalRef, userAlias := catalogCDMText(moduleCode, subModuleCode, fieldCode, label, description)
	who = strings.TrimSpace(who)
	if who == "" {
		who = "domain_catalog_api"
	}

	// name (path) is not unique — multiple labels may share one path. Prefer updating
	// a catalog/sub-module-owned row for this path; otherwise insert a new CDM row
	// so a hand-authored alternate label for the same path is left alone.
	var existingID string
	err = tx.QueryRow(ctx, `
		SELECT variable_id::text
		FROM policyengine_svc.cdm_variable
		WHERE name = $1 AND is_deleted = false
		  AND (
			source_system = $2
			OR source_system IN ('domain_catalog_api', 'domain_catalog_seed', '')
		  )
		ORDER BY
			CASE WHEN source_system = $2 THEN 0 ELSE 1 END,
			created_at ASC NULLS LAST
		LIMIT 1`,
		cdmPath, subModuleCode,
	).Scan(&existingID)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}
	if existingID != "" {
		_, err = tx.Exec(ctx, `
			UPDATE policyengine_svc.cdm_variable SET
				data_type = $2,
				unit = COALESCE($3,''),
				label = $4,
				description = CASE
					WHEN btrim(COALESCE(description, '')) = '' THEN $5
					ELSE description
				END,
				domain = $6,
				source_system = CASE
					WHEN source_system IN ('domain_catalog_api', '') THEN $7
					ELSE source_system
				END,
				canonical_ref = CASE
					WHEN btrim(COALESCE(canonical_ref, '')) = '' THEN $8
					ELSE canonical_ref
				END,
				user_alias = CASE
					WHEN user_alias IS NULL OR btrim(user_alias) = '' THEN NULLIF($9,'')
					ELSE user_alias
				END,
				nullable = $10,
				status = 'Active',
				is_deleted = false,
				processing_status = COALESCE(processing_status, 'APPROVED'),
				last_modified_by = $11,
				last_modified_at = now()
			WHERE variable_id = $1::uuid`,
			existingID, dataType, unit, label, desc, cdmDomain, subModuleCode,
			canonicalRef, userAlias, nullable, who,
		)
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO policyengine_svc.cdm_variable (
			name, data_type, unit, label, description, domain, source_system,
			canonical_ref, user_alias, nullable, status, is_deleted, processing_status,
			created_by, last_modified_by
		)
		VALUES ($1, $2, COALESCE($3,''), $4, $5, $6, $7,
			$8, NULLIF($9,''), $10, 'Active', false, 'APPROVED',
			$11, $11)`,
		cdmPath, dataType, unit, label, desc, cdmDomain, subModuleCode,
		canonicalRef, userAlias, nullable, who,
	)
	return err
}

// ensureCDMCreateAudit writes a CREATE+APPROVED audit when the synced cdm_variable
// has no maker-checker trail yet (seed/upsert path skips the CDM UI create handler).
func ensureCDMCreateAudit(ctx context.Context, tx pgx.Tx, cdmName, who string) error {
	who = strings.TrimSpace(who)
	if who == "" {
		who = "system-seed"
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO policyengine_svc.cdm_variable_audit (
			variable_id, action_type, processing_status, reason,
			requested_by, requested_at, checker_by, checker_at, checker_comment,
			new_name, new_data_type, new_unit, new_label, new_description, new_domain,
			new_source_system, new_canonical_ref, new_user_alias, new_nullable, new_status, new_is_deleted
		)
		SELECT
			c.variable_id, 'CREATE', 'APPROVED',
			'domain_catalog sync — seeded Active/APPROVED',
			COALESCE(NULLIF(TRIM(c.created_by), ''), $2),
			COALESCE(c.created_at, now()),
			COALESCE(NULLIF(TRIM(c.created_by), ''), $2),
			COALESCE(c.created_at, now()),
			'system seed via domain_catalog',
			c.name, c.data_type, c.unit, c.label, c.description, c.domain,
			c.source_system, c.canonical_ref, c.user_alias, c.nullable, c.status, c.is_deleted
		FROM policyengine_svc.cdm_variable c
		WHERE c.name = $1
		  AND NOT EXISTS (
			SELECT 1 FROM policyengine_svc.cdm_variable_audit a WHERE a.variable_id = c.variable_id
		  )`, cdmName, who)
	return err
}

// cdmDomainForSubModule resolves the policyengine_svc.cdm_variable.domain
// value a field's sub_module belongs to. This MUST be the lowercase token the
// frontend's filterCdmByModules() matches against a policy's selected module
// codes (CASH/FX/INVESTMENT_FD/INVESTMENT_MF) — see
// CimplrCorpSaaS-Web/src/features/policyEngine/UseCdmRegistry.ts. It is NOT
// the sub_module_code itself: writing e.g. "FORWARD_BOOKING" as the domain
// means the variable can never match any module filter, so it silently never
// appears in the Rule step's "Tested variable" picker.
func cdmDomainForSubModule(ctx context.Context, tx pgx.Tx, subModuleCode string) (string, error) {
	var moduleCode string
	err := tx.QueryRow(ctx, `
		SELECT module_code FROM domain_catalog.sub_module
		WHERE sub_module_code = $1 AND is_deleted = false`,
		subModuleCode,
	).Scan(&moduleCode)
	if err != nil {
		return "", err
	}
	switch moduleCode {
	case "CASH":
		return "cash", nil
	case "FX":
		return "fx", nil
	case "INVESTMENT_FD", "INVESTMENT_MF":
		return "investment", nil
	default:
		return strings.ToLower(moduleCode), nil
	}
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

type moduleUpsertReq struct {
	ModuleCode   string `json:"module_code"`
	ModuleName   string `json:"module_name"`
	ParentModule string `json:"parent_module"`
	Description  string `json:"description"`
}

func HandleModuleUpsert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req moduleUpsertReq
		if err := decodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.ModuleName = strings.TrimSpace(req.ModuleName)
		if req.ModuleCode == "" || req.ModuleName == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "module_code and module_name are required", "VALIDATION_ERROR")
			return
		}
		var parent interface{}
		if p := strings.TrimSpace(req.ParentModule); p != "" {
			parent = p
		}
		_, err := pool.Exec(r.Context(), `
			INSERT INTO domain_catalog.module (module_code, module_name, parent_module, description, created_by)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (module_code) DO UPDATE SET
				module_name = EXCLUDED.module_name,
				parent_module = EXCLUDED.parent_module,
				description = EXCLUDED.description,
				last_modified_by = EXCLUDED.created_by,
				last_modified_at = now(),
				is_deleted = false`,
			req.ModuleCode, req.ModuleName, parent, req.Description, actor(r))
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog module upsert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to upsert module", "")
			return
		}
		api.RespondEnvelopeSuccess(w, "Module upserted", map[string]interface{}{"module_code": req.ModuleCode})
	}
}

type moduleAliasUpsertReq struct {
	ModuleCode     string `json:"module_code"`
	ConsumerSystem string `json:"consumer_system"`
	AliasCode      string `json:"alias_code"`
}

func HandleModuleAliasUpsert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req moduleAliasUpsertReq
		if err := decodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.ConsumerSystem = normalizeConsumer(req.ConsumerSystem)
		req.AliasCode = strings.TrimSpace(req.AliasCode)
		if req.ModuleCode == "" || req.ConsumerSystem == "" || req.AliasCode == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "module_code, consumer_system, and alias_code are required", "VALIDATION_ERROR")
			return
		}
		_, err := pool.Exec(r.Context(), `
			INSERT INTO domain_catalog.module_alias (module_code, consumer_system, alias_code)
			VALUES ($1, $2, $3)
			ON CONFLICT (consumer_system, alias_code) DO UPDATE SET
				module_code = EXCLUDED.module_code,
				is_deleted = false`,
			req.ModuleCode, req.ConsumerSystem, req.AliasCode)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog module-alias upsert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to upsert module alias — check consumer_system is one of POLICY/NOTIFICATION/APPROVAL/DASHBOARD/DMS/NAV", "")
			return
		}
		api.RespondEnvelopeSuccess(w, "Module alias upserted", map[string]interface{}{
			"module_code": req.ModuleCode, "consumer_system": req.ConsumerSystem, "alias_code": req.AliasCode,
		})
	}
}

// ---------------------------------------------------------------------------
// Sub-module
// ---------------------------------------------------------------------------

type subModuleUpsertReq struct {
	SubModuleCode string `json:"sub_module_code"`
	ModuleCode    string `json:"module_code"`
	SubModuleName string `json:"sub_module_name"`
	Description   string `json:"description"`
}

func HandleSubModuleUpsert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req subModuleUpsertReq
		if err := decodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModuleName = strings.TrimSpace(req.SubModuleName)
		if req.SubModuleCode == "" || req.ModuleCode == "" || req.SubModuleName == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "sub_module_code, module_code, and sub_module_name are required", "VALIDATION_ERROR")
			return
		}
		_, err := pool.Exec(r.Context(), `
			INSERT INTO domain_catalog.sub_module (sub_module_code, module_code, sub_module_name, description, created_by)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (sub_module_code) DO UPDATE SET
				module_code = EXCLUDED.module_code,
				sub_module_name = EXCLUDED.sub_module_name,
				description = EXCLUDED.description,
				last_modified_by = EXCLUDED.created_by,
				last_modified_at = now(),
				is_deleted = false`,
			req.SubModuleCode, req.ModuleCode, req.SubModuleName, req.Description, actor(r))
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog sub-module upsert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to upsert sub-module — check module_code exists", "")
			return
		}
		api.RespondEnvelopeSuccess(w, "Sub-module upserted", map[string]interface{}{"sub_module_code": req.SubModuleCode})
	}
}

type subModuleAliasUpsertReq struct {
	SubModuleCode  string `json:"sub_module_code"`
	ConsumerSystem string `json:"consumer_system"`
	AliasCode      string `json:"alias_code"`
}

func HandleSubModuleAliasUpsert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req subModuleAliasUpsertReq
		if err := decodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.ConsumerSystem = normalizeConsumer(req.ConsumerSystem)
		req.AliasCode = strings.TrimSpace(req.AliasCode)
		if req.SubModuleCode == "" || req.ConsumerSystem == "" || req.AliasCode == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "sub_module_code, consumer_system, and alias_code are required", "VALIDATION_ERROR")
			return
		}
		_, err := pool.Exec(r.Context(), `
			INSERT INTO domain_catalog.sub_module_alias (sub_module_code, consumer_system, alias_code)
			VALUES ($1, $2, $3)
			ON CONFLICT (consumer_system, alias_code) DO UPDATE SET
				sub_module_code = EXCLUDED.sub_module_code,
				is_deleted = false`,
			req.SubModuleCode, req.ConsumerSystem, req.AliasCode)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog sub-module-alias upsert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to upsert sub-module alias", "")
			return
		}
		api.RespondEnvelopeSuccess(w, "Sub-module alias upserted", map[string]interface{}{
			"sub_module_code": req.SubModuleCode, "consumer_system": req.ConsumerSystem, "alias_code": req.AliasCode,
		})
	}
}

// ---------------------------------------------------------------------------
// Part
// ---------------------------------------------------------------------------

type partUpsertReq struct {
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

func HandlePartUpsert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req partUpsertReq
		if err := decodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.PartCode = strings.TrimSpace(req.PartCode)
		req.PartName = strings.TrimSpace(req.PartName)
		if req.SubModuleCode == "" || req.PartCode == "" || req.PartName == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "sub_module_code, part_code, and part_name are required", "VALIDATION_ERROR")
			return
		}
		var partID string
		err := pool.QueryRow(r.Context(), `
			INSERT INTO domain_catalog.part (
				sub_module_code, part_code, part_name, description, api_path, handler_name,
				storage_module_key, s3_prefix, parent_field, files_table, sort_order, created_by
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			ON CONFLICT (sub_module_code, part_code) DO UPDATE SET
				part_name = EXCLUDED.part_name,
				description = EXCLUDED.description,
				api_path = EXCLUDED.api_path,
				handler_name = EXCLUDED.handler_name,
				storage_module_key = EXCLUDED.storage_module_key,
				s3_prefix = EXCLUDED.s3_prefix,
				parent_field = EXCLUDED.parent_field,
				files_table = EXCLUDED.files_table,
				sort_order = EXCLUDED.sort_order,
				last_modified_by = EXCLUDED.created_by,
				last_modified_at = now(),
				is_deleted = false
			RETURNING part_id`,
			req.SubModuleCode, req.PartCode, req.PartName, req.Description, req.APIPath, req.HandlerName,
			req.StorageModuleKey, req.S3Prefix, req.ParentField, req.FilesTable, req.SortOrder, actor(r),
		).Scan(&partID)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog part upsert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to upsert part — check sub_module_code exists", "")
			return
		}
		api.RespondEnvelopeSuccess(w, "Part upserted", map[string]interface{}{"part_id": partID, "part_code": req.PartCode})
	}
}

type partAliasUpsertReq struct {
	SubModuleCode  string `json:"sub_module_code"`
	PartCode       string `json:"part_code"`
	ConsumerSystem string `json:"consumer_system"`
	AliasCode      string `json:"alias_code"`
}

func HandlePartAliasUpsert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req partAliasUpsertReq
		if err := decodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.PartCode = strings.TrimSpace(req.PartCode)
		req.ConsumerSystem = normalizeConsumer(req.ConsumerSystem)
		req.AliasCode = strings.TrimSpace(req.AliasCode)
		if req.SubModuleCode == "" || req.PartCode == "" || req.ConsumerSystem == "" || req.AliasCode == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "sub_module_code, part_code, consumer_system, and alias_code are required", "VALIDATION_ERROR")
			return
		}
		tag, err := pool.Exec(r.Context(), `
			INSERT INTO domain_catalog.part_alias (part_id, consumer_system, alias_code)
			SELECT p.part_id, $3, $4
			FROM domain_catalog.part p
			WHERE p.sub_module_code = $1 AND p.part_code = $2 AND p.is_deleted = false
			ON CONFLICT (consumer_system, alias_code) DO UPDATE SET
				part_id = EXCLUDED.part_id,
				is_deleted = false`,
			req.SubModuleCode, req.PartCode, req.ConsumerSystem, req.AliasCode)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog part-alias upsert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to upsert part alias", "")
			return
		}
		if tag.RowsAffected() == 0 {
			api.RespondEnvelopeError(w, http.StatusNotFound, "no matching part found for sub_module_code/part_code", "NOT_FOUND")
			return
		}
		api.RespondEnvelopeSuccess(w, "Part alias upserted", map[string]interface{}{
			"sub_module_code": req.SubModuleCode, "part_code": req.PartCode,
			"consumer_system": req.ConsumerSystem, "alias_code": req.AliasCode,
		})
	}
}

// ---------------------------------------------------------------------------
// Field — also syncs policyengine_svc.cdm_variable when cdm_path is set,
// matching what fd_booking_catalog_seed.sql did by hand.
// ---------------------------------------------------------------------------

type fieldUpsertReq struct {
	SubModuleCode string `json:"sub_module_code"`
	FieldCode     string `json:"field_code"`
	CDMPath       string `json:"cdm_path"`
	DataType      string `json:"data_type"`
	Unit          string `json:"unit"`
	Label         string `json:"label"`
	Description   string `json:"description"`
	Nullable      *bool  `json:"nullable"`
	SortOrder     int    `json:"sort_order"`
}

var validFieldDataTypes = map[string]bool{
	"number": true, "string": true, "boolean": true, "date": true, "time": true, "datetime": true, "list": true,
}

func HandleFieldUpsert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req fieldUpsertReq
		if err := decodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.FieldCode = strings.TrimSpace(req.FieldCode)
		req.Label = strings.TrimSpace(req.Label)
		req.DataType = strings.ToLower(strings.TrimSpace(req.DataType))
		if req.SubModuleCode == "" || req.FieldCode == "" || req.Label == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "sub_module_code, field_code, and label are required", "VALIDATION_ERROR")
			return
		}
		if req.DataType == "" {
			req.DataType = "string"
		}
		if !validFieldDataTypes[req.DataType] {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "data_type must be one of number/string/boolean/date/time/datetime/list", "VALIDATION_ERROR")
			return
		}
		nullable := true
		if req.Nullable != nil {
			nullable = *req.Nullable
		}
		var cdmPath interface{}
		trimmedPath := strings.TrimSpace(req.CDMPath)
		if trimmedPath != "" {
			cdmPath = trimmedPath
		}

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to begin transaction", "")
			return
		}
		defer tx.Rollback(r.Context())

		var fieldID string
		err = tx.QueryRow(r.Context(), `
			INSERT INTO domain_catalog.field (
				sub_module_code, field_code, cdm_path, data_type, unit, label, description, nullable, sort_order, created_by
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (sub_module_code, field_code) DO UPDATE SET
				cdm_path = EXCLUDED.cdm_path,
				data_type = EXCLUDED.data_type,
				unit = EXCLUDED.unit,
				label = EXCLUDED.label,
				description = EXCLUDED.description,
				nullable = EXCLUDED.nullable,
				sort_order = EXCLUDED.sort_order,
				last_modified_by = EXCLUDED.created_by,
				last_modified_at = now(),
				is_deleted = false
			RETURNING field_id`,
			req.SubModuleCode, req.FieldCode, cdmPath, req.DataType, req.Unit, req.Label, req.Description, nullable, req.SortOrder, actor(r),
		).Scan(&fieldID)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog field upsert: %v", err)
			msg := "failed to upsert field — check sub_module_code exists"
			api.RespondEnvelopeError(w, http.StatusInternalServerError, msg, "")
			return
		}

		if trimmedPath != "" {
			who := actor(r)
			if err := syncCDMVariableFromCatalog(r.Context(), tx, req.SubModuleCode, req.FieldCode, trimmedPath,
				req.DataType, req.Unit, req.Label, req.Description, nullable, who); err != nil {
				api.LogErrorForResponse(w, "domain-catalog field upsert: cdm_variable sync failed: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "field saved but cdm_variable sync failed", "")
				return
			}
			if err := ensureCDMCreateAudit(r.Context(), tx, trimmedPath, who); err != nil {
				api.LogErrorForResponse(w, "domain-catalog field upsert: cdm audit: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "field saved but cdm_variable audit failed", "")
				return
			}
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to commit", "")
			return
		}
		api.RespondEnvelopeSuccess(w, "Field upserted", map[string]interface{}{
			"field_id": fieldID, "field_code": req.FieldCode, "cdm_path": trimmedPath,
		})
	}
}

// ---------------------------------------------------------------------------
// Field alias
// ---------------------------------------------------------------------------

var validUsageKinds = map[string]bool{
	"scalar": true, "list_field": true, "list_name": true, "parent_key": true, "storage_key": true,
}

type fieldAliasUpsertReq struct {
	SubModuleCode  string `json:"sub_module_code"`
	FieldCode      string `json:"field_code"`
	ConsumerSystem string `json:"consumer_system"`
	AliasKey       string `json:"alias_key"`
	ContainerName  string `json:"container_name"`
	UsageKind      string `json:"usage_kind"`
	SortOrder      int    `json:"sort_order"`
}

func HandleFieldAliasUpsert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req fieldAliasUpsertReq
		if err := decodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.FieldCode = strings.TrimSpace(req.FieldCode)
		req.ConsumerSystem = normalizeConsumer(req.ConsumerSystem)
		req.AliasKey = strings.TrimSpace(req.AliasKey)
		req.UsageKind = strings.ToLower(strings.TrimSpace(req.UsageKind))
		if req.SubModuleCode == "" || req.FieldCode == "" || req.ConsumerSystem == "" || req.AliasKey == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "sub_module_code, field_code, consumer_system, and alias_key are required", "VALIDATION_ERROR")
			return
		}
		if req.UsageKind == "" {
			req.UsageKind = "scalar"
		}
		if !validUsageKinds[req.UsageKind] {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "usage_kind must be one of scalar/list_field/list_name/parent_key/storage_key", "VALIDATION_ERROR")
			return
		}
		tag, err := pool.Exec(r.Context(), `
			INSERT INTO domain_catalog.field_alias (field_id, consumer_system, alias_key, container_name, usage_kind, sort_order)
			SELECT f.field_id, $3, $4, $5, $6, $7
			FROM domain_catalog.field f
			WHERE f.sub_module_code = $1 AND f.field_code = $2 AND f.is_deleted = false
			ON CONFLICT (consumer_system, alias_key, container_name) DO UPDATE SET
				field_id = EXCLUDED.field_id,
				usage_kind = EXCLUDED.usage_kind,
				sort_order = EXCLUDED.sort_order,
				is_deleted = false`,
			req.SubModuleCode, req.FieldCode, req.ConsumerSystem, req.AliasKey, req.ContainerName, req.UsageKind, req.SortOrder)
		if err != nil {
			api.LogErrorForResponse(w, "domain-catalog field-alias upsert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to upsert field alias", "")
			return
		}
		if tag.RowsAffected() == 0 {
			api.RespondEnvelopeError(w, http.StatusNotFound, "no matching field found for sub_module_code/field_code", "NOT_FOUND")
			return
		}
		api.RespondEnvelopeSuccess(w, "Field alias upserted", map[string]interface{}{
			"sub_module_code": req.SubModuleCode, "field_code": req.FieldCode,
			"consumer_system": req.ConsumerSystem, "alias_key": req.AliasKey,
			"container_name": req.ContainerName, "usage_kind": req.UsageKind,
		})
	}
}

// ---------------------------------------------------------------------------
// Bulk field + aliases in one call — convenience for seeding a whole
// sub-module without one HTTP round trip per field/alias.
// ---------------------------------------------------------------------------

type bulkFieldEntry struct {
	FieldCode   string `json:"field_code"`
	CDMPath     string `json:"cdm_path"`
	DataType    string `json:"data_type"`
	Unit        string `json:"unit"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Nullable    *bool  `json:"nullable"`
	SortOrder   int    `json:"sort_order"`
	Aliases     []struct {
		ConsumerSystem string `json:"consumer_system"`
		AliasKey       string `json:"alias_key"`
		ContainerName  string `json:"container_name"`
		UsageKind      string `json:"usage_kind"`
		SortOrder      int    `json:"sort_order"`
	} `json:"aliases"`
}

type bulkFieldsUpsertReq struct {
	SubModuleCode string           `json:"sub_module_code"`
	Fields        []bulkFieldEntry `json:"fields"`
}

// HandleBulkFieldsUpsert upserts many fields (and each field's per-consumer
// aliases) for one sub_module_code in a single request — the seeding-time
// equivalent of the "Fields" + three alias VALUES blocks in
// fd_booking_catalog_seed.sql.
func HandleBulkFieldsUpsert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requirePOST(w, r) {
			return
		}
		var req bulkFieldsUpsertReq
		if err := decodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		if req.SubModuleCode == "" || len(req.Fields) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "sub_module_code and at least one field are required", "VALIDATION_ERROR")
			return
		}

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to begin transaction", "")
			return
		}
		defer tx.Rollback(r.Context())

		if _, err := cdmDomainForSubModule(r.Context(), tx, req.SubModuleCode); err != nil {
			api.LogErrorForResponse(w, "domain-catalog bulk field upsert: resolve cdm domain failed: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadRequest, "could not resolve cdm domain — check sub_module_code exists", "")
			return
		}

		fieldsUpserted := 0
		aliasesUpserted := 0
		who := actor(r)

		for _, f := range req.Fields {
			fieldCode := strings.TrimSpace(f.FieldCode)
			label := strings.TrimSpace(f.Label)
			dataType := strings.ToLower(strings.TrimSpace(f.DataType))
			if fieldCode == "" || label == "" {
				api.RespondEnvelopeError(w, http.StatusBadRequest, "each field needs field_code and label (offending field_code=\""+fieldCode+"\")", "VALIDATION_ERROR")
				return
			}
			if dataType == "" {
				dataType = "string"
			}
			if !validFieldDataTypes[dataType] {
				api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid data_type for field_code="+fieldCode, "VALIDATION_ERROR")
				return
			}
			nullable := true
			if f.Nullable != nil {
				nullable = *f.Nullable
			}
			var cdmPath interface{}
			trimmedPath := strings.TrimSpace(f.CDMPath)
			if trimmedPath != "" {
				cdmPath = trimmedPath
			}

			var fieldID string
			err = tx.QueryRow(r.Context(), `
				INSERT INTO domain_catalog.field (
					sub_module_code, field_code, cdm_path, data_type, unit, label, description, nullable, sort_order, created_by
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT (sub_module_code, field_code) DO UPDATE SET
					cdm_path = EXCLUDED.cdm_path,
					data_type = EXCLUDED.data_type,
					unit = EXCLUDED.unit,
					label = EXCLUDED.label,
					description = EXCLUDED.description,
					nullable = EXCLUDED.nullable,
					sort_order = EXCLUDED.sort_order,
					last_modified_by = EXCLUDED.created_by,
					last_modified_at = now(),
					is_deleted = false
				RETURNING field_id`,
				req.SubModuleCode, fieldCode, cdmPath, dataType, f.Unit, label, f.Description, nullable, f.SortOrder, who,
			).Scan(&fieldID)
			if err != nil {
				api.LogErrorForResponse(w, "domain-catalog bulk field upsert (field_code=%s): %v", fieldCode, err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to upsert field_code="+fieldCode+" — check sub_module_code exists", "")
				return
			}
			fieldsUpserted++

			if trimmedPath != "" {
				if err := syncCDMVariableFromCatalog(r.Context(), tx, req.SubModuleCode, fieldCode, trimmedPath,
					dataType, f.Unit, label, f.Description, nullable, who); err != nil {
					api.LogErrorForResponse(w, "domain-catalog bulk field upsert: cdm_variable sync failed for %s: %v", trimmedPath, err)
					api.RespondEnvelopeError(w, http.StatusInternalServerError, "field_code="+fieldCode+" saved but cdm_variable sync failed", "")
					return
				}
				if err := ensureCDMCreateAudit(r.Context(), tx, trimmedPath, who); err != nil {
					api.LogErrorForResponse(w, "domain-catalog bulk field upsert: cdm audit for %s: %v", trimmedPath, err)
					api.RespondEnvelopeError(w, http.StatusInternalServerError, "field_code="+fieldCode+" saved but cdm_variable audit failed", "")
					return
				}
			}

			for _, al := range f.Aliases {
				consumer := normalizeConsumer(al.ConsumerSystem)
				aliasKey := strings.TrimSpace(al.AliasKey)
				usageKind := strings.ToLower(strings.TrimSpace(al.UsageKind))
				if consumer == "" || aliasKey == "" {
					api.RespondEnvelopeError(w, http.StatusBadRequest, "each alias needs consumer_system and alias_key (field_code="+fieldCode+")", "VALIDATION_ERROR")
					return
				}
				if usageKind == "" {
					usageKind = "scalar"
				}
				if !validUsageKinds[usageKind] {
					api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid usage_kind for field_code="+fieldCode+" consumer_system="+consumer, "VALIDATION_ERROR")
					return
				}
				_, err = tx.Exec(r.Context(), `
					INSERT INTO domain_catalog.field_alias (field_id, consumer_system, alias_key, container_name, usage_kind, sort_order)
					VALUES ($1, $2, $3, $4, $5, $6)
					ON CONFLICT (consumer_system, alias_key, container_name) DO UPDATE SET
						field_id = EXCLUDED.field_id,
						usage_kind = EXCLUDED.usage_kind,
						sort_order = EXCLUDED.sort_order,
						is_deleted = false`,
					fieldID, consumer, aliasKey, al.ContainerName, usageKind, al.SortOrder)
				if err != nil {
					api.LogErrorForResponse(w, "domain-catalog bulk field-alias upsert (field_code=%s consumer=%s): %v", fieldCode, consumer, err)
					api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to upsert alias for field_code="+fieldCode+" consumer_system="+consumer, "")
					return
				}
				aliasesUpserted++
			}
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to commit", "")
			return
		}
		api.RespondEnvelopeSuccess(w, "Bulk fields upserted", map[string]interface{}{
			"sub_module_code":  req.SubModuleCode,
			"fields_upserted":  fieldsUpserted,
			"aliases_upserted": aliasesUpserted,
		})
	}
}
