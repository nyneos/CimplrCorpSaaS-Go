package policy

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleAuditLog returns maker-checker audit rows for one policy (newest activity first).
func HandleAuditLog(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req struct {
			PolicyID   string `json:"policy_id"`
			ModuleCode string `json:"module_code"`
			FromDate   string `json:"from_date"`
			ToDate     string `json:"to_date"`
			Limit      int    `json:"limit"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid JSON body", "BAD_REQUEST")
			return
		}
		policyID := strings.TrimSpace(req.PolicyID)
		moduleCode := strings.TrimSpace(req.ModuleCode)
		fromDate := strings.TrimSpace(req.FromDate)
		toDate := strings.TrimSpace(req.ToDate)
		if policyID == "" && moduleCode == "" && fromDate == "" && toDate == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "policy_id, module_code or a date range is required", "BAD_REQUEST")
			return
		}
		for _, d := range []string{fromDate, toDate} {
			if d == "" {
				continue
			}
			if _, err := time.Parse("2006-01-02", d); err != nil {
				api.RespondEnvelopeError(w, http.StatusBadRequest, "from_date and to_date must be yyyy-mm-dd", "BAD_REQUEST")
				return
			}
		}
		limit := req.Limit
		if limit <= 0 || limit > 5000 {
			limit = 1000
		}

		// Explicit columns (uuid::text) — SELECT * JSON-encoded poorly for the FE audit mapper.
		rows, err := pool.Query(r.Context(), `
			SELECT
				audit_id::text,
				policy_id::text,
				action_type,
				processing_status,
				COALESCE(reason, '') AS reason,
				COALESCE(requested_by, '') AS requested_by,
				requested_at,
				COALESCE(requested_ip, '') AS requested_ip,
				COALESCE(checker_by, '') AS checker_by,
				checker_at,
				COALESCE(checker_ip, '') AS checker_ip,
				COALESCE(checker_comment, '') AS checker_comment,
				old_code, new_code,
				old_name, new_name,
				old_description, new_description,
				old_category, new_category,
				old_sub_category, new_sub_category,
				old_source, new_source,
				old_library_ref, new_library_ref,
				old_validation_level, new_validation_level,
				old_criticality, new_criticality,
				old_action_on_breach, new_action_on_breach,
				old_approval_matrix_id, new_approval_matrix_id,
				old_notification_group, new_notification_group,
				old_breach_message, new_breach_message,
				old_requires_approval, new_requires_approval,
				old_applicability, new_applicability,
				old_can_override, new_can_override,
				old_instrument_filter, new_instrument_filter,
				old_currency_filter, new_currency_filter,
				old_tenor_filter, new_tenor_filter,
				old_rating_filter, new_rating_filter,
				old_rule_type, new_rule_type,
				old_null_handling, new_null_handling,
				old_null_handling_default, new_null_handling_default,
				old_thr_variable, new_thr_variable,
				old_thr_operator, new_thr_operator,
				old_thr_value, new_thr_value,
				old_thr_value_mode, new_thr_value_mode,
				old_thr_percent_base, new_thr_percent_base,
				old_thr_unit, new_thr_unit,
				old_slab_variable, new_slab_variable,
				old_slab_unit, new_slab_unit,
				old_slab_percent_base, new_slab_percent_base,
				old_comp_base, new_comp_base,
				old_comp_total_check_variable, new_comp_total_check_variable,
				old_comp_total_check_min, new_comp_total_check_min,
				old_comp_total_check_max, new_comp_total_check_max,
				old_list_target_field, new_list_target_field,
				old_list_mode, new_list_mode,
				old_list_source, new_list_source,
				old_list_dynamic_ref, new_list_dynamic_ref,
				old_list_case_sensitive, new_list_case_sensitive,
				old_formula_expression, new_formula_expression,
				old_formula_return_type, new_formula_return_type,
				old_formula_operator, new_formula_operator,
				old_formula_value, new_formula_value,
				old_addl_expression, new_addl_expression,
				old_status, new_status,
				old_version, new_version,
				old_effective_start, new_effective_start,
				old_effective_end, new_effective_end,
				old_is_deleted, new_is_deleted,
				old_trigger_events, new_trigger_events,
				old_modules, new_modules,
				old_sub_modules, new_sub_modules,
				old_entities_include, new_entities_include,
				old_entities_exclude, new_entities_exclude,
				old_list_values, new_list_values,
				old_slab_rows, new_slab_rows,
				old_comp_buckets, new_comp_buckets,
				old_notification_template_ids, new_notification_template_ids
			FROM policyengine_svc.policy_master_audit
			WHERE ($1 = '' OR policy_id = NULLIF($1,'')::uuid)
			  AND ($2 = '' OR EXISTS (
			        SELECT 1 FROM policyengine_svc.policy_module pmod
			        WHERE pmod.policy_id = policy_master_audit.policy_id
			          AND pmod.is_deleted = false
			          AND pmod.module_code = $2))
			  AND ($3 = '' OR COALESCE(requested_at, checker_at) >= $3::date)
			  AND ($4 = '' OR COALESCE(requested_at, checker_at) < ($4::date + 1))
			ORDER BY GREATEST(
				COALESCE(requested_at, '1970-01-01'::timestamptz),
				COALESCE(checker_at, '1970-01-01'::timestamptz)
			) DESC
			LIMIT $5`, policyID, moduleCode, fromDate, toDate, limit)
		if err != nil {
			api.LogErrorForResponse(w, "policy audit-log: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load policy audit", "POLICY_AUDIT_FAILED")
			return
		}
		defer rows.Close()

		out, err := rowsToMaps(rows)
		if err != nil {
			api.LogErrorForResponse(w, "policy audit-log scan: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load policy audit", "POLICY_AUDIT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Policy audit fetched", out)
	}
}

func rowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	fds := rows.FieldDescriptions()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(fds))
		for i, fd := range fds {
			row[string(fd.Name)] = normalizeAuditValue(vals[i])
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func normalizeAuditValue(v interface{}) interface{} {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().Format(time.RFC3339)
	case *time.Time:
		if t == nil {
			return nil
		}
		return t.UTC().Format(time.RFC3339)
	default:
		return v
	}
}
