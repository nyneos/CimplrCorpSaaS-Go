package policy

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

var revertedHeaderCols = []string{
	"code", "name", "description", "category", "sub_category",
	"validation_level", "criticality", "action_on_breach", "approval_matrix_id", "notification_group",
	"breach_message", "requires_approval", "applicability", "can_override",
	"instrument_filter", "currency_filter", "tenor_filter", "rating_filter",
	"rule_type", "null_handling", "null_handling_default",
	"thr_variable", "thr_operator", "thr_value", "thr_value_mode", "thr_percent_base", "thr_unit",
	"slab_variable", "slab_unit", "slab_percent_base",
	"comp_base", "comp_total_check_variable", "comp_total_check_min", "comp_total_check_max",
	"list_target_field", "list_mode", "list_source", "list_dynamic_ref", "list_case_sensitive",
	"formula_expression", "formula_return_type", "formula_operator", "formula_value",
	"addl_expression", "version",
}

func revertPolicyEdit(r *http.Request, tx pgx.Tx, auditID, policyID, actor string) error {
	ctx := r.Context()
	sets := make([]string, 0, len(revertedHeaderCols)+2)
	for _, c := range revertedHeaderCols {
		sets = append(sets, fmt.Sprintf("%s = a.old_%s", c, c))
	}
	sets = append(sets,
		"effective_start = NULLIF(a.old_effective_start::text, '')::date",
		"effective_end = NULLIF(a.old_effective_end::text, '')::date",
	)
	q := fmt.Sprintf(`
		UPDATE policyengine_svc.policy_master m SET %s,
			processing_status = 'APPROVED', last_modified_by = $3, last_modified_at = now()
		FROM policyengine_svc.policy_master_audit a
		WHERE a.audit_id = $1::uuid AND m.policy_id = $2::uuid AND a.policy_id = m.policy_id`,
		strings.Join(sets, ", "))
	tag, err := tx.Exec(ctx, q, auditID, policyID, actor)
	if err != nil {
		return fmt.Errorf("revert header: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("revert header: policy %s not found", policyID)
	}

	var ruleType, triggers, modules, subModules, incl, excl, listValues, slabs, buckets, templates string
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(old_rule_type, ''), COALESCE(old_trigger_events, ''), COALESCE(old_modules, ''),
		       COALESCE(old_sub_modules, ''), COALESCE(old_entities_include, ''), COALESCE(old_entities_exclude, ''),
		       COALESCE(old_list_values, ''), COALESCE(old_slab_rows, ''), COALESCE(old_comp_buckets, ''),
		       COALESCE(old_notification_template_ids, '')
		FROM policyengine_svc.policy_master_audit WHERE audit_id = $1::uuid`, auditID,
	).Scan(&ruleType, &triggers, &modules, &subModules, &incl, &excl, &listValues, &slabs, &buckets, &templates); err != nil {
		return fmt.Errorf("revert children read: %w", err)
	}

	slabRows, err := parseSlabRows(slabs)
	if err != nil {
		return err
	}
	compBuckets, err := parseCompBuckets(buckets)
	if err != nil {
		return err
	}
	if err := replacePolicyChildren(ctx, tx, r, policyID, policyChildrenSpec{
		TriggerEvents:   splitAuditCSV(triggers),
		Modules:         splitAuditCSV(modules),
		SubModules:      splitAuditCSV(subModules),
		EntitiesInclude: splitAuditCSV(incl),
		EntitiesExclude: splitAuditCSV(excl),
		RuleType:        ruleType,
		RF: ruleFields{
			SlabRows:    slabRows,
			CompBuckets: compBuckets,
			ListValues:  splitAuditCSV(listValues),
		},
	}); err != nil {
		return fmt.Errorf("revert children: %w", err)
	}
	if err := replacePolicyNotificationTemplates(ctx, tx, policyID, splitAuditCSV(templates), actor); err != nil {
		return fmt.Errorf("revert templates: %w", err)
	}
	return nil
}

func splitAuditCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ", ")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseSlabRows(s string) ([]slabRowConfig, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []slabRowConfig
	for _, part := range strings.Split(s, "; ") {
		f := strings.Split(part, "|")
		if len(f) != 6 {
			return nil, fmt.Errorf("revert slab row: unexpected format %q", part)
		}
		from, err := strconv.ParseFloat(f[0], 64)
		if err != nil {
			return nil, fmt.Errorf("revert slab row from: %w", err)
		}
		row := slabRowConfig{From: from, Mode: f[2], Action: f[3], ApprovalRef: f[4], Label: f[5]}
		if f[1] != "∞" {
			to, err := strconv.ParseFloat(f[1], 64)
			if err != nil {
				return nil, fmt.Errorf("revert slab row to: %w", err)
			}
			row.To = &to
		}
		out = append(out, row)
	}
	return out, nil
}

func parseCompBuckets(s string) ([]compositionBucketConfig, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []compositionBucketConfig
	for _, part := range strings.Split(s, "; ") {
		f := strings.Split(part, "|")
		if len(f) != 4 {
			return nil, fmt.Errorf("revert composition bucket: unexpected format %q", part)
		}
		b := compositionBucketConfig{Label: f[0], Variable: f[1]}
		for i, dst := range []**float64{&b.Min, &b.Max} {
			if f[2+i] == "-" {
				continue
			}
			v, err := strconv.ParseFloat(f[2+i], 64)
			if err != nil {
				return nil, fmt.Errorf("revert composition bucket bound: %w", err)
			}
			*dst = &v
		}
		out = append(out, b)
	}
	return out, nil
}
