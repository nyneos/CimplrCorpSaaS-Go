package policy

import (
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5"
)

// auditCols/auditVals build an INSERT's column list and $N placeholders from
// ordered (column, value) pairs — this avoids hand-counting $N positions across
// the ~50 old_/new_ column pairs on policy_master_audit.
type auditRow struct {
	cols []string
	vals []interface{}
}

func (a *auditRow) set(col string, val interface{}) {
	a.cols = append(a.cols, col)
	a.vals = append(a.vals, val)
}

func (a *auditRow) exec(r *http.Request, tx pgx.Tx, table string) error {
	placeholders := make([]string, len(a.vals))
	for i := range a.vals {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(a.cols, ", "), strings.Join(placeholders, ", "))
	_, err := tx.Exec(r.Context(), query, a.vals...)
	return err
}

// insertCreateAudit records the CREATE request row (new_* only — no old_*)
// for a freshly submitted policy, mirroring the master's header columns.
func insertCreateAudit(r *http.Request, tx pgx.Tx, policyID, actor, ip string, req createReq, rf ruleFields) error {
	a := &auditRow{}
	a.set("policy_id", policyID)
	a.set("action_type", "CREATE")
	a.set("processing_status", "PENDING_APPROVAL")
	a.set("requested_by", actor)
	a.set("requested_ip", common.NullIfEmpty(ip))
	a.set("new_code", req.Code)
	a.set("new_name", req.Name)
	a.set("new_description", req.Description)
	a.set("new_category", req.Category)
	a.set("new_sub_category", common.NullIfEmpty(req.SubCategory))
	a.set("new_source", "Custom")
	a.set("new_validation_level", req.ValidationLevel)
	a.set("new_criticality", req.Criticality)
	a.set("new_action_on_breach", req.ActionOnBreach)
	a.set("new_approval_matrix_id", common.NullIfEmpty(req.ApprovalMatrixID))
	a.set("new_notification_group", common.NullIfEmpty(req.NotificationGroup))
	a.set("new_breach_message", req.BreachMessage)
	a.set("new_requires_approval", req.RequiresApproval)
	a.set("new_applicability", req.Applicability)
	a.set("new_can_override", req.CanOverride)
	a.set("new_instrument_filter", common.NullIfEmpty(req.InstrumentFilter))
	a.set("new_currency_filter", common.NullIfEmpty(req.CurrencyFilter))
	a.set("new_tenor_filter", common.NullIfEmpty(req.TenorFilter))
	a.set("new_rating_filter", common.NullIfEmpty(req.RatingFilter))
	a.set("new_rule_type", req.RuleType)
	a.set("new_null_handling", req.NullHandling)
	a.set("new_null_handling_default", common.NullIfEmpty(req.NullHandlingDefault))
	setRuleFieldsNew(a, rf)
	a.set("new_addl_expression", common.NullIfEmpty(req.AddlExpression))
	a.set("new_status", "PendingApproval")
	a.set("new_version", 1)
	a.set("new_effective_start", req.EffectiveStart)
	a.set("new_effective_end", common.NullIfEmpty(req.EffectiveEnd))
	a.set("new_is_deleted", false)
	return a.exec(r, tx, "policyengine_svc.policy_master_audit")
}

// editAuditActor groups the requester identity fields for insertEditAudit so the
// function stays within the project's max-params limit.
type editAuditActor struct {
	PolicyID string
	Actor    string
	IP       string
}

// insertEditAudit records old_/new_ pairs for an EDIT request so a reject can revert.
func insertEditAudit(r *http.Request, tx pgx.Tx, who editAuditActor, old *DetailItem, req createReq, rf ruleFields) error {
	a := &auditRow{}
	a.set("policy_id", who.PolicyID)
	a.set("action_type", "EDIT")
	a.set("processing_status", "PENDING_EDIT_APPROVAL")
	a.set("requested_by", who.Actor)
	a.set("requested_ip", common.NullIfEmpty(who.IP))

	pair := func(col string, oldVal, newVal interface{}) {
		a.set("old_"+col, oldVal)
		a.set("new_"+col, newVal)
	}
	pair("code", old.Code, req.Code)
	pair("name", old.Name, req.Name)
	pair("description", old.Description, req.Description)
	pair("category", old.Category, req.Category)
	pair("sub_category", common.NullIfEmpty(old.SubCategory), common.NullIfEmpty(req.SubCategory))
	pair("validation_level", old.ValidationLevel, req.ValidationLevel)
	pair("criticality", old.Criticality, req.Criticality)
	pair("action_on_breach", old.ActionOnBreach, req.ActionOnBreach)
	pair("approval_matrix_id", common.NullIfEmpty(old.ApprovalMatrixID), common.NullIfEmpty(req.ApprovalMatrixID))
	pair("notification_group", common.NullIfEmpty(old.NotificationGroup), common.NullIfEmpty(req.NotificationGroup))
	pair("breach_message", old.BreachMessage, req.BreachMessage)
	pair("requires_approval", old.RequiresApproval, req.RequiresApproval)
	pair("applicability", old.Applicability, req.Applicability)
	pair("can_override", old.CanOverride, req.CanOverride)
	pair("instrument_filter", common.NullIfEmpty(old.InstrumentFilter), common.NullIfEmpty(req.InstrumentFilter))
	pair("currency_filter", common.NullIfEmpty(old.CurrencyFilter), common.NullIfEmpty(req.CurrencyFilter))
	pair("tenor_filter", common.NullIfEmpty(old.TenorFilter), common.NullIfEmpty(req.TenorFilter))
	pair("rating_filter", common.NullIfEmpty(old.RatingFilter), common.NullIfEmpty(req.RatingFilter))
	pair("rule_type", old.RuleType, req.RuleType)
	pair("null_handling", old.NullHandling, req.NullHandling)
	pair("null_handling_default", common.NullIfEmpty(old.NullHandlingDefault), common.NullIfEmpty(req.NullHandlingDefault))
	pair("thr_variable", common.NullIfEmpty(old.ThrVariable), common.NullIfEmpty(rf.ThrVariable))
	pair("thr_operator", common.NullIfEmpty(old.ThrOperator), common.NullIfEmpty(rf.ThrOperator))
	pair("thr_value", old.ThrValue, rf.ThrValue)
	pair("thr_value_mode", common.NullIfEmpty(old.ThrValueMode), common.NullIfEmpty(rf.ThrValueMode))
	pair("thr_percent_base", common.NullIfEmpty(old.ThrPercentBase), common.NullIfEmpty(rf.ThrPercentBase))
	pair("thr_unit", common.NullIfEmpty(old.ThrUnit), common.NullIfEmpty(rf.ThrUnit))
	pair("slab_variable", common.NullIfEmpty(old.SlabVariable), common.NullIfEmpty(rf.SlabVariable))
	pair("slab_unit", common.NullIfEmpty(old.SlabUnit), common.NullIfEmpty(rf.SlabUnit))
	pair("slab_percent_base", common.NullIfEmpty(old.SlabPercentBase), common.NullIfEmpty(rf.SlabPercentBase))
	pair("comp_base", common.NullIfEmpty(old.CompBase), common.NullIfEmpty(rf.CompBase))
	pair("comp_total_check_variable", common.NullIfEmpty(old.CompTotalCheckVariable), common.NullIfEmpty(rf.CompTotalCheckVariable))
	pair("comp_total_check_min", old.CompTotalCheckMin, rf.CompTotalCheckMin)
	pair("comp_total_check_max", old.CompTotalCheckMax, rf.CompTotalCheckMax)
	pair("list_target_field", common.NullIfEmpty(old.ListTargetField), common.NullIfEmpty(rf.ListTargetField))
	pair("list_mode", common.NullIfEmpty(old.ListMode), common.NullIfEmpty(rf.ListMode))
	pair("list_source", common.NullIfEmpty(old.ListSource), common.NullIfEmpty(rf.ListSource))
	pair("list_dynamic_ref", common.NullIfEmpty(old.ListDynamicRef), common.NullIfEmpty(rf.ListDynamicRef))
	pair("list_case_sensitive", old.ListCaseSensitive, rf.ListCaseSensitive)
	pair("formula_expression", common.NullIfEmpty(old.FormulaExpression), common.NullIfEmpty(rf.FormulaExpression))
	pair("formula_return_type", common.NullIfEmpty(old.FormulaReturnType), common.NullIfEmpty(rf.FormulaReturnType))
	pair("formula_operator", common.NullIfEmpty(old.FormulaOperator), common.NullIfEmpty(rf.FormulaOperator))
	pair("formula_value", old.FormulaValue, rf.FormulaValue)
	pair("addl_expression", common.NullIfEmpty(old.AddlExpression), common.NullIfEmpty(req.AddlExpression))
	a.set("old_status", old.Status)
	a.set("new_status", old.Status)
	pair("version", old.Version, old.Version+1)
	pair("effective_start", old.EffectiveStart, req.EffectiveStart)
	pair("effective_end", common.NullIfEmpty(old.EffectiveEnd), common.NullIfEmpty(req.EffectiveEnd))
	a.set("old_is_deleted", false)
	a.set("new_is_deleted", false)

	// 1:many children — header columns alone miss slab/list/trigger edits.
	pair("trigger_events", joinSortedCSV(old.TriggerEvents), joinSortedCSV(req.TriggerEvents))
	pair("modules", joinSortedCSV(old.Modules), joinSortedCSV(req.Modules))
	pair("sub_modules", joinSortedCSV(old.SubModules), joinSortedCSV(req.SubModules))
	pair("entities_include", joinSortedCSV(old.EntitiesInclude), joinSortedCSV(req.EntitiesInclude))
	pair("entities_exclude", joinSortedCSV(old.EntitiesExclude), joinSortedCSV(req.EntitiesExclude))
	pair("list_values", joinSortedCSV(old.ListValues), joinSortedCSV(rf.ListValues))
	pair("slab_rows", serializeSlabRows(old.SlabRows), serializeSlabRows(rf.SlabRows))
	pair("comp_buckets", serializeCompBuckets(old.CompBuckets), serializeCompBuckets(rf.CompBuckets))
	pair("notification_template_ids", joinSortedCSV(old.NotificationTemplateIDs), joinSortedCSV(req.NotificationTemplateIDs))

	return a.exec(r, tx, "policyengine_svc.policy_master_audit")
}

func setRuleFieldsNew(a *auditRow, rf ruleFields) {
	a.set("new_thr_variable", common.NullIfEmpty(rf.ThrVariable))
	a.set("new_thr_operator", common.NullIfEmpty(rf.ThrOperator))
	a.set("new_thr_value", rf.ThrValue)
	a.set("new_thr_value_mode", common.NullIfEmpty(rf.ThrValueMode))
	a.set("new_thr_percent_base", common.NullIfEmpty(rf.ThrPercentBase))
	a.set("new_thr_unit", common.NullIfEmpty(rf.ThrUnit))
	a.set("new_slab_variable", common.NullIfEmpty(rf.SlabVariable))
	a.set("new_slab_unit", common.NullIfEmpty(rf.SlabUnit))
	a.set("new_slab_percent_base", common.NullIfEmpty(rf.SlabPercentBase))
	a.set("new_comp_base", common.NullIfEmpty(rf.CompBase))
	a.set("new_comp_total_check_variable", common.NullIfEmpty(rf.CompTotalCheckVariable))
	a.set("new_comp_total_check_min", rf.CompTotalCheckMin)
	a.set("new_comp_total_check_max", rf.CompTotalCheckMax)
	a.set("new_list_target_field", common.NullIfEmpty(rf.ListTargetField))
	a.set("new_list_mode", common.NullIfEmpty(rf.ListMode))
	a.set("new_list_source", common.NullIfEmpty(rf.ListSource))
	a.set("new_list_dynamic_ref", common.NullIfEmpty(rf.ListDynamicRef))
	a.set("new_list_case_sensitive", rf.ListCaseSensitive)
	a.set("new_formula_expression", common.NullIfEmpty(rf.FormulaExpression))
	a.set("new_formula_return_type", common.NullIfEmpty(rf.FormulaReturnType))
	a.set("new_formula_operator", common.NullIfEmpty(rf.FormulaOperator))
	a.set("new_formula_value", rf.FormulaValue)
}

// pendingAudit is the minimal projection of a policy_master_audit row needed to
// action a checker decision (approve/reject).
type pendingAudit struct {
	AuditID    string
	ActionType string
}

func findPendingAudit(r *http.Request, tx pgx.Tx, policyID string) (pendingAudit, error) {
	var pa pendingAudit
	err := tx.QueryRow(r.Context(), `
		SELECT audit_id::text, action_type
		FROM policyengine_svc.policy_master_audit
		WHERE policy_id = $1::uuid AND processing_status = ANY($2::text[])
		ORDER BY requested_at DESC
		LIMIT 1
		FOR UPDATE`, policyID, common.PendingProcessingStatuses,
	).Scan(&pa.AuditID, &pa.ActionType)
	return pa, err
}
