package policy

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleReject bulk-rejects pending CREATE/EDIT/DELETE requests. A rejected
// EDIT reverts the master row's header columns to the audit row's old_*
// values (child rows — triggers/modules/rule rows — are not reverted, since
// there is no child-row snapshot; re-edit to fix them if needed). A rejected
// CREATE is marked Inactive; a rejected DELETE simply leaves is_deleted false.
func HandleReject(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req decisionReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		if len(req.IDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "ids is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "policy reject begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject policies", "POLICY_REJECT_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		rejected := make([]string, 0, len(req.IDs))
		errs := make([]string, 0)
		for _, raw := range req.IDs {
			id := strings.TrimSpace(raw)
			if id == "" {
				continue
			}
			pa, err := findPendingAudit(r, tx, id)
			if err != nil {
				errs = append(errs, id+": no pending request")
				continue
			}
			if _, err := tx.Exec(r.Context(), `
				UPDATE policyengine_svc.policy_master_audit
				SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_ip = $2, checker_comment = $3
				WHERE audit_id = $4::uuid`,
				actor, common.NullIfEmpty(ip), common.NullIfEmpty(req.CheckerComment), pa.AuditID,
			); err != nil {
				errs = append(errs, id+": "+err.Error())
				continue
			}

			var execErr error
			switch pa.ActionType {
			case "DELETE":
				_, execErr = tx.Exec(r.Context(), `
					UPDATE policyengine_svc.policy_master
					SET processing_status = 'REJECTED', last_modified_by = $1, last_modified_at = now()
					WHERE policy_id = $2::uuid`, actor, id)
			case "CREATE":
				_, execErr = tx.Exec(r.Context(), `
					UPDATE policyengine_svc.policy_master
					SET processing_status = 'REJECTED', status = 'Inactive', last_modified_by = $1, last_modified_at = now()
					WHERE policy_id = $2::uuid`, actor, id)
			default: // EDIT
				execErr = revertEditFromAudit(r, tx, id, pa.AuditID, actor)
			}
			if execErr != nil {
				errs = append(errs, id+": "+execErr.Error())
				continue
			}
			rejected = append(rejected, id)
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "policy reject commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject policies", "POLICY_REJECT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Policies rejected", map[string]interface{}{
			"rejected": rejected,
			"errors":   errs,
		})
	}
}

// revertEditFromAudit reapplies the audit row's old_* header columns to
// policy_master and flags it REJECTED.
func revertEditFromAudit(r *http.Request, tx pgx.Tx, policyID, auditID, actor string) error {
	var old DetailItem
	err := tx.QueryRow(r.Context(), `
		SELECT old_code, old_name, old_description, old_category, COALESCE(old_sub_category, ''),
		       old_validation_level, old_criticality, old_action_on_breach, COALESCE(old_notification_group, ''),
		       old_breach_message, old_requires_approval, old_applicability, old_can_override,
		       COALESCE(old_instrument_filter, ''), COALESCE(old_currency_filter, ''), COALESCE(old_tenor_filter, ''),
		       COALESCE(old_rating_filter, ''), old_rule_type, old_null_handling,
		       COALESCE(old_thr_variable, ''), COALESCE(old_thr_operator, ''), old_thr_value,
		       COALESCE(old_thr_value_mode, ''), COALESCE(old_thr_percent_base, ''), COALESCE(old_thr_unit, ''),
		       COALESCE(old_slab_variable, ''), COALESCE(old_slab_unit, ''),
		       COALESCE(old_comp_base, ''), COALESCE(old_comp_total_check_variable, ''),
		       old_comp_total_check_min, old_comp_total_check_max,
		       COALESCE(old_list_target_field, ''), COALESCE(old_list_mode, ''), COALESCE(old_list_source, ''),
		       COALESCE(old_list_dynamic_ref, ''), old_list_case_sensitive,
		       COALESCE(old_formula_expression, ''), COALESCE(old_formula_return_type, ''),
		       COALESCE(old_formula_operator, ''), old_formula_value,
		       COALESCE(old_addl_expression, ''), old_version, old_effective_start::text, COALESCE(old_effective_end::text, '')
		FROM policyengine_svc.policy_master_audit
		WHERE audit_id = $1::uuid`, auditID,
	).Scan(&old.Code, &old.Name, &old.Description, &old.Category, &old.SubCategory,
		&old.ValidationLevel, &old.Criticality, &old.ActionOnBreach, &old.NotificationGroup,
		&old.BreachMessage, &old.RequiresApproval, &old.Applicability, &old.CanOverride,
		&old.InstrumentFilter, &old.CurrencyFilter, &old.TenorFilter, &old.RatingFilter, &old.RuleType, &old.NullHandling,
		&old.ThrVariable, &old.ThrOperator, &old.ThrValue, &old.ThrValueMode, &old.ThrPercentBase, &old.ThrUnit,
		&old.SlabVariable, &old.SlabUnit, &old.CompBase, &old.CompTotalCheckVariable,
		&old.CompTotalCheckMin, &old.CompTotalCheckMax,
		&old.ListTargetField, &old.ListMode, &old.ListSource, &old.ListDynamicRef, &old.ListCaseSensitive,
		&old.FormulaExpression, &old.FormulaReturnType, &old.FormulaOperator, &old.FormulaValue,
		&old.AddlExpression, &old.Version, &old.EffectiveStart, &old.EffectiveEnd,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(r.Context(), `
		UPDATE policyengine_svc.policy_master SET
			code = $1, name = $2, description = $3, category = $4, sub_category = NULLIF($5,''),
			validation_level = $6, criticality = $7, action_on_breach = $8, notification_group = NULLIF($9,''),
			breach_message = $10, requires_approval = $11, applicability = $12, can_override = $13,
			instrument_filter = NULLIF($14,''), currency_filter = NULLIF($15,''), tenor_filter = NULLIF($16,''),
			rating_filter = NULLIF($17,''), rule_type = $18, null_handling = $19,
			thr_variable = NULLIF($20,''), thr_operator = NULLIF($21,''), thr_value = $22,
			thr_value_mode = NULLIF($23,''), thr_percent_base = NULLIF($24,''), thr_unit = NULLIF($25,''),
			slab_variable = NULLIF($26,''), slab_unit = NULLIF($27,''),
			comp_base = NULLIF($28,''), comp_total_check_variable = NULLIF($29,''),
			comp_total_check_min = $30, comp_total_check_max = $31,
			list_target_field = NULLIF($32,''), list_mode = NULLIF($33,''), list_source = NULLIF($34,''),
			list_dynamic_ref = NULLIF($35,''), list_case_sensitive = $36,
			formula_expression = NULLIF($37,''), formula_return_type = NULLIF($38,''),
			formula_operator = NULLIF($39,''), formula_value = $40,
			addl_expression = NULLIF($41,''), version = $42, effective_start = $43::date,
			effective_end = NULLIF($44,'')::date,
			processing_status = 'REJECTED', last_modified_by = $45, last_modified_at = now()
		WHERE policy_id = $46::uuid`,
		old.Code, old.Name, old.Description, old.Category, old.SubCategory,
		old.ValidationLevel, old.Criticality, old.ActionOnBreach, old.NotificationGroup,
		old.BreachMessage, old.RequiresApproval, old.Applicability, old.CanOverride,
		old.InstrumentFilter, old.CurrencyFilter, old.TenorFilter, old.RatingFilter, old.RuleType, old.NullHandling,
		old.ThrVariable, old.ThrOperator, old.ThrValue, old.ThrValueMode, old.ThrPercentBase, old.ThrUnit,
		old.SlabVariable, old.SlabUnit,
		old.CompBase, old.CompTotalCheckVariable, old.CompTotalCheckMin, old.CompTotalCheckMax,
		old.ListTargetField, old.ListMode, old.ListSource, old.ListDynamicRef, old.ListCaseSensitive,
		old.FormulaExpression, old.FormulaReturnType, old.FormulaOperator, old.FormulaValue,
		old.AddlExpression, old.Version, old.EffectiveStart, old.EffectiveEnd,
		actor, policyID,
	)
	return err
}
