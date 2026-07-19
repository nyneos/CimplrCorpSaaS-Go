package policy

import (
	"net/http"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type updateReq struct {
	PolicyID string `json:"policy_id"`
	createReq
}

// HandleUpdate applies the edit to policy_master immediately (rule columns +
// children replaced) but flags PENDING_EDIT_APPROVAL; old_/new_ header values
// are recorded so a reject can revert.
func HandleUpdate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req updateReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.trim()
		if req.PolicyID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "policy_id is required", "VALIDATION_ERROR")
			return
		}
		if msg := req.validate(); msg != "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, msg, "VALIDATION_ERROR")
			return
		}
		rf, err := parseRuleConfig(req.RuleType, req.Config)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, err.Error(), "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "policy update begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update policy", "POLICY_UPDATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		old, err := loadPolicyDetail(r, pool, req.PolicyID)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "policy not found", "NOT_FOUND")
			return
		}
		if common.IsPendingStatus(old.ProcessingStatus) {
			api.RespondEnvelopeError(w, http.StatusConflict, "policy already has a pending request", "POLICY_PENDING_EXISTS")
			return
		}
		if req.EffectiveStart == "" {
			req.EffectiveStart = old.EffectiveStart
		}

		if _, err := tx.Exec(r.Context(), `
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
				addl_expression = NULLIF($41,''), effective_start = $42::date, effective_end = NULLIF($43,'')::date,
				version = version + 1, processing_status = 'PENDING_EDIT_APPROVAL',
				last_modified_by = $44, last_modified_at = now()
			WHERE policy_id = $45::uuid`,
			req.Code, req.Name, req.Description, req.Category, req.SubCategory,
			req.ValidationLevel, req.Criticality, req.ActionOnBreach, req.NotificationGroup,
			req.BreachMessage, req.RequiresApproval, req.Applicability, req.CanOverride,
			req.InstrumentFilter, req.CurrencyFilter, req.TenorFilter, req.RatingFilter, req.RuleType, req.NullHandling,
			rf.ThrVariable, rf.ThrOperator, rf.ThrValue, rf.ThrValueMode, rf.ThrPercentBase, rf.ThrUnit,
			rf.SlabVariable, rf.SlabUnit,
			rf.CompBase, rf.CompTotalCheckVariable, rf.CompTotalCheckMin, rf.CompTotalCheckMax,
			rf.ListTargetField, rf.ListMode, rf.ListSource, rf.ListDynamicRef, rf.ListCaseSensitive,
			rf.FormulaExpression, rf.FormulaReturnType, rf.FormulaOperator, rf.FormulaValue,
			req.AddlExpression, req.EffectiveStart, req.EffectiveEnd,
			actor, req.PolicyID,
		); err != nil {
			api.LogErrorForResponse(w, "policy update exec: %v", err)
			api.RespondEnvelopeError(w, http.StatusConflict, "failed to update policy (duplicate code?)", "POLICY_UPDATE_FAILED")
			return
		}

		if err := replacePolicyChildren(r.Context(), tx, r, req.PolicyID, req.TriggerEvents, req.Modules, req.RuleType, rf); err != nil {
			api.LogErrorForResponse(w, "policy update children: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadRequest, "failed to attach triggers/modules/rule rows (unknown code?)", "POLICY_UPDATE_FAILED")
			return
		}

		if err := insertEditAudit(r, tx, req.PolicyID, actor, ip, old, req.createReq, rf); err != nil {
			api.LogErrorForResponse(w, "policy update audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit policy update", "POLICY_UPDATE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "policy update commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to update policy", "POLICY_UPDATE_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Policy edit submitted for approval", map[string]string{"policy_id": req.PolicyID})
	}
}
