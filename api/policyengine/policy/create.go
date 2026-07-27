package policy

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type createReq struct {
	Code              string `json:"code"`
	Name              string `json:"name"`
	Description       string `json:"description"`
	Category          string `json:"category"`
	SubCategory       string `json:"sub_category"`
	ValidationLevel   string `json:"validation_level"`
	Criticality       string `json:"criticality"`
	ActionOnBreach    string `json:"action_on_breach"`
	NotificationGroup string `json:"notification_group"`
	// NotificationTemplateIDs — curated notification_svc.template picks from
	// the policy form's template multi-select. Empty = every enabled channel
	// for NotificationGroup's event fires (unchanged default behavior).
	NotificationTemplateIDs []string        `json:"notification_template_ids"`
	BreachMessage           string          `json:"breach_message"`
	RequiresApproval        bool            `json:"requires_approval"`
	Applicability           string          `json:"applicability"`
	CanOverride             bool            `json:"can_override"`
	Modules                 []string        `json:"modules"`
	// SubModules — domain_catalog sub_module codes (UI "forms"), e.g. FD_BOOKING.
	// Empty = match any sub-module under the selected modules (legacy behaviour).
	SubModules              []string        `json:"sub_modules"`
	TriggerEvents           []string        `json:"trigger_events"`
	EntitiesInclude         []string        `json:"entities_include"`
	EntitiesExclude         []string        `json:"entities_exclude"`
	RuleType                string          `json:"rule_type"`
	Config                  json.RawMessage `json:"config"`
	AddlExpression          string          `json:"addl_expression"`
	NullHandling            string          `json:"null_handling"`
	InstrumentFilter        string          `json:"instrument_filter"`
	CurrencyFilter          string          `json:"currency_filter"`
	TenorFilter             string          `json:"tenor_filter"`
	RatingFilter            string          `json:"rating_filter"`
	EffectiveStart          string          `json:"effective_start"`
	EffectiveEnd            string          `json:"effective_end"`
	ActorID                 string          `json:"actor_id"`
}

func (req *createReq) trim() {
	req.Code = strings.TrimSpace(req.Code)
	req.Name = strings.TrimSpace(req.Name)
	req.Description = strings.TrimSpace(req.Description)
	req.Category = strings.TrimSpace(req.Category)
	req.SubCategory = strings.TrimSpace(req.SubCategory)
	req.ValidationLevel = strings.TrimSpace(req.ValidationLevel)
	req.Criticality = strings.TrimSpace(req.Criticality)
	req.ActionOnBreach = strings.TrimSpace(req.ActionOnBreach)
	req.NotificationGroup = strings.TrimSpace(req.NotificationGroup)
	req.BreachMessage = strings.TrimSpace(req.BreachMessage)
	req.Applicability = strings.TrimSpace(req.Applicability)
	req.RuleType = strings.TrimSpace(req.RuleType)
	req.AddlExpression = strings.TrimSpace(req.AddlExpression)
	req.NullHandling = strings.TrimSpace(req.NullHandling)
	req.InstrumentFilter = strings.TrimSpace(req.InstrumentFilter)
	req.CurrencyFilter = strings.TrimSpace(req.CurrencyFilter)
	req.TenorFilter = strings.TrimSpace(req.TenorFilter)
	req.RatingFilter = strings.TrimSpace(req.RatingFilter)
	req.EffectiveStart = strings.TrimSpace(req.EffectiveStart)
	req.EffectiveEnd = strings.TrimSpace(req.EffectiveEnd)
}

func (req *createReq) validate() string {
	if req.Code == "" || req.Name == "" || req.Description == "" {
		return "code, name, description are required"
	}
	if req.Category == "" || req.ValidationLevel == "" || req.Criticality == "" || req.ActionOnBreach == "" {
		return "category, validation_level, criticality, action_on_breach are required"
	}
	if req.BreachMessage == "" {
		return "breach_message is required"
	}
	if req.Applicability == "" {
		return "applicability is required"
	}
	if req.RuleType == "" {
		return "rule_type is required"
	}
	if len(req.Modules) == 0 {
		return "modules must include at least one module"
	}
	if len(req.SubModules) == 0 {
		return "sub_modules must include at least one sub-module"
	}
	if len(req.TriggerEvents) == 0 {
		return "trigger_events must include at least one event"
	}
	if strings.EqualFold(req.Applicability, "Entity") && len(req.EntitiesInclude) == 0 {
		return "entities_include is required when applicability is Entity"
	}
	if req.NullHandling == "" {
		req.NullHandling = "FailSafe"
	}
	switch req.NullHandling {
	case "FailSafe", "PassThrough", "UseDefault":
	default:
		return "null_handling must be FailSafe, PassThrough, or UseDefault"
	}
	if req.NullHandling == "UseDefault" {
		return "null_handling UseDefault is not supported yet — use FailSafe or PassThrough (missing vars ERROR→HardBlock otherwise)"
	}
	return ""
}

// HandleCreate submits a new policy for checker approval — create IS the
// submission (no separate draft/submit step). status=PendingApproval until approved.
func HandleCreate(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req createReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.trim()
		if msg := req.validate(); msg != "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, msg, "VALIDATION_ERROR")
			return
		}
		rf, err := parseRuleConfig(req.RuleType, req.Config)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, err.Error(), "VALIDATION_ERROR")
			return
		}
		if err := validatePELOnWrite(r.Context(), req.RuleType, rf.FormulaExpression, rf.FormulaReturnType, req.AddlExpression); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, err.Error(), "VALIDATION_ERROR")
			return
		}
		draft := draftConstraintFromReq(req, rf)
		conflictReport, conflictErr := evaluateLaneConflicts(r.Context(), pool, req.Modules, req.SubModules, req.TriggerEvents, "", draft)
		if conflictErr != nil {
			api.LogErrorForResponse(w, "policy create conflict check: %v", conflictErr)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to validate policy conflicts", "POLICY_CONFLICT_CHECK_FAILED")
			return
		}
		if conflictReport.HasImpossible() {
			api.RespondEnvelopeFailureWithData(w, http.StatusBadRequest, conflictErrorMessage(conflictReport), "POLICY_CONFLICT_IMPOSSIBLE", conflictPayload(conflictReport))
			return
		}
		if req.EffectiveStart == "" {
			req.EffectiveStart = time.Now().UTC().Format("2006-01-02")
		}
		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "policy create begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create policy", "POLICY_CREATE_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var policyID string
		err = tx.QueryRow(r.Context(), `
			INSERT INTO policyengine_svc.policy_master (
				code, name, description, category, sub_category, source,
				validation_level, criticality, action_on_breach, notification_group, breach_message,
				requires_approval, applicability, can_override,
				instrument_filter, currency_filter, tenor_filter, rating_filter,
				rule_type, null_handling,
				thr_variable, thr_operator, thr_value, thr_value_date, thr_value_mode, thr_percent_base, thr_unit,
				slab_variable, slab_unit,
				comp_base, comp_total_check_variable, comp_total_check_min, comp_total_check_max,
				list_target_field, list_mode, list_source, list_dynamic_ref, list_case_sensitive,
				formula_expression, formula_return_type, formula_operator, formula_value,
				addl_expression, status, version, effective_start, effective_end,
				processing_status, created_by, last_modified_by
			) VALUES (
				$1,$2,$3,$4,NULLIF($5,''),'Custom',
				$6,$7,$8,NULLIF($9,''),$10,
				$11,$12,$13,
				NULLIF($14,''),NULLIF($15,''),NULLIF($16,''),NULLIF($17,''),
				$18,$19,
				NULLIF($20,''),NULLIF($21,''),$22,$45::date,NULLIF($23,''),NULLIF($24,''),NULLIF($25,''),
				NULLIF($26,''),NULLIF($27,''),
				NULLIF($28,''),NULLIF($29,''),$30,$31,
				NULLIF($32,''),NULLIF($33,''),NULLIF($34,''),NULLIF($35,''),$36,
				NULLIF($37,''),NULLIF($38,''),NULLIF($39,''),$40,
				NULLIF($41,''),'PendingApproval',1,$42::date,NULLIF($43,'')::date,
				'PENDING_APPROVAL',$44,$44
			) RETURNING policy_id::text`,
			req.Code, req.Name, req.Description, req.Category, req.SubCategory,
			req.ValidationLevel, req.Criticality, req.ActionOnBreach, req.NotificationGroup, req.BreachMessage,
			req.RequiresApproval, req.Applicability, req.CanOverride,
			req.InstrumentFilter, req.CurrencyFilter, req.TenorFilter, req.RatingFilter,
			req.RuleType, req.NullHandling,
			rf.ThrVariable, rf.ThrOperator, rf.ThrValue, rf.ThrValueMode, rf.ThrPercentBase, rf.ThrUnit,
			rf.SlabVariable, rf.SlabUnit,
			rf.CompBase, rf.CompTotalCheckVariable, rf.CompTotalCheckMin, rf.CompTotalCheckMax,
			rf.ListTargetField, rf.ListMode, rf.ListSource, rf.ListDynamicRef, rf.ListCaseSensitive,
			rf.FormulaExpression, rf.FormulaReturnType, rf.FormulaOperator, rf.FormulaValue,
			req.AddlExpression, req.EffectiveStart, req.EffectiveEnd,
			actor, rf.ThrValueDate,
		).Scan(&policyID)
		if err != nil {
			api.LogErrorForResponse(w, "policy create insert: %v", err)
			api.RespondEnvelopeError(w, http.StatusConflict, "failed to create policy (duplicate code or invalid reference?)", "POLICY_CREATE_FAILED")
			return
		}

		if err := insertPolicyChildren(r, tx, policyID, req.TriggerEvents, req.Modules, req.SubModules, req.EntitiesInclude, req.EntitiesExclude, req.RuleType, rf); err != nil {
			api.LogErrorForResponse(w, "policy create children: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadRequest, "failed to attach triggers/modules/rule rows (unknown code?)", "POLICY_CREATE_FAILED")
			return
		}

		if err := insertPolicyNotificationTemplates(r.Context(), tx, policyID, req.NotificationTemplateIDs, actor); err != nil {
			api.LogErrorForResponse(w, "policy create notification templates: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadRequest, "failed to attach notification templates (unknown template_id?)", "POLICY_CREATE_FAILED")
			return
		}

		if err := insertCreateAudit(r, tx, policyID, actor, ip, req, rf); err != nil {
			api.LogErrorForResponse(w, "policy create audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit policy create", "POLICY_CREATE_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "policy create commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create policy", "POLICY_CREATE_FAILED")
			return
		}
		data := map[string]interface{}{
			"policy_id":         policyID,
			"code":              req.Code,
			"conflict_warnings": warnPayload(conflictReport),
		}
		msg := "Policy submitted for approval"
		if summary := formatConflictWarnSummary(conflictReport); summary != "" {
			msg = msg + " — warning: " + summary
		}
		api.RespondEnvelopeSuccess(w, msg, data)
	}
}
