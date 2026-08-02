package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	conflictSkip            = "skip"
	conflictOverwrite       = "overwrite"
	conflictCreateAlongside = "create_alongside"
)

type importReq struct {
	FormatVersion int          `json:"format_version"`
	Policies      []DetailItem `json:"policies"`
	OnConflict    string       `json:"on_conflict"`
	ActorID       string       `json:"actor_id"`
}

type importPolicyResult struct {
	Code       string `json:"code"`
	ResultCode string `json:"result"` // created | skipped | overwritten | failed
	PolicyID   string `json:"policy_id,omitempty"`
	NewCode    string `json:"new_code,omitempty"` // set when create_alongside renamed
	Reason     string `json:"reason,omitempty"`
}

type importResponse struct {
	Results []importPolicyResult `json:"results"`
}

// HandleImport creates policies from a previously exported payload.
// Governance: every imported row lands in PendingApproval / PENDING_APPROVAL
// (never Active) so import cannot bypass maker-checker. The whole batch runs
// in one transaction — any failure rolls everything back.
func HandleImport(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req importReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		if req.FormatVersion != 0 && req.FormatVersion != ExportFormatVersion {
			api.RespondEnvelopeError(w, http.StatusBadRequest,
				fmt.Sprintf("unsupported format_version %d (expected %d)", req.FormatVersion, ExportFormatVersion),
				"VALIDATION_ERROR")
			return
		}
		req.OnConflict = strings.TrimSpace(strings.ToLower(req.OnConflict))
		if req.OnConflict == "" {
			req.OnConflict = conflictSkip
		}
		switch req.OnConflict {
		case conflictSkip, conflictOverwrite, conflictCreateAlongside:
		default:
			api.RespondEnvelopeError(w, http.StatusBadRequest,
				"on_conflict must be skip, overwrite, or create_alongside", "VALIDATION_ERROR")
			return
		}
		if len(req.Policies) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "policies array is required", "VALIDATION_ERROR")
			return
		}

		actor := common.RequestActor(r, req.ActorID)
		ip := common.RequestIP(r)

		// Phase 1 — validate every policy and resolve conflict outcomes without writing.
		planned := make([]plannedImport, 0, len(req.Policies))
		results := make([]importPolicyResult, 0, len(req.Policies))
		anyFailed := false

		for i := range req.Policies {
			p := &req.Policies[i]
			pr, plan, errMsg := planOneImport(r.Context(), pool, p, req.OnConflict)
			if errMsg != "" {
				anyFailed = true
				results = append(results, importPolicyResult{
					Code:       strings.TrimSpace(p.Code),
					ResultCode: "failed",
					Reason:     errMsg,
				})
				continue
			}
			results = append(results, pr)
			if plan != nil {
				planned = append(planned, *plan)
			}
		}
		if anyFailed {
			// Nothing written — return per-policy outcomes so the UI can show why.
			api.RespondEnvelopeFailureWithData(w, http.StatusBadRequest,
				"import validation failed — no policies were written",
				"POLICY_IMPORT_FAILED", importResponse{Results: results})
			return
		}

		// Phase 2 — apply all planned writes in one transaction.
		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "policy import begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to import policies", "POLICY_IMPORT_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		for i := range planned {
			plan := &planned[i]
			policyID, appliedCode, applyErr := applyPlannedImport(r, tx, plan, actor, ip)
			if applyErr != nil {
				api.LogErrorForResponse(w, "policy import apply code=%s: %v", plan.Req.Code, applyErr)
				// Annotate the matching result and abort (partial cannot land).
				for j := range results {
					if results[j].Code == plan.OriginalCode && results[j].ResultCode != "skipped" {
						results[j].ResultCode = "failed"
						results[j].Reason = applyErr.Error()
						results[j].PolicyID = ""
						results[j].NewCode = ""
					}
				}
				api.RespondEnvelopeFailureWithData(w, http.StatusBadRequest,
					"import aborted — no policies were written",
					"POLICY_IMPORT_FAILED", importResponse{Results: results})
				return
			}
			for j := range results {
				if results[j].Code == plan.OriginalCode &&
					(results[j].ResultCode == "created" || results[j].ResultCode == "overwritten") {
					results[j].PolicyID = policyID
					if appliedCode != plan.OriginalCode {
						results[j].NewCode = appliedCode
					}
				}
			}
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "policy import commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to import policies", "POLICY_IMPORT_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Policies imported", importResponse{Results: results})
	}
}

type plannedImport struct {
	OriginalCode string
	ExistingID   string // set for overwrite
	Action       string // created | overwritten
	Req          createReq
	RF           ruleFields
}

func planOneImport(ctx context.Context, pool *pgxpool.Pool, p *DetailItem, onConflict string) (importPolicyResult, *plannedImport, string) {
	code := strings.TrimSpace(p.Code)
	if code == "" {
		return importPolicyResult{}, nil, "code is required"
	}

	req, err := createReqFromDetail(p)
	if err != nil {
		return importPolicyResult{}, nil, err.Error()
	}
	req.trim()
	if msg := req.validate(); msg != "" {
		return importPolicyResult{}, nil, msg
	}
	rf, err := parseRuleConfig(req.RuleType, req.Config)
	if err != nil {
		return importPolicyResult{}, nil, err.Error()
	}
	if err := validateNullDefault(req.RuleType, req.NullHandling, req.NullHandlingDefault, rf); err != nil {
		return importPolicyResult{}, nil, err.Error()
	}
	if err := validatePolicyScope(ctx, pool, policyScopeInput{
		Modules: req.Modules, SubModules: req.SubModules, TriggerEvents: req.TriggerEvents,
		ActionOnBreach: req.ActionOnBreach, RF: rf, AddlExpression: req.AddlExpression,
	}); err != nil {
		return importPolicyResult{}, nil, err.Error()
	}
	if err := validatePELOnWrite(ctx, req.RuleType, rf.FormulaExpression, rf.FormulaReturnType, req.AddlExpression); err != nil {
		return importPolicyResult{}, nil, err.Error()
	}

	existingID, err := lookupPolicyIDByCode(ctx, pool, req.Code)
	if err != nil {
		return importPolicyResult{}, nil, "failed to look up existing policy by code"
	}

	excludeID := ""
	action := "created"
	switch {
	case existingID == "":
		// brand-new code
	case onConflict == conflictSkip:
		return importPolicyResult{Code: code, ResultCode: "skipped", Reason: "code already exists", PolicyID: existingID}, nil, ""
	case onConflict == conflictOverwrite:
		action = "overwritten"
		excludeID = existingID
	case onConflict == conflictCreateAlongside:
		newCode, err := allocateAlongsideCode(ctx, pool, req.Code)
		if err != nil {
			return importPolicyResult{}, nil, err.Error()
		}
		req.Code = newCode
		existingID = ""
	}

	draft := draftConstraintFromReq(req, rf)
	conflictReport, conflictErr := evaluateLaneConflicts(ctx, pool, req.Modules, req.SubModules, req.TriggerEvents, excludeID, draft)
	if conflictErr != nil {
		return importPolicyResult{}, nil, "failed to validate policy conflicts"
	}
	if conflictReport.HasImpossible() {
		return importPolicyResult{}, nil, conflictErrorMessage(conflictReport)
	}

	if req.EffectiveStart == "" {
		req.EffectiveStart = time.Now().UTC().Format("2006-01-02")
	}

	plan := &plannedImport{
		OriginalCode: code,
		ExistingID:   existingID,
		Action:       action,
		Req:          req,
		RF:           rf,
	}
	pr := importPolicyResult{Code: code, ResultCode: action}
	if action == "created" && req.Code != code {
		pr.NewCode = req.Code
	}
	return pr, plan, ""
}

func applyPlannedImport(r *http.Request, tx pgx.Tx, plan *plannedImport, actor, ip string) (policyID, appliedCode string, err error) {
	appliedCode = plan.Req.Code
	if plan.Action == "overwritten" && plan.ExistingID != "" {
		if err := overwriteImportedPolicy(r, tx, plan.ExistingID, plan.Req, plan.RF, actor); err != nil {
			return "", "", err
		}
		policyID = plan.ExistingID
		// Audit as CREATE so the row re-enters maker-checker like a fresh submission.
		// Decision: no separate IMPORT action_type (would need a schema change);
		// insertCreateAudit keeps the checker path identical to UI create.
		if err := insertCreateAudit(r, tx, policyID, actor, ip, plan.Req, plan.RF); err != nil {
			return "", "", fmt.Errorf("audit: %w", err)
		}
		return policyID, appliedCode, nil
	}

	policyID, err = insertImportedPolicy(r, tx, plan.Req, plan.RF, actor)
	if err != nil {
		return "", "", err
	}
	if err := insertPolicyChildren(r, tx, policyID, policyChildrenSpec{
		TriggerEvents: plan.Req.TriggerEvents, Modules: plan.Req.Modules, SubModules: plan.Req.SubModules,
		EntitiesInclude: plan.Req.EntitiesInclude, EntitiesExclude: plan.Req.EntitiesExclude,
		RuleType: plan.Req.RuleType, RF: plan.RF,
	}); err != nil {
		return "", "", fmt.Errorf("children: %w", err)
	}
	if err := insertPolicyNotificationTemplates(r.Context(), tx, policyID, plan.Req.NotificationTemplateIDs, actor); err != nil {
		return "", "", fmt.Errorf("notification templates: %w", err)
	}
	if err := insertCreateAudit(r, tx, policyID, actor, ip, plan.Req, plan.RF); err != nil {
		return "", "", fmt.Errorf("audit: %w", err)
	}
	return policyID, appliedCode, nil
}

func insertImportedPolicy(r *http.Request, tx pgx.Tx, req createReq, rf ruleFields, actor string) (string, error) {
	var policyID string
	err := tx.QueryRow(r.Context(), `
		INSERT INTO policyengine_svc.policy_master (
			code, name, description, category, sub_category, source,
			validation_level, criticality, action_on_breach, notification_group, breach_message,
			requires_approval, applicability, can_override,
			instrument_filter, currency_filter, tenor_filter, rating_filter,
			rule_type, null_handling, null_handling_default,
			thr_variable, thr_operator, thr_value, thr_value_date, thr_value_mode, thr_percent_base, thr_unit,
			slab_variable, slab_unit, slab_percent_base,
			comp_base, comp_total_check_variable, comp_total_check_min, comp_total_check_max,
			list_target_field, list_mode, list_source, list_dynamic_ref, list_case_sensitive,
			formula_expression, formula_return_type, formula_operator, formula_value,
			addl_expression, status, version, effective_start, effective_end,
			processing_status, created_by, last_modified_by,
			approved_by, approved_at
		) VALUES (
			$1,$2,$3,$4,NULLIF($5,''),'Custom',
			$6,$7,$8,NULLIF($9,''),$10,
			$11,$12,$13,
			NULLIF($14,''),NULLIF($15,''),NULLIF($16,''),NULLIF($17,''),
			$18,$19,NULLIF($46,''),
			NULLIF($20,''),NULLIF($21,''),$22,$45::date,NULLIF($23,''),NULLIF($24,''),NULLIF($25,''),
			NULLIF($26,''),NULLIF($27,''),NULLIF($47,''),
			NULLIF($28,''),NULLIF($29,''),$30,$31,
			NULLIF($32,''),NULLIF($33,''),NULLIF($34,''),NULLIF($35,''),$36,
			NULLIF($37,''),NULLIF($38,''),NULLIF($39,''),$40,
			NULLIF($41,''),'PendingApproval',1,$42::date,NULLIF($43,'')::date,
			'PENDING_APPROVAL',$44,$44,
			NULL, NULL
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
		actor, rf.ThrValueDate, req.NullHandlingDefault, rf.SlabPercentBase,
	).Scan(&policyID)
	if err != nil {
		return "", fmt.Errorf("insert: %w", err)
	}
	return policyID, nil
}

func overwriteImportedPolicy(r *http.Request, tx pgx.Tx, policyID string, req createReq, rf ruleFields, actor string) error {
	_, err := tx.Exec(r.Context(), `
		UPDATE policyengine_svc.policy_master SET
			code = $1, name = $2, description = $3, category = $4, sub_category = NULLIF($5,''),
			source = 'Custom',
			validation_level = $6, criticality = $7, action_on_breach = $8, notification_group = NULLIF($9,''),
			breach_message = $10, requires_approval = $11, applicability = $12, can_override = $13,
			instrument_filter = NULLIF($14,''), currency_filter = NULLIF($15,''), tenor_filter = NULLIF($16,''),
			rating_filter = NULLIF($17,''), rule_type = $18, null_handling = $19,
			null_handling_default = NULLIF($47,''),
			thr_variable = NULLIF($20,''), thr_operator = NULLIF($21,''), thr_value = $22, thr_value_date = $46::date,
			thr_value_mode = NULLIF($23,''), thr_percent_base = NULLIF($24,''), thr_unit = NULLIF($25,''),
			slab_variable = NULLIF($26,''), slab_unit = NULLIF($27,''), slab_percent_base = NULLIF($48,''),
			comp_base = NULLIF($28,''), comp_total_check_variable = NULLIF($29,''),
			comp_total_check_min = $30, comp_total_check_max = $31,
			list_target_field = NULLIF($32,''), list_mode = NULLIF($33,''), list_source = NULLIF($34,''),
			list_dynamic_ref = NULLIF($35,''), list_case_sensitive = $36,
			formula_expression = NULLIF($37,''), formula_return_type = NULLIF($38,''),
			formula_operator = NULLIF($39,''), formula_value = $40,
			addl_expression = NULLIF($41,''), effective_start = $42::date, effective_end = NULLIF($43,'')::date,
			status = 'PendingApproval', processing_status = 'PENDING_APPROVAL', version = 1,
			approved_by = NULL, approved_at = NULL, approval_matrix_id = NULL, approval_workflow = NULL,
			is_deleted = false,
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
		actor, policyID, rf.ThrValueDate, req.NullHandlingDefault, rf.SlabPercentBase,
	)
	if err != nil {
		return fmt.Errorf("overwrite: %w", err)
	}
	if err := replacePolicyChildren(r.Context(), tx, r, policyID, policyChildrenSpec{
		TriggerEvents: req.TriggerEvents, Modules: req.Modules, SubModules: req.SubModules,
		EntitiesInclude: req.EntitiesInclude, EntitiesExclude: req.EntitiesExclude,
		RuleType: req.RuleType, RF: rf,
	}); err != nil {
		return fmt.Errorf("children: %w", err)
	}
	if err := replacePolicyNotificationTemplates(r.Context(), tx, policyID, req.NotificationTemplateIDs, actor); err != nil {
		return fmt.Errorf("notification templates: %w", err)
	}
	return nil
}

func lookupPolicyIDByCode(ctx context.Context, pool *pgxpool.Pool, code string) (string, error) {
	var id string
	err := pool.QueryRow(ctx, `
		SELECT policy_id::text FROM policyengine_svc.policy_master
		WHERE code = $1 AND is_deleted = false
		LIMIT 1`, code).Scan(&id)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return id, nil
}

func allocateAlongsideCode(ctx context.Context, pool *pgxpool.Pool, base string) (string, error) {
	for i := 1; i < 1000; i++ {
		suffix := fmt.Sprintf("_IMP%d", i)
		maxBase := 100 - len(suffix)
		if maxBase < 1 {
			maxBase = 1
		}
		trimmed := base
		if len(trimmed) > maxBase {
			trimmed = trimmed[:maxBase]
		}
		candidate := trimmed + suffix
		var exists bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS(SELECT 1 FROM policyengine_svc.policy_master WHERE code = $1)`, candidate,
		).Scan(&exists); err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("could not allocate a unique code alongside %q", base)
}

// createReqFromDetail rebuilds the create-endpoint request (including the nested
// rule `config` JSON) from an exported DetailItem so import reuses create.validate
// + parseRuleConfig unchanged.
func createReqFromDetail(it *DetailItem) (createReq, error) {
	cfg, err := configJSONFromDetail(it)
	if err != nil {
		return createReq{}, err
	}
	return createReq{
		Code:                    it.Code,
		Name:                    it.Name,
		Description:             it.Description,
		Category:                it.Category,
		SubCategory:             it.SubCategory,
		ValidationLevel:         it.ValidationLevel,
		Criticality:             it.Criticality,
		ActionOnBreach:          it.ActionOnBreach,
		NotificationGroup:       it.NotificationGroup,
		NotificationTemplateIDs: it.NotificationTemplateIDs,
		BreachMessage:           it.BreachMessage,
		RequiresApproval:        it.RequiresApproval,
		Applicability:           it.Applicability,
		CanOverride:             it.CanOverride,
		Modules:                 it.Modules,
		SubModules:              it.SubModules,
		TriggerEvents:           it.TriggerEvents,
		EntitiesInclude:         it.EntitiesInclude,
		EntitiesExclude:         it.EntitiesExclude,
		RuleType:                it.RuleType,
		Config:                  cfg,
		AddlExpression:          it.AddlExpression,
		NullHandling:            it.NullHandling,
		NullHandlingDefault:     it.NullHandlingDefault,
		InstrumentFilter:        it.InstrumentFilter,
		CurrencyFilter:          it.CurrencyFilter,
		TenorFilter:             it.TenorFilter,
		RatingFilter:            it.RatingFilter,
		EffectiveStart:          it.EffectiveStart,
		EffectiveEnd:            it.EffectiveEnd,
	}, nil
}

func configJSONFromDetail(it *DetailItem) (json.RawMessage, error) {
	switch it.RuleType {
	case "threshold":
		c := thresholdConfig{
			Variable:    it.ThrVariable,
			Operator:    it.ThrOperator,
			ValueMode:   it.ThrValueMode,
			PercentBase: it.ThrPercentBase,
			Unit:        it.ThrUnit,
		}
		if it.ThrValueDate != nil && strings.TrimSpace(*it.ThrValueDate) != "" {
			c.ValueDate = strings.TrimSpace(*it.ThrValueDate)
		} else if it.ThrValue != nil {
			c.Value = *it.ThrValue
		}
		return json.Marshal(c)
	case "slabs":
		return json.Marshal(slabsConfig{
			Variable:    it.SlabVariable,
			Unit:        it.SlabUnit,
			PercentBase: it.SlabPercentBase,
			Rows:        it.SlabRows,
		})
	case "composition":
		c := compositionConfig{
			Buckets: it.CompBuckets,
			Base:    it.CompBase,
		}
		if it.CompTotalCheckVariable != "" || it.CompTotalCheckMin != nil || it.CompTotalCheckMax != nil {
			c.TotalCheck = &totalCheckConfig{
				Variable: it.CompTotalCheckVariable,
				Min:      it.CompTotalCheckMin,
				Max:      it.CompTotalCheckMax,
			}
		}
		return json.Marshal(c)
	case "list":
		return json.Marshal(listConfig{
			TargetField:   it.ListTargetField,
			Mode:          it.ListMode,
			ListSource:    it.ListSource,
			Values:        it.ListValues,
			DynamicRef:    it.ListDynamicRef,
			CaseSensitive: it.ListCaseSensitive,
		})
	case "formula":
		c := formulaConfig{
			Expression: it.FormulaExpression,
			ReturnType: it.FormulaReturnType,
			Operator:   it.FormulaOperator,
		}
		if it.FormulaValue != nil {
			c.Value = *it.FormulaValue
		}
		return json.Marshal(c)
	default:
		return nil, fmt.Errorf("unsupported rule_type: %s", it.RuleType)
	}
}
