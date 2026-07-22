package policy

import (
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type detailReq struct {
	PolicyID string `json:"policy_id"`
}

// DetailItem is the full policy_master row plus its 1:many children, used by
// the policy edit form and by /approve|/reject decision screens.
type DetailItem struct {
	PolicyID               string   `json:"policy_id"`
	Code                   string   `json:"code"`
	Name                   string   `json:"name"`
	Description            string   `json:"description"`
	Category               string   `json:"category"`
	SubCategory            string   `json:"sub_category"`
	Source                 string   `json:"source"`
	LibraryRef             string   `json:"library_ref"`
	ValidationLevel        string   `json:"validation_level"`
	Criticality            string   `json:"criticality"`
	ActionOnBreach         string   `json:"action_on_breach"`
	ApprovalMatrixID       string   `json:"approval_matrix_id"`
	NotificationGroup      string   `json:"notification_group"`
	BreachMessage          string   `json:"breach_message"`
	RequiresApproval       bool     `json:"requires_approval"`
	ApprovalWorkflow       string   `json:"approval_workflow"`
	Applicability          string   `json:"applicability"`
	CanOverride            bool     `json:"can_override"`
	InstrumentFilter       string   `json:"instrument_filter"`
	CurrencyFilter         string   `json:"currency_filter"`
	TenorFilter            string   `json:"tenor_filter"`
	RatingFilter           string   `json:"rating_filter"`
	RuleType               string   `json:"rule_type"`
	NullHandling           string   `json:"null_handling"`
	NullHandlingDefault    string   `json:"null_handling_default"`
	ThrVariable            string   `json:"thr_variable"`
	ThrOperator            string   `json:"thr_operator"`
	ThrValue               *float64 `json:"thr_value"`
	ThrValueMode           string   `json:"thr_value_mode"`
	ThrPercentBase         string   `json:"thr_percent_base"`
	ThrUnit                string   `json:"thr_unit"`
	SlabVariable           string   `json:"slab_variable"`
	SlabUnit               string   `json:"slab_unit"`
	CompBase               string   `json:"comp_base"`
	CompTotalCheckVariable string   `json:"comp_total_check_variable"`
	CompTotalCheckMin      *float64 `json:"comp_total_check_min"`
	CompTotalCheckMax      *float64 `json:"comp_total_check_max"`
	ListTargetField        string   `json:"list_target_field"`
	ListMode               string   `json:"list_mode"`
	ListSource             string   `json:"list_source"`
	ListDynamicRef         string   `json:"list_dynamic_ref"`
	ListCaseSensitive      bool     `json:"list_case_sensitive"`
	FormulaExpression      string   `json:"formula_expression"`
	FormulaReturnType      string   `json:"formula_return_type"`
	FormulaOperator        string   `json:"formula_operator"`
	FormulaValue           *float64 `json:"formula_value"`
	AddlExpression         string   `json:"addl_expression"`
	Status                 string   `json:"status"`
	ProcessingStatus       string   `json:"processing_status"`
	Version                int      `json:"version"`
	EffectiveStart         string   `json:"effective_start"`
	EffectiveEnd           string   `json:"effective_end"`
	CreatedBy              string   `json:"created_by"`
	CreatedAt              string   `json:"created_at"`
	LastModifiedBy         string   `json:"last_modified_by"`
	LastModifiedAt         string   `json:"last_modified_at"`

	TriggerEvents     []string                  `json:"trigger_events"`
	Modules           []string                  `json:"modules"`
	EntitiesInclude   []string                  `json:"entities_include"`
	EntitiesExclude   []string                  `json:"entities_exclude"`
	SlabRows          []slabRowConfig           `json:"slab_rows"`
	CompBuckets       []compositionBucketConfig `json:"comp_buckets"`
	ListValues        []string                  `json:"list_values"`
}

func HandleDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req detailReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.PolicyID = strings.TrimSpace(req.PolicyID)
		if req.PolicyID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "policy_id is required", "VALIDATION_ERROR")
			return
		}

		it, err := loadPolicyDetail(r, pool, req.PolicyID)
		if err != nil {
			api.LogErrorForResponse(w, "policy detail load policy_id=%s: %v", req.PolicyID, err)
			api.RespondEnvelopeError(w, http.StatusNotFound, "policy not found", "NOT_FOUND")
			return
		}
		api.RespondEnvelopeSuccess(w, "Policy fetched", it)
	}
}

func loadPolicyDetail(r *http.Request, pool *pgxpool.Pool, policyID string) (*DetailItem, error) {
	var it DetailItem
	var effectiveEnd, libraryRef, subCategory, approvalMatrixID, notificationGroup, approvalWorkflow,
		instrumentFilter, currencyFilter, tenorFilter, ratingFilter, nullHandlingDefault,
		thrVariable, thrOperator, thrValueMode, thrPercentBase, thrUnit,
		slabVariable, slabUnit, compBase, compTotalCheckVariable,
		listTargetField, listMode, listSource, listDynamicRef,
		formulaExpression, formulaReturnType, formulaOperator, addlExpression,
		createdBy, lastModifiedBy *string
	var thrValue, compTotalCheckMin, compTotalCheckMax, formulaValue *float64
	var createdAt, lastModifiedAt time.Time

	err := pool.QueryRow(r.Context(), `
		SELECT policy_id::text, code, name, description, category, sub_category, source, library_ref,
		       validation_level, criticality, action_on_breach, approval_matrix_id, notification_group,
		       breach_message, requires_approval, approval_workflow, applicability, can_override,
		       instrument_filter, currency_filter, tenor_filter, rating_filter, rule_type,
		       null_handling, null_handling_default,
		       thr_variable, thr_operator, thr_value, thr_value_mode, thr_percent_base, thr_unit,
		       slab_variable, slab_unit, comp_base, comp_total_check_variable, comp_total_check_min, comp_total_check_max,
		       list_target_field, list_mode, list_source, list_dynamic_ref, list_case_sensitive,
		       formula_expression, formula_return_type, formula_operator, formula_value, addl_expression,
		       status, processing_status, version, effective_start::text, effective_end::text,
		       created_by, created_at, last_modified_by, last_modified_at
		FROM policyengine_svc.policy_master
		WHERE policy_id = $1::uuid AND is_deleted = false`, policyID,
	).Scan(&it.PolicyID, &it.Code, &it.Name, &it.Description, &it.Category, &subCategory, &it.Source, &libraryRef,
		&it.ValidationLevel, &it.Criticality, &it.ActionOnBreach, &approvalMatrixID, &notificationGroup,
		&it.BreachMessage, &it.RequiresApproval, &approvalWorkflow, &it.Applicability, &it.CanOverride,
		&instrumentFilter, &currencyFilter, &tenorFilter, &ratingFilter, &it.RuleType,
		&it.NullHandling, &nullHandlingDefault,
		&thrVariable, &thrOperator, &thrValue, &thrValueMode, &thrPercentBase, &thrUnit,
		&slabVariable, &slabUnit, &compBase, &compTotalCheckVariable, &compTotalCheckMin, &compTotalCheckMax,
		&listTargetField, &listMode, &listSource, &listDynamicRef, &it.ListCaseSensitive,
		&formulaExpression, &formulaReturnType, &formulaOperator, &formulaValue, &addlExpression,
		&it.Status, &it.ProcessingStatus, &it.Version, &it.EffectiveStart, &effectiveEnd,
		&createdBy, &createdAt, &lastModifiedBy, &lastModifiedAt,
	)
	if err != nil {
		return nil, err
	}

	it.SubCategory = strOrEmpty(subCategory)
	it.LibraryRef = strOrEmpty(libraryRef)
	it.ApprovalMatrixID = strOrEmpty(approvalMatrixID)
	it.NotificationGroup = strOrEmpty(notificationGroup)
	it.ApprovalWorkflow = strOrEmpty(approvalWorkflow)
	it.InstrumentFilter = strOrEmpty(instrumentFilter)
	it.CurrencyFilter = strOrEmpty(currencyFilter)
	it.TenorFilter = strOrEmpty(tenorFilter)
	it.RatingFilter = strOrEmpty(ratingFilter)
	it.NullHandlingDefault = strOrEmpty(nullHandlingDefault)
	it.ThrVariable = strOrEmpty(thrVariable)
	it.ThrOperator = strOrEmpty(thrOperator)
	it.ThrValue = thrValue
	it.ThrValueMode = strOrEmpty(thrValueMode)
	it.ThrPercentBase = strOrEmpty(thrPercentBase)
	it.ThrUnit = strOrEmpty(thrUnit)
	it.SlabVariable = strOrEmpty(slabVariable)
	it.SlabUnit = strOrEmpty(slabUnit)
	it.CompBase = strOrEmpty(compBase)
	it.CompTotalCheckVariable = strOrEmpty(compTotalCheckVariable)
	it.CompTotalCheckMin = compTotalCheckMin
	it.CompTotalCheckMax = compTotalCheckMax
	it.ListTargetField = strOrEmpty(listTargetField)
	it.ListMode = strOrEmpty(listMode)
	it.ListSource = strOrEmpty(listSource)
	it.ListDynamicRef = strOrEmpty(listDynamicRef)
	it.FormulaExpression = strOrEmpty(formulaExpression)
	it.FormulaReturnType = strOrEmpty(formulaReturnType)
	it.FormulaOperator = strOrEmpty(formulaOperator)
	it.FormulaValue = formulaValue
	it.AddlExpression = strOrEmpty(addlExpression)
	it.EffectiveEnd = strOrEmpty(effectiveEnd)
	it.CreatedBy = strOrEmpty(createdBy)
	it.LastModifiedBy = strOrEmpty(lastModifiedBy)
	it.CreatedAt = createdAt.UTC().Format(time.RFC3339)
	it.LastModifiedAt = lastModifiedAt.UTC().Format(time.RFC3339)

	it.TriggerEvents = make([]string, 0)
	trigRows, err := pool.Query(r.Context(), `
		SELECT event_code FROM policyengine_svc.policy_trigger
		WHERE policy_id = $1::uuid AND is_deleted = false ORDER BY event_code`, policyID)
	if err == nil {
		for trigRows.Next() {
			var code string
			if trigRows.Scan(&code) == nil {
				it.TriggerEvents = append(it.TriggerEvents, code)
			}
		}
		trigRows.Close()
	}

	it.Modules = make([]string, 0)
	modRows, err := pool.Query(r.Context(), `
		SELECT module_code FROM policyengine_svc.policy_module
		WHERE policy_id = $1::uuid AND is_deleted = false ORDER BY module_code`, policyID)
	if err == nil {
		for modRows.Next() {
			var code string
			if modRows.Scan(&code) == nil {
				it.Modules = append(it.Modules, code)
			}
		}
		modRows.Close()
	}

	it.EntitiesInclude = make([]string, 0)
	it.EntitiesExclude = make([]string, 0)
	entRows, err := pool.Query(r.Context(), `
		SELECT entity_code, filter_mode FROM policyengine_svc.policy_entity
		WHERE policy_id = $1::uuid AND is_deleted = false
		ORDER BY filter_mode, entity_code`, policyID)
	if err == nil {
		for entRows.Next() {
			var code, mode string
			if entRows.Scan(&code, &mode) == nil {
				if mode == "exclude" {
					it.EntitiesExclude = append(it.EntitiesExclude, code)
				} else {
					it.EntitiesInclude = append(it.EntitiesInclude, code)
				}
			}
		}
		entRows.Close()
	}

	it.SlabRows = make([]slabRowConfig, 0)
	if it.RuleType == "slabs" {
		slabRows, err := pool.Query(r.Context(), `
			SELECT from_value, to_value, mode, action, COALESCE(approval_ref, ''), COALESCE(label, '')
			FROM policyengine_svc.policy_slab_row
			WHERE policy_id = $1::uuid AND is_deleted = false ORDER BY seq_order`, policyID)
		if err == nil {
			for slabRows.Next() {
				var row slabRowConfig
				if slabRows.Scan(&row.From, &row.To, &row.Mode, &row.Action, &row.ApprovalRef, &row.Label) == nil {
					it.SlabRows = append(it.SlabRows, row)
				}
			}
			slabRows.Close()
		}
	}

	it.CompBuckets = make([]compositionBucketConfig, 0)
	if it.RuleType == "composition" {
		bucketRows, err := pool.Query(r.Context(), `
			SELECT label, variable, min_value, max_value
			FROM policyengine_svc.policy_composition_bucket
			WHERE policy_id = $1::uuid AND is_deleted = false ORDER BY seq_order`, policyID)
		if err == nil {
			for bucketRows.Next() {
				var b compositionBucketConfig
				if bucketRows.Scan(&b.Label, &b.Variable, &b.Min, &b.Max) == nil {
					it.CompBuckets = append(it.CompBuckets, b)
				}
			}
			bucketRows.Close()
		}
	}

	it.ListValues = make([]string, 0)
	if it.RuleType == "list" {
		listRows, err := pool.Query(r.Context(), `
			SELECT value FROM policyengine_svc.policy_list_value
			WHERE policy_id = $1::uuid AND is_deleted = false ORDER BY value`, policyID)
		if err == nil {
			for listRows.Next() {
				var v string
				if listRows.Scan(&v) == nil {
					it.ListValues = append(it.ListValues, v)
				}
			}
			listRows.Close()
		}
	}

	return &it, nil
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
