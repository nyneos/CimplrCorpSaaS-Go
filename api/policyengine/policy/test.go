package policy

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/services/policysvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

type testReq struct {
	RuleType       string            `json:"rule_type"`
	Config         json.RawMessage   `json:"config"`
	Variables      map[string]string `json:"variables"`
	ActionOnBreach string            `json:"action_on_breach"`
	NullHandling   string            `json:"null_handling"`
	AddlExpression string            `json:"addl_expression"`
}

// HandleTest runs the policy-builder workbench harness: one rule config against
// a variable vector, no persistence and no execution_log rows (proxied to the
// standalone CIMPLR-Policy-Service /v1/test, same evaluator as /check).
func HandleTest(pool *pgxpool.Pool) http.HandlerFunc {
	client := policysvc.NewFromEnv()
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req testReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.RuleType = strings.TrimSpace(req.RuleType)
		if req.RuleType == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "rule_type is required", "VALIDATION_ERROR")
			return
		}
		rf, err := parseRuleConfig(req.RuleType, req.Config)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, err.Error(), "VALIDATION_ERROR")
			return
		}
		if req.Variables == nil {
			req.Variables = map[string]string{}
		}
		if req.ActionOnBreach == "" {
			req.ActionOnBreach = "NotifyOnly"
		}
		if req.NullHandling == "" {
			req.NullHandling = "FailSafe"
		}

		snapshot := buildTestSnapshot(req, rf)
		resp, err := client.Test(r.Context(), policysvc.EvaluateRequest{
			EventCode: "TEST_HARNESS",
			Variables: req.Variables,
			Policies:  []map[string]interface{}{snapshot},
		})
		if err != nil {
			api.LogErrorForResponse(w, "policy test: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadGateway, "policy test service failed", "POLICY_SERVICE_ERROR")
			return
		}
		result, detail, message := "PASS", "", ""
		if len(resp.Results) > 0 {
			result = resp.Results[0].Result
			message = resp.Results[0].Message
			detail = resp.Results[0].Action
		}
		api.RespondEnvelopeSuccess(w, "Policy test completed", map[string]interface{}{
			"result":  result,
			"detail":  detail,
			"message": message,
		})
	}
}

func buildTestSnapshot(req testReq, rf ruleFields) map[string]interface{} {
	snap := map[string]interface{}{
		"policy_id":           "test-harness",
		"code":                "TEST_HARNESS",
		"rule_type":           req.RuleType,
		"action_on_breach":    req.ActionOnBreach,
		"null_handling":       req.NullHandling,
		"addl_expression":     req.AddlExpression,
		"thr_variable":        rf.ThrVariable,
		"thr_operator":        rf.ThrOperator,
		"thr_value_mode":      rf.ThrValueMode,
		"thr_percent_base":    rf.ThrPercentBase,
		"slab_variable":       rf.SlabVariable,
		"list_target_field":   rf.ListTargetField,
		"list_mode":           rf.ListMode,
		"list_case_sensitive": rf.ListCaseSensitive,
		"list_values":         rf.ListValues,
		"formula_expression":  rf.FormulaExpression,
		"formula_return_type": rf.FormulaReturnType,
		"formula_operator":    rf.FormulaOperator,
	}
	if rf.ThrValue != nil {
		snap["thr_value"] = *rf.ThrValue
	}
	if rf.FormulaValue != nil {
		snap["formula_value"] = *rf.FormulaValue
	}
	if len(rf.SlabRows) > 0 {
		snap["slab_rows"] = rf.SlabRows
	}
	if len(rf.CompBuckets) > 0 {
		snap["comp_buckets"] = rf.CompBuckets
	}
	return snap
}
