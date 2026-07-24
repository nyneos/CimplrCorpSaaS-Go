package policy

import (
	"encoding/json"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

type testReq struct {
	RuleType       string            `json:"rule_type"`
	Config         json.RawMessage   `json:"config"`
	Variables      map[string]string `json:"variables"`
	ActionOnBreach string            `json:"action_on_breach"`
	NullHandling   string            `json:"null_handling"`
	AddlExpression string            `json:"addl_expression"`
	ModuleCode     string            `json:"module_code"`
	EventCode      string            `json:"event_code"`
	EntityCode     string            `json:"entity_code"`
	// PolicyName echoes the draft's own name back in results[] so the
	// workbench doesn't have to show the raw "TEST_HARNESS" placeholder code.
	PolicyName string `json:"policy_name"`
}

// HandleTest runs the workbench harness: the draft rule plus (when module +
// event are set) every related Active/APPROVED policy for that scope.
// Response always includes results[] so the UI can list all PASS and BREACH.
func HandleTest(pool *pgxpool.Pool) http.HandlerFunc {
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
		policies := []map[string]interface{}{snapshot}

		eventCode := strings.TrimSpace(req.EventCode)
		moduleCode := strings.TrimSpace(req.ModuleCode)
		entityCode := strings.TrimSpace(req.EntityCode)
		if eventCode == "" {
			eventCode = "TEST_HARNESS"
		}

		if moduleCode != "" && eventCode != "TEST_HARNESS" {
			loaded, loadErr := runtime.LoadActivePolicySnapshots(r.Context(), pool, eventCode, moduleCode, entityCode)
			if loadErr != nil {
				api.LogErrorForResponse(w, "policy test load related: %v", loadErr)
			} else {
				policies = append(policies, loaded...)
			}
		}

		checkRes, checkErr := runtime.RunCheck(r.Context(), pool, runtime.CheckRequest{
			EventCode:   eventCode,
			ModuleCode:  moduleCode,
			EntityCode:  entityCode,
			HandlerName: "PolicyWorkbenchTest",
			APIPath:     "/policy-engine/policies/test",
			Variables:   req.Variables,
			Policies:    policies,
		})
		if checkErr != nil {
			api.LogErrorForResponse(w, "policy test: %v", checkErr)
			api.RespondEnvelopeError(w, http.StatusBadGateway, "policy test service failed", "POLICY_SERVICE_ERROR")
			return
		}

		result, detail, message := "PASS", "", ""
		if len(checkRes.Results) > 0 {
			picked := checkRes.Results[0]
			for _, pr := range checkRes.Results {
				if pr.Result == "BREACH" || pr.Result == "ERROR" {
					picked = pr
					break
				}
			}
			result = picked.Result
			message = picked.Message
			detail = picked.Action
			if checkRes.AggregatedAction != "" {
				detail = checkRes.AggregatedAction
			}
		}
		api.RespondEnvelopeSuccess(w, "Policy test completed", map[string]interface{}{
			"result":            result,
			"detail":            detail,
			"message":           message,
			"aggregated_action": checkRes.AggregatedAction,
			"results":           enrichedResultsPayload(checkRes, policies, req.Variables),
			"duration_ms":       checkRes.DurationMS,
		})
	}
}

// enrichedResultsPayload joins each check result with the human policy name
// and the specific CDM variable/value it was tested against — the plain
// policy_id/code from CheckResult.ResultsPayload() doesn't tell the workbench
// UI what was actually evaluated, only pass/fail.
func enrichedResultsPayload(checkRes runtime.CheckResult, policies []map[string]interface{}, variables map[string]string) []map[string]interface{} {
	type meta struct {
		name    string
		testVar string
	}
	metaByID := make(map[string]meta, len(policies))
	for _, p := range policies {
		id, _ := p["policy_id"].(string)
		if id == "" {
			continue
		}
		name, _ := p["name"].(string)
		metaByID[id] = meta{name: name, testVar: testedVariableForSnapshot(p)}
	}

	out := make([]map[string]interface{}, 0, len(checkRes.Results))
	for _, pr := range checkRes.Results {
		m := metaByID[pr.PolicyID]
		row := map[string]interface{}{
			"policy_id": pr.PolicyID,
			"code":      pr.Code,
			"name":      m.name,
			"result":    pr.Result,
			"action":    pr.Action,
			"message":   pr.Message,
		}
		if m.testVar != "" {
			row["tested_variable"] = m.testVar
			row["tested_value"] = variables[m.testVar]
		}
		out = append(out, row)
	}
	return out
}

// testedVariableForSnapshot returns the single CDM variable a policy
// snapshot's rule was evaluated against, when the rule type has exactly one
// (threshold/slabs/list). Composition and formula test multiple/derived
// values, so there's no single variable to surface here.
func testedVariableForSnapshot(snap map[string]interface{}) string {
	switch ruleType, _ := snap["rule_type"].(string); ruleType {
	case "threshold":
		v, _ := snap["thr_variable"].(string)
		return v
	case "slabs":
		v, _ := snap["slab_variable"].(string)
		return v
	case "list":
		v, _ := snap["list_target_field"].(string)
		return v
	default:
		return ""
	}
}

func buildTestSnapshot(req testReq, rf ruleFields) map[string]interface{} {
	snap := map[string]interface{}{
		"policy_id":           "test-harness",
		"code":                "TEST_HARNESS",
		"name":                req.PolicyName,
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
	if rf.ThrValueDate != nil {
		snap["thr_value_date"] = *rf.ThrValueDate
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
