package policy

import (
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Dotted CDM paths: investment.fd.principal_amount, cash.bank.balance, …
var cdmPathRE = regexp.MustCompile(`(?i)[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)+`)

type testInputsReq struct {
	ModuleCode string          `json:"module_code"`
	SubModule  string          `json:"sub_module"`
	EventCode  string          `json:"event_code"`
	EntityCode string          `json:"entity_code"`
	RuleType   string          `json:"rule_type"`
	Config     json.RawMessage `json:"config"`
	AddlExpression string      `json:"addl_expression"`
}

// HandleTestInputs returns every CDM path the dry-run will evaluate for the
// given scope: draft rule (if provided) ∪ related Active/APPROVED policies.
// No module-specific hardcoding — inputs follow whatever policies are in that lane.
func HandleTestInputs(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req testInputsReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		moduleCode := strings.TrimSpace(req.ModuleCode)
		subModule := strings.TrimSpace(req.SubModule)
		eventCode := strings.TrimSpace(req.EventCode)
		entityCode := strings.TrimSpace(req.EntityCode)

		draftVars := map[string]struct{}{}
		relatedVars := map[string]struct{}{}
		relatedPolicies := make([]map[string]string, 0)

		if strings.TrimSpace(req.RuleType) != "" && len(req.Config) > 0 && string(req.Config) != "null" {
			rf, err := parseRuleConfig(req.RuleType, req.Config)
			if err == nil {
				for _, v := range cdmVarsFromRuleFields(rf, req.AddlExpression) {
					draftVars[v] = struct{}{}
				}
			}
		} else if strings.TrimSpace(req.AddlExpression) != "" {
			for _, v := range cdmPathsInText(req.AddlExpression) {
				draftVars[v] = struct{}{}
			}
		}

		if moduleCode != "" && eventCode != "" && subModule != "" {
			loaded, err := runtime.LoadActivePolicySnapshots(r.Context(), pool, eventCode, moduleCode, subModule, entityCode, nil)
			if err != nil {
				api.LogErrorForResponse(w, "policy test-inputs load: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load related policies", "POLICY_TEST_INPUTS_FAILED")
				return
			}
			for _, snap := range loaded {
				code, _ := snap["code"].(string)
				name, _ := snap["name"].(string)
				vars := cdmVarsFromSnapshot(snap)
				for _, v := range vars {
					relatedVars[v] = struct{}{}
				}
				relatedPolicies = append(relatedPolicies, map[string]string{
					"code": code,
					"name": name,
				})
			}
		}

		all := map[string]struct{}{}
		for v := range draftVars {
			all[v] = struct{}{}
		}
		for v := range relatedVars {
			all[v] = struct{}{}
		}

		api.RespondEnvelopeSuccess(w, "Dry-run CDM inputs for scope", map[string]interface{}{
			"draft_variables":      sortedKeys(draftVars),
			"related_variables":    sortedKeys(relatedVars),
			"required_variables":   sortedKeys(all),
			"related_policy_count": len(relatedPolicies),
			"related_policies":     relatedPolicies,
			"module_code":          moduleCode,
			"sub_module":           subModule,
			"event_code":           eventCode,
		})
	}
}

func cdmVarsFromRuleFields(rf ruleFields, addl string) []string {
	set := map[string]struct{}{}
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s != "" {
			set[s] = struct{}{}
		}
	}
	add(rf.ThrVariable)
	add(rf.ThrPercentBase)
	add(rf.SlabVariable)
	add(rf.ListTargetField)
	add(rf.CompBase)
	add(rf.CompTotalCheckVariable)
	for _, b := range rf.CompBuckets {
		add(b.Variable)
	}
	for _, v := range cdmPathsInText(rf.FormulaExpression) {
		set[v] = struct{}{}
	}
	for _, v := range cdmPathsInText(addl) {
		set[v] = struct{}{}
	}
	return sortedKeys(set)
}

func cdmVarsFromSnapshot(snap map[string]interface{}) []string {
	set := map[string]struct{}{}
	add := func(key string) {
		if v, ok := snap[key].(string); ok {
			v = strings.TrimSpace(v)
			if v != "" {
				set[v] = struct{}{}
			}
		}
	}
	add("thr_variable")
	add("thr_percent_base")
	add("slab_variable")
	add("slab_percent_base")
	add("list_target_field")
	add("comp_base")
	add("comp_total_check_variable")
	if buckets, ok := snap["comp_buckets"].([]interface{}); ok {
		for _, raw := range buckets {
			b, _ := raw.(map[string]interface{})
			if b == nil {
				continue
			}
			if v, ok := b["variable"].(string); ok && strings.TrimSpace(v) != "" {
				set[strings.TrimSpace(v)] = struct{}{}
			}
		}
	}
	// Snapshots may also store typed []map from loadActivePolicies
	if buckets, ok := snap["comp_buckets"].([]map[string]interface{}); ok {
		for _, b := range buckets {
			if v, ok := b["variable"].(string); ok && strings.TrimSpace(v) != "" {
				set[strings.TrimSpace(v)] = struct{}{}
			}
		}
	}
	if expr, ok := snap["formula_expression"].(string); ok {
		for _, v := range cdmPathsInText(expr) {
			set[v] = struct{}{}
		}
	}
	if addl, ok := snap["addl_expression"].(string); ok {
		for _, v := range cdmPathsInText(addl) {
			set[v] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func cdmPathsInText(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	matches := cdmPathRE.FindAllString(s, -1)
	if len(matches) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	for _, m := range matches {
		set[m] = struct{}{}
	}
	return sortedKeys(set)
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
