package runtime

import (
	"fmt"
	"strings"
)

type policyScopeSkip struct {
	PolicyID   string
	PolicyCode string
	PolicyName string
	Stage      string
	Code       string
	Reason     string
	Sequence   int
}

type policyLoadTrace struct {
	Candidates []map[string]interface{}
	Applicable []map[string]interface{}
	Skips      []policyScopeSkip
}

func (t policyLoadTrace) candidateCount() int {
	return len(t.Candidates)
}

func (t policyLoadTrace) applicableCount() int {
	return len(t.Applicable)
}

func appendScopeSkip(skips []policyScopeSkip, snap map[string]interface{}, stage, code, reason string) []policyScopeSkip {
	return append(skips, policyScopeSkip{
		PolicyID:   snapshotString(snap, "policy_id"),
		PolicyCode: snapshotString(snap, "code"),
		PolicyName: snapshotString(snap, "name"),
		Stage:      stage,
		Code:       code,
		Reason:     reason,
		Sequence:   len(skips) + 1,
	})
}

func snapshotString(snap map[string]interface{}, key string) string {
	v, _ := snap[key].(string)
	return strings.TrimSpace(v)
}

func dataFilterSkip(instrument, currency, tenor, rating string, vars map[string]string) (string, string, bool) {
	if expected := strings.TrimSpace(currency); expected != "" {
		actuals := currencyFromVars(vars)
		if len(actuals) == 0 {
			return "CURRENCY_FILTER_UNRESOLVED", fmt.Sprintf("currency scope requires %q; request carries no resolvable currency", expected), true
		}
		if !containsFold(actuals, expected) {
			return "CURRENCY_FILTER_MISMATCH", fmt.Sprintf("currency scope requires %q; request resolved %q", expected, strings.Join(actuals, "/")), true
		}
	}
	checks := []struct {
		filter string
		actual string
		code   string
		field  string
	}{
		{instrument, lookupCDMVar(vars, ".instrument_type", ".instrument"), "INSTRUMENT_FILTER_MISMATCH", "instrument"},
		{rating, lookupCDMVar(vars, ".credit_rating", ".rating"), "RATING_FILTER_MISMATCH", "rating"},
		{tenor, tenorBandFromVars(vars), "TENOR_FILTER_MISMATCH", "tenor"},
	}
	for _, check := range checks {
		expected := strings.TrimSpace(check.filter)
		if expected != "" && check.actual != "" && !strings.EqualFold(expected, check.actual) {
			return check.code, fmt.Sprintf("%s scope requires %q; request resolved %q", check.field, expected, check.actual), true
		}
	}
	return "", "", false
}

func entityScopeSkip(applicability, entityCode string, include, exclude []string) (string, string) {
	entityCode = strings.TrimSpace(entityCode)
	for _, excluded := range exclude {
		if strings.EqualFold(strings.TrimSpace(excluded), entityCode) && !containsFold(include, entityCode) {
			return "ENTITY_EXCLUDED", fmt.Sprintf("entity %q is explicitly excluded", entityCode)
		}
	}
	if (applicability == "Entity" || (applicability == "Custom" && len(include) > 0)) && entityCode == "" {
		return "ENTITY_REQUIRED", "policy requires an entity but the request supplied none"
	}
	return "ENTITY_NOT_INCLUDED", fmt.Sprintf("entity %q is not in the policy include scope", entityCode)
}

func scopeSpecificity(applicability string) int {
	switch strings.TrimSpace(applicability) {
	case "Entity":
		return 3
	case "Custom":
		return 2
	default:
		return 1
	}
}

func resolveScopeOverrides(applicable []map[string]interface{}, skips []policyScopeSkip) ([]map[string]interface{}, []policyScopeSkip) {
	if len(applicable) < 2 {
		return applicable, skips
	}
	best := make(map[string]int, len(applicable))
	for _, snap := range applicable {
		key := strings.ToLower(snapshotString(snap, "code"))
		if key == "" {
			continue
		}
		if rank := scopeSpecificity(snapshotString(snap, "applicability")); rank > best[key] {
			best[key] = rank
		}
	}
	kept := make([]map[string]interface{}, 0, len(applicable))
	for _, snap := range applicable {
		key := strings.ToLower(snapshotString(snap, "code"))
		applicability := snapshotString(snap, "applicability")
		if key == "" || scopeSpecificity(applicability) >= best[key] {
			kept = append(kept, snap)
			continue
		}
		skips = appendScopeSkip(skips, snap, "SCOPE_OVERRIDE", "OVERRIDDEN_BY_MORE_SPECIFIC_SCOPE",
			fmt.Sprintf("%q scope instance is overridden by a more specific instance of policy code %q", applicability, snapshotString(snap, "code")))
	}
	return kept, skips
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), strings.TrimSpace(target)) {
			return true
		}
	}
	return false
}
