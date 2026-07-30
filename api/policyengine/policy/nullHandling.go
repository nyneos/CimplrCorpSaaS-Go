package policy

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// validateNullDefault rejects a UseDefault substitute value the evaluator could
// not parse for this rule type. Without it the policy authors fine and only
// fails at evaluation time, where a bad default resolves to ERROR → HardBlock
// on live business actions.
func validateNullDefault(ruleType, nullHandling, def string, rf ruleFields) error {
	if nullHandling != "UseDefault" {
		return nil
	}
	def = strings.TrimSpace(def)
	if def == "" {
		return fmt.Errorf("null_handling_default is required when null_handling is UseDefault")
	}
	switch ruleType {
	case "threshold":
		if rf.ThrValueDate != nil {
			return requireDateDefault(def)
		}
		return requireNumericDefault(def)
	case "slabs", "composition":
		return requireNumericDefault(def)
	default:
		// list compares strings; formula defaults are coerced by the PEL env.
		return nil
	}
}

func requireNumericDefault(def string) error {
	if _, err := strconv.ParseFloat(def, 64); err != nil {
		return fmt.Errorf("null_handling_default %q must be numeric for this rule type", def)
	}
	return nil
}

func requireDateDefault(def string) error {
	if _, err := time.Parse("2006-01-02", def); err == nil {
		return nil
	}
	if _, err := time.Parse(time.RFC3339, def); err == nil {
		return nil
	}
	return fmt.Errorf("null_handling_default %q must be a yyyy-mm-dd date for this rule type", def)
}
