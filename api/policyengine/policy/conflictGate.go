package policy

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// evaluateLaneConflicts loads sibling HardBlock thresholds in the same
// module/sub-module/trigger lane, merges an optional draft constraint, and
// returns impossible → error message / knife-edge → warnings.
func evaluateLaneConflicts(
	ctx context.Context,
	pool *pgxpool.Pool,
	modules, subModules, triggers []string,
	excludePolicyID string,
	draft *runtime.ThrConstraint,
) (runtime.ConflictReport, error) {
	existing, err := runtime.LoadHardBlockThresholdConstraints(ctx, pool, runtime.LaneSpec{
		Modules:         modules,
		SubModules:      subModules,
		Triggers:        triggers,
		ExcludePolicyID: excludePolicyID,
	})
	if err != nil {
		return runtime.ConflictReport{}, err
	}
	all := make([]runtime.ThrConstraint, 0, len(existing)+1)
	all = append(all, existing...)
	if draft != nil {
		all = append(all, *draft)
	}
	return runtime.AnalyzeHardBlockThresholdConflicts(all), nil
}

func draftConstraintFromReq(req createReq, rf ruleFields) *runtime.ThrConstraint {
	valueDate := ""
	if rf.ThrValueDate != nil {
		valueDate = *rf.ThrValueDate
	}
	c, ok := runtime.DraftHardBlockConstraint(runtime.DraftThreshold{
		Code:      req.Code,
		Action:    req.ActionOnBreach,
		RuleType:  req.RuleType,
		Variable:  rf.ThrVariable,
		Operator:  rf.ThrOperator,
		Value:     rf.ThrValue,
		ValueMode: rf.ThrValueMode,
		ValueDate: valueDate,
	})
	if !ok {
		return nil
	}
	return &c
}

func conflictErrorMessage(report runtime.ConflictReport) string {
	msg := report.FirstImpossibleMessage()
	if msg == "" {
		return "HardBlock threshold policies in this lane have an empty PASS set"
	}
	return msg
}

func conflictPayload(report runtime.ConflictReport) map[string]interface{} {
	findings := make([]map[string]interface{}, 0, len(report.Findings))
	for _, f := range report.Findings {
		findings = append(findings, map[string]interface{}{
			"severity":     f.Severity,
			"variable":     f.Variable,
			"message":      f.Message,
			"policy_codes": f.PolicyCodes,
		})
	}
	return map[string]interface{}{
		"conflict_findings": findings,
		"conflict_warnings": warnPayload(report),
	}
}

func warnPayload(report runtime.ConflictReport) []map[string]interface{} {
	warns := report.Warnings()
	out := make([]map[string]interface{}, 0, len(warns))
	for _, f := range warns {
		out = append(out, map[string]interface{}{
			"severity":     f.Severity,
			"variable":     f.Variable,
			"message":      f.Message,
			"policy_codes": f.PolicyCodes,
		})
	}
	return out
}

func formatConflictWarnSummary(report runtime.ConflictReport) string {
	warns := report.Warnings()
	if len(warns) == 0 {
		return ""
	}
	parts := make([]string, 0, len(warns))
	for _, w := range warns {
		parts = append(parts, w.Message)
	}
	return strings.Join(parts, "; ")
}

func requireConflictCheckable(modules, subModules, triggers []string) error {
	if len(modules) == 0 || len(subModules) == 0 || len(triggers) == 0 {
		return fmt.Errorf("modules, sub_modules and trigger_events are required for conflict checks")
	}
	return nil
}
