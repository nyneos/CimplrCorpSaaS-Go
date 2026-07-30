package runtime

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LaneSpec identifies the create/check scope used for conflict analysis.
type LaneSpec struct {
	Modules    []string
	SubModules []string
	Triggers   []string
	// ExcludePolicyID skips this policy when loading siblings (edits).
	ExcludePolicyID string
}

// LoadHardBlockThresholdConstraints returns Active+APPROVED HardBlock
// threshold constraints that share at least one module, one sub-module, and
// one trigger (incl. aliases) with the lane.
func LoadHardBlockThresholdConstraints(ctx context.Context, pool *pgxpool.Pool, lane LaneSpec) ([]ThrConstraint, error) {
	modules := trimNonEmpty(lane.Modules)
	subs := trimNonEmpty(lane.SubModules)
	triggers := ExpandTriggerAliasList(trimNonEmpty(lane.Triggers))
	if len(modules) == 0 || len(subs) == 0 || len(triggers) == 0 {
		return nil, nil
	}

	q := `
		SELECT p.code, COALESCE(p.thr_variable, ''), COALESCE(p.thr_operator, ''), COALESCE(p.thr_value, 0),
		       COALESCE(p.thr_value_mode, ''), COALESCE(p.thr_value_date::text, '')
		FROM policyengine_svc.policy_master p
		WHERE p.is_deleted = false
		  -- Match loadActivePolicies: include Scheduled so conflict analysis sees
		  -- the same set enforcement would (date window below is the real gate).
		  AND p.status IN ('Active', 'Scheduled')
		  AND p.processing_status = 'APPROVED'
		  AND p.rule_type = 'threshold'
		  AND p.action_on_breach = 'HardBlock'
		  AND p.effective_start <= CURRENT_DATE
		  AND (p.effective_end IS NULL OR p.effective_end >= CURRENT_DATE)
		  AND EXISTS (
			SELECT 1 FROM policyengine_svc.policy_module m
			WHERE m.policy_id = p.policy_id AND m.is_deleted = false AND m.module_code = ANY($1::text[])
		  )
		  AND EXISTS (
			SELECT 1 FROM policyengine_svc.policy_sub_module sm
			WHERE sm.policy_id = p.policy_id AND sm.is_deleted = false AND sm.sub_module_code = ANY($2::text[])
		  )
		  AND EXISTS (
			SELECT 1 FROM policyengine_svc.policy_trigger t
			WHERE t.policy_id = p.policy_id AND t.is_deleted = false AND t.event_code = ANY($3::text[])
		  )`
	args := []interface{}{modules, subs, triggers}
	if strings.TrimSpace(lane.ExcludePolicyID) != "" {
		q += ` AND p.policy_id <> $4::uuid`
		args = append(args, strings.TrimSpace(lane.ExcludePolicyID))
	}

	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("load hardblock thresholds: %w", err)
	}
	defer rows.Close()

	out := make([]ThrConstraint, 0)
	for rows.Next() {
		var code, variable, op, mode, valueDate string
		var val float64
		if err := rows.Scan(&code, &variable, &op, &val, &mode, &valueDate); err != nil {
			return nil, err
		}
		if strings.TrimSpace(mode) == "PercentOf" || strings.TrimSpace(valueDate) != "" {
			continue
		}
		variable = strings.TrimSpace(variable)
		op = strings.TrimSpace(op)
		if variable == "" || op == "" {
			continue
		}
		out = append(out, ThrConstraint{
			PolicyCode: code,
			Variable:   variable,
			Operator:   op,
			Value:      val,
		})
	}
	return out, rows.Err()
}

// ExpandTriggerAliasList expands each trigger via ExpandTriggerAliases and dedupes.
func ExpandTriggerAliasList(triggers []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, t := range triggers {
		for _, a := range ExpandTriggerAliases(t) {
			a = strings.TrimSpace(a)
			if a == "" {
				continue
			}
			if _, ok := seen[a]; ok {
				continue
			}
			seen[a] = struct{}{}
			out = append(out, a)
		}
	}
	return out
}

func trimNonEmpty(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

// DraftHardBlockConstraint builds a constraint from a draft create/update/test
// threshold when action is HardBlock.
func DraftHardBlockConstraint(code, action, ruleType, variable, operator string, value *float64, valueMode, valueDate string) (ThrConstraint, bool) {
	if !isHardBlockAction(action) {
		return ThrConstraint{}, false
	}
	if !strings.EqualFold(strings.TrimSpace(ruleType), "threshold") {
		return ThrConstraint{}, false
	}
	if strings.TrimSpace(valueMode) == "PercentOf" || strings.TrimSpace(valueDate) != "" {
		return ThrConstraint{}, false
	}
	if value == nil {
		return ThrConstraint{}, false
	}
	variable = strings.TrimSpace(variable)
	operator = strings.TrimSpace(operator)
	if variable == "" || operator == "" {
		return ThrConstraint{}, false
	}
	if strings.TrimSpace(code) == "" {
		code = "DRAFT"
	}
	return ThrConstraint{
		PolicyCode: code,
		Variable:   variable,
		Operator:   operator,
		Value:      *value,
	}, true
}
