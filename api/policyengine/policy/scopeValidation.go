package policy

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"CimplrCorpSaas/api/policyengine/runtime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// policyScopeInput groups the module/sub-module/trigger scope and rule data
// validated by validatePolicyScope so the function stays within the
// project's max-params limit.
type policyScopeInput struct {
	Modules        []string
	SubModules     []string
	TriggerEvents  []string
	ActionOnBreach string
	RF             ruleFields
	AddlExpression string
}

// validatePolicyScope makes domain_catalog the authority for module/sub-module
// identity. It prevents action-level UI values such as BANK_BALANCE_CREATE from
// being stored where runtime handlers use the canonical BANK_BALANCE code.
func validatePolicyScope(ctx context.Context, pool *pgxpool.Pool, in policyScopeInput) error {
	modules, subModules, triggerEvents := in.Modules, in.SubModules, in.TriggerEvents
	actionOnBreach, rf, addlExpression := in.ActionOnBreach, in.RF, in.AddlExpression
	moduleSet := cleanSet(modules)
	subModuleSet := cleanSet(subModules)
	triggerSet := cleanSet(triggerEvents)

	if len(moduleSet) == 0 || len(subModuleSet) == 0 || len(triggerSet) == 0 {
		return fmt.Errorf("module, sub-module, and trigger are required")
	}

	for moduleCode := range moduleSet {
		var exists bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM domain_catalog.module
			 WHERE module_code = $1 AND is_deleted = false
			)`, moduleCode).Scan(&exists); err != nil {
			return fmt.Errorf("validate module %s: %w", moduleCode, err)
		}
		if !exists {
			return fmt.Errorf("unknown module_code %q; select a module from domain_catalog", moduleCode)
		}
	}

	for subModuleCode := range subModuleSet {
		var parentModule string
		if err := pool.QueryRow(ctx, `
			SELECT module_code
			  FROM domain_catalog.sub_module
			 WHERE sub_module_code = $1 AND is_deleted = false`,
			subModuleCode,
		).Scan(&parentModule); err != nil {
			return fmt.Errorf("unknown sub_module_code %q; use the canonical domain_catalog code (for example BANK_BALANCE, not BANK_BALANCE_CREATE)", subModuleCode)
		}
		if _, ok := moduleSet[parentModule]; !ok {
			return fmt.Errorf("sub_module_code %q belongs to module %q, which is not selected", subModuleCode, parentModule)
		}
	}

	allowColumn, err := breachAllowColumn(actionOnBreach)
	if err != nil {
		return err
	}
	for eventCode := range triggerSet {
		var exists, allowed bool
		query := fmt.Sprintf(`
			SELECT true, %s
			  FROM policyengine_svc.trigger_event
			 WHERE event_code = $1
			   AND is_deleted = false
			   AND processing_status = 'APPROVED'`, allowColumn)
		if err := pool.QueryRow(ctx, query, eventCode).Scan(&exists, &allowed); err != nil || !exists {
			return fmt.Errorf("unknown or inactive trigger event %q", eventCode)
		}
		if !allowed {
			return fmt.Errorf("trigger %q does not allow action_on_breach %q", eventCode, actionOnBreach)
		}
	}

	// Dynamic list refs must exist and be active in the discovery registry
	// (and have a Go-side vetted resolver). Create, update, and import all
	// call validatePolicyScope — so export/import inherits this for free.
	if err := validateDynamicListRef(ctx, pool, rf); err != nil {
		return err
	}

	// Every policy variable must be an active CDM path exposed by a selected
	// catalog sub-module. This couples Policy authoring to the same catalog used
	// by CDM authoring and runtime field mapping.
	for _, cdmPath := range cdmVarsFromRuleFields(rf, addlExpression) {
		var exists bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				  FROM domain_catalog.field f
				  JOIN policyengine_svc.cdm_variable c
				    ON c.name = f.cdm_path AND c.is_deleted = false
				 WHERE f.cdm_path = $1
				   AND f.sub_module_code = ANY($2::text[])
				   AND f.is_deleted = false
			)`, cdmPath, sortedSetKeys(subModuleSet)).Scan(&exists); err != nil {
			return fmt.Errorf("validate CDM path %s: %w", cdmPath, err)
		}
		if !exists {
			return fmt.Errorf("CDM path %q is not active in the selected sub-module(s)", cdmPath)
		}
	}
	return nil
}

// validateDynamicListRef rejects unknown/inactive Dynamic list refs at save
// time so a mistyped dynamicRef never reaches breach-time evaluation.
func validateDynamicListRef(ctx context.Context, pool *pgxpool.Pool, rf ruleFields) error {
	src := strings.TrimSpace(rf.ListSource)
	if src == "" {
		src = "Static"
	}
	if !strings.EqualFold(src, "Dynamic") {
		return nil
	}
	ref := strings.TrimSpace(rf.ListDynamicRef)
	if ref == "" {
		return fmt.Errorf("list: dynamicRef is required when listSource is Dynamic")
	}
	if !runtime.IsKnownDynamicListRef(ref) {
		return fmt.Errorf("list: unknown dynamicRef %q", ref)
	}
	var active bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM policyengine_svc.dynamic_list_registry
			 WHERE ref_code = $1
			   AND is_active = true
			   AND is_deleted = false
		)`, ref).Scan(&active); err != nil {
		return fmt.Errorf("validate dynamicRef %s: %w", ref, err)
	}
	if !active {
		return fmt.Errorf("list: dynamicRef %q is unknown or inactive", ref)
	}
	return nil
}

func cleanSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func sortedSetKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func breachAllowColumn(action string) (string, error) {
	switch strings.TrimSpace(action) {
	case "HardBlock":
		return "allows_hard_block", nil
	case "SoftWarning":
		return "allows_soft_warning", nil
	case "TriggerApproval":
		return "allows_trigger_approval", nil
	case "NotifyOnly":
		return "allows_notify_only", nil
	default:
		return "", fmt.Errorf("unsupported action_on_breach %q", action)
	}
}
