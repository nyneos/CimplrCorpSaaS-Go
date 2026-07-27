// Package runtime exposes policy check for other modules (e.g. FD booking create).
package runtime

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/services/policysvc"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckRequest is the in-process equivalent of POST /policy-engine/policies/check.
type CheckRequest struct {
	EventCode          string
	ModuleCode         string
	SubModule          string
	FormID             string
	EntityCode         string
	ActorUserID        string
	HandlerName        string
	APIPath            string
	CorrelationID      string
	TraceID            string
	BusinessRecordType string
	BusinessRecordID   string
	SourceFileName     string
	SourceFileID       string
	BatchID            string
	Variables          map[string]string
	Policies           []map[string]interface{}
}

// CheckResult holds evaluation output for module handlers.
type CheckResult struct {
	AggregatedAction string
	Results          []policysvc.PolicyResult
	DurationMS       int
	ConflictReport   ConflictReport
}

// BlocksSubmit is true when the aggregated breach action is HardBlock.
func (r CheckResult) BlocksSubmit() bool {
	return r.AggregatedAction == "HardBlock"
}

// CountPassedFailed returns how many policies passed vs BREACH/ERROR.
func (r CheckResult) CountPassedFailed() (passed, failed int) {
	for _, pr := range r.Results {
		if pr.Result == "BREACH" || pr.Result == "ERROR" {
			failed++
		} else {
			passed++
		}
	}
	return passed, failed
}

// SummaryLine is a short toast-friendly line with policy names when available.
func (r CheckResult) SummaryLine() string {
	passed, failed := r.CountPassedFailed()
	if passed+failed == 0 {
		return ""
	}
	passNames, failNames := r.policyNameLists()
	if failed == 0 {
		if len(passNames) > 0 {
			return fmt.Sprintf("Policy check passed: %s", strings.Join(passNames, ", "))
		}
		return fmt.Sprintf("Policy check: %d passed", passed)
	}
	parts := []string{fmt.Sprintf("Policy check failed (%d of %d)", failed, passed+failed)}
	if len(failNames) > 0 {
		parts = append(parts, strings.Join(failNames, ", "))
	}
	return strings.Join(parts, " — ")
}

func (r CheckResult) policyNameLists() (passed, failed []string) {
	for _, pr := range r.Results {
		label := strings.TrimSpace(pr.Name)
		if label == "" {
			label = strings.TrimSpace(pr.Code)
		}
		if label == "" {
			label = "policy"
		}
		if pr.Result == "BREACH" || pr.Result == "ERROR" {
			failed = append(failed, label)
		} else {
			passed = append(passed, label)
		}
	}
	return passed, failed
}

// ClientMessage pairs SummaryLine with the first breach detail for HTTP message/toasts.
func (r CheckResult) ClientMessage(fallback string) string {
	sum := r.SummaryLine()
	detail := r.FirstBreachMessage()
	if detail == "" {
		detail = strings.TrimSpace(fallback)
	}
	// Prefer human policy label over raw evaluator internals when both exist.
	switch {
	case sum != "" && detail != "" && !strings.Contains(sum, detail):
		return sum + " — " + detail
	case sum != "":
		return sum
	case detail != "":
		return "Policy check failed — " + detail
	default:
		return "Policy check failed — blocked by policy"
	}
}

// FirstBreachMessage returns the first BREACH/ERROR message, if any.
func (r CheckResult) FirstBreachMessage() string {
	for _, pr := range r.Results {
		if pr.Result == "BREACH" || pr.Result == "ERROR" {
			label := strings.TrimSpace(pr.Name)
			if label == "" {
				label = strings.TrimSpace(pr.Code)
			}
			msg := strings.TrimSpace(pr.Message)
			if label != "" && msg != "" {
				return label + ": " + msg
			}
			if msg != "" {
				return msg
			}
			if label != "" {
				return label
			}
		}
	}
	return ""
}

// ResultsPayload is the JSON-friendly shape for UI / API error data.
func (r CheckResult) ResultsPayload() []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(r.Results))
	for _, pr := range r.Results {
		out = append(out, map[string]interface{}{
			"policy_id": pr.PolicyID,
			"code":      pr.Code,
			"name":      pr.Name,
			"result":    pr.Result,
			"action":    pr.Action,
			"message":   pr.Message,
		})
	}
	return out
}

// BlockPayload is attached to HardBlock HTTP responses so clients can list
// every related policy that passed or failed.
func (r CheckResult) BlockPayload() map[string]interface{} {
	passed, failed := r.CountPassedFailed()
	return map[string]interface{}{
		"aggregated_action": r.AggregatedAction,
		"policy_results":    r.ResultsPayload(),
		"duration_ms":       r.DurationMS,
		"passed_count":      passed,
		"failed_count":      failed,
		"summary":           r.SummaryLine(),
	}
}

// WriteSummaryHeader sets X-Policy-Summary when at least one policy ran (pass path).
func (r CheckResult) WriteSummaryHeader(w http.ResponseWriter) {
	if w == nil {
		return
	}
	if line := r.SummaryLine(); line != "" {
		w.Header().Set("X-Policy-Summary", line)
	}
}

// LoadActivePolicySnapshots returns Active+APPROVED policy snapshots for the
// given trigger/module/sub-module/entity (same set RunCheck loads).
func LoadActivePolicySnapshots(ctx context.Context, pool *pgxpool.Pool, eventCode, moduleCode, subModule, entityCode string) ([]map[string]interface{}, error) {
	return loadActivePolicies(ctx, pool, eventCode, moduleCode, subModule, entityCode)
}

// RunCheck loads applicable policies, evaluates via CIMPLR-Policy-Service, writes execution_log.
func RunCheck(ctx context.Context, pool *pgxpool.Pool, req CheckRequest) (CheckResult, error) {
	if !PolicyChecksEnabled() {
		return CheckResult{AggregatedAction: ""}, nil
	}
	client := policysvc.NewFromEnv()
	if req.Variables == nil {
		req.Variables = map[string]string{}
	}

	policies := req.Policies
	if len(policies) == 0 {
		loaded, err := loadActivePolicies(ctx, pool, req.EventCode, req.ModuleCode, req.SubModule, req.EntityCode)
		if err != nil {
			return CheckResult{}, err
		}
		policies = loaded
	}

	conflictReport := AnalyzeHardBlockThresholdConflicts(ConstraintsFromPolicySnapshots(policies))
	if conflictReport.HasImpossible() {
		msg := conflictReport.FirstImpossibleMessage()
		synthetic := policysvc.PolicyResult{
			PolicyID: "lane-conflict",
			Code:     "POLICY_CONFLICT_IMPOSSIBLE",
			Name:     "Lane HardBlock conflict",
			Result:   "ERROR",
			Action:   "HardBlock",
			Message:  msg,
		}
		duration := 0
		_, _ = pool.Exec(ctx, `
			INSERT INTO policyengine_svc.execution_log (
				correlation_id, trace_id, event_code, module_code, sub_module, form_id,
				handler_name, api_path, actor_user_id, entity_code,
				business_record_type, business_record_id, source_file_name, source_file_id, batch_id,
				policy_code, result, action_fired, detail_message, fail_code, fail_reason, duration_ms
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
				$16,'ERROR','HardBlock',$17,'POLICY_CONFLICT_IMPOSSIBLE',$17,$18)`,
			common.NullIfEmpty(req.CorrelationID), common.NullIfEmpty(req.TraceID), req.EventCode, common.NullIfEmpty(req.ModuleCode),
			common.NullIfEmpty(req.SubModule), common.NullIfEmpty(req.FormID), common.NullIfEmpty(req.HandlerName), common.NullIfEmpty(req.APIPath),
			common.NullIfEmpty(req.ActorUserID), common.NullIfEmpty(req.EntityCode),
			common.NullIfEmpty(req.BusinessRecordType), common.NullIfEmpty(req.BusinessRecordID),
			common.NullIfEmpty(req.SourceFileName), common.NullIfEmpty(req.SourceFileID), common.NullIfEmpty(req.BatchID),
			"POLICY_CONFLICT_IMPOSSIBLE", msg, duration,
		)
		return CheckResult{
			AggregatedAction: "HardBlock",
			Results:          []policysvc.PolicyResult{synthetic},
			DurationMS:       duration,
			ConflictReport:   conflictReport,
		}, nil
	}

	started := time.Now()
	evalReq := policysvc.EvaluateRequest{
		EventCode:  req.EventCode,
		ModuleCode: req.ModuleCode,
		FormID:     req.FormID,
		EntityCode: req.EntityCode,
		ActorUser:  req.ActorUserID,
		Variables:  req.Variables,
		Policies:   policies,
	}
	resp, err := client.Evaluate(ctx, evalReq)
	duration := int(time.Since(started).Milliseconds())
	if err != nil {
		_, _ = pool.Exec(ctx, `
			INSERT INTO policyengine_svc.execution_log (
				correlation_id, trace_id, event_code, module_code, sub_module, form_id,
				handler_name, api_path, actor_user_id, entity_code,
				business_record_type, business_record_id, source_file_name, source_file_id, batch_id,
				result, fail_code, fail_reason, detail_message, duration_ms
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'ERROR','POLICY_SERVICE_ERROR',$16,$16,$17)`,
			common.NullIfEmpty(req.CorrelationID), common.NullIfEmpty(req.TraceID), req.EventCode, common.NullIfEmpty(req.ModuleCode),
			common.NullIfEmpty(req.SubModule), common.NullIfEmpty(req.FormID), common.NullIfEmpty(req.HandlerName), common.NullIfEmpty(req.APIPath),
			common.NullIfEmpty(req.ActorUserID), common.NullIfEmpty(req.EntityCode),
			common.NullIfEmpty(req.BusinessRecordType), common.NullIfEmpty(req.BusinessRecordID),
			common.NullIfEmpty(req.SourceFileName), common.NullIfEmpty(req.SourceFileID), common.NullIfEmpty(req.BatchID),
			err.Error(), duration,
		)
		return CheckResult{}, err
	}

	for _, pr := range resp.Results {
		failCode, failReason := "", ""
		if pr.Result == "BREACH" || pr.Result == "ERROR" {
			failCode = pr.Result
			failReason = pr.Message
		}
		_, _ = pool.Exec(ctx, `
			INSERT INTO policyengine_svc.execution_log (
				correlation_id, trace_id, event_code, module_code, sub_module, form_id,
				handler_name, api_path, actor_user_id, entity_code,
				business_record_type, business_record_id, source_file_name, source_file_id, batch_id,
				policy_id, policy_code, result, action_fired, detail_message, fail_code, fail_reason, duration_ms
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
				NULLIF($16,'')::uuid, $17, $18, $19, $20, $21, $22, $23)`,
			common.NullIfEmpty(req.CorrelationID), common.NullIfEmpty(req.TraceID), req.EventCode, common.NullIfEmpty(req.ModuleCode),
			common.NullIfEmpty(req.SubModule), common.NullIfEmpty(req.FormID), common.NullIfEmpty(req.HandlerName), common.NullIfEmpty(req.APIPath),
			common.NullIfEmpty(req.ActorUserID), common.NullIfEmpty(req.EntityCode),
			common.NullIfEmpty(req.BusinessRecordType), common.NullIfEmpty(req.BusinessRecordID),
			common.NullIfEmpty(req.SourceFileName), common.NullIfEmpty(req.SourceFileID), common.NullIfEmpty(req.BatchID),
			pr.PolicyID, pr.Code, pr.Result, common.NullIfEmpty(pr.Action), common.NullIfEmpty(pr.Message),
			common.NullIfEmpty(failCode), common.NullIfEmpty(failReason), duration,
		)
	}

	dispatchNotifyBreaches(ctx, pool, req, policies, resp.Results)

	return CheckResult{
		AggregatedAction: resp.AggregatedAction,
		Results:          resp.Results,
		DurationMS:       duration,
		ConflictReport:   conflictReport,
	}, nil
}

func loadActivePolicies(ctx context.Context, pool *pgxpool.Pool, eventCode, moduleCode, subModule, entityCode string) ([]map[string]interface{}, error) {
	codes := ExpandTriggerAliases(eventCode)
	q := `
		SELECT p.policy_id::text, p.code, p.name, p.rule_type, p.action_on_breach, p.null_handling,
		       COALESCE(p.null_handling_default, ''), COALESCE(p.addl_expression, ''),
		       COALESCE(p.thr_variable, ''), COALESCE(p.thr_operator, ''), COALESCE(p.thr_value, 0),
		       COALESCE(p.thr_value_date::text, ''),
		       COALESCE(p.thr_value_mode, ''), COALESCE(p.thr_percent_base, ''),
		       COALESCE(p.list_target_field, ''), COALESCE(p.list_mode, ''), COALESCE(p.list_source, ''),
		       COALESCE(p.list_dynamic_ref, ''), COALESCE(p.list_case_sensitive, false),
		       COALESCE(p.notification_group, ''),
		       COALESCE(p.formula_expression, ''), COALESCE(p.formula_return_type, ''),
		       COALESCE(p.formula_operator, ''), COALESCE(p.formula_value, 0),
		       COALESCE(p.slab_variable, ''),
		       COALESCE(p.comp_base, ''), COALESCE(p.comp_total_check_variable, ''),
		       p.comp_total_check_min, p.comp_total_check_max,
		       COALESCE(p.applicability, 'Global')
		FROM policyengine_svc.policy_master p
		INNER JOIN policyengine_svc.policy_trigger t
			ON t.policy_id = p.policy_id AND t.is_deleted = false AND t.event_code = ANY($1::text[])
		WHERE p.is_deleted = false AND p.status = 'Active' AND p.processing_status = 'APPROVED'
		  AND p.effective_start <= CURRENT_DATE
		  AND (p.effective_end IS NULL OR p.effective_end >= CURRENT_DATE)`
	args := []interface{}{codes}
	argN := 2
	if strings.TrimSpace(moduleCode) != "" {
		q += fmt.Sprintf(`
		AND (
			NOT EXISTS (SELECT 1 FROM policyengine_svc.policy_module m WHERE m.policy_id = p.policy_id AND m.is_deleted = false)
			OR EXISTS (
				SELECT 1 FROM policyengine_svc.policy_module m
				WHERE m.policy_id = p.policy_id AND m.is_deleted = false AND m.module_code = $%d
			)
		)`, argN)
		args = append(args, moduleCode)
		argN++
	}
	// Sub-module filter (same shape as module): no rows → match any; else must include handler SubModule.
	if strings.TrimSpace(subModule) != "" {
		q += fmt.Sprintf(`
		AND (
			NOT EXISTS (SELECT 1 FROM policyengine_svc.policy_sub_module sm WHERE sm.policy_id = p.policy_id AND sm.is_deleted = false)
			OR EXISTS (
				SELECT 1 FROM policyengine_svc.policy_sub_module sm
				WHERE sm.policy_id = p.policy_id AND sm.is_deleted = false AND sm.sub_module_code = $%d
			)
		)`, argN)
		args = append(args, strings.TrimSpace(subModule))
		argN++
	}

	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]interface{}, 0)
	policyIDs := make([]string, 0)
	for rows.Next() {
		var (
			id, code, name, ruleType, action, nullH, nullDef, addl string
			thrVar, thrOp, thrValueDate, thrMode, thrBase          string
			listField, listMode, listSource, listDynRef            string
			notifGroup, formulaExpr, formulaRet, formulaOp         string
			slabVar, compBase, compTotalVar, applicability         string
			thrVal, formulaVal                                     float64
			compTotalMin, compTotalMax                             *float64
			listCase                                               bool
		)
		if err := rows.Scan(&id, &code, &name, &ruleType, &action, &nullH, &nullDef, &addl,
			&thrVar, &thrOp, &thrVal, &thrValueDate, &thrMode, &thrBase, &listField, &listMode, &listSource, &listDynRef, &listCase,
			&notifGroup, &formulaExpr, &formulaRet, &formulaOp, &formulaVal, &slabVar,
			&compBase, &compTotalVar, &compTotalMin, &compTotalMax, &applicability); err != nil {
			return nil, err
		}
		snap := map[string]interface{}{
			"policy_id":             id,
			"code":                  code,
			"name":                  name,
			"rule_type":             ruleType,
			"action_on_breach":      action,
			"null_handling":         nullH,
			"null_handling_default": nullDef,
			"addl_expression":       addl,
			"thr_variable":          thrVar,
			"thr_operator":          thrOp,
			"thr_value":             thrVal,
			"thr_value_date":        thrValueDate,
			"thr_value_mode":        thrMode,
			"thr_percent_base":      thrBase,
			"list_target_field":     listField,
			"list_mode":             listMode,
			"list_source":           listSource,
			"list_dynamic_ref":      listDynRef,
			"list_case_sensitive":   listCase,
			"notification_group":    notifGroup,
			"formula_expression":    formulaExpr,
			"formula_return_type":   formulaRet,
			"formula_operator":      formulaOp,
			"formula_value":         formulaVal,
			"slab_variable":         slabVar,
			"comp_base":             compBase,
			"comp_total_check_variable": compTotalVar,
			"applicability":         applicability,
		}
		if compTotalMin != nil {
			snap["comp_total_check_min"] = *compTotalMin
		}
		if compTotalMax != nil {
			snap["comp_total_check_max"] = *compTotalMax
		}
		if ruleType == "list" {
			vals, _ := loadListValues(ctx, pool, id)
			snap["list_values"] = vals
		}
		if ruleType == "slabs" {
			slabRows, _ := loadSlabRows(ctx, pool, id)
			snap["slab_rows"] = slabRows
		}
		if ruleType == "composition" {
			buckets, _ := loadCompositionBuckets(ctx, pool, id)
			snap["comp_buckets"] = buckets
		}
		out = append(out, snap)
		policyIDs = append(policyIDs, id)
	}

	entityMap, err := loadPolicyEntityScopes(ctx, pool, policyIDs)
	if err != nil {
		return nil, err
	}
	entityCode = strings.TrimSpace(entityCode)
	filtered := make([]map[string]interface{}, 0, len(out))
	for _, snap := range out {
		id, _ := snap["policy_id"].(string)
		applicability, _ := snap["applicability"].(string)
		scope := entityMap[id]
		if policyAppliesToEntity(applicability, entityCode, scope.include, scope.exclude) {
			filtered = append(filtered, snap)
		}
	}
	return filtered, nil
}

type entityScopeLists struct {
	include []string
	exclude []string
}

func loadPolicyEntityScopes(ctx context.Context, pool *pgxpool.Pool, policyIDs []string) (map[string]entityScopeLists, error) {
	out := make(map[string]entityScopeLists, len(policyIDs))
	if len(policyIDs) == 0 {
		return out, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT policy_id::text, entity_code, filter_mode
		FROM policyengine_svc.policy_entity
		WHERE policy_id = ANY($1::uuid[]) AND is_deleted = false`, policyIDs)
	if err != nil {
		// Table may not exist yet on older DBs — treat as no entity filters.
		if strings.Contains(err.Error(), "policy_entity") {
			return out, nil
		}
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, code, mode string
		if err := rows.Scan(&id, &code, &mode); err != nil {
			return nil, err
		}
		s := out[id]
		if mode == "exclude" {
			s.exclude = append(s.exclude, code)
		} else {
			s.include = append(s.include, code)
		}
		out[id] = s
	}
	return out, nil
}

// policyAppliesToEntity enforces mutual exclusion semantics:
// exclude always removes; when include is non-empty the entity must be listed.
func policyAppliesToEntity(applicability, entityCode string, include, exclude []string) bool {
	applicability = strings.TrimSpace(applicability)
	if applicability == "" {
		applicability = "Global"
	}

	incSet := make(map[string]struct{}, len(include))
	for _, e := range include {
		e = strings.TrimSpace(e)
		if e != "" {
			incSet[e] = struct{}{}
		}
	}
	for _, e := range exclude {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if _, inInc := incSet[e]; inInc {
			continue // include wins over exclude if both somehow present
		}
		if entityCode != "" && e == entityCode {
			return false
		}
	}

	switch applicability {
	case "Entity":
		if len(incSet) == 0 {
			return false
		}
		if entityCode == "" {
			return false
		}
		_, ok := incSet[entityCode]
		return ok
	case "Custom":
		if len(incSet) > 0 {
			if entityCode == "" {
				return false
			}
			_, ok := incSet[entityCode]
			return ok
		}
		return true
	default:
		// Global / Scheme / Module — entity lists only act as excludes (already applied)
		return true
	}
}

func loadListValues(ctx context.Context, pool *pgxpool.Pool, policyID string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT value FROM policyengine_svc.policy_list_value
		WHERE policy_id = $1::uuid AND is_deleted = false`, policyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	vals := make([]string, 0)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	return vals, nil
}

func loadSlabRows(ctx context.Context, pool *pgxpool.Pool, policyID string) ([]map[string]interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT from_value, to_value, mode, action, COALESCE(approval_ref, ''), COALESCE(label, '')
		FROM policyengine_svc.policy_slab_row
		WHERE policy_id = $1::uuid AND is_deleted = false
		ORDER BY seq_order`, policyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var from float64
		var to *float64
		var mode, action, approvalRef, label string
		if err := rows.Scan(&from, &to, &mode, &action, &approvalRef, &label); err != nil {
			return nil, err
		}
		row := map[string]interface{}{
			"from":         from,
			"mode":         mode,
			"action":       action,
			"approval_ref": approvalRef,
			"label":        label,
		}
		if to != nil {
			row["to"] = *to
		} else {
			row["to"] = nil
		}
		out = append(out, row)
	}
	return out, nil
}

func loadCompositionBuckets(ctx context.Context, pool *pgxpool.Pool, policyID string) ([]map[string]interface{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT label, variable, min_value, max_value
		FROM policyengine_svc.policy_composition_bucket
		WHERE policy_id = $1::uuid AND is_deleted = false
		ORDER BY seq_order`, policyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var label, variable string
		var minVal, maxVal *float64
		if err := rows.Scan(&label, &variable, &minVal, &maxVal); err != nil {
			return nil, err
		}
		bucket := map[string]interface{}{
			"label":    label,
			"variable": variable,
		}
		if minVal != nil {
			bucket["min"] = *minVal
		}
		if maxVal != nil {
			bucket["max"] = *maxVal
		}
		out = append(out, bucket)
	}
	return out, nil
}
