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

	"github.com/google/uuid"
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
	ActorRole          string
	RequestedIP        string
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
	// NotifyFields are domain_catalog field_code → value (pre-CDM). Used by
	// dispatchNotifyBreaches to build template keys via NOTIFICATION aliases.
	NotifyFields map[string]interface{}
	Policies     []map[string]interface{}
}

// CheckResult holds evaluation output for module handlers.
type CheckResult struct {
	RunID            string
	AggregatedAction string
	Results          []policysvc.PolicyResult
	DurationMS       int
	ConflictReport   ConflictReport
	// TriggerApprovalMatrixID is the approval matrix pinned on the first breached
	// TriggerApproval policy. Empty when nothing breached that way or no matrix
	// was chosen — callers then resolve their own matrix as before.
	TriggerApprovalMatrixID string
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
// Prefers author user_message (substituted breach_message) over evaluator Message.
func (r CheckResult) FirstBreachMessage() string {
	for _, pr := range r.Results {
		if pr.Result == "BREACH" || pr.Result == "ERROR" {
			label := strings.TrimSpace(pr.Name)
			if label == "" {
				label = strings.TrimSpace(pr.Code)
			}
			msg := strings.TrimSpace(pr.UserMessage)
			if msg == "" {
				msg = strings.TrimSpace(pr.Message)
			}
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
		row := map[string]interface{}{
			"policy_id": pr.PolicyID,
			"code":      pr.Code,
			"name":      pr.Name,
			"result":    pr.Result,
			"action":    pr.Action,
			"message":   pr.Message,
		}
		if um := strings.TrimSpace(pr.UserMessage); um != "" {
			row["user_message"] = um
		}
		if len(pr.Comparisons) > 0 {
			row["comparisons"] = pr.Comparisons
		}
		out = append(out, row)
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
	if line := headerASCII(r.headerSummaryLine()); line != "" {
		w.Header().Set("X-Policy-Summary", line)
	}
}

func (r CheckResult) headerSummaryLine() string {
	passed, failed := r.CountPassedFailed()
	if passed+failed == 0 {
		return ""
	}
	passNames, failNames := r.policyNameLists()
	if failed == 0 {
		if len(passNames) > 0 {
			return "Policy check passed: " + strings.Join(passNames, ", ")
		}
		return "Policy check passed"
	}
	if len(failNames) > 0 {
		return "Policy check failed - " + strings.Join(failNames, ", ")
	}
	return "Policy check failed"
}

func headerASCII(s string) string {
	var b strings.Builder
	for _, ch := range s {
		switch {
		case ch == '–' || ch == '—':
			b.WriteByte('-')
		case ch >= 32 && ch < 127:
			b.WriteRune(ch)
		}
	}
	return strings.TrimSpace(b.String())
}

// LoadActivePolicySnapshots returns Active+APPROVED policy snapshots for the
// given trigger/module/sub-module/entity (same set RunCheck loads).
func LoadActivePolicySnapshots(ctx context.Context, pool *pgxpool.Pool, eventCode, moduleCode, subModule, entityCode string, vars map[string]string) ([]map[string]interface{}, error) {
	trace, err := loadActivePoliciesWithTrace(ctx, pool, eventCode, moduleCode, subModule, entityCode, vars)
	return trace.Applicable, err
}

// RunCheck loads applicable policies, evaluates via CIMPLR-Policy-Service, writes execution_log.
func RunCheck(ctx context.Context, pool *pgxpool.Pool, req CheckRequest) (CheckResult, error) {
	if !PolicyChecksEnabled() {
		return CheckResult{AggregatedAction: ""}, nil
	}
	runID := uuid.NewString()
	runStarted := time.Now()
	client := policysvc.NewFromEnv()
	if req.Variables == nil {
		req.Variables = map[string]string{}
	}
	if strings.TrimSpace(req.EntityCode) == "" {
		req.EntityCode = entityFromRecord(req.NotifyFields, req.Variables)
	}

	policies := req.Policies
	loadTrace := policyLoadTrace{}
	loadStarted := time.Now()
	if len(policies) == 0 {
		if strings.TrimSpace(req.ModuleCode) != "" && strings.TrimSpace(req.SubModule) == "" {
			err := fmt.Errorf("sub_module is required when module_code is set (refusing to load all sub-modules for %s)", req.ModuleCode)
			run := completedExecutionRun(runID, req, runStarted, execDurations{Load: time.Since(loadStarted)}, loadTrace, execOutcome{Outcome: "ERROR"})
			_ = persistExecutionRun(ctx, pool, run, technicalExecution("INVALID_SCOPE_REQUEST", "Policy check request scope is invalid"), nil)
			return CheckResult{RunID: runID}, err
		}
		loaded, err := loadActivePoliciesWithTrace(ctx, pool, req.EventCode, req.ModuleCode, req.SubModule, req.EntityCode, req.Variables)
		if err != nil {
			logExecutionError(ctx, req.TraceID, "policy execution scope load failed: %v", err)
			run := completedExecutionRun(runID, req, runStarted, execDurations{Load: time.Since(loadStarted)}, loadTrace, execOutcome{Outcome: "ERROR"})
			_ = persistExecutionRun(ctx, pool, run, technicalExecution("POLICY_LOAD_ERROR", "Policy scope could not be loaded"), nil)
			return CheckResult{RunID: runID}, err
		}
		loadTrace = loaded
		policies = loaded.Applicable
	} else {
		loadTrace.Candidates = policies
		loadTrace.Applicable = policies
	}
	loadDuration := time.Since(loadStarted)

	if len(policies) == 0 {
		run := completedExecutionRun(runID, req, runStarted, execDurations{Load: loadDuration}, loadTrace, execOutcome{Outcome: "NO_APPLICABLE"})
		if err := persistExecutionRun(ctx, pool, run, nil, nil); err != nil {
			return CheckResult{RunID: runID}, err
		}
		return CheckResult{RunID: runID}, nil
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
		run := completedExecutionRun(runID, req, runStarted, execDurations{Load: loadDuration}, loadTrace, execOutcome{
			Results: []policysvc.PolicyResult{synthetic}, Action: "HardBlock", Outcome: "ERROR",
		})
		_ = persistExecutionRun(ctx, pool, run, technicalExecution("POLICY_CONFLICT_IMPOSSIBLE", msg), nil)
		return CheckResult{
			RunID:            runID,
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
	evalDuration := time.Since(started)
	duration := int(evalDuration.Milliseconds())
	if err == nil && (resp == nil || !resp.Success) {
		detail := "empty policy service response"
		if resp != nil && strings.TrimSpace(resp.Error) != "" {
			detail = resp.Error
		}
		err = fmt.Errorf("policy service rejected evaluation: %s", detail)
	}
	if err != nil {
		logExecutionError(ctx, req.TraceID, "policy evaluation service failed: %v", err)
		run := completedExecutionRun(runID, req, runStarted, execDurations{Load: loadDuration, Evaluation: evalDuration}, loadTrace, execOutcome{Outcome: "ERROR"})
		_ = persistExecutionRun(ctx, pool, run, technicalExecution("POLICY_SERVICE_ERROR", "Policy evaluation service failed"), nil)
		return CheckResult{RunID: runID}, err
	}

	outcome := resultOutcome(resp.Results)
	run := completedExecutionRun(runID, req, runStarted, execDurations{Load: loadDuration, Evaluation: evalDuration}, loadTrace, execOutcome{
		Results: resp.Results, Action: resp.AggregatedAction, Outcome: outcome,
	})
	if err := persistExecutionRun(ctx, pool, run, nil, resp.Results); err != nil {
		return CheckResult{RunID: runID}, err
	}

	dispatchNotifyBreaches(ctx, pool, req, policies, resp.Results)

	return CheckResult{
		RunID:                   runID,
		AggregatedAction:        resp.AggregatedAction,
		Results:                 resp.Results,
		DurationMS:              duration,
		ConflictReport:          conflictReport,
		TriggerApprovalMatrixID: breachedTriggerApprovalMatrix(req.EventCode, policies, resp.Results),
	}, nil
}

// breachedTriggerApprovalMatrix returns the approval matrix pinned on the first
// breached TriggerApproval policy, or "" when none applies.
func breachedTriggerApprovalMatrix(eventCode string, policies []map[string]interface{}, results []policysvc.PolicyResult) string {
	if common.ForbidsTriggerApproval(eventCode) {
		return ""
	}
	matrixByPolicy := make(map[string]string, len(policies))
	for _, p := range policies {
		id, _ := p["policy_id"].(string)
		if id == "" {
			continue
		}
		if m, ok := p["approval_matrix_id"].(string); ok {
			matrixByPolicy[id] = strings.TrimSpace(m)
		}
	}
	for _, pr := range results {
		if pr.Result != "BREACH" || pr.Action != common.BreachTriggerApproval {
			continue
		}
		if m := matrixByPolicy[pr.PolicyID]; m != "" {
			return m
		}
	}
	return ""
}


var entityFieldKeys = []string{"entity_id", "entity_name", "entity_code", "entity", "entity_level_0"}

func entityFromRecord(fields map[string]interface{}, vars map[string]string) string {
	for _, key := range entityFieldKeys {
		if raw, ok := fields[key]; ok && raw != nil {
			if s := strings.TrimSpace(stringifyCDMValue(raw)); s != "" {
				return s
			}
		}
	}
	for _, key := range entityFieldKeys {
		suffix := "." + key
		for path, val := range vars {
			if strings.HasSuffix(path, suffix) {
				if s := strings.TrimSpace(val); s != "" {
					return s
				}
			}
		}
	}
	return ""
}

func loadActivePoliciesWithTrace(ctx context.Context, pool *pgxpool.Pool, eventCode, moduleCode, subModule, entityCode string, vars map[string]string) (policyLoadTrace, error) {
	codes := ExpandTriggerAliases(eventCode)
	q := `
		SELECT p.policy_id::text, p.code, p.name, p.rule_type, p.action_on_breach, p.null_handling,
		       COALESCE(p.null_handling_default, ''), COALESCE(p.addl_expression, ''),
		       COALESCE(p.thr_variable, ''), COALESCE(p.thr_operator, ''), COALESCE(p.thr_value, 0),
		       COALESCE(p.thr_value_date::text, ''),
		       COALESCE(p.thr_value_mode, ''), COALESCE(p.thr_percent_base, ''),
		       COALESCE(p.list_target_field, ''), COALESCE(p.list_mode, ''), COALESCE(p.list_source, ''),
		       COALESCE(p.list_dynamic_ref, ''), COALESCE(p.list_case_sensitive, false),
		       COALESCE(p.notification_group, ''), COALESCE(p.breach_message, ''),
		       COALESCE(p.formula_expression, ''), COALESCE(p.formula_return_type, ''),
		       COALESCE(p.formula_operator, ''), COALESCE(p.formula_value, 0),
		       COALESCE(p.slab_variable, ''), COALESCE(p.slab_percent_base, ''),
		       COALESCE(p.comp_base, ''), COALESCE(p.comp_total_check_variable, ''),
		       p.comp_total_check_min, p.comp_total_check_max,
		       COALESCE(p.applicability, 'Global'),
		       COALESCE(p.instrument_filter, ''), COALESCE(p.currency_filter, ''),
		       COALESCE(p.tenor_filter, ''), COALESCE(p.rating_filter, ''),
		       COALESCE(p.approval_matrix_id, '')
		FROM policyengine_svc.policy_master p
		INNER JOIN policyengine_svc.policy_trigger t
			ON t.policy_id = p.policy_id AND t.is_deleted = false AND t.event_code = ANY($1::text[])
		-- Include Scheduled: a missed lifecycle cron must never silently skip enforcement;
		-- the date window below is the real gate (stale UI label is acceptable).
		WHERE p.is_deleted = false AND p.status IN ('Active', 'Scheduled') AND p.processing_status = 'APPROVED'
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
		return policyLoadTrace{}, err
	}
	defer rows.Close()

	out := make([]map[string]interface{}, 0)
	policyIDs := make([]string, 0)
	for rows.Next() {
		var (
			id, code, name, ruleType, action, nullH, nullDef, addl          string
			thrVar, thrOp, thrValueDate, thrMode, thrBase                   string
			listField, listMode, listSource, listDynRef                     string
			notifGroup, breachMsg, formulaExpr, formulaRet, formulaOp       string
			slabVar, slabPercentBase, compBase, compTotalVar, applicability string
			fltInstrument, fltCurrency, fltTenor, fltRating                 string
			approvalMatrixID                                                string
			thrVal, formulaVal                                              float64
			compTotalMin, compTotalMax                                      *float64
			listCase                                                        bool
		)
		if err := rows.Scan(&id, &code, &name, &ruleType, &action, &nullH, &nullDef, &addl,
			&thrVar, &thrOp, &thrVal, &thrValueDate, &thrMode, &thrBase, &listField, &listMode, &listSource, &listDynRef, &listCase,
			&notifGroup, &breachMsg, &formulaExpr, &formulaRet, &formulaOp, &formulaVal, &slabVar, &slabPercentBase,
			&compBase, &compTotalVar, &compTotalMin, &compTotalMax, &applicability,
			&fltInstrument, &fltCurrency, &fltTenor, &fltRating, &approvalMatrixID); err != nil {
			return policyLoadTrace{}, err
		}
		snap := map[string]interface{}{
			"approval_matrix_id":        approvalMatrixID,
			"policy_id":                 id,
			"code":                      code,
			"name":                      name,
			"rule_type":                 ruleType,
			"action_on_breach":          action,
			"null_handling":             nullH,
			"null_handling_default":     nullDef,
			"addl_expression":           addl,
			"thr_variable":              thrVar,
			"thr_operator":              thrOp,
			"thr_value":                 thrVal,
			"thr_value_date":            thrValueDate,
			"thr_value_mode":            thrMode,
			"thr_percent_base":          thrBase,
			"list_target_field":         listField,
			"list_mode":                 listMode,
			"list_source":               listSource,
			"list_dynamic_ref":          listDynRef,
			"list_case_sensitive":       listCase,
			"notification_group":        notifGroup,
			"breach_message":            breachMsg,
			"formula_expression":        formulaExpr,
			"formula_return_type":       formulaRet,
			"formula_operator":          formulaOp,
			"formula_value":             formulaVal,
			"slab_variable":             slabVar,
			"slab_percent_base":         slabPercentBase,
			"comp_base":                 compBase,
			"comp_total_check_variable": compTotalVar,
			"applicability":             applicability,
			"_filter_instrument":        fltInstrument,
			"_filter_currency":          fltCurrency,
			"_filter_tenor":             fltTenor,
			"_filter_rating":            fltRating,
		}
		if compTotalMin != nil {
			snap["comp_total_check_min"] = *compTotalMin
		}
		if compTotalMax != nil {
			snap["comp_total_check_max"] = *compTotalMax
		}
		if ruleType == "list" {
			vals, _ := loadListValues(ctx, pool, id, listSource, listDynRef)
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
		return policyLoadTrace{}, err
	}
	entityCode = strings.TrimSpace(entityCode)
	entityCodes := entityCodeAliases(ctx, pool, entityCode)
	filtered := make([]map[string]interface{}, 0, len(out))
	skips := make([]policyScopeSkip, 0)
	for _, snap := range out {
		id, _ := snap["policy_id"].(string)
		applicability, _ := snap["applicability"].(string)
		if code, reason, skipped := dataFilterSkip(
			snapshotString(snap, "_filter_instrument"),
			snapshotString(snap, "_filter_currency"),
			snapshotString(snap, "_filter_tenor"),
			snapshotString(snap, "_filter_rating"),
			vars,
		); skipped {
			skips = appendScopeSkip(skips, snap, "DATA_FILTER", code, reason)
			continue
		}
		scope := entityMap[id]
		if policyAppliesToEntity(applicability, entityCodes, scope.include, scope.exclude) {
			delete(snap, "_filter_instrument")
			delete(snap, "_filter_currency")
			delete(snap, "_filter_tenor")
			delete(snap, "_filter_rating")
			filtered = append(filtered, snap)
		} else {
			code, reason := entityScopeSkip(applicability, entityCode, scope.include, scope.exclude)
			skips = appendScopeSkip(skips, snap, "ENTITY_SCOPE", code, reason)
		}
	}
	filtered, skips = resolveScopeOverrides(filtered, skips)
	return policyLoadTrace{Candidates: out, Applicable: filtered, Skips: skips}, nil
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
func entityCodeAliases(ctx context.Context, pool *pgxpool.Pool, entityCode string) []string {
	entityCode = strings.TrimSpace(entityCode)
	if entityCode == "" || pool == nil {
		return nil
	}
	aliases := []string{entityCode}
	// Both entity masters: cash records carry masterentitycash codes, while FX /
	// FD / investment records carry masterentity names. Resolving only one left
	// the other's include/exclude lists matchable by exact string alone.
	rows, err := pool.Query(ctx, `
		SELECT COALESCE(entity_id::text,''), COALESCE(entity_name,'')
		FROM masterentitycash
		WHERE lower(entity_id::text) = lower($1) OR lower(entity_name) = lower($1)
		UNION
		SELECT COALESCE(entity_id::text,''), COALESCE(entity_name,'')
		FROM masterentity
		WHERE lower(entity_id::text) = lower($1) OR lower(entity_name) = lower($1)`, entityCode)
	if err != nil {
		return aliases
	}
	defer rows.Close()
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			continue
		}
		for _, candidate := range []string{id, name} {
			candidate = strings.TrimSpace(candidate)
			if candidate != "" && !containsFold(aliases, candidate) {
				aliases = append(aliases, candidate)
			}
		}
	}
	return aliases
}

func policyAppliesToEntity(applicability string, entityCodes []string, include, exclude []string) bool {
	applicability = strings.TrimSpace(applicability)
	if applicability == "" {
		applicability = "Global"
	}

	matchesRequest := func(scopeEntry string) bool {
		return containsFold(entityCodes, scopeEntry)
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
		if matchesRequest(e) {
			return false
		}
	}

	includedByRequest := func() bool {
		if len(entityCodes) == 0 {
			return false
		}
		for e := range incSet {
			if matchesRequest(e) {
				return true
			}
		}
		return false
	}

	switch applicability {
	case "Entity":
		if len(incSet) == 0 {
			return false
		}
		return includedByRequest()
	case "Custom":
		if len(incSet) > 0 {
			return includedByRequest()
		}
		return true
	default:
		// Global / Scheme / Module — entity lists only act as excludes (already applied)
		return true
	}
}

func loadListValues(ctx context.Context, pool *pgxpool.Pool, policyID, listSource, listDynRef string) ([]string, error) {
	if strings.EqualFold(strings.TrimSpace(listSource), "Dynamic") {
		// Materialise masters into the snapshot before the wire call — Policy
		// Service has no DB. Empty/error leaves list_values empty so the
		// evaluator's Dynamic+empty guard stays fail-safe (ERROR → HardBlock).
		vals, err := resolveDynamicList(ctx, pool, listDynRef)
		if err != nil {
			return []string{}, err
		}
		return vals, nil
	}
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
