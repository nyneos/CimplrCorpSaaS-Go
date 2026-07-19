// Package runtime exposes policy check for other modules (e.g. FD booking create).
package runtime

import (
	"context"
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
}

// BlocksSubmit is true when the aggregated breach action is HardBlock.
func (r CheckResult) BlocksSubmit() bool {
	return r.AggregatedAction == "HardBlock"
}

// FirstBreachMessage returns the first BREACH/ERROR message, if any.
func (r CheckResult) FirstBreachMessage() string {
	for _, pr := range r.Results {
		if pr.Result == "BREACH" || pr.Result == "ERROR" {
			if strings.TrimSpace(pr.Message) != "" {
				return pr.Message
			}
		}
	}
	return ""
}

// RunCheck loads applicable policies, evaluates via CIMPLR-Policy-Service, writes execution_log.
func RunCheck(ctx context.Context, pool *pgxpool.Pool, req CheckRequest) (CheckResult, error) {
	client := policysvc.NewFromEnv()
	if req.Variables == nil {
		req.Variables = map[string]string{}
	}

	policies := req.Policies
	if len(policies) == 0 {
		loaded, err := loadActivePolicies(ctx, pool, req.EventCode, req.ModuleCode)
		if err != nil {
			return CheckResult{}, err
		}
		policies = loaded
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

	return CheckResult{
		AggregatedAction: resp.AggregatedAction,
		Results:          resp.Results,
		DurationMS:       duration,
	}, nil
}

func loadActivePolicies(ctx context.Context, pool *pgxpool.Pool, eventCode, moduleCode string) ([]map[string]interface{}, error) {
	q := `
		SELECT p.policy_id::text, p.code, p.rule_type, p.action_on_breach, p.null_handling,
		       COALESCE(p.null_handling_default, ''), COALESCE(p.addl_expression, ''),
		       COALESCE(p.thr_variable, ''), COALESCE(p.thr_operator, ''), COALESCE(p.thr_value, 0),
		       COALESCE(p.thr_value_mode, ''), COALESCE(p.thr_percent_base, ''),
		       COALESCE(p.list_target_field, ''), COALESCE(p.list_mode, ''), COALESCE(p.list_case_sensitive, false)
		FROM policyengine_svc.policy_master p
		INNER JOIN policyengine_svc.policy_trigger t
			ON t.policy_id = p.policy_id AND t.is_deleted = false AND t.event_code = $1
		WHERE p.is_deleted = false AND p.status = 'Active' AND p.processing_status = 'APPROVED'
		  AND p.effective_start <= CURRENT_DATE
		  AND (p.effective_end IS NULL OR p.effective_end >= CURRENT_DATE)`
	args := []interface{}{eventCode}
	if strings.TrimSpace(moduleCode) != "" {
		q += `
		AND (
			NOT EXISTS (SELECT 1 FROM policyengine_svc.policy_module m WHERE m.policy_id = p.policy_id AND m.is_deleted = false)
			OR EXISTS (
				SELECT 1 FROM policyengine_svc.policy_module m
				WHERE m.policy_id = p.policy_id AND m.is_deleted = false AND m.module_code = $2
			)
		)`
		args = append(args, moduleCode)
	}

	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var (
			id, code, ruleType, action, nullH, nullDef, addl     string
			thrVar, thrOp, thrMode, thrBase, listField, listMode string
			thrVal                                               float64
			listCase                                             bool
		)
		if err := rows.Scan(&id, &code, &ruleType, &action, &nullH, &nullDef, &addl,
			&thrVar, &thrOp, &thrVal, &thrMode, &thrBase, &listField, &listMode, &listCase); err != nil {
			return nil, err
		}
		snap := map[string]interface{}{
			"policy_id":             id,
			"code":                  code,
			"rule_type":             ruleType,
			"action_on_breach":      action,
			"null_handling":         nullH,
			"null_handling_default": nullDef,
			"addl_expression":       addl,
			"thr_variable":          thrVar,
			"thr_operator":          thrOp,
			"thr_value":             thrVal,
			"thr_value_mode":        thrMode,
			"thr_percent_base":      thrBase,
			"list_target_field":     listField,
			"list_mode":             listMode,
			"list_case_sensitive":   listCase,
		}
		if ruleType == "list" {
			vals, _ := loadListValues(ctx, pool, id)
			snap["list_values"] = vals
		}
		out = append(out, snap)
	}
	return out, nil
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
