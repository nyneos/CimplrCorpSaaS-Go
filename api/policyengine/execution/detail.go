package execution

import (
	"context"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type detailRequest struct {
	RunID string `json:"run_id"`
}

type runDetail struct {
	RunID, CorrelationID, TraceID, EventCode, ModuleCode, SubModule, FormID string
	HandlerName, APIPath, ActorUserID, ActorRole, EntityCode, RequestedIP   string
	BusinessRecordType, BusinessRecordID, SourceFileName, SourceFileID      string
	BatchID, AggregatedAction, Outcome, Status                              string
	CandidateCount, ApplicableCount, EvaluatedCount, SkippedCount           int
	PassCount, BreachCount, ErrorCount                                      int
	LoadDurationMS, EvaluationDurationMS, TotalDurationMS                   int
	StartedAt, CompletedAt                                                  time.Time
	IsLegacy                                                                bool
}

func HandleDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req detailRequest
		if err := common.DecodeJSON(r, &req); err != nil || uuid.Validate(strings.TrimSpace(req.RunID)) != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "valid run_id is required", "BAD_REQUEST")
			return
		}

		ctx := r.Context()
		run, err := loadRunDetail(ctx, pool, req.RunID)
		if err == pgx.ErrNoRows {
			run, err = loadLegacyRunDetail(ctx, pool, req.RunID)
		}
		if err == pgx.ErrNoRows {
			api.RespondEnvelopeError(w, http.StatusNotFound, "execution run not found", "EXECUTION_RUN_NOT_FOUND")
			return
		}
		if err != nil {
			api.LogErrorForResponse(w, "execution detail run: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to fetch execution run", "EXECUTION_DETAIL_FAILED")
			return
		}

		evaluated, err := loadEvaluatedPolicies(ctx, pool, run.RunID, run.IsLegacy)
		if err != nil {
			api.LogErrorForResponse(w, "execution detail evaluated policies: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to fetch execution run", "EXECUTION_DETAIL_FAILED")
			return
		}
		comparisons, err := loadComparisons(ctx, pool, run.RunID, run.IsLegacy)
		if err != nil {
			api.LogErrorForResponse(w, "execution detail comparisons: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to fetch execution run", "EXECUTION_DETAIL_FAILED")
			return
		}
		skips, err := loadSkips(ctx, pool, run.RunID, run.IsLegacy)
		if err != nil {
			api.LogErrorForResponse(w, "execution detail skips: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to fetch execution run", "EXECUTION_DETAIL_FAILED")
			return
		}

		// Nest comparisons under each result — matches UI / agreed detail contract.
		api.RespondEnvelopeSuccess(w, "Execution run fetched", map[string]interface{}{
			"run":     runPayload(run),
			"results": nestComparisons(evaluated, comparisons),
			"skips":   skips,
		})
	}
}

func nestComparisons(results, comparisons []map[string]interface{}) []map[string]interface{} {
	byExec := make(map[string][]map[string]interface{}, len(results))
	for _, c := range comparisons {
		execID, _ := c["execution_id"].(string)
		if execID == "" {
			continue
		}
		// Drop redundant execution_id from nested payload; keep explainability fields.
		item := map[string]interface{}{
			"sequence": c["sequence"], "variable": c["variable"],
			"actual_value": c["actual_value"], "operator": c["operator"],
			"expected_value": c["expected_value"], "lower_bound": c["lower_bound"],
			"upper_bound": c["upper_bound"], "unit": c["unit"],
			"label": c["label"], "outcome": c["outcome"],
		}
		byExec[execID] = append(byExec[execID], item)
	}
	out := make([]map[string]interface{}, 0, len(results))
	for _, r := range results {
		execID, _ := r["execution_id"].(string)
		row := make(map[string]interface{}, len(r)+1)
		for k, v := range r {
			row[k] = v
		}
		nested := byExec[execID]
		if nested == nil {
			nested = []map[string]interface{}{}
		}
		row["comparisons"] = nested
		out = append(out, row)
	}
	return out
}

func loadRunDetail(ctx context.Context, pool *pgxpool.Pool, runID string) (runDetail, error) {
	var run runDetail
	err := pool.QueryRow(ctx, `
		SELECT run_id::text, COALESCE(correlation_id,''), COALESCE(trace_id,''),
		       COALESCE(event_code,''), COALESCE(module_code,''), COALESCE(sub_module,''),
		       COALESCE(form_id,''), COALESCE(handler_name,''), COALESCE(api_path,''),
		       COALESCE(actor_user_id,''), COALESCE(actor_role,''), COALESCE(entity_code,''),
		       COALESCE(requested_ip,''), COALESCE(business_record_type,''),
		       COALESCE(business_record_id,''), COALESCE(source_file_name,''),
		       COALESCE(source_file_id,''), COALESCE(batch_id,''),
		       candidate_count, applicable_count, evaluated_count, skipped_count,
		       pass_count, breach_count, error_count, COALESCE(aggregated_action,''),
		       load_duration_ms, evaluation_duration_ms, total_duration_ms,
		       outcome, status, started_at, completed_at
		FROM policyengine_svc.execution_run WHERE run_id = $1::uuid`, runID).Scan(
		&run.RunID, &run.CorrelationID, &run.TraceID, &run.EventCode, &run.ModuleCode,
		&run.SubModule, &run.FormID, &run.HandlerName, &run.APIPath, &run.ActorUserID,
		&run.ActorRole, &run.EntityCode, &run.RequestedIP, &run.BusinessRecordType,
		&run.BusinessRecordID, &run.SourceFileName, &run.SourceFileID, &run.BatchID,
		&run.CandidateCount, &run.ApplicableCount, &run.EvaluatedCount, &run.SkippedCount,
		&run.PassCount, &run.BreachCount, &run.ErrorCount, &run.AggregatedAction,
		&run.LoadDurationMS, &run.EvaluationDurationMS, &run.TotalDurationMS,
		&run.Outcome, &run.Status, &run.StartedAt, &run.CompletedAt,
	)
	return run, err
}

func loadLegacyRunDetail(ctx context.Context, pool *pgxpool.Pool, runID string) (runDetail, error) {
	var run runDetail
	err := pool.QueryRow(ctx, `
		SELECT execution_id::text, COALESCE(correlation_id,''), COALESCE(trace_id,''),
		       COALESCE(event_code,''), COALESCE(module_code,''), COALESCE(sub_module,''),
		       COALESCE(form_id,''), COALESCE(handler_name,''), COALESCE(api_path,''),
		       COALESCE(actor_user_id,''), COALESCE(actor_role,''), COALESCE(entity_code,''),
		       COALESCE(requested_ip,''), COALESCE(business_record_type,''),
		       COALESCE(business_record_id,''), COALESCE(source_file_name,''),
		       COALESCE(source_file_id,''), COALESCE(batch_id,''),
		       1,1,1,0,
		       CASE WHEN result='PASS' THEN 1 ELSE 0 END,
		       CASE WHEN result='BREACH' THEN 1 ELSE 0 END,
		       CASE WHEN result='ERROR' THEN 1 ELSE 0 END,
		       COALESCE(action_fired,''), 0, COALESCE(duration_ms,0), COALESCE(duration_ms,0),
		       result, CASE WHEN result='ERROR' THEN 'ERROR' ELSE 'COMPLETED' END,
		       evaluated_at, evaluated_at
		FROM policyengine_svc.execution_log
		WHERE execution_id = $1::uuid AND run_id IS NULL`, runID).Scan(
		&run.RunID, &run.CorrelationID, &run.TraceID, &run.EventCode, &run.ModuleCode,
		&run.SubModule, &run.FormID, &run.HandlerName, &run.APIPath, &run.ActorUserID,
		&run.ActorRole, &run.EntityCode, &run.RequestedIP, &run.BusinessRecordType,
		&run.BusinessRecordID, &run.SourceFileName, &run.SourceFileID, &run.BatchID,
		&run.CandidateCount, &run.ApplicableCount, &run.EvaluatedCount, &run.SkippedCount,
		&run.PassCount, &run.BreachCount, &run.ErrorCount, &run.AggregatedAction,
		&run.LoadDurationMS, &run.EvaluationDurationMS, &run.TotalDurationMS,
		&run.Outcome, &run.Status, &run.StartedAt, &run.CompletedAt,
	)
	run.IsLegacy = err == nil
	return run, err
}

func loadEvaluatedPolicies(ctx context.Context, pool *pgxpool.Pool, runID string, legacy bool) ([]map[string]interface{}, error) {
	whereSQL := "el.run_id = $1::uuid"
	if legacy {
		whereSQL = "el.execution_id = $1::uuid AND el.run_id IS NULL"
	}
	rows, err := pool.Query(ctx, `
		SELECT el.execution_id::text, COALESCE(el.policy_id::text,''), COALESCE(el.policy_code,''),
		       COALESCE(pm.name,''), el.result, COALESCE(el.action_fired,''),
		       COALESCE(el.detail_message,''),
		       COALESCE(el.fail_code,''), COALESCE(el.fail_reason,''),
		       COALESCE(el.compared_variable,''), COALESCE(el.compared_value,''),
		       COALESCE(el.limit_value,''), COALESCE(el.duration_ms,0), el.evaluated_at
		FROM policyengine_svc.execution_log el
		LEFT JOIN policyengine_svc.policy_master pm ON pm.policy_id = el.policy_id
		WHERE `+whereSQL+`
		ORDER BY el.evaluated_at, el.execution_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var executionID, policyID, policyCode, policyName, result, action, detail, failCode, failReason string
		var variable, value, limit string
		var duration int
		var evaluatedAt time.Time
		if err := rows.Scan(&executionID, &policyID, &policyCode, &policyName, &result, &action, &detail,
			&failCode, &failReason, &variable, &value, &limit, &duration, &evaluatedAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]interface{}{
			"execution_id": executionID, "policy_id": policyID, "policy_code": policyCode,
			"policy_name": policyName, "result": result, "action_fired": action,
			// Persist prefers user_message into detail_message; expose both for UI.
			"user_message": detail, "detail_message": detail,
			"fail_code": failCode, "fail_reason": failReason, "compared_variable": variable,
			"compared_value": value, "limit_value": limit, "duration_ms": duration,
			"evaluated_at": evaluatedAt.UTC().Format(time.RFC3339),
		})
	}
	return out, rows.Err()
}

func loadComparisons(ctx context.Context, pool *pgxpool.Pool, runID string, legacy bool) ([]map[string]interface{}, error) {
	if legacy {
		return []map[string]interface{}{}, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT comparison_id::text, execution_id::text, COALESCE(policy_id::text,''),
		       COALESCE(policy_code,''), sequence, COALESCE(variable,''),
		       COALESCE(actual_value,''), COALESCE(operator,''), COALESCE(expected_value,''),
		       COALESCE(lower_bound,''), COALESCE(upper_bound,''), COALESCE(unit,''),
		       COALESCE(label,''), outcome
		FROM policyengine_svc.execution_comparison
		WHERE run_id = $1::uuid ORDER BY execution_id, sequence`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var comparisonID, executionID, policyID, policyCode, variable, actual, operator string
		var expected, lower, upper, unit, label, outcome string
		var sequence int
		if err := rows.Scan(&comparisonID, &executionID, &policyID, &policyCode, &sequence,
			&variable, &actual, &operator, &expected, &lower, &upper, &unit, &label, &outcome); err != nil {
			return nil, err
		}
		out = append(out, map[string]interface{}{
			"comparison_id": comparisonID, "execution_id": executionID, "policy_id": policyID,
			"policy_code": policyCode, "sequence": sequence, "variable": variable,
			"actual_value": actual, "operator": operator, "expected_value": expected,
			"lower_bound": lower, "upper_bound": upper, "unit": unit, "label": label, "outcome": outcome,
		})
	}
	return out, rows.Err()
}

func loadSkips(ctx context.Context, pool *pgxpool.Pool, runID string, legacy bool) ([]map[string]interface{}, error) {
	if legacy {
		return []map[string]interface{}{}, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT skip_id::text, COALESCE(policy_id::text,''), COALESCE(policy_code,''),
		       COALESCE(policy_name,''), skip_stage, skip_code, skip_reason, sequence, created_at
		FROM policyengine_svc.execution_policy_skip
		WHERE run_id = $1::uuid ORDER BY sequence`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var skipID, policyID, policyCode, policyName, stage, code, reason string
		var sequence int
		var createdAt time.Time
		if err := rows.Scan(&skipID, &policyID, &policyCode, &policyName, &stage, &code,
			&reason, &sequence, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]interface{}{
			"skip_id": skipID, "policy_id": policyID, "policy_code": policyCode,
			"policy_name": policyName, "skip_stage": stage, "skip_code": code,
			"skip_reason": reason, "sequence": sequence,
			"created_at": createdAt.UTC().Format(time.RFC3339),
		})
	}
	return out, rows.Err()
}

func runPayload(run runDetail) map[string]interface{} {
	return map[string]interface{}{
		"run_id": run.RunID, "is_legacy": run.IsLegacy, "correlation_id": run.CorrelationID,
		"trace_id": run.TraceID, "event_code": run.EventCode, "module_code": run.ModuleCode,
		"sub_module": run.SubModule, "form_id": run.FormID, "handler_name": run.HandlerName,
		"api_path": run.APIPath, "actor_user_id": run.ActorUserID, "actor_role": run.ActorRole,
		"entity_code": run.EntityCode, "requested_ip": run.RequestedIP,
		"business_record_type": run.BusinessRecordType, "business_record_id": run.BusinessRecordID,
		"source_file_name": run.SourceFileName, "source_file_id": run.SourceFileID, "batch_id": run.BatchID,
		"candidate_count": run.CandidateCount, "applicable_count": run.ApplicableCount,
		"evaluated_count": run.EvaluatedCount, "skipped_count": run.SkippedCount,
		"pass_count": run.PassCount, "breach_count": run.BreachCount, "error_count": run.ErrorCount,
		"aggregated_action": run.AggregatedAction, "load_duration_ms": run.LoadDurationMS,
		"evaluation_duration_ms": run.EvaluationDurationMS, "total_duration_ms": run.TotalDurationMS,
		"outcome": run.Outcome, "status": run.Status,
		"started_at":   run.StartedAt.UTC().Format(time.RFC3339),
		"completed_at": run.CompletedAt.UTC().Format(time.RFC3339),
	}
}
