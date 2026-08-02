package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/policysvc"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type executionRunRecord struct {
	RunID, CorrelationID, TraceID, EventCode, ModuleCode, SubModule, FormID string
	HandlerName, APIPath, ActorUserID, ActorRole, EntityCode, RequestedIP   string
	BusinessRecordType, BusinessRecordID, SourceFileName, SourceFileID      string
	BatchID, AggregatedAction, Outcome, Status                              string
	CandidateCount, ApplicableCount, EvaluatedCount, SkippedCount           int
	PassCount, BreachCount, ErrorCount                                      int
	LoadDurationMS, EvaluationDurationMS, TotalDurationMS                   int
	StartedAt, CompletedAt                                                  time.Time
	Skips                                                                   []policyScopeSkip
}

type technicalExecutionRow struct {
	Code    string
	Message string
}

type comparisonRecord struct {
	Sequence                                       int
	Variable, ActualValue, Operator, ExpectedValue string
	LowerBound, UpperBound, Unit, Label, Outcome   string
}

func logExecutionError(ctx context.Context, traceID, message string, args ...interface{}) {
	if strings.TrimSpace(traceID) != "" {
		ctx = logger.WithTraceID(ctx, traceID)
	}
	logger.LogErrorCtx(ctx, message, args...)
}

func technicalExecution(code, clientSafeMessage string) *technicalExecutionRow {
	return &technicalExecutionRow{Code: code, Message: clientSafeMessage}
}

// execDurations groups the load/evaluation timings for completedExecutionRun
// so the function stays within the project's max-params limit.
type execDurations struct {
	Load       time.Duration
	Evaluation time.Duration
}

// execOutcome groups the evaluated results and their aggregated action/outcome
// for completedExecutionRun so the function stays within the project's
// max-params limit.
type execOutcome struct {
	Results []policysvc.PolicyResult
	Action  string
	Outcome string
}

func completedExecutionRun(runID string, req CheckRequest, started time.Time, durations execDurations, trace policyLoadTrace, out execOutcome) executionRunRecord {
	results, action, outcome := out.Results, out.Action, out.Outcome
	passCount, breachCount, errorCount := countResults(results)
	if outcome == "ERROR" && errorCount == 0 {
		errorCount = 1
	}
	completed := time.Now()
	status := "COMPLETED"
	if outcome == "ERROR" {
		status = "ERROR"
	}
	return executionRunRecord{
		RunID: runID, CorrelationID: req.CorrelationID, TraceID: req.TraceID,
		EventCode: req.EventCode, ModuleCode: req.ModuleCode, SubModule: req.SubModule,
		FormID: req.FormID, HandlerName: req.HandlerName, APIPath: req.APIPath,
		ActorUserID: req.ActorUserID, ActorRole: req.ActorRole, EntityCode: req.EntityCode,
		RequestedIP: req.RequestedIP, BusinessRecordType: req.BusinessRecordType,
		BusinessRecordID: req.BusinessRecordID, SourceFileName: req.SourceFileName,
		SourceFileID: req.SourceFileID, BatchID: req.BatchID,
		CandidateCount: trace.candidateCount(), ApplicableCount: trace.applicableCount(),
		EvaluatedCount: len(results), SkippedCount: len(trace.Skips),
		PassCount: passCount, BreachCount: breachCount, ErrorCount: errorCount,
		AggregatedAction: action, LoadDurationMS: durationMilliseconds(durations.Load),
		EvaluationDurationMS: durationMilliseconds(durations.Evaluation),
		TotalDurationMS:      durationMilliseconds(completed.Sub(started)),
		Outcome:              outcome, Status: status, StartedAt: started, CompletedAt: completed,
		Skips: trace.Skips,
	}
}

func durationMilliseconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(d.Milliseconds())
}

func countResults(results []policysvc.PolicyResult) (pass, breach, errors int) {
	for _, result := range results {
		switch strings.ToUpper(strings.TrimSpace(result.Result)) {
		case "PASS":
			pass++
		case "BREACH":
			breach++
		default:
			errors++
		}
	}
	return
}

func resultOutcome(results []policysvc.PolicyResult) string {
	pass, breach, errors := countResults(results)
	switch {
	case errors > 0:
		return "ERROR"
	case breach > 0:
		return "BREACH"
	case pass > 0:
		return "PASS"
	default:
		return "NO_APPLICABLE"
	}
}

func persistExecutionRun(ctx context.Context, pool *pgxpool.Pool, run executionRunRecord, technical *technicalExecutionRow, results []policysvc.PolicyResult) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin execution run persistence: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		INSERT INTO policyengine_svc.execution_run (
			run_id, correlation_id, trace_id, event_code, module_code, sub_module, form_id,
			handler_name, api_path, actor_user_id, actor_role, entity_code, requested_ip,
			business_record_type, business_record_id, source_file_name, source_file_id, batch_id,
			candidate_count, applicable_count, evaluated_count, skipped_count,
			pass_count, breach_count, error_count, aggregated_action,
			load_duration_ms, evaluation_duration_ms, total_duration_ms,
			outcome, status, started_at, completed_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
			$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33
		)`,
		run.RunID, common.NullIfEmpty(run.CorrelationID), common.NullIfEmpty(run.TraceID),
		common.NullIfEmpty(run.EventCode), common.NullIfEmpty(run.ModuleCode), common.NullIfEmpty(run.SubModule),
		common.NullIfEmpty(run.FormID), common.NullIfEmpty(run.HandlerName), common.NullIfEmpty(run.APIPath),
		common.NullIfEmpty(run.ActorUserID), common.NullIfEmpty(run.ActorRole), common.NullIfEmpty(run.EntityCode),
		common.NullIfEmpty(run.RequestedIP), common.NullIfEmpty(run.BusinessRecordType), common.NullIfEmpty(run.BusinessRecordID),
		common.NullIfEmpty(run.SourceFileName), common.NullIfEmpty(run.SourceFileID), common.NullIfEmpty(run.BatchID),
		run.CandidateCount, run.ApplicableCount, run.EvaluatedCount, run.SkippedCount,
		run.PassCount, run.BreachCount, run.ErrorCount, common.NullIfEmpty(run.AggregatedAction),
		run.LoadDurationMS, run.EvaluationDurationMS, run.TotalDurationMS,
		run.Outcome, run.Status, run.StartedAt, run.CompletedAt,
	); err != nil {
		return fmt.Errorf("insert execution run: %w", err)
	}

	for _, skip := range run.Skips {
		if _, err := tx.Exec(ctx, `
			INSERT INTO policyengine_svc.execution_policy_skip (
				run_id, policy_id, policy_code, policy_name, skip_stage,
				skip_code, skip_reason, sequence
			) VALUES ($1, NULLIF($2, '')::uuid, $3, $4, $5, $6, $7, $8)`,
			run.RunID, skip.PolicyID, common.NullIfEmpty(skip.PolicyCode), common.NullIfEmpty(skip.PolicyName),
			skip.Stage, skip.Code, skip.Reason, skip.Sequence,
		); err != nil {
			return fmt.Errorf("insert execution skip: %w", err)
		}
	}

	if technical != nil {
		if _, err := insertExecutionLog(ctx, tx, run, executionLogOutcome{
			PolicyCode: technical.Code, Result: "ERROR",
			Detail: technical.Message, FailCode: technical.Code, FailReason: technical.Message,
		}, nil); err != nil {
			return err
		}
	}
	for _, result := range results {
		comparisons := policyResultComparisons(result)
		detail := result.Message
		if strings.TrimSpace(result.UserMessage) != "" {
			detail = result.UserMessage
		}
		failCode, failReason := "", ""
		if result.Result == "BREACH" || result.Result == "ERROR" {
			failCode, failReason = result.Result, result.Message
		}
		executionID, err := insertExecutionLog(ctx, tx, run, executionLogOutcome{
			PolicyID: result.PolicyID, PolicyCode: result.Code, Result: result.Result,
			Action: result.Action, Detail: detail, FailCode: failCode, FailReason: failReason,
		}, comparisons)
		if err != nil {
			return err
		}
		if err := insertComparisons(ctx, tx, comparisonKey{
			RunID: run.RunID, ExecutionID: executionID, PolicyID: result.PolicyID,
			PolicyCode: result.Code, Result: result.Result,
		}, comparisons); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit execution run: %w", err)
	}
	return nil
}

// comparisonKey identifies the run/execution/policy a batch of comparisons
// belongs to, grouped so insertComparisons stays within the project's
// max-params limit.
type comparisonKey struct {
	RunID       string
	ExecutionID string
	PolicyID    string
	PolicyCode  string
	Result      string
}

func insertComparisons(ctx context.Context, tx pgx.Tx, key comparisonKey, comparisons []comparisonRecord) error {
	for _, comparison := range comparisons {
		outcome := strings.ToUpper(strings.TrimSpace(comparison.Outcome))
		if outcome == "" {
			outcome = strings.ToUpper(strings.TrimSpace(key.Result))
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO policyengine_svc.execution_comparison (
				run_id, execution_id, policy_id, policy_code, sequence, variable,
				actual_value, operator, expected_value, lower_bound, upper_bound,
				unit, label, outcome
			) VALUES ($1,$2,NULLIF($3,'')::uuid,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
			key.RunID, key.ExecutionID, key.PolicyID, common.NullIfEmpty(key.PolicyCode), comparison.Sequence,
			common.NullIfEmpty(comparison.Variable), common.NullIfEmpty(comparison.ActualValue),
			common.NullIfEmpty(comparison.Operator), common.NullIfEmpty(comparison.ExpectedValue),
			common.NullIfEmpty(comparison.LowerBound), common.NullIfEmpty(comparison.UpperBound),
			common.NullIfEmpty(comparison.Unit), common.NullIfEmpty(comparison.Label), outcome,
		); err != nil {
			return fmt.Errorf("insert execution comparison: %w", err)
		}
	}
	return nil
}

// executionLogOutcome groups the policy result fields for insertExecutionLog
// so the function stays within the project's max-params limit.
type executionLogOutcome struct {
	PolicyID   string
	PolicyCode string
	Result     string
	Action     string
	Detail     string
	FailCode   string
	FailReason string
}

func insertExecutionLog(ctx context.Context, tx pgx.Tx, run executionRunRecord, out executionLogOutcome, comparisons []comparisonRecord) (string, error) {
	policyID, policyCode, result := out.PolicyID, out.PolicyCode, out.Result
	action, detail, failCode, failReason := out.Action, out.Detail, out.FailCode, out.FailReason
	comparedVariable, comparedValue, limitValue := firstComparisonLegacyValues(comparisons)
	var executionID string
	err := tx.QueryRow(ctx, `
		INSERT INTO policyengine_svc.execution_log (
			run_id, correlation_id, trace_id, event_code, module_code, sub_module, form_id,
			handler_name, api_path, actor_user_id, actor_role, entity_code, requested_ip,
			business_record_type, business_record_id, source_file_name, source_file_id, batch_id,
			policy_id, policy_code, result, action_fired, detail_message, fail_code, fail_reason,
			compared_variable, compared_value, limit_value, duration_ms
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
			NULLIF($19,'')::uuid,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29
		) RETURNING execution_id::text`,
		run.RunID, common.NullIfEmpty(run.CorrelationID), common.NullIfEmpty(run.TraceID),
		common.NullIfEmpty(run.EventCode), common.NullIfEmpty(run.ModuleCode), common.NullIfEmpty(run.SubModule),
		common.NullIfEmpty(run.FormID), common.NullIfEmpty(run.HandlerName), common.NullIfEmpty(run.APIPath),
		common.NullIfEmpty(run.ActorUserID), common.NullIfEmpty(run.ActorRole), common.NullIfEmpty(run.EntityCode),
		common.NullIfEmpty(run.RequestedIP), common.NullIfEmpty(run.BusinessRecordType), common.NullIfEmpty(run.BusinessRecordID),
		common.NullIfEmpty(run.SourceFileName), common.NullIfEmpty(run.SourceFileID), common.NullIfEmpty(run.BatchID),
		policyID, common.NullIfEmpty(policyCode), result, common.NullIfEmpty(action), common.NullIfEmpty(detail),
		common.NullIfEmpty(failCode), common.NullIfEmpty(failReason), common.NullIfEmpty(comparedVariable),
		common.NullIfEmpty(comparedValue), common.NullIfEmpty(limitValue), run.EvaluationDurationMS,
	).Scan(&executionID)
	if err != nil {
		return "", fmt.Errorf("insert execution log: %w", err)
	}
	return executionID, nil
}

func firstComparisonLegacyValues(comparisons []comparisonRecord) (comparedVariable, comparedValue, limitValue string) {
	if len(comparisons) > 0 {
		comparedVariable = comparisons[0].Variable
		comparedValue = comparisons[0].ActualValue
		limitValue = comparisons[0].ExpectedValue
		if limitValue == "" && (comparisons[0].LowerBound != "" || comparisons[0].UpperBound != "") {
			limitValue = strings.TrimSpace(comparisons[0].LowerBound + ".." + comparisons[0].UpperBound)
		}
	}
	return comparedVariable, comparedValue, limitValue
}

func policyResultComparisons(result policysvc.PolicyResult) []comparisonRecord {
	out := make([]comparisonRecord, 0, len(result.Comparisons))
	for i, comparison := range result.Comparisons {
		sequence := comparison.Sequence
		if sequence <= 0 {
			sequence = i + 1
		}
		out = append(out, comparisonRecord{
			Sequence: sequence, Variable: comparison.Variable,
			ActualValue: comparison.ActualValue, Operator: comparison.Operator,
			ExpectedValue: comparison.ExpectedValue, LowerBound: comparison.LowerBound,
			UpperBound: comparison.UpperBound, Unit: comparison.Unit,
			Label: comparison.Label, Outcome: comparison.Outcome,
		})
	}
	return out
}
