package execution

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/policyengine/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

func HandleList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req common.PageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			req = common.PageRequest{}
		}
		page, pageSize, offset := common.NormalizePage(req)
		search := common.SearchPattern(req.Search)

		ctx := r.Context()
		where := `WHERE 1=1`
		countArgs := []interface{}{}
		if search != "" {
			where += ` AND (
				COALESCE(event_code, '') ILIKE $1 OR COALESCE(module_code, '') ILIKE $1
				OR COALESCE(entity_code, '') ILIKE $1 OR COALESCE(correlation_id, '') ILIKE $1
				OR COALESCE(handler_name, '') ILIKE $1 OR COALESCE(outcome, '') ILIKE $1
			)`
			countArgs = append(countArgs, search)
		}

		// Legacy execution_log rows remain accessible as one-row synthetic runs.
		// New rows always have run_id and are represented only by execution_run.
		baseQ := `
			WITH run_rows AS (
				SELECT run_id::text AS run_id, false AS is_legacy,
				       correlation_id, trace_id, event_code, module_code, sub_module,
				       form_id, handler_name, api_path, actor_user_id, actor_role,
				       entity_code, requested_ip, business_record_type, business_record_id,
				       source_file_name, source_file_id, batch_id,
				       candidate_count, applicable_count, evaluated_count, skipped_count,
				       pass_count, breach_count, error_count, aggregated_action,
				       load_duration_ms, evaluation_duration_ms, total_duration_ms,
				       outcome, status, started_at, completed_at
				FROM policyengine_svc.execution_run
				UNION ALL
				SELECT execution_id::text, true,
				       correlation_id, trace_id, event_code, module_code, sub_module,
				       form_id, handler_name, api_path, actor_user_id, actor_role,
				       entity_code, requested_ip, business_record_type, business_record_id,
				       source_file_name, source_file_id, batch_id,
				       1, 1, 1, 0,
				       CASE WHEN result = 'PASS' THEN 1 ELSE 0 END,
				       CASE WHEN result = 'BREACH' THEN 1 ELSE 0 END,
				       CASE WHEN result = 'ERROR' THEN 1 ELSE 0 END,
				       action_fired, 0, COALESCE(duration_ms, 0), COALESCE(duration_ms, 0),
				       result, CASE WHEN result = 'ERROR' THEN 'ERROR' ELSE 'COMPLETED' END,
				       evaluated_at, evaluated_at
				FROM policyengine_svc.execution_log
				WHERE run_id IS NULL
			)`
		var total, compliantCount, breachCount, errorCount, noApplicableCount, averageDurationMS int
		summaryQ := baseQ + `
			SELECT COUNT(*),
			       COUNT(*) FILTER (WHERE outcome = 'PASS'),
			       COUNT(*) FILTER (WHERE outcome = 'BREACH'),
			       COUNT(*) FILTER (WHERE outcome = 'ERROR'),
			       COUNT(*) FILTER (WHERE outcome = 'NO_APPLICABLE'),
			       COALESCE(ROUND(AVG(total_duration_ms))::int, 0)
			FROM run_rows ` + where
		if err := pool.QueryRow(ctx, summaryQ, countArgs...).Scan(
			&total,
			&compliantCount,
			&breachCount,
			&errorCount,
			&noApplicableCount,
			&averageDurationMS,
		); err != nil {
			api.LogErrorForResponse(w, "execution list count: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list execution runs", "EXECUTION_LIST_FAILED")
			return
		}

		listQ := baseQ + `
			SELECT run_id, is_legacy,
			       COALESCE(correlation_id, ''), COALESCE(trace_id, ''),
			       COALESCE(event_code, ''), COALESCE(module_code, ''), COALESCE(sub_module, ''),
			       COALESCE(form_id, ''), COALESCE(handler_name, ''), COALESCE(api_path, ''),
			       COALESCE(actor_user_id, ''), COALESCE(actor_role, ''), COALESCE(entity_code, ''),
			       COALESCE(requested_ip, ''),
			       COALESCE(business_record_type, ''), COALESCE(business_record_id, ''),
			       COALESCE(source_file_name, ''), COALESCE(source_file_id, ''), COALESCE(batch_id, ''),
			       candidate_count, applicable_count, evaluated_count, skipped_count,
			       pass_count, breach_count, error_count, COALESCE(aggregated_action, ''),
			       load_duration_ms, evaluation_duration_ms, total_duration_ms,
			       outcome, status, started_at, completed_at
			FROM run_rows ` + where + `
			ORDER BY started_at DESC`
		listArgs := append([]interface{}{}, countArgs...)
		argN := len(listArgs) + 1
		listQ += ` LIMIT $` + strconv.Itoa(argN) + ` OFFSET $` + strconv.Itoa(argN+1)
		listArgs = append(listArgs, pageSize, offset)

		rows, err := pool.Query(ctx, listQ, listArgs...)
		if err != nil {
			api.LogErrorForResponse(w, "execution list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list execution logs", "EXECUTION_LIST_FAILED")
			return
		}
		defer rows.Close()

		type item struct {
			RunID                string `json:"run_id"`
			IsLegacy             bool   `json:"is_legacy"`
			CorrelationID        string `json:"correlation_id"`
			TraceID              string `json:"trace_id"`
			EventCode            string `json:"event_code"`
			ModuleCode           string `json:"module_code"`
			SubModule            string `json:"sub_module"`
			FormID               string `json:"form_id"`
			HandlerName          string `json:"handler_name"`
			APIPath              string `json:"api_path"`
			ActorUserID          string `json:"actor_user_id"`
			ActorRole            string `json:"actor_role"`
			EntityCode           string `json:"entity_code"`
			RequestedIP          string `json:"requested_ip"`
			BusinessRecordType   string `json:"business_record_type"`
			BusinessRecordID     string `json:"business_record_id"`
			SourceFileName       string `json:"source_file_name"`
			SourceFileID         string `json:"source_file_id"`
			BatchID              string `json:"batch_id"`
			CandidateCount       int    `json:"candidate_count"`
			ApplicableCount      int    `json:"applicable_count"`
			EvaluatedCount       int    `json:"evaluated_count"`
			SkippedCount         int    `json:"skipped_count"`
			PassCount            int    `json:"pass_count"`
			BreachCount          int    `json:"breach_count"`
			ErrorCount           int    `json:"error_count"`
			AggregatedAction     string `json:"aggregated_action"`
			LoadDurationMS       int    `json:"load_duration_ms"`
			EvaluationDurationMS int    `json:"evaluation_duration_ms"`
			TotalDurationMS      int    `json:"total_duration_ms"`
			Outcome              string `json:"outcome"`
			Status               string `json:"status"`
			StartedAt            string `json:"started_at"`
			CompletedAt          string `json:"completed_at"`
		}
		out := make([]item, 0)
		for rows.Next() {
			var it item
			var startedAt, completedAt time.Time
			if err := rows.Scan(
				&it.RunID, &it.IsLegacy, &it.CorrelationID, &it.TraceID,
				&it.EventCode, &it.ModuleCode, &it.SubModule,
				&it.FormID, &it.HandlerName, &it.APIPath,
				&it.ActorUserID, &it.ActorRole, &it.EntityCode, &it.RequestedIP,
				&it.BusinessRecordType, &it.BusinessRecordID,
				&it.SourceFileName, &it.SourceFileID, &it.BatchID,
				&it.CandidateCount, &it.ApplicableCount, &it.EvaluatedCount, &it.SkippedCount,
				&it.PassCount, &it.BreachCount, &it.ErrorCount, &it.AggregatedAction,
				&it.LoadDurationMS, &it.EvaluationDurationMS, &it.TotalDurationMS,
				&it.Outcome, &it.Status, &startedAt, &completedAt,
			); err != nil {
				api.LogErrorForResponse(w, "execution list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list execution logs", "EXECUTION_LIST_FAILED")
				return
			}
			it.StartedAt = startedAt.UTC().Format(time.RFC3339)
			it.CompletedAt = completedAt.UTC().Format(time.RFC3339)
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Execution runs fetched", map[string]interface{}{
			"rows":            out,
			"total":           total,
			"page":            page,
			"page_size":       pageSize,
			"summary": map[string]int{
				"total_runs":          total,
				"compliant_count":     compliantCount,
				"breach_count":        breachCount,
				"error_count":         errorCount,
				"no_applicable_count": noApplicableCount,
				"average_duration_ms": averageDurationMS,
			},
			"legacy_behavior": "execution_log rows without run_id are returned as synthetic one-policy runs",
		})
	}
}
