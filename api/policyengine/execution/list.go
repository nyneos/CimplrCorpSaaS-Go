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
				COALESCE(policy_code, '') ILIKE $1 OR COALESCE(event_code, '') ILIKE $1
				OR COALESCE(module_code, '') ILIKE $1 OR COALESCE(fail_reason, '') ILIKE $1
				OR COALESCE(entity_code, '') ILIKE $1 OR COALESCE(correlation_id, '') ILIKE $1
				OR COALESCE(handler_name, '') ILIKE $1
			)`
			countArgs = append(countArgs, search)
		}

		var total int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM policyengine_svc.execution_log `+where, countArgs...).Scan(&total); err != nil {
			api.LogErrorForResponse(w, "execution list count: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list execution logs", "EXECUTION_LIST_FAILED")
			return
		}

		listQ := `
			SELECT execution_id::text,
			       COALESCE(correlation_id, ''), COALESCE(trace_id, ''),
			       COALESCE(event_code, ''), COALESCE(module_code, ''), COALESCE(sub_module, ''),
			       COALESCE(form_id, ''), COALESCE(handler_name, ''), COALESCE(api_path, ''),
			       COALESCE(actor_user_id, ''), COALESCE(actor_role, ''), COALESCE(entity_code, ''),
			       COALESCE(requested_ip, ''),
			       COALESCE(business_record_type, ''), COALESCE(business_record_id, ''),
			       COALESCE(source_file_name, ''), COALESCE(source_file_id, ''), COALESCE(batch_id, ''),
			       COALESCE(policy_id::text, ''), COALESCE(policy_code, ''),
			       result, COALESCE(action_fired, ''),
			       COALESCE(detail_message, ''), COALESCE(fail_code, ''), COALESCE(fail_reason, ''),
			       evaluated_at
			FROM policyengine_svc.execution_log ` + where + `
			ORDER BY evaluated_at DESC`
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
			ExecutionID        string `json:"execution_id"`
			CorrelationID      string `json:"correlation_id"`
			TraceID            string `json:"trace_id"`
			EventCode          string `json:"event_code"`
			ModuleCode         string `json:"module_code"`
			SubModule          string `json:"sub_module"`
			FormID             string `json:"form_id"`
			HandlerName        string `json:"handler_name"`
			APIPath            string `json:"api_path"`
			ActorUserID        string `json:"actor_user_id"`
			ActorRole          string `json:"actor_role"`
			EntityCode         string `json:"entity_code"`
			RequestedIP        string `json:"requested_ip"`
			BusinessRecordType string `json:"business_record_type"`
			BusinessRecordID   string `json:"business_record_id"`
			SourceFileName     string `json:"source_file_name"`
			SourceFileID       string `json:"source_file_id"`
			BatchID            string `json:"batch_id"`
			PolicyID           string `json:"policy_id"`
			PolicyCode         string `json:"policy_code"`
			Result             string `json:"result"`
			ActionFired        string `json:"action_fired"`
			DetailMessage      string `json:"detail_message"`
			FailCode           string `json:"fail_code"`
			FailReason         string `json:"fail_reason"`
			EvaluatedAt        string `json:"evaluated_at"`
		}
		out := make([]item, 0)
		for rows.Next() {
			var it item
			var evaluatedAt time.Time
			if err := rows.Scan(
				&it.ExecutionID, &it.CorrelationID, &it.TraceID,
				&it.EventCode, &it.ModuleCode, &it.SubModule,
				&it.FormID, &it.HandlerName, &it.APIPath,
				&it.ActorUserID, &it.ActorRole, &it.EntityCode, &it.RequestedIP,
				&it.BusinessRecordType, &it.BusinessRecordID,
				&it.SourceFileName, &it.SourceFileID, &it.BatchID,
				&it.PolicyID, &it.PolicyCode,
				&it.Result, &it.ActionFired,
				&it.DetailMessage, &it.FailCode, &it.FailReason,
				&evaluatedAt,
			); err != nil {
				api.LogErrorForResponse(w, "execution list scan: %v", err)
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list execution logs", "EXECUTION_LIST_FAILED")
				return
			}
			it.EvaluatedAt = evaluatedAt.UTC().Format(time.RFC3339)
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Execution logs fetched", map[string]interface{}{
			"rows":      out,
			"total":     total,
			"page":      page,
			"page_size": pageSize,
		})
	}
}
