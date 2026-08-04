package rules

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type executionLogReq struct {
	RuleID        string `json:"rule_id"`
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
	Status        string `json:"status"` // ALL | SUCCESS | FAILED | PARTIAL | RUNNING
	DateFrom      string `json:"date_from"`
	DateTo        string `json:"date_to"`
	Search        string `json:"search"`
	Limit         int    `json:"limit"`
	Offset        int    `json:"offset"`
}

type executionLogItem struct {
	RunID         string  `json:"run_id"`
	RuleID        string  `json:"rule_id"`
	RuleName      string  `json:"rule_name"`
	VersionID     string  `json:"version_id"`
	TriggerType   string  `json:"trigger_type"`
	TriggeredBy   string  `json:"triggered_by"`
	StartedAt     string  `json:"started_at"`
	FinishedAt    *string `json:"finished_at"`
	Status        string  `json:"status"`
	ErrorDetail   string  `json:"error_detail"`
	WindowStart   *string `json:"window_start"`
	WindowEnd     *string `json:"window_end"`
	DocCount      int     `json:"doc_count"`
	DispatchSent  int     `json:"dispatch_sent"`
	DispatchFail  int     `json:"dispatch_failed"`
	DispatchPend  int     `json:"dispatch_pending"`
}

// HandleListExecutionLog lists generation_run rows (Transformation-style execution log).
func HandleListExecutionLog(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req executionLogReq
		if r.ContentLength != 0 {
			if err := common.DecodeJSON(r, &req); err != nil {
				api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
				return
			}
		}
		req.RuleID = strings.TrimSpace(req.RuleID)
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)
		req.Status = strings.ToUpper(strings.TrimSpace(req.Status))
		req.Search = strings.TrimSpace(req.Search)
		if req.Status == "" {
			req.Status = "ALL"
		}
		if req.Limit <= 0 || req.Limit > 500 {
			req.Limit = 100
		}
		if req.Offset < 0 {
			req.Offset = 0
		}

		query := `
			SELECT gr.run_id::text, COALESCE(gr.rule_id::text, ''),
			       COALESCE(r.name, CASE WHEN gr.trigger_type = 'ADHOC' THEN 'Ad-hoc' ELSE '' END),
			       COALESCE(gr.version_id::text, ''),
			       gr.trigger_type, COALESCE(gr.triggered_by, ''),
			       gr.started_at, gr.finished_at, gr.status, COALESCE(gr.error_detail, ''),
			       gr.window_start, gr.window_end,
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document gd WHERE gd.run_id = gr.run_id), 0),
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document_dispatch d
			                 JOIN dms_svc.generated_document gd ON gd.doc_id = d.doc_id
			                 WHERE gd.run_id = gr.run_id AND d.dispatch_status = 'SENT'), 0),
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document_dispatch d
			                 JOIN dms_svc.generated_document gd ON gd.doc_id = d.doc_id
			                 WHERE gd.run_id = gr.run_id AND d.dispatch_status = 'FAILED'), 0),
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document_dispatch d
			                 JOIN dms_svc.generated_document gd ON gd.doc_id = d.doc_id
			                 WHERE gd.run_id = gr.run_id AND d.dispatch_status = 'PENDING'), 0)
			FROM dms_svc.generation_run gr
			LEFT JOIN dms_svc.generation_rule r ON r.rule_id = gr.rule_id
			WHERE 1=1`
		args := []interface{}{}

		if req.RuleID != "" {
			args = append(args, req.RuleID)
			query += " AND gr.rule_id = $" + strconv.Itoa(len(args)) + "::uuid"
		}
		if req.ModuleCode != "" {
			args = append(args, req.ModuleCode)
			n := strconv.Itoa(len(args))
			query += ` AND (
				r.module_code = $` + n + `
				OR EXISTS (
					SELECT 1 FROM dms_svc.generated_document gd
					JOIN dms_svc.template t ON t.template_id = gd.document_template_id
					WHERE gd.run_id = gr.run_id AND t.module_code = $` + n + `
				)
			)`
		}
		if req.SubModuleCode != "" {
			args = append(args, req.SubModuleCode)
			n := strconv.Itoa(len(args))
			query += ` AND (
				r.sub_module_code = $` + n + `
				OR EXISTS (
					SELECT 1 FROM dms_svc.generated_document gd
					JOIN dms_svc.template t ON t.template_id = gd.document_template_id
					WHERE gd.run_id = gr.run_id AND t.sub_module_code = $` + n + `
				)
			)`
		}
		if req.Status != "ALL" {
			args = append(args, req.Status)
			query += " AND gr.status = $" + strconv.Itoa(len(args))
		}
		if req.DateFrom != "" {
			args = append(args, req.DateFrom)
			query += " AND gr.started_at::date >= $" + strconv.Itoa(len(args)) + "::date"
		}
		if req.DateTo != "" {
			args = append(args, req.DateTo)
			query += " AND gr.started_at::date <= $" + strconv.Itoa(len(args)) + "::date"
		}
		if req.Search != "" {
			args = append(args, "%"+strings.ToLower(req.Search)+"%")
			n := strconv.Itoa(len(args))
			query += " AND (LOWER(COALESCE(r.name, '')) LIKE $" + n +
				" OR LOWER(gr.run_id::text) LIKE $" + n +
				" OR LOWER(COALESCE(gr.error_detail, '')) LIKE $" + n +
				" OR LOWER(COALESCE(gr.triggered_by, '')) LIKE $" + n +
				" OR LOWER(gr.trigger_type) LIKE $" + n + ")"
		}

		args = append(args, req.Limit, req.Offset)
		query += " ORDER BY gr.started_at DESC LIMIT $" + strconv.Itoa(len(args)-1) +
			" OFFSET $" + strconv.Itoa(len(args))

		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			api.LogErrorForResponse(w, "dms execution-log list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list execution log", "DMS_EXECUTION_LOG_FAILED")
			return
		}
		defer rows.Close()

		out := make([]executionLogItem, 0)
		for rows.Next() {
			var it executionLogItem
			var startedAt time.Time
			var finishedAt *time.Time
			var windowStart, windowEnd *time.Time
			if err := rows.Scan(
				&it.RunID, &it.RuleID, &it.RuleName, &it.VersionID,
				&it.TriggerType, &it.TriggeredBy,
				&startedAt, &finishedAt, &it.Status, &it.ErrorDetail,
				&windowStart, &windowEnd,
				&it.DocCount, &it.DispatchSent, &it.DispatchFail, &it.DispatchPend,
			); err != nil {
				continue
			}
			it.StartedAt = startedAt.UTC().Format(time.RFC3339)
			if finishedAt != nil {
				s := finishedAt.UTC().Format(time.RFC3339)
				it.FinishedAt = &s
			}
			if windowStart != nil {
				s := windowStart.Format("2006-01-02")
				it.WindowStart = &s
			}
			if windowEnd != nil {
				s := windowEnd.Format("2006-01-02")
				it.WindowEnd = &s
			}
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Execution log fetched", out)
	}
}
