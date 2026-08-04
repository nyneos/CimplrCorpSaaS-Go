package rules

import (
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type executionDetailReq struct {
	RunID string `json:"run_id"`
}

type executionDetailDoc struct {
	DocID              string `json:"doc_id"`
	DocumentTemplateID string `json:"document_template_id"`
	TemplateName       string `json:"template_name"`
	S3Key              string `json:"s3_key"`
	FileFormat         string `json:"file_format"`
	FileSize           *int64 `json:"file_size"`
	Status             string `json:"status"`
	CreatedAt          string `json:"created_at"`
	DispatchPending    int    `json:"dispatch_pending"`
	DispatchSent       int    `json:"dispatch_sent"`
	DispatchFailed     int    `json:"dispatch_failed"`
}

type executionDetailResp struct {
	RunID        string               `json:"run_id"`
	RuleID       string               `json:"rule_id"`
	RuleName     string               `json:"rule_name"`
	VersionID    string               `json:"version_id"`
	TriggerType  string               `json:"trigger_type"`
	TriggeredBy  string               `json:"triggered_by"`
	StartedAt    string               `json:"started_at"`
	FinishedAt   *string              `json:"finished_at"`
	Status       string               `json:"status"`
	ErrorDetail  string               `json:"error_detail"`
	WindowStart  *string              `json:"window_start"`
	WindowEnd    *string              `json:"window_end"`
	Documents    []executionDetailDoc `json:"documents"`
}

// HandleExecutionDetail returns one generation_run with its generated documents.
func HandleExecutionDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req executionDetailReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		runID := strings.TrimSpace(req.RunID)
		if runID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "run_id is required", "BAD_REQUEST")
			return
		}

		var out executionDetailResp
		var startedAt time.Time
		var finishedAt *time.Time
		var windowStart, windowEnd *time.Time
		err := pool.QueryRow(r.Context(), `
			SELECT gr.run_id::text, COALESCE(gr.rule_id::text, ''),
			       COALESCE(r.name, CASE WHEN gr.trigger_type = 'ADHOC' THEN 'Ad-hoc' ELSE '' END),
			       COALESCE(gr.version_id::text, ''),
			       gr.trigger_type, COALESCE(gr.triggered_by, ''),
			       gr.started_at, gr.finished_at, gr.status, COALESCE(gr.error_detail, ''),
			       gr.window_start, gr.window_end
			FROM dms_svc.generation_run gr
			LEFT JOIN dms_svc.generation_rule r ON r.rule_id = gr.rule_id
			WHERE gr.run_id = $1::uuid`, runID,
		).Scan(
			&out.RunID, &out.RuleID, &out.RuleName, &out.VersionID,
			&out.TriggerType, &out.TriggeredBy,
			&startedAt, &finishedAt, &out.Status, &out.ErrorDetail,
			&windowStart, &windowEnd,
		)
		if err != nil {
			api.LogErrorForResponse(w, "dms execution detail: %v", err)
			api.RespondEnvelopeError(w, http.StatusNotFound, "run not found", "DMS_RUN_NOT_FOUND")
			return
		}
		out.StartedAt = startedAt.UTC().Format(time.RFC3339)
		if finishedAt != nil {
			s := finishedAt.UTC().Format(time.RFC3339)
			out.FinishedAt = &s
		}
		if windowStart != nil {
			s := windowStart.Format("2006-01-02")
			out.WindowStart = &s
		}
		if windowEnd != nil {
			s := windowEnd.Format("2006-01-02")
			out.WindowEnd = &s
		}

		drows, err := pool.Query(r.Context(), `
			SELECT gd.doc_id::text, gd.document_template_id::text, COALESCE(t.name, ''),
			       gd.s3_key, gd.file_format, gd.file_size, gd.status, gd.created_at,
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document_dispatch d
			                 WHERE d.doc_id = gd.doc_id AND d.dispatch_status = 'PENDING'), 0),
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document_dispatch d
			                 WHERE d.doc_id = gd.doc_id AND d.dispatch_status = 'SENT'), 0),
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document_dispatch d
			                 WHERE d.doc_id = gd.doc_id AND d.dispatch_status = 'FAILED'), 0)
			FROM dms_svc.generated_document gd
			LEFT JOIN dms_svc.template t ON t.template_id = gd.document_template_id
			WHERE gd.run_id = $1::uuid
			ORDER BY gd.created_at ASC`, runID)
		if err != nil {
			api.LogErrorForResponse(w, "dms execution detail docs: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load run documents", "DMS_RUN_DETAIL_FAILED")
			return
		}
		defer drows.Close()

		out.Documents = make([]executionDetailDoc, 0)
		for drows.Next() {
			var d executionDetailDoc
			var createdAt time.Time
			var fileSize *int64
			if err := drows.Scan(
				&d.DocID, &d.DocumentTemplateID, &d.TemplateName,
				&d.S3Key, &d.FileFormat, &fileSize, &d.Status, &createdAt,
				&d.DispatchPending, &d.DispatchSent, &d.DispatchFailed,
			); err != nil {
				continue
			}
			d.FileSize = fileSize
			d.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			out.Documents = append(out.Documents, d)
		}

		api.RespondEnvelopeSuccess(w, "Execution detail fetched", out)
	}
}
