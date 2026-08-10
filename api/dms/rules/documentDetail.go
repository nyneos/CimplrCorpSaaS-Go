package rules

import (
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type documentDetailReq struct {
	DocID string `json:"doc_id"`
}

type dispatchItem struct {
	DispatchID     string  `json:"dispatch_id"`
	OutboxID       string  `json:"outbox_id"`
	Recipient      string  `json:"recipient"`
	DispatchStatus string  `json:"dispatch_status"`
	DispatchedAt   *string `json:"dispatched_at"`
	ErrorDetail    string  `json:"error_detail"`
	CreatedAt      string  `json:"created_at"`
}

type documentDetailResp struct {
	DocID              string         `json:"doc_id"`
	RunID              string         `json:"run_id"`
	RuleID             string         `json:"rule_id"`
	RuleName           string         `json:"rule_name"`
	DocumentTemplateID string         `json:"document_template_id"`
	TemplateName       string         `json:"template_name"`
	S3Key              string         `json:"s3_key"`
	FileFormat         string         `json:"file_format"`
	OutputFilename     string         `json:"output_filename"`
	LocalPath          string         `json:"local_path"`
	FileSize           *int64         `json:"file_size"`
	Status             string         `json:"status"`
	CreatedAt          string         `json:"created_at"`
	RunStatus          string         `json:"run_status"`
	TriggerType        string         `json:"trigger_type"`
	TriggeredBy        string         `json:"triggered_by"`
	RunStartedAt       string         `json:"run_started_at"`
	RunFinishedAt      *string        `json:"run_finished_at"`
	ErrorDetail        string         `json:"error_detail"`
	WindowStart        *string        `json:"window_start"`
	WindowEnd          *string        `json:"window_end"`
	Dispatches         []dispatchItem `json:"dispatches"`
}

// HandleDocumentDetail returns one generated document with run context + dispatch rows.
func HandleDocumentDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req documentDetailReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		docID := strings.TrimSpace(req.DocID)
		if docID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "doc_id is required", "BAD_REQUEST")
			return
		}

		var out documentDetailResp
		var createdAt, runStarted time.Time
		var runFinished *time.Time
		var windowStart, windowEnd *time.Time
		var fileSize *int64
		err := pool.QueryRow(r.Context(), `
			SELECT gd.doc_id::text, gd.run_id::text, COALESCE(gr.rule_id::text, ''),
			       COALESCE(r.name, CASE WHEN gr.trigger_type = 'ADHOC' THEN 'Ad-hoc' ELSE '' END),
			       gd.document_template_id::text, COALESCE(t.name, ''),
			       gd.s3_key, gd.file_format, COALESCE(gd.output_filename, ''), COALESCE(gd.local_path, ''),
			       gd.file_size, gd.status, gd.created_at,
			       gr.status, gr.trigger_type, COALESCE(gr.triggered_by, ''),
			       gr.started_at, gr.finished_at, COALESCE(gr.error_detail, ''),
			       gr.window_start, gr.window_end
			FROM dms_svc.generated_document gd
			JOIN dms_svc.generation_run gr ON gr.run_id = gd.run_id
			LEFT JOIN dms_svc.generation_rule r ON r.rule_id = gr.rule_id
			LEFT JOIN dms_svc.template t ON t.template_id = gd.document_template_id
			WHERE gd.doc_id = $1::uuid`, docID,
		).Scan(
			&out.DocID, &out.RunID, &out.RuleID, &out.RuleName,
			&out.DocumentTemplateID, &out.TemplateName,
			&out.S3Key, &out.FileFormat, &out.OutputFilename, &out.LocalPath,
			&fileSize, &out.Status, &createdAt,
			&out.RunStatus, &out.TriggerType, &out.TriggeredBy,
			&runStarted, &runFinished, &out.ErrorDetail,
			&windowStart, &windowEnd,
		)
		if err != nil {
			api.LogErrorForResponse(w, "dms document detail: %v", err)
			api.RespondEnvelopeError(w, http.StatusNotFound, "document not found", "DMS_DOCUMENT_NOT_FOUND")
			return
		}
		out.FileSize = fileSize
		out.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		out.RunStartedAt = runStarted.UTC().Format(time.RFC3339)
		if runFinished != nil {
			s := runFinished.UTC().Format(time.RFC3339)
			out.RunFinishedAt = &s
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
			SELECT dispatch_id::text, COALESCE(outbox_id, ''), recipient, dispatch_status,
			       dispatched_at, COALESCE(error_detail, ''), created_at
			FROM dms_svc.generated_document_dispatch
			WHERE doc_id = $1::uuid
			ORDER BY created_at ASC`, docID)
		if err != nil {
			api.LogErrorForResponse(w, "dms document dispatches: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to load dispatches", "DMS_DOCUMENT_DETAIL_FAILED")
			return
		}
		defer drows.Close()

		out.Dispatches = make([]dispatchItem, 0)
		for drows.Next() {
			var d dispatchItem
			var dCreated time.Time
			var dAt *time.Time
			if err := drows.Scan(&d.DispatchID, &d.OutboxID, &d.Recipient, &d.DispatchStatus, &dAt, &d.ErrorDetail, &dCreated); err != nil {
				continue
			}
			d.CreatedAt = dCreated.UTC().Format(time.RFC3339)
			if dAt != nil {
				s := dAt.UTC().Format(time.RFC3339)
				d.DispatchedAt = &s
			}
			out.Dispatches = append(out.Dispatches, d)
		}

		api.RespondEnvelopeSuccess(w, "Document detail fetched", out)
	}
}
