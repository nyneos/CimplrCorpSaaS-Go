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

type documentListReq struct {
	RuleID        string `json:"rule_id"`
	RunID         string `json:"run_id"`
	ModuleCode    string `json:"module_code"`
	SubModuleCode string `json:"sub_module_code"`
	Limit         int    `json:"limit"`
}

type documentListItem struct {
	DocID              string  `json:"doc_id"`
	RunID              string  `json:"run_id"`
	RuleID             string  `json:"rule_id"`
	RuleName           string  `json:"rule_name"`
	DocumentTemplateID string  `json:"document_template_id"`
	S3Key              string  `json:"s3_key"`
	FileFormat         string  `json:"file_format"`
	OutputFilename     string  `json:"output_filename"`
	FileSize           *int64  `json:"file_size"`
	Status             string  `json:"status"`
	CreatedAt          string  `json:"created_at"`
	RunStatus          string  `json:"run_status"`
	TriggerType        string  `json:"trigger_type"`
	DispatchPending    int     `json:"dispatch_pending"`
	DispatchSent       int     `json:"dispatch_sent"`
	DispatchFailed     int     `json:"dispatch_failed"`
}

// HandleListDocuments is the Sent Box data source — generated documents newest first.
// Includes ADHOC runs (rule_id NULL). Optional module/sub-module scopes via rule or template.
func HandleListDocuments(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req documentListReq
		if r.ContentLength != 0 {
			if err := common.DecodeJSON(r, &req); err != nil {
				api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
				return
			}
		}
		req.RuleID = strings.TrimSpace(req.RuleID)
		req.RunID = strings.TrimSpace(req.RunID)
		req.ModuleCode = strings.TrimSpace(req.ModuleCode)
		req.SubModuleCode = strings.TrimSpace(req.SubModuleCode)

		query := `
			SELECT gd.doc_id::text, gd.run_id::text, COALESCE(gr.rule_id::text, ''),
			       COALESCE(r.name, CASE WHEN gr.trigger_type = 'ADHOC' THEN 'Ad-hoc' ELSE '' END),
			       gd.document_template_id::text, gd.s3_key, gd.file_format,
			       COALESCE(gd.output_filename, ''), gd.file_size,
			       gd.status, gd.created_at, gr.status, gr.trigger_type,
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document_dispatch d
			                 WHERE d.doc_id = gd.doc_id AND d.dispatch_status = 'PENDING'), 0),
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document_dispatch d
			                 WHERE d.doc_id = gd.doc_id AND d.dispatch_status = 'SENT'), 0),
			       COALESCE((SELECT COUNT(*) FROM dms_svc.generated_document_dispatch d
			                 WHERE d.doc_id = gd.doc_id AND d.dispatch_status = 'FAILED'), 0)
			FROM dms_svc.generated_document gd
			JOIN dms_svc.generation_run gr ON gr.run_id = gd.run_id
			LEFT JOIN dms_svc.generation_rule r ON r.rule_id = gr.rule_id
			LEFT JOIN dms_svc.template t ON t.template_id = gd.document_template_id
			WHERE 1=1`
		args := []interface{}{}
		if req.RuleID != "" {
			args = append(args, req.RuleID)
			query += " AND gr.rule_id = $" + strconv.Itoa(len(args)) + "::uuid"
		}
		if req.RunID != "" {
			args = append(args, req.RunID)
			query += " AND gd.run_id = $" + strconv.Itoa(len(args)) + "::uuid"
		}
		if req.ModuleCode != "" {
			args = append(args, req.ModuleCode)
			n := strconv.Itoa(len(args))
			query += " AND COALESCE(r.module_code, t.module_code) = $" + n
		}
		if req.SubModuleCode != "" {
			args = append(args, req.SubModuleCode)
			n := strconv.Itoa(len(args))
			query += " AND COALESCE(r.sub_module_code, t.sub_module_code) = $" + n
		}
		query += " ORDER BY gd.created_at DESC"
		if req.Limit > 0 {
			args = append(args, req.Limit)
			query += " LIMIT $" + strconv.Itoa(len(args))
		}

		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			api.LogErrorForResponse(w, "dms documents list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list documents", "DMS_DOCUMENTS_LIST_FAILED")
			return
		}
		defer rows.Close()

		out := make([]documentListItem, 0)
		for rows.Next() {
			var it documentListItem
			var createdAt time.Time
			var fileSize *int64
			if err := rows.Scan(
				&it.DocID, &it.RunID, &it.RuleID, &it.RuleName,
				&it.DocumentTemplateID, &it.S3Key, &it.FileFormat, &it.OutputFilename, &fileSize,
				&it.Status, &createdAt, &it.RunStatus, &it.TriggerType,
				&it.DispatchPending, &it.DispatchSent, &it.DispatchFailed,
			); err != nil {
				continue
			}
			it.FileSize = fileSize
			it.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			out = append(out, it)
		}
		api.RespondEnvelopeSuccess(w, "Documents fetched", out)
	}
}
