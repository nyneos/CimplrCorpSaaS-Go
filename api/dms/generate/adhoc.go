package generate

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"

	"github.com/jackc/pgx/v5/pgxpool"
)

type adhocReq struct {
	DocumentTemplateID string            `json:"document_template_id"`
	OutputFormat       string            `json:"output_format"`
	ModuleCode         string            `json:"module_code"`
	SubModuleCode      string            `json:"sub_module_code"`
	SourceIDs          []string          `json:"source_ids"`
	MergeOverrides     map[string]string `json:"merge_overrides"`
	SendEmail          bool              `json:"send_email"`
	EmailTemplateID    string            `json:"email_template_id"`
	EmailTo            []string          `json:"email_to"`
	EmailCc            []string          `json:"email_cc"`
	EmailSubject       string            `json:"email_subject"`
	EmailBodyHTML      string            `json:"email_body_html"`
	ActorID            string            `json:"actor_id"`
}

// HandleAdhoc generates documents from selected rows + an approved template (no rule).
func HandleAdhoc(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req adhocReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		tplID := strings.TrimSpace(req.DocumentTemplateID)
		if tplID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "document_template_id is required", "VALIDATION_ERROR")
			return
		}
		actor := common.RequestActor(r, req.ActorID)

		result, err := dmsjobs.RunAdhocGeneration(r.Context(), pool, dmsjobs.AdhocRequest{
			ModuleCode:         strings.TrimSpace(req.ModuleCode),
			SubModuleCode:      strings.TrimSpace(req.SubModuleCode),
			DocumentTemplateID: tplID,
			OutputFormat:       req.OutputFormat,
			SourceIDs:          req.SourceIDs,
			TriggeredBy:        actor,
			MergeOverrides:     req.MergeOverrides,
			SendEmail:          req.SendEmail,
			EmailTemplateID:    strings.TrimSpace(req.EmailTemplateID),
			EmailTo:            req.EmailTo,
			EmailCc:            req.EmailCc,
			EmailSubject:       req.EmailSubject,
			EmailBodyHTML:      req.EmailBodyHTML,
		})
		if err != nil {
			api.LogErrorForResponse(w, "dms adhoc: %v", err)
			api.RespondEnvelopeFailureWithData(w, http.StatusUnprocessableEntity, err.Error(), "DMS_ADHOC_FAILED",
				map[string]interface{}{"run_id": result.RunID})
			return
		}
		api.RespondEnvelopeSuccess(w, "Ad-hoc generation completed", map[string]interface{}{
			"run_id":       result.RunID,
			"doc_ids":      result.DocIDs,
			"filenames":    result.Filenames,
			"html_preview": result.HTMLPreview,
		})
	}
}
