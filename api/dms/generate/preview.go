package generate

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"

	"github.com/jackc/pgx/v5/pgxpool"
)

type previewReq struct {
	DocumentTemplateID string            `json:"document_template_id"`
	HTML               string            `json:"html"`
	MergeFields        map[string]string `json:"merge_fields"` // field_key → field_code (draft preview)
	MergeOverrides     map[string]string `json:"merge_overrides"`
	SourceID           string            `json:"source_id"`
	SourceIDs          []string          `json:"source_ids"`
	ModuleCode         string            `json:"module_code"`
	SubModuleCode      string            `json:"sub_module_code"`
	OutputFormat       string            `json:"output_format"`
}

// HandlePreview returns merged HTML for an approved template and/or draft body.
// Used by Template Editor Preview and ad-hoc Generate Preview.
func HandlePreview(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req previewReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}

		format := strings.ToUpper(strings.TrimSpace(req.OutputFormat))
		if format == "" {
			format = "HTML"
		}

		sourceID := strings.TrimSpace(req.SourceID)
		if sourceID == "" && len(req.SourceIDs) > 0 {
			sourceID = strings.TrimSpace(req.SourceIDs[0])
		}

		var row map[string]any
		if sourceID != "" {
			fetched, err := dmsjobs.FetchAdhocSourceRow(r.Context(), pool, req.ModuleCode, req.SubModuleCode, sourceID)
			if err != nil {
				api.LogErrorForResponse(w, "dms preview fetch source: %v", err)
				api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, "failed to load source row", "DMS_PREVIEW_SOURCE")
				return
			}
			row = fetched
		}

		html, err := dmsjobs.PreviewMergedOrDraft(r.Context(), pool, dmsjobs.PreviewInput{
			DocumentTemplateID: strings.TrimSpace(req.DocumentTemplateID),
			HTML:               req.HTML,
			MergeFields:        req.MergeFields,
			Row:                row,
			Overrides:          req.MergeOverrides,
			Format:             format,
		})
		if err != nil {
			api.LogErrorForResponse(w, "dms preview: %v", err)
			api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, err.Error(), "DMS_PREVIEW_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Preview ready", map[string]interface{}{
			"html": html,
		})
	}
}
