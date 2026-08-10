package templates

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

type detailReq struct {
	TemplateID string `json:"template_id"`
	// VersionID optionally selects which version's content to return;
	// defaults to the template's current_version_id, or its latest version
	// if nothing has been approved yet.
	VersionID string `json:"version_id"`
}

// HandleDetail returns a template's header, every version summary, and the
// full content (+ merge fields / chart placeholders) of one selected version —
// everything the template studio needs to open a template for editing.
func HandleDetail(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req detailReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.TemplateID = strings.TrimSpace(req.TemplateID)
		if req.TemplateID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "template_id is required", "VALIDATION_ERROR")
			return
		}

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms template detail begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to fetch template", "DMS_TEMPLATE_DETAIL_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		var d templateDetail
		var createdAt, lastModifiedAt time.Time
		var currentVersionID *string
		if err := tx.QueryRow(r.Context(), `
			SELECT template_id::text, name, description, template_type, module_code, sub_module_code,
			       entity_id, entity_name,
			       status, processing_status, current_version_id::text,
			       COALESCE(created_by,''), created_at, COALESCE(last_modified_by,''), last_modified_at
			FROM dms_svc.template
			WHERE template_id = $1::uuid AND is_deleted = false`, req.TemplateID,
		).Scan(&d.TemplateID, &d.Name, &d.Description, &d.TemplateType, &d.ModuleCode, &d.SubModuleCode,
			&d.EntityID, &d.EntityName,
			&d.Status, &d.ProcessingStatus, &currentVersionID, &d.CreatedBy, &createdAt, &d.LastModifiedBy, &lastModifiedAt,
		); err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "template not found", "NOT_FOUND")
			return
		}
		scope := ctxutil.FromContext(r.Context())
		entityID := ""
		if d.EntityID != nil {
			entityID = *d.EntityID
		}
		if !common.RequireEntityAccess(w, scope, entityID) {
			return
		}
		d.CurrentVersionID = currentVersionID
		d.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		d.LastModifiedAt = lastModifiedAt.UTC().Format(time.RFC3339)

		versionRows, err := tx.Query(r.Context(), `
			SELECT version_id::text, version_no, status, source, COALESCE(created_by,''),
			       created_at, approved_by, approved_at, source_file_name
			FROM dms_svc.template_version
			WHERE template_id = $1::uuid AND is_deleted = false
			ORDER BY version_no DESC`, req.TemplateID)
		if err != nil {
			api.LogErrorForResponse(w, "dms template detail versions: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to fetch template", "DMS_TEMPLATE_DETAIL_FAILED")
			return
		}
		d.Versions = make([]versionSummary, 0)
		for versionRows.Next() {
			var v versionSummary
			var vCreatedAt time.Time
			var vApprovedAt *time.Time
			if err := versionRows.Scan(&v.VersionID, &v.VersionNo, &v.Status, &v.Source, &v.CreatedBy,
				&vCreatedAt, &v.ApprovedBy, &vApprovedAt, &v.SourceFile); err != nil {
				continue
			}
			v.CreatedAt = vCreatedAt.UTC().Format(time.RFC3339)
			if vApprovedAt != nil {
				s := vApprovedAt.UTC().Format(time.RFC3339)
				v.ApprovedAt = &s
			}
			v.IsCurrent = currentVersionID != nil && *currentVersionID == v.VersionID
			d.Versions = append(d.Versions, v)
		}
		versionRows.Close()

		selectedVersionID := strings.TrimSpace(req.VersionID)
		if selectedVersionID == "" && currentVersionID != nil {
			selectedVersionID = *currentVersionID
		}
		if selectedVersionID == "" && len(d.Versions) > 0 {
			selectedVersionID = d.Versions[0].VersionID
		}
		if selectedVersionID != "" {
			var contentRaw []byte
			if err := tx.QueryRow(r.Context(), `
				SELECT content_json FROM dms_svc.template_version WHERE version_id = $1::uuid`,
				selectedVersionID,
			).Scan(&contentRaw); err == nil {
				d.ContentJSON = json.RawMessage(contentRaw)
			}
			mergeFields, placeholders, err := loadVersionChildren(r.Context(), tx, selectedVersionID)
			if err == nil {
				d.MergeFields = mergeFields
				d.ChartPlaceholders = placeholders
			}
		}

		api.RespondEnvelopeSuccess(w, "Template fetched", d)
	}
}
