package templates

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

type createVersionReq struct {
	TemplateID        string                `json:"template_id"`
	ContentJSON       json.RawMessage       `json:"content_json"`
	MergeFields       []mergeFieldReq       `json:"merge_fields"`
	ChartPlaceholders []chartPlaceholderReq `json:"chart_placeholders"`
	Reason            string                `json:"reason"`
	ActorID           string                `json:"actor_id"`
}

// HandleCreateVersion raises a new content version for an existing template.
// Version rows are immutable once approved — an edit always creates a new
// PENDING_APPROVAL version rather than mutating an approved one (version
// control). The previously approved version stays live until this one is
// approved (current_version_id only advances on approve — see approve.go).
func HandleCreateVersion(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req createVersionReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.TemplateID = strings.TrimSpace(req.TemplateID)
		if req.TemplateID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "template_id is required", "VALIDATION_ERROR")
			return
		}
		if len(req.ContentJSON) == 0 {
			req.ContentJSON = json.RawMessage(`{}`)
		}
		actor, ip := requestActorAndIP(r, req.ActorID)

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms template version create begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create template version", "DMS_TEMPLATE_VERSION_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		if err := requirePendingFree(r.Context(), tx, req.TemplateID); err != nil {
			api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_TEMPLATE_PENDING_EXISTS")
			return
		}

		var nextVersionNo int
		if err := tx.QueryRow(r.Context(), `
			SELECT COALESCE(MAX(version_no), 0) + 1 FROM dms_svc.template_version
			WHERE template_id = $1::uuid`, req.TemplateID,
		).Scan(&nextVersionNo); err != nil {
			api.LogErrorForResponse(w, "dms template version seq: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create template version", "DMS_TEMPLATE_VERSION_FAILED")
			return
		}

		var versionID string
		if err := tx.QueryRow(r.Context(), `
			INSERT INTO dms_svc.template_version (template_id, version_no, content_json, source, status, created_by)
			VALUES ($1::uuid, $2, $3::jsonb, 'EDITOR', 'PENDING_APPROVAL', $4)
			RETURNING version_id::text`,
			req.TemplateID, nextVersionNo, string(req.ContentJSON), actor,
		).Scan(&versionID); err != nil {
			api.LogErrorForResponse(w, "dms template version insert: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create template version", "DMS_TEMPLATE_VERSION_FAILED")
			return
		}

		if err := insertVersionChildren(r.Context(), tx, versionID, req.MergeFields, req.ChartPlaceholders); err != nil {
			api.LogErrorForResponse(w, "dms template version children: %v", err)
			api.RespondEnvelopeError(w, http.StatusBadRequest, "failed to attach merge fields / chart placeholders (unknown domain_catalog field?)", "DMS_TEMPLATE_VERSION_FAILED")
			return
		}

		if _, err := tx.Exec(r.Context(), `
			UPDATE dms_svc.template SET processing_status = 'PENDING_EDIT_APPROVAL'
			WHERE template_id = $1::uuid`, req.TemplateID); err != nil {
			api.LogErrorForResponse(w, "dms template version flag: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create template version", "DMS_TEMPLATE_VERSION_FAILED")
			return
		}

		a := &auditRow{}
		a.set("template_id", req.TemplateID)
		a.set("version_id", versionID)
		a.set("action_type", "CREATE_VERSION")
		a.set("processing_status", "PENDING_EDIT_APPROVAL")
		a.set("reason", common.NullIfEmpty(req.Reason))
		a.set("requested_by", actor)
		a.set("requested_ip", common.NullIfEmpty(ip))
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms template version audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit template version", "DMS_TEMPLATE_VERSION_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms template version commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create template version", "DMS_TEMPLATE_VERSION_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Template version submitted for approval", map[string]interface{}{
			"template_id": req.TemplateID,
			"version_id":  versionID,
			"version_no":  nextVersionNo,
		})
	}
}

type listVersionsReq struct {
	TemplateID string `json:"template_id"`
}

// HandleListVersions returns every version of a template, newest first.
func HandleListVersions(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req listVersionsReq
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		req.TemplateID = strings.TrimSpace(req.TemplateID)
		if req.TemplateID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "template_id is required", "VALIDATION_ERROR")
			return
		}

		var currentVersionID *string
		if err := pool.QueryRow(r.Context(), `
			SELECT current_version_id::text FROM dms_svc.template WHERE template_id = $1::uuid`, req.TemplateID,
		).Scan(&currentVersionID); err != nil {
			api.RespondEnvelopeError(w, http.StatusNotFound, "template not found", "NOT_FOUND")
			return
		}

		rows, err := pool.Query(r.Context(), `
			SELECT version_id::text, version_no, status, source, COALESCE(created_by,''),
			       created_at, approved_by, approved_at, source_file_name
			FROM dms_svc.template_version
			WHERE template_id = $1::uuid
			ORDER BY version_no DESC`, req.TemplateID)
		if err != nil {
			api.LogErrorForResponse(w, "dms template versions list: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to list template versions", "DMS_TEMPLATE_VERSIONS_FAILED")
			return
		}
		defer rows.Close()

		out := make([]versionSummary, 0)
		for rows.Next() {
			var v versionSummary
			var createdAt time.Time
			var approvedAt *time.Time
			if err := rows.Scan(&v.VersionID, &v.VersionNo, &v.Status, &v.Source, &v.CreatedBy,
				&createdAt, &v.ApprovedBy, &approvedAt, &v.SourceFile); err != nil {
				continue
			}
			v.CreatedAt = createdAt.UTC().Format(time.RFC3339)
			if approvedAt != nil {
				s := approvedAt.UTC().Format(time.RFC3339)
				v.ApprovedAt = &s
			}
			v.IsCurrent = currentVersionID != nil && *currentVersionID == v.VersionID
			out = append(out, v)
		}
		api.RespondEnvelopeSuccess(w, "Template versions fetched", out)
	}
}
