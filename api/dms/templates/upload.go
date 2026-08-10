package templates

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	s3storage "CimplrCorpSaas/api/utils/s3storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxTemplateUploadBytes = 25 << 20 // 25 MB

const errFailedUploadTemplate = "failed to upload template"

// HandleUpload registers an externally authored file (e.g. .docx) as a
// template version. content_json stays the '{}' default for these rows —
// there is no Tiptap body to parse, so the version is upload-only and not
// editable in the studio (a stated limitation, not a silent gap). If
// template_id is blank, a new template is created first (name/module_code/
// sub_module_code required in that case); if it's set, this becomes a new
// pending version on that existing template.
func HandleUpload(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		if err := r.ParseMultipartForm(maxTemplateUploadBytes); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid multipart form", "BAD_REQUEST")
			return
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "file is required", "VALIDATION_ERROR")
			return
		}
		defer file.Close()

		body, err := io.ReadAll(io.LimitReader(file, maxTemplateUploadBytes+1))
		if err != nil {
			api.LogErrorForResponse(w, "dms template upload read: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to read uploaded file", "DMS_TEMPLATE_UPLOAD_FAILED")
			return
		}
		if len(body) > maxTemplateUploadBytes {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "file too large (max 25MB)", "VALIDATION_ERROR")
			return
		}
		safeName := filepath.Base(strings.TrimSpace(header.Filename))
		if safeName == "" || safeName == "." {
			safeName = "template-upload"
		}
		ext := filepath.Ext(safeName)
		contentType := header.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		templateID := strings.TrimSpace(r.FormValue("template_id"))
		actor, ip := requestActorAndIP(r, r.FormValue("actor_id"))

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.LogErrorForResponse(w, "dms template upload begin: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedUploadTemplate, "DMS_TEMPLATE_UPLOAD_FAILED")
			return
		}
		defer tx.Rollback(r.Context())

		auditActionType := "CREATE_VERSION"
		if templateID == "" {
			name := strings.TrimSpace(r.FormValue("name"))
			moduleCode := strings.TrimSpace(r.FormValue("module_code"))
			subModuleCode := strings.TrimSpace(r.FormValue("sub_module_code"))
			if name == "" || moduleCode == "" || subModuleCode == "" {
				api.RespondEnvelopeError(w, http.StatusBadRequest, "name, module_code and sub_module_code are required when template_id is not set", "VALIDATION_ERROR")
				return
			}
			if err := tx.QueryRow(r.Context(), `
				INSERT INTO dms_svc.template (name, description, module_code, sub_module_code, status, processing_status, created_by)
				VALUES ($1, $2, $3, $4, 'PendingApproval', 'PENDING_APPROVAL', $5)
				RETURNING template_id::text`,
				name, r.FormValue("description"), moduleCode, subModuleCode, actor,
			).Scan(&templateID); err != nil {
				api.LogErrorForResponse(w, "dms template upload create: %v", err)
				api.RespondEnvelopeError(w, http.StatusConflict, "failed to create template (duplicate name, or unknown module/sub_module?)", "DMS_TEMPLATE_UPLOAD_FAILED")
				return
			}
			auditActionType = "CREATE"
		} else {
			if err := requirePendingFree(r.Context(), tx, templateID); err != nil {
				api.RespondEnvelopeError(w, http.StatusConflict, err.Error(), "DMS_TEMPLATE_PENDING_EXISTS")
				return
			}
		}

		sum := sha256.Sum256(body)
		s3Key := s3storage.BuildModuleS3Key("dms", "templates", hex.EncodeToString(sum[:]), ext)
		if err := s3storage.PutObjectToS3(r.Context(), s3Key, body, contentType); err != nil {
			api.LogErrorForResponse(w, "dms template upload s3: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to store uploaded file", "DMS_TEMPLATE_UPLOAD_FAILED")
			return
		}

		var nextVersionNo int
		if err := tx.QueryRow(r.Context(), `
			SELECT COALESCE(MAX(version_no), 0) + 1 FROM dms_svc.template_version
			WHERE template_id = $1::uuid`, templateID,
		).Scan(&nextVersionNo); err != nil {
			api.LogErrorForResponse(w, "dms template upload seq: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedUploadTemplate, "DMS_TEMPLATE_UPLOAD_FAILED")
			return
		}

		var versionID string
		if err := tx.QueryRow(r.Context(), `
			INSERT INTO dms_svc.template_version
				(template_id, version_no, content_json, source, status, created_by,
				 source_file_s3_key, source_file_name, source_file_mime_type)
			VALUES ($1::uuid, $2, '{}'::jsonb, 'UPLOAD', 'PENDING_APPROVAL', $3, $4, $5, $6)
			RETURNING version_id::text`,
			templateID, nextVersionNo, actor, s3Key, safeName, contentType,
		).Scan(&versionID); err != nil {
			api.LogErrorForResponse(w, "dms template upload version: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedUploadTemplate, "DMS_TEMPLATE_UPLOAD_FAILED")
			return
		}

		if _, err := tx.Exec(r.Context(), `
			UPDATE dms_svc.template SET processing_status = 'PENDING_EDIT_APPROVAL'
			WHERE template_id = $1::uuid AND processing_status <> 'PENDING_APPROVAL'`, templateID); err != nil {
			api.LogErrorForResponse(w, "dms template upload flag: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedUploadTemplate, "DMS_TEMPLATE_UPLOAD_FAILED")
			return
		}

		a := &auditRow{}
		a.set("template_id", templateID)
		a.set("version_id", versionID)
		a.set("action_type", auditActionType)
		if auditActionType == "CREATE" {
			a.set("processing_status", "PENDING_APPROVAL")
			a.set("new_status", "PendingApproval")
			a.set("new_is_deleted", false)
		} else {
			a.set("processing_status", "PENDING_EDIT_APPROVAL")
		}
		a.set("requested_by", actor)
		a.set("requested_ip", common.NullIfEmpty(ip))
		if err := a.exec(r.Context(), tx); err != nil {
			api.LogErrorForResponse(w, "dms template upload audit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to audit template upload", "DMS_TEMPLATE_UPLOAD_FAILED")
			return
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.LogErrorForResponse(w, "dms template upload commit: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, errFailedUploadTemplate, "DMS_TEMPLATE_UPLOAD_FAILED")
			return
		}
		api.RespondEnvelopeSuccess(w, "Template file uploaded and submitted for approval", map[string]interface{}{
			"template_id": templateID,
			"version_id":  versionID,
			"version_no":  nextVersionNo,
		})
	}
}
