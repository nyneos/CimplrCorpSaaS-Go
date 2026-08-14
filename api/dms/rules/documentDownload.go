package rules

import (
	"encoding/base64"
	"net/http"
	"os"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	dmsjobs "CimplrCorpSaas/internal/jobs/dms"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleDocumentDownload returns a download URL for a generated document
// (presigned S3, or a data URL for local-only artifacts).
func HandleDocumentDownload(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !common.RequirePOST(w, r) {
			return
		}
		var req struct {
			DocID   string `json:"doc_id"`
			Preview bool   `json:"preview"`
		}
		if err := common.DecodeJSON(r, &req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
			return
		}
		docID := strings.TrimSpace(req.DocID)
		if docID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "doc_id is required", "BAD_REQUEST")
			return
		}

		var s3Key, fileFormat, outputFilename, localPath, storageBackend string
		err := pool.QueryRow(r.Context(), `
			SELECT s3_key, file_format, COALESCE(output_filename, ''), COALESCE(local_path, ''), storage_backend
			FROM dms_svc.generated_document
			WHERE doc_id = $1::uuid`, docID,
		).Scan(&s3Key, &fileFormat, &outputFilename, &localPath, &storageBackend)
		if err != nil {
			api.LogErrorForResponse(w, "dms document download: %v", err)
			api.RespondEnvelopeError(w, http.StatusNotFound, "document not found", "DMS_DOCUMENT_NOT_FOUND")
			return
		}
		s3Key = strings.TrimSpace(s3Key)
		localPath = strings.TrimSpace(localPath)
		if s3Key == "" && localPath == "" {
			api.RespondEnvelopeError(w, http.StatusNotFound, "document has no storage key", "DMS_DOCUMENT_NO_S3")
			return
		}

		fileName := strings.TrimSpace(outputFilename)
		if fileName == "" {
			fileName = fileNameFromKey(s3Key, fileFormat)
		}

		if dmsjobs.IsLocalStorageKey(s3Key) {
			pathKey := s3Key
			if localPath != "" {
				pathKey = localPath
			}
			abs, resolveErr := dmsjobs.ResolveLocalStoragePath(pathKey)
			if resolveErr != nil {
				api.LogErrorForResponse(w, "dms local resolve: %v", resolveErr)
				api.RespondEnvelopeError(w, http.StatusNotFound, "local document not found", "DMS_DOCUMENT_LOCAL_MISSING")
				return
			}
			body, readErr := os.ReadFile(abs)
			if readErr != nil {
				api.LogErrorForResponse(w, "dms local read: %v", readErr)
				api.RespondEnvelopeError(w, http.StatusNotFound, "local document not found", "DMS_DOCUMENT_LOCAL_MISSING")
				return
			}
			mime := mimeForFormat(fileFormat)
			dataURL := "data:" + mime + ";base64," + base64.StdEncoding.EncodeToString(body)
			api.RespondEnvelopeSuccess(w, "Document download URL ready", map[string]interface{}{
				"download_url": dataURL,
				"file_format":  fileFormat,
				"s3_key":       s3Key,
				"local_path":   localPath,
				"file_name":    fileName,
			})
			return
		}

		var signedURL string
		if storageBackend == "DOCSVC_S3" {
			signedURL, err = dmsjobs.DownloadURLViaDocSvc(r.Context(), s3Key, mimeForFormat(fileFormat), req.Preview)
		} else if req.Preview {
			signedURL, err = s3storage.GetInlinePresignedURL(r.Context(), s3Key, 15*time.Minute)
		} else {
			signedURL, err = s3storage.GetDownloadPresignedURL(r.Context(), s3Key, 15*time.Minute)
		}
		if err != nil {
			api.LogErrorForResponse(w, "dms document presign: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create download link", "DMS_DOCUMENT_PRESIGN_FAILED")
			return
		}

		api.RespondEnvelopeSuccess(w, "Document download URL ready", map[string]interface{}{
			"download_url": signedURL,
			"file_format":  fileFormat,
			"s3_key":       s3Key,
			"local_path":   localPath,
			"file_name":    fileName,
		})
	}
}

func fileNameFromKey(s3Key, format string) string {
	base := s3Key
	if i := strings.LastIndex(s3Key, "/"); i >= 0 && i+1 < len(s3Key) {
		base = s3Key[i+1:]
	}
	if base != "" && strings.Contains(base, ".") {
		return base
	}
	ext := strings.ToLower(strings.TrimSpace(format))
	if ext == "" {
		ext = "bin"
	}
	return "document." + ext
}

func mimeForFormat(format string) string {
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case "PDF":
		return "application/pdf"
	case "DOCX":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case "XLSX":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case "HTML":
		return "text/html"
	case "EML":
		return "message/rfc822"
	default:
		return "application/octet-stream"
	}
}
