package rules

import (
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dms/common"
	s3storage "CimplrCorpSaas/api/utils/s3storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HandleDocumentDownload returns a presigned S3 URL for a generated document
// (same shape as /email/attachments/download — preview=true → inline disposition).
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

		var s3Key, fileFormat, outputFilename string
		err := pool.QueryRow(r.Context(), `
			SELECT s3_key, file_format, COALESCE(output_filename, '')
			FROM dms_svc.generated_document
			WHERE doc_id = $1::uuid`, docID,
		).Scan(&s3Key, &fileFormat, &outputFilename)
		if err != nil {
			api.LogErrorForResponse(w, "dms document download: %v", err)
			api.RespondEnvelopeError(w, http.StatusNotFound, "document not found", "DMS_DOCUMENT_NOT_FOUND")
			return
		}
		s3Key = strings.TrimSpace(s3Key)
		if s3Key == "" {
			api.RespondEnvelopeError(w, http.StatusNotFound, "document has no S3 key", "DMS_DOCUMENT_NO_S3")
			return
		}

		var signedURL string
		if req.Preview {
			signedURL, err = s3storage.GetInlinePresignedURL(r.Context(), s3Key, 15*time.Minute)
		} else {
			signedURL, err = s3storage.GetDownloadPresignedURL(r.Context(), s3Key, 15*time.Minute)
		}
		if err != nil {
			api.LogErrorForResponse(w, "dms document presign: %v", err)
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to create download link", "DMS_DOCUMENT_PRESIGN_FAILED")
			return
		}

		fileName := strings.TrimSpace(outputFilename)
		if fileName == "" {
			fileName = fileNameFromKey(s3Key, fileFormat)
		}

		api.RespondEnvelopeSuccess(w, "Document download URL ready", map[string]interface{}{
			"download_url": signedURL,
			"file_format":  fileFormat,
			"s3_key":       s3Key,
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
