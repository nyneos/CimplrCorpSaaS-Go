package bankstatement

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	apipreval "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/api/utils/s3storage"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// stagingFileRecord holds S3 metadata for an original staged upload file.
type stagingFileRecord struct {
	UploadS3Key  string
	ContentHash  string
	ContentType  string
	FileSize     int64
	UploadedBy   string
	UploadedAt   time.Time
}

func (r *stagingFileRecord) hasFile() bool {
	return r != nil && strings.TrimSpace(r.UploadS3Key) != ""
}

// uploadStagingOriginalFile stores the original bytes in S3 (when enabled).
func uploadStagingOriginalFile(ctx context.Context, fileBytes []byte, originalFilename, uploadedBy string) (*stagingFileRecord, error) {
	if len(fileBytes) == 0 {
		return nil, nil
	}
	uploadedBy = strings.TrimSpace(uploadedBy)
	if uploadedBy == "" {
		uploadedBy = "system"
	}
	if !s3storage.IsS3UploadEnabled() {
		logger.LogInfo("[STAGING-S3] upload skipped (S3 disabled) filename=%q", originalFilename)
		return nil, nil
	}

	uploadedAt := time.Now().UTC()
	contentType := s3storage.DetectContentType(fileBytes)
	storedName := s3storage.BuildUploadedFilename(originalFilename, uploadedBy, uploadedAt)
	folder := s3storage.GetStoragePrefix(bankStatementUploadModule)
	s3Key := s3storage.BuildNamedS3Key(folder, "", storedName)

	if err := s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
		return nil, err
	}

	return &stagingFileRecord{
		UploadS3Key: s3Key,
		ContentHash: s3storage.ContentHashHex(fileBytes),
		ContentType: contentType,
		FileSize:    int64(len(fileBytes)),
		UploadedBy:  uploadedBy,
		UploadedAt:  uploadedAt,
	}, nil
}

func applyStagingFileRecordToInsert(p *insertStagingStatementParams, rec *stagingFileRecord) {
	if rec == nil || !rec.hasFile() {
		return
	}
	p.UploadS3Key = rec.UploadS3Key
	p.ContentHash = rec.ContentHash
	p.ContentType = rec.ContentType
	p.FileSize = rec.FileSize
	p.UploadedBy = rec.UploadedBy
	p.UploadedAt = rec.UploadedAt
}

func updateStagingStatementFileStorage(ctx context.Context, pool *pgxpool.Pool, stagingID string, rec *stagingFileRecord) error {
	if stagingID == "" || rec == nil || !rec.hasFile() {
		return nil
	}
	_, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.pdf_staging_statement
		   SET upload_s3_key = $1,
		       content_hash = $2,
		       content_type = $3,
		       file_size = $4,
		       uploaded_by = $5,
		       uploaded_at = $6,
		       updated_at = now()
		 WHERE staging_id = $7
	`, rec.UploadS3Key, rec.ContentHash, rec.ContentType, rec.FileSize, rec.UploadedBy, rec.UploadedAt, stagingID)
	return err
}

// stagingFileJSONFields are included in list/get staging API responses.
func stagingFileJSONFields(uploadS3Key, contentType sql.NullString, fileSize sql.NullInt64, uploadedBy sql.NullString, uploadedAt sql.NullTime) map[string]interface{} {
	hasFile := uploadS3Key.Valid && strings.TrimSpace(uploadS3Key.String) != ""
	out := map[string]interface{}{
		"has_file": hasFile,
	}
	if hasFile {
		out["upload_s3_key"] = uploadS3Key.String
	}
	if contentType.Valid && strings.TrimSpace(contentType.String) != "" {
		out["content_type"] = contentType.String
	}
	if fileSize.Valid && fileSize.Int64 > 0 {
		out["file_size"] = fileSize.Int64
	}
	if uploadedBy.Valid && strings.TrimSpace(uploadedBy.String) != "" {
		out["uploaded_by"] = uploadedBy.String
	}
	if uploadedAt.Valid {
		out["uploaded_at"] = uploadedAt.Time.UTC().Format(time.RFC3339)
	}
	return out
}

func mergeStagingFileJSON(target map[string]interface{}, uploadS3Key, contentType sql.NullString, fileSize sql.NullInt64, uploadedBy sql.NullString, uploadedAt sql.NullTime) {
	for k, v := range stagingFileJSONFields(uploadS3Key, contentType, fileSize, uploadedBy, uploadedAt) {
		target[k] = v
	}
}

func collectStagingS3KeysForDelete(ctx context.Context, pool *pgxpool.Pool, stagingIDs []string) ([]string, error) {
	if len(stagingIDs) == 0 {
		return nil, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT trim(upload_s3_key)
		  FROM cimplrcorpsaas.pdf_staging_statement
		 WHERE staging_id::text = ANY($1::text[])
		   AND committed_bs_id IS NULL
		   AND upload_s3_key IS NOT NULL
		   AND trim(upload_s3_key) <> ''
	`, stagingIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	seen := map[string]struct{}{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func collectStagingS3KeysForBatchDelete(ctx context.Context, pool *pgxpool.Pool, batchID string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT trim(upload_s3_key)
		  FROM cimplrcorpsaas.pdf_staging_statement
		 WHERE batch_id = $1
		   AND committed_bs_id IS NULL
		   AND upload_s3_key IS NOT NULL
		   AND trim(upload_s3_key) <> ''
	`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	seen := map[string]struct{}{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func s3KeyStillReferenced(ctx context.Context, pool *pgxpool.Pool, s3Key string) (bool, error) {
	s3Key = strings.TrimSpace(s3Key)
	if s3Key == "" {
		return false, nil
	}
	var stagingCount, committedCount int
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM cimplrcorpsaas.pdf_staging_statement WHERE upload_s3_key = $1),
			(SELECT COUNT(*) FROM cimplrcorpsaas.bank_statements WHERE upload_s3_key = $1 AND COALESCE(is_deleted, false) = false)
	`, s3Key).Scan(&stagingCount, &committedCount); err != nil {
		return false, err
	}
	return stagingCount > 0 || committedCount > 0, nil
}

func deleteStagingS3KeysBestEffort(ctx context.Context, pool *pgxpool.Pool, keys []string) {
	if !s3storage.IsS3UploadEnabled() {
		return
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		stillUsed, err := s3KeyStillReferenced(ctx, pool, key)
		if err != nil {
			logger.LogError("[STAGING-S3] ref check failed key=%q: %v", key, err)
			continue
		}
		if stillUsed {
			continue
		}
		if err := s3storage.DeleteFromS3(ctx, key); err != nil {
			logger.LogError("[STAGING-S3] delete failed key=%q: %v", key, err)
		}
	}
}

// GetStagingFileDownloadURLHandler returns a presigned URL for the original staged file (DMS preview).
// POST {"staging_id":"..."}
func GetStagingFileDownloadURLHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respondWithError(w, nil, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			StagingID string `json:"staging_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.StagingID) == "" {
			respondWithError(w, nil, "staging_id is required", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		sessionUID := apipreval.GetUserIDFromContext(ctx)
		if sessionUID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var (
			uploadS3Key      sql.NullString
			originalFilename string
			contentType      sql.NullString
			committedID      sql.NullString
		)
		err := pool.QueryRow(ctx, `
			SELECT s.upload_s3_key, s.original_filename, s.content_type, s.committed_bs_id
			  FROM cimplrcorpsaas.pdf_staging_statement s
			  INNER JOIN cimplrcorpsaas.pdf_staging_batch b ON b.batch_id = s.batch_id
			 WHERE s.staging_id = $1 AND b.user_id = $2
		`, body.StagingID, sessionUID).Scan(&uploadS3Key, &originalFilename, &contentType, &committedID)
		if errors.Is(err, pgx.ErrNoRows) {
			respondWithError(w, nil, constants.ErrStatementNotFound, http.StatusNotFound)
			return
		}
		if err != nil {
			respondWithError(w, err, "failed to resolve staging file", http.StatusInternalServerError)
			return
		}
		if !uploadS3Key.Valid || strings.TrimSpace(uploadS3Key.String) == "" {
			respondWithError(w, nil, "no file available for this staging statement", http.StatusNotFound)
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, uploadS3Key.String, 15*time.Minute)
		if err != nil {
			respondWithError(w, err, "Failed to generate download URL", http.StatusInternalServerError)
			return
		}

		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"download_url":      downloadURL,
			"original_filename": originalFilename,
			"content_type":      contentType.String,
			"staging_id":        body.StagingID,
			"committed":         committedID.Valid && strings.TrimSpace(committedID.String) != "",
		})
	})
}
