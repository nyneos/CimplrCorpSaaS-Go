package additionalfiles

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type FileRecord struct {
	FileID         string    `json:"file_id"`
	StoredFileName string    `json:"stored_file_name"`
	ContentType    string    `json:"content_type,omitempty"`
	FileSize       int64     `json:"file_size,omitempty"`
	UploadS3Key    string    `json:"upload_s3_key,omitempty"`
	UploadedBy     string    `json:"uploaded_by,omitempty"`
	UploadedAt     time.Time `json:"uploaded_at"`
}

type CreateInput struct {
	ParentID       string
	StoredFileName string
	ContentType    string
	FileSize       int64
	FileHash       string
	UploadS3Key    string
	UploadedBy     string
	UploadedAt     time.Time
}

type Config struct {
	Module        string
	ParentIDField string
	FolderName    string
	List          func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]FileRecord, error)
	Create        func(ctx context.Context, tx pgx.Tx, input CreateInput) error
	GetOne        func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*FileRecord, error)
	GetMany       func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]FileRecord, []string, error)
	SoftDelete    func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error)
}

type downloadRequest struct {
	UserID string `json:"user_id"`
	FileID string `json:"file_id"`
}

type downloadSelectedRequest struct {
	UserID  string   `json:"user_id"`
	FileIDs []string `json:"file_ids"`
}

func NewListHandler(pool *pgxpool.Pool, cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		body, parentID, err := decodeParentJSON(r, cfg.ParentIDField)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		if strings.TrimSpace(body.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		files, err := cfg.List(r.Context(), pool, parentID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeSuccess(w, map[string]interface{}{"files": files})
	}
}

func NewUploadHandler(pool *pgxpool.Pool, cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		if err := r.ParseMultipartForm(64 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		parentID := strings.TrimSpace(r.FormValue(cfg.ParentIDField))
		if parentID == "" {
			api.RespondWithError(w, http.StatusBadRequest, fmt.Sprintf("%s required", cfg.ParentIDField))
			return
		}

		userID := strings.TrimSpace(r.FormValue("user_id"))
		uploadedBy := strings.TrimSpace(api.RequestedByFromCtx(r.Context(), userID))
		if uploadedBy == "" {
			uploadedBy = userID
		}

		fileHeaders := collectMultipartFiles(r, "file", "files")
		if len(fileHeaders) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "no files provided")
			return
		}

		uploaded := make([]FileRecord, 0, len(fileHeaders))
		for _, header := range fileHeaders {
			record, err := uploadOneFile(r.Context(), pool, cfg, parentID, uploadedBy, header)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, err.Error())
				return
			}
			uploaded = append(uploaded, *record)
		}

		writeSuccess(w, map[string]interface{}{"files": uploaded})
	}
}

func NewDownloadHandler(pool *pgxpool.Pool, cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		body, parentID, err := decodeParentJSON(r, cfg.ParentIDField)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		if strings.TrimSpace(body.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		var req downloadRequest
		if err := json.NewDecoder(strings.NewReader(body.Raw)).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if strings.TrimSpace(req.FileID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "file_id required")
			return
		}

		record, err := cfg.GetOne(r.Context(), pool, parentID, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if record == nil || strings.TrimSpace(record.UploadS3Key) == "" {
			api.RespondWithError(w, http.StatusNotFound, "file not found")
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), record.UploadS3Key, 15*time.Minute)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to generate download url: "+err.Error())
			return
		}

		writeSuccess(w, map[string]interface{}{
			"download_url": downloadURL,
			"file_id":      record.FileID,
		})
	}
}

func NewDownloadSelectedHandler(pool *pgxpool.Pool, cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		body, parentID, err := decodeParentJSON(r, cfg.ParentIDField)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		if strings.TrimSpace(body.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		var req downloadSelectedRequest
		if err := json.NewDecoder(strings.NewReader(body.Raw)).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		fileIDs := trimStringList(req.FileIDs)
		if len(fileIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "file_ids required")
			return
		}

		records, failedIDs, err := cfg.GetMany(r.Context(), pool, parentID, fileIDs)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		files := make([]map[string]string, 0, len(records))
		for _, record := range records {
			if strings.TrimSpace(record.UploadS3Key) == "" {
				failedIDs = append(failedIDs, record.FileID)
				continue
			}

			downloadURL, presignErr := s3storage.GetDownloadPresignedURL(r.Context(), record.UploadS3Key, 15*time.Minute)
			if presignErr != nil {
				failedIDs = append(failedIDs, record.FileID)
				continue
			}
			files = append(files, map[string]string{
				"file_id":      record.FileID,
				"download_url": downloadURL,
			})
		}

		writeSuccess(w, map[string]interface{}{
			"files":      files,
			"failed_ids": uniqueStrings(failedIDs),
		})
	}
}

func NewDeleteHandler(pool *pgxpool.Pool, cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		body, parentID, err := decodeParentJSON(r, cfg.ParentIDField)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		var req downloadRequest
		if err := json.NewDecoder(strings.NewReader(body.Raw)).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if strings.TrimSpace(req.FileID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "file_id required")
			return
		}

		deletedBy := strings.TrimSpace(api.RequestedByFromCtx(r.Context(), req.UserID))
		if deletedBy == "" {
			deletedBy = strings.TrimSpace(req.UserID)
		}

		deleted, err := cfg.SoftDelete(r.Context(), pool, parentID, req.FileID, deletedBy, time.Now().UTC())
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !deleted {
			api.RespondWithError(w, http.StatusNotFound, "file not found")
			return
		}

		api.RespondWithResult(w, true, "")
	}
}

func QueryFiles(ctx context.Context, pool *pgxpool.Pool, query string, args ...interface{}) ([]FileRecord, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files := make([]FileRecord, 0)
	for rows.Next() {
		var record FileRecord
		var contentType sql.NullString
		var fileSize sql.NullInt64
		var uploadS3Key sql.NullString
		var uploadedBy sql.NullString
		var uploadedAt sql.NullTime

		if err := rows.Scan(
			&record.FileID,
			&record.StoredFileName,
			&contentType,
			&fileSize,
			&uploadS3Key,
			&uploadedBy,
			&uploadedAt,
		); err != nil {
			return nil, err
		}

		record.ContentType = strings.TrimSpace(contentType.String)
		record.FileSize = fileSize.Int64
		record.UploadS3Key = strings.TrimSpace(uploadS3Key.String)
		record.UploadedBy = strings.TrimSpace(uploadedBy.String)
		if uploadedAt.Valid {
			record.UploadedAt = uploadedAt.Time
		}
		files = append(files, record)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return files, nil
}

func FirstFile(ctx context.Context, pool *pgxpool.Pool, query string, args ...interface{}) (*FileRecord, error) {
	files, err := QueryFiles(ctx, pool, query, args...)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	return &files[0], nil
}

func AdditionalFilesFolder() string {
	return "additional files"
}

func BuildAdditionalFilesS3Key(module, storedFileName string, folderNames ...string) string {
	additionalFolder := AdditionalFilesFolder()
	if len(folderNames) > 0 && strings.TrimSpace(folderNames[0]) != "" {
		additionalFolder = strings.TrimSpace(folderNames[0])
	}

	folder := strings.Trim(strings.TrimSpace(s3storage.GetStoragePrefix(module)), "/")
	if folder == "" {
		folder = additionalFolder
	} else {
		folder = folder + "/" + additionalFolder
	}
	return s3storage.BuildNamedS3Key(folder, "", storedFileName)
}

func InsertAdditionalFileRow(ctx context.Context, tx pgx.Tx, tableName, parentColumn string, input CreateInput, parentScopeQuery string, parentScopeArgs ...interface{}) error {
	if strings.TrimSpace(parentScopeQuery) == "" {
		return errors.New("parent scope query required")
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			%s,
			stored_file_name,
			content_type,
			file_size,
			file_hash,
			upload_s3_key,
			uploaded_by,
			uploaded_at,
			is_deleted
		)
		SELECT
			parent_id,
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			FALSE
		FROM (
			%s
		) scoped_parent(parent_id)
	`, tableName, parentColumn, parentScopeQuery)

	args := []interface{}{
		input.StoredFileName,
		input.ContentType,
		input.FileSize,
		input.FileHash,
		input.UploadS3Key,
		input.UploadedBy,
		input.UploadedAt,
	}
	args = append(args, parentScopeArgs...)

	result, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return errors.New("parent record not found or access denied")
	}
	return nil
}

func collectMultipartFiles(r *http.Request, keys ...string) []*multipart.FileHeader {
	if r.MultipartForm == nil {
		return nil
	}
	files := make([]*multipart.FileHeader, 0)
	for _, key := range keys {
		files = append(files, r.MultipartForm.File[key]...)
	}
	return files
}

func uploadOneFile(ctx context.Context, pool *pgxpool.Pool, cfg Config, parentID, uploadedBy string, header *multipart.FileHeader) (*FileRecord, error) {
	file, err := header.Open()
	if err != nil {
		return nil, fmt.Errorf("open file %s: %w", header.Filename, err)
	}
	defer file.Close()

	body, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", header.Filename, err)
	}

	uploadedAt := time.Now().UTC()
	contentType := s3storage.DetectContentType(body)
	storedFileName := s3storage.BuildUploadedFilename(header.Filename, uploadedBy, uploadedAt)
	s3Key := BuildAdditionalFilesS3Key(cfg.Module, storedFileName, cfg.FolderName)
	fileHash := s3storage.ContentHashHex(body)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if err := s3storage.PutObjectToS3(ctx, s3Key, body, contentType); err != nil {
		return nil, err
	}

	input := CreateInput{
		ParentID:       parentID,
		StoredFileName: storedFileName,
		ContentType:    contentType,
		FileSize:       header.Size,
		FileHash:       fileHash,
		UploadS3Key:    s3Key,
		UploadedBy:     uploadedBy,
		UploadedAt:     uploadedAt,
	}
	if err := cfg.Create(ctx, tx, input); err != nil {
		_ = s3storage.DeleteFromS3(ctx, s3Key)
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		_ = s3storage.DeleteFromS3(ctx, s3Key)
		return nil, err
	}

	return &FileRecord{
		StoredFileName: input.StoredFileName,
		ContentType:    input.ContentType,
		FileSize:       input.FileSize,
		UploadS3Key:    input.UploadS3Key,
		UploadedBy:     input.UploadedBy,
		UploadedAt:     input.UploadedAt,
	}, nil
}

type decodedParentBody struct {
	UserID string
	Raw    string
}

func decodeParentJSON(r *http.Request, parentField string) (decodedParentBody, string, error) {
	var payload map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		return decodedParentBody{}, "", fmt.Errorf("%s: %w", constants.ErrInvalidJSONShort, err)
	}

	userID := strings.TrimSpace(toString(payload["user_id"]))
	parentID := strings.TrimSpace(toString(payload[parentField]))
	raw, _ := json.Marshal(payload)
	if parentID == "" {
		return decodedParentBody{}, "", fmt.Errorf("%s required", parentField)
	}

	return decodedParentBody{UserID: userID, Raw: string(raw)}, parentID, nil
}

func toString(value interface{}) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprint(value)
	}
}

func trimStringList(values []string) []string {
	trimmed := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		candidate := strings.TrimSpace(value)
		if candidate == "" {
			continue
		}
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		trimmed = append(trimmed, candidate)
	}
	return trimmed
}

func uniqueStrings(values []string) []string {
	return trimStringList(values)
}

func writeSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: true,
		"data":                 data,
	})
}
