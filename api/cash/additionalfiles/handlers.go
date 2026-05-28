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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	fileAuditCreateAction   = "CREATE"
	fileAuditDownloadAction = "DOWNLOAD"
	fileAuditDeleteAction   = "DELETE"

	fileAuditCompletedStatus       = "COMPLETED"
	fileAuditApprovedStatus        = constants.StatusApproved
	fileAuditRejectedStatus        = constants.StatusRejected
	fileAuditPendingDeleteApproval = constants.StatusPendingDeleteApproval
	fileAuditActiveStatus          = constants.StatusActive
	defaultFileAuditTableName      = "cimplrcorpsaas.cash_additional_file_audit"
)

var ErrFileAlreadyUploaded = errors.New("file already uploaded")

type FileRecord struct {
	FileID            string     `json:"file_id"`
	StoredFileName    string     `json:"stored_file_name"`
	ContentType       string     `json:"content_type,omitempty"`
	FileSize          int64      `json:"file_size,omitempty"`
	UploadS3Key       string     `json:"upload_s3_key,omitempty"`
	UploadedBy        string     `json:"uploaded_by,omitempty"`
	UploadedAt        time.Time  `json:"uploaded_at"`
	ProcessingStatus  string     `json:"processing_status,omitempty"`
	DeleteRequestedBy string     `json:"delete_requested_by,omitempty"`
	DeleteRequestedAt *time.Time `json:"delete_requested_at,omitempty"`
	CheckerBy         string     `json:"checker_by,omitempty"`
	CheckerAt         *time.Time `json:"checker_at,omitempty"`
	CheckerComment    string     `json:"checker_comment,omitempty"`
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

type MainUploadAuditPayload struct {
	FileID     string    `json:"file_id,omitempty"`
	FileName   string    `json:"file_name"`
	UploadedBy string    `json:"uploaded_by"`
	UploadedAt time.Time `json:"uploaded_at"`
}

type Config struct {
	Module                string
	AuditSource           string
	AuditTableName        string
	ParentIDField         string
	FolderName            string
	List                  func(ctx context.Context, pool *pgxpool.Pool, parentID string) ([]FileRecord, error)
	Create                func(ctx context.Context, tx pgx.Tx, input CreateInput) error
	CreateReturning       func(ctx context.Context, tx pgx.Tx, input CreateInput) (string, error)
	GetOne                func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*FileRecord, error)
	GetAnyFile            func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID string) (*FileRecord, error)
	GetMany               func(ctx context.Context, pool *pgxpool.Pool, parentID string, fileIDs []string) ([]FileRecord, []string, error)
	DuplicateExists       func(ctx context.Context, pool *pgxpool.Pool, parentID, fileHash string) (bool, error)
	SoftDelete            func(ctx context.Context, pool *pgxpool.Pool, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error)
	SoftDeleteTx          func(ctx context.Context, tx pgx.Tx, parentID, fileID, deletedBy string, deletedAt time.Time) (bool, error)
	RecordMainUploadAudit func(ctx context.Context, tx pgx.Tx, parentID string, payload MainUploadAuditPayload) error
}

type downloadRequest struct {
	UserID string `json:"user_id"`
	FileID string `json:"file_id"`
}

type downloadSelectedRequest struct {
	UserID  string   `json:"user_id"`
	FileIDs []string `json:"file_ids"`
}

type deleteRequest struct {
	UserID string `json:"user_id"`
	FileID string `json:"file_id"`
	Reason string `json:"reason"`
}

type deleteDecisionRequest struct {
	UserID  string `json:"user_id"`
	FileID  string `json:"file_id"`
	Comment string `json:"comment"`
}

type fileAuditEvent struct {
	AuditID          int64      `json:"-"`
	EntityID         string     `json:"entity_id"`
	ModuleKey        string     `json:"-"`
	ParentRecordID   string     `json:"-"`
	FileID           string     `json:"-"`
	ActionType       string     `json:"action_type"`
	ProcessingStatus string     `json:"processing_status"`
	RequestedBy      string     `json:"requested_by,omitempty"`
	RequestedAt      *time.Time `json:"requested_at,omitempty"`
	CheckerBy        string     `json:"checker_by,omitempty"`
	CheckerAt        *time.Time `json:"checker_at,omitempty"`
	CheckerComment   string     `json:"checker_comment,omitempty"`
	Reason           string     `json:"reason,omitempty"`
}

type pendingDeleteState struct {
	FileID           string
	RequestedBy      string
	RequestedAt      *time.Time
	CheckerBy        string
	CheckerAt        *time.Time
	CheckerComment   string
	ProcessingStatus string
}

type auditExecutor interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		files, err := cfg.List(r.Context(), pool, parentID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if auditEnabled(cfg) {
			files, err = enrichFilesWithAudit(r.Context(), pool, cfg, parentID, files)
			if err != nil {
				if !isUndefinedTableError(err) {
					api.RespondWithError(w, http.StatusInternalServerError, err.Error())
					return
				}
			}
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
				if errors.Is(err, ErrFileAlreadyUploaded) {
					api.RespondWithError(w, http.StatusConflict, "This file was already uploaded earlier. Please upload a different file.")
					return
				}
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		var req downloadRequest
		if err := json.NewDecoder(strings.NewReader(body.Raw)).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		var record *FileRecord
		if strings.TrimSpace(req.FileID) == "" {
			if cfg.List == nil {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrFileIDRequired)
				return
			}
			records, err := cfg.List(r.Context(), pool, parentID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			for i := range records {
				if strings.TrimSpace(records[i].UploadS3Key) != "" {
					record = &records[i]
					break
				}
			}
		} else {
			var err error
			record, err = cfg.GetOne(r.Context(), pool, parentID, req.FileID)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		if record == nil || strings.TrimSpace(record.UploadS3Key) == "" {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), record.UploadS3Key, 15*time.Minute)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to generate download url: "+err.Error())
			return
		}

		if auditEnabled(cfg) {
			performedBy := requestedByOrFallback(r.Context(), req.UserID)
			if err := recordDownloadAudit(r.Context(), pool, cfg, parentID, *record, performedBy); err != nil {
				if !isUndefinedTableError(err) {
					api.RespondWithError(w, http.StatusInternalServerError, err.Error())
					return
				}
			}
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
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
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
		performedBy := requestedByOrFallback(r.Context(), req.UserID)
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
			if auditEnabled(cfg) {
				if auditErr := recordDownloadAudit(r.Context(), pool, cfg, parentID, record, performedBy); auditErr != nil {
					if isUndefinedTableError(auditErr) {
						files = append(files, map[string]string{
							"file_id":      record.FileID,
							"download_url": downloadURL,
						})
						continue
					}
					failedIDs = append(failedIDs, record.FileID)
					continue
				}
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

		if !auditEnabled(cfg) {
			deleteImmediate(w, r, pool, cfg, parentID, body.Raw)
			return
		}

		var req deleteRequest
		if err := json.NewDecoder(strings.NewReader(body.Raw)).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		if strings.TrimSpace(req.FileID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFileIDRequired)
			return
		}
		if strings.TrimSpace(req.Reason) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "reason required")
			return
		}

		record, err := cfg.GetOne(r.Context(), pool, parentID, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if record == nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
			return
		}

		pending, err := latestPendingDelete(r.Context(), pool, cfg, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if pending != nil {
			api.RespondWithError(w, http.StatusConflict, "delete approval is already pending for this file")
			return
		}

		requestedBy := requestedByOrFallback(r.Context(), req.UserID)
		if err := recordDeleteRequestAudit(r.Context(), pool, cfg, parentID, *record, requestedBy, strings.TrimSpace(req.Reason)); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeSuccess(w, map[string]interface{}{"message": "Delete submitted for approval."})
	}
}

func NewApproveDeleteHandler(pool *pgxpool.Pool, cfg Config) http.HandlerFunc {
	return newDeleteDecisionHandler(pool, cfg, fileAuditApprovedStatus)
}

func NewRejectDeleteHandler(pool *pgxpool.Pool, cfg Config) http.HandlerFunc {
	return newDeleteDecisionHandler(pool, cfg, fileAuditRejectedStatus)
}

func NewAuditHandler(pool *pgxpool.Pool, cfg Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}
		if !auditEnabled(cfg) {
			api.RespondWithError(w, http.StatusNotFound, "audit not configured for this module")
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
		if strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		if strings.TrimSpace(req.FileID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFileIDRequired)
			return
		}

		fileLookup := cfg.GetAnyFile
		if fileLookup == nil {
			fileLookup = cfg.GetOne
		}
		if fileLookup == nil {
			api.RespondWithError(w, http.StatusInternalServerError, "file lookup not configured")
			return
		}

		record, err := fileLookup(r.Context(), pool, parentID, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if record == nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
			return
		}

		events, err := listAuditEvents(r.Context(), pool, cfg, parentID, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		writeAuditSuccess(w, events)
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
	_, err := InsertAdditionalFileRowReturningID(ctx, tx, tableName, parentColumn, input, parentScopeQuery, parentScopeArgs...)
	return err
}

func InsertAdditionalFileRowReturningID(ctx context.Context, tx pgx.Tx, tableName, parentColumn string, input CreateInput, parentScopeQuery string, parentScopeArgs ...interface{}) (string, error) {
	if strings.TrimSpace(parentScopeQuery) == "" {
		return "", errors.New("parent scope query required")
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
		RETURNING file_id
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

	var fileID string
	if err := tx.QueryRow(ctx, query, args...).Scan(&fileID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", errors.New("parent record not found or access denied")
		}
		return "", err
	}
	return strings.TrimSpace(fileID), nil
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

	if cfg.DuplicateExists != nil {
		exists, err := cfg.DuplicateExists(ctx, pool, parentID, fileHash)
		if err != nil {
			return nil, fmt.Errorf("failed to check duplicate upload: %w", err)
		}
		if exists {
			return nil, ErrFileAlreadyUploaded
		}
	}

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

	var fileID string
	switch {
	case cfg.CreateReturning != nil:
		fileID, err = cfg.CreateReturning(ctx, tx, input)
	case cfg.Create != nil:
		err = cfg.Create(ctx, tx, input)
	default:
		err = errors.New("create handler not configured")
	}
	if err != nil {
		_ = s3storage.DeleteFromS3(ctx, s3Key)
		return nil, err
	}

	if auditEnabled(cfg) && strings.TrimSpace(fileID) != "" {
		if err := recordCreateAuditTx(ctx, tx, cfg, parentID, fileID, input); err != nil {
			if !isUndefinedTableError(err) {
				_ = s3storage.DeleteFromS3(ctx, s3Key)
				return nil, err
			}
		}

		if cfg.RecordMainUploadAudit != nil {
			err := recordMainUploadAuditSafely(ctx, tx, cfg, parentID, MainUploadAuditPayload{
				FileID:     strings.TrimSpace(fileID),
				FileName:   strings.TrimSpace(header.Filename),
				UploadedBy: input.UploadedBy,
				UploadedAt: input.UploadedAt,
			})
			if err != nil {
				_ = s3storage.DeleteFromS3(ctx, s3Key)
				return nil, err
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		_ = s3storage.DeleteFromS3(ctx, s3Key)
		return nil, err
	}

	return &FileRecord{
		FileID:           strings.TrimSpace(fileID),
		StoredFileName:   input.StoredFileName,
		ContentType:      input.ContentType,
		FileSize:         input.FileSize,
		UploadS3Key:      input.UploadS3Key,
		UploadedBy:       input.UploadedBy,
		UploadedAt:       input.UploadedAt,
		ProcessingStatus: fileAuditActiveStatus,
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

func newDeleteDecisionHandler(pool *pgxpool.Pool, cfg Config, nextStatus string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}
		if !auditEnabled(cfg) {
			api.RespondWithError(w, http.StatusNotFound, "delete workflow not configured for this module")
			return
		}

		body, parentID, err := decodeParentJSON(r, cfg.ParentIDField)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		var req deleteDecisionRequest
		if err := json.NewDecoder(strings.NewReader(body.Raw)).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if strings.TrimSpace(req.UserID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		if strings.TrimSpace(req.FileID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFileIDRequired)
			return
		}

		fileLookup := cfg.GetAnyFile
		if fileLookup == nil {
			fileLookup = cfg.GetOne
		}
		if fileLookup == nil {
			api.RespondWithError(w, http.StatusInternalServerError, "file lookup not configured")
			return
		}

		record, err := fileLookup(r.Context(), pool, parentID, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if record == nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
			return
		}

		tx, err := pool.Begin(r.Context())
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer tx.Rollback(r.Context())

		auditRow, err := latestPendingDeleteForUpdate(r.Context(), tx, cfg, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if auditRow == nil {
			api.RespondWithError(w, http.StatusNotFound, "pending delete request not found")
			return
		}

		checkerBy := requestedByOrFallback(r.Context(), req.UserID)
		checkerAt := time.Now().UTC()
		if err := updateDeleteAuditDecision(r.Context(), tx, cfg, deleteAuditDecisionParams{AuditID: auditRow.AuditID, NextStatus: nextStatus, CheckerBy: checkerBy, CheckerAt: checkerAt, CheckerComment: strings.TrimSpace(req.Comment)}); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		if nextStatus == fileAuditApprovedStatus {
			if cfg.SoftDeleteTx == nil {
				api.RespondWithError(w, http.StatusInternalServerError, "transactional delete handler not configured")
				return
			}
			deleted, deleteErr := cfg.SoftDeleteTx(r.Context(), tx, parentID, req.FileID, checkerBy, checkerAt)
			if deleteErr != nil {
				api.RespondWithError(w, http.StatusInternalServerError, deleteErr.Error())
				return
			}
			if !deleted {
				api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
				return
			}
		}

		if err := tx.Commit(r.Context()); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		message := "Delete rejected."
		if nextStatus == fileAuditApprovedStatus {
			message = "Delete approved."
		}
		writeSuccess(w, map[string]interface{}{"message": message})
	}
}

func deleteImmediate(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, cfg Config, parentID, rawBody string) {
	var req downloadRequest
	if err := json.NewDecoder(strings.NewReader(rawBody)).Decode(&req); err != nil {
		api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
		return
	}
	if strings.TrimSpace(req.FileID) == "" {
		api.RespondWithError(w, http.StatusBadRequest, constants.ErrFileIDRequired)
		return
	}
	if cfg.SoftDelete == nil {
		api.RespondWithError(w, http.StatusInternalServerError, "delete handler not configured")
		return
	}

	deletedBy := requestedByOrFallback(r.Context(), req.UserID)
	deleted, err := cfg.SoftDelete(r.Context(), pool, parentID, req.FileID, deletedBy, time.Now().UTC())
	if err != nil {
		api.RespondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !deleted {
		api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
		return
	}

	api.RespondWithResult(w, true, "")
}

func recordMainUploadAuditSafely(ctx context.Context, tx pgx.Tx, cfg Config, parentID string, payload MainUploadAuditPayload) error {
	if cfg.RecordMainUploadAudit == nil {
		return nil
	}

	savepoint, err := tx.Begin(ctx)
	if err != nil {
		return err
	}

	if err := cfg.RecordMainUploadAudit(ctx, savepoint, parentID, payload); err != nil {
		_ = savepoint.Rollback(ctx)
		if isUndefinedTableError(err) || isCheckViolationError(err) {
			return nil
		}
		return err
	}

	return savepoint.Commit(ctx)
}

func auditEnabled(cfg Config) bool {
	return strings.TrimSpace(cfg.AuditSource) != ""
}

func auditTableName(cfg Config) string {
	if tableName := strings.TrimSpace(cfg.AuditTableName); tableName != "" {
		return tableName
	}
	return defaultFileAuditTableName
}

func enrichFilesWithAudit(ctx context.Context, pool *pgxpool.Pool, cfg Config, parentID string, files []FileRecord) ([]FileRecord, error) {
	if len(files) == 0 {
		return files, nil
	}

	fileIDs := make([]string, 0, len(files))
	for i := range files {
		files[i].ProcessingStatus = fileAuditActiveStatus
		fileIDs = append(fileIDs, files[i].FileID)
	}

	query := `
		SELECT DISTINCT ON (file_id)
			file_id,
			requested_by,
			requested_at,
			checker_by,
			checker_at,
			checker_comment,
			processing_status
		FROM ` + auditTableName(cfg) + `
		WHERE module_key = $1
		  AND parent_record_id = $2
		  AND file_id = ANY($3)
		  AND action_type = $4
		  AND processing_status = ANY($5)
		ORDER BY file_id, requested_at DESC, audit_id DESC
	`
	rows, err := pool.Query(
		ctx,
		query,
		cfg.Module,
		parentID,
		fileIDs,
		fileAuditDeleteAction,
		[]string{fileAuditPendingDeleteApproval, fileAuditRejectedStatus},
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := make(map[string]pendingDeleteState, len(files))
	for rows.Next() {
		var state pendingDeleteState
		var requestedBy sql.NullString
		var requestedAt sql.NullTime
		var checkerBy sql.NullString
		var checkerAt sql.NullTime
		var checkerComment sql.NullString
		var processingStatus sql.NullString

		if err := rows.Scan(
			&state.FileID,
			&requestedBy,
			&requestedAt,
			&checkerBy,
			&checkerAt,
			&checkerComment,
			&processingStatus,
		); err != nil {
			return nil, err
		}

		state.RequestedBy = strings.TrimSpace(requestedBy.String)
		if requestedAt.Valid {
			t := requestedAt.Time
			state.RequestedAt = &t
		}
		state.CheckerBy = strings.TrimSpace(checkerBy.String)
		if checkerAt.Valid {
			t := checkerAt.Time
			state.CheckerAt = &t
		}
		state.CheckerComment = strings.TrimSpace(checkerComment.String)
		state.ProcessingStatus = strings.TrimSpace(processingStatus.String)
		states[state.FileID] = state
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range files {
		if state, ok := states[files[i].FileID]; ok {
			files[i].ProcessingStatus = defaultString(state.ProcessingStatus, fileAuditPendingDeleteApproval)
			files[i].DeleteRequestedBy = state.RequestedBy
			files[i].DeleteRequestedAt = state.RequestedAt
			files[i].CheckerBy = state.CheckerBy
			files[i].CheckerAt = state.CheckerAt
			files[i].CheckerComment = state.CheckerComment
		}
	}

	return files, nil
}

func recordCreateAuditTx(ctx context.Context, exec auditExecutor, cfg Config, parentID, fileID string, input CreateInput) error {
	return insertAuditEvent(ctx, exec, cfg, fileAuditEvent{
		EntityID:         fileID,
		ModuleKey:        cfg.Module,
		ParentRecordID:   parentID,
		FileID:           fileID,
		ActionType:       fileAuditCreateAction,
		ProcessingStatus: fileAuditCompletedStatus,
		RequestedBy:      input.UploadedBy,
		RequestedAt:      &input.UploadedAt,
	})
}

func recordDownloadAudit(ctx context.Context, exec auditExecutor, cfg Config, parentID string, file FileRecord, requestedBy string) error {
	requestedAt := time.Now().UTC()
	return insertAuditEvent(ctx, exec, cfg, fileAuditEvent{
		EntityID:         file.FileID,
		ModuleKey:        cfg.Module,
		ParentRecordID:   parentID,
		FileID:           file.FileID,
		ActionType:       fileAuditDownloadAction,
		ProcessingStatus: fileAuditCompletedStatus,
		RequestedBy:      requestedBy,
		RequestedAt:      &requestedAt,
	})
}

func recordDeleteRequestAudit(ctx context.Context, exec auditExecutor, cfg Config, parentID string, file FileRecord, requestedBy, reason string) error {
	requestedAt := time.Now().UTC()
	return insertAuditEvent(ctx, exec, cfg, fileAuditEvent{
		EntityID:         file.FileID,
		ModuleKey:        cfg.Module,
		ParentRecordID:   parentID,
		FileID:           file.FileID,
		ActionType:       fileAuditDeleteAction,
		ProcessingStatus: fileAuditPendingDeleteApproval,
		RequestedBy:      requestedBy,
		RequestedAt:      &requestedAt,
		Reason:           reason,
	})
}

func insertAuditEvent(ctx context.Context, exec auditExecutor, cfg Config, event fileAuditEvent) error {
	query := `
		INSERT INTO ` + auditTableName(cfg) + ` (
			module_key,
			parent_record_id,
			file_id,
			action_type,
			processing_status,
			requested_by,
			requested_at,
			checker_by,
			checker_at,
			checker_comment,
			reason
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
	`
	_, err := exec.Exec(
		ctx,
		query,
		event.ModuleKey,
		event.ParentRecordID,
		event.FileID,
		event.ActionType,
		event.ProcessingStatus,
		event.RequestedBy,
		event.RequestedAt,
		nullIfBlank(event.CheckerBy),
		event.CheckerAt,
		nullIfBlank(event.CheckerComment),
		nullIfBlank(event.Reason),
	)
	return err
}

func MainUploadAuditReasonJSON(payload MainUploadAuditPayload) (string, error) {
	reasonPayload := map[string]interface{}{
		"event_type":  "ADDITIONAL_FILE_UPLOAD",
		"file_name":   strings.TrimSpace(payload.FileName),
		"uploaded_by": strings.TrimSpace(payload.UploadedBy),
		"uploaded_at": payload.UploadedAt.UTC().Format(time.RFC3339),
	}
	if fileID := strings.TrimSpace(payload.FileID); fileID != "" {
		reasonPayload["file_id"] = fileID
	}

	out, err := json.Marshal(reasonPayload)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func InsertMainUploadAudit(ctx context.Context, tx pgx.Tx, tableName, parentColumn, actionColumn, parentID string, payload MainUploadAuditPayload) error {
	reason, err := MainUploadAuditReasonJSON(payload)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (%s, %s, processing_status, reason, requested_by, requested_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		tableName,
		parentColumn,
		actionColumn,
	)
	_, err = tx.Exec(ctx, query, parentID, "UPLOAD_FILE", fileAuditApprovedStatus, reason, payload.UploadedBy, payload.UploadedAt)
	return err
}

func listAuditEvents(ctx context.Context, pool *pgxpool.Pool, cfg Config, parentID, fileID string) ([]fileAuditEvent, error) {
	query := `
		SELECT audit_id,
		       module_key,
		       parent_record_id,
		       file_id,
		       action_type,
		       processing_status,
		       requested_by,
		       requested_at,
		       checker_by,
		       checker_at,
		       checker_comment,
		       reason
		FROM ` + auditTableName(cfg) + `
		WHERE module_key = $1
		  AND parent_record_id = $2
		  AND file_id = $3
		ORDER BY requested_at ASC, audit_id ASC
	`

	rows, err := pool.Query(ctx, query, cfg.Module, parentID, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]fileAuditEvent, 0)
	for rows.Next() {
		var event fileAuditEvent
		var moduleKey sql.NullString
		var parentRecordID sql.NullString
		var requestedBy sql.NullString
		var requestedAt sql.NullTime
		var checkerBy sql.NullString
		var checkerAt sql.NullTime
		var checkerComment sql.NullString
		var reason sql.NullString
		if err := rows.Scan(
			&event.AuditID,
			&moduleKey,
			&parentRecordID,
			&event.FileID,
			&event.ActionType,
			&event.ProcessingStatus,
			&requestedBy,
			&requestedAt,
			&checkerBy,
			&checkerAt,
			&checkerComment,
			&reason,
		); err != nil {
			return nil, err
		}

		event.EntityID = event.FileID
		event.ModuleKey = strings.TrimSpace(moduleKey.String)
		event.ParentRecordID = strings.TrimSpace(parentRecordID.String)
		event.RequestedBy = strings.TrimSpace(requestedBy.String)
		if requestedAt.Valid {
			t := requestedAt.Time
			event.RequestedAt = &t
		}
		event.CheckerBy = strings.TrimSpace(checkerBy.String)
		if checkerAt.Valid {
			t := checkerAt.Time
			event.CheckerAt = &t
		}
		event.CheckerComment = strings.TrimSpace(checkerComment.String)
		event.Reason = strings.TrimSpace(reason.String)

		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func latestPendingDelete(ctx context.Context, pool *pgxpool.Pool, cfg Config, fileID string) (*fileAuditEvent, error) {
	return latestPendingDeleteForQuery(ctx, pool, cfg, fileID, false)
}

func latestPendingDeleteForUpdate(ctx context.Context, tx pgx.Tx, cfg Config, fileID string) (*fileAuditEvent, error) {
	return latestPendingDeleteForQuery(ctx, tx, cfg, fileID, true)
}

type queryRower interface {
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

func latestPendingDeleteForQuery(ctx context.Context, queryer queryRower, cfg Config, fileID string, forUpdate bool) (*fileAuditEvent, error) {
	query := `
		SELECT audit_id,
		       module_key,
		       parent_record_id,
		       file_id,
		       action_type,
		       processing_status,
		       requested_by,
		       requested_at,
		       checker_by,
		       checker_at,
		       checker_comment,
		       reason
		FROM ` + auditTableName(cfg) + `
		WHERE module_key = $1
		  AND file_id = $2
		  AND action_type = $3
		  AND processing_status = $4
		ORDER BY requested_at DESC, audit_id DESC
		LIMIT 1
	`
	if forUpdate {
		query += ` FOR UPDATE`
	}

	var event fileAuditEvent
	var module sql.NullString
	var parent sql.NullString
	var requestedBy sql.NullString
	var requestedAt sql.NullTime
	var checkerBy sql.NullString
	var checkerAt sql.NullTime
	var checkerComment sql.NullString
	var reason sql.NullString
	err := queryer.QueryRow(ctx, query, cfg.Module, fileID, fileAuditDeleteAction, fileAuditPendingDeleteApproval).Scan(
		&event.AuditID,
		&module,
		&parent,
		&event.FileID,
		&event.ActionType,
		&event.ProcessingStatus,
		&requestedBy,
		&requestedAt,
		&checkerBy,
		&checkerAt,
		&checkerComment,
		&reason,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	event.ModuleKey = strings.TrimSpace(module.String)
	event.ParentRecordID = strings.TrimSpace(parent.String)
	event.RequestedBy = strings.TrimSpace(requestedBy.String)
	if requestedAt.Valid {
		t := requestedAt.Time
		event.RequestedAt = &t
	}
	event.CheckerBy = strings.TrimSpace(checkerBy.String)
	if checkerAt.Valid {
		t := checkerAt.Time
		event.CheckerAt = &t
	}
	event.CheckerComment = strings.TrimSpace(checkerComment.String)
	event.Reason = strings.TrimSpace(reason.String)
	return &event, nil
}

type deleteAuditDecisionParams struct {
	AuditID        int64
	NextStatus     string
	CheckerBy      string
	CheckerAt      time.Time
	CheckerComment string
}

func updateDeleteAuditDecision(ctx context.Context, exec auditExecutor, cfg Config, p deleteAuditDecisionParams) error {
	auditID := p.AuditID
	nextStatus := p.NextStatus
	checkerBy := p.CheckerBy
	checkerAt := p.CheckerAt
	checkerComment := p.CheckerComment
	query := `
		UPDATE ` + auditTableName(cfg) + `
		SET processing_status = $2,
		    checker_by = $3,
		    checker_at = $4,
		    checker_comment = $5
		WHERE audit_id = $1
	`
	_, err := exec.Exec(ctx, query, auditID, nextStatus, checkerBy, checkerAt, nullIfBlank(checkerComment))
	return err
}

func requestedByOrFallback(ctx context.Context, userID string) string {
	requestedBy := strings.TrimSpace(api.RequestedByFromCtx(ctx, userID))
	if requestedBy == "" {
		requestedBy = strings.TrimSpace(userID)
	}
	return requestedBy
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func nullIfBlank(value string) interface{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func isUndefinedTableError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

func isCheckViolationError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514"
}

func writeAuditSuccess(w http.ResponseWriter, rows []fileAuditEvent) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: true,
		"audit_logs":           rows,
	})
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
