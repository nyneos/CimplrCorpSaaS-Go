package fdMaturityAndRollover

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	fdSummaryModuleMaster  = "fd-master-additional"
	fdSummaryModuleBooking = "fd-booking-additional"
	fdSummaryModuleClosure = "fd-closure-cimplr-additional"
)

type fdSummaryFilesRequest struct {
	UserID  string   `json:"user_id"`
	FDID    string   `json:"fd_id"`
	FileID  string   `json:"file_id"`
	FileIDs []string `json:"file_ids"`
}

type fdSummaryFileSource struct {
	Module   string
	Label    string
	RecordID string
	Kind     string
}

type fdSummaryFileRecord struct {
	FileID             string     `json:"file_id"`
	StoredFileName     string     `json:"stored_file_name"`
	ContentType        string     `json:"content_type,omitempty"`
	FileSize           int64      `json:"file_size,omitempty"`
	UploadS3Key        string     `json:"-"`
	UploadedBy         string     `json:"uploaded_by,omitempty"`
	UploadedAt         time.Time  `json:"uploaded_at"`
	ProcessingStatus   string     `json:"processing_status,omitempty"`
	DeleteRequestedBy  string     `json:"delete_requested_by,omitempty"`
	DeleteRequestedAt  *time.Time `json:"delete_requested_at,omitempty"`
	CheckerBy          string     `json:"checker_by,omitempty"`
	CheckerAt          *time.Time `json:"checker_at,omitempty"`
	CheckerComment     string     `json:"checker_comment,omitempty"`
	SourceModule       string     `json:"source_module"`
	SourceModuleLabel  string     `json:"source_module_label"`
	SourceRecordID     string     `json:"source_record_id"`
	SourceRecordIDName string     `json:"source_record_id_name"`
}

type fdSummaryFileAuditEvent struct {
	EntityID         string     `json:"entity_id"`
	SourceModule     string     `json:"source_module"`
	SourceRecordID   string     `json:"source_record_id"`
	FileID           string     `json:"file_id"`
	ActionType       string     `json:"action_type"`
	ProcessingStatus string     `json:"processing_status"`
	RequestedBy      string     `json:"requested_by,omitempty"`
	RequestedAt      *time.Time `json:"requested_at,omitempty"`
	CheckerBy        string     `json:"checker_by,omitempty"`
	CheckerAt        *time.Time `json:"checker_at,omitempty"`
	CheckerComment   string     `json:"checker_comment,omitempty"`
	Reason           string     `json:"reason,omitempty"`
}

func CimplrMaturitySummaryFilesList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := decodeFDSummaryFilesRequest(w, r)
		if !ok {
			return
		}
		files, err := listFDMaturitySummaryFiles(r.Context(), pool, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to list maturity summary files: "+err.Error())
			return
		}
		writeFDSummaryFilesJSON(w, map[string]interface{}{
			constants.ValueSuccess: true,
			"data":                 map[string]interface{}{"files": files},
		})
	}
}

func CimplrMaturitySummaryFilesDownload(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := decodeFDSummaryFilesRequest(w, r)
		if !ok {
			return
		}
		if strings.TrimSpace(req.FileID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFileIDRequired)
			return
		}
		file, err := findFDMaturitySummaryFile(r.Context(), pool, req.FDID, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "download lookup failed: "+err.Error())
			return
		}
		if file == nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
			return
		}
		url, err := s3storage.GetDownloadPresignedURL(r.Context(), file.UploadS3Key, 15*time.Minute)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to generate download URL: "+err.Error())
			return
		}
		_ = recordFDMaturitySummaryDownloadAudit(r.Context(), pool, *file, getUserEmail(r.Context()))
		writeFDSummaryFilesJSON(w, map[string]interface{}{
			constants.ValueSuccess: true,
			"data":                 map[string]interface{}{"download_url": url},
		})
	}
}

func CimplrMaturitySummaryFilesDownloadBulk(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := decodeFDSummaryFilesRequest(w, r)
		if !ok {
			return
		}
		want := map[string]struct{}{}
		for _, id := range req.FileIDs {
			if id = strings.TrimSpace(id); id != "" {
				want[id] = struct{}{}
			}
		}
		if len(want) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFileIDRequired)
			return
		}
		files, err := listFDMaturitySummaryFiles(r.Context(), pool, req.FDID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "download lookup failed: "+err.Error())
			return
		}
		out := []map[string]interface{}{}
		found := map[string]struct{}{}
		for _, file := range files {
			if _, ok := want[file.FileID]; !ok {
				continue
			}
			url, err := s3storage.GetDownloadPresignedURL(r.Context(), file.UploadS3Key, 15*time.Minute)
			if err != nil {
				continue
			}
			found[file.FileID] = struct{}{}
			_ = recordFDMaturitySummaryDownloadAudit(r.Context(), pool, file, getUserEmail(r.Context()))
			out = append(out, map[string]interface{}{
				"file_id":      file.FileID,
				"download_url": url,
			})
		}
		failed := []string{}
		for id := range want {
			if _, ok := found[id]; !ok {
				failed = append(failed, id)
			}
		}
		writeFDSummaryFilesJSON(w, map[string]interface{}{
			constants.ValueSuccess: true,
			"data":                 map[string]interface{}{"files": out, "failed_ids": failed},
		})
	}
}

func CimplrMaturitySummaryFilesAudit(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, ok := decodeFDSummaryFilesRequest(w, r)
		if !ok {
			return
		}
		if strings.TrimSpace(req.FileID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFileIDRequired)
			return
		}
		file, err := findFDMaturitySummaryFile(r.Context(), pool, req.FDID, req.FileID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "audit lookup failed: "+err.Error())
			return
		}
		if file == nil {
			api.RespondWithError(w, http.StatusNotFound, constants.ErrFileNotFound)
			return
		}
		events, err := listFDMaturitySummaryFileAudit(r.Context(), pool, *file)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to list file audit: "+err.Error())
			return
		}
		writeFDSummaryFilesJSON(w, map[string]interface{}{
			constants.ValueSuccess: true,
			"audit_logs":           events,
		})
	}
}

func CimplrMaturitySummaryFilesUpload(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userEmail := getUserEmail(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}
		if err := r.ParseMultipartForm(64 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrFailedToParseMultipartForm)
			return
		}
		fdID := strings.TrimSpace(r.FormValue("fd_id"))
		if fdID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "fd_id required")
			return
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "file is required")
			return
		}
		defer file.Close()
		body, err := io.ReadAll(file)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "failed to read uploaded file")
			return
		}
		if len(body) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "uploaded file is empty")
			return
		}
		target, err := resolveFDMaturitySummaryUploadTarget(r.Context(), pool, fdID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "upload target lookup failed: "+err.Error())
			return
		}
		if strings.TrimSpace(target.RecordID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, "no valid FD maturity summary upload target found")
			return
		}
		record, err := createFDMaturitySummaryFile(r.Context(), pool, target, userEmail, header.Filename, header.Header.Get("Content-Type"), body)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "file upload failed: "+err.Error())
			return
		}
		writeFDSummaryFilesJSON(w, map[string]interface{}{
			constants.ValueSuccess: true,
			"data":                 map[string]interface{}{"file_id": record.FileID, "files": []fdSummaryFileRecord{record}},
		})
	}
}

func decodeFDSummaryFilesRequest(w http.ResponseWriter, r *http.Request) (fdSummaryFilesRequest, bool) {
	if r.Method != http.MethodPost {
		api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
		return fdSummaryFilesRequest{}, false
	}
	var req fdSummaryFilesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
		return fdSummaryFilesRequest{}, false
	}
	req.FDID = strings.TrimSpace(req.FDID)
	if req.FDID == "" {
		api.RespondWithError(w, http.StatusBadRequest, "fd_id required")
		return fdSummaryFilesRequest{}, false
	}
	return req, true
}

func writeFDSummaryFilesJSON(w http.ResponseWriter, payload map[string]interface{}) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(payload)
}

func loadFDMaturitySummarySources(ctx context.Context, pool *pgxpool.Pool, fdID string) ([]fdSummaryFileSource, error) {
	var masterFDID, bookingID, initiateID, confirmID, newBookingID string
	err := pool.QueryRow(ctx, `
		SELECT
			m.fd_id,
			COALESCE(m.booking_id,''),
			COALESCE(ci.closure_initiate_id,''),
			COALESCE(cc.closure_confirm_id,''),
			COALESCE(cc.new_booking_id,'')
		FROM investment.fd_master m
		LEFT JOIN LATERAL (
			SELECT * FROM cimplr.fd_closure_confirm c
			WHERE c.fd_id=m.fd_id AND COALESCE(c.is_deleted,false)=false
			ORDER BY
				CASE WHEN c.closure_type='PREMATURE' THEN 0 ELSE 1 END,
				c.closure_confirm_id DESC
			LIMIT 1
		) cc ON true
		LEFT JOIN LATERAL (
			SELECT * FROM cimplr.fd_closure_initiate i
			WHERE i.fd_id=m.fd_id AND COALESCE(i.is_deleted,false)=false
			ORDER BY i.closure_initiate_id DESC LIMIT 1
		) ci ON true
		WHERE m.fd_id=$1 AND COALESCE(m.is_deleted,false)=false`, strings.TrimSpace(fdID)).
		Scan(&masterFDID, &bookingID, &initiateID, &confirmID, &newBookingID)
	if err != nil {
		return nil, err
	}
	sources := []fdSummaryFileSource{
		{Module: fdSummaryModuleMaster, Label: "FD Master", RecordID: masterFDID, Kind: "fd_id"},
	}
	if bookingID != "" {
		sources = append(sources, fdSummaryFileSource{Module: fdSummaryModuleBooking, Label: "FD Booking", RecordID: bookingID, Kind: "booking_id"})
	}
	if initiateID != "" {
		sources = append(sources, fdSummaryFileSource{Module: fdSummaryModuleClosure, Label: "FD Closure Initiate", RecordID: initiateID, Kind: "closure_initiate_id"})
	}
	if confirmID != "" {
		sources = append(sources, fdSummaryFileSource{Module: fdSummaryModuleClosure, Label: "FD Closure Confirm", RecordID: confirmID, Kind: "closure_confirm_id"})
	}
	if newBookingID != "" && newBookingID != bookingID {
		sources = append(sources, fdSummaryFileSource{Module: fdSummaryModuleBooking, Label: "Rollover New Booking", RecordID: newBookingID, Kind: "new_booking_id"})
	}
	return sources, nil
}

func resolveFDMaturitySummaryUploadTarget(ctx context.Context, pool *pgxpool.Pool, fdID string) (fdSummaryFileSource, error) {
	sources, err := loadFDMaturitySummarySources(ctx, pool, fdID)
	if err != nil {
		return fdSummaryFileSource{}, err
	}
	for _, source := range sources {
		if source.Kind == "booking_id" {
			return source, nil
		}
	}
	for _, source := range sources {
		if source.Kind == "new_booking_id" {
			return source, nil
		}
	}
	for _, source := range sources {
		if source.Kind == "closure_confirm_id" {
			return source, nil
		}
	}
	for _, source := range sources {
		if source.Kind == "closure_initiate_id" {
			return source, nil
		}
	}
	for _, source := range sources {
		if source.Kind == "fd_id" {
			return source, nil
		}
	}
	return fdSummaryFileSource{}, nil
}

func listFDMaturitySummaryFiles(ctx context.Context, pool *pgxpool.Pool, fdID string) ([]fdSummaryFileRecord, error) {
	sources, err := loadFDMaturitySummarySources(ctx, pool, fdID)
	if err != nil {
		return nil, err
	}
	out := make([]fdSummaryFileRecord, 0)
	for _, source := range sources {
		files, err := listFDMaturitySummarySourceFiles(ctx, pool, source)
		if err != nil {
			return nil, err
		}
		out = append(out, files...)
	}
	return out, nil
}

func findFDMaturitySummaryFile(ctx context.Context, pool *pgxpool.Pool, fdID, fileID string) (*fdSummaryFileRecord, error) {
	files, err := listFDMaturitySummaryFiles(ctx, pool, fdID)
	if err != nil {
		return nil, err
	}
	fileID = strings.TrimSpace(fileID)
	for _, file := range files {
		if file.FileID == fileID {
			return &file, nil
		}
	}
	return nil, nil
}

func listFDMaturitySummarySourceFiles(ctx context.Context, pool *pgxpool.Pool, source fdSummaryFileSource) ([]fdSummaryFileRecord, error) {
	switch source.Module {
	case fdSummaryModuleMaster:
		return listGenericFDMaturitySummaryFiles(ctx, pool, source, "investment.fd_master_files", "fd_id")
	case fdSummaryModuleBooking:
		return listGenericFDMaturitySummaryFiles(ctx, pool, source, "investment.fd_booking_request_files", "booking_id")
	case fdSummaryModuleClosure:
		return listCimplrFDMaturitySummaryFiles(ctx, pool, source)
	default:
		return []fdSummaryFileRecord{}, nil
	}
}

func listGenericFDMaturitySummaryFiles(ctx context.Context, pool *pgxpool.Pool, source fdSummaryFileSource, tableName, parentColumn string) ([]fdSummaryFileRecord, error) {
	query := fmt.Sprintf(`
		SELECT f.file_id::text, COALESCE(f.stored_file_name,''), COALESCE(f.content_type,''), COALESCE(f.file_size,0),
		       COALESCE(f.upload_s3_key,''), COALESCE(f.uploaded_by,''), f.uploaded_at,
		       COALESCE(a.processing_status,'ACTIVE'), COALESCE(a.requested_by,''), a.requested_at,
		       COALESCE(a.checker_by,''), a.checker_at, COALESCE(a.checker_comment,'')
		FROM %s f
		LEFT JOIN LATERAL (
			SELECT *
			FROM investment.additional_file_audit a
			WHERE a.module_key=$2 AND a.parent_record_id=$1 AND a.file_id=f.file_id::text
			ORDER BY a.requested_at DESC NULLS LAST, a.audit_id DESC
			LIMIT 1
		) a ON true
		WHERE f.%s::text=$1 AND COALESCE(f.is_deleted,false)=false
		ORDER BY f.uploaded_at DESC`, tableName, parentColumn)
	rows, err := pool.Query(ctx, query, source.RecordID, source.Module)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFDMaturitySummaryFiles(rows, source)
}

func listCimplrFDMaturitySummaryFiles(ctx context.Context, pool *pgxpool.Pool, source fdSummaryFileSource) ([]fdSummaryFileRecord, error) {
	column := "closure_initiate_id"
	if source.Kind == "closure_confirm_id" {
		column = "closure_confirm_id"
	}
	query := fmt.Sprintf(`
		SELECT f.file_id::text, COALESCE(f.stored_file_name,''), COALESCE(f.content_type,''), COALESCE(f.file_size,0),
		       COALESCE(f.upload_s3_key,''), COALESCE(f.uploaded_by,''), f.uploaded_at,
		       COALESCE(a.processing_status,'ACTIVE'), COALESCE(a.requested_by,''), a.requested_at,
		       COALESCE(a.checker_by,''), a.checker_at, COALESCE(a.checker_comment,'')
		FROM cimplr.fd_closure_files f
		LEFT JOIN LATERAL (
			SELECT *
			FROM cimplr.fd_closure_files_audit a
			WHERE a.file_id=f.file_id
			ORDER BY a.requested_at DESC NULLS LAST, a.audit_id DESC
			LIMIT 1
		) a ON true
		WHERE f.%s=$1 AND COALESCE(f.is_deleted,false)=false
		ORDER BY f.uploaded_at DESC`, column)
	rows, err := pool.Query(ctx, query, source.RecordID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFDMaturitySummaryFiles(rows, source)
}

func scanFDMaturitySummaryFiles(rows pgx.Rows, source fdSummaryFileSource) ([]fdSummaryFileRecord, error) {
	out := []fdSummaryFileRecord{}
	for rows.Next() {
		var rec fdSummaryFileRecord
		var uploadedAt sql.NullTime
		var requestedAt, checkerAt sql.NullTime
		var deleteRequestedBy string
		if err := rows.Scan(
			&rec.FileID,
			&rec.StoredFileName,
			&rec.ContentType,
			&rec.FileSize,
			&rec.UploadS3Key,
			&rec.UploadedBy,
			&uploadedAt,
			&rec.ProcessingStatus,
			&deleteRequestedBy,
			&requestedAt,
			&rec.CheckerBy,
			&checkerAt,
			&rec.CheckerComment,
		); err != nil {
			return nil, err
		}
		if uploadedAt.Valid {
			rec.UploadedAt = uploadedAt.Time
		}
		rec.ProcessingStatus = normalizeFDMaturitySummaryFileStatus(rec.ProcessingStatus)
		if strings.EqualFold(rec.ProcessingStatus, "PENDING_DELETE_APPROVAL") {
			rec.DeleteRequestedBy = deleteRequestedBy
			if requestedAt.Valid {
				t := requestedAt.Time
				rec.DeleteRequestedAt = &t
			}
		}
		if checkerAt.Valid {
			t := checkerAt.Time
			rec.CheckerAt = &t
		}
		rec.SourceModule = source.Module
		rec.SourceModuleLabel = source.Label
		rec.SourceRecordID = source.RecordID
		rec.SourceRecordIDName = source.Kind
		out = append(out, rec)
	}
	return out, rows.Err()
}

func normalizeFDMaturitySummaryFileStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "", "ACTIVE", "COMPLETED":
		return "APPROVED"
	default:
		return strings.ToUpper(strings.TrimSpace(status))
	}
}

func createFDMaturitySummaryFile(ctx context.Context, pool *pgxpool.Pool, target fdSummaryFileSource, userEmail, originalName, contentType string, body []byte) (fdSummaryFileRecord, error) {
	uploadedAt := time.Now().UTC()
	if contentType == "" {
		contentType = s3storage.DetectContentType(body)
	}
	storedFileName := s3storage.BuildUploadedFilename(originalName, userEmail, uploadedAt)
	fileHash := s3storage.ContentHashHex(body)
	moduleForStorage := target.Module
	if target.Module == fdSummaryModuleClosure {
		moduleForStorage = "fd-closure-additional"
	}
	s3Key := s3storage.BuildNamedS3Key(s3storage.GetStoragePrefix(moduleForStorage), target.RecordID, storedFileName)
	if err := s3storage.PutObjectToS3(ctx, s3Key, body, contentType); err != nil {
		return fdSummaryFileRecord{}, err
	}
	rec, err := insertFDMaturitySummaryFileMetadata(ctx, pool, target, storedFileName, originalName, contentType, int64(len(body)), fileHash, s3Key, userEmail, uploadedAt)
	if err != nil {
		_ = s3storage.DeleteFromS3(ctx, s3Key)
		return fdSummaryFileRecord{}, err
	}
	return rec, nil
}

func insertFDMaturitySummaryFileMetadata(ctx context.Context, pool *pgxpool.Pool, target fdSummaryFileSource, storedFileName, originalName, contentType string, fileSize int64, fileHash, s3Key, userEmail string, uploadedAt time.Time) (fdSummaryFileRecord, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fdSummaryFileRecord{}, err
	}
	defer tx.Rollback(ctx)
	var fileID string
	if target.Module == fdSummaryModuleClosure {
		initiateID, confirmID := "", ""
		if target.Kind == "closure_initiate_id" {
			initiateID = target.RecordID
		} else {
			confirmID = target.RecordID
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO cimplr.fd_closure_files (
				closure_initiate_id, closure_confirm_id, file_type, stored_file_name, original_file_name,
				content_type, file_size, file_hash, upload_s3_key, uploaded_by
			) VALUES (NULLIF($1,''), NULLIF($2,''), 'SUPPORTING', $3, $4, $5, $6, $7, $8, $9)
			RETURNING file_id::text`,
			initiateID, confirmID, storedFileName, originalName, contentType, fileSize, fileHash, s3Key, userEmail,
		).Scan(&fileID)
		if err == nil {
			_, err = tx.Exec(ctx, `
				INSERT INTO cimplr.fd_closure_files_audit (
					file_id, closure_initiate_id, closure_confirm_id, action_type, processing_status,
					reason, requested_by
				) VALUES ($1::uuid, NULLIF($2,''), NULLIF($3,''), 'CREATE', 'APPROVED', $4, $5)`,
				fileID, initiateID, confirmID, "File uploaded from FD Maturity Summary", userEmail,
			)
		}
	} else {
		tableName, parentColumn := "investment.fd_master_files", "fd_id"
		if target.Module == fdSummaryModuleBooking {
			tableName, parentColumn = "investment.fd_booking_request_files", "booking_id"
		}
		err = tx.QueryRow(ctx, fmt.Sprintf(`
			INSERT INTO %s (
				%s, stored_file_name, content_type, file_size, file_hash,
				upload_s3_key, uploaded_by, uploaded_at, is_deleted
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, false)
			RETURNING file_id::text`, tableName, parentColumn),
			target.RecordID, storedFileName, contentType, fileSize, fileHash, s3Key, userEmail, uploadedAt,
		).Scan(&fileID)
		if err == nil {
			_, err = tx.Exec(ctx, `
				INSERT INTO investment.additional_file_audit (
					module_key, parent_record_id, file_id, action_type, processing_status,
					requested_by, requested_at, reason
				) VALUES ($1, $2, $3, 'CREATE', 'APPROVED', $4, $5, $6)`,
				target.Module, target.RecordID, fileID, userEmail, uploadedAt, "File uploaded from FD Maturity Summary",
			)
		}
	}
	if err != nil {
		return fdSummaryFileRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return fdSummaryFileRecord{}, err
	}
	return fdSummaryFileRecord{
		FileID:             strings.TrimSpace(fileID),
		StoredFileName:     storedFileName,
		ContentType:        contentType,
		FileSize:           fileSize,
		UploadS3Key:        s3Key,
		UploadedBy:         userEmail,
		UploadedAt:         uploadedAt,
		ProcessingStatus:   "ACTIVE",
		SourceModule:       target.Module,
		SourceModuleLabel:  target.Label,
		SourceRecordID:     target.RecordID,
		SourceRecordIDName: target.Kind,
	}, nil
}

func recordFDMaturitySummaryDownloadAudit(ctx context.Context, pool *pgxpool.Pool, file fdSummaryFileRecord, userEmail string) error {
	userEmail = strings.TrimSpace(userEmail)
	if userEmail == "" {
		userEmail = "system"
	}
	now := time.Now().UTC()
	if file.SourceModule == fdSummaryModuleClosure {
		initiateID, confirmID := "", ""
		if file.SourceRecordIDName == "closure_initiate_id" {
			initiateID = file.SourceRecordID
		} else {
			confirmID = file.SourceRecordID
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO cimplr.fd_closure_files_audit (
				file_id, closure_initiate_id, closure_confirm_id, action_type, processing_status,
				reason, requested_by, requested_at
			) VALUES ($1::uuid, NULLIF($2,''), NULLIF($3,''), 'DOWNLOAD', 'COMPLETED', $4, $5, $6)`,
			file.FileID, initiateID, confirmID, "File downloaded from FD Maturity Summary", userEmail, now,
		)
		return err
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO investment.additional_file_audit (
			module_key, parent_record_id, file_id, action_type, processing_status,
			requested_by, requested_at, reason
		) VALUES ($1, $2, $3, 'DOWNLOAD', 'COMPLETED', $4, $5, $6)`,
		file.SourceModule, file.SourceRecordID, file.FileID, userEmail, now, "File downloaded from FD Maturity Summary",
	)
	return err
}

func listFDMaturitySummaryFileAudit(ctx context.Context, pool *pgxpool.Pool, file fdSummaryFileRecord) ([]fdSummaryFileAuditEvent, error) {
	if file.SourceModule == fdSummaryModuleClosure {
		return listCimplrFDMaturitySummaryFileAudit(ctx, pool, file)
	}
	rows, err := pool.Query(ctx, `
		SELECT file_id, action_type, processing_status, requested_by, requested_at,
		       checker_by, checker_at, checker_comment, reason
		FROM investment.additional_file_audit
		WHERE module_key=$1 AND parent_record_id=$2 AND file_id=$3
		ORDER BY requested_at ASC NULLS LAST, audit_id ASC`,
		file.SourceModule, file.SourceRecordID, file.FileID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFDMaturitySummaryAudit(rows, file)
}

func listCimplrFDMaturitySummaryFileAudit(ctx context.Context, pool *pgxpool.Pool, file fdSummaryFileRecord) ([]fdSummaryFileAuditEvent, error) {
	rows, err := pool.Query(ctx, `
		SELECT file_id::text, action_type, processing_status, requested_by, requested_at,
		       checker_by, checker_at, checker_comment, reason
		FROM cimplr.fd_closure_files_audit
		WHERE file_id=$1::uuid
		ORDER BY requested_at ASC NULLS LAST, audit_id ASC`, file.FileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFDMaturitySummaryAudit(rows, file)
}

func scanFDMaturitySummaryAudit(rows pgx.Rows, file fdSummaryFileRecord) ([]fdSummaryFileAuditEvent, error) {
	out := []fdSummaryFileAuditEvent{}
	for rows.Next() {
		var ev fdSummaryFileAuditEvent
		var requestedBy, checkerBy, checkerComment, reason sql.NullString
		var requestedAt, checkerAt sql.NullTime
		if err := rows.Scan(
			&ev.FileID,
			&ev.ActionType,
			&ev.ProcessingStatus,
			&requestedBy,
			&requestedAt,
			&checkerBy,
			&checkerAt,
			&checkerComment,
			&reason,
		); err != nil {
			return nil, err
		}
		ev.EntityID = ev.FileID
		ev.SourceModule = file.SourceModule
		ev.SourceRecordID = file.SourceRecordID
		ev.RequestedBy = strings.TrimSpace(requestedBy.String)
		if requestedAt.Valid {
			t := requestedAt.Time
			ev.RequestedAt = &t
		}
		ev.CheckerBy = strings.TrimSpace(checkerBy.String)
		if checkerAt.Valid {
			t := checkerAt.Time
			ev.CheckerAt = &t
		}
		ev.CheckerComment = strings.TrimSpace(checkerComment.String)
		ev.Reason = strings.TrimSpace(reason.String)
		out = append(out, ev)
	}
	return out, rows.Err()
}
