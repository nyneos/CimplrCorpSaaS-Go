package bankstatement

import (
	"CimplrCorpSaas/api/constants"
	apipreval "CimplrCorpSaas/api/middlewares"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// ── DB helpers ────────────────────────────────────────────────────────────────

// insertStagingBatch creates a new batch row and returns its ID.
func insertStagingBatch(ctx context.Context, pool *pgxpool.Pool, userID, sourceFilename string, totalFiles int) (string, error) {
	return insertZipStagingBatch(ctx, pool, userID, sourceFilename, totalFiles, nil)
}

// insertZipStagingBatch creates a batch row for async ZIP or PDF staging.
func insertZipStagingBatch(ctx context.Context, pool *pgxpool.Pool, userID, sourceFilename string, totalFiles int, uploadParams []byte) (string, error) {
	var batchID string
	batchType := "pdf"
	if uploadParams != nil {
		batchType = "zip"
	}
	err := pool.QueryRow(ctx, `
		INSERT INTO cimplrcorpsaas.pdf_staging_batch
		    (user_id, source_filename, status, total_files, batch_type, upload_params)
		VALUES ($1, $2, 'processing', $3, $4, $5)
		RETURNING batch_id
	`, userID, sourceFilename, totalFiles, batchType, uploadParams).Scan(&batchID)
	return batchID, err
}

// insertStagingStatementParams carries row data for insertStagingStatement.
// RawStatement is the serialised RecalculateCleanData-shaped map (or nil if not yet parsed).
type insertStagingStatementParams struct {
	BatchID      string
	Filename     string
	CSVURL       string
	RawStatement interface{}
	Status       string
	ErrMsg       string
	FileIndex    int
	UploadS3Key  string
	ContentHash  string
	ContentType  string
	FileSize     int64
	UploadedBy   string
	UploadedAt   time.Time
}

// insertStagingStatement creates a statement row inside a batch.
func insertStagingStatement(ctx context.Context, pool *pgxpool.Pool, p insertStagingStatementParams) (string, error) {
	raw, err := json.Marshal(p.RawStatement)
	if err != nil {
		return "", fmt.Errorf("marshal raw_statement: %w", err)
	}
	var stagingID string
	var errMsgParam interface{}
	if p.ErrMsg != "" {
		errMsgParam = p.ErrMsg
	}
	var uploadedAt interface{}
	if !p.UploadedAt.IsZero() {
		uploadedAt = p.UploadedAt.UTC()
	}
	var fileSize interface{}
	if p.FileSize > 0 {
		fileSize = p.FileSize
	}
	err = pool.QueryRow(ctx, `
		INSERT INTO cimplrcorpsaas.pdf_staging_statement
		    (batch_id, original_filename, csv_url, raw_statement, status, error_message, file_index,
		     upload_s3_key, content_hash, content_type, file_size, uploaded_by, uploaded_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8,''), NULLIF($9,''), NULLIF($10,''), $11, NULLIF($12,''), $13)
		RETURNING staging_id
	`, p.BatchID, p.Filename, p.CSVURL, raw, p.Status, errMsgParam, p.FileIndex,
		p.UploadS3Key, p.ContentHash, p.ContentType, fileSize, p.UploadedBy, uploadedAt).Scan(&stagingID)
	return stagingID, err
}

func updateStagingStatementStatus(ctx context.Context, pool *pgxpool.Pool, stagingID, status, errMsg string) error {
	var errParam interface{}
	if strings.TrimSpace(errMsg) != "" {
		errParam = errMsg
	}
	_, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.pdf_staging_statement
		   SET status = $1, error_message = $2, updated_at = now()
		 WHERE staging_id = $3
	`, status, errParam, stagingID)
	return err
}

// updateStagingStatementPreviewData stores parsed preview in raw_statement (and optional preview_result).
func updateStagingStatementPreviewData(ctx context.Context, pool *pgxpool.Pool, stagingID string, rawPreview, previewResult map[string]interface{}) error {
	rawJSON, err := json.Marshal(rawPreview)
	if err != nil {
		return fmt.Errorf("marshal raw_statement: %w", err)
	}
	var previewJSON []byte
	if previewResult != nil {
		previewJSON, err = json.Marshal(previewResult)
		if err != nil {
			return fmt.Errorf("marshal preview_result: %w", err)
		}
	}
	_, err = pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.pdf_staging_statement
		   SET raw_statement = $1,
		       preview_result = COALESCE($2, preview_result),
		       status = 'parsed',
		       updated_at = now()
		 WHERE staging_id = $3
	`, rawJSON, previewJSON, stagingID)
	return err
}

func incrementStagingBatchProgress(ctx context.Context, pool *pgxpool.Pool, batchID string, succeeded bool) {
	if pool == nil || batchID == "" {
		return
	}
	if succeeded {
		_, _ = pool.Exec(ctx, `
			UPDATE cimplrcorpsaas.pdf_staging_batch
			   SET processed_files = processed_files + 1, updated_at = now()
			 WHERE batch_id = $1
		`, batchID)
		return
	}
	_, _ = pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.pdf_staging_batch
		   SET failed_files = failed_files + 1, updated_at = now()
		 WHERE batch_id = $1
	`, batchID)
}

func updateStagingStatementV2Result(ctx context.Context, pool *pgxpool.Pool, stagingID string, resp map[string]interface{}, bankStatementID string) error {
	previewJSON, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	var bsParam interface{}
	if strings.TrimSpace(bankStatementID) != "" {
		bsParam = bankStatementID
	}
	_, err = pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.pdf_staging_statement
		   SET preview_result = $1, status = 'parsed', bank_statement_id = $2, updated_at = now()
		 WHERE staging_id = $3
	`, previewJSON, bsParam, stagingID)
	return err
}

// finaliseStagingBatch updates processed/failed counts and resolves the batch status.
func finaliseStagingBatch(ctx context.Context, pool *pgxpool.Pool, batchID string, processed, failed int) error {
	status := "ready"
	if failed > 0 {
		status = "partial_ready"
	}
	_, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.pdf_staging_batch
		   SET processed_files = $1, failed_files = $2, status = $3, updated_at = now()
		 WHERE batch_id = $4
	`, processed, failed, status, batchID)
	return err
}

// stagingStatementDisplayStatus is the status returned by list/get APIs. If a row has a
// committed_bs_id, it is treated as committed even when the status column was not updated
// (e.g. older code paths or manual DB edits).
func stagingStatementDisplayStatus(dbStatus string, committedBSID sql.NullString) string {
	if committedBSID.Valid && strings.TrimSpace(committedBSID.String) != "" {
		return "committed"
	}
	return dbStatus
}

// markStagingStatementCommitted records the bank_statement_id after a commit.
func markStagingStatementCommitted(ctx context.Context, pool *pgxpool.Pool, stagingID, bankStatementID string) error {
	_, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.pdf_staging_statement
		   SET status = 'committed', committed_bs_id = $1, updated_at = now()
		 WHERE staging_id = $2
	`, bankStatementID, stagingID)
	return err
}

// ── HTTP Handlers ─────────────────────────────────────────────────────────────

// GetStagingBatchHandler returns batch metadata and a summary of its statements.
// POST {"batch_id":"..."}
func GetStagingBatchHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			BatchID string `json:"batch_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.BatchID == "" {
			respondWithError(w, nil, "batch_id is required", http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		sessionUID := apipreval.GetUserIDFromContext(ctx)
		if sessionUID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var (
			batchID        string
			userID         string
			sourceFilename string
			status         string
			totalFiles     int
			processed      int
			failed         int
			createdAt      time.Time
		)
		err := pool.QueryRow(ctx, `
			SELECT batch_id, user_id, source_filename, status,
			       total_files, processed_files, failed_files, created_at
			FROM cimplrcorpsaas.pdf_staging_batch
			WHERE batch_id = $1 AND user_id = $2
		`, body.BatchID, sessionUID).Scan(&batchID, &userID, &sourceFilename, &status,
			&totalFiles, &processed, &failed, &createdAt)
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			respondWithError(w, nil, "batch not found", http.StatusNotFound)
			return
		}
		if err != nil {
			respondWithError(w, err, "failed to fetch batch", http.StatusInternalServerError)
			return
		}

		rows, err := pool.Query(ctx, `
			SELECT staging_id, original_filename, csv_url, status, error_message,
			       committed_bs_id, created_at, COALESCE(file_index, 0),
			       upload_s3_key, content_type, file_size, uploaded_by, uploaded_at
			FROM cimplrcorpsaas.pdf_staging_statement
			WHERE batch_id = $1
			  AND (committed_bs_id IS NULL OR trim(committed_bs_id::text) = '')
			  AND lower(coalesce(status, '')) <> 'committed'
			ORDER BY COALESCE(file_index, 0), created_at
		`, batchID)
		if err != nil {
			respondWithError(w, err, "failed to fetch statements", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type stmtSummary struct {
			StagingID    string `json:"staging_id"`
			Filename     string `json:"filename"`
			CSVUrl       string `json:"csv_url"`
			Status       string `json:"status"`
			ErrorMessage string `json:"error_message,omitempty"`
			CommittedID  string `json:"committed_bs_id,omitempty"`
			HasFile      bool   `json:"has_file"`
		}
		var stmts []stmtSummary
		for rows.Next() {
			var s stmtSummary
			var errMsg, committedID sql.NullString
			var csvURL, uploadS3Key, contentType, uploadedBy sql.NullString
			var fileSize sql.NullInt64
			var uploadedAt sql.NullTime
			var stmtCreatedAt time.Time
			var fileIndex int
			if err := rows.Scan(&s.StagingID, &s.Filename, &csvURL, &s.Status, &errMsg, &committedID, &stmtCreatedAt, &fileIndex,
				&uploadS3Key, &contentType, &fileSize, &uploadedBy, &uploadedAt); err != nil {
				_ = stmtCreatedAt
				continue
			}
			if csvURL.Valid {
				s.CSVUrl = csvURL.String
			}
			if errMsg.Valid {
				s.ErrorMessage = errMsg.String
			}
			if committedID.Valid {
				s.CommittedID = committedID.String
			}
			s.Status = stagingStatementDisplayStatus(s.Status, committedID)
			s.HasFile = uploadS3Key.Valid && strings.TrimSpace(uploadS3Key.String) != ""
			stmts = append(stmts, s)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"batch_id":        batchID,
				"user_id":         userID,
				"source_filename": sourceFilename,
				"status":          status,
				"total_files":     totalFiles,
				"processed_files": processed,
				"failed_files":    failed,
				"created_at":      createdAt,
				"statements":      stmts,
			},
		})
	})
}

// GetStagingStatementHandler returns a single staged statement including its
// raw_statement JSON so the front-end can display and edit transactions.
// POST {"staging_id":"..."}
func GetStagingStatementHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			StagingID string `json:"staging_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.StagingID == "" {
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
			stagingID   string
			batchID     sql.NullString
			filename    string
			csvURL      sql.NullString
			rawStmt     []byte
			status      string
			errMsg      sql.NullString
			committedID sql.NullString
			createdAt   time.Time
			uploadS3Key sql.NullString
			contentType sql.NullString
			fileSize    sql.NullInt64
			uploadedBy  sql.NullString
			uploadedAt  sql.NullTime
		)
		err := pool.QueryRow(ctx, `
			SELECT s.staging_id, s.batch_id, s.original_filename, s.csv_url,
			       s.raw_statement, s.status, s.error_message, s.committed_bs_id, s.created_at,
			       s.upload_s3_key, s.content_type, s.file_size, s.uploaded_by, s.uploaded_at
			FROM cimplrcorpsaas.pdf_staging_statement s
			INNER JOIN cimplrcorpsaas.pdf_staging_batch b ON b.batch_id = s.batch_id
			WHERE s.staging_id = $1 AND b.user_id = $2
			  AND (s.committed_bs_id IS NULL OR trim(s.committed_bs_id::text) = '')
			  AND lower(coalesce(s.status, '')) <> 'committed'
		`, body.StagingID, sessionUID).Scan(&stagingID, &batchID, &filename, &csvURL,
			&rawStmt, &status, &errMsg, &committedID, &createdAt,
			&uploadS3Key, &contentType, &fileSize, &uploadedBy, &uploadedAt)
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			respondWithError(w, nil, constants.ErrStatementNotFound, http.StatusNotFound)
			return
		}
		if err != nil {
			respondWithError(w, err, "failed to fetch statement", http.StatusInternalServerError)
			return
		}

		var rawStatementParsed interface{}
		if len(rawStmt) > 0 {
			if err := json.Unmarshal(rawStmt, &rawStatementParsed); err != nil {
				logger.LogError("[staging] warn: failed to parse raw_statement for %s: %v", stagingID, err)
				rawStatementParsed = nil
			}
		}

		displayStatus := stagingStatementDisplayStatus(status, committedID)

		respData := map[string]interface{}{
			"staging_id":        stagingID,
			"batch_id":          batchID.String,
			"original_filename": filename,
			"csv_url":           csvURL.String,
			"raw_statement":     rawStatementParsed,
			"status":            displayStatus,
			"error_message":     errMsg.String,
			"committed_bs_id":   committedID.String,
			"created_at":        createdAt,
		}
		mergeStagingFileJSON(respData, uploadS3Key, contentType, fileSize, uploadedBy, uploadedAt)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    respData,
		})
	})
}

// ListStagingByUserHandler returns staged batches and non-committed statement summaries
// for the authenticated user only (user_id from session / prevalidation context).
// Committed statements are omitted; batches where every statement is committed are omitted entirely.
func ListStagingByUserHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := apipreval.GetUserIDFromContext(ctx)
		if userID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}

		rows, err := pool.Query(ctx, `
			SELECT b.batch_id, b.source_filename, b.status,
			       b.total_files, b.processed_files, b.failed_files, b.created_at,
			       s.staging_id, s.original_filename, s.csv_url, s.status,
			       s.error_message, s.committed_bs_id, s.created_at,
			       s.upload_s3_key, s.content_type, s.file_size, s.uploaded_by, s.uploaded_at
			FROM cimplrcorpsaas.pdf_staging_batch b
			INNER JOIN cimplrcorpsaas.pdf_staging_statement s ON s.batch_id = b.batch_id
			  AND (s.committed_bs_id IS NULL OR trim(s.committed_bs_id::text) = '')
			  AND lower(coalesce(s.status, '')) <> 'committed'
			WHERE b.user_id = $1
			ORDER BY b.created_at DESC, s.created_at ASC
		`, userID)
		if err != nil {
			respondWithError(w, err, "failed to fetch staging data", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type stmtRow struct {
			StagingID   string `json:"staging_id"`
			Filename    string `json:"filename"`
			CSVUrl      string `json:"csv_url,omitempty"`
			Status      string `json:"status"`
			ErrorMsg    string `json:"error_message,omitempty"`
			CommittedID string `json:"committed_bs_id,omitempty"`
			CreatedAt   string `json:"created_at"`
			HasFile     bool   `json:"has_file"`
		}
		type batchRow struct {
			BatchID        string    `json:"batch_id"`
			SourceFilename string    `json:"source_filename"`
			Status         string    `json:"status"`
			TotalFiles     int       `json:"total_files"`
			ProcessedFiles int       `json:"processed_files"`
			FailedFiles    int       `json:"failed_files"`
			CreatedAt      time.Time `json:"created_at"`
			Statements     []stmtRow `json:"statements"`
		}

		batchMap := map[string]*batchRow{}
		var batchOrder []string

		for rows.Next() {
			var (
				batchID, srcFile, batchStatus        string
				total, processed, failed             int
				batchCreatedAt                       time.Time
				stagingID, stmtFilename, stmtStatus  sql.NullString
				csvURL, errMsg, committedID          sql.NullString
				uploadS3Key, contentType, uploadedBy sql.NullString
				fileSize                             sql.NullInt64
				uploadedAt                           sql.NullTime
				stmtCreatedAt                        sql.NullTime
			)
			if err := rows.Scan(
				&batchID, &srcFile, &batchStatus,
				&total, &processed, &failed, &batchCreatedAt,
				&stagingID, &stmtFilename, &csvURL, &stmtStatus,
				&errMsg, &committedID, &stmtCreatedAt,
				&uploadS3Key, &contentType, &fileSize, &uploadedBy, &uploadedAt,
			); err != nil {
				continue
			}
			if _, seen := batchMap[batchID]; !seen {
				batchMap[batchID] = &batchRow{
					BatchID:        batchID,
					SourceFilename: srcFile,
					Status:         batchStatus,
					TotalFiles:     total,
					ProcessedFiles: processed,
					FailedFiles:    failed,
					CreatedAt:      batchCreatedAt,
					Statements:     []stmtRow{},
				}
				batchOrder = append(batchOrder, batchID)
			}
			if stagingID.Valid {
				s := stmtRow{
					StagingID: stagingID.String,
					Filename:  stmtFilename.String,
					Status:    stagingStatementDisplayStatus(stmtStatus.String, committedID),
					HasFile:   uploadS3Key.Valid && strings.TrimSpace(uploadS3Key.String) != "",
				}
				if csvURL.Valid {
					s.CSVUrl = csvURL.String
				}
				if errMsg.Valid {
					s.ErrorMsg = errMsg.String
				}
				if committedID.Valid {
					s.CommittedID = committedID.String
				}
				if stmtCreatedAt.Valid {
					s.CreatedAt = stmtCreatedAt.Time.Format(time.RFC3339)
				}
				batchMap[batchID].Statements = append(batchMap[batchID].Statements, s)
			}
		}

		batches := make([]*batchRow, 0, len(batchOrder))
		for _, id := range batchOrder {
			b := batchMap[id]
			if len(b.Statements) == 0 {
				continue
			}
			batches = append(batches, b)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    batches,
		})
	})
}

// UpdateStagingStatementHandler lets a user update the raw_statement JSON
// (e.g. add/remove/edit transactions) before recalculating and committing.
// POST {"staging_id":"...", "raw_statement": {...}}
func UpdateStagingStatementHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			StagingID    string                 `json:"staging_id"`
			RawStatement map[string]interface{} `json:"raw_statement"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.StagingID == "" {
			respondWithError(w, nil, "staging_id and raw_statement are required", http.StatusBadRequest)
			return
		}
		raw, err := json.Marshal(body.RawStatement)
		if err != nil {
			respondWithError(w, err, "invalid raw_statement JSON", http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		sessionUID := apipreval.GetUserIDFromContext(ctx)
		if sessionUID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}
		res, err := pool.Exec(ctx, `
			UPDATE cimplrcorpsaas.pdf_staging_statement st
			   SET raw_statement = $1, status = 'parsed', updated_at = now()
			  FROM cimplrcorpsaas.pdf_staging_batch b
			 WHERE st.staging_id = $2 AND st.batch_id = b.batch_id AND b.user_id = $3
			   AND st.status != 'committed'
		`, raw, body.StagingID, sessionUID)
		if err != nil {
			respondWithError(w, err, "failed to update statement", http.StatusInternalServerError)
			return
		}
		n := res.RowsAffected()
		if n == 0 {
			respondWithError(w, nil, "statement not found or already committed", http.StatusNotFound)
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
	})
}

// DeleteStagingStatementHandler removes one pdf_staging_statement row only.
// It does not modify bank_statements or bank_statement_transactions.
//
// GET /cash/staging/statement/delete?staging_id=...
//
// If no pdf_staging_statement rows remain for the parent batch_id, pdf_staging_batch is deleted as well.
func DeleteStagingStatementHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respondWithError(w, nil, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		sid := strings.TrimSpace(r.URL.Query().Get("staging_id"))
		if sid == "" {
			respondWithError(w, nil, "staging_id query parameter is required", http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		sessionUID := apipreval.GetUserIDFromContext(ctx)
		if sessionUID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var batchID sql.NullString
		err := pool.QueryRow(ctx, `
			SELECT s.batch_id::text
			  FROM cimplrcorpsaas.pdf_staging_statement s
			  INNER JOIN cimplrcorpsaas.pdf_staging_batch b ON b.batch_id = s.batch_id
			 WHERE s.staging_id = $1 AND b.user_id = $2
		`, sid, sessionUID).Scan(&batchID)
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			respondWithError(w, nil, constants.ErrStatementNotFound, http.StatusNotFound)
			return
		}
		if err != nil {
			respondWithError(w, err, "failed to resolve staging row", http.StatusInternalServerError)
			return
		}

		s3Keys, err := collectStagingS3KeysForDelete(ctx, pool, []string{sid})
		if err != nil {
			respondWithError(w, err, constants.ErrFailedToResolveStagingFiles, http.StatusInternalServerError)
			return
		}

		res, err := pool.Exec(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_statement WHERE staging_id = $1`, sid)
		if err != nil {
			respondWithError(w, err, "failed to delete staging statement", http.StatusInternalServerError)
			return
		}
		if n := res.RowsAffected(); n == 0 {
			respondWithError(w, nil, constants.ErrStatementNotFound, http.StatusNotFound)
			return
		}

		batchDeleted := false
		remaining := 0
		if batchID.Valid && strings.TrimSpace(batchID.String) != "" {
			bid := strings.TrimSpace(batchID.String)
			_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.pdf_staging_statement WHERE batch_id = $1`, bid).Scan(&remaining)
			if remaining == 0 {
				if _, err := pool.Exec(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_batch WHERE batch_id = $1`, bid); err != nil {
					respondWithError(w, err, "failed to delete empty staging batch", http.StatusInternalServerError)
					return
				}
				batchDeleted = true
			}
		}

		deleteStagingS3KeysBestEffort(ctx, pool, s3Keys)

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":            true,
			"message":            "Staging statement deleted",
			"staging_id":         sid,
			"batch_deleted":      batchDeleted,
			"remaining_in_batch": remaining,
		})
	})
}

// DeleteStagingBatchHandler deletes staging data for the authenticated user.
// It does not touch committed bank statement tables.
//
// GET /cash/staging/batch/delete?batch_id=... — deletes every pdf_staging_statement in the batch, then the batch row.
//
// POST /cash/staging/batch/delete — JSON {"staging_ids":["..."],"reason":"...","user_id":"..."}.
// Deletes only those statements that belong to the user's batches. If a batch has no statements left, the batch row is removed.
// user_id is optional; when sent it must equal the session user id.
func DeleteStagingBatchHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		sessionUID := apipreval.GetUserIDFromContext(ctx)
		if sessionUID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}

		switch r.Method {
		case http.MethodGet:
			deleteStagingBatchGET(w, r, pool, ctx, sessionUID)
		case http.MethodPost:
			deleteStagingBatchPOST(w, r, pool, ctx, sessionUID)
		default:
			respondWithError(w, nil, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
		}
	})
}

func deleteStagingBatchGET(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, ctx context.Context, sessionUID string) {
	bid := strings.TrimSpace(r.URL.Query().Get("batch_id"))
	if bid == "" {
		respondWithError(w, nil, "batch_id query parameter is required", http.StatusBadRequest)
		return
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		respondWithError(w, err, "failed to start transaction", http.StatusInternalServerError)
		return
	}

	var batchExists int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.pdf_staging_batch WHERE batch_id = $1 AND user_id = $2`, bid, sessionUID).Scan(&batchExists); err != nil {
		_ = tx.Rollback(ctx)
		respondWithError(w, err, "failed to verify batch", http.StatusInternalServerError)
		return
	}
	if batchExists == 0 {
		_ = tx.Rollback(ctx)
		respondWithError(w, nil, "batch not found", http.StatusNotFound)
		return
	}

	s3Keys, err := collectStagingS3KeysForBatchDelete(ctx, pool, bid)
	if err != nil {
		_ = tx.Rollback(ctx)
		respondWithError(w, err, constants.ErrFailedToResolveStagingFiles, http.StatusInternalServerError)
		return
	}

	resSt, err := tx.Exec(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_statement WHERE batch_id = $1`, bid)
	if err != nil {
		_ = tx.Rollback(ctx)
		respondWithError(w, err, constants.ErrFailedToDeleteStagedStatements, http.StatusInternalServerError)
		return
	}
	stRemoved := resSt.RowsAffected()

	if _, err := tx.Exec(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_batch WHERE batch_id = $1`, bid); err != nil {
		_ = tx.Rollback(ctx)
		respondWithError(w, err, "failed to delete staging batch", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		respondWithError(w, err, "failed to commit delete", http.StatusInternalServerError)
		return
	}

	deleteStagingS3KeysBestEffort(ctx, pool, s3Keys)

	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":            true,
		"message":            "Staging batch and linked statements deleted",
		"batch_id":           bid,
		"statements_deleted": stRemoved,
	})
}

func deleteStagingBatchPOST(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool, ctx context.Context, sessionUID string) {
	var body struct {
		StagingIDs []string `json:"staging_ids"`
		Reason     string   `json:"reason"`
		UserID     string   `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondWithError(w, err, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if trimmed := strings.TrimSpace(body.UserID); trimmed != "" && trimmed != strings.TrimSpace(sessionUID) {
		respondWithError(w, nil, "user_id does not match authenticated user", http.StatusForbidden)
		return
	}

	seen := make(map[string]struct{})
	var ids []string
	for _, id := range body.StagingIDs {
		s := strings.TrimSpace(id)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		ids = append(ids, s)
	}
	if len(ids) == 0 {
		respondWithError(w, nil, "staging_ids must contain at least one non-empty id", http.StatusBadRequest)
		return
	}

	s3Keys, err := collectStagingS3KeysForDelete(ctx, pool, ids)
	if err != nil {
		respondWithError(w, err, constants.ErrFailedToResolveStagingFiles, http.StatusInternalServerError)
		return
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		respondWithError(w, err, "failed to start transaction", http.StatusInternalServerError)
		return
	}

	delRows, err := tx.Query(ctx, `
		DELETE FROM cimplrcorpsaas.pdf_staging_statement s
		USING cimplrcorpsaas.pdf_staging_batch b
		WHERE s.batch_id = b.batch_id
		  AND b.user_id = $1
		  AND s.staging_id::text = ANY($2::text[])
		RETURNING s.batch_id::text, s.staging_id::text
	`, sessionUID, ids)
	if err != nil {
		_ = tx.Rollback(ctx)
		respondWithError(w, err, constants.ErrFailedToDeleteStagedStatements, http.StatusInternalServerError)
		return
	}
	defer delRows.Close()

	type delRow struct {
		batchID, stagingID string
	}
	var deleted []delRow
	for delRows.Next() {
		var batchID, stagingID string
		if err := delRows.Scan(&batchID, &stagingID); err != nil {
			_ = tx.Rollback(ctx)
			respondWithError(w, err, "failed to read delete result", http.StatusInternalServerError)
			return
		}
		deleted = append(deleted, delRow{batchID, stagingID})
	}
	if err := delRows.Err(); err != nil {
		_ = tx.Rollback(ctx)
		respondWithError(w, err, constants.ErrFailedToDeleteStagedStatements, http.StatusInternalServerError)
		return
	}

	if len(deleted) == 0 {
		_ = tx.Rollback(ctx)
		respondWithError(w, nil, "no staging statements deleted — check ids and ownership", http.StatusNotFound)
		return
	}

	affectedBatches := make(map[string]struct{})
	var deletedStagingIDs []string
	for _, d := range deleted {
		deletedStagingIDs = append(deletedStagingIDs, d.stagingID)
		affectedBatches[d.batchID] = struct{}{}
	}

	var batchesRemoved []string
	for bid := range affectedBatches {
		var remaining int
		if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.pdf_staging_statement WHERE batch_id = $1`, bid).Scan(&remaining); err != nil {
			_ = tx.Rollback(ctx)
			respondWithError(w, err, "failed to count remaining statements", http.StatusInternalServerError)
			return
		}
		if remaining < 1 {
			if _, err := tx.Exec(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_batch WHERE batch_id = $1`, bid); err != nil {
				_ = tx.Rollback(ctx)
				respondWithError(w, err, "failed to delete empty staging batch", http.StatusInternalServerError)
				return
			}
			batchesRemoved = append(batchesRemoved, bid)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		respondWithError(w, err, "failed to commit delete", http.StatusInternalServerError)
		return
	}

	deleteStagingS3KeysBestEffort(ctx, pool, s3Keys)

	if body.Reason != "" || len(deletedStagingIDs) > 0 {
		log.Printf("pdf staging delete by ids: session_user=%s reason=%q deleted=%d batches_emptied=%d",
			sessionUID, strings.TrimSpace(body.Reason), len(deletedStagingIDs), len(batchesRemoved))
	}

	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":               true,
		"message":               "Staging statements processed",
		"deleted_staging_ids":   deletedStagingIDs,
		"batches_deleted":       batchesRemoved,
		"requested_staging_ids": ids,
	})
}
