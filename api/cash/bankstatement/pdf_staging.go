package bankstatement

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// ── DB helpers ────────────────────────────────────────────────────────────────

// insertStagingBatch creates a new batch row and returns its ID.
func insertStagingBatch(ctx context.Context, db *sql.DB, userID, sourceFilename string, totalFiles int) (string, error) {
	var batchID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO cimplrcorpsaas.pdf_staging_batch
		    (user_id, source_filename, status, total_files)
		VALUES ($1, $2, 'processing', $3)
		RETURNING batch_id
	`, userID, sourceFilename, totalFiles).Scan(&batchID)
	return batchID, err
}

// insertStagingStatement creates a statement row inside a batch. rawStatement
// is the serialised RecalculateCleanData-shaped map (or nil if not yet parsed).
func insertStagingStatement(ctx context.Context, db *sql.DB, batchID, filename, csvURL string, rawStatement interface{}, status, errMsg string) (string, error) {
	raw, err := json.Marshal(rawStatement)
	if err != nil {
		return "", fmt.Errorf("marshal raw_statement: %w", err)
	}
	var stagingID string
	var errMsgParam interface{}
	if errMsg != "" {
		errMsgParam = errMsg
	}
	err = db.QueryRowContext(ctx, `
		INSERT INTO cimplrcorpsaas.pdf_staging_statement
		    (batch_id, original_filename, csv_url, raw_statement, status, error_message)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING staging_id
	`, batchID, filename, csvURL, raw, status, errMsgParam).Scan(&stagingID)
	return stagingID, err
}

// finaliseStagingBatch updates processed/failed counts and resolves the batch status.
func finaliseStagingBatch(ctx context.Context, db *sql.DB, batchID string, processed, failed int) error {
	status := "ready"
	if failed > 0 && processed == 0 {
		status = "processing" // all failed — leave as processing so front-end shows error
	} else if failed > 0 {
		status = "partial_ready"
	}
	_, err := db.ExecContext(ctx, `
		UPDATE cimplrcorpsaas.pdf_staging_batch
		   SET processed_files = $1, failed_files = $2, status = $3, updated_at = now()
		 WHERE batch_id = $4
	`, processed, failed, status, batchID)
	return err
}

// markStagingStatementCommitted records the bank_statement_id after a commit.
func markStagingStatementCommitted(ctx context.Context, db *sql.DB, stagingID, bankStatementID string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE cimplrcorpsaas.pdf_staging_statement
		   SET status = 'committed', committed_bs_id = $1, updated_at = now()
		 WHERE staging_id = $2
	`, bankStatementID, stagingID)
	return err
}

// ── HTTP Handlers ─────────────────────────────────────────────────────────────

// GetStagingBatchHandler returns batch metadata and a summary of its statements.
// POST {"batch_id":"..."}
func GetStagingBatchHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			BatchID string `json:"batch_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.BatchID == "" {
			respondWithError(w, nil, "batch_id is required", http.StatusBadRequest)
			return
		}
		ctx := r.Context()

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
		err := db.QueryRowContext(ctx, `
			SELECT batch_id, user_id, source_filename, status,
			       total_files, processed_files, failed_files, created_at
			FROM cimplrcorpsaas.pdf_staging_batch
			WHERE batch_id = $1
		`, body.BatchID).Scan(&batchID, &userID, &sourceFilename, &status,
			&totalFiles, &processed, &failed, &createdAt)
		if err == sql.ErrNoRows {
			respondWithError(w, nil, "batch not found", http.StatusNotFound)
			return
		}
		if err != nil {
			respondWithError(w, err, "failed to fetch batch", http.StatusInternalServerError)
			return
		}

		rows, err := db.QueryContext(ctx, `
			SELECT staging_id, original_filename, csv_url, status, error_message,
			       committed_bs_id, created_at
			FROM cimplrcorpsaas.pdf_staging_statement
			WHERE batch_id = $1
			ORDER BY created_at
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
		}
		var stmts []stmtSummary
		for rows.Next() {
			var s stmtSummary
			var errMsg, committedID sql.NullString
			var csvURL sql.NullString
			var stmtCreatedAt time.Time
			if err := rows.Scan(&s.StagingID, &s.Filename, &csvURL, &s.Status, &errMsg, &committedID, &stmtCreatedAt); err != nil {
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
			stmts = append(stmts, s)
		}

		w.Header().Set("Content-Type", "application/json")
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
func GetStagingStatementHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			StagingID string `json:"staging_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.StagingID == "" {
			respondWithError(w, nil, "staging_id is required", http.StatusBadRequest)
			return
		}
		ctx := r.Context()

		var (
			stagingID    string
			batchID      sql.NullString
			filename     string
			csvURL       sql.NullString
			rawStmt      []byte
			status       string
			errMsg       sql.NullString
			committedID  sql.NullString
			createdAt    time.Time
		)
		err := db.QueryRowContext(ctx, `
			SELECT staging_id, batch_id, original_filename, csv_url,
			       raw_statement, status, error_message, committed_bs_id, created_at
			FROM cimplrcorpsaas.pdf_staging_statement
			WHERE staging_id = $1
		`, body.StagingID).Scan(&stagingID, &batchID, &filename, &csvURL,
			&rawStmt, &status, &errMsg, &committedID, &createdAt)
		if err == sql.ErrNoRows {
			respondWithError(w, nil, "statement not found", http.StatusNotFound)
			return
		}
		if err != nil {
			respondWithError(w, err, "failed to fetch statement", http.StatusInternalServerError)
			return
		}

		var rawStatementParsed interface{}
		if len(rawStmt) > 0 {
			if err := json.Unmarshal(rawStmt, &rawStatementParsed); err != nil {
				log.Printf("[staging] warn: failed to parse raw_statement for %s: %v", stagingID, err)
				rawStatementParsed = nil
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"staging_id":       stagingID,
				"batch_id":         batchID.String,
				"original_filename": filename,
				"csv_url":          csvURL.String,
				"raw_statement":    rawStatementParsed,
				"status":           status,
				"error_message":    errMsg.String,
				"committed_bs_id":  committedID.String,
				"created_at":       createdAt,
			},
		})
	})
}

// ListStagingByUserHandler returns all staged batches and their statement summaries
// for a given user_id. Supports GET ?user_id=... or POST {"user_id":"..."}.
func ListStagingByUserHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var userID string
		if r.Method == http.MethodGet {
			userID = r.URL.Query().Get("user_id")
		} else {
			var body struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
				userID = body.UserID
			}
		}
		if userID == "" {
			respondWithError(w, nil, "user_id is required", http.StatusBadRequest)
			return
		}
		ctx := r.Context()

		rows, err := db.QueryContext(ctx, `
			SELECT b.batch_id, b.source_filename, b.status,
			       b.total_files, b.processed_files, b.failed_files, b.created_at,
			       s.staging_id, s.original_filename, s.csv_url, s.status,
			       s.error_message, s.committed_bs_id, s.created_at
			FROM cimplrcorpsaas.pdf_staging_batch b
			LEFT JOIN cimplrcorpsaas.pdf_staging_statement s ON s.batch_id = b.batch_id
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
				batchID, srcFile, batchStatus                  string
				total, processed, failed                        int
				batchCreatedAt                                  time.Time
				stagingID, stmtFilename, stmtStatus            sql.NullString
				csvURL, errMsg, committedID                     sql.NullString
				stmtCreatedAt                                   sql.NullTime
			)
			if err := rows.Scan(
				&batchID, &srcFile, &batchStatus,
				&total, &processed, &failed, &batchCreatedAt,
				&stagingID, &stmtFilename, &csvURL, &stmtStatus,
				&errMsg, &committedID, &stmtCreatedAt,
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
					Status:    stmtStatus.String,
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
			batches = append(batches, batchMap[id])
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    batches,
		})
	})
}

// UpdateStagingStatementHandler lets a user update the raw_statement JSON
// (e.g. add/remove/edit transactions) before recalculating and committing.
// POST {"staging_id":"...", "raw_statement": {...}}
func UpdateStagingStatementHandler(db *sql.DB) http.Handler {
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
		res, err := db.ExecContext(ctx, `
			UPDATE cimplrcorpsaas.pdf_staging_statement
			   SET raw_statement = $1, status = 'parsed', updated_at = now()
			 WHERE staging_id = $2 AND status != 'committed'
		`, raw, body.StagingID)
		if err != nil {
			respondWithError(w, err, "failed to update statement", http.StatusInternalServerError)
			return
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			respondWithError(w, nil, "statement not found or already committed", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
	})
}
