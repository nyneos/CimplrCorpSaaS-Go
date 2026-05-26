package bankstatement

import (
	"CimplrCorpSaas/api/constants"
	apipreval "CimplrCorpSaas/api/middlewares"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"

	"CimplrCorpSaas/internal/logger"
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

// insertStagingStatementParams carries row data for insertStagingStatement.
// RawStatement is the serialised RecalculateCleanData-shaped map (or nil if not yet parsed).
type insertStagingStatementParams struct {
	BatchID      string
	Filename     string
	CSVURL       string
	RawStatement interface{}
	Status       string
	ErrMsg       string
}

// insertStagingStatement creates a statement row inside a batch.
func insertStagingStatement(ctx context.Context, db *sql.DB, p insertStagingStatementParams) (string, error) {
	raw, err := json.Marshal(p.RawStatement)
	if err != nil {
		return "", fmt.Errorf("marshal raw_statement: %w", err)
	}
	var stagingID string
	var errMsgParam interface{}
	if p.ErrMsg != "" {
		errMsgParam = p.ErrMsg
	}
	err = db.QueryRowContext(ctx, `
		INSERT INTO cimplrcorpsaas.pdf_staging_statement
		    (batch_id, original_filename, csv_url, raw_statement, status, error_message)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING staging_id
	`, p.BatchID, p.Filename, p.CSVURL, raw, p.Status, errMsgParam).Scan(&stagingID)
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
		err := db.QueryRowContext(ctx, `
			SELECT batch_id, user_id, source_filename, status,
			       total_files, processed_files, failed_files, created_at
			FROM cimplrcorpsaas.pdf_staging_batch
			WHERE batch_id = $1 AND user_id = $2
		`, body.BatchID, sessionUID).Scan(&batchID, &userID, &sourceFilename, &status,
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
			  AND (committed_bs_id IS NULL OR trim(committed_bs_id::text) = '')
			  AND lower(coalesce(status, '')) <> 'committed'
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
			s.Status = stagingStatementDisplayStatus(s.Status, committedID)
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
		)
		err := db.QueryRowContext(ctx, `
			SELECT s.staging_id, s.batch_id, s.original_filename, s.csv_url,
			       s.raw_statement, s.status, s.error_message, s.committed_bs_id, s.created_at
			FROM cimplrcorpsaas.pdf_staging_statement s
			INNER JOIN cimplrcorpsaas.pdf_staging_batch b ON b.batch_id = s.batch_id
			WHERE s.staging_id = $1 AND b.user_id = $2
			  AND (s.committed_bs_id IS NULL OR trim(s.committed_bs_id::text) = '')
			  AND lower(coalesce(s.status, '')) <> 'committed'
		`, body.StagingID, sessionUID).Scan(&stagingID, &batchID, &filename, &csvURL,
			&rawStmt, &status, &errMsg, &committedID, &createdAt)
		if err == sql.ErrNoRows {
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"staging_id":        stagingID,
				"batch_id":          batchID.String,
				"original_filename": filename,
				"csv_url":           csvURL.String,
				"raw_statement":     rawStatementParsed,
				"status":            displayStatus,
				"error_message":     errMsg.String,
				"committed_bs_id":   committedID.String,
				"created_at":        createdAt,
			},
		})
	})
}

// ListStagingByUserHandler returns staged batches and non-committed statement summaries
// for the authenticated user only (user_id from session / prevalidation context).
// Committed statements are omitted; batches where every statement is committed are omitted entirely.
func ListStagingByUserHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := apipreval.GetUserIDFromContext(ctx)
		if userID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}

		rows, err := db.QueryContext(ctx, `
			SELECT b.batch_id, b.source_filename, b.status,
			       b.total_files, b.processed_files, b.failed_files, b.created_at,
			       s.staging_id, s.original_filename, s.csv_url, s.status,
			       s.error_message, s.committed_bs_id, s.created_at
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
				batchID, srcFile, batchStatus       string
				total, processed, failed            int
				batchCreatedAt                      time.Time
				stagingID, stmtFilename, stmtStatus sql.NullString
				csvURL, errMsg, committedID         sql.NullString
				stmtCreatedAt                       sql.NullTime
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
					Status:    stagingStatementDisplayStatus(stmtStatus.String, committedID),
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
		sessionUID := apipreval.GetUserIDFromContext(ctx)
		if sessionUID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}
		res, err := db.ExecContext(ctx, `
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
		n, _ := res.RowsAffected()
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
func DeleteStagingStatementHandler(db *sql.DB) http.Handler {
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
		err := db.QueryRowContext(ctx, `
			SELECT s.batch_id::text
			  FROM cimplrcorpsaas.pdf_staging_statement s
			  INNER JOIN cimplrcorpsaas.pdf_staging_batch b ON b.batch_id = s.batch_id
			 WHERE s.staging_id = $1 AND b.user_id = $2
		`, sid, sessionUID).Scan(&batchID)
		if err == sql.ErrNoRows {
			respondWithError(w, nil, constants.ErrStatementNotFound, http.StatusNotFound)
			return
		}
		if err != nil {
			respondWithError(w, err, "failed to resolve staging row", http.StatusInternalServerError)
			return
		}

		res, err := db.ExecContext(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_statement WHERE staging_id = $1`, sid)
		if err != nil {
			respondWithError(w, err, "failed to delete staging statement", http.StatusInternalServerError)
			return
		}
		if n, _ := res.RowsAffected(); n == 0 {
			respondWithError(w, nil, constants.ErrStatementNotFound, http.StatusNotFound)
			return
		}

		batchDeleted := false
		remaining := 0
		if batchID.Valid && strings.TrimSpace(batchID.String) != "" {
			bid := strings.TrimSpace(batchID.String)
			_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.pdf_staging_statement WHERE batch_id = $1`, bid).Scan(&remaining)
			if remaining == 0 {
				if _, err := db.ExecContext(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_batch WHERE batch_id = $1`, bid); err != nil {
					respondWithError(w, err, "failed to delete empty staging batch", http.StatusInternalServerError)
					return
				}
				batchDeleted = true
			}
		}

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
func DeleteStagingBatchHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		sessionUID := apipreval.GetUserIDFromContext(ctx)
		if sessionUID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}

		switch r.Method {
		case http.MethodGet:
			deleteStagingBatchGET(w, r, db, ctx, sessionUID)
		case http.MethodPost:
			deleteStagingBatchPOST(w, r, db, ctx, sessionUID)
		default:
			respondWithError(w, nil, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
		}
	})
}

func deleteStagingBatchGET(w http.ResponseWriter, r *http.Request, db *sql.DB, ctx context.Context, sessionUID string) {
	bid := strings.TrimSpace(r.URL.Query().Get("batch_id"))
	if bid == "" {
		respondWithError(w, nil, "batch_id query parameter is required", http.StatusBadRequest)
		return
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		respondWithError(w, err, "failed to start transaction", http.StatusInternalServerError)
		return
	}

	var batchExists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.pdf_staging_batch WHERE batch_id = $1 AND user_id = $2`, bid, sessionUID).Scan(&batchExists); err != nil {
		_ = tx.Rollback()
		respondWithError(w, err, "failed to verify batch", http.StatusInternalServerError)
		return
	}
	if batchExists == 0 {
		_ = tx.Rollback()
		respondWithError(w, nil, "batch not found", http.StatusNotFound)
		return
	}

	resSt, err := tx.ExecContext(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_statement WHERE batch_id = $1`, bid)
	if err != nil {
		_ = tx.Rollback()
		respondWithError(w, err, constants.ErrFailedToDeleteStagedStatements, http.StatusInternalServerError)
		return
	}
	stRemoved, _ := resSt.RowsAffected()

	if _, err := tx.ExecContext(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_batch WHERE batch_id = $1`, bid); err != nil {
		_ = tx.Rollback()
		respondWithError(w, err, "failed to delete staging batch", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		respondWithError(w, err, "failed to commit delete", http.StatusInternalServerError)
		return
	}

	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":            true,
		"message":            "Staging batch and linked statements deleted",
		"batch_id":           bid,
		"statements_deleted": stRemoved,
	})
}

func deleteStagingBatchPOST(w http.ResponseWriter, r *http.Request, db *sql.DB, ctx context.Context, sessionUID string) {
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

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		respondWithError(w, err, "failed to start transaction", http.StatusInternalServerError)
		return
	}

	delRows, err := tx.QueryContext(ctx, `
		DELETE FROM cimplrcorpsaas.pdf_staging_statement s
		USING cimplrcorpsaas.pdf_staging_batch b
		WHERE s.batch_id = b.batch_id
		  AND b.user_id = $1
		  AND s.staging_id::text = ANY($2::text[])
		RETURNING s.batch_id::text, s.staging_id::text
	`, sessionUID, pq.Array(ids))
	if err != nil {
		_ = tx.Rollback()
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
			_ = tx.Rollback()
			respondWithError(w, err, "failed to read delete result", http.StatusInternalServerError)
			return
		}
		deleted = append(deleted, delRow{batchID, stagingID})
	}
	if err := delRows.Err(); err != nil {
		_ = tx.Rollback()
		respondWithError(w, err, constants.ErrFailedToDeleteStagedStatements, http.StatusInternalServerError)
		return
	}

	if len(deleted) == 0 {
		_ = tx.Rollback()
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
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.pdf_staging_statement WHERE batch_id = $1`, bid).Scan(&remaining); err != nil {
			_ = tx.Rollback()
			respondWithError(w, err, "failed to count remaining statements", http.StatusInternalServerError)
			return
		}
		if remaining < 1 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_batch WHERE batch_id = $1`, bid); err != nil {
				_ = tx.Rollback()
				respondWithError(w, err, "failed to delete empty staging batch", http.StatusInternalServerError)
				return
			}
			batchesRemoved = append(batchesRemoved, bid)
		}
	}

	if err := tx.Commit(); err != nil {
		respondWithError(w, err, "failed to commit delete", http.StatusInternalServerError)
		return
	}

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
