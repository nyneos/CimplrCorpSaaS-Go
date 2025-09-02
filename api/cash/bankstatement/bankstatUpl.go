package bankstatement

import (
	// ...existing imports...
	"CimplrCorpSaas/api/auth"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// Helper: get file extension
func getFileExt(filename string) string {
	return strings.ToLower(filepath.Ext(filename))
}

// Helper: parse uploaded file into [][]string
func parseBankStatementFile(file multipart.File, ext string) ([][]string, error) {
	if ext == ".csv" {
		r := csv.NewReader(file)
		return r.ReadAll()
	}
	if ext == ".xlsx" || ext == ".xls" {
		f, err := excelize.OpenReader(file)
		if err != nil {
			return nil, err
		}
		sheet := f.GetSheetName(0)
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	return nil, errors.New("unsupported file type")
}

// Handler: UploadBankStatement
func UploadBankStatement(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Parse user_id from body (JSON or form field)
		userID := ""
		if r.Header.Get("Content-Type") == "application/json" {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				http.Error(w, "user_id required in body", http.StatusBadRequest)
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue("user_id")
			if userID == "" {
				http.Error(w, "user_id required in form", http.StatusBadRequest)
				return
			}
		}

		// Fetch user name from active sessions (in-process)
		userName := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			http.Error(w, "User not found in active sessions", http.StatusUnauthorized)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, "Failed to parse multipart form", http.StatusBadRequest)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			http.Error(w, "No files uploaded", http.StatusBadRequest)
			return
		}
		batchIDs := make([]string, 0, len(files))
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				http.Error(w, "Failed to open file: "+fileHeader.Filename, http.StatusBadRequest)
				return
			}
			ext := getFileExt(fileHeader.Filename)
			records, err := parseBankStatementFile(file, ext)
			file.Close()
			if err != nil || len(records) < 2 {
				http.Error(w, "Invalid or empty file: "+fileHeader.Filename, http.StatusBadRequest)
				return
			}
			headerRow := records[0]
			dataRows := records[1:]
			batchID := uuid.New().String()
			batchIDs = append(batchIDs, batchID)
			colCount := len(headerRow)
			copyRows := make([][]interface{}, len(dataRows))
			for i, row := range dataRows {
				vals := make([]interface{}, colCount+1)
				vals[0] = batchID
				for j := 0; j < colCount; j++ {
					if j < len(row) {
						vals[j+1] = row[j]
					} else {
						vals[j+1] = nil
					}
				}
				copyRows[i] = vals
			}
			columns := append([]string{"upload_batch_id"}, headerRow...)
			_, err = pgxPool.CopyFrom(
				ctx,
				pgx.Identifier{"input_bank_statement_table"},
				columns,
				pgx.CopyFromRows(copyRows),
			)
			if err != nil {
				http.Error(w, "Failed to stage data: "+err.Error(), http.StatusInternalServerError)
				return
			}
			// Read mapping
			mapRows, err := pgxPool.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_bank_statement`)
			if err != nil {
				http.Error(w, "Mapping error", http.StatusInternalServerError)
				return
			}
			mapping := make(map[string]string)
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					mapping[src] = tgt
				}
			}
			mapRows.Close()
			var srcCols, tgtCols []string
			for src, tgt := range mapping {
				srcCols = append(srcCols, src)
				tgtCols = append(tgtCols, tgt)
			}
			// Add created_by to insert
			tgtCols = append(tgtCols, "createdby")
			srcColsStr := strings.Join(srcCols, ", ")
			tgtColsStr := strings.Join(tgtCols, ", ")

			// Move to final table (resolve FKs via join)
			_, err = pgxPool.Exec(ctx, fmt.Sprintf(`
			       INSERT INTO bank_statement (%s)
			       SELECT %s, $1
			       FROM input_bank_statement_table s
			       WHERE s.upload_batch_id = $2
		       `, tgtColsStr, srcColsStr), userName, batchID)
			if err != nil {
				http.Error(w, "Final insert error: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":   true,
			"batch_ids": batchIDs,
			"message":   "All bank statements uploaded and processed",
		})
	}
}

// Handler: GetBankStatements
// Returns all columns from bank_statement plus entity_name and bank_name joined
func GetBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		query := `
			SELECT 
				s.bankstatementid,
				s.entityid,
				s.account_number,
				s.statementdate,
				s.openingbalance,
				s.closingbalance,
				s.currencycode,
				s.transactiondate,
				s.description,
				s.debitamount,
				s.creditamount,
				s.balanceaftertxn,
				s.createdby,
				s.createdat,
				s.status,
				e.entity_name,
				b.bank_name
			FROM bank_statement s
			JOIN masterbankaccount mba ON s.account_number = mba.account_number
			JOIN masterentity e ON mba.entity_id = e.entity_id
			JOIN masterbank b ON mba.bank_id = b.bank_id
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		defer rows.Close()

		type Item struct {
			BankStatementID string    `json:"bankstatementid"`
			EntityID        string    `json:"entityid"`
			AccountNumber   string    `json:"account_number"`
			StatementDate   time.Time `json:"statementdate"`
			OpeningBalance  *float64  `json:"openingbalance"`
			ClosingBalance  *float64  `json:"closingbalance"`
			CurrencyCode    string    `json:"currencycode"`
			TransactionDate time.Time `json:"transactiondate"`
			Description     *string   `json:"description"`
			DebitAmount     *float64  `json:"debitamount"`
			CreditAmount    *float64  `json:"creditamount"`
			BalanceAfterTxn *float64  `json:"balanceaftertxn"`
			CreatedBy       *string   `json:"createdby"`
			CreatedAt       time.Time `json:"createdat"`
			Status          string    `json:"status"`
			EntityName      string    `json:"entity_name"`
			BankName        string    `json:"bank_name"`
		}

		results := make([]Item, 0)
		for rows.Next() {
			var it Item
			if err := rows.Scan(
				&it.BankStatementID,
				&it.EntityID,
				&it.AccountNumber,
				&it.StatementDate,
				&it.OpeningBalance,
				&it.ClosingBalance,
				&it.CurrencyCode,
				&it.TransactionDate,
				&it.Description,
				&it.DebitAmount,
				&it.CreditAmount,
				&it.BalanceAfterTxn,
				&it.CreatedBy,
				&it.CreatedAt,
				&it.Status,
				&it.EntityName,
				&it.BankName,
			); err != nil {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
				return
			}
			results = append(results, it)
		}
		if rows.Err() != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": rows.Err().Error()})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "data": results})
	}
}

// Bulk approve handler for bank statements
func BulkApproveBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bankstatement_ids"`
			// Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BankStatementIDs) == 0 {
			http.Error(w, "Invalid JSON or missing fields", http.StatusBadRequest)
			return
		}
		// Approve: if status is PENDING_DELETE_APPROVAL, delete; else set status to APPROVED
		// Delete records with PENDING_DELETE_APPROVAL
		delSQL := `DELETE FROM bank_statement WHERE bankstatementid = ANY($1) AND status = 'PENDING_DELETE_APPROVAL' RETURNING bankstatementid`
		delRows, delErr := pgxPool.Query(ctx, delSQL, req.BankStatementIDs)
		var deleted []string
		if delErr == nil {
			defer delRows.Close()
			for delRows.Next() {
				var id string
				delRows.Scan(&id)
				deleted = append(deleted, id)
			}
		}
		// Approve remaining (not deleted)
		approveSQL := `UPDATE bank_statement SET status = 'Approved' WHERE bankstatementid = ANY($1) AND status != 'PENDING_DELETE_APPROVAL' RETURNING bankstatementid`
		approveRows, approveErr := pgxPool.Query(ctx, approveSQL, req.BankStatementIDs)
		var approved []string
		if approveErr == nil {
			defer approveRows.Close()
			for approveRows.Next() {
				var id string
				approveRows.Scan(&id)
				approved = append(approved, id)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"approved": approved,
			"deleted":  deleted,
		})
	}
}

// Bulk reject handler for bank statements
func BulkRejectBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bankstatement_ids"`
			// Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BankStatementIDs) == 0 {
			http.Error(w, "Invalid JSON or missing fields", http.StatusBadRequest)
			return
		}
		rejectSQL := `UPDATE bank_statement SET status = 'REJECTED' WHERE bankstatementid = ANY($1) RETURNING bankstatementid`
		rows, err := pgxPool.Query(ctx, rejectSQL, req.BankStatementIDs)
		var rejected []string
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id string
				rows.Scan(&id)
				rejected = append(rejected, id)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"rejected": rejected,
		})
	}
}

// Bulk delete handler for bank statements (set status to PENDING_DELETE_APPROVAL)
func BulkDeleteBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bankstatement_ids"`
			// Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.BankStatementIDs) == 0 {
			http.Error(w, "Invalid JSON or missing fields", http.StatusBadRequest)
			return
		}
		delSQL := `UPDATE bank_statement SET status = 'PENDING_DELETE_APPROVAL' WHERE bankstatementid = ANY($1) RETURNING bankstatementid`
		rows, err := pgxPool.Query(ctx, delSQL, req.BankStatementIDs)
		var updated []string
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id string
				rows.Scan(&id)
				updated = append(updated, id)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":        true,
			"pending_delete": updated,
		})
	}
}
