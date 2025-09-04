package payablerecievable

import (
	"CimplrCorpSaas/api/auth"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"

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
func parseUploadFile(file multipart.File, ext string) ([][]string, error) {
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

// Helper: normalize date string to YYYY-MM-DD
func normalizeDate(dateStr string) string {
	layouts := []string{"2006-01-02", "02-01-2006", "01/02/2006", "2 Jan 2006", "2006/01/02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t.Format("2006-01-02")
		}
	}
	return dateStr // fallback, let DB error if invalid
}

// Handler: UploadPayRec (for payables/receivables)
func UploadPayRec(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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

		// Fetch user name from active sessions
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
		if len(r.MultipartForm.File) == 0 {
			http.Error(w, "No files uploaded", http.StatusBadRequest)
			return
		}

		// Read mapping
		mapRows, err := pgxPool.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_input_transactions`)
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

		batchIDs := make([]string, 0)
		for txType, files := range r.MultipartForm.File {
			txTypeUpper := strings.ToUpper(txType)
			for _, fileHeader := range files {
				file, err := fileHeader.Open()
				if err != nil {
					http.Error(w, "Failed to open file: "+fileHeader.Filename, http.StatusBadRequest)
					return
				}
				ext := getFileExt(fileHeader.Filename)
				records, err := parseUploadFile(file, ext)
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
					vals := make([]interface{}, colCount+2) // +2 for batchID and transaction_type
					vals[0] = batchID
					vals[1] = txTypeUpper
					for j := 0; j < colCount; j++ {
						val := ""
						if j < len(row) {
							val = row[j]
						}
						// Normalize date columns
						if mapping[headerRow[j]] == "invoice_date" || mapping[headerRow[j]] == "due_date" {
							val = normalizeDate(val)
						}
						vals[j+2] = val
					}
					copyRows[i] = vals
				}
				columns := append([]string{"upload_batch_id", "transaction_type"}, headerRow...)
				_, err = pgxPool.CopyFrom(
					ctx,
					pgx.Identifier{"input_transactions"},
					columns,
					pgx.CopyFromRows(copyRows),
				)
				if err != nil {
					http.Error(w, "Failed to stage data: "+err.Error(), http.StatusInternalServerError)
					return
				}

				// Move to final table(s)
				if txTypeUpper == "PAYABLE" {
					// Insert into payables and get payable_ids
					rows, err := pgxPool.Query(ctx, `
						INSERT INTO payables (entity_id, vendor_id, invoice_number, invoice_date, due_date, amount, currency_code)
						SELECT entity_id, vendor_id, invoice_number, invoice_date::date, due_date::date, amount::numeric, currency_code
						FROM input_transactions WHERE upload_batch_id = $1
						RETURNING payable_id
					`, batchID)
					if err != nil {
						http.Error(w, "Final insert error (payables): "+err.Error(), http.StatusInternalServerError)
						return
					}
					var payableIDs []string
					for rows.Next() {
						var payableID string
						if err := rows.Scan(&payableID); err == nil {
							payableIDs = append(payableIDs, payableID)
						}
					}
					rows.Close()
					if len(payableIDs) > 0 {
						// Bulk insert audit logs
						var auditValues []string
						for _, pid := range payableIDs {
							auditValues = append(auditValues, fmt.Sprintf("('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())", pid, userName))
						}
						auditSQL := "INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES " + strings.Join(auditValues, ",")
						_, auditErr := pgxPool.Exec(ctx, auditSQL)
						if auditErr != nil {
							http.Error(w, "Audit log error (payables): "+auditErr.Error(), http.StatusInternalServerError)
							return
						}
					}
				} else if txTypeUpper == "RECEIVABLE" {
					// Insert into receivables and get receivable_ids
					rows, err := pgxPool.Query(ctx, `
						INSERT INTO receivables (entity_id, customer_id, invoice_number, invoice_date, due_date, invoice_amount, currency_code)
						SELECT entity_id, customer_id, invoice_number, invoice_date::date, due_date::date, invoice_amount::numeric, currency_code
						FROM input_transactions WHERE upload_batch_id = $1
						RETURNING receivable_id
					`, batchID)
					if err != nil {
						http.Error(w, "Final insert error (receivables): "+err.Error(), http.StatusInternalServerError)
						return
					}
					var receivableIDs []string
					for rows.Next() {
						var receivableID string
						if err := rows.Scan(&receivableID); err == nil {
							receivableIDs = append(receivableIDs, receivableID)
						}
					}
					rows.Close()
					if len(receivableIDs) > 0 {
						// Bulk insert audit logs
						var auditValues []string
						for _, rid := range receivableIDs {
							auditValues = append(auditValues, fmt.Sprintf("('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())", rid, userName))
						}
						auditSQL := "INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES " + strings.Join(auditValues, ",")
						_, auditErr := pgxPool.Exec(ctx, auditSQL)
						if auditErr != nil {
							http.Error(w, "Audit log error (receivables): "+auditErr.Error(), http.StatusInternalServerError)
							return
						}
					}
				} else {
					http.Error(w, "Unknown transaction_type: "+txTypeUpper, http.StatusBadRequest)
					return
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "All transactions uploaded and processed",
		})
	}
}
