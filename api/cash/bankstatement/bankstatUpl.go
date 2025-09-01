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
