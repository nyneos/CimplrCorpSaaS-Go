package bankbalances

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// Helper: get file extension
func ubGetFileExt(filename string) string {
	return strings.ToLower(filepath.Ext(filename))
}

// Helper: parse uploaded file into [][]string
func ubParseUploadFile(file multipart.File, ext string) ([][]string, error) {
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
	return nil, errors.New(constants.ErrUnsupportedFileType)
}

// Helper: normalize date string to YYYY-MM-DD
func normalizeDate(dateStr string) string {
	layouts := []string{constants.DateFormat, constants.DateFormatAlt, "01/02/2006", "2 Jan 2006", "2006/01/02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t.Format(constants.DateFormat)
		}
	}
	return dateStr // fallback, let DB error if invalid
}

// Helper: normalize time string to HH:MM:SS
func normalizeTime(timeStr string) string {
	layouts := []string{"15:04:05", "15:04", "3:04PM", "3:04 PM", "15.04.05"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, timeStr); err == nil {
			return t.Format("15:04:05")
		}
	}
	return timeStr
}

// Handler: UploadBankBalances
func UploadBankBalances(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		userID := r.FormValue(constants.KeyUserID)
		if userID == "" {
			// also allow JSON body with user_id
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
				userID = req.UserID
			}
		}
		if userID == "" {
			http.Error(w, constants.ErrUserIDRequired, http.StatusBadRequest)
			return
		}

		// verify session
		sessions := auth.GetActiveSessions()
		userName := ""
		for _, s := range sessions {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			http.Error(w, constants.ErrInvalidSession, http.StatusUnauthorized)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, constants.ErrFailedToParseMultipartForm, http.StatusBadRequest)
			return
		}

		batchIDs := make([]string, 0)
		for _, files := range r.MultipartForm.File {
			for _, fileHeader := range files {
				file, err := fileHeader.Open()
				if err != nil {
					continue
				}
				ext := ubGetFileExt(fileHeader.Filename)
				records, err := ubParseUploadFile(file, ext)
				file.Close()
				if err != nil || len(records) < 2 {
					continue
				}
				headerRow := records[0]
				// prepare normalized header keys for lookup (trim, lower, spaces->underscores)
				colCount := len(headerRow)
				headersNorm := make([]string, colCount)
				for i, h := range headerRow {
					hn := strings.TrimSpace(h)
					hn = strings.Trim(hn, ", \t\n\r")
					hn = strings.Trim(hn, "'\"`")
					hn = strings.ToLower(hn)
					hn = strings.ReplaceAll(hn, " ", "_")
					headersNorm[i] = hn
				}
				dataRows := records[1:]
				batchID := uuid.New().String()
				batchIDs = append(batchIDs, batchID)
				copyRows := make([][]interface{}, len(dataRows))
				for i, row := range dataRows {
					vals := make([]interface{}, 0, colCount+1)
					vals = append(vals, batchID)
					// normalize row to header length
					// fields that should be stored as numeric (NULL when empty)
					numericFields := map[string]bool{
						"balance_amount":  true,
						"opening_balance": true,
						"total_credits":   true,
						"total_debits":    true,
						"closing_balance": true,
					}
					for j := 0; j < colCount; j++ {
						cell := ""
						if j < len(row) {
							cell = strings.TrimSpace(row[j])
						}
						key := headersNorm[j]
						// normalize date/time
						if key == "as_of_date" {
							cell = normalizeDate(cell)
						}
						if key == "as_of_time" {
							cell = normalizeTime(cell)
						}

						// sanitize account_no: Excel sometimes writes large numbers in scientific notation
						if key == "account_no" && cell != "" {
							if strings.ContainsAny(cell, "Ee") {
								if f, err := strconv.ParseFloat(cell, 64); err == nil {
									// prefer integer representation when possible
									s := strconv.FormatFloat(f, 'f', -1, 64)
									s = strings.TrimSuffix(s, ".0")
									cell = s
								}
							}
						}

						// numeric fields -> pass as nil or float64
						if numericFields[key] {
							if cell == "" {
								vals = append(vals, nil)
							} else {
								// strip common thousand separators
								clean := strings.ReplaceAll(cell, ",", "")
								if f, err := strconv.ParseFloat(clean, 64); err == nil {
									vals = append(vals, f)
								} else {
									// fallback: send raw string, DB may cast or error
									vals = append(vals, cell)
								}
							}
						} else {
							if cell == "" {
								vals = append(vals, nil)
							} else {
								vals = append(vals, cell)
							}
						}
					}
					copyRows[i] = vals
				}
				columns := append([]string{"upload_batch_id"}, headersNorm...)
				if _, err = pgxPool.CopyFrom(
					ctx,
					pgx.Identifier{"input_bank_balance_table"},
					columns,
					pgx.CopyFromRows(copyRows),
				); err != nil {
					continue
				}
				tx, err := pgxPool.Begin(ctx)
				if err != nil {
					api.RespondWithError(w, http.StatusInternalServerError, "failed to start db transaction: "+err.Error())
					return
				}
				mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_bank_balance`)
				if err != nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, "mapping read error: "+err.Error())
					return
				}
				mapping := make(map[string]string)
				for mapRows.Next() {
					var src, tgt string
					if err := mapRows.Scan(&src, &tgt); err == nil {
						key := strings.TrimSpace(strings.ToLower(src))
						key = strings.ReplaceAll(key, " ", "_")
						mapping[key] = tgt
					}
				}
				mapRows.Close()

				var selectExprs []string
				var tgtCols []string
				for _, hn := range headersNorm {
					if tgt, ok := mapping[hn]; ok && tgt != "" {
						selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", hn, tgt))
						tgtCols = append(tgtCols, tgt)
					}
				}
				hasBalanceID := false
				for _, c := range tgtCols {
					if c == "balance_id" {
						hasBalanceID = true
						break
					}
				}
				if !hasBalanceID {
					selectExprs = append([]string{"('BBAL-' || lpad((floor(random()*1000000))::int::text,6,'0')) AS balance_id"}, selectExprs...)
					tgtCols = append([]string{"balance_id"}, tgtCols...)
				}

				if len(tgtCols) == 0 || len(selectExprs) == 0 {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusBadRequest, "no mapped columns found for upload file")
					return
				}

				var stagedCount int
				if err := tx.QueryRow(ctx, `SELECT count(*) FROM input_bank_balance_table WHERE upload_batch_id = $1`, batchID).Scan(&stagedCount); err != nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, "failed to count staged rows: "+err.Error())
					return
				}
				if stagedCount == 0 {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("no staged rows found for batch %s", batchID))
					return
				}

				tgtColsStr := strings.Join(tgtCols, ", ")
				srcColsStr := strings.Join(selectExprs, ", ")

				insertSQL := fmt.Sprintf(`
					INSERT INTO bank_balances_manual (%s)
					SELECT %s
					FROM input_bank_balance_table s
					WHERE s.upload_batch_id = $1
					RETURNING balance_id
				`, tgtColsStr, srcColsStr)

				rows, err := tx.Query(ctx, insertSQL, batchID)
				if err != nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, "final insert error: "+err.Error())
					return
				}
				var insertedIDs []string
				for rows.Next() {
					var id string
					if err := rows.Scan(&id); err == nil {
						insertedIDs = append(insertedIDs, id)
					}
				}
				rows.Close()
				if len(insertedIDs) == 0 {
					tx.Rollback(ctx)
					msg := fmt.Sprintf("staged rows present for batch %s but no rows inserted into final table (staged=%d)", batchID, stagedCount)
					api.RespondWithError(w, http.StatusInternalServerError, msg)
					return
				}
				for _, bid := range insertedIDs {
					if _, err := tx.Exec(ctx, `INSERT INTO auditactionbankbalances (balance_id, actiontype, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`, bid, userName); err != nil {
						tx.Rollback(ctx)
						api.RespondWithError(w, http.StatusInternalServerError, "failed to create audit action: "+err.Error())
						return
					}
				}

				if err := tx.Commit(ctx); err != nil {
					tx.Rollback(ctx)
					api.RespondWithError(w, http.StatusInternalServerError, constants.ErrCommitFailed+err.Error())
					return
				}
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"batch_ids":            batchIDs,
		})
	}
}
