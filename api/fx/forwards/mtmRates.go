package forwards

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/xuri/excelize/v2"
)

// Helper: send JSON error response
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

func UploadMTMFiles(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var userID string
		ct := r.Header.Get("Content-Type")
		if strings.HasPrefix(ct, "multipart/form-data") {
			err := r.ParseMultipartForm(32 << 20)
			if err != nil {
				respondWithError(w, http.StatusBadRequest, "Failed to parse form-data")
				return
			}
			userID = r.FormValue("user_id")
		// } else if strings.HasPrefix(ct, "application/json") {
		// 	var bodyMap map[string]interface{}
		// 	_ = json.NewDecoder(r.Body).Decode(&bodyMap)
		// 	if uid, ok := bodyMap["user_id"].(string); ok {
		// 		userID = uid
		// 	}
		}
		if userID == "" {
			respondWithError(w, http.StatusBadRequest, "Missing user_id")
			return
		}

		// Get business units from middleware context
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}
		
		// Get files from multipart form
		form := r.MultipartForm
		files := form.File["files"]
		if len(files) == 0 {
			respondWithError(w, http.StatusBadRequest, "No files uploaded")
			return
		}

		results := []map[string]interface{}{}
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				results = append(results, map[string]interface{}{
					"filename": fileHeader.Filename,
					"error":    "Failed to open file",
				})
				continue
			}
			defer file.Close()

			ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
			var rowsData []map[string]interface{}
			if ext == ".csv" {
				reader := csv.NewReader(file)
				headers, err := reader.Read()
				if err != nil {
					results = append(results, map[string]interface{}{
						"filename": fileHeader.Filename,
						"error":    "Failed to read CSV headers",
					})
					continue
				}
				for {
					row, err := reader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						continue
					}
					obj := map[string]interface{}{}
					for i, h := range headers {
						obj[h] = row[i]
					}
					rowsData = append(rowsData, obj)
				}
			} else if ext == ".xls" || ext == ".xlsx" {
				xl, err := excelize.OpenReader(file)
				if err != nil {
					results = append(results, map[string]interface{}{
						"filename": fileHeader.Filename,
						"error":    "Failed to read Excel file",
					})
					continue
				}
				sheet := xl.GetSheetName(0)
				xRows, err := xl.GetRows(sheet)
				if err != nil || len(xRows) < 1 {
					results = append(results, map[string]interface{}{
						"filename": fileHeader.Filename,
						"error":    "No data in Excel file",
					})
					continue
				}
				headers := xRows[0]
				for _, row := range xRows[1:] {
					obj := map[string]interface{}{}
					for i, h := range headers {
						if i < len(row) {
							obj[h] = row[i]
						} else {
							obj[h] = nil
						}
					}
					rowsData = append(rowsData, obj)
				}
			} else {
				results = append(results, map[string]interface{}{
					"filename": fileHeader.Filename,
					"error":    "Unsupported file type",
				})
				continue
			}

			if len(rowsData) == 0 {
				results = append(results, map[string]interface{}{
					"filename": fileHeader.Filename,
					"error":    "No data to upload",
				})
				continue
			}

			var fileError error
			validRows := [][]interface{}{}
			refIds := []string{}
			for _, row := range rowsData {
				if v, ok := row["internal_reference_id"].(string); ok && v != "" {
					refIds = append(refIds, v)
				}
			}
			bookingMap := map[string]string{}
			bookingDetailsMap := map[string]map[string]interface{}{}
			bookingIdList := []string{}
			if len(refIds) > 0 {
				query := `SELECT system_transaction_id, internal_reference_id, order_type, booking_amount, maturity_date, total_rate, currency_pair FROM forward_bookings WHERE internal_reference_id = ANY($1)`
				rows, err := db.Query(query, pq.Array(refIds))
				if err == nil {
					for rows.Next() {
						var system_transaction_id, internal_reference_id, order_type, currency_pair string
						var booking_amount, total_rate float64
						var maturity_date string
						rows.Scan(&system_transaction_id, &internal_reference_id, &order_type, &booking_amount, &maturity_date, &total_rate, &currency_pair)
						bookingMap[internal_reference_id] = system_transaction_id
						bookingDetailsMap[internal_reference_id] = map[string]interface{}{
							"order_type":      order_type,
							"booking_amount":  booking_amount,
							"maturity_date":   maturity_date,
							"total_rate":      total_rate,
							"currency_pair":   currency_pair,
						}
						bookingIdList = append(bookingIdList, system_transaction_id)
					}
					rows.Close()
				}
			}
			ledgerMap := map[string]map[string]interface{}{}
			if len(bookingIdList) > 0 {
				query := `SELECT booking_id, running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = ANY($1)`
				rows, err := db.Query(query, pq.Array(bookingIdList))
				if err == nil {
					for rows.Next() {
						var booking_id string
						var running_open_amount float64
						var ledger_sequence int
						rows.Scan(&booking_id, &running_open_amount, &ledger_sequence)
						if lm, ok := ledgerMap[booking_id]; !ok || ledger_sequence > lm["ledger_sequence"].(int) {
							ledgerMap[booking_id] = map[string]interface{}{
								"running_open_amount": running_open_amount,
								"ledger_sequence":      ledger_sequence,
							}
						}
					}
					rows.Close()
				}
			}
			for i, row := range rowsData {
				entity, _ := row["entity"].(string)
				if !containsString(buNames, entity) {
					fileError = fmt.Errorf("business unit not allowed: %s (row %d)", entity, i+1)
					break
				}
				internalRef, _ := row["internal_reference_id"].(string)
				bookingId := bookingMap[internalRef]
				if bookingId == "" {
					fileError = fmt.Errorf("booking not found for internal_reference_id: %s (row %d)", internalRef, i+1)
					break
				}
				booking := bookingDetailsMap[internalRef]
				if booking == nil {
					fileError = fmt.Errorf("booking details not found for internal_reference_id: %s (row %d)", internalRef, i+1)
					break
				}
				openAmount := booking["booking_amount"].(float64)
				if lm, ok := ledgerMap[bookingId]; ok {
					openAmount = lm["running_open_amount"].(float64)
				}
				mismatchFields := []string{}
				if str(row["buy_sell"]) != str(booking["order_type"]) {
					mismatchFields = append(mismatchFields, "buy_sell/order_type")
				}
				if num(row["notional_amount"]) != openAmount {
					mismatchFields = append(mismatchFields, "notional_amount/open_amount")
				}
				if num(row["contract_rate"]) != booking["total_rate"].(float64) {
					mismatchFields = append(mismatchFields, "contract_rate/total_rate")
				}
				if str(row["currency_pair"]) != str(booking["currency_pair"]) {
					mismatchFields = append(mismatchFields, "currency_pair")
				}
				if len(mismatchFields) > 0 {
					fileError = fmt.Errorf("reconciliation failed for internal_reference_id: %s (row %d). Mismatched fields: %s", internalRef, i+1, strings.Join(mismatchFields, ", "))
					break
				}
				mtmRate := num(row["mtm_rate"])
				contractRate := num(row["contract_rate"])
				notionalAmount := num(row["notional_amount"])
				mtmValue := (mtmRate - contractRate) * notionalAmount
				dealDate := str(row["deal_date"])
				maturityDate := str(row["maturity_date"])
				daysToMaturity := calcDaysToMaturity(dealDate, maturityDate, row["days_to_maturity"])
				status := str(row["status"])
				if status == "" {
					status = "pending"
				}
				validRows = append(validRows, []interface{}{
					uuid.New().String(),
					bookingId,
					dealDate,
					maturityDate,
					str(row["currency_pair"]),
					str(row["buy_sell"]),
					notionalAmount,
					contractRate,
					mtmRate,
					mtmValue,
					daysToMaturity,
					status,
					internalRef,
					entity,
				})
			}
			if fileError != nil {
				results = append(results, map[string]interface{}{
					"filename": fileHeader.Filename,
					"error":    fileError.Error(),
				})
				continue
			}
			tx, err := db.Begin()
			if err != nil {
				results = append(results, map[string]interface{}{
					"filename": fileHeader.Filename,
					"error":    "Failed to start DB transaction",
				})
				continue
			}
			if len(validRows) > 0 {
				valueStrings := []string{}
				valueArgs := []interface{}{}
				for i, row := range validRows {
					valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*14+1, i*14+2, i*14+3, i*14+4, i*14+5, i*14+6, i*14+7, i*14+8, i*14+9, i*14+10, i*14+11, i*14+12, i*14+13, i*14+14))
					valueArgs = append(valueArgs, row...)
				}
				insertQuery := "INSERT INTO forward_mtm (mtm_id, booking_id, deal_date, maturity_date, currency_pair, buy_sell, notional_amount, contract_rate, mtm_rate, mtm_value, days_to_maturity, status, internal_reference_id, entity) VALUES " + strings.Join(valueStrings, ",")
				_, err := tx.Exec(insertQuery, valueArgs...)
				if err != nil {
					tx.Rollback()
					results = append(results, map[string]interface{}{
						"filename": fileHeader.Filename,
						"error":    "Failed to insert data: " + err.Error(),
					})
					continue
				}
			}
			err = tx.Commit()
			if err != nil {
				results = append(results, map[string]interface{}{
					"filename": fileHeader.Filename,
					"error":    "Failed to commit transaction: " + err.Error(),
				})
				continue
			}
			results = append(results, map[string]interface{}{
				"filename": fileHeader.Filename,
				"inserted": len(validRows),
			})
		}

		hasErrors := false
		for _, r := range results {
			if r["error"] != nil {
				hasErrors = true
				break
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": !hasErrors,
			"results": results,
		})
	}
}

// Helper functions
func containsString(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}
func str(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
func num(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	default:
		return 0
	}
}
func calcDaysToMaturity(dealDateStr, maturityDateStr string, fallback interface{}) int {
	layout := "2006-01-02"
	dealDate, err1 := time.Parse(layout, dealDateStr)
	maturityDate, err2 := time.Parse(layout, maturityDateStr)
	if err1 == nil && err2 == nil {
		days := int(maturityDate.Sub(dealDate).Hours() / 24)
		return days
	}
	if fallback != nil {
		switch t := fallback.(type) {
		case int:
			return t
		case float64:
			return int(t)
		case string:
			v, _ := strconv.Atoi(t)
			return v
		}
	}
	return 0
}

// Handler: GetMTMData - returns MTM data for allowed business units from middleware
func GetMTMData(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
            UserID string `json:"user_id"`
        }
        ct := r.Header.Get("Content-Type")
        if strings.HasPrefix(ct, "application/json") {
            _ = json.NewDecoder(r.Body).Decode(&req)
        }

		// Get business units from middleware context
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		// Fetch MTM data for allowed business units
		rows, err := db.Query(`SELECT * FROM forward_mtm WHERE entity = ANY($1)`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch MTM data")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		data := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = vals[i]
			}
			data = append(data, rowMap)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    data,
		})
	}
}