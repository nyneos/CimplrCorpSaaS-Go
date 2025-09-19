package forwards

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"fmt"
	"strconv"
	"time"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/xuri/excelize/v2"
)

func NormalizeDate(dateStr string) string {
    dateStr = strings.TrimSpace(dateStr)
    if dateStr == "" {
        return ""
    }

    layouts := []string{
        "2006-01-02",
        "02-01-2006",
        "2006/01/02",
        "02/01/2006",
        "2006.01.02",
        "02.01.2006",
        time.RFC3339,
        "2006-01-02 15:04:05",
        "2006-01-02T15:04:05",
    }
	
	    layouts = append(layouts, []string{
        "02-Jan-2006",
        "02-Jan-06",
        "2-Jan-2006",
        "2-Jan-06",
        "02-Jan-2006 15:04:05",
    }...)

    for _, l := range layouts {
        if t, err := time.Parse(l, dateStr); err == nil {
            return t.Format("2006-01-02")
        }
    }

    return ""
}

func normalizeOrderType(orderType string) string {
	// Normalize input
	orderType = strings.TrimSpace(orderType)
	orderType = strings.ToLower(orderType)

	// Define mappings
	buyAliases := map[string]bool{
		"buy":      true,
		"purchase": true,
		"b":        true, // optional shorthand
	}

	sellAliases := map[string]bool{
		"sell": true,
		"sale": true,
		"s":    true, // optional shorthand
	}

	// Check mapping
	if buyAliases[orderType] {
		return "Buy"
	}
	if sellAliases[orderType] {
		return "Sell"
	}

	// Unknown type — keep original formatting
	// You may want to return "" or "Invalid" instead if you want strict validation
	return orderType
}


// Handler: AddForwardBookingManualEntry
func AddForwardBookingManualEntry(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                      string  `json:"user_id"`
			InternalReferenceID         string  `json:"internal_reference_id"`
			EntityLevel0                string  `json:"entity_level_0"`
			EntityLevel1                string  `json:"entity_level_1"`
			EntityLevel2                string  `json:"entity_level_2"`
			EntityLevel3                string  `json:"entity_level_3"`
			LocalCurrency               string  `json:"local_currency"`
			OrderType                   string  `json:"order_type"`
			TransactionType             string  `json:"transaction_type"`
			Counterparty                string  `json:"counterparty"`
			ModeOfDelivery              string  `json:"mode_of_delivery"`
			DeliveryPeriod              string  `json:"delivery_period"`
			AddDate                     string  `json:"add_date"`
			SettlementDate              string  `json:"settlement_date"`
			MaturityDate                string  `json:"maturity_date"`
			DeliveryDate                string  `json:"delivery_date"`
			CurrencyPair                string  `json:"currency_pair"`
			BaseCurrency                string  `json:"base_currency"`
			QuoteCurrency               string  `json:"quote_currency"`
			BookingAmount               float64 `json:"booking_amount"`
			ValueType                   string  `json:"value_type"`
			ActualValueBaseCurrency     float64 `json:"actual_value_base_currency"`
			SpotRate                    float64 `json:"spot_rate"`
			ForwardPoints               float64 `json:"forward_points"`
			BankMargin                  float64 `json:"bank_margin"`
			TotalRate                   float64 `json:"total_rate"`
			ValueQuoteCurrency          float64 `json:"value_quote_currency"`
			InterveningRateQuoteToLocal float64 `json:"intervening_rate_quote_to_local"`
			ValueLocalCurrency          float64 `json:"value_local_currency"`
			InternalDealer              string  `json:"internal_dealer"`
			CounterpartyDealer          string  `json:"counterparty_dealer"`
			Remarks                     string  `json:"remarks"`
			Narration                   string  `json:"narration"`
			TransactionTimestamp        string  `json:"transaction_timestamp"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		// Check entity_level_0 access
		found := false
		for _, bu := range buNames {
			if bu == req.EntityLevel0 {
				found = true
				break
			}
		}
		if !found {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "You do not have access to this business unit"})
			return
		}
		query := `INSERT INTO forward_bookings (
		       internal_reference_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, order_type, transaction_type, counterparty, mode_of_delivery, delivery_period, add_date, settlement_date, maturity_date, delivery_date, currency_pair, base_currency, quote_currency, booking_amount, value_type, actual_value_base_currency, spot_rate, forward_points, bank_margin, total_rate, value_quote_currency, intervening_rate_quote_to_local, value_local_currency, internal_dealer, counterparty_dealer, remarks, narration, transaction_timestamp, processing_status
	       ) VALUES (
		       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34
	       ) RETURNING internal_reference_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, order_type, transaction_type, counterparty, mode_of_delivery, delivery_period, add_date, settlement_date, maturity_date, delivery_date, currency_pair, base_currency, quote_currency, booking_amount, value_type, actual_value_base_currency, spot_rate, forward_points, bank_margin, total_rate, value_quote_currency, intervening_rate_quote_to_local, value_local_currency, internal_dealer, counterparty_dealer, remarks, narration, transaction_timestamp, processing_status`
		values := []interface{}{
			req.InternalReferenceID,
			req.EntityLevel0,
			req.EntityLevel1,
			req.EntityLevel2,
			req.EntityLevel3,
			req.LocalCurrency,
			req.OrderType,
			req.TransactionType,
			req.Counterparty,
			req.ModeOfDelivery,
			req.DeliveryPeriod,
			req.AddDate,
			req.SettlementDate,
			req.MaturityDate,
			req.DeliveryDate,
			req.CurrencyPair,
			req.BaseCurrency,
			req.QuoteCurrency,
			req.BookingAmount,
			req.ValueType,
			req.ActualValueBaseCurrency,
			req.SpotRate,
			req.ForwardPoints,
			req.BankMargin,
			req.TotalRate,
			req.ValueQuoteCurrency,
			req.InterveningRateQuoteToLocal,
			req.ValueLocalCurrency,
			req.InternalDealer,
			req.CounterpartyDealer,
			req.Remarks,
			req.Narration,
			req.TransactionTimestamp,
			"pending",
		}
		row := db.QueryRow(query, values...)
		cols := []string{"internal_reference_id", "entity_level_0", "entity_level_1", "entity_level_2", "entity_level_3", "local_currency", "order_type", "transaction_type", "counterparty", "mode_of_delivery", "delivery_period", "add_date", "settlement_date", "maturity_date", "delivery_date", "currency_pair", "base_currency", "quote_currency", "booking_amount", "value_type", "actual_value_base_currency", "spot_rate", "forward_points", "bank_margin", "total_rate", "value_quote_currency", "intervening_rate_quote_to_local", "value_local_currency", "internal_dealer", "counterparty_dealer", "remarks", "narration", "transaction_timestamp", "processing_status"}
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := row.Scan(valPtrs...); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		result := make(map[string]interface{})
		for i, col := range cols {
			result[col] = vals[i]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "data": result})
	}
}


// Handler: GetEntityRelevantForwardBookings
func GetEntityRelevantForwardBookings(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		rows, err := db.Query(`SELECT * FROM forward_bookings WHERE entity_level_0 = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		cols, _ := rows.Columns()
		var data []map[string]interface{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err == nil {
				rowMap := make(map[string]interface{})
				for i, col := range cols {
					v := vals[i]
					switch val := v.(type) {
					case nil:
						rowMap[col] = nil
					case []byte:
						// Try to convert []byte to string, float, or UUID
						s := string(val)
						// Try float
						if f, err := strconv.ParseFloat(s, 64); err == nil {
							rowMap[col] = f
						} else {
							rowMap[col] = s
						}
					case int64:
						rowMap[col] = val
					case float64:
						rowMap[col] = val
					case bool:
						rowMap[col] = val
					case string:
						rowMap[col] = val
					default:
						rowMap[col] = fmt.Sprintf("%v", val)
					}
				}
				data = append(data, rowMap)
			}
		}
		rows.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "data": data})
	}
}
// Example Forward Booking handler
func ForwardBooking(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
		// Add other booking fields as needed
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// TODO: Add session validation and booking logic here

	w.Write([]byte("Booking forwarded!"))
}


// Handler: UploadForwardBookingsMulti
// Multi-file upload for forward bookings (CSV/Excel)
// Accepts multipart/form-data with files, uses user_id from middleware, checks buName access

func UploadForwardBookingsMulti(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse multipart form
		err := r.ParseMultipartForm(32 << 20) // 32MB
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to parse form: " + err.Error()})
			return
		}
		files := r.MultipartForm.File["files"]
		if len(files) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No files uploaded"})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		results := []map[string]interface{}{}
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to open file", "details": err.Error()})
				continue
			}
			defer file.Close()
			ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
			var rows []map[string]interface{}
			if ext == ".csv" {
				reader := csv.NewReader(file)
				headers, err := reader.Read()
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to read CSV headers", "details": err.Error()})
					continue
				}
				for i, h := range headers {
					headers[i] = strings.TrimSpace(strings.TrimPrefix(h, "\uFEFF"))
				}
				for {
					record, err := reader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						continue
					}
					row := make(map[string]interface{})
					for i, h := range headers {
						row[h] = record[i]
					}
					rows = append(rows, row)
				}
			} else if ext == ".xls" || ext == ".xlsx" {
				tmpFile, err := os.CreateTemp("", "upload-*.xlsx")
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to create temp file", "details": err.Error()})
					continue
				}
				defer os.Remove(tmpFile.Name())
				_, err = io.Copy(tmpFile, file)
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to copy file", "details": err.Error()})
					tmpFile.Close()
					continue
				}
				tmpFile.Close()
				f, err := excelize.OpenFile(tmpFile.Name())
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to parse Excel file", "details": err.Error()})
					continue
				}
				sheetName := f.GetSheetName(0)
				rowsData, err := f.GetRows(sheetName)
				if err != nil || len(rowsData) < 2 {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "No data to upload"})
					continue
				}
				headers := rowsData[0]
				for _, record := range rowsData[1:] {
					row := make(map[string]interface{})
					for i, h := range headers {
						if i < len(record) {
							row[h] = record[i]
						} else {
							row[h] = nil
						}
					}
					rows = append(rows, row)
				}
			} else {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Unsupported file type"})
				continue
			}
			if len(rows) == 0 {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "No data to upload"})
				continue
			}
			successCount := 0
			errorRows := []map[string]interface{}{}
			invalidRows := []map[string]interface{}{}
			for i, r := range rows {
				entityLevel0, _ := r["entity_level_0"].(string)
				if !contains(buNames, entityLevel0) {
					invalidRows = append(invalidRows, map[string]interface{}{"row": i + 1, "entity": entityLevel0})
					continue
				}
				if ot, ok := r["order_type"].(string); ok {
					r["order_type"] = normalizeOrderType(ot)
				}
					
					for k, v := range r {
						// Normalize dates
						if isDateColumn(k) {
							if v == nil || v == "" {
								r[k] = nil
								continue
							}
							norm := NormalizeDate(fmt.Sprint(v))
							if norm == "" {
								r[k] = nil
							} else {
								r[k] = norm
							}
							continue
						}

						lower := strings.ToLower(k)
						if lower == "booking_amount" || lower == "actual_value_base_currency" || lower == "spot_rate" || lower == "forward_points" || lower == "bank_margin" || lower == "total_rate" || lower == "value_quote_currency" || lower == "intervening_rate_quote_to_local" || lower == "value_local_currency" {
							s := fmt.Sprint(v)
							s = strings.TrimSpace(s)
							if s == "" {
								r[k] = nil
							} else {
								if f, err := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64); err == nil {
									r[k] = f
								} else {
									r[k] = v
								}
							}
						}
					}
				query := `INSERT INTO forward_bookings (
					internal_reference_id, entity_level_0, entity_level_1, entity_level_2, entity_level_3, local_currency, order_type, transaction_type, counterparty, mode_of_delivery, delivery_period, add_date, settlement_date, maturity_date, delivery_date, currency_pair, base_currency, quote_currency, booking_amount, value_type, actual_value_base_currency, spot_rate, forward_points, bank_margin, total_rate, value_quote_currency, intervening_rate_quote_to_local, value_local_currency, internal_dealer, counterparty_dealer, remarks, narration, transaction_timestamp, processing_status
				) VALUES (
					$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34
				)`
				values := []interface{}{
					r["internal_reference_id"], r["entity_level_0"], r["entity_level_1"], r["entity_level_2"], r["entity_level_3"], r["local_currency"], r["order_type"], r["transaction_type"], r["counterparty"], r["mode_of_delivery"], r["delivery_period"], r["add_date"], r["settlement_date"], r["maturity_date"], r["delivery_date"], r["currency_pair"], r["base_currency"], r["quote_currency"], r["booking_amount"], r["value_type"], r["actual_value_base_currency"], r["spot_rate"], r["forward_points"], r["bank_margin"], r["total_rate"], r["value_quote_currency"], r["intervening_rate_quote_to_local"], r["value_local_currency"], r["internal_dealer"], r["counterparty_dealer"], r["remarks"], r["narration"], r["transaction_timestamp"], "pending",
				}
				_, err := db.Exec(query, values...)
				if err != nil {
					errorRows = append(errorRows, map[string]interface{}{"row": i + 1, "error": "Database insert error", "details": err.Error()})
				} else {
					successCount++
				}
			}
			results = append(results, map[string]interface{}{
				"filename": fileHeader.Filename,
				"inserted": successCount,
				"errors": errorRows,
				"invalidRows": invalidRows,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "results": results})
	}
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}


// Handler: UploadForwardConfirmationsMulti
// Multi-file upload for forward confirmations (CSV/Excel) - UPDATE existing records
// Place below imports and package declaration
func UploadForwardConfirmationsMulti(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseMultipartForm(32 << 20)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to parse form: " + err.Error()})
			return
		}
		files := r.MultipartForm.File["files"]
		if len(files) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No files uploaded"})
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		results := []map[string]interface{}{}
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to open file", "details": err.Error()})
				continue
			}
			defer file.Close()
			ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
			var rows []map[string]interface{}
			if ext == ".csv" {
				reader := csv.NewReader(file)
				headers, err := reader.Read()
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to read CSV headers", "details": err.Error()})
					continue
				}
				for i, h := range headers {
					headers[i] = strings.TrimSpace(strings.TrimPrefix(h, "\uFEFF"))
				}
				for {
					record, err := reader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						continue
					}
					row := make(map[string]interface{})
					for i, h := range headers {
						row[h] = record[i]
					}
					rows = append(rows, row)
				}
			} else if ext == ".xls" || ext == ".xlsx" {
				tmpFile, err := os.CreateTemp("", "upload-*.xlsx")
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to create temp file", "details": err.Error()})
					continue
				}
				defer os.Remove(tmpFile.Name())
				_, err = io.Copy(tmpFile, file)
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to copy file", "details": err.Error()})
					tmpFile.Close()
					continue
				}
				tmpFile.Close()
				f, err := excelize.OpenFile(tmpFile.Name())
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to parse Excel file", "details": err.Error()})
					continue
				}
				sheetName := f.GetSheetName(0)
				rowsData, err := f.GetRows(sheetName)
				if err != nil || len(rowsData) < 2 {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "No data to upload"})
					continue
				}
				headers := rowsData[0]
				for _, record := range rowsData[1:] {
					row := make(map[string]interface{})
					for i, h := range headers {
						if i < len(record) {
							row[h] = record[i]
						} else {
							row[h] = nil
						}
					}
					rows = append(rows, row)
				}
			} else {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Unsupported file type"})
				continue
			}
			if len(rows) == 0 {
				results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "No data to upload"})
				continue
			}
			successCount := 0
			errorRows := []map[string]interface{}{}
			invalidRows := []map[string]interface{}{}
			for i, r := range rows {
				entityLevel0, _ := r["entity_level_0"].(string)
				if !contains(buNames, entityLevel0) {
					invalidRows = append(invalidRows, map[string]interface{}{"row": i + 1, "entity": entityLevel0})
					continue
				}
				// Normalize bank_confirmation_date if provided
				if v, ok := r["bank_confirmation_date"]; ok {
					if v == nil || v == "" {
						r["bank_confirmation_date"] = nil
					} else {
						n := NormalizeDate(fmt.Sprint(v))
						if n == "" {
							r["bank_confirmation_date"] = nil
						} else {
							r["bank_confirmation_date"] = n
						}
					}
				}
				updateQuery := `UPDATE forward_bookings SET
					status = 'Confirmed',
					bank_transaction_id = $1,
					swift_unique_id = $2,
					bank_confirmation_date = $3,
					processing_status = 'pending'
				WHERE internal_reference_id = $4 AND status = 'Pending Confirmation' AND entity_level_0 = $5
				RETURNING *`
				updateValues := []interface{}{
					r["bank_transaction_id"],
					r["swift_unique_id"],
					r["bank_confirmation_date"],
					r["internal_reference_id"],
					r["entity_level_0"],
				}
				row := db.QueryRow(updateQuery, updateValues...)
				var dummy string
				err := row.Scan(&dummy)
				if err == nil {
					successCount++
				} else {
					errorRows = append(errorRows, map[string]interface{}{"row": i + 1, "error": "No matching record found or already confirmed"})
				}
			}
			results = append(results, map[string]interface{}{
				"filename": fileHeader.Filename,
				"updated": successCount,
				"errors": errorRows,
				"invalidRows": invalidRows,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "results": results})
	}
}



func UploadBankForwardBookingsMulti(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseMultipartForm(32 << 20)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to parse form: " + err.Error()})
			return
		}
		userID := r.FormValue("user_id")
		if userID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Please login to continue."})
			return
		}
		// sessions := getSessionsByUserId(userID)
		// if len(sessions) == 0 {
		// 	w.WriteHeader(http.StatusNotFound)
		// 	json.NewEncoder(w).Encode(map[string]interface{}{"error": "Session expired or not found. Please login again."})
		// 	return
		// }
		uploadBatchID := uuid.New().String()
		// Get staging table columns
		stagingCols := []string{}
		colRows, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'staging_bank_forward'`)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to fetch staging table columns"})
			return
		}
		defer colRows.Close()
		for colRows.Next() {
			var col string
			if err := colRows.Scan(&col); err == nil {
				stagingCols = append(stagingCols, col)
			}
		}
		results := []map[string]interface{}{}
		// Loop over file arrays (e.g., forward_axis, forward_hdfc)
		for arrayName, files := range r.MultipartForm.File {
			bankIdentifier := strings.ToUpper(strings.Replace(arrayName, "forward_", "", 1))
			for _, fileHeader := range files {
				file, err := fileHeader.Open()
				if err != nil {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to open file", "details": err.Error()})
					continue
				}
				ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
				var rows []map[string]interface{}
				if ext == ".csv" {
					reader := csv.NewReader(file)
					headers, err := reader.Read()
					if err != nil {
						results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to read CSV headers", "details": err.Error()})
						file.Close()
						continue
					}
					for i, h := range headers {
						headers[i] = strings.TrimSpace(strings.TrimPrefix(h, "\uFEFF"))
					}
					for {
						record, err := reader.Read()
						if err == io.EOF {
							break
						}
						if err != nil {
							continue
						}
						row := make(map[string]interface{})
						for i, h := range headers {
							row[h] = record[i]
						}
						rows = append(rows, row)
					}
				} else if ext == ".xls" || ext == ".xlsx" {
					tmpFile, err := os.CreateTemp("", "upload-*.xlsx")
					if err != nil {
						results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to create temp file", "details": err.Error()})
						file.Close()
						continue
					}
					_, err = io.Copy(tmpFile, file)
					if err != nil {
						results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to copy file", "details": err.Error()})
						tmpFile.Close()
						file.Close()
						continue
					}
					tmpFile.Close()
					f, err := excelize.OpenFile(tmpFile.Name())
					if err != nil {
						results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Failed to parse Excel file", "details": err.Error()})
						file.Close()
						continue
					}
					sheetName := f.GetSheetName(0)
					rowsData, err := f.GetRows(sheetName)
					if err != nil || len(rowsData) < 2 {
						results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "No data to upload"})
						file.Close()
						continue
					}
					headers := rowsData[0]
					for _, record := range rowsData[1:] {
						row := make(map[string]interface{})
						for i, h := range headers {
							if i < len(record) {
								row[h] = record[i]
							} else {
								row[h] = nil
							}
						}
						rows = append(rows, row)
					}
					os.Remove(tmpFile.Name())
				} else {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "Unsupported file type"})
					file.Close()
					continue
				}
				file.Close()
				if len(rows) == 0 {
					results = append(results, map[string]interface{}{"filename": fileHeader.Filename, "error": "No data to upload"})
					continue
				}
				// Insert into staging_bank_forward
				stagingSuccess := 0
				stagingErrors := []map[string]interface{}{}
				for i, r := range rows {
					insertObj := map[string]interface{}{
						"upload_batch_id": uploadBatchID,
						"bank_identifier": bankIdentifier,
						"bank_reference_id": getStringOrUUID(r["bank_reference_id"]),
						"row_number": i + 1,
						"source_reference_id": r["source_reference_id"],
						"raw_data": marshalJSON(r),
						"processing_status": "Pending",
					}
					// Add other columns from stagingCols
					for _, col := range stagingCols {
						if _, ok := insertObj[col]; ok {
							continue
						}
						value := r[col]
						// For date columns, normalize and set NULL if empty/invalid
						if isDateColumn(col) {
							if value == nil || value == "" {
								insertObj[col] = nil
							} else {
								norm := NormalizeDate(fmt.Sprint(value))
								if norm == "" {
									insertObj[col] = nil
								} else {
									insertObj[col] = norm
								}
							}
						} else if value != nil {
							insertObj[col] = value
						}
					}
					// Build dynamic insert
					fields := []string{}
					placeholders := []string{}
					values := []interface{}{}
					idx := 1
					for k, v := range insertObj {
						fields = append(fields, fmt.Sprintf("\"%s\"", k))
						placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
						values = append(values, v)
						idx++
					}
					_, err := db.Exec(fmt.Sprintf("INSERT INTO staging_bank_forward (%s) VALUES (%s)", strings.Join(fields, ", "), strings.Join(placeholders, ", ")), values...)
					if err != nil {
						stagingErrors = append(stagingErrors, map[string]interface{}{"row": i + 1, "error": err.Error()})
					} else {
						stagingSuccess++
					}
				}
				// Mapping and insert into forward_bookings
				bookingSuccess := 0
				bookingErrors := []map[string]interface{}{}
				// Get all mappings for this bank
				mappings := []map[string]interface{}{}
				mapRows, err := db.Query(`SELECT * FROM fwd_mapping_bank_forward WHERE bank_identifier = $1 AND is_active = true`, bankIdentifier)
				if err == nil {
					defer mapRows.Close()
					cols, _ := mapRows.Columns()
					for mapRows.Next() {
						vals := make([]interface{}, len(cols))
						valPtrs := make([]interface{}, len(cols))
						for i := range vals {
							valPtrs[i] = &vals[i]
						}
						if err := mapRows.Scan(valPtrs...); err == nil {
							m := map[string]interface{}{}
							for i, col := range cols {
								m[col] = vals[i]
							}
							mappings = append(mappings, m)
						}
					}
				} else {
					bookingErrors = append(bookingErrors, map[string]interface{}{"error": "Failed to fetch mapping: " + err.Error()})
				}
				for i, r := range rows {
					booking := map[string]interface{}{}
					unmapped := map[string]interface{}{}
					for key, val := range r {
						found := false
						for _, m := range mappings {
							if m["source_field"] == key && m["target_table"] == "forward_bookings" {
								// Transformation logic can be added here
								booking[m["target_field"].(string)] = val
								found = true
								break
							}
						}
						if !found {
							unmapped[key] = val
						}
					}
					booking["additional_bank_details"] = marshalJSON(unmapped)
					// Required fields fallback
					booking["entity_level_0"] = getStringOrDefault(booking["entity_level_0"], r["entity_level_0"])
					booking["maturity_date"] = getStringOrDefault(booking["maturity_date"], r["maturity_date"])
					booking["currency_pair"] = getStringOrDefault(booking["currency_pair"], r["currency_pair"])
					booking["order_type"] = getStringOrDefault(booking["order_type"], r["order_type"])
					booking["booking_amount"] = getStringOrDefault(booking["booking_amount"], r["booking_amount"])
					booking["total_rate"] = getStringOrDefault(booking["total_rate"], r["total_rate"])
					booking["processing_status"] = "pending"
					// Normalize date fields in booking before insert
					for k, v := range booking {
						if isDateColumn(k) {
							if v == nil || v == "" {
								booking[k] = nil
							} else {
								norm := NormalizeDate(fmt.Sprint(v))
								if norm == "" {
									booking[k] = nil
								} else {
									booking[k] = norm
								}
							}
						}
					}

					// Build dynamic insert
					fields := []string{}
					placeholders := []string{}
					values := []interface{}{}
					idx := 1
					for k, v := range booking {
						fields = append(fields, k)
						placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
						values = append(values, v)
						idx++
					}
					_, err := db.Exec(fmt.Sprintf("INSERT INTO forward_bookings (%s) VALUES (%s)", strings.Join(fields, ", "), strings.Join(placeholders, ", ")), values...)
					if err != nil {
						bookingErrors = append(bookingErrors, map[string]interface{}{"row": i + 1, "error": err.Error()})
					} else {
						bookingSuccess++
					}
				}
				results = append(results, map[string]interface{}{
					"filename": fileHeader.Filename,
					"staging_inserted": stagingSuccess,
					"staging_errors": stagingErrors,
					"bookings_inserted": bookingSuccess,
					"booking_errors": bookingErrors,
				})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "batch_id": uploadBatchID, "results": results})
	}
}

// Helper: marshal map to JSON
func marshalJSON(m map[string]interface{}) []byte {
	b, _ := json.Marshal(m)
	return b
}

// Helper: get string or generate UUID
func getStringOrUUID(val interface{}) string {
	s, ok := val.(string)
	if ok && s != "" {
		return s
	}
	return uuid.New().String()
}

// Helper: get string or fallback
func getStringOrDefault(a interface{}, b interface{}) interface{} {
	if a != nil && a != "" {
		return a
	}
	return b
}

// Helper: check if column is a date column
func isDateColumn(col string) bool {
	dateCols := []string{"trade_date", "booking_date", "expiry_date", "delivery_from_date", "delivery_to_date", "option_start_date", "maturity_date", "processed_at", "loaded_at", "add_date", "settlement_date", "delivery_date", "bank_confirmation_date", "transaction_timestamp"}
	for _, d := range dateCols {
		if strings.EqualFold(col, d) {
			return true
		}
	}
	return false
}
