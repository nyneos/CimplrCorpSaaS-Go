package exposures

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"math"
	"net/http"
	"strings"

	"github.com/lib/pq"
)

// Helper: send JSON error response
// func respondWithError(w http.ResponseWriter, status int, errMsg string) {
// 	w.Header().Set("Content-Type", "application/json")
// 	w.WriteHeader(status)
// 	json.NewEncoder(w).Encode(map[string]interface{}{
// 		"success": false,
// 		"error":   errMsg,
// 	})
// }

// Handler: HedgeLinksDetails
func HedgeLinksDetails(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct { UserID string `json:"user_id"` }
		ct := r.Header.Get("Content-Type")
		if strings.HasPrefix(ct, "application/json") {
			_ = json.NewDecoder(r.Body).Decode(&req)
		// } else if strings.HasPrefix(ct, "multipart/form-data") {
		// 	r.ParseMultipartForm(32 << 20)
		// 	req.UserID = r.FormValue("user_id")
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		rows, err := db.Query(`SELECT l.*, h.document_id, f.internal_reference_id FROM exposure_hedge_links l LEFT JOIN exposure_headers h ON l.exposure_header_id = h.exposure_header_id LEFT JOIN forward_bookings f ON l.booking_id = f.system_transaction_id WHERE h.entity = ANY($1) AND l.is_active = TRUE`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch hedge links details")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		data := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals { valPtrs[i] = &vals[i] }
			if err := rows.Scan(valPtrs...); err != nil { continue }
			rowMap := map[string]interface{}{}
			for i, col := range cols { rowMap[col] = vals[i] }
			data = append(data, rowMap)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

// Handler: ExpFwdLinkingBookings
func ExpFwdLinkingBookings(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct { UserID string `json:"user_id"` }
		ct := r.Header.Get("Content-Type")
		if strings.HasPrefix(ct, "application/json") {
			_ = json.NewDecoder(r.Body).Decode(&req)
		// } else if strings.HasPrefix(ct, "multipart/form-data") {
		// 	r.ParseMultipartForm(32 << 20)
		// 	req.UserID = r.FormValue("user_id")
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		bookRows, err := db.Query(`SELECT system_transaction_id, entity_level_0, order_type, quote_currency, maturity_date, booking_amount, counterparty_dealer FROM forward_bookings WHERE processing_status = 'approved' OR processing_status = 'Approved'`)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch bookings")
			return
		}
		defer bookRows.Close()
		bookCols, _ := bookRows.Columns()
		bookings := []map[string]interface{}{}
		for bookRows.Next() {
			vals := make([]interface{}, len(bookCols))
			valPtrs := make([]interface{}, len(bookCols))
			for i := range vals { valPtrs[i] = &vals[i] }
			bookRows.Scan(valPtrs...)
			row := map[string]interface{}{}
			for i, col := range bookCols { row[col] = vals[i] }
			if containsString(buNames, row["entity_level_0"].(string)) {
				bookings = append(bookings, row)
			}
		}
		bookingIds := []interface{}{}
		for _, b := range bookings {
			bookingIds = append(bookingIds, b["system_transaction_id"])
		}
		hedgeMap := map[interface{}]float64{}
		if len(bookingIds) > 0 {
			hedgeRows, err := db.Query(`SELECT booking_id, SUM(hedged_amount) AS linked_amount FROM exposure_hedge_links WHERE booking_id = ANY($1) GROUP BY booking_id`, pq.Array(bookingIds))
			if err == nil {
				for hedgeRows.Next() {
					var booking_id interface{}
					var linked_amount float64
					hedgeRows.Scan(&booking_id, &linked_amount)
					hedgeMap[booking_id] = linked_amount
				}
				hedgeRows.Close()
			}
		}
		buCompliance := map[string]bool{}
		buRows, err := db.Query(`SELECT entity_name FROM masterEntity WHERE (approval_status = 'Approved' OR approval_status = 'approved')`)
		if err == nil {
			for buRows.Next() {
				var name string
				buRows.Scan(&name)
				buCompliance[name] = true
			}
			buRows.Close()
		}
		response := []map[string]interface{}{}
		for _, b := range bookings {
			linkedAmount := hedgeMap[b["system_transaction_id"]]
			response = append(response, map[string]interface{}{
				"bu": b["entity_level_0"],
				"system_transaction_id": b["system_transaction_id"],
				"type": b["order_type"],
				"currency": b["quote_currency"],
				"maturity_date": b["maturity_date"],
				"amount": b["booking_amount"],
				"linked_amount": linkedAmount,
				"bu_unit_compliance": buCompliance[b["entity_level_0"].(string)],
				"Bank": b["counterparty_dealer"],
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// Handler: ExpFwdLinking
func ExpFwdLinking(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct { UserID string `json:"user_id"` }
		ct := r.Header.Get("Content-Type")
		if strings.HasPrefix(ct, "application/json") {
			_ = json.NewDecoder(r.Body).Decode(&req)
		// } else if strings.HasPrefix(ct, "multipart/form-data") {
		// 	r.ParseMultipartForm(32 << 20)
		// 	req.UserID = r.FormValue("user_id")
		}
		if req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "Please login to continue.")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusNotFound, "No accessible business units found")
			return
		}
		headRows, err := db.Query(`SELECT exposure_header_id, entity, exposure_type, currency, document_date, total_open_amount, counterparty_name FROM exposure_headers WHERE approval_status = 'Approved' OR approval_status = 'approved'`)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch exposure headers")
			return
		}
		defer headRows.Close()
		headCols, _ := headRows.Columns()
		headers := []map[string]interface{}{}
		for headRows.Next() {
			vals := make([]interface{}, len(headCols))
			valPtrs := make([]interface{}, len(headCols))
			for i := range vals { valPtrs[i] = &vals[i] }
			headRows.Scan(valPtrs...)
			row := map[string]interface{}{}
			for i, col := range headCols { row[col] = vals[i] }
			if containsString(buNames, row["entity"].(string)) {
				headers = append(headers, row)
			}
		}
		headerIds := []interface{}{}
		for _, h := range headers {
			headerIds = append(headerIds, h["exposure_header_id"])
		}
		hedgeMap := map[interface{}]float64{}
		if len(headerIds) > 0 {
			hedgeRows, err := db.Query(`SELECT exposure_header_id, SUM(hedged_amount) AS hedge_amount FROM exposure_hedge_links WHERE exposure_header_id = ANY($1) GROUP BY exposure_header_id`, pq.Array(headerIds))
			if err == nil {
				for hedgeRows.Next() {
					var exposure_header_id interface{}
					var hedge_amount float64
					hedgeRows.Scan(&exposure_header_id, &hedge_amount)
					hedgeMap[exposure_header_id] = hedge_amount
				}
				hedgeRows.Close()
			}
		}
		buCompliance := map[string]bool{}
		buRows, err := db.Query(`SELECT entity_name FROM masterEntity WHERE (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)`)
		if err == nil {
			for buRows.Next() {
				var name string
				buRows.Scan(&name)
				buCompliance[name] = true
			}
			buRows.Close()
		}
		response := []map[string]interface{}{}
		for _, h := range headers {
			hedgeAmount := hedgeMap[h["exposure_header_id"]]
			if hedgeAmount < float64(h["total_open_amount"].(float64)) {
				// Only show if hedgeAmount < total_open_amount
				response = append(response, map[string]interface{}{
					"bu": h["entity"],
					"exposure_header_id": h["exposure_header_id"],
					"type": h["exposure_type"],
					"currency": h["currency"],
					"maturity_date": h["document_date"],
					"amount": h["total_open_amount"],
					"hedge_amount": hedgeAmount,
					"bu_unit_compliance": buCompliance[h["entity"].(string)],
					"Bank": h["counterparty_name"],
				})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// Handler: LinkExposureHedge - upsert exposure_hedge_links and log to forward_booking_ledger
func LinkExposureHedge(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string  `json:"user_id"`
			ExposureHeaderID string  `json:"exposure_header_id"`
			BookingID        string  `json:"booking_id"`
			HedgedAmount     float64 `json:"hedged_amount"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || req.ExposureHeaderID == "" || req.BookingID == "" || req.HedgedAmount == 0 {
			respondWithError(w, http.StatusBadRequest, "user_id, exposure_header_id, booking_id, and hedged_amount are required")
			return
		}
		// Upsert exposure_hedge_links
		upsertQuery := `
			INSERT INTO exposure_hedge_links (exposure_header_id, booking_id, hedged_amount, is_active)
			VALUES ($1, $2, $3, true)
			ON CONFLICT (exposure_header_id, booking_id)
			DO UPDATE SET hedged_amount = EXCLUDED.hedged_amount, is_active = true
			RETURNING exposure_header_id, booking_id, hedged_amount, is_active`
		var link struct {
			ExposureHeaderID string
			BookingID        string
			HedgedAmount     float64
			IsActive         bool
		}
		err := db.QueryRow(upsertQuery, req.ExposureHeaderID, req.BookingID, req.HedgedAmount).Scan(
			&link.ExposureHeaderID,
			&link.BookingID,
			&link.HedgedAmount,
			&link.IsActive,
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to upsert exposure hedge link")
			return
		}
		linkMap := map[string]interface{}{
			"exposure_header_id": link.ExposureHeaderID,
			"booking_id":        link.BookingID,
			"hedged_amount":     link.HedgedAmount,
			"is_active":         link.IsActive,
		}
		// Get booking amount
		var bookingAmount float64
		_ = db.QueryRow("SELECT Booking_Amount FROM forward_bookings WHERE system_transaction_id = $1", req.BookingID).Scan(&bookingAmount)
		// Sum previous actions
		var totalUtilized float64
		sumQuery := `SELECT COALESCE(SUM(amount_changed), 0) FROM forward_booking_ledger WHERE booking_id = $1 AND action_type IN ('UTILIZATION', 'CANCELLATION', 'ROLLOVER')`
		_ = db.QueryRow(sumQuery, req.BookingID).Scan(&totalUtilized)
		newOpenAmount := math.Abs(math.Abs(bookingAmount) - math.Abs(totalUtilized))
		// Log to forward_booking_ledger
		ledgerQuery := `INSERT INTO forward_booking_ledger (booking_id, action_type, action_id, action_date, amount_changed, running_open_amount, user_id) VALUES ($1, 'UTILIZATION', $2, CURRENT_DATE, $3, $4, $5)`
		_, _ = db.Exec(ledgerQuery, req.BookingID, req.ExposureHeaderID, req.HedgedAmount, newOpenAmount, req.UserID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "link": linkMap})
	}
}

// Helper: containsString
func containsString(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}
