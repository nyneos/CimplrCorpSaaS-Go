<<<<<<< HEAD
package cfo

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/lib/pq"
)

func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Println("[ERROR]", errMsg)
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: false,
		constants.ValueError:   errMsg,
	})
}

func GetAvgForwardMaturity(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		rows, err := db.Query(`
    SELECT 
        fb.system_transaction_id AS booking_id,
        fb.base_currency,
        fb.maturity_date,
        ABS(fb.maturity_date::date - CURRENT_DATE) AS days_to_maturity,
        COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount
    FROM forward_bookings fb
    LEFT JOIN LATERAL (
        SELECT fbl.running_open_amount
        FROM forward_booking_ledger fbl
        WHERE fbl.booking_id = fb.system_transaction_id
        ORDER BY fbl.ledger_sequence DESC
        LIMIT 1
    ) fbl ON TRUE
    WHERE fb.maturity_date IS NOT NULL
      AND fb.entity_level_0 = ANY($1)
      AND LOWER(fb.processing_status) = 'approved'
`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrDB)
			return
		}
		defer rows.Close()
		var weightedSum, totalAmount float64
		for rows.Next() {
			var bookingID string
			var currency string
			var maturityDate time.Time
			var daysToMaturity int
			var amount float64

			if err := rows.Scan(&bookingID, &currency, &maturityDate, &daysToMaturity, &amount); err != nil {
				continue
			}

			rate := rates[strings.ToUpper(currency)]
			if rate == 0 {
				rate = 1.0
			}

			usdAmount := math.Abs(amount) * rate
			weightedSum += usdAmount * float64(daysToMaturity)
			totalAmount += usdAmount
		}
		avgMaturity := 0
		if totalAmount > 0 {
			avgMaturity = int(weightedSum/totalAmount + 0.5)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"avgForwardMaturity": avgMaturity})
	}
}

func GetForwardBuySellTotals(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		rows, err := db.Query(`
			SELECT 
				fb.system_transaction_id AS booking_id,
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency,
				fb.order_type
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.booking_amount IS NOT NULL
			  AND fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrDB)
			return
		}
		defer rows.Close()

		var buyTotal, sellTotal float64

		for rows.Next() {
			var bookingID string
			var amount float64
			var currency, orderType string

			if err := rows.Scan(&bookingID, &amount, &currency, &orderType); err != nil {
				continue
			}

			// convert to USD using rates map
			rate := rates[strings.ToUpper(currency)]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := math.Abs(amount) * rate

			if strings.ToUpper(orderType) == "BUY" {
				buyTotal += usdAmount
			} else if strings.ToUpper(orderType) == "SELL" {
				sellTotal += usdAmount
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"buyForwardsUSD":  format2f(buyTotal),
			"sellForwardsUSD": format2f(sellTotal),
		})
	}
}

// Handler: GetUserCurrency
func GetUserCurrency(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		var buName string
		if err := db.QueryRow("SELECT business_unit_name FROM users WHERE id = $1  AND (status = 'Approved' OR status = 'approved')", req.UserID).Scan(&buName); err != nil {
			respondWithError(w, http.StatusNotFound, "User not found")
			return
		}
		if buName == "" {
			respondWithError(w, http.StatusNotFound, "User has no business unit assigned")
			return
		}
		var defaultCurrency string
		if err := db.QueryRow("SELECT default_currency FROM masterentity WHERE entity_name = $1", buName).Scan(&defaultCurrency); err != nil {
			respondWithError(w, http.StatusNotFound, "No entity found for given business unit")
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		// json.NewEncoder(w).Encode(map[string]interface{}{"defaultCurrency": defaultCurrency})
		json.NewEncoder(w).Encode(map[string]any{"defaultCurrency": defaultCurrency})
	}
}

// Handler: GetActiveForwardsCount
func GetActiveForwardsCount(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
			// Route: dash/cfo/fwd/bu-maturity-currency-summary
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		now := time.Now().Format(constants.DateFormat)
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM forward_bookings WHERE maturity_date > $1 AND entity_level_0 = ANY($2)  AND (processing_status = 'Approved' OR processing_status = 'approved')", now, pq.Array(buNames)).Scan(&count)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error fetching active forwards count")
			return
			// Route: dash/cfo/fwd/active-forwards-count
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{"ActiveForward": count})
	}
}

func GetRecentTradesDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		now := time.Now()
		sevenDaysAgo := now.AddDate(0, 0, -7).Format(constants.DateFormat)
		nowStr := now.Format(constants.DateFormat)

		rows, err := db.Query(`
			SELECT 
				fb.system_transaction_id AS booking_id,
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency,
				fb.currency_pair,
				fb.counterparty,
				fb.maturity_date
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.maturity_date >= $1
			  AND fb.maturity_date <= $2
			  AND fb.entity_level_0 = ANY($3)
			  AND LOWER(fb.processing_status) = 'approved'
		`, sevenDaysAgo, nowStr, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error fetching recent trades dashboard")
			return
		}
		defer rows.Close()

		var totalTrades int
		var totalVolume float64
		bankMap := map[string]struct {
			Pair   string
			Bank   string
			Amount float64
		}{}

		for rows.Next() {
			var bookingID string
			var amount float64
			var currency, pair, bank string
			var maturityDate time.Time

			if err := rows.Scan(&bookingID, &amount, &currency, &pair, &bank, &maturityDate); err != nil {
				continue
			}

			amount = math.Abs(amount)
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}

			amountUsd := amount * rate
			totalTrades++
			totalVolume += amountUsd

			pairLabel := strings.TrimSpace(pair) + " Forward"
			if bank == "" {
				bank = "Unknown Bank"
			}
			key := bank + "__" + pairLabel

			if _, exists := bankMap[key]; !exists {
				bankMap[key] = struct {
					Pair   string
					Bank   string
					Amount float64
				}{Pair: pairLabel, Bank: bank, Amount: 0}
			}

			entry := bankMap[key]
			entry.Amount += amountUsd
			bankMap[key] = entry
		}

		formatAmount := func(amt float64) string {
			if amt >= 1e6 {
				return "$" + format2f(amt/1e6) + "M"
			}
			if amt >= 1e3 {
				return "$" + format2f(amt/1e3) + "K"
			}
			return "$" + format2f(amt)
		}

		banks := []map[string]interface{}{}
		for _, b := range bankMap {
			banks = append(banks, map[string]interface{}{
				"pair":   b.Pair,
				"bank":   b.Bank,
				"amount": formatAmount(b.Amount),
			})
		}

		response := map[string]interface{}{
			"Total Trades": map[string]interface{}{"value": totalTrades},
			"Total Volume": map[string]interface{}{"value": formatAmount(totalVolume)},
			"Avg Trade Size": map[string]interface{}{"value": func() string {
				if totalTrades > 0 {
					return formatAmount(totalVolume / float64(totalTrades))
				}
				return "$0"
			}()},
			"BANKS": banks,
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(response)
	}
}

// Helper: format2f
func format2f(x float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.4f", x), "0"), ".")
}

// Helper: contains
func contains(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}

func GetTotalUsdSumDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		rows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrDB)
			return
		}
		defer rows.Close()

		totalUsd := 0.0
		for rows.Next() {
			var amount float64
			var currency string
			if err := rows.Scan(&amount, &currency); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := math.Abs(amount) * rate
			totalUsd += usdAmount
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"totalUsdSum": format2f(totalUsd),
		})
	}
}

func GetOpenAmountToBookingRatioDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// -------- OPEN AMOUNT ----------
		openRows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND fb.status = 'OPEN'
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error (open)")
			return
		}
		defer openRows.Close()

		openUsd := 0.0
		for openRows.Next() {
			var amount float64
			var currency string
			if err := openRows.Scan(&amount, &currency); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := math.Abs(amount) * rate
			openUsd += usdAmount
		}

		// -------- TOTAL BOOKED AMOUNT ----------
		totalRows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error (total)")
			return
		}
		defer totalRows.Close()

		totalUsd := 0.0
		for totalRows.Next() {
			var amount float64
			var currency string
			if err := totalRows.Scan(&amount, &currency); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := math.Abs(amount) * rate
			totalUsd += usdAmount
		}

		// -------- RATIO ----------
		ratio := 0.0
		if totalUsd > 0 {
			ratio = openUsd / totalUsd
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"openToBookingRatio": format2f(ratio),
			"openAmountUsd":      format2f(openUsd),
			"totalBookedUsd":     format2f(totalUsd),
		})
	}
}

func GetTotalUsdSumByCurrencyDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		rows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrDB)
			return
		}
		defer rows.Close()

		currencyTotals := map[string]float64{}

		for rows.Next() {
			var amount float64
			var currency string
			if err := rows.Scan(&amount, &currency); err != nil {
				continue
			}

			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}

			usdAmount := math.Abs(amount) * rate
			currencyTotals[currency] += usdAmount
		}

		// Prepare response
		result := []map[string]interface{}{}
		for currency, total := range currencyTotals {
			result = append(result, map[string]interface{}{
				"currency": currency,
				"totalUsd": format2f(total),
			})
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(result)
	}
}

func GetForwardBookingMaturityBucketsDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		rows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency,
				fb.delivery_period
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrDB)
			return
		}
		defer rows.Close()

		// Map: internal keys -> human-readable labels
		bucketLabels := map[string]string{
			"month_1":     "1 Month",
			"month_2":     "2 Month",
			"month_3":     "3 Month",
			"month_4":     "4 Month",
			"month_4_6":   "4-6 Month",
			"month_6plus": "6 Month +",
		}

		normalizeDeliveryPeriod := func(period string) string {
			if period == "" {
				return "month_1"
			}
			p := strings.ToLower(period)
			p = strings.ReplaceAll(p, " ", "")
			p = strings.ReplaceAll(p, "-", "")
			p = strings.ReplaceAll(p, "_", "")

			switch {
			case contains([]string{"1m", "1month", "month1", "m1", "mon1"}, p):
				return "month_1"
			case contains([]string{"2m", "2month", "month2", "m2", "mon2"}, p):
				return "month_2"
			case contains([]string{"3m", "3month", "month3", "m3", "mon3"}, p):
				return "month_3"
			case contains([]string{"4m", "4month", "month4", "m4", "mon4"}, p):
				return "month_4"
			case contains([]string{"46m", "4to6month", "month46", "month4to6", "m46", "mon46", "4_6month", "4_6m", "4-6m", "4-6month"}, p):
				return "month_4_6"
			case contains([]string{"6mplus", "6monthplus", "month6plus", "6plus", "m6plus", "mon6plus", "6m+", "6month+", "month6+"}, p):
				return "month_6plus"
			}

			// fallback heuristics
			if strings.Contains(p, "6") {
				return "month_6plus"
			}
			if strings.Contains(p, "4") {
				return "month_4"
			}
			if strings.Contains(p, "3") {
				return "month_3"
			}
			if strings.Contains(p, "2") {
				return "month_2"
			}
			return "month_1"
		}

		// Totals by bucket
		bucketTotals := map[string]float64{}
		for rows.Next() {
			var amount float64
			var currency, deliveryPeriod string
			if err := rows.Scan(&amount, &currency, &deliveryPeriod); err != nil {
				continue
			}

			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}

			usdAmount := math.Abs(amount) * rate
			bucketKey := normalizeDeliveryPeriod(deliveryPeriod)
			bucketTotals[bucketKey] += usdAmount
		}

		// Prepare response
		result := []map[string]interface{}{}
		for bucket, total := range bucketTotals {
			label := bucketLabels[bucket]
			if label == "" {
				label = "1 Month"
			}
			result = append(result, map[string]interface{}{
				"maturityBucket": label,
				"totalUsd":       format2f(total),
			})
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(result)
	}
}

// Handler: GetRolloverCountsByCurrency
func GetRolloverCountsByCurrency(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		rows, err := db.Query(`SELECT fb.quote_currency, COUNT(fr.rollover_id) AS rollover_count FROM forward_bookings fb LEFT JOIN forward_rollovers fr ON fr.booking_id = fb.system_transaction_id WHERE fb.entity_level_0 = ANY($1)  AND (fb.processing_status = 'Approved' OR fb.processing_status = 'approved') GROUP BY fb.quote_currency`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error fetching rollover counts")
			return
		}
		defer rows.Close()
		total := 0
		data := []map[string]string{}
		for rows.Next() {
			var currency string
			var count int
			if err := rows.Scan(&currency, &count); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			total += count
			if currency != "" {
				data = append(data, map[string]string{
					"label": currency + " Rollovers:",
					"value": fmt.Sprintf("%d", count),
				})
			}
		}
		// Add total at the top
		data = append([]map[string]string{{"label": "Total Rollovers:", "value": fmt.Sprintf("%d", total)}}, data...)
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(data)
	}
}

func GetBankTradesData(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// Ledger-aware query: prefer latest ledger.running_open_amount, fallback to booking_amount
		rows, err := db.Query(`
			SELECT 
				fb.counterparty,
				fb.order_type,
				fb.base_currency,
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error fetching bank trades data")
			return
		}
		defer rows.Close()

		// Bank -> Trade -> Amount(USD)
		bankMap := map[string]map[string]float64{}

		for rows.Next() {
			var bank, orderType, baseCurrency string
			var effectiveAmount float64
			if err := rows.Scan(&bank, &orderType, &baseCurrency, &effectiveAmount); err != nil {
				continue
			}

			if bank == "" {
				bank = "Unknown Bank"
			}

			// Normalize trade label (e.g., "BUY USD", "SELL EUR")
			trade := strings.TrimSpace(orderType) + " " + strings.TrimSpace(baseCurrency)
			trade = strings.ReplaceAll(trade, "  ", " ")
			trade = strings.TrimSpace(trade)

			// Convert to USD
			currency := strings.ToUpper(baseCurrency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			amountUsd := math.Abs(effectiveAmount) * rate

			if bankMap[bank] == nil {
				bankMap[bank] = map[string]float64{}
			}
			bankMap[bank][trade] += amountUsd
		}

		// Build response
		forwardsData := []map[string]interface{}{}
		for bank, trades := range bankMap {
			tradeNames := []string{}
			amounts := []string{}

			for trade, amt := range trades {
				tradeNames = append(tradeNames, trade)

				if amt >= 1e6 {
					amounts = append(amounts, "$"+format2f(amt/1e6)+"M")
				} else if amt >= 1e3 {
					amounts = append(amounts, "$"+format2f(amt/1e3)+"K")
				} else {
					amounts = append(amounts, "$"+format2f(amt))
				}
			}

			forwardsData = append(forwardsData, map[string]interface{}{
				"bank":    bank,
				"trades":  tradeNames,
				"amounts": amounts,
			})
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(forwardsData)
	}
}

func GetMaturityBucketsDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// Ledger-aware query
		rows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency,
				fb.maturity_date
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.maturity_date IS NOT NULL
			  AND fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrDB)
			return
		}
		defer rows.Close()

		now := time.Now()
		buckets := map[string]struct {
			Amount    float64
			Contracts int
		}{
			"Next 30 Days": {},
			"31-90 Days":   {},
			"91-180 Days":  {},
			"180+ Days":    {},
		}

		for rows.Next() {
			var amount float64
			var currency string
			var maturityDate time.Time
			if err := rows.Scan(&amount, &currency, &maturityDate); err != nil {
				continue
			}

			amount = math.Abs(amount)
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			amountUsd := amount * rate

			diffDays := int(math.Ceil(maturityDate.Sub(now).Hours() / 24))
			var bucket string
			if diffDays <= 30 {
				bucket = "Next 30 Days"
			} else if diffDays <= 90 {
				bucket = "31-90 Days"
			} else if diffDays <= 180 {
				bucket = "91-180 Days"
			} else if diffDays > 180 {
				bucket = "180+ Days"
			}

			if bucket != "" {
				b := buckets[bucket]
				b.Amount += amountUsd
				b.Contracts++
				buckets[bucket] = b
			}
		}

		formatAmount := func(amt float64) string {
			if amt >= 1e6 {
				return fmt.Sprintf("$%.1fM", amt/1e6)
			}
			if amt >= 1e3 {
				return fmt.Sprintf("$%.1fK", amt/1e3)
			}
			return fmt.Sprintf("$%.0f", amt)
		}

		response := map[string]map[string]string{}
		for key, val := range buckets {
			response[key] = map[string]string{
				"amount":    formatAmount(val.Amount),
				"contracts": fmt.Sprintf("%d Contracts", val.Contracts),
			}
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(response)
	}
}

// Handler: GetTotalBankMarginFromForwardBookings
func GetTotalBankMarginFromForwardBookings(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}
		rows, err := db.Query(`SELECT bank_margin, quote_currency FROM forward_bookings WHERE entity_level_0 = ANY($1) AND bank_margin IS NOT NULL AND (processing_status = 'Approved' OR processing_status = 'approved')`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, constants.ErrDB)
			return
		}
		defer rows.Close()
		totalBankMargin := 0.0
		for rows.Next() {
			var margin float64
			var currency string
			if err := rows.Scan(&margin, &currency); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			totalBankMargin += margin * rate
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]float64{"totalBankmargin": totalBankMargin})
	}
}

// Handler: GetOpenAmountToBookingRatioSimple
func GetOpenAmountToBookingRatioSimple(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		var totalOpen, totalBooking float64

		err := db.QueryRow(`
			WITH booking_totals AS (
				SELECT 
					COALESCE(SUM(ABS(eh.total_open_amount)), 0) AS total_open
				FROM exposure_headers eh
				WHERE eh.entity = ANY($1)
			),
			booking_amounts AS (
				SELECT 
					COALESCE(SUM(ABS(
						COALESCE(fbl.running_open_amount, fb.booking_amount)
					)), 0) AS total_booking
				FROM forward_bookings fb
				LEFT JOIN LATERAL (
					SELECT fbl.running_open_amount
					FROM forward_booking_ledger fbl
					WHERE fbl.booking_id = fb.system_transaction_id
					ORDER BY fbl.ledger_sequence DESC
					LIMIT 1
				) fbl ON TRUE
				WHERE fb.entity_level_0 = ANY($1)
				  AND LOWER(fb.processing_status) = 'approved'
			)
			SELECT bt.total_open, ba.total_booking
			FROM booking_totals bt, booking_amounts ba
		`, pq.Array(buNames)).Scan(&totalOpen, &totalBooking)

		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error calculating open amount to booking ratio")
			return
		}

		ratio := 0.0
		if math.Abs(totalBooking) > 0 {
			ratio = math.Abs(totalOpen) / math.Abs(totalBooking) * 100
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ratio": fmt.Sprintf("%.3f", ratio),
		})
	}
}
=======
package cfo

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"
)

func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Println("[ERROR]", errMsg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

func GetAvgForwardMaturity(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}
		rows, err := db.Query(`
    SELECT 
        fb.system_transaction_id AS booking_id,
        fb.base_currency,
        fb.maturity_date,
        ABS(fb.maturity_date::date - CURRENT_DATE) AS days_to_maturity,
        COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount
    FROM forward_bookings fb
    LEFT JOIN LATERAL (
        SELECT fbl.running_open_amount
        FROM forward_booking_ledger fbl
        WHERE fbl.booking_id = fb.system_transaction_id
        ORDER BY fbl.ledger_sequence DESC
        LIMIT 1
    ) fbl ON TRUE
    WHERE fb.maturity_date IS NOT NULL
      AND fb.entity_level_0 = ANY($1)
      AND LOWER(fb.processing_status) = 'approved'
`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
			return
		}
		defer rows.Close()
		var weightedSum, totalAmount float64
		for rows.Next() {
			var bookingID string
			var currency string
			var maturityDate time.Time
			var daysToMaturity int
			var amount float64

			if err := rows.Scan(&bookingID, &currency, &maturityDate, &daysToMaturity, &amount); err != nil {
				continue
			}

			rate := rates[strings.ToUpper(currency)]
			if rate == 0 {
				rate = 1.0
			}

			usdAmount := math.Abs(amount) * rate
			weightedSum += usdAmount * float64(daysToMaturity)
			totalAmount += usdAmount
		}
		avgMaturity := 0
		if totalAmount > 0 {
			avgMaturity = int(weightedSum/totalAmount + 0.5)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"avgForwardMaturity": avgMaturity})
	}
}

func GetForwardBuySellTotals(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		rows, err := db.Query(`
			SELECT 
				fb.system_transaction_id AS booking_id,
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency,
				fb.order_type
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.booking_amount IS NOT NULL
			  AND fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
			return
		}
		defer rows.Close()

		var buyTotal, sellTotal float64

		for rows.Next() {
			var bookingID string
			var amount float64
			var currency, orderType string

			if err := rows.Scan(&bookingID, &amount, &currency, &orderType); err != nil {
				continue
			}

			// convert to USD using rates map
			rate := rates[strings.ToUpper(currency)]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := math.Abs(amount) * rate

			if strings.ToUpper(orderType) == "BUY" {
				buyTotal += usdAmount
			} else if strings.ToUpper(orderType) == "SELL" {
				sellTotal += usdAmount
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"buyForwardsUSD":  format2f(buyTotal),
			"sellForwardsUSD": format2f(sellTotal),
		})
	}
}

// Handler: GetUserCurrency
func GetUserCurrency(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}
		var buName string
		if err := db.QueryRow("SELECT business_unit_name FROM users WHERE id = $1  AND (status = 'Approved' OR status = 'approved')", req.UserID).Scan(&buName); err != nil {
			respondWithError(w, http.StatusNotFound, "User not found")
			return
		}
		if buName == "" {
			respondWithError(w, http.StatusNotFound, "User has no business unit assigned")
			return
		}
		var defaultCurrency string
		if err := db.QueryRow("SELECT default_currency FROM masterentity WHERE entity_name = $1", buName).Scan(&defaultCurrency); err != nil {
			respondWithError(w, http.StatusNotFound, "No entity found for given business unit")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// json.NewEncoder(w).Encode(map[string]interface{}{"defaultCurrency": defaultCurrency})
		json.NewEncoder(w).Encode(map[string]any{"defaultCurrency": defaultCurrency})
	}
}

// Handler: GetActiveForwardsCount
func GetActiveForwardsCount(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
			// Route: dash/cfo/fwd/bu-maturity-currency-summary
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}
		now := time.Now().Format("2006-01-02")
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM forward_bookings WHERE maturity_date > $1 AND entity_level_0 = ANY($2)  AND (processing_status = 'Approved' OR processing_status = 'approved')", now, pq.Array(buNames)).Scan(&count)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error fetching active forwards count")
			return
			// Route: dash/cfo/fwd/active-forwards-count
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"ActiveForward": count})
	}
}

func GetRecentTradesDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		now := time.Now()
		sevenDaysAgo := now.AddDate(0, 0, -7).Format("2006-01-02")
		nowStr := now.Format("2006-01-02")

		rows, err := db.Query(`
			SELECT 
				fb.system_transaction_id AS booking_id,
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency,
				fb.currency_pair,
				fb.counterparty,
				fb.maturity_date
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.maturity_date >= $1
			  AND fb.maturity_date <= $2
			  AND fb.entity_level_0 = ANY($3)
			  AND LOWER(fb.processing_status) = 'approved'
		`, sevenDaysAgo, nowStr, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error fetching recent trades dashboard")
			return
		}
		defer rows.Close()

		var totalTrades int
		var totalVolume float64
		bankMap := map[string]struct {
			Pair   string
			Bank   string
			Amount float64
		}{}

		for rows.Next() {
			var bookingID string
			var amount float64
			var currency, pair, bank string
			var maturityDate time.Time

			if err := rows.Scan(&bookingID, &amount, &currency, &pair, &bank, &maturityDate); err != nil {
				continue
			}

			amount = math.Abs(amount)
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}

			amountUsd := amount * rate
			totalTrades++
			totalVolume += amountUsd

			pairLabel := strings.TrimSpace(pair) + " Forward"
			if bank == "" {
				bank = "Unknown Bank"
			}
			key := bank + "__" + pairLabel

			if _, exists := bankMap[key]; !exists {
				bankMap[key] = struct {
					Pair   string
					Bank   string
					Amount float64
				}{Pair: pairLabel, Bank: bank, Amount: 0}
			}

			entry := bankMap[key]
			entry.Amount += amountUsd
			bankMap[key] = entry
		}

		formatAmount := func(amt float64) string {
			if amt >= 1e6 {
				return "$" + format2f(amt/1e6) + "M"
			}
			if amt >= 1e3 {
				return "$" + format2f(amt/1e3) + "K"
			}
			return "$" + format2f(amt)
		}

		banks := []map[string]interface{}{}
		for _, b := range bankMap {
			banks = append(banks, map[string]interface{}{
				"pair":   b.Pair,
				"bank":   b.Bank,
				"amount": formatAmount(b.Amount),
			})
		}

		response := map[string]interface{}{
			"Total Trades": map[string]interface{}{"value": totalTrades},
			"Total Volume": map[string]interface{}{"value": formatAmount(totalVolume)},
			"Avg Trade Size": map[string]interface{}{"value": func() string {
				if totalTrades > 0 {
					return formatAmount(totalVolume / float64(totalTrades))
				}
				return "$0"
			}()},
			"BANKS": banks,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// Helper: format2f
func format2f(x float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.4f", x), "0"), ".")
}

// Helper: contains
func contains(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}

func GetTotalUsdSumDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		rows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
			return
		}
		defer rows.Close()

		totalUsd := 0.0
		for rows.Next() {
			var amount float64
			var currency string
			if err := rows.Scan(&amount, &currency); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := math.Abs(amount) * rate
			totalUsd += usdAmount
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"totalUsdSum": format2f(totalUsd),
		})
	}
}

func GetOpenAmountToBookingRatioDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		// -------- OPEN AMOUNT ----------
		openRows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND fb.status = 'OPEN'
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error (open)")
			return
		}
		defer openRows.Close()

		openUsd := 0.0
		for openRows.Next() {
			var amount float64
			var currency string
			if err := openRows.Scan(&amount, &currency); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := math.Abs(amount) * rate
			openUsd += usdAmount
		}

		// -------- TOTAL BOOKED AMOUNT ----------
		totalRows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error (total)")
			return
		}
		defer totalRows.Close()

		totalUsd := 0.0
		for totalRows.Next() {
			var amount float64
			var currency string
			if err := totalRows.Scan(&amount, &currency); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := math.Abs(amount) * rate
			totalUsd += usdAmount
		}

		// -------- RATIO ----------
		ratio := 0.0
		if totalUsd > 0 {
			ratio = openUsd / totalUsd
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"openToBookingRatio": format2f(ratio),
			"openAmountUsd":      format2f(openUsd),
			"totalBookedUsd":     format2f(totalUsd),
		})
	}
}

func GetTotalUsdSumByCurrencyDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		rows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
			return
		}
		defer rows.Close()

		currencyTotals := map[string]float64{}

		for rows.Next() {
			var amount float64
			var currency string
			if err := rows.Scan(&amount, &currency); err != nil {
				continue
			}

			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}

			usdAmount := math.Abs(amount) * rate
			currencyTotals[currency] += usdAmount
		}

		// Prepare response
		result := []map[string]interface{}{}
		for currency, total := range currencyTotals {
			result = append(result, map[string]interface{}{
				"currency": currency,
				"totalUsd": format2f(total),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

func GetForwardBookingMaturityBucketsDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		rows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency,
				fb.delivery_period
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
			return
		}
		defer rows.Close()

		// Map: internal keys -> human-readable labels
		bucketLabels := map[string]string{
			"month_1":     "1 Month",
			"month_2":     "2 Month",
			"month_3":     "3 Month",
			"month_4":     "4 Month",
			"month_4_6":   "4-6 Month",
			"month_6plus": "6 Month +",
		}

		normalizeDeliveryPeriod := func(period string) string {
			if period == "" {
				return "month_1"
			}
			p := strings.ToLower(period)
			p = strings.ReplaceAll(p, " ", "")
			p = strings.ReplaceAll(p, "-", "")
			p = strings.ReplaceAll(p, "_", "")

			switch {
			case contains([]string{"1m", "1month", "month1", "m1", "mon1"}, p):
				return "month_1"
			case contains([]string{"2m", "2month", "month2", "m2", "mon2"}, p):
				return "month_2"
			case contains([]string{"3m", "3month", "month3", "m3", "mon3"}, p):
				return "month_3"
			case contains([]string{"4m", "4month", "month4", "m4", "mon4"}, p):
				return "month_4"
			case contains([]string{"46m", "4to6month", "month46", "month4to6", "m46", "mon46", "4_6month", "4_6m", "4-6m", "4-6month"}, p):
				return "month_4_6"
			case contains([]string{"6mplus", "6monthplus", "month6plus", "6plus", "m6plus", "mon6plus", "6m+", "6month+", "month6+"}, p):
				return "month_6plus"
			}

			// fallback heuristics
			if strings.Contains(p, "6") {
				return "month_6plus"
			}
			if strings.Contains(p, "4") {
				return "month_4"
			}
			if strings.Contains(p, "3") {
				return "month_3"
			}
			if strings.Contains(p, "2") {
				return "month_2"
			}
			return "month_1"
		}

		// Totals by bucket
		bucketTotals := map[string]float64{}
		for rows.Next() {
			var amount float64
			var currency, deliveryPeriod string
			if err := rows.Scan(&amount, &currency, &deliveryPeriod); err != nil {
				continue
			}

			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}

			usdAmount := math.Abs(amount) * rate
			bucketKey := normalizeDeliveryPeriod(deliveryPeriod)
			bucketTotals[bucketKey] += usdAmount
		}

		// Prepare response
		result := []map[string]interface{}{}
		for bucket, total := range bucketTotals {
			label := bucketLabels[bucket]
			if label == "" {
				label = "1 Month"
			}
			result = append(result, map[string]interface{}{
				"maturityBucket": label,
				"totalUsd":       format2f(total),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

// Handler: GetRolloverCountsByCurrency
func GetRolloverCountsByCurrency(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}
		rows, err := db.Query(`SELECT fb.quote_currency, COUNT(fr.rollover_id) AS rollover_count FROM forward_bookings fb LEFT JOIN forward_rollovers fr ON fr.booking_id = fb.system_transaction_id WHERE fb.entity_level_0 = ANY($1)  AND (fb.processing_status = 'Approved' OR fb.processing_status = 'approved') GROUP BY fb.quote_currency`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error fetching rollover counts")
			return
		}
		defer rows.Close()
		total := 0
		data := []map[string]string{}
		for rows.Next() {
			var currency string
			var count int
			if err := rows.Scan(&currency, &count); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			total += count
			if currency != "" {
				data = append(data, map[string]string{
					"label": currency + " Rollovers:",
					"value": fmt.Sprintf("%d", count),
				})
			}
		}
		// Add total at the top
		data = append([]map[string]string{{"label": "Total Rollovers:", "value": fmt.Sprintf("%d", total)}}, data...)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	}
}

func GetBankTradesData(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		// Ledger-aware query: prefer latest ledger.running_open_amount, fallback to booking_amount
		rows, err := db.Query(`
			SELECT 
				fb.counterparty,
				fb.order_type,
				fb.base_currency,
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error fetching bank trades data")
			return
		}
		defer rows.Close()

		// Bank -> Trade -> Amount(USD)
		bankMap := map[string]map[string]float64{}

		for rows.Next() {
			var bank, orderType, baseCurrency string
			var effectiveAmount float64
			if err := rows.Scan(&bank, &orderType, &baseCurrency, &effectiveAmount); err != nil {
				continue
			}

			if bank == "" {
				bank = "Unknown Bank"
			}

			// Normalize trade label (e.g., "BUY USD", "SELL EUR")
			trade := strings.TrimSpace(orderType) + " " + strings.TrimSpace(baseCurrency)
			trade = strings.ReplaceAll(trade, "  ", " ")
			trade = strings.TrimSpace(trade)

			// Convert to USD
			currency := strings.ToUpper(baseCurrency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			amountUsd := math.Abs(effectiveAmount) * rate

			if bankMap[bank] == nil {
				bankMap[bank] = map[string]float64{}
			}
			bankMap[bank][trade] += amountUsd
		}

		// Build response
		forwardsData := []map[string]interface{}{}
		for bank, trades := range bankMap {
			tradeNames := []string{}
			amounts := []string{}

			for trade, amt := range trades {
				tradeNames = append(tradeNames, trade)

				if amt >= 1e6 {
					amounts = append(amounts, "$"+format2f(amt/1e6)+"M")
				} else if amt >= 1e3 {
					amounts = append(amounts, "$"+format2f(amt/1e3)+"K")
				} else {
					amounts = append(amounts, "$"+format2f(amt))
				}
			}

			forwardsData = append(forwardsData, map[string]interface{}{
				"bank":    bank,
				"trades":  tradeNames,
				"amounts": amounts,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(forwardsData)
	}
}

func GetMaturityBucketsDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		// Ledger-aware query
		rows, err := db.Query(`
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.quote_currency,
				fb.maturity_date
			FROM forward_bookings fb
			LEFT JOIN LATERAL (
				SELECT fbl.running_open_amount
				FROM forward_booking_ledger fbl
				WHERE fbl.booking_id = fb.system_transaction_id
				ORDER BY fbl.ledger_sequence DESC
				LIMIT 1
			) fbl ON TRUE
			WHERE fb.maturity_date IS NOT NULL
			  AND fb.entity_level_0 = ANY($1)
			  AND LOWER(fb.processing_status) = 'approved'
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
			return
		}
		defer rows.Close()

		now := time.Now()
		buckets := map[string]struct {
			Amount    float64
			Contracts int
		}{
			"Next 30 Days": {},
			"31-90 Days":   {},
			"91-180 Days":  {},
			"180+ Days":    {},
		}

		for rows.Next() {
			var amount float64
			var currency string
			var maturityDate time.Time
			if err := rows.Scan(&amount, &currency, &maturityDate); err != nil {
				continue
			}

			amount = math.Abs(amount)
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			amountUsd := amount * rate

			diffDays := int(math.Ceil(maturityDate.Sub(now).Hours() / 24))
			var bucket string
			if diffDays <= 30 {
				bucket = "Next 30 Days"
			} else if diffDays <= 90 {
				bucket = "31-90 Days"
			} else if diffDays <= 180 {
				bucket = "91-180 Days"
			} else if diffDays > 180 {
				bucket = "180+ Days"
			}

			if bucket != "" {
				b := buckets[bucket]
				b.Amount += amountUsd
				b.Contracts++
				buckets[bucket] = b
			}
		}

		formatAmount := func(amt float64) string {
			if amt >= 1e6 {
				return fmt.Sprintf("$%.1fM", amt/1e6)
			}
			if amt >= 1e3 {
				return fmt.Sprintf("$%.1fK", amt/1e3)
			}
			return fmt.Sprintf("$%.0f", amt)
		}

		response := map[string]map[string]string{}
		for key, val := range buckets {
			response[key] = map[string]string{
				"amount":    formatAmount(val.Amount),
				"contracts": fmt.Sprintf("%d Contracts", val.Contracts),
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// Handler: GetTotalBankMarginFromForwardBookings
func GetTotalBankMarginFromForwardBookings(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}
		rows, err := db.Query(`SELECT bank_margin, quote_currency FROM forward_bookings WHERE entity_level_0 = ANY($1) AND bank_margin IS NOT NULL AND (processing_status = 'Approved' OR processing_status = 'approved')`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
			return
		}
		defer rows.Close()
		totalBankMargin := 0.0
		for rows.Next() {
			var margin float64
			var currency string
			if err := rows.Scan(&margin, &currency); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			totalBankMargin += margin * rate
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]float64{"totalBankmargin": totalBankMargin})
	}
}

// Handler: GetOpenAmountToBookingRatioSimple
func GetOpenAmountToBookingRatioSimple(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		var totalOpen, totalBooking float64

		err := db.QueryRow(`
			WITH booking_totals AS (
				SELECT 
					COALESCE(SUM(ABS(eh.total_open_amount)), 0) AS total_open
				FROM exposure_headers eh
				WHERE eh.entity = ANY($1)
			),
			booking_amounts AS (
				SELECT 
					COALESCE(SUM(ABS(
						COALESCE(fbl.running_open_amount, fb.booking_amount)
					)), 0) AS total_booking
				FROM forward_bookings fb
				LEFT JOIN LATERAL (
					SELECT fbl.running_open_amount
					FROM forward_booking_ledger fbl
					WHERE fbl.booking_id = fb.system_transaction_id
					ORDER BY fbl.ledger_sequence DESC
					LIMIT 1
				) fbl ON TRUE
				WHERE fb.entity_level_0 = ANY($1)
				  AND LOWER(fb.processing_status) = 'approved'
			)
			SELECT bt.total_open, ba.total_booking
			FROM booking_totals bt, booking_amounts ba
		`, pq.Array(buNames)).Scan(&totalOpen, &totalBooking)

		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error calculating open amount to booking ratio")
			return
		}

		ratio := 0.0
		if math.Abs(totalBooking) > 0 {
			ratio = math.Abs(totalOpen) / math.Abs(totalBooking) * 100
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ratio": fmt.Sprintf("%.3f", ratio),
		})
	}
}
>>>>>>> origin/main
