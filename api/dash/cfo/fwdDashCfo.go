package cfo

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"
)

var rates = map[string]float64{
	"USD": 1.0,
	"EUR": 1.1,
	"INR": 0.012,
	"GBP": 1.25,
	"AUD": 0.68,
	"CAD": 0.75,
	"CHF": 1.1,
	"CNY": 0.14,
	"JPY": 0.0068,
}

// Handler for forward dashboard for CFO

// Handler: GetAvgForwardMaturity
func GetAvgForwardMaturity(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		rows, err := db.Query(`SELECT booking_amount, base_currency, maturity_date, ABS(CAST(maturity_date AS date) - CURRENT_DATE) AS days_to_maturity FROM forward_bookings WHERE maturity_date IS NOT NULL`)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error"})
			return
		}
		defer rows.Close()
		var weightedSum, totalAmount float64
		for rows.Next() {
			var amount float64
			var currency string
			var maturityDate string
			var daysToMaturity int
			if err := rows.Scan(&amount, &currency, &maturityDate, &daysToMaturity); err != nil {
				continue
			}
			rate := rates[strings.ToUpper(currency)]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := abs(amount) * rate
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

// Handler: GetForwardBuySellTotals
func GetForwardBuySellTotals(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		rows, err := db.Query(`SELECT booking_amount, quote_currency, order_type FROM forward_bookings WHERE booking_amount IS NOT NULL`)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error"})
			return
		}
		defer rows.Close()
		var buyTotal, sellTotal float64
		for rows.Next() {
			var amount float64
			var currency, orderType string
			if err := rows.Scan(&amount, &currency, &orderType); err != nil {
				continue
			}
			rate := rates[strings.ToUpper(currency)]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := abs(amount) * rate
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
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		var buName string
		if err := db.QueryRow("SELECT business_unit_name FROM users WHERE id = $1", req.UserID).Scan(&buName); err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "User not found"})
			return
		}
		if buName == "" {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "User has no business unit assigned"})
			return
		}
		var defaultCurrency string
		if err := db.QueryRow("SELECT default_currency FROM masterentity WHERE entity_name = $1", buName).Scan(&defaultCurrency); err != nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No entity found for given business unit"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"defaultCurrency": defaultCurrency})
	}
}

// Handler: GetBuMaturityCurrencySummaryJoined
func GetBuMaturityCurrencySummaryJoined(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		buNames, ok := r.Context().Value("BusinessUnitsKey").([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		rows, err := db.Query(`SELECT entity_level_0, delivery_period, quote_currency, order_type, booking_amount FROM forward_bookings WHERE entity_level_0 = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error"})
			return
		}
		defer rows.Close()
		bucketLabels := map[string]string{
			"month_1":  "1 Month",
			"month_2":  "2 Month",
			"month_3":  "3 Month",
			"month_4":  "4 Month",
			"month_4_6": "4-6 Month",
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
			if contains([]string{"1m", "1month", "month1", "m1", "mon1"}, p) {
				return "month_1"
			}
			if contains([]string{"2m", "2month", "month2", "m2", "mon2"}, p) {
				return "month_2"
			}
			if contains([]string{"3m", "3month", "month3", "m3", "mon3"}, p) {
				return "month_3"
			}
			if contains([]string{"4m", "4month", "month4", "m4", "mon4"}, p) {
				return "month_4"
			}
			if contains([]string{"46m", "4to6month", "month46", "month4to6", "m46", "mon46", "4_6month", "4_6m", "4-6m", "4-6month"}, p) {
				return "month_4_6"
			}
			if contains([]string{"6mplus", "6monthplus", "month6plus", "6plus", "m6plus", "mon6plus", "6m+", "6month+", "month6+"}, p) {
				return "month_6plus"
			}
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
		summary := map[string]map[string]interface{}{}
		for rows.Next() {
			var bu, deliveryPeriod, currency, orderType string
			var bookingAmount float64
			if err := rows.Scan(&bu, &deliveryPeriod, &currency, &orderType, &bookingAmount); err != nil {
				continue
			}
			bucketKey := normalizeDeliveryPeriod(deliveryPeriod)
			maturity := bucketLabels[bucketKey]
			if maturity == "" {
				maturity = "1 Month"
			}
			currency = strings.ToUpper(currency)
			orderType = strings.ToLower(orderType)
			amount := abs(bookingAmount)
			key := bu + "__" + maturity + "__" + currency
			if _, exists := summary[key]; !exists {
				summary[key] = map[string]interface{}{
					"bu":       bu,
					"maturity": maturity,
					"currency": currency,
					"forwardBuy":  0.0,
					"forwardSell": 0.0,
				}
			}
			if orderType == "buy" {
				summary[key]["forwardBuy"] = summary[key]["forwardBuy"].(float64) + amount
			} else {
				summary[key]["forwardSell"] = summary[key]["forwardSell"].(float64) + amount
			}
		}
		result := []map[string]interface{}{}
		for _, v := range summary {
			result = append(result, v)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

// Handler: GetActiveForwardsCount
func GetActiveForwardsCount(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		now := time.Now().Format("2006-01-02")
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM forward_bookings WHERE maturity_date > $1", now).Scan(&count)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Error fetching active forwards count", "details": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{ "ActiveForward": count })
	}
}

// Handler: GetRecentTradesDashboard
func GetRecentTradesDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rates := map[string]float64{
			"USD": 1.0, "AUD": 0.68, "CAD": 0.75, "CHF": 1.1, "CNY": 0.14, "RMB": 0.14,
			"EUR": 1.09, "GBP": 1.28, "JPY": 0.0067, "SEK": 0.095, "INR": 0.0117,
		}
		now := time.Now()
		sevenDaysAgo := now.AddDate(0, 0, -7).Format("2006-01-02")
		nowStr := now.Format("2006-01-02")
		rows, err := db.Query(`SELECT booking_amount, quote_currency, currency_pair, counterparty_dealer, maturity_date FROM forward_bookings WHERE maturity_date >= $1 AND maturity_date <= $2`, sevenDaysAgo, nowStr)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Error fetching recent trades dashboard", "details": err.Error()})
			return
		}
		defer rows.Close()
		var totalTrades int
		var totalVolume float64
		bankMap := map[string]struct {
			Pair string
			Bank string
			Amount float64
		}{}
		for rows.Next() {
			var amount float64
			var currency, pair, bank string
			var maturityDate string
			if err := rows.Scan(&amount, &currency, &pair, &bank, &maturityDate); err != nil { continue }
			amount = abs(amount)
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 { rate = 1.0 }
			amountUsd := amount * rate
			totalTrades++
			totalVolume += amountUsd
			pairLabel := strings.TrimSpace(pair) + " Forward"
			if bank == "" { bank = "Unknown Bank" }
			key := bank + "__" + pairLabel
			if _, exists := bankMap[key]; !exists {
				bankMap[key] = struct {
					Pair string
					Bank string
					Amount float64
				}{Pair: pairLabel, Bank: bank, Amount: 0}
			}
			entry := bankMap[key]
			entry.Amount += amountUsd
			bankMap[key] = entry
		}
		formatAmount := func(amt float64) string {
			if amt >= 1e6 { return "$" + format2f(amt/1e6) + "M" }
			if amt >= 1e3 { return "$" + format2f(amt/1e3) + "K" }
			return "$" + format2f(amt)
		}
		banks := []map[string]interface{}{}
		for _, b := range bankMap {
			banks = append(banks, map[string]interface{}{
				"pair": b.Pair,
				"bank": b.Bank,
				"amount": formatAmount(b.Amount),
			})
		}
		response := map[string]interface{}{
			"Total Trades": map[string]interface{}{ "value": totalTrades },
			"Total Volume": map[string]interface{}{ "value": formatAmount(totalVolume) },
			"Avg Trade Size": map[string]interface{}{ "value": func() string {
				if totalTrades > 0 { return formatAmount(totalVolume / float64(totalTrades)) }
				return "$0"
			}() },
			"BANKS": banks,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// Helper: abs
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// Helper: format2f
func format2f(x float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", x), "0"), ".")
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

// Handler: GetTotalUsdSumDashboard
func GetTotalUsdSumDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		buNames, ok := r.Context().Value("BusinessUnitsKey").([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		rows, err := db.Query(`SELECT booking_amount, quote_currency FROM forward_bookings WHERE entity_level_0 = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error"})
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
			usdAmount := abs(amount) * rate
			totalUsd += usdAmount
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"totalUsdSum": format2f(totalUsd),
		})
	}
}

// Handler: GetOpenAmountToBookingRatioDashboard
func GetOpenAmountToBookingRatioDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		buNames, ok := r.Context().Value("BusinessUnitsKey").([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		// Query open amounts (where status = 'OPEN')
		openRows, err := db.Query(`SELECT booking_amount, quote_currency FROM forward_bookings WHERE entity_level_0 = ANY($1) AND status = 'OPEN'`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error (open)"})
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
			usdAmount := abs(amount) * rate
			openUsd += usdAmount
		}
		// Query total booked amounts
		totalRows, err := db.Query(`SELECT booking_amount, quote_currency FROM forward_bookings WHERE entity_level_0 = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error (total)"})
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
			usdAmount := abs(amount) * rate
			totalUsd += usdAmount
		}
		ratio := 0.0
		if totalUsd > 0 {
			ratio = openUsd / totalUsd
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"openToBookingRatio": format2f(ratio),
			"openAmountUsd": format2f(openUsd),
			"totalBookedUsd": format2f(totalUsd),
		})
	}
}

// Handler: GetTotalBankMarginDashboard
func GetTotalBankMarginDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		buNames, ok := r.Context().Value("BusinessUnitsKey").([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		rows, err := db.Query(`SELECT counterparty_dealer, margin_amount, quote_currency FROM forward_bookings WHERE entity_level_0 = ANY($1) AND margin_amount IS NOT NULL`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error"})
			return
		}
		defer rows.Close()
		bankMargins := map[string]float64{}
		for rows.Next() {
			var bank string
			var margin float64
			var currency string
			if err := rows.Scan(&bank, &margin, &currency); err != nil {
				continue
			}
			currency = strings.ToUpper(currency)
			rate := rates[currency]
			if rate == 0 {
				rate = 1.0
			}
			usdMargin := abs(margin) * rate
			if bank == "" {
				bank = "Unknown Bank"
			}
			bankMargins[bank] += usdMargin
		}
		result := []map[string]interface{}{}
		for bank, margin := range bankMargins {
			result = append(result, map[string]interface{}{
				"bank": bank,
				"totalMarginUsd": format2f(margin),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}

// Handler: GetTotalUsdSumByCurrencyDashboard
func GetTotalUsdSumByCurrencyDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		buNames, ok := r.Context().Value("BusinessUnitsKey").([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		rows, err := db.Query(`SELECT booking_amount, quote_currency FROM forward_bookings WHERE entity_level_0 = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error"})
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
			usdAmount := abs(amount) * rate
			currencyTotals[currency] += usdAmount
		}
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

// Handler: GetForwardBookingMaturityBucketsDashboard
func GetForwardBookingMaturityBucketsDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		buNames, ok := r.Context().Value("BusinessUnitsKey").([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		rows, err := db.Query(`SELECT booking_amount, quote_currency, delivery_period FROM forward_bookings WHERE entity_level_0 = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error"})
			return
		}
		defer rows.Close()
		bucketLabels := map[string]string{
			"month_1": "1 Month",
			"month_2": "2 Month",
			"month_3": "3 Month",
			"month_4": "4 Month",
			"month_4_6": "4-6 Month",
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
			if contains([]string{"1m", "1month", "month1", "m1", "mon1"}, p) {
				return "month_1"
			}
			if contains([]string{"2m", "2month", "month2", "m2", "mon2"}, p) {
				return "month_2"
			}
			if contains([]string{"3m", "3month", "month3", "m3", "mon3"}, p) {
				return "month_3"
			}
			if contains([]string{"4m", "4month", "month4", "m4", "mon4"}, p) {
				return "month_4"
			}
			if contains([]string{"46m", "4to6month", "month46", "month4to6", "m46", "mon46", "4_6month", "4_6m", "4-6m", "4-6month"}, p) {
				return "month_4_6"
			}
			if contains([]string{"6mplus", "6monthplus", "month6plus", "6plus", "m6plus", "mon6plus", "6m+", "6month+", "month6+"}, p) {
				return "month_6plus"
			}
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
			usdAmount := abs(amount) * rate
			bucketKey := normalizeDeliveryPeriod(deliveryPeriod)
			bucketTotals[bucketKey] += usdAmount
		}
		result := []map[string]interface{}{}
		for bucket, total := range bucketTotals {
			label := bucketLabels[bucket]
			if label == "" {
				label = "1 Month"
			}
			result = append(result, map[string]interface{}{
				"maturityBucket": label,
				"totalUsd": format2f(total),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}
