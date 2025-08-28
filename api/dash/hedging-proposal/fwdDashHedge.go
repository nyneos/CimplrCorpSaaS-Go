package hedgeproposal

import (
	"database/sql"
	"encoding/json"

	// "fmt"
	"log"
	"net/http"
	"strings"

	// "time"

	"CimplrCorpSaas/api"

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

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func contains(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}
// Handler for forward dashboard for Hedging Proposal
// Handler: GetBuMaturityCurrencySummaryJoined
func GetBuMaturityCurrencySummaryJoined(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct{ UserID string `json:"user_id"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "user_id required")
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}
		rows, err := db.Query(`SELECT entity_level_0, delivery_period, quote_currency, order_type, booking_amount FROM forward_bookings WHERE entity_level_0 = ANY($1)  AND (processing_status = 'Approved' OR processing_status = 'approved')`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
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
				// Route: dash/cfo/fwd/avg-forward-maturity
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
				// Route: dash/cfo/fwd/buy-sell-totals
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
				// Route: dash/cfo/fwd/user-currency
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}