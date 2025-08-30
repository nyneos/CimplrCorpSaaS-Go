package fxops

// Handler for exposure dashboard for FX Ops

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

var rates = map[string]float64{
	"USD": 1.0,
	"AUD": 0.68,
	"CAD": 0.75,
	"CHF": 1.1,
	"CNY": 0.14,
	"CNH": 0.14,
	"RMB": 0.14,
	"EUR": 1.09,
	"GBP": 1.28,
	"JPY": 0.0067,
	"SEK": 0.095,
	"INR": 0.0117,
}

func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	log.Println("[ERROR]", errMsg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   errMsg,
	})
}

// Endpoint: Top Currencies from Headers
func GetTopCurrenciesFromHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusForbidden)
			return
		}
		query := `SELECT total_open_amount, currency FROM exposure_headers WHERE entity = ANY($1) AND (approval_status = 'Approved' OR approval_status = 'approved')`
		rows, err := db.QueryContext(r.Context(), query, pq.Array(buNames))
		if err != nil {
			http.Error(w, "Failed to query", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		currencyTotals := map[string]float64{}
		for rows.Next() {
			var amount sql.NullFloat64
			var currency sql.NullString
			if err := rows.Scan(&amount, &currency); err != nil {
				continue
			}
			cur := strings.ToUpper(currency.String)
			val := math.Abs(amount.Float64)
			usdValue := val * (rates[cur])
			currencyTotals[cur] += usdValue
		}
		// Sort currencies by value descending and take top 5
		type pair struct {
			Currency string
			Value    float64
		}
		var sorted []pair
		for k, v := range currencyTotals {
			sorted = append(sorted, pair{k, v})
		}
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Value > sorted[j].Value })
		colors := []string{"bg-green-400", "bg-blue-400", "bg-yellow-400", "bg-red-400", "bg-purple-400"}
		var topCurrencies []map[string]interface{}
		for idx, p := range sorted {
			if idx >= 5 {
				break
			}
			topCurrencies = append(topCurrencies, map[string]interface{}{
				"currency": p.Currency,
				"value":    math.Round(p.Value*10) / 10, // one decimal place
				"color":    colors[idx],
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(topCurrencies)
	}
}

// Endpoint: Count of Forward Bookings Maturing Today
func GetForwardBookingsMaturingTodayCount(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusForbidden)
			return
		}
		query := `SELECT COUNT(*) FROM forward_bookings WHERE entity_level_0 = ANY($1) AND maturity_date = CURRENT_DATE AND (processing_status = 'Approved' OR processing_status = 'approved')`
		var count sql.NullInt64
		err := db.QueryRowContext(r.Context(), query, pq.Array(buNames)).Scan(&count)
		if err != nil {
			http.Error(w, "Failed to query", http.StatusInternalServerError)
			return
		}
		result := int64(0)
		if count.Valid {
			result = count.Int64
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int64{"maturingTodayCount": result})
	}
}

func GetTodayBookingAmountSum(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusForbidden)
			return
		}
		query := `
			SELECT booking_amount, base_currency, DATE(transaction_timestamp) 
			FROM forward_bookings 
			WHERE entity_level_0 = ANY($1) 
			AND (DATE(transaction_timestamp) = CURRENT_DATE OR DATE(transaction_timestamp) = CURRENT_DATE - INTERVAL '1 day') 
			AND (processing_status = 'Approved' OR processing_status = 'approved')
		`

		rows, err := db.QueryContext(r.Context(), query, pq.Array(buNames))
		if err != nil {
			http.Error(w, "Failed to query", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var todaySum, yesterdaySum float64
		todayStr := time.Now().Format("2006-01-02")
		yesterdayStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

		for rows.Next() {
			var amount sql.NullFloat64
			var currency sql.NullString
			var txnDate sql.NullTime

			if err := rows.Scan(&amount, &currency, &txnDate); err != nil {
				http.Error(w, "Error scanning row", http.StatusInternalServerError)
				return
			}

			// Handle missing or unknown currency conversion rate
			cur := strings.ToUpper(currency.String)
			rate, found := rates[cur]
			if !found {
				rate = 1.0 // Default to 1 if no rate is found
				log.Printf("No rate found for currency: %s. Using default rate of 1.0", cur)
			}

			usdValue := math.Abs(amount.Float64) * rate

			if txnDate.Valid {
				if txnDate.Time.Format("2006-01-02") == todayStr {
					todaySum += usdValue
				} else if txnDate.Time.Format("2006-01-02") == yesterdayStr {
					yesterdaySum += usdValue
				}
			}
		}

		// Check if rows.Next() had any errors
		if err := rows.Err(); err != nil {
			http.Error(w, "Error iterating over rows", http.StatusInternalServerError)
			return
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"todayBookingAmountSumUsd":     formatAmount(todaySum),
			"yesterdayBookingAmountSumUsd": formatAmount(yesterdaySum),
		})
	}
}

func GetMaturityBucketsByCurrencyPair(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}
		query := `
			SELECT 
				COALESCE(fbl.running_open_amount, fb.booking_amount) AS effective_amount,
				fb.currency_pair,
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
		`

		rows, err := db.QueryContext(r.Context(), query, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
			return
		}
		defer rows.Close()

		// Prepare date calculations
		now := time.Now()
		today := now.Format("2006-01-02")
		tomorrow := now.Add(24 * time.Hour).Format("2006-01-02")
		nextWeek := now.Add(7 * 24 * time.Hour).Format("2006-01-02")

		// Initialize categories for Today, Tomorrow, and Next Week
		categories := map[string]map[string]float64{
			"Today":     {},
			"Tomorrow":  {},
			"Next Week": {},
		}

		// Loop over rows to categorize by maturity_date and currency_pair
		for rows.Next() {
			var amount float64
			var currencyPair string
			var maturityDate time.Time
			if err := rows.Scan(&amount, &currencyPair, &maturityDate); err != nil {
				continue
			}

			// Convert to USD
			currencyPair = strings.ToUpper(currencyPair)
			curr := ""
			if len(currencyPair) >= 3 {
				curr = strings.ToUpper(currencyPair[:3])
			}
			rate := rates[curr]
			// rate := rates[currencyPair]
			if rate == 0 {
				rate = 1.0
			}
			amountUsd := math.Abs(amount) * rate

			// Categorize based on maturity_date
			if maturityDate.Format("2006-01-02") == today {
				categories["Today"][currencyPair] += amountUsd
			} else if maturityDate.Format("2006-01-02") == tomorrow {
				categories["Tomorrow"][currencyPair] += amountUsd
			} else if maturityDate.Format("2006-01-02") <= nextWeek {
				categories["Next Week"][currencyPair] += amountUsd
			}
		}

		// Check if rows.Next() had any errors
		if err := rows.Err(); err != nil {
			respondWithError(w, http.StatusInternalServerError, "Error iterating over rows")
			return
		}

		// Format the amounts
		formatAmount := func(amt float64) string {
			if amt >= 1e6 {
				return fmt.Sprintf("$%.1fM", amt/1e6)
			}
			if amt >= 1e3 {
				return fmt.Sprintf("$%.1fK", amt/1e3)
			}
			return fmt.Sprintf("$%.0f", amt)
		}

		// Prepare response items in the desired format
		items := []map[string]interface{}{}
		for label, currencyMap := range categories {
			var value []map[string]interface{}
			for currency, totalAmount := range currencyMap {
				value = append(value, map[string]interface{}{
					"currency_pair": currency,
					"value":         fmt.Sprintf("%s %s", currency, formatAmount(totalAmount)),
				})
			}
			items = append(items, map[string]interface{}{
				"label": label,
				"value": value,
			})
		}

		// Send the response as JSON
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"items": items,
		})
	}
}
