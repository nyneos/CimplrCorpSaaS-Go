package fxops

// Handler for exposure dashboard for FX Ops

import (
	"database/sql"
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/lib/pq"
)

var rates = map[string]float64{
	"USD": 1.0,
	"AUD": 0.68,
	"CAD": 0.75,
	"CHF": 1.1,
	"CNY": 0.14,
	"RMB": 0.14,
	"EUR": 1.09,
	"GBP": 1.28,
	"JPY": 0.0067,
	"SEK": 0.095,
	"INR": 0.0117,
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
