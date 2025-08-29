package cfo

import (
	// "context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

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

// Helper to get user_id from context
func getUserID(r *http.Request) string {
	val := r.Context().Value("user_id")
	if id, ok := val.(string); ok {
		return id
	}
	return ""
}

// 1. Total Open Amount USD Sum
func GetTotalOpenAmountUsdSumFromHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// userID := getUserID(r)
		// if userID == "" {
		//     http.Error(w, "Unauthorized", http.StatusUnauthorized)
		//     return
		// }
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
		var totalUsd float64
		for rows.Next() {
			var amount sql.NullFloat64
			var currency sql.NullString
			if err := rows.Scan(&amount, &currency); err != nil {
				continue
			}
			val := math.Abs(amount.Float64)
			cur := strings.ToUpper(currency.String)
			rate := rates[cur]
			if rate == 0 {
				rate = 1.0
			}
			totalUsd += val * rate
		}
		json.NewEncoder(w).Encode(map[string]float64{"totalUsd": totalUsd})
	}
}

// 2. Payables By Currency
func GetPayablesByCurrencyFromHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// userID := getUserID(r)
		// if userID == "" {
		// 	http.Error(w, "Unauthorized", http.StatusUnauthorized)
		// 	return
		// }
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusForbidden)
			return
		}
		query := `SELECT total_open_amount, currency FROM exposure_headers WHERE (exposure_type = 'PO' OR exposure_type = 'creditors' OR exposure_type = 'grn') AND entity = ANY($1) AND (approval_status = 'Approved' OR approval_status = 'approved')`
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
			val := math.Abs(amount.Float64)
			cur := strings.ToUpper(currency.String)
			currencyTotals[cur] += val * (rates[cur])
		}
		type Payable struct {
			Currency string `json:"currency"`
			Amount   string `json:"amount"`
		}
		var payablesData []Payable
		for cur, amt := range currencyTotals {
			payablesData = append(payablesData, Payable{cur, "$" + formatK(amt)})
		}
		json.NewEncoder(w).Encode(payablesData)
	}
}

// 3. Receivables By Currency
func GetReceivablesByCurrencyFromHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// userID := getUserID(r)
		// if userID == "" {
		// 	http.Error(w, "Unauthorized", http.StatusUnauthorized)
		// 	return
		// }
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusForbidden)
			return
		}
		query := `SELECT total_open_amount, currency FROM exposure_headers WHERE (exposure_type = 'SO' OR exposure_type = 'LC' OR exposure_type = 'debitors') AND entity = ANY($1) AND (approval_status = 'Approved' OR approval_status = 'approved')`
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
			val := math.Abs(amount.Float64)
			cur := strings.ToUpper(currency.String)
			currencyTotals[cur] += val * (rates[cur])
		}
		type Receivable struct {
			Currency string `json:"currency"`
			Amount   string `json:"amount"`
		}
		var receivablesData []Receivable
		for cur, amt := range currencyTotals {
			receivablesData = append(receivablesData, Receivable{cur, "$" + formatK(amt)})
		}
		json.NewEncoder(w).Encode(receivablesData)
	}
}

// 4. Amount By Currency
func GetAmountByCurrencyFromHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// userID := getUserID(r)
		// if userID == "" {
		// 	http.Error(w, "Unauthorized", http.StatusUnauthorized)
		// 	return
		// }
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
			val := math.Abs(amount.Float64)
			cur := strings.ToUpper(currency.String)
			currencyTotals[cur] += val * (rates[cur])
		}
		type Amount struct {
			Currency string `json:"currency"`
			Amount   string `json:"amount"`
		}
		var payablesData []Amount
		for cur, amt := range currencyTotals {
			payablesData = append(payablesData, Amount{cur, "$" + formatK(amt)})
		}
		json.NewEncoder(w).Encode(payablesData)
	}
}

// 5. Business Unit Currency Summary
func GetBusinessUnitCurrencySummaryFromHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// userID := getUserID(r)
		// if userID == "" {
		// 	http.Error(w, "Unauthorized", http.StatusUnauthorized)
		// 	return
		// }
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusForbidden)
			return
		}
		query := `SELECT entity, currency, total_open_amount FROM exposure_headers WHERE entity = ANY($1) AND (approval_status = 'Approved' OR approval_status = 'approved')`
		rows, err := db.QueryContext(r.Context(), query, pq.Array(buNames))
		if err != nil {
			http.Error(w, "Failed to query", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		buMap := map[string]map[string]float64{}
		for rows.Next() {
			var entity, currency sql.NullString
			var amount sql.NullFloat64
			if err := rows.Scan(&entity, &currency, &amount); err != nil {
				continue
			}
			bu := entity.String
			if bu == "" {
				bu = "Unknown"
			}
			cur := strings.ToUpper(currency.String)
			if cur == "" {
				cur = "Unknown"
			}
			val := math.Abs(amount.Float64)
			usdAmount := val * (rates[cur])
			if buMap[bu] == nil {
				buMap[bu] = map[string]float64{}
			}
			buMap[bu][cur] += usdAmount
		}
		type Currency struct {
			Code   string `json:"code"`
			Amount string `json:"amount"`
		}
		type Output struct {
			Name       string     `json:"name"`
			Total      string     `json:"total"`
			Currencies []Currency `json:"currencies"`
		}
		var output []Output
		for bu, currencies := range buMap {
			var total float64
			var currs []Currency
			for code, amt := range currencies {
				total += amt
				currs = append(currs, Currency{code, "$" + formatK(amt)})
			}
			output = append(output, Output{bu, "$" + formatK(total), currs})
		}
		json.NewEncoder(w).Encode(output)
	}
}

// 6. Maturity Expiry Summary
func GetMaturityExpirySummaryFromHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// userID := getUserID(r)
		// if userID == "" {
		// 	http.Error(w, "Unauthorized", http.StatusUnauthorized)
		// 	return
		// }
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusForbidden)
			return
		}
		query := `SELECT total_open_amount, currency, document_date FROM exposure_headers WHERE document_date IS NOT NULL AND entity = ANY($1) AND (approval_status = 'Approved' OR approval_status = 'approved')`
		rows, err := db.QueryContext(r.Context(), query, pq.Array(buNames))
		if err != nil {
			http.Error(w, "Failed to query", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		now := time.Now()
		var sum7, sum30, sumTotal float64
		for rows.Next() {
			var amount sql.NullFloat64
			var currency sql.NullString
			var docDate sql.NullTime
			if err := rows.Scan(&amount, &currency, &docDate); err != nil {
				continue
			}
			val := math.Abs(amount.Float64)
			cur := strings.ToUpper(currency.String)
			rate := rates[cur]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := val * rate
			if !docDate.Valid {
				continue
			}
			diffDays := int(docDate.Time.Sub(now).Hours() / 24)
			if diffDays >= 0 {
				sumTotal += usdAmount
				if diffDays <= 7 {
					sum7 += usdAmount
				}
				if diffDays <= 30 {
					sum30 += usdAmount
				}
			}
		}
		output := []map[string]string{
			{"label": "Next 7 Days", "value": "$" + formatK(sum7)},
			{"label": "Next 30 Days", "value": "$" + formatK(sum30)},
			{"label": "Total Upcoming", "value": "$" + formatK(sumTotal)},
		}
		json.NewEncoder(w).Encode(output)
	}
}

// Helper to format as K
func formatK(val float64) string {
	absVal := math.Abs(val)
	switch {
	case absVal >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", absVal/1_000_000_000)
	case absVal >= 1_000_000:
		return fmt.Sprintf("%.2fM", absVal/1_000_000)
	case absVal >= 1_000:
		return fmt.Sprintf("%.2fK", absVal/1_000)
	default:
		return fmt.Sprintf("%.2f", absVal)
	}
}

// 7. Average Exposure Maturity
func GetAvgExposureMaturity(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// userID := getUserID(r)
		// if userID == "" {
		// 	http.Error(w, "Unauthorized", http.StatusUnauthorized)
		// 	return
		// }
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusForbidden)
			return
		}
		query := `SELECT total_original_amount AS amount, currency, document_date, ABS(CAST(document_date AS date) - CURRENT_DATE) AS days_to_maturity FROM exposure_headers WHERE document_date IS NOT NULL AND entity = ANY($1) AND (approval_status = 'Approved' OR approval_status = 'approved')`
		rows, err := db.QueryContext(r.Context(), query, pq.Array(buNames))
		if err != nil {
			http.Error(w, "Error calculating Avg Exposure Maturity", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		var weightedSum, totalAmount float64
		for rows.Next() {
			var amount sql.NullFloat64
			var currency sql.NullString
			var documentDate sql.NullTime
			var daysToMaturity sql.NullInt64
			if err := rows.Scan(&amount, &currency, &documentDate, &daysToMaturity); err != nil {
				continue
			}
			rate := rates[strings.ToUpper(currency.String)]
			if rate == 0 {
				rate = 1.0
			}
			usdAmount := math.Abs(amount.Float64) * rate
			weightedSum += usdAmount * float64(daysToMaturity.Int64)
			totalAmount += usdAmount
		}
		avgMaturity := 0
		if totalAmount > 0 {
			avgMaturity = int(math.Round(weightedSum / totalAmount))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"avgExposureMaturity": avgMaturity})
	}
}

// Endpoint: Maturity Expiry Count for Next 7 Days
func GetMaturityExpiryCount7DaysFromHeaders(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			http.Error(w, "No accessible business units found", http.StatusForbidden)
			return
		}
		query := `SELECT document_date FROM exposure_headers WHERE document_date IS NOT NULL AND entity = ANY($1) AND (approval_status = 'Approved' OR approval_status = 'approved')`
		rows, err := db.QueryContext(r.Context(), query, pq.Array(buNames))
		if err != nil {
			http.Error(w, "Failed to query", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		now := time.Now()
		count7 := 0
		for rows.Next() {
			var docDate sql.NullTime
			if err := rows.Scan(&docDate); err != nil {
				continue
			}
			if !docDate.Valid {
				continue
			}
			diffDays := int(math.Ceil(docDate.Time.Sub(now).Hours() / 24))
			if diffDays >= 0 && diffDays <= 7 {
				count7++
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"value": count7})
	}
}
