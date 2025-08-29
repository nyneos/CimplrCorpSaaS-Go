package hedgeproposal

import (
	"database/sql"
	"encoding/json"

	"fmt"
	"log"
	"net/http"
	"strings"

	"math"

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
				fb.base_currency,
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

// Helper: format2f
func format2f(x float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.4f", x), "0"), ".")
}

// Handler: GetForwardBookingsDashboard
func GetForwardBookingsDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, "No accessible business units found")
			return
		}

		rows, err := db.Query(`
			SELECT 
				internal_reference_id, counterparty, entity_level_0, base_currency, local_currency,
				value_quote_currency, spot_rate, total_rate, bank_margin, value_local_currency,
				maturity_date, add_date
			FROM forward_bookings
			WHERE entity_level_0 = ANY($1)
			  AND (processing_status = 'Approved' OR processing_status = 'approved')
		`, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "DB error")
			return
		}
		defer rows.Close()

		type ForwardRow struct {
			DealID       interface{} `json:"dealId"`
			Bank         interface{} `json:"bank"`
			BU           interface{} `json:"bu"`
			FCY          interface{} `json:"fcy"`
			LCY          interface{} `json:"lcy"`
			FCYAmount    interface{} `json:"fcyAmount"`
			SpotRate     interface{} `json:"spotRate"`
			ForwardRate  interface{} `json:"forwardRate"`
			MarginBps    interface{} `json:"marginBps"`
			LCYValue     interface{} `json:"lcyValue"`
			MaturityDate interface{} `json:"maturityDate"`
			DealDate     interface{} `json:"dealDate"`
		}

		var result []ForwardRow

		for rows.Next() {
			var (
				dealId, bank, bu, fcy, lcy                            sql.NullString
				fcyAmount, spotRate, forwardRate, marginBps, lcyValue sql.NullFloat64
				maturityDate, dealDate                                sql.NullTime
			)

			if err := rows.Scan(
				&dealId, &bank, &bu, &fcy, &lcy,
				&fcyAmount, &spotRate, &forwardRate, &marginBps, &lcyValue,
				&maturityDate, &dealDate,
			); err != nil {
				continue
			}

			// Helper for NullString → interface{}
			strOrNil := func(ns sql.NullString) interface{} {
				if ns.Valid {
					return ns.String
				}
				return nil
			}
			// Helper for NullFloat64 → interface{}
			floatOrNil := func(nf sql.NullFloat64) interface{} {
				if nf.Valid {
					return nf.Float64
				}
				return nil
			}
			// Helper for NullTime → interface{} (formatted as d/m/yyyy)
			dateOrNil := func(nt sql.NullTime) interface{} {
				if nt.Valid {
					return nt.Time.Format("2/1/2006")
				}
				return nil
			}

			row := ForwardRow{
				DealID:       strOrNil(dealId),
				Bank:         strOrNil(bank),
				BU:           strOrNil(bu),
				FCY:          strOrNil(fcy),
				LCY:          strOrNil(lcy),
				FCYAmount:    floatOrNil(fcyAmount),
				SpotRate:     floatOrNil(spotRate),
				ForwardRate:  floatOrNil(forwardRate),
				MarginBps:    floatOrNil(marginBps),
				LCYValue:     floatOrNil(lcyValue),
				MaturityDate: dateOrNil(maturityDate),
				DealDate:     dateOrNil(dealDate),
			}
			result = append(result, row)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}
