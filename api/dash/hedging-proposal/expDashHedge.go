package hedgeproposal

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"math"
	"net/http"
	"strings"

	"github.com/lib/pq"
)

// Handler: GetBuMaturityCurrencySummaryJoinedFromHeaders
func GetBuMaturityCurrencySummaryJoinedFromHeaders(db *sql.DB) http.HandlerFunc {
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
		rows, err := db.Query(`SELECT h.entity AS business_unit, h.currency, h.exposure_type, h.total_open_amount, b.month_1, b.month_2, b.month_3, b.month_4, b.month_4_6, b.month_6plus FROM exposure_headers h JOIN exposure_bucketing b ON h.exposure_header_id = b.exposure_header_id WHERE h.entity = ANY($1) AND h.user_id = $2 AND (h.approval_status = 'Approved' OR h.approval_status = 'approved') AND (b.status_bucketing = 'Approved' OR b.status_bucketing = 'approved')`, pq.Array(buNames), req.UserID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "DB error"})
			return
		}
		defer rows.Close()
		maturityBuckets := []string{"month_1", "month_2", "month_3", "month_4", "month_4_6", "month_6plus"}
		bucketLabels := map[string]string{
			"month_1":     "1 Month",
			"month_2":     "2 Month",
			"month_3":     "3 Month",
			"month_4":     "4 Month",
			"month_4_6":   "4-6 Month",
			"month_6plus": "6 Month +",
		}
		summary := map[string]map[string]map[string]struct{ Payables, Receivables float64 }{}
		for rows.Next() {
			var bu, currency, exposureType string
			var totalOpenAmount sql.NullFloat64
			var month1, month2, month3, month4, month46, month6plus sql.NullFloat64
			if err := rows.Scan(&bu, &currency, &exposureType, &totalOpenAmount, &month1, &month2, &month3, &month4, &month46, &month6plus); err != nil {
				continue
			}
			bu = strings.TrimSpace(bu)
			if bu == "" {
				bu = "Unknown"
			}
			currency = strings.ToUpper(strings.TrimSpace(currency))
			if currency == "" {
				currency = "Unknown"
			}
			exposureType = strings.ToUpper(strings.TrimSpace(exposureType))
			bucketValues := map[string]float64{
				"month_1":     math.Abs(month1.Float64),
				"month_2":     math.Abs(month2.Float64),
				"month_3":     math.Abs(month3.Float64),
				"month_4":     math.Abs(month4.Float64),
				"month_4_6":   math.Abs(month46.Float64),
				"month_6plus": math.Abs(month6plus.Float64),
			}
			for _, bucket := range maturityBuckets {
				amount := bucketValues[bucket]
				if amount == 0 {
					continue
				}
				if summary[bucket] == nil {
					summary[bucket] = map[string]map[string]struct{ Payables, Receivables float64 }{}
				}
				if summary[bucket][bu] == nil {
					summary[bucket][bu] = map[string]struct{ Payables, Receivables float64 }{}
				}
				if summary[bucket][bu][currency] == (struct{ Payables, Receivables float64 }{}) {
					summary[bucket][bu][currency] = struct{ Payables, Receivables float64 }{0, 0}
				}
				entry := summary[bucket][bu][currency]
				if exposureType == "PO" || exposureType == "CREDITORS" {
					entry.Payables += amount
				} else if exposureType == "SO" || exposureType == "LC" || exposureType == "DEBITORS" {
					entry.Receivables += amount
				}
				summary[bucket][bu][currency] = entry
			}
		}
		response := []map[string]interface{}{}
		for bucket, buMap := range summary {
			maturityLabel := bucketLabels[bucket]
			if maturityLabel == "" {
				maturityLabel = bucket
			}
			for bu, currencyMap := range buMap {
				for currency, entry := range currencyMap {
					response = append(response, map[string]interface{}{
						"maturity":      maturityLabel,
						"business_unit": bu,
						"currency":      currency,
						"payables":      entry.Payables,
						"receivables":   entry.Receivables,
					})
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}
