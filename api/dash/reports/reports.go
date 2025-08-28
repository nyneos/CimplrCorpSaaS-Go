package reports

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/lib/pq"
)

type Summary struct {
	ExposureHeaderID    string  `json:"exposure_header_id"`
	CompanyCode         string  `json:"company_code"`
	Entity              string  `json:"entity"`
	Entity1             string  `json:"entity1"`
	Entity2             string  `json:"entity2"`
	Entity3             string  `json:"entity3"`
	ExposureType        string  `json:"exposure_type"`
	DocumentID          string  `json:"document_id"`
	DocumentDate        string  `json:"document_date"`
	MaturityMonth       string  `json:"maturity_month"`
	CounterpartyName    string  `json:"counterparty_name"`
	Currency            string  `json:"currency"`
	TotalOriginalAmount float64 `json:"total_original_amount"`
	TotalOpenAmount     float64 `json:"total_open_amount"`
	ValueDate           string  `json:"value_date"`
	HedgedValue         float64 `json:"hedged_value"`
	UnhedgedValue       float64 `json:"unhedged_value"`
}
type Exposure struct {
	ExposureHeaderID    string
	CompanyCode         string
	Entity              string
	Entity1             sql.NullString
	Entity2             sql.NullString
	Entity3             sql.NullString
	ExposureType        string
	DocumentID          string
	DocumentDate        string
	CounterpartyName    string
	Currency            string
	TotalOriginalAmount float64
	TotalOpenAmount     float64
	ValueDate           string
}

// Handler: GetExposureSummary
func GetExposureSummary(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		// User verification via middleware
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}

		expRows, err := db.Query(`SELECT exposure_header_id, company_code, entity, entity1, entity2, entity3, exposure_type, document_id, document_date, counterparty_name, currency, total_original_amount, total_open_amount, value_date FROM exposure_headers WHERE entity = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to fetch exposures"})
			return
		}

		var exposures []Exposure
		var exposureIds []string
		for expRows.Next() {
			var e Exposure
			err := expRows.Scan(&e.ExposureHeaderID, &e.CompanyCode, &e.Entity, &e.Entity1, &e.Entity2, &e.Entity3, &e.ExposureType, &e.DocumentID, &e.DocumentDate, &e.CounterpartyName, &e.Currency, &e.TotalOriginalAmount, &e.TotalOpenAmount, &e.ValueDate)
			if err == nil {
				exposures = append(exposures, e)
				exposureIds = append(exposureIds, e.ExposureHeaderID)
			}
		}
		expRows.Close()
		// 4. Fetch all hedged values in one query
		hedgeMap := make(map[string]float64)
		if len(exposureIds) > 0 {
			hedgeRows, err := db.Query(`SELECT exposure_header_id, COALESCE(SUM(hedged_amount), 0) AS hedged_value FROM exposure_hedge_links WHERE exposure_header_id = ANY($1) GROUP BY exposure_header_id`, pq.Array(exposureIds))
			if err == nil {
				for hedgeRows.Next() {
					var eid string
					var hedgedValue float64
					if err := hedgeRows.Scan(&eid, &hedgedValue); err == nil {
						hedgeMap[eid] = hedgedValue
					}
				}
				hedgeRows.Close()
			}
		}
		// 5. Build summary

		var summary []Summary
		for _, exp := range exposures {
			hedgedValue := hedgeMap[exp.ExposureHeaderID]
			unhedgedValue := exp.TotalOpenAmount - hedgedValue
			maturityMonth := ""
			if exp.DocumentDate != "" {
				// Format MM-YY
				// Assume DocumentDate is YYYY-MM-DD
				parts := strings.Split(exp.DocumentDate, "-")
				if len(parts) >= 2 {
					mm := parts[1]
					yy := ""
					if len(parts[0]) == 4 {
						yy = parts[0][2:]
					}
					maturityMonth = mm + "-" + yy
				}
			}
			summary = append(summary, Summary{
				ExposureHeaderID: exp.ExposureHeaderID,
				CompanyCode:      exp.CompanyCode,
				Entity:           exp.Entity,
				Entity1: func() string {
					if exp.Entity1.Valid {
						return exp.Entity1.String
					} else {
						return ""
					}
				}(),
				Entity2: func() string {
					if exp.Entity2.Valid {
						return exp.Entity2.String
					} else {
						return ""
					}
				}(),
				Entity3: func() string {
					if exp.Entity3.Valid {
						return exp.Entity3.String
					} else {
						return ""
					}
				}(),
				ExposureType:        exp.ExposureType,
				DocumentID:          exp.DocumentID,
				DocumentDate:        exp.DocumentDate,
				MaturityMonth:       maturityMonth,
				CounterpartyName:    exp.CounterpartyName,
				Currency:            exp.Currency,
				TotalOriginalAmount: exp.TotalOriginalAmount,
				TotalOpenAmount:     exp.TotalOpenAmount,
				ValueDate:           exp.ValueDate,
				HedgedValue:         hedgedValue,
				UnhedgedValue:       unhedgedValue,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"summary": summary})
	}
}

// Handler: GetLinkedSummaryByCategory
func GetLinkedSummaryByCategory(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "user_id required"})
			return
		}
		// User verification via middleware
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "No accessible business units found"})
			return
		}
		// 1. Fwd Booking (filter by buNames)
		fwdRows, err := db.Query(`SELECT * FROM forward_bookings WHERE entity_level_0 = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to fetch forward bookings"})
			return
		}
		var fwdBooking []map[string]interface{}
		cols, _ := fwdRows.Columns()
		for fwdRows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := fwdRows.Scan(valPtrs...); err == nil {
				rowMap := make(map[string]interface{})
				for i, col := range cols {
					rowMap[col] = vals[i]
				}
				fwdBooking = append(fwdBooking, rowMap)
			}
		}
		fwdRows.Close()

		// 2. Fwd Rollovers (join to forward_bookings for entity fields, filter by buNames)
		rollRows, err := db.Query(`SELECT r.*, b.entity_level_0, b.entity_level_1, b.entity_level_2, b.entity_level_3 FROM forward_rollovers r LEFT JOIN forward_bookings b ON r.booking_id = b.system_transaction_id WHERE b.entity_level_0 = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to fetch forward rollovers"})
			return
		}
		var fwdRollovers []map[string]interface{}
		rollCols, _ := rollRows.Columns()
		for rollRows.Next() {
			vals := make([]interface{}, len(rollCols))
			valPtrs := make([]interface{}, len(rollCols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rollRows.Scan(valPtrs...); err == nil {
				rowMap := make(map[string]interface{})
				for i, col := range rollCols {
					rowMap[col] = vals[i]
				}
				fwdRollovers = append(fwdRollovers, rowMap)
			}
		}
		rollRows.Close()

		// 3. Fwd Cancellation (join to forward_bookings for entity fields, filter by buNames)
		cancelRows, err := db.Query(`SELECT c.*, b.entity_level_0, b.entity_level_1, b.entity_level_2, b.entity_level_3 FROM forward_cancellations c LEFT JOIN forward_bookings b ON c.booking_id = b.system_transaction_id WHERE b.entity_level_0 = ANY($1)`, pq.Array(buNames))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "Failed to fetch forward cancellations"})
			return
		}
		var fwdCancellation []map[string]interface{}
		cancelCols, _ := cancelRows.Columns()
		for cancelRows.Next() {
			vals := make([]interface{}, len(cancelCols))
			valPtrs := make([]interface{}, len(cancelCols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := cancelRows.Scan(valPtrs...); err == nil {
				rowMap := make(map[string]interface{})
				for i, col := range cancelCols {
					rowMap[col] = vals[i]
				}
				fwdCancellation = append(fwdCancellation, rowMap)
			}
		}
		cancelRows.Close()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"Fwd Booking":      fwdBooking,
			"Fwd Rollovers":    fwdRollovers,
			"Fwd Cancellation": fwdCancellation,
		})
	}
}
