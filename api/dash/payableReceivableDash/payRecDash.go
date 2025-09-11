package payablereceivabledash

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PayRecRow struct {
	Counterparty string  `json:"counterparty"`
	Type         string  `json:"type"`
	InvoiceNo    string  `json:"invoice_no"`
	DueDate      string  `json:"due_date"`
	Amount       float64 `json:"amount"`
	Entity       string  `json:"entity"`
}

// GetPayablesReceivables returns combined payable and receivable rows for the dashboard
func GetPayablesReceivables(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// static spot rates to USD (example/fallback)
	spotRates := map[string]float64{
		"USD": 1.0,
		"INR": 0.0117,
		"EUR": 1.09,
		"GBP": 1.28,
		"AUD": 0.68,
		"CAD": 0.75,
		"CHF": 1.1,
		"CNY": 0.14,
		"JPY": 0.0067,
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithResult(w, false, "Method not allowed")
			return
		}

		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			api.RespondWithResult(w, false, "Missing or invalid user_id in body")
			return
		}

		ctx := context.Background()

		out := make([]PayRecRow, 0)

		// Payables
		payQ := `
			SELECT p.invoice_number, p.due_date, p.amount, p.currency_code, c.counterparty_name, p.entity_id
			FROM payables p
			JOIN mastercounterparty c ON p.vendor_id = c.counterparty_id
			WHERE (SELECT a.processing_status FROM auditactionpayable a WHERE a.payable_id = p.payable_id ORDER BY a.requested_at DESC LIMIT 1) = 'APPROVED'
			ORDER BY p.due_date
		`
		rows, err := pgxPool.Query(ctx, payQ)
		if err != nil {
			api.RespondWithResult(w, false, "DB error: "+err.Error())
			return
		}
		for rows.Next() {
			var invoice string
			var due time.Time
			var amt float64
			var currency, counterparty, entity string
			if err := rows.Scan(&invoice, &due, &amt, &currency, &counterparty, &entity); err != nil {
				continue
			}
			rate := 1.0
			if r, ok := spotRates[currency]; ok && r > 0 {
				rate = r
			}
			converted := math.Abs(amt) * rate
			out = append(out, PayRecRow{
				Counterparty: counterparty,
				Type:         "Payable",
				InvoiceNo:    invoice,
				DueDate:      due.Format("2006-01-02"),
				Amount:       converted,
				Entity:       entity,
			})
		}
		rows.Close()

		// Receivables
		recQ := `
			SELECT r.invoice_number, r.due_date, r.invoice_amount, r.currency_code, c.counterparty_name, r.entity_id
			FROM receivables r
			JOIN mastercounterparty c ON r.customer_id = c.counterparty_id
			WHERE (SELECT a.processing_status FROM auditactionreceivable a WHERE a.receivable_id = r.receivable_id ORDER BY a.requested_at DESC LIMIT 1) = 'APPROVED'
			ORDER BY r.due_date
		`
		rows2, err := pgxPool.Query(ctx, recQ)
		if err != nil {
			api.RespondWithResult(w, false, "DB error: "+err.Error())
			return
		}
		for rows2.Next() {
			var invoice string
			var due time.Time
			var amt float64
			var currency, counterparty, entity string
			if err := rows2.Scan(&invoice, &due, &amt, &currency, &counterparty, &entity); err != nil {
				continue
			}
			rate := 1.0
			if r, ok := spotRates[currency]; ok && r > 0 {
				rate = r
			}
			converted := math.Abs(amt) * rate
			out = append(out, PayRecRow{
				Counterparty: counterparty,
				Type:         "Receivable",
				InvoiceNo:    invoice,
				DueDate:      due.Format("2006-01-02"),
				Amount:       converted,
				Entity:       entity,
			})
		}
		rows2.Close()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": out})
	}
}

// GetPayRecForecast returns a nested forecast grouped by date ranges -> entities -> counterparties -> currencies
func GetPayRecForecast(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithResult(w, false, "Method not allowed")
			return
		}
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			api.RespondWithResult(w, false, "Missing or invalid user_id in body")
			return
		}

		today := time.Now().UTC()
		start := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
		next30 := start.AddDate(0, 0, 30)
		next60 := start.AddDate(0, 0, 60)
		// quarter end
		month := int(start.Month())
		q := (month-1)/3 + 1
		qEndMonth := q * 3
		qEnd := time.Date(start.Year(), time.Month(qEndMonth), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, -1) // last day of quarter

		// top-level map: bucket -> entity -> counterparty -> type -> currency -> amount
		buckets := map[string]map[string]map[string]map[string]map[string]float64{}
		// make buckets exclusive so they don't all start from 'start'
		next60Start := next30.AddDate(0, 0, 1)
		quarterStart := next60.AddDate(0, 0, 1)
		ranges := []struct {
			Key, Label string
			Start, End time.Time
		}{
			{"next30", "Next 30 Days", start, next30},
			{"next60", "Next 60 Days", next60Start, next60},
			{"quarter", "This Quarter", quarterStart, qEnd},
		}
		for _, b := range ranges {
			buckets[b.Key] = map[string]map[string]map[string]map[string]float64{}
		}

		ctx := context.Background()

		// fetch approved payables within max range (start..qEnd)
		payQ := `
			SELECT COALESCE(me.entity_name, p.entity_id) AS entity_name, c.counterparty_name, p.due_date, p.amount, p.currency_code
			FROM payables p
			JOIN mastercounterparty c ON p.vendor_id = c.counterparty_id
			LEFT JOIN masterentitycash me ON p.entity_id = me.entity_id
			WHERE p.due_date BETWEEN $1 AND $2
			  AND (SELECT a.processing_status FROM auditactionpayable a WHERE a.payable_id = p.payable_id ORDER BY a.requested_at DESC LIMIT 1) = 'APPROVED'
		`
		rows, err := pgxPool.Query(ctx, payQ, start, qEnd)
		if err == nil {
			for rows.Next() {
				var entity, counterparty, currency string
				var due time.Time
				var amt float64
				if err := rows.Scan(&entity, &counterparty, &due, &amt, &currency); err != nil {
					continue
				}
				// determine bucket
				var key string
				switch {
				case !due.Before(ranges[0].Start) && !due.After(ranges[0].End):
					key = "next30"
				case !due.Before(ranges[1].Start) && !due.After(ranges[1].End):
					key = "next60"
				case !due.Before(ranges[2].Start) && !due.After(ranges[2].End):
					key = "quarter"
				default:
					continue
				}
				if _, ok := buckets[key][entity]; !ok {
					buckets[key][entity] = map[string]map[string]map[string]float64{}
				}
				if _, ok := buckets[key][entity][counterparty]; !ok {
					buckets[key][entity][counterparty] = map[string]map[string]float64{}
				}
				if _, ok := buckets[key][entity][counterparty]["Payable"]; !ok {
					buckets[key][entity][counterparty]["Payable"] = map[string]float64{}
				}
				buckets[key][entity][counterparty]["Payable"][currency] += amt
			}
			rows.Close()
		}

		// fetch approved receivables within max range
		recQ := `
			SELECT COALESCE(me.entity_name, r.entity_id) AS entity_name, c.counterparty_name, r.due_date, r.invoice_amount, r.currency_code
			FROM receivables r
			JOIN mastercounterparty c ON r.customer_id = c.counterparty_id
			LEFT JOIN masterentitycash me ON r.entity_id = me.entity_id
			WHERE r.due_date BETWEEN $1 AND $2
			  AND (SELECT a.processing_status FROM auditactionreceivable a WHERE a.receivable_id = r.receivable_id ORDER BY a.requested_at DESC LIMIT 1) = 'APPROVED'
		`
		rows2, err := pgxPool.Query(ctx, recQ, start, qEnd)
		if err == nil {
			for rows2.Next() {
				var entity, counterparty, currency string
				var due time.Time
				var amt float64
				if err := rows2.Scan(&entity, &counterparty, &due, &amt, &currency); err != nil {
					continue
				}
				var key string
				switch {
				case !due.Before(ranges[0].Start) && !due.After(ranges[0].End):
					key = "next30"
				case !due.Before(ranges[1].Start) && !due.After(ranges[1].End):
					key = "next60"
				case !due.Before(ranges[2].Start) && !due.After(ranges[2].End):
					key = "quarter"
				default:
					continue
				}
				if _, ok := buckets[key][entity]; !ok {
					buckets[key][entity] = map[string]map[string]map[string]float64{}
				}
				if _, ok := buckets[key][entity][counterparty]; !ok {
					buckets[key][entity][counterparty] = map[string]map[string]float64{}
				}
				if _, ok := buckets[key][entity][counterparty]["Receivable"]; !ok {
					buckets[key][entity][counterparty]["Receivable"] = map[string]float64{}
				}
				buckets[key][entity][counterparty]["Receivable"][currency] += amt
			}
			rows2.Close()
		}

		// build response matching the mock shape
		resp := make([]map[string]interface{}, 0, len(ranges))
		for _, b := range ranges {
			entArr := make([]map[string]interface{}, 0)
			for entity, cps := range buckets[b.Key] {
				cpArr := make([]map[string]interface{}, 0)
				for cpName, types := range cps {
					// produce currencies array with explicit type per entry to match frontend mock
					curArr := make([]map[string]interface{}, 0)
					for ttype, curMap := range types {
						for cur, v := range curMap {
							curArr = append(curArr, map[string]interface{}{"type": ttype, "currency": cur, "amount": v})
						}
					}
					cpArr = append(cpArr, map[string]interface{}{"name": cpName, "currencies": curArr})
				}
				entArr = append(entArr, map[string]interface{}{"entity": entity, "counterparties": cpArr})
			}
			resp = append(resp, map[string]interface{}{"date_range": b.Label, "entities": entArr})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "rows": resp})
	}
}
