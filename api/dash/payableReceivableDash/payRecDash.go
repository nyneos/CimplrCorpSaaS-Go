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

	// tie queries to the request context and add a timeout to avoid runaway queries
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

		out := make([]PayRecRow, 0)

		// Payables
	// Avoid per-row correlated subqueries by selecting the latest audit row once
	payQ := `
		SELECT p.invoice_number, p.due_date, p.amount, p.currency_code, c.counterparty_name, p.entity_name
		FROM tr_payables p
		JOIN mastercounterparty c ON p.counterparty_name = c.counterparty_name
		JOIN (
		  SELECT DISTINCT ON (payable_id) payable_id, processing_status
		  FROM auditactionpayable
		  ORDER BY payable_id, requested_at DESC
		) a ON a.payable_id = p.payable_id
		WHERE a.processing_status = 'APPROVED'
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
	// Use the same DISTINCT ON trick for receivables
	recQ := `
		SELECT r.invoice_number, r.due_date, r.invoice_amount, r.currency_code, c.counterparty_name, r.entity_name
		FROM tr_receivables r
		JOIN mastercounterparty c ON r.counterparty_name = c.counterparty_name
		JOIN (
		  SELECT DISTINCT ON (receivable_id) receivable_id, processing_status
		  FROM auditactionreceivable
		  ORDER BY receivable_id, requested_at DESC
		) a ON a.receivable_id = r.receivable_id
		WHERE a.processing_status = 'APPROVED'
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

	// bind to request context with timeout to guard long-running forecasts
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

		// fetch canonical payables within max range using the due_date index only
		// then fetch latest audit processing_status only for those ids (avoids scanning the whole audit table)
		type payRow struct {
			Entity      string
			Counterparty string
			Due         time.Time
			Amt         float64
			Currency    string
			ID          string
		}
		payQ := `
			SELECT COALESCE(me.entity_name, p.entity_name) AS entity_name, c.counterparty_name, p.due_date, p.amount, p.currency_code, p.payable_id
			FROM tr_payables p
			JOIN mastercounterparty c ON p.counterparty_name = c.counterparty_name
			LEFT JOIN masterentitycash me ON p.entity_name = me.entity_name
			WHERE p.due_date BETWEEN $1 AND $2
		`
		payRows := make([]payRow, 0)
		rows, err := pgxPool.Query(ctx, payQ, start, qEnd)
		if err == nil {
			for rows.Next() {
				var pr payRow
				if err := rows.Scan(&pr.Entity, &pr.Counterparty, &pr.Due, &pr.Amt, &pr.Currency, &pr.ID); err != nil {
					continue
				}
				payRows = append(payRows, pr)
			}
			rows.Close()
		}

		// if we have any payables in range, fetch their latest audit status in one small query
		if len(payRows) > 0 {
			ids := make([]string, 0, len(payRows))
			for _, r := range payRows {
				ids = append(ids, r.ID)
			}

			auditsQ := `
				SELECT DISTINCT ON (payable_id) payable_id, processing_status
				FROM auditactionpayable
				WHERE payable_id = ANY($1)
				ORDER BY payable_id, requested_at DESC
			`
			statusMap := map[string]string{}
			arows, err2 := pgxPool.Query(ctx, auditsQ, ids)
			if err2 == nil {
				for arows.Next() {
					var id, st string
					if err := arows.Scan(&id, &st); err != nil {
						continue
					}
					statusMap[id] = st
				}
				arows.Close()
			}

			for _, r := range payRows {
				if statusMap[r.ID] != "APPROVED" {
					continue
				}
				// determine bucket
				var key string
				switch {
				case !r.Due.Before(ranges[0].Start) && !r.Due.After(ranges[0].End):
					key = "next30"
				case !r.Due.Before(ranges[1].Start) && !r.Due.After(ranges[1].End):
					key = "next60"
				case !r.Due.Before(ranges[2].Start) && !r.Due.After(ranges[2].End):
					key = "quarter"
				default:
					continue
				}
				if _, ok := buckets[key][r.Entity]; !ok {
					buckets[key][r.Entity] = map[string]map[string]map[string]float64{}
				}
				if _, ok := buckets[key][r.Entity][r.Counterparty]; !ok {
					buckets[key][r.Entity][r.Counterparty] = map[string]map[string]float64{}
				}
				if _, ok := buckets[key][r.Entity][r.Counterparty]["Payable"]; !ok {
					buckets[key][r.Entity][r.Counterparty]["Payable"] = map[string]float64{}
				}
				buckets[key][r.Entity][r.Counterparty]["Payable"][r.Currency] += r.Amt
			}
		}

				// fetch canonical receivables within max range, then fetch latest audit status for those ids
		type recRow struct {
			Entity      string
			Counterparty string
			Due         time.Time
			Amt         float64
			Currency    string
			ID          string
		}
		recQ := `
			SELECT COALESCE(me.entity_name, r.entity_name) AS entity_name, c.counterparty_name, r.due_date, r.invoice_amount, r.currency_code, r.receivable_id
			FROM tr_receivables r
			JOIN mastercounterparty c ON r.counterparty_name = c.counterparty_name
			LEFT JOIN masterentitycash me ON r.entity_name = me.entity_name
			WHERE r.due_date BETWEEN $1 AND $2
		`
		recRows := make([]recRow, 0)
		rows2, err := pgxPool.Query(ctx, recQ, start, qEnd)
		if err == nil {
			for rows2.Next() {
				var rr recRow
				if err := rows2.Scan(&rr.Entity, &rr.Counterparty, &rr.Due, &rr.Amt, &rr.Currency, &rr.ID); err != nil {
					continue
				}
				recRows = append(recRows, rr)
			}
			rows2.Close()
		}

		if len(recRows) > 0 {
			ids := make([]string, 0, len(recRows))
			for _, r := range recRows {
				ids = append(ids, r.ID)
			}
			auditsQ := `
				SELECT DISTINCT ON (receivable_id) receivable_id, processing_status
				FROM auditactionreceivable
				WHERE receivable_id = ANY($1)
				ORDER BY receivable_id, requested_at DESC
			`
			statusMap := map[string]string{}
			arows, err2 := pgxPool.Query(ctx, auditsQ, ids)
			if err2 == nil {
				for arows.Next() {
					var id, st string
					if err := arows.Scan(&id, &st); err != nil {
						continue
					}
					statusMap[id] = st
				}
				arows.Close()
			}

			for _, r := range recRows {
				if statusMap[r.ID] != "APPROVED" {
					continue
				}
				var key string
				switch {
				case !r.Due.Before(ranges[0].Start) && !r.Due.After(ranges[0].End):
					key = "next30"
				case !r.Due.Before(ranges[1].Start) && !r.Due.After(ranges[1].End):
					key = "next60"
				case !r.Due.Before(ranges[2].Start) && !r.Due.After(ranges[2].End):
					key = "quarter"
				default:
					continue
				}
				if _, ok := buckets[key][r.Entity]; !ok {
					buckets[key][r.Entity] = map[string]map[string]map[string]float64{}
				}
				if _, ok := buckets[key][r.Entity][r.Counterparty]; !ok {
					buckets[key][r.Entity][r.Counterparty] = map[string]map[string]float64{}
				}
				if _, ok := buckets[key][r.Entity][r.Counterparty]["Receivable"]; !ok {
					buckets[key][r.Entity][r.Counterparty]["Receivable"] = map[string]float64{}
				}
				buckets[key][r.Entity][r.Counterparty]["Receivable"][r.Currency] += r.Amt
			}
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
