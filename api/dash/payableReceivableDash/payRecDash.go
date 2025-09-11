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
