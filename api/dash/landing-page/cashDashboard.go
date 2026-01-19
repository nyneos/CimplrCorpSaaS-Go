package landingpage

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"time"
	"log"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NOTE: These INR conversion rates are example values. Replace with live FX rates or a rates table.
var inrRates = map[string]float64{
	"INR": 1.0,
	"USD": 82.5,
	"EUR": 90.0,
	"GBP": 102.0,
	"JPY": 0.61,
	"AUD": 55.0,
	"CAD": 60.0,
	"CHF": 90.0,
	"CNY": 11.5,
	"RMB": 11.5,
	"SEK": 7.5,
	"AED": 22.5,
}

func toINR(amount float64, currency string) float64 {
	if amount == 0 {
		return 0
	}
	cur := strings.ToUpper(strings.TrimSpace(currency))
	rate, ok := inrRates[cur]
	if !ok || rate == 0 {
		// fallback assume INR
		return amount
	}
	// multiply by rate to get INR (rates map is INR per unit currency)
	return amount * rate
}

// GetLandingCashDashboard returns aggregated data for the landing page cash dashboard.
// Accepts JSON body: { "user_id":"...", "horizon":70, "bank":"optional", "account":"optional", "entity":"optional", "currency":"optional" }
func GetLandingCashDashboard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID   string `json:"user_id"`
		Horizon  int    `json:"horizon"`
		Bank     string `json:"bank,omitempty"`
		Account  string `json:"account,omitempty"`
		Entity   string `json:"entity,omitempty"`
		Currency string `json:"currency,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: "invalid json body"})
			return
		}

		if req.Horizon <= 0 {
			req.Horizon = 70 // default horizon days (approx 10 weeks)
		}

		ctx := context.Background()

		// 1) Fetch latest APPROVED balances per account with account metadata
		dateToday := time.Now().UTC().Format(constants.DateFormat)
		q := `
SELECT DISTINCT ON (COALESCE(bbm.account_no, bbm.iban, bbm.nickname))
	bbm.account_no, COALESCE(bbm.currency_code,'INR') as currency_code, COALESCE(bbm.closing_balance,0)::float8 as closing_balance,
	COALESCE(mba.bank_name,'') as bank_name, COALESCE(me.entity_name,'') as entity_name,
	COALESCE(bbm.country, mba.country, '') as country
FROM bank_balances_manual bbm
JOIN masterbankaccount mba ON bbm.account_no = mba.account_number
LEFT JOIN masterentitycash me ON mba.entity_id = me.entity_id
JOIN (
  SELECT DISTINCT ON (balance_id) balance_id, processing_status
  FROM auditactionbankbalances
  ORDER BY balance_id, requested_at DESC
) a ON a.balance_id = bbm.balance_id AND a.processing_status = 'APPROVED'
WHERE bbm.as_of_date <= $1
ORDER BY COALESCE(bbm.account_no, bbm.iban, bbm.nickname), bbm.as_of_date DESC, bbm.as_of_time DESC
`

		rows, err := pgxPool.Query(ctx, q, dateToday)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
			return
		}
		defer rows.Close()

		type balRow struct {
			Account  string
			Currency string
			Balance  float64
			Bank     string
			Entity   string
			Country  string
		}
		balRows := []balRow{}
		for rows.Next() {
			var a, cur, bank, ent, country string
			var bal float64
			if err := rows.Scan(&a, &cur, &bal, &bank, &ent, &country); err != nil {
				continue
			}
			// apply filters if provided
			if req.Account != "" && strings.TrimSpace(req.Account) != a {
				continue
			}
			if req.Bank != "" && !strings.EqualFold(strings.TrimSpace(req.Bank), strings.TrimSpace(bank)) {
				continue
			}
			if req.Entity != "" && !strings.EqualFold(strings.TrimSpace(req.Entity), strings.TrimSpace(ent)) {
				continue
			}
			if req.Currency != "" && !strings.EqualFold(strings.TrimSpace(req.Currency), strings.TrimSpace(cur)) {
				continue
			}

			balRows = append(balRows, balRow{Account: a, Currency: cur, Balance: bal, Bank: bank, Entity: ent, Country: country})
		}

		// compute KPIs
		var totalINR float64
		currencySums := map[string]float64{}
		bankSums := map[string]float64{}
		entitySums := map[string]float64{}
		accountSums := map[string]float64{}
		countrySums := map[string]float64{}

		for _, r0 := range balRows {
			v := toINR(r0.Balance, r0.Currency)
			totalINR += v
			currencySums[strings.ToUpper(r0.Currency)] += v
			bankSums[r0.Bank] += v
			entitySums[r0.Entity] += v
			accountSums[r0.Account] += v
			countrySums[strings.ToUpper(strings.TrimSpace(r0.Country))] += v
		}

		// Round aggregates to 2 decimals to avoid tiny float artifacts
		totalINR = math.Round(totalINR*100) / 100
		for k, v := range currencySums {
			currencySums[k] = math.Round(v*100) / 100
		}
		for k, v := range bankSums {
			bankSums[k] = math.Round(v*100) / 100
		}
		for k, v := range entitySums {
			entitySums[k] = math.Round(v*100) / 100
		}
		for k, v := range accountSums {
			accountSums[k] = math.Round(v*100) / 100
		}
		for k, v := range countrySums {
			countrySums[k] = math.Round(v*100) / 100
		}

		// Build KPI responses (string formatting kept simple)
		topKPIs := []map[string]interface{}{
			{"title": "Total Cash & Equivalents", "value": totalINR},
			{"title": "Total Multi-Currency Balances", "value": func() float64 {
				var s float64
				for k, v := range currencySums {
					if k != "INR" {
						s += v
					}
				}
				return s
			}()},
			{"title": "Total Bank Balances", "value": totalINR},
			{"title": "Total Entity Balances", "value": totalINR},
		}

		// 2) Forecast KPIs: inflows/outflows over horizon
		start := time.Now().UTC()
		end := start.AddDate(0, 0, req.Horizon-1)
		startStr := start.Format(constants.DateFormat)
		endStr := end.Format(constants.DateFormat)

		// Actual inflows (receivables) - approved
		inflowQ := `SELECT COALESCE(SUM(r.invoice_amount),0)::float8 as sum_amount, COALESCE(r.currency_code,'INR') FROM tr_receivables r
JOIN auditactionreceivable a ON a.receivable_id = r.receivable_id AND a.processing_status = 'APPROVED'
WHERE r.due_date BETWEEN $1 AND $2 GROUP BY r.currency_code`
		inflowRows, err := pgxPool.Query(ctx, inflowQ, startStr, endStr)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
			return
		}
		var actualInflowsINR float64
		for inflowRows.Next() {
			var amt float64
			var cur string
			if err := inflowRows.Scan(&amt, &cur); err != nil {
				continue
			}
			actualInflowsINR += toINR(amt, cur)
		}
		inflowRows.Close()
		actualInflowsINR = math.Round(actualInflowsINR*100) / 100

		// Actual outflows (payables) - approved
		outflowQ := `SELECT COALESCE(SUM(p.amount),0)::float8 as sum_amount, COALESCE(p.currency_code,'INR') FROM tr_payables p
JOIN auditactionpayable a ON a.payable_id = p.payable_id AND a.processing_status = 'APPROVED'
WHERE p.due_date BETWEEN $1 AND $2 GROUP BY p.currency_code`
		outflowRows, err := pgxPool.Query(ctx, outflowQ, startStr, endStr)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
			return
		}
		var actualOutflowsINR float64
		for outflowRows.Next() {
			var amt float64
			var cur string
			if err := outflowRows.Scan(&amt, &cur); err != nil {
				continue
			}
			actualOutflowsINR += toINR(amt, cur)
		}
		outflowRows.Close()
		actualOutflowsINR = math.Round(actualOutflowsINR*100) / 100

		// Forecasts: use cashflow_proposal_item.expected_amount where start_date between
		forecastQ := `SELECT COALESCE(SUM(i.expected_amount),0)::float8 as sum_amount, COALESCE(p.currency_code,'INR')
FROM cashflow_proposal_item i
JOIN cashflow_proposal p ON i.proposal_id = p.proposal_id
WHERE i.start_date BETWEEN $1 AND $2
AND (
  SELECT a.processing_status FROM audit_action_cashflow_proposal a WHERE a.proposal_id = p.proposal_id ORDER BY a.requested_at DESC LIMIT 1
) = 'APPROVED' GROUP BY p.currency_code`
		frows, err := pgxPool.Query(ctx, forecastQ, startStr, endStr)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
			return
		}
		var forecastTotalINR float64
		for frows.Next() {
			var amt float64
			var cur string
			if err := frows.Scan(&amt, &cur); err != nil {
				continue
			}
			forecastTotalINR += toINR(amt, cur)
		}
		frows.Close()
		forecastTotalINR = math.Round(forecastTotalINR*100) / 100

		// Build Forecast KPI set similar to mockForecastKPIs
		projectedClosing := math.Round((totalINR + (actualInflowsINR - actualOutflowsINR)) * 100) / 100
		liquidityGap := math.Round(math.Abs(projectedClosing - totalINR) * 100) / 100
		forecastKPIs := []map[string]interface{}{
			{"title": "Current Balance", "value": totalINR},
			{"title": "Projected Inflows", "value": actualInflowsINR},
			{"title": "Projected Outflows", "value": actualOutflowsINR},
			{"title": "Projected Closing", "value": projectedClosing},
			{"title": "Liquidity Gap", "value": liquidityGap},
		}

		// Build pie datasets
		type PieDatum struct {
			ID, Label, Color string
			Value            float64
		}
		currencyPie := []PieDatum{}
		for cur, val := range currencySums {
			currencyPie = append(currencyPie, PieDatum{ID: cur, Label: cur, Color: "#0f766e", Value: val})
		}
		bankPie := []PieDatum{}
		for b, val := range bankSums {
			bankPie = append(bankPie, PieDatum{ID: b, Label: b, Color: "#16a34a", Value: val})
		}
		entityPie := []PieDatum{}
		for e, val := range entitySums {
			entityPie = append(entityPie, PieDatum{ID: e, Label: e, Color: "#60a5fa", Value: val})
		}

		// Statement rows (top few)
		type StatementRow struct {
			Title  string  `json:"title"`
			Inflow float64 `json:"inflow"`
		}
		countryStatement := []StatementRow{}
		for k, v := range countrySums {
			if k == "" {
				k = "Unknown"
			}
			countryStatement = append(countryStatement, StatementRow{Title: k, Inflow: v})
		}
		accountStatement := []StatementRow{}
		for a, v := range accountSums {
			accountStatement = append(accountStatement, StatementRow{Title: a, Inflow: v})
		}
		currencyStatement := []StatementRow{}
		for c, v := range currencySums {
			currencyStatement = append(currencyStatement, StatementRow{Title: c, Inflow: v})
		}

		// Weekwise forecast vs actual (by week period) - use weekly buckets
		weeks := make([]map[string]interface{}, 0)
		// compute number of weeks from horizon
		numWeeks := req.Horizon / 7
		if numWeeks <= 0 {
			numWeeks = 10
		}
		startWeek := time.Now().UTC()
		for i := 0; i < numWeeks; i++ {
			ws := startWeek.AddDate(0, 0, i*7)
			we := ws.AddDate(0, 0, 6)
			wsStr := ws.Format(constants.DateFormat)
			weStr := we.Format(constants.DateFormat)

			// actual inflow
			var aIn float64
			var aOut float64
			// actual inflow
			arq := `SELECT COALESCE(SUM(invoice_amount),0)::float8, COALESCE(currency_code,'INR') FROM tr_receivables r
JOIN auditactionreceivable a ON a.receivable_id = r.receivable_id AND a.processing_status = 'APPROVED'
WHERE r.due_date BETWEEN $1 AND $2 GROUP BY r.currency_code`
			arrows, err := pgxPool.Query(ctx, arq, wsStr, weStr)
			if err != nil {
				log.Printf("[WARN] cashDashboard actual inflow query failed for %s - %s: %v", wsStr, weStr, err)
			} else {
				for arrows.Next() {
					var amt float64
					var cur string
					if err := arrows.Scan(&amt, &cur); err == nil {
						aIn += toINR(amt, cur)
					}
				}
				arrows.Close()
			}

			// actual outflow
			aoq := `SELECT COALESCE(SUM(amount),0)::float8, COALESCE(currency_code,'INR') FROM tr_payables p
JOIN auditactionpayable a ON a.payable_id = p.payable_id AND a.processing_status = 'APPROVED'
WHERE p.due_date BETWEEN $1 AND $2 GROUP BY p.currency_code`
			arows2, err := pgxPool.Query(ctx, aoq, wsStr, weStr)
			if err != nil {
				log.Printf("[WARN] cashDashboard actual outflow query failed for %s - %s: %v", wsStr, weStr, err)
			} else {
				for arows2.Next() {
					var amt float64
					var cur string
					if err := arows2.Scan(&amt, &cur); err == nil {
						aOut += toINR(amt, cur)
					}
				}
				arows2.Close()
			}

			// forecast inflow/outflow from cashflow_proposal_item.start_date
			var fIn, fOut float64
			fq := `SELECT COALESCE(SUM(CASE WHEN i.cashflow_type='Inflow' THEN i.expected_amount ELSE 0 END),0)::float8,
COALESCE(SUM(CASE WHEN i.cashflow_type='Outflow' THEN i.expected_amount ELSE 0 END),0)::float8, COALESCE(p.currency_code,'INR')
FROM cashflow_proposal_item i JOIN cashflow_proposal p ON i.proposal_id = p.proposal_id
WHERE i.start_date BETWEEN $1 AND $2
AND (
  SELECT a.processing_status FROM audit_action_cashflow_proposal a WHERE a.proposal_id = p.proposal_id ORDER BY a.requested_at DESC LIMIT 1
) = 'APPROVED'
GROUP BY p.currency_code`
			frows2, err := pgxPool.Query(ctx, fq, wsStr, weStr)
			if err != nil {
				log.Printf("[WARN] cashDashboard forecast query failed for %s - %s: %v", wsStr, weStr, err)
			} else {
				for frows2.Next() {
					var infl, outf float64
					var cur string
					if err := frows2.Scan(&infl, &outf, &cur); err == nil {
						fIn += toINR(infl, cur)
						fOut += toINR(outf, cur)
					}
				}
				frows2.Close()
			}

			weeks = append(weeks, map[string]interface{}{
				"week":             ws.Format("Jan 2") + " - " + we.Format("Jan 2"),
				"forecast_inflow":  math.Round(fIn*100) / 100,
				"actual_inflow":    math.Round(aIn*100) / 100,
				"inflow_variance":  math.Round((fIn-aIn)*100) / 100,
				"forecast_outflow": math.Round(fOut*100) / 100,
				"actual_outflow":   math.Round(aOut*100) / 100,
				"outflow_variance": math.Round((fOut-aOut)*100) / 100,
				"net_variance":     math.Round(((fIn-fOut)-(aIn-aOut))*100) / 100,
			})
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"top_kpis":             topKPIs,
			"forecast_kpis":        forecastKPIs,
			"currency_pie":         currencyPie,
			"bank_pie":             bankPie,
			"entity_pie":           entityPie,
			"country_statement":    countryStatement,
			"account_statement":    accountStatement,
			"currency_statement":   currencyStatement,
			"weekwise":             weeks,
		})
	}
}
