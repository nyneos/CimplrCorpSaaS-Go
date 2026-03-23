package landingpage

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/dash/ticker"

	"github.com/jackc/pgx/v5/pgxpool"
)

// toINR converts an amount in `currency` to INR using the ticker package.
func toINR(amount float64, currency string) float64 {
	if amount == 0 {
		return 0
	}
	cur := strings.ToUpper(strings.TrimSpace(currency))
	rate, err := ticker.RateBetween(cur, "INR")
	if err != nil || rate == 0 {
		// fallback: treat amount as INR
		return amount
	}
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
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
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

		// 1) Fetch latest APPROVED balances per account with account metadata (using KPI pattern)
		q := `
		WITH latest_approved_balance AS (
			SELECT DISTINCT ON (bbm.account_no)
				bbm.account_no,
				COALESCE(bbm.closing_balance, 0) AS closing_balance,
				COALESCE(bbm.currency_code,'INR') as currency_code,
				COALESCE(bbm.nickname,'') as nickname,
				COALESCE(bbm.country, mba.country, '') as country
			FROM public.bank_balances_manual bbm
			JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
			JOIN masterbankaccount mba ON bbm.account_no = mba.account_number
			WHERE a.processing_status = 'APPROVED'
				AND mba.is_deleted = false
				AND COALESCE(mba.status, 'Active') = 'Active'
			ORDER BY bbm.account_no, bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
		)
		SELECT 
			lab.account_no,
			lab.nickname,
			lab.currency_code,
			lab.closing_balance::float8 as closing_balance,
			COALESCE(mba.bank_name,'') as bank_name,
			COALESCE(me.entity_name,'') as entity_name,
			lab.country
		FROM latest_approved_balance lab
		JOIN masterbankaccount mba ON lab.account_no = mba.account_number
		LEFT JOIN masterentitycash me ON me.entity_id::text = mba.entity_id
		ORDER BY lab.account_no
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, constants.ValueError: err.Error()})
			return
		}
		defer rows.Close()

		type balRow struct {
			Account  string
			Nickname string
			Currency string
			Balance  float64
			Bank     string
			Entity   string
			Country  string
		}
		balRows := []balRow{}
		for rows.Next() {
			var a, n, cur, bank, ent, country string
			var bal float64
			if err := rows.Scan(&a, &n, &cur, &bal, &bank, &ent, &country); err != nil {
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

			balRows = append(balRows, balRow{Account: a, Nickname: n, Currency: cur, Balance: bal, Bank: bank, Entity: ent, Country: country})
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

		// Actual inflows/outflows computed from approved bank statement transactions
		// Previous implementation used receivables/payables. Keep old queries commented
		// for reference.
		// OLD inflow/outflow queries (receivables/payables) are intentionally commented out.

		inflowQ := `SELECT COALESCE(SUM(COALESCE(t.deposit_amount,0)),0)::float8 AS sum_amount, COALESCE(NULLIF(m.currency,''),'INR') as currency_code
	FROM cimplrcorpsaas.bank_statement_transactions t
	JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
	JOIN cimplrcorpsaas.auditactionbankstatement a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
	LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
	WHERE t.value_date BETWEEN $1 AND $2 GROUP BY COALESCE(NULLIF(m.currency,''),'INR')`

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

		outflowQ := `SELECT COALESCE(SUM(COALESCE(t.withdrawal_amount,0)),0)::float8 AS sum_amount, COALESCE(NULLIF(m.currency,''),'INR') as currency_code
	FROM cimplrcorpsaas.bank_statement_transactions t
	JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
	JOIN cimplrcorpsaas.auditactionbankstatement a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
	LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
	WHERE t.value_date BETWEEN $1 AND $2 GROUP BY COALESCE(NULLIF(m.currency,''),'INR')`

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

		// Forecasts: aggregate monthly projections from cashflow_projection_monthly
		// Compute month-key range for the projection table (year*12 + month)
		monthStartKey := start.Year()*12 + int(start.Month())
		monthEndKey := end.Year()*12 + int(end.Month())

		// Diagnostic: log the month key range used for the projection query
		log.Printf("[DEBUG] projection month keys start=%d end=%d", monthStartKey, monthEndKey)

		// Diagnostic counts: total projection rows in range, and rows joined to approved proposals
		var rawCount int
		diagQ := `SELECT COUNT(*) FROM cimplrcorpsaas.cashflow_projection_monthly cpm WHERE (cpm.year*12 + cpm.month) BETWEEN $1 AND $2`
		if err := pgxPool.QueryRow(ctx, diagQ, monthStartKey, monthEndKey).Scan(&rawCount); err != nil {
			log.Printf("[DEBUG] projection diag raw count query error: %v", err)
		} else {
			log.Printf("[DEBUG] projection raw rows in range: %d", rawCount)
		}

		var approvedCount int
		diagQ2 := `SELECT COUNT(*) FROM cimplrcorpsaas.cashflow_projection_monthly cpm
JOIN cimplrcorpsaas.cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id
JOIN cimplrcorpsaas.cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
LEFT JOIN LATERAL (SELECT processing_status FROM cimplrcorpsaas.audit_action_cashflow_proposal a WHERE a.proposal_id = cp.proposal_id ORDER BY a.requested_at DESC LIMIT 1) aa ON TRUE
WHERE (cpm.year*12 + cpm.month) BETWEEN $1 AND $2 AND aa.processing_status = 'APPROVED'`
		if err := pgxPool.QueryRow(ctx, diagQ2, monthStartKey, monthEndKey).Scan(&approvedCount); err != nil {
			log.Printf("[DEBUG] projection diag approved count query error: %v", err)
		} else {
			log.Printf("[DEBUG] projection rows joined to approved proposals: %d", approvedCount)
		}

		// Aggregate projected inflows/outflows per currency for KPI totals
		projAggQ := `SELECT COALESCE(SUM(CASE WHEN cpi.cashflow_type='Inflow' THEN cpm.projected_amount ELSE 0 END),0)::float8 AS sum_in,
COALESCE(SUM(CASE WHEN cpi.cashflow_type='Outflow' THEN cpm.projected_amount ELSE 0 END),0)::float8 AS sum_out,
COALESCE(NULLIF(cpi.currency_code,''), COALESCE(cp.base_currency_code,'INR')) as currency_code
FROM cimplrcorpsaas.cashflow_projection_monthly cpm
JOIN cimplrcorpsaas.cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id
JOIN cimplrcorpsaas.cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
LEFT JOIN LATERAL (SELECT processing_status FROM cimplrcorpsaas.audit_action_cashflow_proposal a WHERE a.proposal_id = cp.proposal_id ORDER BY a.requested_at DESC LIMIT 1) aa ON TRUE
WHERE (cpm.year*12 + cpm.month) BETWEEN $1 AND $2
AND aa.processing_status = 'APPROVED' GROUP BY COALESCE(NULLIF(cpi.currency_code,''), COALESCE(cp.base_currency_code,'INR'))`

		aggRows, err := pgxPool.Query(ctx, projAggQ, monthStartKey, monthEndKey)
		if err != nil {
			log.Printf("[DEBUG] projection agg query error: %v", err)
		}

		var projectedInflowsINR float64
		var projectedOutflowsINR float64
		if aggRows != nil {
			for aggRows.Next() {
				var sumIn, sumOut float64
				var cur string
				if err := aggRows.Scan(&sumIn, &sumOut, &cur); err != nil {
					log.Printf("[DEBUG] projection agg scan error: %v", err)
					continue
				}
				projectedInflowsINR += toINR(sumIn, cur)
				projectedOutflowsINR += toINR(sumOut, cur)
			}
			aggRows.Close()
		}

		// Build daily projection maps by distributing monthly projections evenly across days of that month.
		projDetailQ := `SELECT cpm.year, cpm.month, cpm.projected_amount, cpi.cashflow_type,
COALESCE(NULLIF(cpi.currency_code,''), COALESCE(cp.base_currency_code,'INR')) as currency_code
FROM cimplrcorpsaas.cashflow_projection_monthly cpm
JOIN cimplrcorpsaas.cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id
JOIN cimplrcorpsaas.cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
LEFT JOIN LATERAL (SELECT processing_status FROM cimplrcorpsaas.audit_action_cashflow_proposal a WHERE a.proposal_id = cp.proposal_id ORDER BY a.requested_at DESC LIMIT 1) aa ON TRUE
WHERE (cpm.year*12 + cpm.month) BETWEEN $1 AND $2
AND aa.processing_status = 'APPROVED'`

		projRows, err := pgxPool.Query(ctx, projDetailQ, monthStartKey, monthEndKey)
		if err != nil {
			log.Printf("[DEBUG] projection detail query error: %v", err)
		}

		projectedDailyInflow := map[string]float64{}
		projectedDailyOutflow := map[string]float64{}
		if projRows != nil {
			for projRows.Next() {
				var yr, mon int
				var amt float64
				var ctype, cur string
				if err := projRows.Scan(&yr, &mon, &amt, &ctype, &cur); err != nil {
					log.Printf("[DEBUG] projection detail scan error: %v", err)
					continue
				}
				daysInMonth := time.Date(yr, time.Month(mon)+1, 0, 0, 0, 0, 0, time.UTC).Day()
				if daysInMonth <= 0 {
					continue
				}
				perDay := amt / float64(daysInMonth)
				for d := 1; d <= daysInMonth; d++ {
					dt := time.Date(yr, time.Month(mon), d, 0, 0, 0, 0, time.UTC)
					if dt.Before(start) || dt.After(end) {
						continue
					}
					key := dt.Format(constants.DateFormat)
					if strings.EqualFold(strings.TrimSpace(ctype), "Inflow") {
						projectedDailyInflow[key] += toINR(perDay, cur)
					} else {
						projectedDailyOutflow[key] += toINR(perDay, cur)
					}
				}
			}
			projRows.Close()
		}

		projectedInflowsINR = math.Round(projectedInflowsINR*100) / 100
		projectedOutflowsINR = math.Round(projectedOutflowsINR*100) / 100

		// Build Forecast KPI set similar to mockForecastKPIs
		projectedClosing := math.Round((totalINR+(projectedInflowsINR-projectedOutflowsINR))*100) / 100
		liquidityGap := math.Round(math.Abs(projectedClosing-totalINR)*100) / 100
		forecastKPIs := []map[string]interface{}{
			{"title": "Current Balance", "value": totalINR},
			{"title": "Projected Inflows", "value": projectedInflowsINR},
			{"title": "Projected Outflows", "value": projectedOutflowsINR},
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
			Title    string  `json:"title"`
			Inflow   float64 `json:"inflow"`
			Bank     string  `json:"bank"`
			Nickname string  `json:"nickname"`
		}
		countryStatement := []StatementRow{}
		for k, v := range countrySums {
			if k == "" {
				k = "Unknown"
			}
			countryStatement = append(countryStatement, StatementRow{Title: k, Inflow: v})
		}
		// build quick lookup maps for account -> bank/nickname so we can enrich the
		// account statement safely without additional DB hits
		accountToBank := map[string]string{}
		accountToNickname := map[string]string{}
		for _, br := range balRows {
			if br.Account != "" {
				accountToBank[br.Account] = br.Bank
				accountToNickname[br.Account] = br.Nickname
			}
		}

		accountStatement := []StatementRow{}
		for a, v := range accountSums {
			accountStatement = append(accountStatement, StatementRow{Title: a, Inflow: v, Bank: accountToBank[a], Nickname: accountToNickname[a]})
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

			// actual inflow/outflow for the week derived from approved bank statement transactions
			var aIn float64
			var aOut float64
			// OLD: receivables/payables queries retained as comments for reference
			// arq := `SELECT COALESCE(SUM(invoice_amount),0)::float8, COALESCE(currency_code,'INR') FROM tr_receivables r
			// JOIN auditactionreceivable a ON a.receivable_id = r.receivable_id AND a.processing_status = 'APPROVED'
			// WHERE r.due_date BETWEEN $1 AND $2 GROUP BY r.currency_code`
			arq := `SELECT COALESCE(SUM(COALESCE(t.deposit_amount,0)),0)::float8 AS sum_amount, COALESCE(NULLIF(m.currency,''),'INR') as currency_code
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
JOIN cimplrcorpsaas.auditactionbankstatement a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
WHERE t.value_date BETWEEN $1 AND $2 GROUP BY COALESCE(NULLIF(m.currency,''),'INR')`
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
			// aoq := `SELECT COALESCE(SUM(amount),0)::float8, COALESCE(currency_code,'INR') FROM tr_payables p
			// JOIN auditactionpayable a ON a.payable_id = p.payable_id AND a.processing_status = 'APPROVED'
			// WHERE p.due_date BETWEEN $1 AND $2 GROUP BY p.currency_code`
			aoq := `SELECT COALESCE(SUM(COALESCE(t.withdrawal_amount,0)),0)::float8 AS sum_amount, COALESCE(NULLIF(m.currency,''),'INR') as currency_code
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
JOIN cimplrcorpsaas.auditactionbankstatement a ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
WHERE t.value_date BETWEEN $1 AND $2 GROUP BY COALESCE(NULLIF(m.currency,''),'INR')`
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

			// forecast inflow/outflow: sum daily-prorated monthly projections for the week
			var fIn, fOut float64
			// iterate each day of the week and sum from the projected daily maps
			for d := ws; !d.After(we); d = d.AddDate(0, 0, 1) {
				key := d.Format(constants.DateFormat)
				if v, ok := projectedDailyInflow[key]; ok {
					fIn += v
				}
				if v, ok := projectedDailyOutflow[key]; ok {
					fOut += v
				}
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
