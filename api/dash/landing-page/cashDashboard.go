package landingpage

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/dash/ticker"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// toINR converts an amount in `currency` to INR using the ticker package.
func toINR(amount float64, currency string) float64 {
	if amount == 0 {
		return 0
	}
	cur := strings.ToUpper(strings.TrimSpace(currency))
	rate, err := ticker.RateBetween(cur, "INR")
	if err != nil || rate == 0 {
		return amount
	}
	return amount * rate
}

// cleanFilter returns "" if val is empty or the "all" sentinel (case-insensitive).
func cleanFilter(val string) string {
	v := strings.TrimSpace(val)
	if strings.EqualFold(v, "all") {
		return ""
	}
	return v
}

// GetLandingCashDashboard returns aggregated data for the landing page cash dashboard.
//
// Accepted JSON body:
//
//	{
//	  "user_id":   "...",
//	  "horizon":   "30"  | 30,           // days; ignored when from_date+to_date are provided
//	  "from_date": "2025-01-01",          // custom range start (YYYY-MM-DD)
//	  "to_date":   constants.DateMaxCustom4,          // custom range end   (YYYY-MM-DD)
//	  "bank":      "HDFC Bank",           // optional; "" or "all" = no filter
//	  "account":   "1234567890",          // optional account_number
//	  "entity":    "<entity_id (UUID)>",  // optional; "" or "all" = no filter
//	  "currency":  "USD"                  // optional; "" or "all" = no filter
//	}
func GetLandingCashDashboard(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID   string `json:"user_id"`
		Horizon  string `json:"horizon,omitempty"`    // "7", "30", "90", "Year to Date", etc.
		FromDate string `json:"from_date,omitempty"`  // YYYY-MM-DD
		ToDate   string `json:"to_date,omitempty"`    // YYYY-MM-DD
		AsOnDate string `json:"as_on_date,omitempty"` // YYYY-MM-DD — snapshot date for balances
		Bank     string `json:"bank,omitempty"`
		Account  string `json:"account,omitempty"`
		Entity   string `json:"entity,omitempty"` // entity_id
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

		// Parse horizon — handles numeric strings ("7","30","90"), named periods,
		// and falls back to 70 days (≈ 10 weeks) when unrecognised or empty.
		horizon := 70
		h := strings.TrimSpace(req.Horizon)
		if n, err := strconv.Atoi(h); err == nil && n > 0 {
			horizon = n
		} else {
			switch strings.ToLower(h) {
			case "last 7 days", "7 days", "7":
				horizon = 7
			case "last 30 days", "30 days", "30":
				horizon = 30
			case "last 90 days", "90 days", "90":
				horizon = 90
			case "year to date", "ytd":
				// from Jan 1 of current year to today
				now := time.Now().UTC()
				horizon = int(now.Sub(time.Date(now.Year(), 1, 1, 0, 0, 0, 0, time.UTC)).Hours()/24) + 1
				// leave horizon = 70 for any other string
			}
		}

		// Normalise filters — treat "all" the same as blank (= no filter).
		filterBank := cleanFilter(req.Bank)
		filterAccount := cleanFilter(req.Account)
		filterEntity := cleanFilter(req.Entity) // entity_id UUID
		filterCurrency := cleanFilter(req.Currency)
		filterAsOnDate := cleanFilter(req.AsOnDate) // snapshot date for balances

		// also accept as_on_date from query params
		if filterAsOnDate == "" {
			filterAsOnDate = cleanFilter(r.URL.Query().Get("as_on_date"))
		}

		ctx := r.Context()

		// ── Determine date window ──
		// Always backward-looking: end = today (or to_date), start = end - horizon (or from_date).
		// The balance as_of_date filter ALWAYS uses this window — a Sep 2025 balance will NOT
		// appear when the user selects "Last 90 Days" (Jan–Apr window), for example.
		var start, end time.Time
		if fd := strings.TrimSpace(req.FromDate); fd != "" {
			if t, err := time.Parse(constants.DateFormat, fd); err == nil {
				start = t
			}
		}
		if td := strings.TrimSpace(req.ToDate); td != "" {
			if t, err := time.Parse(constants.DateFormat, td); err == nil {
				end = t
			}
		}
		if start.IsZero() || end.IsZero() {
			// Backward-looking window from horizon: end = today, start = today - (horizon-1).
			end = time.Now().UTC().Truncate(24 * time.Hour)
			start = end.AddDate(0, 0, -(horizon - 1))
		}
		startStr := start.Format(constants.DateFormat)
		endStr := end.Format(constants.DateFormat)

		// Balance date filter always applied — both horizon and custom ranges restrict as_of_date.
		balDateStart := startStr
		balDateEnd := endStr

		// ── 1) Fetch balance per account ──
		//   • as_on_date provided → last transaction's running balance on/before that date (from statement transactions)
		//   • otherwise          → latest APPROVED balance from bank_balances_manual within the date window
		var q string
		var qArgs []interface{}
		if filterAsOnDate != "" {
			// Transaction-derived balance: pick the last tx.balance (running balance) per account
			// where the transaction date is on or before as_on_date.
			q = `
			WITH tx_balance AS (
				SELECT DISTINCT ON (mba.account_number)
					mba.account_number                           AS account_no,
					COALESCE(tx.balance, 0)                      AS closing_balance,
					COALESCE(mba.currency, 'INR')                AS currency_code,
					COALESCE(mba.nickname, '')                   AS nickname,
					COALESCE(mba.country, '')                    AS country
				FROM public.masterbankaccount mba
				JOIN cimplrcorpsaas.bank_statements bs
					ON bs.account_number = mba.account_number
				JOIN (
					SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status
					FROM cimplrcorpsaas.auditactionbankstatement
					ORDER BY bankstatementid, requested_at DESC
				) aab ON aab.bankstatementid = bs.bank_statement_id AND aab.processing_status = 'APPROVED'
				JOIN cimplrcorpsaas.bank_statement_transactions tx
					ON tx.bank_statement_id = bs.bank_statement_id
				WHERE mba.is_deleted = false
				  AND COALESCE(mba.status, 'Active') = 'Active'
				  AND COALESCE(tx.transaction_date, tx.value_date) <= $1::date
				  AND ($2 = '' OR LOWER(TRIM(mba.bank_name))              = LOWER(TRIM($2)))
				  AND ($3 = '' OR mba.account_number                      = $3)
				  AND ($4 = '' OR mba.entity_id::text                     = $4)
				  AND ($5 = '' OR UPPER(TRIM(COALESCE(mba.currency, ''))) = UPPER(TRIM($5)))
				ORDER BY mba.account_number, COALESCE(tx.transaction_date, tx.value_date) DESC, tx.transaction_id DESC
			)
			SELECT
				lab.account_no,
				lab.nickname,
				lab.currency_code,
				lab.closing_balance::float8,
				COALESCE(mba.bank_name,  '')  AS bank_name,
				COALESCE(me.entity_name, '')  AS entity_name,
				lab.country
			FROM tx_balance lab
			JOIN masterbankaccount  mba ON mba.account_number = lab.account_no
			LEFT JOIN masterentitycash me  ON me.entity_id::text = mba.entity_id
			ORDER BY lab.account_no
			`
			qArgs = []interface{}{filterAsOnDate, filterBank, filterAccount, filterEntity, filterCurrency}
		} else {
			// Manual approved balance snapshot within the date window
			q = `
			WITH latest_approved_balance AS (
				SELECT DISTINCT ON (bbm.account_no)
					bbm.account_no,
					COALESCE(bbm.closing_balance, 0)       AS closing_balance,
					COALESCE(bbm.currency_code, 'INR')     AS currency_code,
					COALESCE(bbm.nickname, '')              AS nickname,
					COALESCE(bbm.country, mba.country, '') AS country
				FROM public.bank_balances_manual bbm
				JOIN public.auditactionbankbalances  a   ON a.balance_id   = bbm.balance_id
				JOIN masterbankaccount               mba ON mba.account_number = bbm.account_no
				WHERE a.processing_status = 'APPROVED'
				  AND mba.is_deleted = false
				  AND COALESCE(mba.status, 'Active') = 'Active'
				  AND ($1 = '' OR LOWER(TRIM(mba.bank_name))                              = LOWER(TRIM($1)))
				  AND ($2 = '' OR mba.account_number                                      = $2)
				  AND ($3 = '' OR mba.entity_id::text                                     = $3)
				  AND ($4 = '' OR UPPER(TRIM(COALESCE(bbm.currency_code, '')))            = UPPER(TRIM($4)))
				  AND ($5 = '' OR bbm.as_of_date >= $5::date)
				  AND ($6 = '' OR bbm.as_of_date <= $6::date)
				ORDER BY bbm.account_no, bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
			)
			SELECT
				lab.account_no,
				lab.nickname,
				lab.currency_code,
				lab.closing_balance::float8,
				COALESCE(mba.bank_name,  '')   AS bank_name,
				COALESCE(me.entity_name, '')   AS entity_name,
				lab.country
			FROM latest_approved_balance lab
			JOIN masterbankaccount  mba ON mba.account_number = lab.account_no
			LEFT JOIN masterentitycash me  ON me.entity_id::text = mba.entity_id
			ORDER BY lab.account_no
			`
			qArgs = []interface{}{filterBank, filterAccount, filterEntity, filterCurrency, balDateStart, balDateEnd}
		}

		rows, err := pgxPool.Query(ctx, q, qArgs...)

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
		var balRows []balRow
		for rows.Next() {
			var a, n, cur, bank, ent, country string
			var bal float64
			if err := rows.Scan(&a, &n, &cur, &bal, &bank, &ent, &country); err != nil {
				continue
			}
			balRows = append(balRows, balRow{Account: a, Nickname: n, Currency: cur, Balance: bal, Bank: bank, Entity: ent, Country: country})
		}

		// ── Aggregate totals ──
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

		rd2 := func(v float64) float64 { return math.Round(v*100) / 100 }
		totalINR = rd2(totalINR)
		for k, v := range currencySums {
			currencySums[k] = rd2(v)
		}
		for k, v := range bankSums {
			bankSums[k] = rd2(v)
		}
		for k, v := range entitySums {
			entitySums[k] = rd2(v)
		}
		for k, v := range accountSums {
			accountSums[k] = rd2(v)
		}
		for k, v := range countrySums {
			countrySums[k] = rd2(v)
		}

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

		// ── 2) Transaction date window (already computed above) ──

		// ── Shared transaction filter snippet ──
		// $1=startStr, $2=endStr, $3=filterBank, $4=filterAccount, $5=filterEntity, $6=filterCurrency
		txnFilter := func(sumCol string) string {
			return `SELECT COALESCE(SUM(COALESCE(` + sumCol + `,0)),0)::float8,
	COALESCE(NULLIF(m.currency,''),'INR') AS currency_code
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
JOIN cimplrcorpsaas.auditactionbankstatement a
     ON a.bankstatementid = bs.bank_statement_id AND a.processing_status = 'APPROVED'
LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
WHERE t.value_date BETWEEN $1 AND $2
  AND ($3 = '' OR LOWER(TRIM(m.bank_name))   = LOWER(TRIM($3)))
  AND ($4 = '' OR m.account_number            = $4)
  AND ($5 = '' OR m.entity_id::text           = $5)
  AND ($6 = '' OR UPPER(TRIM(COALESCE(m.currency,''))) = UPPER(TRIM($6)))
GROUP BY COALESCE(NULLIF(m.currency,''),'INR')`
		}

		// ── 3) Projection KPIs ──
		monthStartKey := start.Year()*12 + int(start.Month())
		monthEndKey := end.Year()*12 + int(end.Month())
		logger.LogInfo("[DEBUG] projection month keys start=%d end=%d", monthStartKey, monthEndKey)

		var rawCount int
		if err := pgxPool.QueryRow(ctx,
			`SELECT COUNT(*) FROM cimplrcorpsaas.cashflow_projection_monthly cpm WHERE (cpm.year*12+cpm.month) BETWEEN $1 AND $2`,
			monthStartKey, monthEndKey,
		).Scan(&rawCount); err != nil {
			logger.LogError("[DEBUG] diag raw count error: %v", err)
		} else {
			logger.LogInfo("[DEBUG] projection raw rows: %d", rawCount)
		}

		projAggQ := `SELECT
	COALESCE(SUM(CASE WHEN cpi.cashflow_type='Inflow'  THEN cpm.projected_amount ELSE 0 END),0)::float8,
	COALESCE(SUM(CASE WHEN cpi.cashflow_type='Outflow' THEN cpm.projected_amount ELSE 0 END),0)::float8,
	COALESCE(NULLIF(cpi.currency_code,''), COALESCE(cp.base_currency_code,'INR')) AS currency_code
FROM cimplrcorpsaas.cashflow_projection_monthly cpm
JOIN cimplrcorpsaas.cashflow_proposal_item cpi ON cpm.item_id   = cpi.item_id
JOIN cimplrcorpsaas.cashflow_proposal      cp  ON cpi.proposal_id = cp.proposal_id
LEFT JOIN masterentitycash me ON LOWER(TRIM(me.entity_name)) = LOWER(TRIM(COALESCE(cpi.entity_name,'')))
LEFT JOIN LATERAL (
	SELECT processing_status FROM cimplrcorpsaas.audit_action_cashflow_proposal a
	WHERE a.proposal_id = cp.proposal_id ORDER BY a.requested_at DESC LIMIT 1
) aa ON TRUE
WHERE (cpm.year*12+cpm.month) BETWEEN $1 AND $2
  AND aa.processing_status = 'APPROVED'
  AND ($3 = '' OR me.entity_id::text = $3)
  AND ($4 = '' OR UPPER(TRIM(COALESCE(cp.base_currency_code,''))) = UPPER(TRIM($4)))
GROUP BY COALESCE(NULLIF(cpi.currency_code,''), COALESCE(cp.base_currency_code,'INR'))`

		var projectedInflowsINR, projectedOutflowsINR float64
		if aggRows, err := pgxPool.Query(ctx, projAggQ, monthStartKey, monthEndKey, filterEntity, filterCurrency); err != nil {
			logger.LogError("[DEBUG] projection agg error: %v", err)
		} else {
			for aggRows.Next() {
				var si, so float64
				var cur string
				if err := aggRows.Scan(&si, &so, &cur); err == nil {
					projectedInflowsINR += toINR(si, cur)
					projectedOutflowsINR += toINR(so, cur)
				}
			}
			aggRows.Close()
		}

		// Daily distribution of projections (for weekwise report)
		projDetailQ := `SELECT cpm.year, cpm.month, cpm.projected_amount, cpi.cashflow_type,
	COALESCE(NULLIF(cpi.currency_code,''), COALESCE(cp.base_currency_code,'INR')) AS currency_code
FROM cimplrcorpsaas.cashflow_projection_monthly cpm
JOIN cimplrcorpsaas.cashflow_proposal_item cpi ON cpm.item_id   = cpi.item_id
JOIN cimplrcorpsaas.cashflow_proposal      cp  ON cpi.proposal_id = cp.proposal_id
LEFT JOIN masterentitycash me ON LOWER(TRIM(me.entity_name)) = LOWER(TRIM(COALESCE(cpi.entity_name,'')))
LEFT JOIN LATERAL (
	SELECT processing_status FROM cimplrcorpsaas.audit_action_cashflow_proposal a
	WHERE a.proposal_id = cp.proposal_id ORDER BY a.requested_at DESC LIMIT 1
) aa ON TRUE
WHERE (cpm.year*12+cpm.month) BETWEEN $1 AND $2
  AND aa.processing_status = 'APPROVED'
  AND ($3 = '' OR me.entity_id::text = $3)
  AND ($4 = '' OR UPPER(TRIM(COALESCE(cp.base_currency_code,''))) = UPPER(TRIM($4)))`

		projectedDailyInflow := map[string]float64{}
		projectedDailyOutflow := map[string]float64{}
		if projRows, err := pgxPool.Query(ctx, projDetailQ, monthStartKey, monthEndKey, filterEntity, filterCurrency); err != nil {
			logger.LogError("[DEBUG] projection detail error: %v", err)
		} else {
			for projRows.Next() {
				var yr, mon int
				var amt float64
				var ctype, cur string
				if err := projRows.Scan(&yr, &mon, &amt, &ctype, &cur); err != nil {
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

		projectedInflowsINR = rd2(projectedInflowsINR)
		projectedOutflowsINR = rd2(projectedOutflowsINR)
		projectedClosing := rd2(totalINR + (projectedInflowsINR - projectedOutflowsINR))
		liquidityGap := rd2(math.Abs(projectedClosing - totalINR))

		forecastKPIs := []map[string]interface{}{
			{"title": "Current Balance", "value": totalINR},
			{"title": "Projected Inflows", "value": projectedInflowsINR},
			{"title": "Projected Outflows", "value": projectedOutflowsINR},
			{"title": "Projected Closing", "value": projectedClosing},
			{"title": "Liquidity Gap", "value": liquidityGap},
		}

		// ── 4) Pie datasets ──
		type PieDatum struct {
			ID, Label, Color string
			Value            float64
		}
		currencyPie := make([]PieDatum, 0, len(currencySums))
		for cur, val := range currencySums {
			currencyPie = append(currencyPie, PieDatum{ID: cur, Label: cur, Color: "#0f766e", Value: val})
		}
		bankPie := make([]PieDatum, 0, len(bankSums))
		for b, val := range bankSums {
			bankPie = append(bankPie, PieDatum{ID: b, Label: b, Color: "#16a34a", Value: val})
		}
		entityPie := make([]PieDatum, 0, len(entitySums))
		for e, val := range entitySums {
			entityPie = append(entityPie, PieDatum{ID: e, Label: e, Color: "#60a5fa", Value: val})
		}

		// ── 5) Statement rows ──
		type StatementRow struct {
			Title    string  `json:"title"`
			Inflow   float64 `json:"inflow"`
			Bank     string  `json:"bank"`
			Nickname string  `json:"nickname"`
		}
		countryStatement := make([]StatementRow, 0, len(countrySums))
		for k, v := range countrySums {
			if k == "" {
				k = "Unknown"
			}
			countryStatement = append(countryStatement, StatementRow{Title: k, Inflow: v})
		}
		accountToBank := map[string]string{}
		accountToNickname := map[string]string{}
		for _, br := range balRows {
			if br.Account != "" {
				accountToBank[br.Account] = br.Bank
				accountToNickname[br.Account] = br.Nickname
			}
		}
		accountStatement := make([]StatementRow, 0, len(accountSums))
		for a, v := range accountSums {
			accountStatement = append(accountStatement, StatementRow{Title: a, Inflow: v, Bank: accountToBank[a], Nickname: accountToNickname[a]})
		}
		currencyStatement := make([]StatementRow, 0, len(currencySums))
		for c, v := range currencySums {
			currencyStatement = append(currencyStatement, StatementRow{Title: c, Inflow: v})
		}

		// ── 6) Weekwise forecast vs actual ──
		totalDays := int(end.Sub(start).Hours()/24) + 1
		numWeeks := (totalDays + 6) / 7
		if numWeeks <= 0 {
			numWeeks = 10
		}
		if numWeeks > 52 {
			numWeeks = 52
		}

		weeks := make([]map[string]interface{}, 0, numWeeks)
		for i := 0; i < numWeeks; i++ {
			ws := start.AddDate(0, 0, i*7)
			we := ws.AddDate(0, 0, 6)
			if we.After(end) {
				we = end
			}
			wsStr := ws.Format(constants.DateFormat)
			weStr := we.Format(constants.DateFormat)

			filters := txnRangeFilters{Bank: filterBank, Account: filterAccount, Entity: filterEntity, Currency: filterCurrency}
			aIn := sumTxnRange(ctx, pgxPool, txnFilter("t.deposit_amount"), wsStr, weStr, filters)
			aOut := sumTxnRange(ctx, pgxPool, txnFilter("t.withdrawal_amount"), wsStr, weStr, filters)

			var fIn, fOut float64
			for d := ws; !d.After(we); d = d.AddDate(0, 0, 1) {
				key := d.Format(constants.DateFormat)
				fIn += projectedDailyInflow[key]
				fOut += projectedDailyOutflow[key]
			}

			weeks = append(weeks, map[string]interface{}{
				"week":             ws.Format("Jan 2") + " - " + we.Format("Jan 2"),
				"forecast_inflow":  rd2(fIn),
				"actual_inflow":    rd2(aIn),
				"inflow_variance":  rd2(fIn - aIn),
				"forecast_outflow": rd2(fOut),
				"actual_outflow":   rd2(aOut),
				"outflow_variance": rd2(fOut - aOut),
				"net_variance":     rd2((fIn - fOut) - (aIn - aOut)),
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

type txnRangeFilters struct {
	Bank     string
	Account  string
	Entity   string
	Currency string
}

// sumTxnRange executes a transaction aggregation query for the given period and filters,
// returns the total in INR.
func sumTxnRange(ctx context.Context, pgxPool *pgxpool.Pool, q, startStr, endStr string, f txnRangeFilters) float64 {
	rs, err := pgxPool.Query(ctx, q, startStr, endStr, f.Bank, f.Account, f.Entity, f.Currency)
	if err != nil {
		return 0
	}
	defer rs.Close()
	var total float64
	for rs.Next() {
		var amt float64
		var cur string
		if err := rs.Scan(&amt, &cur); err == nil {
			total += toINR(amt, cur)
		}
	}
	return math.Round(total*100) / 100
}
