package landingpage

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api"
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

func normalizedLower(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if s := strings.ToLower(strings.TrimSpace(value)); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func normalizedUpper(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if s := strings.ToUpper(strings.TrimSpace(value)); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func approvedAccountNumbers(ctx context.Context) []string {
	accounts, _ := ctx.Value(api.ApprovedBankAccountsKey).([]map[string]string)
	out := make([]string, 0, len(accounts))
	for _, account := range accounts {
		if s := strings.TrimSpace(account["account_number"]); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func containsString(values []string, needle string) bool {
	needle = strings.TrimSpace(needle)
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), needle) {
			return true
		}
	}
	return false
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
				// Fiscal year starts April 1 — from April 1 of current FY to today
				now := time.Now().UTC()
				fyStart := time.Date(now.Year(), time.April, 1, 0, 0, 0, 0, time.UTC)
				if now.Before(fyStart) {
					fyStart = time.Date(now.Year()-1, time.April, 1, 0, 0, 0, 0, time.UTC)
				}
				horizon = int(now.Sub(fyStart).Hours()/24) + 1
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

		t0Handler := time.Now()
		ctx := r.Context()
		allowedEntityIDs := api.GetEntityIDsFromCtx(ctx)
		allowedAccounts := approvedAccountNumbers(ctx)
		allowedBanks := normalizedLower(api.GetBankNamesFromCtx(ctx))
		allowedCurrencies := normalizedUpper(api.GetCurrencyCodesFromCtx(ctx))
		logger.LogInfo("[TIMING] cash/middleware→handler: %v (entities=%d accounts=%d)", time.Since(t0Handler), len(allowedEntityIDs), len(allowedAccounts))
		if len(allowedEntityIDs) == 0 || len(allowedAccounts) == 0 || len(allowedBanks) == 0 || len(allowedCurrencies) == 0 {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}
		if filterEntity != "" && !api.IsEntityAllowed(ctx, filterEntity) {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}
		if filterBank != "" && !api.IsBankAllowed(ctx, filterBank) {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}
		if filterAccount != "" && !containsString(allowedAccounts, filterAccount) {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}
		if filterCurrency != "" && !api.IsCurrencyAllowed(ctx, filterCurrency) {
			http.Error(w, constants.ErrNoAccessibleBusinessUnit, http.StatusForbidden)
			return
		}

		// ── Determine date window ──
		// Always backward-looking: end = today (or to_date), start = end - horizon (or from_date).
		var start, end time.Time
		explicitFromDate := false
		if fd := strings.TrimSpace(req.FromDate); fd != "" {
			if t, err := time.Parse(constants.DateFormat, fd); err == nil {
				start = t
				explicitFromDate = true
			}
		}
		if td := strings.TrimSpace(req.ToDate); td != "" {
			if t, err := time.Parse(constants.DateFormat, td); err == nil {
				end = t
			}
		}
		if start.IsZero() || end.IsZero() {
			end = time.Now().UTC().Truncate(24 * time.Hour)
			start = end.AddDate(0, 0, -(horizon - 1))
		}
		startStr := start.Format(constants.DateFormat)
		endStr := end.Format(constants.DateFormat)

		// For balance snapshot (bank_balances_manual): no lower-bound on as_of_date unless the
		// user explicitly provided from_date. The DISTINCT ON picks the latest balance per account
		// automatically — we never want to hide the current balance just because the last statement
		// is older than the horizon window.
		balDateStart := ""
		if explicitFromDate {
			balDateStart = startStr
		}
		balDateEnd := endStr

		// ── 1) Fetch balance per account ──
		//   • as_on_date provided → last transaction's running balance on/before that date (from statement transactions)
		//   • otherwise          → latest APPROVED balance from bank_balances_manual within the date window
		t0Balance := time.Now()
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
					ON bs.account_number = mba.account_number AND bs.is_deleted = false AND bs.current_status = 'APPROVED'
				JOIN cimplrcorpsaas.bank_statement_transactions tx
					ON tx.bank_statement_id = bs.bank_statement_id
				WHERE mba.is_deleted = false
				  AND COALESCE(mba.status, 'Active') = 'Active'
				  AND COALESCE(tx.transaction_date, tx.value_date) <= $1::date
				  AND ($2 = '' OR LOWER(TRIM(mba.bank_name))              = LOWER(TRIM($2)))
				  AND ($3 = '' OR mba.account_number                      = $3)
				  AND ($4 = '' OR mba.entity_id::text                     = $4)
				  AND ($5 = '' OR UPPER(TRIM(COALESCE(mba.currency, ''))) = UPPER(TRIM($5)))
				  AND mba.entity_id::text = ANY($6)
				  AND mba.account_number = ANY($7)
				  AND LOWER(TRIM(COALESCE(mba.bank_name, ''))) = ANY($8)
				  AND UPPER(TRIM(COALESCE(mba.currency, ''))) = ANY($9)
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
			JOIN masterbankaccount  mba ON mba.account_number = lab.account_no AND mba.is_deleted = false
			LEFT JOIN masterentitycash me  ON me.entity_id::text = mba.entity_id
			ORDER BY lab.account_no
			`
			qArgs = []interface{}{filterAsOnDate, filterBank, filterAccount, filterEntity, filterCurrency, allowedEntityIDs, allowedAccounts, allowedBanks, allowedCurrencies}
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
				  AND mba.entity_id::text = ANY($7)
				  AND mba.account_number = ANY($8)
				  AND LOWER(TRIM(COALESCE(mba.bank_name, ''))) = ANY($9)
				  AND UPPER(TRIM(COALESCE(bbm.currency_code, ''))) = ANY($10)
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
			JOIN masterbankaccount  mba ON mba.account_number = lab.account_no AND mba.is_deleted = false
			LEFT JOIN masterentitycash me  ON me.entity_id::text = mba.entity_id
			ORDER BY lab.account_no
			`
			qArgs = []interface{}{filterBank, filterAccount, filterEntity, filterCurrency, balDateStart, balDateEnd, allowedEntityIDs, allowedAccounts, allowedBanks, allowedCurrencies}
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
		logger.LogInfo("[TIMING] cash/1-balance: %v (rows=%d)", time.Since(t0Balance), len(balRows))

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

		// ── 3) Projection KPIs ──
		t0Proj := time.Now()
		monthStartKey := start.Year()*12 + int(start.Month())
		monthEndKey := end.Year()*12 + int(end.Month())
		logger.LogInfo("[DEBUG] projection month keys start=%d end=%d", monthStartKey, monthEndKey)

		var rawCount int
		if err := pgxPool.QueryRow(ctx,
			`SELECT COUNT(*)
			FROM cimplrcorpsaas.cashflow_projection_monthly cpm
			JOIN cimplrcorpsaas.cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id AND COALESCE(cpi.is_deleted, false) = false
			JOIN cimplrcorpsaas.cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
			LEFT JOIN masterentitycash me ON LOWER(TRIM(me.entity_name)) = LOWER(TRIM(COALESCE(cpi.entity_name,'')))
			WHERE (cpm.year*12+cpm.month) BETWEEN $1 AND $2
			  AND me.entity_id::text = ANY($3)
			  AND UPPER(TRIM(COALESCE(cp.base_currency_code, cpi.currency_code, ''))) = ANY($4)`,
			monthStartKey, monthEndKey, allowedEntityIDs, allowedCurrencies,
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
JOIN cimplrcorpsaas.cashflow_proposal_item cpi ON cpm.item_id   = cpi.item_id AND COALESCE(cpi.is_deleted, false) = false
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
  AND me.entity_id::text = ANY($5)
  AND UPPER(TRIM(COALESCE(cp.base_currency_code, cpi.currency_code, ''))) = ANY($6)
GROUP BY COALESCE(NULLIF(cpi.currency_code,''), COALESCE(cp.base_currency_code,'INR'))`

		var projectedInflowsINR, projectedOutflowsINR float64
		if aggRows, err := pgxPool.Query(ctx, projAggQ, monthStartKey, monthEndKey, filterEntity, filterCurrency, allowedEntityIDs, allowedCurrencies); err != nil {
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
JOIN cimplrcorpsaas.cashflow_proposal_item cpi ON cpm.item_id   = cpi.item_id AND COALESCE(cpi.is_deleted, false) = false
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
  AND me.entity_id::text = ANY($5)
  AND UPPER(TRIM(COALESCE(cp.base_currency_code, cpi.currency_code, ''))) = ANY($6)`

		projectedDailyInflow := map[string]float64{}
		projectedDailyOutflow := map[string]float64{}
		if projRows, err := pgxPool.Query(ctx, projDetailQ, monthStartKey, monthEndKey, filterEntity, filterCurrency, allowedEntityIDs, allowedCurrencies); err != nil {
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
		logger.LogInfo("[TIMING] cash/3-projection: %v", time.Since(t0Proj))

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
		// Week slots and actuals follow the selected period exactly:
		//   • YTD       → April 1 of current FY to today
		//   • Last N    → N days back from today
		//   • Custom    → user-supplied from_date / to_date
		// Actuals will be 0 for periods where no statement data has been uploaded yet;
		// users can use Custom Date Range to reach historical data (e.g. Oct 2025 – Feb 2026).
		totalDays := int(end.Sub(start).Hours()/24) + 1
		numWeeks := (totalDays + 6) / 7
		if numWeeks <= 0 {
			numWeeks = 10
		}
		if numWeeks > 52 {
			numWeeks = 52
		}

		// Compute all week boundaries upfront for the label map.
		type weekBounds struct{ ws, we time.Time }
		weekSlots := make([]weekBounds, 0, numWeeks)
		for i := 0; i < numWeeks; i++ {
			ws := start.AddDate(0, 0, i*7)
			we := ws.AddDate(0, 0, 6)
			if we.After(end) {
				we = end
			}
			weekSlots = append(weekSlots, weekBounds{ws, we})
		}

		// One query returns per-day deposit+withdrawal sums; we aggregate into weeks in Go.
		weekActuals := map[string][2]float64{} // key = week-start YYYY-MM-DD → [deposit, withdrawal]
		t0Week := time.Now()
		// Uses current_status on bank_statements (maintained by trigger) — no audit table JOIN.
		// Covering index idx_bst_covering_weekwise makes this an index-only scan at any data volume.
		batchWeekQ := `SELECT
    t.value_date::date AS txn_date,
    COALESCE(SUM(COALESCE(t.deposit_amount, 0)), 0)::float8    AS deposit_sum,
    COALESCE(SUM(COALESCE(t.withdrawal_amount, 0)), 0)::float8 AS withdrawal_sum
FROM cimplrcorpsaas.bank_statement_transactions t
JOIN cimplrcorpsaas.bank_statements bs
     ON t.bank_statement_id = bs.bank_statement_id AND bs.is_deleted = false AND bs.current_status = 'APPROVED'
LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
WHERE t.value_date BETWEEN $1 AND $2
  AND ($3 = '' OR LOWER(TRIM(m.bank_name))                      = LOWER(TRIM($3)))
  AND ($4 = '' OR m.account_number                              = $4)
  AND ($5 = '' OR m.entity_id::text                             = $5)
  AND ($6 = '' OR UPPER(TRIM(COALESCE(m.currency, '')))         = UPPER(TRIM($6)))
  AND m.entity_id::text                                         = ANY($7)
  AND m.account_number                                          = ANY($8)
  AND LOWER(TRIM(COALESCE(m.bank_name, '')))                    = ANY($9)
  AND UPPER(TRIM(COALESCE(m.currency, '')))                     = ANY($10)
GROUP BY t.value_date::date`

		if bwRows, err := pgxPool.Query(ctx, batchWeekQ,
			startStr, endStr,
			filterBank, filterAccount, filterEntity, filterCurrency,
			allowedEntityIDs, allowedAccounts, allowedBanks, allowedCurrencies,
		); err != nil {
			logger.LogError("[TIMING] weekwise batch query error: %v", err)
		} else {
			for bwRows.Next() {
				var txnDate time.Time
				var dep, wdl float64
				if err := bwRows.Scan(&txnDate, &dep, &wdl); err == nil {
					// Assign to the week-start that contains this date.
					for _, wb := range weekSlots {
						if !txnDate.Before(wb.ws) && !txnDate.After(wb.we) {
							key := wb.ws.Format(constants.DateFormat)
							v := weekActuals[key]
							v[0] += dep
							v[1] += wdl
							weekActuals[key] = v
							break
						}
					}
				}
			}
			bwRows.Close()
		}
		logger.LogInfo("[TIMING] weekwise batch query: %v", time.Since(t0Week))

		weeks := make([]map[string]interface{}, 0, numWeeks)
		for _, wb := range weekSlots {
			key := wb.ws.Format(constants.DateFormat)
			aIn := rd2(toINR(weekActuals[key][0], "INR"))
			aOut := rd2(toINR(weekActuals[key][1], "INR"))

			var fIn, fOut float64
			for d := wb.ws; !d.After(wb.we); d = d.AddDate(0, 0, 1) {
				k := d.Format(constants.DateFormat)
				fIn += projectedDailyInflow[k]
				fOut += projectedDailyOutflow[k]
			}

			weeks = append(weeks, map[string]interface{}{
				"week":             wb.ws.Format("Jan 2") + " - " + wb.we.Format("Jan 2"),
				"forecast_inflow":  rd2(fIn),
				"actual_inflow":    aIn,
				"inflow_variance":  rd2(fIn - aIn),
				"forecast_outflow": rd2(fOut),
				"actual_outflow":   aOut,
				"outflow_variance": rd2(fOut - aOut),
				"net_variance":     rd2((fIn - fOut) - (aIn - aOut)),
			})
		}

		logger.LogInfo("[TIMING] cash/total-handler: %v", time.Since(t0Handler))
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

