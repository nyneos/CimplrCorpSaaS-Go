package cashflowforecast

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Rates used to normalize amounts to USD equivalent
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

type ForecastKPIs struct {
	StartingBalance         float64 `json:"starting_balance"`
	TotalInflows            float64 `json:"total_inflows"`
	TotalOutflows           float64 `json:"total_outflows"`
	ProjectedNetFlow        float64 `json:"projected_net_flow"`
	ProjectedClosingBalance float64 `json:"projected_closing_balance"`
}

type ForecastRow struct {
	Date          string  `json:"date"` // YYYY-MM-01 (month bucket)
	Type          string  `json:"type"`
	Category      string  `json:"category"`
	Description   string  `json:"description"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	NormalizedUSD float64 `json:"normalized_usd"`
}

type DailyRow struct {
	Date       string  `json:"date"`
	OpeningUSD float64 `json:"opening"`
	NetFlowUSD float64 `json:"net_flow"`
	ClosingUSD float64 `json:"closing"`
}

// Handler: POST /cash/forecast/monthly
// Body: {constants.KeyUserID:"<id>", "horizon":90, "entity_name": "..." (optional), "currency":"USD" (optional) }
func GetCashflowForecastHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID     string `json:"user_id"`
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 90
		}

		kpis, err := GetForecastKPIs(pgxPool, req.Horizon, req.EntityName, req.Currency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rows, err := GetForecastRows(pgxPool, req.Horizon, req.EntityName, req.Currency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{
			constants.ValueSuccess: true,
			"kpis":                 kpis,
			"rows":                 rows,
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// getStartingBalanceUSD returns the normalized USD starting balance as of the given date (inclusive)
func getStartingBalanceUSD(pgxPool *pgxpool.Pool, asOf time.Time) (float64, error) {
	// Use balance_amount (latest entry per account up to asOf date) as requested
	startDate := asOf.Format(constants.DateFormat)
	openingQ := `SELECT DISTINCT ON (COALESCE(account_no, iban, nickname)) balance_amount, currency_code
        FROM bank_balances_manual
        WHERE as_of_date <= $1
        ORDER BY COALESCE(account_no, iban, nickname), as_of_date DESC, as_of_time DESC`
	openingRows, err := pgxPool.Query(context.Background(), openingQ, startDate)
	if err != nil {
		return 0, fmt.Errorf("opening balances query: %w", err)
	}
	defer openingRows.Close()
	var startingUSD float64
	for openingRows.Next() {
		var amt float64
		var cur string
		if err := openingRows.Scan(&amt, &cur); err != nil {
			return 0, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		startingUSD += amt * rate
	}
	return startingUSD, nil
}

// GetForecastKPIs computes the KPI summary for the given horizon and optional filters
func GetForecastKPIs(pgxPool *pgxpool.Pool, horizon int, entityName, currency string) (ForecastKPIs, error) {
	var k ForecastKPIs
	today := time.Now().UTC()
	start := today
	end := today.AddDate(0, 0, horizon-1)

	sb, err := getStartingBalanceUSD(pgxPool, start)
	if err != nil {
		return k, err
	}
	k.StartingBalance = sb

	// Build month key range
	startKey := start.Year()*12 + int(start.Month())
	endKey := end.Year()*12 + int(end.Month())

	aggQ := `
		SELECT cpi.cashflow_type, cp.currency_code, COALESCE(SUM(cpm.projected_amount),0)::float8 AS amount
		FROM cashflow_projection_monthly cpm
		JOIN cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id
		JOIN cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
		LEFT JOIN LATERAL (
			SELECT processing_status FROM audit_action_cashflow_proposal a2 WHERE a2.proposal_id = cp.proposal_id ORDER BY requested_at DESC LIMIT 1
		) aa ON TRUE
		WHERE (cpm.year * 12 + cpm.month) BETWEEN $1 AND $2
		  AND aa.processing_status = 'APPROVED'
	`
	// append GROUP BY after optional filters to ensure WHERE clause is complete
	aggGroup := ` GROUP BY cpi.cashflow_type, cp.currency_code`
	args := []interface{}{startKey, endKey}
	if entityName != "" {
		ph := len(args) + 1
		aggQ += fmt.Sprintf(constants.QuerryEntityName, ph)
		args = append(args, entityName)
	}
	if currency != "" {
		ph := len(args) + 1
		aggQ += fmt.Sprintf(constants.QuerryCurrencyCode, ph)
		args = append(args, currency)
	}

	// finalize query
	finalAggQ := aggQ + aggGroup
	rows, err := pgxPool.Query(context.Background(), finalAggQ, args...)
	if err != nil {
		return k, fmt.Errorf("aggregate projections: %v | query: %s | args: %v", err, finalAggQ, args)
	}
	defer rows.Close()

	var inflowsUSD, outflowsUSD float64
	for rows.Next() {
		var ctype, cur string
		var amt float64
		if err := rows.Scan(&ctype, &cur, &amt); err != nil {
			return k, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		n := amt * rate
		if ctype == "Inflow" {
			inflowsUSD += n
		} else if ctype == "Outflow" {
			outflowsUSD += n
		}
	}

	k.TotalInflows = inflowsUSD
	k.TotalOutflows = outflowsUSD
	k.ProjectedNetFlow = inflowsUSD - outflowsUSD
	k.ProjectedClosingBalance = k.StartingBalance + k.ProjectedNetFlow
	return k, nil
}

// GetForecastRows returns detailed monthly projection rows (one row per year-month/type/category/description)
func GetForecastRows(pgxPool *pgxpool.Pool, horizon int, entityName, currency string) ([]ForecastRow, error) {
	var rowsOut []ForecastRow
	today := time.Now().UTC()
	start := today
	end := today.AddDate(0, 0, horizon-1)
	startKey := start.Year()*12 + int(start.Month())
	endKey := end.Year()*12 + int(end.Month())

	fetchQ := `
        SELECT cpm.year, cpm.month, cpi.cashflow_type, cpi.category_id, cpi.description, COALESCE(SUM(cpm.projected_amount),0)::float8 AS amount, cp.currency_code
        FROM cashflow_projection_monthly cpm
        JOIN cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id
        JOIN cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
        LEFT JOIN LATERAL (
            SELECT processing_status FROM audit_action_cashflow_proposal a2 WHERE a2.proposal_id = cp.proposal_id ORDER BY requested_at DESC LIMIT 1
        ) aa ON TRUE
        WHERE (cpm.year * 12 + cpm.month) BETWEEN $1 AND $2
          AND aa.processing_status = 'APPROVED'
    `
	args := []interface{}{startKey, endKey}
	if entityName != "" {
		ph := len(args) + 1
		fetchQ += fmt.Sprintf(constants.QuerryEntityName, ph)
		args = append(args, entityName)
	}
	if currency != "" {
		ph := len(args) + 1
		fetchQ += fmt.Sprintf(constants.QuerryCurrencyCode, ph)
		args = append(args, currency)
	}
	fetchGroup := ` GROUP BY cpm.year, cpm.month, cpi.cashflow_type, cpi.category_id, cpi.description, cp.currency_code ORDER BY cpm.year, cpm.month;`

	finalFetchQ := fetchQ + fetchGroup

	prow, err := pgxPool.Query(context.Background(), finalFetchQ, args...)
	if err != nil {
		return rowsOut, fmt.Errorf("fetch projections: %v | query: %s | args: %v", err, finalFetchQ, args)
	}
	defer prow.Close()

	for prow.Next() {
		var year int
		var month int
		var ctype, category, desc, cur string
		var amt float64
		if err := prow.Scan(&year, &month, &ctype, &category, &desc, &amt, &cur); err != nil {
			return rowsOut, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		n := amt * rate
		date := fmt.Sprintf("%04d-%02d-01", year, month)
		rowsOut = append(rowsOut, ForecastRow{
			Date:          date,
			Type:          ctype,
			Category:      category,
			Description:   desc,
			Amount:        amt,
			Currency:      cur,
			NormalizedUSD: n,
		})
	}
	return rowsOut, nil
}

// GetForecastDailyRows computes daily opening/net/closing balances (USD-normalized)
func GetForecastDailyRows(pgxPool *pgxpool.Pool, horizon int, entityName, currency string) ([]DailyRow, error) {
	var out []DailyRow
	today := time.Now().UTC()
	end := today.AddDate(0, 0, horizon-1)

	// 1) find most recent balance date <= today
	var baseDate time.Time
	err := pgxPool.QueryRow(context.Background(), `SELECT MAX(as_of_date) FROM bank_balances_manual WHERE as_of_date <= $1`, today.Format(constants.DateFormat)).Scan(&baseDate)
	if err != nil {
		return out, fmt.Errorf("find base date: %w", err)
	}
	if baseDate.IsZero() {
		return out, fmt.Errorf("no historical balances found")
	}

	// 2) compute aggregated closing (USD) as of baseDate using latest per-account <= baseDate
	openingQ := `SELECT DISTINCT ON (COALESCE(account_no, iban, nickname)) balance_amount, currency_code
        FROM bank_balances_manual
        WHERE as_of_date <= $1
        ORDER BY COALESCE(account_no, iban, nickname), as_of_date DESC, as_of_time DESC`
	rows, err := pgxPool.Query(context.Background(), openingQ, baseDate.Format(constants.DateFormat))
	if err != nil {
		return out, fmt.Errorf("opening balances query: %w", err)
	}
	defer rows.Close()
	var baseClosingUSD float64
	for rows.Next() {
		var amt float64
		var cur string
		if err := rows.Scan(&amt, &cur); err != nil {
			return out, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		baseClosingUSD += amt * rate
	}

	// 3) fetch monthly projections (approved) between base month and end month and distribute evenly across days in month
	startKey := baseDate.Year()*12 + int(baseDate.Month())
	endKey := end.Year()*12 + int(end.Month())

	monthlyQ := `
        SELECT cpm.year, cpm.month, cpi.cashflow_type, cp.currency_code, cpi.start_date, COALESCE(SUM(cpm.projected_amount),0)::float8 AS amount
        FROM cashflow_projection_monthly cpm
        JOIN cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id
        JOIN cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
        LEFT JOIN LATERAL (
            SELECT processing_status FROM audit_action_cashflow_proposal a2 WHERE a2.proposal_id = cp.proposal_id ORDER BY requested_at DESC LIMIT 1
        ) aa ON TRUE
        WHERE aa.processing_status = 'APPROVED'
          AND (cpm.year * 12 + cpm.month) BETWEEN $1 AND $2
    `
	margs := []interface{}{startKey, endKey}
	if entityName != "" {
		ph := len(margs) + 1
		monthlyQ += fmt.Sprintf(constants.QuerryEntityName, ph)
		margs = append(margs, entityName)
	}
	if currency != "" {
		ph := len(margs) + 1
		monthlyQ += fmt.Sprintf(constants.QuerryCurrencyCode, ph)
		margs = append(margs, currency)
	}
	monthlyGroup := ` GROUP BY cpm.year, cpm.month, cpi.cashflow_type, cp.currency_code, cpi.start_date ORDER BY cpm.year, cpm.month;`

	finalMonthlyQ := monthlyQ + monthlyGroup

	mrows, err := pgxPool.Query(context.Background(), finalMonthlyQ, margs...)
	if err != nil {
		return out, fmt.Errorf("fetch monthly projections: %v | query: %s | args: %v", err, finalMonthlyQ, margs)
	}
	defer mrows.Close()

	inflows := map[string]float64{}
	outflows := map[string]float64{}
	for mrows.Next() {
		var year int
		var month int
		var ctype, cur string
		var startDate *time.Time
		var amt float64
		if err := mrows.Scan(&year, &month, &ctype, &cur, &startDate, &amt); err != nil {
			return out, err
		}
		// If the item has a start_date, use its day-of-month as the effective date for that projection (e.g., 16 => 16th of each month)
		// Otherwise fall back to distributing evenly across the month (legacy behaviour).
		firstOfMonth := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		daysInMonth := time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()

		if startDate != nil {
			effDay := startDate.Day()
			if effDay > daysInMonth {
				effDay = daysInMonth
			}
			target := time.Date(year, time.Month(month), effDay, 0, 0, 0, 0, time.UTC)
			// only include if inside our simulation window (baseDate+1 .. end)
			if !target.Before(baseDate.AddDate(0, 0, 1)) && !target.After(end) {
				key := target.Format(constants.DateFormat)
				rate := rates[cur]
				if rate == 0 {
					rate = 1.0
				}
				if ctype == "Inflow" {
					inflows[key] += amt * rate
				} else if ctype == "Outflow" {
					outflows[key] += amt * rate
				}
			}
		} else {
			// legacy pro-rata per calendar day
			daily := amt / float64(daysInMonth)
			for d := firstOfMonth; d.Month() == time.Month(month); d = d.AddDate(0, 0, 1) {
				if d.Before(baseDate.AddDate(0, 0, 1)) { // skip days before baseDate+1
					continue
				}
				if d.After(end) {
					break
				}
				key := d.Format(constants.DateFormat)
				rate := rates[cur]
				if rate == 0 {
					rate = 1.0
				}
				if ctype == "Inflow" {
					inflows[key] += daily * rate
				} else if ctype == "Outflow" {
					outflows[key] += daily * rate
				}
			}
		}
	}

	// 4) simulate day-by-day from baseDate+1 to end
	dailyRows := []DailyRow{}
	prevClosing := baseClosingUSD
	// If baseDate == today, we will include today's row using baseClosing as authoritative
	// Start simulation date = baseDate + 1
	simDate := baseDate.AddDate(0, 0, 1)

	// But we might need the row for baseDate (today) if baseDate == today; we'll handle later
	for d := simDate; !d.After(end); d = d.AddDate(0, 0, 1) {
		key := d.Format(constants.DateFormat)
		infl := inflows[key]
		outf := outflows[key]
		net := infl - outf
		opening := prevClosing
		closing := opening + net
		dailyRows = append(dailyRows, DailyRow{Date: key, OpeningUSD: opening, NetFlowUSD: net, ClosingUSD: closing})
		prevClosing = closing
	}

	// Build final output starting from today's row
	todayKey := today.Format(constants.DateFormat)
	// If baseDate == today, create today's row using baseClosing as closing and compute opening = closing - net(today)
	if baseDate.Format(constants.DateFormat) == todayKey {
		netToday := inflows[todayKey] - outflows[todayKey]
		openingToday := baseClosingUSD - netToday
		row := DailyRow{Date: todayKey, OpeningUSD: openingToday, NetFlowUSD: netToday, ClosingUSD: baseClosingUSD}
		out = append(out, row)
	} else {
		// baseDate < today: find the row in rows corresponding to todayKey
		found := false
		for _, r := range dailyRows {
			if r.Date == todayKey {
				out = append(out, r)
				found = true
				break
			}
		}
		if !found {
			// no entries for today in range (e.g., baseDate < today but no projections), compute today's opening as prevClosing at end of sim up to today
			// Re-simulate up to today
			prev := baseClosingUSD
			for d := baseDate.AddDate(0, 0, 1); !d.After(today); d = d.AddDate(0, 0, 1) {
				k := d.Format(constants.DateFormat)
				net := inflows[k] - outflows[k]
				opening := prev
				closing := opening + net
				if k == todayKey {
					out = append(out, DailyRow{Date: k, OpeningUSD: opening, NetFlowUSD: net, ClosingUSD: closing})
					break
				}
				prev = closing
			}
		}
	}

	// append subsequent rows (tomorrow .. end)
	for _, r := range dailyRows {
		// include only dates >= today
		if r.Date >= todayKey {
			out = append(out, r)
		}
	}

	return out, nil
}

// POST /dash/cash/forecast/daily
func GetForecastDailyHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID     string `json:"user_id"`
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 30
		}
		rows, err := GetForecastDailyRows(pgxPool, req.Horizon, req.EntityName, req.Currency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{constants.ValueSuccess: true, "rows": rows}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// HTTP handler wrappers
// POST /dash/cash/forecast/kpi
func GetForecastKPIsHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID     string `json:"user_id"`
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 90
		}
		kpis, err := GetForecastKPIs(pgxPool, req.Horizon, req.EntityName, req.Currency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{constants.ValueSuccess: true, "kpis": kpis}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// POST /dash/cash/forecast/rows
func GetForecastRowsHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID     string `json:"user_id"`
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 90
		}
		rows, err := GetForecastRows(pgxPool, req.Horizon, req.EntityName, req.Currency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{constants.ValueSuccess: true, "rows": rows}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}

// GetForecastCategorySums returns sum per unique category with cashflow type appended, normalized to USD
func GetForecastCategorySums(pgxPool *pgxpool.Pool, horizon int, entityName, currency string) (map[string]float64, error) {
	// We're summing per-item expected_amount (not monthly projections) and normalizing to USD.
	baseQ := `
		SELECT cpi.category_id, cpi.cashflow_type, COALESCE(cp.currency_code, 'USD') AS currency_code, COALESCE(SUM(cpi.expected_amount),0)::float8 AS amount
		FROM cashflow_proposal_item cpi
		JOIN cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
		LEFT JOIN LATERAL (
			SELECT processing_status FROM audit_action_cashflow_proposal a2 WHERE a2.proposal_id = cp.proposal_id ORDER BY requested_at DESC LIMIT 1
		) aa ON TRUE
		WHERE aa.processing_status = 'APPROVED'
	`

	// Build WHERE clause params
	whereClauses := ""
	args := []interface{}{}
	if entityName != "" {
		args = append(args, entityName)
		whereClauses += fmt.Sprintf(constants.QuerryEntityName, len(args))
	}
	if currency != "" {
		args = append(args, currency)
		whereClauses += fmt.Sprintf(constants.QuerryCurrencyCode, len(args))
	}

	q := baseQ + whereClauses + " GROUP BY cpi.category_id, cpi.cashflow_type, cp.currency_code"

	rows, err := pgxPool.Query(context.Background(), q, args...)
	if err != nil {
		return nil, fmt.Errorf("fetch category sums: %w", err)
	}
	defer rows.Close()

	out := map[string]float64{}
	for rows.Next() {
		var cat, ctype, cur string
		var amt float64
		if err := rows.Scan(&cat, &ctype, &cur, &amt); err != nil {
			return nil, err
		}
		rate := rates[cur]
		if rate == 0 {
			rate = 1.0
		}
		key := fmt.Sprintf("%s-(%s)", cat, ctype)
		out[key] += amt * rate
	}
	return out, nil
}

// POST /dash/cash/forecast/categories
func GetForecastCategorySumsHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	type reqBody struct {
		UserID     string `json:"user_id"`
		Horizon    int    `json:"horizon"`
		EntityName string `json:"entity_name,omitempty"`
		Currency   string `json:"currency,omitempty"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var req reqBody
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if req.Horizon <= 0 {
			req.Horizon = 90
		}
		sums, err := GetForecastCategorySums(pgxPool, req.Horizon, req.EntityName, req.Currency)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := map[string]interface{}{constants.ValueSuccess: true, "category_sums": sums}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	}
}
