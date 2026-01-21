package landingpage

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/lib/pq"
)

// HomePageRequest carries optional knobs; entity scope comes from BusinessUnitMiddleware.
type HomePageRequest struct {
	UserID      string `json:"user_id"`
	HorizonDays int    `json:"horizon_days,omitempty"` // for cash-on-hand simulation; default 90
}

// GlobalLiquidity aggregates cash and runway metrics.
type GlobalLiquidity struct {
	ConsolidatedCashBalance float64 `json:"consolidated_cash_balance"`
	AvailableLiquidity      float64 `json:"available_liquidity"`
	NetOperatingCashflow    float64 `json:"net_operating_cashflow"`
	MonthlyBurnRate         float64 `json:"monthly_burn_rate"`
	CashOnHandDays          int     `json:"cash_on_hand_days"`
	WeekCashOnHandHigh      int     `json:"week_cash_on_hand_high"`
	WeekCashOnHandLow       int     `json:"week_cash_on_hand_low"`
}

// PortfolioInvestments summarizes investment KPIs for the landing view.
type PortfolioInvestments struct {
	PortfolioBalance float64 `json:"portfolio_balance"`
	PortfolioYield   float64 `json:"portfolio_yield_pct"`
}

// HomePageDashboardResponse is the combined payload.
type HomePageDashboardResponse struct {
	GlobalLiquidity      GlobalLiquidity        `json:"global_liquidity"`
	PortfolioInvestments PortfolioInvestments   `json:"portfolio_investments"`
	RiskAndLeverage      FXOpsDashboardResponse `json:"risk_and_leverage"`
}

// GetHomePageDashboard aggregates liquidity, investment, and risk/leverage sections.
func GetHomePageDashboard(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req HomePageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidRequestBody)
			return
		}

		ctx := r.Context()
		allowedEntities := api.GetEntityNamesFromCtx(ctx)
		if len(allowedEntities) == 0 {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}

		horizon := req.HorizonDays
		if horizon <= 0 {
			horizon = 900
		}

		// Liquidity block
		gl, err := buildGlobalLiquidity(ctx, db, allowedEntities, horizon)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("liquidity: %v", err))
			return
		}

		// Portfolio block
		pi, err := buildPortfolioInvestments(ctx, db, allowedEntities)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("portfolio: %v", err))
			return
		}

		// Risk & leverage reused from FX Ops dashboard helpers (same package)
		// Risk & leverage: homepage-specific computation (do not alter FX Ops dashboard endpoint).
		fxPayload, err := computeHomeRiskLeverage(ctx, db, allowedEntities)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, fmt.Sprintf("fx-ops: %v", err))
			return
		}

		resp := HomePageDashboardResponse{
			GlobalLiquidity:      gl,
			PortfolioInvestments: pi,
			RiskAndLeverage:      fxPayload,
		}
		api.RespondWithPayload(w, true, "", resp)
	}
}

// computeFXOpsDashboard mirrors GetFXOpsDashboard logic but callable internally with context.
func computeFXOpsDashboard(ctx context.Context, db *sql.DB, req FXOpsDashboardRequest) (FXOpsDashboardResponse, error) {
	days := 0
	if req.TimePeriod > 0 {
		days = req.TimePeriod
	}

	alerts := []Alert{
		getMaturingExposuresAlert(ctx, db, req.Entities, req.Currencies),
		getSettlementFailedAlert(ctx, db, req.Entities),
		getHighUnhedgedAlert(ctx, db, req.Entities, req.Currencies),
	}

	unhedgedCount, unhedgedAmount := getUnhedgedExposures(ctx, db, req.Entities, req.Currencies, days)
	kpis := []KPICard{
		{Title: "Exposures Requiring Attention", Value: unhedgedCount, SubTitle: formatAmount(unhedgedAmount)},
		{Title: "Trades Maturing Today", Value: getTradesMaturingToday(ctx, db, req.Entities, req.Currencies), SubTitle: "Forward bookings"},
	}

	_, overallUnhedged := getUnhedgedExposures(ctx, db, req.Entities, req.Currencies, 0)
	kpis = append(kpis, KPICard{Title: "Overall Unhedged", Value: formatAmount(overallUnhedged), SubTitle: "Spot exposure"})
	kpis = append(kpis, KPICard{Title: "Pending Settlement Today", Value: getPendingSettlementToday(ctx, db, req.Entities), SubTitle: "Requires approval"})

	exposures := getExposureMaturities(ctx, db, req.Entities, req.Currencies)
	settlement := getSettlementPerformance(ctx, db, req.Entities)
	netPositions := getTopCurrencyPositions(ctx, db, req.Entities, 5)
	spotRates := getTopTradeCurrencies()

	return FXOpsDashboardResponse{
		Alerts:            alerts,
		KPIs:              kpis,
		Exposures:         exposures,
		SettlementSummary: settlement,
		NetPositions:      netPositions,
		SpotRates:         spotRates,
		BankLimits:        []BankLimit{},
	}, nil
}

// computeHomeRiskLeverage crafts KPIs closer to the homepage design (hedged ratio, unhedged bars, VaR).
func computeHomeRiskLeverage(ctx context.Context, db *sql.DB, entities []string) (FXOpsDashboardResponse, error) {
	hedgedINR, unhedgedINR, err := getHedgedVsUnhedged(ctx, db, entities)
	if err != nil {
		return FXOpsDashboardResponse{}, err
	}

	ratio := 0.0
	total := hedgedINR + unhedgedINR
	if total > 0 {
		ratio = (hedgedINR / total) * 100
	}

	topUnhedged, err := getTopUnhedgedByCurrency(ctx, db, entities, 5)
	if err != nil {
		return FXOpsDashboardResponse{}, err
	}

	varValue, varDelta, err := getValueAtRisk(ctx, db, entities)
	if err != nil {
		return FXOpsDashboardResponse{}, err
	}

	kpis := []KPICard{
		{Title: "Hedged Exposure Ratio", Value: fmt.Sprintf("%.2f%%", ratio), SubTitle: fmt.Sprintf("Hedged %s | Unhedged %s", formatAmount(hedgedINR), formatAmount(unhedgedINR))},
		{Title: "FX Value at Risk", Value: formatAmount(varValue), SubTitle: fmt.Sprintf("%+.1f%% vs prior", varDelta)},
		{Title: "Unhedged Exposure", Value: formatAmount(unhedgedINR), SubTitle: "Currency split below"},
	}

	return FXOpsDashboardResponse{
		Alerts:            []Alert{},
		KPIs:              kpis,
		Exposures:         []ExposureBlock{},
		SettlementSummary: SettlementSummary{},
		NetPositions:      topUnhedged,
		SpotRates:         getTopTradeCurrencies(),
		BankLimits:        []BankLimit{},
	}, nil
}

// buildGlobalLiquidity aggregates cash balance, burn, net operating cashflow, and runway days.
func buildGlobalLiquidity(ctx context.Context, db *sql.DB, entities []string, horizon int) (GlobalLiquidity, error) {
	today := time.Now().UTC()
	yesterday := today.AddDate(0, 0, -1)

	latestBalance, err := getLatestBalanceINR(ctx, db, today, entities)
	if err != nil {
		return GlobalLiquidity{}, err
	}

	yesterdayClosing, err := getLatestBalanceINR(ctx, db, yesterday, entities)
	if err != nil {
		return GlobalLiquidity{}, err
	}

	futurePayables, err := getPayablesSumINR(ctx, db, entities, today, time.Time{})
	if err != nil {
		return GlobalLiquidity{}, err
	}
	futureReceivables, err := getReceivablesSumINR(ctx, db, entities, today, time.Time{})
	if err != nil {
		return GlobalLiquidity{}, err
	}

	netOpCashflow := yesterdayClosing - futurePayables + futureReceivables

	burnStart := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, time.UTC)
	burnEnd := burnStart.AddDate(0, 1, -1)
	monthlyBurn, err := getPayablesSumINR(ctx, db, entities, burnStart, burnEnd)
	if err != nil {
		return GlobalLiquidity{}, err
	}

	runwayDays, weekHigh, weekLow, err := computeCashOnHandDays(ctx, db, entities, latestBalance, horizon)
	if err != nil {
		return GlobalLiquidity{}, err
	}

	return GlobalLiquidity{
		ConsolidatedCashBalance: latestBalance,
		AvailableLiquidity:      latestBalance,
		NetOperatingCashflow:    netOpCashflow,
		MonthlyBurnRate:         monthlyBurn,
		CashOnHandDays:          runwayDays,
		WeekCashOnHandHigh:      weekHigh,
		WeekCashOnHandLow:       weekLow,
	}, nil
}

// buildPortfolioInvestments derives balance and yield from portfolio_snapshot.
func buildPortfolioInvestments(ctx context.Context, db *sql.DB, entities []string) (PortfolioInvestments, error) {
	filterSQL := ""
	args := []interface{}{}
	if len(entities) > 0 {
		filterSQL = " WHERE ps.entity_name = ANY($1)"
		args = append(args, pqStringArray(entities))
	}

	query := fmt.Sprintf(`SELECT COALESCE(SUM(ps.current_value),0)::float8 AS total_current,
		COALESCE(SUM(ps.total_invested_amount),0)::float8 AS total_invested
		FROM investment.portfolio_snapshot ps%s`, filterSQL)

	var totalCurrent, totalInvested float64
	if err := db.QueryRowContext(ctx, query, args...).Scan(&totalCurrent, &totalInvested); err != nil {
		return PortfolioInvestments{}, err
	}

	yieldPct := 0.0
	if totalInvested > 0 {
		yieldPct = (totalCurrent - totalInvested) / totalInvested * 100
	}

	return PortfolioInvestments{PortfolioBalance: totalCurrent, PortfolioYield: yieldPct}, nil
}

// getHedgedVsUnhedged aggregates approved exposures into hedged and unhedged INR buckets.
func getHedgedVsUnhedged(ctx context.Context, db *sql.DB, entities []string) (float64, float64, error) {
	entityFilter := ""
	args := []interface{}{}
	if len(entities) > 0 {
		entityFilter = " AND eh.entity = ANY($1)"
		args = append(args, pqStringArray(entities))
	}

	query := fmt.Sprintf(`SELECT ABS(eh.total_open_amount)::float8 AS amt, COALESCE(eh.currency,'INR'), (hl.exposure_header_id IS NOT NULL) AS hedged
		FROM exposure_headers eh
		LEFT JOIN (
			SELECT DISTINCT exposure_header_id FROM exposure_hedge_links WHERE is_active = true
		) hl ON hl.exposure_header_id = eh.exposure_header_id
		WHERE LOWER(eh.approval_status) = 'approved'%s AND (eh.value_date IS NULL OR eh.value_date >= CURRENT_DATE)`, entityFilter)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var hedged, unhedged float64
	for rows.Next() {
		var amt float64
		var cur string
		var isHedged bool
		if err := rows.Scan(&amt, &cur, &isHedged); err != nil {
			return 0, 0, err
		}
		v := convertToINR(amt, cur)
		if isHedged {
			hedged += v
		} else {
			unhedged += v
		}
	}

	return hedged, unhedged, nil
}

// getTopUnhedgedByCurrency returns top N unhedged exposures grouped by currency (INR converted).
func getTopUnhedgedByCurrency(ctx context.Context, db *sql.DB, entities []string, topN int) ([]NetPosition, error) {
	entityFilter := ""
	args := []interface{}{}
	argPos := 1
	if len(entities) > 0 {
		entityFilter = fmt.Sprintf(constants.QuerryEntity, argPos)
		args = append(args, pqStringArray(entities))
		argPos++
	}

	query := fmt.Sprintf(`SELECT COALESCE(SUM(ABS(eh.total_open_amount)),0)::float8 AS amt, eh.currency
		FROM exposure_headers eh
		LEFT JOIN (
			SELECT DISTINCT exposure_header_id FROM exposure_hedge_links WHERE is_active = true
		) hl ON hl.exposure_header_id = eh.exposure_header_id
		WHERE LOWER(eh.approval_status) = 'approved' AND hl.exposure_header_id IS NULL%s AND (eh.value_date IS NULL OR eh.value_date >= CURRENT_DATE)
		GROUP BY eh.currency
		ORDER BY amt DESC
		LIMIT %d`, entityFilter, topN)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := []NetPosition{}
	for rows.Next() {
		var amt float64
		var cur string
		if err := rows.Scan(&amt, &cur); err != nil {
			return nil, err
		}
		res = append(res, NetPosition{
			Currency: cur,
			Amount:   formatAmount(convertToINR(amt, cur)),
		})
	}
	return res, nil
}

// getValueAtRisk approximates VaR as total unhedged exposure in the next 30 days, with delta vs prior 30 days.
func getValueAtRisk(ctx context.Context, db *sql.DB, entities []string) (float64, float64, error) {
	today := time.Now().Format(constants.DateFormat)
	next30 := time.Now().AddDate(0, 0, 30).Format(constants.DateFormat)
	prevStart := time.Now().AddDate(0, 0, -30).Format(constants.DateFormat)
	prevEnd := time.Now().AddDate(0, 0, -1).Format(constants.DateFormat)

	current, err := sumUnhedgedBetween(ctx, db, entities, today, next30)
	if err != nil {
		return 0, 0, err
	}
	prior, err := sumUnhedgedBetween(ctx, db, entities, prevStart, prevEnd)
	if err != nil {
		return 0, 0, err
	}

	delta := 0.0
	if prior > 0 {
		delta = ((current - prior) / prior) * 100
	}
	return current, delta, nil
}

// sumUnhedgedBetween sums unhedged exposures between two dates (inclusive) converted to INR.
func sumUnhedgedBetween(ctx context.Context, db *sql.DB, entities []string, startDate, endDate string) (float64, error) {
	entityFilter := ""
	args := []interface{}{startDate, endDate}
	if len(entities) > 0 {
		entityFilter = " AND eh.entity = ANY($3)"
		args = append(args, pqStringArray(entities))
	}

	query := fmt.Sprintf(`SELECT ABS(eh.total_open_amount)::float8 AS amt, COALESCE(eh.currency,'INR')
		FROM exposure_headers eh
		LEFT JOIN (
			SELECT DISTINCT exposure_header_id FROM exposure_hedge_links WHERE is_active = true
		) hl ON hl.exposure_header_id = eh.exposure_header_id
		WHERE LOWER(eh.approval_status) = 'approved' AND hl.exposure_header_id IS NULL AND eh.value_date BETWEEN $1 AND $2%s`, entityFilter)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var total float64
	for rows.Next() {
		var amt float64
		var cur string
		if err := rows.Scan(&amt, &cur); err != nil {
			return 0, err
		}
		total += convertToINR(amt, cur)
	}
	return total, nil
}

// getLatestBalanceINR sums latest balance per account up to asOfDate in INR.
func getLatestBalanceINR(ctx context.Context, db *sql.DB, asOfDate time.Time, entities []string) (float64, error) {
	dateStr := asOfDate.Format(constants.DateFormat)
	args := []interface{}{dateStr}
	filter := ""
	if len(entities) > 0 {
		// tolerate both entity names and entity IDs passed in 'entities'
		filter = " AND (mec.entity_name = ANY($2) OR mba.entity_id = ANY($2))"
		args = append(args, pqStringArray(entities))
	}

	// Prefer bank_statements (approved, latest per account) to match dashboard queries
	bsArgs := []interface{}{dateStr}
	bsFilter := ""
	if len(entities) > 0 {
		// accept either entity names or entity IDs from context
		bsFilter = " AND (me.entity_name = ANY($2) OR mba.entity_id = ANY($2))"
		bsArgs = append(bsArgs, pqStringArray(entities))
	}

	bsQuery := fmt.Sprintf(`
		WITH latest_approved AS (
			SELECT s.account_number, MAX(s.statement_period_end) AS maxdate
			FROM cimplrcorpsaas.bank_statements s
			JOIN (
				SELECT DISTINCT ON (bankstatementid) bankstatementid, processing_status, requested_at
				FROM cimplrcorpsaas.auditactionbankstatement
				ORDER BY bankstatementid, requested_at DESC
			) a ON a.bankstatementid = s.bank_statement_id
			WHERE a.processing_status = 'APPROVED'
			GROUP BY s.account_number
		)
		SELECT COALESCE(SUM(s.closing_balance),0)::float8 AS sum_amt, COALESCE(mba.currency,'INR') as currency_code
		FROM cimplrcorpsaas.bank_statements s
		JOIN latest_approved la ON la.account_number = s.account_number AND la.maxdate = s.statement_period_end
		JOIN masterbankaccount mba ON s.account_number = mba.account_number
		JOIN masterentitycash me ON mba.entity_id = me.entity_id
		WHERE mba.is_deleted = false AND s.statement_period_end <= $1 %s
		GROUP BY mba.currency`, bsFilter)

	bsRows, err := db.QueryContext(ctx, bsQuery, bsArgs...)
	if err != nil {
		return 0, err
	}
	defer bsRows.Close()

	var total float64
	for bsRows.Next() {
		var amt float64
		var cur string
		if err := bsRows.Scan(&amt, &cur); err != nil {
			return 0, err
		}
		total += convertToINR(amt, cur)
	}

	// Fallback: if no results from bank_statement, fall back to manual balances (existing logic)
	if total > 0 {
		return total, nil
	}

	// Fallback query: latest approved in bank_balances_manual
	query := fmt.Sprintf(`SELECT closing_balance, COALESCE(currency_code,'INR') FROM (
		WITH latest_approved_balance AS (
			SELECT DISTINCT ON (balance_id) balance_id, processing_status
			FROM public.auditactionbankbalances
			ORDER BY balance_id, requested_at DESC
		)
		SELECT DISTINCT ON (COALESCE(bbm.account_no, bbm.iban, bbm.nickname))
			bbm.closing_balance,
			bbm.currency_code,
			mec.entity_name
		FROM bank_balances_manual bbm
		JOIN latest_approved_balance lab ON lab.balance_id = bbm.balance_id AND lab.processing_status = 'APPROVED'
		JOIN masterbankaccount mba ON bbm.account_no = mba.account_number
		JOIN masterentitycash mec ON mba.entity_id = mec.entity_id
		WHERE mba.is_deleted = false AND bbm.as_of_date <= $1%s
		ORDER BY COALESCE(bbm.account_no, bbm.iban, bbm.nickname), bbm.as_of_date DESC, bbm.as_of_time DESC, bbm.balance_id DESC
	) t`, filter)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var amt float64
		var cur string
		if err := rows.Scan(&amt, &cur); err != nil {
			return 0, err
		}
		total += convertToINR(amt, cur)
	}

	return total, nil
}

// getPayablesSumINR aggregates approved payables in INR between dates (inclusive). Empty end means open-ended.
func getPayablesSumINR(ctx context.Context, db *sql.DB, entities []string, start, end time.Time) (float64, error) {
	args := []interface{}{start.Format(constants.DateFormat)}
	where := "WHERE p.due_date >= $1"
	idx := 2
	if !end.IsZero() {
		where += fmt.Sprintf(" AND p.due_date <= $%d", idx)
		args = append(args, end.Format(constants.DateFormat))
		idx++
	}
	if len(entities) > 0 {
		where += fmt.Sprintf(" AND p.entity_name = ANY($%d)", idx)
		args = append(args, pqStringArray(entities))
		idx++
	}

	query := fmt.Sprintf(`SELECT COALESCE(SUM(p.amount),0)::float8, COALESCE(p.currency_code,'INR')
		FROM tr_payables p
		JOIN auditactionpayable a ON a.payable_id = p.payable_id AND a.processing_status = 'APPROVED'
		%s GROUP BY p.currency_code`, where)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var total float64
	for rows.Next() {
		var amt float64
		var cur string
		if err := rows.Scan(&amt, &cur); err != nil {
			return 0, err
		}
		total += convertToINR(amt, cur)
	}
	return total, nil
}

// getReceivablesSumINR aggregates approved receivables in INR between dates (inclusive). Empty end means open-ended.
func getReceivablesSumINR(ctx context.Context, db *sql.DB, entities []string, start, end time.Time) (float64, error) {
	args := []interface{}{start.Format(constants.DateFormat)}
	where := "WHERE r.due_date >= $1"
	idx := 2
	if !end.IsZero() {
		where += fmt.Sprintf(" AND r.due_date <= $%d", idx)
		args = append(args, end.Format(constants.DateFormat))
		idx++
	}
	if len(entities) > 0 {
		where += fmt.Sprintf(" AND r.entity_name = ANY($%d)", idx)
		args = append(args, pqStringArray(entities))
		idx++
	}

	query := fmt.Sprintf(`SELECT COALESCE(SUM(r.invoice_amount),0)::float8, COALESCE(r.currency_code,'INR')
		FROM tr_receivables r
		JOIN auditactionreceivable a ON a.receivable_id = r.receivable_id AND a.processing_status = 'APPROVED'
		%s GROUP BY r.currency_code`, where)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var total float64
	for rows.Next() {
		var amt float64
		var cur string
		if err := rows.Scan(&amt, &cur); err != nil {
			return 0, err
		}
		total += convertToINR(amt, cur)
	}
	return total, nil
}

// computeCashOnHandDays simulates daily balances using receivables/payables and projections to derive runway.
func computeCashOnHandDays(ctx context.Context, db *sql.DB, entities []string, openingBalance float64, horizon int) (int, int, int, error) {
	if horizon < 30 {
		horizon = 30
	}

	start := time.Now().UTC()
	end := start.AddDate(0, 0, horizon-1)

	inflowMap, outflowMap, err := buildDailyMaps(ctx, db, entities, start, end)
	if err != nil {
		return 0, 0, 0, err
	}

	positiveDays := 0
	weeklyCounts := []int{}
	balance := openingBalance
	dayCounter := 0
	weekPos := 0

	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		key := d.Format(constants.DateFormat)
		net := inflowMap[key] - outflowMap[key]
		balance += net
		if balance > 0 {
			positiveDays++
			weekPos++
		}
		dayCounter++
		if dayCounter%7 == 0 {
			weeklyCounts = append(weeklyCounts, weekPos)
			weekPos = 0
		}
	}

	if dayCounter%7 != 0 {
		weeklyCounts = append(weeklyCounts, weekPos)
	}

	weekHigh, weekLow := 0, 0
	if len(weeklyCounts) > 0 {
		weekHigh = weeklyCounts[0]
		weekLow = weeklyCounts[0]
		for _, v := range weeklyCounts {
			if v > weekHigh {
				weekHigh = v
			}
			if v < weekLow {
				weekLow = v
			}
		}
	}

	return positiveDays, weekHigh, weekLow, nil
}

// buildDailyMaps returns INR inflow/outflow per day combining receivables, payables, and approved projections.
func buildDailyMaps(ctx context.Context, db *sql.DB, entities []string, start, end time.Time) (map[string]float64, map[string]float64, error) {
	inflows := map[string]float64{}
	outflows := map[string]float64{}

	// Receivables
	recArgs := []interface{}{start.Format(constants.DateFormat), end.Format(constants.DateFormat)}
	recWhere := "WHERE r.due_date BETWEEN $1 AND $2"
	idx := 3
	if len(entities) > 0 {
		recWhere += fmt.Sprintf(" AND r.entity_name = ANY($%d)", idx)
		recArgs = append(recArgs, pqStringArray(entities))
		idx++
	}
	recQ := fmt.Sprintf(`SELECT r.due_date::date, COALESCE(SUM(r.invoice_amount),0)::float8, COALESCE(r.currency_code,'INR') FROM tr_receivables r
		JOIN auditactionreceivable a ON a.receivable_id = r.receivable_id AND a.processing_status = 'APPROVED'
		%s GROUP BY r.due_date::date, r.currency_code`, recWhere)
	recRows, err := db.QueryContext(ctx, recQ, recArgs...)
	if err != nil {
		return nil, nil, err
	}
	for recRows.Next() {
		var dt time.Time
		var amt float64
		var cur string
		if err := recRows.Scan(&dt, &amt, &cur); err != nil {
			recRows.Close()
			return nil, nil, err
		}
		key := dt.Format(constants.DateFormat)
		inflows[key] += convertToINR(amt, cur)
	}
	recRows.Close()

	// Payables
	payArgs := []interface{}{start.Format(constants.DateFormat), end.Format(constants.DateFormat)}
	payWhere := "WHERE p.due_date BETWEEN $1 AND $2"
	idx = 3
	if len(entities) > 0 {
		payWhere += fmt.Sprintf(" AND p.entity_name = ANY($%d)", idx)
		payArgs = append(payArgs, pqStringArray(entities))
		idx++
	}
	payQ := fmt.Sprintf(`SELECT p.due_date::date, COALESCE(SUM(p.amount),0)::float8, COALESCE(p.currency_code,'INR') FROM tr_payables p
		JOIN auditactionpayable a ON a.payable_id = p.payable_id AND a.processing_status = 'APPROVED'
		%s GROUP BY p.due_date::date, p.currency_code`, payWhere)
	payRows, err := db.QueryContext(ctx, payQ, payArgs...)
	if err != nil {
		return nil, nil, err
	}
	for payRows.Next() {
		var dt time.Time
		var amt float64
		var cur string
		if err := payRows.Scan(&dt, &amt, &cur); err != nil {
			payRows.Close()
			return nil, nil, err
		}
		key := dt.Format(constants.DateFormat)
		outflows[key] += convertToINR(amt, cur)
	}
	payRows.Close()

	// Projections (cashflow_projection_monthly) distribute daily similar to forecast logic
	baseMonth := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
	endMonthKey := end.Year()*12 + int(end.Month())
	projArgs := []interface{}{baseMonth.Year()*12 + int(baseMonth.Month()), endMonthKey}
	projWhere := ""
	if len(entities) > 0 {
		projWhere = fmt.Sprintf(" AND cpi.entity_name = ANY($%d)", len(projArgs)+1)
		projArgs = append(projArgs, pqStringArray(entities))
	}

	projQ := fmt.Sprintf(`SELECT cpm.year, cpm.month, cpi.cashflow_type, COALESCE(cp.currency_code,'INR'), cpi.start_date, COALESCE(SUM(cpm.projected_amount),0)::float8
		FROM cashflow_projection_monthly cpm
		JOIN cashflow_proposal_item cpi ON cpm.item_id = cpi.item_id
		JOIN cashflow_proposal cp ON cpi.proposal_id = cp.proposal_id
		LEFT JOIN LATERAL (SELECT processing_status FROM audit_action_cashflow_proposal a2 WHERE a2.proposal_id = cp.proposal_id ORDER BY requested_at DESC LIMIT 1) aa ON TRUE
		WHERE aa.processing_status = 'APPROVED' AND (cpm.year*12 + cpm.month) BETWEEN $1 AND $2%s
		GROUP BY cpm.year, cpm.month, cpi.cashflow_type, cp.currency_code, cpi.start_date`, projWhere)

	projRows, err := db.QueryContext(ctx, projQ, projArgs...)
	if err != nil {
		return nil, nil, err
	}
	for projRows.Next() {
		var year, month int
		var ctype, cur string
		var startDate *time.Time
		var amt float64
		if err := projRows.Scan(&year, &month, &ctype, &cur, &startDate, &amt); err != nil {
			projRows.Close()
			return nil, nil, err
		}
		firstOfMonth := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
		daysInMonth := time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
		rateAmt := convertToINR(amt, cur)

		if startDate != nil {
			effDay := startDate.Day()
			if effDay > daysInMonth {
				effDay = daysInMonth
			}
			target := time.Date(year, time.Month(month), effDay, 0, 0, 0, 0, time.UTC)
			if target.Before(start) || target.After(end) {
				continue
			}
			key := target.Format(constants.DateFormat)
			if ctype == "Inflow" {
				inflows[key] += rateAmt
			} else {
				outflows[key] += rateAmt
			}
		} else {
			daily := rateAmt / float64(daysInMonth)
			for d := firstOfMonth; d.Month() == time.Month(month); d = d.AddDate(0, 0, 1) {
				if d.Before(start) || d.After(end) {
					continue
				}
				key := d.Format(constants.DateFormat)
				if ctype == "Inflow" {
					inflows[key] += daily
				} else {
					outflows[key] += daily
				}
			}
		}
	}
	projRows.Close()

	return inflows, outflows, nil
}

// pqStringArray ensures []string works with ANY($n).
func pqStringArray(list []string) interface{} {
	return pq.Array(list)
}
