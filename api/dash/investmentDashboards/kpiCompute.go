package investmentdashboards

import (
	"context"
	"fmt"
	"math"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/portfolio"

	"github.com/jackc/pgx/v5/pgxpool"
)

// computeKPIs is the single source of truth for investment overview KPI cards.
// GetInvestmentOverviewKPIs and the portfolio dashboard both call this function.
func computeKPIs(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, now time.Time) (interface{}, error) {
	fyStart := getFinancialYearStart(now)
	prevFYStart := fyStart.AddDate(-1, 0, 0)
	lastMonthEnd := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)

	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}

	psScope := SQLParamsEntityScope("ps.entity_name")
	otScope := SQLParamsEntityScope("COALESCE(ot.entity_name, '')")
	txScope := SQLParamsEntityScope("COALESCE(entity_name, '')")
	meScope := SQLParamsEntityScope("me.entity_name")

	kpiQuery := fmt.Sprintf(`
	WITH params AS (
		SELECT $1::text AS entity_filter, $2::date AS fy_start, $3::date AS last_month_end, $4::date AS prev_fy_start, $5::text[] AS allowed_entities
	),
	current_aum AS (
		SELECT COALESCE(SUM(ps.current_value), 0)::float8 AS total_aum
		FROM investment.portfolio_snapshot ps, params p
		WHERE %s
	),
	last_month_holdings AS (
		SELECT COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref, ms.amfi_scheme_code,
		  SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
		           WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0) ELSE 0 END) AS units
		FROM investment.approved_onboard_transaction ot
		LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))), params p
		WHERE ot.transaction_date <= p.last_month_end AND %s
		GROUP BY COALESCE(ot.scheme_id,ot.scheme_internal_code), ms.amfi_scheme_code
		HAVING SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
		               WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0) ELSE 0 END) > 0
	),
	last_month_navs AS (
		SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_code, nav_value
		FROM investment.amfi_nav_staging, params p WHERE nav_date <= p.last_month_end ORDER BY scheme_code, nav_date DESC
	),
	last_month_aum AS (
		SELECT COALESCE(SUM(h.units * COALESCE(n.nav_value,0)),0)::float8 AS aum FROM last_month_holdings h
		LEFT JOIN last_month_navs n ON n.scheme_code = h.amfi_scheme_code::text
	),
	fy_start_holdings AS (
		SELECT COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref, ms.amfi_scheme_code,
		  SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
		           WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0) ELSE 0 END) AS units
		FROM investment.approved_onboard_transaction ot
		LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))), params p
		WHERE ot.transaction_date < p.fy_start AND %s
		GROUP BY COALESCE(ot.scheme_id,ot.scheme_internal_code), ms.amfi_scheme_code
		HAVING SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
		               WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0) ELSE 0 END) > 0
	),
	fy_start_navs AS (
		SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_code, nav_value
		FROM investment.amfi_nav_staging, params p WHERE nav_date < p.fy_start ORDER BY scheme_code, nav_date DESC
	),
	fy_start_aum AS (
		SELECT COALESCE(SUM(h.units * COALESCE(n.nav_value,0)),0)::float8 AS aum FROM fy_start_holdings h
		LEFT JOIN fy_start_navs n ON n.scheme_code = h.amfi_scheme_code::text
	),
	prev_fy_start_holdings AS (
		SELECT COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref, ms.amfi_scheme_code,
		  SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
		           WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0) ELSE 0 END) AS units
		FROM investment.approved_onboard_transaction ot
		LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))), params p
		WHERE ot.transaction_date < p.prev_fy_start AND %s
		GROUP BY COALESCE(ot.scheme_id,ot.scheme_internal_code), ms.amfi_scheme_code
		HAVING SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
		               WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0) ELSE 0 END) > 0
	),
	prev_fy_start_navs AS (
		SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_code, nav_value
		FROM investment.amfi_nav_staging, params p WHERE nav_date <= p.prev_fy_start ORDER BY scheme_code, nav_date DESC
	),
	prev_fy_start_aum AS (
		SELECT COALESCE(SUM(h.units * COALESCE(n.nav_value,0)),0)::float8 AS aum FROM prev_fy_start_holdings h
		LEFT JOIN prev_fy_start_navs n ON n.scheme_code = h.amfi_scheme_code::text
	),
	ytd_flows AS (
		SELECT
		  COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('buy','purchase','subscription','switch_in') THEN amount ELSE 0 END),0)::float8 AS buys,
		  COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('sell','redemption','switch_out') THEN amount ELSE 0 END),0)::float8 AS sells
		FROM investment.approved_onboard_transaction, params p
		WHERE transaction_date >= p.fy_start AND %s
	),
	cash_balances AS (
		SELECT COALESCE(SUM(bs.closingbalance),0)::float8 AS total_cash
		FROM bank_statement bs
		JOIN masterbankaccount mba ON bs.account_number = mba.account_number
		LEFT JOIN masterentity me ON mba.entity_id = me.entity_id
		CROSS JOIN params p
		WHERE bs.status = 'Approved' AND %s
	),
	manual_balances AS (
		SELECT 0::float8 AS total_manual
	),
	sellable_mf AS (
		SELECT COALESCE(SUM(current_value),0)::float8 AS total_sellable
		FROM investment.portfolio_snapshot ps, params p
		WHERE COALESCE(ps.total_units,0) > 0 AND COALESCE(ps.current_value,0) > 0 AND %s
	)
	SELECT ca.total_aum, lma.aum, fsa.aum, pfa.aum, yf.buys, yf.sells, (cb.total_cash + mb.total_manual), sm.total_sellable
	FROM current_aum ca, last_month_aum lma, fy_start_aum fsa, prev_fy_start_aum pfa, ytd_flows yf, cash_balances cb, manual_balances mb, sellable_mf sm
	`, psScope, otScope, otScope, otScope, txScope, meScope, psScope)

	var totalAUM, lastMonthAUM, fyStartAUM, prevFyAUM, ytdBuys, ytdSells, totalCash, sellableMF float64
	err := pgxPool.QueryRow(ctx, kpiQuery,
		nullIfEmpty(entityFilter), fyStart.Format(constants.DateFormat),
		lastMonthEnd.Format(constants.DateFormat), prevFYStart.Format(constants.DateFormat),
		allowedParam,
	).Scan(&totalAUM, &lastMonthAUM, &fyStartAUM, &prevFyAUM, &ytdBuys, &ytdSells, &totalCash, &sellableMF)
	if err != nil {
		return nil, fmt.Errorf("kpi query: %w", err)
	}

	ytdTxRows, ytdTxErr := portfolio.QueryPortfolioTransactions(ctx, pgxPool, portfolio.TxFilter{
		EntityName:      entityFilter,
		AllowedEntities: allowedEntities,
		DateFrom:        fyStart.Format(constants.DateFormat),
		DateTo:          now.Format(constants.DateFormat),
	})
	if ytdTxErr == nil {
		ytdBuys, ytdSells = portfolio.SummarizeYTDFlows(ytdTxRows)
	}

	liveKPI, liveErr := portfolio.QueryHoldingsKPI(ctx, pgxPool, entityFilter, allowedEntities)
	if liveErr == nil {
		totalAUM = liveKPI.TotalAUM
		sellableMF = liveKPI.TotalAUM
	}

	openingAUM := fyStartAUM
	if openingAUM == 0 {
		openingAUM = lastMonthAUM
	}
	ytdPL := totalAUM + ytdSells - ytdBuys - openingAUM
	if liveErr == nil {
		ytdPL = liveKPI.TotalPnL
	}

	aumTrendPct := 0.0
	aumTrendNote := ""
	if lastMonthAUM > 0 {
		rawTrend := ((totalAUM - lastMonthAUM) / lastMonthAUM) * 100
		if rawTrend > 200 && ytdBuys > 0 && ytdBuys >= (totalAUM-lastMonthAUM)*0.4 {
			base := openingAUM
			if base <= 0 {
				base = lastMonthAUM
			}
			if base > 0 {
				aumTrendPct = ((totalAUM - base) / base) * 100
				aumTrendNote = "YTD growth after large inflows this period"
			} else {
				aumTrendPct = rawTrend
			}
		} else {
			aumTrendPct = rawTrend
		}
	}

	monthBeforeLastEnd := lastMonthEnd.AddDate(0, -1, 0)
	previousMonthAUM := getAUMAtDate(ctx, pgxPool, entityFilter, allowedEntities, monthBeforeLastEnd)
	prevAumTrendPct := 0.0
	if previousMonthAUM > 0 {
		prevAumTrendPct = ((lastMonthAUM - previousMonthAUM) / previousMonthAUM) * 100
	}

	xirrVal := calculatePortfolioXIRR(ctx, pgxPool, entityFilter, allowedEntities, totalAUM, now)
	prevXirr := 0.0
	if prevFyAUM != 0 {
		prevXirr = calculatePortfolioXIRR(ctx, pgxPool, entityFilter, allowedEntities, prevFyAUM, prevFYStart)
	} else {
		prevXirr = calculatePortfolioXIRR(ctx, pgxPool, entityFilter, allowedEntities, lastMonthAUM, lastMonthEnd)
	}

	liquidityTotal := totalCash + sellableMF
	baselineAUM := prevFyAUM
	if baselineAUM == 0 {
		baselineAUM = lastMonthAUM
	}
	liquidityBaseline := prevFyAUM
	if liquidityBaseline == 0 {
		liquidityBaseline = lastMonthAUM
	}

	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	allTransactions := fetchRawTransactionDetails(ctx, pgxPool, entityFilter, allowedEntities, fyStart)
	ytdTransactions, transactionGroups := transactionDetailPayload(allTransactions, fyStart)

	return map[string]interface{}{
		"cards": []KPICard{
			KPIChartFromValues("Total AUM", totalAUM, baselineAUM),
			{Title: "AUM Trend", Value: math.Round(aumTrendPct*100) / 100, LastValue: math.Round(prevAumTrendPct*100) / 100, Change: math.Round((aumTrendPct-prevAumTrendPct)*100) / 100},
			{Title: "YTD P&L", Value: math.Round(ytdPL*100) / 100, LastValue: openingAUM, Change: 0},
			func() KPICard {
				currentPct := math.Round(xirrVal*10000) / 100
				prevPct := math.Round(prevXirr*10000) / 100
				return KPICard{Title: "Portfolio XIRR", Value: currentPct, LastValue: prevPct, Change: math.Round((currentPct-prevPct)*100) / 100}
			}(),
			KPIChartFromValues("Liquidity Position", liquidityTotal, liquidityBaseline),
		},
		"details": map[string]interface{}{
			"total_aum": totalAUM, "last_month_aum": lastMonthAUM, "last_fy_aum": prevFyAUM,
			"fy_start_aum": fyStartAUM, "ytd_buys": ytdBuys, "ytd_sells": ytdSells, "ytd_pnl": ytdPL,
			"cash_balance": totalCash, "sellable_mf": sellableMF, "liquidity_total": liquidityTotal,
			"xirr_annualized": xirrVal * 100,
			"financial_year":  fmt.Sprintf("FY %d-%d", fyStart.Year(), fyStart.Year()+1),
			"period_start":    fyStart.Format(constants.DateFormat), "period_end": now.Format(constants.DateFormat),
			"aum_trend_note": aumTrendNote,
		},
		"aum_detail":           aumDetails,
		"transaction_detail":   ytdTransactions,
		"transaction_groups":   transactionGroups,
		"generated_at":         time.Now().UTC().Format(time.RFC3339),
	}, nil
}
