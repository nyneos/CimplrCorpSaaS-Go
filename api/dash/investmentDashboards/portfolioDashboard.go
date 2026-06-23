// Package investmentdashboards provides the single-shot portfolio dashboard endpoint.
//
// POST /dash/investment/portfolio/dashboard
//
// This handler orchestrates all investment sub-computations concurrently using
// sync.WaitGroup + goroutines so the total latency is bounded by the slowest
// individual sub-query rather than the sum of all sub-queries.
//
// The response envelope keys are identical to the individual endpoints so no
// TypeScript/frontend changes are required.
//
// Response shape:
//
//	{
//	  "success": true,
//	  "generated_at": "<RFC3339>",
//	  "kpis":                   { ... },  // /dash/investment/overview/kpis
//	  "entity_performance":     { ... },  // /dash/investment/overview/entity
//	  "amc_performance":        { ... },  // /dash/investment/overview/amc-performance
//	  "waterfall":              { ... },  // /dash/investment/overview/waterfall (AMC waterfall)
//	  "aum_movement":           { ... },  // /dash/investment/portfolio/aum-movement
//	  "aum_breakdown":          { ... },  // /dash/investment/portfolio/aum-breakdown
//	  "performance_attribution":{ ... },  // /dash/investment/performance/performance-attribution
//	  "pnl_heatmap":            { ... },  // /dash/investment/performance/pnl-heatmap
//	  "portfolio_vs_benchmark": { ... },  // /dash/investment/performance/portfolio-vs-benchmark
//	  "ticker":                 { ... },  // /dash/investment/overview/market-rates-ticker
//	  "top_performing":         { ... },  // /dash/investment/overview/top-performing
//	  "aum_composition":        { ... },  // /dash/investment/overview/aum-composition
//	  "consolidated_risk":      { ... },  // /dash/investment/overview/consolidated
//	  "combined":               { ... },  // /dash/investment/overview/combined
//	}
package investmentdashboards

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"

	// "strings"
	"sync"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/portfolio"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PortfolioDashboardHandler returns a single http.HandlerFunc that concurrently
// executes every investment sub-computation and merges the results into one
// response payload.  All individual endpoint response shapes are preserved
// so the UI requires zero changes.
func PortfolioDashboardHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		ctx := r.Context()

		// ── Request parsing ────────────────────────────────────────────────────
		var req struct {
			EntityName  string `json:"entity_name,omitempty"`
			GroupBy     string `json:"group_by,omitempty"` // for aum_breakdown; default "amc"
			Limit       int    `json:"limit,omitempty"`    // for top_performing; default 5
			Year        int    `json:"year,omitempty"`     // FY start year; default current FY
			Benchmark   string `json:"benchmark,omitempty"`
			PeriodStart string `json:"period_start,omitempty"`
			PeriodEnd   string `json:"period_end,omitempty"`
		}
		// Body may be empty; ignore decode error.
		_ = json.NewDecoder(r.Body).Decode(&req)

		entityFilter, allowedEntities, scopeMsg := investmentDashboardEntityScope(ctx, req.EntityName)
		if scopeMsg != "" {
			api.RespondWithError(w, http.StatusForbidden, scopeMsg)
			return
		}

		// Defaults
		if req.Limit <= 0 {
			req.Limit = 5
		}
		if req.GroupBy == "" {
			req.GroupBy = "amc"
		}
		if req.Benchmark == "" {
			req.Benchmark = constants.Nifty50
		}

		now := time.Now().UTC()
		if req.Year <= 0 {
			if now.Month() >= time.April {
				req.Year = now.Year()
			} else {
				req.Year = now.Year() - 1
			}
		}
		if req.PeriodEnd == "" {
			req.PeriodEnd = now.Format(constants.DateFormat)
		}
		if req.PeriodStart == "" {
			req.PeriodStart = getFinancialYearStart(now).Format(constants.DateFormat)
		}

		// ── Concurrent sub-computation helpers ────────────────────────────────
		// Each sub-computation is wrapped in a goroutine.  We use a mutex-protected
		// results map rather than channels to keep the code simple and readable.
		type result struct {
			data interface{}
			err  error
		}

		results := make(map[string]result, 14)
		var mu sync.Mutex
		var wg sync.WaitGroup

		setResult := func(key string, data interface{}, err error) {
			mu.Lock()
			results[key] = result{data: data, err: err}
			mu.Unlock()
		}

		// ── 1. KPIs ───────────────────────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeKPIs(ctx, pgxPool, entityFilter, allowedEntities, now)
			setResult("kpis", data, err)
		}()

		// ── 2. Entity Performance ─────────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeEntityPerformance(ctx, pgxPool, entityFilter, allowedEntities, req.Limit)
			setResult("entity_performance", data, err)
		}()

		// ── 3. AMC Performance ────────────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeAMCPerformance(ctx, pgxPool, entityFilter, allowedEntities)
			setResult("amc_performance", data, err)
		}()

		// ── 4. AMC Waterfall ──────────────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeAMCWaterfall(ctx, pgxPool, entityFilter, allowedEntities, req.PeriodStart, req.PeriodEnd)
			setResult("waterfall", data, err)
		}()

		// ── 5. AUM Movement Waterfall ─────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeAUMMovement(ctx, pgxPool, entityFilter, allowedEntities, req.PeriodStart, req.PeriodEnd, now)
			setResult("aum_movement", data, err)
		}()

		// ── 6. AUM Breakdown ──────────────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeAUMBreakdown(ctx, pgxPool, entityFilter, allowedEntities, req.GroupBy)
			setResult("aum_breakdown", data, err)
		}()

		// ── 7. Performance Attribution ────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computePerformanceAttribution(ctx, pgxPool, entityFilter, allowedEntities, req.Year, req.Benchmark, now)
			setResult("performance_attribution", data, err)
		}()

		// ── 8. Daily P&L Heatmap ─────────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computePnLHeatmap(ctx, pgxPool, entityFilter, allowedEntities, req.GroupBy)
			setResult("pnl_heatmap", data, err)
		}()

		// ── 9. Portfolio vs Benchmark ─────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computePortfolioVsBenchmark(ctx, pgxPool, entityFilter, allowedEntities, req.Year, req.Benchmark, now)
			setResult("portfolio_vs_benchmark", data, err)
		}()

		// ── 10. Market Rates Ticker ───────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeMarketTicker(ctx, pgxPool, entityFilter, allowedEntities, req.Limit)
			setResult("ticker", data, err)
		}()

		// ── 11. Top Performing Assets ─────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeTopPerforming(ctx, pgxPool, entityFilter, allowedEntities, req.Limit)
			setResult("top_performing", data, err)
		}()

		// ── 12. AUM Composition Trend ─────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeAUMCompositionTrend(ctx, pgxPool, entityFilter, allowedEntities, req.Year, now)
			setResult("aum_composition", data, err)
		}()

		// ── 13. Consolidated Risk ─────────────────────────────────────────────
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeConsolidatedRisk(ctx, pgxPool, entityFilter, allowedEntities)
			setResult("consolidated_risk", data, err)
		}()

		// ── 14. Combined Overview (lightweight, uses portfolio_snapshot only) ──
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := computeCombinedOverview(ctx, pgxPool, entityFilter, allowedEntities, req.Limit, now)
			setResult("combined", data, err)
		}()

		// Wait for all goroutines to finish.
		wg.Wait()

		// ── Build response ─────────────────────────────────────────────────────
		// On partial errors we still return whatever succeeded; each key carries an
		// optional "error" field so the UI can surface degraded sub-sections.
		resp := map[string]interface{}{
			"success":      true,
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		}
		for key, res := range results {
			if res.err != nil {
				resp[key] = map[string]interface{}{
					"error": res.err.Error(),
				}
			} else {
				resp[key] = res.data
			}
		}

		api.RespondWithPayload(w, true, "", resp)
	}
}

// ─── Sub-computation helpers ─────────────────────────────────────────────────
// Each function contains the same business logic as the corresponding HTTP
// handler but operates directly on the database pool (no HTTP recording).

// computeKPIs mirrors GetInvestmentOverviewKPIs business logic.
func computeKPIs(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, now time.Time) (interface{}, error) {
	fyStart := getFinancialYearStart(now)
	prevFYStart := fyStart.AddDate(-1, 0, 0)
	lastMonthEnd := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)

	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}

	kpiQuery := `
	WITH params AS (
		SELECT $1::text AS entity_filter, $2::date AS fy_start, $3::date AS last_month_end, $4::date AS prev_fy_start, $5::text[] AS allowed_entities
	),
	current_aum AS (
		SELECT COALESCE(SUM(ps.current_value), 0)::float8 AS total_aum
		FROM investment.portfolio_snapshot ps, params p
		WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
		  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
	),
	last_month_holdings AS (
		SELECT COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref, ms.amfi_scheme_code,
		  SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
		           WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0) ELSE 0 END) AS units
		FROM investment.approved_onboard_transaction ot
		LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))), params p
		WHERE ot.transaction_date <= p.last_month_end
		  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name,'')=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name,'')=ANY(p.allowed_entities))
		GROUP BY COALESCE(ot.scheme_id,ot.scheme_internal_code), ms.amfi_scheme_code
		HAVING SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
		               WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0) ELSE 0 END) > 0
	),
	last_month_navs AS (
		SELECT DISTINCT ON (scheme_code) scheme_code::text, nav_value
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
		WHERE ot.transaction_date < p.fy_start
		  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name,'')=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name,'')=ANY(p.allowed_entities))
		GROUP BY COALESCE(ot.scheme_id,ot.scheme_internal_code), ms.amfi_scheme_code
		HAVING SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
		               WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0) ELSE 0 END) > 0
	),
	fy_start_navs AS (
		SELECT DISTINCT ON (scheme_code) scheme_code::text, nav_value
		FROM investment.amfi_nav_staging, params p WHERE nav_date < p.fy_start ORDER BY scheme_code, nav_date DESC
	),
	fy_start_aum AS (
		SELECT COALESCE(SUM(h.units * COALESCE(n.nav_value,0)),0)::float8 AS aum FROM fy_start_holdings h
		LEFT JOIN fy_start_navs n ON n.scheme_code = h.amfi_scheme_code::text
	),
	ytd_flows AS (
		SELECT
		  COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('buy','purchase','subscription','switch_in') THEN amount ELSE 0 END),0)::float8 AS buys,
		  COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('sell','redemption','switch_out') THEN amount ELSE 0 END),0)::float8 AS sells
		FROM investment.approved_onboard_transaction, params p
		WHERE transaction_date >= p.fy_start
		  AND (p.entity_filter IS NULL OR COALESCE(entity_name,'')=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR COALESCE(entity_name,'')=ANY(p.allowed_entities))
	),
	cash_balances AS (
		SELECT COALESCE(SUM(bs.closingbalance),0)::float8 AS total_cash
		FROM bank_statement bs
		JOIN masterbankaccount mba ON bs.account_number = mba.account_number
		LEFT JOIN masterentity me ON mba.entity_id = me.entity_id
		CROSS JOIN params p
		WHERE bs.status = 'Approved'
		  AND (p.entity_filter IS NULL OR me.entity_name = p.entity_filter)
		  AND (p.allowed_entities IS NULL OR me.entity_name = ANY(p.allowed_entities))
	),
	sellable_mf AS (
		SELECT COALESCE(SUM(current_value),0)::float8 AS total_sellable
		FROM investment.portfolio_snapshot ps, params p
		WHERE COALESCE(ps.total_units,0) > 0 AND COALESCE(ps.current_value,0) > 0
		  AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
		  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
	)
	SELECT ca.total_aum, lma.aum, fsa.aum, yf.buys, yf.sells, cb.total_cash, sm.total_sellable
	FROM current_aum ca, last_month_aum lma, fy_start_aum fsa, ytd_flows yf, cash_balances cb, sellable_mf sm
	`

	var totalAUM, lastMonthAUM, fyStartAUM, ytdBuys, ytdSells, totalCash, sellableMF float64
	err := pgxPool.QueryRow(ctx, kpiQuery,
		nullIfEmpty(entityFilter), fyStart.Format(constants.DateFormat),
		lastMonthEnd.Format(constants.DateFormat), prevFYStart.Format(constants.DateFormat),
		allowedParam,
	).Scan(&totalAUM, &lastMonthAUM, &fyStartAUM, &ytdBuys, &ytdSells, &totalCash, &sellableMF)
	if err != nil {
		return nil, fmt.Errorf("kpi query: %w", err)
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
	if lastMonthAUM > 0 {
		aumTrendPct = ((totalAUM - lastMonthAUM) / lastMonthAUM) * 100
	}
	monthBeforeLastEnd := lastMonthEnd.AddDate(0, -1, 0)
	prevMonthAUM := getAUMAtDate(ctx, pgxPool, entityFilter, allowedEntities, monthBeforeLastEnd)
	prevAumTrendPct := 0.0
	if prevMonthAUM > 0 {
		prevAumTrendPct = ((lastMonthAUM - prevMonthAUM) / prevMonthAUM) * 100
	}
	xirrVal := calculatePortfolioXIRR(ctx, pgxPool, entityFilter, allowedEntities, totalAUM, now)
	liquidityTotal := totalCash + sellableMF
	baselineAUM := prevMonthAUM
	if baselineAUM == 0 {
		baselineAUM = lastMonthAUM
	}

	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	rawTransactions := fetchRawTransactionDetails(ctx, pgxPool, entityFilter, allowedEntities, fyStart)

	return map[string]interface{}{
		"cards": []KPICard{
			KPIChartFromValues("Total AUM", totalAUM, baselineAUM),
			{Title: "AUM Trend", Value: math.Round(aumTrendPct*100) / 100, LastValue: math.Round(prevAumTrendPct*100) / 100, Change: math.Round((aumTrendPct-prevAumTrendPct)*100) / 100},
			{Title: "YTD P&L", Value: math.Round(ytdPL*100) / 100, LastValue: openingAUM},
			func() KPICard {
				pct := math.Round(xirrVal*10000) / 100
				return KPICard{Title: "Portfolio XIRR", Value: pct, LastValue: 0}
			}(),
			KPIChartFromValues("Liquidity Position", liquidityTotal, baselineAUM),
		},
		"details": map[string]interface{}{
			"total_aum": totalAUM, "last_month_aum": lastMonthAUM, "fy_start_aum": fyStartAUM,
			"ytd_buys": ytdBuys, "ytd_sells": ytdSells, "ytd_pnl": ytdPL,
			"cash_balance": totalCash, "sellable_mf": sellableMF, "liquidity_total": liquidityTotal,
			"xirr_annualized": xirrVal * 100,
			"financial_year":  fmt.Sprintf("FY %d-%d", fyStart.Year(), fyStart.Year()+1),
			"period_start":    fyStart.Format(constants.DateFormat), "period_end": now.Format(constants.DateFormat),
		},
		"aum_detail":         aumDetails,
		"transaction_detail": rawTransactions,
		"generated_at":       time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computeEntityPerformance mirrors GetEntityPerformance business logic.
func computeEntityPerformance(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, limit int) (interface{}, error) {
	fyStart := getFinancialYearStart(time.Now())
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	if limit <= 0 {
		limit = 50
	}

	q := `
	WITH params AS (SELECT $1::date AS fy_start, $2::text AS entity_filter, $3::text AS amc_filter, $4::text[] AS allowed_entities),
	tx AS (
		SELECT COALESCE(ot.scheme_id, ot.scheme_internal_code, ms.scheme_id::text) AS scheme_ref,
		       COALESCE(ms.scheme_name, ot.scheme_internal_code, ot.scheme_id::text) AS scheme_name,
		       COALESCE(ms.amc_name,'') AS amc_name,
		       SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in') THEN COALESCE(ot.amount,0) ELSE 0 END) AS buys_since,
		       SUM(CASE WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN COALESCE(ot.amount,0) ELSE 0 END) AS sells_since
		FROM investment.approved_onboard_transaction ot
		LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))), params p
		WHERE ot.transaction_date >= p.fy_start
		  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name,'')=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name,'')=ANY(p.allowed_entities))
		GROUP BY COALESCE(ot.scheme_id,ot.scheme_internal_code,ms.scheme_id::text), COALESCE(ms.scheme_name,ot.scheme_internal_code,ot.scheme_id::text), ms.amc_name
	),
	pv AS (
		SELECT COALESCE(ps.scheme_id,ps.isin,ps.scheme_name::text,'') AS scheme_ref, SUM(COALESCE(ps.current_value,0)) AS current_value
		FROM investment.portfolio_snapshot ps, params p
		WHERE (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
		GROUP BY COALESCE(ps.scheme_id,ps.isin,ps.scheme_name::text,'')
	),
	start_snap AS (
		SELECT DISTINCT ON (COALESCE(ps.scheme_id,ps.isin)) COALESCE(ps.scheme_id,ps.isin,ps.scheme_name::text) AS scheme_ref, COALESCE(ps.current_value,0)::numeric AS start_value
		FROM investment.portfolio_snapshot ps, params p
		WHERE ps.created_at <= p.fy_start
		  AND (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
		ORDER BY COALESCE(ps.scheme_id,ps.isin), ps.created_at DESC
	)
	SELECT COALESCE(t.scheme_name,'Unknown'),
	       (COALESCE(pv.current_value,0) - COALESCE(s.start_value,0) - (COALESCE(t.buys_since,0) - COALESCE(t.sells_since,0)))::float8,
	       t.amc_name
	FROM tx t LEFT JOIN pv ON pv.scheme_ref=t.scheme_ref LEFT JOIN start_snap s ON s.scheme_ref=t.scheme_ref, params p
	ORDER BY 2 DESC LIMIT $5
	`
	rows, err := pgxPool.Query(ctx, q, fyStart.Format(constants.DateFormat), nullIfEmpty(entityFilter), nil, allowedParam, limit)
	if err != nil {
		return nil, fmt.Errorf("entity performance: %w", err)
	}
	defer rows.Close()

	out := make([]EntityPerformanceRow, 0)
	for rows.Next() {
		var name, amc string
		var ytd float64
		if err := rows.Scan(&name, &ytd, &amc); err != nil {
			continue
		}
		out = append(out, EntityPerformanceRow{Label: name, YTD: ytd})
	}
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	rawTx := fetchRawTransactionDetails(ctx, pgxPool, entityFilter, allowedEntities, fyStart)
	return map[string]interface{}{
		"rows": out, "portfolio_detail": aumDetails, "transaction_detail": rawTx,
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computeAMCPerformance mirrors GetAMCPerformance business logic.
func computeAMCPerformance(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	q := `
	WITH params AS (SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities)
	SELECT COALESCE(ms.amc_name,'') AS amc_name,
	       SUM(COALESCE(ps.total_units::numeric,0)*COALESCE(ps.avg_nav::numeric,0))::float8,
	       SUM(COALESCE(ps.total_units::numeric,0)*COALESCE(ps.current_nav::numeric,0))::float8,
	       (SUM(COALESCE(ps.total_units::numeric,0)*COALESCE(ps.current_nav::numeric,0))-SUM(COALESCE(ps.total_units::numeric,0)*COALESCE(ps.avg_nav::numeric,0)))::float8,
	       CASE WHEN SUM(COALESCE(ps.total_units::numeric,0)*COALESCE(ps.avg_nav::numeric,0))=0 THEN 0
	            ELSE ((SUM(COALESCE(ps.total_units::numeric,0)*COALESCE(ps.current_nav::numeric,0))-SUM(COALESCE(ps.total_units::numeric,0)*COALESCE(ps.avg_nav::numeric,0)))/SUM(COALESCE(ps.total_units::numeric,0)*COALESCE(ps.avg_nav::numeric,0)))*100 END
	FROM investment.portfolio_snapshot ps
	LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ps.scheme_id)))), params p
	WHERE (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
	  AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
	GROUP BY COALESCE(ms.amc_name,'') ORDER BY 4 DESC
	`
	rows, err := pgxPool.Query(ctx, q, nullIfEmpty(entityFilter), allowedParam)
	if err != nil {
		return nil, fmt.Errorf("amc performance: %w", err)
	}
	defer rows.Close()

	out := make([]AMCPerfRow, 0)
	for rows.Next() {
		var r AMCPerfRow
		var pctRaw float64
		if err := rows.Scan(&r.AMCName, &r.StartValue, &r.CurrentValue, &r.PnL, &pctRaw); err != nil {
			continue
		}
		pct := pctRaw
		r.PnLPercent = &pct
		out = append(out, r)
	}
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"rows": out, "portfolio_detail": aumDetails,
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computeAMCWaterfall mirrors GetAMCWaterfall business logic.
func computeAMCWaterfall(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, periodStart, periodEnd string) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	q := `
	WITH params AS (SELECT $1::text AS entity_filter, $4::text[] AS allowed_entities),
	start_snap AS (
	  SELECT DISTINCT ON (COALESCE(ps.scheme_id,ps.isin)) COALESCE(ps.scheme_id,ps.isin)::text AS scheme_ref,
	         ps.total_units::numeric, ps.avg_nav::numeric, ps.current_nav::numeric, ps.current_value::numeric, ps.entity_name
	  FROM investment.portfolio_snapshot ps, params p
	  WHERE ps.created_at <= $2::date
	    AND (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
	    AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
	  ORDER BY COALESCE(ps.scheme_id,ps.isin), ps.created_at DESC
	), end_snap AS (
	  SELECT DISTINCT ON (COALESCE(ps.scheme_id,ps.isin)) COALESCE(ps.scheme_id,ps.isin)::text AS scheme_ref,
	         ps.total_units::numeric, ps.avg_nav::numeric, ps.current_nav::numeric, ps.current_value::numeric, ps.entity_name
	  FROM investment.portfolio_snapshot ps, params p
	  WHERE ps.created_at <= $3::date
	    AND (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
	    AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
	  ORDER BY COALESCE(ps.scheme_id,ps.isin), ps.created_at DESC
	), start_amc AS (
	  SELECT COALESCE(ms.amc_name,'') AS amc_name, SUM(COALESCE(s.total_units,0)*COALESCE(s.avg_nav,0))::numeric AS sv
	  FROM start_snap s LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(s.scheme_ref), '') IS NOT NULL AND ms.scheme_id::text = TRIM(s.scheme_ref)) OR (NULLIF(TRIM(s.scheme_ref), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(s.scheme_ref)) OR (NULLIF(TRIM(s.scheme_ref), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(s.scheme_ref))))
	  GROUP BY COALESCE(ms.amc_name,'')
	), end_amc AS (
	  SELECT COALESCE(ms.amc_name,'') AS amc_name, SUM(COALESCE(e.total_units,0)*COALESCE(e.current_nav,0))::numeric AS ev
	  FROM end_snap e LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(e.scheme_ref), '') IS NOT NULL AND ms.scheme_id::text = TRIM(e.scheme_ref)) OR (NULLIF(TRIM(e.scheme_ref), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(e.scheme_ref)) OR (NULLIF(TRIM(e.scheme_ref), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(e.scheme_ref))))
	  GROUP BY COALESCE(ms.amc_name,'')
	), amc_delta AS (
	  SELECT COALESCE(e.amc_name,s.amc_name) AS amc_name, COALESCE(s.sv,0)::float8 AS sv, COALESCE(e.ev,0)::float8 AS ev, (COALESCE(e.ev,0)-COALESCE(s.sv,0))::float8 AS delta
	  FROM start_amc s FULL OUTER JOIN end_amc e ON e.amc_name=s.amc_name
	), totals AS (SELECT SUM(sv)::float8 AS ot, SUM(ev)::float8 AS ct FROM amc_delta)
	SELECT ad.amc_name, ad.sv, ad.ev, ad.delta, t.ot, t.ct FROM amc_delta ad, totals t ORDER BY ad.delta DESC
	`
	rows, err := pgxPool.Query(ctx, q, nullIfEmpty(entityFilter), periodStart, periodEnd, allowedParam)
	if err != nil {
		return nil, fmt.Errorf("amc waterfall: %w", err)
	}
	defer rows.Close()

	type rowOut struct {
		AMCName               string
		sv, ev, delta, ot, ct float64
	}
	var amcRows []rowOut
	var openingTotal, closingTotal float64
	for rows.Next() {
		var rr rowOut
		if err := rows.Scan(&rr.AMCName, &rr.sv, &rr.ev, &rr.delta, &rr.ot, &rr.ct); err != nil {
			continue
		}
		openingTotal = rr.ot
		closingTotal = rr.ct
		amcRows = append(amcRows, rr)
	}
	if openingTotal == 0 {
		if t, err2 := time.Parse(constants.DateFormat, periodStart); err2 == nil {
			openingTotal = getAUMAtDate(ctx, pgxPool, entityFilter, allowedEntities, t)
		}
	}
	out := []WaterfallRow{{Label: "Opening AUM", OpeningAUM: &openingTotal}}
	for _, rr := range amcRows {
		d := rr.delta
		out = append(out, WaterfallRow{Label: rr.AMCName, Contribution: &d})
	}
	out = append(out, WaterfallRow{Label: "Closing AUM", ClosingAUM: &closingTotal})
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"rows": out, "opening_total": openingTotal, "closing_total": closingTotal,
		"portfolio_detail": aumDetails, "generated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computeAUMMovement mirrors GetAUMMovementWaterfall business logic.
func computeAUMMovement(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, periodStart, periodEnd string, now time.Time) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	periodStartT, _ := time.Parse(constants.DateFormat, periodStart)
	openingDate := periodStartT.AddDate(0, 0, -1).Format(constants.DateFormat)

	q := `
	WITH params AS (SELECT $1::text AS entity_filter, $2::date AS opening_date, $3::date AS period_start, $4::date AS period_end, $5::text[] AS allowed_entities),
	closing_aum AS (
		SELECT COALESCE(SUM(current_value),0)::float8 AS closing FROM investment.portfolio_snapshot ps, params p
		WHERE (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter) AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
	),
	units_at_start AS (
		SELECT COALESCE(ot.scheme_id,ot.scheme_internal_code) AS scheme_ref, ms.amfi_scheme_code,
		  SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest','idcw_reinvest') THEN COALESCE(ot.units,0)
		           WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') THEN -COALESCE(ot.units,0) ELSE 0 END) AS total_units
		FROM investment.approved_onboard_transaction ot
		LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))), params p
		WHERE ot.transaction_date <= p.opening_date
		  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name,'')=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name,'')=ANY(p.allowed_entities))
		GROUP BY COALESCE(ot.scheme_id,ot.scheme_internal_code), ms.amfi_scheme_code
		HAVING SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest','idcw_reinvest') THEN COALESCE(ot.units,0)
		               WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') THEN -COALESCE(ot.units,0) ELSE 0 END) > 0
	),
	navs_at_start AS (
		SELECT DISTINCT ON (scheme_code) scheme_code::text, nav_value FROM investment.amfi_nav_staging, params p
		WHERE nav_date <= p.opening_date ORDER BY scheme_code, nav_date DESC
	),
	opening_aum AS (
		SELECT COALESCE(SUM(u.total_units * COALESCE(n.nav_value,0)),0)::float8 AS opening
		FROM units_at_start u LEFT JOIN navs_at_start n ON n.scheme_code=u.amfi_scheme_code::text
	),
	period_flows AS (
		SELECT
		  COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('buy','purchase','subscription','switch_in') THEN amount ELSE 0 END),0)::float8 AS inflows,
		  COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('sell','redemption','switch_out') THEN amount ELSE 0 END),0)::float8 AS outflows,
		  COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('dividend','interest','dividend_payout','idcw','idcw_payout') THEN amount ELSE 0 END),0)::float8 AS income
		FROM investment.approved_onboard_transaction, params p
		WHERE transaction_date >= p.period_start AND transaction_date <= p.period_end
		  AND (p.entity_filter IS NULL OR COALESCE(entity_name,'')=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR COALESCE(entity_name,'')=ANY(p.allowed_entities))
	)
	SELECT o.opening, pf.inflows, pf.outflows, pf.income, c.closing
	FROM opening_aum o, period_flows pf, closing_aum c
	`
	var openingAUM, inflows, outflows, income, closingAUM float64
	err := pgxPool.QueryRow(ctx, q, nullIfEmpty(entityFilter), openingDate, periodStart, periodEnd, allowedParam).
		Scan(&openingAUM, &inflows, &outflows, &income, &closingAUM)
	if err != nil {
		return nil, fmt.Errorf("aum movement: %w", err)
	}
	marketGainsLosses := closingAUM - openingAUM - inflows + outflows - income
	mType := "positive"
	if marketGainsLosses < 0 {
		mType = "negative"
	}
	waterfall := []map[string]interface{}{
		{"label": "Opening Balance", "value": openingAUM, "type": "total"},
		{"label": "Inflows", "value": inflows, "type": "positive"},
		{"label": "Market Gains/Losses", "value": marketGainsLosses, "type": mType},
		{"label": "Income", "value": income, "type": "positive"},
		{"label": "Outflows", "value": outflows, "type": "negative"},
		{"label": "Closing Balance", "value": closingAUM, "type": "total"},
	}
	fyStart := getFinancialYearStart(now)
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	rawTx := fetchRawTransactionDetails(ctx, pgxPool, entityFilter, allowedEntities, fyStart)
	return map[string]interface{}{
		"opening": openingAUM, "inflows": inflows, "market_gains_losses": marketGainsLosses,
		"income": income, "outflows": outflows, "closing": closingAUM,
		"waterfall": waterfall, "portfolio_detail": aumDetails, "transaction_detail": rawTx,
		"period_start": periodStart, "period_end": periodEnd,
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computeAUMBreakdown mirrors GetAUMBreakdown business logic.
func computeAUMBreakdown(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, groupBy string) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	var query string
	var args []interface{}
	switch groupBy {
	case "scheme":
		query = `WITH params AS (SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities)
		SELECT COALESCE(ps.scheme_name,'Unknown'), SUM(COALESCE(ps.current_value,0))::float8
		FROM investment.portfolio_snapshot ps, params p
		WHERE COALESCE(ps.current_value,0) > 0 AND (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
		GROUP BY COALESCE(ps.scheme_name,'Unknown') ORDER BY 2 DESC`
		args = []interface{}{nullIfEmpty(entityFilter), allowedParam}
	case "entity":
		query = `WITH params AS (SELECT $1::text[] AS allowed_entities)
		SELECT COALESCE(ps.entity_name,'Unknown'), SUM(COALESCE(ps.current_value,0))::float8
		FROM investment.portfolio_snapshot ps, params p
		WHERE COALESCE(ps.current_value,0) > 0 AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
		GROUP BY COALESCE(ps.entity_name,'Unknown') ORDER BY 2 DESC`
		args = []interface{}{allowedParam}
	default:
		groupBy = "amc"
		query = `WITH params AS (SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities)
		SELECT COALESCE(ms.amc_name,'Unknown'), SUM(COALESCE(ps.current_value,0))::float8
		FROM investment.portfolio_snapshot ps
		LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ps.scheme_id)))), params p
		WHERE COALESCE(ps.current_value,0) > 0 AND (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
		GROUP BY COALESCE(ms.amc_name,'Unknown') ORDER BY 2 DESC`
		args = []interface{}{nullIfEmpty(entityFilter), allowedParam}
	}
	rows, err := pgxPool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("aum breakdown: %w", err)
	}
	defer rows.Close()

	var total float64
	breakdown := make([]AUMBreakdownItem, 0)
	for rows.Next() {
		var item AUMBreakdownItem
		if err := rows.Scan(&item.Label, &item.Amount); err != nil {
			continue
		}
		breakdown = append(breakdown, item)
		total += item.Amount
	}
	out := make([]map[string]interface{}, 0, len(breakdown))
	for _, item := range breakdown {
		pct := 0.0
		if total > 0 {
			pct = math.Round((item.Amount/total)*10000) / 100
		}
		out = append(out, map[string]interface{}{"label": item.Label, "amount": item.Amount, "percentage": pct})
	}
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"breakdown": out, "total": total, "group_by": groupBy,
		"portfolio_detail": aumDetails, "generated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computePerformanceAttribution mirrors GetPerformanceAttribution business logic.
func computePerformanceAttribution(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, year int, benchmark string, now time.Time) (interface{}, error) {
	fyStart := time.Date(year, time.April, 1, 0, 0, 0, 0, time.UTC)
	fyEnd := time.Date(year+1, time.March, 31, 23, 59, 59, 0, time.UTC)
	if fyEnd.After(now) {
		fyEnd = now
	}

	// Portfolio return via XIRR
	var terminalValue float64
	_ = pgxPool.QueryRow(ctx, `SELECT COALESCE(SUM(current_value),0) FROM investment.portfolio_snapshot WHERE ($1::text IS NULL OR entity_name=$1) AND ($2::text[] IS NULL OR entity_name=ANY($2))`, nullIfEmpty(entityFilter), allowedEntities).Scan(&terminalValue)
	portfolioXIRR := calculatePortfolioXIRR(ctx, pgxPool, entityFilter, allowedEntities, terminalValue, fyEnd)
	portfolioReturn := clampXIRRPercent(portfolioXIRR)

	benchmarkWeights := map[string]float64{"Equity Scheme": 60, "Debt Scheme": 25, "Hybrid Scheme": 10, "Solution Oriented": 3, "Other": 2}
	benchmarkReturns := map[string]float64{"Equity Scheme": 12, "Debt Scheme": 7, "Hybrid Scheme": 9, "Solution Oriented": 8, "Other": 6}

	benchmarkReturn := 0.0
	totalBW := 0.0
	for cat, wt := range benchmarkWeights {
		if ret, ok := benchmarkReturns[cat]; ok {
			benchmarkReturn += wt * ret / 100
			totalBW += wt
		}
	}
	if totalBW > 0 {
		benchmarkReturn = math.Round((benchmarkReturn/totalBW)*10000) / 100
	}

	allocationEffect := 0.0
	selectionEffect := 0.0
	otherEffects := math.Round((portfolioReturn-benchmarkReturn-allocationEffect-selectionEffect)*100) / 100

	attribution := []AttributionItem{
		{Name: "Benchmark Return", Value: benchmarkReturn},
		{Name: "Allocation Effect", Value: allocationEffect},
		{Name: "Selection Effect", Value: selectionEffect},
		{Name: "Other Effects", Value: otherEffects},
		{Name: "Portfolio Return", Value: portfolioReturn},
	}
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"attribution": attribution, "benchmark": benchmark,
		"benchmark_return": benchmarkReturn, "portfolio_return": portfolioReturn,
		"excess_return":     math.Round((portfolioReturn-benchmarkReturn)*100) / 100,
		"allocation_effect": allocationEffect, "selection_effect": selectionEffect,
		"other_effects": otherEffects, "portfolio_detail": aumDetails,
		"financial_year": fmt.Sprintf(constants.FormatFiscalYear, year, year+1),
		"period_start":   fyStart.Format(constants.DateFormat), "period_end": fyEnd.Format(constants.DateFormat),
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computePnLHeatmap mirrors GetDailyPnLHeatmap business logic.
func computePnLHeatmap(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, groupBy string) (interface{}, error) {
	byScheme := groupBy == "scheme"
	if !byScheme {
		groupBy = "amc"
	}

	pnlRows, err := portfolio.QueryHoldingsPnLHeatmap(ctx, pgxPool, entityFilter, allowedEntities, byScheme)
	if err != nil {
		return nil, fmt.Errorf("pnl heatmap: %w", err)
	}

	heatmap := make([]HeatmapCell, 0, len(pnlRows))
	entities := map[string]bool{}
	amcs := map[string]bool{}
	var totalPnL, maxProfit, maxLoss float64
	profitCount, lossCount := 0, 0
	for _, row := range pnlRows {
		cell := HeatmapCell{
			Entity: row.Entity,
			AMC:    row.AMC,
			Scheme: row.Scheme,
			PnL:    row.UnrealizedPnL,
		}
		heatmap = append(heatmap, cell)
		entities[cell.Entity] = true
		amcs[cell.AMC] = true
		totalPnL += cell.PnL
		if cell.PnL > 0 {
			profitCount++
			if cell.PnL > maxProfit {
				maxProfit = cell.PnL
			}
		} else if cell.PnL < 0 {
			lossCount++
			if cell.PnL < maxLoss {
				maxLoss = cell.PnL
			}
		}
	}
	entityList := make([]string, 0, len(entities))
	for e := range entities {
		entityList = append(entityList, e)
	}
	amcList := make([]string, 0, len(amcs))
	for a := range amcs {
		amcList = append(amcList, a)
	}
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"heatmap": heatmap, "entities": entityList, "amcs": amcList, "group_by": groupBy,
		"portfolio_detail": aumDetails,
		"summary": map[string]interface{}{
			"total_pnl": totalPnL, "profit_cells": profitCount, "loss_cells": lossCount,
			"max_profit": maxProfit, "max_loss": maxLoss, "total_cells": len(heatmap),
			"unique_entities": len(entities), "unique_amcs": len(amcs),
		},
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computePortfolioVsBenchmark mirrors GetPortfolioVsBenchmark business logic.
func computePortfolioVsBenchmark(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, year int, benchmark string, now time.Time) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	monthNames := []string{"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"}
	monthDates := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		var ms time.Time
		if i < 9 {
			ms = time.Date(year, time.Month(4+i), 1, 0, 0, 0, 0, time.UTC)
		} else {
			ms = time.Date(year+1, time.Month(i-8), 1, 0, 0, 0, 0, time.UTC)
		}
		me := ms.AddDate(0, 1, -1)
		if ms.After(now) {
			break
		}
		if me.After(now) {
			me = now
		}
		monthDates = append(monthDates, me.Format(constants.DateFormat))
	}
	if len(monthDates) == 0 {
		return map[string]interface{}{
			"series":         []BenchmarkPoint{{Month: "Apr", Portfolio: 100, Benchmark: 100}},
			"benchmark_name": benchmark, "generated_at": time.Now().UTC().Format(time.RFC3339),
		}, nil
	}
	batchQ := `
	WITH params AS (SELECT $1::text AS entity_filter, $3::text[] AS allowed_entities),
	month_dates AS (SELECT ordinality AS month_idx, month_end::date FROM UNNEST($2::date[]) WITH ORDINALITY AS t(month_end, ordinality)),
	monthly_invested AS (
		SELECT md.month_idx,
		       COALESCE(SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','sip','switch_in') THEN ABS(COALESCE(ot.amount,0))
		                        WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -ABS(COALESCE(ot.amount,0)) ELSE 0 END),0)::float8 AS invested
		FROM month_dates md LEFT JOIN investment.approved_onboard_transaction ot ON ot.transaction_date <= md.month_end, params p
		WHERE (p.entity_filter IS NULL OR COALESCE(ot.entity_name,'')=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name,'')=ANY(p.allowed_entities))
		GROUP BY md.month_idx
	),
	current_value AS (
		SELECT COALESCE(SUM(current_value),0)::float8 AS value FROM investment.portfolio_snapshot ps, params p
		WHERE (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
	)
	SELECT mi.month_idx, mi.invested, cv.value FROM monthly_invested mi, current_value cv ORDER BY mi.month_idx
	`
	rows, err := pgxPool.Query(ctx, batchQ, nullIfEmpty(entityFilter), monthDates, allowedParam)
	if err != nil {
		return nil, fmt.Errorf("portfolio vs benchmark: %w", err)
	}
	defer rows.Close()

	monthlyInvested := map[int]float64{}
	var currentValue float64
	for rows.Next() {
		var idx int
		var inv, cv float64
		if err := rows.Scan(&idx, &inv, &cv); err != nil {
			continue
		}
		monthlyInvested[idx] = inv
		currentValue = cv
	}

	benchmarkMonthlyReturns := map[string][]float64{
		constants.Nifty50: {0.8, 1.0, 0.6, 0.9, 1.1, 0.7, 0.5, 0.8, 1.2, 0.9, 0.6, 0.8},
		"NIFTY 100":       {0.7, 0.9, 0.5, 0.8, 1.0, 0.6, 0.4, 0.7, 1.1, 0.8, 0.5, 0.7},
		"SENSEX":          {0.75, 0.95, 0.55, 0.85, 1.05, 0.65, 0.45, 0.75, 1.15, 0.85, 0.55, 0.75},
	}
	monthlyReturns := benchmarkMonthlyReturns[benchmark]
	if monthlyReturns == nil {
		monthlyReturns = benchmarkMonthlyReturns[constants.Nifty50]
	}

	points := make([]BenchmarkPoint, 0, len(monthDates))
	portfolioIndexed := 100.0
	benchmarkIndexed := 100.0
	for i := 0; i < len(monthDates) && i < len(monthNames); i++ {
		if i == 0 {
			points = append(points, BenchmarkPoint{Month: monthNames[i], Portfolio: 100.0, Benchmark: 100.0})
			continue
		}
		invested := monthlyInvested[i+1]
		if invested > 0 && currentValue > 0 {
			totalReturn := (currentValue - invested) / invested
			monthlyReturn := totalReturn / float64(len(monthDates)) * 100
			portfolioIndexed = portfolioIndexed * (1 + monthlyReturn/100)
		}
		benchmarkIndexed = benchmarkIndexed * (1 + monthlyReturns[i%12]/100)
		points = append(points, BenchmarkPoint{
			Month:     monthNames[i],
			Portfolio: math.Round(portfolioIndexed*100) / 100,
			Benchmark: math.Round(benchmarkIndexed*100) / 100,
		})
	}
	if len(points) == 0 {
		points = append(points, BenchmarkPoint{Month: "Apr", Portfolio: 100, Benchmark: 100})
	}
	latest := points[len(points)-1]
	pRet := math.Round((latest.Portfolio-100)*100) / 100
	bRet := math.Round((latest.Benchmark-100)*100) / 100
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"series": points, "benchmark_name": benchmark, "portfolio_detail": aumDetails,
		"summary": map[string]interface{}{
			"portfolio_return": pRet, "benchmark_return": bRet,
			"alpha": math.Round((pRet-bRet)*100) / 100, "outperforming": pRet > bRet,
		},
		"financial_year": fmt.Sprintf(constants.FormatFiscalYear, year, year+1),
		"generated_at":   time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computeMarketTicker mirrors GetMarketRatesTicker business logic.
func computeMarketTicker(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, limit int) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	if limit <= 0 {
		limit = 20
	}
	query := `
	WITH params AS (SELECT $1::text AS entity_filter, $3::text[] AS allowed_entities),
	holdings AS (
		SELECT ps.scheme_name, ps.scheme_id, ps.isin, ps.current_nav, ps.gain_loss, ps.total_units, ps.current_value,
		       COALESCE(ms.amc_name,'') AS amc_name, COALESCE(ms.internal_scheme_code,'') AS internal_code,
		       COALESCE(ms.amfi_scheme_code::text,'') AS amfi_code
		FROM investment.portfolio_snapshot ps
		LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ps.scheme_id)))), params p
		WHERE COALESCE(ps.current_value,0) > 0 AND (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
		  AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
		ORDER BY ps.current_value DESC LIMIT $2
	),
	nav_data AS (
		SELECT h.*, COALESCE((SELECT nav_value FROM investment.amfi_nav_staging ans WHERE ans.scheme_code::text=h.amfi_code ORDER BY nav_date DESC LIMIT 1),h.current_nav)::float8 AS latest_nav,
		             COALESCE((SELECT nav_value FROM investment.amfi_nav_staging ans WHERE ans.scheme_code::text=h.amfi_code ORDER BY nav_date DESC LIMIT 1 OFFSET 1),0)::float8 AS prev_nav
		FROM holdings h
	)
	SELECT COALESCE(scheme_name,'Unknown'), COALESCE(amc_name,'Unknown'), COALESCE(amfi_code,''), COALESCE(internal_code,''), COALESCE(isin,''),
	       latest_nav, prev_nav,
	       CASE WHEN prev_nav > 0 THEN ROUND(((latest_nav-prev_nav)/prev_nav*100)::numeric,2) ELSE 0 END,
	       COALESCE(gain_loss,0)::float8, COALESCE(total_units,0)::float8, COALESCE(current_value,0)::float8
	FROM nav_data
	`
	rows, err := pgxPool.Query(ctx, query, nullIfEmpty(entityFilter), limit, allowedParam)
	if err != nil {
		return nil, fmt.Errorf("market ticker: %w", err)
	}
	defer rows.Close()

	ticker := make([]MutualFundTickerRow, 0, limit)
	var totalValue, totalMTM float64
	gainers, losers := 0, 0
	for rows.Next() {
		var row MutualFundTickerRow
		if err := rows.Scan(&row.SchemeName, &row.AMC, &row.AMFICode, &row.InternalCode, &row.ISIN,
			&row.NAV, &row.PrevNAV, &row.Change1D, &row.MTM, &row.Units, &row.Value); err != nil {
			continue
		}
		row.NAV = math.Round(row.NAV*100) / 100
		row.PrevNAV = math.Round(row.PrevNAV*100) / 100
		row.MTM = math.Round(row.MTM*100) / 100
		row.Units = math.Round(row.Units*1000) / 1000
		row.Value = math.Round(row.Value*100) / 100
		ticker = append(ticker, row)
		totalValue += row.Value
		totalMTM += row.MTM
		if row.Change1D > 0 {
			gainers++
		} else if row.Change1D < 0 {
			losers++
		}
	}
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"mutual_funds": ticker, "portfolio_detail": aumDetails,
		"summary": map[string]interface{}{
			"total_schemes": len(ticker), "total_value": math.Round(totalValue*100) / 100,
			"total_mtm": math.Round(totalMTM*100) / 100, "gainers": gainers, "losers": losers,
			"unchanged": len(ticker) - gainers - losers,
		},
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computeTopPerforming mirrors GetTopPerformingAssets business logic.
func computeTopPerforming(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, limit int) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	if limit <= 0 {
		limit = 3
	}
	q := `
	WITH params AS (SELECT $1::text AS entity_filter, $3::text[] AS allowed_entities)
	SELECT ps.scheme_name, COALESCE(ms.amc_name,''),
	       (ps.total_units::numeric*ps.avg_nav::numeric)::float8,
	       (ps.total_units::numeric*ps.current_nav::numeric)::float8,
	       CASE WHEN ps.avg_nav=0 THEN NULL ELSE (((ps.total_units*ps.current_nav)-(ps.total_units*ps.avg_nav))/NULLIF((ps.total_units*ps.avg_nav),0))*100 END
	FROM investment.portfolio_snapshot ps
	LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ps.scheme_id)))), params p
	WHERE (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter)
	  AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
	ORDER BY 5 DESC NULLS LAST LIMIT $2
	`
	rows, err := pgxPool.Query(ctx, q, nullIfEmpty(entityFilter), limit, allowedParam)
	if err != nil {
		return nil, fmt.Errorf("top performing: %w", err)
	}
	defer rows.Close()

	out := make([]TopAssetRow, 0, limit)
	for rows.Next() {
		var name, amc string
		var startV, endV float64
		var pctRaw *float64
		if err := rows.Scan(&name, &amc, &startV, &endV, &pctRaw); err != nil {
			continue
		}
		pctStr := "0%"
		if pctRaw != nil {
			pctStr = formatPercent(*pctRaw)
		}
		out = append(out, TopAssetRow{Title: name, Subtitle: amc, Pct: pctStr, Value: endV})
	}
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"rows": out, "portfolio_detail": aumDetails,
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// computeAUMCompositionTrend mirrors GetAUMCompositionTrend business logic.
func computeAUMCompositionTrend(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, year int, now time.Time) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	fyMonths := []time.Month{time.April, time.May, time.June, time.July, time.August, time.September, time.October, time.November, time.December, time.January, time.February, time.March}
	monthLabels := []string{"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"}
	type monthInfo struct {
		Label    string
		MonthIdx int
		EndDate  string
	}
	months := make([]monthInfo, 0, 12)
	monthDates := make([]string, 0, 12)
	for i, m := range fyMonths {
		yr := year
		if i >= 9 {
			yr = year + 1
		}
		firstOfMonth := time.Date(yr, m, 1, 0, 0, 0, 0, time.UTC)
		lastOfMonth := firstOfMonth.AddDate(0, 1, -1)
		if lastOfMonth.After(now) {
			if !firstOfMonth.After(now) {
				months = append(months, monthInfo{monthLabels[i], i, now.Format(constants.DateFormat)})
				monthDates = append(monthDates, now.Format(constants.DateFormat))
			}
			continue
		}
		months = append(months, monthInfo{monthLabels[i], i, lastOfMonth.Format(constants.DateFormat)})
		monthDates = append(monthDates, lastOfMonth.Format(constants.DateFormat))
	}
	if len(months) == 0 {
		return map[string]interface{}{
			"rows": map[string]interface{}{"rows": []interface{}{}, "amc_names": []string{},
				"fy_label": fmt.Sprintf(constants.FormatFiscalYear, year, (year+1)%100), "generated_at": time.Now().UTC().Format(time.RFC3339)},
			"success": true,
		}, nil
	}

	batchQuery := `
	WITH params AS (SELECT $1::text AS entity_filter, $3::text[] AS allowed_entities),
	month_dates AS (SELECT ordinality-1 AS month_idx, month_end::date FROM UNNEST($2::date[]) WITH ORDINALITY AS t(month_end, ordinality)),
	current_snapshot AS (
		SELECT COALESCE(ms.amc_name,'Unknown') AS amc_name, SUM(COALESCE(ps.current_value,0))::float8 AS aum_value
		FROM investment.portfolio_snapshot ps
		LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ps.scheme_id)))), params p
		WHERE (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter) AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
		GROUP BY COALESCE(ms.amc_name,'Unknown')
	),
	historical_aum AS (
		SELECT md.month_idx, COALESCE(net_scheme.amc_name,'Unknown') AS amc_name,
		       SUM(net_scheme.net_units * COALESCE(nav.nav_value, net_scheme.avg_nav, 10))::float8 AS aum_value
		FROM month_dates md
		CROSS JOIN LATERAL (
			SELECT COALESCE(ms.amc_name,'Unknown') AS amc_name, COALESCE(ms.amfi_scheme_code::text,'') AS amfi_scheme_code,
			  SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest') THEN COALESCE(ot.units,0)
			           WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') THEN -COALESCE(ot.units,0) ELSE 0 END) AS net_units,
			  CASE WHEN SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest') THEN COALESCE(ot.units,0) ELSE 0 END) > 0
			    THEN SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest') THEN COALESCE(ot.units,0)*COALESCE(ot.nav,0) ELSE 0 END)
			         /SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest') THEN COALESCE(ot.units,0) ELSE 0 END)
			    ELSE 0 END AS avg_nav
			FROM investment.approved_onboard_transaction ot
			LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ot.scheme_id)) OR (NULLIF(TRIM(ot.scheme_internal_code), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ot.scheme_internal_code)) OR (NULLIF(TRIM(ot.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ot.scheme_id)))), params p
			WHERE ot.transaction_date <= md.month_end
			  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name,'')=p.entity_filter)
			  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name,'')=ANY(p.allowed_entities))
			GROUP BY COALESCE(ms.amc_name,'Unknown'), COALESCE(ms.amfi_scheme_code::text,'')
			HAVING SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest') THEN COALESCE(ot.units,0)
			               WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') THEN -COALESCE(ot.units,0) ELSE 0 END) > 0
		) net_scheme
		LEFT JOIN LATERAL (
			SELECT nav_value FROM investment.amfi_nav_staging WHERE scheme_code::text=net_scheme.amfi_scheme_code AND nav_date <= md.month_end ORDER BY nav_date DESC LIMIT 1
		) nav ON true
		WHERE md.month_idx < (SELECT MAX(month_idx) FROM month_dates)
		GROUP BY md.month_idx, COALESCE(net_scheme.amc_name,'Unknown')
	),
	combined AS (
		SELECT month_idx, amc_name, aum_value FROM historical_aum
		UNION ALL
		SELECT (SELECT MAX(month_idx) FROM month_dates), amc_name, aum_value FROM current_snapshot
	)
	SELECT month_idx, amc_name, COALESCE(SUM(aum_value),0)::float8 FROM combined GROUP BY month_idx, amc_name ORDER BY month_idx, amc_name
	`
	rows, err := pgxPool.Query(ctx, batchQuery, nullIfEmpty(entityFilter), monthDates, allowedParam)
	if err != nil {
		return nil, fmt.Errorf("aum composition trend: %w", err)
	}
	defer rows.Close()

	amcSet := map[string]bool{}
	resultMap := map[int]map[string]float64{}
	for _, m := range months {
		resultMap[m.MonthIdx] = map[string]float64{}
	}
	for rows.Next() {
		var monthIdx int
		var amcName string
		var aumValue float64
		if err := rows.Scan(&monthIdx, &amcName, &aumValue); err != nil {
			continue
		}
		if _, ok := resultMap[monthIdx]; ok {
			resultMap[monthIdx][amcName] = aumValue
		}
		amcSet[amcName] = true
	}
	amcNames := make([]string, 0, len(amcSet))
	for amc := range amcSet {
		amcNames = append(amcNames, amc)
	}
	sort.Strings(amcNames)

	outRows := make([]map[string]interface{}, 0, len(months))
	for _, m := range months {
		row := map[string]interface{}{"month": m.Label}
		for _, amc := range amcNames {
			if val, ok := resultMap[m.MonthIdx][amc]; ok {
				row[amc] = val
			} else {
				row[amc] = 0.0
			}
		}
		outRows = append(outRows, row)
	}
	aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"rows": map[string]interface{}{
			"amc_names":        amcNames,
			"fy_label":         fmt.Sprintf(constants.FormatFiscalYear, year, (year+1)%100),
			"generated_at":     time.Now().UTC().Format(time.RFC3339),
			"portfolio_detail": aumDetails,
			"rows":             outRows,
		},
		"success": true,
	}, nil
}

// computeConsolidatedRisk mirrors GetConsolidatedRisk business logic.
func computeConsolidatedRisk(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	q := `
	WITH params AS (SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities)
	SELECT
	  COALESCE(SUM(val*(CASE LOWER(COALESCE(ms.internal_risk_rating,'medium')) WHEN 'low' THEN 15 WHEN 'medium' THEN 50 WHEN 'high' THEN 85 ELSE 50 END))/NULLIF(SUM(val),0),0)::float8,
	  COALESCE(SUM(val),0)::float8,
	  COALESCE(SUM(CASE WHEN LOWER(COALESCE(ms.internal_risk_rating,'medium'))='low' THEN val ELSE 0 END),0)::float8,
	  COALESCE(SUM(CASE WHEN LOWER(COALESCE(ms.internal_risk_rating,'medium'))='medium' THEN val ELSE 0 END),0)::float8,
	  COALESCE(SUM(CASE WHEN LOWER(COALESCE(ms.internal_risk_rating,'medium'))='high' THEN val ELSE 0 END),0)::float8
	FROM (
	  SELECT COALESCE(ps.current_value::numeric,(ps.total_units::numeric*ps.current_nav::numeric),0) AS val, ps.scheme_id, ps.isin, ps.entity_name
	  FROM investment.portfolio_snapshot ps, params p
	  WHERE (p.entity_filter IS NULL OR ps.entity_name=p.entity_filter) AND (p.allowed_entities IS NULL OR ps.entity_name=ANY(p.allowed_entities))
	) ps
	LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.scheme_id::text = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(ps.scheme_id)) OR (NULLIF(TRIM(ps.scheme_id), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(ps.scheme_id))))
	`
	var lcr, totalValue, lowValue, mediumValue, highValue float64
	if err := pgxPool.QueryRow(ctx, q, nullIfEmpty(entityFilter), allowedParam).Scan(&lcr, &totalValue, &lowValue, &mediumValue, &highValue); err != nil {
		return nil, fmt.Errorf("consolidated risk: %w", err)
	}
	aumDetails := fetchAUMDetailsWithRisk(ctx, pgxPool, entityFilter, allowedEntities)
	return map[string]interface{}{
		"rows": map[string]interface{}{
			"generated_at": time.Now().UTC().Format(time.RFC3339),
			"high_value":   highValue, "lcr": lcr, "low_value": lowValue,
			"medium_value": mediumValue, "portfolio_detail": aumDetails, "total_value": totalValue,
		},
		"success": true,
	}, nil
}

// computeCombinedOverview computes the lightweight combined snapshot (equivalent to
// the /dash/investment/overview/combined endpoint but as a pure function for reuse
// inside the portfolio dashboard goroutine).
func computeCombinedOverview(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, limit int, now time.Time) (interface{}, error) {
	var allowedParam interface{}
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}

	// Use the same netting query as the fixed combinedOverview.go
	netHoldingsQuery := `
	WITH params AS (SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities),
	latest_nav AS (SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_code, nav_value FROM investment.amfi_nav_staging ORDER BY scheme_code, nav_date DESC),
	net_holdings AS (
	  SELECT COALESCE(ot.entity_name,'') AS entity_name, COALESCE(ot.scheme_id,ot.scheme_internal_code)::text AS scheme_ref, ot.folio_number,
	    SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest','merger_in') THEN COALESCE(ot.units,0)
	             WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') THEN -COALESCE(ot.units,0) ELSE 0 END) AS net_units,
	    CASE WHEN SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest','merger_in') THEN COALESCE(ot.units,0) ELSE 0 END) > 0
	      THEN SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest','merger_in') THEN COALESCE(ot.units,0)*COALESCE(ot.nav,0) ELSE 0 END)
	           /SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest','merger_in') THEN COALESCE(ot.units,0) ELSE 0 END)
	      ELSE 0 END AS avg_purchase_nav,
	    SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest','merger_in') THEN COALESCE(ABS(ot.amount),0) ELSE 0 END) -
	    SUM(CASE WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') THEN COALESCE(ABS(ot.amount),0) ELSE 0 END) AS invested_amount
	  FROM investment.approved_onboard_transaction ot, params p
	  WHERE ($1::text IS NULL OR COALESCE(ot.entity_name,'')=$1) AND ($2::text[] IS NULL OR COALESCE(ot.entity_name,'')=ANY($2))
	  GROUP BY COALESCE(ot.entity_name,''), COALESCE(ot.scheme_id,ot.scheme_internal_code)::text, ot.folio_number
	  HAVING SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest','merger_in') THEN COALESCE(ot.units,0)
	                  WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') THEN -COALESCE(ot.units,0) ELSE 0 END) > 0
	)
	SELECT nh.entity_name, COALESCE(ms.amc_name,'Unknown'), COALESCE(ms.scheme_name,nh.scheme_ref) AS scheme_name, nh.scheme_ref,
	       nh.net_units::float8, COALESCE(ln.nav_value,0)::float8, (nh.net_units*COALESCE(ln.nav_value,0))::float8, COALESCE(ln.nav_value,0)::float8,
	       COALESCE(ms.amfi_scheme_code::text,''), COALESCE(nh.folio_number,''),
	       ((nh.net_units*COALESCE(ln.nav_value,0))-GREATEST(nh.invested_amount,0))::float8,
	       CASE WHEN nh.invested_amount>0 THEN (((nh.net_units*COALESCE(ln.nav_value,0))-nh.invested_amount)/nh.invested_amount*100)::float8 ELSE 0 END,
	       COALESCE(INITCAP(ms.internal_risk_rating),'Medium'),
	       CASE LOWER(COALESCE(ms.internal_risk_rating,'medium')) WHEN 'low' THEN 1 WHEN 'medium' THEN 2 WHEN 'high' THEN 3 ELSE 2 END,
	       GREATEST(nh.invested_amount,0)::float8, COALESCE(ms.isin,''), nh.avg_purchase_nav::float8,
	       COALESCE(asm.scheme_category,'Other'), COALESCE(asm.scheme_sub_category,'Other'), COALESCE(asm.scheme_type,'Other')
	FROM net_holdings nh
	LEFT JOIN investment.masterscheme ms ON (COALESCE(ms.is_deleted, false) = false AND ((NULLIF(TRIM(nh.scheme_ref), '') IS NOT NULL AND ms.scheme_id::text = TRIM(nh.scheme_ref)) OR (NULLIF(TRIM(nh.scheme_ref), '') IS NOT NULL AND ms.internal_scheme_code = TRIM(nh.scheme_ref)) OR (NULLIF(TRIM(nh.scheme_ref), '') IS NOT NULL AND ms.amfi_scheme_code = TRIM(nh.scheme_ref))))
	LEFT JOIN investment.amfi_scheme_master_staging asm ON asm.scheme_code::text=ms.amfi_scheme_code::text
	LEFT JOIN latest_nav ln ON ln.scheme_code=COALESCE(ms.amfi_scheme_code::text,asm.scheme_code::text)
	`

	rows, err := pgxPool.Query(ctx, netHoldingsQuery, nullIfEmpty(entityFilter), allowedParam)
	if err != nil {
		return nil, fmt.Errorf("combined overview: %w", err)
	}
	defer rows.Close()

	type holdingRow struct {
		EntityName, AmcName, SchemeName, SchemeID, AmfiSchemeCode, FolioNumber, RiskRating    string
		Units, Nav, CurrentValue, AmfiNav, GainLoss, GainLossPct, InvestedAmount, PurchaseNav float64
		InternalRiskScore                                                                     int
		Isin, SchemeCategory, SchemeSubCategory, SchemeType                                   string
	}
	portfolio := make([]holdingRow, 0)
	var totalAUM float64
	for rows.Next() {
		var h holdingRow
		if err := rows.Scan(&h.EntityName, &h.AmcName, &h.SchemeName, &h.SchemeID, &h.Units, &h.Nav, &h.CurrentValue, &h.AmfiNav,
			&h.AmfiSchemeCode, &h.FolioNumber, &h.GainLoss, &h.GainLossPct, &h.RiskRating, &h.InternalRiskScore,
			&h.InvestedAmount, &h.Isin, &h.PurchaseNav, &h.SchemeCategory, &h.SchemeSubCategory, &h.SchemeType); err != nil {
			continue
		}
		portfolio = append(portfolio, h)
		totalAUM += h.CurrentValue
	}

	riskBuckets := map[int]float64{1: 15, 2: 50, 3: 85}
	var weighted, lowValue, mediumValue, highValue float64
	for _, h := range portfolio {
		score := riskBuckets[h.InternalRiskScore]
		weighted += h.CurrentValue * score
		switch h.InternalRiskScore {
		case 1:
			lowValue += h.CurrentValue
		case 2:
			mediumValue += h.CurrentValue
		case 3:
			highValue += h.CurrentValue
		}
	}
	lcr := 0.0
	if totalAUM > 0 {
		lcr = weighted / totalAUM
	}

	type aggRow struct {
		Title, Subtitle string
		Value           float64
		weightedPct     float64
	}
	agg := map[string]*aggRow{}
	for _, p := range portfolio {
		key := p.SchemeName + "||" + p.AmcName
		a, ok := agg[key]
		if !ok {
			a = &aggRow{Title: p.SchemeName, Subtitle: p.AmcName}
			agg[key] = a
		}
		a.Value += p.CurrentValue
		a.weightedPct += p.GainLossPct * p.CurrentValue
	}
	aggs := make([]*aggRow, 0, len(agg))
	for _, v := range agg {
		aggs = append(aggs, v)
	}
	sort.Slice(aggs, func(i, j int) bool { return aggs[i].Value > aggs[j].Value })
	topRows := make([]TopAssetRow, 0, limit)
	for i := 0; i < len(aggs) && i < limit; i++ {
		a := aggs[i]
		pct := 0.0
		if a.Value > 0 {
			pct = a.weightedPct / a.Value
		}
		topRows = append(topRows, TopAssetRow{Title: a.Title, Subtitle: a.Subtitle, Pct: formatPercent(pct), Value: a.Value})
	}

	aumByAMC := map[string]float64{}
	for _, h := range portfolio {
		aumByAMC[h.AmcName] += h.CurrentValue
	}
	aumRows := make([]map[string]interface{}, 0, len(aumByAMC))
	amcNames := make([]string, 0, len(aumByAMC))
	for amc, v := range aumByAMC {
		aumRows = append(aumRows, map[string]interface{}{"label": amc, "amount": v})
		amcNames = append(amcNames, amc)
	}

	ticker := make([]MutualFundTickerRow, 0, len(portfolio))
	for _, h := range portfolio {
		ticker = append(ticker, MutualFundTickerRow{SchemeName: h.SchemeName, AMC: h.AmcName, AMFICode: h.AmfiSchemeCode, InternalCode: h.SchemeID, ISIN: h.Isin, NAV: h.Nav, Units: h.Units, Value: h.CurrentValue})
	}

	portfolioDetail := make([]InvestmentDetail, 0, len(portfolio))
	for _, h := range portfolio {
		portfolioDetail = append(portfolioDetail, InvestmentDetail{
			EntityName: h.EntityName, AMCName: h.AmcName, SchemeName: h.SchemeName, SchemeID: h.SchemeID,
			AMFISchemeCode: h.AmfiSchemeCode, SchemeCategory: h.SchemeCategory, SchemeSubCategory: h.SchemeSubCategory,
			SchemeType: h.SchemeType, FolioNumber: h.FolioNumber, ISIN: h.Isin,
			Units: h.Units, NAV: h.Nav, AMFINAV: h.AmfiNav, PurchaseNAV: h.PurchaseNav,
			InvestedAmount: h.InvestedAmount, CurrentValue: h.CurrentValue, GainLoss: h.GainLoss,
			GainLossPct: h.GainLossPct, RiskRating: h.RiskRating, InternalRiskScore: h.InternalRiskScore,
		})
	}

	fyStart := getFinancialYearStart(now)
	transactionDetail := fetchRawTransactionDetails(ctx, pgxPool, entityFilter, allowedEntities, fyStart)

	return map[string]interface{}{
		"generated_at": time.Now().UTC().Format(time.RFC3339),
		"kpis": map[string]interface{}{
			"aum_detail": portfolioDetail, "transaction_detail": transactionDetail,
		},
		"consolidated": map[string]interface{}{
			"generated_at": time.Now().UTC().Format(time.RFC3339),
			"high_value":   highValue, "lcr": lcr, "low_value": lowValue,
			"medium_value": mediumValue, "portfolio_detail": portfolioDetail, "total_value": totalAUM,
		},
		"top": map[string]interface{}{"portfolio_detail": portfolioDetail, "rows": topRows, "generated_at": time.Now().UTC().Format(time.RFC3339)},
		"aum": map[string]interface{}{
			"amc_names": amcNames, "fy_label": fmt.Sprintf("FY %d-%d", fyStart.Year(), fyStart.Year()+1),
			"generated_at": time.Now().UTC().Format(time.RFC3339), "portfolio_detail": portfolioDetail, "rows": aumRows,
		},
		"market": map[string]interface{}{
			"mutual_funds": ticker, "portfolio_detail": portfolioDetail,
			"summary":      map[string]interface{}{"total_schemes": len(ticker), "total_value": totalAUM},
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		},
	}, nil
}
