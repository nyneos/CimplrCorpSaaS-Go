package investmentdashboards

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Optimized combined endpoint: single recompute (two DB queries) to derive
// KPIs, consolidated risk, top performers, AUM composition and market ticker.
func GetCombinedInvestmentOverview(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		ctx := r.Context()

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Limit      int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		var allowedEntities interface{} = nil
		allowed := api.GetEntityNamesFromCtx(ctx)
		if len(allowed) > 0 {
			allowedEntities = allowed
		}
		if req.Limit <= 0 {
			req.Limit = 3
		}

		// Query A: get all holdings (no aggregation)
		masterPortfolioQuery := `
		WITH params AS (
		  SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities
		), latest_nav AS (
		  SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_code, nav_value
		  FROM investment.amfi_nav_staging
		  ORDER BY scheme_code, nav_date DESC
		)
		SELECT
			ot.entity_name,
			COALESCE(ms.amc_name,'Unknown') AS amc_name,
			COALESCE(ms.scheme_name, ot.scheme_internal_code, ot.scheme_id::text) AS scheme_name,
			COALESCE(ot.scheme_id, ot.scheme_internal_code)::text AS scheme_id,
			COALESCE(ot.units,0)::float8 AS units,
			COALESCE(ln.nav_value,0)::float8 AS nav,
			(COALESCE(ot.units,0) * COALESCE(ln.nav_value,0))::float8 AS current_value,
			COALESCE(ln.nav_value,0)::float8 AS amfi_nav,
			ms.amfi_scheme_code::text AS amfi_scheme_code,
			ot.folio_number,
			(COALESCE(ot.units,0) * COALESCE(ln.nav_value,0) - COALESCE(ot.amount,0))::float8 AS gain_loss,
			CASE WHEN COALESCE(ot.amount,0) > 0 THEN ((COALESCE(ot.units,0) * COALESCE(ln.nav_value,0) - COALESCE(ot.amount,0)) / COALESCE(ot.amount,0) * 100)::float8 ELSE 0 END AS gain_loss_pct,
			COALESCE(INITCAP(ms.internal_risk_rating), 'Medium') AS risk_rating,
			CASE LOWER(COALESCE(ms.internal_risk_rating, 'medium'))
				WHEN 'low' THEN 1
				WHEN 'medium' THEN 2
				WHEN 'high' THEN 3
				ELSE 2
			END AS internal_risk_score,
			COALESCE(ot.amount,0)::float8 AS invested_amount,
			ms.isin,
			COALESCE(ot.nav,0)::float8 AS purchase_nav,
			COALESCE(asm.scheme_category, 'Other') AS scheme_category,
			COALESCE(asm.scheme_sub_category, 'Other') AS scheme_sub_category,
			COALESCE(asm.scheme_type, 'Other') AS scheme_type
		FROM investment.onboard_transaction ot
		LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id)
		LEFT JOIN investment.amfi_scheme_master_staging asm ON asm.scheme_code::text = ms.amfi_scheme_code::text
		LEFT JOIN latest_nav ln ON ln.scheme_code = COALESCE(ms.amfi_scheme_code::text, asm.scheme_code::text)
		, params p
		WHERE ($1::text IS NULL OR COALESCE(ot.entity_name,'') = $1)
			AND ($2::text[] IS NULL OR COALESCE(ot.entity_name,'') = ANY($2))
			AND COALESCE(ot.units,0) > 0
		`

		rows, err := pgxPool.Query(ctx, masterPortfolioQuery, nullIfEmpty(entityFilter), allowedEntities)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "portfolio query failed: "+err.Error())
			return
		}
		defer rows.Close()

		type holdingRow struct {
			EntityName        string
			AmcName           string
			SchemeName        string
			SchemeID          string
			Units             float64
			Nav               float64
			CurrentValue      float64
			AmfiNav           float64
			AmfiSchemeCode    string
			FolioNumber       string
			GainLoss          float64
			GainLossPct       float64
			RiskRating        string
			InternalRiskScore int
			InvestedAmount    float64
			Isin              string
			PurchaseNav       float64
			SchemeCategory    string
			SchemeSubCategory string
			SchemeType        string
		}

		portfolio := make([]holdingRow, 0)
		var totalAUM float64
		for rows.Next() {
			var h holdingRow
			if err := rows.Scan(&h.EntityName, &h.AmcName, &h.SchemeName, &h.SchemeID, &h.Units, &h.Nav, &h.CurrentValue, &h.AmfiNav, &h.AmfiSchemeCode, &h.FolioNumber, &h.GainLoss, &h.GainLossPct, &h.RiskRating, &h.InternalRiskScore, &h.InvestedAmount, &h.Isin, &h.PurchaseNav, &h.SchemeCategory, &h.SchemeSubCategory, &h.SchemeType); err != nil {
				continue
			}
			portfolio = append(portfolio, h)
			totalAUM += h.CurrentValue
		}

		// Use the shared helper (same as individual handler) to fetch recent transactions
		// We keep the same fromDate as FY start to be consistent with KPIs
		fromDate := getFinancialYearStart(time.Now().UTC())
		transactionDetail := fetchRawTransactionDetails(ctx, pgxPool, entityFilter, api.GetEntityNamesFromCtx(ctx), fromDate)

		// Query B: YTD flows (simple inflows/outflows)
		fyStart := getFinancialYearStart(time.Now().UTC()).Format(constants.DateFormat)
		flowsQ := `SELECT COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('buy','purchase','subscription','sip','switch_in') THEN amount ELSE 0 END),0)::float8 AS inflows, COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('sell','redemption','switch_out') THEN amount ELSE 0 END),0)::float8 AS outflows FROM investment.onboard_transaction WHERE transaction_date >= $1 AND ($2::text IS NULL OR COALESCE(entity_name,'') = $2) AND ($3::text[] IS NULL OR COALESCE(entity_name,'') = ANY($3))`

		var inflows, outflows float64
		if err := pgxPool.QueryRow(ctx, flowsQ, fyStart, nullIfEmpty(entityFilter), allowedEntities).Scan(&inflows, &outflows); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "flows query failed: "+err.Error())
			return
		}

		// Additional KPI computations to mirror individual endpoint
		allowedSlice := api.GetEntityNamesFromCtx(ctx)
		today := time.Now().UTC()
		fyStartTime := getFinancialYearStart(today)
		prevFYStart := fyStartTime.AddDate(-1, 0, 0)
		lastMonthEnd := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)

		// Get AUM at key dates
		lastMonthAUM := getAUMAtDate(ctx, pgxPool, entityFilter, allowedSlice, lastMonthEnd)
		prevFyAUM := getAUMAtDate(ctx, pgxPool, entityFilter, allowedSlice, prevFYStart)
		fyStartAUM := getAUMAtDate(ctx, pgxPool, entityFilter, allowedSlice, fyStartTime)

		// YTD buys/sells (amount sums)
		// NOTE: this used to run a second identical query (ytdQ). To avoid an extra DB round-trip,
		// reuse the results from `flowsQ` above which computes the same inflows/outflows over the FY.
		var ytdBuys, ytdSells float64
		ytdBuys = inflows
		ytdSells = outflows

		// YTD P&L (same formula as individual endpoint)
		openingAUM := fyStartAUM
		if openingAUM == 0 {
			openingAUM = lastMonthAUM
		}
		ytdPL := totalAUM + ytdSells - ytdBuys - openingAUM

		// AUM Trend (month-over-month)
		aumTrendPct := 0.0
		if lastMonthAUM > 0 {
			aumTrendPct = ((totalAUM - lastMonthAUM) / lastMonthAUM) * 100
		}

		// Previous AUM trend baseline
		monthBeforeLastEnd := lastMonthEnd.AddDate(0, -1, 0)
		previousMonthAUM := getAUMAtDate(ctx, pgxPool, entityFilter, allowedSlice, monthBeforeLastEnd)
		prevAumTrendPct := 0.0
		if previousMonthAUM > 0 {
			prevAumTrendPct = ((lastMonthAUM - previousMonthAUM) / previousMonthAUM) * 100
		}

		// XIRR
		xirrVal := calculatePortfolioXIRR(ctx, pgxPool, entityFilter, allowedSlice, totalAUM, today)
		prevXirr := 0.0
		if prevFyAUM != 0 {
			prevXirr = calculatePortfolioXIRR(ctx, pgxPool, entityFilter, allowedSlice, prevFyAUM, prevFYStart)
		} else {
			prevXirr = calculatePortfolioXIRR(ctx, pgxPool, entityFilter, allowedSlice, lastMonthAUM, lastMonthEnd)
		}

		// Liquidity position
		liquidityTotal := 0.0 // we don't have totalCash here; leave as 0 or compute if needed

		// Build KPI cards same as individual endpoint
		baselineAUM := prevFyAUM
		if baselineAUM == 0 {
			baselineAUM = lastMonthAUM
		}
		liquidityBaseline := prevFyAUM
		if liquidityBaseline == 0 {
			liquidityBaseline = lastMonthAUM
		}
		cards := []KPICard{
			KPIChartFromValues("Total AUM", totalAUM, baselineAUM),
			{Title: "AUM Trend", Value: math.Round(aumTrendPct*100) / 100, LastValue: math.Round(prevAumTrendPct*100) / 100, Change: math.Round((aumTrendPct-prevAumTrendPct)*100) / 100},
			{Title: "YTD P&L", Value: math.Round(ytdPL*100) / 100, LastValue: openingAUM, Change: 0},
			func() KPICard {
				currentPct := math.Round(xirrVal*10000) / 100
				prevPct := math.Round(prevXirr*10000) / 100
				return KPICard{Title: "Portfolio XIRR", Value: currentPct, LastValue: prevPct, Change: math.Round((currentPct-prevPct)*100) / 100}
			}(),
			KPIChartFromValues("Liquidity Position", liquidityTotal, liquidityBaseline),
		}

		// AUM detail (fetch same detailed structure as individual endpoint)
		// Consolidated risk (weighted score)
		riskBuckets := map[int]float64{1: 15, 2: 50, 3: 85}
		var weighted float64
		var lowValue, mediumValue, highValue float64
		for _, h := range portfolio {
			score := riskBuckets[h.InternalRiskScore]
			weighted += h.CurrentValue * score
			if h.InternalRiskScore == 1 {
				lowValue += h.CurrentValue
			} else if h.InternalRiskScore == 2 {
				mediumValue += h.CurrentValue
			} else if h.InternalRiskScore == 3 {
				highValue += h.CurrentValue
			}
		}
		lcr := 0.0
		if totalAUM > 0 {
			lcr = weighted / totalAUM
		}

		// Top performing assets (aggregated by scheme + AMC, by current value)
		type aggRow struct {
			Title       string
			Subtitle    string
			Value       float64
			weightedPct float64 // sum of (pct * value)
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

		topRows := make([]TopAssetRow, 0, req.Limit)
		for i := 0; i < len(aggs) && i < req.Limit; i++ {
			a := aggs[i]
			pct := 0.0
			if a.Value > 0 {
				pct = a.weightedPct / a.Value
			}
			topRows = append(topRows, TopAssetRow{Title: a.Title, Subtitle: a.Subtitle, Pct: formatPercent(pct), Value: a.Value})
		}

		// AUM composition by AMC
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

		// Market ticker (use portfolio rows as mutual fund rows)
		ticker := make([]MutualFundTickerRow, 0, len(portfolio))
		for _, h := range portfolio {
			ticker = append(ticker, MutualFundTickerRow{SchemeName: h.SchemeName, AMC: h.AmcName, AMFICode: h.AmfiSchemeCode, InternalCode: h.SchemeID, ISIN: h.Isin, NAV: h.Nav, PrevNAV: 0, Change1D: 0, MTM: 0, Units: h.Units, Value: h.CurrentValue})
		}

		// portfolio_detail: map holdings into InvestmentDetail
		portfolioDetail := make([]InvestmentDetail, 0, len(portfolio))
		for _, h := range portfolio {
			portfolioDetail = append(portfolioDetail, InvestmentDetail{
				EntityName:        h.EntityName,
				AMCName:           h.AmcName,
				SchemeName:        h.SchemeName,
				SchemeID:          h.SchemeID,
				AMFISchemeCode:    h.AmfiSchemeCode,
				SchemeCategory:    h.SchemeCategory,
				SchemeSubCategory: h.SchemeSubCategory,
				SchemeType:        h.SchemeType,
				FolioNumber:       h.FolioNumber,
				ISIN:              h.Isin,
				Units:             h.Units,
				NAV:               h.Nav,
				AMFINAV:           h.AmfiNav,
				PurchaseNAV:       h.PurchaseNav,
				InvestedAmount:    h.InvestedAmount,
				CurrentValue:      h.CurrentValue,
				GainLoss:          h.GainLoss,
				GainLossPct:       h.GainLossPct,
				RiskRating:        h.RiskRating,
				InternalRiskScore: h.InternalRiskScore,
			})
		}

		detailsMap := map[string]interface{}{
			"total_aum":       totalAUM,
			"last_month_aum":  lastMonthAUM,
			"last_fy_aum":     prevFyAUM,
			"fy_start_aum":    fyStartAUM,
			"ytd_buys":        ytdBuys,
			"ytd_sells":       ytdSells,
			"ytd_pnl":         ytdPL,
			"cash_balance":    0,
			"sellable_mf":     0,
			"liquidity_total": liquidityTotal,
			"xirr_annualized": xirrVal * 100,
			"financial_year":  fmt.Sprintf("FY %d-%d", fyStartTime.Year(), fyStartTime.Year()+1),
			"period_start":    fyStartTime.Format(constants.DateFormat),
			"period_end":      today.Format(constants.DateFormat),
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"generated_at": time.Now().UTC().Format(time.RFC3339),
			"kpis": map[string]interface{}{
				"cards":              cards,
				"details":            detailsMap,
				"aum_detail":         portfolioDetail,
				"transaction_detail": transactionDetail,
			},
			"consolidated": map[string]interface{}{
				"generated_at":     time.Now().UTC().Format(time.RFC3339),
				"high_value":       highValue,
				"lcr":              lcr,
				"low_value":        lowValue,
				"medium_value":     mediumValue,
				"portfolio_detail": portfolioDetail,
				"total_value":      totalAUM,
			},
			"top": map[string]interface{}{
				"portfolio_detail": portfolioDetail,
				"rows":             topRows,
				"generated_at":     time.Now().UTC().Format(time.RFC3339),
			},
			"aum": map[string]interface{}{
				"amc_names":        amcNames,
				"fy_label":         "FY 2025-26",
				"generated_at":     time.Now().UTC().Format(time.RFC3339),
				"portfolio_detail": portfolioDetail,
				"rows":             aumRows,
			},
			"market": map[string]interface{}{
				"mutual_funds":     ticker,
				"portfolio_detail": portfolioDetail,
				"summary": map[string]interface{}{
					"total_schemes": len(ticker),
					"total_value":   totalAUM,
				},
				"generated_at": time.Now().UTC().Format(time.RFC3339),
			},
		})
	}
}
