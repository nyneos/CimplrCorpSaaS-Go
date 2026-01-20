package investmentdashboards

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"io"
	"strconv"
	"sync"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TransactionDetail is the common structure for transaction-related data
// InvestmentDetail is the common detail structure for all investment-related data
// Used to explain how each number is computed with full traceability
type InvestmentDetail struct {
	EntityName        string  `json:"entity_name"`
	AMCName           string  `json:"amc_name,omitempty"`
	SchemeName        string  `json:"scheme_name,omitempty"`
	SchemeID          string  `json:"scheme_id,omitempty"`
	AMFISchemeCode    string  `json:"amfi_scheme_code,omitempty"`
	SchemeCategory    string  `json:"scheme_category,omitempty"`
	SchemeSubCategory string  `json:"scheme_sub_category,omitempty"`
	SchemeType        string  `json:"scheme_type,omitempty"`
	FolioNumber       string  `json:"folio_number,omitempty"`
	DematAccount      string  `json:"demat_account,omitempty"`
	ISIN              string  `json:"isin,omitempty"`
	Units             float64 `json:"units,omitempty"`
	NAV               float64 `json:"nav,omitempty"`
	AMFINAV           float64 `json:"amfi_nav,omitempty"`
	PurchaseNAV       float64 `json:"purchase_nav,omitempty"`
	InvestedAmount    float64 `json:"invested_amount,omitempty"`
	CurrentValue      float64 `json:"current_value,omitempty"`
	GainLoss          float64 `json:"gain_loss,omitempty"`
	GainLossPct       float64 `json:"gain_loss_pct,omitempty"`
	RiskRating        string  `json:"risk_rating,omitempty"`
	InternalRiskScore int     `json:"internal_risk_score,omitempty"`
	LockInPeriod      string  `json:"lock_in_period,omitempty"`
	ExitLoad          string  `json:"exit_load,omitempty"`
}

type TransactionDetail struct {
	EntityName        string  `json:"entity_name"`
	TransactionType   string  `json:"transaction_type"`
	TransactionDate   string  `json:"transaction_date"`
	SchemeName        string  `json:"scheme_name,omitempty"`
	SchemeID          string  `json:"scheme_id,omitempty"`
	AMFISchemeCode    string  `json:"amfi_scheme_code,omitempty"`
	AMCName           string  `json:"amc_name,omitempty"`
	SchemeCategory    string  `json:"scheme_category,omitempty"`
	SchemeSubCategory string  `json:"scheme_sub_category,omitempty"`
	FolioNumber       string  `json:"folio_number,omitempty"`
	DematAccount      string  `json:"demat_account,omitempty"`
	ISIN              string  `json:"isin,omitempty"`
	Units             float64 `json:"units,omitempty"`
	NAV               float64 `json:"nav,omitempty"`
	AMFINAV           float64 `json:"amfi_nav,omitempty"`
	Amount            float64 `json:"amount"`
	RiskRating        string  `json:"risk_rating,omitempty"`
}

// AggregatedValue is used for grouping data by any dimension
type AggregatedValue struct {
	GroupKey   string  `json:"group_key"`  // The grouping dimension value (entity name, amc name, etc.)
	GroupType  string  `json:"group_type"` // What the grouping represents: "entity", "amc", "scheme", "category"
	Value      float64 `json:"value"`      // The aggregated value
	Count      int     `json:"count"`      // Number of items in this group
	Percentage float64 `json:"percentage"` // Percentage of total
}

// KPICard represents a single KPI metric card
type KPICard struct {
	Title     string  `json:"title"`
	Value     float64 `json:"value"`
	LastValue float64 `json:"lastValue"`
	Change    float64 `json:"change"`
}

// GetInvestmentOverviewKPIs computes the 4 KPI cards: Total AUM, AUM Trend, YTD P&L, Portfolio XIRR, Liquidity Position
// OPTIMIZED: Single CTE query for all metrics, proper entity validation, correct formulas
// Request JSON: { "entity_name": "Optional entity filter" }
func GetInvestmentOverviewKPIs(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Validate entity from context if not provided
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}

		// Get allowed entities from context for filtering
		allowedEntities := api.GetEntityNamesFromCtx(ctx)
		entityFilterSQL := ""
		if entityFilter != "" {
			entityFilterSQL = entityFilter
		}

		today := time.Now().UTC()
		fyStart := getFinancialYearStart(today)

		// Use previous fiscal year start as the comparison point (not last month)
		// e.g., if today is within FY 2025-26 (starts Apr 1 2025), previous FY start = Apr 1 2024
		prevFYStart := fyStart.AddDate(-1, 0, 0)
		// Compute last month end (used for month-over-month AUM comparison)
		lastMonthEnd := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)

		// OPTIMIZED: Single comprehensive query for all KPIs
		kpiQuery := `
		WITH params AS (
			SELECT 
				$1::text AS entity_filter,
				$2::date AS fy_start,
				$3::date AS last_month_end,
				$4::date AS prev_fy_start,
				$5::text[] AS allowed_entities
		),
		-- Current AUM from portfolio_snapshot
		current_aum AS (
			SELECT COALESCE(SUM(ps.current_value), 0)::float8 AS total_aum
			FROM investment.portfolio_snapshot ps, params p
			WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
		),
		-- Last month end AUM (units as of last month * NAV at that date)
		last_month_holdings AS (
			SELECT 
				COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref,
				ms.amfi_scheme_code,
				SUM(CASE 
					WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') 
					THEN COALESCE(ot.units, 0)
					WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') 
					THEN -COALESCE(ot.units, 0)
					ELSE 0
				END) AS units
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.masterscheme ms ON (
				ms.scheme_id = ot.scheme_id OR 
				ms.internal_scheme_code = ot.scheme_internal_code OR 
				ms.isin = ot.scheme_id
			), params p
			WHERE ot.transaction_date <= p.last_month_end
			  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name, '') = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name, '') = ANY(p.allowed_entities))
			GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code), ms.amfi_scheme_code
			HAVING SUM(CASE 
				WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') 
				THEN COALESCE(ot.units, 0)
				WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') 
				THEN -COALESCE(ot.units, 0)
				ELSE 0
			END) > 0
		),
		last_month_navs AS (
			SELECT DISTINCT ON (scheme_code) 
				scheme_code::text AS scheme_code, 
				nav_value
			FROM investment.amfi_nav_staging, params p
			WHERE nav_date <= p.last_month_end
			ORDER BY scheme_code, nav_date DESC
		),
            last_month_aum AS (
			SELECT COALESCE(SUM(h.units * COALESCE(n.nav_value, 0)), 0)::float8 AS aum
			FROM last_month_holdings h
			LEFT JOIN last_month_navs n ON n.scheme_code = h.amfi_scheme_code::text
		),
		-- FY start AUM (for YTD P&L)
		fy_start_holdings AS (
			SELECT 
				COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref,
				ms.amfi_scheme_code,
				SUM(CASE 
					WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') 
					THEN COALESCE(ot.units, 0)
					WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') 
					THEN -COALESCE(ot.units, 0)
					ELSE 0
				END) AS units
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.masterscheme ms ON (
				ms.scheme_id = ot.scheme_id OR 
				ms.internal_scheme_code = ot.scheme_internal_code OR 
				ms.isin = ot.scheme_id
			), params p
			WHERE ot.transaction_date < p.fy_start
			  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name, '') = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name, '') = ANY(p.allowed_entities))
			GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code), ms.amfi_scheme_code
			HAVING SUM(CASE 
				WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') 
				THEN COALESCE(ot.units, 0)
				WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') 
				THEN -COALESCE(ot.units, 0)
				ELSE 0
			END) > 0
		),
		fy_start_navs AS (
			SELECT DISTINCT ON (scheme_code) 
				scheme_code::text AS scheme_code, 
				nav_value
			FROM investment.amfi_nav_staging, params p
			WHERE nav_date < p.fy_start
			ORDER BY scheme_code, nav_date DESC
		),
		fy_start_aum AS (
			SELECT COALESCE(SUM(h.units * COALESCE(n.nav_value, 0)), 0)::float8 AS aum
			FROM fy_start_holdings h
			LEFT JOIN fy_start_navs n ON n.scheme_code = h.amfi_scheme_code::text
		),
		
		-- Previous fiscal year start AUM (for last-year comparison)
		prev_fy_start_holdings AS (
			SELECT 
				COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref,
				ms.amfi_scheme_code,
				SUM(CASE 
					WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') 
					THEN COALESCE(ot.units, 0)
					WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') 
					THEN -COALESCE(ot.units, 0)
					ELSE 0
				END) AS units
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.masterscheme ms ON (
				ms.scheme_id = ot.scheme_id OR 
				ms.internal_scheme_code = ot.scheme_internal_code OR 
				ms.isin = ot.scheme_id
			), params p
			WHERE ot.transaction_date < p.prev_fy_start
			  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name, '') = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name, '') = ANY(p.allowed_entities))
			GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code), ms.amfi_scheme_code
			HAVING SUM(CASE 
				WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') 
				THEN COALESCE(ot.units, 0)
				WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') 
				THEN -COALESCE(ot.units, 0)
				ELSE 0
			END) > 0
		),
		prev_fy_start_navs AS (
			SELECT DISTINCT ON (scheme_code) 
				scheme_code::text AS scheme_code, 
				nav_value
			FROM investment.amfi_nav_staging, params p
			WHERE nav_date <= p.prev_fy_start
			ORDER BY scheme_code, nav_date DESC
		),
		prev_fy_start_aum AS (
			SELECT COALESCE(SUM(h.units * COALESCE(n.nav_value, 0)), 0)::float8 AS aum
			FROM prev_fy_start_holdings h
			LEFT JOIN prev_fy_start_navs n ON n.scheme_code = h.amfi_scheme_code::text
		),

		-- YTD flows for P&L calculation
		ytd_flows AS (
			SELECT 
				COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('buy','purchase','subscription','switch_in') THEN amount ELSE 0 END), 0)::float8 AS buys,
				COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('sell','redemption','switch_out') THEN amount ELSE 0 END), 0)::float8 AS sells
			FROM investment.onboard_transaction, params p
			WHERE transaction_date >= p.fy_start
			  AND (p.entity_filter IS NULL OR COALESCE(entity_name, '') = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR COALESCE(entity_name, '') = ANY(p.allowed_entities))
		),
		-- Liquidity: Cash from bank statements (simplified - just get total approved balances)
		cash_balances AS (
			SELECT COALESCE(SUM(bs.closingbalance), 0)::float8 AS total_cash
			FROM bank_statement bs
			JOIN masterbankaccount mba ON bs.account_number = mba.account_number
			LEFT JOIN masterentity me ON mba.entity_id = me.entity_id
			CROSS JOIN params p
			WHERE bs.status = 'Approved'
			  AND (p.entity_filter IS NULL OR me.entity_name = p.entity_filter)
		),
		-- Manual balances for liquidity
		manual_balances AS (
			SELECT 0::float8 AS total_manual
		),
		-- Sellable MF holdings
		sellable_mf AS (
			SELECT COALESCE(SUM(current_value), 0)::float8 AS total_sellable
			FROM investment.portfolio_snapshot ps, params p
			WHERE COALESCE(ps.total_units, 0) > 0
			  AND COALESCE(ps.current_value, 0) > 0
			  AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
		)
		SELECT 
			ca.total_aum,
			lma.aum AS last_month_aum,
			fsa.aum AS fy_start_aum,
			pf.aum AS prev_fy_aum,
			yf.buys AS ytd_buys,
			yf.sells AS ytd_sells,
			(cb.total_cash + mb.total_manual) AS total_cash,
			sm.total_sellable
		FROM current_aum ca, last_month_aum lma, fy_start_aum fsa, prev_fy_start_aum pf, ytd_flows yf, cash_balances cb, manual_balances mb, sellable_mf sm
		`

		var totalAUM, lastMonthAUM, fyStartAUM, prevFyAUM, ytdBuys, ytdSells, totalCash, sellableMF float64
		var allowedEntitiesParam interface{} = nil
		if len(allowedEntities) > 0 {
			allowedEntitiesParam = allowedEntities
		}

		err := pgxPool.QueryRow(ctx, kpiQuery,
			nullIfEmpty(entityFilterSQL),
			fyStart.Format(constants.DateFormat),
			lastMonthEnd.Format(constants.DateFormat),
			prevFYStart.Format(constants.DateFormat),
			allowedEntitiesParam,
		).Scan(&totalAUM, &lastMonthAUM, &fyStartAUM, &prevFyAUM, &ytdBuys, &ytdSells, &totalCash, &sellableMF)

		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "KPI query failed: "+err.Error())
			return
		}

		// Calculate YTD P&L: Current AUM - Opening AUM - Net Investments + Redemption Proceeds
		// Formula: (Closing - Opening) - (Buys - Sells) = Market Gain/Loss
		// Simplified: Current AUM + Sells - Buys - Opening AUM
		// Determine opening AUM for YTD P&L. Prefer FY start; fallback to last month if missing.
		openingAUM := fyStartAUM
		if openingAUM == 0 {
			openingAUM = lastMonthAUM
		}
		// Calculate YTD P&L: Current AUM + Sells - Buys - Opening AUM
		ytdPL := totalAUM + ytdSells - ytdBuys - openingAUM

		// Calculate AUM Trend (month-over-month): compare vs last month end (better UX than prev FY)
		aumTrendPct := 0.0
		if lastMonthAUM > 0 {
			aumTrendPct = ((totalAUM - lastMonthAUM) / lastMonthAUM) * 100
		}

		// Calculate previous month's AUM (for consistent percent-vs-percent comparison)
		monthBeforeLastEnd := lastMonthEnd.AddDate(0, -1, 0)
		previousMonthAUM := getAUMAtDate(ctx, pgxPool, entityFilterSQL, allowedEntities, monthBeforeLastEnd)
		prevAumTrendPct := 0.0
		if previousMonthAUM > 0 {
			prevAumTrendPct = ((lastMonthAUM - previousMonthAUM) / previousMonthAUM) * 100
		}

		// XIRR calculation (separate query for cash flows)
		// Calculate current XIRR using terminal value = totalAUM at today (returns decimal, e.g., 0.0907)
		xirrVal := calculatePortfolioXIRR(ctx, pgxPool, entityFilterSQL, allowedEntities, totalAUM, today)
		// Compute a previous reference XIRR using previous FY terminal AUM at prevFYStart (best-effort)
		prevXirr := 0.0
		if prevFyAUM != 0 {
			prevXirr = calculatePortfolioXIRR(ctx, pgxPool, entityFilterSQL, allowedEntities, prevFyAUM, prevFYStart)
		} else {
			// Fallback: if prev FY AUM is not available, use last month end as the terminal point
			prevXirr = calculatePortfolioXIRR(ctx, pgxPool, entityFilterSQL, allowedEntities, lastMonthAUM, lastMonthEnd)
		}

		// Liquidity Position
		liquidityTotal := totalCash + sellableMF

		// Build KPI cards (use last-month comparison by default, with sensible fallbacks)
		// Build KPI cards
		// Total AUM: compare against previous FY if available, fallback to last month
		baselineAUM := prevFyAUM
		if baselineAUM == 0 {
			baselineAUM = lastMonthAUM
		}
		// Choose liquidity baseline: prefer prev FY AUM (sellable MF approx), fallback to last month
		liquidityBaseline := prevFyAUM
		if liquidityBaseline == 0 {
			liquidityBaseline = lastMonthAUM
		}
		cards := []KPICard{
			KPIChartFromValues("Total AUM", totalAUM, baselineAUM),
			{Title: "AUM Trend", Value: math.Round(aumTrendPct*100) / 100, LastValue: math.Round(prevAumTrendPct*100) / 100, Change: math.Round((aumTrendPct-prevAumTrendPct)*100) / 100},
			{Title: "YTD P&L", Value: math.Round(ytdPL*100) / 100, LastValue: openingAUM, Change: 0},
			// Portfolio XIRR: display percent (e.g., 9.07 means 9.07%) and compare vs previous period
			func() KPICard {
				currentPct := math.Round(xirrVal*10000) / 100
				prevPct := math.Round(prevXirr*10000) / 100
				return KPICard{Title: "Portfolio XIRR", Value: currentPct, LastValue: prevPct, Change: math.Round((currentPct-prevPct)*100) / 100}
			}(),
			KPIChartFromValues("Liquidity Position", liquidityTotal, liquidityBaseline),
		}

		// Fetch AUM breakdown by entity, AMC, scheme for transparency
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilterSQL, allowedEntities)

		// Fetch YTD raw transactions (not grouped)
		rawTransactions := fetchRawTransactionDetails(ctx, pgxPool, entityFilterSQL, allowedEntities, fyStart)

		resp := map[string]interface{}{
			"cards":        cards,
			"generated_at": time.Now().UTC().Format(time.RFC3339),
			"details": map[string]interface{}{
				"total_aum":       totalAUM,
				"last_month_aum":  lastMonthAUM,
				"last_fy_aum":     prevFyAUM,
				"fy_start_aum":    fyStartAUM,
				"ytd_buys":        ytdBuys,
				"ytd_sells":       ytdSells,
				"ytd_pnl":         ytdPL,
				"cash_balance":    totalCash,
				"sellable_mf":     sellableMF,
				"liquidity_total": liquidityTotal,
				"xirr_annualized": xirrVal * 100,
				"financial_year":  fmt.Sprintf("FY %d-%d", fyStart.Year(), fyStart.Year()+1),
				"period_start":    fyStart.Format(constants.DateFormat),
				"period_end":      today.Format(constants.DateFormat),
			},
			"aum_detail":         aumDetails,
			"transaction_detail": rawTransactions,
		}
		api.RespondWithPayload(w, true, "", resp)
	}
}

// fetchAUMDetails returns detailed breakdown of current AUM by entity, AMC, scheme
// OPTIMIZED: Uses CTE to pre-compute latest NAV per scheme (eliminates N+1 LATERAL joins)
func fetchAUMDetails(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string) []InvestmentDetail {
	query := `
		WITH latest_nav AS (
			SELECT DISTINCT ON (scheme_code) scheme_code::text, nav_value
			FROM investment.amfi_nav_staging
			ORDER BY scheme_code, nav_date DESC
		)
		SELECT 
			ps.entity_name,
			COALESCE(ms.amc_name, 'Unknown') AS amc_name,
			COALESCE(ps.scheme_name, ms.scheme_name, 'Unknown') AS scheme_name,
			COALESCE(ps.scheme_id, '') AS scheme_id,
			COALESCE(ms.amfi_scheme_code::text, asm.scheme_code::text, '') AS amfi_scheme_code,
			COALESCE(asm.scheme_category, 'Other') AS scheme_category,
			COALESCE(asm.scheme_sub_category, '') AS scheme_sub_category,
			COALESCE(asm.scheme_type, '') AS scheme_type,
			COALESCE(ps.folio_number, '') AS folio_number,
			COALESCE(ps.demat_acc_number, '') AS demat_account,
			COALESCE(ps.isin, ms.isin, '') AS isin,
			COALESCE(ps.total_units, 0)::float8 AS units,
			COALESCE(ps.current_nav, 0)::float8 AS nav,
			COALESCE(ln.nav_value, ps.current_nav, 0)::float8 AS amfi_nav,
			COALESCE(ps.avg_nav, 0)::float8 AS purchase_nav,
			COALESCE(ps.total_invested_amount, 0)::float8 AS invested_amount,
			COALESCE(ps.current_value, 0)::float8 AS current_value,
			COALESCE(ps.gain_loss, 0)::float8 AS gain_loss,
			CASE WHEN COALESCE(ps.total_invested_amount, 0) > 0 
				THEN ((COALESCE(ps.current_value, 0) - COALESCE(ps.total_invested_amount, 0)) / COALESCE(ps.total_invested_amount, 0) * 100)
				ELSE 0 
			END::float8 AS gain_loss_pct,
			COALESCE(INITCAP(ms.internal_risk_rating), 'Medium') AS risk_rating,
			CASE LOWER(COALESCE(ms.internal_risk_rating, 'medium'))
				WHEN 'low' THEN 1
				WHEN 'medium' THEN 2
				WHEN 'high' THEN 3
				ELSE 2
			END AS internal_risk_score
		FROM investment.portfolio_snapshot ps
		LEFT JOIN investment.masterscheme ms ON ms.scheme_id = ps.scheme_id
		LEFT JOIN investment.amfi_scheme_master_staging asm ON asm.scheme_code::text = ms.amfi_scheme_code::text
		LEFT JOIN latest_nav ln ON ln.scheme_code = COALESCE(ms.amfi_scheme_code::text, asm.scheme_code::text)
		WHERE COALESCE(ps.current_value, 0) > 0
		ORDER BY ps.entity_name, ms.amc_name, ps.current_value DESC
	`

	rows, err := pgxPool.Query(ctx, query)
	if err != nil {
		log.Printf("[investmentOverview] fetchAUMDetails query error: %v", err)
		return []InvestmentDetail{}
	}
	defer rows.Close()

	details := make([]InvestmentDetail, 0, 100)
	for rows.Next() {
		var d InvestmentDetail
		if err := rows.Scan(&d.EntityName, &d.AMCName, &d.SchemeName, &d.SchemeID, &d.AMFISchemeCode,
			&d.SchemeCategory, &d.SchemeSubCategory, &d.SchemeType,
			&d.FolioNumber, &d.DematAccount, &d.ISIN, &d.Units, &d.NAV, &d.AMFINAV, &d.PurchaseNAV,
			&d.InvestedAmount, &d.CurrentValue, &d.GainLoss, &d.GainLossPct,
			&d.RiskRating, &d.InternalRiskScore); err != nil {
			continue
		}
		details = append(details, d)
	}
	return details
}

// fetchRawTransactionDetails returns RAW TABULAR transaction data - no grouping
// OPTIMIZED: Uses CTE to pre-compute latest NAV per scheme (eliminates N+1 LATERAL joins)
func fetchRawTransactionDetails(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, fromDate time.Time) []TransactionDetail {
	query := `
		WITH latest_nav AS (
			SELECT DISTINCT ON (scheme_code) scheme_code::text, nav_value
			FROM investment.amfi_nav_staging
			ORDER BY scheme_code, nav_date DESC
		)
		SELECT 
			COALESCE(ot.entity_name, '') AS entity_name,
			COALESCE(ot.transaction_type, '') AS transaction_type,
			COALESCE(ot.transaction_date::text, '') AS transaction_date,
			COALESCE(ms.scheme_name, COALESCE(ot.scheme_internal_code, ot.scheme_id::text), 'Unknown') AS scheme_name,
			COALESCE(ot.scheme_id, ms.scheme_id, '') AS scheme_id,
			COALESCE(ms.amfi_scheme_code::text, asm.scheme_code::text, '') AS amfi_scheme_code,
			COALESCE(ms.amc_name, 'Unknown') AS amc_name,
			COALESCE(asm.scheme_category, 'Other') AS scheme_category,
			COALESCE(asm.scheme_sub_category, '') AS scheme_sub_category,
			COALESCE(ot.folio_number, '') AS folio_number,
			COALESCE(ot.demat_acc_number, '') AS demat_account,
			COALESCE(ms.isin, ot.scheme_id, '') AS isin,
			COALESCE(ot.units, 0)::float8 AS units,
			COALESCE(ot.nav, 0)::float8 AS nav,
			COALESCE(ln.nav_value, ot.nav, 0)::float8 AS amfi_nav,
			COALESCE(ABS(ot.amount), 0)::float8 AS amount,
			COALESCE(INITCAP(ms.internal_risk_rating), 'Medium') AS risk_rating
		FROM investment.onboard_transaction ot
		LEFT JOIN investment.masterscheme ms ON ms.scheme_id = ot.scheme_id
		LEFT JOIN investment.amfi_scheme_master_staging asm ON asm.scheme_code::text = ms.amfi_scheme_code::text
		LEFT JOIN latest_nav ln ON ln.scheme_code = COALESCE(ms.amfi_scheme_code::text, asm.scheme_code::text)
		WHERE ot.transaction_date >= $1
		  AND ($2::text IS NULL OR COALESCE(ot.entity_name,'') = $2)
		  AND ($3::text[] IS NULL OR COALESCE(ot.entity_name,'') = ANY($3))
		ORDER BY ot.transaction_date DESC, ot.entity_name, ot.amount DESC
	`

	// Pass entity filters the same way other queries do to ensure consistent results
	rows, err := pgxPool.Query(ctx, query, fromDate, nullIfEmpty(entityFilter), allowedEntities)
	if err != nil {
		log.Printf("[investmentOverview] fetchRawTransactionDetails query error: %v", err)
		return []TransactionDetail{}
	}
	defer rows.Close()

	transactions := make([]TransactionDetail, 0, 200)
	for rows.Next() {
		var t TransactionDetail
		if err := rows.Scan(&t.EntityName, &t.TransactionType, &t.TransactionDate,
			&t.SchemeName, &t.SchemeID, &t.AMFISchemeCode, &t.AMCName,
			&t.SchemeCategory, &t.SchemeSubCategory,
			&t.FolioNumber, &t.DematAccount, &t.ISIN,
			&t.Units, &t.NAV, &t.AMFINAV, &t.Amount, &t.RiskRating); err != nil {
			continue
		}
		transactions = append(transactions, t)
	}
	return transactions
}

// getAUMAtDate computes portfolio AUM as of a given date (units as of date * latest nav <= date)
func getAUMAtDate(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, asOf time.Time) float64 {
	q := `
	WITH holdings AS (
		SELECT 
			COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref,
			COALESCE(ms.amfi_scheme_code::text, '') AS amfi_code,
			SUM(CASE 
				WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
				WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0)
				ELSE 0 END) AS units
		FROM investment.onboard_transaction ot
		LEFT JOIN investment.masterscheme ms ON (
			ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id
		)
		WHERE ot.transaction_date <= $1
		  AND ($2::text IS NULL OR COALESCE(ot.entity_name,'') = $2)
		  AND ($3::text[] IS NULL OR COALESCE(ot.entity_name,'') = ANY($3))
		GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code), ms.amfi_scheme_code
		HAVING SUM(CASE 
				WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','dividend_reinvest') THEN COALESCE(ot.units,0)
				WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -COALESCE(ot.units,0)
				ELSE 0 END) <> 0
	), navs AS (
		SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_code, nav_value
		FROM investment.amfi_nav_staging
		WHERE nav_date <= $1
		ORDER BY scheme_code, nav_date DESC
	)
	SELECT COALESCE(SUM(h.units * COALESCE(n.nav_value,0)),0)::float8 FROM holdings h
	LEFT JOIN navs n ON n.scheme_code = h.amfi_code
	`

	var allowedParam interface{} = nil
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}
	var aum float64
	err := pgxPool.QueryRow(ctx, q, asOf.Format(constants.DateFormat), nullIfEmpty(entityFilter), allowedParam).Scan(&aum)
	if err != nil {
		log.Printf("[investmentOverview] getAUMAtDate query error: %v", err)
		return 0
	}
	return aum
}

// fetchAUMDetailsWithRisk returns detailed breakdown with risk ratings
// OPTIMIZED: Uses CTE to pre-compute latest NAV per scheme (eliminates N+1 LATERAL joins)
func fetchAUMDetailsWithRisk(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string) []InvestmentDetail {
	query := `
		WITH latest_nav AS (
			SELECT DISTINCT ON (scheme_code) scheme_code::text, nav_value
			FROM investment.amfi_nav_staging
			ORDER BY scheme_code, nav_date DESC
		)
		SELECT 
			ps.entity_name,
			COALESCE(ms.amc_name, 'Unknown') AS amc_name,
			COALESCE(ps.scheme_name, ms.scheme_name, 'Unknown') AS scheme_name,
			COALESCE(ps.scheme_id, '') AS scheme_id,
			COALESCE(ms.amfi_scheme_code::text, asm.scheme_code::text, '') AS amfi_scheme_code,
			COALESCE(asm.scheme_category, 'Other') AS scheme_category,
			COALESCE(asm.scheme_sub_category, '') AS scheme_sub_category,
			COALESCE(asm.scheme_type, '') AS scheme_type,
			COALESCE(ps.folio_number, '') AS folio_number,
			COALESCE(ps.demat_acc_number, '') AS demat_account,
			COALESCE(ps.isin, ms.isin, '') AS isin,
			COALESCE(ps.total_units, 0)::float8 AS units,
			COALESCE(ps.current_nav, 0)::float8 AS nav,
			COALESCE(ln.nav_value, ps.current_nav, 0)::float8 AS amfi_nav,
			COALESCE(ps.avg_nav, 0)::float8 AS purchase_nav,
			COALESCE(ps.total_invested_amount, 0)::float8 AS invested_amount,
			COALESCE(ps.current_value, 0)::float8 AS current_value,
			COALESCE(ps.gain_loss, 0)::float8 AS gain_loss,
			CASE WHEN COALESCE(ps.total_invested_amount, 0) > 0 
				THEN ((COALESCE(ps.current_value, 0) - COALESCE(ps.total_invested_amount, 0)) / COALESCE(ps.total_invested_amount, 0) * 100)
				ELSE 0 
			END::float8 AS gain_loss_pct,
			COALESCE(INITCAP(ms.internal_risk_rating), 'Medium') AS risk_rating,
			CASE LOWER(COALESCE(ms.internal_risk_rating, 'medium'))
				WHEN 'low' THEN 1
				WHEN 'medium' THEN 2
				WHEN 'high' THEN 3
				ELSE 2
			END AS internal_risk_score
		FROM investment.portfolio_snapshot ps
		LEFT JOIN investment.masterscheme ms ON ms.scheme_id = ps.scheme_id
		LEFT JOIN investment.amfi_scheme_master_staging asm ON asm.scheme_code::text = ms.amfi_scheme_code::text
		LEFT JOIN latest_nav ln ON ln.scheme_code = COALESCE(ms.amfi_scheme_code::text, asm.scheme_code::text)
		WHERE COALESCE(ps.current_value, 0) > 0
		ORDER BY ps.entity_name, ms.internal_risk_rating, ps.current_value DESC
	`

	rows, err := pgxPool.Query(ctx, query)
	if err != nil {
		log.Printf("[investmentOverview] fetchAUMDetailsWithRisk query error: %v", err)
		return []InvestmentDetail{}
	}
	defer rows.Close()

	details := make([]InvestmentDetail, 0)
	for rows.Next() {
		var d InvestmentDetail
		if err := rows.Scan(&d.EntityName, &d.AMCName, &d.SchemeName, &d.SchemeID, &d.AMFISchemeCode,
			&d.SchemeCategory, &d.SchemeSubCategory, &d.SchemeType,
			&d.FolioNumber, &d.DematAccount, &d.ISIN, &d.Units, &d.NAV, &d.AMFINAV, &d.PurchaseNAV,
			&d.InvestedAmount, &d.CurrentValue, &d.GainLoss, &d.GainLossPct,
			&d.RiskRating, &d.InternalRiskScore); err != nil {
			log.Printf("[investmentOverview] fetchAUMDetailsWithRisk row scan error: %v", err)
			continue
		}
		details = append(details, d)
	}
	log.Printf("[investmentOverview] fetchAUMDetailsWithRisk returned %d rows", len(details))
	return details
}

// calculatePortfolioXIRR computes XIRR from cash flows - optimized to batch query
func calculatePortfolioXIRR(ctx context.Context, pgxPool *pgxpool.Pool, entityFilter string, allowedEntities []string, terminalValue float64, terminalDate time.Time) float64 {
	flowsQ := `
		SELECT transaction_date, 
			   CASE WHEN LOWER(transaction_type) IN ('buy','purchase','subscription','switch_in') THEN -amount 
					WHEN LOWER(transaction_type) IN ('sell','redemption','switch_out') THEN amount 
					ELSE 0 END AS flow_amount 
		FROM investment.onboard_transaction
		WHERE LOWER(transaction_type) IN ('buy','purchase','subscription','sell','redemption','switch_in','switch_out')
		  AND ($1::text IS NULL OR COALESCE(entity_name,'') = $1)
		  AND ($2::text[] IS NULL OR COALESCE(entity_name,'') = ANY($2))
		  AND transaction_date <= $3::date
		ORDER BY transaction_date
	`
	var allowedParam interface{} = nil
	if len(allowedEntities) > 0 {
		allowedParam = allowedEntities
	}

	rows, err := pgxPool.Query(ctx, flowsQ, nullIfEmpty(entityFilter), allowedParam, terminalDate)
	if err != nil {
		return 0
	}
	defer rows.Close()

	flows := make([]CashFlow, 0, 100)
	for rows.Next() {
		var d time.Time
		var amt float64
		if err := rows.Scan(&d, &amt); err == nil {
			if amt != 0 {
				flows = append(flows, CashFlow{Date: d, Amount: amt})
			}
		}
	}

	// If there are no flows and no terminal value, return 0
	if len(flows) == 0 && terminalValue == 0 {
		return 0
	}

	// Add terminal value at the terminal date
	flows = append(flows, CashFlow{Date: terminalDate, Amount: terminalValue})

	xirrVal, err := ComputeXIRR(flows)
	if err != nil || math.IsNaN(xirrVal) || math.IsInf(xirrVal, 0) {
		return 0
	}
	// Defensive normalization: some upstream data or historical codepaths may return
	// XIRR expressed as percentage (e.g., 9.07) instead of decimal (0.0907).
	// Normalize to decimal form if the returned value looks like a percent (> 1.0).
	if math.Abs(xirrVal) > 1.0 {
		xirrVal = xirrVal / 100.0
	}
	return xirrVal
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func getFinancialYearStart(t time.Time) time.Time {
	y := t.Year()
	// FY starts on April 1st
	fy := time.Date(y, time.April, 1, 0, 0, 0, 0, time.UTC)
	if t.Before(fy) {
		fy = fy.AddDate(-1, 0, 0)
	}
	return fy
}

// MFAPI cache to reduce external HTTP requests
type mfapiCacheEntry struct {
	Name      string
	NAV       float64
	PrevNAV   float64
	FetchedAt time.Time
}

var (
	mfapiCacheMu  sync.RWMutex
	mfapiCache    = make(map[string]mfapiCacheEntry)
	mfapiCacheTTL = 90 * time.Second
)

// getMFAPIData fetches latest and previous NAV from MFAPI with caching.
// Returns scheme name (if available), nav and prevNav (0 if not found).
func getMFAPIData(code string) (string, float64, float64) {
	if code == "" {
		return "", 0, 0
	}

	// Check cache
	mfapiCacheMu.RLock()
	entry, ok := mfapiCache[code]
	mfapiCacheMu.RUnlock()
	if ok && time.Since(entry.FetchedAt) < mfapiCacheTTL {
		return entry.Name, entry.NAV, entry.PrevNAV
	}

	apiURL := fmt.Sprintf("https://api.mfapi.in/mf/%s", code)
	resp, err := http.Get(apiURL)
	if err != nil || resp == nil {
		if resp != nil {
			resp.Body.Close()
		}
		return "", 0, 0
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var apiResp struct {
		Meta struct {
			SchemeName string `json:"scheme_name"`
			SchemeCode string `json:"scheme_code"`
		} `json:"meta"`
		Data []struct {
			Date string `json:"date"`
			NAV  string `json:"nav"`
		} `json:"data"`
		Status string `json:"status"`
	}

	name := ""
	var nav, prev float64
	if json.Unmarshal(body, &apiResp) == nil && len(apiResp.Data) > 0 {
		name = apiResp.Meta.SchemeName
		if apiResp.Meta.SchemeName == "" {
			// sometimes name might be in Data or not present; leave empty if missing
			name = ""
		}
		// parse latest
		if val, err := strconv.ParseFloat(strings.ReplaceAll(apiResp.Data[0].NAV, ",", ""), 64); err == nil {
			nav = val
		}
		// parse previous (if present)
		if len(apiResp.Data) > 1 {
			if val, err := strconv.ParseFloat(strings.ReplaceAll(apiResp.Data[1].NAV, ",", ""), 64); err == nil {
				prev = val
			}
		}
	}

	// Store in cache
	mfapiCacheMu.Lock()
	mfapiCache[code] = mfapiCacheEntry{Name: name, NAV: nav, PrevNAV: prev, FetchedAt: time.Now()}
	mfapiCacheMu.Unlock()

	return name, nav, prev
}

// CashFlow used for XIRR
type CashFlow struct {
	Date   time.Time
	Amount float64
}

// ComputeXIRR uses Newton-Raphson to solve for rate r where NPV = 0
func ComputeXIRR(flows []CashFlow) (float64, error) {
	if len(flows) < 2 {
		return 0, nil
	}
	// convert to days from first date
	base := flows[0].Date
	days := func(d time.Time) float64 { return d.Sub(base).Hours() / 24.0 / 365.0 }
	npv := func(r float64) float64 {
		s := 0.0
		for _, f := range flows {
			s += f.Amount / math.Pow(1.0+r, days(f.Date))
		}
		return s
	}
	// derivative approximate
	deriv := func(r float64) float64 {
		h := 1e-6
		return (npv(r+h) - npv(r-h)) / (2 * h)
	}
	r := 0.1
	for i := 0; i < 100; i++ {
		f := npv(r)
		df := deriv(r)
		if math.Abs(df) < 1e-12 {
			break
		}
		nr := r - f/df
		if math.IsNaN(nr) {
			break
		}
		if math.Abs(nr-r) < 1e-9 {
			break
		}
		r = nr
	}
	return r, nil
}
func KPIChartFromValues(title string, value, last float64) KPICard {
	var change float64
	if last == 0 {
		change = 0
	} else {
		change = ((value - last) / math.Abs(last)) * 100
	}
	return KPICard{Title: title, Value: value, LastValue: last, Change: math.Round(change*100) / 100}
}

func xirrErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// package investmentdashboards

// EntityPerformanceRow matches the frontend sample: label is scheme name, YTD P&L value
type EntityPerformanceRow struct {
	Label string  `json:"label"`
	YTD   float64 `json:"YTD P&L"`
}

// GetEntityPerformance returns per-scheme YTD P&L (since FY start) for an entity with optional filters
// Request JSON: { "entity_name": "", "amc_name": "", "scheme_category": "", "scheme_sub_category": "", "limit": 10 }
// OPTIMIZED: Entity validation, proper context, single CTE query
func GetEntityPerformance(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}
		var req struct {
			EntityName     string `json:"entity_name,omitempty"`
			AMCName        string `json:"amc_name,omitempty"`
			SchemeCategory string `json:"scheme_category,omitempty"`
			SchemeSubCat   string `json:"scheme_sub_category,omitempty"`
			Limit          int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		if req.Limit <= 0 {
			req.Limit = 50
		}

		fyStart := getFinancialYearStart(time.Now())

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		// QUERY 1: Summary rows for chart
		// Compute true YTD P&L per scheme: Closing - Opening - (Buys - Sells)
		q := `
				WITH params AS (
						SELECT $1::date AS fy_start, $2::text AS entity_filter, $3::text AS amc_filter, $4::text[] AS allowed_entities
				),
				-- Transactions since FY start
				tx AS (
						SELECT COALESCE(ot.scheme_id, ot.scheme_internal_code, ms.scheme_id::text) AS scheme_ref,
									 COALESCE(ms.scheme_name, ot.scheme_internal_code, ot.scheme_id::text) AS scheme_name,
									 COALESCE(ms.amc_name,'') AS amc_name,
									 SUM(CASE WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in') THEN COALESCE(ot.amount,0) ELSE 0 END) AS buys_since,
									 SUM(CASE WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN COALESCE(ot.amount,0) ELSE 0 END) AS sells_since
						FROM investment.onboard_transaction ot
						LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id), params p
						WHERE ot.transaction_date >= p.fy_start
							AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name,'') = p.entity_filter)
							AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name,'') = ANY(p.allowed_entities))
						GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code, ms.scheme_id::text), COALESCE(ms.scheme_name, ot.scheme_internal_code, ot.scheme_id::text), ms.amc_name
				),
				-- Closing values (current)
				pv AS (
						SELECT COALESCE(ps.scheme_id, ps.isin, ps.scheme_name::text, '') AS scheme_ref, SUM(COALESCE(ps.current_value,0)) AS current_value
						FROM investment.portfolio_snapshot ps, params p
						WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
							AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
						GROUP BY COALESCE(ps.scheme_id, ps.isin, ps.scheme_name::text, '')
				),
				-- Opening values as of fy_start (take latest snapshot before or on fy_start per scheme)
				start_snap AS (
						SELECT DISTINCT ON (COALESCE(ps.scheme_id, ps.isin)) COALESCE(ps.scheme_id, ps.isin, ps.scheme_name::text) AS scheme_ref,
									 COALESCE(ps.current_value,0)::numeric AS start_value
						FROM investment.portfolio_snapshot ps, params p
						WHERE ps.created_at <= p.fy_start
							AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
							AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
						ORDER BY COALESCE(ps.scheme_id, ps.isin), ps.created_at DESC
				)
				SELECT COALESCE(t.scheme_name,'Unknown') AS scheme_name,
							 (COALESCE(pv.current_value,0) - COALESCE(s.start_value,0) - (COALESCE(t.buys_since,0) - COALESCE(t.sells_since,0)))::float8 AS ytd_pl,
							 t.amc_name
				FROM tx t
				LEFT JOIN pv ON pv.scheme_ref = t.scheme_ref
				LEFT JOIN start_snap s ON s.scheme_ref = t.scheme_ref, params p
				WHERE (p.amc_filter IS NULL OR t.amc_name = p.amc_filter)
				ORDER BY ytd_pl DESC
				LIMIT $5
				`

		rows, err := pgxPool.Query(ctx, q, fyStart.Format(constants.DateFormat), nullIfEmpty(entityFilter), nullIfEmpty(req.AMCName), allowedParam, req.Limit)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := make([]EntityPerformanceRow, 0, req.Limit)
		for rows.Next() {
			var name, amc string
			var ytd float64
			if err := rows.Scan(&name, &ytd, &amc); err != nil {
				continue
			}
			out = append(out, EntityPerformanceRow{Label: name, YTD: ytd})
		}

		// RAW TABULAR DATA: Portfolio snapshot rows that create the performance numbers
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		// RAW TABULAR DATA: YTD transactions contributing to performance
		rawTransactions := fetchRawTransactionDetails(ctx, pgxPool, entityFilter, allowedEntities, fyStart)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows":               out,
			"portfolio_detail":   aumDetails,
			"transaction_detail": rawTransactions,
			"generated_at":       time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// ConsolidatedRiskRow is the response structure for consolidated risk
type ConsolidatedRiskRow struct {
	LCR         float64 `json:"lcr"`
	TotalValue  float64 `json:"total_value"`
	LowValue    float64 `json:"low_value"`
	MediumValue float64 `json:"medium_value"`
	HighValue   float64 `json:"high_value"`
	GeneratedAt string  `json:"generated_at"`
}

// GetConsolidatedRisk computes a weighted internal risk score (0-100) for an entity
// based on scheme internal risk rating (Low/Medium/High) and current investment values.
// OPTIMIZED: Entity validation, proper context
func GetConsolidatedRisk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		// Mapping: Low -> 15, Medium -> 50, High -> 85 (gives values within the gauge zones)
		q := `
		WITH params AS (
			SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities
		)
        SELECT
          COALESCE(
            SUM(val * (
                CASE LOWER(COALESCE(ms.internal_risk_rating, 'medium'))
                  WHEN 'low' THEN 15
                  WHEN 'medium' THEN 50
                  WHEN 'high' THEN 85
                  ELSE 50
                END
            )) / NULLIF(SUM(val), 0),
            0
          )::float8 AS lcr,
          COALESCE(SUM(val),0)::float8 AS total_value,
          COALESCE(SUM(CASE WHEN LOWER(COALESCE(ms.internal_risk_rating,'medium')) = 'low' THEN val ELSE 0 END),0)::float8 AS low_value,
          COALESCE(SUM(CASE WHEN LOWER(COALESCE(ms.internal_risk_rating,'medium')) = 'medium' THEN val ELSE 0 END),0)::float8 AS medium_value,
          COALESCE(SUM(CASE WHEN LOWER(COALESCE(ms.internal_risk_rating,'medium')) = 'high' THEN val ELSE 0 END),0)::float8 AS high_value
        FROM (
          SELECT COALESCE(ps.current_value::numeric, (ps.total_units::numeric * ps.current_nav::numeric), 0) AS val,
                 ps.scheme_id, ps.isin, ps.entity_name
          FROM investment.portfolio_snapshot ps, params p
          WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
            AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
        ) ps
        LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ps.scheme_id OR ms.internal_scheme_code = ps.scheme_id OR ms.isin = ps.isin)
        `

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		row := pgxPool.QueryRow(ctx, q, nullIfEmpty(entityFilter), allowedParam)

		var out ConsolidatedRiskRow
		if err := row.Scan(&out.LCR, &out.TotalValue, &out.LowValue, &out.MediumValue, &out.HighValue); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		out.GeneratedAt = time.Now().UTC().Format(time.RFC3339)

		// RAW TABULAR DATA: Portfolio details with risk ratings
		aumDetails := fetchAUMDetailsWithRisk(ctx, pgxPool, entityFilter, allowedEntities)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows": map[string]interface{}{
				"generated_at":     out.GeneratedAt,
				"high_value":       out.HighValue,
				"lcr":              out.LCR,
				"low_value":        out.LowValue,
				"medium_value":     out.MediumValue,
				"portfolio_detail": aumDetails,
				"total_value":      out.TotalValue,
			},
			"success": true,
		})
	}
}

type WaterfallRow struct {
	Label        string   `json:"label"`
	Contribution *float64 `json:"contribution,omitempty"`
	OpeningAUM   *float64 `json:"opening_aum,omitempty"`
	ClosingAUM   *float64 `json:"closing_aum,omitempty"`
}

// GetAMCWaterfall computes opening/closing AUM per AMC and returns waterfall-style rows
// OPTIMIZED: Entity validation, proper context
func GetAMCWaterfall(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName  string `json:"entity_name,omitempty"`
			PeriodStart string `json:"period_start,omitempty"` // YYYY-MM-DD
			PeriodEnd   string `json:"period_end,omitempty"`   // YYYY-MM-DD
			Limit       int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		// default date range: start = financial year start (1 Apr), end = today
		now := time.Now().UTC()
		if req.PeriodEnd == "" {
			req.PeriodEnd = now.Format(constants.DateFormat)
		}
		if req.PeriodStart == "" {
			fyStart := getFinancialYearStart(now)
			req.PeriodStart = fyStart.Format(constants.DateFormat)
		}

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		q := `
		WITH params AS (
			SELECT $1::text AS entity_filter, $4::text[] AS allowed_entities
		),
        start_snap AS (
          SELECT DISTINCT ON (COALESCE(ps.scheme_id, ps.isin)) COALESCE(ps.scheme_id, ps.isin)::text AS scheme_ref,
                 ps.total_units::numeric AS total_units, ps.avg_nav::numeric AS avg_nav, ps.current_nav::numeric AS current_nav,
                 ps.current_value::numeric AS current_value, ps.entity_name
          FROM investment.portfolio_snapshot ps, params p
          WHERE ps.created_at <= $2::date
            AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
            AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
          ORDER BY COALESCE(ps.scheme_id, ps.isin), ps.created_at DESC
        ), end_snap AS (
          SELECT DISTINCT ON (COALESCE(ps.scheme_id, ps.isin)) COALESCE(ps.scheme_id, ps.isin)::text AS scheme_ref,
                 ps.total_units::numeric AS total_units, ps.avg_nav::numeric AS avg_nav, ps.current_nav::numeric AS current_nav,
                 ps.current_value::numeric AS current_value, ps.entity_name
          FROM investment.portfolio_snapshot ps, params p
          WHERE ps.created_at <= $3::date
            AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
            AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
          ORDER BY COALESCE(ps.scheme_id, ps.isin), ps.created_at DESC
        ), start_amc AS (
          SELECT COALESCE(ms.amc_name,'') AS amc_name,
                 SUM(COALESCE(s.total_units,0) * COALESCE(s.avg_nav,0))::numeric AS start_value
          FROM start_snap s
          LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = s.scheme_ref OR ms.internal_scheme_code = s.scheme_ref OR ms.isin = s.scheme_ref)
          GROUP BY COALESCE(ms.amc_name,'')
        ), end_amc AS (
          SELECT COALESCE(ms.amc_name,'') AS amc_name,
                 SUM(COALESCE(e.total_units,0) * COALESCE(e.current_nav,0))::numeric AS end_value
          FROM end_snap e
          LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = e.scheme_ref OR ms.internal_scheme_code = e.scheme_ref OR ms.isin = e.scheme_ref)
          GROUP BY COALESCE(ms.amc_name,'')
        ), amc_delta AS (
          SELECT COALESCE(e.amc_name, s.amc_name) AS amc_name,
                 COALESCE(s.start_value,0)::float8 AS start_value,
                 COALESCE(e.end_value,0)::float8 AS end_value,
                 (COALESCE(e.end_value,0) - COALESCE(s.start_value,0))::float8 AS delta
          FROM start_amc s
          FULL OUTER JOIN end_amc e ON e.amc_name = s.amc_name
        ), totals AS (
          SELECT SUM(start_value)::float8 AS opening_total, SUM(end_value)::float8 AS closing_total FROM amc_delta
        )
        SELECT ad.amc_name, ad.start_value, ad.end_value, ad.delta, t.opening_total, t.closing_total
        FROM amc_delta ad, totals t
        ORDER BY ad.delta DESC
        `

		rows, err := pgxPool.Query(ctx, q, nullIfEmpty(entityFilter), req.PeriodStart, req.PeriodEnd, allowedParam)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		type rowOut struct {
			AMCName      string  `json:"amc_name"`
			StartValue   float64 `json:"start_value"`
			EndValue     float64 `json:"end_value"`
			Delta        float64 `json:"delta"`
			OpeningTotal float64 `json:"opening_total"`
			ClosingTotal float64 `json:"closing_total"`
		}

		amcRows := make([]rowOut, 0)
		var openingTotal, closingTotal float64
		for rows.Next() {
			var r rowOut
			if err := rows.Scan(&r.AMCName, &r.StartValue, &r.EndValue, &r.Delta, &r.OpeningTotal, &r.ClosingTotal); err != nil {
				continue
			}
			openingTotal = r.OpeningTotal
			closingTotal = r.ClosingTotal
			amcRows = append(amcRows, r)
		}

		// If opening total is zero (no snapshots), fallback to holdings-based AUM as of period start
		if openingTotal == 0 {
			// parse period start
			if t, err := time.Parse(constants.DateFormat, req.PeriodStart); err == nil {
				openingTotal = getAUMAtDate(ctx, pgxPool, entityFilter, allowedEntities, t)
			}
		}

		// Build waterfall rows: Opening, each AMC delta, Closing
		out := make([]WaterfallRow, 0)
		ot := openingTotal
		out = append(out, WaterfallRow{Label: "Opening AUM", OpeningAUM: &ot})

		for _, r := range amcRows {
			d := r.Delta
			out = append(out, WaterfallRow{Label: r.AMCName, Contribution: &d})
		}

		ct := closingTotal
		out = append(out, WaterfallRow{Label: "Closing AUM", ClosingAUM: &ct})

		// RAW TABULAR DATA: Portfolio snapshot rows that create the waterfall
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows":             out,
			"opening_total":    openingTotal,
			"closing_total":    closingTotal,
			"portfolio_detail": aumDetails,
			"generated_at":     time.Now().UTC().Format(time.RFC3339),
		})
	}
}

type AMCPerfRow struct {
	AMCName      string   `json:"amc_name"`
	StartValue   float64  `json:"start_value"`
	CurrentValue float64  `json:"current_value"`
	PnL          float64  `json:"pnl"`
	PnLPercent   *float64 `json:"pnl_percent"`
}

// GetAMCPerformance returns per-AMC AUM at FY start (1 Apr) and now, with P&L
// OPTIMIZED: Entity validation, proper context
func GetAMCPerformance(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}
		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Limit      int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		if req.Limit <= 0 {
			req.Limit = 100
		}

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		q := `
		WITH params AS (
			SELECT $1::text AS entity_filter, $3::text[] AS allowed_entities
		)
        SELECT
            COALESCE(ms.amc_name, '') AS amc_name,
            SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0))::float8 AS start_value,
            SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.current_nav::numeric,0))::float8 AS current_value,
            (SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.current_nav::numeric,0)) - SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0)))::float8 AS pnl,
            CASE WHEN SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0)) = 0 THEN 0
                     ELSE ((SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.current_nav::numeric,0)) - SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0))) / SUM(COALESCE(ps.total_units::numeric,0) * COALESCE(ps.avg_nav::numeric,0))) * 100
            END AS pnl_percent
        FROM investment.portfolio_snapshot ps
        LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ps.scheme_id OR ms.internal_scheme_code = ps.scheme_id OR ms.isin = ps.isin), params p
        WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
          AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
        GROUP BY COALESCE(ms.amc_name, '')
        ORDER BY pnl DESC
        LIMIT $2::int
        `

		rows, err := pgxPool.Query(ctx, q, nullIfEmpty(entityFilter), req.Limit, allowedParam)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := make([]AMCPerfRow, 0)
		var totalCurrentValue float64
		for rows.Next() {
			var (
				name     string
				startVal float64
				currVal  float64
				pnl      float64
				pct      sql.NullFloat64
				pctPtr   *float64
			)

			if err := rows.Scan(&name, &startVal, &currVal, &pnl, &pct); err != nil {
				continue
			}
			if pct.Valid {
				v := pct.Float64
				pctPtr = &v
			} else {
				pctPtr = nil
			}

			out = append(out, AMCPerfRow{AMCName: name, StartValue: startVal, CurrentValue: currVal, PnL: pnl, PnLPercent: pctPtr})
			totalCurrentValue += currVal
		}

		// RAW TABULAR DATA: Portfolio snapshot rows that create AMC performance numbers
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows":             out,
			"portfolio_detail": aumDetails,
			"generated_at":     time.Now().UTC().Format(time.RFC3339),
		})
	}
}

type TopAssetRow struct {
	Title    string  `json:"title"`
	Subtitle string  `json:"subtitle"`
	Pct      string  `json:"pct"`
	Value    float64 `json:"value"`
}

// GetTopPerformingAssets returns top N performing assets using snapshot only
// OPTIMIZED: Entity validation, proper context
func GetTopPerformingAssets(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Limit      int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		// default limit
		if req.Limit <= 0 {
			req.Limit = 3
		}

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		// Clean, simple SQL with entity validation
		q := `
		WITH params AS (
			SELECT $1::text AS entity_filter, $3::text[] AS allowed_entities
		)
		SELECT
			ps.scheme_name,
			COALESCE(ms.amc_name, '') AS amc_name,
			(ps.total_units::numeric * ps.avg_nav::numeric)::float8 AS start_value,
			(ps.total_units::numeric * ps.current_nav::numeric)::float8 AS end_value,
			CASE 
				WHEN ps.avg_nav = 0 THEN NULL
				ELSE (((ps.total_units * ps.current_nav) - (ps.total_units * ps.avg_nav)) / NULLIF((ps.total_units * ps.avg_nav), 0)) * 100
			END AS pct
		FROM investment.portfolio_snapshot ps
		LEFT JOIN investment.masterscheme ms ON (
			ms.scheme_id = ps.scheme_id OR 
			ms.internal_scheme_code = ps.scheme_id OR 
			ms.isin = ps.isin
		), params p
		WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
		  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
		ORDER BY pct DESC NULLS LAST
		LIMIT $2;
		`

		rows, err := pgxPool.Query(ctx, q, nullIfEmpty(entityFilter), req.Limit, allowedParam)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := make([]TopAssetRow, 0)

		for rows.Next() {
			var (
				name   string
				amc    string
				startV float64
				endV   float64
				pct    sql.NullFloat64
			)

			if err := rows.Scan(&name, &amc, &startV, &endV, &pct); err != nil {
				continue
			}

			pctStr := "0%"

			if pct.Valid {
				pctStr = formatPercent(pct.Float64)
			}

			out = append(out, TopAssetRow{
				Title:    name,
				Subtitle: amc,
				Pct:      pctStr,
				Value:    endV,
			})
		}

		// RAW TABULAR DATA: Full portfolio snapshot for top performers
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows":             out,
			"portfolio_detail": aumDetails,
			"generated_at":     time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// Format percentage nicely
func formatPercent(v float64) string {
	sign := ""
	if v > 0 {
		sign = "+"
	}
	return fmt.Sprintf("%s%.1f%% YTD", sign, v)
}

// GetAUMCompositionTrend returns monthly AUM breakdown by AMC for stacked area chart (Financial Year: Apr-Mar)
// Request JSON: { "entity_name": "optional", "year": 2025 } (year = FY start year, e.g., 2025 means FY 2025-26: Apr 2025 to Mar 2026)
// Response: { rows: [{ month: "Apr", "AMC1": 1000, "AMC2": 2000, ... }, ...], amc_names: [...] }
//
// OPTIMIZED: Single batch query for all months instead of 12+ queries in loop
// Uses entity validation from context
func GetAUMCompositionTrend(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Year       int    `json:"year,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		now := time.Now().UTC()
		if req.Year <= 0 {
			if now.Month() >= time.April {
				req.Year = now.Year()
			} else {
				req.Year = now.Year() - 1
			}
		}

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		// Generate month boundaries
		type monthInfo struct {
			Label     string
			MonthIdx  int
			EndDate   string
			IsCurrent bool
		}
		fyMonths := []time.Month{
			time.April, time.May, time.June, time.July, time.August, time.September,
			time.October, time.November, time.December, time.January, time.February, time.March,
		}
		monthLabels := []string{"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"}

		months := make([]monthInfo, 0, 12)
		monthDates := make([]string, 0, 12) // For SQL array parameter

		for i, m := range fyMonths {
			year := req.Year
			if i >= 9 { // Jan, Feb, Mar are in next calendar year
				year = req.Year + 1
			}
			firstOfMonth := time.Date(year, m, 1, 0, 0, 0, 0, time.UTC)
			lastOfMonth := firstOfMonth.AddDate(0, 1, -1)

			if lastOfMonth.After(now) {
				if firstOfMonth.Before(now) || firstOfMonth.Equal(now) {
					months = append(months, monthInfo{
						Label:     monthLabels[i],
						MonthIdx:  i,
						EndDate:   now.Format(constants.DateFormat),
						IsCurrent: true,
					})
					monthDates = append(monthDates, now.Format(constants.DateFormat))
				}
				continue
			}

			months = append(months, monthInfo{
				Label:     monthLabels[i],
				MonthIdx:  i,
				EndDate:   lastOfMonth.Format(constants.DateFormat),
				IsCurrent: false,
			})
			monthDates = append(monthDates, lastOfMonth.Format(constants.DateFormat))
		}

		if len(months) == 0 {
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"rows": []interface{}{}, "amc_names": []string{}, "fy_label": fmt.Sprintf(constants.FormatFiscalYear, req.Year, (req.Year+1)%100), "generated_at": time.Now().UTC().Format(time.RFC3339),
			})
			return
		}

		// OPTIMIZED: Single batch query for all months using UNNEST and lateral join
		// This computes AUM for all months in one database round-trip
		batchQuery := `
		WITH params AS (
			SELECT $1::text AS entity_filter, $3::text[] AS allowed_entities
		),
		month_dates AS (
			SELECT ordinality - 1 AS month_idx, month_end::date AS month_end
			FROM UNNEST($2::date[]) WITH ORDINALITY AS t(month_end, ordinality)
		),
		-- Get current AUM from portfolio_snapshot (for current month only)
		current_snapshot AS (
			SELECT COALESCE(ms.amc_name, 'Unknown') AS amc_name,
			       SUM(COALESCE(ps.current_value, 0))::float8 AS aum_value
			FROM investment.portfolio_snapshot ps
			LEFT JOIN investment.masterscheme ms ON (
				ms.scheme_id = ps.scheme_id OR ms.internal_scheme_code = ps.scheme_id OR ms.isin = ps.isin
			), params p
			WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
			GROUP BY COALESCE(ms.amc_name, 'Unknown')
		),
		-- For each historical month, compute units held and value
		historical_aum AS (
			SELECT md.month_idx,
			       COALESCE(ot.amc_name, 'Unknown') AS amc_name,
			       SUM(
			       	CASE 
			       		WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest') 
			       		THEN COALESCE(ot.units, 0)
			       		WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') 
			       		THEN -COALESCE(ot.units, 0)
			       		ELSE 0
					END * COALESCE(nav.nav_value, ot.nav, 10)
			       )::float8 AS aum_value
			FROM month_dates md
			CROSS JOIN LATERAL (
				SELECT ot.*, ms.amc_name, ms.amfi_scheme_code
				FROM investment.onboard_transaction ot
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id
				), params p
				WHERE ot.transaction_date <= md.month_end
				  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name, '') = p.entity_filter)
				  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name, '') = ANY(p.allowed_entities))
			) ot
			LEFT JOIN LATERAL (
				SELECT nav_value FROM investment.amfi_nav_staging
				WHERE scheme_code::text = ot.amfi_scheme_code::text
				  AND nav_date <= md.month_end
				ORDER BY nav_date DESC LIMIT 1
			) nav ON true
			WHERE md.month_idx < (SELECT MAX(month_idx) FROM month_dates) -- All except current
			GROUP BY md.month_idx, COALESCE(ot.amc_name, 'Unknown')
		),
		-- Combine: use current_snapshot for the last month, historical for others
		combined AS (
			SELECT month_idx, amc_name, aum_value FROM historical_aum
			UNION ALL
			SELECT (SELECT MAX(month_idx) FROM month_dates) AS month_idx, amc_name, aum_value FROM current_snapshot
		)
		SELECT month_idx, amc_name, COALESCE(SUM(aum_value), 0)::float8 AS aum_value
		FROM combined
		GROUP BY month_idx, amc_name
		ORDER BY month_idx, amc_name
		`

		rows, err := pgxPool.Query(ctx, batchQuery, nullIfEmpty(entityFilter), monthDates, allowedParam)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		// Build results map: monthIdx -> amcName -> value
		amcSet := make(map[string]bool)
		resultMap := make(map[int]map[string]float64)
		for _, m := range months {
			resultMap[m.MonthIdx] = make(map[string]float64)
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

		// Convert to sorted AMC list
		amcNames := make([]string, 0, len(amcSet))
		for amc := range amcSet {
			amcNames = append(amcNames, amc)
		}
		// Simple sort
		for i := 0; i < len(amcNames); i++ {
			for j := i + 1; j < len(amcNames); j++ {
				if amcNames[i] > amcNames[j] {
					amcNames[i], amcNames[j] = amcNames[j], amcNames[i]
				}
			}
		}

		// Build output rows
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

		// RAW TABULAR DATA: Full portfolio snapshot with AMC breakdown
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"rows": map[string]interface{}{
				"amc_names":        amcNames,
				"fy_label":         fmt.Sprintf(constants.FormatFiscalYear, req.Year, (req.Year+1)%100),
				"generated_at":     time.Now().UTC().Format(time.RFC3339),
				"portfolio_detail": aumDetails,
				"rows":             outRows,
			},
			"success": true,
		})
	}
}

// pgxRows interface to handle both query result types
type pgxRows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close()
}

// AUMMovementData represents the waterfall bridge chart data
type AUMMovementData struct {
	Opening           float64 `json:"opening"`             // Opening Balance (Grey) - AUM at start
	Inflows           float64 `json:"inflows"`             // Money added - Buy/Subscription (Green)
	MarketGainsLosses float64 `json:"market_gains_losses"` // Unrealized + Realized gains (Green/Red)
	Income            float64 `json:"income"`              // Dividend/Interest received (Green)
	Outflows          float64 `json:"outflows"`            // Money withdrawn - Sell/Redemption (Red)
	Closing           float64 `json:"closing"`             // Closing Balance (Grey) - Current AUM
}

// GetAUMMovementWaterfall returns the AUM movement waterfall/bridge chart data
// Request JSON: { "entity_name": "optional", "period_start": "YYYY-MM-DD", "period_end": "YYYY-MM-DD" }
// Response: { opening, inflows, market_gains_losses, income, outflows, closing }
//
// OPTIMIZED: Single CTE query instead of 5 sequential queries, entity validation
//
// Waterfall equation: Opening + Inflows + Market Gains + Income - Outflows = Closing
func GetAUMMovementWaterfall(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName  string `json:"entity_name,omitempty"`
			PeriodStart string `json:"period_start,omitempty"`
			PeriodEnd   string `json:"period_end,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		now := time.Now().UTC()

		if req.PeriodEnd == "" {
			req.PeriodEnd = now.Format(constants.DateFormat)
		}
		if req.PeriodStart == "" {
			fyStart := getFinancialYearStart(now)
			req.PeriodStart = fyStart.Format(constants.DateFormat)
		}

		periodStart, _ := time.Parse(constants.DateFormat, req.PeriodStart)
		openingDate := periodStart.AddDate(0, 0, -1).Format(constants.DateFormat)

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		// OPTIMIZED: Single comprehensive CTE query for all waterfall components
		waterfallQuery := `
		WITH params AS (
			SELECT $1::text AS entity_filter, $2::date AS opening_date, $3::date AS period_start, $4::date AS period_end, $5::text[] AS allowed_entities
		),
		-- Current closing AUM from portfolio_snapshot
		closing_aum AS (
			SELECT COALESCE(SUM(current_value), 0)::float8 AS closing
			FROM investment.portfolio_snapshot ps, params p
			WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
		),
		-- Opening AUM: units held at opening_date × NAV at that date
		units_at_start AS (
			SELECT 
				COALESCE(ot.scheme_id, ot.scheme_internal_code) AS scheme_ref,
				ms.amfi_scheme_code,
				SUM(
					CASE 
						WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest','idcw_reinvest') 
						THEN COALESCE(ot.units, 0)
						WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') 
						THEN -COALESCE(ot.units, 0)
						ELSE 0
					END
				) AS total_units
			FROM investment.onboard_transaction ot
			LEFT JOIN investment.masterscheme ms ON (
				ms.scheme_id = ot.scheme_id OR ms.internal_scheme_code = ot.scheme_internal_code OR ms.isin = ot.scheme_id
			), params p
			WHERE ot.transaction_date <= p.opening_date
			  AND (p.entity_filter IS NULL OR COALESCE(ot.entity_name, '') = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name, '') = ANY(p.allowed_entities))
			GROUP BY COALESCE(ot.scheme_id, ot.scheme_internal_code), ms.amfi_scheme_code
			HAVING SUM(
				CASE 
					WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','switch_in','bonus','merger_in','dividend_reinvest','idcw_reinvest') 
					THEN COALESCE(ot.units, 0)
					WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out','merger_out') 
					THEN -COALESCE(ot.units, 0)
					ELSE 0
				END
			) > 0
		),
		navs_at_start AS (
			SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_code, nav_value
			FROM investment.amfi_nav_staging, params p
			WHERE nav_date <= p.opening_date
			ORDER BY scheme_code, nav_date DESC
		),
		opening_aum AS (
			SELECT COALESCE(SUM(u.total_units * COALESCE(n.nav_value, 0)), 0)::float8 AS opening
			FROM units_at_start u
			LEFT JOIN navs_at_start n ON n.scheme_code = u.amfi_scheme_code::text
		),
		-- Period flows: inflows, outflows, income in one pass
		period_flows AS (
			SELECT 
				COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('buy','purchase','subscription','switch_in') THEN amount ELSE 0 END), 0)::float8 AS inflows,
				COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('sell','redemption','switch_out') THEN amount ELSE 0 END), 0)::float8 AS outflows,
				COALESCE(SUM(CASE WHEN LOWER(transaction_type) IN ('dividend','interest','dividend_payout','idcw','idcw_payout') THEN amount ELSE 0 END), 0)::float8 AS income
			FROM investment.onboard_transaction, params p
			WHERE transaction_date >= p.period_start AND transaction_date <= p.period_end
			  AND (p.entity_filter IS NULL OR COALESCE(entity_name, '') = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR COALESCE(entity_name, '') = ANY(p.allowed_entities))
		)
		SELECT o.opening, pf.inflows, pf.outflows, pf.income, c.closing
		FROM opening_aum o, period_flows pf, closing_aum c
		`

		var openingAUM, inflows, outflows, income, closingAUM float64
		err := pgxPool.QueryRow(ctx, waterfallQuery, nullIfEmpty(entityFilter), openingDate, req.PeriodStart, req.PeriodEnd, allowedParam).
			Scan(&openingAUM, &inflows, &outflows, &income, &closingAUM)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Market Gains = Closing - Opening - Inflows + Outflows - Income
		marketGainsLosses := closingAUM - openingAUM - inflows + outflows - income

		// Build waterfall sequence
		marketType := "positive"
		if marketGainsLosses < 0 {
			marketType = "negative"
		}

		waterfall := []map[string]interface{}{
			{"label": "Opening Balance", "value": openingAUM, "type": "total"},
			{"label": "Inflows", "value": inflows, "type": "positive"},
			{"label": "Market Gains/Losses", "value": marketGainsLosses, "type": marketType},
			{"label": "Income", "value": income, "type": "positive"},
			{"label": "Outflows", "value": outflows, "type": "negative"},
			{"label": "Closing Balance", "value": closingAUM, "type": "total"},
		}

		// RAW TABULAR DATA: Portfolio snapshot and transactions that create the waterfall
		fyStart := getFinancialYearStart(time.Now())
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)
		rawTransactions := fetchRawTransactionDetails(ctx, pgxPool, entityFilter, allowedEntities, fyStart)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"opening": openingAUM, "inflows": inflows, "market_gains_losses": marketGainsLosses,
			"income": income, "outflows": outflows, "closing": closingAUM,
			"waterfall": waterfall, "portfolio_detail": aumDetails, "transaction_detail": rawTransactions,
			"period_start": req.PeriodStart, "period_end": req.PeriodEnd,
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// AUMBreakdownItem represents a slice in the donut chart
type AUMBreakdownItem struct {
	Label  string  `json:"label"`
	Amount float64 `json:"amount"`
}

// GetAUMBreakdown returns AUM breakdown for donut chart with dynamic grouping
// Request JSON: { "entity_name": "optional", "group_by": "amc|scheme|entity" }
// Response: { breakdown: [{ label: "AMC Name", amount: 1000000 }, ...], total: 5000000, group_by: "amc" }
// OPTIMIZED: Entity validation added
func GetAUMBreakdown(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			GroupBy    string `json:"group_by,omitempty"` // amc, scheme, entity
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		// Default to "amc" grouping
		if req.GroupBy == "" {
			req.GroupBy = "amc"
		}

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		var query string
		var args []interface{}

		switch req.GroupBy {
		case "scheme":
			query = `
			WITH params AS (SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities)
			SELECT COALESCE(ps.scheme_name, 'Unknown') AS label, SUM(COALESCE(ps.current_value, 0))::float8 AS amount
			FROM investment.portfolio_snapshot ps, params p
			WHERE COALESCE(ps.current_value, 0) > 0
			  AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
			GROUP BY COALESCE(ps.scheme_name, 'Unknown') ORDER BY amount DESC
			`
			args = []interface{}{nullIfEmpty(entityFilter), allowedParam}

		case "entity":
			query = `
			WITH params AS (SELECT $1::text[] AS allowed_entities)
			SELECT COALESCE(ps.entity_name, 'Unknown') AS label, SUM(COALESCE(ps.current_value, 0))::float8 AS amount
			FROM investment.portfolio_snapshot ps, params p
			WHERE COALESCE(ps.current_value, 0) > 0
			  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
			GROUP BY COALESCE(ps.entity_name, 'Unknown') ORDER BY amount DESC
			`
			args = []interface{}{allowedParam}

		default: // "amc"
			query = `
			WITH params AS (SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities)
			SELECT COALESCE(ms.amc_name, 'Unknown') AS label, SUM(COALESCE(ps.current_value, 0))::float8 AS amount
			FROM investment.portfolio_snapshot ps
			LEFT JOIN investment.masterscheme ms ON (ms.scheme_id = ps.scheme_id OR ms.internal_scheme_code = ps.scheme_id OR ms.isin = ps.isin), params p
			WHERE COALESCE(ps.current_value, 0) > 0
			  AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
			GROUP BY COALESCE(ms.amc_name, 'Unknown') ORDER BY amount DESC
			`
			args = []interface{}{nullIfEmpty(entityFilter), allowedParam}
			req.GroupBy = "amc"
		}

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		breakdown := make([]AUMBreakdownItem, 0, 20)
		var total float64

		for rows.Next() {
			var item AUMBreakdownItem
			if err := rows.Scan(&item.Label, &item.Amount); err != nil {
				continue
			}
			breakdown = append(breakdown, item)
			total += item.Amount
		}

		// Calculate percentages
		breakdownWithPct := make([]map[string]interface{}, 0, len(breakdown))
		for _, item := range breakdown {
			pct := 0.0
			if total > 0 {
				pct = (item.Amount / total) * 100
			}
			breakdownWithPct = append(breakdownWithPct, map[string]interface{}{
				"label": item.Label, "amount": item.Amount, "percentage": math.Round(pct*100) / 100,
			})
		}

		// RAW TABULAR DATA: Portfolio snapshot that creates the breakdown
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"breakdown": breakdownWithPct, "total": total, "group_by": req.GroupBy,
			"portfolio_detail": aumDetails, "generated_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// AttributionItem represents a single bar in the performance attribution waterfall
type AttributionItem struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

// GetPerformanceAttribution returns Brinson-Fachler style performance attribution
// Shows: Benchmark Return → Allocation Effect → Selection Effect → Portfolio Return
// Answers: "The market gave 10%. We got 12%. Where did the extra 2% come from?"
// Request JSON: { "entity_name": "", "year": 2024, "benchmark": constants.Nifty50 }
func GetPerformanceAttribution(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Year       int    `json:"year,omitempty"`
			Benchmark  string `json:"benchmark,omitempty"` // e.g., constants.Nifty50, "NIFTY 100"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		// Default to current financial year
		now := time.Now()
		if req.Year == 0 {
			if now.Month() >= time.April {
				req.Year = now.Year()
			} else {
				req.Year = now.Year() - 1
			}
		}
		if req.Benchmark == "" {
			req.Benchmark = constants.Nifty50
		}

		// Financial year boundaries
		fyStart := time.Date(req.Year, time.April, 1, 0, 0, 0, 0, time.UTC)
		fyEnd := time.Date(req.Year+1, time.March, 31, 23, 59, 59, 0, time.UTC)
		if fyEnd.After(now) {
			fyEnd = now
		}

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		// 1) Calculate Portfolio Return using XIRR
		txnQuery := `
			WITH params AS (
				SELECT 
					$1::text AS entity_filter,
					$2::timestamp AS fy_start,
					$3::timestamp AS fy_end,
					$4::text[] AS allowed_entities
			)
			SELECT 
				transaction_date,
				CASE 
					WHEN transaction_type IN ('BUY','SWITCH_IN','PURCHASE','SIP') THEN -ABS(COALESCE(amount,0))
					ELSE ABS(COALESCE(amount,0))
				END AS flow
			FROM investment.onboard_transaction, params p
			WHERE (p.entity_filter IS NULL OR entity_name = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR entity_name = ANY(p.allowed_entities))
			  AND transaction_date >= p.fy_start AND transaction_date <= p.fy_end
			ORDER BY transaction_date
		`
		txnRows, err := pgxPool.Query(ctx, txnQuery, nullIfEmpty(entityFilter), fyStart, fyEnd, allowedEntities)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer txnRows.Close()

		var flows []CashFlow
		for txnRows.Next() {
			var dt time.Time
			var amt float64
			if err := txnRows.Scan(&dt, &amt); err != nil {
				continue
			}
			flows = append(flows, CashFlow{Date: dt, Amount: amt})
		}

		// Add terminal value (current portfolio value)
		var terminalValue float64
		tvQuery := `
			SELECT COALESCE(SUM(current_value), 0)
			FROM investment.portfolio_snapshot
			WHERE ($1::text IS NULL OR entity_name = $1::text)
			  AND ($2::text[] IS NULL OR entity_name = ANY($2::text[]))
		`
		_ = pgxPool.QueryRow(ctx, tvQuery, nullIfEmpty(entityFilter), allowedEntities).Scan(&terminalValue)

		if terminalValue > 0 {
			flows = append(flows, CashFlow{Date: fyEnd, Amount: terminalValue})
		}

		portfolioReturn := 0.0
		if len(flows) >= 2 {
			xirrVal, _ := ComputeXIRR(flows)
			if !math.IsNaN(xirrVal) {
				portfolioReturn = xirrVal * 100 // Convert to percentage
			}
		}

		// 2) Calculate category-wise returns for attribution analysis
		// Get portfolio weights and returns by scheme category
		categoryQuery := `
			WITH params AS (
				SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities
			),
			portfolio_categories AS (
				SELECT 
					COALESCE(asm.scheme_category, 'Other') AS category,
					SUM(COALESCE(ps.current_value, 0)) AS current_value,
					SUM(COALESCE(ps.total_invested_amount, 0)) AS invested_amount
				FROM investment.portfolio_snapshot ps
				CROSS JOIN params p
				LEFT JOIN investment.amfi_scheme_master_staging asm ON (
					asm.scheme_code::text = ps.scheme_id OR
					asm.isin_div_payout_growth = ps.isin OR
					asm.isin_div_reinvestment = ps.isin
				)
				WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
				  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
				  AND COALESCE(ps.current_value, 0) > 0
				GROUP BY COALESCE(asm.scheme_category, 'Other')
			),
			totals AS (
				SELECT 
					SUM(current_value) AS total_current,
					SUM(invested_amount) AS total_invested
				FROM portfolio_categories
			)
			SELECT 
				pc.category,
				pc.current_value,
				pc.invested_amount,
				CASE WHEN t.total_current > 0 THEN (pc.current_value / t.total_current) * 100 ELSE 0 END AS weight_pct,
				CASE WHEN pc.invested_amount > 0 THEN ((pc.current_value - pc.invested_amount) / pc.invested_amount) * 100 ELSE 0 END AS category_return
			FROM portfolio_categories pc, totals t
			ORDER BY pc.current_value DESC
		`

		catRows, err := pgxPool.Query(ctx, categoryQuery, nullIfEmpty(entityFilter), allowedEntities)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer catRows.Close()

		type CategoryData struct {
			Category       string
			CurrentValue   float64
			InvestedAmount float64
			WeightPct      float64
			CategoryReturn float64
		}

		var categories []CategoryData
		for catRows.Next() {
			var cd CategoryData
			if err := catRows.Scan(&cd.Category, &cd.CurrentValue, &cd.InvestedAmount, &cd.WeightPct, &cd.CategoryReturn); err != nil {
				continue
			}
			categories = append(categories, cd)
		}

		// 3) Calculate Benchmark Return
		// Use a proxy based on broad market return or stored benchmark data
		// For now, we'll estimate using the weighted average of category benchmark returns
		// In production, this should fetch actual NSE index data for the period

		// Benchmark category weights (typical market-cap weighted index like Nifty 50)
		benchmarkWeights := map[string]float64{
			"Equity Scheme":     60.0,
			"Debt Scheme":       25.0,
			"Hybrid Scheme":     10.0,
			"Solution Oriented": 3.0,
			"Other":             2.0,
		}

		// Benchmark expected returns per category (simplified - should use actual index returns)
		benchmarkReturns := map[string]float64{
			"Equity Scheme":     12.0, // Proxy for Nifty return
			"Debt Scheme":       7.0,  // Proxy for debt fund returns
			"Hybrid Scheme":     9.0,  // Blend
			"Solution Oriented": 8.0,
			"Other":             6.0,
		}

		// Calculate benchmark return (weighted average)
		benchmarkReturn := 0.0
		totalBenchmarkWeight := 0.0
		for cat, wt := range benchmarkWeights {
			if ret, ok := benchmarkReturns[cat]; ok {
				benchmarkReturn += wt * ret / 100
				totalBenchmarkWeight += wt
			}
		}
		if totalBenchmarkWeight > 0 {
			benchmarkReturn = (benchmarkReturn / totalBenchmarkWeight) * 100
		}

		// 4) Calculate Allocation Effect
		// Allocation Effect = Σ (Portfolio Weight - Benchmark Weight) × Benchmark Sector Return
		allocationEffect := 0.0
		for _, cat := range categories {
			benchWeight := benchmarkWeights[cat.Category]
			if benchWeight == 0 {
				benchWeight = benchmarkWeights["Other"]
			}
			benchRet := benchmarkReturns[cat.Category]
			if benchRet == 0 {
				benchRet = benchmarkReturns["Other"]
			}
			// Weight difference × benchmark return
			weightDiff := (cat.WeightPct - benchWeight) / 100
			allocationEffect += weightDiff * benchRet
		}

		// 5) Calculate Selection Effect
		// Selection Effect = Σ Portfolio Weight × (Portfolio Sector Return - Benchmark Sector Return)
		selectionEffect := 0.0
		for _, cat := range categories {
			benchRet := benchmarkReturns[cat.Category]
			if benchRet == 0 {
				benchRet = benchmarkReturns["Other"]
			}
			// Portfolio weight × excess return
			portfolioWeight := cat.WeightPct / 100
			excessReturn := cat.CategoryReturn - benchRet
			selectionEffect += portfolioWeight * excessReturn
		}

		// 6) Calculate Interaction/Other Effects (residual)
		// This captures any attribution that doesn't fit cleanly into allocation or selection
		totalExplained := benchmarkReturn + allocationEffect + selectionEffect
		otherEffects := portfolioReturn - totalExplained

		// Round values
		benchmarkReturn = math.Round(benchmarkReturn*100) / 100
		allocationEffect = math.Round(allocationEffect*100) / 100
		selectionEffect = math.Round(selectionEffect*100) / 100
		otherEffects = math.Round(otherEffects*100) / 100
		portfolioReturn = math.Round(portfolioReturn*100) / 100

		// Build attribution waterfall
		attribution := []AttributionItem{
			{Name: "Benchmark Return", Value: benchmarkReturn},
			{Name: "Allocation Effect", Value: allocationEffect},
			{Name: "Selection Effect", Value: selectionEffect},
			{Name: "Other Effects", Value: otherEffects},
			{Name: "Portfolio Return", Value: portfolioReturn},
		}

		// Category breakdown for detailed view
		categoryBreakdown := make([]map[string]interface{}, 0, len(categories))
		for _, cat := range categories {
			benchWeight := benchmarkWeights[cat.Category]
			if benchWeight == 0 {
				benchWeight = benchmarkWeights["Other"]
			}
			benchRet := benchmarkReturns[cat.Category]
			if benchRet == 0 {
				benchRet = benchmarkReturns["Other"]
			}
			categoryBreakdown = append(categoryBreakdown, map[string]interface{}{
				"category":          cat.Category,
				"portfolio_weight":  math.Round(cat.WeightPct*100) / 100,
				"benchmark_weight":  benchWeight,
				"portfolio_return":  math.Round(cat.CategoryReturn*100) / 100,
				"benchmark_return":  benchRet,
				"allocation_effect": math.Round(((cat.WeightPct-benchWeight)/100*benchRet)*100) / 100,
				"selection_effect":  math.Round((cat.WeightPct/100*(cat.CategoryReturn-benchRet))*100) / 100,
			})
		}

		// RAW TABULAR DATA: Portfolio snapshot with categories for attribution
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		response := map[string]interface{}{
			"attribution":        attribution,
			"benchmark":          req.Benchmark,
			"benchmark_return":   benchmarkReturn,
			"portfolio_return":   portfolioReturn,
			"excess_return":      math.Round((portfolioReturn-benchmarkReturn)*100) / 100,
			"allocation_effect":  allocationEffect,
			"selection_effect":   selectionEffect,
			"other_effects":      otherEffects,
			"category_breakdown": categoryBreakdown,
			"portfolio_detail":   aumDetails,
			"financial_year":     fmt.Sprintf(constants.FormatFiscalYear, req.Year, req.Year+1),
			"period_start":       fyStart.Format(constants.DateFormat),
			"period_end":         fyEnd.Format(constants.DateFormat),
			"generated_at":       time.Now().UTC().Format(time.RFC3339),
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// HeatmapCell represents a single cell in the P&L heatmap matrix
type HeatmapCell struct {
	Entity string  `json:"entity"`
	AMC    string  `json:"amc"`
	Scheme string  `json:"scheme"`
	PnL    float64 `json:"pnl"`
}

// GetDailyPnLHeatmap returns a heatmap matrix with Entity, AMC, Scheme and P&L
// Used for visualizing where gains/losses are occurring across the portfolio
// OPTIMIZED: Entity validation, proper context
// Request JSON: { "group_by": "amc" | "scheme" }
func GetDailyPnLHeatmap(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			GroupBy    string `json:"group_by,omitempty"`    // "amc" or "scheme" for aggregation level
			EntityName string `json:"entity_name,omitempty"` // Optional filter
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		// Query returns entity, amc, scheme, pnl - always all four fields
		// group_by controls the aggregation level
		var query string

		if req.GroupBy == "scheme" {
			// Detailed view: each scheme as a separate row
			query = `
				WITH params AS (
					SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities
				)
				SELECT 
					COALESCE(ps.entity_name, 'Unknown') AS entity,
					COALESCE(ms.amc_name, 'Unknown') AS amc,
					COALESCE(ps.scheme_name, 'Unknown') AS scheme,
					SUM(COALESCE(ps.gain_loss, 0))::float8 AS pnl
				FROM investment.portfolio_snapshot ps
				CROSS JOIN params p
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = ps.scheme_id OR 
					ms.internal_scheme_code = ps.scheme_id OR 
					ms.isin = ps.isin
				)
				WHERE COALESCE(ps.current_value, 0) > 0
				  AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
				  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
				GROUP BY ps.entity_name, ms.amc_name, ps.scheme_name
				HAVING SUM(COALESCE(ps.gain_loss, 0)) != 0
				ORDER BY ps.entity_name, ms.amc_name, ABS(SUM(COALESCE(ps.gain_loss, 0))) DESC
			`
		} else {
			// AMC view (default): aggregate all schemes under each AMC
			query = `
				WITH params AS (
					SELECT $1::text AS entity_filter, $2::text[] AS allowed_entities
				)
				SELECT 
					COALESCE(ps.entity_name, 'Unknown') AS entity,
					COALESCE(ms.amc_name, 'Unknown') AS amc,
					'All Schemes' AS scheme,
					SUM(COALESCE(ps.gain_loss, 0))::float8 AS pnl
				FROM investment.portfolio_snapshot ps
				CROSS JOIN params p
				LEFT JOIN investment.masterscheme ms ON (
					ms.scheme_id = ps.scheme_id OR 
					ms.internal_scheme_code = ps.scheme_id OR 
					ms.isin = ps.isin
				)
				WHERE COALESCE(ps.current_value, 0) > 0
				  AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
				  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
				GROUP BY ps.entity_name, ms.amc_name
				HAVING SUM(COALESCE(ps.gain_loss, 0)) != 0
				ORDER BY ps.entity_name, ABS(SUM(COALESCE(ps.gain_loss, 0))) DESC
			`
			req.GroupBy = "amc" // normalize
		}

		rows, err := pgxPool.Query(ctx, query, nullIfEmpty(entityFilter), allowedEntities)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		heatmap := make([]HeatmapCell, 0)
		entities := make(map[string]bool)
		amcs := make(map[string]bool)
		var totalPnL float64

		for rows.Next() {
			var cell HeatmapCell
			if err := rows.Scan(&cell.Entity, &cell.AMC, &cell.Scheme, &cell.PnL); err != nil {
				continue
			}
			heatmap = append(heatmap, cell)
			entities[cell.Entity] = true
			amcs[cell.AMC] = true
			totalPnL += cell.PnL
		}

		// Convert maps to sorted slices for column/row headers
		entityList := make([]string, 0, len(entities))
		for e := range entities {
			entityList = append(entityList, e)
		}
		amcList := make([]string, 0, len(amcs))
		for a := range amcs {
			amcList = append(amcList, a)
		}

		// Calculate summary stats
		var maxProfit, maxLoss float64
		profitCount, lossCount := 0, 0
		for _, cell := range heatmap {
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

		// RAW TABULAR DATA: Portfolio snapshot with P&L details
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		response := map[string]interface{}{
			"heatmap":          heatmap,
			"entities":         entityList,
			"amcs":             amcList,
			"group_by":         req.GroupBy,
			"portfolio_detail": aumDetails,
			"summary": map[string]interface{}{
				"total_pnl":       totalPnL,
				"profit_cells":    profitCount,
				"loss_cells":      lossCount,
				"max_profit":      maxProfit,
				"max_loss":        maxLoss,
				"total_cells":     len(heatmap),
				"unique_entities": len(entities),
				"unique_amcs":     len(amcs),
			},
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		}

		api.RespondWithPayload(w, true, "", response)
	}
}

// BenchmarkPoint represents a single point comparing portfolio vs benchmark performance
type BenchmarkPoint struct {
	Month     string  `json:"month"`
	Portfolio float64 `json:"portfolio"`
	Benchmark float64 `json:"benchmark"`
}

// GetPortfolioVsBenchmark returns indexed performance comparison (base 100)
// Shows portfolio vs benchmark performance over time for the financial year
// OPTIMIZED: Single batch query instead of 24+ queries in loop, entity validation
// Request JSON: { "entity_name": "", "year": 2024, "benchmark": constants.Nifty50 }
func GetPortfolioVsBenchmark(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Year       int    `json:"year,omitempty"`
			Benchmark  string `json:"benchmark,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		now := time.Now()
		if req.Year == 0 {
			if now.Month() >= time.April {
				req.Year = now.Year()
			} else {
				req.Year = now.Year() - 1
			}
		}
		if req.Benchmark == "" {
			req.Benchmark = constants.Nifty50
		}

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		// Generate month end dates for batch query
		monthNames := []string{"Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"}
		monthDates := make([]string, 0, 12)

		for i := 0; i < 12; i++ {
			var monthStart time.Time
			if i < 9 { // Apr-Dec
				monthStart = time.Date(req.Year, time.Month(4+i), 1, 0, 0, 0, 0, time.UTC)
			} else { // Jan-Mar next year
				monthStart = time.Date(req.Year+1, time.Month(i-8), 1, 0, 0, 0, 0, time.UTC)
			}
			monthEnd := monthStart.AddDate(0, 1, -1)

			if monthStart.After(now) {
				break
			}
			if monthEnd.After(now) {
				monthEnd = now
			}
			monthDates = append(monthDates, monthEnd.Format(constants.DateFormat))
		}

		if len(monthDates) == 0 {
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"series":         []BenchmarkPoint{{Month: "Apr", Portfolio: 100, Benchmark: 100}},
				"benchmark_name": req.Benchmark, "financial_year": fmt.Sprintf(constants.FormatFiscalYear, req.Year, req.Year+1),
				"generated_at": time.Now().UTC().Format(time.RFC3339),
			})
			return
		}

		// OPTIMIZED: Single batch query for all months - get cost basis at each month end
		// Using a simpler approach: get total invested vs total current for each month
		batchQuery := `
		WITH params AS (
			SELECT $1::text AS entity_filter, $3::text[] AS allowed_entities
		),
		month_dates AS (
			SELECT ordinality AS month_idx, month_end::date AS month_end
			FROM UNNEST($2::date[]) WITH ORDINALITY AS t(month_end, ordinality)
		),
		monthly_invested AS (
			SELECT md.month_idx,
			       COALESCE(SUM(
			         CASE 
			           WHEN LOWER(ot.transaction_type) IN ('buy','purchase','subscription','sip','switch_in') THEN ABS(COALESCE(ot.amount, 0))
			           WHEN LOWER(ot.transaction_type) IN ('sell','redemption','switch_out') THEN -ABS(COALESCE(ot.amount, 0))
			           ELSE 0
			         END
			       ), 0)::float8 AS invested
			FROM month_dates md
			LEFT JOIN investment.onboard_transaction ot ON ot.transaction_date <= md.month_end, params p
			WHERE (p.entity_filter IS NULL OR COALESCE(ot.entity_name, '') = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR COALESCE(ot.entity_name, '') = ANY(p.allowed_entities))
			GROUP BY md.month_idx
		),
		-- Current portfolio value (latest month only for simplicity)
		current_value AS (
			SELECT COALESCE(SUM(current_value), 0)::float8 AS value
			FROM investment.portfolio_snapshot ps, params p
			WHERE (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
		)
		SELECT mi.month_idx, mi.invested, cv.value AS current_value
		FROM monthly_invested mi, current_value cv
		ORDER BY mi.month_idx
		`

		rows, err := pgxPool.Query(ctx, batchQuery, nullIfEmpty(entityFilter), monthDates, allowedParam)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		// Collect monthly invested amounts
		monthlyInvested := make(map[int]float64)
		var currentValue float64
		for rows.Next() {
			var monthIdx int
			var invested, cv float64
			if err := rows.Scan(&monthIdx, &invested, &cv); err != nil {
				continue
			}
			monthlyInvested[monthIdx] = invested
			currentValue = cv
		}

		// Benchmark monthly returns (simplified static data)
		benchmarkMonthlyReturns := map[string][]float64{
			constants.Nifty50: {0.8, 1.0, 0.6, 0.9, 1.1, 0.7, 0.5, 0.8, 1.2, 0.9, 0.6, 0.8},
			"NIFTY 100":       {0.7, 0.9, 0.5, 0.8, 1.0, 0.6, 0.4, 0.7, 1.1, 0.8, 0.5, 0.7},
			"SENSEX":          {0.75, 0.95, 0.55, 0.85, 1.05, 0.65, 0.45, 0.75, 1.15, 0.85, 0.55, 0.75},
		}
		monthlyReturns := benchmarkMonthlyReturns[req.Benchmark]
		if monthlyReturns == nil {
			monthlyReturns = benchmarkMonthlyReturns[constants.Nifty50]
		}

		// Build points with indexed returns
		points := make([]BenchmarkPoint, 0, len(monthDates))
		portfolioIndexed := 100.0
		benchmarkIndexed := 100.0

		baseInvested := monthlyInvested[1]
		if baseInvested <= 0 {
			baseInvested = 100
		}

		for i := 0; i < len(monthDates) && i < len(monthNames); i++ {
			if i == 0 {
				points = append(points, BenchmarkPoint{Month: monthNames[i], Portfolio: 100.0, Benchmark: 100.0})
				continue
			}

			// Portfolio return: simplified using ratio of invested to current
			invested := monthlyInvested[i+1]
			if invested > 0 && currentValue > 0 {
				// Estimate monthly return as fraction of total return
				totalReturn := (currentValue - invested) / invested
				monthlyReturn := totalReturn / float64(len(monthDates)) * 100
				portfolioIndexed = portfolioIndexed * (1 + monthlyReturn/100)
			}

			// Benchmark grows by its monthly return
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

		// Summary metrics
		latestPortfolio := points[len(points)-1].Portfolio
		latestBenchmark := points[len(points)-1].Benchmark
		portfolioReturn := latestPortfolio - 100
		benchmarkReturn := latestBenchmark - 100

		// RAW TABULAR DATA: Portfolio snapshot for comparison
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"series": points, "benchmark_name": req.Benchmark,
			"portfolio_detail": aumDetails,
			"summary": map[string]interface{}{
				"portfolio_return": math.Round(portfolioReturn*100) / 100,
				"benchmark_return": math.Round(benchmarkReturn*100) / 100,
				"alpha":            math.Round((portfolioReturn-benchmarkReturn)*100) / 100,
				"outperforming":    portfolioReturn > benchmarkReturn,
			},
			"financial_year": fmt.Sprintf(constants.FormatFiscalYear, req.Year, req.Year+1),
			"generated_at":   time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// MutualFundTickerRow represents a single row in the market rates ticker
type MutualFundTickerRow struct {
	SchemeName   string  `json:"scheme_name"`
	AMC          string  `json:"amc"`
	AMFICode     string  `json:"amfi_code"`
	InternalCode string  `json:"internal_code"`
	ISIN         string  `json:"isin"`
	NAV          float64 `json:"nav"`
	PrevNAV      float64 `json:"prev_nav"`
	Change1D     float64 `json:"change_1d"`
	MTM          float64 `json:"mtm"`
	Units        float64 `json:"units"`
	Value        float64 `json:"current_value"`
}

// GetMarketRatesTicker returns mutual fund holdings with NAV and daily change
// OPTIMIZED: Uses cached NAV data from amfi_nav_staging instead of external HTTP calls
// Single batch query for all data - no loops, no external API calls
// Request JSON: { "entity_name": "", "limit": 20 }
func GetMarketRatesTicker(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			EntityName string `json:"entity_name,omitempty"`
			Limit      int    `json:"limit,omitempty"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		ctx := r.Context()

		// Entity validation
		entityFilter := strings.TrimSpace(req.EntityName)
		if entityFilter != "" && !api.IsEntityAllowed(ctx, entityFilter) {
			api.RespondWithError(w, http.StatusForbidden, constants.ErrEntityNotFound)
			return
		}
		allowedEntities := api.GetEntityNamesFromCtx(ctx)

		if req.Limit <= 0 {
			req.Limit = 20
		}

		var allowedParam interface{}
		if len(allowedEntities) > 0 {
			allowedParam = allowedEntities
		}

		// OPTIMIZED: Single query with lateral joins for NAV lookup from cached data
		// Fetches current NAV and previous day NAV from amfi_nav_staging
		query := `
		WITH params AS (
			SELECT $1::text AS entity_filter, $3::text[] AS allowed_entities
		),
		holdings AS (
			SELECT 
				ps.scheme_name,
				ps.scheme_id,
				ps.isin,
				ps.current_nav,
				ps.gain_loss,
				ps.total_units,
				ps.current_value,
				COALESCE(ms.amc_name, '') AS amc_name,
				COALESCE(ms.internal_scheme_code, '') AS internal_code,
				COALESCE(ms.amfi_scheme_code::text, '') AS amfi_code
			FROM investment.portfolio_snapshot ps
			LEFT JOIN investment.masterscheme ms ON (
				ms.scheme_id = ps.scheme_id OR ms.internal_scheme_code = ps.scheme_id OR ms.isin = ps.isin
			), params p
			WHERE COALESCE(ps.current_value, 0) > 0
			  AND (p.entity_filter IS NULL OR ps.entity_name = p.entity_filter)
			  AND (p.allowed_entities IS NULL OR ps.entity_name = ANY(p.allowed_entities))
			ORDER BY ps.current_value DESC
			LIMIT $2
		),
		nav_data AS (
			SELECT h.*,
			       -- Current NAV (latest)
			       COALESCE((
			         SELECT nav_value FROM investment.amfi_nav_staging ans
			         WHERE ans.scheme_code::text = h.amfi_code
			         ORDER BY nav_date DESC LIMIT 1
			       ), h.current_nav)::float8 AS latest_nav,
			       -- Previous day NAV
			       COALESCE((
			         SELECT nav_value FROM investment.amfi_nav_staging ans
			         WHERE ans.scheme_code::text = h.amfi_code
			         ORDER BY nav_date DESC LIMIT 1 OFFSET 1
			       ), 0)::float8 AS prev_nav
			FROM holdings h
		)
		SELECT 
			COALESCE(scheme_name, 'Unknown') AS scheme_name,
			COALESCE(amc_name, 'Unknown') AS amc,
			COALESCE(amfi_code, '') AS amfi_code,
			COALESCE(internal_code, '') AS internal_code,
			COALESCE(isin, '') AS isin,
			latest_nav AS nav,
			prev_nav,
			CASE WHEN prev_nav > 0 THEN ROUND(((latest_nav - prev_nav) / prev_nav * 100)::numeric, 2) ELSE 0 END AS change_1d,
			COALESCE(gain_loss, 0)::float8 AS mtm,
			COALESCE(total_units, 0)::float8 AS units,
			COALESCE(current_value, 0)::float8 AS current_value
		FROM nav_data
		`

		rows, err := pgxPool.Query(ctx, query, nullIfEmpty(entityFilter), req.Limit, allowedParam)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		ticker := make([]MutualFundTickerRow, 0, req.Limit)
		var totalValue, totalMTM float64
		gainers, losers := 0, 0

		for rows.Next() {
			var row MutualFundTickerRow
			if err := rows.Scan(&row.SchemeName, &row.AMC, &row.AMFICode, &row.InternalCode, &row.ISIN,
				&row.NAV, &row.PrevNAV, &row.Change1D, &row.MTM, &row.Units, &row.Value); err != nil {
				continue
			}

			// Round values
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

		// RAW TABULAR DATA: Full portfolio snapshot for market rates
		aumDetails := fetchAUMDetails(ctx, pgxPool, entityFilter, allowedEntities)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"mutual_funds":     ticker,
			"portfolio_detail": aumDetails,
			"summary": map[string]interface{}{
				"total_schemes": len(ticker),
				"total_value":   math.Round(totalValue*100) / 100,
				"total_mtm":     math.Round(totalMTM*100) / 100,
				"gainers":       gainers,
				"losers":        losers,
				"unchanged":     len(ticker) - gainers - losers,
			},
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// MarketRateTickerLiteRow is a compact row for light-weight tickers
type MarketRateTickerLiteRow struct {
	SchemeName string  `json:"scheme_name"`
	SchemeID   string  `json:"scheme_id"`
	AMFICode   string  `json:"amfi_code"`
	NAV        float64 `json:"nav"`
	PrevNAV    float64 `json:"prev_nav"`
	Change     float64 `json:"change"`
	ChangePct  float64 `json:"change_pct"`
}

// GetMarketRatesTickerLite returns a lightweight list of approved schemes
// with current NAV, previous NAV and percentage change. Request JSON: { "limit": 100 }
func GetMarketRatesTickerLite(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			Limit      int  `json:"limit,omitempty"`
			AllSchemes bool `json:"all_schemes,omitempty"` // when true, include all schemes from masterscheme (not just approved)
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Limit <= 0 || req.Limit > 10000 {
			req.Limit = 100
		}

		ctx := r.Context()

		// Choose query based on AllSchemes flag. Default: approved + active schemes only.
		var query string
		if req.AllSchemes {
			// When all_schemes=true we want to iterate AMFI codes present in amfi_nav_staging
			// and fetch latest/previous NAVs from MFapi. Build a simple list of distinct AMFI codes.
			query = `
		SELECT DISTINCT scheme_code::text AS amfi_code
		FROM investment.amfi_nav_staging
		ORDER BY amfi_code
		LIMIT $1
		`
		} else {
			// Approved + active schemes only (existing behaviour)
			query = `
		WITH latest_nav AS (
			SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_code, nav_value, nav_date
			FROM investment.amfi_nav_staging
			ORDER BY scheme_code, nav_date DESC
		),
		prev_nav AS (
			SELECT DISTINCT ON (scheme_code) scheme_code::text AS scheme_code, nav_value
			FROM investment.amfi_nav_staging
			ORDER BY scheme_code, nav_date DESC OFFSET 1
		),
		latest_scheme AS (
			SELECT DISTINCT ON (scheme_id) scheme_id, processing_status
			FROM investment.auditactionscheme
			ORDER BY scheme_id, requested_at DESC
		),
		latest_amc AS (
			SELECT DISTINCT ON (amc.amc_id) amc.amc_id, m.amc_name, amc.processing_status, m.status
			FROM investment.auditactionamc amc
			JOIN investment.masteramc m ON amc.amc_id = m.amc_id
			ORDER BY amc.amc_id, amc.requested_at DESC
		)
		SELECT
			COALESCE(m.scheme_name,'') AS scheme_name,
			m.scheme_id,
			COALESCE(m.amfi_scheme_code::text,'') AS amfi_code,
			COALESCE(ln.nav_value,0)::float8 AS nav,
			COALESCE(pn.nav_value,0)::float8 AS prev_nav
		FROM investment.masterscheme m
		JOIN latest_scheme ls ON ls.scheme_id = m.scheme_id
		JOIN latest_amc la ON UPPER(la.amc_name) = UPPER(m.amc_name)
		LEFT JOIN latest_nav ln ON ln.scheme_code = COALESCE(m.amfi_scheme_code::text, '')
		LEFT JOIN prev_nav pn ON pn.scheme_code = ln.scheme_code
		WHERE UPPER(ls.processing_status) = 'APPROVED'
		  AND UPPER(m.status) = 'ACTIVE'
		  AND COALESCE(m.is_deleted,false) = false
		  AND UPPER(la.processing_status) = 'APPROVED'
		  AND UPPER(la.status) = 'ACTIVE'
		ORDER BY m.scheme_name
		LIMIT $1
		`
		}

		rows, err := pgxPool.Query(ctx, query, req.Limit)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		out := make([]MarketRateTickerLiteRow, 0)

		if req.AllSchemes {
			// Collect AMFI codes from rows
			codes := make([]string, 0)
			for rows.Next() {
				var code string
				if err := rows.Scan(&code); err != nil {
					continue
				}
				codes = append(codes, code)
			}
			rows.Close()

			type mfRes struct {
				Code string
				Name string
				NAV  float64
				Prev float64
			}

			resCh := make(chan mfRes, len(codes))
			var wg sync.WaitGroup
			sem := make(chan struct{}, 16) // concurrency limit

			for _, c := range codes {
				wg.Add(1)
				go func(code string) {
					defer wg.Done()
					sem <- struct{}{}
					name, nav, prev := getMFAPIData(code)
					<-sem
					resCh <- mfRes{Code: code, Name: name, NAV: nav, Prev: prev}
				}(c)
			}

			wg.Wait()
			close(resCh)

			for r := range resCh {
				nav := r.NAV
				prevNav := r.Prev
				change := nav - prevNav
				// Round to 4 decimals
				nav = math.Round(nav*10000) / 10000
				prevNav = math.Round(prevNav*10000) / 10000
				change = math.Round(change*10000) / 10000
				pct := 0.0
				if prevNav > 0 {
					pct = math.Round(((change/prevNav)*100)*10000) / 10000
				}
				out = append(out, MarketRateTickerLiteRow{
					SchemeName: r.Name,
					SchemeID:   "",
					AMFICode:   r.Code,
					NAV:        nav,
					PrevNAV:    prevNav,
					Change:     change,
					ChangePct:  pct,
				})
			}
		} else {
			for rows.Next() {
				var rname, sid, amfi string
				var nav, prevNav float64
				if err := rows.Scan(&rname, &sid, &amfi, &nav, &prevNav); err != nil {
					continue
				}

				// If NAVs not available locally, try MFapi (mfapi.in) as a fallback
				if (nav == 0 || prevNav == 0) && amfi != "" {
					name, mnav, mprev := getMFAPIData(amfi)
					if name != "" && rname == "" {
						rname = name
					}
					if nav == 0 {
						nav = mnav
					}
					if prevNav == 0 {
						prevNav = mprev
					}
				}

				change := nav - prevNav

				// Round to 4 decimal places
				nav = math.Round(nav*10000) / 10000
				prevNav = math.Round(prevNav*10000) / 10000
				change = math.Round(change*10000) / 10000

				pct := 0.0
				if prevNav > 0 {
					pct = math.Round(((change/prevNav)*100)*10000) / 10000
				}

				out = append(out, MarketRateTickerLiteRow{
					SchemeName: rname,
					SchemeID:   sid,
					AMFICode:   amfi,
					NAV:        nav,
					PrevNAV:    prevNav,
					Change:     change,
					ChangePct:  pct,
				})
			}
		}

		api.RespondWithPayload(w, true, "", out)
	}
}
