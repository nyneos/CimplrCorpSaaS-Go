package dash

import (
	"CimplrCorpSaas/api"
	bankbalance "CimplrCorpSaas/api/dash/bank-balance"
	benchmarks "CimplrCorpSaas/api/dash/benchmarks"
	"CimplrCorpSaas/api/dash/buCurrExpDash"
	cashflowforecast "CimplrCorpSaas/api/dash/cashflowforecast"
	categorywisedata "CimplrCorpSaas/api/dash/categorywiseData"
	"CimplrCorpSaas/api/dash/cfo"
	commonpool "CimplrCorpSaas/api/dash/commonpool"
	forecastVsActual "CimplrCorpSaas/api/dash/forecastVsActual"
	fxops "CimplrCorpSaas/api/dash/fx-ops"
	hedgeproposal "CimplrCorpSaas/api/dash/hedging-proposal"
	investmentdashboards "CimplrCorpSaas/api/dash/investmentDashboards"
	landingpage "CimplrCorpSaas/api/dash/landing-page"
	liqsnap "CimplrCorpSaas/api/dash/liqsnap"
	payablereceivabledash "CimplrCorpSaas/api/dash/payableReceivableDash"
	plannedinflowoutflowdash "CimplrCorpSaas/api/dash/plannedInflowOutflowDash"
	projectiondash "CimplrCorpSaas/api/dash/projectionDash"
	realtimebalances "CimplrCorpSaas/api/dash/real-time-balances"
	reports "CimplrCorpSaas/api/dash/reports"
	statementstatus "CimplrCorpSaas/api/dash/statementstatus"
	middlewares "CimplrCorpSaas/api/middlewares"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func StartDashService(db *sql.DB) {
	mux := http.NewServeMux()
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, name)
	pgxPool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("failed to connect to pgxpool DB: %v", err)
	}
	// Statement Status Dashboard
	mux.Handle("/dash/statement-status", middlewares.PreValidationMiddleware(pgxPool)(statementstatus.GetStatementStatusHandler(pgxPool)))
	mux.Handle("/dash/transaction-pool", middlewares.PreValidationMiddleware(pgxPool)(commonpool.GetTransactionPoolHandler(db)))
	// Categorywise Breakdown Dashboard
	mux.Handle("/dash/categorywise-breakdown", middlewares.PreValidationMiddleware(pgxPool)(categorywisedata.GetCategorywiseBreakdownHandler(pgxPool)))
	mux.HandleFunc("/dash/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Dashboard Service is active"))
	})

	// Real-time Balances KPI Route
	mux.Handle("/dash/realtime-balances/kpi", middlewares.PreValidationMiddleware(pgxPool)(realtimebalances.GetKpiHandler(db)))
	// Bank balance endpoints: use PreValidationMiddleware ONLY (no BusinessUnitMiddleware to avoid overriding entity scope)
	mux.Handle("/dash/bank-balance/approved", middlewares.PreValidationMiddleware(pgxPool)(bankbalance.GetApprovedBankBalances(pgxPool)))
	mux.Handle("/dash/bank-balance/currency-wise", middlewares.PreValidationMiddleware(pgxPool)(bankbalance.GetCurrencyWiseBalancesFromManual(pgxPool)))
	mux.Handle("/dash/bank-balance/currency-wise-dashboard", middlewares.PreValidationMiddleware(pgxPool)(bankbalance.GetCurrencyWiseDashboard(pgxPool)))
	mux.Handle("/dash/bank-balance/approved-manual", middlewares.PreValidationMiddleware(pgxPool)(bankbalance.GetApprovedBalancesFromManual(pgxPool)))

	// Business Unit/Currency Exposure Dashboard
	// mux.Handle("/dash/bu-curr-exp-dashboard", http.HandlerFunc(buCurrExpDash.GetDashboard(db)))
	mux.Handle("/dash/bu-curr-exp-dashboard", api.BusinessUnitMiddleware(db)(buCurrExpDash.GetDashboard(db)))

	// CFO Dashboard Endpoints
	// --- Forward Dashboard Routes ---
	mux.Handle("/dash/cfo/fwd/waht", api.BusinessUnitMiddleware(db)(cfo.GetAvgForwardMaturity(db)))
	mux.Handle("/dash/cfo/fwd/buysell", api.BusinessUnitMiddleware(db)(cfo.GetForwardBuySellTotals(db)))
	mux.Handle("/dash/cfo/fwd/localcurr", api.BusinessUnitMiddleware(db)(cfo.GetUserCurrency(db)))
	mux.Handle("/dash/cfo/fwd/active-forwards", api.BusinessUnitMiddleware(db)(cfo.GetActiveForwardsCount(db)))
	mux.Handle("/dash/cfo/fwd/recent-trades-dashboard", api.BusinessUnitMiddleware(db)(cfo.GetRecentTradesDashboard(db)))
	mux.Handle("/dash/cfo/fwd/total-usd-sum", api.BusinessUnitMiddleware(db)(cfo.GetTotalUsdSumDashboard(db)))
	mux.Handle("/dash/cfo/fwd/open-to-booking-ratio", api.BusinessUnitMiddleware(db)(cfo.GetOpenAmountToBookingRatioDashboard(db)))
	// mux.Handle("/dash/cfo/fwd/total-bank-margin", api.BusinessUnitMiddleware(db)(cfo.GetTotalBankMarginDashboard(db)))
	mux.Handle("/dash/cfo/fwd/total-usd-sum-by-currency", api.BusinessUnitMiddleware(db)(cfo.GetTotalUsdSumByCurrencyDashboard(db)))
	mux.Handle("/dash/cfo/fwd/forward-booking-maturity-buckets", api.BusinessUnitMiddleware(db)(cfo.GetForwardBookingMaturityBucketsDashboard(db)))
	mux.Handle("/dash/cfo/fwd/maturity-buckets", api.BusinessUnitMiddleware(db)(cfo.GetMaturityBucketsDashboard(db)))
	mux.Handle("/dash/cfo/fwd/rollover-counts", api.BusinessUnitMiddleware(db)(cfo.GetRolloverCountsByCurrency(db)))
	mux.Handle("/dash/cfo/fwd/total-bankmargin", api.BusinessUnitMiddleware(db)(cfo.GetTotalBankMarginFromForwardBookings(db)))
	mux.Handle("/dash/cfo/fwd/hedge-ratio", api.BusinessUnitMiddleware(db)(cfo.GetOpenAmountToBookingRatioSimple(db)))
	mux.Handle("/dash/cfo/fwd/bank-trades", api.BusinessUnitMiddleware(db)(cfo.GetBankTradesData(db)))

	// --- Exposure Dashboard Routes ---
	mux.Handle("/dash/cfo/exp/total-open-amount-usd", api.BusinessUnitMiddleware(db)(cfo.GetTotalOpenAmountUsdSumFromHeaders(db)))
	mux.Handle("/dash/cfo/exp/payables-by-currency", api.BusinessUnitMiddleware(db)(cfo.GetPayablesByCurrencyFromHeaders(db)))
	mux.Handle("/dash/cfo/exp/receivables-by-currency", api.BusinessUnitMiddleware(db)(cfo.GetReceivablesByCurrencyFromHeaders(db)))
	mux.Handle("/dash/cfo/exp/amount-by-currency", api.BusinessUnitMiddleware(db)(cfo.GetAmountByCurrencyFromHeaders(db)))
	mux.Handle("/dash/cfo/exp/business-unit-currency-summary", api.BusinessUnitMiddleware(db)(cfo.GetBusinessUnitCurrencySummaryFromHeaders(db)))
	mux.Handle("/dash/cfo/exp/maturity-expiry-summary", api.BusinessUnitMiddleware(db)(cfo.GetMaturityExpirySummaryFromHeaders(db)))
	mux.Handle("/dash/cfo/exp/maturity-expiry-count-7-days", api.BusinessUnitMiddleware(db)(cfo.GetMaturityExpiryCount7DaysFromHeaders(db)))
	mux.Handle("/dash/cfo/exp/waet", api.BusinessUnitMiddleware(db)(cfo.GetAvgExposureMaturity(db)))

	// --- FX Ops Dashboard Routes ---
	// Top Currencies
	mux.Handle("/dash/fx-ops/top-currencies-from-headers", api.BusinessUnitMiddleware(db)(fxops.GetTopCurrenciesFromHeaders(db)))
	mux.Handle("/dash/fx-ops/ready-for-settlement", api.BusinessUnitMiddleware(db)(fxops.GetForwardBookingsMaturingTodayCount(db)))
	mux.Handle("/dash/fx-ops/daily-traded-volume", api.BusinessUnitMiddleware(db)(fxops.GetTodayBookingAmountSum(db)))
	mux.Handle("/dash/fx-ops/maturity-buckets-currencypair", api.BusinessUnitMiddleware(db)(fxops.GetMaturityBucketsByCurrencyPair(db)))
	// Comprehensive FX Ops Dashboard (with filters)
	mux.Handle("/dash/landingpage/dashboard", api.BusinessUnitMiddleware(db)(landingpage.GetFXOpsDashboard(db)))
	// Home/Landing dashboard (liquidity + investments + risk)
	mux.Handle("/dash/landingpage/home", middlewares.PreValidationMiddleware(pgxPool)(landingpage.GetHomePageDashboard(db)))
	mux.Handle("/dash/landingpage/cash", middlewares.PreValidationMiddleware(pgxPool)(landingpage.GetLandingCashDashboard(pgxPool)))
	// Combined investment overview (aggregates multiple investment endpoints sequentially)
	mux.Handle("/dash/investment/overview/combined", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetCombinedInvestmentOverview(pgxPool)))

	// --- Hedging Proposal Dashboard Routes ---
	// Forward Dashboard
	mux.Handle("/dash/hedge/fwd/bu-maturity-currency-summary", api.BusinessUnitMiddleware(db)(hedgeproposal.GetForwardBookingMaturityBucketsDashboard(db)))
	mux.Handle("/dash/hedge/fwd/forward-bookings", api.BusinessUnitMiddleware(db)(hedgeproposal.GetForwardBookingsDashboard(db)))
	// Exposure Dashboard
	mux.Handle("/dash/hedge/exp/bu-maturity-currency-summary", api.BusinessUnitMiddleware(db)(hedgeproposal.GetBuMaturityCurrencySummaryJoinedFromHeaders(db)))
	mux.Handle("/dash/hedge/exp/exposure-rows", api.BusinessUnitMiddleware(db)(hedgeproposal.GetExposureRowsDashboard(db)))

	//Liquidity Snapshot
	mux.Handle("/dash/liquidity/total-cash-balance-by-entity", api.BusinessUnitMiddleware(db)(liqsnap.TotalCashBalanceByEntityHandler(pgxPool)))
	mux.Handle("/dash/liquidity/liquidity-coverage-ratio", api.BusinessUnitMiddleware(db)(liqsnap.LiquidityCoverageRatioHandler(pgxPool)))
	mux.Handle("/dash/liquidity/entity-currency-wise-cash", api.BusinessUnitMiddleware(db)(liqsnap.EntityCurrencyWiseCashHandler(pgxPool)))
	mux.Handle("/dash/liquidity/kpi", api.BusinessUnitMiddleware(db)(liqsnap.KpiCardsHandler(pgxPool)))
	mux.Handle("/dash/liquidity/daily", api.BusinessUnitMiddleware(db)(liqsnap.DetailedDailyCashFlowHandler(pgxPool)))

	// Investment Overview KPIs
	mux.Handle("/dash/investment/overview/kpis", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetInvestmentOverviewKPIs(pgxPool)))
	// Investment: Entity Performance (YTD P&L per scheme)
	mux.Handle("/dash/investment/overview/entity", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetEntityPerformance(pgxPool)))
	// Investment: AMC Performance (start-of-FY vs now AUM + P&L)
	mux.Handle("/dash/investment/overview/amc-performance", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetAMCPerformance(pgxPool)))
	// Investment: Contribution to AUM Change (AMC-wise waterfall)
	mux.Handle("/dash/investment/overview/waterfall", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetAMCWaterfall(pgxPool)))
	// Investment: AUM Movement Waterfall (Bridge Chart)
	mux.Handle("/dash/investment/portfolio/aum-movement", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetAUMMovementWaterfall(pgxPool)))
	// Investment: AUM Breakdown (Dynamic Donut - by AMC/Scheme/Entity)
	mux.Handle("/dash/investment/portfolio/aum-breakdown", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetAUMBreakdown(pgxPool)))
	// Investment: Performance Attribution (Brinson-Fachler Waterfall)
	mux.Handle("/dash/investment/performance/performance-attribution", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetPerformanceAttribution(pgxPool)))
	// Investment: Daily P&L Heatmap (Entity × AMC/Scheme Matrix)
	mux.Handle("/dash/investment/performance/pnl-heatmap", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetDailyPnLHeatmap(pgxPool)))
	// Investment: Portfolio vs Benchmark (Indexed Performance Comparison)
	mux.Handle("/dash/investment/performance/portfolio-vs-benchmark", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetPortfolioVsBenchmark(pgxPool)))
	// Investment: Market Rates Ticker (Mutual Funds with NAV, Change, MTM)
	mux.Handle("/dash/investment/overview/market-rates-ticker", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetMarketRatesTicker(pgxPool)))
	// Investment: Top performing assets (YTD)
	mux.Handle("/dash/investment/overview/top-performing", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetTopPerformingAssets(pgxPool)))
	// Investment: AUM Composition & Trend (stacked area by AMC)
	mux.Handle("/dash/investment/overview/aum-composition", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetAUMCompositionTrend(pgxPool)))
	// Benchmarks: NSE live data feeds (index list, graph series, market data)
	mux.Handle("/dash/benchmarks/index-list", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetIndexList()))
	mux.Handle("/dash/benchmarks/index-series", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetIndexSeries()))
	mux.Handle("/dash/benchmarks/index-snapshot", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetIndexSnapshot()))
	mux.Handle("/dash/benchmarks/index-constituents", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetIndexConstituents()))
	mux.Handle("/dash/benchmarks/market-movers", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetMarketMovers()))
	mux.Handle("/dash/benchmarks/market-status", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetMarketStatus()))
	mux.Handle("/dash/benchmarks/market-heatmap", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetMarketHeatmap()))
	mux.Handle("/dash/benchmarks/advance-declines", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetAdvanceDeclines()))
	mux.Handle("/dash/benchmarks/marquee", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetMarqueeData()))
	mux.Handle("/dash/benchmarks/52-week-hl", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.Get52WeekHighLow()))
	mux.Handle("/dash/benchmarks/market-turnover", middlewares.PreValidationMiddleware(pgxPool)(benchmarks.GetMarketTurnover()))
	// Investment: Consolidated Risk Gauge (entity-level)
	mux.Handle("/dash/investment/overview/consolidated", middlewares.PreValidationMiddleware(pgxPool)(investmentdashboards.GetConsolidatedRisk(pgxPool)))
	// Cashflow Forecast (monthly aggregated projections + KPIs)
	mux.Handle("/dash/cash/forecast/monthly", api.BusinessUnitMiddleware(db)(cashflowforecast.GetCashflowForecastHandler(pgxPool)))
	mux.Handle("/dash/cash/forecast/kpi", api.BusinessUnitMiddleware(db)(cashflowforecast.GetForecastKPIsHandler(pgxPool)))
	mux.Handle("/dash/cash/forecast/rows", api.BusinessUnitMiddleware(db)(cashflowforecast.GetForecastRowsHandler(pgxPool)))
	mux.Handle("/dash/cash/forecast/categories", api.BusinessUnitMiddleware(db)(cashflowforecast.GetForecastCategorySumsHandler(pgxPool)))
	mux.Handle("/dash/cash/forecast/daily", api.BusinessUnitMiddleware(db)(cashflowforecast.GetForecastDailyHandler(pgxPool)))

	// Forecast vs Actual
	mux.Handle("/dash/forecast-vs-actual/rows", api.BusinessUnitMiddleware(db)(forecastVsActual.GetForecastVsActualRowsHandler(pgxPool)))
	mux.Handle("/dash/forecast-vs-actual/kpi", api.BusinessUnitMiddleware(db)(forecastVsActual.GetForecastVsActualKPIHandler(pgxPool)))
	mux.Handle("/dash/forecast-vs-actual/by-date", api.BusinessUnitMiddleware(db)(forecastVsActual.GetForecastVsActualByDateHandler(pgxPool)))
	mux.Handle("/dash/forecast-vs-actual/by-month", api.BusinessUnitMiddleware(db)(forecastVsActual.GetForecastVsActualByMonthHandler(pgxPool)))

	// --- Reports Dashboard Routes ---
	mux.Handle("/dash/reports/exposure-summary", api.BusinessUnitMiddleware(db)(reports.GetExposureSummary(db)))
	mux.Handle("/dash/reports/linked-summary-by-category", api.BusinessUnitMiddleware(db)(reports.GetLinkedSummaryByCategory(db)))

	// Projection Pipeline Dashboard
	mux.Handle("/dash/projection-pipeline/kpi", api.BusinessUnitMiddleware(db)(projectiondash.GetProjectionPipelineKPI(pgxPool)))
	mux.Handle("/dash/projection-pipeline/detailed", api.BusinessUnitMiddleware(db)(projectiondash.GetDetailedPipeline(pgxPool)))
	mux.Handle("/dash/projection-pipeline/by-entity", api.BusinessUnitMiddleware(db)(projectiondash.GetProjectionByEntity(pgxPool)))

	// Payables / Receivables dashboard rows
	mux.Handle("/dash/payrec/rows", api.BusinessUnitMiddleware(db)(payablereceivabledash.GetPayablesReceivables(pgxPool)))
	mux.Handle("/dash/payrec/forecast", api.BusinessUnitMiddleware(db)(payablereceivabledash.GetPayRecForecast(pgxPool)))

	// Planned Inflow/Outflow Dashboard
	mux.Handle("/dash/planned-inflow-outflow", api.BusinessUnitMiddleware(db)(plannedinflowoutflowdash.GetPlannedIODash(pgxPool)))

	log.Println("Dashboard Service started on :4143")
	err = http.ListenAndServe(":4143", mux)
	if err != nil {
		log.Fatalf("Dashboard Service failed: %v", err)
	}
}
