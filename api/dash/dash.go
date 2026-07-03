package dash

import (
	bankbalance "CimplrCorpSaas/api/dash/bank-balance"
	benchmarks "CimplrCorpSaas/api/dash/benchmarks"
	"CimplrCorpSaas/api/dash/buCurrExpDash"
	cashflowforecast "CimplrCorpSaas/api/dash/cashflowforecast"
	categorywisedata "CimplrCorpSaas/api/dash/categorywiseData"
	"CimplrCorpSaas/api/dash/cfo"
	commonpool "CimplrCorpSaas/api/dash/commonpool"
	dashboardbuilder "CimplrCorpSaas/api/dash/dashboard-builder"
	forecastVsActual "CimplrCorpSaas/api/dash/forecastVsActual"
	fxops "CimplrCorpSaas/api/dash/fx-ops"
	hedgeproposal "CimplrCorpSaas/api/dash/hedging-proposal"
	investmentdashboards "CimplrCorpSaas/api/dash/investmentDashboards"
	landingpage "CimplrCorpSaas/api/dash/landing-page"
	liqsnap "CimplrCorpSaas/api/dash/liqsnap"
	notifDash "CimplrCorpSaas/api/dash/notification"
	payablereceivabledash "CimplrCorpSaas/api/dash/payableReceivableDash"
	plannedinflowoutflowdash "CimplrCorpSaas/api/dash/plannedInflowOutflowDash"
	projectiondash "CimplrCorpSaas/api/dash/projectionDash"
	realtimebalances "CimplrCorpSaas/api/dash/real-time-balances"
	reports "CimplrCorpSaas/api/dash/reports"
	statementstatus "CimplrCorpSaas/api/dash/statementstatus"
	ticker "CimplrCorpSaas/api/dash/ticker"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/dbutil"
	"CimplrCorpSaas/internal/observability"
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func NewDashServer(port string) (*http.Server, *pgxpool.Pool, error) {
	const serviceName = "dash"
	mux := http.NewServeMux()
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	sslMode := dbutil.EffectiveSSLMode(host)
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, dbPort, name, sslMode)
	pgxPool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to pgxpool DB: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pgxPool.Ping(pingCtx); err != nil {
		logger.LogError("Dash: failed to verify pgxpool DB connectivity at startup: %v", err)
	}

	midDashFull := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(
			middlewares.GlobalIndependentMiddleware(pgxPool)(
				middlewares.GlobalDependentMiddleware(pgxPool)(
					middlewares.CashMiddleware(pgxPool)(
						middlewares.InvestmentMFMiddleware(pgxPool)(
							middlewares.InvestmentFDMiddleware(pgxPool)(h),
						),
					),
				),
			),
		)
	}
	// Read-only investment dashboards: prevalidation v2 entity scope + light path (no FD/MF master preload).
	midDashInvestRead := func(h http.Handler) http.Handler {
		return investmentdashboards.CacheDashboardHandler(
			middlewares.PreValidationMiddleware(pgxPool)(
				middlewares.GlobalIndependentMiddleware(pgxPool)(
					middlewares.GlobalDependentMiddleware(pgxPool)(h),
				),
			),
		)
	}
	// Cash dashboard only needs entity/bank/currency/account context — skip MF+FD master preloads
	// (InvestmentMFMiddleware scans all AMFI schemes with correlated subqueries, causing 5-10 min hangs).
	midDashCash := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(
			middlewares.GlobalIndependentMiddleware(pgxPool)(
				middlewares.GlobalDependentMiddleware(pgxPool)(h),
			),
		)
	}
	// Statement Status Dashboard
	mux.Handle("/dash/statement-status", midDashFull(statementstatus.GetStatementStatusHandler(pgxPool)))
	mux.Handle("/dash/transaction-pool", midDashFull(commonpool.GetTransactionPoolHandler(pgxPool)))
	// Categorywise Breakdown Dashboard
	mux.Handle("/dash/categorywise-breakdown", midDashFull(categorywisedata.GetCategorywiseBreakdownHandler(pgxPool)))
	mux.HandleFunc("/dash/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Dashboard Service is active"))
	})
	mux.Handle("/dash/metrics", observability.MetricsHandler(serviceName))

	// Real-time Balances KPI Route
	mux.Handle("/dash/realtime-balances/kpi", midDashFull(realtimebalances.GetKpiHandler(pgxPool)))

	// Simple rates ticker (POST)
	mux.Handle("/dash/ticker", midDashFull(ticker.GetTickerHandler()))
	mux.Handle("/dash/ticker/inr-rates", midDashFull(ticker.GetInrRatesHandler()))
	// Bank balance endpoints use the shared v2 master middleware chain.
	mux.Handle("/dash/bank-balance/approved", midDashFull(bankbalance.GetApprovedBankBalances(pgxPool)))
	mux.Handle("/dash/bank-balance/currency-wise", midDashFull(bankbalance.GetCurrencyWiseBalancesFromManual(pgxPool)))
	mux.Handle("/dash/bank-balance/currency-wise-dashboard", midDashFull(bankbalance.GetCurrencyWiseDashboard(pgxPool)))
	mux.Handle("/dash/bank-balance/approved-manual", midDashFull(bankbalance.GetApprovedBalancesFromManual(pgxPool)))

	// Business Unit/Currency Exposure Dashboard
	// mux.Handle("/dash/bu-curr-exp-dashboard", http.HandlerFunc(buCurrExpDash.GetDashboard(pgxPool)))
	mux.Handle("/dash/bu-curr-exp-dashboard", midDashFull(buCurrExpDash.GetDashboard(pgxPool)))

	// CFO Dashboard Endpoints
	// --- Forward Dashboard Routes ---
	mux.Handle("/dash/cfo/fwd/waht", midDashFull(cfo.GetAvgForwardMaturity(pgxPool)))
	mux.Handle("/dash/cfo/fwd/buysell", midDashFull(cfo.GetForwardBuySellTotals(pgxPool)))
	mux.Handle("/dash/cfo/fwd/localcurr", midDashFull(cfo.GetUserCurrency(pgxPool)))
	mux.Handle("/dash/cfo/fwd/active-forwards", midDashFull(cfo.GetActiveForwardsCount(pgxPool)))
	mux.Handle("/dash/cfo/fwd/recent-trades-dashboard", midDashFull(cfo.GetRecentTradesDashboard(pgxPool)))
	mux.Handle("/dash/cfo/fwd/total-usd-sum", midDashFull(cfo.GetTotalUsdSumDashboard(pgxPool)))
	mux.Handle("/dash/cfo/fwd/open-to-booking-ratio", midDashFull(cfo.GetOpenAmountToBookingRatioDashboard(pgxPool)))
	// mux.Handle("/dash/cfo/fwd/total-bank-margin", midDashFull(cfo.GetTotalBankMarginDashboard(pgxPool)))
	mux.Handle("/dash/cfo/fwd/total-usd-sum-by-currency", midDashFull(cfo.GetTotalUsdSumByCurrencyDashboard(pgxPool)))
	mux.Handle("/dash/cfo/fwd/forward-booking-maturity-buckets", midDashFull(cfo.GetForwardBookingMaturityBucketsDashboard(pgxPool)))
	mux.Handle("/dash/cfo/fwd/maturity-buckets", midDashFull(cfo.GetMaturityBucketsDashboard(pgxPool)))
	mux.Handle("/dash/cfo/fwd/rollover-counts", midDashFull(cfo.GetRolloverCountsByCurrency(pgxPool)))
	mux.Handle("/dash/cfo/fwd/total-bankmargin", midDashFull(cfo.GetTotalBankMarginFromForwardBookings(pgxPool)))
	mux.Handle("/dash/cfo/fwd/hedge-ratio", midDashFull(cfo.GetOpenAmountToBookingRatioSimple(pgxPool)))
	mux.Handle("/dash/cfo/fwd/bank-trades", midDashFull(cfo.GetBankTradesData(pgxPool)))

	// --- Exposure Dashboard Routes ---
	mux.Handle("/dash/cfo/exp/total-open-amount-usd", midDashFull(cfo.GetTotalOpenAmountUsdSumFromHeaders(pgxPool)))
	mux.Handle("/dash/cfo/exp/payables-by-currency", midDashFull(cfo.GetPayablesByCurrencyFromHeaders(pgxPool)))
	mux.Handle("/dash/cfo/exp/receivables-by-currency", midDashFull(cfo.GetReceivablesByCurrencyFromHeaders(pgxPool)))
	mux.Handle("/dash/cfo/exp/amount-by-currency", midDashFull(cfo.GetAmountByCurrencyFromHeaders(pgxPool)))
	mux.Handle("/dash/cfo/exp/business-unit-currency-summary", midDashFull(cfo.GetBusinessUnitCurrencySummaryFromHeaders(pgxPool)))
	mux.Handle("/dash/cfo/exp/maturity-expiry-summary", midDashFull(cfo.GetMaturityExpirySummaryFromHeaders(pgxPool)))
	mux.Handle("/dash/cfo/exp/maturity-expiry-count-7-days", midDashFull(cfo.GetMaturityExpiryCount7DaysFromHeaders(pgxPool)))
	mux.Handle("/dash/cfo/exp/waet", midDashFull(cfo.GetAvgExposureMaturity(pgxPool)))

	// --- FX Ops Dashboard Routes ---
	// Top Currencies
	mux.Handle("/dash/fx-ops/top-currencies-from-headers", midDashFull(fxops.GetTopCurrenciesFromHeaders(pgxPool)))
	mux.Handle("/dash/fx-ops/ready-for-settlement", midDashFull(fxops.GetForwardBookingsMaturingTodayCount(pgxPool)))
	mux.Handle("/dash/fx-ops/daily-traded-volume", midDashFull(fxops.GetTodayBookingAmountSum(pgxPool)))
	mux.Handle("/dash/fx-ops/maturity-buckets-currencypair", midDashFull(fxops.GetMaturityBucketsByCurrencyPair(pgxPool)))
	// Comprehensive FX Ops Dashboard (with filters)
	mux.Handle("/dash/landingpage/dashboard", midDashFull(landingpage.GetFXOpsDashboard(pgxPool)))
	// Home/Landing dashboard (liquidity + investments + risk)
	mux.Handle("/dash/landingpage/home", midDashFull(landingpage.GetHomePageDashboard(pgxPool)))
	mux.Handle("/dash/landingpage/cash", midDashCash(landingpage.GetLandingCashDashboard(pgxPool)))
	// Combined investment overview (aggregates multiple investment endpoints sequentially)
	mux.Handle("/dash/investment/overview/combined", midDashInvestRead(investmentdashboards.GetCombinedInvestmentOverview(pgxPool)))
	mux.Handle("/dash/investment/portfolio/dashboard", midDashInvestRead(investmentdashboards.PortfolioDashboardHandler(pgxPool)))
	// FD CFO Dashboard — KPIs, charts, governance, FD list (all FD tables, concurrent)
	mux.Handle("/dash/investment/fd/cfo-dashboard", midDashFull(investmentdashboards.GetFDCfoDashboard(pgxPool)))
	// FD Treasury Manager Dashboard — surplus deployment, negotiations, maturity ladder, rate heatmap
	mux.Handle("/dash/investment/fd/treasury-dashboard", midDashFull(investmentdashboards.GetFDTreasuryDashboard(pgxPool)))
	// FD Operational Team Dashboard — booking queue, confirmations, exceptions, TDS, SLA
	mux.Handle("/dash/investment/fd/operational-dashboard", midDashFull(investmentdashboards.GetFDOperationalDashboard(pgxPool)))
	// FD BOD/EOD Control Room — maturities, confirmations due, accrual runs, receipts, GL postings, checklist
	mux.Handle("/dash/investment/fd/bod-eod-dashboard", midDashFull(investmentdashboards.GetFDBodEodDashboard(pgxPool)))
	// FD BOD/EOD Checklist — persist per-item user overrides + final EOD sign-off
	mux.Handle("/dash/investment/fd/bod-eod-checklist/save", midDashFull(investmentdashboards.SaveBodEodChecklistItems(pgxPool)))
	// FD Audit & Governance Dashboard — audit log, maker-checker rate, overrides, missing evidence, period reopens
	mux.Handle("/dash/investment/fd/audit-dashboard", midDashFull(investmentdashboards.GetFDAuditDashboard(pgxPool)))

	// --- Hedging Proposal Dashboard Routes ---
	// Forward Dashboard
	mux.Handle("/dash/hedge/fwd/bu-maturity-currency-summary", midDashFull(hedgeproposal.GetForwardBookingMaturityBucketsDashboard(pgxPool)))
	mux.Handle("/dash/hedge/fwd/forward-bookings", midDashFull(hedgeproposal.GetForwardBookingsDashboard(pgxPool)))
	// Exposure Dashboard
	mux.Handle("/dash/hedge/exp/bu-maturity-currency-summary", midDashFull(hedgeproposal.GetBuMaturityCurrencySummaryJoinedFromHeaders(pgxPool)))
	mux.Handle("/dash/hedge/exp/exposure-rows", midDashFull(hedgeproposal.GetExposureRowsDashboard(pgxPool)))

	//Liquidity Snapshot
	mux.Handle("/dash/liquidity/total-cash-balance-by-entity", midDashFull(liqsnap.TotalCashBalanceByEntityHandler(pgxPool)))
	mux.Handle("/dash/liquidity/liquidity-coverage-ratio", midDashFull(liqsnap.LiquidityCoverageRatioHandler(pgxPool)))
	mux.Handle("/dash/liquidity/entity-currency-wise-cash", midDashFull(liqsnap.EntityCurrencyWiseCashHandler(pgxPool)))
	mux.Handle("/dash/liquidity/kpi", midDashFull(liqsnap.KpiCardsHandler(pgxPool)))
	mux.Handle("/dash/liquidity/daily", midDashFull(liqsnap.DetailedDailyCashFlowHandler(pgxPool)))

	// Investment Overview KPIs
	mux.Handle("/dash/investment/overview/kpis", midDashInvestRead(investmentdashboards.GetInvestmentOverviewKPIs(pgxPool)))
	mux.Handle("/dash/investment/overview/entity", midDashInvestRead(investmentdashboards.GetEntityPerformance(pgxPool)))
	mux.Handle("/dash/investment/overview/amc-performance", midDashInvestRead(investmentdashboards.GetAMCPerformance(pgxPool)))
	mux.Handle("/dash/investment/overview/waterfall", midDashInvestRead(investmentdashboards.GetAMCWaterfall(pgxPool)))
	mux.Handle("/dash/investment/portfolio/aum-movement", midDashInvestRead(investmentdashboards.GetAUMMovementWaterfall(pgxPool)))
	mux.Handle("/dash/investment/portfolio/aum-breakdown", midDashInvestRead(investmentdashboards.GetAUMBreakdown(pgxPool)))
	mux.Handle("/dash/investment/performance/performance-attribution", midDashInvestRead(investmentdashboards.GetPerformanceAttribution(pgxPool)))
	mux.Handle("/dash/investment/performance/pnl-heatmap", midDashInvestRead(investmentdashboards.GetDailyPnLHeatmap(pgxPool)))
	mux.Handle("/dash/investment/performance/portfolio-vs-benchmark", midDashInvestRead(investmentdashboards.GetPortfolioVsBenchmark(pgxPool)))
	mux.Handle("/dash/investment/overview/market-rates-ticker", midDashInvestRead(investmentdashboards.GetMarketRatesTicker(pgxPool)))
	mux.Handle("/dash/investment/ticker", midDashInvestRead(investmentdashboards.GetMarketRatesTickerLite(pgxPool)))
	mux.Handle("/dash/investment/overview/top-performing", midDashInvestRead(investmentdashboards.GetTopPerformingAssets(pgxPool)))
	mux.Handle("/dash/investment/overview/aum-composition", midDashInvestRead(investmentdashboards.GetAUMCompositionTrend(pgxPool)))
	// Benchmarks: NSE live data feeds (index list, graph series, market data)
	mux.Handle("/dash/benchmarks/index-list", midDashFull(benchmarks.GetIndexList()))
	mux.Handle("/dash/benchmarks/catalog", midDashFull(benchmarks.GetBenchmarkCatalog()))
	mux.Handle("/dash/benchmarks/index-series", midDashFull(benchmarks.GetIndexSeries()))
	mux.Handle("/dash/benchmarks/index-snapshot", midDashFull(benchmarks.GetIndexSnapshot()))
	mux.Handle("/dash/benchmarks/index-constituents", midDashFull(benchmarks.GetIndexConstituents()))
	mux.Handle("/dash/benchmarks/market-movers", midDashFull(benchmarks.GetMarketMovers()))
	mux.Handle("/dash/benchmarks/market-status", midDashFull(benchmarks.GetMarketStatus()))
	mux.Handle("/dash/benchmarks/market-heatmap", midDashFull(benchmarks.GetMarketHeatmap()))
	mux.Handle("/dash/benchmarks/advance-declines", midDashFull(benchmarks.GetAdvanceDeclines()))
	mux.Handle("/dash/benchmarks/marquee", midDashFull(benchmarks.GetMarqueeData()))
	mux.Handle("/dash/benchmarks/52-week-hl", midDashFull(benchmarks.Get52WeekHighLow()))
	mux.Handle("/dash/benchmarks/market-turnover", midDashFull(benchmarks.GetMarketTurnover()))
	// Investment: Consolidated Risk Gauge (entity-level)
	mux.Handle("/dash/investment/overview/consolidated", midDashInvestRead(investmentdashboards.GetConsolidatedRisk(pgxPool)))
	// Cashflow Forecast (monthly aggregated projections + KPIs)
	mux.Handle("/dash/cash/forecast/monthly", midDashFull(cashflowforecast.GetCashflowForecastHandler(pgxPool)))
	mux.Handle("/dash/cash/forecast/kpi", midDashFull(cashflowforecast.GetForecastKPIsHandler(pgxPool)))
	mux.Handle("/dash/cash/forecast/rows", midDashFull(cashflowforecast.GetForecastRowsHandler(pgxPool)))
	mux.Handle("/dash/cash/forecast/categories", midDashFull(cashflowforecast.GetForecastCategorySumsHandler(pgxPool)))
	mux.Handle("/dash/cash/forecast/daily", midDashFull(cashflowforecast.GetForecastDailyHandler(pgxPool)))

	// Forecast vs Actual
	mux.Handle("/dash/forecast-vs-actual/rows", midDashFull(forecastVsActual.GetForecastVsActualRowsHandler(pgxPool)))
	mux.Handle("/dash/forecast-vs-actual/kpi", midDashFull(forecastVsActual.GetForecastVsActualKPIHandler(pgxPool)))
	mux.Handle("/dash/forecast-vs-actual/by-date", midDashFull(forecastVsActual.GetForecastVsActualByDateHandler(pgxPool)))
	mux.Handle("/dash/forecast-vs-actual/by-month", midDashFull(forecastVsActual.GetForecastVsActualByMonthHandler(pgxPool)))

	// --- Reports Dashboard Routes ---
	mux.Handle("/dash/reports/exposure-summary", midDashFull(reports.GetExposureSummary(pgxPool)))
	mux.Handle("/dash/reports/linked-summary-by-category", midDashFull(reports.GetLinkedSummaryByCategory(pgxPool)))

	// Projection Pipeline Dashboard
	mux.Handle("/dash/projection-pipeline/kpi", midDashFull(projectiondash.GetProjectionPipelineKPI(pgxPool)))
	mux.Handle("/dash/projection-pipeline/detailed", midDashFull(projectiondash.GetDetailedPipeline(pgxPool)))
	mux.Handle("/dash/projection-pipeline/by-entity", midDashFull(projectiondash.GetProjectionByEntity(pgxPool)))

	// Payables / Receivables dashboard rows
	mux.Handle("/dash/payrec/rows", midDashFull(payablereceivabledash.GetPayablesReceivables(pgxPool)))
	mux.Handle("/dash/payrec/forecast", midDashFull(payablereceivabledash.GetPayRecForecast(pgxPool)))

	// Planned Inflow/Outflow Dashboard
	mux.Handle("/dash/planned-inflow-outflow", midDashFull(plannedinflowoutflowdash.GetPlannedIODash(pgxPool)))

	// ── Dashboard Builder ─────────────────────────────────────────────────────
	mux.Handle("/dash/builder/dashboard/save", midDashFull(dashboardbuilder.SaveDashboard(pgxPool)))
	mux.Handle("/dash/builder/dashboard/list", midDashFull(dashboardbuilder.ListDashboards(pgxPool)))
	mux.Handle("/dash/builder/dashboard/get", midDashFull(dashboardbuilder.GetDashboardByID(pgxPool)))
	mux.Handle("/dash/builder/dashboard/delete", midDashFull(dashboardbuilder.DeleteDashboard(pgxPool)))
	mux.Handle("/dash/builder/data", midDashFull(dashboardbuilder.GetDataSource(pgxPool)))

	// ── Notification Dashboard ────────────────────────────────────────────────
	mux.Handle("/dash/notification/kpi", midDashFull(notifDash.GetKPI(pgxPool)))
	mux.Handle("/dash/notification/logs", midDashFull(notifDash.GetLogs(pgxPool)))
	mux.Handle("/dash/notification/channel-breakdown", midDashFull(notifDash.GetChannelBreakdown(pgxPool)))
	mux.Handle("/dash/notification/hourly-trend", midDashFull(notifDash.GetHourlyTrend(pgxPool)))
	mux.Handle("/dash/notification/top-events", midDashFull(notifDash.GetTopEvents(pgxPool)))
	mux.Handle("/dash/notification/timeline", midDashFull(notifDash.GetTimeline(pgxPool)))
	mux.Handle("/dash/notification/retry-stats", midDashFull(notifDash.GetRetryStats(pgxPool)))
	mux.Handle("/dash/notification/send-history", midDashFull(notifDash.GetSendHistory(pgxPool)))
	mux.Handle("/dash/notification/provider-stats", midDashFull(notifDash.GetProviderStats(pgxPool)))
	mux.Handle("/dash/notification/event-config", midDashFull(notifDash.GetEventConfig(pgxPool)))
	mux.Handle("/dash/notification/failure-reasons", midDashFull(notifDash.GetFailureReasons(pgxPool)))
	mux.Handle("/dash/notification/overview", midDashFull(notifDash.GetOverview(pgxPool)))

	server := &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
	return server, pgxPool, nil
}

func StartDashService(port string) {
	server, pool, err := NewDashServer(port)
	if err != nil {
		logger.LogError("Dashboard Service failed: %v", err)
		return
	}
	defer pool.Close()

	logger.LogInfo("Dashboard Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("Dashboard Service failed: %v", err)
	}
}
