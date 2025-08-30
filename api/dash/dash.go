package dash

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dash/buCurrExpDash"
	"CimplrCorpSaas/api/dash/cfo"
	fxops "CimplrCorpSaas/api/dash/fx-ops"
	hedgeproposal "CimplrCorpSaas/api/dash/hedging-proposal"
	reports "CimplrCorpSaas/api/dash/reports"
	"database/sql"
	"log"
	"net/http"
)

func StartDashService(db *sql.DB) {
	mux := http.NewServeMux()
	mux.HandleFunc("/dash/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Dashboard Service"))
	})

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

	// --- Hedging Proposal Dashboard Routes ---
	// Forward Dashboard
	mux.Handle("/dash/hedge/fwd/bu-maturity-currency-summary", api.BusinessUnitMiddleware(db)(hedgeproposal.GetForwardBookingMaturityBucketsDashboard(db)))
	mux.Handle("/dash/hedge/fwd/forward-bookings", api.BusinessUnitMiddleware(db)(hedgeproposal.GetForwardBookingsDashboard(db)))
	// Exposure Dashboard
	mux.Handle("/dash/hedge/exp/bu-maturity-currency-summary", api.BusinessUnitMiddleware(db)(hedgeproposal.GetBuMaturityCurrencySummaryJoinedFromHeaders(db)))
	mux.Handle("/dash/hedge/exp/exposure-rows", api.BusinessUnitMiddleware(db)(hedgeproposal.GetExposureRowsDashboard(db)))

	// --- Reports Dashboard Routes ---
	mux.Handle("/dash/reports/exposure-summary", api.BusinessUnitMiddleware(db)(reports.GetExposureSummary(db)))
	mux.Handle("/dash/reports/linked-summary-by-category", api.BusinessUnitMiddleware(db)(reports.GetLinkedSummaryByCategory(db)))

	log.Println("Dashboard Service started on :4143")
	err := http.ListenAndServe(":4143", mux)
	if err != nil {
		log.Fatalf("Dashboard Service failed: %v", err)
	}
}


