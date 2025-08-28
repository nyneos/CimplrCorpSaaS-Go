package dash

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/dash/buCurrExpDash"
	"CimplrCorpSaas/api/dash/cfo"
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
	mux.Handle("/dash/bu-curr-exp-dashboard", http.HandlerFunc(buCurrExpDash.GetDashboard(db)))

	// CFO Dashboard Endpoints
 	// --- Forward Dashboard Routes ---
 	mux.Handle("/dash/cfo/fwd/avg-forward-maturity", api.BusinessUnitMiddleware(db)(cfo.GetAvgForwardMaturity(db)))
 	mux.Handle("/dash/cfo/fwd/buy-sell-totals", api.BusinessUnitMiddleware(db)(cfo.GetForwardBuySellTotals(db)))
 	mux.Handle("/dash/cfo/fwd/user-currency", api.BusinessUnitMiddleware(db)(cfo.GetUserCurrency(db)))
 	mux.Handle("/dash/cfo/fwd/active-forwards-count", api.BusinessUnitMiddleware(db)(cfo.GetActiveForwardsCount(db)))
 	mux.Handle("/dash/cfo/fwd/recent-trades", api.BusinessUnitMiddleware(db)(cfo.GetRecentTradesDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/total-usd-sum", api.BusinessUnitMiddleware(db)(cfo.GetTotalUsdSumDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/open-to-booking-ratio", api.BusinessUnitMiddleware(db)(cfo.GetOpenAmountToBookingRatioDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/total-bank-margin", api.BusinessUnitMiddleware(db)(cfo.GetTotalBankMarginDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/total-usd-sum-by-currency", api.BusinessUnitMiddleware(db)(cfo.GetTotalUsdSumByCurrencyDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/forward-booking-maturity-buckets", api.BusinessUnitMiddleware(db)(cfo.GetForwardBookingMaturityBucketsDashboard(db)))

 	// --- Exposure Dashboard Routes ---
 	mux.Handle("/dash/cfo/exp/total-open-amount-usd", api.BusinessUnitMiddleware(db)(cfo.GetTotalOpenAmountUsdSumFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/payables-by-currency", api.BusinessUnitMiddleware(db)(cfo.GetPayablesByCurrencyFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/receivables-by-currency", api.BusinessUnitMiddleware(db)(cfo.GetReceivablesByCurrencyFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/amount-by-currency", api.BusinessUnitMiddleware(db)(cfo.GetAmountByCurrencyFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/business-unit-currency-summary", api.BusinessUnitMiddleware(db)(cfo.GetBusinessUnitCurrencySummaryFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/maturity-expiry-summary", api.BusinessUnitMiddleware(db)(cfo.GetMaturityExpirySummaryFromHeaders(db)))
	mux.Handle("/dash/cfo/exp/avg-exposure-maturity", api.BusinessUnitMiddleware(db)(cfo.GetAvgExposureMaturity(db)))

	// --- Hedging Proposal Dashboard Routes ---
	// Forward Dashboard
	mux.Handle("/dash/hedge/fwd/bu-maturity-currency-summary", api.BusinessUnitMiddleware(db)(hedgeproposal.GetBuMaturityCurrencySummaryJoined(db)))

	// Exposure Dashboard
	mux.Handle("/dash/hedge/exp/bu-maturity-currency-summary", api.BusinessUnitMiddleware(db)(hedgeproposal.GetBuMaturityCurrencySummaryJoinedFromHeaders(db)))

	// --- Reports Dashboard Routes ---
	mux.Handle("/dash/reports/exposure-summary", api.BusinessUnitMiddleware(db)(reports.GetExposureSummary(db)))
	mux.Handle("/dash/reports/linked-summary-by-category", api.BusinessUnitMiddleware(db)(reports.GetLinkedSummaryByCategory(db)))

	log.Println("Dashboard Service started on :4143")
	err := http.ListenAndServe(":4143", mux)
	if err != nil {
		log.Fatalf("Dashboard Service failed: %v", err)
	}
}
