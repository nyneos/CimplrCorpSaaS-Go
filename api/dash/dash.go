package dash

import (
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
 	mux.Handle("/dash/cfo/fwd/avg-forward-maturity", http.HandlerFunc(cfo.GetAvgForwardMaturity(db)))
 	mux.Handle("/dash/cfo/fwd/buy-sell-totals", http.HandlerFunc(cfo.GetForwardBuySellTotals(db)))
 	mux.Handle("/dash/cfo/fwd/user-currency", http.HandlerFunc(cfo.GetUserCurrency(db)))
 	mux.Handle("/dash/cfo/fwd/active-forwards-count", http.HandlerFunc(cfo.GetActiveForwardsCount(db)))
 	mux.Handle("/dash/cfo/fwd/recent-trades", http.HandlerFunc(cfo.GetRecentTradesDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/total-usd-sum", http.HandlerFunc(cfo.GetTotalUsdSumDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/open-to-booking-ratio", http.HandlerFunc(cfo.GetOpenAmountToBookingRatioDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/total-bank-margin", http.HandlerFunc(cfo.GetTotalBankMarginDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/total-usd-sum-by-currency", http.HandlerFunc(cfo.GetTotalUsdSumByCurrencyDashboard(db)))
 	mux.Handle("/dash/cfo/fwd/forward-booking-maturity-buckets", http.HandlerFunc(cfo.GetForwardBookingMaturityBucketsDashboard(db)))

 	// --- Exposure Dashboard Routes ---
 	mux.Handle("/dash/cfo/exp/total-open-amount-usd", http.HandlerFunc(cfo.GetTotalOpenAmountUsdSumFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/payables-by-currency", http.HandlerFunc(cfo.GetPayablesByCurrencyFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/receivables-by-currency", http.HandlerFunc(cfo.GetReceivablesByCurrencyFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/amount-by-currency", http.HandlerFunc(cfo.GetAmountByCurrencyFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/business-unit-currency-summary", http.HandlerFunc(cfo.GetBusinessUnitCurrencySummaryFromHeaders(db)))
 	mux.Handle("/dash/cfo/exp/maturity-expiry-summary", http.HandlerFunc(cfo.GetMaturityExpirySummaryFromHeaders(db)))
	mux.Handle("/dash/cfo/exp/avg-exposure-maturity", http.HandlerFunc(cfo.GetAvgExposureMaturity(db)))

	// --- Hedging Proposal Dashboard Routes ---
	// Forward Dashboard
	mux.Handle("/dash/hedge/fwd/bu-maturity-currency-summary", http.HandlerFunc(hedgeproposal.GetBuMaturityCurrencySummaryJoined(db)))

	// Exposure Dashboard
	mux.Handle("/dash/hedge/exp/bu-maturity-currency-summary", http.HandlerFunc(hedgeproposal.GetBuMaturityCurrencySummaryJoinedFromHeaders(db)))

	// --- Reports Dashboard Routes ---
	mux.Handle("/dash/reports/exposure-summary", http.HandlerFunc(reports.GetExposureSummary(db)))
	mux.Handle("/dash/reports/linked-summary-by-category", http.HandlerFunc(reports.GetLinkedSummaryByCategory(db)))

	log.Println("Dashboard Service started on :4143")
	err := http.ListenAndServe(":4143", mux)
	if err != nil {
		log.Fatalf("Dashboard Service failed: %v", err)
	}
}

