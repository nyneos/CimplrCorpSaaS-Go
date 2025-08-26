package dash

import (
	"CimplrCorpSaas/api/dash/buCurrExpDash"
	"CimplrCorpSaas/api/dash/cfo"
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
	mux.Handle("/dash/bu-maturity-currency-summary", http.HandlerFunc(cfo.GetBuMaturityCurrencySummaryJoined(db)))
	mux.Handle("/dash/forward-booking-maturity-buckets", http.HandlerFunc(cfo.GetForwardBookingMaturityBucketsDashboard(db)))
	mux.Handle("/dash/avg-forward-maturity", http.HandlerFunc(cfo.GetAvgForwardMaturity(db)))
	mux.Handle("/dash/forward-buy-sell-totals", http.HandlerFunc(cfo.GetForwardBuySellTotals(db)))
	mux.Handle("/dash/user-currency", http.HandlerFunc(cfo.GetUserCurrency(db)))
	mux.Handle("/dash/active-forwards-count", http.HandlerFunc(cfo.GetActiveForwardsCount(db)))
	mux.Handle("/dash/recent-trades-dashboard", http.HandlerFunc(cfo.GetRecentTradesDashboard(db)))
	mux.Handle("/dash/total-usd-sum", http.HandlerFunc(cfo.GetTotalUsdSumDashboard(db)))
	mux.Handle("/dash/open-amount-to-booking-ratio", http.HandlerFunc(cfo.GetOpenAmountToBookingRatioDashboard(db)))
	mux.Handle("/dash/total-bank-margin", http.HandlerFunc(cfo.GetTotalBankMarginDashboard(db)))
	mux.Handle("/dash/total-usd-sum-by-currency", http.HandlerFunc(cfo.GetTotalUsdSumByCurrencyDashboard(db)))

	log.Println("Dashboard Service started on :4143")
	err := http.ListenAndServe(":4143", mux)
	if err != nil {
		log.Fatalf("Dashboard Service failed: %v", err)
	}
}

