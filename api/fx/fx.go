package fx

import (
	"CimplrCorpSaas/api"
	// "CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/fx/exposures" // <-- Import exposures
	"CimplrCorpSaas/api/fx/forwards"  // <-- Import forwards
	"database/sql"

	// "encoding/json"
	"log"
	"net/http"
)

func StartFXService(db *sql.DB) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fx/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from FX Service"))
	})
	// mux.HandleFunc("/fx/forward-booking", ForwardBooking)
	/*-------------     exposures    ;)      --------------------*/
	/*upload */
	mux.Handle("/fx/exposures/upload-exposure", api.BusinessUnitMiddleware(db)(http.HandlerFunc(exposures.UploadExposure)))
	mux.Handle("/fx/exposures/edit", api.BusinessUnitMiddleware(db)(exposures.EditExposureHeadersLineItemsJoined(db)))
	mux.Handle("/fx/exposures/headers-line-items", api.BusinessUnitMiddleware(db)(exposures.GetExposureHeadersLineItems(db)))
	mux.Handle("/fx/exposures/pending-headers-line-items", api.BusinessUnitMiddleware(db)(exposures.GetPendingApprovalHeadersLineItems(db)))
	mux.Handle("/fx/exposures/delete-multiple-headers", api.BusinessUnitMiddleware(db)(exposures.DeleteExposureHeaders(db)))
	mux.Handle("/fx/exposures/reject-multiple-headers", api.BusinessUnitMiddleware(db)(exposures.RejectMultipleExposureHeaders(db)))
	mux.Handle("/fx/exposures/approve-multiple-headers", api.BusinessUnitMiddleware(db)(exposures.ApproveMultipleExposureHeaders(db)))
	mux.Handle("/fx/exposures/batch-upload-staging", api.BusinessUnitMiddleware(db)(exposures.BatchUploadStagingData(db)))
	/*bucketing */
	mux.Handle("/fx/exposures/update-bucketing", api.BusinessUnitMiddleware(db)(exposures.UpdateExposureHeadersLineItemsBucketing(db)))
	mux.Handle("/fx/exposures/get-bucketing", api.BusinessUnitMiddleware(db)(exposures.GetExposureHeadersLineItemsBucketing(db)))
	mux.Handle("/fx/exposures/approve-bucketing-status", api.BusinessUnitMiddleware(db)(exposures.ApproveBucketingStatus(db)))
	mux.Handle("/fx/exposures/reject-bucketing-status", api.BusinessUnitMiddleware(db)(exposures.RejectBucketingStatus(db)))
	/*hedging-proposals */
	mux.Handle("/fx/exposures/get-hedging-proposals", api.BusinessUnitMiddleware(db)(exposures.GetHedgingProposalsAggregated(db)))
/*linkage */
	mux.Handle("/fx/exposures/hedge-links-details", api.BusinessUnitMiddleware(db)(exposures.HedgeLinksDetails(db)))
	mux.Handle("/fx/exposures/expfwd-linking-bookings", api.BusinessUnitMiddleware(db)(exposures.ExpFwdLinkingBookings(db)))
	mux.Handle("/fx/exposures/expfwd-linking", api.BusinessUnitMiddleware(db)(exposures.ExpFwdLinking(db)))
	mux.Handle("/fx/exposures/link-exposure-hedge", api.BusinessUnitMiddleware(db)(exposures.LinkExposureHedge(db)))

// Settlement endpoints
	mux.Handle("/fx/exposures/filter-forward-bookings-for-settlement", api.BusinessUnitMiddleware(db)(exposures.FilterForwardBookingsForSettlement(db)))
	mux.Handle("/fx/exposures/get-forward-bookings-by-entity-currency", api.BusinessUnitMiddleware(db)(exposures.GetForwardBookingsByEntityAndCurrency(db)))

	/*-------------     forward    ;)      --------------------*/
	/*mtm upload */
	mux.Handle("/fx/forwards/upload-mtm", api.BusinessUnitMiddleware(db)(forwards.UploadMTMFiles(db)))
	mux.Handle("/fx/forwards/get-mtm", api.BusinessUnitMiddleware(db)(forwards.GetMTMData(db)))

	// Forward cancel/roll endpoints
	mux.Handle("/fx/forwards/forward-booking-list", api.BusinessUnitMiddleware(db)(forwards.GetForwardBookingList(db)))
	mux.Handle("/fx/forwards/exposures-by-booking-ids", api.BusinessUnitMiddleware(db)(forwards.GetExposuresByBookingIds(db)))
	mux.Handle("/fx/forwards/create-forward-cancellations", api.BusinessUnitMiddleware(db)(forwards.CreateForwardCancellations(db)))

	log.Println("FX Service started on :3143")
	err := http.ListenAndServe(":3143", withCORS(mux))
	if err != nil {
		log.Fatalf("FX Service failed: %v", err)
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*") // Or your frontend URL
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

