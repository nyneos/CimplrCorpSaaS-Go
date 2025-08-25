package fx

import (
	"CimplrCorpSaas/api"
	// "CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/fx/exposures" // <-- Import exposures
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
	log.Println("FX Service started on :3143")
	err := http.ListenAndServe(":3143", mux)
	if err != nil {
		log.Fatalf("FX Service failed: %v", err)
	}
}

// func ForwardBooking(w http.ResponseWriter, r *http.Request) {
// 	var req struct {
// 		UserID string `json:"user_id"`
// 		// ...other booking fields...
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, "Invalid request", http.StatusBadRequest)
// 		return
// 	}

// 	activeSessions := auth.GetActiveSessions()
// 	found := false
// 	for _, session := range activeSessions {
// 		if session.UserID == req.UserID {
// 			found = true
// 			break
// 		}
// 	}

// 	if !found {
// 		http.Error(w, "Unauthorized: invalid session", http.StatusUnauthorized)
// 		return
// 	}

// 	// Forward the booking request
// 	w.Write([]byte("Booking forwarded!"))
// }
