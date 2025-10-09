package fx

import (
	"CimplrCorpSaas/api"
	"context"
	"os"
	"CimplrCorpSaas/api/fx/exposures" 
	"CimplrCorpSaas/api/fx/forwards" 
	v91 "CimplrCorpSaas/api/fx/v91"  
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func StartFXService(db *sql.DB) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fx/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("FX Service is active"))
	})
	// mux.HandleFunc("/fx/forward-booking", ForwardBooking)
	
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	if user != "" && pass != "" && host != "" && port != "" && name != "" {
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, name)

		// wrapper creates pool per-request and ensures Close() after handler returns
		v91Wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 uploader: failed to create pgx pool: %v", err)
				http.Error(w, "internal server error: db connection", http.StatusInternalServerError)
				return
			}
			// ensure pool closed when request processing finishes
			defer pool.Close()

			// call the v91 handler that uses this pool
			h := v91.BatchUploadStagingData(pool)
			h.ServeHTTP(w, r)
		})

		// dashboard wrappers: create per-request pool and call the v91 dashboard handlers
		v91DashAll := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 dashboard: failed to create pgx pool: %v", err)
				http.Error(w, "internal server error: db connection", http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.GetAllExposures(pool)
			h.ServeHTTP(w, r)
		})

		v91DashByYear := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 dashboard: failed to create pgx pool: %v", err)
				http.Error(w, "internal server error: db connection", http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.GetExposuresByYear(pool)
			h.ServeHTTP(w, r)
		})

		mux.Handle("/fx/exposures/upload/v91", api.BusinessUnitMiddleware(db)(v91Wrapper))
		mux.Handle("/fx/exposures/dashboard/all/v91", api.BusinessUnitMiddleware(db)(v91DashAll))
		mux.Handle("/fx/exposures/dashboard/by-year/v91", api.BusinessUnitMiddleware(db)(v91DashByYear))
	} else {
		log.Println("v91 uploader route not registered: DB env vars not set")
	}
	
	/*-------------     exposures    ;)      --------------------*/
	/*upload */

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
	mux.Handle("/fx/forwards/create-forward-rollover", api.BusinessUnitMiddleware(db)(forwards.RolloverForwardBooking(db)))
	mux.Handle("/fx/forwards/pending-cancellations", api.BusinessUnitMiddleware(db)(forwards.GetPendingCancellations(db)))
	mux.Handle("/fx/forwards/pending-rollovers", api.BusinessUnitMiddleware(db)(forwards.GetPendingRollovers(db)))
	// Checker (approval) routes
	mux.Handle("/fx/forwards/cancellation-status-request", api.BusinessUnitMiddleware(db)(forwards.CancellationStatusRequest(db)))
	mux.Handle("/fx/forwards/rollover-status-request", api.BusinessUnitMiddleware(db)(forwards.RolloverStatusRequest(db)))

	// New Forward Booking & Confirmation routes
	mux.Handle("/fx/forwards/manual-entry", api.BusinessUnitMiddleware(db)(forwards.AddForwardBookingManualEntry(db)))
	mux.Handle("/fx/forwards/entity-relevant-list", api.BusinessUnitMiddleware(db)(forwards.GetEntityRelevantForwardBookings(db)))
	mux.Handle("/fx/forwards/update-fields", api.BusinessUnitMiddleware(db)(forwards.UpdateForwardBookingFields(db)))
	// mux.Handle("/fx/forwards/update-processing-status", api.BusinessUnitMiddleware(db)(forwards.UpdateForwardBookingProcessingStatus(db)))
	mux.Handle("/fx/forwards/bulk-update-processing-status", api.BusinessUnitMiddleware(db)(forwards.BulkUpdateForwardBookingProcessingStatus(db)))
	mux.Handle("/fx/forwards/bulk-delete", api.BusinessUnitMiddleware(db)(forwards.BulkDeleteForwardBookings(db)))
	mux.Handle("/fx/forwards/manual-confirmation-entry", api.BusinessUnitMiddleware(db)(forwards.AddForwardConfirmationManualEntry(db)))
	mux.Handle("/fx/forwards/upload-multi", api.BusinessUnitMiddleware(db)(forwards.UploadForwardBookingsMulti(db)))
	mux.Handle("/fx/forwards/upload-confirmations-multi", api.BusinessUnitMiddleware(db)(forwards.UploadForwardConfirmationsMulti(db)))
	mux.Handle("/fx/forwards/upload-bank-multi", api.BusinessUnitMiddleware(db)(forwards.UploadBankForwardBookingsMulti(db)))

	log.Println("FX Service started on :3143")
	err := http.ListenAndServe(":3143", mux)
	if err != nil {
		log.Fatalf("FX Service failed: %v", err)
	}
}

