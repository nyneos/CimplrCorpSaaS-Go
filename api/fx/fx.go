package fx

import (
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/fx/exposures"
	"CimplrCorpSaas/api/fx/forwards"
	v91 "CimplrCorpSaas/api/fx/v91"
	middlewares "CimplrCorpSaas/api/middlewares"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

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

		// create a shared pgx pool once for the v91 and prevalidation middleware
		pgxPool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			log.Fatalf("failed to connect to pgxpool DB: %v", err)
		}

		// wrapper calls the v91 handler using the shared pool
		v91Wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := v91.BatchUploadStagingData(pgxPool)
			h.ServeHTTP(w, r)
		})

		// dashboard wrappers: create per-request pool and call the v91 dashboard handlers
		v91DashAll := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 dashboard: failed to create pgx pool: %v", err)
				http.Error(w, constants.ErrDBConnection, http.StatusInternalServerError)
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
				http.Error(w, constants.ErrDBConnection, http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.GetExposuresByYear(pool)
			h.ServeHTTP(w, r)
		})

		v91BulkUpdate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 bulk-update: failed to create pgx pool: %v", err)
				http.Error(w, constants.ErrDBConnection, http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.BulkUpdateValueDates(pool)
			h.ServeHTTP(w, r)
		})

		v91BulkApprove := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 bulk-approve: failed to create pgx pool: %v", err)
				http.Error(w, constants.ErrDBConnection, http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.BulkApproveExposures(pool)
			h.ServeHTTP(w, r)
		})

		v91BulkReject := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 bulk-reject: failed to create pgx pool: %v", err)
				http.Error(w, constants.ErrDBConnection, http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.BulkRejectExposures(pool)
			h.ServeHTTP(w, r)
		})

		v91BulkDelete := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 bulk-delete: failed to create pgx pool: %v", err)
				http.Error(w, constants.ErrDBConnection, http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.BulkDeleteExposures(pool)
			h.ServeHTTP(w, r)
		})

		v91BatchesMinimal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 batches minimal: failed to create pgx pool: %v", err)
				http.Error(w, constants.ErrDBConnection, http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.GetExposureUploadBatchesMinimal(pool)
			h.ServeHTTP(w, r)
		})

		// per-request wrapper for EditAllocationHandler (v91)
		v91EditAllocation := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				log.Printf("v91 edit-allocation: failed to create pgx pool: %v", err)
				http.Error(w, constants.ErrDBConnection, http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.EditAllocationsHandler(pool)
			h.ServeHTTP(w, r)
		})

		mux.Handle("/fx/exposures/upload/v91", middlewares.PreValidationMiddleware(pgxPool)(v91Wrapper))
		mux.Handle("/fx/exposures/dashboard/all/v91", middlewares.PreValidationMiddleware(pgxPool)(v91DashAll))
		mux.Handle("/fx/exposures/dashboard/by-year/v91", middlewares.PreValidationMiddleware(pgxPool)(v91DashByYear))
		mux.Handle("/fx/exposures/bulk-update-value-dates/v91", middlewares.PreValidationMiddleware(pgxPool)(v91BulkUpdate))
		// v91 bulk approve/reject/delete handlers
		mux.Handle("/fx/exposures/bulk-approve/v91", middlewares.PreValidationMiddleware(pgxPool)(v91BulkApprove))
		mux.Handle("/fx/exposures/bulk-reject/v91", middlewares.PreValidationMiddleware(pgxPool)(v91BulkReject))
		mux.Handle("/fx/exposures/bulk-delete/v91", middlewares.PreValidationMiddleware(pgxPool)(v91BulkDelete))
		mux.Handle("/fx/exposures/edit-allocation/v91", middlewares.PreValidationMiddleware(pgxPool)(v91EditAllocation))
		mux.Handle("/fx/exposures/get-file/v91", middlewares.PreValidationMiddleware(pgxPool)(v91BatchesMinimal))
		mux.Handle("/fx/exposures/batch-upload-staging", middlewares.PreValidationMiddleware(pgxPool)(exposures.BatchUploadStagingData(db)))
		mux.Handle("/fx/exposures/edit", middlewares.PreValidationMiddleware(pgxPool)(exposures.EditExposureHeadersLineItemsJoined(db)))
		mux.Handle("/fx/exposures/headers-line-items", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetExposureHeadersLineItems(db)))
		mux.Handle("/fx/exposures/pending-headers-line-items", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetPendingApprovalHeadersLineItems(db)))
		mux.Handle("/fx/exposures/delete-multiple-headers", middlewares.PreValidationMiddleware(pgxPool)(exposures.DeleteExposureHeaders(db)))
		mux.Handle("/fx/exposures/reject-multiple-headers", middlewares.PreValidationMiddleware(pgxPool)(exposures.RejectMultipleExposureHeaders(db)))
		mux.Handle("/fx/exposures/approve-multiple-headers", middlewares.PreValidationMiddleware(pgxPool)(exposures.ApproveMultipleExposureHeaders(db)))

		/*bucketing */
		mux.Handle("/fx/exposures/update-bucketing", middlewares.PreValidationMiddleware(pgxPool)(exposures.UpdateExposureHeadersLineItemsBucketing(db)))
		mux.Handle("/fx/exposures/get-bucketing", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetExposureHeadersLineItemsBucketing(db)))
		mux.Handle("/fx/exposures/approve-bucketing-status", middlewares.PreValidationMiddleware(pgxPool)(exposures.ApproveBucketingStatus(db)))
		mux.Handle("/fx/exposures/reject-bucketing-status", middlewares.PreValidationMiddleware(pgxPool)(exposures.RejectBucketingStatus(db)))
		/*hedging-proposals */
		mux.Handle("/fx/exposures/get-hedging-proposals", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetHedgingProposalsAggregated(db)))
		/*linkage */
		mux.Handle("/fx/exposures/hedge-links-details", middlewares.PreValidationMiddleware(pgxPool)(exposures.HedgeLinksDetails(db)))
		mux.Handle("/fx/exposures/expfwd-linking-bookings", middlewares.PreValidationMiddleware(pgxPool)(exposures.ExpFwdLinkingBookings(db)))
		mux.Handle("/fx/exposures/expfwd-linking", middlewares.PreValidationMiddleware(pgxPool)(exposures.ExpFwdLinking(db)))
		mux.Handle("/fx/exposures/link-exposure-hedge", middlewares.PreValidationMiddleware(pgxPool)(exposures.LinkExposureHedge(db)))

		// Settlement endpoints
		mux.Handle("/fx/exposures/filter-forward-bookings-for-settlement", middlewares.PreValidationMiddleware(pgxPool)(exposures.FilterForwardBookingsForSettlement(db)))
		mux.Handle("/fx/exposures/get-forward-bookings-by-entity-currency", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetForwardBookingsByEntityAndCurrency(db)))

		/*-------------     forward    ;)      --------------------*/
		/*mtm upload */
		mux.Handle("/fx/forwards/upload-mtm", middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadMTMFiles(db)))
		mux.Handle("/fx/forwards/get-mtm", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetMTMData(db)))

		// Forward cancel/roll endpoints
		mux.Handle("/fx/forwards/forward-booking-list", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetForwardBookingList(db)))
		mux.Handle("/fx/forwards/exposures-by-booking-ids", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetExposuresByBookingIds(db)))
		mux.Handle("/fx/forwards/create-forward-cancellations", middlewares.PreValidationMiddleware(pgxPool)(forwards.CreateForwardCancellations(db)))
		mux.Handle("/fx/forwards/create-forward-rollover", middlewares.PreValidationMiddleware(pgxPool)(forwards.RolloverForwardBooking(db)))
		mux.Handle("/fx/forwards/pending-cancellations", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetPendingCancellations(db)))
		mux.Handle("/fx/forwards/pending-rollovers", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetPendingRollovers(db)))
		// Checker (approval) routes
		mux.Handle("/fx/forwards/cancellation-status-request", middlewares.PreValidationMiddleware(pgxPool)(forwards.CancellationStatusRequest(db)))
		mux.Handle("/fx/forwards/rollover-status-request", middlewares.PreValidationMiddleware(pgxPool)(forwards.RolloverStatusRequest(db)))

		// New Forward Booking & Confirmation routes
		mux.Handle("/fx/forwards/manual-entry", middlewares.PreValidationMiddleware(pgxPool)(forwards.AddForwardBookingManualEntry(db)))
		mux.Handle("/fx/forwards/entity-relevant-list", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetEntityRelevantForwardBookings(db)))
		mux.Handle("/fx/forwards/update-fields", middlewares.PreValidationMiddleware(pgxPool)(forwards.UpdateForwardBookingFields(db)))
		// mux.Handle("/fx/forwards/update-processing-status",  middlewares.PreValidationMiddleware(pgxPool)(forwards.UpdateForwardBookingProcessingStatus(db)))
		mux.Handle("/fx/forwards/bulk-update-processing-status", middlewares.PreValidationMiddleware(pgxPool)(forwards.BulkUpdateForwardBookingProcessingStatus(db)))
		mux.Handle("/fx/forwards/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(forwards.BulkDeleteForwardBookings(db)))
		mux.Handle("/fx/forwards/manual-confirmation-entry", middlewares.PreValidationMiddleware(pgxPool)(forwards.AddForwardConfirmationManualEntry(db)))
		mux.Handle("/fx/forwards/upload-multi", middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadForwardBookingsMulti(db)))
		mux.Handle("/fx/forwards/upload-confirmations-multi", middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadForwardConfirmationsMulti(db)))
		mux.Handle("/fx/forwards/upload-bank-multi", middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadBankForwardBookingsMulti(db)))

	} else {
		log.Println("v91 uploader route not registered: DB env vars not set")
	}
	/*-------------     exposures    ;)      --------------------*/
	// /*upload */

	// mux.Handle("/fx/exposures/edit",  middlewares.PreValidationMiddleware(pgxPool)(exposures.EditExposureHeadersLineItemsJoined(db)))
	// mux.Handle("/fx/exposures/headers-line-items",  middlewares.PreValidationMiddleware(pgxPool)(exposures.GetExposureHeadersLineItems(db)))
	// mux.Handle("/fx/exposures/pending-headers-line-items",  middlewares.PreValidationMiddleware(pgxPool)(exposures.GetPendingApprovalHeadersLineItems(db)))
	// mux.Handle("/fx/exposures/delete-multiple-headers",  middlewares.PreValidationMiddleware(pgxPool)(exposures.DeleteExposureHeaders(db)))
	// mux.Handle("/fx/exposures/reject-multiple-headers",  middlewares.PreValidationMiddleware(pgxPool)(exposures.RejectMultipleExposureHeaders(db)))
	// mux.Handle("/fx/exposures/approve-multiple-headers",  middlewares.PreValidationMiddleware(pgxPool)(exposures.ApproveMultipleExposureHeaders(db)))

	// /*bucketing */
	// mux.Handle("/fx/exposures/update-bucketing",  middlewares.PreValidationMiddleware(pgxPool)(exposures.UpdateExposureHeadersLineItemsBucketing(db)))
	// mux.Handle("/fx/exposures/get-bucketing",  middlewares.PreValidationMiddleware(pgxPool)(exposures.GetExposureHeadersLineItemsBucketing(db)))
	// mux.Handle("/fx/exposures/approve-bucketing-status",  middlewares.PreValidationMiddleware(pgxPool)(exposures.ApproveBucketingStatus(db)))
	// mux.Handle("/fx/exposures/reject-bucketing-status",  middlewares.PreValidationMiddleware(pgxPool)(exposures.RejectBucketingStatus(db)))
	// /*hedging-proposals */
	// mux.Handle("/fx/exposures/get-hedging-proposals",  middlewares.PreValidationMiddleware(pgxPool)(exposures.GetHedgingProposalsAggregated(db)))
	// /*linkage */
	// mux.Handle("/fx/exposures/hedge-links-details",  middlewares.PreValidationMiddleware(pgxPool)(exposures.HedgeLinksDetails(db)))
	// mux.Handle("/fx/exposures/expfwd-linking-bookings",  middlewares.PreValidationMiddleware(pgxPool)(exposures.ExpFwdLinkingBookings(db)))
	// mux.Handle("/fx/exposures/expfwd-linking",  middlewares.PreValidationMiddleware(pgxPool)(exposures.ExpFwdLinking(db)))
	// mux.Handle("/fx/exposures/link-exposure-hedge",  middlewares.PreValidationMiddleware(pgxPool)(exposures.LinkExposureHedge(db)))

	// // Settlement endpoints
	// mux.Handle("/fx/exposures/filter-forward-bookings-for-settlement",  middlewares.PreValidationMiddleware(pgxPool)(exposures.FilterForwardBookingsForSettlement(db)))
	// mux.Handle("/fx/exposures/get-forward-bookings-by-entity-currency",  middlewares.PreValidationMiddleware(pgxPool)(exposures.GetForwardBookingsByEntityAndCurrency(db)))

	// /*-------------     forward    ;)      --------------------*/
	// /*mtm upload */
	// mux.Handle("/fx/forwards/upload-mtm",  middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadMTMFiles(db)))
	// mux.Handle("/fx/forwards/get-mtm",  middlewares.PreValidationMiddleware(pgxPool)(forwards.GetMTMData(db)))

	// // Forward cancel/roll endpoints
	// mux.Handle("/fx/forwards/forward-booking-list",  middlewares.PreValidationMiddleware(pgxPool)(forwards.GetForwardBookingList(db)))
	// mux.Handle("/fx/forwards/exposures-by-booking-ids",  middlewares.PreValidationMiddleware(pgxPool)(forwards.GetExposuresByBookingIds(db)))
	// mux.Handle("/fx/forwards/create-forward-cancellations",  middlewares.PreValidationMiddleware(pgxPool)(forwards.CreateForwardCancellations(db)))
	// mux.Handle("/fx/forwards/create-forward-rollover",  middlewares.PreValidationMiddleware(pgxPool)(forwards.RolloverForwardBooking(db)))
	// mux.Handle("/fx/forwards/pending-cancellations",  middlewares.PreValidationMiddleware(pgxPool)(forwards.GetPendingCancellations(db)))
	// mux.Handle("/fx/forwards/pending-rollovers",  middlewares.PreValidationMiddleware(pgxPool)(forwards.GetPendingRollovers(db)))
	// // Checker (approval) routes
	// mux.Handle("/fx/forwards/cancellation-status-request",  middlewares.PreValidationMiddleware(pgxPool)(forwards.CancellationStatusRequest(db)))
	// mux.Handle("/fx/forwards/rollover-status-request",  middlewares.PreValidationMiddleware(pgxPool)(forwards.RolloverStatusRequest(db)))

	// // New Forward Booking & Confirmation routes
	// mux.Handle("/fx/forwards/manual-entry",  middlewares.PreValidationMiddleware(pgxPool)(forwards.AddForwardBookingManualEntry(db)))
	// mux.Handle("/fx/forwards/entity-relevant-list",  middlewares.PreValidationMiddleware(pgxPool)(forwards.GetEntityRelevantForwardBookings(db)))
	// mux.Handle("/fx/forwards/update-fields",  middlewares.PreValidationMiddleware(pgxPool)(forwards.UpdateForwardBookingFields(db)))
	// // mux.Handle("/fx/forwards/update-processing-status",  middlewares.PreValidationMiddleware(pgxPool)(forwards.UpdateForwardBookingProcessingStatus(db)))
	// mux.Handle("/fx/forwards/bulk-update-processing-status",  middlewares.PreValidationMiddleware(pgxPool)(forwards.BulkUpdateForwardBookingProcessingStatus(db)))
	// mux.Handle("/fx/forwards/bulk-delete",  middlewares.PreValidationMiddleware(pgxPool)(forwards.BulkDeleteForwardBookings(db)))
	// mux.Handle("/fx/forwards/manual-confirmation-entry",  middlewares.PreValidationMiddleware(pgxPool)(forwards.AddForwardConfirmationManualEntry(db)))
	// mux.Handle("/fx/forwards/upload-multi",  middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadForwardBookingsMulti(db)))
	// mux.Handle("/fx/forwards/upload-confirmations-multi",  middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadForwardConfirmationsMulti(db)))
	// mux.Handle("/fx/forwards/upload-bank-multi",  middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadBankForwardBookingsMulti(db)))

	log.Println("FX Service started on :3143")
	err := http.ListenAndServe(":3143", mux)
	if err != nil {
		log.Fatalf("FX Service failed: %v", err)
	}
}
