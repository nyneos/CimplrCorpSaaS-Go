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
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func StartFXService(db *sql.DB, port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/fx/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("FX Service is active"))
	})
	// mux.HandleFunc("/fx/forward-booking", ForwardBooking)

	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	if user != "" && pass != "" && host != "" && dbPort != "" && name != "" {
		sslMode := os.Getenv("DB_SSLMODE")
		if sslMode == "" {
			sslMode = "disable"
		}
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, dbPort, name, sslMode)

		// create a shared pgx pool once for the v91 and prevalidation middleware
		pgxPool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			logger.LogError("failed to connect to pgxpool DB: %v", err)
			return
		}
		defer pgxPool.Close()

		// wrapper calls the v91 handler using the shared pool
		v91Wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := v91.BatchUploadStagingData(pgxPool)
			h.ServeHTTP(w, r)
		})

		// dashboard wrappers: create per-request pool and call the v91 dashboard handlers
		v91DashAll := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				logger.LogError("v91 dashboard: failed to create pgx pool: %v", err)
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
				logger.LogError("v91 dashboard: failed to create pgx pool: %v", err)
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
				logger.LogError("v91 bulk-update: failed to create pgx pool: %v", err)
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
				logger.LogError("v91 bulk-approve: failed to create pgx pool: %v", err)
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
				logger.LogError("v91 bulk-reject: failed to create pgx pool: %v", err)
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
				logger.LogError("v91 bulk-delete: failed to create pgx pool: %v", err)
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
				logger.LogError("v91 batches minimal: failed to create pgx pool: %v", err)
				http.Error(w, constants.ErrDBConnection, http.StatusInternalServerError)
				return
			}
			defer pool.Close()
			h := v91.GetExposureUploadBatchesMinimal(pool)
			h.ServeHTTP(w, r)
		})

		v91Download := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := v91.GetExposureDownloadURL(pgxPool)
			h.ServeHTTP(w, r)
		})

		v91BulkDownload := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := v91.GetExposureBulkDownloadURL(pgxPool)
			h.ServeHTTP(w, r)
		})

		// per-request wrapper for EditAllocationHandler (v91)
		v91EditAllocation := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pool, err := pgxpool.New(context.Background(), dsn)
			if err != nil {
				logger.LogError("v91 edit-allocation: failed to create pgx pool: %v", err)
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
		mux.Handle("/fx/exposures/download/v91", middlewares.PreValidationMiddleware(pgxPool)(v91Download))
		mux.Handle("/fx/exposures/download-bulk/v91", middlewares.PreValidationMiddleware(pgxPool)(v91BulkDownload))
		mux.Handle("/fx/exposures/audit", middlewares.PreValidationMiddleware(pgxPool)(NewFXAuditHandler(db, fxExposureAuditConfig())))
		mux.Handle("/fx/exposures/batch-upload-staging", middlewares.PreValidationMiddleware(pgxPool)(exposures.BatchUploadStagingData(db)))
		// For batch-staging uploads (exposure_headers)
		mux.Handle("/fx/exposures/download", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetExposureDownloadURL(db)))
		mux.Handle("/fx/exposures/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetExposureBulkDownloadURL(db)))
		mux.Handle("/fx/exposures/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(exposures.ListAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/exposures/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(exposures.UploadAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/exposures/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(exposures.DownloadAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/exposures/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(exposures.DownloadSelectedAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/exposures/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(exposures.DeleteAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/exposures/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(exposures.AuditAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/exposures/additional-files/delete/approve", middlewares.PreValidationMiddleware(pgxPool)(exposures.ApproveAdditionalFileDeleteHandler(pgxPool)))
		mux.Handle("/fx/exposures/additional-files/delete/reject", middlewares.PreValidationMiddleware(pgxPool)(exposures.RejectAdditionalFileDeleteHandler(pgxPool)))
		mux.Handle("/fx/exposures/edit", middlewares.PreValidationMiddleware(pgxPool)(exposures.EditExposureHeadersLineItemsJoined(db)))
		mux.Handle("/fx/exposures/headers-line-items", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetExposureHeadersLineItems(db)))
		mux.Handle("/fx/exposures/pending-headers-line-items", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetPendingApprovalHeadersLineItems(db)))
		mux.Handle("/fx/exposures/delete-multiple-headers", middlewares.PreValidationMiddleware(pgxPool)(exposures.DeleteExposureHeaders(db)))
		mux.Handle("/fx/exposures/reject-multiple-headers", middlewares.PreValidationMiddleware(pgxPool)(exposures.RejectMultipleExposureHeaders(db)))
		mux.Handle("/fx/exposures/approve-multiple-headers", middlewares.PreValidationMiddleware(pgxPool)(exposures.ApproveMultipleExposureHeaders(db)))

		/*bucketing */
		mux.Handle("/fx/exposures/update-bucketing", middlewares.PreValidationMiddleware(pgxPool)(exposures.UpdateExposureHeadersLineItemsBucketing(db)))
		mux.Handle("/fx/exposures/get-bucketing", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetExposureHeadersLineItemsBucketing(db)))
		mux.Handle("/fx/exposures/bucketing/delete-multiple-headers", middlewares.PreValidationMiddleware(pgxPool)(exposures.DeleteBucketingStatus(db)))
		mux.Handle("/fx/exposures/approve-bucketing-status", middlewares.PreValidationMiddleware(pgxPool)(exposures.ApproveBucketingStatus(db)))
		mux.Handle("/fx/exposures/reject-bucketing-status", middlewares.PreValidationMiddleware(pgxPool)(exposures.RejectBucketingStatus(db)))
		mux.Handle("/fx/exposures/bucketing/audit", middlewares.PreValidationMiddleware(pgxPool)(NewFXAuditHandler(db, fxBucketingAuditConfig())))
		mux.Handle("/fx/exposures/bucketing/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(exposures.ListExposureBucketingAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/exposures/bucketing/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(exposures.UploadExposureBucketingAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/exposures/bucketing/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(exposures.DownloadExposureBucketingAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/exposures/bucketing/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(exposures.DownloadSelectedExposureBucketingAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/exposures/bucketing/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(exposures.DeleteExposureBucketingAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/exposures/bucketing/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(exposures.AuditExposureBucketingAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/exposures/bucketing/additional-files/delete/approve", middlewares.PreValidationMiddleware(pgxPool)(exposures.ApproveExposureBucketingAdditionalFileDeleteHandler(pgxPool)))
		mux.Handle("/fx/exposures/bucketing/additional-files/delete/reject", middlewares.PreValidationMiddleware(pgxPool)(exposures.RejectExposureBucketingAdditionalFileDeleteHandler(pgxPool)))
		mux.Handle("/fx/exposures/pending-bucketing/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(exposures.ListPendingExposureBucketingAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/exposures/pending-bucketing/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(exposures.UploadPendingExposureBucketingAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/exposures/pending-bucketing/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(exposures.DownloadPendingExposureBucketingAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/exposures/pending-bucketing/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(exposures.DownloadSelectedPendingExposureBucketingAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/exposures/pending-bucketing/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(exposures.DeletePendingExposureBucketingAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/exposures/pending-bucketing/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(exposures.AuditPendingExposureBucketingAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/exposures/pending-bucketing/additional-files/delete/approve", middlewares.PreValidationMiddleware(pgxPool)(exposures.ApprovePendingExposureBucketingAdditionalFileDeleteHandler(pgxPool)))
		mux.Handle("/fx/exposures/pending-bucketing/additional-files/delete/reject", middlewares.PreValidationMiddleware(pgxPool)(exposures.RejectPendingExposureBucketingAdditionalFileDeleteHandler(pgxPool)))
		/*hedging-proposals */
		mux.Handle("/fx/exposures/get-hedging-proposals", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetHedgingProposalsAggregated(db)))
		mux.Handle("/fx/exposures/hedge-proposal/audit", middlewares.PreValidationMiddleware(pgxPool)(NewFXAuditHandler(db, fxHedgeProposalAuditConfig())))
		/*linkage */
		mux.Handle("/fx/exposures/hedge-links-details", middlewares.PreValidationMiddleware(pgxPool)(exposures.HedgeLinksDetails(db)))
		mux.Handle("/fx/exposures/expfwd-linking-bookings", middlewares.PreValidationMiddleware(pgxPool)(exposures.ExpFwdLinkingBookings(db)))
		mux.Handle("/fx/exposures/expfwd-linking", middlewares.PreValidationMiddleware(pgxPool)(exposures.ExpFwdLinking(db)))
		mux.Handle("/fx/exposures/link-exposure-hedge", middlewares.PreValidationMiddleware(pgxPool)(exposures.LinkExposureHedge(db)))
		mux.Handle("/fx/exposures/hedge-link/audit", middlewares.PreValidationMiddleware(pgxPool)(NewFXAuditHandler(db, fxHedgeLinkAuditConfig())))

		// Settlement endpoints
		mux.Handle("/fx/exposures/filter-forward-bookings-for-settlement", middlewares.PreValidationMiddleware(pgxPool)(exposures.FilterForwardBookingsForSettlement(db)))
		mux.Handle("/fx/exposures/get-forward-bookings-by-entity-currency", middlewares.PreValidationMiddleware(pgxPool)(exposures.GetForwardBookingsByEntityAndCurrency(db)))

		/*-------------     forward    ;)      --------------------*/
		/*mtm upload */
		mux.Handle("/fx/forwards/upload-mtm", middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadMTMFiles(db)))
		mux.Handle("/fx/forwards/get-mtm", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetMTMData(db)))
		mux.Handle("/fx/forwards/download-mtm", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetMTMDownloadURL(db)))
		mux.Handle("/fx/forwards/download-mtm-bulk", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetMTMBulkDownloadURL(db)))
		mux.Handle("/fx/forwards/mtm/delete", middlewares.PreValidationMiddleware(pgxPool)(forwards.RequestDeleteMTMRecords(db)))
		mux.Handle("/fx/forwards/mtm/update-processing-status", middlewares.PreValidationMiddleware(pgxPool)(forwards.BulkUpdateMTMProcessingStatus(db)))
		mux.Handle("/fx/forwards/mtm/audit", middlewares.PreValidationMiddleware(pgxPool)(NewFXAuditHandler(db, fxMTMAuditConfig())))
		mux.Handle("/fx/forwards/mtm/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(forwards.ListMTMAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/forwards/mtm/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadMTMAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/forwards/mtm/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(forwards.DownloadMTMAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/forwards/mtm/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(forwards.DownloadSelectedMTMAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/forwards/mtm/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(forwards.DeleteMTMAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/forwards/mtm/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(forwards.AuditMTMAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/forwards/mtm/additional-files/delete/approve", middlewares.PreValidationMiddleware(pgxPool)(forwards.ApproveMTMAdditionalFileDeleteHandler(pgxPool)))
		mux.Handle("/fx/forwards/mtm/additional-files/delete/reject", middlewares.PreValidationMiddleware(pgxPool)(forwards.RejectMTMAdditionalFileDeleteHandler(pgxPool)))

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
		mux.Handle("/fx/forwards/cancellation/audit", middlewares.PreValidationMiddleware(pgxPool)(NewFXAuditHandler(db, fxCancellationAuditConfig())))
		mux.Handle("/fx/forwards/rollover/audit", middlewares.PreValidationMiddleware(pgxPool)(NewFXAuditHandler(db, fxRolloverAuditConfig())))

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
		mux.Handle("/fx/forwards/download", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetForwardDownloadURL(db)))
		mux.Handle("/fx/forwards/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetForwardBulkDownloadURL(db)))
		mux.Handle("/fx/forwards/audit", middlewares.PreValidationMiddleware(pgxPool)(NewFXAuditHandler(db, fxForwardAuditConfig())))
		mux.Handle("/fx/forwards/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(forwards.ListAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/forwards/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(forwards.UploadAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/forwards/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(forwards.DownloadAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/forwards/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(forwards.DownloadSelectedAdditionalFilesHandler(pgxPool)))
		mux.Handle("/fx/forwards/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(forwards.DeleteAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/forwards/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(forwards.AuditAdditionalFileHandler(pgxPool)))
		mux.Handle("/fx/forwards/additional-files/delete/approve", middlewares.PreValidationMiddleware(pgxPool)(forwards.ApproveAdditionalFileDeleteHandler(pgxPool)))
		mux.Handle("/fx/forwards/additional-files/delete/reject", middlewares.PreValidationMiddleware(pgxPool)(forwards.RejectAdditionalFileDeleteHandler(pgxPool)))
		mux.Handle("/fx/forwards/download-confirmations-bulk", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetForwardConfirmationBulkDownloadURL(db)))
		mux.Handle("/fx/forwards/download-bank-bulk", middlewares.PreValidationMiddleware(pgxPool)(forwards.GetForwardBankBulkDownloadURL(db)))

	} else {
		logger.LogInfo("v91 uploader route not registered: DB env vars not set")
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

	logger.LogInfo("FX Service started on :%s", port)
	err := http.ListenAndServe(":"+port, mux)
	if err != nil {
		logger.LogError("FX Service failed: %v", err)
	}
}
