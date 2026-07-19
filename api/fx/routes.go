package fx

import (
	"CimplrCorpSaas/api/fx/exposures"
	"CimplrCorpSaas/api/fx/forwards"
	v91 "CimplrCorpSaas/api/fx/v91"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/observability"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterFXRoutes wires every /fx/* route onto mux. Route registration
// only — handler logic lives in the exposures/forwards/v91 packages.
func RegisterFXRoutes(mux *http.ServeMux, serviceName string, pgxPool *pgxpool.Pool) {
	mux.HandleFunc("/fx/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("FX Service is active"))
	})
	mux.Handle("/fx/metrics", observability.MetricsHandler(serviceName))

	// FX uses session + global independent (banks, currencies) + global dependent
	// (bank accounts, GL accounts) — sufficient for forward/exposure entity filtering.
	midFX := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(
			middlewares.GlobalIndependentMiddleware(pgxPool)(
				middlewares.GlobalDependentMiddleware(pgxPool)(h),
			),
		)
	}

	// wrapper calls the v91 handler using the shared pool
	v91Wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := v91.BatchUploadStagingData(pgxPool)
		h.ServeHTTP(w, r)
	})

	v91DashAll := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.GetAllExposures(pgxPool).ServeHTTP(w, r)
	})

	v91DashByYear := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.GetExposuresByYear(pgxPool).ServeHTTP(w, r)
	})

	v91BulkUpdate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.BulkUpdateValueDates(pgxPool).ServeHTTP(w, r)
	})

	v91BulkApprove := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.BulkApproveExposures(pgxPool).ServeHTTP(w, r)
	})

	v91BulkReject := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.BulkRejectExposures(pgxPool).ServeHTTP(w, r)
	})

	v91BulkDelete := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.BulkDeleteExposures(pgxPool).ServeHTTP(w, r)
	})

	v91BatchesMinimal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.GetExposureUploadBatchesMinimal(pgxPool).ServeHTTP(w, r)
	})

	v91Download := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := v91.GetExposureDownloadURL(pgxPool)
		h.ServeHTTP(w, r)
	})

	v91BulkDownload := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := v91.GetExposureBulkDownloadURL(pgxPool)
		h.ServeHTTP(w, r)
	})

	v91EditAllocation := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.EditAllocationsHandler(pgxPool).ServeHTTP(w, r)
	})

	v91BatchDetail := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.GetBatchDetailV91(pgxPool).ServeHTTP(w, r)
	})

	v91BatchAudit := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v91.GetBatchExposureAuditV91(pgxPool).ServeHTTP(w, r)
	})

	mux.Handle("/fx/exposures/v91/upload", midFX(v91Wrapper))
	mux.Handle("/fx/exposures/v91/dashboard-all", midFX(v91DashAll))
	mux.Handle("/fx/exposures/v91/dashboard-by-year", midFX(v91DashByYear))
	mux.Handle("/fx/exposures/v91/bulk-update-value-dates", midFX(v91BulkUpdate))
	mux.Handle("/fx/exposures/v91/bulk-approve", midFX(v91BulkApprove))
	mux.Handle("/fx/exposures/v91/bulk-reject", midFX(v91BulkReject))
	mux.Handle("/fx/exposures/v91/bulk-delete", midFX(v91BulkDelete))
	mux.Handle("/fx/exposures/v91/upload/bulk-approve", midFX(v91BulkApprove))
	mux.Handle("/fx/exposures/v91/upload/bulk-reject", midFX(v91BulkReject))
	mux.Handle("/fx/exposures/v91/upload/bulk-delete", midFX(v91BulkDelete))
	mux.Handle("/fx/exposures/v91/edit-allocation", midFX(v91EditAllocation))
	mux.Handle("/fx/exposures/v91/upload-detail", midFX(v91BatchDetail))
	mux.Handle("/fx/exposures/v91/upload-audit", midFX(v91BatchAudit))
	mux.Handle("/fx/exposures/v91/files", midFX(v91BatchesMinimal))
	mux.Handle("/fx/exposures/v91/download", midFX(v91Download))
	mux.Handle("/fx/exposures/v91/download-bulk", midFX(v91BulkDownload))

	// Legacy v91 route aliases (deprecated — use /fx/exposures/v91/* above)
	mux.Handle("/fx/exposures/upload/v91", midFX(v91Wrapper))
	mux.Handle("/fx/exposures/dashboard/all/v91", midFX(v91DashAll))
	mux.Handle("/fx/exposures/dashboard/by-year/v91", midFX(v91DashByYear))
	mux.Handle("/fx/exposures/bulk-update-value-dates/v91", midFX(v91BulkUpdate))
	mux.Handle("/fx/exposures/bulk-approve/v91", midFX(v91BulkApprove))
	mux.Handle("/fx/exposures/bulk-reject/v91", midFX(v91BulkReject))
	mux.Handle("/fx/exposures/bulk-delete/v91", midFX(v91BulkDelete))
	mux.Handle("/fx/exposures/edit-allocation/v91", midFX(v91EditAllocation))
	mux.Handle("/fx/exposures/batch-detail/v91", midFX(v91BatchDetail))
	mux.Handle("/fx/exposures/upload-detail/v91", midFX(v91BatchDetail))
	mux.Handle("/fx/exposures/batch-audit/v91", midFX(v91BatchAudit))
	mux.Handle("/fx/exposures/upload-audit/v91", midFX(v91BatchAudit))
	mux.Handle("/fx/exposures/get-file/v91", midFX(v91BatchesMinimal))
	mux.Handle("/fx/exposures/download/v91", midFX(v91Download))
	mux.Handle("/fx/exposures/download-bulk/v91", midFX(v91BulkDownload))
	mux.Handle("/fx/exposures/audit", midFX(NewFXAuditHandler(pgxPool, fxExposureAuditConfig())))
	mux.Handle("/fx/exposures/batch-upload-staging", midFX(exposures.BatchUploadStagingData(pgxPool)))
	// For batch-staging uploads (exposure_headers)
	mux.Handle("/fx/exposures/download", midFX(exposures.GetExposureDownloadURL(pgxPool)))
	mux.Handle("/fx/exposures/download-bulk", midFX(exposures.GetExposureBulkDownloadURL(pgxPool)))
	mux.Handle("/fx/exposures/package-zip", midFX(exposures.DownloadExposurePackageZipHandler(pgxPool)))
	mux.Handle("/fx/exposures/additional-files/list", midFX(exposures.ListAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/exposures/additional-files/upload", midFX(exposures.UploadAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/exposures/additional-files/download", midFX(exposures.DownloadAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/exposures/additional-files/download-bulk", midFX(exposures.DownloadSelectedAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/exposures/additional-files/delete", midFX(exposures.DeleteAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/exposures/additional-files/audit", midFX(exposures.AuditAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/exposures/additional-files/delete/approve", midFX(exposures.ApproveAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/fx/exposures/additional-files/delete/reject", midFX(exposures.RejectAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/fx/exposures/edit", midFX(exposures.EditExposureHeadersLineItemsJoined(pgxPool)))
	mux.Handle("/fx/exposures/headers-line-items", midFX(exposures.GetExposureHeadersLineItems(pgxPool)))
	mux.Handle("/fx/exposures/pending-headers-line-items", midFX(exposures.GetPendingApprovalHeadersLineItems(pgxPool)))
	mux.Handle("/fx/exposures/delete-multiple-headers", midFX(exposures.DeleteExposureHeaders(pgxPool)))
	mux.Handle("/fx/exposures/reject-multiple-headers", midFX(exposures.RejectMultipleExposureHeaders(pgxPool)))
	mux.Handle("/fx/exposures/approve-multiple-headers", midFX(exposures.ApproveMultipleExposureHeaders(pgxPool)))

	/*bucketing */
	mux.Handle("/fx/exposures/update-bucketing", midFX(exposures.UpdateExposureHeadersLineItemsBucketing(pgxPool)))
	mux.Handle("/fx/exposures/get-bucketing", midFX(exposures.GetExposureHeadersLineItemsBucketing(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/delete-multiple-headers", midFX(exposures.DeleteBucketingStatus(pgxPool)))
	mux.Handle("/fx/exposures/approve-bucketing-status", midFX(exposures.ApproveBucketingStatus(pgxPool)))
	mux.Handle("/fx/exposures/reject-bucketing-status", midFX(exposures.RejectBucketingStatus(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/audit", midFX(NewFXAuditHandler(pgxPool, fxBucketingAuditConfig())))
	mux.Handle("/fx/exposures/bucketing/package-zip", midFX(exposures.DownloadExposureBucketingPackageZipHandler(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/additional-files/list", midFX(exposures.ListExposureBucketingAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/additional-files/upload", midFX(exposures.UploadExposureBucketingAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/additional-files/download", midFX(exposures.DownloadExposureBucketingAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/additional-files/download-bulk", midFX(exposures.DownloadSelectedExposureBucketingAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/additional-files/delete", midFX(exposures.DeleteExposureBucketingAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/additional-files/audit", midFX(exposures.AuditExposureBucketingAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/additional-files/delete/approve", midFX(exposures.ApproveExposureBucketingAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/fx/exposures/bucketing/additional-files/delete/reject", midFX(exposures.RejectExposureBucketingAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/fx/exposures/pending-bucketing/package-zip", midFX(exposures.DownloadPendingExposureBucketingPackageZipHandler(pgxPool)))
	mux.Handle("/fx/exposures/pending-bucketing/additional-files/list", midFX(exposures.ListPendingExposureBucketingAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/exposures/pending-bucketing/additional-files/upload", midFX(exposures.UploadPendingExposureBucketingAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/exposures/pending-bucketing/additional-files/download", midFX(exposures.DownloadPendingExposureBucketingAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/exposures/pending-bucketing/additional-files/download-bulk", midFX(exposures.DownloadSelectedPendingExposureBucketingAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/exposures/pending-bucketing/additional-files/delete", midFX(exposures.DeletePendingExposureBucketingAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/exposures/pending-bucketing/additional-files/audit", midFX(exposures.AuditPendingExposureBucketingAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/exposures/pending-bucketing/additional-files/delete/approve", midFX(exposures.ApprovePendingExposureBucketingAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/fx/exposures/pending-bucketing/additional-files/delete/reject", midFX(exposures.RejectPendingExposureBucketingAdditionalFileDeleteHandler(pgxPool)))
	/*hedging-proposals */
	mux.Handle("/fx/exposures/get-hedging-proposals", midFX(exposures.GetHedgingProposalsAggregated(pgxPool)))
	mux.Handle("/fx/exposures/hedge-proposal/audit", midFX(NewFXAuditHandler(pgxPool, fxHedgeProposalAuditConfig())))
	/*linkage */
	mux.Handle("/fx/exposures/hedge-links-details", midFX(exposures.HedgeLinksDetails(pgxPool)))
	mux.Handle("/fx/exposures/expfwd-linking-bookings", midFX(exposures.ExpFwdLinkingBookings(pgxPool)))
	mux.Handle("/fx/exposures/expfwd-linking", midFX(exposures.ExpFwdLinking(pgxPool)))
	mux.Handle("/fx/exposures/link-exposure-hedge", midFX(exposures.LinkExposureHedge(pgxPool)))
	mux.Handle("/fx/exposures/hedge-link/audit", midFX(NewFXAuditHandler(pgxPool, fxHedgeLinkAuditConfig())))

	// Settlement endpoints
	mux.Handle("/fx/exposures/filter-forward-bookings-for-settlement", midFX(exposures.FilterForwardBookingsForSettlement(pgxPool)))
	mux.Handle("/fx/exposures/get-forward-bookings-by-entity-currency", midFX(exposures.GetForwardBookingsByEntityAndCurrency(pgxPool)))

	/*-------------     forward    ;)      --------------------*/
	/*mtm upload */
	mux.Handle("/fx/forwards/upload-mtm", midFX(forwards.UploadMTMFiles(pgxPool)))
	mux.Handle("/fx/forwards/get-mtm", midFX(forwards.GetMTMData(pgxPool)))
	mux.Handle("/fx/forwards/download-mtm", midFX(forwards.GetMTMDownloadURL(pgxPool)))
	mux.Handle("/fx/forwards/download-mtm-bulk", midFX(forwards.GetMTMBulkDownloadURL(pgxPool)))
	mux.Handle("/fx/forwards/mtm/delete", midFX(forwards.RequestDeleteMTMRecords(pgxPool)))
	mux.Handle("/fx/forwards/mtm/update-processing-status", midFX(forwards.BulkUpdateMTMProcessingStatus(pgxPool)))
	mux.Handle("/fx/forwards/mtm/audit", midFX(NewFXAuditHandler(pgxPool, fxMTMAuditConfig())))
	mux.Handle("/fx/forwards/mtm/package-zip", midFX(forwards.DownloadMTMPackageZipHandler(pgxPool)))
	mux.Handle("/fx/forwards/mtm/additional-files/list", midFX(forwards.ListMTMAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/forwards/mtm/additional-files/upload", midFX(forwards.UploadMTMAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/forwards/mtm/additional-files/download", midFX(forwards.DownloadMTMAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/forwards/mtm/additional-files/download-bulk", midFX(forwards.DownloadSelectedMTMAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/forwards/mtm/additional-files/delete", midFX(forwards.DeleteMTMAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/forwards/mtm/additional-files/audit", midFX(forwards.AuditMTMAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/forwards/mtm/additional-files/delete/approve", midFX(forwards.ApproveMTMAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/fx/forwards/mtm/additional-files/delete/reject", midFX(forwards.RejectMTMAdditionalFileDeleteHandler(pgxPool)))

	mux.Handle("/fx/forwards/forward-booking-list", midFX(forwards.GetForwardBookingList(pgxPool)))
	mux.Handle("/fx/forwards/exposures-by-booking-ids", midFX(forwards.GetExposuresByBookingIds(pgxPool)))
	mux.Handle("/fx/forwards/create-forward-cancellations", midFX(forwards.CreateForwardCancellations(pgxPool)))
	mux.Handle("/fx/forwards/create-forward-rollover", midFX(forwards.RolloverForwardBooking(pgxPool)))
	mux.Handle("/fx/forwards/pending-cancellations", midFX(forwards.GetPendingCancellations(pgxPool)))
	mux.Handle("/fx/forwards/pending-rollovers", midFX(forwards.GetPendingRollovers(pgxPool)))
	mux.Handle("/fx/forwards/cancel-roll/all", midFX(forwards.GetAllCancellationRollovers(pgxPool)))
	mux.Handle("/fx/forwards/cancel-roll/action", midFX(forwards.CancellationRolloverAction(pgxPool)))

	// Checker (approval) routes
	mux.Handle("/fx/forwards/cancellation-status-request", midFX(forwards.CancellationStatusRequest(pgxPool)))
	mux.Handle("/fx/forwards/rollover-status-request", midFX(forwards.RolloverStatusRequest(pgxPool)))
	mux.Handle("/fx/forwards/cancellation/audit", midFX(NewFXAuditHandler(pgxPool, fxCancellationAuditConfig())))
	mux.Handle("/fx/forwards/rollover/audit", midFX(NewFXAuditHandler(pgxPool, fxRolloverAuditConfig())))

	// New Forward Booking & Confirmation routes
	mux.Handle("/fx/forwards/manual-entry", midFX(forwards.AddForwardBookingManualEntry(pgxPool)))
	mux.Handle("/fx/forwards/entity-relevant-list", midFX(forwards.GetEntityRelevantForwardBookings(pgxPool)))
	mux.Handle("/fx/forwards/update-fields", midFX(forwards.UpdateForwardBookingFields(pgxPool)))
	// mux.Handle("/fx/forwards/update-processing-status", midFX(forwards.UpdateForwardBookingProcessingStatus(db)))
	mux.Handle("/fx/forwards/bulk-update-processing-status", midFX(forwards.BulkUpdateForwardBookingProcessingStatus(pgxPool)))
	mux.Handle("/fx/forwards/bulk-delete", midFX(forwards.BulkDeleteForwardBookings(pgxPool)))
	mux.Handle("/fx/forwards/manual-confirmation-entry", midFX(forwards.AddForwardConfirmationManualEntry(pgxPool)))
	mux.Handle("/fx/forwards/upload-multi", midFX(forwards.UploadForwardBookingsMulti(pgxPool)))
	mux.Handle("/fx/forwards/upload-confirmations-multi", midFX(forwards.UploadForwardConfirmationsMulti(pgxPool)))
	mux.Handle("/fx/forwards/upload-bank-multi", midFX(forwards.UploadBankForwardBookingsMulti(pgxPool)))
	mux.Handle("/fx/forwards/download", midFX(forwards.GetForwardDownloadURL(pgxPool)))
	mux.Handle("/fx/forwards/download-bulk", midFX(forwards.GetForwardBulkDownloadURL(pgxPool)))
	mux.Handle("/fx/forwards/audit", midFX(NewFXAuditHandler(pgxPool, fxForwardAuditConfig())))
	mux.Handle("/fx/forwards/package-zip", midFX(forwards.DownloadForwardPackageZipHandler(pgxPool)))
	mux.Handle("/fx/forwards/additional-files/list", midFX(forwards.ListAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/forwards/additional-files/upload", midFX(forwards.UploadAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/forwards/additional-files/download", midFX(forwards.DownloadAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/forwards/additional-files/download-bulk", midFX(forwards.DownloadSelectedAdditionalFilesHandler(pgxPool)))
	mux.Handle("/fx/forwards/additional-files/delete", midFX(forwards.DeleteAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/forwards/additional-files/audit", midFX(forwards.AuditAdditionalFileHandler(pgxPool)))
	mux.Handle("/fx/forwards/additional-files/delete/approve", midFX(forwards.ApproveAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/fx/forwards/additional-files/delete/reject", midFX(forwards.RejectAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/fx/forwards/download-confirmations-bulk", midFX(forwards.GetForwardConfirmationBulkDownloadURL(pgxPool)))
	mux.Handle("/fx/forwards/download-bank-bulk", midFX(forwards.GetForwardBankBulkDownloadURL(pgxPool)))

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
}
