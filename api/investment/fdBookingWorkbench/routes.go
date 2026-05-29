package fdBooking

import (
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterFDBookingRoutes registers all FD Booking & Confirmation routes on mux.
// Middleware chain: Session → GlobalIndependent → GlobalDependent → InvestmentFD
// This loads only what FD handlers need: entity scope, banks, currencies, holiday
// calendars, bank accounts, GL accounts, and all FD-specific master lookups.
func RegisterFDBookingRoutes(mux *http.ServeMux, pool *pgxpool.Pool) {
	mid := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(
					middlewares.InvestmentFDMiddleware(pool)(h),
				),
			),
		)
	}

	// ── Booking ──────────────────────────────────────────────────────────────
	mux.Handle("/investment/fd/booking/create",
		mid(http.HandlerFunc(CreateBookingSingle(pool))))
	mux.Handle("/investment/fd/booking/create-bulk",
		mid(http.HandlerFunc(CreateBookingBulk(pool))))
	mux.Handle("/investment/fd/booking/update",
		mid(http.HandlerFunc(UpdateBooking(pool))))
	mux.Handle("/investment/fd/booking/delete",
		mid(http.HandlerFunc(DeleteBooking(pool))))
	mux.Handle("/investment/fd/booking/approve",
		mid(http.HandlerFunc(BulkApproveBooking(pool))))
	mux.Handle("/investment/fd/booking/reject",
		mid(http.HandlerFunc(BulkRejectBooking(pool))))
	mux.Handle("/investment/fd/booking/all",
		mid(http.HandlerFunc(GetBookingsWithAudit(pool))))
	mux.Handle("/investment/fd/booking/detail",
		mid(http.HandlerFunc(GetBookingDetail(pool))))
	mux.Handle("/investment/fd/booking/audit",
		mid(http.HandlerFunc(GetBookingAuditHandler(pool))))
	mux.Handle("/investment/fd/booking/approved-active",
		mid(http.HandlerFunc(GetApprovedActiveBookings(pool))))
	mux.Handle("/investment/fd/booking/send-to-bank",
		mid(http.HandlerFunc(MarkAsSentToBank(pool))))

	// ── Confirmation ─────────────────────────────────────────────────────────
	mux.Handle("/investment/fd/confirmation/preflight",
		mid(http.HandlerFunc(GetConfirmationPreflight(pool))))
	mux.Handle("/investment/fd/confirmation/capture",
		mid(http.HandlerFunc(CaptureConfirmation(pool))))
	mux.Handle("/investment/fd/confirmation/download",
		mid(http.HandlerFunc(GetFDConfirmationDownloadURL(pool))))
	mux.Handle("/investment/fd/confirmation/download-bulk",
		mid(http.HandlerFunc(GetFDConfirmationBulkDownloadURL(pool))))
	mux.Handle("/investment/fd/confirmation/edit",
		mid(http.HandlerFunc(EditConfirmation(pool))))
	mux.Handle("/investment/fd/confirmation/resolve-variance",
		mid(http.HandlerFunc(ResolveVariance(pool))))
	mux.Handle("/investment/fd/confirmation/variance-resolve",
		mid(http.HandlerFunc(VarianceResolve(pool))))
	mux.Handle("/investment/fd/confirmation/variance-exception",
		mid(http.HandlerFunc(VarianceException(pool))))
	mux.Handle("/investment/fd/confirmation/approve",
		mid(http.HandlerFunc(BulkApproveConfirmation(pool))))
	mux.Handle("/investment/fd/confirmation/reject",
		mid(http.HandlerFunc(BulkRejectConfirmation(pool))))
	mux.Handle("/investment/fd/confirmation/delete",
		mid(http.HandlerFunc(DeleteConfirmation(pool))))
	mux.Handle("/investment/fd/confirmation/detail",
		mid(http.HandlerFunc(GetConfirmationDetail(pool))))
	mux.Handle("/investment/fd/confirmation/all",
		mid(http.HandlerFunc(GetConfirmationsWithAudit(pool))))
	mux.Handle("/investment/fd/confirmation/audit",
		mid(http.HandlerFunc(GetConfirmationAuditHandler(pool))))
	mux.Handle("/investment/fd/confirmation/confirmed",
		mid(http.HandlerFunc(GetConfirmedConfirmations(pool))))
}
