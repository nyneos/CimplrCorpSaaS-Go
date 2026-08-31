// Package reopen implements the fd_closing_reopen_request handlers —
// Section 5 of database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md.
// Same stage-then-apply shape as lock (the request row IS its own audit
// trail, no separate *_audit sibling table), plus supporting-evidence file
// attachments delegated to the shared generic upload engine
// (api/cash/additionalfiles) and a Relock convenience action.
package reopen

import (
	"net/http"

	cashfiles "CimplrCorpSaas/api/cash/additionalfiles"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Middleware is the wrapper function passed down from fdMonthEndClosing.go's
// bootstrap (Session → GlobalIndependent → GlobalDependent → InvestmentFD).
// RegisterReopenRoutes never builds middleware itself — that responsibility
// lives entirely in the bootstrap file, per CLAUDE.md's routes.go rule ("ONLY
// mux.Handle(...) calls, nothing else").
type Middleware func(http.Handler) http.Handler

// RegisterReopenRoutes registers every /investment/fd-closing/reopen/* route
// on mux. All POST — every parameter travels in the JSON body (multipart
// form for file upload), per this repo's convention.
func RegisterReopenRoutes(mux *http.ServeMux, pool *pgxpool.Pool, mid Middleware) {
	mux.Handle("/investment/fd-closing/reopen/request",
		mid(http.HandlerFunc(RequestReopen(pool))))
	mux.Handle("/investment/fd-closing/reopen/approve",
		mid(http.HandlerFunc(ApproveReopen(pool))))
	mux.Handle("/investment/fd-closing/reopen/bulk-approve",
		mid(http.HandlerFunc(ApproveReopen(pool))))
	mux.Handle("/investment/fd-closing/reopen/reject",
		mid(http.HandlerFunc(RejectReopen(pool))))
	mux.Handle("/investment/fd-closing/reopen/bulk-reject",
		mid(http.HandlerFunc(RejectReopen(pool))))
	mux.Handle("/investment/fd-closing/reopen/apply",
		mid(http.HandlerFunc(ApplyReopen(pool))))
	mux.Handle("/investment/fd-closing/reopen/relock",
		mid(http.HandlerFunc(RelockCycle(pool))))
	mux.Handle("/investment/fd-closing/reopen/list",
		mid(http.HandlerFunc(ListReopenRequests(pool))))
	mux.Handle("/investment/fd-closing/reopen/detail",
		mid(http.HandlerFunc(DetailReopenRequest(pool))))

	// Supporting-evidence file attachments — thin delegation to the shared
	// generic upload engine, per the handler spec's Section 5 (identical shape
	// to Section 3's checklist item files), including list/download-bulk/audit
	// to match every other additional-files module.
	filesCfg := reopenFilesConfig()
	mux.Handle("/investment/fd-closing/reopen/files/upload",
		mid(http.HandlerFunc(cashfiles.NewUploadHandler(pool, filesCfg))))
	mux.Handle("/investment/fd-closing/reopen/files/delete",
		mid(http.HandlerFunc(cashfiles.NewDeleteHandler(pool, filesCfg))))
	mux.Handle("/investment/fd-closing/reopen/files/approve-delete",
		mid(http.HandlerFunc(cashfiles.NewApproveDeleteHandler(pool, filesCfg))))
	mux.Handle("/investment/fd-closing/reopen/files/reject-delete",
		mid(http.HandlerFunc(cashfiles.NewRejectDeleteHandler(pool, filesCfg))))
	mux.Handle("/investment/fd-closing/reopen/files/download",
		mid(http.HandlerFunc(cashfiles.NewDownloadHandler(pool, filesCfg))))
	mux.Handle("/investment/fd-closing/reopen/files/list",
		mid(http.HandlerFunc(cashfiles.NewListHandler(pool, filesCfg))))
	mux.Handle("/investment/fd-closing/reopen/files/download-bulk",
		mid(http.HandlerFunc(cashfiles.NewDownloadSelectedHandler(pool, filesCfg))))
	mux.Handle("/investment/fd-closing/reopen/files/audit",
		mid(http.HandlerFunc(cashfiles.NewAuditHandler(pool, filesCfg))))
}
