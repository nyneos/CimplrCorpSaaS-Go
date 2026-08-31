package checklist

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Middleware mirrors cycle.Middleware exactly — the wrapper chain is built
// once in fdMonthEndClosing.go's bootstrap (Session → GlobalIndependent →
// GlobalDependent → InvestmentFD) and passed down here.
// RegisterChecklistRoutes never builds middleware itself, per CLAUDE.md's
// routes.go rule ("ONLY mux.Handle(...) calls, nothing else").
type Middleware func(http.Handler) http.Handler

// RegisterChecklistRoutes registers every /investment/fd-closing/checklist/*
// route on mux — Section 3 of
// database/2026-08-27/HANDLER_SPEC_fd_month_quarter_end_closing.md. Status
// updates are apply-immediately/self-approved (no approval instance at all —
// see statusUpdate.go); file sub-routes delegate entirely to the shared
// generic upload/delete-approval engine in api/cash/additionalfiles,
// configured in files.go for module_key='fd-closing-checklist-additional'.
func RegisterChecklistRoutes(mux *http.ServeMux, pool *pgxpool.Pool, mid Middleware) {
	mux.Handle("/investment/fd-closing/checklist/update",
		mid(http.HandlerFunc(UpdateChecklistItem(pool))))
	mux.Handle("/investment/fd-closing/checklist/list",
		mid(http.HandlerFunc(ListChecklistItems(pool))))
	mux.Handle("/investment/fd-closing/checklist/detail",
		mid(http.HandlerFunc(DetailChecklistItem(pool))))
	mux.Handle("/investment/fd-closing/checklist/audit",
		mid(http.HandlerFunc(AuditChecklistItem(pool))))

	mux.Handle("/investment/fd-closing/checklist/files/upload",
		mid(UploadChecklistFilesHandler(pool)))
	mux.Handle("/investment/fd-closing/checklist/files/delete",
		mid(DeleteChecklistFileHandler(pool)))
	mux.Handle("/investment/fd-closing/checklist/files/approve-delete",
		mid(ApproveDeleteChecklistFileHandler(pool)))
	mux.Handle("/investment/fd-closing/checklist/files/reject-delete",
		mid(RejectDeleteChecklistFileHandler(pool)))
	mux.Handle("/investment/fd-closing/checklist/files/download",
		mid(DownloadChecklistFileHandler(pool)))
	mux.Handle("/investment/fd-closing/checklist/files/list",
		mid(ListChecklistFilesHandler(pool)))
	mux.Handle("/investment/fd-closing/checklist/files/download-bulk",
		mid(DownloadBulkChecklistFilesHandler(pool)))
	mux.Handle("/investment/fd-closing/checklist/files/audit",
		mid(AuditChecklistFilesHandler(pool)))
}
