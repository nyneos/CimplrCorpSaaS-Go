package evidencePack

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Middleware is the wrapper function passed down from fdMonthEndClosing.go's
// bootstrap (Session → GlobalIndependent → GlobalDependent → InvestmentFD).
// RegisterEvidencePackRoutes never builds middleware itself — that
// responsibility lives entirely in the bootstrap file, per CLAUDE.md's
// routes.go rule ("ONLY mux.Handle(...) calls, nothing else"). Mirrors
// cycle/routes.go's Middleware type exactly (kept as its own copy since
// cycle's is unexported).
type Middleware func(http.Handler) http.Handler

// RegisterEvidencePackRoutes registers every
// /investment/fd-closing/evidence/* route on mux. fd_closing_evidence_pack is
// an append-only generated-artifact log (Section 6 of the handler spec) —
// there is no maker-checker on the pack record itself, so unlike cycle/
// scope/lock/reopen there are no approve/reject/bulk-* routes here. The
// files/* routes delegate to the shared generic additional-files engine
// (api/cash/additionalfiles), same as every other module's file sub-flow.
func RegisterEvidencePackRoutes(mux *http.ServeMux, pool *pgxpool.Pool, mid Middleware) {
	mux.Handle("/investment/fd-closing/evidence/generate",
		mid(http.HandlerFunc(GenerateEvidencePack(pool))))
	mux.Handle("/investment/fd-closing/evidence/list",
		mid(http.HandlerFunc(ListEvidencePacks(pool))))
	mux.Handle("/investment/fd-closing/evidence/download",
		mid(http.HandlerFunc(DownloadEvidencePack(pool))))

	mux.Handle("/investment/fd-closing/evidence/files/upload",
		mid(http.HandlerFunc(UploadEvidencePackFilesHandler(pool))))
	mux.Handle("/investment/fd-closing/evidence/files/delete",
		mid(http.HandlerFunc(DeleteEvidencePackFileHandler(pool))))
	mux.Handle("/investment/fd-closing/evidence/files/approve-delete",
		mid(http.HandlerFunc(ApproveDeleteEvidencePackFileHandler(pool))))
	mux.Handle("/investment/fd-closing/evidence/files/reject-delete",
		mid(http.HandlerFunc(RejectDeleteEvidencePackFileHandler(pool))))
	mux.Handle("/investment/fd-closing/evidence/files/download",
		mid(http.HandlerFunc(DownloadEvidencePackFileHandler(pool))))
	mux.Handle("/investment/fd-closing/evidence/files/list",
		mid(http.HandlerFunc(ListEvidencePackFilesHandler(pool))))
	mux.Handle("/investment/fd-closing/evidence/files/download-bulk",
		mid(http.HandlerFunc(DownloadBulkEvidencePackFilesHandler(pool))))
	mux.Handle("/investment/fd-closing/evidence/files/audit",
		mid(http.HandlerFunc(AuditEvidencePackFilesHandler(pool))))
}
