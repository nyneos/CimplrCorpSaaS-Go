package fdReceipt

import (
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterFDReceiptRoutes registers all FD Interest Receipt & TDS routes.
// Middleware chain: Session → GlobalIndependent → GlobalDependent → InvestmentFD
func RegisterFDReceiptRoutes(mux *http.ServeMux, pool *pgxpool.Pool) {
	mid := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(
					middlewares.InvestmentFDMiddleware(pool)(h),
				),
			),
		)
	}

	// ── Interest ingestion ────────────────────────────────────────────────────
	mux.Handle("/investment/fd/receipt/create",
		mid(http.HandlerFunc(CreateReceipt(pool))))
	mux.Handle("/investment/fd/receipt/update",
		mid(http.HandlerFunc(UpdateReceipt(pool))))
	// Legacy submit endpoint — no-op in new schema, kept for compatibility
	mux.Handle("/investment/fd/receipt/submit",
		mid(http.HandlerFunc(SubmitReceiptForApproval(pool))))
	mux.Handle("/investment/fd/receipt/approve-bulk",
		mid(http.HandlerFunc(BulkApproveReceipt(pool))))
	mux.Handle("/investment/fd/receipt/reject-bulk",
		mid(http.HandlerFunc(BulkRejectReceipt(pool))))
	mux.Handle("/investment/fd/receipt/delete-bulk",
		mid(http.HandlerFunc(DeleteReceipt(pool))))
	mux.Handle("/investment/fd/receipt/all",
		mid(http.HandlerFunc(GetReceiptsWithAudit(pool))))
	// Approved-only list views
	mux.Handle("/investment/fd/receipt/approved-active",
		mid(http.HandlerFunc(GetApprovedActiveReceipts(pool))))
	mux.Handle("/investment/fd/receipt/tds/all",
		mid(http.HandlerFunc(GetTDSReceiptsAll(pool))))
	mux.Handle("/investment/fd/receipt/tds/approved-active",
		mid(http.HandlerFunc(GetApprovedActiveTDS(pool))))
	mux.Handle("/investment/fd/receipt/detail",
		mid(http.HandlerFunc(GetReceiptDetail(pool))))
	mux.Handle("/investment/fd/receipt/audit",
		mid(http.HandlerFunc(GetReceiptAuditHistory(pool))))
	mux.Handle("/investment/fd/receipt/post-journals",
		mid(http.HandlerFunc(PostReceiptJournals(pool))))

	// ── TDS endpoints ─────────────────────────────────────────────────────────
	mux.Handle("/investment/fd/receipt/tds/update",
		mid(http.HandlerFunc(UpdateTDS(pool))))
	mux.Handle("/investment/fd/receipt/tds/detail",
		mid(http.HandlerFunc(GetTDSDetail(pool))))
	mux.Handle("/investment/fd/receipt/tds/audit",
		mid(http.HandlerFunc(GetTDSAuditHistory(pool))))
	mux.Handle("/investment/fd/receipt/tds-register",
		mid(http.HandlerFunc(GetTDSRegister(pool))))

	// ── Reconciliation ────────────────────────────────────────────────────────
	// /run   → dry-run preview (no DB writes, returns projected outcome inline)
	// /ingest → actual commit (fires goroutine, poll /status for completion)
	// /status → metadata + counters for a specific run_id
	// /results → result rows for a specific run_id
	mux.Handle("/investment/fd/reconcile/run",
		mid(http.HandlerFunc(RunReconciliation(pool))))
	mux.Handle("/investment/fd/reconcile/ingest",
		mid(http.HandlerFunc(IngestReconciliation(pool))))
	mux.Handle("/investment/fd/reconcile/status",
		mid(http.HandlerFunc(GetReconcileRunStatus(pool))))
	mux.Handle("/investment/fd/reconcile/results",
		mid(http.HandlerFunc(GetReconcileResults(pool))))
	// /detail → single run: full metadata + enriched result rows (receipt_type, audit, period, exceptions)
	mux.Handle("/investment/fd/reconcile/detail",
		mid(http.HandlerFunc(GetReconcileDetail(pool))))
	// /candidates → feeder API: APPROVED receipts+TDS not yet in any reconcile result (safe to run)
	mux.Handle("/investment/fd/reconcile/candidates",
		mid(http.HandlerFunc(GetReconcileCandidates(pool))))

	// ── Exceptions ────────────────────────────────────────────────────────────
	mux.Handle("/investment/fd/exception/all",
		mid(http.HandlerFunc(GetExceptions(pool))))
	mux.Handle("/investment/fd/exception/detail",
		mid(http.HandlerFunc(GetExceptionDetail(pool))))
	mux.Handle("/investment/fd/exception/edit",
		mid(http.HandlerFunc(EditVariance(pool))))
	mux.Handle("/investment/fd/exception/resolve",
		mid(http.HandlerFunc(ResolveException(pool))))
	mux.Handle("/investment/fd/exception/approve",
		mid(http.HandlerFunc(ApproveException(pool))))
	mux.Handle("/investment/fd/exception/close",
		mid(http.HandlerFunc(CloseException(pool))))
	mux.Handle("/investment/fd/exception/reject",
		mid(http.HandlerFunc(RejectException(pool))))
}
