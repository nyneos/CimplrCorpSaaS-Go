package fdReceipt

import (
	"database/sql"
	"net/http"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterFDReceiptRoutes registers all FD Interest Receipt & TDS routes.
func RegisterFDReceiptRoutes(mux *http.ServeMux, pool *pgxpool.Pool, db *sql.DB) {
	mid := api.BusinessUnitMiddleware(db)

	mux.Handle("/investment/fd/receipt/create",
		mid(http.HandlerFunc(CreateReceipt(pool))))
	mux.Handle("/investment/fd/receipt/update",
		mid(http.HandlerFunc(UpdateReceipt(pool))))
	mux.Handle("/investment/fd/receipt/delete",
		mid(http.HandlerFunc(DeleteReceipt(pool))))
	mux.Handle("/investment/fd/receipt/submit",
		mid(http.HandlerFunc(SubmitReceiptForApproval(pool))))
	mux.Handle("/investment/fd/receipt/bulk-approve",
		mid(http.HandlerFunc(BulkApproveReceipt(pool))))
	mux.Handle("/investment/fd/receipt/bulk-reject",
		mid(http.HandlerFunc(BulkRejectReceipt(pool))))
	mux.Handle("/investment/fd/receipt/all",
		mid(http.HandlerFunc(GetReceiptsWithAudit(pool))))
	mux.Handle("/investment/fd/receipt/detail",
		mid(http.HandlerFunc(GetReceiptDetail(pool))))
	mux.Handle("/investment/fd/receipt/audit-history",
		mid(http.HandlerFunc(GetReceiptAuditHistory(pool))))
	mux.Handle("/investment/fd/receipt/tds-register",
		mid(http.HandlerFunc(GetTDSRegister(pool))))
	mux.Handle("/investment/fd/receipt/reconcile/run",
		mid(http.HandlerFunc(RunReconciliation(pool))))
	mux.Handle("/investment/fd/receipt/reconcile/status",
		mid(http.HandlerFunc(GetReconcileRunStatus(pool))))
	mux.Handle("/investment/fd/receipt/reconcile/results",
		mid(http.HandlerFunc(GetReconcileResults(pool))))
	mux.Handle("/investment/fd/receipt/exceptions",
		mid(http.HandlerFunc(GetExceptions(pool))))
	mux.Handle("/investment/fd/receipt/exceptions/resolve",
		mid(http.HandlerFunc(ResolveException(pool))))
	mux.Handle("/investment/fd/receipt/exceptions/approve",
		mid(http.HandlerFunc(ApproveException(pool))))
	mux.Handle("/investment/fd/receipt/exceptions/close",
		mid(http.HandlerFunc(CloseException(pool))))
	mux.Handle("/investment/fd/receipt/post-journals",
		mid(http.HandlerFunc(PostReceiptJournals(pool))))

	// TDS endpoints
	mux.Handle("/investment/fd/receipt/tds/update",
		mid(http.HandlerFunc(UpdateTDS(pool))))
	mux.Handle("/investment/fd/receipt/tds/detail",
		mid(http.HandlerFunc(GetTDSDetail(pool))))
	mux.Handle("/investment/fd/receipt/tds/audit-history",
		mid(http.HandlerFunc(GetTDSAuditHistory(pool))))
}
