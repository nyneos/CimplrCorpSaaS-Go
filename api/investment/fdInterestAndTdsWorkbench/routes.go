package fdInterestAndTdsWorkbench

import (
	"database/sql"
	"net/http"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterFDInterestWorkbenchRoutes registers all workbench dashboard routes.
func RegisterFDInterestWorkbenchRoutes(mux *http.ServeMux, pool *pgxpool.Pool, db *sql.DB) {
	mid := api.BusinessUnitMiddleware(db)
	mux.Handle("/investment/fd/workbench/interest-summary", mid(http.HandlerFunc(GetInterestWorkbenchSummary(pool))))
	mux.Handle("/investment/fd/workbench/tds-summary", mid(http.HandlerFunc(GetTDSWorkbenchSummary(pool))))
	mux.Handle("/investment/fd/workbench/reconciliation-dashboard", mid(http.HandlerFunc(GetReconciliationDashboard(pool))))
	mux.Handle("/investment/fd/workbench/interest-vs-accrual", mid(http.HandlerFunc(GetInterestVsAccrualAnalysis(pool))))

	// TDS Register (standalone workbench ingestion — ingestion_source=TDS_WORKBENCH)
	mux.Handle("/investment/fd/tds-register/create", mid(http.HandlerFunc(CreateTDSRegister(pool))))
	mux.Handle("/investment/fd/tds-register/view", mid(http.HandlerFunc(GetTDSRegisterView(pool))))
	mux.Handle("/investment/fd/tds-register/reconcile", mid(http.HandlerFunc(ReconcileTDSRegister(pool))))
	mux.Handle("/investment/fd/tds-register/journal", mid(http.HandlerFunc(GetTDSJournalEntries(pool))))
	mux.Handle("/investment/fd/tds-register/audit", mid(http.HandlerFunc(GetTDSReceiptAuditHandler(pool))))
	mux.Handle("/investment/fd/tds-register/approve", mid(http.HandlerFunc(ApproveTDSRegister(pool))))
	mux.Handle("/investment/fd/tds-register/reject", mid(http.HandlerFunc(RejectTDSRegister(pool))))
	mux.Handle("/investment/fd/tds-register/approve-bulk", mid(http.HandlerFunc(BulkApproveTDSRegister(pool))))
	mux.Handle("/investment/fd/tds-register/reject-bulk", mid(http.HandlerFunc(BulkRejectTDSRegister(pool))))
	mux.Handle("/investment/fd/tds-register/update", mid(http.HandlerFunc(UpdateTDSRegister(pool))))
	mux.Handle("/investment/fd/tds-register/delete-bulk", mid(http.HandlerFunc(BulkDeleteTDSRegister(pool))))
}
