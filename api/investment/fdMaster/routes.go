package fdMaster

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterFDMasterRoutes(mux *http.ServeMux, pool *pgxpool.Pool, db *sql.DB) {
	mid := api.BusinessUnitMiddleware(db)

	mux.Handle("/investment/fd/master/activate", mid(http.HandlerFunc(ActivateFD(pool))))
	mux.Handle("/investment/fd/master/approve", mid(http.HandlerFunc(BulkApproveActivation(pool))))
	mux.Handle("/investment/fd/master/reject", mid(http.HandlerFunc(BulkRejectActivation(pool))))
	mux.Handle("/investment/fd/master/detail", mid(http.HandlerFunc(GetFDMasterDetail(pool))))
	mux.Handle("/investment/fd/master/all", mid(http.HandlerFunc(GetFDMasterWithAudit(pool))))
	mux.Handle("/investment/fd/master/audit", mid(http.HandlerFunc(GetFDMasterAuditHistory(pool))))
	mux.Handle("/investment/fd/master/cashflows", mid(http.HandlerFunc(GetCashflowSchedule(pool))))
	mux.Handle("/investment/fd/master/cashflow/edit", mid(http.HandlerFunc(EditCashflowLineItem(pool))))
	mux.Handle("/investment/fd/master/cashflow/approve", mid(http.HandlerFunc(ApproveCashflowEdit(pool))))
	mux.Handle("/investment/fd/master/cashflow/reject", mid(http.HandlerFunc(RejectCashflowEdit(pool))))
	mux.Handle("/investment/fd/master/cashflow/delete", mid(http.HandlerFunc(DeleteCashflowLineItem(pool))))
	mux.Handle("/investment/fd/master/cashflow/approve-delete", mid(http.HandlerFunc(ApproveDeleteCashflow(pool))))
	mux.Handle("/investment/fd/master/cashflow/bulk-approve", mid(http.HandlerFunc(BulkApproveCashflowEdit(pool))))
	mux.Handle("/investment/fd/master/cashflow/bulk-reject", mid(http.HandlerFunc(BulkRejectCashflowEdit(pool))))
	mux.Handle("/investment/fd/master/cashflow/bulk-delete", mid(http.HandlerFunc(BulkDeleteCashflow(pool))))
	mux.Handle("/investment/fd/master/cashflow/audit", mid(http.HandlerFunc(GetCashflowAuditHistory(pool))))
	mux.Handle("/investment/fd/master/cashflow/group", mid(http.HandlerFunc(GetCashflowGroupView(pool))))
	mux.Handle("/investment/fd/master/journals", mid(http.HandlerFunc(GetFDJournalEntries(pool))))

	// ── Cashflow Simulator (no persistence) ────────────────────────────────
	// POST /investment/fd/simulator/cashflow           — full what-if cashflow run
	// POST /investment/fd/simulator/cashflow/diff      — booking vs confirmation diff
	// GET|POST /investment/fd/simulator/holidays       — range list (or single-date check)
	// POST /investment/fd/simulator/maturity-date      — maturity date with holiday adjustment
	mux.Handle("/investment/fd/simulator/cashflow", mid(http.HandlerFunc(SimulateCashflowHandler(pool))))
	mux.Handle("/investment/fd/simulator/cashflow/diff", mid(http.HandlerFunc(SimulateDiffHandler(pool))))
	mux.Handle("/investment/fd/simulator/holidays", mid(http.HandlerFunc(GetHolidayListHandler(pool))))
	mux.Handle("/investment/fd/simulator/maturity-date", mid(http.HandlerFunc(MaturityDateHandler(pool))))
}
