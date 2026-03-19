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
	mux.Handle("/investment/fd/master/journals", mid(http.HandlerFunc(GetFDJournalEntries(pool))))
}
