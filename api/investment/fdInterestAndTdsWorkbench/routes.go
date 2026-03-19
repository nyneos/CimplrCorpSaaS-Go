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
}
