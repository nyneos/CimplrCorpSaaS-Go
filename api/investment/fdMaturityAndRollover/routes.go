package fdMaturityAndRollover

import (
	"CimplrCorpSaas/api"
	"database/sql"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterFDMaturityRoutes wires all FD Closure / Maturity lifecycle endpoints into the given mux.
func RegisterFDMaturityRoutes(mux *http.ServeMux, pool *pgxpool.Pool, db *sql.DB) {
	mid := api.BusinessUnitMiddleware(db)

	mux.Handle("/investment/fd/closure/maturity-dashboard",
		mid(http.HandlerFunc(GetFDsNearMaturity(pool))))
	mux.Handle("/investment/fd/closure/validate",
		mid(http.HandlerFunc(ValidateClosure(pool))))
	mux.Handle("/investment/fd/closure/initiate",
		mid(http.HandlerFunc(InitiateClosure(pool))))
	mux.Handle("/investment/fd/closure/update",
		mid(http.HandlerFunc(UpdateClosure(pool))))
	mux.Handle("/investment/fd/closure/all",
		mid(http.HandlerFunc(GetAllClosureRequests(pool))))
	mux.Handle("/investment/fd/closure/detail",
		mid(http.HandlerFunc(GetClosureDetail(pool))))
	mux.Handle("/investment/fd/closure/bulk-approve",
		mid(http.HandlerFunc(BulkApproveClosureRequest(pool))))
	mux.Handle("/investment/fd/closure/bulk-reject",
		mid(http.HandlerFunc(BulkRejectClosureRequest(pool))))
	mux.Handle("/investment/fd/closure/delete",
		mid(http.HandlerFunc(DeleteClosureRequest(pool))))
	mux.Handle("/investment/fd/closure/approval-list",
		mid(http.HandlerFunc(GetClosureApprovalList(pool))))
}
