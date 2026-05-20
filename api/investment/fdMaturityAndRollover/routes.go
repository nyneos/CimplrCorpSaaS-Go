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
	mux.Handle("/investment/fd/closure/download",
		mid(http.HandlerFunc(GetFDClosureDownloadURL(pool))))
	mux.Handle("/investment/fd/closure/download-bulk",
		mid(http.HandlerFunc(GetFDClosureBulkDownloadURL(pool))))
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

	// Maturity Summary — pivot-table view of all FDs
	mux.Handle("/investment/fd/maturity/summary",
		mid(http.HandlerFunc(GetFDMaturitySummary(pool))))

	mux.Handle("/investment/fd/closure/audit",
		mid(http.HandlerFunc(GetClosureAuditHandler(pool))))

	// New cimplr-schema Maturity & Rollover workflow.
	mux.Handle("/investment/fd/closure/initiate/validate",
		mid(http.HandlerFunc(CimplrInitiateValidate(pool))))
	mux.Handle("/investment/fd/closure/initiate/create",
		mid(http.HandlerFunc(CimplrInitiateCreate(pool))))
	mux.Handle("/investment/fd/closure/initiate/edit",
		mid(http.HandlerFunc(CimplrInitiateEdit(pool))))
	mux.Handle("/investment/fd/closure/initiate/detail",
		mid(http.HandlerFunc(CimplrInitiateDetail(pool))))
	mux.Handle("/investment/fd/closure/initiate/upload",
		mid(http.HandlerFunc(CimplrClosureUpload(pool))))
	mux.Handle("/investment/fd/closure/initiate/download",
		mid(http.HandlerFunc(CimplrClosureDownload(pool))))
	mux.Handle("/investment/fd/closure/initiate/delete",
		mid(http.HandlerFunc(CimplrInitiateDelete(pool))))
	mux.Handle("/investment/fd/closure/initiate/approve",
		mid(http.HandlerFunc(CimplrInitiateApprove(pool))))
	mux.Handle("/investment/fd/closure/initiate/reject",
		mid(http.HandlerFunc(CimplrInitiateReject(pool))))
	mux.Handle("/investment/fd/closure/initiate/all",
		mid(http.HandlerFunc(CimplrInitiateAll(pool))))
	mux.Handle("/investment/fd/closure/initiate/approved-active",
		mid(http.HandlerFunc(CimplrInitiateApprovedActive(pool))))

	mux.Handle("/investment/fd/closure/confirm/validate",
		mid(http.HandlerFunc(CimplrConfirmValidate(pool))))
	mux.Handle("/investment/fd/closure/confirm/create",
		mid(http.HandlerFunc(CimplrConfirmCreate(pool))))
	mux.Handle("/investment/fd/closure/confirm/edit",
		mid(http.HandlerFunc(CimplrConfirmEdit(pool))))
	mux.Handle("/investment/fd/closure/confirm/detail",
		mid(http.HandlerFunc(CimplrConfirmDetail(pool))))
	mux.Handle("/investment/fd/closure/confirm/upload",
		mid(http.HandlerFunc(CimplrClosureUpload(pool))))
	mux.Handle("/investment/fd/closure/confirm/download",
		mid(http.HandlerFunc(CimplrClosureDownload(pool))))
	mux.Handle("/investment/fd/closure/confirm/delete",
		mid(http.HandlerFunc(CimplrConfirmDelete(pool))))
	mux.Handle("/investment/fd/closure/confirm/approve",
		mid(http.HandlerFunc(CimplrConfirmApprove(pool))))
	mux.Handle("/investment/fd/closure/confirm/reject",
		mid(http.HandlerFunc(CimplrConfirmReject(pool))))
	mux.Handle("/investment/fd/closure/confirm/all",
		mid(http.HandlerFunc(CimplrConfirmAll(pool))))
	mux.Handle("/investment/fd/closure/confirm/approved-active",
		mid(http.HandlerFunc(CimplrConfirmApprovedActive(pool))))
}
