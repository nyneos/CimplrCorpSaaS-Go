package limit

import (
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterLimitRoutes registers all bank limit and utilization routes
func RegisterLimitRoutes(mux *http.ServeMux, pgxPool *pgxpool.Pool) {
	cashChain := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(
			middlewares.GlobalIndependentMiddleware(pgxPool)(
				middlewares.GlobalDependentMiddleware(pgxPool)(
					middlewares.CashMiddleware(pgxPool)(h),
				),
			),
		)
	}

	// Bank Limit Management Routes
	mux.Handle("/cash/limit/create", cashChain(CreateBankLimit(pgxPool)))
	mux.Handle("/cash/limit/bulk-create", cashChain(BulkCreateBankLimit(pgxPool)))
	mux.Handle("/cash/limit/update", cashChain(UpdateBankLimit(pgxPool)))
	mux.Handle("/cash/limit/delete", cashChain(DeleteBankLimit(pgxPool)))
	mux.Handle("/cash/limit/all", cashChain(GetAllBankLimits(pgxPool)))
	mux.Handle("/cash/limit/approved", cashChain(GetApprovedBankLimits(pgxPool)))
	mux.Handle("/cash/limit/approve", cashChain(BulkApproveBankLimits(pgxPool)))
	mux.Handle("/cash/limit/reject", cashChain(BulkRejectBankLimits(pgxPool)))
	mux.Handle("/cash/limit/audit", cashChain(GetLimitAuditHandler(pgxPool)))
	mux.Handle("/cash/limit/additional-files/list", cashChain(ListLimitAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/limit/additional-files/upload", cashChain(UploadLimitAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/limit/additional-files/download", cashChain(DownloadLimitAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/limit/additional-files/download-bulk", cashChain(DownloadSelectedLimitAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/limit/additional-files/delete", cashChain(DeleteLimitAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/limit/additional-files/audit", cashChain(AuditLimitAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/limit/additional-files/delete/approve", cashChain(ApproveLimitAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/limit/additional-files/delete/reject", cashChain(RejectLimitAdditionalFileDeleteHandler(pgxPool)))

	// Limit Utilization Routes
	mux.Handle("/cash/utilization/create", cashChain(CreateUtilization(pgxPool)))
	mux.Handle("/cash/utilization/bulk-create", cashChain(BulkCreateUtilization(pgxPool)))
	mux.Handle("/cash/utilization/update", cashChain(UpdateUtilization(pgxPool)))
	mux.Handle("/cash/utilization/delete", cashChain(DeleteUtilization(pgxPool)))
	mux.Handle("/cash/utilization/all", cashChain(GetAllUtilizations(pgxPool)))
	mux.Handle("/cash/utilization/approved", cashChain(GetApprovedUtilizations(pgxPool)))
	mux.Handle("/cash/utilization/approved-grouped", cashChain(GetApprovedUtilizationsGrouped(pgxPool)))
	mux.Handle("/cash/utilization/approve", cashChain(BulkApproveUtilizations(pgxPool)))
	mux.Handle("/cash/utilization/reject", cashChain(BulkRejectUtilizations(pgxPool)))
	mux.Handle("/cash/utilization/audit", cashChain(GetUtilizationAuditHandler(pgxPool)))
	mux.Handle("/cash/utilization/upload", cashChain(UploadUtilization(pgxPool)))
	mux.Handle("/cash/utilization/download", cashChain(DownloadUtilizationUploadFile(pgxPool)))
	mux.Handle("/cash/utilization/download-bulk", cashChain(DownloadSelectedUtilizationUploadFiles(pgxPool)))
	mux.Handle("/cash/utilization/additional-files/list", cashChain(ListUtilizationAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/utilization/additional-files/upload", cashChain(UploadUtilizationAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/utilization/additional-files/download", cashChain(DownloadUtilizationAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/utilization/additional-files/download-bulk", cashChain(DownloadSelectedUtilizationAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/utilization/additional-files/delete", cashChain(DeleteUtilizationAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/utilization/additional-files/audit", cashChain(AuditUtilizationAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/utilization/additional-files/delete/approve", cashChain(ApproveUtilizationAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/utilization/additional-files/delete/reject", cashChain(RejectUtilizationAdditionalFileDeleteHandler(pgxPool)))
}
