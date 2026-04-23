package limit

import (
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterLimitRoutes registers all bank limit and utilization routes
func RegisterLimitRoutes(mux *http.ServeMux, pgxPool *pgxpool.Pool) {
	// Bank Limit Management Routes
	mux.Handle("/cash/limit/create", middlewares.PreValidationMiddleware(pgxPool)(CreateBankLimit(pgxPool)))
	mux.Handle("/cash/limit/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(BulkCreateBankLimit(pgxPool)))
	mux.Handle("/cash/limit/update", middlewares.PreValidationMiddleware(pgxPool)(UpdateBankLimit(pgxPool)))
	mux.Handle("/cash/limit/delete", middlewares.PreValidationMiddleware(pgxPool)(DeleteBankLimit(pgxPool)))
	mux.Handle("/cash/limit/all", middlewares.PreValidationMiddleware(pgxPool)(GetAllBankLimits(pgxPool)))
	mux.Handle("/cash/limit/approved", middlewares.PreValidationMiddleware(pgxPool)(GetApprovedBankLimits(pgxPool)))
	mux.Handle("/cash/limit/approve", middlewares.PreValidationMiddleware(pgxPool)(BulkApproveBankLimits(pgxPool)))
	mux.Handle("/cash/limit/reject", middlewares.PreValidationMiddleware(pgxPool)(BulkRejectBankLimits(pgxPool)))

	// Limit Utilization Routes
	mux.Handle("/cash/utilization/create", middlewares.PreValidationMiddleware(pgxPool)(CreateUtilization(pgxPool)))
	mux.Handle("/cash/utilization/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(BulkCreateUtilization(pgxPool)))
	mux.Handle("/cash/utilization/update", middlewares.PreValidationMiddleware(pgxPool)(UpdateUtilization(pgxPool)))
	mux.Handle("/cash/utilization/delete", middlewares.PreValidationMiddleware(pgxPool)(DeleteUtilization(pgxPool)))
	mux.Handle("/cash/utilization/all", middlewares.PreValidationMiddleware(pgxPool)(GetAllUtilizations(pgxPool)))
	mux.Handle("/cash/utilization/approved", middlewares.PreValidationMiddleware(pgxPool)(GetApprovedUtilizations(pgxPool)))
	mux.Handle("/cash/utilization/approved-grouped", middlewares.PreValidationMiddleware(pgxPool)(GetApprovedUtilizationsGrouped(pgxPool)))
	mux.Handle("/cash/utilization/approve", middlewares.PreValidationMiddleware(pgxPool)(BulkApproveUtilizations(pgxPool)))
	mux.Handle("/cash/utilization/reject", middlewares.PreValidationMiddleware(pgxPool)(BulkRejectUtilizations(pgxPool)))
	mux.Handle("/cash/utilization/upload", middlewares.PreValidationMiddleware(pgxPool)(UploadUtilization(pgxPool)))
}
