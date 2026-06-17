package investment

import (
	"database/sql"
	"net/http"

	"CimplrCorpSaas/api"
	accountingworkbench "CimplrCorpSaas/api/investment/accountingWorkbench"
	investmentfiles "CimplrCorpSaas/api/investment/additionalfiles"
	amfisync "CimplrCorpSaas/api/investment/amfi-sync"
	fdAccrual "CimplrCorpSaas/api/investment/fdAccrual"
	fdBooking "CimplrCorpSaas/api/investment/fdBookingWorkbench"
	fdInterestWorkbench "CimplrCorpSaas/api/investment/fdInterestAndTdsWorkbench"
	fdMaster "CimplrCorpSaas/api/investment/fdMaster"
	fdMaturityAndRollover "CimplrCorpSaas/api/investment/fdMaturityAndRollover"
	fdReceipt "CimplrCorpSaas/api/investment/fdReceipt"
	investmentsuite "CimplrCorpSaas/api/investment/investment-suite"
	onboard "CimplrCorpSaas/api/investment/onboarding"
	portfolio "CimplrCorpSaas/api/investment/portfolio"
	redemption "CimplrCorpSaas/api/investment/redemption"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/observability"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func NewInvestmentServer(pool *pgxpool.Pool, db *sql.DB, port string) *http.Server {
	const serviceName = "investment"
	mux := http.NewServeMux()

	mux.HandleFunc("/investment/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Investment Service is active"))
	})
	mux.Handle("/investment/metrics", observability.MetricsHandler(serviceName))

	mfMid := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(
					middlewares.InvestmentMFMiddleware(pool)(h),
				),
			),
		)
	}

	fdMid := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pool)(
			middlewares.GlobalIndependentMiddleware(pool)(
				middlewares.GlobalDependentMiddleware(pool)(
					middlewares.InvestmentFDMiddleware(pool)(h),
				),
			),
		)
	}

	// // Onboarding workbench (protected by BusinessUnitMiddleware)
	// mux.Handle("/investment/onboard/workbench", mfMid(http.HandlerFunc(onboard.OnboardPortfolioWorkbench(pool))))

	// Onboarding utility endpoints (AMFI/schemes/folios/demat)
	mux.Handle("/investment/onboard/amc-enriched", mfMid(http.HandlerFunc(onboard.GetAMFISchemeAMCEnriched(pool))))
	mux.Handle("/investment/onboard/schemes-enriched", mfMid(http.HandlerFunc(onboard.GetAMFISchemesByMultipleAMCs(pool))))
	mux.Handle("/investment/onboard/folios-enriched", mfMid(http.HandlerFunc(onboard.GetFoliosBySchemeListSimple(pool))))
	mux.Handle("/investment/onboard/folios-grouped", mfMid(http.HandlerFunc(onboard.GetFoliosBySchemeListGrouped(pool))))
	mux.Handle("/investment/onboard/entity-folios", mfMid(http.HandlerFunc(onboard.GetFoliosByEntity(pool))))
	mux.Handle("/investment/onboard/demat-enriched", mfMid(http.HandlerFunc(onboard.GetDematWithDPInfo(pool))))
	mux.Handle("/investment/onboard/dps-enriched", mfMid(http.HandlerFunc(onboard.GetAllDPs(pool))))
	mux.Handle("/investment/onboard/upload", mfMid(http.HandlerFunc(onboard.UploadInvestmentBulkk(pool))))
	mux.Handle("/investment/onboard/download", mfMid(http.HandlerFunc(onboard.GetOnboardDownloadURL(pool))))
	mux.Handle("/investment/onboard/download-bulk", mfMid(http.HandlerFunc(onboard.GetOnboardBulkDownloadURL(pool))))
	mux.Handle("/investment/onboard/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadOnboardPackageZipHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/list", mfMid(http.HandlerFunc(investmentfiles.ListOnboardAdditionalFilesHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/upload", mfMid(http.HandlerFunc(investmentfiles.UploadOnboardAdditionalFilesHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/download", mfMid(http.HandlerFunc(investmentfiles.DownloadOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/download-bulk", mfMid(http.HandlerFunc(investmentfiles.DownloadSelectedOnboardAdditionalFilesHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/delete", mfMid(http.HandlerFunc(investmentfiles.DeleteOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/audit", mfMid(http.HandlerFunc(investmentfiles.AuditOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/delete/approve", mfMid(http.HandlerFunc(investmentfiles.ApproveDeleteOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/delete/reject", mfMid(http.HandlerFunc(investmentfiles.RejectDeleteOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/kpi", mfMid(http.HandlerFunc(onboard.PostPortfolioSnapshot(pool))))
	mux.Handle("/investment/onboard/snapshot/refresh", mfMid(http.HandlerFunc(onboard.RefreshPortfolioSnapshot(pool))))

	mux.Handle("/investment/onboard/batch/approve", mfMid(http.HandlerFunc(onboard.BulkApproveBatch(pool))))
	mux.Handle("/investment/onboard/batch/info", mfMid(http.HandlerFunc(onboard.GetBatchInfo(pool))))
	mux.Handle("/investment/onboard/batch/delete", mfMid(http.HandlerFunc(onboard.DeleteOnboardBatch(pool))))
	mux.Handle("/investment/onboard/batch", mfMid(http.HandlerFunc(onboard.GetAllBatches(pool))))
	mux.Handle("/investment/onboard/audit-history", mfMid(http.HandlerFunc(onboard.GetOnboardingAuditHistory(pool))))

	// Investment suite manual actions
	mux.Handle("/investment/proposals/create", mfMid(http.HandlerFunc(investmentsuite.CreateInvestmentProposal(pool))))
	mux.Handle("/investment/proposals/update", mfMid(http.HandlerFunc(investmentsuite.UpdateInvestmentProposal(pool))))
	mux.Handle("/investment/proposals/approve", mfMid(http.HandlerFunc(investmentsuite.BulkApproveProposals(pool))))
	mux.Handle("/investment/proposals/reject", mfMid(http.HandlerFunc(investmentsuite.BulkRejectProposals(pool))))
	mux.Handle("/investment/proposals/delete", mfMid(http.HandlerFunc(investmentsuite.BulkDeleteProposals(pool))))
	mux.Handle("/investment/proposals/meta", mfMid(http.HandlerFunc(investmentsuite.GetProposalMeta(pool))))
	mux.Handle("/investment/proposals/approved-active", mfMid(http.HandlerFunc(investmentsuite.GetApprovedProposalMeta(pool))))
	mux.Handle("/investment/proposals/detail", mfMid(http.HandlerFunc(investmentsuite.GetProposalDetail(pool))))
	mux.Handle("/investment/proposals/entity-holdings", mfMid(http.HandlerFunc(investmentsuite.GetEntitySchemeHoldings(pool))))
	mux.Handle("/investment/proposals/entity-accounts", mfMid(http.HandlerFunc(investmentsuite.GetEntityAccounts(pool))))
	mux.Handle("/investment/proposals/audit-history", mfMid(http.HandlerFunc(investmentsuite.GetProposalAuditHistory(pool))))
	mux.Handle("/investment/proposals/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadProposalPackageZipHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/list", mfMid(http.HandlerFunc(investmentfiles.ListProposalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/upload", mfMid(http.HandlerFunc(investmentfiles.UploadProposalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/download", mfMid(http.HandlerFunc(investmentfiles.DownloadProposalAdditionalFileHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/download-bulk", mfMid(http.HandlerFunc(investmentfiles.DownloadSelectedProposalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/delete", mfMid(http.HandlerFunc(investmentfiles.DeleteProposalAdditionalFileHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/audit", mfMid(http.HandlerFunc(investmentfiles.AuditProposalAdditionalFileHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/delete/approve", mfMid(http.HandlerFunc(investmentfiles.ApproveDeleteProposalAdditionalFileHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/delete/reject", mfMid(http.HandlerFunc(investmentfiles.RejectDeleteProposalAdditionalFileHandler(pool))))

	// Investment initiation endpoints
	// mux.Handle("/investment/initiation/upload", mfMid(http.HandlerFunc(investmentsuite.UploadInitiationSimple(pool))))
	mux.Handle("/investment/initiation/create", mfMid(http.HandlerFunc(investmentsuite.CreateInitiationSingle(pool))))
	mux.Handle("/investment/initiation/create-bulk", mfMid(http.HandlerFunc(investmentsuite.CreateInitiationBulk(pool))))
	mux.Handle("/investment/initiation/update", mfMid(http.HandlerFunc(investmentsuite.UpdateInitiation(pool))))
	mux.Handle("/investment/initiation/update-bulk", mfMid(http.HandlerFunc(investmentsuite.UpdateInitiationBulk(pool))))
	mux.Handle("/investment/initiation/delete", mfMid(http.HandlerFunc(investmentsuite.DeleteInitiation(pool))))
	mux.Handle("/investment/initiation/approve", mfMid(http.HandlerFunc(investmentsuite.BulkApproveInitiationActions(pool))))
	mux.Handle("/investment/initiation/reject", mfMid(http.HandlerFunc(investmentsuite.BulkRejectInitiationActions(pool))))
	mux.Handle("/investment/initiation/approved-active", mfMid(http.HandlerFunc(investmentsuite.GetApprovedActiveInitiations(pool))))
	mux.Handle("/investment/initiation/all", mfMid(http.HandlerFunc(investmentsuite.GetInitiationsWithAudit(pool))))
	mux.Handle("/investment/initiation/detail", mfMid(http.HandlerFunc(investmentsuite.GetInitiationDetail(pool))))
	mux.Handle("/investment/initiation/audit-history", mfMid(http.HandlerFunc(investmentsuite.GetInitiationAuditHistory(pool))))
	mux.Handle("/investment/initiation/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadInitiationPackageZipHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/list", mfMid(http.HandlerFunc(investmentfiles.ListInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/upload", mfMid(http.HandlerFunc(investmentfiles.UploadInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/download", mfMid(http.HandlerFunc(investmentfiles.DownloadInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/download-bulk", mfMid(http.HandlerFunc(investmentfiles.DownloadSelectedInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/delete", mfMid(http.HandlerFunc(investmentfiles.DeleteInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/audit", mfMid(http.HandlerFunc(investmentfiles.AuditInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/delete/approve", mfMid(http.HandlerFunc(investmentfiles.ApproveDeleteInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/delete/reject", mfMid(http.HandlerFunc(investmentfiles.RejectDeleteInitiationAdditionalFileHandler(pool))))

	// Investment confirmation endpoints
	mux.Handle("/investment/confirmation/create", mfMid(http.HandlerFunc(investmentsuite.CreateConfirmationSingle(pool))))
	mux.Handle("/investment/confirmation/download", mfMid(http.HandlerFunc(investmentsuite.GetConfirmationDownloadURL(pool))))
	mux.Handle("/investment/confirmation/download-bulk", mfMid(http.HandlerFunc(investmentsuite.GetConfirmationBulkDownloadURL(pool))))
	mux.Handle("/investment/confirmation/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadConfirmationPackageZipHandler(pool))))
	mux.Handle("/investment/confirmation/create-bulk", mfMid(http.HandlerFunc(investmentsuite.CreateConfirmationBulk(pool))))
	mux.Handle("/investment/confirmation/update", mfMid(http.HandlerFunc(investmentsuite.UpdateConfirmation(pool))))
	mux.Handle("/investment/confirmation/update-bulk", mfMid(http.HandlerFunc(investmentsuite.UpdateConfirmationBulk(pool))))
	mux.Handle("/investment/confirmation/delete", mfMid(http.HandlerFunc(investmentsuite.DeleteConfirmation(pool))))
	mux.Handle("/investment/confirmation/approve", mfMid(http.HandlerFunc(investmentsuite.BulkApproveConfirmationActions(pool))))
	mux.Handle("/investment/confirmation/reject", mfMid(http.HandlerFunc(investmentsuite.BulkRejectConfirmationActions(pool))))
	mux.Handle("/investment/confirmation/all", mfMid(http.HandlerFunc(investmentsuite.GetConfirmationsWithAudit(pool))))
	mux.Handle("/investment/confirmation/detail", mfMid(http.HandlerFunc(investmentsuite.GetConfirmationDetail(pool))))
	// mux.Handle("/investment/confirmations/all", mfMid(http.HandlerFunc(investmentsuite.GetAllConfirmationsWithAudit(pool))))
	mux.Handle("/investment/confirmation/approved", mfMid(http.HandlerFunc(investmentsuite.GetApprovedConfirmations(pool))))
	mux.Handle("/investment/confirmation/confirm", mfMid(http.HandlerFunc(investmentsuite.ConfirmInvestment(pool))))
	mux.Handle("/investment/confirmation/audit-history", mfMid(http.HandlerFunc(investmentsuite.GetConfirmationAuditHistory(pool))))
	mux.Handle("/investment/confirmation/additional-files/list", mfMid(http.HandlerFunc(investmentfiles.ListConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/upload", mfMid(http.HandlerFunc(investmentfiles.UploadConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/download", mfMid(http.HandlerFunc(investmentfiles.DownloadConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/download-bulk", mfMid(http.HandlerFunc(investmentfiles.DownloadSelectedConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/delete", mfMid(http.HandlerFunc(investmentfiles.DeleteConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/audit", mfMid(http.HandlerFunc(investmentfiles.AuditConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/delete/approve", mfMid(http.HandlerFunc(investmentfiles.ApproveDeleteConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/delete/reject", mfMid(http.HandlerFunc(investmentfiles.RejectDeleteConfirmationAdditionalFileHandler(pool))))

	// Investment redemption/portfolio endpoints
	mux.Handle("/investment/portfolio/get", mfMid(http.HandlerFunc(redemption.GetPortfolioWithTransactions(pool))))
	mux.Handle("/investment/portfolio/refresh", mfMid(http.HandlerFunc(portfolio.RefreshPortfolioSnapshots(pool))))
	mux.Handle("/investment/portfolio/transactions", mfMid(http.HandlerFunc(portfolio.GetPortfolioTransactions(pool))))
	mux.Handle("/investment/onboard/batch/delete-transaction", mfMid(http.HandlerFunc(onboard.DeleteOnboardTransaction(pool))))
	mux.Handle("/investment/redemption/calculate-fifo", mfMid(http.HandlerFunc(redemption.CalculateRedemptionFIFO(pool))))

	// Redemption initiation endpoints
	mux.Handle("/investment/redemption/initiation/create", mfMid(http.HandlerFunc(redemption.CreateRedemptionSingle(pool))))
	mux.Handle("/investment/redemption/initiation/create-bulk", mfMid(http.HandlerFunc(redemption.CreateRedemptionBulk(pool))))
	mux.Handle("/investment/redemption/initiation/update", mfMid(http.HandlerFunc(redemption.UpdateRedemption(pool))))
	mux.Handle("/investment/redemption/initiation/update-bulk", mfMid(http.HandlerFunc(redemption.UpdateRedemptionBulk(pool))))
	mux.Handle("/investment/redemption/initiation/delete", mfMid(http.HandlerFunc(redemption.DeleteRedemption(pool))))
	mux.Handle("/investment/redemption/initiation/approve", mfMid(http.HandlerFunc(redemption.BulkApproveRedemptionActions(pool))))
	mux.Handle("/investment/redemption/initiation/reject", mfMid(http.HandlerFunc(redemption.BulkRejectRedemptionActions(pool))))
	mux.Handle("/investment/redemption/initiation/detail", mfMid(http.HandlerFunc(redemption.GetRedemptionInitiationDetail(pool))))
	mux.Handle("/investment/redemption/initiation/all", mfMid(http.HandlerFunc(redemption.GetRedemptionsWithAudit(pool))))
	mux.Handle("/investment/redemption/initiation/approved", mfMid(http.HandlerFunc(redemption.GetApprovedRedemptions(pool))))
	mux.Handle("/investment/redemption/initiation/audit-history", mfMid(http.HandlerFunc(redemption.GetRedemptionInitiationAuditHistory(pool))))
	mux.Handle("/investment/redemption/initiation/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadRedemptionInitiationPackageZipHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/list", mfMid(http.HandlerFunc(investmentfiles.ListRedemptionInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/upload", mfMid(http.HandlerFunc(investmentfiles.UploadRedemptionInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/download", mfMid(http.HandlerFunc(investmentfiles.DownloadRedemptionInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/download-bulk", mfMid(http.HandlerFunc(investmentfiles.DownloadSelectedRedemptionInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/delete", mfMid(http.HandlerFunc(investmentfiles.DeleteRedemptionInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/audit", mfMid(http.HandlerFunc(investmentfiles.AuditRedemptionInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/delete/approve", mfMid(http.HandlerFunc(investmentfiles.ApproveDeleteRedemptionInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/delete/reject", mfMid(http.HandlerFunc(investmentfiles.RejectDeleteRedemptionInitiationAdditionalFileHandler(pool))))

	// Redemption confirmation endpoints
	mux.Handle("/investment/redemption/confirmation/create", mfMid(http.HandlerFunc(redemption.CreateRedemptionConfirmationSingle(pool))))
	mux.Handle("/investment/redemption/confirmation/download", mfMid(http.HandlerFunc(redemption.GetRedemptionConfirmationDownloadURL(pool))))
	mux.Handle("/investment/redemption/confirmation/download-bulk", mfMid(http.HandlerFunc(redemption.GetRedemptionConfirmationBulkDownloadURL(pool))))
	mux.Handle("/investment/redemption/confirmation/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadRedemptionConfirmationPackageZipHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/create-bulk", mfMid(http.HandlerFunc(redemption.CreateRedemptionConfirmationBulk(pool))))
	mux.Handle("/investment/redemption/confirmation/update", mfMid(http.HandlerFunc(redemption.UpdateRedemptionConfirmation(pool))))
	mux.Handle("/investment/redemption/confirmation/update-bulk", mfMid(http.HandlerFunc(redemption.UpdateRedemptionConfirmationBulk(pool))))
	mux.Handle("/investment/redemption/confirmation/delete", mfMid(http.HandlerFunc(redemption.DeleteRedemptionConfirmation(pool))))
	mux.Handle("/investment/redemption/confirmation/approve", mfMid(http.HandlerFunc(redemption.BulkApproveRedemptionConfirmationActions(pool))))
	mux.Handle("/investment/redemption/confirmation/reject", mfMid(http.HandlerFunc(redemption.BulkRejectRedemptionConfirmationActions(pool))))
	mux.Handle("/investment/redemption/confirmation/all", mfMid(http.HandlerFunc(redemption.GetRedemptionConfirmationsWithAudit(pool))))
	mux.Handle("/investment/redemption/confirmation/detail", mfMid(http.HandlerFunc(redemption.GetRedemptionConfirmationDetail(pool))))
	mux.Handle("/investment/redemption/confirmation/approved", mfMid(http.HandlerFunc(redemption.GetApprovedRedemptionConfirmations(pool))))
	mux.Handle("/investment/redemption/confirmation/confirm", mfMid(http.HandlerFunc(redemption.ConfirmRedemption(pool))))
	mux.Handle("/investment/redemption/confirmation/audit-history", mfMid(http.HandlerFunc(redemption.GetRedemptionConfirmationAuditHistory(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/list", mfMid(http.HandlerFunc(investmentfiles.ListRedemptionConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/upload", mfMid(http.HandlerFunc(investmentfiles.UploadRedemptionConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/download", mfMid(http.HandlerFunc(investmentfiles.DownloadRedemptionConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/download-bulk", mfMid(http.HandlerFunc(investmentfiles.DownloadSelectedRedemptionConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/delete", mfMid(http.HandlerFunc(investmentfiles.DeleteRedemptionConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/audit", mfMid(http.HandlerFunc(investmentfiles.AuditRedemptionConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/delete/approve", mfMid(http.HandlerFunc(investmentfiles.ApproveDeleteRedemptionConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/delete/reject", mfMid(http.HandlerFunc(investmentfiles.RejectDeleteRedemptionConfirmationAdditionalFileHandler(pool))))

	// Accounting Workbench - Main Activity endpoints
	mux.Handle("/investment/accounting/activity/create", mfMid(http.HandlerFunc(accountingworkbench.CreateActivitySingle(pool))))
	mux.Handle("/investment/accounting/activity/create-bulk", mfMid(http.HandlerFunc(accountingworkbench.CreateActivityBulk(pool))))
	mux.Handle("/investment/accounting/activity/update", mfMid(http.HandlerFunc(accountingworkbench.UpdateActivity(pool))))
	mux.Handle("/investment/accounting/activity/delete", mfMid(http.HandlerFunc(accountingworkbench.DeleteActivity(pool))))
	mux.Handle("/investment/accounting/activity/approve", mfMid(http.HandlerFunc(accountingworkbench.BulkApproveActivityActions(pool))))
	mux.Handle("/investment/accounting/activity/reject", mfMid(http.HandlerFunc(accountingworkbench.BulkRejectActivityActions(pool))))
	mux.Handle("/investment/accounting/activity/all", mfMid(http.HandlerFunc(accountingworkbench.GetActivitiesWithAudit(pool))))
	mux.Handle("/investment/accounting/activity/approved", mfMid(http.HandlerFunc(accountingworkbench.GetApprovedActivities(pool))))
	mux.Handle("/investment/accounting/activity/audit-history", mfMid(http.HandlerFunc(accountingworkbench.GetAccountingActivityAuditHistory(pool))))
	mux.Handle("/investment/accounting/activity/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadAccountingActivityPackageZipHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/list", mfMid(http.HandlerFunc(investmentfiles.ListAccountingActivityAdditionalFilesHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/upload", mfMid(http.HandlerFunc(investmentfiles.UploadAccountingActivityAdditionalFilesHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/download", mfMid(http.HandlerFunc(investmentfiles.DownloadAccountingActivityAdditionalFileHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/download-bulk", mfMid(http.HandlerFunc(investmentfiles.DownloadSelectedAccountingActivityAdditionalFilesHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/delete", mfMid(http.HandlerFunc(investmentfiles.DeleteAccountingActivityAdditionalFileHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/audit", mfMid(http.HandlerFunc(investmentfiles.AuditAccountingActivityAdditionalFileHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/delete/approve", mfMid(http.HandlerFunc(investmentfiles.ApproveDeleteAccountingActivityAdditionalFileHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/delete/reject", mfMid(http.HandlerFunc(investmentfiles.RejectDeleteAccountingActivityAdditionalFileHandler(pool))))

	// FD Booking & Creation additional files
	mux.Handle("/investment/fd/booking/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDBookingPackageZipHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDBookingAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDBookingAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDBookingAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDConfirmationPackageZipHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDMasterPackageZipHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDMasterAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDMasterAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDMasterAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDClosurePackageZipHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDClosureAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDClosureAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDClosureAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDRolloverPackageZipHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDRolloverAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDRolloverAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDRolloverAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDCashflowPackageZipHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDCashflowAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDCashflowAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDCashflowAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDCashflowAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDCashflowAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDCashflowAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDCashflowAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDCashflowAdditionalFileHandler(pool))))

	// FD Interest, TDS & Reconciliation additional files
	mux.Handle("/investment/fd/receipt/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDInterestReceiptPackageZipHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDInterestReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDInterestReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDInterestReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDTDSReceiptPackageZipHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDTDSReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDTDSReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/tds-reconciliation/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDTDSReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDTDSReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDReconcileResultPackageZipHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDReconcileResultAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDReconcileResultAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDReconcileResultAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDReceiptExceptionPackageZipHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDReceiptExceptionAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDReceiptExceptionAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDReceiptExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDReceiptExceptionAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDReceiptExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDReceiptExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDReceiptExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDReceiptExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/variance-exception/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadVarianceExceptionPackageZipHandler(pool))))
	mux.Handle("/investment/variance-exception/additional-files/list", mfMid(http.HandlerFunc(investmentfiles.ListVarianceExceptionAdditionalFilesHandler(pool))))
	mux.Handle("/investment/variance-exception/additional-files/upload", mfMid(http.HandlerFunc(investmentfiles.UploadVarianceExceptionAdditionalFilesHandler(pool))))
	mux.Handle("/investment/variance-exception/additional-files/download", mfMid(http.HandlerFunc(investmentfiles.DownloadVarianceExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/variance-exception/additional-files/download-bulk", mfMid(http.HandlerFunc(investmentfiles.DownloadSelectedVarianceExceptionAdditionalFilesHandler(pool))))
	mux.Handle("/investment/variance-exception/additional-files/delete", mfMid(http.HandlerFunc(investmentfiles.DeleteVarianceExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/variance-exception/additional-files/audit", mfMid(http.HandlerFunc(investmentfiles.AuditVarianceExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/variance-exception/additional-files/delete/approve", mfMid(http.HandlerFunc(investmentfiles.ApproveDeleteVarianceExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/variance-exception/additional-files/delete/reject", mfMid(http.HandlerFunc(investmentfiles.RejectDeleteVarianceExceptionAdditionalFileHandler(pool))))

	// FD Accrual & Accounting additional files
	mux.Handle("/investment/fd/accrual/schedule/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDAccrualScheduleConfigPackageZipHandler(pool))))
	mux.Handle("/investment/fd/accrual/schedule/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDAccrualScheduleConfigAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/schedule/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDAccrualScheduleConfigAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/schedule/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDAccrualScheduleConfigAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/schedule/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDAccrualScheduleConfigAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/schedule/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDAccrualScheduleConfigAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/schedule/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDAccrualScheduleConfigAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/schedule/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDAccrualScheduleConfigAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/schedule/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDAccrualScheduleConfigAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDAccrualRunPackageZipHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDAccrualRunAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDAccrualRunAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDAccrualRunAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDAccrualLedgerPackageZipHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDAccrualLedgerAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDAccrualLedgerAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDAccrualLedgerAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDAccountingJournalPackageZipHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/list", fdMid(http.HandlerFunc(investmentfiles.ListFDAccountingJournalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/upload", fdMid(http.HandlerFunc(investmentfiles.UploadFDAccountingJournalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/download", fdMid(http.HandlerFunc(investmentfiles.DownloadFDAccountingJournalAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/download-bulk", fdMid(http.HandlerFunc(investmentfiles.DownloadSelectedFDAccountingJournalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/delete", fdMid(http.HandlerFunc(investmentfiles.DeleteFDAccountingJournalAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/audit", fdMid(http.HandlerFunc(investmentfiles.AuditFDAccountingJournalAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/delete/approve", fdMid(http.HandlerFunc(investmentfiles.ApproveDeleteFDAccountingJournalAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/delete/reject", fdMid(http.HandlerFunc(investmentfiles.RejectDeleteFDAccountingJournalAdditionalFileHandler(pool))))

	// Accounting Workbench - Journal Entry endpoints
	mux.Handle("/investment/accounting/journal-entries", mfMid(http.HandlerFunc(accountingworkbench.GetJournalEntries(pool))))

	// Accounting Workbench - MTM endpoints
	mux.Handle("/investment/accounting/mtm/create", mfMid(http.HandlerFunc(accountingworkbench.CreateMTMSingle(pool))))
	mux.Handle("/investment/accounting/mtm/create-bulk", mfMid(http.HandlerFunc(accountingworkbench.CreateMTMBulk(pool))))
	mux.Handle("/investment/accounting/mtm/preview", mfMid(http.HandlerFunc(accountingworkbench.PreviewMTMBulk(pool))))
	mux.Handle("/investment/accounting/mtm/commit", mfMid(http.HandlerFunc(accountingworkbench.CommitMTMBulk(pool))))
	mux.Handle("/investment/accounting/mtm/update", mfMid(http.HandlerFunc(accountingworkbench.UpdateMTM(pool))))
	mux.Handle("/investment/accounting/mtm/all", mfMid(http.HandlerFunc(accountingworkbench.GetMTMWithAudit(pool))))

	// Accounting Workbench - Dividend endpoints
	mux.Handle("/investment/accounting/dividend/create", mfMid(http.HandlerFunc(accountingworkbench.CreateDividendSingle(pool))))
	mux.Handle("/investment/accounting/dividend/create-bulk", mfMid(http.HandlerFunc(accountingworkbench.CreateDividendBulk(pool))))
	mux.Handle("/investment/accounting/dividend/update", mfMid(http.HandlerFunc(accountingworkbench.UpdateDividend(pool))))
	mux.Handle("/investment/accounting/dividend/all", mfMid(http.HandlerFunc(accountingworkbench.GetDividendsWithAudit(pool))))

	// Accounting Workbench - Corporate Action endpoints
	mux.Handle("/investment/accounting/corporate-action/create", mfMid(http.HandlerFunc(accountingworkbench.CreateCorporateActionSingle(pool))))
	mux.Handle("/investment/accounting/corporate-action/create-bulk", mfMid(http.HandlerFunc(accountingworkbench.CreateCorporateActionBulk(pool))))
	mux.Handle("/investment/accounting/corporate-action/update", mfMid(http.HandlerFunc(accountingworkbench.UpdateCorporateAction(pool))))
	mux.Handle("/investment/accounting/corporate-action/all", mfMid(http.HandlerFunc(accountingworkbench.GetCorporateActionsWithAudit(pool))))

	// Accounting Workbench - Fair Value Override endpoints
	mux.Handle("/investment/accounting/fvo/create", mfMid(http.HandlerFunc(accountingworkbench.CreateFVOSingle(pool))))
	mux.Handle("/investment/accounting/fvo/download", mfMid(http.HandlerFunc(accountingworkbench.GetFVODownloadURL(pool))))
	mux.Handle("/investment/accounting/fvo/download-bulk", mfMid(http.HandlerFunc(accountingworkbench.GetFVOBulkDownloadURL(pool))))
	mux.Handle("/investment/accounting/fvo/package-zip", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFVOPackageZipHandler(pool))))
	mux.Handle("/investment/accounting/fvo/create-bulk", mfMid(http.HandlerFunc(accountingworkbench.CreateFVOBulk(pool))))
	mux.Handle("/investment/accounting/fvo/update", mfMid(http.HandlerFunc(accountingworkbench.UpdateFVO(pool))))
	mux.Handle("/investment/accounting/fvo/all", mfMid(http.HandlerFunc(accountingworkbench.GetFVOsWithAudit(pool))))

	// Accounting Workbench - Temporal Query endpoints (NEW - for historical data)
	mux.Handle("/investment/accounting/scheme-data-as-of", mfMid(http.HandlerFunc(accountingworkbench.GetSchemeDataAsOf(pool))))
	mux.Handle("/investment/accounting/mtm-report-as-of", mfMid(http.HandlerFunc(accountingworkbench.GetMTMReportAsOf(pool))))

	// Temporal Query Test & Validation endpoints
	mux.Handle("/investment/accounting/test-temporal", mfMid(http.HandlerFunc(accountingworkbench.TestTemporalQueriesHandler(pool))))
	mux.Handle("/investment/accounting/verify-queries", mfMid(http.HandlerFunc(accountingworkbench.VerifyAllQueriesHandler(pool))))

	// AMFI sync endpoints
	mux.HandleFunc("/investment/amfi/sync-schemes", amfisync.SyncSchemesHandler(pool))
	mux.HandleFunc("/investment/amfi/update-nav", amfisync.UpdateNAVHandler(pool))

	// AMFI data retrieval endpoints
	mux.HandleFunc("/investment/amfi/get-schemes", amfisync.GetSchemeDataHandler(pool))

	// FD Booking Workbench (booking + confirmation)
	fdBooking.RegisterFDBookingRoutes(mux, pool)
	fdMaster.RegisterFDMasterRoutes(mux, pool)
	fdAccrual.RegisterFDAccrualRoutes(mux, pool)
	fdReceipt.RegisterFDReceiptRoutes(mux, pool)
	fdInterestWorkbench.RegisterFDInterestWorkbenchRoutes(mux, pool)
	fdMaturityAndRollover.RegisterFDMaturityRoutes(mux, pool)

	// Example routes for future implementation:
	// mux.HandleFunc("/investment/portfolio", portfolioHandler)
	// mux.HandleFunc("/investment/schemes", schemesHandler)

	return &http.Server{
		Addr:    ":" + port,
		Handler: observability.WrapHTTP(serviceName, mux),
	}
}

func StartInvestmentService(pool *pgxpool.Pool, db *sql.DB, port string) {
	server := NewInvestmentServer(pool, db, port)

	logger.LogInfo("Investment Service started on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.LogError("Investment service failed: %v", err)
	}
}
