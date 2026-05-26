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

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func StartInvestmentService(pool *pgxpool.Pool, db *sql.DB, port string) {
	mux := http.NewServeMux()

	mux.HandleFunc("/investment/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Investment Service is active"))
	})

	// // Onboarding workbench (protected by BusinessUnitMiddleware)
	// mux.Handle("/investment/onboard/workbench", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.OnboardPortfolioWorkbench(pool))))

	// Onboarding utility endpoints (AMFI/schemes/folios/demat)
	mux.Handle("/investment/onboard/amc-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetAMFISchemeAMCEnriched(pool))))
	mux.Handle("/investment/onboard/schemes-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetAMFISchemesByMultipleAMCs(pool))))
	mux.Handle("/investment/onboard/folios-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetFoliosBySchemeListSimple(pool))))
	mux.Handle("/investment/onboard/folios-grouped", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetFoliosBySchemeListGrouped(pool))))
	mux.Handle("/investment/onboard/demat-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetDematWithDPInfo(pool))))
	mux.Handle("/investment/onboard/dps-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetAllDPs(pool))))
	mux.Handle("/investment/onboard/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.UploadInvestmentBulkk(pool))))
	mux.Handle("/investment/onboard/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetOnboardDownloadURL(pool))))
	mux.Handle("/investment/onboard/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetOnboardBulkDownloadURL(pool))))
	mux.Handle("/investment/onboard/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListOnboardAdditionalFilesHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadOnboardAdditionalFilesHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedOnboardAdditionalFilesHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteOnboardAdditionalFileHandler(pool))))
	mux.Handle("/investment/onboard/kpi", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.PostPortfolioSnapshot(pool))))
	mux.Handle("/investment/onboard/snapshot/refresh", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.RefreshPortfolioSnapshot(pool))))

	mux.Handle("/investment/onboard/batch/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.BulkApproveBatch(pool))))
	mux.Handle("/investment/onboard/batch/info", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetBatchInfo(pool))))
	mux.Handle("/investment/onboard/batch", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetAllBatches(pool))))
	mux.Handle("/investment/onboard/audit-history", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetOnboardingAuditHistory(pool))))

	// Investment suite manual actions
	mux.Handle("/investment/proposals/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateInvestmentProposal(pool))))
	mux.Handle("/investment/proposals/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateInvestmentProposal(pool))))
	mux.Handle("/investment/proposals/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkApproveProposals(pool))))
	mux.Handle("/investment/proposals/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkRejectProposals(pool))))
	mux.Handle("/investment/proposals/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkDeleteProposals(pool))))
	mux.Handle("/investment/proposals/meta", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetProposalMeta(pool))))
	mux.Handle("/investment/proposals/approved-active", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetApprovedProposalMeta(pool))))
	mux.Handle("/investment/proposals/detail", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetProposalDetail(pool))))
	mux.Handle("/investment/proposals/entity-holdings", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetEntitySchemeHoldings(pool))))
	mux.Handle("/investment/proposals/entity-accounts", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetEntityAccounts(pool))))
	mux.Handle("/investment/proposals/audit-history", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetProposalAuditHistory(pool))))
	mux.Handle("/investment/proposals/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListProposalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadProposalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadProposalAdditionalFileHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedProposalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteProposalAdditionalFileHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditProposalAdditionalFileHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteProposalAdditionalFileHandler(pool))))
	mux.Handle("/investment/proposals/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteProposalAdditionalFileHandler(pool))))

	// Investment initiation endpoints
	// mux.Handle("/investment/initiation/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UploadInitiationSimple(pool))))
	mux.Handle("/investment/initiation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateInitiationSingle(pool))))
	mux.Handle("/investment/initiation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateInitiationBulk(pool))))
	mux.Handle("/investment/initiation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateInitiation(pool))))
	mux.Handle("/investment/initiation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateInitiationBulk(pool))))
	mux.Handle("/investment/initiation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.DeleteInitiation(pool))))
	mux.Handle("/investment/initiation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkApproveInitiationActions(pool))))
	mux.Handle("/investment/initiation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkRejectInitiationActions(pool))))
	mux.Handle("/investment/initiation/approved-active", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetApprovedActiveInitiations(pool))))
	mux.Handle("/investment/initiation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetInitiationsWithAudit(pool))))
	mux.Handle("/investment/initiation/audit-history", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetInitiationAuditHistory(pool))))
	mux.Handle("/investment/initiation/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/initiation/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteInitiationAdditionalFileHandler(pool))))

	// Investment confirmation endpoints
	mux.Handle("/investment/confirmation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateConfirmationSingle(pool))))
	mux.Handle("/investment/confirmation/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetConfirmationDownloadURL(pool))))
	mux.Handle("/investment/confirmation/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetConfirmationBulkDownloadURL(pool))))
	mux.Handle("/investment/confirmation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateConfirmationBulk(pool))))
	mux.Handle("/investment/confirmation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateConfirmation(pool))))
	mux.Handle("/investment/confirmation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateConfirmationBulk(pool))))
	mux.Handle("/investment/confirmation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.DeleteConfirmation(pool))))
	mux.Handle("/investment/confirmation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkApproveConfirmationActions(pool))))
	mux.Handle("/investment/confirmation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkRejectConfirmationActions(pool))))
	mux.Handle("/investment/confirmation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetConfirmationsWithAudit(pool))))
	// mux.Handle("/investment/confirmations/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetAllConfirmationsWithAudit(pool))))
	mux.Handle("/investment/confirmation/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetApprovedConfirmations(pool))))
	mux.Handle("/investment/confirmation/confirm", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.ConfirmInvestment(pool))))
	mux.Handle("/investment/confirmation/audit-history", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetConfirmationAuditHistory(pool))))
	mux.Handle("/investment/confirmation/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/confirmation/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteConfirmationAdditionalFileHandler(pool))))

	// Investment redemption/portfolio endpoints
	mux.Handle("/investment/portfolio/get", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetPortfolioWithTransactions(pool))))
	mux.Handle("/investment/portfolio/refresh", api.BusinessUnitMiddleware(db)(http.HandlerFunc(portfolio.RefreshPortfolioSnapshots(pool))))
	mux.Handle("/investment/redemption/calculate-fifo", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CalculateRedemptionFIFO(pool))))

	// Redemption initiation endpoints
	mux.Handle("/investment/redemption/initiation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionSingle(pool))))
	mux.Handle("/investment/redemption/initiation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionBulk(pool))))
	mux.Handle("/investment/redemption/initiation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemption(pool))))
	mux.Handle("/investment/redemption/initiation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemptionBulk(pool))))
	mux.Handle("/investment/redemption/initiation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.DeleteRedemption(pool))))
	mux.Handle("/investment/redemption/initiation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkApproveRedemptionActions(pool))))
	mux.Handle("/investment/redemption/initiation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkRejectRedemptionActions(pool))))
	mux.Handle("/investment/redemption/initiation/detail", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionInitiationDetail(pool))))
	mux.Handle("/investment/redemption/initiation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionsWithAudit(pool))))
	mux.Handle("/investment/redemption/initiation/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetApprovedRedemptions(pool))))
	mux.Handle("/investment/redemption/initiation/audit-history", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionInitiationAuditHistory(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListRedemptionInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadRedemptionInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadRedemptionInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedRedemptionInitiationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteRedemptionInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditRedemptionInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteRedemptionInitiationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/initiation/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteRedemptionInitiationAdditionalFileHandler(pool))))

	// Redemption confirmation endpoints
	mux.Handle("/investment/redemption/confirmation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionConfirmationSingle(pool))))
	mux.Handle("/investment/redemption/confirmation/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionConfirmationDownloadURL(pool))))
	mux.Handle("/investment/redemption/confirmation/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionConfirmationBulkDownloadURL(pool))))
	mux.Handle("/investment/redemption/confirmation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionConfirmationBulk(pool))))
	mux.Handle("/investment/redemption/confirmation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemptionConfirmation(pool))))
	mux.Handle("/investment/redemption/confirmation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemptionConfirmationBulk(pool))))
	mux.Handle("/investment/redemption/confirmation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.DeleteRedemptionConfirmation(pool))))
	mux.Handle("/investment/redemption/confirmation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkApproveRedemptionConfirmationActions(pool))))
	mux.Handle("/investment/redemption/confirmation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkRejectRedemptionConfirmationActions(pool))))
	mux.Handle("/investment/redemption/confirmation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionConfirmationsWithAudit(pool))))
	mux.Handle("/investment/redemption/confirmation/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetApprovedRedemptionConfirmations(pool))))
	mux.Handle("/investment/redemption/confirmation/confirm", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.ConfirmRedemption(pool))))
	mux.Handle("/investment/redemption/confirmation/audit-history", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionConfirmationAuditHistory(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListRedemptionConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadRedemptionConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadRedemptionConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedRedemptionConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteRedemptionConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditRedemptionConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteRedemptionConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/redemption/confirmation/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteRedemptionConfirmationAdditionalFileHandler(pool))))

	// Accounting Workbench - Main Activity endpoints
	mux.Handle("/investment/accounting/activity/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateActivitySingle(pool))))
	mux.Handle("/investment/accounting/activity/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateActivityBulk(pool))))
	mux.Handle("/investment/accounting/activity/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateActivity(pool))))
	mux.Handle("/investment/accounting/activity/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.DeleteActivity(pool))))
	mux.Handle("/investment/accounting/activity/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.BulkApproveActivityActions(pool))))
	mux.Handle("/investment/accounting/activity/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.BulkRejectActivityActions(pool))))
	mux.Handle("/investment/accounting/activity/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetActivitiesWithAudit(pool))))
	mux.Handle("/investment/accounting/activity/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetApprovedActivities(pool))))
	mux.Handle("/investment/accounting/activity/audit-history", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetAccountingActivityAuditHistory(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListAccountingActivityAdditionalFilesHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadAccountingActivityAdditionalFilesHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadAccountingActivityAdditionalFileHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedAccountingActivityAdditionalFilesHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteAccountingActivityAdditionalFileHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditAccountingActivityAdditionalFileHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteAccountingActivityAdditionalFileHandler(pool))))
	mux.Handle("/investment/accounting/activity/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteAccountingActivityAdditionalFileHandler(pool))))

	// FD Booking & Creation additional files
	mux.Handle("/investment/fd/booking/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDBookingAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDBookingAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDBookingAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/booking/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDBookingAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDConfirmationAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/confirmation/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDConfirmationAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDMasterAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDMasterAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDMasterAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDMasterAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDClosureAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDClosureAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDClosureAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/closure/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDClosureAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDRolloverAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDRolloverAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDRolloverAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/rollover/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDRolloverAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDCashflowAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDCashflowAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDCashflowAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDCashflowAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDCashflowAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDCashflowAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDCashflowAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/master/cashflow/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDCashflowAdditionalFileHandler(pool))))

	// FD Interest, TDS & Reconciliation additional files
	mux.Handle("/investment/fd/receipt/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDInterestReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDInterestReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDInterestReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/receipt/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDInterestReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDTDSReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDTDSReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDTDSReceiptAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/tds-register/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDTDSReceiptAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDReconcileResultAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDReconcileResultAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDReconcileResultAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/reconcile/result/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDReconcileResultAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDReceiptExceptionAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDReceiptExceptionAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDReceiptExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDReceiptExceptionAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDReceiptExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDReceiptExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDReceiptExceptionAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/exception/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDReceiptExceptionAdditionalFileHandler(pool))))

	// FD Accrual & Accounting additional files
	mux.Handle("/investment/fd/accrual/run/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDAccrualRunAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDAccrualRunAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDAccrualRunAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/run/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDAccrualRunAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDAccrualLedgerAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDAccrualLedgerAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDAccrualLedgerAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accrual/ledger/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDAccrualLedgerAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/list", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ListFDAccountingJournalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.UploadFDAccountingJournalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadFDAccountingJournalAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DownloadSelectedFDAccountingJournalAdditionalFilesHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.DeleteFDAccountingJournalAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.AuditFDAccountingJournalAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/delete/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.ApproveDeleteFDAccountingJournalAdditionalFileHandler(pool))))
	mux.Handle("/investment/fd/accounting/journal/additional-files/delete/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentfiles.RejectDeleteFDAccountingJournalAdditionalFileHandler(pool))))

	// Accounting Workbench - Journal Entry endpoints
	mux.Handle("/investment/accounting/journal-entries", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetJournalEntries(pool))))

	// Accounting Workbench - MTM endpoints
	mux.Handle("/investment/accounting/mtm/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateMTMSingle(pool))))
	mux.Handle("/investment/accounting/mtm/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateMTMBulk(pool))))
	mux.Handle("/investment/accounting/mtm/preview", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.PreviewMTMBulk(pool))))
	mux.Handle("/investment/accounting/mtm/commit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CommitMTMBulk(pool))))
	mux.Handle("/investment/accounting/mtm/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateMTM(pool))))
	mux.Handle("/investment/accounting/mtm/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetMTMWithAudit(pool))))

	// Accounting Workbench - Dividend endpoints
	mux.Handle("/investment/accounting/dividend/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateDividendSingle(pool))))
	mux.Handle("/investment/accounting/dividend/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateDividendBulk(pool))))
	mux.Handle("/investment/accounting/dividend/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateDividend(pool))))
	mux.Handle("/investment/accounting/dividend/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetDividendsWithAudit(pool))))

	// Accounting Workbench - Corporate Action endpoints
	mux.Handle("/investment/accounting/corporate-action/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateCorporateActionSingle(pool))))
	mux.Handle("/investment/accounting/corporate-action/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateCorporateActionBulk(pool))))
	mux.Handle("/investment/accounting/corporate-action/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateCorporateAction(pool))))
	mux.Handle("/investment/accounting/corporate-action/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetCorporateActionsWithAudit(pool))))

	// Accounting Workbench - Fair Value Override endpoints
	mux.Handle("/investment/accounting/fvo/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateFVOSingle(pool))))
	mux.Handle("/investment/accounting/fvo/download", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetFVODownloadURL(pool))))
	mux.Handle("/investment/accounting/fvo/download-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetFVOBulkDownloadURL(pool))))
	mux.Handle("/investment/accounting/fvo/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateFVOBulk(pool))))
	mux.Handle("/investment/accounting/fvo/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateFVO(pool))))
	mux.Handle("/investment/accounting/fvo/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetFVOsWithAudit(pool))))

	// Accounting Workbench - Temporal Query endpoints (NEW - for historical data)
	mux.Handle("/investment/accounting/scheme-data-as-of", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetSchemeDataAsOf(pool))))
	mux.Handle("/investment/accounting/mtm-report-as-of", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetMTMReportAsOf(pool))))

	// Temporal Query Test & Validation endpoints
	mux.Handle("/investment/accounting/test-temporal", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.TestTemporalQueriesHandler(pool))))
	mux.Handle("/investment/accounting/verify-queries", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.VerifyAllQueriesHandler(pool))))

	// AMFI sync endpoints
	mux.HandleFunc("/investment/amfi/sync-schemes", amfisync.SyncSchemesHandler(pool))
	mux.HandleFunc("/investment/amfi/update-nav", amfisync.UpdateNAVHandler(pool))

	// AMFI data retrieval endpoints
	mux.HandleFunc("/investment/amfi/get-schemes", amfisync.GetSchemeDataHandler(pool))

	// FD Booking Workbench (booking + confirmation)
	fdBooking.RegisterFDBookingRoutes(mux, pool, db)
	fdMaster.RegisterFDMasterRoutes(mux, pool, db)
	fdAccrual.RegisterFDAccrualRoutes(mux, pool, db)
	fdReceipt.RegisterFDReceiptRoutes(mux, pool, db)
	fdInterestWorkbench.RegisterFDInterestWorkbenchRoutes(mux, pool, db)
	fdMaturityAndRollover.RegisterFDMaturityRoutes(mux, pool, db)

	// Example routes for future implementation:
	// mux.HandleFunc("/investment/portfolio", portfolioHandler)
	// mux.HandleFunc("/investment/schemes", schemesHandler)

	logger.LogInfo("Investment Service started on :%s", port)
	err := http.ListenAndServe(":"+port, mux)
	if err != nil {
		logger.LogError("Investment service failed: %v", err)
	}
}
