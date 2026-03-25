package investment

import (
	"database/sql"
	"net/http"
	// "log"
	// "os"

	"CimplrCorpSaas/api"
	accountingworkbench "CimplrCorpSaas/api/investment/accountingWorkbench"
	amfisync "CimplrCorpSaas/api/investment/amfi-sync"
	investmentsuite "CimplrCorpSaas/api/investment/investment-suite"
	onboard "CimplrCorpSaas/api/investment/onboarding"
	portfolio "CimplrCorpSaas/api/investment/portfolio"
	redemption "CimplrCorpSaas/api/investment/redemption"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterInvestmentRoutes(mux *http.ServeMux, db *sql.DB, pgxPool *pgxpool.Pool) {
	// mux := http.NewServeMux()

	mux.HandleFunc("/investment/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Investment Service is active"))
	})

	// Onboarding workbench (protected by BusinessUnitMiddleware)
	// mux.Handle("/investment/onboard/workbench", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.OnboardPortfolioWorkbench(pool))))

	// Onboarding utility endpoints (AMFI/schemes/folios/demat)
	mux.Handle("/investment/onboard/amc-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetAMFISchemeAMCEnriched(pgxPool))))
	mux.Handle("/investment/onboard/schemes-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetAMFISchemesByMultipleAMCs(pgxPool))))
	mux.Handle("/investment/onboard/folios-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetFoliosBySchemeListSimple(pgxPool))))
	mux.Handle("/investment/onboard/folios-grouped", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetFoliosBySchemeListGrouped(pgxPool))))
	mux.Handle("/investment/onboard/demat-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetDematWithDPInfo(pgxPool))))
	mux.Handle("/investment/onboard/dps-enriched", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetAllDPs(pgxPool))))
	mux.Handle("/investment/onboard/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.UploadInvestmentBulkk(pgxPool))))
	mux.Handle("/investment/onboard/kpi", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.PostPortfolioSnapshot(pgxPool))))
	mux.Handle("/investment/onboard/snapshot/refresh", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.RefreshPortfolioSnapshot(pgxPool))))

	mux.Handle("/investment/onboard/batch/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.BulkApproveBatch(pgxPool))))
	mux.Handle("/investment/onboard/batch/info", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetBatchInfo(pgxPool))))
	mux.Handle("/investment/onboard/batch", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetAllBatches(pgxPool))))

	// Investment suite manual actions
	mux.Handle("/investment/proposals/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateInvestmentProposal(pgxPool))))
	mux.Handle("/investment/proposals/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateInvestmentProposal(pgxPool))))
	mux.Handle("/investment/proposals/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkApproveProposals(pgxPool))))
	mux.Handle("/investment/proposals/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkRejectProposals(pgxPool))))
	mux.Handle("/investment/proposals/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkDeleteProposals(pgxPool))))
	mux.Handle("/investment/proposals/meta", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetProposalMeta(pgxPool))))
	mux.Handle("/investment/proposals/approved-active", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetApprovedProposalMeta(pgxPool))))
	mux.Handle("/investment/proposals/detail", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetProposalDetail(pgxPool))))
	mux.Handle("/investment/proposals/entity-holdings", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetEntitySchemeHoldings(pgxPool))))
	mux.Handle("/investment/proposals/entity-accounts", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetEntityAccounts(pgxPool))))

	// Investment initiation endpoints
	// mux.Handle("/investment/initiation/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UploadInitiationSimple(pool))))
	mux.Handle("/investment/initiation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateInitiationSingle(pgxPool))))
	mux.Handle("/investment/initiation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateInitiationBulk(pgxPool))))
	mux.Handle("/investment/initiation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateInitiation(pgxPool))))
	mux.Handle("/investment/initiation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateInitiationBulk(pgxPool))))
	mux.Handle("/investment/initiation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.DeleteInitiation(pgxPool))))
	mux.Handle("/investment/initiation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkApproveInitiationActions(pgxPool))))
	mux.Handle("/investment/initiation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkRejectInitiationActions(pgxPool))))
	mux.Handle("/investment/initiation/approved-active", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetApprovedActiveInitiations(pgxPool))))
	mux.Handle("/investment/initiation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetInitiationsWithAudit(pgxPool))))

	// Investment confirmation endpoints
	mux.Handle("/investment/confirmation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateConfirmationSingle(pgxPool))))
	mux.Handle("/investment/confirmation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateConfirmationBulk(pgxPool))))
	mux.Handle("/investment/confirmation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateConfirmation(pgxPool))))
	mux.Handle("/investment/confirmation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateConfirmationBulk(pgxPool))))
	mux.Handle("/investment/confirmation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.DeleteConfirmation(pgxPool))))
	mux.Handle("/investment/confirmation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkApproveConfirmationActions(pgxPool))))
	mux.Handle("/investment/confirmation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkRejectConfirmationActions(pgxPool))))
	mux.Handle("/investment/confirmation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetConfirmationsWithAudit(pgxPool))))
	// mux.Handle("/investment/confirmations/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetAllConfirmationsWithAudit(pool))))
	mux.Handle("/investment/confirmation/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetApprovedConfirmations(pgxPool))))
	mux.Handle("/investment/confirmation/confirm", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.ConfirmInvestment(pgxPool))))

	// Investment redemption/portfolio endpoints
	mux.Handle("/investment/portfolio/get", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetPortfolioWithTransactions(pgxPool))))
	mux.Handle("/investment/portfolio/refresh", api.BusinessUnitMiddleware(db)(http.HandlerFunc(portfolio.RefreshPortfolioSnapshots(pgxPool))))
	mux.Handle("/investment/redemption/calculate-fifo", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CalculateRedemptionFIFO(pgxPool))))

	// Redemption initiation endpoints
	mux.Handle("/investment/redemption/initiation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionSingle(pgxPool))))
	mux.Handle("/investment/redemption/initiation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionBulk(pgxPool))))
	mux.Handle("/investment/redemption/initiation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemption(pgxPool))))
	mux.Handle("/investment/redemption/initiation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemptionBulk(pgxPool))))
	mux.Handle("/investment/redemption/initiation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.DeleteRedemption(pgxPool))))
	mux.Handle("/investment/redemption/initiation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkApproveRedemptionActions(pgxPool))))
	mux.Handle("/investment/redemption/initiation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkRejectRedemptionActions(pgxPool))))
	mux.Handle("/investment/redemption/initiation/detail", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionInitiationDetail(pgxPool))))
	mux.Handle("/investment/redemption/initiation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionsWithAudit(pgxPool))))
	mux.Handle("/investment/redemption/initiation/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetApprovedRedemptions(pgxPool))))

	// Redemption confirmation endpoints
	mux.Handle("/investment/redemption/confirmation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionConfirmationSingle(pgxPool))))
	mux.Handle("/investment/redemption/confirmation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionConfirmationBulk(pgxPool))))
	mux.Handle("/investment/redemption/confirmation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemptionConfirmation(pgxPool))))
	mux.Handle("/investment/redemption/confirmation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemptionConfirmationBulk(pgxPool))))
	mux.Handle("/investment/redemption/confirmation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.DeleteRedemptionConfirmation(pgxPool))))
	mux.Handle("/investment/redemption/confirmation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkApproveRedemptionConfirmationActions(pgxPool))))
	mux.Handle("/investment/redemption/confirmation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkRejectRedemptionConfirmationActions(pgxPool))))
	mux.Handle("/investment/redemption/confirmation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionConfirmationsWithAudit(pgxPool))))
	mux.Handle("/investment/redemption/confirmation/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetApprovedRedemptionConfirmations(pgxPool))))
	mux.Handle("/investment/redemption/confirmation/confirm", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.ConfirmRedemption(pgxPool))))

	// Accounting Workbench - Main Activity endpoints
	mux.Handle("/investment/accounting/activity/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateActivitySingle(pgxPool))))
	mux.Handle("/investment/accounting/activity/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateActivityBulk(pgxPool))))
	mux.Handle("/investment/accounting/activity/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateActivity(pgxPool))))
	mux.Handle("/investment/accounting/activity/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.DeleteActivity(pgxPool))))
	mux.Handle("/investment/accounting/activity/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.BulkApproveActivityActions(pgxPool))))
	mux.Handle("/investment/accounting/activity/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.BulkRejectActivityActions(pgxPool))))
	mux.Handle("/investment/accounting/activity/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetActivitiesWithAudit(pgxPool))))
	mux.Handle("/investment/accounting/activity/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetApprovedActivities(pgxPool))))

	// Accounting Workbench - Journal Entry endpoints
	mux.Handle("/investment/accounting/journal-entries", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetJournalEntries(pgxPool))))

	// Accounting Workbench - MTM endpoints
	mux.Handle("/investment/accounting/mtm/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateMTMSingle(pgxPool))))
	mux.Handle("/investment/accounting/mtm/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateMTMBulk(pgxPool))))
	mux.Handle("/investment/accounting/mtm/preview", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.PreviewMTMBulk(pgxPool))))
	mux.Handle("/investment/accounting/mtm/commit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CommitMTMBulk(pgxPool))))
	mux.Handle("/investment/accounting/mtm/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateMTM(pgxPool))))
	mux.Handle("/investment/accounting/mtm/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetMTMWithAudit(pgxPool))))

	// Accounting Workbench - Dividend endpoints
	mux.Handle("/investment/accounting/dividend/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateDividendSingle(pgxPool))))
	mux.Handle("/investment/accounting/dividend/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateDividendBulk(pgxPool))))
	mux.Handle("/investment/accounting/dividend/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateDividend(pgxPool))))
	mux.Handle("/investment/accounting/dividend/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetDividendsWithAudit(pgxPool))))

	// Accounting Workbench - Corporate Action endpoints
	mux.Handle("/investment/accounting/corporate-action/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateCorporateActionSingle(pgxPool))))
	mux.Handle("/investment/accounting/corporate-action/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateCorporateActionBulk(pgxPool))))
	mux.Handle("/investment/accounting/corporate-action/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateCorporateAction(pgxPool))))
	mux.Handle("/investment/accounting/corporate-action/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetCorporateActionsWithAudit(pgxPool))))

	// Accounting Workbench - Fair Value Override endpoints
	mux.Handle("/investment/accounting/fvo/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateFVOSingle(pgxPool))))
	mux.Handle("/investment/accounting/fvo/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.CreateFVOBulk(pgxPool))))
	mux.Handle("/investment/accounting/fvo/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.UpdateFVO(pgxPool))))
	mux.Handle("/investment/accounting/fvo/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetFVOsWithAudit(pgxPool))))

	// Accounting Workbench - Temporal Query endpoints (NEW - for historical data)
	mux.Handle("/investment/accounting/scheme-data-as-of", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetSchemeDataAsOf(pgxPool))))
	mux.Handle("/investment/accounting/mtm-report-as-of", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.GetMTMReportAsOf(pgxPool))))

	// Temporal Query Test & Validation endpoints
	mux.Handle("/investment/accounting/test-temporal", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.TestTemporalQueriesHandler(pgxPool))))
	mux.Handle("/investment/accounting/verify-queries", api.BusinessUnitMiddleware(db)(http.HandlerFunc(accountingworkbench.VerifyAllQueriesHandler(pgxPool))))

	// AMFI sync endpoints
	mux.HandleFunc("/investment/amfi/sync-schemes", amfisync.SyncSchemesHandler(pgxPool))
	mux.HandleFunc("/investment/amfi/update-nav", amfisync.UpdateNAVHandler(pgxPool))

	// AMFI data retrieval endpoints
	mux.HandleFunc("/investment/amfi/get-schemes", amfisync.GetSchemeDataHandler(pgxPool))

	// Example routes for future implementation:
	// mux.HandleFunc("/investment/portfolio", portfolioHandler)
	// mux.HandleFunc("/investment/schemes", schemesHandler)

	// portEnv := os.Getenv("INVESTMENT_PORT")
	// if portEnv == "" {
	// 	portEnv = "7143"
	// }
	// log.Printf("Investment Service starting on :%s", portEnv)
	// err := http.ListenAndServe(":"+portEnv, mux)
	// if err != nil {
	// 	log.Fatalf("Investment service failed: %v", err)
	// }
}

/*
func StartInvestmentService(pool *pgxpool.Pool, db *sql.DB) {
	mux := http.NewServeMux()
	RegisterInvestmentRoutes(mux, db, pool)
}
*/
