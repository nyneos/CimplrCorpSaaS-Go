package investment

import (
	"database/sql"
	"log"
	"net/http"

	"CimplrCorpSaas/api"
	amfisync "CimplrCorpSaas/api/investment/amfi-sync"
	investmentsuite "CimplrCorpSaas/api/investment/investment-suite"
	onboard "CimplrCorpSaas/api/investment/onboarding"
	redemption "CimplrCorpSaas/api/investment/redemption"

	"github.com/jackc/pgx/v5/pgxpool"
)

func StartInvestmentService(pool *pgxpool.Pool, db *sql.DB) {
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
	mux.Handle("/investment/onboard/kpi", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.PostPortfolioSnapshot(pool))))
	mux.Handle("/investment/onboard/snapshot/refresh", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.RefreshPortfolioSnapshot(pool))))

	mux.Handle("/investment/onboard/batch/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.BulkApproveBatch(pool))))
	mux.Handle("/investment/onboard/batch/info", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetBatchInfo(pool))))
	mux.Handle("/investment/onboard/batch", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetAllBatches(pool))))

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

	// Investment initiation endpoints
	mux.Handle("/investment/initiation/upload", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UploadInitiationSimple(pool))))
	mux.Handle("/investment/initiation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateInitiationSingle(pool))))
	mux.Handle("/investment/initiation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateInitiationBulk(pool))))
	mux.Handle("/investment/initiation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateInitiation(pool))))
	mux.Handle("/investment/initiation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.UpdateInitiationBulk(pool))))
	mux.Handle("/investment/initiation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.DeleteInitiation(pool))))
	mux.Handle("/investment/initiation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkApproveInitiationActions(pool))))
	mux.Handle("/investment/initiation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.BulkRejectInitiationActions(pool))))
	mux.Handle("/investment/initiation/approved-active", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetApprovedActiveInitiations(pool))))
	mux.Handle("/investment/initiation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetInitiationsWithAudit(pool))))

	// Investment confirmation endpoints
	mux.Handle("/investment/confirmation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.CreateConfirmationSingle(pool))))
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

	// Investment redemption/portfolio endpoints
	mux.Handle("/investment/portfolio/get", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetPortfolioWithTransactions(pool))))
	mux.Handle("/investment/redemption/calculate-fifo", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CalculateRedemptionFIFO(pool))))

	// Redemption initiation endpoints
	mux.Handle("/investment/redemption/initiation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionSingle(pool))))
	mux.Handle("/investment/redemption/initiation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionBulk(pool))))
	mux.Handle("/investment/redemption/initiation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemption(pool))))
	mux.Handle("/investment/redemption/initiation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemptionBulk(pool))))
	mux.Handle("/investment/redemption/initiation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.DeleteRedemption(pool))))
	mux.Handle("/investment/redemption/initiation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkApproveRedemptionActions(pool))))
	mux.Handle("/investment/redemption/initiation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkRejectRedemptionActions(pool))))
	mux.Handle("/investment/redemption/initiation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionsWithAudit(pool))))
	mux.Handle("/investment/redemption/initiation/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetApprovedRedemptions(pool))))

	// Redemption confirmation endpoints
	mux.Handle("/investment/redemption/confirmation/create", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionConfirmationSingle(pool))))
	mux.Handle("/investment/redemption/confirmation/create-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.CreateRedemptionConfirmationBulk(pool))))
	mux.Handle("/investment/redemption/confirmation/update", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemptionConfirmation(pool))))
	mux.Handle("/investment/redemption/confirmation/update-bulk", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.UpdateRedemptionConfirmationBulk(pool))))
	mux.Handle("/investment/redemption/confirmation/delete", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.DeleteRedemptionConfirmation(pool))))
	mux.Handle("/investment/redemption/confirmation/approve", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkApproveRedemptionConfirmationActions(pool))))
	mux.Handle("/investment/redemption/confirmation/reject", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.BulkRejectRedemptionConfirmationActions(pool))))
	mux.Handle("/investment/redemption/confirmation/all", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetRedemptionConfirmationsWithAudit(pool))))
	mux.Handle("/investment/redemption/confirmation/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.GetApprovedRedemptionConfirmations(pool))))
	mux.Handle("/investment/redemption/confirmation/confirm", api.BusinessUnitMiddleware(db)(http.HandlerFunc(redemption.ConfirmRedemption(pool))))

	mux.Handle("/investment/proposal/initiation/details", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetInvestmentProposalDetails(pool))))
	// AMFI sync endpoints
	mux.HandleFunc("/investment/amfi/sync-schemes", amfisync.SyncSchemesHandler(pool))
	mux.HandleFunc("/investment/amfi/update-nav", amfisync.UpdateNAVHandler(pool))

	// AMFI data retrieval endpoints
	mux.HandleFunc("/investment/amfi/get-schemes", amfisync.GetSchemeDataHandler(pool))

	// TODO: Add more investment-related endpoints here
	// Example routes for future implementation:
	// mux.HandleFunc("/investment/portfolio", portfolioHandler)
	// mux.HandleFunc("/investment/schemes", schemesHandler)

	log.Println("Investment Service started on :7143")
	err := http.ListenAndServe(":7143", mux)
	if err != nil {
		log.Fatalf("Investment service failed: %v", err)
	}
}
