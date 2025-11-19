package investment

import (
	"database/sql"
	"log"
	"net/http"

	"CimplrCorpSaas/api"
	amfisync "CimplrCorpSaas/api/investment/amfi-sync"
	investmentsuite "CimplrCorpSaas/api/investment/investment-suite"
	onboard "CimplrCorpSaas/api/investment/onboarding"

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
	mux.Handle("/investment/proposals/meta/approved", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetApprovedProposalMeta(pool))))
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
	mux.Handle("/investment/initiation/with-audit", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetInitiationsWithAudit(pool))))

	mux.Handle("/investment/proposals/details", api.BusinessUnitMiddleware(db)(http.HandlerFunc(investmentsuite.GetInvestmentProposalDetails(pool))))
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
