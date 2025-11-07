package investment

import (
	"log"
	"net/http"
	"database/sql"

	amfisync "CimplrCorpSaas/api/investment/amfi-sync"
	onboard "CimplrCorpSaas/api/investment/onboarding"
	"CimplrCorpSaas/api"

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
	mux.Handle("/investment/onboard/kpi", api.BusinessUnitMiddleware(db)(http.HandlerFunc(onboard.GetPortfolioSnapshot(pool))))
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
