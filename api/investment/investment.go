package investment

import (
	"log"
	"net/http"

	// amfisync "CimplrCorpSaas/api/investment/amfi-sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

func StartInvestmentService(pool *pgxpool.Pool) {
	mux := http.NewServeMux()

	mux.HandleFunc("/investment/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Investment Service is active"))
	})

	// AMFI sync endpoints
	// mux.HandleFunc("/investment/amfi/sync-schemes", amfisync.SyncSchemesHandler(pool))
	// mux.HandleFunc("/investment/amfi/update-nav", amfisync.UpdateNAVHandler(pool))

	log.Println("Investment Service started on :7143")
	err := http.ListenAndServe(":7143", mux)
	if err != nil {
		log.Fatalf("Investment service failed: %v", err)
	}
}
