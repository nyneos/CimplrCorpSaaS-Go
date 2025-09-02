package cash

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/bankstatement"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func StartCashService(db *sql.DB) {
	mux := http.NewServeMux()
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, name)
	pgxPool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatalf("failed to connect to pgxpool DB: %v", err)
	}
	mux.Handle("/cash/upload-bank-statement", bankstatement.UploadBankStatement(pgxPool))

	mux.Handle("/cash/bank-statements/all", api.BusinessUnitMiddleware(db)(bankstatement.GetBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-approve", bankstatement.BulkApproveBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/bulk-reject", bankstatement.BulkRejectBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/bulk-delete", bankstatement.BulkDeleteBankStatements(pgxPool))
	mux.HandleFunc("/cash/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Cash Service"))
	})
	log.Println("Cash Service started on :6143")
	err = http.ListenAndServe(":6143", mux)
	if err != nil {
		log.Fatalf("Cash Service failed: %v", err)
	}
}


