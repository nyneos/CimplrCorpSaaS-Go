package cash

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/bankstatement"
	"CimplrCorpSaas/api/cash/payablerecievable"
	"CimplrCorpSaas/api/cash/projection"
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
	mux.Handle("/cash/upload-payrec", api.BusinessUnitMiddleware(db)(payablerecievable.UploadPayRec(pgxPool)))
	mux.Handle("/cash/bank-statements/all", api.BusinessUnitMiddleware(db)(bankstatement.GetBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-approve", bankstatement.BulkApproveBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/bulk-reject", bankstatement.BulkRejectBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/bulk-delete", bankstatement.BulkDeleteBankStatements(pgxPool))
	mux.Handle("/cash/payrec/all", api.BusinessUnitMiddleware(db)(payablerecievable.GetAllPayableReceivable(pgxPool)))

	// Bulk audit action routes for payables
	mux.Handle("/cash/payable/bulk-delete", payablerecievable.BulkDeletePayableAudit(pgxPool))
	mux.Handle("/cash/payable/bulk-reject", payablerecievable.BulkRejectPayableAuditActions(pgxPool))
	mux.Handle("/cash/payable/bulk-approve", payablerecievable.BulkApprovePayableAuditActions(pgxPool))

	// Bulk audit action routes for receivables
	mux.Handle("/cash/receivable/bulk-delete", payablerecievable.BulkDeleteReceivableAudit(pgxPool))
	mux.Handle("/cash/receivable/bulk-reject", payablerecievable.BulkRejectReceivableAuditActions(pgxPool))
	mux.Handle("/cash/receivable/bulk-approve", payablerecievable.BulkApproveReceivableAuditActions(pgxPool))

	
	// Cash flow projection routes
	mux.Handle("/cash/cashflow-projection/create", api.BusinessUnitMiddleware(db)(projection.CreateAndSyncCashFlowProposals(pgxPool)))
	mux.Handle("/cash/cashflow-projection/all", api.BusinessUnitMiddleware(db)(projection.GetCashFlowProposals(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-delete", api.BusinessUnitMiddleware(db)(projection.DeleteCashFlowProposal(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-reject", api.BusinessUnitMiddleware(db)(projection.BulkRejectCashFlowProposalActions(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-approve", api.BusinessUnitMiddleware(db)(projection.BulkApproveCashFlowProposalActions(pgxPool)))

	mux.Handle("/cash/cashflow-projection/make", api.BusinessUnitMiddleware(db)(projection.AbsorbFlattenedProjections(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get", api.BusinessUnitMiddleware(db)(projection.GetFlattenedProjections(pgxPool)))
	mux.Handle("/cash/cashflow-projection/audit", api.BusinessUnitMiddleware(db)(projection.GetAuditActions(pgxPool)))

	mux.HandleFunc("/cash/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Cash Service"))
	})
	log.Println("Cash Service started on :6143")
	err = http.ListenAndServe(":6143", mux)
	if err != nil {
		log.Fatalf("Cash Service failed: %v", err)
	}
}



