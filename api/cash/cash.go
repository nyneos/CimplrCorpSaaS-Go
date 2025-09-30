package cash

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/bankbalances"
	"CimplrCorpSaas/api/cash/bankstatement"
	"CimplrCorpSaas/api/cash/payablerecievable"
	"CimplrCorpSaas/api/cash/projection"
	"CimplrCorpSaas/api/cash/fundplanning"
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
	// mux.Handle("/cash/bank-statements/all", api.BusinessUnitMiddleware(db)(bankstatement.GetBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-approve", bankstatement.BulkApproveBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/bulk-reject", bankstatement.BulkRejectBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/bulk-delete", bankstatement.BulkDeleteBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/create", api.BusinessUnitMiddleware(db)(bankstatement.CreateBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/update", api.BusinessUnitMiddleware(db)(bankstatement.UpdateBankStatement(pgxPool)))
	mux.Handle("/cash/bank-statements/all", api.BusinessUnitMiddleware(db)(bankstatement.GetAllBankStatements(pgxPool)))
	
	mux.Handle("/cash/payrec/all", api.BusinessUnitMiddleware(db)(payablerecievable.GetAllPayableReceivable(pgxPool)))
	

	mux.Handle("/cash/fund-planning", api.BusinessUnitMiddleware(db)(fundplanning.GetFundPlanning(pgxPool)))


	// Bulk audit action routes for payables
	mux.Handle("/cash/payable/bulk-delete", payablerecievable.BulkDeletePayableAudit(pgxPool))
	mux.Handle("/cash/payable/bulk-reject", payablerecievable.BulkRejectPayableAuditActions(pgxPool))
	mux.Handle("/cash/payable/bulk-approve", payablerecievable.BulkApprovePayableAuditActions(pgxPool))
	mux.Handle("/cash/payable/create", api.BusinessUnitMiddleware(db)(payablerecievable.CreatePayable(pgxPool)))
	mux.Handle("/cash/payable/update", api.BusinessUnitMiddleware(db)(payablerecievable.UpdatePayable(pgxPool)))

	// Bulk audit action routes for receivables
	mux.Handle("/cash/receivable/bulk-delete", payablerecievable.BulkDeleteReceivableAudit(pgxPool))
	mux.Handle("/cash/receivable/bulk-reject", payablerecievable.BulkRejectReceivableAuditActions(pgxPool))
	mux.Handle("/cash/receivable/bulk-approve", payablerecievable.BulkApproveReceivableAuditActions(pgxPool))
	mux.Handle("/cash/receivable/create", api.BusinessUnitMiddleware(db)(payablerecievable.CreateReceivable(pgxPool)))
	mux.Handle("/cash/receivable/update", api.BusinessUnitMiddleware(db)(payablerecievable.UpdateReceivable(pgxPool)))
	
	// Cash flow projection routes
	mux.Handle("/cash/cashflow-projection/bulk-delete", api.BusinessUnitMiddleware(db)(projection.DeleteCashFlowProposal(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-reject", api.BusinessUnitMiddleware(db)(projection.BulkRejectCashFlowProposalActions(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-approve", api.BusinessUnitMiddleware(db)(projection.BulkApproveCashFlowProposalActions(pgxPool)))

	mux.Handle("/cash/cashflow-projection/make", api.BusinessUnitMiddleware(db)(projection.AbsorbFlattenedProjections(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-projection", api.BusinessUnitMiddleware(db)(projection.GetProposalVersion(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-header", api.BusinessUnitMiddleware(db)(projection.GetProjectionsSummary(pgxPool)))
	mux.Handle("/cash/cashflow-projection/update", api.BusinessUnitMiddleware(db)(projection.UpdateCashFlowProposal(pgxPool)))

	//bank balance 
	mux.Handle("/cash/bank-balances/create", api.BusinessUnitMiddleware(db)(bankbalances.CreateBankBalance(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-approve", api.BusinessUnitMiddleware(db)(bankbalances.BulkApproveBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-reject", api.BusinessUnitMiddleware(db)(bankbalances.BulkRejectBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-delete", api.BusinessUnitMiddleware(db)(bankbalances.BulkRequestDeleteBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/all", api.BusinessUnitMiddleware(db)(bankbalances.GetBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/upload", api.BusinessUnitMiddleware(db)(bankbalances.UploadBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/update", api.BusinessUnitMiddleware(db)(bankbalances.UpdateBankBalance(pgxPool)))

	mux.HandleFunc("/cash/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Cash Service"))
	})
	log.Println("Cash Service started on :6143")
	err = http.ListenAndServe(":6143", mux)
	if err != nil {
		log.Fatalf("Cash Service failed: %v", err)
	}
}











