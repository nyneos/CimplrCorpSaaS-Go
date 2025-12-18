package cash

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/bankbalances"
	"CimplrCorpSaas/api/cash/bankstatement"
	"CimplrCorpSaas/api/cash/fundplanning"
	"CimplrCorpSaas/api/cash/payablerecievable"
	"CimplrCorpSaas/api/cash/projection"
	sweepconfig "CimplrCorpSaas/api/cash/sweepConfig"
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
	mux.Handle("/cash/upload-bank-statement", bankstatement.UploadBankStatementV2Handler(db))
	// Category Master APIs
	mux.Handle("/cash/category/create", bankstatement.CreateTransactionCategoryHandler(db))
	mux.Handle("/cash/category/list", bankstatement.ListTransactionCategoriesHandler(db))
	mux.Handle("/cash/category/scope/create", bankstatement.CreateRuleScopeHandler(db))
	mux.Handle("/cash/category/rule/create", bankstatement.CreateCategoryRuleHandler(db))
	mux.Handle("/cash/category/rule-component/create", bankstatement.CreateCategoryRuleComponentHandler(db))
	mux.Handle("/cash/category/delete", bankstatement.DeleteMultipleTransactionCategoriesHandler(db))
	// V2 Bank Statement APIs
	mux.Handle("/cash/bank-statements/v2/get", bankstatement.GetAllBankStatementsHandler(db))
	mux.Handle("/cash/bank-statements/v2/transactions", bankstatement.GetBankStatementTransactionsHandler(db))
	mux.Handle("/cash/bank-statements/v2/recompute-kpis", bankstatement.RecomputeBankStatementSummaryHandler(db))
	mux.Handle("/cash/bank-statements/v2/approve", bankstatement.ApproveBankStatementHandler(db))
	mux.Handle("/cash/bank-statements/v2/reject", bankstatement.RejectBankStatementHandler(db))
	mux.Handle("/cash/bank-statements/v2/delete", bankstatement.DeleteBankStatementHandler(db))
	mux.Handle("/cash/upload-payrec", api.BusinessUnitMiddleware(db)(payablerecievable.UploadPayRec(pgxPool)))
	// mux.Handle("/cash/bank-statements/all", api.BusinessUnitMiddleware(db)(bankstatement.GetBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-approve", bankstatement.BulkApproveBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/bulk-reject", bankstatement.BulkRejectBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/bulk-delete", bankstatement.BulkDeleteBankStatements(pgxPool))
	mux.Handle("/cash/bank-statements/create", api.BusinessUnitMiddleware(db)(bankstatement.CreateBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/update", api.BusinessUnitMiddleware(db)(bankstatement.UpdateBankStatement(pgxPool)))
	mux.Handle("/cash/bank-statements/all", api.BusinessUnitMiddleware(db)(bankstatement.GetAllBankStatements(pgxPool)))

	// Unified bulk endpoints for transactions (payables & receivables)
	mux.Handle("/cash/transactions/bulk-delete", api.BusinessUnitMiddleware(db)(payablerecievable.BulkRequestDeleteTransactions(pgxPool)))
	mux.Handle("/cash/transactions/bulk-reject", api.BusinessUnitMiddleware(db)(payablerecievable.BulkRejectTransactions(pgxPool)))
	mux.Handle("/cash/transactions/bulk-approve", api.BusinessUnitMiddleware(db)(payablerecievable.BulkApproveTransactions(pgxPool)))
	mux.Handle("/cash/transactions/create", api.BusinessUnitMiddleware(db)(payablerecievable.BulkCreateTransactions(pgxPool)))
	mux.Handle("/cash/transactions/update", api.BusinessUnitMiddleware(db)(payablerecievable.UpdateTransaction(pgxPool)))
	mux.Handle("/cash/transactions/upload-payrec-batch", api.BusinessUnitMiddleware(db)(payablerecievable.BatchUploadTransactionsV2(pgxPool))) //twotwo
	mux.Handle("/cash/transactions/all", api.BusinessUnitMiddleware(db)(payablerecievable.GetAllPayableReceivable(pgxPool)))

	//fundplanning
	mux.Handle("/cash/fund-planning", api.BusinessUnitMiddleware(db)(fundplanning.GetFundPlanningEnhanced(pgxPool)))
	mux.Handle("/cash/fund-planning/create", api.BusinessUnitMiddleware(db)(fundplanning.CreateFundPlan(pgxPool)))
	mux.Handle("/cash/fund-planning/summary", api.BusinessUnitMiddleware(db)(fundplanning.GetFundPlanSummary(pgxPool)))
	mux.Handle("/cash/fund-planning/details", api.BusinessUnitMiddleware(db)(fundplanning.GetFundPlanDetails(pgxPool)))
	mux.Handle("/cash/fund-planning/bulk-approve", api.BusinessUnitMiddleware(db)(fundplanning.BulkApproveFundPlans(pgxPool)))
	mux.Handle("/cash/fund-planning/bulk-reject", api.BusinessUnitMiddleware(db)(fundplanning.BulkRejectFundPlans(pgxPool)))
	mux.Handle("/cash/fund-planning/bulk-delete", api.BusinessUnitMiddleware(db)(fundplanning.BulkRequestDeleteFundPlans(pgxPool)))
	mux.Handle("/cash/fund-planning/bank-accounts", api.BusinessUnitMiddleware(db)(fundplanning.GetApprovedBankAccountsForFundPlanning(pgxPool)))

	// Sweep configuration routes
	mux.Handle("/cash/sweep-config/create", api.BusinessUnitMiddleware(db)(sweepconfig.CreateSweepConfiguration(pgxPool)))
	mux.Handle("/cash/sweep-config/update", api.BusinessUnitMiddleware(db)(sweepconfig.UpdateSweepConfiguration(pgxPool)))
	mux.Handle("/cash/sweep-config/all", api.BusinessUnitMiddleware(db)(sweepconfig.GetSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config/bulk-approve", api.BusinessUnitMiddleware(db)(sweepconfig.BulkApproveSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config/bulk-reject", api.BusinessUnitMiddleware(db)(sweepconfig.BulkRejectSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config/request-delete", api.BusinessUnitMiddleware(db)(sweepconfig.BulkRequestDeleteSweepConfigurations(pgxPool)))
	// Cash flow projection routes
	mux.Handle("/cash/cashflow-projection/bulk-delete", api.BusinessUnitMiddleware(db)(projection.DeleteCashFlowProposal(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-reject", api.BusinessUnitMiddleware(db)(projection.BulkRejectCashFlowProposalActions(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-approve", api.BusinessUnitMiddleware(db)(projection.BulkApproveCashFlowProposalActions(pgxPool)))

	mux.Handle("/cash/cashflow-projection/make", api.BusinessUnitMiddleware(db)(projection.AbsorbFlattenedProjections(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-projection", api.BusinessUnitMiddleware(db)(projection.GetProposalVersion(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-header", api.BusinessUnitMiddleware(db)(projection.GetProjectionsSummary(pgxPool)))
	mux.Handle("/cash/cashflow-projection/update", api.BusinessUnitMiddleware(db)(projection.UpdateCashFlowProposal(pgxPool)))

	mux.Handle("/cash/cashflow-projection/upload", api.BusinessUnitMiddleware(db)(projection.UploadCashflowProposalSimple(pgxPool)))

	//bank balance
	mux.Handle("/cash/bank-balances/create", api.BusinessUnitMiddleware(db)(bankbalances.CreateBankBalance(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-approve", api.BusinessUnitMiddleware(db)(bankbalances.BulkApproveBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-reject", api.BusinessUnitMiddleware(db)(bankbalances.BulkRejectBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-delete", api.BusinessUnitMiddleware(db)(bankbalances.BulkRequestDeleteBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/all", api.BusinessUnitMiddleware(db)(bankbalances.GetBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/upload", api.BusinessUnitMiddleware(db)(bankbalances.UploadBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/update", api.BusinessUnitMiddleware(db)(bankbalances.UpdateBankBalance(pgxPool)))

	mux.HandleFunc("/cash/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Cash Service is active"))
	})
	log.Println("Cash Service started on :6143")
	err = http.ListenAndServe(":6143", mux)
	if err != nil {
		log.Fatalf("Cash Service failed: %v", err)
	}
}
