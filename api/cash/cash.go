package cash

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/bankbalances"
	"CimplrCorpSaas/api/cash/bankstatement"
	fundavailibilty "CimplrCorpSaas/api/cash/fundavailibilty"
	"CimplrCorpSaas/api/cash/fundplanning"
	"CimplrCorpSaas/api/cash/payablerecievable"
	"CimplrCorpSaas/api/cash/projection"
	sweepconfig "CimplrCorpSaas/api/cash/sweepConfig"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/api/travel"
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
	mux.Handle("/cash/upload-bank-statement", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.UploadBankStatementV2Handler(db)))
	mux.Handle("/cash/upload-bank-statement-zip", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.UploadZippedBankStatementsHandler(db)))

	// New streaming preview and management endpoints
	mux.Handle("/cash/preview", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.UploadBankStatementV3Handler(db)))
	mux.Handle("/cash/recalculate", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.RecalculateHandler(db)))
	mux.Handle("/cash/commit", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CommitHandler(db)))
	mux.Handle("/cash/get-pdf", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetPDFMetadataHandler(db)))
	mux.Handle("/cash/download-pdf", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.DownloadPDFHandler(db)))
	// Category Master APIs
	mux.Handle("/cash/category/create", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateTransactionCategoryHandler(db)))
	mux.Handle("/cash/category/list", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.ListTransactionCategoriesHandler(db)))
	mux.Handle("/cash/category/user-list", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.ListCategoriesForUserHandler(db)))
	mux.Handle("/cash/category/scope/create", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateRuleScopeHandler(db)))
	mux.Handle("/cash/category/rule/create", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateCategoryRuleHandler(db)))
	mux.Handle("/cash/category/rule-component/create", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateCategoryRuleComponentHandler(db)))
	mux.Handle("/cash/category/delete", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.DeleteMultipleTransactionCategoriesHandler(db)))
	mux.Handle("/cash/transactions/map-category", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.MapTransactionsToCategoryHandler(db)))
	mux.Handle("/cash/transactions/categorize-uncategorized", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CategorizeUncategorizedTransactionsHandler(db)))
	mux.Handle("/cash/transactions/recompute-uncategorized", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.RecomputeUncategorizedTransactionsHandler(db)))
	mux.Handle("/cash/transactions/auto-categorize-trigger", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.ManualCategorizationTriggerHandler(pgxPool)))
	// V2 Bank Statement APIs
	mux.Handle("/cash/bank-statements/v2/get", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetAllBankStatementsHandler(db)))
	mux.Handle("/cash/bank-statements/v2/transactions", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetBankStatementTransactionsHandler(db)))
	mux.Handle("/cash/bank-statements/v2/recompute-kpis", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.RecomputeBankStatementSummaryHandler(db)))
	mux.Handle("/cash/bank-statements/v2/approve", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.ApproveBankStatementHandler(db)))
	mux.Handle("/cash/bank-statements/v2/reject", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.RejectBankStatementHandler(db)))
	mux.Handle("/cash/bank-statements/v2/delete", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.DeleteBankStatementHandler(db)))
	mux.Handle("/cash/upload-payrec", api.BusinessUnitMiddleware(db)(payablerecievable.UploadPayRec(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/transactions/misclassify", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.MarkBankStatementTransactionsMisclassifiedHandler(db)))
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

	// Sweep configuration routes (load entity, bank and approved accounts into context)
	mux.Handle("/cash/sweep-config/create", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.CreateSweepConfiguration(pgxPool)))
	mux.Handle("/cash/sweep-config/update", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.UpdateSweepConfiguration(pgxPool)))
	mux.Handle("/cash/sweep-config/all", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkApproveSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkRejectSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config/request-delete", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkRequestDeleteSweepConfigurations(pgxPool)))

	// Sweep execution and monitoring routes (require prevalidation: entities, banks, accounts, currencies)
	mux.Handle("/cash/sweep-config/execution-logs", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepExecutionLogs(pgxPool)))
	mux.Handle("/cash/sweep-config/statistics", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepStatistics(pgxPool)))
	mux.Handle("/cash/sweep-config/manual-trigger", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.ManualTriggerSweep(pgxPool)))

	// Sweep V2 configuration routes (new table structure with source/target accounts, sweep types)
	mux.Handle("/cash/sweep-config-v2/create", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.CreateSweepConfigurationV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkCreateSweepConfigurationV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/update", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.UpdateSweepConfigurationV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/all", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepConfigurationsV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/approved-active", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetApprovedActiveSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/approved-with-potentials", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetApprovedActiveSweepConfigurationsEnhanced(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkApproveSweepConfigurationsV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkRejectSweepConfigurationsV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkRequestDeleteSweepConfigurationsV2(pgxPool)))

	// Sweep V2 initiation routes (manual/scheduled initiation with overrides)
	mux.Handle("/cash/sweep-initiation/create", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.CreateSweepInitiation(pgxPool)))
	mux.Handle("/cash/sweep-initiation/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkCreateSweepInitiation(pgxPool)))
	mux.Handle("/cash/sweep-initiation/update", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.UpdateSweepInitiation(pgxPool)))
	mux.Handle("/cash/sweep-initiation/all", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepInitiations(pgxPool)))
	mux.Handle("/cash/sweep-initiation/with-details", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepInitiationsWithJoinedData(pgxPool)))
	mux.Handle("/cash/sweep-initiation/approved-active", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetApprovedActiveSweepInitiations(pgxPool)))
	mux.Handle("/cash/sweep-initiation/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkApproveSweepInitiations(pgxPool)))
	mux.Handle("/cash/sweep-initiation/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkRejectSweepInitiations(pgxPool)))
	mux.Handle("/cash/sweep-initiation/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkDeleteSweepInitiations(pgxPool)))

	// Sweep V2 execution routes (logs, statistics, manual trigger)
	mux.Handle("/cash/sweep-execution-v2/logs", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepExecutionLogsV2(pgxPool)))
	mux.Handle("/cash/sweep-execution-v2/all-logs", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetAllSweepExecutionLogsV2(pgxPool)))
	mux.Handle("/cash/sweep-execution-v2/statistics", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepStatisticsV2(pgxPool)))
	// DEPRECATED: /cash/sweep-execution-v2/manual-trigger-direct - ALL sweeps now use initiation workflow
	// Use /cash/sweep-execution-v2/manual-trigger instead
	// mux.Handle("/cash/sweep-execution-v2/manual-trigger-direct", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.ManualTriggerSweepV2Direct(pgxPool)))
	mux.Handle("/cash/sweep-execution-v2/manual-trigger", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.ManualTriggerSweepV2(pgxPool)))

	// Sweep V2 Simulation & Analytics (comprehensive pre-execution analysis, CFO decision support)
	mux.Handle("/cash/sweep/simulate", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.SimulateSweepExecution(pgxPool)))
	mux.Handle("/cash/sweep/balance-snapshot", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetBalanceSnapshot(pgxPool)))
	mux.Handle("/cash/sweep/validate", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.ValidateSweepConfiguration(pgxPool)))
	mux.Handle("/cash/sweep/analytics", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepAnalytics(pgxPool)))
	mux.Handle("/cash/sweep/suggestions", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepSuggestions(pgxPool)))
	mux.Handle("/cash/sweep/execution-graph", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetSweepExecutionGraph(pgxPool)))

	// Treasury KPI Dashboard
	mux.Handle("/cash/sweep/kpi", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.GetTreasuryKPI(pgxPool)))

	// Cash flow projection routes (V1)
	mux.Handle("/cash/cashflow-projection/bulk-delete", api.BusinessUnitMiddleware(db)(projection.DeleteCashFlowProposal(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-reject", api.BusinessUnitMiddleware(db)(projection.BulkRejectCashFlowProposalActions(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-approve", api.BusinessUnitMiddleware(db)(projection.BulkApproveCashFlowProposalActions(pgxPool)))

	mux.Handle("/cash/cashflow-projection/make", api.BusinessUnitMiddleware(db)(projection.AbsorbFlattenedProjections(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-projection", api.BusinessUnitMiddleware(db)(projection.GetProposalVersion(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-header", api.BusinessUnitMiddleware(db)(projection.GetProjectionsSummary(pgxPool)))
	mux.Handle("/cash/cashflow-projection/update", api.BusinessUnitMiddleware(db)(projection.UpdateCashFlowProposal(pgxPool)))

	mux.Handle("/cash/cashflow-projection/upload", api.BusinessUnitMiddleware(db)(projection.UploadCashflowProposalSimple(pgxPool)))

	// Cash flow projection routes (V2 - Auto-calculated monthly projections)
	mux.Handle("/cash/projection/v2/create", middlewares.PreValidationMiddleware(pgxPool)(projection.CreateCashFlowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/update", middlewares.PreValidationMiddleware(pgxPool)(projection.UpdateCashFlowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/detail", middlewares.PreValidationMiddleware(pgxPool)(projection.GetProposalDetailV2(pgxPool)))
	mux.Handle("/cash/projection/v2/list", middlewares.PreValidationMiddleware(pgxPool)(projection.ListProposalsV2(pgxPool)))
	mux.Handle("/cash/projection/v2/delete", middlewares.PreValidationMiddleware(pgxPool)(projection.DeleteCashFlowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/approve", middlewares.PreValidationMiddleware(pgxPool)(projection.BulkApproveCashFlowProposalActionsV2(pgxPool)))
	mux.Handle("/cash/projection/v2/reject", middlewares.PreValidationMiddleware(pgxPool)(projection.BulkRejectCashFlowProposalActionsV2(pgxPool)))
	mux.Handle("/cash/projection/v2/upload", middlewares.PreValidationMiddleware(pgxPool)(projection.UploadCashflowProposalV2(pgxPool)))

	//bank balance
	mux.Handle("/cash/bank-balances/create", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.CreateBankBalance(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.BulkApproveBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.BulkRejectBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.BulkRequestDeleteBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/all", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.GetBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/upload", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.UploadBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/update", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.UpdateBankBalance(pgxPool)))

	// Fund Availability - Combined Actuals & Projections
	mux.Handle("/cash/fund-availability/combined", middlewares.PreValidationMiddleware(pgxPool)(fundavailibilty.GetFundAvailability(pgxPool)))

	// Travel package endpoints
	mux.Handle("/cash/package/create", (travel.CreatePackageHandler(db)))
	mux.Handle("/cash/package", (travel.GetPackageHandler(db)))
	mux.Handle("/cash/package/delete", (travel.DeletePackageHandler(db)))

	mux.HandleFunc("/cash/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Cash Service is active"))
	})
	log.Println("Cash Service started on :6143")
	err = http.ListenAndServe(":6143", mux)
	if err != nil {
		log.Fatalf("Cash Service failed: %v", err)
	}
}
