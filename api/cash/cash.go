package cash

import (
	// "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/bankbalances"
	"CimplrCorpSaas/api/cash/bankstatement"
	fundavailibilty "CimplrCorpSaas/api/cash/fundavailibilty"
	"CimplrCorpSaas/api/cash/fundplanning"
	"CimplrCorpSaas/api/cash/limit"
	"CimplrCorpSaas/api/cash/payablerecievable"
	"CimplrCorpSaas/api/cash/projection"
	sweepconfig "CimplrCorpSaas/api/cash/sweepConfig"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/api/travel"
	"CimplrCorpSaas/internal/telemetry"
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var cashServer *http.Server
var cashServerStop context.CancelFunc
var cashTracerShutdown func(context.Context) error

func StartCashService(pgxPool *pgxpool.Pool, port string) {
	mux := http.NewServeMux()
	tracerProvider, tracerShutdown, err := telemetry.InitTracerProvider("cash")
	if err != nil {
		log.Fatalf("failed to initialize OpenTelemetry tracer for cash service: %v", err)
	}
	cashTracerShutdown = tracerShutdown
	mux.Handle("/cash/upload-bank-statement", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.UploadBankStatementV2Handler(pgxPool)))
	mux.Handle("/cash/upload-bank-statement-zip", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.UploadZippedBankStatementsHandler(pgxPool)))

	// Preview and categorization endpoints (NO DB insertion, fast preview with categorization)
	mux.Handle("/cash/preview-categorize", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.PreviewBankStatementHandler(pgxPool)))

	// Load uncategorized transactions with pagination
	mux.Handle("/cash/uncategorized-transactions", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetUncategorizedTransactionsHandler(pgxPool)))

	// New streaming preview and management endpoints
	mux.Handle("/cash/preview", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.UploadBankStatementV3Handler(pgxPool)))
	mux.Handle("/cash/recalculate", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.RecalculateHandler(pgxPool)))
	mux.Handle("/cash/commit", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CommitHandler(pgxPool)))
	mux.Handle("/cash/get-pdf", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetPDFMetadataHandler(pgxPool)))
	mux.Handle("/cash/download-pdf", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.DownloadPDFHandler(pgxPool)))
	// Category Master APIs
	mux.Handle("/cash/category/create", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateTransactionCategoryHandler(pgxPool)))
	mux.Handle("/cash/category/list", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.ListTransactionCategoriesHandler(pgxPool)))
	mux.Handle("/cash/category/user-list", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.ListCategoriesForUserHandler(pgxPool)))
	mux.Handle("/cash/category/scope/create", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateRuleScopeHandler(pgxPool)))
	mux.Handle("/cash/category/rule/create", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateCategoryRuleHandler(pgxPool)))
	mux.Handle("/cash/category/rule-component/create", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateCategoryRuleComponentHandler(pgxPool)))
	mux.Handle("/cash/category/rule-master", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateCategoryRuleMasterHandler(pgxPool)))
	mux.Handle("/cash/category/delete", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.DeleteMultipleTransactionCategoriesHandler(pgxPool)))
	mux.Handle("/cash/transactions/map-category", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.MapTransactionsToCategoryHandler(pgxPool)))
	mux.Handle("/cash/transactions/categorize-uncategorized", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CategorizeUncategorizedTransactionsHandler(pgxPool)))
	mux.Handle("/cash/transactions/recompute-uncategorized", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.RecomputeUncategorizedTransactionsHandler(pgxPool)))
	mux.Handle("/cash/transactions/auto-categorize-trigger", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.ManualCategorizationTriggerHandler(pgxPool)))
	// V2 Bank Statement APIs
	mux.Handle("/cash/bank-statements/v2/get", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetAllBankStatementsHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/transactions", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetBankStatementTransactionsHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/download", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetBankStatementDownloadURLHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetBankStatementBulkDownloadURLHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/recompute-kpis", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.RecomputeBankStatementSummaryHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/approve", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.ApproveBankStatementHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/reject", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.RejectBankStatementHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/delete", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.DeleteBankStatementHandler(pgxPool)))
	mux.Handle("/cash/upload-payrec", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.UploadPayRec(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/transactions/misclassify", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.MarkBankStatementTransactionsMisclassifiedHandler(pgxPool)))
	// mux.Handle("/cash/bank-statements/all", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.BulkApproveBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.BulkRejectBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.BulkDeleteBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/create", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.CreateBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/update", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.UpdateBankStatement(pgxPool)))
	mux.Handle("/cash/bank-statements/all", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.GetAllBankStatements(pgxPool)))

	// Unified bulk endpoints for transactions (payables & receivables)
	mux.Handle("/cash/transactions/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.BulkRequestDeleteTransactions(pgxPool)))
	mux.Handle("/cash/transactions/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.BulkRejectTransactions(pgxPool)))
	mux.Handle("/cash/transactions/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.BulkApproveTransactions(pgxPool)))
	mux.Handle("/cash/transactions/create", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.BulkCreateTransactions(pgxPool)))
	mux.Handle("/cash/transactions/update", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.UpdateTransaction(pgxPool)))
	mux.Handle("/cash/transactions/upload-payrec-batch", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.BatchUploadTransactionsV2(pgxPool))) //twotwo
	mux.Handle("/cash/transactions/download", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.GetTransactionDownloadURL(pgxPool)))
	mux.Handle("/cash/transactions/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.GetTransactionBulkDownloadURL(pgxPool)))
	mux.Handle("/cash/transactions/all", middlewares.PreValidationMiddleware(pgxPool)(payablerecievable.GetAllPayableReceivable(pgxPool)))

	//fundplanning
	mux.Handle("/cash/fund-planning", middlewares.PreValidationMiddleware(pgxPool)(fundplanning.GetFundPlanningEnhanced(pgxPool)))
	mux.Handle("/cash/fund-planning/create", middlewares.PreValidationMiddleware(pgxPool)(fundplanning.CreateFundPlan(pgxPool)))
	mux.Handle("/cash/fund-planning/summary", middlewares.PreValidationMiddleware(pgxPool)(fundplanning.GetFundPlanSummary(pgxPool)))
	mux.Handle("/cash/fund-planning/details", middlewares.PreValidationMiddleware(pgxPool)(fundplanning.GetFundPlanDetails(pgxPool)))
	mux.Handle("/cash/fund-planning/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(fundplanning.BulkApproveFundPlans(pgxPool)))
	mux.Handle("/cash/fund-planning/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(fundplanning.BulkRejectFundPlans(pgxPool)))
	mux.Handle("/cash/fund-planning/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(fundplanning.BulkRequestDeleteFundPlans(pgxPool)))
	mux.Handle("/cash/fund-planning/bank-accounts", middlewares.PreValidationMiddleware(pgxPool)(fundplanning.GetApprovedBankAccountsForFundPlanning(pgxPool)))

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
	mux.Handle("/cash/sweep-config-v2/approved", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.GetApprovedActiveSweepConfigurationsEnhanced(pgxPool)))
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
	// Super admin bulk trigger: Creates sweep config (if needed) + initiation with auto-approval, bypasses approval workflow for urgent CFO actions
	mux.Handle("/cash/sweep-execution-v2/bulk-manual", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.BulkManualTriggerSweepV2WithAutoApproval(pgxPool)))

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
	mux.Handle("/cash/cashflow-projection/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(projection.DeleteCashFlowProposal(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(projection.BulkRejectCashFlowProposalActions(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(projection.BulkApproveCashFlowProposalActions(pgxPool)))

	mux.Handle("/cash/cashflow-projection/make", middlewares.PreValidationMiddleware(pgxPool)(projection.AbsorbFlattenedProjections(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-projection", middlewares.PreValidationMiddleware(pgxPool)(projection.GetProposalVersion(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-header", middlewares.PreValidationMiddleware(pgxPool)(projection.GetProjectionsSummary(pgxPool)))
	mux.Handle("/cash/cashflow-projection/download", middlewares.PreValidationMiddleware(pgxPool)(projection.GetProjectionDownloadURL(pgxPool)))
	mux.Handle("/cash/cashflow-projection/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(projection.GetProjectionBulkDownloadURL(pgxPool)))
	mux.Handle("/cash/cashflow-projection/update", middlewares.PreValidationMiddleware(pgxPool)(projection.UpdateCashFlowProposal(pgxPool)))

	mux.Handle("/cash/cashflow-projection/upload", middlewares.PreValidationMiddleware(pgxPool)(projection.UploadCashflowProposalSimple(pgxPool)))

	// Cash flow projection routes (V2 - Auto-calculated monthly projections)
	mux.Handle("/cash/projection/v2/create", middlewares.PreValidationMiddleware(pgxPool)(projection.CreateCashFlowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/update", middlewares.PreValidationMiddleware(pgxPool)(projection.UpdateCashFlowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/detail", middlewares.PreValidationMiddleware(pgxPool)(projection.GetProposalDetailV2(pgxPool)))
	mux.Handle("/cash/projection/v2/list", middlewares.PreValidationMiddleware(pgxPool)(projection.ListProposalsV2(pgxPool)))
	mux.Handle("/cash/projection/v2/delete", middlewares.PreValidationMiddleware(pgxPool)(projection.DeleteCashFlowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/approve", middlewares.PreValidationMiddleware(pgxPool)(projection.BulkApproveCashFlowProposalActionsV2(pgxPool)))
	mux.Handle("/cash/projection/v2/reject", middlewares.PreValidationMiddleware(pgxPool)(projection.BulkRejectCashFlowProposalActionsV2(pgxPool)))
	mux.Handle("/cash/projection/v2/upload", middlewares.PreValidationMiddleware(pgxPool)(projection.UploadCashflowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/download", middlewares.PreValidationMiddleware(pgxPool)(projection.GetProjectionDownloadURLV2(pgxPool)))
	mux.Handle("/cash/projection/v2/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(projection.GetProjectionBulkDownloadURLV2(pgxPool)))

	//bank balance
	mux.Handle("/cash/bank-balances/create", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.CreateBankBalance(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.BulkApproveBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.BulkRejectBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.BulkRequestDeleteBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/all", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.GetBankBalances(pgxPool)))
	// mux.Handle("/cash/bank-balances/upload",(bankbalances.UploadBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/upload", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.UploadBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/download", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.GetBankBalanceDownloadURL(pgxPool)))
	mux.Handle("/cash/bank-balances/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.GetBankBalanceBulkDownloadURL(pgxPool)))
	mux.Handle("/cash/bank-balances/update", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.UpdateBankBalance(pgxPool)))

	// Fund Availability - Combined Actuals & Projections
	mux.Handle("/cash/fund-availability/combined", middlewares.PreValidationMiddleware(pgxPool)(fundavailibilty.GetFundAvailability(pgxPool)))

	// Bank Limit Management - Limit sanctioning and tracking
	limit.RegisterLimitRoutes(mux, pgxPool)

	// Travel package endpoints
	mux.Handle("/cash/package/create", middlewares.PreValidationMiddleware(pgxPool)(travel.CreatePackageHandler(pgxPool)))
	mux.Handle("/cash/package", middlewares.PreValidationMiddleware(pgxPool)(travel.GetPackageHandler(pgxPool)))
	mux.Handle("/cash/package/delete", middlewares.PreValidationMiddleware(pgxPool)(travel.DeletePackageHandler(pgxPool)))

	mux.HandleFunc("/cash/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Cash Service is active"))
	})
	// closing pool fix
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      otelhttp.NewHandler(mux, "cash", otelhttp.WithTracerProvider(tracerProvider)),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	cashServer = server

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	cashServerStop = stop
	defer stop()

	go func() {
		log.Printf("Cash Service started on :%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Cash Service failed: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil && err != http.ErrServerClosed {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Cash Service stopped gracefully")
}

func shutdownCashService() error {
	if cashServerStop != nil {
		cashServerStop()
	}
	if cashServer == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := cashServer.Shutdown(ctx); err != nil && err != http.ErrServerClosed {
		return err
	}
	if cashTracerShutdown != nil {
		if err := cashTracerShutdown(ctx); err != nil {
			return err
		}
	}
	return nil
}
