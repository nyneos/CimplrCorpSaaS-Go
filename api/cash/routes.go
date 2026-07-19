package cash

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/bankbalances"
	"CimplrCorpSaas/api/cash/bankstatement"
	fundavailibilty "CimplrCorpSaas/api/cash/fundavailibilty"
	"CimplrCorpSaas/api/cash/fundplanning"
	"CimplrCorpSaas/api/cash/limit"
	"CimplrCorpSaas/api/cash/payablerecievable"
	"CimplrCorpSaas/api/cash/projection"
	sweepconfig "CimplrCorpSaas/api/cash/sweepConfig"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/observability"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterCashRoutes wires every /cash/* route onto mux. Route registration
// only — handler logic lives in the bankbalances/bankstatement/fundavailibilty/
// fundplanning/limit/payablerecievable/projection/sweepConfig packages.
func RegisterCashRoutes(mux *http.ServeMux, serviceName string, pgxPool *pgxpool.Pool) {
	cashChain := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(
			middlewares.GlobalIndependentMiddleware(pgxPool)(
				middlewares.GlobalDependentMiddleware(pgxPool)(
					middlewares.CashMiddleware(pgxPool)(h),
				),
			),
		)
	}
	mux.Handle("/cash/upload-bank-statement", cashChain(bankstatement.UploadBankStatementV2Handler(pgxPool)))
	mux.Handle("/cash/upload-bank-statement-zip", cashChain(bankstatement.UploadZippedBankStatementsHandler(pgxPool)))
	// LLM extraction test — no DB, no auth required on data, safe to call manually
	mux.Handle("/cash/bank-statement/llm-extract", cashChain(bankstatement.LLMExtractTestHandler()))

	// Preview and categorization endpoints (NO DB insertion, fast preview with categorization)
	mux.Handle("/cash/preview-categorize", cashChain(bankstatement.PreviewBankStatementHandler(pgxPool)))

	// Load uncategorized transactions with pagination
	mux.Handle("/cash/uncategorized-transactions", cashChain(bankstatement.GetUncategorizedTransactionsHandler(pgxPool)))

	// New streaming preview and management endpoints
	mux.Handle("/cash/preview", cashChain(bankstatement.UploadBankStatementV3Handler(pgxPool)))
	mux.Handle("/cash/recalculate", cashChain(bankstatement.RecalculateHandler(pgxPool)))
	mux.Handle("/cash/commit", cashChain(bankstatement.CommitHandler(pgxPool)))
	mux.Handle("/cash/get-pdf", cashChain(bankstatement.GetPDFMetadataHandler(pgxPool)))
	mux.Handle("/cash/download-pdf", cashChain(bankstatement.DownloadPDFHandler(pgxPool)))
	// PDF staging endpoints (multi-PDF ZIP flow: batch → review → recalculate → commit)
	mux.Handle("/cash/staging/batch/get", cashChain(bankstatement.GetStagingBatchHandler(pgxPool)))
	mux.Handle("/cash/staging/batch/results", cashChain(bankstatement.GetStagingBatchResultsHandler(pgxPool)))
	mux.Handle("/cash/staging/statement/get", cashChain(bankstatement.GetStagingStatementHandler(pgxPool)))
	mux.Handle("/cash/staging/statement/download", cashChain(bankstatement.GetStagingFileDownloadURLHandler(pgxPool)))
	mux.Handle("/cash/staging/statement/update", cashChain(bankstatement.UpdateStagingStatementHandler(pgxPool)))
	mux.Handle("/cash/staging/statement/delete", cashChain(bankstatement.DeleteStagingStatementHandler(pgxPool)))
	mux.Handle("/cash/staging/batch/delete", cashChain(bankstatement.DeleteStagingBatchHandler(pgxPool)))
	mux.Handle("/cash/staging/list", cashChain(bankstatement.ListStagingByUserHandler(pgxPool)))
	// Category Master APIs
	mux.Handle("/cash/category/create", cashChain(bankstatement.CreateTransactionCategoryHandler(pgxPool)))
	mux.Handle("/cash/category/list", cashChain(bankstatement.ListTransactionCategoriesHandler(pgxPool)))
	mux.Handle("/cash/category/user-list", cashChain(bankstatement.ListCategoriesForUserHandler(pgxPool)))
	mux.Handle("/cash/category/scope/create", cashChain(bankstatement.CreateRuleScopeHandler(pgxPool)))
	mux.Handle("/cash/category/rule/create", cashChain(bankstatement.CreateCategoryRuleHandler(pgxPool)))
	mux.Handle("/cash/category/rule-component/create", cashChain(bankstatement.CreateCategoryRuleComponentHandler(pgxPool)))
	mux.Handle("/cash/category/rule-master", cashChain(bankstatement.CreateCategoryRuleMasterHandler(pgxPool)))
	mux.Handle("/cash/rule-master/additional-files/list", cashChain(bankstatement.ListRuleMasterAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/rule-master/additional-files/upload", cashChain(bankstatement.UploadRuleMasterAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/rule-master/additional-files/download", cashChain(bankstatement.DownloadRuleMasterAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/rule-master/additional-files/download-bulk", cashChain(bankstatement.DownloadSelectedRuleMasterAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/rule-master/package-zip", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.DownloadRuleMasterPackageZipHandler(pgxPool)))
	mux.Handle("/cash/rule-master/additional-files/delete", cashChain(bankstatement.DeleteRuleMasterAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/rule-master/additional-files/delete/approve", cashChain(bankstatement.ApproveRuleMasterAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/rule-master/additional-files/delete/reject", cashChain(bankstatement.RejectRuleMasterAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/rule-master/additional-files/audit", cashChain(bankstatement.AuditRuleMasterAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/category/delete", cashChain(bankstatement.DeleteMultipleTransactionCategoriesHandler(pgxPool)))
	mux.Handle("/cash/transactions/map-category", cashChain(bankstatement.MapTransactionsToCategoryHandler(pgxPool)))
	mux.Handle("/cash/transactions/categorize-uncategorized", cashChain(bankstatement.CategorizeUncategorizedTransactionsHandler(pgxPool)))
	mux.Handle("/cash/transactions/recompute-uncategorized", cashChain(bankstatement.RecomputeUncategorizedTransactionsHandler(pgxPool)))
	mux.Handle("/cash/transactions/auto-categorize-trigger", cashChain(bankstatement.ManualCategorizationTriggerHandler(pgxPool)))
	// Smart Categorization Engine
	mux.Handle("/cash/smart-cat/status", cashChain(bankstatement.SmartCatStatusHandler(pgxPool)))
	mux.Handle("/cash/smart-cat/review-queue", cashChain(bankstatement.GetReviewQueueHandler(pgxPool)))
	mux.Handle("/cash/smart-cat/review-action", cashChain(bankstatement.ReviewActionHandler(pgxPool)))
	mux.Handle("/cash/smart-cat/correction", cashChain(bankstatement.SaveCorrectionHandler(pgxPool)))
	mux.Handle("/cash/smart-cat/gl-mapping/create", cashChain(bankstatement.CreateGLMappingHandler(pgxPool)))
	// AI Rule Manager (list/approve/reject AI_SUGGESTED rules)
	mux.Handle("/cash/smart-cat/rules/ai-suggested", cashChain(bankstatement.ListAISuggestedRulesHandler(pgxPool)))
	mux.Handle("/cash/smart-cat/rule/approve", cashChain(bankstatement.ApproveAIRuleHandler(pgxPool)))
	mux.Handle("/cash/smart-cat/rule/reject", cashChain(bankstatement.RejectAIRuleHandler(pgxPool)))
	// AI Bulk Suggest — batch-classify all PENDING UNALLOCATED review queue items (async)
	mux.Handle("/cash/smart-cat/ai-bulk-suggest", cashChain(bankstatement.AiBulkSuggestHandler(pgxPool)))
	mux.Handle("/cash/smart-cat/ai-bulk-suggest/status", cashChain(bankstatement.AiBulkSuggestStatusHandler(pgxPool)))
	// Bulk Correct — apply a category to N transactions at once
	mux.Handle("/cash/smart-cat/bulk-correct", cashChain(bankstatement.BulkCorrectHandler(pgxPool)))
	// V2 Bank Statement APIs
	mux.Handle("/cash/bank-statements/v2/get", cashChain(bankstatement.GetAllBankStatementsHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/transactions", cashChain(bankstatement.GetBankStatementTransactionsHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/download", cashChain(bankstatement.GetBankStatementDownloadURLHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/download-bulk", cashChain(bankstatement.GetBankStatementBulkDownloadURLHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/package-zip", middlewares.PreValidationMiddleware(pgxPool)(bankstatement.DownloadBankStatementPackageZipHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/additional-files/list", cashChain(bankstatement.ListAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/additional-files/upload", cashChain(bankstatement.UploadAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/additional-files/download", cashChain(bankstatement.DownloadAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/additional-files/download-bulk", cashChain(bankstatement.DownloadSelectedAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/additional-files/delete", cashChain(bankstatement.DeleteAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/additional-files/delete/approve", cashChain(bankstatement.ApproveAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/additional-files/delete/reject", cashChain(bankstatement.RejectAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/additional-files/audit", cashChain(bankstatement.AuditAdditionalFileHandler(pgxPool)))
	// Audit bank statement
	mux.Handle("/cash/bank-statements/v2/audit", cashChain(bankstatement.GetBankStatementAuditHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/audit/downloads", cashChain(bankstatement.GetBankStatementDownloadAuditHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/audit/balance-impact", cashChain(bankstatement.GetBankStatementBalanceImpactAuditHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/audit/transactions", cashChain(bankstatement.GetBankStatementTransactionAuditHandler(pgxPool)))
	// -----------

	mux.Handle("/cash/bank-statements/v2/recompute-kpis", cashChain(bankstatement.RecomputeBankStatementSummaryHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/approve", cashChain(bankstatement.ApproveBankStatementHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/reject", cashChain(bankstatement.RejectBankStatementHandler(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/delete", cashChain(bankstatement.DeleteBankStatementHandler(pgxPool)))
	mux.Handle("/cash/upload-payrec", cashChain(payablerecievable.UploadPayRec(pgxPool)))
	mux.Handle("/cash/bank-statements/v2/transactions/misclassify", cashChain(bankstatement.MarkBankStatementTransactionsMisclassifiedHandler(pgxPool)))
	// mux.Handle("/cash/bank-statements/all", cashChain(bankstatement.GetBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-approve", cashChain(bankstatement.BulkApproveBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-reject", cashChain(bankstatement.BulkRejectBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/bulk-delete", cashChain(bankstatement.BulkDeleteBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/create", cashChain(bankstatement.CreateBankStatements(pgxPool)))
	mux.Handle("/cash/bank-statements/update", cashChain(bankstatement.UpdateBankStatement(pgxPool)))
	mux.Handle("/cash/bank-statements/all", cashChain(bankstatement.GetAllBankStatements(pgxPool)))

	// Unified bulk endpoints for transactions (payables & receivables)
	mux.Handle("/cash/transactions/bulk-delete", cashChain(payablerecievable.BulkRequestDeleteTransactions(pgxPool)))
	mux.Handle("/cash/transactions/bulk-reject", cashChain(payablerecievable.BulkRejectTransactions(pgxPool)))
	mux.Handle("/cash/transactions/bulk-approve", cashChain(payablerecievable.BulkApproveTransactions(pgxPool)))
	mux.Handle("/cash/transactions/create", cashChain(payablerecievable.BulkCreateTransactions(pgxPool)))
	mux.Handle("/cash/transactions/update", cashChain(payablerecievable.UpdateTransaction(pgxPool)))
	mux.Handle("/cash/transactions/upload-payrec-batch", cashChain(payablerecievable.BatchUploadTransactionsV2(pgxPool))) //twotwo
	mux.Handle("/cash/transactions/download", cashChain(payablerecievable.GetTransactionDownloadURL(pgxPool)))
	mux.Handle("/cash/transactions/download-bulk", cashChain(payablerecievable.GetTransactionBulkDownloadURL(pgxPool)))
	mux.Handle("/cash/transactions/payables/package-zip", api.BusinessUnitMiddleware(pgxPool)(payablerecievable.DownloadPayablePackageZipHandler(pgxPool)))
	mux.Handle("/cash/transactions/payables/additional-files/list", cashChain(payablerecievable.ListPayableAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/transactions/payables/additional-files/upload", cashChain(payablerecievable.UploadPayableAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/transactions/payables/additional-files/download", cashChain(payablerecievable.DownloadPayableAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/transactions/payables/additional-files/download-bulk", cashChain(payablerecievable.DownloadSelectedPayableAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/transactions/payables/additional-files/delete", cashChain(payablerecievable.DeletePayableAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/transactions/payables/additional-files/delete/approve", cashChain(payablerecievable.ApprovePayableAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/transactions/payables/additional-files/delete/reject", cashChain(payablerecievable.RejectPayableAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/transactions/payables/additional-files/audit", cashChain(payablerecievable.AuditPayableAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/transactions/receivables/additional-files/list", cashChain(payablerecievable.ListReceivableAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/transactions/receivables/package-zip", api.BusinessUnitMiddleware(pgxPool)(payablerecievable.DownloadReceivablePackageZipHandler(pgxPool)))
	mux.Handle("/cash/transactions/receivables/additional-files/upload", cashChain(payablerecievable.UploadReceivableAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/transactions/receivables/additional-files/download", cashChain(payablerecievable.DownloadReceivableAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/transactions/receivables/additional-files/download-bulk", cashChain(payablerecievable.DownloadSelectedReceivableAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/transactions/receivables/additional-files/delete", cashChain(payablerecievable.DeleteReceivableAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/transactions/receivables/additional-files/delete/approve", cashChain(payablerecievable.ApproveReceivableAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/transactions/receivables/additional-files/delete/reject", cashChain(payablerecievable.RejectReceivableAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/transactions/receivables/additional-files/audit", cashChain(payablerecievable.AuditReceivableAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/transactions/audit", cashChain(payablerecievable.GetTransactionAuditHandler(pgxPool)))
	mux.Handle("/cash/transactions/all", cashChain(payablerecievable.GetAllPayableReceivable(pgxPool)))

	//fundplanning
	mux.Handle("/cash/fund-planning", cashChain(fundplanning.GetFundPlanningEnhanced(pgxPool)))
	mux.Handle("/cash/fund-planning/create", cashChain(fundplanning.CreateFundPlan(pgxPool)))
	mux.Handle("/cash/fund-planning/summary", cashChain(fundplanning.GetFundPlanSummary(pgxPool)))
	mux.Handle("/cash/fund-planning/details", cashChain(fundplanning.GetFundPlanDetails(pgxPool)))
	mux.Handle("/cash/fund-planning/bulk-approve", cashChain(fundplanning.BulkApproveFundPlans(pgxPool)))
	mux.Handle("/cash/fund-planning/bulk-reject", cashChain(fundplanning.BulkRejectFundPlans(pgxPool)))
	mux.Handle("/cash/fund-planning/bulk-delete", cashChain(fundplanning.BulkRequestDeleteFundPlans(pgxPool)))
	mux.Handle("/cash/fund-planning/bank-accounts", cashChain(fundplanning.GetApprovedBankAccountsForFundPlanning(pgxPool)))
	mux.Handle("/cash/fund-planning/additional-files/list", cashChain(fundplanning.ListAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/fund-planning/additional-files/upload", cashChain(fundplanning.UploadAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/fund-planning/additional-files/download", cashChain(fundplanning.DownloadAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/fund-planning/additional-files/download-bulk", cashChain(fundplanning.DownloadSelectedAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/fund-planning/package-zip", api.BusinessUnitMiddleware(pgxPool)(fundplanning.DownloadFundPlanPackageZipHandler(pgxPool)))
	mux.Handle("/cash/fund-planning/additional-files/delete", cashChain(fundplanning.DeleteAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/fund-planning/additional-files/delete/approve", cashChain(fundplanning.ApproveAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/fund-planning/additional-files/delete/reject", cashChain(fundplanning.RejectAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/fund-planning/additional-files/audit", cashChain(fundplanning.AuditAdditionalFileHandler(pgxPool)))

	// Sweep configuration routes (load entity, bank and approved accounts into context)
	mux.Handle("/cash/sweep-config/create", cashChain(sweepconfig.CreateSweepConfiguration(pgxPool)))
	mux.Handle("/cash/sweep-config/update", cashChain(sweepconfig.UpdateSweepConfiguration(pgxPool)))
	mux.Handle("/cash/sweep-config/all", cashChain(sweepconfig.GetSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config/bulk-approve", cashChain(sweepconfig.BulkApproveSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config/bulk-reject", cashChain(sweepconfig.BulkRejectSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config/request-delete", cashChain(sweepconfig.BulkRequestDeleteSweepConfigurations(pgxPool)))

	// Sweep execution and monitoring routes (require prevalidation: entities, banks, accounts, currencies)
	mux.Handle("/cash/sweep-config/execution-logs", cashChain(sweepconfig.GetSweepExecutionLogs(pgxPool)))
	mux.Handle("/cash/sweep-config/statistics", cashChain(sweepconfig.GetSweepStatistics(pgxPool)))
	mux.Handle("/cash/sweep-config/manual-trigger", cashChain(sweepconfig.ManualTriggerSweep(pgxPool)))

	// Sweep V2 configuration routes (new table structure with source/target accounts, sweep types)
	mux.Handle("/cash/sweep-config-v2/create", cashChain(sweepconfig.CreateSweepConfigurationV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/bulk-create", cashChain(sweepconfig.BulkCreateSweepConfigurationV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/update", cashChain(sweepconfig.UpdateSweepConfigurationV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/all", cashChain(sweepconfig.GetSweepConfigurationsV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/approved-active", cashChain(sweepconfig.GetApprovedActiveSweepConfigurations(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/approved", cashChain(sweepconfig.GetApprovedActiveSweepConfigurationsEnhanced(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/bulk-approve", cashChain(sweepconfig.BulkApproveSweepConfigurationsV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/bulk-reject", cashChain(sweepconfig.BulkRejectSweepConfigurationsV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/bulk-delete", cashChain(sweepconfig.BulkRequestDeleteSweepConfigurationsV2(pgxPool)))
	mux.Handle("/cash/sweep-config-v2/audit", cashChain(sweepconfig.GetSweepConfigAuditHandler(pgxPool)))
	mux.Handle("/cash/sweep-planning/additional-files/list", cashChain(sweepconfig.ListSweepPlanningAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/sweep-planning/additional-files/upload", cashChain(sweepconfig.UploadSweepPlanningAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/sweep-planning/additional-files/download", cashChain(sweepconfig.DownloadSweepPlanningAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/sweep-planning/additional-files/download-bulk", cashChain(sweepconfig.DownloadSelectedSweepPlanningAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/sweep-planning/package-zip", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.DownloadSweepPlanningPackageZipHandler(pgxPool)))
	mux.Handle("/cash/sweep-planning/additional-files/delete", cashChain(sweepconfig.DeleteSweepPlanningAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/sweep-planning/additional-files/delete/approve", cashChain(sweepconfig.ApproveSweepPlanningAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/sweep-planning/additional-files/delete/reject", cashChain(sweepconfig.RejectSweepPlanningAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/sweep-planning/additional-files/audit", cashChain(sweepconfig.AuditSweepPlanningAdditionalFileHandler(pgxPool)))

	// Sweep V2 initiation routes (manual/scheduled initiation with overrides)
	mux.Handle("/cash/sweep-initiation/create", cashChain(sweepconfig.CreateSweepInitiation(pgxPool)))
	mux.Handle("/cash/sweep-initiation/bulk-create", cashChain(sweepconfig.BulkCreateSweepInitiation(pgxPool)))
	mux.Handle("/cash/sweep-initiation/update", cashChain(sweepconfig.UpdateSweepInitiation(pgxPool)))
	mux.Handle("/cash/sweep-initiation/all", cashChain(sweepconfig.GetSweepInitiations(pgxPool)))
	mux.Handle("/cash/sweep-initiation/with-details", cashChain(sweepconfig.GetSweepInitiationsWithJoinedData(pgxPool)))
	mux.Handle("/cash/sweep-initiation/approved-active", cashChain(sweepconfig.GetApprovedActiveSweepInitiations(pgxPool)))
	mux.Handle("/cash/sweep-initiation/bulk-approve", cashChain(sweepconfig.BulkApproveSweepInitiations(pgxPool)))
	mux.Handle("/cash/sweep-initiation/bulk-reject", cashChain(sweepconfig.BulkRejectSweepInitiations(pgxPool)))
	mux.Handle("/cash/sweep-initiation/bulk-delete", cashChain(sweepconfig.BulkDeleteSweepInitiations(pgxPool)))
	mux.Handle("/cash/sweep-initiation/audit", cashChain(sweepconfig.GetSweepInitiationAuditHandler(pgxPool)))
	mux.Handle("/cash/sweep-initiation/additional-files/list", cashChain(sweepconfig.ListSweepInitiationAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/sweep-initiation/additional-files/upload", cashChain(sweepconfig.UploadSweepInitiationAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/sweep-initiation/additional-files/download", cashChain(sweepconfig.DownloadSweepInitiationAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/sweep-initiation/additional-files/download-bulk", cashChain(sweepconfig.DownloadSelectedSweepInitiationAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/sweep-initiation/package-zip", middlewares.PreValidationMiddleware(pgxPool)(sweepconfig.DownloadSweepInitiationPackageZipHandler(pgxPool)))
	mux.Handle("/cash/sweep-initiation/additional-files/delete", cashChain(sweepconfig.DeleteSweepInitiationAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/sweep-initiation/additional-files/delete/approve", cashChain(sweepconfig.ApproveSweepInitiationAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/sweep-initiation/additional-files/delete/reject", cashChain(sweepconfig.RejectSweepInitiationAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/sweep-initiation/additional-files/audit", cashChain(sweepconfig.AuditSweepInitiationAdditionalFileHandler(pgxPool)))

	// Sweep V2 execution routes (logs, statistics, manual trigger)
	mux.Handle("/cash/sweep-execution-v2/logs", cashChain(sweepconfig.GetSweepExecutionLogsV2(pgxPool)))
	mux.Handle("/cash/sweep-execution-v2/all-logs", cashChain(sweepconfig.GetAllSweepExecutionLogsV2(pgxPool)))
	mux.Handle("/cash/sweep-execution-v2/statistics", cashChain(sweepconfig.GetSweepStatisticsV2(pgxPool)))
	// DEPRECATED: /cash/sweep-execution-v2/manual-trigger-direct - ALL sweeps now use initiation workflow
	// Use /cash/sweep-execution-v2/manual-trigger instead
	// mux.Handle("/cash/sweep-execution-v2/manual-trigger-direct", cashChain(sweepconfig.ManualTriggerSweepV2Direct(pgxPool)))
	mux.Handle("/cash/sweep-execution-v2/manual-trigger", cashChain(sweepconfig.ManualTriggerSweepV2(pgxPool)))
	// Super admin bulk trigger: Creates sweep config (if needed) + initiation with auto-approval, bypasses approval workflow for urgent CFO actions
	mux.Handle("/cash/sweep-execution-v2/bulk-manual", cashChain(sweepconfig.BulkManualTriggerSweepV2WithAutoApproval(pgxPool)))

	// Sweep V2 Simulation & Analytics (comprehensive pre-execution analysis, CFO decision support)
	mux.Handle("/cash/sweep/simulate", cashChain(sweepconfig.SimulateSweepExecution(pgxPool)))
	mux.Handle("/cash/sweep/balance-snapshot", cashChain(sweepconfig.GetBalanceSnapshot(pgxPool)))
	mux.Handle("/cash/sweep/validate", cashChain(sweepconfig.ValidateSweepConfiguration(pgxPool)))
	mux.Handle("/cash/sweep/analytics", cashChain(sweepconfig.GetSweepAnalytics(pgxPool)))
	mux.Handle("/cash/sweep/suggestions", cashChain(sweepconfig.GetSweepSuggestions(pgxPool)))
	mux.Handle("/cash/sweep/execution-graph", cashChain(sweepconfig.GetSweepExecutionGraph(pgxPool)))

	// Treasury KPI Dashboard
	mux.Handle("/cash/sweep/kpi", cashChain(bankbalances.GetTreasuryKPI(pgxPool)))

	// Cash flow projection routes (V1)
	mux.Handle("/cash/cashflow-projection/bulk-delete", cashChain(projection.DeleteCashFlowProposal(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-reject", cashChain(projection.BulkRejectCashFlowProposalActions(pgxPool)))
	mux.Handle("/cash/cashflow-projection/bulk-approve", cashChain(projection.BulkApproveCashFlowProposalActions(pgxPool)))

	mux.Handle("/cash/cashflow-projection/make", cashChain(projection.AbsorbFlattenedProjections(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-projection", cashChain(projection.GetProposalVersion(pgxPool)))
	mux.Handle("/cash/cashflow-projection/get-header", cashChain(projection.GetProjectionsSummary(pgxPool)))
	mux.Handle("/cash/cashflow-projection/download", cashChain(projection.GetProjectionDownloadURL(pgxPool)))
	mux.Handle("/cash/cashflow-projection/download-bulk", cashChain(projection.GetProjectionBulkDownloadURL(pgxPool)))
	mux.Handle("/cash/cashflow-projection/update", cashChain(projection.UpdateCashFlowProposal(pgxPool)))

	mux.Handle("/cash/cashflow-projection/upload", cashChain(projection.UploadCashflowProposalSimple(pgxPool)))

	// Cash flow projection routes (V2 - Auto-calculated monthly projections)
	mux.Handle("/cash/projection/v2/create", cashChain(projection.CreateCashFlowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/update", cashChain(projection.UpdateCashFlowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/detail", cashChain(projection.GetProposalDetailV2(pgxPool)))
	mux.Handle("/cash/projection/v2/list", cashChain(projection.ListProposalsV2(pgxPool)))
	mux.Handle("/cash/projection/v2/delete", cashChain(projection.DeleteCashFlowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/approve", cashChain(projection.BulkApproveCashFlowProposalActionsV2(pgxPool)))
	mux.Handle("/cash/projection/v2/reject", cashChain(projection.BulkRejectCashFlowProposalActionsV2(pgxPool)))
	mux.Handle("/cash/projection/v2/upload", cashChain(projection.UploadCashflowProposalV2(pgxPool)))
	mux.Handle("/cash/projection/v2/download", cashChain(projection.GetProjectionDownloadURLV2(pgxPool)))
	mux.Handle("/cash/projection/v2/download-bulk", cashChain(projection.GetProjectionBulkDownloadURLV2(pgxPool)))
	mux.Handle("/cash/projection/v2/package-zip", middlewares.PreValidationMiddleware(pgxPool)(projection.DownloadProjectionPackageZipHandler(pgxPool)))
	mux.Handle("/cash/projection/v2/additional-files/list", cashChain(projection.ListAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/projection/v2/additional-files/upload", cashChain(projection.UploadAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/projection/v2/additional-files/download", cashChain(projection.DownloadAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/projection/v2/additional-files/download-bulk", cashChain(projection.DownloadSelectedAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/projection/v2/additional-files/delete", cashChain(projection.DeleteAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/projection/v2/additional-files/delete/approve", cashChain(projection.ApproveAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/projection/v2/additional-files/delete/reject", cashChain(projection.RejectAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/projection/v2/additional-files/audit", cashChain(projection.AuditAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/projections/audit", cashChain(projection.GetProjectionAuditHandler(pgxPool)))

	//bank balance
	mux.Handle("/cash/bank-balances/create", cashChain(bankbalances.CreateBankBalance(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-approve", cashChain(bankbalances.BulkApproveBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-reject", cashChain(bankbalances.BulkRejectBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/bulk-delete", cashChain(bankbalances.BulkRequestDeleteBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/all", cashChain(bankbalances.GetBankBalances(pgxPool)))
	// mux.Handle("/cash/bank-balances/upload",(bankbalances.UploadBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/upload", cashChain(bankbalances.UploadBankBalances(pgxPool)))
	mux.Handle("/cash/bank-balances/audit", cashChain(bankbalances.GetBankBalanceAuditHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/download", cashChain(bankbalances.GetBankBalanceDownloadURL(pgxPool)))
	mux.Handle("/cash/bank-balances/download-bulk", cashChain(bankbalances.GetBankBalanceBulkDownloadURL(pgxPool)))
	mux.Handle("/cash/bank-balances/package-zip", middlewares.PreValidationMiddleware(pgxPool)(bankbalances.DownloadBankBalancePackageZipHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/additional-files/list", cashChain(bankbalances.ListAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/additional-files/upload", cashChain(bankbalances.UploadAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/additional-files/download", cashChain(bankbalances.DownloadAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/additional-files/download-bulk", cashChain(bankbalances.DownloadSelectedAdditionalFilesHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/additional-files/delete", cashChain(bankbalances.DeleteAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/additional-files/delete/approve", cashChain(bankbalances.ApproveAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/additional-files/delete/reject", cashChain(bankbalances.RejectAdditionalFileDeleteHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/additional-files/audit", cashChain(bankbalances.AuditAdditionalFileHandler(pgxPool)))
	mux.Handle("/cash/bank-balances/update", cashChain(bankbalances.UpdateBankBalance(pgxPool)))

	// Fund Availability - Combined Actuals & Projections
	mux.Handle("/cash/fund-availability/combined", cashChain(fundavailibilty.GetFundAvailability(pgxPool)))

	// Bank Limit Management - Limit sanctioning and tracking
	limit.RegisterLimitRoutes(mux, pgxPool)

	mux.HandleFunc("/cash/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Cash Service is active"))
	})
	mux.Handle("/cash/metrics", observability.MetricsHandler(serviceName))
}
