package master

import (
	allMaster "CimplrCorpSaas/api/master/allMasters"
	counterpartyHub "CimplrCorpSaas/api/master/counterpartyHub"
	investmentMasters "CimplrCorpSaas/api/master/investmentMasters"
	middlewares "CimplrCorpSaas/api/middlewares"
	"CimplrCorpSaas/internal/dbutil"
	"CimplrCorpSaas/internal/observability"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func registerMasterAdditionalFileAliasRoutes(mux *http.ServeMux) {
	mux.Handle("/master/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/additional-files/delete/approve"):
			rewritten := r.Clone(r.Context())
			rewritten.URL.Path = strings.TrimSuffix(path, "/delete/approve") + "/approve-delete"
			mux.ServeHTTP(w, rewritten)
		case strings.HasSuffix(path, "/additional-files/delete/reject"):
			rewritten := r.Clone(r.Context())
			rewritten.URL.Path = strings.TrimSuffix(path, "/delete/reject") + "/reject-delete"
			mux.ServeHTTP(w, rewritten)
		default:
			http.NotFound(w, r)
		}
	}))
}

func StartMasterService(db *sql.DB, port string) {
	const serviceName = "master"
	mux := http.NewServeMux()
	registerMasterAdditionalFileAliasRoutes(mux)
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	sslMode := dbutil.EffectiveSSLMode(host)
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, dbPort, name, sslMode)
	pgxPool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		logger.LogError("failed to connect to pgxpool DB: %v", err)
		return
	}
	defer pgxPool.Close()
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pgxPool.Ping(pingCtx); err != nil {
		logger.LogError("Master: failed to verify pgxpool DB connectivity at startup: %v", err)
	}

	mux.HandleFunc("/master/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Masters Service is healthy"))
	})
	mux.Handle("/master/metrics", observability.MetricsHandler(serviceName))

	// --- New Module-Specific Middleware Chains ---
	midGlobalIndep := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(middlewares.GlobalIndependentMiddleware(pgxPool)(h))
	}
	midGlobalDep := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(middlewares.GlobalIndependentMiddleware(pgxPool)(middlewares.GlobalDependentMiddleware(pgxPool)(h)))
	}
	midCash := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(middlewares.GlobalIndependentMiddleware(pgxPool)(middlewares.GlobalDependentMiddleware(pgxPool)(middlewares.CashMiddleware(pgxPool)(h))))
	}
	midInvestMF := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(middlewares.GlobalIndependentMiddleware(pgxPool)(middlewares.GlobalDependentMiddleware(pgxPool)(middlewares.InvestmentMFMiddleware(pgxPool)(h))))
	}
	midInvestFD := func(h http.Handler) http.Handler {
		return middlewares.SessionMiddleware(pgxPool)(middlewares.GlobalIndependentMiddleware(pgxPool)(middlewares.GlobalDependentMiddleware(pgxPool)(middlewares.InvestmentFDMiddleware(pgxPool)(h))))
	}
	// ----------------------------------------------

	// Cost / Profit Center Master routes
	mux.Handle("/master/v2/costprofit-center/bulk-create-sync", midCash(allMaster.CreateAndSyncCostProfitCenters(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/upload", midCash(allMaster.UploadAndSyncCostProfitCenters(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/updatebulk", midCash(allMaster.UpdateAndSyncCostProfitCenters(pgxPool)))
	mux.Handle("/master/costprofit-center/approved-active", midCash(allMaster.GetApprovedActiveCostProfitCenters(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/hierarchy", midCash(allMaster.GetCostProfitCenterHierarchy(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/find-parent-at-level", midCash(allMaster.FindParentCostProfitCenterAtLevel(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/delete", midCash(allMaster.DeleteCostProfitCenter(pgxPool)))
	mux.Handle("/master/costprofit-center/bulk-reject", midCash(allMaster.BulkRejectCostProfitCenterActions(pgxPool)))
	mux.Handle("/master/costprofit-center/bulk-approve", midCash(allMaster.BulkApproveCostProfitCenterActions(pgxPool)))
	mux.Handle("/master/costprofit-center/audit-history", midCash(allMaster.GetCostProfitCenterAuditHistory(pgxPool)))

	mux.Handle("/master/costprofit-center/upload-simple", midCash(allMaster.UploadCostProfitCenterSimple(pgxPool)))

	// Payable/Receivable Master routes
	mux.Handle("/master/v2/payablereceivable/create", midCash(allMaster.CreatePayableReceivableTypes(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/names", midCash(allMaster.GetPayableReceivableNamesWithID(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/updatebulk", midCash(allMaster.UpdatePayableReceivableBulk(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/delete", midCash(allMaster.DeletePayableReceivable(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/bulk-approve", midCash(allMaster.BulkApprovePayableReceivableActions(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/bulk-reject", midCash(allMaster.BulkRejectPayableReceivableActions(pgxPool)))
	mux.Handle("/master/payablereceivable/upload", midCash(allMaster.UploadPayableReceivable(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/approved-active", midCash(allMaster.GetApprovedActivePayableReceivable(pgxPool)))
	mux.Handle("/master/payablereceivable/audit-history", midCash(allMaster.GetPayableReceivableAuditHistory(pgxPool)))

	// Counterparty Master routes
	mux.Handle("/master/v2/counterparty/create", midCash(allMaster.CreateCounterparties(pgxPool)))
	mux.Handle("/master/v2/counterparty/names", midCash(allMaster.GetCounterpartyNamesWithID(pgxPool)))
	mux.Handle("/master/v2counterparty/updatebulk", midCash(allMaster.UpdateCounterpartyBulk(pgxPool)))
	mux.Handle("/master/counterparty/delete", midCash(allMaster.DeleteCounterparty(pgxPool)))
	mux.Handle("/master/v2/counterparty/bulk-approve", midCash(allMaster.BulkApproveCounterpartyActions(pgxPool)))
	mux.Handle("/master/counterparty/bulk-reject", midCash(allMaster.BulkRejectCounterpartyActions(pgxPool)))
	mux.Handle("/master/v2/counterparty/upload", midCash(allMaster.UploadCounterparty(pgxPool)))

	mux.Handle("/master/counterparty/upload-simple", midCash(allMaster.UploadCounterpartySimple(pgxPool)))
	mux.Handle("/master/counterparty/audit-history", midCash(allMaster.GetCounterpartyAuditHistory(pgxPool)))
	mux.Handle("/master/counterparty/banks/upload-simple", midCash(allMaster.UploadCounterpartyBankSimple(pgxPool)))

	mux.Handle("/master/counterparty/approved-active", midCash(allMaster.GetApprovedActiveCounterparties(pgxPool)))

	mux.Handle("/master/v2/counterparty/banks", midCash(allMaster.GetCounterpartyBanks(pgxPool)))
	mux.Handle("/master/v2/counterparty/banks/updatebulk", midCash(allMaster.UpdateCounterpartyBanksBulk(pgxPool)))

	// GL Account Master routes
	mux.Handle("/master/v2/glaccount/create", midGlobalDep(allMaster.CreateGLAccounts(pgxPool)))
	mux.Handle("/master/v2/glaccount/hierarchy", midGlobalDep(allMaster.GetGLAccountNamesWithID(pgxPool)))
	mux.Handle("/master/v2/glaccount/updatebulk", midGlobalDep(allMaster.UpdateAndSyncGLAccounts(pgxPool)))
	mux.Handle("/master/v2/glaccount/delete", midGlobalDep(allMaster.DeleteGLAccount(pgxPool)))
	mux.Handle("/master/v2/glaccount/bulk-approve", midGlobalDep(allMaster.BulkApproveGLAccountActions(pgxPool)))
	mux.Handle("/master/v2/glaccount/bulk-reject", midGlobalDep(allMaster.BulkRejectGLAccountActions(pgxPool)))
	mux.Handle("/master/glaccount/upload", midGlobalDep(allMaster.UploadGLAccount(pgxPool)))
	mux.Handle("/master/glaccount/approved-active", midGlobalDep(allMaster.GetApprovedActiveGLAccounts(pgxPool)))
	mux.Handle("/master/v2/glaccount/find-parent-at-level", midGlobalDep(allMaster.FindParentGLAccountAtLevel(pgxPool)))
	mux.Handle("/master/glaccount/upload-simple", midGlobalDep(allMaster.UploadGLAccountSimple(pgxPool)))
	mux.Handle("/master/glaccount/audit-history", midGlobalDep(allMaster.GetGLAccountMasterAuditHistory(pgxPool)))

	// Cash Flow Category Master routes
	mux.Handle("/master/cashflow-category/delete", midCash(allMaster.DeleteCashFlowCategory(pgxPool)))
	mux.Handle("/master/v2/cashflow-category/hierarchy", midCash(allMaster.GetCashFlowCategoryHierarchyPGX(pgxPool)))
	mux.Handle("/master/cashflow-category/find-parent-at-level", midCash(allMaster.FindParentCashFlowCategoryAtLevel(pgxPool)))
	mux.Handle("/master/cashflow-category/names", midCash(allMaster.GetCashFlowCategoryNamesWithID(pgxPool)))
	mux.Handle("/master/v2/cashflow-category/updatebulk", midCash(allMaster.UpdateCashFlowCategoryBulk(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-reject", midCash(allMaster.BulkRejectCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-approve", midCash(allMaster.BulkApproveCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/cashflow-category/audit-history", midCash(allMaster.GetCashFlowCategoryAuditHistory(pgxPool)))
	mux.Handle("/master/v2/cashflow-category/bulk-create-sync", midCash(allMaster.CreateAndSyncCashFlowCategories(pgxPool)))
	mux.Handle("/master/cashflow-category/upload", midCash(allMaster.UploadCashFlowCategory(pgxPool)))

	mux.Handle("/master/cashflow-category/upload-simple", midCash(allMaster.UploadCashFlowCategorySimple(pgxPool)))
	// Currency Master routes (pgx-backed)
	mux.Handle("/master/currency/create", midGlobalIndep(allMaster.CreateCurrencyMaster(pgxPool)))
	mux.Handle("/master/currency/all", midGlobalIndep(allMaster.GetAllCurrencyMaster(pgxPool)))
	mux.Handle("/master/currency/update", midGlobalIndep(allMaster.UpdateCurrencyMasterBulk(pgxPool)))
	mux.Handle("/master/currency/bulk-approve", midGlobalIndep(allMaster.BulkApproveAuditActions(pgxPool)))
	mux.Handle("/master/currency/bulk-reject", midGlobalIndep(allMaster.BulkRejectAuditActions(pgxPool)))
	mux.Handle("/master/currency/bulk-delete", midGlobalIndep(allMaster.BulkDeleteCurrencyAudit(pgxPool)))
	mux.Handle("/master/currency/audit-history", midGlobalIndep(allMaster.GetCurrencyMasterAuditHistory(pgxPool)))
	mux.Handle("/master/currency/active-approved", midGlobalIndep(allMaster.GetActiveApprovedCurrencyCodes(pgxPool)))

	// Bank Master routes (pgx-backed)
	mux.Handle("/master/bank/create", midGlobalIndep(allMaster.CreateBankMaster(pgxPool)))
	mux.Handle("/master/bank/upload", midGlobalIndep(allMaster.UploadBank(pgxPool)))
	mux.Handle("/master/bank/all", midGlobalIndep(allMaster.GetAllBankMaster(pgxPool)))
	mux.Handle("/master/bank/names", midGlobalIndep(allMaster.GetBankNamesWithID(pgxPool)))
	mux.Handle("/master/bank/update", midGlobalIndep(allMaster.UpdateBankMasterBulk(pgxPool)))
	mux.Handle("/master/bank/bulk-approve", midGlobalIndep(allMaster.BulkApproveBankAuditActions(pgxPool)))
	mux.Handle("/master/bank/bulk-reject", midGlobalIndep(allMaster.BulkRejectBankAuditActions(pgxPool)))
	mux.Handle("/master/bank/bulk-delete", midGlobalIndep(allMaster.BulkDeleteBankAudit(pgxPool)))
	mux.Handle("/master/bank/audit-history", midGlobalIndep(allMaster.GetBankMasterAuditHistory(pgxPool)))

	// Bank Account Master routes
	mux.Handle("/master/bankaccount/create", midCash(allMaster.CreateBankAccountMaster(pgxPool)))
	mux.Handle("/master/bankaccount/upload", midCash(allMaster.UploadBankAccount(pgxPool)))
	// mux.Handle("/master/bankaccount/all", midCash(allMaster.GetAllBankAccountMaster(pgxPool)))
	mux.Handle("/master/bankaccount/update", midCash(allMaster.UpdateBankAccountMasterBulk(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-delete", midCash(allMaster.BulkDeleteBankAccountAudit(pgxPool)))
	mux.Handle("/master/bankaccount/audit-history", midCash(allMaster.GetBankAccountAuditHistory(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-reject", midCash(allMaster.BulkRejectBankAccountAuditActions(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-approve", midCash(allMaster.BulkApproveBankAccountAuditActions(pgxPool)))
	mux.Handle("/master/bankaccount/names", midCash(allMaster.GetBankNamesWithIDForAccount(pgxPool)))
	mux.Handle("/master/bankaccount/approved-with-entity", midCash(allMaster.GetApprovedBankAccountsWithBankEntity(pgxPool)))
	mux.Handle("/master/bankaccount/pre-populate", midCash(allMaster.GetApprovedBankAccountsSimple(pgxPool)))
	mux.Handle("/master/bankaccount/all", midCash(allMaster.GetBankAccountMetaAll(pgxPool)))
	mux.Handle("/master/bankaccount/for-user", midCash(allMaster.GetBankAccountsForUser(pgxPool)))

	// Entity Master routes
	mux.Handle("/master/entity/bulk-create-sync", midGlobalIndep(allMaster.CreateAndSyncEntities(db)))
	mux.Handle("/master/entity/hierarchy", midGlobalIndep(allMaster.GetEntityHierarchy(db)))
	mux.Handle("/master/entity/delete", midGlobalIndep(allMaster.DeleteEntity(db)))
	mux.Handle("/master/entity/findParentAtLevel", midGlobalIndep(allMaster.FindParentAtLevel(db)))
	mux.Handle("/master/entity/approve", midGlobalIndep(allMaster.ApproveEntity(db)))
	mux.Handle("/master/entity/reject-bulk", midGlobalIndep(allMaster.RejectEntitiesBulk(db)))
	mux.Handle("/master/entity/update", midGlobalIndep(allMaster.UpdateEntity(db)))
	mux.Handle("/master/entity/all-names", midGlobalIndep(allMaster.GetAllEntityNames(db)))
	mux.Handle("/master/entity/render-vars-hierarchical", midGlobalIndep(allMaster.GetRenderVarsHierarchical(db)))

	// Entity Cash Master routes (pgx-backed)
	mux.Handle("/master/entitycash/bulk-create-sync", midCash(allMaster.CreateAndSyncCashEntities(pgxPool)))
	mux.Handle("/master/entitycash/upload", midCash(allMaster.UploadEntityCash(pgxPool)))
	mux.Handle("/master/entitycash/upload-simple", midCash(allMaster.UploadEntitySimple(pgxPool)))
	mux.Handle("/master/entitycash/hierarchy", midCash(allMaster.GetCashEntityHierarchy(pgxPool)))
	mux.Handle("/master/entitycash/logo-url", midCash(allMaster.GetCashEntityLogoURL(pgxPool)))
	mux.Handle("/master/entitycash/updatebulk", midCash(allMaster.UpdateCashEntityBulk(pgxPool)))
	mux.Handle("/master/entitycash/find-parent-at-level", midCash(allMaster.FindParentCashEntityAtLevel(pgxPool)))
	mux.Handle("/master/entitycash/delete", midCash(allMaster.DeleteCashEntity(pgxPool)))
	mux.Handle("/master/entitycash/bulk-approve", midCash(allMaster.BulkApproveCashEntityActions(pgxPool)))
	mux.Handle("/master/entitycash/bulk-reject", midCash(allMaster.BulkRejectCashEntityActions(pgxPool)))
	mux.Handle("/master/entitycash/audit-history", midCash(allMaster.GetEntityCashAuditHistory(pgxPool)))
	mux.Handle("/master/entitycash/assigned-names", midCash(allMaster.GetAssignedCashEntityNamesWithID(pgxPool)))
	mux.Handle("/master/entitycash/all-names", midCash(allMaster.GetCashEntityNamesWithID(pgxPool)))

	//Amc Master routes
	mux.Handle("/master/amc/create", midGlobalDep(investmentMasters.CreateAMCsingle(pgxPool)))
	mux.Handle("/master/amc/bulk-create", midGlobalDep(investmentMasters.CreateAMC(pgxPool)))
	mux.Handle("/master/amc/bulk-update", midGlobalDep(investmentMasters.UpdateAMCBulk(pgxPool)))
	mux.Handle("/master/amc/update", midGlobalDep(investmentMasters.UpdateAMC(pgxPool)))
	mux.Handle("/master/amc/bulk-delete", midGlobalDep(investmentMasters.DeleteAMC(pgxPool)))
	mux.Handle("/master/amc/bulk-approve", midGlobalDep(investmentMasters.BulkApproveAMCActions(pgxPool)))
	mux.Handle("/master/amc/bulk-reject", midGlobalDep(investmentMasters.BulkRejectAMCActions(pgxPool)))
	mux.Handle("/master/amc/all", midGlobalDep(investmentMasters.GetAMCsWithAudit(pgxPool)))
	mux.Handle("/master/amc/approved-active", midGlobalDep(investmentMasters.GetApprovedActiveAMCs(pgxPool)))
	mux.Handle("/master/amc/upload", midGlobalDep(investmentMasters.UploadAMCSimple(pgxPool)))
	mux.Handle("/master/amc/audit-history", midGlobalDep(investmentMasters.GetAMCAuditHistory(pgxPool)))

	// Scheme Master routes
	mux.Handle("/master/scheme/upload", midInvestMF(investmentMasters.UploadSchemeSimple(pgxPool)))
	mux.Handle("/master/scheme/create", midInvestMF(investmentMasters.CreateSchemeSingle(pgxPool)))
	mux.Handle("/master/scheme/bulk-create", midInvestMF(investmentMasters.CreateScheme(pgxPool)))
	mux.Handle("/master/scheme/update", midInvestMF(investmentMasters.UpdateScheme(pgxPool)))
	mux.Handle("/master/scheme/bulk-update", midInvestMF(investmentMasters.UpdateSchemeBulk(pgxPool)))
	mux.Handle("/master/scheme/bulk-delete", midInvestMF(investmentMasters.DeleteScheme(pgxPool)))
	mux.Handle("/master/scheme/bulk-approve", midInvestMF(investmentMasters.BulkApproveSchemeActions(pgxPool)))
	mux.Handle("/master/scheme/bulk-reject", midInvestMF(investmentMasters.BulkRejectSchemeActions(pgxPool)))
	mux.Handle("/master/scheme/approved-active", midInvestMF(investmentMasters.GetApprovedActiveSchemes(pgxPool)))
	mux.Handle("/master/scheme/all", midInvestMF(investmentMasters.GetSchemesWithAudit(pgxPool)))
	mux.Handle("/master/scheme/by-amc", midInvestMF(investmentMasters.GetApprovedActiveSchemesByAMC(pgxPool)))
	mux.Handle("/master/scheme/audit-history", midInvestMF(investmentMasters.GetSchemeAuditHistory(pgxPool)))

	// DP Master routes
	mux.Handle("/master/dp/upload", midInvestMF(investmentMasters.UploadDPSimple(pgxPool)))
	mux.Handle("/master/dp/create", midInvestMF(investmentMasters.CreateDPSingle(pgxPool)))
	mux.Handle("/master/dp/bulk-create", midInvestMF(investmentMasters.CreateDPBulk(pgxPool)))
	mux.Handle("/master/dp/update", midInvestMF(investmentMasters.UpdateDP(pgxPool)))
	mux.Handle("/master/dp/bulk-update", midInvestMF(investmentMasters.UpdateDPBulk(pgxPool)))
	mux.Handle("/master/dp/bulk-delete", midInvestMF(investmentMasters.DeleteDP(pgxPool)))
	mux.Handle("/master/dp/bulk-approve", midInvestMF(investmentMasters.BulkApproveDPActions(pgxPool)))
	mux.Handle("/master/dp/bulk-reject", midInvestMF(investmentMasters.BulkRejectDPActions(pgxPool)))
	mux.Handle("/master/dp/approved-active", midInvestMF(investmentMasters.GetApprovedActiveDPs(pgxPool)))
	mux.Handle("/master/dp/all", midInvestMF(investmentMasters.GetDPsWithAudit(pgxPool)))
	mux.Handle("/master/dp/audit-history", midInvestMF(investmentMasters.GetDPAuditHistory(pgxPool)))

	// Demat Master routes
	mux.Handle("/master/demat/upload", midInvestMF(investmentMasters.UploadDematSimple(pgxPool)))
	mux.Handle("/master/demat/create", midInvestMF(investmentMasters.CreateDematSingle(pgxPool)))
	mux.Handle("/master/demat/bulk-create", midInvestMF(investmentMasters.CreateDematBulk(pgxPool)))
	mux.Handle("/master/demat/update", midInvestMF(investmentMasters.UpdateDemat(pgxPool)))
	mux.Handle("/master/demat/bulk-update", midInvestMF(investmentMasters.UpdateDematBulk(pgxPool)))
	mux.Handle("/master/demat/bulk-delete", midInvestMF(investmentMasters.DeleteDemat(pgxPool)))
	mux.Handle("/master/demat/bulk-approve", midInvestMF(investmentMasters.BulkApproveDematActions(pgxPool)))
	mux.Handle("/master/demat/bulk-reject", midInvestMF(investmentMasters.BulkRejectDematActions(pgxPool)))
	mux.Handle("/master/demat/approved-active", midInvestMF(investmentMasters.GetApprovedActiveDemats(pgxPool)))
	mux.Handle("/master/demat/all", midInvestMF(investmentMasters.GetDematsWithAudit(pgxPool)))
	mux.Handle("/master/demat/audit-history", midInvestMF(investmentMasters.GetDematAuditHistory(pgxPool)))

	// Interest Type Master routes
	mux.Handle("/master/interest-type/create", midInvestFD(investmentMasters.CreateInterestTypeSingle(pgxPool)))
	mux.Handle("/master/interest-type/bulk-create", midInvestFD(investmentMasters.CreateInterestType(pgxPool)))
	mux.Handle("/master/interest-type/update", midInvestFD(investmentMasters.UpdateInterestType(pgxPool)))
	mux.Handle("/master/interest-type/bulk-update", midInvestFD(investmentMasters.UpdateInterestTypeBulk(pgxPool)))
	mux.Handle("/master/interest-type/bulk-delete", midInvestFD(investmentMasters.DeleteInterestType(pgxPool)))
	mux.Handle("/master/interest-type/bulk-approve", midInvestFD(investmentMasters.BulkApproveInterestType(pgxPool)))
	mux.Handle("/master/interest-type/bulk-reject", midInvestFD(investmentMasters.BulkRejectInterestType(pgxPool)))
	mux.Handle("/master/interest-type/approved-active", midInvestFD(investmentMasters.GetInterestTypesApprovedActive(pgxPool)))
	mux.Handle("/master/interest-type/all", midInvestFD(investmentMasters.GetInterestTypesWithAudit(pgxPool)))
	mux.Handle("/master/interest-type/audit-history", midInvestFD(investmentMasters.GetInterestTypeAuditHistory(pgxPool)))
	mux.Handle("/master/interest-type/upload", midInvestFD(investmentMasters.UploadInterestTypeSimple(pgxPool)))
	mux.Handle("/master/interest-type/get", midInvestFD(investmentMasters.GetInterestType(pgxPool)))

	// Penalty Structure Master routes
	mux.Handle("/master/penalty-structure/create", midInvestFD(investmentMasters.CreatePenaltyStructureSingle(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-create", midInvestFD(investmentMasters.CreatePenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/update", midInvestFD(investmentMasters.UpdatePenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-update", midInvestFD(investmentMasters.UpdatePenaltyStructureBulk(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-delete", midInvestFD(investmentMasters.DeletePenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-approve", midInvestFD(investmentMasters.BulkApprovePenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-reject", midInvestFD(investmentMasters.BulkRejectPenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/approved-active", midInvestFD(investmentMasters.GetPenaltyStructuresApprovedActive(pgxPool)))
	mux.Handle("/master/penalty-structure/all", midInvestFD(investmentMasters.GetPenaltyStructuresWithAudit(pgxPool)))
	mux.Handle("/master/penalty-structure/audit-history", midInvestFD(investmentMasters.GetPenaltyStructureAuditHistory(pgxPool)))
	mux.Handle("/master/penalty-structure/upload", midInvestFD(investmentMasters.UploadPenaltyStructureSimple(pgxPool)))
	mux.Handle("/master/penalty-structure/get", midInvestFD(investmentMasters.GetPenaltyStructure(pgxPool)))

	// Compounding Frequency Master routes
	mux.Handle("/master/compounding-frequency/create", midInvestFD(investmentMasters.CreateCompoundingFrequencySingle(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-create", midInvestFD(investmentMasters.CreateCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/update", midInvestFD(investmentMasters.UpdateCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-update", midInvestFD(investmentMasters.UpdateCompoundingFrequencyBulk(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-delete", midInvestFD(investmentMasters.DeleteCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-approve", midInvestFD(investmentMasters.BulkApproveCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-reject", midInvestFD(investmentMasters.BulkRejectCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/approved-active", midInvestFD(investmentMasters.GetCompoundingFrequenciesApprovedActive(pgxPool)))
	mux.Handle("/master/compounding-frequency/all", midInvestFD(investmentMasters.GetCompoundingFrequenciesWithAudit(pgxPool)))
	mux.Handle("/master/compounding-frequency/audit-history", midInvestFD(investmentMasters.GetCompoundingFrequencyAuditHistory(pgxPool)))
	mux.Handle("/master/compounding-frequency/upload", midInvestFD(investmentMasters.UploadCompoundingFrequencySimple(pgxPool)))
	mux.Handle("/master/compounding-frequency/get", midInvestFD(investmentMasters.GetCompoundingFrequency(pgxPool)))

	// TDS Plan Master routes
	mux.Handle("/master/tds-plan/create", midInvestFD(investmentMasters.CreateTDSPlanSingle(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-create", midInvestFD(investmentMasters.CreateTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/update", midInvestFD(investmentMasters.UpdateTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-update", midInvestFD(investmentMasters.UpdateTDSPlanBulk(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-delete", midInvestFD(investmentMasters.DeleteTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-approve", midInvestFD(investmentMasters.BulkApproveTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-reject", midInvestFD(investmentMasters.BulkRejectTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/approved-active", midInvestFD(investmentMasters.GetTDSPlansApprovedActive(pgxPool)))
	mux.Handle("/master/tds-plan/all", midInvestFD(investmentMasters.GetTDSPlansWithAudit(pgxPool)))
	mux.Handle("/master/tds-plan/audit-history", midInvestFD(investmentMasters.GetTDSPlanAuditHistory(pgxPool)))
	mux.Handle("/master/tds-plan/upload", midInvestFD(investmentMasters.UploadTDSPlanSimple(pgxPool)))
	mux.Handle("/master/tds-plan/get", midInvestFD(investmentMasters.GetTDSPlan(pgxPool)))

	// Folio Master routes
	mux.Handle("/master/folio/upload", midInvestMF(investmentMasters.UploadFolio(pgxPool)))
	mux.Handle("/master/folio/all", midInvestMF(investmentMasters.GetFoliosWithAudit(pgxPool)))
	mux.Handle("/master/folio/approved-active", midInvestMF(investmentMasters.GetApprovedActiveFolios(pgxPool)))
	mux.Handle("/master/folio/create", midInvestMF(investmentMasters.CreateFolioSingle(pgxPool)))
	mux.Handle("/master/folio/bulk-create", midInvestMF(investmentMasters.CreateFolioBulk(pgxPool)))
	mux.Handle("/master/folio/update", midInvestMF(investmentMasters.UpdateFolio(pgxPool)))
	mux.Handle("/master/folio/bulk-update", midInvestMF(investmentMasters.UpdateFolioBulk(pgxPool)))
	mux.Handle("/master/folio/bulk-delete", midInvestMF(investmentMasters.DeleteFolio(pgxPool)))
	mux.Handle("/master/folio/bulk-approve", midInvestMF(investmentMasters.BulkApproveFolioActions(pgxPool)))
	mux.Handle("/master/folio/bulk-reject", midInvestMF(investmentMasters.BulkRejectFolioActions(pgxPool)))
	mux.Handle("/master/folio/meta", midInvestMF(investmentMasters.GetSingleFolioWithAudit(pgxPool)))
	mux.Handle("/master/folio/schemes-by-approved", midInvestMF(investmentMasters.GetSchemesByApprovedFolios(pgxPool)))
	mux.Handle("/master/folio/audit-history", midInvestMF(investmentMasters.GetFolioAuditHistory(pgxPool)))

	// AMFI Config Master routes
	mux.Handle("/master/amfi/scheme", midInvestMF(investmentMasters.GetAMFISchemeMasterSimple(pgxPool)))
	mux.Handle("/master/amfi/nav", midInvestMF(investmentMasters.GetAMFINavStagingSimple(pgxPool)))
	mux.Handle("/master/amfi/approved-amc", midInvestMF(investmentMasters.GetApprovedAMCsAndSchemes(pgxPool)))
	mux.Handle("/master/amfi/distinct-amcs", midInvestMF(investmentMasters.GetDistinctAMCNamesFromAMFI(pgxPool)))

	// Holiday calendar exports (ICS/feed/share links)
	mux.Handle("/master/calendar/export/ics/", midGlobalIndep(investmentMasters.ExportCalendarICS(pgxPool)))
	mux.Handle("/master/calendar/feed/", midGlobalIndep(investmentMasters.CalendarFeedICS(pgxPool)))
	mux.Handle("/master/calendar/share-links/", midGlobalIndep(investmentMasters.ShareLinksCalendar(pgxPool)))

	// Calendar Master routes (pgx-backed)
	mux.Handle("/master/calendar/create", midGlobalIndep(investmentMasters.CreateCalendarSingle(pgxPool)))
	mux.Handle("/master/calendar/holiday/bulk-create", midGlobalIndep(investmentMasters.CreateHolidayBulk(pgxPool)))
	mux.Handle("/master/calendar/upload", midGlobalIndep(investmentMasters.UploadCalendarBulk(pgxPool)))
	mux.Handle("/master/calendar/holiday/upload", midGlobalIndep(investmentMasters.UploadHolidayBulk(pgxPool)))
	mux.Handle("/master/calendar/meta", midGlobalIndep(investmentMasters.GetCalendarsWithAudit(pgxPool)))
	mux.Handle("/master/calendar/list", midGlobalIndep(investmentMasters.GetCalendarListLite(pgxPool)))
	mux.Handle("/master/calendar/all", midGlobalIndep(investmentMasters.GetCalendarWithHolidays(pgxPool)))
	mux.Handle("/master/calendar/approved-active", midGlobalIndep(investmentMasters.GetApprovedActiveCalendars(pgxPool)))
	mux.Handle("/master/calendar/view", midGlobalIndep(investmentMasters.GetCalendarWithHolidaysApproved(pgxPool)))
	mux.Handle("/master/calendar/bulk-delete", midGlobalIndep(investmentMasters.DeleteCalendar(pgxPool)))
	mux.Handle("/master/calendar/bulk-approve", midGlobalIndep(investmentMasters.BulkApproveCalendarActions(pgxPool)))
	mux.Handle("/master/calendar/bulk-reject", midGlobalIndep(investmentMasters.BulkRejectCalendarActions(pgxPool)))
	mux.Handle("/master/calendar/update", midGlobalIndep(investmentMasters.UpdateCalendar(pgxPool)))
	mux.Handle("/master/calendar/holiday/update", midGlobalIndep(investmentMasters.UpdateHoliday(pgxPool)))
	mux.Handle("/master/calendar/update-with-holidays", midGlobalIndep(investmentMasters.UpdateCalendarWithHolidays(pgxPool)))
	mux.Handle("/master/calendar/years", midGlobalIndep(investmentMasters.GetPastYearsHolidays(pgxPool)))
	mux.Handle("/master/calendar/audit-history", midGlobalIndep(investmentMasters.GetCalendarAuditHistory(pgxPool)))

	// Day Count Convention Master routes
	mux.Handle("/master/day-count-convention/create", midInvestFD(investmentMasters.CreateDayCountConventionSingle(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-create", midInvestFD(investmentMasters.CreateDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/update", midInvestFD(investmentMasters.UpdateDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-update", midInvestFD(investmentMasters.UpdateDayCountConventionBulk(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-delete", midInvestFD(investmentMasters.DeleteDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-approve", midInvestFD(investmentMasters.BulkApproveDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-reject", midInvestFD(investmentMasters.BulkRejectDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/approved-active", midInvestFD(investmentMasters.GetDayCountConventionsApprovedActive(pgxPool)))
	mux.Handle("/master/day-count-convention/all", midInvestFD(investmentMasters.GetDayCountConventionsWithAudit(pgxPool)))
	mux.Handle("/master/day-count-convention/audit-history", midInvestFD(investmentMasters.GetDayCountConventionAuditHistory(pgxPool)))
	mux.Handle("/master/day-count-convention/upload", midInvestFD(investmentMasters.UploadDayCountConventionSimple(pgxPool)))
	mux.Handle("/master/day-count-convention/get", midInvestFD(investmentMasters.GetDayCountConvention(pgxPool)))

	// Bank Config Master routes
	mux.Handle("/master/bank-config/create", midInvestFD(investmentMasters.CreateBankConfigSingle(pgxPool)))
	mux.Handle("/master/bank-config/bulk-create", midInvestFD(investmentMasters.CreateBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/update", midInvestFD(investmentMasters.UpdateBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/bulk-update", midInvestFD(investmentMasters.UpdateBankConfigBulk(pgxPool)))
	mux.Handle("/master/bank-config/bulk-delete", midInvestFD(investmentMasters.DeleteBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/bulk-approve", midInvestFD(investmentMasters.BulkApproveBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/bulk-reject", midInvestFD(investmentMasters.BulkRejectBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/approved-active", midInvestFD(investmentMasters.GetBankConfigsApprovedActive(pgxPool)))
	mux.Handle("/master/bank-config/all", midInvestFD(investmentMasters.GetBankConfigsWithAudit(pgxPool)))
	mux.Handle("/master/bank-config/audit-history", midInvestFD(investmentMasters.GetBankConfigAuditHistory(pgxPool)))
	mux.Handle("/master/bank-config/upload", midInvestFD(investmentMasters.UploadBankConfigSimple(pgxPool)))
	mux.Handle("/master/bank-config/get", midInvestFD(investmentMasters.GetBankConfig(pgxPool)))

	// Bank Rate Card Master routes
	mux.Handle("/master/bank-rate-card/create", midInvestFD(investmentMasters.CreateBankRateCardSingle(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-create", midInvestFD(investmentMasters.CreateBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/update", midInvestFD(investmentMasters.UpdateBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-update", midInvestFD(investmentMasters.UpdateBankRateCardBulk(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-delete", midInvestFD(investmentMasters.DeleteBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-approve", midInvestFD(investmentMasters.BulkApproveBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-reject", midInvestFD(investmentMasters.BulkRejectBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/approved-active", midInvestFD(investmentMasters.GetBankRateCardsApprovedActive(pgxPool)))
	mux.Handle("/master/bank-rate-card/all", midInvestFD(investmentMasters.GetBankRateCardsWithAudit(pgxPool)))
	mux.Handle("/master/bank-rate-card/audit-history", midInvestFD(investmentMasters.GetBankRateCardAuditHistory(pgxPool)))
	mux.Handle("/master/bank-rate-card/upload", midInvestFD(investmentMasters.UploadBankRateCardSimple(pgxPool)))
	mux.Handle("/master/bank-rate-card/get", midInvestFD(investmentMasters.GetBankRateCard(pgxPool)))

	// ── Additional Files routes (supporting document uploads per master record) ──

	// Bank Master
	bankMF := allMaster.BankMasterFilesHandlers(pgxPool)
	mux.Handle("/master/bank/additional-files/list", midGlobalIndep(bankMF.List))
	mux.Handle("/master/bank/additional-files/upload", midGlobalIndep(bankMF.Upload))
	mux.Handle("/master/bank/additional-files/download", midGlobalIndep(bankMF.Download))
	mux.Handle("/master/bank/additional-files/download-bulk", midGlobalIndep(bankMF.DownloadBulk))
	mux.Handle("/master/bank/additional-files/delete", midGlobalIndep(bankMF.Delete))
	mux.Handle("/master/bank/additional-files/audit", midGlobalIndep(bankMF.Audit))
	mux.Handle("/master/bank/additional-files/approve-delete", midGlobalIndep(bankMF.ApproveDelete))
	mux.Handle("/master/bank/additional-files/reject-delete", midGlobalIndep(bankMF.RejectDelete))

	// Currency Master
	currencyMF := allMaster.CurrencyMasterFilesHandlers(pgxPool)
	mux.Handle("/master/currency/additional-files/list", midGlobalIndep(currencyMF.List))
	mux.Handle("/master/currency/additional-files/upload", midGlobalIndep(currencyMF.Upload))
	mux.Handle("/master/currency/additional-files/download", midGlobalIndep(currencyMF.Download))
	mux.Handle("/master/currency/additional-files/download-bulk", midGlobalIndep(currencyMF.DownloadBulk))
	mux.Handle("/master/currency/additional-files/delete", midGlobalIndep(currencyMF.Delete))
	mux.Handle("/master/currency/additional-files/audit", midGlobalIndep(currencyMF.Audit))
	mux.Handle("/master/currency/additional-files/approve-delete", midGlobalIndep(currencyMF.ApproveDelete))
	mux.Handle("/master/currency/additional-files/reject-delete", midGlobalIndep(currencyMF.RejectDelete))

	// Bank Account Master
	bankAccountMF := allMaster.BankAccountMasterFilesHandlers(pgxPool)
	mux.Handle("/master/bankaccount/additional-files/list", midCash(bankAccountMF.List))
	mux.Handle("/master/bankaccount/additional-files/upload", midCash(bankAccountMF.Upload))
	mux.Handle("/master/bankaccount/additional-files/download", midCash(bankAccountMF.Download))
	mux.Handle("/master/bankaccount/additional-files/download-bulk", midCash(bankAccountMF.DownloadBulk))
	mux.Handle("/master/bankaccount/additional-files/delete", midCash(bankAccountMF.Delete))
	mux.Handle("/master/bankaccount/additional-files/audit", midCash(bankAccountMF.Audit))
	mux.Handle("/master/bankaccount/additional-files/approve-delete", midCash(bankAccountMF.ApproveDelete))
	mux.Handle("/master/bankaccount/additional-files/reject-delete", midCash(bankAccountMF.RejectDelete))

	// Counterparty Master
	counterpartyMF := allMaster.CounterpartyMasterFilesHandlers(pgxPool)
	mux.Handle("/master/counterparty/additional-files/list", midCash(counterpartyMF.List))
	mux.Handle("/master/counterparty/additional-files/upload", midCash(counterpartyMF.Upload))
	mux.Handle("/master/counterparty/additional-files/download", midCash(counterpartyMF.Download))
	mux.Handle("/master/counterparty/additional-files/download-bulk", midCash(counterpartyMF.DownloadBulk))
	mux.Handle("/master/counterparty/additional-files/delete", midCash(counterpartyMF.Delete))
	mux.Handle("/master/counterparty/additional-files/audit", midCash(counterpartyMF.Audit))
	mux.Handle("/master/counterparty/additional-files/approve-delete", midCash(counterpartyMF.ApproveDelete))
	mux.Handle("/master/counterparty/additional-files/reject-delete", midCash(counterpartyMF.RejectDelete))

	// GL Account Master
	glAccountMF := allMaster.GLAccountMasterFilesHandlers(pgxPool)
	mux.Handle("/master/glaccount/additional-files/list", midGlobalDep(glAccountMF.List))
	mux.Handle("/master/glaccount/additional-files/upload", midGlobalDep(glAccountMF.Upload))
	mux.Handle("/master/glaccount/additional-files/download", midGlobalDep(glAccountMF.Download))
	mux.Handle("/master/glaccount/additional-files/download-bulk", midGlobalDep(glAccountMF.DownloadBulk))
	mux.Handle("/master/glaccount/additional-files/delete", midGlobalDep(glAccountMF.Delete))
	mux.Handle("/master/glaccount/additional-files/audit", midGlobalDep(glAccountMF.Audit))
	mux.Handle("/master/glaccount/additional-files/approve-delete", midGlobalDep(glAccountMF.ApproveDelete))
	mux.Handle("/master/glaccount/additional-files/reject-delete", midGlobalDep(glAccountMF.RejectDelete))

	// Cash Flow Category Master
	cashflowCategoryMF := allMaster.CashFlowCategoryMasterFilesHandlers(pgxPool)
	mux.Handle("/master/cashflow-category/additional-files/list", midCash(cashflowCategoryMF.List))
	mux.Handle("/master/cashflow-category/additional-files/upload", midCash(cashflowCategoryMF.Upload))
	mux.Handle("/master/cashflow-category/additional-files/download", midCash(cashflowCategoryMF.Download))
	mux.Handle("/master/cashflow-category/additional-files/download-bulk", midCash(cashflowCategoryMF.DownloadBulk))
	mux.Handle("/master/cashflow-category/additional-files/delete", midCash(cashflowCategoryMF.Delete))
	mux.Handle("/master/cashflow-category/additional-files/audit", midCash(cashflowCategoryMF.Audit))
	mux.Handle("/master/cashflow-category/additional-files/approve-delete", midCash(cashflowCategoryMF.ApproveDelete))
	mux.Handle("/master/cashflow-category/additional-files/reject-delete", midCash(cashflowCategoryMF.RejectDelete))

	// Cost Profit Center Master
	costProfitCenterMF := allMaster.CostProfitCenterMasterFilesHandlers(pgxPool)
	mux.Handle("/master/costprofit-center/additional-files/list", midCash(costProfitCenterMF.List))
	mux.Handle("/master/costprofit-center/additional-files/upload", midCash(costProfitCenterMF.Upload))
	mux.Handle("/master/costprofit-center/additional-files/download", midCash(costProfitCenterMF.Download))
	mux.Handle("/master/costprofit-center/additional-files/download-bulk", midCash(costProfitCenterMF.DownloadBulk))
	mux.Handle("/master/costprofit-center/additional-files/delete", midCash(costProfitCenterMF.Delete))
	mux.Handle("/master/costprofit-center/additional-files/audit", midCash(costProfitCenterMF.Audit))
	mux.Handle("/master/costprofit-center/additional-files/approve-delete", midCash(costProfitCenterMF.ApproveDelete))
	mux.Handle("/master/costprofit-center/additional-files/reject-delete", midCash(costProfitCenterMF.RejectDelete))

	// Payable Receivable Master
	payableReceivableMF := allMaster.PayableReceivableMasterFilesHandlers(pgxPool)
	mux.Handle("/master/payablereceivable/additional-files/list", midCash(payableReceivableMF.List))
	mux.Handle("/master/payablereceivable/additional-files/upload", midCash(payableReceivableMF.Upload))
	mux.Handle("/master/payablereceivable/additional-files/download", midCash(payableReceivableMF.Download))
	mux.Handle("/master/payablereceivable/additional-files/download-bulk", midCash(payableReceivableMF.DownloadBulk))
	mux.Handle("/master/payablereceivable/additional-files/delete", midCash(payableReceivableMF.Delete))
	mux.Handle("/master/payablereceivable/additional-files/audit", midCash(payableReceivableMF.Audit))
	mux.Handle("/master/payablereceivable/additional-files/approve-delete", midCash(payableReceivableMF.ApproveDelete))
	mux.Handle("/master/payablereceivable/additional-files/reject-delete", midCash(payableReceivableMF.RejectDelete))

	// Entity Cash Master
	entityCashMF := allMaster.EntityCashMasterFilesHandlers(pgxPool)
	mux.Handle("/master/entitycash/additional-files/list", midCash(entityCashMF.List))
	mux.Handle("/master/entitycash/additional-files/upload", midCash(entityCashMF.Upload))
	mux.Handle("/master/entitycash/additional-files/download", midCash(entityCashMF.Download))
	mux.Handle("/master/entitycash/additional-files/download-bulk", midCash(entityCashMF.DownloadBulk))
	mux.Handle("/master/entitycash/additional-files/delete", midCash(entityCashMF.Delete))
	mux.Handle("/master/entitycash/additional-files/audit", midCash(entityCashMF.Audit))
	mux.Handle("/master/entitycash/additional-files/approve-delete", midCash(entityCashMF.ApproveDelete))
	mux.Handle("/master/entitycash/additional-files/reject-delete", midCash(entityCashMF.RejectDelete))

	// Entity Master
	entityMF := allMaster.EntityMasterFilesHandlers(pgxPool)
	mux.Handle("/master/entity/additional-files/list", midGlobalIndep(entityMF.List))
	mux.Handle("/master/entity/additional-files/upload", midGlobalIndep(entityMF.Upload))
	mux.Handle("/master/entity/additional-files/download", midGlobalIndep(entityMF.Download))
	mux.Handle("/master/entity/additional-files/download-bulk", midGlobalIndep(entityMF.DownloadBulk))
	mux.Handle("/master/entity/additional-files/delete", midGlobalIndep(entityMF.Delete))
	mux.Handle("/master/entity/additional-files/audit", midGlobalIndep(entityMF.Audit))
	mux.Handle("/master/entity/additional-files/approve-delete", midGlobalIndep(entityMF.ApproveDelete))
	mux.Handle("/master/entity/additional-files/reject-delete", midGlobalIndep(entityMF.RejectDelete))

	// AMC Master
	amcMF := investmentMasters.AMCMasterFilesHandlers(pgxPool)
	mux.Handle("/master/amc/additional-files/list", midInvestMF(amcMF.List))
	mux.Handle("/master/amc/additional-files/upload", midInvestMF(amcMF.Upload))
	mux.Handle("/master/amc/additional-files/download", midInvestMF(amcMF.Download))
	mux.Handle("/master/amc/additional-files/download-bulk", midInvestMF(amcMF.DownloadBulk))
	mux.Handle("/master/amc/additional-files/delete", midInvestMF(amcMF.Delete))
	mux.Handle("/master/amc/additional-files/audit", midInvestMF(amcMF.Audit))
	mux.Handle("/master/amc/additional-files/approve-delete", midInvestMF(amcMF.ApproveDelete))
	mux.Handle("/master/amc/additional-files/reject-delete", midInvestMF(amcMF.RejectDelete))

	// Scheme Master
	schemeMF := investmentMasters.SchemeMasterFilesHandlers(pgxPool)
	mux.Handle("/master/scheme/additional-files/list", midInvestMF(schemeMF.List))
	mux.Handle("/master/scheme/additional-files/upload", midInvestMF(schemeMF.Upload))
	mux.Handle("/master/scheme/additional-files/download", midInvestMF(schemeMF.Download))
	mux.Handle("/master/scheme/additional-files/download-bulk", midInvestMF(schemeMF.DownloadBulk))
	mux.Handle("/master/scheme/additional-files/delete", midInvestMF(schemeMF.Delete))
	mux.Handle("/master/scheme/additional-files/audit", midInvestMF(schemeMF.Audit))
	mux.Handle("/master/scheme/additional-files/approve-delete", midInvestMF(schemeMF.ApproveDelete))
	mux.Handle("/master/scheme/additional-files/reject-delete", midInvestMF(schemeMF.RejectDelete))

	// DP (Depository Participant) Master
	dpMF := investmentMasters.DPMasterFilesHandlers(pgxPool)
	mux.Handle("/master/dp/additional-files/list", midInvestMF(dpMF.List))
	mux.Handle("/master/dp/additional-files/upload", midInvestMF(dpMF.Upload))
	mux.Handle("/master/dp/additional-files/download", midInvestMF(dpMF.Download))
	mux.Handle("/master/dp/additional-files/download-bulk", midInvestMF(dpMF.DownloadBulk))
	mux.Handle("/master/dp/additional-files/delete", midInvestMF(dpMF.Delete))
	mux.Handle("/master/dp/additional-files/audit", midInvestMF(dpMF.Audit))
	mux.Handle("/master/dp/additional-files/approve-delete", midInvestMF(dpMF.ApproveDelete))
	mux.Handle("/master/dp/additional-files/reject-delete", midInvestMF(dpMF.RejectDelete))

	// Demat Master
	dematMF := investmentMasters.DematMasterFilesHandlers(pgxPool)
	mux.Handle("/master/demat/additional-files/list", midInvestMF(dematMF.List))
	mux.Handle("/master/demat/additional-files/upload", midInvestMF(dematMF.Upload))
	mux.Handle("/master/demat/additional-files/download", midInvestMF(dematMF.Download))
	mux.Handle("/master/demat/additional-files/download-bulk", midInvestMF(dematMF.DownloadBulk))
	mux.Handle("/master/demat/additional-files/delete", midInvestMF(dematMF.Delete))
	mux.Handle("/master/demat/additional-files/audit", midInvestMF(dematMF.Audit))
	mux.Handle("/master/demat/additional-files/approve-delete", midInvestMF(dematMF.ApproveDelete))
	mux.Handle("/master/demat/additional-files/reject-delete", midInvestMF(dematMF.RejectDelete))

	// Folio Master
	folioMF := investmentMasters.FolioMasterFilesHandlers(pgxPool)
	mux.Handle("/master/folio/additional-files/list", midInvestMF(folioMF.List))
	mux.Handle("/master/folio/additional-files/upload", midInvestMF(folioMF.Upload))
	mux.Handle("/master/folio/additional-files/download", midInvestMF(folioMF.Download))
	mux.Handle("/master/folio/additional-files/download-bulk", midInvestMF(folioMF.DownloadBulk))
	mux.Handle("/master/folio/additional-files/delete", midInvestMF(folioMF.Delete))
	mux.Handle("/master/folio/additional-files/audit", midInvestMF(folioMF.Audit))
	mux.Handle("/master/folio/additional-files/approve-delete", midInvestMF(folioMF.ApproveDelete))
	mux.Handle("/master/folio/additional-files/reject-delete", midInvestMF(folioMF.RejectDelete))

	// Interest Type Master
	interestTypeMF := investmentMasters.InterestTypeMasterFilesHandlers(pgxPool)
	mux.Handle("/master/interest-type/additional-files/list", midInvestFD(interestTypeMF.List))
	mux.Handle("/master/interest-type/additional-files/upload", midInvestFD(interestTypeMF.Upload))
	mux.Handle("/master/interest-type/additional-files/download", midInvestFD(interestTypeMF.Download))
	mux.Handle("/master/interest-type/additional-files/download-bulk", midInvestFD(interestTypeMF.DownloadBulk))
	mux.Handle("/master/interest-type/additional-files/delete", midInvestFD(interestTypeMF.Delete))
	mux.Handle("/master/interest-type/additional-files/audit", midInvestFD(interestTypeMF.Audit))
	mux.Handle("/master/interest-type/additional-files/approve-delete", midInvestFD(interestTypeMF.ApproveDelete))
	mux.Handle("/master/interest-type/additional-files/reject-delete", midInvestFD(interestTypeMF.RejectDelete))

	// Penalty Structure Master
	penaltyStructureMF := investmentMasters.PenaltyStructureMasterFilesHandlers(pgxPool)
	mux.Handle("/master/penalty-structure/additional-files/list", midInvestFD(penaltyStructureMF.List))
	mux.Handle("/master/penalty-structure/additional-files/upload", midInvestFD(penaltyStructureMF.Upload))
	mux.Handle("/master/penalty-structure/additional-files/download", midInvestFD(penaltyStructureMF.Download))
	mux.Handle("/master/penalty-structure/additional-files/download-bulk", midInvestFD(penaltyStructureMF.DownloadBulk))
	mux.Handle("/master/penalty-structure/additional-files/delete", midInvestFD(penaltyStructureMF.Delete))
	mux.Handle("/master/penalty-structure/additional-files/audit", midInvestFD(penaltyStructureMF.Audit))
	mux.Handle("/master/penalty-structure/additional-files/approve-delete", midInvestFD(penaltyStructureMF.ApproveDelete))
	mux.Handle("/master/penalty-structure/additional-files/reject-delete", midInvestFD(penaltyStructureMF.RejectDelete))

	// Compounding Frequency Master
	compoundingFrequencyMF := investmentMasters.CompoundingFrequencyMasterFilesHandlers(pgxPool)
	mux.Handle("/master/compounding-frequency/additional-files/list", midInvestFD(compoundingFrequencyMF.List))
	mux.Handle("/master/compounding-frequency/additional-files/upload", midInvestFD(compoundingFrequencyMF.Upload))
	mux.Handle("/master/compounding-frequency/additional-files/download", midInvestFD(compoundingFrequencyMF.Download))
	mux.Handle("/master/compounding-frequency/additional-files/download-bulk", midInvestFD(compoundingFrequencyMF.DownloadBulk))
	mux.Handle("/master/compounding-frequency/additional-files/delete", midInvestFD(compoundingFrequencyMF.Delete))
	mux.Handle("/master/compounding-frequency/additional-files/audit", midInvestFD(compoundingFrequencyMF.Audit))
	mux.Handle("/master/compounding-frequency/additional-files/approve-delete", midInvestFD(compoundingFrequencyMF.ApproveDelete))
	mux.Handle("/master/compounding-frequency/additional-files/reject-delete", midInvestFD(compoundingFrequencyMF.RejectDelete))

	// TDS Plan Master
	tdsPlanMF := investmentMasters.TDSPlanMasterFilesHandlers(pgxPool)
	mux.Handle("/master/tds-plan/additional-files/list", midInvestFD(tdsPlanMF.List))
	mux.Handle("/master/tds-plan/additional-files/upload", midInvestFD(tdsPlanMF.Upload))
	mux.Handle("/master/tds-plan/additional-files/download", midInvestFD(tdsPlanMF.Download))
	mux.Handle("/master/tds-plan/additional-files/download-bulk", midInvestFD(tdsPlanMF.DownloadBulk))
	mux.Handle("/master/tds-plan/additional-files/delete", midInvestFD(tdsPlanMF.Delete))
	mux.Handle("/master/tds-plan/additional-files/audit", midInvestFD(tdsPlanMF.Audit))
	mux.Handle("/master/tds-plan/additional-files/approve-delete", midInvestFD(tdsPlanMF.ApproveDelete))
	mux.Handle("/master/tds-plan/additional-files/reject-delete", midInvestFD(tdsPlanMF.RejectDelete))

	// Calendar (Holiday) Master
	calendarMF := investmentMasters.CalendarMasterFilesHandlers(pgxPool)
	mux.Handle("/master/calendar/additional-files/list", midGlobalIndep(calendarMF.List))
	mux.Handle("/master/calendar/additional-files/upload", midGlobalIndep(calendarMF.Upload))
	mux.Handle("/master/calendar/additional-files/download", midGlobalIndep(calendarMF.Download))
	mux.Handle("/master/calendar/additional-files/download-bulk", midGlobalIndep(calendarMF.DownloadBulk))
	mux.Handle("/master/calendar/additional-files/delete", midGlobalIndep(calendarMF.Delete))
	mux.Handle("/master/calendar/additional-files/audit", midGlobalIndep(calendarMF.Audit))
	mux.Handle("/master/calendar/additional-files/approve-delete", midGlobalIndep(calendarMF.ApproveDelete))
	mux.Handle("/master/calendar/additional-files/reject-delete", midGlobalIndep(calendarMF.RejectDelete))

	// Day Count Convention Master
	dayCountConventionMF := investmentMasters.DayCountConventionMasterFilesHandlers(pgxPool)
	mux.Handle("/master/day-count-convention/additional-files/list", midInvestFD(dayCountConventionMF.List))
	mux.Handle("/master/day-count-convention/additional-files/upload", midInvestFD(dayCountConventionMF.Upload))
	mux.Handle("/master/day-count-convention/additional-files/download", midInvestFD(dayCountConventionMF.Download))
	mux.Handle("/master/day-count-convention/additional-files/download-bulk", midInvestFD(dayCountConventionMF.DownloadBulk))
	mux.Handle("/master/day-count-convention/additional-files/delete", midInvestFD(dayCountConventionMF.Delete))
	mux.Handle("/master/day-count-convention/additional-files/audit", midInvestFD(dayCountConventionMF.Audit))
	mux.Handle("/master/day-count-convention/additional-files/approve-delete", midInvestFD(dayCountConventionMF.ApproveDelete))
	mux.Handle("/master/day-count-convention/additional-files/reject-delete", midInvestFD(dayCountConventionMF.RejectDelete))

	// Bank Config Master
	bankConfigMF := investmentMasters.BankConfigMasterFilesHandlers(pgxPool)
	mux.Handle("/master/bank-config/additional-files/list", midInvestFD(bankConfigMF.List))
	mux.Handle("/master/bank-config/additional-files/upload", midInvestFD(bankConfigMF.Upload))
	mux.Handle("/master/bank-config/additional-files/download", midInvestFD(bankConfigMF.Download))
	mux.Handle("/master/bank-config/additional-files/download-bulk", midInvestFD(bankConfigMF.DownloadBulk))
	mux.Handle("/master/bank-config/additional-files/delete", midInvestFD(bankConfigMF.Delete))
	mux.Handle("/master/bank-config/additional-files/audit", midInvestFD(bankConfigMF.Audit))
	mux.Handle("/master/bank-config/additional-files/approve-delete", midInvestFD(bankConfigMF.ApproveDelete))
	mux.Handle("/master/bank-config/additional-files/reject-delete", midInvestFD(bankConfigMF.RejectDelete))

	// Bank Rate Card Master
	bankRateCardMF := investmentMasters.BankRateCardMasterFilesHandlers(pgxPool)
	mux.Handle("/master/bank-rate-card/additional-files/list", midInvestFD(bankRateCardMF.List))
	mux.Handle("/master/bank-rate-card/additional-files/upload", midInvestFD(bankRateCardMF.Upload))
	mux.Handle("/master/bank-rate-card/additional-files/download", midInvestFD(bankRateCardMF.Download))
	mux.Handle("/master/bank-rate-card/additional-files/download-bulk", midInvestFD(bankRateCardMF.DownloadBulk))
	mux.Handle("/master/bank-rate-card/additional-files/delete", midInvestFD(bankRateCardMF.Delete))
	mux.Handle("/master/bank-rate-card/additional-files/audit", midInvestFD(bankRateCardMF.Audit))
	mux.Handle("/master/bank-rate-card/additional-files/approve-delete", midInvestFD(bankRateCardMF.ApproveDelete))
	mux.Handle("/master/bank-rate-card/additional-files/reject-delete", midInvestFD(bankRateCardMF.RejectDelete))

	// Counterparty Hub Master routes
	counterpartyHub.RegisterCounterpartyHubRoutes(mux, pgxPool, db)

	logger.LogInfo("Master Service started on :%s", port)
	err = http.ListenAndServe(":"+port, observability.WrapHTTP(serviceName, mux))
	if err != nil {
		logger.LogError("Master Service failed: %v", err)
	}
}
