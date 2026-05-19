package master

import (
	allMaster "CimplrCorpSaas/api/master/allMasters"
	counterpartyHub "CimplrCorpSaas/api/master/counterpartyHub"
	investmentMasters "CimplrCorpSaas/api/master/investmentMasters"
	middlewares "CimplrCorpSaas/api/middlewares"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

func StartMasterService(db *sql.DB, port string) {
	mux := http.NewServeMux()
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	sslMode := os.Getenv("DB_SSLMODE")
	if sslMode == "" {
		sslMode = "disable"
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, pass, host, dbPort, name, sslMode)
	pgxPool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		logger.LogError("failed to connect to pgxpool DB: %v", err)
	}
	// ensure pool is closed when service exits
	defer pgxPool.Close()

	mux.HandleFunc("/master/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Masters Service is healthy"))
	})

	// Cost / Profit Center Master routes
	mux.Handle("/master/v2/costprofit-center/bulk-create-sync", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreateAndSyncCostProfitCenters(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/upload", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadAndSyncCostProfitCenters(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/updatebulk", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateAndSyncCostProfitCenters(pgxPool)))
	mux.Handle("/master/costprofit-center/approved-active", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetApprovedActiveCostProfitCenters(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/hierarchy", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetCostProfitCenterHierarchy(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/find-parent-at-level", middlewares.PreValidationMiddleware(pgxPool)(allMaster.FindParentCostProfitCenterAtLevel(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.DeleteCostProfitCenter(pgxPool)))
	mux.Handle("/master/costprofit-center/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectCostProfitCenterActions(pgxPool)))
	mux.Handle("/master/costprofit-center/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApproveCostProfitCenterActions(pgxPool)))

	mux.Handle("/master/costprofit-center/upload-simple", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadCostProfitCenterSimple(pgxPool)))

	// Payable/Receivable Master routes
	mux.Handle("/master/v2/payablereceivable/create", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreatePayableReceivableTypes(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/names", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetPayableReceivableNamesWithID(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/updatebulk", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdatePayableReceivableBulk(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.DeletePayableReceivable(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApprovePayableReceivableActions(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectPayableReceivableActions(pgxPool)))
	mux.Handle("/master/payablereceivable/upload", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadPayableReceivable(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/approved-active", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetApprovedActivePayableReceivable(pgxPool)))

	// Counterparty Master routes
	mux.Handle("/master/v2/counterparty/create", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreateCounterparties(pgxPool)))
	mux.Handle("/master/v2/counterparty/names", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetCounterpartyNamesWithID(pgxPool)))
	mux.Handle("/master/v2counterparty/updatebulk", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateCounterpartyBulk(pgxPool)))
	mux.Handle("/master/counterparty/delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.DeleteCounterparty(pgxPool)))
	mux.Handle("/master/v2/counterparty/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApproveCounterpartyActions(pgxPool)))
	mux.Handle("/master/counterparty/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectCounterpartyActions(pgxPool)))
	mux.Handle("/master/v2/counterparty/upload", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadCounterparty(pgxPool)))

	mux.Handle("/master/counterparty/upload-simple", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadCounterpartySimple(pgxPool)))
	mux.Handle("/master/counterparty/banks/upload-simple", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadCounterpartyBankSimple(pgxPool)))

	mux.Handle("/master/counterparty/approved-active", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetApprovedActiveCounterparties(pgxPool)))

	mux.Handle("/master/v2/counterparty/banks", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetCounterpartyBanks(pgxPool)))
	mux.Handle("/master/v2/counterparty/banks/updatebulk", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateCounterpartyBanksBulk(pgxPool)))

	// GL Account Master routes
	mux.Handle("/master/v2/glaccount/create", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreateGLAccounts(pgxPool)))
	mux.Handle("/master/v2/glaccount/hierarchy", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetGLAccountNamesWithID(pgxPool)))
	mux.Handle("/master/v2/glaccount/updatebulk", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateAndSyncGLAccounts(pgxPool)))
	mux.Handle("/master/v2/glaccount/delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.DeleteGLAccount(pgxPool)))
	mux.Handle("/master/v2/glaccount/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApproveGLAccountActions(pgxPool)))
	mux.Handle("/master/v2/glaccount/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectGLAccountActions(pgxPool)))
	mux.Handle("/master/glaccount/upload", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadGLAccount(pgxPool)))
	mux.Handle("/master/glaccount/approved-active", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetApprovedActiveGLAccounts(pgxPool)))
	mux.Handle("/master/v2/glaccount/find-parent-at-level", middlewares.PreValidationMiddleware(pgxPool)(allMaster.FindParentGLAccountAtLevel(pgxPool)))
	mux.Handle("/master/glaccount/upload-simple", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadGLAccountSimple(pgxPool)))

	// Cash Flow Category Master routes
	mux.Handle("/master/cashflow-category/delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.DeleteCashFlowCategory(pgxPool)))
	mux.Handle("/master/v2/cashflow-category/hierarchy", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetCashFlowCategoryHierarchyPGX(pgxPool)))
	mux.Handle("/master/cashflow-category/find-parent-at-level", middlewares.PreValidationMiddleware(pgxPool)(allMaster.FindParentCashFlowCategoryAtLevel(pgxPool)))
	mux.Handle("/master/cashflow-category/names", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetCashFlowCategoryNamesWithID(pgxPool)))
	mux.Handle("/master/v2/cashflow-category/updatebulk", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateCashFlowCategoryBulk(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApproveCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/v2/cashflow-category/bulk-create-sync", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreateAndSyncCashFlowCategories(pgxPool)))
	mux.Handle("/master/cashflow-category/upload", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadCashFlowCategory(pgxPool)))

	mux.Handle("/master/cashflow-category/upload-simple", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadCashFlowCategorySimple(pgxPool)))
	// Currency Master routes (pgx-backed)
	mux.Handle("/master/currency/create", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreateCurrencyMaster(pgxPool)))
	mux.Handle("/master/currency/all", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetAllCurrencyMaster(pgxPool)))
	mux.Handle("/master/currency/update", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateCurrencyMasterBulk(pgxPool)))
	mux.Handle("/master/currency/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApproveAuditActions(pgxPool)))
	mux.Handle("/master/currency/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectAuditActions(pgxPool)))
	mux.Handle("/master/currency/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkDeleteCurrencyAudit(pgxPool)))
	mux.Handle("/master/currency/active-approved", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetActiveApprovedCurrencyCodes(pgxPool)))

	// Bank Master routes (pgx-backed)
	mux.Handle("/master/bank/create", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreateBankMaster(pgxPool)))
	mux.Handle("/master/bank/upload", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadBank(pgxPool)))
	mux.Handle("/master/bank/all", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetAllBankMaster(pgxPool)))
	mux.Handle("/master/bank/names", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetBankNamesWithID(pgxPool)))
	mux.Handle("/master/bank/update", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateBankMasterBulk(pgxPool)))
	mux.Handle("/master/bank/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApproveBankAuditActions(pgxPool)))
	mux.Handle("/master/bank/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectBankAuditActions(pgxPool)))
	mux.Handle("/master/bank/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkDeleteBankAudit(pgxPool)))

	// Bank Account Master routes
	mux.Handle("/master/bankaccount/create", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreateBankAccountMaster(pgxPool)))
	mux.Handle("/master/bankaccount/upload", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadBankAccount(pgxPool)))
	// mux.Handle("/master/bankaccount/all", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetAllBankAccountMaster(pgxPool)))
	mux.Handle("/master/bankaccount/update", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateBankAccountMasterBulk(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkDeleteBankAccountAudit(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectBankAccountAuditActions(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApproveBankAccountAuditActions(pgxPool)))
	mux.Handle("/master/bankaccount/names", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetBankNamesWithIDForAccount(pgxPool)))
	mux.Handle("/master/bankaccount/approved-with-entity", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetApprovedBankAccountsWithBankEntity(pgxPool)))
	mux.Handle("/master/bankaccount/pre-populate", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetApprovedBankAccountsSimple(pgxPool)))
	mux.Handle("/master/bankaccount/all", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetBankAccountMetaAll(pgxPool)))
	mux.Handle("/master/bankaccount/for-user", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetBankAccountsForUser(pgxPool)))

	// Entity Master routes
	mux.Handle("/master/entity/bulk-create-sync", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreateAndSyncEntities(db)))
	mux.Handle("/master/entity/hierarchy", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetEntityHierarchy(db)))
	mux.Handle("/master/entity/delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.DeleteEntity(db)))
	mux.Handle("/master/entity/findParentAtLevel", middlewares.PreValidationMiddleware(pgxPool)(allMaster.FindParentAtLevel(db)))
	mux.Handle("/master/entity/approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.ApproveEntity(db)))
	mux.Handle("/master/entity/reject-bulk", middlewares.PreValidationMiddleware(pgxPool)(allMaster.RejectEntitiesBulk(db)))
	mux.Handle("/master/entity/update", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateEntity(db)))
	mux.Handle("/master/entity/all-names", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetAllEntityNames(db)))
	mux.Handle("/master/entity/render-vars-hierarchical", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetRenderVarsHierarchical(db)))

	// Entity Cash Master routes (pgx-backed)
	mux.Handle("/master/entitycash/bulk-create-sync", middlewares.PreValidationMiddleware(pgxPool)(allMaster.CreateAndSyncCashEntities(pgxPool)))
	mux.Handle("/master/entitycash/upload", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadEntityCash(pgxPool)))
	mux.Handle("/master/entitycash/upload-simple", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UploadEntitySimple(pgxPool)))
	mux.Handle("/master/entitycash/hierarchy", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetCashEntityHierarchy(pgxPool)))
	mux.Handle("/master/entitycash/logo-url", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetCashEntityLogoURL(pgxPool)))
	mux.Handle("/master/entitycash/updatebulk", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateCashEntityBulk(pgxPool)))
	mux.Handle("/master/entitycash/find-parent-at-level", middlewares.PreValidationMiddleware(pgxPool)(allMaster.FindParentCashEntityAtLevel(pgxPool)))
	mux.Handle("/master/entitycash/delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.DeleteCashEntity(pgxPool)))
	mux.Handle("/master/entitycash/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApproveCashEntityActions(pgxPool)))
	mux.Handle("/master/entitycash/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectCashEntityActions(pgxPool)))
	mux.Handle("/master/entitycash/assigned-names", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetAssignedCashEntityNamesWithID(pgxPool)))
	mux.Handle("/master/entitycash/all-names", middlewares.PreValidationMiddleware(pgxPool)(allMaster.GetCashEntityNamesWithID(pgxPool)))

	mux.Handle("/master/amc/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateAMCsingle(pgxPool)))
	mux.Handle("/master/amc/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateAMC(pgxPool)))
	mux.Handle("/master/amc/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateAMCBulk(pgxPool)))
	mux.Handle("/master/amc/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateAMC(pgxPool)))
	mux.Handle("/master/amc/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteAMC(pgxPool)))
	mux.Handle("/master/amc/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveAMCActions(pgxPool)))
	mux.Handle("/master/amc/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectAMCActions(pgxPool)))
	mux.Handle("/master/amc/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetAMCsWithAudit(pgxPool)))
	mux.Handle("/master/amc/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetApprovedActiveAMCs(pgxPool)))
	mux.Handle("/master/amc/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadAMCSimple(pgxPool)))

	// Scheme Master routes
	mux.Handle("/master/scheme/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadSchemeSimple(pgxPool)))
	mux.Handle("/master/scheme/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateSchemeSingle(pgxPool)))
	mux.Handle("/master/scheme/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateScheme(pgxPool)))
	mux.Handle("/master/scheme/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateScheme(pgxPool)))
	mux.Handle("/master/scheme/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateSchemeBulk(pgxPool)))
	mux.Handle("/master/scheme/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteScheme(pgxPool)))
	mux.Handle("/master/scheme/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveSchemeActions(pgxPool)))
	mux.Handle("/master/scheme/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectSchemeActions(pgxPool)))
	mux.Handle("/master/scheme/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetApprovedActiveSchemes(pgxPool)))
	mux.Handle("/master/scheme/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetSchemesWithAudit(pgxPool)))
	mux.Handle("/master/scheme/by-amc", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetApprovedActiveSchemesByAMC(pgxPool)))

	// DP Master routes
	mux.Handle("/master/dp/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadDPSimple(pgxPool)))
	mux.Handle("/master/dp/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateDPSingle(pgxPool)))
	mux.Handle("/master/dp/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateDPBulk(pgxPool)))
	mux.Handle("/master/dp/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateDP(pgxPool)))
	mux.Handle("/master/dp/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateDPBulk(pgxPool)))
	mux.Handle("/master/dp/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteDP(pgxPool)))
	mux.Handle("/master/dp/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveDPActions(pgxPool)))
	mux.Handle("/master/dp/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectDPActions(pgxPool)))
	mux.Handle("/master/dp/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetApprovedActiveDPs(pgxPool)))
	mux.Handle("/master/dp/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetDPsWithAudit(pgxPool)))

	// Demat Master routes
	mux.Handle("/master/demat/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadDematSimple(pgxPool)))
	mux.Handle("/master/demat/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateDematSingle(pgxPool)))
	mux.Handle("/master/demat/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateDematBulk(pgxPool)))
	mux.Handle("/master/demat/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateDemat(pgxPool)))
	mux.Handle("/master/demat/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateDematBulk(pgxPool)))
	mux.Handle("/master/demat/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteDemat(pgxPool)))
	mux.Handle("/master/demat/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveDematActions(pgxPool)))
	mux.Handle("/master/demat/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectDematActions(pgxPool)))
	mux.Handle("/master/demat/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetApprovedActiveDemats(pgxPool)))
	mux.Handle("/master/demat/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetDematsWithAudit(pgxPool)))

	// Interest Type Master routes
	mux.Handle("/master/interest-type/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateInterestTypeSingle(pgxPool)))
	mux.Handle("/master/interest-type/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateInterestType(pgxPool)))
	mux.Handle("/master/interest-type/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateInterestType(pgxPool)))
	mux.Handle("/master/interest-type/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateInterestTypeBulk(pgxPool)))
	mux.Handle("/master/interest-type/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteInterestType(pgxPool)))
	mux.Handle("/master/interest-type/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveInterestType(pgxPool)))
	mux.Handle("/master/interest-type/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectInterestType(pgxPool)))
	mux.Handle("/master/interest-type/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetInterestTypesApprovedActive(pgxPool)))
	mux.Handle("/master/interest-type/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetInterestTypesWithAudit(pgxPool)))
	mux.Handle("/master/interest-type/audit-history", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetInterestTypeAuditHistory(pgxPool)))
	mux.Handle("/master/interest-type/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadInterestTypeSimple(pgxPool)))
	mux.Handle("/master/interest-type/get", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetInterestType(pgxPool)))

	// Penalty Structure Master routes
	mux.Handle("/master/penalty-structure/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreatePenaltyStructureSingle(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreatePenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdatePenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdatePenaltyStructureBulk(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeletePenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApprovePenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectPenaltyStructure(pgxPool)))
	mux.Handle("/master/penalty-structure/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetPenaltyStructuresApprovedActive(pgxPool)))
	mux.Handle("/master/penalty-structure/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetPenaltyStructuresWithAudit(pgxPool)))
	mux.Handle("/master/penalty-structure/audit-history", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetPenaltyStructureAuditHistory(pgxPool)))
	mux.Handle("/master/penalty-structure/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadPenaltyStructureSimple(pgxPool)))
	mux.Handle("/master/penalty-structure/get", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetPenaltyStructure(pgxPool)))

	// Compounding Frequency Master routes
	mux.Handle("/master/compounding-frequency/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateCompoundingFrequencySingle(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateCompoundingFrequencyBulk(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectCompoundingFrequency(pgxPool)))
	mux.Handle("/master/compounding-frequency/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetCompoundingFrequenciesApprovedActive(pgxPool)))
	mux.Handle("/master/compounding-frequency/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetCompoundingFrequenciesWithAudit(pgxPool)))
	mux.Handle("/master/compounding-frequency/audit-history", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetCompoundingFrequencyAuditHistory(pgxPool)))
	mux.Handle("/master/compounding-frequency/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadCompoundingFrequencySimple(pgxPool)))
	mux.Handle("/master/compounding-frequency/get", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetCompoundingFrequency(pgxPool)))

	// TDS Plan Master routes
	mux.Handle("/master/tds-plan/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateTDSPlanSingle(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateTDSPlanBulk(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectTDSPlan(pgxPool)))
	mux.Handle("/master/tds-plan/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetTDSPlansApprovedActive(pgxPool)))
	mux.Handle("/master/tds-plan/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetTDSPlansWithAudit(pgxPool)))
	mux.Handle("/master/tds-plan/audit-history", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetTDSPlanAuditHistory(pgxPool)))
	mux.Handle("/master/tds-plan/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadTDSPlanSimple(pgxPool)))
	mux.Handle("/master/tds-plan/get", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetTDSPlan(pgxPool)))

	// Folio Master routes
	mux.Handle("/master/folio/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadFolio(pgxPool)))
	mux.Handle("/master/folio/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetFoliosWithAudit(pgxPool)))
	mux.Handle("/master/folio/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetApprovedActiveFolios(pgxPool)))
	mux.Handle("/master/folio/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateFolioSingle(pgxPool)))
	mux.Handle("/master/folio/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateFolioBulk(pgxPool)))
	mux.Handle("/master/folio/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateFolio(pgxPool)))
	mux.Handle("/master/folio/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateFolioBulk(pgxPool)))
	mux.Handle("/master/folio/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteFolio(pgxPool)))
	mux.Handle("/master/folio/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveFolioActions(pgxPool)))
	mux.Handle("/master/folio/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectFolioActions(pgxPool)))
	mux.Handle("/master/folio/meta", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetSingleFolioWithAudit(pgxPool)))
	mux.Handle("/master/folio/schemes-by-approved", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetSchemesByApprovedFolios(pgxPool)))

	// AMFI Config Master routes
	mux.Handle("/master/amfi/scheme", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetAMFISchemeMasterSimple(pgxPool)))
	mux.Handle("/master/amfi/nav", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetAMFINavStagingSimple(pgxPool)))
	mux.Handle("/master/amfi/approved-amc", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetApprovedAMCsAndSchemes(pgxPool)))
	mux.Handle("/master/amfi/distinct-amcs", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetDistinctAMCNamesFromAMFI(pgxPool)))

	// Holiday calendar exports (ICS/feed/share links)
	mux.Handle("/master/calendar/export/ics/", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.ExportCalendarICS(pgxPool)))
	mux.Handle("/master/calendar/feed/", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CalendarFeedICS(pgxPool)))
	mux.Handle("/master/calendar/share-links/", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.ShareLinksCalendar(pgxPool)))

	// Calendar Master routes (pgx-backed)
	mux.Handle("/master/calendar/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateCalendarSingle(pgxPool)))
	mux.Handle("/master/calendar/holiday/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateHolidayBulk(pgxPool)))
	mux.Handle("/master/calendar/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadCalendarBulk(pgxPool)))
	mux.Handle("/master/calendar/holiday/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadHolidayBulk(pgxPool)))
	mux.Handle("/master/calendar/meta", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetCalendarsWithAudit(pgxPool)))
	mux.Handle("/master/calendar/list", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetCalendarListLite(pgxPool)))
	mux.Handle("/master/calendar/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetCalendarWithHolidays(pgxPool)))
	mux.Handle("/master/calendar/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetApprovedActiveCalendars(pgxPool)))
	mux.Handle("/master/calendar/view", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetCalendarWithHolidaysApproved(pgxPool)))
	mux.Handle("/master/calendar/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteCalendar(pgxPool)))
	mux.Handle("/master/calendar/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveCalendarActions(pgxPool)))
	mux.Handle("/master/calendar/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectCalendarActions(pgxPool)))
	mux.Handle("/master/calendar/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateCalendar(pgxPool)))
	mux.Handle("/master/calendar/holiday/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateHoliday(pgxPool)))
	mux.Handle("/master/calendar/update-with-holidays", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateCalendarWithHolidays(pgxPool)))
	mux.Handle("/master/calendar/years", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetPastYearsHolidays(pgxPool)))

	// Day Count Convention Master routes
	mux.Handle("/master/day-count-convention/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateDayCountConventionSingle(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateDayCountConventionBulk(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectDayCountConvention(pgxPool)))
	mux.Handle("/master/day-count-convention/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetDayCountConventionsApprovedActive(pgxPool)))
	mux.Handle("/master/day-count-convention/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetDayCountConventionsWithAudit(pgxPool)))
	mux.Handle("/master/day-count-convention/audit-history", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetDayCountConventionAuditHistory(pgxPool)))
	mux.Handle("/master/day-count-convention/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadDayCountConventionSimple(pgxPool)))
	mux.Handle("/master/day-count-convention/get", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetDayCountConvention(pgxPool)))

	// Bank Config Master routes
	mux.Handle("/master/bank-config/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateBankConfigSingle(pgxPool)))
	mux.Handle("/master/bank-config/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateBankConfigBulk(pgxPool)))
	mux.Handle("/master/bank-config/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectBankConfig(pgxPool)))
	mux.Handle("/master/bank-config/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetBankConfigsApprovedActive(pgxPool)))
	mux.Handle("/master/bank-config/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetBankConfigsWithAudit(pgxPool)))
	mux.Handle("/master/bank-config/audit-history", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetBankConfigAuditHistory(pgxPool)))
	mux.Handle("/master/bank-config/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadBankConfigSimple(pgxPool)))
	mux.Handle("/master/bank-config/get", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetBankConfig(pgxPool)))

	// Bank Rate Card Master routes
	mux.Handle("/master/bank-rate-card/create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateBankRateCardSingle(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-create", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.CreateBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-update", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UpdateBankRateCardBulk(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-delete", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.DeleteBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkApproveBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.BulkRejectBankRateCard(pgxPool)))
	mux.Handle("/master/bank-rate-card/approved-active", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetBankRateCardsApprovedActive(pgxPool)))
	mux.Handle("/master/bank-rate-card/all", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetBankRateCardsWithAudit(pgxPool)))
	mux.Handle("/master/bank-rate-card/audit-history", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetBankRateCardAuditHistory(pgxPool)))
	mux.Handle("/master/bank-rate-card/upload", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.UploadBankRateCardSimple(pgxPool)))
	mux.Handle("/master/bank-rate-card/get", middlewares.PreValidationMiddleware(pgxPool)(investmentMasters.GetBankRateCard(pgxPool)))

	// ── Additional Files routes (supporting document uploads per master record) ──

	// Bank Master
	bankMF := allMaster.BankMasterFilesHandlers(pgxPool)
	mux.Handle("/master/bank/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(bankMF.List))
	mux.Handle("/master/bank/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(bankMF.Upload))
	mux.Handle("/master/bank/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(bankMF.Download))
	mux.Handle("/master/bank/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(bankMF.DownloadBulk))
	mux.Handle("/master/bank/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(bankMF.Delete))
	mux.Handle("/master/bank/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(bankMF.Audit))
	mux.Handle("/master/bank/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(bankMF.ApproveDelete))
	mux.Handle("/master/bank/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(bankMF.RejectDelete))

	// Currency Master
	currencyMF := allMaster.CurrencyMasterFilesHandlers(pgxPool)
	mux.Handle("/master/currency/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(currencyMF.List))
	mux.Handle("/master/currency/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(currencyMF.Upload))
	mux.Handle("/master/currency/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(currencyMF.Download))
	mux.Handle("/master/currency/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(currencyMF.DownloadBulk))
	mux.Handle("/master/currency/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(currencyMF.Delete))
	mux.Handle("/master/currency/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(currencyMF.Audit))
	mux.Handle("/master/currency/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(currencyMF.ApproveDelete))
	mux.Handle("/master/currency/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(currencyMF.RejectDelete))

	// Bank Account Master
	bankAccountMF := allMaster.BankAccountMasterFilesHandlers(pgxPool)
	mux.Handle("/master/bankaccount/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(bankAccountMF.List))
	mux.Handle("/master/bankaccount/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(bankAccountMF.Upload))
	mux.Handle("/master/bankaccount/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(bankAccountMF.Download))
	mux.Handle("/master/bankaccount/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(bankAccountMF.DownloadBulk))
	mux.Handle("/master/bankaccount/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(bankAccountMF.Delete))
	mux.Handle("/master/bankaccount/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(bankAccountMF.Audit))
	mux.Handle("/master/bankaccount/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(bankAccountMF.ApproveDelete))
	mux.Handle("/master/bankaccount/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(bankAccountMF.RejectDelete))

	// Counterparty Master
	counterpartyMF := allMaster.CounterpartyMasterFilesHandlers(pgxPool)
	mux.Handle("/master/counterparty/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(counterpartyMF.List))
	mux.Handle("/master/counterparty/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(counterpartyMF.Upload))
	mux.Handle("/master/counterparty/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(counterpartyMF.Download))
	mux.Handle("/master/counterparty/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(counterpartyMF.DownloadBulk))
	mux.Handle("/master/counterparty/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(counterpartyMF.Delete))
	mux.Handle("/master/counterparty/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(counterpartyMF.Audit))
	mux.Handle("/master/counterparty/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(counterpartyMF.ApproveDelete))
	mux.Handle("/master/counterparty/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(counterpartyMF.RejectDelete))

	// GL Account Master
	glAccountMF := allMaster.GLAccountMasterFilesHandlers(pgxPool)
	mux.Handle("/master/glaccount/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(glAccountMF.List))
	mux.Handle("/master/glaccount/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(glAccountMF.Upload))
	mux.Handle("/master/glaccount/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(glAccountMF.Download))
	mux.Handle("/master/glaccount/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(glAccountMF.DownloadBulk))
	mux.Handle("/master/glaccount/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(glAccountMF.Delete))
	mux.Handle("/master/glaccount/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(glAccountMF.Audit))
	mux.Handle("/master/glaccount/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(glAccountMF.ApproveDelete))
	mux.Handle("/master/glaccount/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(glAccountMF.RejectDelete))

	// Cash Flow Category Master
	cashflowCategoryMF := allMaster.CashFlowCategoryMasterFilesHandlers(pgxPool)
	mux.Handle("/master/cashflow-category/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(cashflowCategoryMF.List))
	mux.Handle("/master/cashflow-category/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(cashflowCategoryMF.Upload))
	mux.Handle("/master/cashflow-category/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(cashflowCategoryMF.Download))
	mux.Handle("/master/cashflow-category/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(cashflowCategoryMF.DownloadBulk))
	mux.Handle("/master/cashflow-category/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(cashflowCategoryMF.Delete))
	mux.Handle("/master/cashflow-category/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(cashflowCategoryMF.Audit))
	mux.Handle("/master/cashflow-category/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(cashflowCategoryMF.ApproveDelete))
	mux.Handle("/master/cashflow-category/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(cashflowCategoryMF.RejectDelete))

	// Cost Profit Center Master
	costProfitCenterMF := allMaster.CostProfitCenterMasterFilesHandlers(pgxPool)
	mux.Handle("/master/costprofit-center/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(costProfitCenterMF.List))
	mux.Handle("/master/costprofit-center/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(costProfitCenterMF.Upload))
	mux.Handle("/master/costprofit-center/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(costProfitCenterMF.Download))
	mux.Handle("/master/costprofit-center/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(costProfitCenterMF.DownloadBulk))
	mux.Handle("/master/costprofit-center/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(costProfitCenterMF.Delete))
	mux.Handle("/master/costprofit-center/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(costProfitCenterMF.Audit))
	mux.Handle("/master/costprofit-center/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(costProfitCenterMF.ApproveDelete))
	mux.Handle("/master/costprofit-center/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(costProfitCenterMF.RejectDelete))

	// Payable Receivable Master
	payableReceivableMF := allMaster.PayableReceivableMasterFilesHandlers(pgxPool)
	mux.Handle("/master/payablereceivable/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(payableReceivableMF.List))
	mux.Handle("/master/payablereceivable/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(payableReceivableMF.Upload))
	mux.Handle("/master/payablereceivable/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(payableReceivableMF.Download))
	mux.Handle("/master/payablereceivable/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(payableReceivableMF.DownloadBulk))
	mux.Handle("/master/payablereceivable/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(payableReceivableMF.Delete))
	mux.Handle("/master/payablereceivable/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(payableReceivableMF.Audit))
	mux.Handle("/master/payablereceivable/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(payableReceivableMF.ApproveDelete))
	mux.Handle("/master/payablereceivable/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(payableReceivableMF.RejectDelete))

	// Entity Cash Master
	entityCashMF := allMaster.EntityCashMasterFilesHandlers(pgxPool)
	mux.Handle("/master/entitycash/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(entityCashMF.List))
	mux.Handle("/master/entitycash/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(entityCashMF.Upload))
	mux.Handle("/master/entitycash/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(entityCashMF.Download))
	mux.Handle("/master/entitycash/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(entityCashMF.DownloadBulk))
	mux.Handle("/master/entitycash/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(entityCashMF.Delete))
	mux.Handle("/master/entitycash/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(entityCashMF.Audit))
	mux.Handle("/master/entitycash/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(entityCashMF.ApproveDelete))
	mux.Handle("/master/entitycash/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(entityCashMF.RejectDelete))

	// Entity Master
	entityMF := allMaster.EntityMasterFilesHandlers(pgxPool)
	mux.Handle("/master/entity/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(entityMF.List))
	mux.Handle("/master/entity/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(entityMF.Upload))
	mux.Handle("/master/entity/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(entityMF.Download))
	mux.Handle("/master/entity/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(entityMF.DownloadBulk))
	mux.Handle("/master/entity/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(entityMF.Delete))
	mux.Handle("/master/entity/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(entityMF.Audit))
	mux.Handle("/master/entity/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(entityMF.ApproveDelete))
	mux.Handle("/master/entity/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(entityMF.RejectDelete))

	// AMC Master
	amcMF := investmentMasters.AMCMasterFilesHandlers(pgxPool)
	mux.Handle("/master/amc/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(amcMF.List))
	mux.Handle("/master/amc/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(amcMF.Upload))
	mux.Handle("/master/amc/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(amcMF.Download))
	mux.Handle("/master/amc/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(amcMF.DownloadBulk))
	mux.Handle("/master/amc/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(amcMF.Delete))
	mux.Handle("/master/amc/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(amcMF.Audit))
	mux.Handle("/master/amc/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(amcMF.ApproveDelete))
	mux.Handle("/master/amc/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(amcMF.RejectDelete))

	// Scheme Master
	schemeMF := investmentMasters.SchemeMasterFilesHandlers(pgxPool)
	mux.Handle("/master/scheme/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(schemeMF.List))
	mux.Handle("/master/scheme/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(schemeMF.Upload))
	mux.Handle("/master/scheme/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(schemeMF.Download))
	mux.Handle("/master/scheme/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(schemeMF.DownloadBulk))
	mux.Handle("/master/scheme/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(schemeMF.Delete))
	mux.Handle("/master/scheme/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(schemeMF.Audit))
	mux.Handle("/master/scheme/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(schemeMF.ApproveDelete))
	mux.Handle("/master/scheme/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(schemeMF.RejectDelete))

	// DP (Depository Participant) Master
	dpMF := investmentMasters.DPMasterFilesHandlers(pgxPool)
	mux.Handle("/master/dp/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(dpMF.List))
	mux.Handle("/master/dp/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(dpMF.Upload))
	mux.Handle("/master/dp/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(dpMF.Download))
	mux.Handle("/master/dp/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(dpMF.DownloadBulk))
	mux.Handle("/master/dp/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(dpMF.Delete))
	mux.Handle("/master/dp/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(dpMF.Audit))
	mux.Handle("/master/dp/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(dpMF.ApproveDelete))
	mux.Handle("/master/dp/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(dpMF.RejectDelete))

	// Demat Master
	dematMF := investmentMasters.DematMasterFilesHandlers(pgxPool)
	mux.Handle("/master/demat/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(dematMF.List))
	mux.Handle("/master/demat/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(dematMF.Upload))
	mux.Handle("/master/demat/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(dematMF.Download))
	mux.Handle("/master/demat/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(dematMF.DownloadBulk))
	mux.Handle("/master/demat/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(dematMF.Delete))
	mux.Handle("/master/demat/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(dematMF.Audit))
	mux.Handle("/master/demat/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(dematMF.ApproveDelete))
	mux.Handle("/master/demat/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(dematMF.RejectDelete))

	// Folio Master
	folioMF := investmentMasters.FolioMasterFilesHandlers(pgxPool)
	mux.Handle("/master/folio/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(folioMF.List))
	mux.Handle("/master/folio/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(folioMF.Upload))
	mux.Handle("/master/folio/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(folioMF.Download))
	mux.Handle("/master/folio/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(folioMF.DownloadBulk))
	mux.Handle("/master/folio/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(folioMF.Delete))
	mux.Handle("/master/folio/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(folioMF.Audit))
	mux.Handle("/master/folio/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(folioMF.ApproveDelete))
	mux.Handle("/master/folio/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(folioMF.RejectDelete))

	// Interest Type Master
	interestTypeMF := investmentMasters.InterestTypeMasterFilesHandlers(pgxPool)
	mux.Handle("/master/interest-type/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(interestTypeMF.List))
	mux.Handle("/master/interest-type/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(interestTypeMF.Upload))
	mux.Handle("/master/interest-type/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(interestTypeMF.Download))
	mux.Handle("/master/interest-type/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(interestTypeMF.DownloadBulk))
	mux.Handle("/master/interest-type/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(interestTypeMF.Delete))
	mux.Handle("/master/interest-type/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(interestTypeMF.Audit))
	mux.Handle("/master/interest-type/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(interestTypeMF.ApproveDelete))
	mux.Handle("/master/interest-type/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(interestTypeMF.RejectDelete))

	// Penalty Structure Master
	penaltyStructureMF := investmentMasters.PenaltyStructureMasterFilesHandlers(pgxPool)
	mux.Handle("/master/penalty-structure/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(penaltyStructureMF.List))
	mux.Handle("/master/penalty-structure/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(penaltyStructureMF.Upload))
	mux.Handle("/master/penalty-structure/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(penaltyStructureMF.Download))
	mux.Handle("/master/penalty-structure/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(penaltyStructureMF.DownloadBulk))
	mux.Handle("/master/penalty-structure/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(penaltyStructureMF.Delete))
	mux.Handle("/master/penalty-structure/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(penaltyStructureMF.Audit))
	mux.Handle("/master/penalty-structure/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(penaltyStructureMF.ApproveDelete))
	mux.Handle("/master/penalty-structure/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(penaltyStructureMF.RejectDelete))

	// Compounding Frequency Master
	compoundingFrequencyMF := investmentMasters.CompoundingFrequencyMasterFilesHandlers(pgxPool)
	mux.Handle("/master/compounding-frequency/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(compoundingFrequencyMF.List))
	mux.Handle("/master/compounding-frequency/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(compoundingFrequencyMF.Upload))
	mux.Handle("/master/compounding-frequency/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(compoundingFrequencyMF.Download))
	mux.Handle("/master/compounding-frequency/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(compoundingFrequencyMF.DownloadBulk))
	mux.Handle("/master/compounding-frequency/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(compoundingFrequencyMF.Delete))
	mux.Handle("/master/compounding-frequency/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(compoundingFrequencyMF.Audit))
	mux.Handle("/master/compounding-frequency/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(compoundingFrequencyMF.ApproveDelete))
	mux.Handle("/master/compounding-frequency/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(compoundingFrequencyMF.RejectDelete))

	// TDS Plan Master
	tdsPlanMF := investmentMasters.TDSPlanMasterFilesHandlers(pgxPool)
	mux.Handle("/master/tds-plan/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(tdsPlanMF.List))
	mux.Handle("/master/tds-plan/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(tdsPlanMF.Upload))
	mux.Handle("/master/tds-plan/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(tdsPlanMF.Download))
	mux.Handle("/master/tds-plan/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(tdsPlanMF.DownloadBulk))
	mux.Handle("/master/tds-plan/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(tdsPlanMF.Delete))
	mux.Handle("/master/tds-plan/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(tdsPlanMF.Audit))
	mux.Handle("/master/tds-plan/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(tdsPlanMF.ApproveDelete))
	mux.Handle("/master/tds-plan/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(tdsPlanMF.RejectDelete))

	// Calendar (Holiday) Master
	calendarMF := investmentMasters.CalendarMasterFilesHandlers(pgxPool)
	mux.Handle("/master/calendar/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(calendarMF.List))
	mux.Handle("/master/calendar/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(calendarMF.Upload))
	mux.Handle("/master/calendar/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(calendarMF.Download))
	mux.Handle("/master/calendar/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(calendarMF.DownloadBulk))
	mux.Handle("/master/calendar/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(calendarMF.Delete))
	mux.Handle("/master/calendar/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(calendarMF.Audit))
	mux.Handle("/master/calendar/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(calendarMF.ApproveDelete))
	mux.Handle("/master/calendar/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(calendarMF.RejectDelete))

	// Day Count Convention Master
	dayCountConventionMF := investmentMasters.DayCountConventionMasterFilesHandlers(pgxPool)
	mux.Handle("/master/day-count-convention/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(dayCountConventionMF.List))
	mux.Handle("/master/day-count-convention/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(dayCountConventionMF.Upload))
	mux.Handle("/master/day-count-convention/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(dayCountConventionMF.Download))
	mux.Handle("/master/day-count-convention/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(dayCountConventionMF.DownloadBulk))
	mux.Handle("/master/day-count-convention/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(dayCountConventionMF.Delete))
	mux.Handle("/master/day-count-convention/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(dayCountConventionMF.Audit))
	mux.Handle("/master/day-count-convention/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(dayCountConventionMF.ApproveDelete))
	mux.Handle("/master/day-count-convention/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(dayCountConventionMF.RejectDelete))

	// Bank Config Master
	bankConfigMF := investmentMasters.BankConfigMasterFilesHandlers(pgxPool)
	mux.Handle("/master/bank-config/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(bankConfigMF.List))
	mux.Handle("/master/bank-config/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(bankConfigMF.Upload))
	mux.Handle("/master/bank-config/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(bankConfigMF.Download))
	mux.Handle("/master/bank-config/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(bankConfigMF.DownloadBulk))
	mux.Handle("/master/bank-config/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(bankConfigMF.Delete))
	mux.Handle("/master/bank-config/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(bankConfigMF.Audit))
	mux.Handle("/master/bank-config/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(bankConfigMF.ApproveDelete))
	mux.Handle("/master/bank-config/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(bankConfigMF.RejectDelete))

	// Bank Rate Card Master
	bankRateCardMF := investmentMasters.BankRateCardMasterFilesHandlers(pgxPool)
	mux.Handle("/master/bank-rate-card/additional-files/list", middlewares.PreValidationMiddleware(pgxPool)(bankRateCardMF.List))
	mux.Handle("/master/bank-rate-card/additional-files/upload", middlewares.PreValidationMiddleware(pgxPool)(bankRateCardMF.Upload))
	mux.Handle("/master/bank-rate-card/additional-files/download", middlewares.PreValidationMiddleware(pgxPool)(bankRateCardMF.Download))
	mux.Handle("/master/bank-rate-card/additional-files/download-bulk", middlewares.PreValidationMiddleware(pgxPool)(bankRateCardMF.DownloadBulk))
	mux.Handle("/master/bank-rate-card/additional-files/delete", middlewares.PreValidationMiddleware(pgxPool)(bankRateCardMF.Delete))
	mux.Handle("/master/bank-rate-card/additional-files/audit", middlewares.PreValidationMiddleware(pgxPool)(bankRateCardMF.Audit))
	mux.Handle("/master/bank-rate-card/additional-files/approve-delete", middlewares.PreValidationMiddleware(pgxPool)(bankRateCardMF.ApproveDelete))
	mux.Handle("/master/bank-rate-card/additional-files/reject-delete", middlewares.PreValidationMiddleware(pgxPool)(bankRateCardMF.RejectDelete))

	// Counterparty Hub Master routes
	counterpartyHub.RegisterCounterpartyHubRoutes(mux, pgxPool, db)

	logger.LogInfo("Master Service started on :%s", port)
	err = http.ListenAndServe(":"+port, mux)
	if err != nil {
		logger.LogError("Master Service failed: %v", err)
	}
}
