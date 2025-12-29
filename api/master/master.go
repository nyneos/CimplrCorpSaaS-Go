package master

import (
	allMaster "CimplrCorpSaas/api/master/allMasters"
	investmentMasters "CimplrCorpSaas/api/master/investmentMasters"
	middlewares "CimplrCorpSaas/api/middlewares"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func StartMasterService(db *sql.DB) {
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
	mux.Handle("/master/entitycash/updatebulk", middlewares.PreValidationMiddleware(pgxPool)(allMaster.UpdateCashEntityBulk(pgxPool)))
	mux.Handle("/master/entitycash/find-parent-at-level", middlewares.PreValidationMiddleware(pgxPool)(allMaster.FindParentCashEntityAtLevel(pgxPool)))
	mux.Handle("/master/entitycash/delete", middlewares.PreValidationMiddleware(pgxPool)(allMaster.DeleteCashEntity(pgxPool)))
	mux.Handle("/master/entitycash/bulk-approve", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkApproveCashEntityActions(pgxPool)))
	mux.Handle("/master/entitycash/bulk-reject", middlewares.PreValidationMiddleware(pgxPool)(allMaster.BulkRejectCashEntityActions(pgxPool)))
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

	err = http.ListenAndServe(":2143", mux)
	if err != nil {
		log.Fatalf("Master Service failed: %v", err)
	}
}
