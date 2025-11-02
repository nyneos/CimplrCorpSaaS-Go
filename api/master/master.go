package master

import (
	"CimplrCorpSaas/api"
	// "CimplrCorpSaas/api/master/allMaster"

	allMaster "CimplrCorpSaas/api/master/allMasters"
	
	investmentMasters "CimplrCorpSaas/api/master/investmentMasters"
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
	mux.Handle("/master/v2/costprofit-center/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncCostProfitCenters(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadAndSyncCostProfitCenters(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateAndSyncCostProfitCenters(pgxPool)))
	mux.Handle("/master/costprofit-center/approved-active", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedActiveCostProfitCenters(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetCostProfitCenterHierarchy(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentCostProfitCenterAtLevel(pgxPool)))
	mux.Handle("/master/v2/costprofit-center/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCostProfitCenter(pgxPool)))
	mux.Handle("/master/costprofit-center/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCostProfitCenterActions(pgxPool)))
	mux.Handle("/master/costprofit-center/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCostProfitCenterActions(pgxPool)))


	mux.Handle("/master/costprofit-center/upload-simple", api.BusinessUnitMiddleware(db)(allMaster.UploadCostProfitCenterSimple(pgxPool)))

	// Payable/Receivable Master routes
	mux.Handle("/master/v2/payablereceivable/create", api.BusinessUnitMiddleware(db)(allMaster.CreatePayableReceivableTypes(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/names", api.BusinessUnitMiddleware(db)(allMaster.GetPayableReceivableNamesWithID(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdatePayableReceivableBulk(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/delete", api.BusinessUnitMiddleware(db)(allMaster.DeletePayableReceivable(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApprovePayableReceivableActions(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectPayableReceivableActions(pgxPool)))
	mux.Handle("/master/payablereceivable/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadPayableReceivable(pgxPool)))
	mux.Handle("/master/v2/payablereceivable/approved-active", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedActivePayableReceivable(pgxPool)))

	// Counterparty Master routes
	mux.Handle("/master/v2/counterparty/create", api.BusinessUnitMiddleware(db)(allMaster.CreateCounterparties(pgxPool)))
	mux.Handle("/master/v2/counterparty/names", api.BusinessUnitMiddleware(db)(allMaster.GetCounterpartyNamesWithID(pgxPool)))
	mux.Handle("/master/v2counterparty/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCounterpartyBulk(pgxPool)))
	mux.Handle("/master/counterparty/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCounterparty(pgxPool)))
	mux.Handle("/master/v2/counterparty/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCounterpartyActions(pgxPool)))
	mux.Handle("/master/counterparty/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCounterpartyActions(pgxPool)))
	mux.Handle("/master/v2/counterparty/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadCounterparty(pgxPool)))

	mux.Handle("/master/counterparty/upload-simple", api.BusinessUnitMiddleware(db)(allMaster.UploadCounterpartySimple(pgxPool)))
	mux.Handle("/master/counterparty/banks/upload-simple", api.BusinessUnitMiddleware(db)(allMaster.UploadCounterpartyBankSimple(pgxPool)))
	
	mux.Handle("/master/counterparty/approved-active", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedActiveCounterparties(pgxPool)))

	mux.Handle("/master/v2/counterparty/banks", api.BusinessUnitMiddleware(db)(allMaster.GetCounterpartyBanks(pgxPool)))
	mux.Handle("/master/v2/counterparty/banks/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCounterpartyBanksBulk(pgxPool)))

	// GL Account Master routes
	mux.Handle("/master/v2/glaccount/create", api.BusinessUnitMiddleware(db)(allMaster.CreateGLAccounts(pgxPool)))
	mux.Handle("/master/v2/glaccount/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetGLAccountNamesWithID(pgxPool)))
	mux.Handle("/master/v2/glaccount/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateAndSyncGLAccounts(pgxPool)))
	mux.Handle("/master/v2/glaccount/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteGLAccount(pgxPool)))
	mux.Handle("/master/v2/glaccount/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveGLAccountActions(pgxPool)))
	mux.Handle("/master/v2/glaccount/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectGLAccountActions(pgxPool)))
	mux.Handle("/master/glaccount/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadGLAccount(pgxPool)))
	mux.Handle("/master/glaccount/approved-active", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedActiveGLAccounts(pgxPool)))
	mux.Handle("/master/v2/glaccount/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentGLAccountAtLevel(pgxPool)))
	mux.Handle("/master/glaccount/upload-simple", api.BusinessUnitMiddleware(db)(allMaster.UploadGLAccountSimple(pgxPool)))

	// Cash Flow Category Master routes
	mux.Handle("/master/cashflow-category/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCashFlowCategory(pgxPool)))
	mux.Handle("/master/v2/cashflow-category/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetCashFlowCategoryHierarchyPGX(pgxPool)))
	mux.Handle("/master/cashflow-category/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentCashFlowCategoryAtLevel(pgxPool)))
	mux.Handle("/master/cashflow-category/names", api.BusinessUnitMiddleware(db)(allMaster.GetCashFlowCategoryNamesWithID(pgxPool)))
	mux.Handle("/master/v2/cashflow-category/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCashFlowCategoryBulk(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/v2/cashflow-category/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncCashFlowCategories(pgxPool)))
	mux.Handle("/master/cashflow-category/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadCashFlowCategory(pgxPool)))

	
	mux.Handle("/master/cashflow-category/upload-simple", api.BusinessUnitMiddleware(db)(allMaster.UploadCashFlowCategorySimple(pgxPool)))
	// Currency Master routes (pgx-backed)
	mux.Handle("/master/currency/create", api.BusinessUnitMiddleware(db)(allMaster.CreateCurrencyMaster(pgxPool)))
	mux.Handle("/master/currency/all", api.BusinessUnitMiddleware(db)(allMaster.GetAllCurrencyMaster(pgxPool)))
	mux.Handle("/master/currency/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateCurrencyMasterBulk(pgxPool)))
	mux.Handle("/master/currency/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveAuditActions(pgxPool)))
	mux.Handle("/master/currency/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectAuditActions(pgxPool)))
	mux.Handle("/master/currency/bulk-delete", api.BusinessUnitMiddleware(db)(allMaster.BulkDeleteCurrencyAudit(pgxPool)))
	mux.Handle("/master/currency/active-approved", api.BusinessUnitMiddleware(db)(allMaster.GetActiveApprovedCurrencyCodes(pgxPool)))

	// Bank Master routes (pgx-backed)
	mux.Handle("/master/bank/create", api.BusinessUnitMiddleware(db)(allMaster.CreateBankMaster(pgxPool)))
	mux.Handle("/master/bank/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadBank(pgxPool)))
	mux.Handle("/master/bank/all", api.BusinessUnitMiddleware(db)(allMaster.GetAllBankMaster(pgxPool)))
	mux.Handle("/master/bank/names", api.BusinessUnitMiddleware(db)(allMaster.GetBankNamesWithID(pgxPool)))
	mux.Handle("/master/bank/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateBankMasterBulk(pgxPool)))
	mux.Handle("/master/bank/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveBankAuditActions(pgxPool)))
	mux.Handle("/master/bank/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectBankAuditActions(pgxPool)))
	mux.Handle("/master/bank/bulk-delete", api.BusinessUnitMiddleware(db)(allMaster.BulkDeleteBankAudit(pgxPool)))

	// Bank Account Master routes
	mux.Handle("/master/bankaccount/create", api.BusinessUnitMiddleware(db)(allMaster.CreateBankAccountMaster(pgxPool)))
	mux.Handle("/master/bankaccount/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadBankAccount(pgxPool)))
	// mux.Handle("/master/bankaccount/all", api.BusinessUnitMiddleware(db)(allMaster.GetAllBankAccountMaster(pgxPool)))
	mux.Handle("/master/bankaccount/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateBankAccountMasterBulk(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-delete", api.BusinessUnitMiddleware(db)(allMaster.BulkDeleteBankAccountAudit(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectBankAccountAuditActions(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveBankAccountAuditActions(pgxPool)))
	mux.Handle("/master/bankaccount/names", api.BusinessUnitMiddleware(db)(allMaster.GetBankNamesWithIDForAccount(pgxPool)))
	mux.Handle("/master/bankaccount/approved-with-entity", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedBankAccountsWithBankEntity(pgxPool)))
	mux.Handle("/master/bankaccount/pre-populate", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedBankAccountsSimple(pgxPool)))
	mux.Handle("/master/bankaccount/all", api.BusinessUnitMiddleware(db)(allMaster.GetBankAccountMetaAll(pgxPool)))
	mux.Handle("/master/bankaccount/for-user", api.BusinessUnitMiddleware(db)(allMaster.GetBankAccountsForUser(pgxPool)))

	
	// Entity Master routes
	mux.Handle("/master/entity/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncEntities(db)))
	mux.Handle("/master/entity/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetEntityHierarchy(db)))
	mux.Handle("/master/entity/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteEntity(db)))
	mux.Handle("/master/entity/findParentAtLevel", api.BusinessUnitMiddleware(db)(allMaster.FindParentAtLevel(db)))
	mux.Handle("/master/entity/approve", api.BusinessUnitMiddleware(db)(allMaster.ApproveEntity(db)))
	mux.Handle("/master/entity/reject-bulk", api.BusinessUnitMiddleware(db)(allMaster.RejectEntitiesBulk(db)))
	mux.Handle("/master/entity/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateEntity(db)))
	mux.Handle("/master/entity/all-names", api.BusinessUnitMiddleware(db)(allMaster.GetAllEntityNames(db)))
	mux.Handle("/master/entity/render-vars-hierarchical", api.BusinessUnitMiddleware(db)(allMaster.GetRenderVarsHierarchical(db)))

	// Entity Cash Master routes (pgx-backed)
	mux.Handle("/master/entitycash/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncCashEntities(pgxPool)))
	mux.Handle("/master/entitycash/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadEntityCash(pgxPool)))
	mux.Handle("/master/entitycash/upload-simple", api.BusinessUnitMiddleware(db)(allMaster.UploadEntitySimple(pgxPool)))
	mux.Handle("/master/entitycash/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetCashEntityHierarchy(pgxPool)))
	mux.Handle("/master/entitycash/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCashEntityBulk(pgxPool)))
	mux.Handle("/master/entitycash/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentCashEntityAtLevel(pgxPool)))
	mux.Handle("/master/entitycash/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCashEntity(pgxPool)))
	mux.Handle("/master/entitycash/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCashEntityActions(pgxPool)))
	mux.Handle("/master/entitycash/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCashEntityActions(pgxPool)))
	mux.Handle("/master/entitycash/all-names", api.BusinessUnitMiddleware(db)(allMaster.GetCashEntityNamesWithID(pgxPool)))

	mux.Handle("/master/amc/create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateAMCsingle(pgxPool)))
	mux.Handle("/master/amc/bulk-create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateAMC(pgxPool)))
	mux.Handle("/master/amc/bulk-update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateAMCBulk(pgxPool)))
	mux.Handle("/master/amc/update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateAMC(pgxPool)))
	mux.Handle("/master/amc/bulk-delete", api.BusinessUnitMiddleware(db)(investmentMasters.DeleteAMC(pgxPool)))
	mux.Handle("/master/amc/bulk-approve", api.BusinessUnitMiddleware(db)(investmentMasters.BulkApproveAMCActions(pgxPool)))
	mux.Handle("/master/amc/bulk-reject", api.BusinessUnitMiddleware(db)(investmentMasters.BulkRejectAMCActions(pgxPool)))
	mux.Handle("/master/amc/all", api.BusinessUnitMiddleware(db)(investmentMasters.GetAMCsWithAudit(pgxPool)))
	mux.Handle("/master/amc/approved-active", api.BusinessUnitMiddleware(db)(investmentMasters.GetApprovedActiveAMCs(pgxPool)))
	mux.Handle("/master/amc/upload", api.BusinessUnitMiddleware(db)(investmentMasters.UploadAMCSimple(pgxPool)))

	// Scheme Master routes
	mux.Handle("/master/scheme/upload", api.BusinessUnitMiddleware(db)(investmentMasters.UploadSchemeSimple(pgxPool)))
	mux.Handle("/master/scheme/create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateSchemeSingle(pgxPool)))
	mux.Handle("/master/scheme/bulk-create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateScheme(pgxPool)))
	mux.Handle("/master/scheme/update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateScheme(pgxPool)))
	mux.Handle("/master/scheme/bulk-update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateSchemeBulk(pgxPool)))
	mux.Handle("/master/scheme/bulk-delete", api.BusinessUnitMiddleware(db)(investmentMasters.DeleteScheme(pgxPool)))
	mux.Handle("/master/scheme/bulk-approve", api.BusinessUnitMiddleware(db)(investmentMasters.BulkApproveSchemeActions(pgxPool)))
	mux.Handle("/master/scheme/bulk-reject", api.BusinessUnitMiddleware(db)(investmentMasters.BulkRejectSchemeActions(pgxPool)))
	mux.Handle("/master/scheme/approved-active", api.BusinessUnitMiddleware(db)(investmentMasters.GetApprovedActiveSchemes(pgxPool)))
	mux.Handle("/master/scheme/all", api.BusinessUnitMiddleware(db)(investmentMasters.GetSchemesWithAudit(pgxPool)))
	
	// DP Master routes
	mux.Handle("/master/dp/upload", api.BusinessUnitMiddleware(db)(investmentMasters.UploadDPSimple(pgxPool)))
	mux.Handle("/master/dp/create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateDPSingle(pgxPool)))
	mux.Handle("/master/dp/bulk-create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateDPBulk(pgxPool)))
	mux.Handle("/master/dp/update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateDP(pgxPool)))
	mux.Handle("/master/dp/bulk-update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateDPBulk(pgxPool)))
	mux.Handle("/master/dp/bulk-delete", api.BusinessUnitMiddleware(db)(investmentMasters.DeleteDP(pgxPool)))
	mux.Handle("/master/dp/bulk-approve", api.BusinessUnitMiddleware(db)(investmentMasters.BulkApproveDPActions(pgxPool)))
	mux.Handle("/master/dp/bulk-reject", api.BusinessUnitMiddleware(db)(investmentMasters.BulkRejectDPActions(pgxPool)))
	mux.Handle("/master/dp/approved-active", api.BusinessUnitMiddleware(db)(investmentMasters.GetApprovedActiveDPs(pgxPool)))
	mux.Handle("/master/dp/all", api.BusinessUnitMiddleware(db)(investmentMasters.GetDPsWithAudit(pgxPool)))

	// Demat Master routes
	mux.Handle("/master/demat/upload", api.BusinessUnitMiddleware(db)(investmentMasters.UploadDematSimple(pgxPool)))
	mux.Handle("/master/demat/create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateDematSingle(pgxPool)))
	mux.Handle("/master/demat/bulk-create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateDematBulk(pgxPool)))
	mux.Handle("/master/demat/update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateDemat(pgxPool)))
	mux.Handle("/master/demat/bulk-update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateDematBulk(pgxPool)))
	mux.Handle("/master/demat/bulk-delete", api.BusinessUnitMiddleware(db)(investmentMasters.DeleteDemat(pgxPool)))
	mux.Handle("/master/demat/bulk-approve", api.BusinessUnitMiddleware(db)(investmentMasters.BulkApproveDematActions(pgxPool)))
	mux.Handle("/master/demat/bulk-reject", api.BusinessUnitMiddleware(db)(investmentMasters.BulkRejectDematActions(pgxPool)))
	mux.Handle("/master/demat/approved-active", api.BusinessUnitMiddleware(db)(investmentMasters.GetApprovedActiveDemats(pgxPool)))
	mux.Handle("/master/demat/all", api.BusinessUnitMiddleware(db)(investmentMasters.GetDematsWithAudit(pgxPool)))

	// Folio Master routes
	mux.Handle("/master/folio/upload", api.BusinessUnitMiddleware(db)(investmentMasters.UploadFolio(pgxPool)))
	mux.Handle("/master/folio/all", api.BusinessUnitMiddleware(db)(investmentMasters.GetFoliosWithAudit(pgxPool)))
	mux.Handle("/master/folio/approved-active", api.BusinessUnitMiddleware(db)(investmentMasters.GetApprovedActiveFolios(pgxPool)))
	mux.Handle("/master/folio/create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateFolioSingle(pgxPool)))
	mux.Handle("/master/folio/bulk-create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateFolioBulk(pgxPool)))
	mux.Handle("/master/folio/update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateFolio(pgxPool)))
	mux.Handle("/master/folio/bulk-update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateFolioBulk(pgxPool)))
	mux.Handle("/master/folio/bulk-delete", api.BusinessUnitMiddleware(db)(investmentMasters.DeleteFolio(pgxPool)))
	mux.Handle("/master/folio/bulk-approve", api.BusinessUnitMiddleware(db)(investmentMasters.BulkApproveFolioActions(pgxPool)))
	mux.Handle("/master/folio/bulk-reject", api.BusinessUnitMiddleware(db)(investmentMasters.BulkRejectFolioActions(pgxPool)))
	mux.Handle("/master/folio/meta", api.BusinessUnitMiddleware(db)(investmentMasters.GetSingleFolioWithAudit(pgxPool)))
	
	// AMFI Config Master routes
	mux.Handle("/master/amfi/scheme", api.BusinessUnitMiddleware(db)(investmentMasters.GetAMFISchemeMasterSimple(pgxPool)))
	mux.Handle("/master/amfi/nav", api.BusinessUnitMiddleware(db)(investmentMasters.GetAMFINavStagingSimple(pgxPool)))

	)

	// Holiday calendar exports (ICS/feed/share links)
	mux.Handle("/master/calendar/export/ics/", api.BusinessUnitMiddleware(db)(investmentMasters.ExportCalendarICS(pgxPool)))
	mux.Handle("/master/calendar/feed/", api.BusinessUnitMiddleware(db)(investmentMasters.CalendarFeedICS(pgxPool)))
	mux.Handle("/master/calendar/share-links/", api.BusinessUnitMiddleware(db)(investmentMasters.ShareLinksCalendar(pgxPool)))

	// Calendar Master routes (pgx-backed)
	mux.Handle("/master/calendar/create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateCalendarSingle(pgxPool)))
	mux.Handle("/master/calendar/holiday/bulk-create", api.BusinessUnitMiddleware(db)(investmentMasters.CreateHolidayBulk(pgxPool)))
	mux.Handle("/master/calendar/upload", api.BusinessUnitMiddleware(db)(investmentMasters.UploadCalendarBulk(pgxPool)))
	mux.Handle("/master/calendar/holiday/upload", api.BusinessUnitMiddleware(db)(investmentMasters.UploadHolidayBulk(pgxPool)))
	mux.Handle("/master/calendar/meta", api.BusinessUnitMiddleware(db)(investmentMasters.GetCalendarsWithAudit(pgxPool)))
	mux.Handle("/master/calendar/list", api.BusinessUnitMiddleware(db)(investmentMasters.GetCalendarListLite(pgxPool)))
	mux.Handle("/master/calendar/all", api.BusinessUnitMiddleware(db)(investmentMasters.GetCalendarWithHolidays(pgxPool)))
	mux.Handle("/master/calendar/approved-active", api.BusinessUnitMiddleware(db)(investmentMasters.GetApprovedActiveCalendars(pgxPool)))
	mux.Handle("/master/calendar/view", api.BusinessUnitMiddleware(db)(investmentMasters.GetCalendarWithHolidaysApproved(pgxPool)))
	mux.Handle("/master/calendar/bulk-delete", api.BusinessUnitMiddleware(db)(investmentMasters.DeleteCalendar(pgxPool)))
	mux.Handle("/master/calendar/bulk-approve", api.BusinessUnitMiddleware(db)(investmentMasters.BulkApproveCalendarActions(pgxPool)))
	mux.Handle("/master/calendar/bulk-reject", api.BusinessUnitMiddleware(db)(investmentMasters.BulkRejectCalendarActions(pgxPool)))
	mux.Handle("/master/calendar/update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateCalendar(pgxPool)))
	mux.Handle("/master/calendar/holiday/update", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateHoliday(pgxPool)))
	mux.Handle("/master/calendar/update-with-holidays", api.BusinessUnitMiddleware(db)(investmentMasters.UpdateCalendarWithHolidays(pgxPool)))

	
	err = http.ListenAndServe(":2143", mux)
	if err != nil {
		log.Fatalf("Master Service failed: %v", err)
	}
}


