package master

import (
	"CimplrCorpSaas/api"
	// "CimplrCorpSaas/api/master/allMaster"

	allMaster "CimplrCorpSaas/api/master/allMasters"
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

	mux.HandleFunc("/master/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Master Service"))
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

	// Payable/Receivable Master routes
	mux.Handle("/master/payablereceivable/create", api.BusinessUnitMiddleware(db)(allMaster.CreatePayableReceivableTypes(pgxPool)))
	mux.Handle("/master/payablereceivable/names", api.BusinessUnitMiddleware(db)(allMaster.GetPayableReceivableNamesWithID(pgxPool)))
	mux.Handle("/master/payablereceivable/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdatePayableReceivableBulk(pgxPool)))
	mux.Handle("/master/payablereceivable/delete", api.BusinessUnitMiddleware(db)(allMaster.DeletePayableReceivable(pgxPool)))
	mux.Handle("/master/payablereceivable/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApprovePayableReceivableActions(pgxPool)))
	mux.Handle("/master/payablereceivable/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectPayableReceivableActions(pgxPool)))
	mux.Handle("/master/payablereceivable/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadPayableReceivable(pgxPool)))
	mux.Handle("/master/payablereceivable/approved-active", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedActivePayableReceivable(pgxPool)))

	// Counterparty Master routes
	mux.Handle("/master/counterparty/create", api.BusinessUnitMiddleware(db)(allMaster.CreateCounterparties(pgxPool)))
	mux.Handle("/master/counterparty/names", api.BusinessUnitMiddleware(db)(allMaster.GetCounterpartyNamesWithID(pgxPool)))
	mux.Handle("/master/counterparty/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCounterpartyBulk(pgxPool)))
	mux.Handle("/master/counterparty/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCounterparty(pgxPool)))
	mux.Handle("/master/counterparty/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCounterpartyActions(pgxPool)))
	mux.Handle("/master/counterparty/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCounterpartyActions(pgxPool)))
	mux.Handle("/master/counterparty/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadCounterparty(pgxPool)))
	mux.Handle("/master/counterparty/approved-active", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedActiveCounterparties(pgxPool)))

	// GL Account Master routes
	mux.Handle("/master/glaccount/create", api.BusinessUnitMiddleware(db)(allMaster.CreateGLAccounts(pgxPool)))
	mux.Handle("/master/glaccount/names", api.BusinessUnitMiddleware(db)(allMaster.GetGLAccountNamesWithID(pgxPool)))
	mux.Handle("/master/glaccount/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateGLAccountBulk(pgxPool)))
	mux.Handle("/master/glaccount/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteGLAccount(pgxPool)))
	mux.Handle("/master/glaccount/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveGLAccountActions(pgxPool)))
	mux.Handle("/master/glaccount/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectGLAccountActions(pgxPool)))
	mux.Handle("/master/glaccount/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadGLAccount(pgxPool)))
	mux.Handle("/master/glaccount/approved-active", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedActiveGLAccounts(pgxPool)))

	// Cash Flow Category Master routes
	mux.Handle("/master/cashflow-category/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCashFlowCategory(pgxPool)))
	mux.Handle("/master/cashflow-category/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetCashFlowCategoryHierarchyPGX(pgxPool)))
	mux.Handle("/master/cashflow-category/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentCashFlowCategoryAtLevel(pgxPool)))
	mux.Handle("/master/cashflow-category/names", api.BusinessUnitMiddleware(db)(allMaster.GetCashFlowCategoryNamesWithID(pgxPool)))
	mux.Handle("/master/cashflow-category/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCashFlowCategoryBulk(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncCashFlowCategories(pgxPool)))
	mux.Handle("/master/cashflow-category/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadCashFlowCategory(pgxPool)))

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
	mux.Handle("/master/bankaccount/all", api.BusinessUnitMiddleware(db)(allMaster.GetAllBankAccountMaster(pgxPool)))
	mux.Handle("/master/bankaccount/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateBankAccountMasterBulk(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-delete", api.BusinessUnitMiddleware(db)(allMaster.BulkDeleteBankAccountAudit(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectBankAccountAuditActions(pgxPool)))
	mux.Handle("/master/bankaccount/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveBankAccountAuditActions(pgxPool)))
	mux.Handle("/master/bankaccount/names", api.BusinessUnitMiddleware(db)(allMaster.GetBankNamesWithIDForAccount(pgxPool)))
	mux.Handle("/master/bankaccount/approved-with-entity", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedBankAccountsWithBankEntity(pgxPool)))
	mux.Handle("/master/bankaccount/pre-populate", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedBankAccountsSimple(pgxPool)))

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
	mux.Handle("/master/entitycash/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetCashEntityHierarchy(pgxPool)))
	mux.Handle("/master/entitycash/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCashEntityBulk(pgxPool)))
	mux.Handle("/master/entitycash/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentCashEntityAtLevel(pgxPool)))
	mux.Handle("/master/entitycash/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCashEntity(pgxPool)))
	mux.Handle("/master/entitycash/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCashEntityActions(pgxPool)))
	mux.Handle("/master/entitycash/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCashEntityActions(pgxPool)))
	mux.Handle("/master/entitycash/all-names", api.BusinessUnitMiddleware(db)(allMaster.GetCashEntityNamesWithID(pgxPool)))
	err = http.ListenAndServe(":2143", mux)
	if err != nil {
		log.Fatalf("Master Service failed: %v", err)
	}
}





