package master

import (
	"CimplrCorpSaas/api"
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
	// create a pgx pool for handlers that prefer pgx (bulk uploads/syncs)
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
	// Payable/Receivable Master routes
	mux.Handle("/master/payablereceivable/create", api.BusinessUnitMiddleware(db)(allMaster.CreatePayableReceivableTypes(pgxPool)))
	mux.Handle("/master/payablereceivable/all", api.BusinessUnitMiddleware(db)(allMaster.GetPayableReceivableNamesWithID(pgxPool)))
	mux.Handle("/master/payablereceivable/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdatePayableReceivableBulk(pgxPool)))
	mux.Handle("/master/payablereceivable/delete", api.BusinessUnitMiddleware(db)(allMaster.DeletePayableReceivable(pgxPool)))
	mux.Handle("/master/payablereceivable/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApprovePayableReceivableActions(pgxPool)))
	mux.Handle("/master/payablereceivable/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectPayableReceivableActions(pgxPool)))
	mux.Handle("/master/payablereceivable/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadPayableReceivable(pgxPool)))
	mux.Handle("/master/payablereceivable/approved-active", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedActivePayableReceivable(pgxPool)))

	// Cash Flow Category Master routes
	mux.Handle("/master/cashflow-category/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncCashFlowCategories(pgxPool)))
	mux.Handle("/master/cashflow-category/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCashFlowCategory(pgxPool)))
	mux.Handle("/master/cashflow-category/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetCashFlowCategoryHierarchyPGX(pgxPool)))
	mux.Handle("/master/cashflow-category/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentCashFlowCategoryAtLevel(pgxPool)))
	mux.Handle("/master/cashflow-category/names", api.BusinessUnitMiddleware(db)(allMaster.GetCashFlowCategoryNamesWithID(pgxPool)))
	mux.Handle("/master/cashflow-category/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCashFlowCategoryBulk(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/cashflow-category/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCashFlowCategoryActions(pgxPool)))
	mux.Handle("/master/cashflow-category/upload", api.BusinessUnitMiddleware(db)(allMaster.UploadCashFlowCategory(pgxPool)))

	// Currency Master routes
	mux.Handle("/master/currency/create", api.BusinessUnitMiddleware(db)(allMaster.CreateCurrencyMaster(db)))
	mux.Handle("/master/currency/all", api.BusinessUnitMiddleware(db)(allMaster.GetAllCurrencyMaster(db)))
	mux.Handle("/master/currency/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateCurrencyMasterBulk(db)))
	mux.Handle("/master/currency/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveAuditActions(db)))
	mux.Handle("/master/currency/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectAuditActions(db)))
	mux.Handle("/master/currency/bulk-delete", api.BusinessUnitMiddleware(db)(allMaster.BulkDeleteCurrencyAudit(db)))
	mux.Handle("/master/currency/active-approved", api.BusinessUnitMiddleware(db)(allMaster.GetActiveApprovedCurrencyCodes(db)))

	// Bank Master routes
	mux.Handle("/master/bank/create", api.BusinessUnitMiddleware(db)(allMaster.CreateBankMaster(db)))
	mux.Handle("/master/bank/all", api.BusinessUnitMiddleware(db)(allMaster.GetAllBankMaster(db)))
	mux.Handle("/master/bank/names", api.BusinessUnitMiddleware(db)(allMaster.GetBankNamesWithID(db)))
	mux.Handle("/master/bank/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateBankMasterBulk(db)))
	mux.Handle("/master/bank/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveBankAuditActions(db)))
	mux.Handle("/master/bank/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectBankAuditActions(db)))
	mux.Handle("/master/bank/bulk-delete", api.BusinessUnitMiddleware(db)(allMaster.BulkDeleteBankAudit(db)))

	// Bank Account Master routes
	mux.Handle("/master/bankaccount/create", api.BusinessUnitMiddleware(db)(allMaster.CreateBankAccountMaster(db)))
	mux.Handle("/master/bankaccount/all", api.BusinessUnitMiddleware(db)(allMaster.GetAllBankAccountMaster(db)))
	mux.Handle("/master/bankaccount/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateBankAccountMasterBulk(db)))
	mux.Handle("/master/bankaccount/bulk-delete", api.BusinessUnitMiddleware(db)(allMaster.BulkDeleteBankAccountAudit(db)))
	mux.Handle("/master/bankaccount/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectBankAccountAuditActions(db)))
	mux.Handle("/master/bankaccount/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveBankAccountAuditActions(db)))
	mux.Handle("/master/bankaccount/names", api.BusinessUnitMiddleware(db)(allMaster.GetBankNamesWithIDForAccount(db)))
	mux.Handle("/master/bankaccount/approved-with-entity", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedBankAccountsWithBankEntity(db)))

	// Entity Master routes
	mux.Handle("/master/entity/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncEntities(db)))
	mux.Handle("/master/entity/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetEntityHierarchy(db)))
	mux.Handle("/master/entity/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteEntity(db)))
	mux.Handle("/master/entity/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentAtLevel(db)))
	mux.Handle("/master/entity/approve", api.BusinessUnitMiddleware(db)(allMaster.ApproveEntity(db)))
	mux.Handle("/master/entity/reject-bulk", api.BusinessUnitMiddleware(db)(allMaster.RejectEntitiesBulk(db)))
	mux.Handle("/master/entity/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateEntity(db)))
	mux.Handle("/master/entity/all-names", api.BusinessUnitMiddleware(db)(allMaster.GetAllEntityNames(db)))
	mux.Handle("/master/entity/render-vars-hierarchical", api.BusinessUnitMiddleware(db)(allMaster.GetRenderVarsHierarchical(db)))

	// Entity Cash Master routes
	mux.Handle("/master/entitycash/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncCashEntities(db)))
	mux.Handle("/master/entitycash/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetCashEntityHierarchy(db)))
	mux.Handle("/master/entitycash/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCashEntityBulk(db)))
	mux.Handle("/master/entitycash/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentCashEntityAtLevel(db)))
	mux.Handle("/master/entitycash/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCashEntity(db)))
	mux.Handle("/master/entitycash/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCashEntityActions(db)))
	mux.Handle("/master/entitycash/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCashEntityActions(db)))
	mux.Handle("/master/entitycash/all-names", api.BusinessUnitMiddleware(db)(allMaster.GetCashEntityNamesWithID(db)))
	err = http.ListenAndServe(":2143", mux)
	if err != nil {
		log.Fatalf("Master Service failed: %v", err)
	}
}




