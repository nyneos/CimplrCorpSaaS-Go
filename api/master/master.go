package master

import (
	"database/sql"
	"log"
	"net/http"

	"CimplrCorpSaas/api"
	allMaster "CimplrCorpSaas/api/master/allMasters"
)

func StartMasterService(db *sql.DB) {
	mux := http.NewServeMux()
	mux.HandleFunc("/master/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Master Service"))
	})
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

	// Entity Cash Master routes
	mux.Handle("/master/entitycash/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncCashEntities(db)))
	mux.Handle("/master/entitycash/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetCashEntityHierarchy(db)))
	mux.Handle("/master/entitycash/updatebulk", api.BusinessUnitMiddleware(db)(allMaster.UpdateCashEntityBulk(db)))
	mux.Handle("/master/entitycash/find-parent-at-level", api.BusinessUnitMiddleware(db)(allMaster.FindParentCashEntityAtLevel(db)))
	mux.Handle("/master/entitycash/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteCashEntity(db)))
	mux.Handle("/master/entitycash/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveCashEntityActions(db)))
	mux.Handle("/master/entitycash/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectCashEntityActions(db)))
	mux.Handle("/master/entitycash/all-names", api.BusinessUnitMiddleware(db)(allMaster.GetCashEntityNamesWithID(db)))	

	// Bank Account Master routes
	mux.Handle("/master/bankaccount/create", api.BusinessUnitMiddleware(db)(allMaster.CreateBankAccountMaster(db)))
	mux.Handle("/master/bankaccount/all", api.BusinessUnitMiddleware(db)(allMaster.GetAllBankAccountMaster(db)))
	mux.Handle("/master/bankaccount/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateBankAccountMasterBulk(db)))
	mux.Handle("/master/bankaccount/bulk-delete", api.BusinessUnitMiddleware(db)(allMaster.BulkDeleteBankAccountAudit(db)))
	mux.Handle("/master/bankaccount/bulk-reject", api.BusinessUnitMiddleware(db)(allMaster.BulkRejectBankAccountAuditActions(db)))
	mux.Handle("/master/bankaccount/bulk-approve", api.BusinessUnitMiddleware(db)(allMaster.BulkApproveBankAccountAuditActions(db)))
	mux.Handle("/master/bankaccount/names", api.BusinessUnitMiddleware(db)(allMaster.GetBankNamesWithIDForAccount(db)))
	mux.Handle("/master/bankaccount/approved-with-entity", api.BusinessUnitMiddleware(db)(allMaster.GetApprovedBankAccountsWithBankEntity(db)))

	mux.Handle("/master/entity/bulk-create-sync", api.BusinessUnitMiddleware(db)(allMaster.CreateAndSyncEntities(db)))
	mux.Handle("/master/entity/hierarchy", api.BusinessUnitMiddleware(db)(allMaster.GetEntityHierarchy(db)))
	mux.Handle("/master/entity/delete", api.BusinessUnitMiddleware(db)(allMaster.DeleteEntity(db)))
	mux.Handle("/master/entity/findParentAtLevel", api.BusinessUnitMiddleware(db)(allMaster.FindParentAtLevel(db)))
	mux.Handle("/master/entity/approve", api.BusinessUnitMiddleware(db)(allMaster.ApproveEntity(db)))
	mux.Handle("/master/entity/reject-bulk", api.BusinessUnitMiddleware(db)(allMaster.RejectEntitiesBulk(db)))
	mux.Handle("/master/entity/update", api.BusinessUnitMiddleware(db)(allMaster.UpdateEntity(db)))
	mux.Handle("/master/entity/all-names", api.BusinessUnitMiddleware(db)(allMaster.GetAllEntityNames(db)))
	mux.Handle("/master/entity/render-vars-hierarchical", api.BusinessUnitMiddleware(db)(allMaster.GetRenderVarsHierarchical(db)))
	log.Println("Master Service started on :2143")
	err := http.ListenAndServe(":2143", mux)
	if err != nil {
		log.Fatalf("Master Service failed: %v", err)
	}
}



