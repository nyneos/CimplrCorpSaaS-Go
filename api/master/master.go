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
