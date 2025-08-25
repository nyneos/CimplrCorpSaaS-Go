package uam

import (
	"CimplrCorpSaas/api"
	// "CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/uam/role" // <-- Import role
	"CimplrCorpSaas/api/uam/user" // <-- Import user
	"database/sql"
	"log"
	"net/http"
)

func StartUAMService(db *sql.DB) {
	mux := http.NewServeMux()
	mux.HandleFunc("/uam/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from UAM Service"))
	})
/*users*/
	mux.Handle("/uam/users/create-user", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.CreateUser(db))))
	mux.Handle("/uam/users/get-users", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.GetUsers(db))))
	mux.Handle("/uam/users/get-user-by-id", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.GetUserById(db))))
	mux.Handle("/uam/users/update-user", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.UpdateUser(db))))
	mux.Handle("/uam/users/delete-user", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.DeleteUser(db))))
	mux.Handle("/uam/users/approve-multiple-users", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.ApproveMultipleUsers(db))))
	mux.Handle("/uam/users/reject-multiple-users", api.BusinessUnitMiddleware(db)(http.HandlerFunc(user.RejectMultipleUsers(db))))
/*roles*/ 
	mux.Handle("/uam/roles/create-role", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.CreateRole(db))))
	mux.Handle("/uam/roles/page-data", api.BusinessUnitMiddleware(db)(http.HandlerFunc(role.GetRolesPageData(db))))

	log.Println("UAM Service started on :5143")
	err := http.ListenAndServe(":5143", mux)
	if err != nil {
		log.Fatalf("UAM Service failed: %v", err)
	}
}
