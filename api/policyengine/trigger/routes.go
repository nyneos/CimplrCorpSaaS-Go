package trigger

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/policy-engine/triggers/list", chain(http.HandlerFunc(HandleList(pool))))
	mux.Handle("/policy-engine/triggers/list-approved-active", chain(http.HandlerFunc(HandleListApprovedActive(pool))))
	mux.Handle("/policy-engine/triggers/create", chain(http.HandlerFunc(HandleCreate(pool))))
	mux.Handle("/policy-engine/triggers/update", chain(http.HandlerFunc(HandleUpdate(pool))))
	mux.Handle("/policy-engine/triggers/delete", chain(http.HandlerFunc(HandleDelete(pool))))
	mux.Handle("/policy-engine/triggers/approve", chain(http.HandlerFunc(HandleApprove(pool))))
	mux.Handle("/policy-engine/triggers/reject", chain(http.HandlerFunc(HandleReject(pool))))
}
