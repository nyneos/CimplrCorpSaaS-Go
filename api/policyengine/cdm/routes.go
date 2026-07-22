package cdm

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/policy-engine/cdm/list", chain(http.HandlerFunc(HandleList(pool))))
	mux.Handle("/policy-engine/cdm/list-approved-active", chain(http.HandlerFunc(HandleListApprovedActive(pool))))
	mux.Handle("/policy-engine/cdm/detail", chain(http.HandlerFunc(HandleDetail(pool))))
	mux.Handle("/policy-engine/cdm/create", chain(http.HandlerFunc(HandleCreate(pool))))
	mux.Handle("/policy-engine/cdm/update", chain(http.HandlerFunc(HandleUpdate(pool))))
	mux.Handle("/policy-engine/cdm/delete", chain(http.HandlerFunc(HandleDelete(pool))))
	mux.Handle("/policy-engine/cdm/approve", chain(http.HandlerFunc(HandleApprove(pool))))
	mux.Handle("/policy-engine/cdm/reject", chain(http.HandlerFunc(HandleReject(pool))))
	mux.Handle("/policy-engine/cdm/resolve", chain(http.HandlerFunc(HandleResolve(pool))))
}
