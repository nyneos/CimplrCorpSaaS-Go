package policy

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/policy-engine/policies/list", chain(http.HandlerFunc(HandleList(pool))))
	mux.Handle("/policy-engine/policies/list-approved-active", chain(http.HandlerFunc(HandleListApprovedActive(pool))))
	mux.Handle("/policy-engine/policies/detail", chain(http.HandlerFunc(HandleDetail(pool))))
	mux.Handle("/policy-engine/policies/create", chain(http.HandlerFunc(HandleCreate(pool))))
	mux.Handle("/policy-engine/policies/update", chain(http.HandlerFunc(HandleUpdate(pool))))
	mux.Handle("/policy-engine/policies/delete", chain(http.HandlerFunc(HandleDelete(pool))))
	mux.Handle("/policy-engine/policies/approve", chain(http.HandlerFunc(HandleApprove(pool))))
	mux.Handle("/policy-engine/policies/reject", chain(http.HandlerFunc(HandleReject(pool))))
	mux.Handle("/policy-engine/policies/check", chain(http.HandlerFunc(HandleCheck(pool))))
	mux.Handle("/policy-engine/policies/test", chain(http.HandlerFunc(HandleTest(pool))))
	mux.Handle("/policy-engine/policies/audit-log", chain(http.HandlerFunc(HandleAuditLog(pool))))
	mux.Handle("/policy-engine/pel/validate", chain(http.HandlerFunc(HandlePelValidate(pool))))
	mux.Handle("/policy-engine/service/health", chain(http.HandlerFunc(HandleServiceHealth(pool))))
}
