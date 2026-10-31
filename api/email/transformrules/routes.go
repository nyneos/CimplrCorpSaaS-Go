package transformrules

import (
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterTransformRuleRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	perm := func(page, action string, h http.Handler) http.Handler {
		return chain(middlewares.RequirePermission(pool, page, action)(h))
	}

	mux.Handle("/email/transform-rules/list", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleList(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/create", perm("transformation-integration", middlewares.PermCreate, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleCreate(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/update", perm("transformation-integration", middlewares.PermEdit, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleUpdate(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/delete", perm("transformation-integration", middlewares.PermDelete, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleDelete(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/approve", perm("transformation-integration", middlewares.PermApprove, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleApprove(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/reject", perm("transformation-integration", middlewares.PermReject, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleReject(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/list-mappings", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleListMappings(w, r)
	})))
	mux.Handle("/email/transform-rules/audit-log", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleAuditLog(w, r, pool)
	})))

	// Unauthenticated on purpose: these two are a dummy partner API for
	// locally testing an "API" destination end to end (storage.Put's putAPI
	// posts here as if it were a real external partner), so they can't sit
	// behind the session chain like the CRUD routes above.
	mux.HandleFunc("/email/transform-rules/test-receive", testReceiveHandler)
	mux.HandleFunc("/email/transform-rules/test-receive-2", testReceive2Handler)
}
