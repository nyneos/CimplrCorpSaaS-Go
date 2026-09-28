package transformrules

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterTransformRuleRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/email/transform-rules/list", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleList(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/create", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleCreate(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/update", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleUpdate(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/delete", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleDelete(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/approve", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleApprove(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/reject", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleReject(w, r, pool)
	})))
	mux.Handle("/email/transform-rules/list-mappings", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleListMappings(w, r)
	})))
	mux.Handle("/email/transform-rules/audit-log", chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleAuditLog(w, r, pool)
	})))
}
