package generate

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterRoutes wires /dms/generate/* routes.
func RegisterRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/dms/generate/preview", chain(HandlePreview(pool)))
	mux.Handle("/dms/generate/adhoc", chain(HandleAdhoc(pool)))
	mux.Handle("/dms/generate/adhoc-approve", chain(HandleAdhocApprove(pool)))
	mux.Handle("/dms/generate/adhoc-reject", chain(HandleAdhocReject(pool)))
	mux.Handle("/dms/generate/adhoc-list-pending", chain(HandleAdhocListPending(pool)))
	mux.Handle("/dms/generate/adhoc-audit-log", chain(HandleAdhocAuditLog(pool)))
}
