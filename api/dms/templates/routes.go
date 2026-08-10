package templates

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterRoutes wires every /dms/templates/* route onto mux. Route
// registration only — handler logic lives in this package's other files.
func RegisterRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/dms/templates/list", chain(HandleList(pool)))
	mux.Handle("/dms/templates/detail", chain(HandleDetail(pool)))
	mux.Handle("/dms/templates/create", chain(HandleCreate(pool)))
	mux.Handle("/dms/templates/update", chain(HandleUpdate(pool)))
	mux.Handle("/dms/templates/delete", chain(HandleDelete(pool)))
	mux.Handle("/dms/templates/approve", chain(HandleApprove(pool)))
	mux.Handle("/dms/templates/reject", chain(HandleReject(pool)))
	mux.Handle("/dms/templates/upload", chain(HandleUpload(pool)))
	mux.Handle("/dms/templates/versions/list", chain(HandleListVersions(pool)))
	mux.Handle("/dms/templates/versions/create", chain(HandleCreateVersion(pool)))
	mux.Handle("/dms/templates/versions/activate", chain(HandleActivateVersion(pool)))
	mux.Handle("/dms/templates/versions/delete", chain(HandleDeleteVersion(pool)))
	mux.Handle("/dms/templates/audit-log", chain(HandleAuditLog(pool)))
}
