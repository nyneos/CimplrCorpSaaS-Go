package rules

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterRoutes wires every /dms/rules/* route onto mux. Route registration
// only — handler logic lives in this package's other files.
func RegisterRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/dms/rules/list", chain(HandleList(pool)))
	mux.Handle("/dms/rules/detail", chain(HandleDetail(pool)))
	mux.Handle("/dms/rules/create", chain(HandleCreate(pool)))
	mux.Handle("/dms/rules/update", chain(HandleUpdate(pool)))
	mux.Handle("/dms/rules/delete", chain(HandleDelete(pool)))
	mux.Handle("/dms/rules/approve", chain(HandleApprove(pool)))
	mux.Handle("/dms/rules/reject", chain(HandleReject(pool)))
	mux.Handle("/dms/rules/versions/list", chain(HandleListVersions(pool)))
	mux.Handle("/dms/rules/versions/create", chain(HandleCreateVersion(pool)))
	mux.Handle("/dms/rules/versions/activate", chain(HandleActivateVersion(pool)))
	mux.Handle("/dms/rules/versions/delete", chain(HandleDeleteVersion(pool)))
	mux.Handle("/dms/rules/run", chain(HandleRun(pool)))
	mux.Handle("/dms/rules/documents/list", chain(HandleListDocuments(pool)))
	mux.Handle("/dms/rules/documents/detail", chain(HandleDocumentDetail(pool)))
	mux.Handle("/dms/rules/documents/download", chain(HandleDocumentDownload(pool)))
	mux.Handle("/dms/rules/execution-log/list", chain(HandleListExecutionLog(pool)))
	mux.Handle("/dms/rules/execution-log/detail", chain(HandleExecutionDetail(pool)))
	mux.Handle("/dms/rules/audit-log", chain(HandleAuditLog(pool)))
}
