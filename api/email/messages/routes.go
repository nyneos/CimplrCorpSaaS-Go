package emailmessages

import (
	"net/http"

	emailcommon "CimplrCorpSaas/api/email/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterMessageRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/email/messages/list", chain(http.HandlerFunc(HandleMessageList(pool))))
	mux.Handle("/email/transform-results/list", chain(http.HandlerFunc(HandleTransformResultsList(pool))))
	mux.Handle("/email/transform-results/stats", chain(http.HandlerFunc(HandleTransformResultsStats(pool))))
	mux.Handle("/email/transform-results/get", chain(http.HandlerFunc(HandleTransformResultGet(pool))))
	mux.Handle("/email/transform-results/preview-content", chain(http.HandlerFunc(HandleTransformResultPreviewContent(pool))))
	mux.Handle("/email/transform-results/bulk-download", chain(http.HandlerFunc(HandleTransformResultsBulkDownload(pool))))
	mux.Handle("/email/messages/get", chain(http.HandlerFunc(HandleMessageGet(pool))))
	mux.Handle("/email/messages/extract", chain(http.HandlerFunc(HandleMessageExtract(pool))))
	mux.Handle("/email/messages/link", chain(http.HandlerFunc(HandleMessageLink(pool))))
	mux.Handle("/email/attachments/download", chain(http.HandlerFunc(HandleAttachmentDownload(pool))))
	mux.Handle("/email/messages/audit-log", chain(http.HandlerFunc(emailcommon.HandleMessageAuditLog(pool))))
}
