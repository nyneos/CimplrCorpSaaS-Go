package emailmessages

import (
	"net/http"

	emailcommon "CimplrCorpSaas/api/email/common"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterMessageRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/email/messages/list", chain(http.HandlerFunc(HandleMessageList(pool))))
	mux.Handle("/email/messages/get", chain(http.HandlerFunc(HandleMessageGet(pool))))
	mux.Handle("/email/messages/extract", chain(http.HandlerFunc(HandleMessageExtract(pool))))
	mux.Handle("/email/messages/link", chain(http.HandlerFunc(HandleMessageLink(pool))))
	mux.Handle("/email/attachments/download", chain(http.HandlerFunc(HandleAttachmentDownload(pool))))
	mux.Handle("/email/messages/audit-log", chain(http.HandlerFunc(emailcommon.HandleMessageAuditLog(pool))))
}
