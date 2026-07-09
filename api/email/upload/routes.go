package emailupload

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterUploadRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/email/upload/eml", chain(http.HandlerFunc(HandleUploadEml(pool))))
	mux.Handle("/email/attachments/upload", chain(http.HandlerFunc(HandleAttachmentUpload(pool))))
}
