package emailupload

import (
	"net/http"

	middlewares "CimplrCorpSaas/api/middlewares"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterUploadRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	perm := func(page, action string, h http.Handler) http.Handler {
		return chain(middlewares.RequirePermission(pool, page, action)(h))
	}

	mux.Handle("/email/upload/eml", perm("email-upload", middlewares.PermCreate, http.HandlerFunc(HandleUploadEml(pool))))
	mux.Handle("/email/attachments/upload", perm("email-upload", middlewares.PermCreate, http.HandlerFunc(HandleAttachmentUpload(pool))))
}
