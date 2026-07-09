package emailinbox

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterInboxRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/email/inbox/list", chain(http.HandlerFunc(HandleInboxList(pool))))
	mux.Handle("/email/inbox/create", chain(http.HandlerFunc(HandleInboxCreate(pool))))
	mux.Handle("/email/inbox/update", chain(http.HandlerFunc(HandleInboxUpdate(pool))))
}
