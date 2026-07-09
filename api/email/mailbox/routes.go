package emailmailbox

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterMailboxRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/email/imap/test", chain(http.HandlerFunc(HandleIMAPTest(pool))))
	mux.Handle("/email/graph/test", chain(http.HandlerFunc(HandleGraphTest(pool))))
}
