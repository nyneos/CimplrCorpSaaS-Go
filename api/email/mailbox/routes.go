package emailmailbox

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterMailboxRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/email/imap/test", chain(http.HandlerFunc(HandleIMAPTest(pool))))
	mux.Handle("/email/graph/test", chain(http.HandlerFunc(HandleGraphTest(pool))))
	mux.Handle("/email/google-workspace/test", chain(http.HandlerFunc(HandleGoogleWorkspaceTest(pool))))
	mux.Handle("/email/oauth/start", chain(http.HandlerFunc(HandleOAuthStart(pool))))
	mux.Handle("/email/oauth/status", chain(http.HandlerFunc(HandleOAuthStatus(pool))))
	mux.Handle("/email/oauth/test", chain(http.HandlerFunc(HandleOAuthTest(pool))))
	// Browser redirect from IdP — session cookie required.
	mux.Handle("/email/oauth/callback", chain(http.HandlerFunc(HandleOAuthCallback(pool))))
}
