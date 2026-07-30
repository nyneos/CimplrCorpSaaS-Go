package execution

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RegisterRoutes(mux *http.ServeMux, pool *pgxpool.Pool, chain func(http.Handler) http.Handler) {
	mux.Handle("/policy-engine/execution/list", chain(http.HandlerFunc(HandleList(pool))))
	mux.Handle("/policy-engine/execution/detail", chain(http.HandlerFunc(HandleDetail(pool))))
}
