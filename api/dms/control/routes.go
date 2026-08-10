package control

import (
	"net/http"
)

func RegisterRoutes(mux *http.ServeMux, chain func(http.Handler) http.Handler) {
	mux.Handle("/dms/generation/control/status", chain(http.HandlerFunc(HandleStatus)))
	mux.Handle("/dms/generation/control/health", chain(http.HandlerFunc(HandleServiceHealth)))
}
