package dash

import (
	"context"
	"net/http"
	"time"
)

const dashQueryTimeout = 3 * time.Minute

func withDashQueryTimeout(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), dashQueryTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
