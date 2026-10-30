package fx

import (
	"net/http"
	"strings"
)

const (
	fxMultipartBodyLimit = 256 << 20
	fxJSONBodyLimit      = 8 << 20
)

// limitFXRequestBody caps every request body: uploads at fxMultipartBodyLimit,
// everything else at fxJSONBodyLimit. Reads past the cap fail inside the handler.
func limitFXRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			limit := int64(fxJSONBodyLimit)
			if strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "multipart/") {
				limit = fxMultipartBodyLimit
			}
			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}
		next.ServeHTTP(w, r)
	})
}
