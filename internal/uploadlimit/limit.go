package uploadlimit

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const defaultMaxBytes int64 = 16 << 20 // 16 MiB

// MaxBytes returns UPLOAD_MAX_BYTES when set (10–25 MiB recommended), else 16 MiB.
func MaxBytes() int64 {
	raw := strings.TrimSpace(os.Getenv("UPLOAD_MAX_BYTES"))
	if raw == "" {
		return defaultMaxBytes
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return defaultMaxBytes
	}
	return n
}

// LimitRequestBody wraps r.Body with MaxBytesReader using MaxBytes().
func LimitRequestBody(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBytes())
}

// ReadAllLimited reads at most MaxBytes()+1; returns an error if the limit is exceeded.
func ReadAllLimited(r io.Reader) ([]byte, error) {
	limit := MaxBytes()
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("upload exceeds maximum size of %d bytes", limit)
	}
	return data, nil
}
