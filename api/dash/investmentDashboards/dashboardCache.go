package investmentdashboards

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api"
)

type investmentDashboardCacheEntry struct {
	status  int
	header  http.Header
	body    []byte
	expires time.Time
}

type investmentDashboardInflight struct {
	wg sync.WaitGroup
}

var investmentDashboardCacheState = struct {
	sync.Mutex
	entries  map[string]investmentDashboardCacheEntry
	inflight map[string]*investmentDashboardInflight
}{
	entries:  make(map[string]investmentDashboardCacheEntry),
	inflight: make(map[string]*investmentDashboardInflight),
}

// CacheDashboardHandler adds a short, access-scoped cache for dashboard widgets.
// It reduces the page-mount fan-out where many widgets ask for the same data
// at once, while preserving each handler's response shape.
func CacheDashboardHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ttl := investmentDashboardCacheTTL()
		if ttl <= 0 || r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))

		key := investmentDashboardCacheKey(r, body)
		now := time.Now()

		if entry, ok := getInvestmentDashboardCacheEntry(key, now); ok {
			writeCachedDashboardResponse(w, entry)
			return
		}

		waiter := beginInvestmentDashboardInflight(key)
		if waiter != nil {
			waiter.wg.Wait()
			if entry, ok := getInvestmentDashboardCacheEntry(key, time.Now()); ok {
				writeCachedDashboardResponse(w, entry)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}

		rec := httptest.NewRecorder()
		next.ServeHTTP(rec, r)

		status := rec.Code
		if status == 0 {
			status = http.StatusOK
		}
		entry := investmentDashboardCacheEntry{
			status:  status,
			header:  rec.Header().Clone(),
			body:    append([]byte(nil), rec.Body.Bytes()...),
			expires: now.Add(ttl),
		}
		if status >= http.StatusOK && status < http.StatusMultipleChoices {
			setInvestmentDashboardCacheEntry(key, entry)
		}
		endInvestmentDashboardInflight(key)
		writeCachedDashboardResponse(w, entry)
	})
}

func investmentDashboardCacheTTL() time.Duration {
	raw := strings.TrimSpace(os.Getenv("INVESTMENT_DASHBOARD_CACHE_TTL_SECONDS"))
	if raw == "" {
		return 60 * time.Second
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 {
		return 15 * time.Second
	}
	return time.Duration(seconds) * time.Second
}

func investmentDashboardCacheKey(r *http.Request, body []byte) string {
	normalizedBody := normalizeDashboardRequestBody(body)
	allowedEntities := append([]string(nil), api.GetEntityNamesFromCtx(r.Context())...)
	allowedEntityIDs := append([]string(nil), api.GetEntityIDsFromCtx(r.Context())...)
	sort.Strings(allowedEntities)
	sort.Strings(allowedEntityIDs)

	material := strings.Join([]string{
		r.Method,
		r.URL.Path,
		string(normalizedBody),
		strings.Join(allowedEntities, "|"),
		strings.Join(allowedEntityIDs, "|"),
		strconv.FormatBool(contextBool(r, "is_admin_override")),
	}, "\n")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])
}

func normalizeDashboardRequestBody(body []byte) []byte {
	var obj map[string]interface{}
	if err := json.Unmarshal(body, &obj); err != nil {
		return bytes.TrimSpace(body)
	}
	delete(obj, "user_id")
	normalized, err := json.Marshal(obj)
	if err != nil {
		return bytes.TrimSpace(body)
	}
	return normalized
}

func contextBool(r *http.Request, key string) bool {
	value, _ := r.Context().Value(key).(bool)
	return value
}

func getInvestmentDashboardCacheEntry(key string, now time.Time) (investmentDashboardCacheEntry, bool) {
	investmentDashboardCacheState.Lock()
	defer investmentDashboardCacheState.Unlock()
	entry, ok := investmentDashboardCacheState.entries[key]
	if now.After(entry.expires) {
		if ok {
			delete(investmentDashboardCacheState.entries, key)
		}
		return investmentDashboardCacheEntry{}, false
	}
	return entry, true
}

func setInvestmentDashboardCacheEntry(key string, entry investmentDashboardCacheEntry) {
	investmentDashboardCacheState.Lock()
	investmentDashboardCacheState.entries[key] = entry
	investmentDashboardCacheState.Unlock()
}

func beginInvestmentDashboardInflight(key string) *investmentDashboardInflight {
	investmentDashboardCacheState.Lock()
	defer investmentDashboardCacheState.Unlock()
	if current := investmentDashboardCacheState.inflight[key]; current != nil {
		return current
	}
	current := &investmentDashboardInflight{}
	current.wg.Add(1)
	investmentDashboardCacheState.inflight[key] = current
	return nil
}

func endInvestmentDashboardInflight(key string) {
	investmentDashboardCacheState.Lock()
	current := investmentDashboardCacheState.inflight[key]
	delete(investmentDashboardCacheState.inflight, key)
	investmentDashboardCacheState.Unlock()
	if current != nil {
		current.wg.Done()
	}
}

func writeCachedDashboardResponse(w http.ResponseWriter, entry investmentDashboardCacheEntry) {
	for key, values := range entry.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(entry.status)
	_, _ = w.Write(entry.body)
}
