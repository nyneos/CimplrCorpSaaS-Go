package benchmarks

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// NSE Client with Cookie Handling (NSE requires cookies from initial page load)
// ─────────────────────────────────────────────────────────────────────────────

var (
	nseClient     *http.Client
	nseClientOnce sync.Once
	nseCookieMu   sync.Mutex
	nseCookieExp  time.Time
)

const nseBaseURL = "https://www.nseindia.com"
const nseAPIBase = nseBaseURL + "/api/NextApi/apiClient"

// getNSEClient returns a shared HTTP client with cookie jar for NSE
func getNSEClient() *http.Client {
	nseClientOnce.Do(func() {
		jar, _ := cookiejar.New(nil)
		nseClient = &http.Client{
			Timeout: 20 * time.Second,
			Jar:     jar,
		}
	})
	return nseClient
}

// ensureNSECookies fetches the NSE homepage to obtain session cookies if expired
func ensureNSECookies(ctx context.Context) error {
	nseCookieMu.Lock()
	defer nseCookieMu.Unlock()

	if time.Now().Before(nseCookieExp) {
		return nil // cookies still valid
	}

	client := getNSEClient()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, nseBaseURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("nse cookie preflight failed: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body) // drain body

	// Cache cookies for 5 minutes
	nseCookieExp = time.Now().Add(5 * time.Minute)
	return nil
}

// nseRequest performs an authenticated request to NSE API
func nseRequest(ctx context.Context, pathSuffix string, params url.Values) ([]byte, error) {
	if err := ensureNSECookies(ctx); err != nil {
		return nil, err
	}

	u := nseAPIBase + pathSuffix
	if params != nil {
		u += "?" + params.Encode()
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Referer", nseBaseURL)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	client := getNSEClient()
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("nse api status %d: %s", resp.StatusCode, string(b))
	}
	return io.ReadAll(resp.Body)
}

// ─────────────────────────────────────────────────────────────────────────────
// NSE ENDPOINT FUNCTIONS (all discovered endpoints)
// ─────────────────────────────────────────────────────────────────────────────

// A. Index List / Metadata

func nseGetIndexData(ctx context.Context, indexType string) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getIndexData")
	p.Set("type", indexType)
	return nseRequest(ctx, "", p)
}

func nseGetIndicesData(ctx context.Context, index string) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getIndicesData")
	p.Set("type", index)
	return nseRequest(ctx, "", p)
}

func nseGetIndexConstituents(ctx context.Context, index string) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getParticularIndexStockOIData")
	p.Set("key", index)
	return nseRequest(ctx, "", p)
}

func nseGetSectorIndices(ctx context.Context) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "sectorIndices")
	return nseRequest(ctx, "", p)
}

func nseGetMarketHeatMap(ctx context.Context, heatmapType string) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getMarketHeatMap")
	p.Set("type", heatmapType)
	return nseRequest(ctx, "", p)
}

// B. Chart / Graph Data

func nseGetGraphChart(ctx context.Context, index, flag string) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getGraphChart")
	p.Set("type", index)
	if flag != "" {
		p.Set("flag", flag)
	}
	return nseRequest(ctx, "/historicalGraph", p)
}

func nseGetAnalysisChart(ctx context.Context, symbol string) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getAnalysisChart")
	p.Set("symbol", symbol)
	return nseRequest(ctx, "", p)
}

func nseGetMarketTurnoverChart(ctx context.Context) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "marketTurnOverChart")
	return nseRequest(ctx, "", p)
}

// C. Market Movers

func nseGetGainerLoser(ctx context.Context, index string) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getGainerLoser")
	p.Set("type", index)
	return nseRequest(ctx, "", p)
}

func nseGetAdvanceDeclines(ctx context.Context) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getAllAdvanceDeclines")
	return nseRequest(ctx, "", p)
}

func nseGet52WeekHighLow(ctx context.Context) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "weekHighLowStocks")
	return nseRequest(ctx, "", p)
}

// D. Market Snapshot / Summary

func nseGetMarketStatus(ctx context.Context) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "marketStatus")
	return nseRequest(ctx, "", p)
}

func nseGetMarqueeData(ctx context.Context) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getMarqueeData")
	return nseRequest(ctx, "", p)
}

func nseGetNiftyIndicesData(ctx context.Context) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getNiftyIndicesData")
	return nseRequest(ctx, "", p)
}

func nseGetHomePageNavBarData(ctx context.Context, key string) ([]byte, error) {
	p := url.Values{}
	p.Set("functionName", "getHomePageNavBarData")
	p.Set("key", key)
	return nseRequest(ctx, "", p)
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP HANDLERS
// ─────────────────────────────────────────────────────────────────────────────

// GetBenchmarkCatalog returns curated NSE/BSE benchmarks for portfolio comparison dropdowns.
func GetBenchmarkCatalog() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		catalog := GetCuratedBenchmarks()
		options := make([]map[string]string, 0, len(catalog))
		for _, def := range catalog {
			options = append(options, map[string]string{
				"value": def.ID, "label": def.Name, "provider": def.Provider,
			})
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"benchmarks": catalog,
			"options":    options,
			"providers":  []string{"nse", "bse"},
		})
	}
}

// GetIndexList returns all available indices for dropdown
func GetIndexList() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := strings.ToLower(r.URL.Query().Get("provider"))
		if provider == "" {
			provider = "nse"
		}

		ctx := r.Context()
		switch provider {
		case "nse":
			indexData, err1 := nseGetIndexData(ctx, "All")
			sectorData, err2 := nseGetSectorIndices(ctx)

			result := map[string]interface{}{
				"provider": "nse",
			}
			if err1 == nil {
				result["indices"] = json.RawMessage(indexData)
			} else {
				result["indices_error"] = err1.Error()
			}
			if err2 == nil {
				result["sectors"] = json.RawMessage(sectorData)
			} else {
				result["sectors_error"] = err2.Error()
			}
			api.RespondWithPayload(w, true, "", result)
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// GetIndexSeries returns graph/chart data for a specific index
func GetIndexSeries() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			Provider string `json:"provider"`
			Index    string `json:"index"`
			Flag     string `json:"flag"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if req.Provider == "" {
			req.Provider = "nse"
		}
		if req.Index == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrIndexRequired)
			return
		}
		if req.Flag == "" {
			req.Flag = "1Y"
		}

		ctx := r.Context()
		switch strings.ToLower(req.Provider) {
		case "nse":
			body, err := nseGetGraphChart(ctx, req.Index, req.Flag)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			points, parseErr := parseNSEGraphPoints(body)
			normalized := normalizeNSEGraphData(body)
			if parseErr == nil && len(points) > 0 {
				normalized = indexPointsToNormalized(points)
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider":   "nse",
				"index":      req.Index,
				"flag":       req.Flag,
				"normalized": normalized,
				"raw":        json.RawMessage(body),
			})
		case "bse":
			def, ok := ResolveBenchmark(req.Index)
			if !ok || def.BSEIndex == 0 {
				api.RespondWithError(w, http.StatusBadRequest, "unknown BSE benchmark index")
				return
			}
			points, err := FetchIndexSeries(ctx, def, req.Flag)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider":   "bse",
				"index":      req.Index,
				"bse_index":  def.BSEIndex,
				"flag":       req.Flag,
				"normalized": indexPointsToNormalized(points),
			})
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// GetIndexSnapshot returns current snapshot data for an index
func GetIndexSnapshot() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			Provider string `json:"provider"`
			Index    string `json:"index"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if req.Provider == "" {
			req.Provider = "nse"
		}
		if req.Index == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrIndexRequired)
			return
		}

		ctx := r.Context()
		switch strings.ToLower(req.Provider) {
		case "nse":
			body, err := nseGetIndicesData(ctx, req.Index)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider": "nse",
				"index":    req.Index,
				"raw":      json.RawMessage(body),
			})
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// GetIndexConstituents returns stocks in an index
func GetIndexConstituents() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			Provider string `json:"provider"`
			Index    string `json:"index"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if req.Provider == "" {
			req.Provider = "nse"
		}
		if req.Index == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrIndexRequired)
			return
		}

		ctx := r.Context()
		switch strings.ToLower(req.Provider) {
		case "nse":
			body, err := nseGetIndexConstituents(ctx, req.Index)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider": "nse",
				"index":    req.Index,
				"raw":      json.RawMessage(body),
			})
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// GetMarketMovers returns gainers/losers for an index
func GetMarketMovers() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req struct {
			Provider string `json:"provider"`
			Index    string `json:"index"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if req.Provider == "" {
			req.Provider = "nse"
		}
		if req.Index == "" {
			req.Index = constants.Nifty50
		}

		ctx := r.Context()
		switch strings.ToLower(req.Provider) {
		case "nse":
			body, err := nseGetGainerLoser(ctx, req.Index)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider": "nse",
				"index":    req.Index,
				"raw":      json.RawMessage(body),
			})
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// GetMarketStatus returns current market status
func GetMarketStatus() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := strings.ToLower(r.URL.Query().Get("provider"))
		if provider == "" {
			provider = "nse"
		}

		ctx := r.Context()
		switch provider {
		case "nse":
			body, err := nseGetMarketStatus(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider": "nse",
				"raw":      json.RawMessage(body),
			})
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// GetMarketHeatmap returns index heatmap data
func GetMarketHeatmap() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := strings.ToLower(r.URL.Query().Get("provider"))
		if provider == "" {
			provider = "nse"
		}
		heatmapType := r.URL.Query().Get("type")
		if heatmapType == "" {
			heatmapType = "Index"
		}

		ctx := r.Context()
		switch provider {
		case "nse":
			body, err := nseGetMarketHeatMap(ctx, heatmapType)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider": "nse",
				"type":     heatmapType,
				"raw":      json.RawMessage(body),
			})
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// GetAdvanceDeclines returns market breadth data
func GetAdvanceDeclines() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := strings.ToLower(r.URL.Query().Get("provider"))
		if provider == "" {
			provider = "nse"
		}

		ctx := r.Context()
		switch provider {
		case "nse":
			body, err := nseGetAdvanceDeclines(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider": "nse",
				"raw":      json.RawMessage(body),
			})
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// GetMarqueeData returns ticker/marquee data
func GetMarqueeData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := strings.ToLower(r.URL.Query().Get("provider"))
		if provider == "" {
			provider = "nse"
		}

		ctx := r.Context()
		switch provider {
		case "nse":
			marquee, err1 := nseGetMarqueeData(ctx)
			niftyData, err2 := nseGetNiftyIndicesData(ctx)

			result := map[string]interface{}{
				"provider": "nse",
			}
			if err1 == nil {
				result["marquee"] = json.RawMessage(marquee)
			} else {
				result["marquee_error"] = err1.Error()
			}
			if err2 == nil {
				result["nifty_indices"] = json.RawMessage(niftyData)
			} else {
				result["nifty_indices_error"] = err2.Error()
			}
			api.RespondWithPayload(w, true, "", result)
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// Get52WeekHighLow returns stocks at 52-week highs/lows
func Get52WeekHighLow() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := strings.ToLower(r.URL.Query().Get("provider"))
		if provider == "" {
			provider = "nse"
		}

		ctx := r.Context()
		switch provider {
		case "nse":
			body, err := nseGet52WeekHighLow(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider": "nse",
				"raw":      json.RawMessage(body),
			})
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// GetMarketTurnover returns volume/turnover chart data
func GetMarketTurnover() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		provider := strings.ToLower(r.URL.Query().Get("provider"))
		if provider == "" {
			provider = "nse"
		}

		ctx := r.Context()
		switch provider {
		case "nse":
			body, err := nseGetMarketTurnoverChart(ctx)
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, err.Error())
				return
			}
			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"provider": "nse",
				"raw":      json.RawMessage(body),
			})
		default:
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUnsupportedProvider)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NORMALIZERS
// ─────────────────────────────────────────────────────────────────────────────

type NormalizedPoint struct {
	Timestamp int64   `json:"t"`
	Value     float64 `json:"v"`
	Date      string  `json:"date"`
	Label     string  `json:"label"`
	Indexed   float64 `json:"indexed"`
}

func normalizeNSEGraphData(body []byte) interface{} {
	points, err := parseNSEGraphPoints(body)
	if err != nil || len(points) == 0 {
		var parsed map[string]interface{}
		if json.Unmarshal(body, &parsed) == nil {
			return parsed
		}
		return nil
	}
	return indexPointsToNormalized(points)
}

func indexPointsToNormalized(points []IndexPoint) []NormalizedPoint {
	out := make([]NormalizedPoint, 0, len(points))
	var baseValue float64
	for i, p := range points {
		np := NormalizedPoint{
			Timestamp: p.Timestamp,
			Value:     p.Value,
			Date:      p.Date,
		}
		if p.Date != "" {
			if t, err := time.Parse(constants.DateFormat, p.Date); err == nil {
				np.Label = t.Format("Jan")
			}
		}
		if i == 0 {
			baseValue = p.Value
		}
		if baseValue > 0 {
			np.Indexed = (p.Value / baseValue) * 100
		}
		out = append(out, np)
	}
	return out
}
