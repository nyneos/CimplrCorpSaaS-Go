package benchmarks

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// IndexPoint is a single close/value observation for an index time series.
type IndexPoint struct {
	Timestamp int64   `json:"t"`
	Date      string  `json:"date"`
	Value     float64 `json:"v"`
	Indexed   float64 `json:"indexed,omitempty"`
}

// BenchmarkDefinition maps a user-facing benchmark name to NSE or BSE API params.
type BenchmarkDefinition struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Provider string `json:"provider"` // "nse" or "bse"
	NSEType  string `json:"nse_type,omitempty"`
	BSEIndex int    `json:"bse_index,omitempty"`
}

var curatedBenchmarks = []BenchmarkDefinition{
	{ID: constants.Nifty50, Name: constants.Nifty50, Provider: "nse", NSEType: constants.Nifty50},
	{ID: "NIFTY 100", Name: "NIFTY 100", Provider: "nse", NSEType: "NIFTY 100"},
	{ID: "NIFTY BANK", Name: "NIFTY BANK", Provider: "nse", NSEType: "NIFTY BANK"},
	{ID: "NIFTY IT", Name: "NIFTY IT", Provider: "nse", NSEType: "NIFTY IT"},
	{ID: "SENSEX", Name: "BSE SENSEX", Provider: "bse", BSEIndex: 16},
	{ID: "BSE BANKEX", Name: "BSE BANKEX", Provider: "bse", BSEIndex: 53},
	{ID: "BSE FOCUSED IT", Name: "BSE Focused IT", Provider: "bse", BSEIndex: 134},
}

// GetCuratedBenchmarks returns the supported benchmark list for portfolio comparison.
func GetCuratedBenchmarks() []BenchmarkDefinition {
	out := make([]BenchmarkDefinition, len(curatedBenchmarks))
	copy(out, curatedBenchmarks)
	return out
}

// ResolveBenchmark finds a benchmark definition by id or display name (case-insensitive).
func ResolveBenchmark(name string) (BenchmarkDefinition, bool) {
	key := strings.TrimSpace(name)
	if key == "" {
		return BenchmarkDefinition{}, false
	}
	upper := strings.ToUpper(key)
	for _, def := range curatedBenchmarks {
		if strings.EqualFold(def.ID, key) || strings.EqualFold(def.Name, key) {
			return def, true
		}
		if upper == "SENSEX" && def.BSEIndex == 16 {
			return def, true
		}
	}
	// Allow any NSE index name not in curated list.
	return BenchmarkDefinition{
		ID: key, Name: key, Provider: "nse", NSEType: key,
	}, true
}

type seriesCacheEntry struct {
	points []IndexPoint
	expiry time.Time
}

var (
	seriesCache   sync.Map
	seriesCacheTTL = 15 * time.Minute
)

// FetchIndexSeries loads daily index closes from NSE or BSE for the given period flag.
func FetchIndexSeries(ctx context.Context, def BenchmarkDefinition, flag string) ([]IndexPoint, error) {
	if flag == "" {
		flag = "1Y"
	}
	cacheKey := fmt.Sprintf("%s:%s:%s:%d", def.Provider, def.NSEType, flag, def.BSEIndex)
	if v, ok := seriesCache.Load(cacheKey); ok {
		entry := v.(seriesCacheEntry)
		if time.Now().Before(entry.expiry) {
			out := make([]IndexPoint, len(entry.points))
			copy(out, entry.points)
			return out, nil
		}
	}

	var points []IndexPoint
	var err error
	switch strings.ToLower(def.Provider) {
	case "bse":
		body, fetchErr := bseGetGraphData(ctx, def.BSEIndex, flag)
		if fetchErr != nil {
			return nil, fetchErr
		}
		points, err = parseBSEGraphData(body)
	case "nse":
		nseFlag := flag
		if nseFlag == "12M" {
			nseFlag = "1Y"
		}
		body, fetchErr := nseGetGraphChart(ctx, def.NSEType, nseFlag)
		if fetchErr != nil {
			return nil, fetchErr
		}
		points, err = parseNSEGraphPoints(body)
	default:
		return nil, fmt.Errorf("unsupported benchmark provider: %s", def.Provider)
	}
	if err != nil {
		return nil, err
	}

	seriesCache.Store(cacheKey, seriesCacheEntry{points: points, expiry: time.Now().Add(seriesCacheTTL)})
	out := make([]IndexPoint, len(points))
	copy(out, points)
	return out, nil
}

// BuildIndexedMonthSeries indexes an index to base 100 at the first month-end anchor,
// same methodology as NSE/BSE charts: Indexed(t) = 100 × Close(t) / Close(t₀).
func BuildIndexedMonthSeries(points []IndexPoint, monthEnds []time.Time) []float64 {
	if len(monthEnds) == 0 {
		return nil
	}
	baseValue, ok := valueAtOrBefore(points, monthEnds[0])
	if !ok || baseValue <= 0 {
		baseValue, ok = firstPositiveValue(points)
		if !ok {
			return nil
		}
	}

	out := make([]float64, len(monthEnds))
	for i, me := range monthEnds {
		if i == 0 {
			out[i] = 100
			continue
		}
		val, found := valueAtOrBefore(points, me)
		if !found || val <= 0 {
			out[i] = out[i-1]
			continue
		}
		out[i] = math.Round((100*val/baseValue)*100) / 100
	}
	return out
}

// BuildPortfolioIndexedSeriesTWR chain-links monthly returns after stripping net cash flows.
// Indexed(0)=100 at first FY month-end; later months use (AUM_end - AUM_start - CF) / AUM_start.
func BuildPortfolioIndexedSeriesTWR(monthEndAUM map[int]float64, monthCashFlow map[int]float64, monthCount int) []float64 {
	if monthCount <= 0 {
		return nil
	}

	aumAt := func(monthIdx int) float64 {
		return monthEndAUM[monthIdx]
	}
	cfAt := func(monthIdx int) float64 {
		return monthCashFlow[monthIdx]
	}

	out := make([]float64, monthCount)
	for i := 0; i < monthCount; i++ {
		monthIdx := i + 1
		aumEnd := aumAt(monthIdx)

		if i == 0 {
			out[i] = 100
			continue
		}

		aumPrev := aumAt(monthIdx - 1)
		if aumPrev <= 0 {
			if aumEnd > 0 {
				out[i] = 100
			} else {
				out[i] = out[i-1]
			}
			continue
		}

		cf := cfAt(monthIdx)
		monthReturn := (aumEnd - aumPrev - cf) / aumPrev
		out[i] = RoundIndexed(out[i-1] * (1 + monthReturn))
	}
	return out
}

// BuildPortfolioIndexedSeriesFromAUM indexes portfolio to 100 at the first month with AUM > 0.
// Indexed(t) = 100 × AUM(t) / AUM(first_active_month) — same method as NSE/BSE index charts.
func BuildPortfolioIndexedSeriesFromAUM(monthEndAUM map[int]float64, monthCount int) []float64 {
	if monthCount <= 0 {
		return nil
	}

	aumAt := func(monthIdx int) float64 {
		return monthEndAUM[monthIdx]
	}

	baseIdx := -1
	var baseAUM float64
	for i := 1; i <= monthCount; i++ {
		if v := aumAt(i); v > 0 {
			baseIdx = i - 1 // 0-based output index
			baseAUM = v
			break
		}
	}

	out := make([]float64, monthCount)
	for i := 0; i < monthCount; i++ {
		if baseIdx < 0 || i < baseIdx {
			out[i] = 100
			continue
		}
		if i == baseIdx {
			out[i] = 100
			continue
		}
		v := aumAt(i + 1)
		if v <= 0 || baseAUM <= 0 {
			out[i] = out[i-1]
			continue
		}
		out[i] = RoundIndexed(100 * v / baseAUM)
	}
	return out
}

func parseNSEGraphPoints(body []byte) ([]IndexPoint, error) {
	var parsed map[string]interface{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}

	dataArr := extractNSEDataArray(parsed)
	if len(dataArr) == 0 {
		return nil, fmt.Errorf("nse graph: no data points")
	}

	points := make([]IndexPoint, 0, len(dataArr))
	for _, item := range dataArr {
		pt := indexPointFromNSEItem(item)
		if pt.Value > 0 {
			points = append(points, pt)
		}
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("nse graph: no valid points")
	}
	return points, nil
}

func extractNSEDataArray(parsed map[string]interface{}) []interface{} {
	if dataObj, ok := parsed["data"].(map[string]interface{}); ok {
		if d, ok := dataObj["grapthData"].([]interface{}); ok {
			return d
		}
		if d, ok := dataObj["graphData"].([]interface{}); ok {
			return d
		}
	}
	if d, ok := parsed["data"].([]interface{}); ok {
		return d
	}
	if d, ok := parsed["chartData"].([]interface{}); ok {
		return d
	}
	if d, ok := parsed["series"].([]interface{}); ok {
		return d
	}
	return nil
}

func indexPointFromNSEItem(item interface{}) IndexPoint {
	pt := IndexPoint{}
	switch v := item.(type) {
	case []interface{}:
		if len(v) >= 2 {
			if ts, ok := v[0].(float64); ok {
				pt.Timestamp = int64(ts)
				pt.Date = time.UnixMilli(int64(ts)).UTC().Format(constants.DateFormat)
			}
			if val, ok := v[1].(float64); ok {
				pt.Value = val
			}
		}
	case map[string]interface{}:
		if ts, ok := v["timestamp"].(float64); ok {
			pt.Timestamp = int64(ts)
			pt.Date = time.UnixMilli(int64(ts)).UTC().Format(constants.DateFormat)
		} else if ts, ok := v["time"].(float64); ok {
			pt.Timestamp = int64(ts)
			pt.Date = time.UnixMilli(int64(ts)).UTC().Format(constants.DateFormat)
		} else if dateStr, ok := v["date"].(string); ok {
			pt.Date = dateStr
			if t, err := time.Parse(constants.DateFormatDash, dateStr); err == nil {
				pt.Timestamp = t.UnixMilli()
			}
		}
		for _, key := range []string{"value", "close", "indexCloseOnline", "CLOSE"} {
			if val, ok := v[key].(float64); ok {
				pt.Value = val
				break
			}
		}
	}
	return pt
}

func valueAtOrBefore(points []IndexPoint, t time.Time) (float64, bool) {
	target := t.UTC().Truncate(24 * time.Hour)
	var best IndexPoint
	found := false
	for _, p := range points {
		pt, err := time.Parse(constants.DateFormat, p.Date)
		if err != nil {
			continue
		}
		pt = pt.UTC().Truncate(24 * time.Hour)
		if pt.After(target) {
			continue
		}
		if !found || pt.After(parsePointDate(best)) {
			best = p
			found = true
		}
	}
	if !found {
		return 0, false
	}
	return best.Value, true
}

func firstPositiveValue(points []IndexPoint) (float64, bool) {
	for _, p := range points {
		if p.Value > 0 {
			return p.Value, true
		}
	}
	return 0, false
}

func parsePointDate(p IndexPoint) time.Time {
	if p.Date != "" {
		if t, err := time.Parse(constants.DateFormat, p.Date); err == nil {
			return t.UTC().Truncate(24 * time.Hour)
		}
	}
	return time.UnixMilli(p.Timestamp).UTC().Truncate(24 * time.Hour)
}

// FYChartFlag picks an NSE/BSE chart flag that covers the financial year window.
func FYChartFlag(monthCount int) string {
	switch {
	case monthCount <= 1:
		return "1M"
	case monthCount <= 3:
		return "3M"
	case monthCount <= 6:
		return "6M"
	default:
		return "1Y"
	}
}

// ParseMonthEndDates converts YYYY-MM-DD strings to time.Time month-end values.
func ParseMonthEndDates(dates []string) []time.Time {
	out := make([]time.Time, 0, len(dates))
	for _, d := range dates {
		t, err := time.Parse(constants.DateFormat, d)
		if err != nil {
			continue
		}
		out = append(out, t.UTC())
	}
	return out
}

// RoundIndexed rounds indexed values for API output.
func RoundIndexed(v float64) float64 {
	return math.Round(v*100) / 100
}
