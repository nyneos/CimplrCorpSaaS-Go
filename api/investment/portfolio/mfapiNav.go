package portfolio

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type mfapiHistoryCacheEntry struct {
	navByDate map[string]float64
	expiry    time.Time
}

var (
	mfapiHistoryCache   sync.Map
	mfapiHistoryCacheTTL = 6 * time.Hour
)

// FetchMFAPINAVHistory loads daily NAV for a scheme between from and to (inclusive).
// Keys are YYYY-MM-DD dates.
func FetchMFAPINAVHistory(ctx context.Context, schemeCode string, from, to time.Time) (map[string]float64, error) {
	code := strings.TrimSpace(schemeCode)
	if code == "" {
		return map[string]float64{}, nil
	}
	fromStr := from.Format(constants.DateFormat)
	toStr := to.Format(constants.DateFormat)
	cacheKey := fmt.Sprintf("%s:%s:%s", code, fromStr, toStr)
	if v, ok := mfapiHistoryCache.Load(cacheKey); ok {
		entry := v.(mfapiHistoryCacheEntry)
		if time.Now().Before(entry.expiry) {
			out := make(map[string]float64, len(entry.navByDate))
			for k, v := range entry.navByDate {
				out[k] = v
			}
			return out, nil
		}
	}

	apiURL := fmt.Sprintf("https://api.mfapi.in/mf/%s?startDate=%s&endDate=%s", code, fromStr, toStr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mfapi status %d", resp.StatusCode)
	}

	var parsed struct {
		Data []struct {
			Date string `json:"date"`
			NAV  string `json:"nav"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}

	navByDate := make(map[string]float64, len(parsed.Data))
	for _, row := range parsed.Data {
		navVal, err := strconv.ParseFloat(strings.TrimSpace(row.NAV), 64)
		if err != nil || navVal <= 0 {
			continue
		}
		dateKey, ok := normalizeMFAPIDate(row.Date)
		if !ok {
			continue
		}
		navByDate[dateKey] = navVal
	}

	mfapiHistoryCache.Store(cacheKey, mfapiHistoryCacheEntry{
		navByDate: navByDate,
		expiry:    time.Now().Add(mfapiHistoryCacheTTL),
	})
	return navByDate, nil
}

func normalizeMFAPIDate(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{constants.DateFormat, constants.DateFormatAlt, "02-Jan-2006", "02-Jan-06"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.Format(constants.DateFormat), true
		}
	}
	return "", false
}

// NAVAsOf returns the latest NAV on or before asOf from a MFAPI history map.
func NAVAsOf(navByDate map[string]float64, asOf time.Time) float64 {
	if len(navByDate) == 0 {
		return 0
	}
	asOfStr := asOf.Format(constants.DateFormat)
	if nav, ok := navByDate[asOfStr]; ok {
		return nav
	}
	var bestDate string
	var bestNav float64
	for dateStr, nav := range navByDate {
		if dateStr <= asOfStr && (bestDate == "" || dateStr > bestDate) {
			bestDate = dateStr
			bestNav = nav
		}
	}
	return bestNav
}
