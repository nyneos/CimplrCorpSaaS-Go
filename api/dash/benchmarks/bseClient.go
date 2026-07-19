package benchmarks

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const bseBaseURL = "https://www.bseindia.com"
const bseGraphAPI = "https://api.bseindia.com/BseIndiaAPI/api/SensexGraphData/w"

var bseHTTPClient = &http.Client{Timeout: 20 * time.Second}

// bseGetGraphData fetches index chart data from BSE public API.
// index: 16=SENSEX, 53=BANKEX, 134=Focused IT
// flag: 1M, 3M, 6M, 12M (BSE uses 12M for one year)
func bseGetGraphData(ctx context.Context, index int, flag string) ([]byte, error) {
	if flag == "" {
		flag = "12M"
	}
	if flag == "1Y" {
		flag = "12M"
	}

	u := fmt.Sprintf("%s?index=%d&flag=%s&sector=&seriesid=&frd=null&tod=null", bseGraphAPI, index, flag)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Referer", bseBaseURL+"/")

	resp, err := bseHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bse api status %d: %s", resp.StatusCode, string(b))
	}
	return io.ReadAll(resp.Body)
}

func parseBSEGraphData(body []byte) ([]IndexPoint, error) {
	raw := strings.TrimSpace(string(body))
	parts := strings.Split(raw, "#@#")
	if len(parts) < 2 {
		return nil, fmt.Errorf("bse graph: unexpected response format")
	}

	seriesJSON := strings.TrimSpace(parts[1])
	var rows []struct {
		Date  string `json:"date"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(seriesJSON), &rows); err != nil {
		return nil, fmt.Errorf("bse graph parse: %w", err)
	}

	points := make([]IndexPoint, 0, len(rows))
	for _, row := range rows {
		val, err := strconv.ParseFloat(strings.TrimSpace(row.Value), 64)
		if err != nil || val <= 0 {
			continue
		}
		t, err := time.Parse("Mon Jan 02 2006 15:04:05", strings.TrimSpace(row.Date))
		if err != nil {
			continue
		}
		points = append(points, IndexPoint{
			Timestamp: t.UTC().UnixMilli(),
			Date:      t.UTC().Format(constants.DateFormat),
			Value:     val,
		})
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("bse graph: no data points")
	}
	return points, nil
}
