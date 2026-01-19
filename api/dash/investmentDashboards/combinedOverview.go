package investmentdashboards

import (
	"bytes"
	// "context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GetCombinedInvestmentOverview calls the five investment overview endpoints sequentially
// to produce a single consolidated payload. Running sequentially reduces concurrent
// DB load and avoids hitting Postgres `max_client` limit when callers request all
// five endpoints together.
func GetCombinedInvestmentOverview(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		// Read incoming body once and reuse for the sub-requests (they accept similar params)
		bodyBytes, _ := io.ReadAll(r.Body)
		// restore original body so middleware or other readers can still access if needed
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		ctx := r.Context()

		// Allowed entities context is expected to be set by middleware (PreValidation/BusinessUnit)
		// We'll call each handler sequentially and capture their JSON responses.

		// helper to call a handler and return parsed payload
		callHandler := func(h http.HandlerFunc, inBody []byte) (map[string]interface{}, error) {
			// create request with same context and body
			req := httptest.NewRequest(http.MethodPost, "/internal", bytes.NewReader(inBody))
			req = req.WithContext(ctx)
			// response recorder
			rr := httptest.NewRecorder()

			// execute handler (sequentially)
			h.ServeHTTP(rr, req)

			resp := rr.Result()
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			var out map[string]interface{}
			if err := json.Unmarshal(data, &out); err != nil {
				return nil, err
			}
			return out, nil
		}

		// Use the same request body for all sub-endpoints; handlers will ignore unknown fields.
		// Run them sequentially (not goroutines) to avoid parallel DB connections.

		// 1) KPIs
		kpiHandler := GetInvestmentOverviewKPIs(pgxPool)
		kpiResp, err := callHandler(kpiHandler, bodyBytes)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch KPIs: "+err.Error())
			return
		}

		// 2) Consolidated risk
		consolidatedHandler := GetConsolidatedRisk(pgxPool)
		consolidatedResp, err := callHandler(consolidatedHandler, bodyBytes)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch consolidated risk: "+err.Error())
			return
		}

		// 3) Top performing
		topHandler := GetTopPerformingAssets(pgxPool)
		topResp, err := callHandler(topHandler, bodyBytes)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch top performing: "+err.Error())
			return
		}

		// 4) AUM composition
		aumHandler := GetAUMCompositionTrend(pgxPool)
		aumResp, err := callHandler(aumHandler, bodyBytes)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch aum composition: "+err.Error())
			return
		}

		// 5) Market rates ticker
		marketHandler := GetMarketRatesTicker(pgxPool)
		marketResp, err := callHandler(marketHandler, bodyBytes)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to fetch market rates: "+err.Error())
			return
		}

		// Build combined payload
		combined := map[string]interface{}{
			"kpis":         kpiResp,
			"consolidated": consolidatedResp,
			"top":          topResp,
			"aum":          aumResp,
			"market":       marketResp,
			"generated_at": time.Now().UTC().Format(time.RFC3339),
		}

		api.RespondWithPayload(w, true, "", combined)
	}
}
