package ticker

import (
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"

	"CimplrCorpSaas/internal/logger"
)

// inrRates holds INR-per-unit for each currency (loaded from api/dash/rate.json).
var inrRates map[string]float64

// TickerRequest represents the POST payload.
type TickerRequest struct {
	Base   string   `json:"base"`
	Bases  []string `json:"bases,omitempty"`
	Target string   `json:"target,omitempty"`
	Spot   bool     `json:"spot,omitempty"`
}

// RateEntry mirrors the minimal fields from rate.json
type RateEntry struct {
	CurrencyCode      string  `json:"currencyCode"`
	InrEquivalentRate float64 `json:"inrEquivalentRate"`
}

// init tries to load the rate file; if it fails, it fallbacks to a minimal set.
func init() {
	// Try a few common relative paths so running from different working directories works.
	paths := []string{"api/dash/rate.json", "../api/dash/rate.json", "./api/dash/rate.json"}
	var lastErr error
	for _, p := range paths {
		if err := LoadInrRates(p); err == nil {
			logger.LogInfo("ticker: loaded rates from %s", p)
			return
		} else {
			lastErr = err
		}
	}

	logger.LogError("ticker: failed to load rate.json from known paths: %v, using minimal fallback", lastErr)
	inrRates = map[string]float64{
		"USD": 83.1,
		"EUR": 90.2,
		"GBP": 105.5,
		"INR": 1.0,
	}
}

// LoadInrRates reads the given JSON file and populates the inrRates map.
// The file should be an array of objects containing `currencyCode` and `inrEquivalentRate`.
func LoadInrRates(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// Try array format first: [{"currencyCode":"USD","inrEquivalentRate":83.1}, ...]
	var entries []RateEntry
	if err := json.Unmarshal(b, &entries); err == nil && len(entries) > 0 {
		m := make(map[string]float64, len(entries))
		for _, e := range entries {
			code := strings.ToUpper(strings.TrimSpace(e.CurrencyCode))
			if code == "" || e.InrEquivalentRate == 0 {
				continue
			}
			if _, exists := m[code]; !exists {
				m[code] = e.InrEquivalentRate
			}
		}
		if _, ok := m["INR"]; !ok {
			m["INR"] = 1.0
		}
		inrRates = m
		return nil
	}

	// Try object map format: {"USD":0.01203, "EUR":0.01108, "INR":1}
	var obj map[string]float64
	if err := json.Unmarshal(b, &obj); err == nil && len(obj) > 0 {
		// Normalize keys
		tmp := make(map[string]float64, len(obj))
		for k, v := range obj {
			tmp[strings.ToUpper(strings.TrimSpace(k))] = v
		}

		// If INR==1 and most values <= 1, assume the map is "units per INR" (foreign per 1 INR)
		if v, ok := tmp["INR"]; ok && v == 1 {
			// invert each value to get INR per 1 unit of currency
			m := make(map[string]float64, len(tmp))
			for k, v := range tmp {
				if v <= 0 {
					continue
				}
				// avoid inverting INR itself (1 -> 1)
				if k == "INR" {
					m[k] = 1.0
					continue
				}
				m[k] = 1.0 / v
			}
			inrRates = m
			return nil
		}

		// Otherwise, assume object contains INR-per-unit already
		if _, ok := tmp["INR"]; !ok {
			tmp["INR"] = 1.0
		}
		inrRates = tmp
		return nil
	}

	return fmt.Errorf("unsupported rate.json format")
}

// RatesForBase returns a map of currency -> units-per-1-base for the provided base currency.
// Example: RatesForBase("EUR")["USD"] == how many USD equal 1 EUR.
func RatesForBase(base string) (map[string]float64, error) {
	if inrRates == nil {
		return nil, fmt.Errorf("rates not loaded")
	}
	b := strings.ToUpper(strings.TrimSpace(base))
	if b == "" {
		b = "INR"
	}
	baseInr, ok := inrRates[b]
	if !ok {
		return nil, fmt.Errorf("base currency not found: %s", b)
	}
	out := make(map[string]float64, len(inrRates))
	for code, r := range inrRates {
		out[code] = baseInr / r
	}
	return out, nil
}

// RateBetween returns units of target per 1 base.
func RateBetween(base, target string) (float64, error) {
	if inrRates == nil {
		return 0, fmt.Errorf("rates not loaded")
	}
	b := strings.ToUpper(strings.TrimSpace(base))
	if b == "" {
		b = "INR"
	}
	t := strings.ToUpper(strings.TrimSpace(target))
	baseInr, ok := inrRates[b]
	if !ok {
		return 0, fmt.Errorf("base currency not found: %s", b)
	}
	tr, ok := inrRates[t]
	if !ok {
		return 0, fmt.Errorf("target currency not found: %s", t)
	}
	return baseInr / tr, nil
}

// InrEquivalentRates returns a copy of INR-per-unit rates (same as rate.json inrEquivalentRate).
func InrEquivalentRates() map[string]float64 {
	if inrRates == nil {
		return map[string]float64{"INR": 1.0}
	}
	out := make(map[string]float64, len(inrRates))
	for code, rate := range inrRates {
		out[code] = rate
	}
	return out
}

// ConvertAmountToINR converts amount in the given currency to INR using loaded ticker rates.
// Empty, unknown, or INR currency returns the amount unchanged when no rate is available.
func ConvertAmountToINR(amount float64, currency string) float64 {
	if amount == 0 {
		return 0
	}
	cur := strings.ToUpper(strings.TrimSpace(currency))
	if cur == "" || cur == "INR" {
		return amount
	}
	rate, err := RateBetween(cur, "INR")
	if err != nil || rate == 0 {
		return amount
	}
	return amount * rate
}

// ProcessTicker executes the ticker logic for a given request and returns
// the resulting payload (already ready to encode to JSON), an HTTP status
// code and an error (nil on success). Call this directly from other handlers.
func ProcessTicker(req TickerRequest) (interface{}, int, error) {
	// normalize bases
	base := strings.ToUpper(strings.TrimSpace(req.Base))
	bases := make([]string, 0)
	if len(req.Bases) > 0 {
		for _, b := range req.Bases {
			if s := strings.ToUpper(strings.TrimSpace(b)); s != "" {
				bases = append(bases, s)
			}
		}
	} else {
		if base == "" {
			base = "INR"
		}
		bases = append(bases, base)
	}

	// target case
	if req.Target != "" {
		target := strings.ToUpper(strings.TrimSpace(req.Target))
		result := make(map[string]interface{})
		for _, b := range bases {
			baseInr, okB := inrRates[b]
			trInr, okT := inrRates[target]
			if !okB || !okT {
				result[b] = map[string]string{"error": "base or target not found"}
				continue
			}
			// BASE/TARGET = units of TARGET per 1 BASE = INR_per_BASE / INR_per_TARGET
			rate := baseInr / trInr
			rate = math.Round(rate*10000) / 10000
			if req.Spot {
				result[b] = map[string]interface{}{"pair": fmt.Sprintf("%s/%s", b, target), "rate": rate}
			} else {
				result[b] = map[string]interface{}{"base": b, "target": target, "rate": rate}
			}
		}
		// if caller provided a single base, return just that inner value
		if len(bases) == 1 {
			return result[bases[0]], http.StatusOK, nil
		}
		return result, http.StatusOK, nil
	}

	// full-list case
	codes := make([]string, 0, len(inrRates))
	for c := range inrRates {
		codes = append(codes, c)
	}
	sort.Strings(codes)

	baseResults := make(map[string]interface{}, len(bases))
	for _, b := range bases {
		baseInr, ok := inrRates[b]
		if !ok {
			baseResults[b] = map[string]string{"error": "base not found"}
			continue
		}
		if req.Spot {
			type PairRate struct {
				Pair string  `json:"pair"`
				Rate float64 `json:"rate"`
			}
			pairs := make([]PairRate, 0, len(codes))
			for _, c := range codes {
				r := inrRates[c]
				// BASE/TARGET = INR_per_BASE / INR_per_TARGET
				rate := baseInr / r
				rate = math.Round(rate*10000) / 10000
				pairs = append(pairs, PairRate{Pair: fmt.Sprintf("%s/%s", b, c), Rate: rate})
			}
			baseResults[b] = pairs
		} else {
			out := make(map[string]float64, len(codes))
			for _, c := range codes {
				r := inrRates[c]
				// BASE/TARGET = INR_per_BASE / INR_per_TARGET
				out[c] = math.Round(baseInr/r*10000) / 10000
			}
			baseResults[b] = out
		}
	}

	if len(bases) == 1 {
		return baseResults[bases[0]], http.StatusOK, nil
	}
	return baseResults, http.StatusOK, nil
}

// GetTickerHandler returns an http.Handler suitable for registration.
func GetTickerHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// enforce POST at the HTTP entrypoint
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		tickerHandler(w, r)
	})
}

// GetInrRatesHandler returns INR-per-unit rates for all known currencies.
func GetInrRatesHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSONUTF8)
		json.NewEncoder(w).Encode(InrEquivalentRates())
	})
}

func tickerHandler(w http.ResponseWriter, r *http.Request) {

	var req TickerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	// normalize bases
	base := strings.ToUpper(strings.TrimSpace(req.Base))
	bases := make([]string, 0)
	if len(req.Bases) > 0 {
		for _, b := range req.Bases {
			if s := strings.ToUpper(strings.TrimSpace(b)); s != "" {
				bases = append(bases, s)
			}
		}
	} else {
		if base == "" {
			base = "INR"
		}
		bases = append(bases, base)
	}

	// single target case: support multiple bases -> return rate per base for the same target
	if req.Target != "" {
		target := strings.ToUpper(strings.TrimSpace(req.Target))
		result := make(map[string]interface{})
		for _, b := range bases {
			baseInr, okB := inrRates[b]
			trInr, okT := inrRates[target]
			if !okB || !okT {
				result[b] = map[string]string{"error": "base or target not found"}
				continue
			}
			// BASE/TARGET = units of TARGET per 1 BASE = INR_per_BASE / INR_per_TARGET
			rate := baseInr / trInr
			rate = math.Round(rate*10000) / 10000
			if req.Spot {
				result[b] = map[string]interface{}{"pair": fmt.Sprintf("%s/%s", b, target), "rate": rate}
			} else {
				result[b] = map[string]interface{}{"base": b, "target": target, "rate": rate}
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSONUTF8)
		json.NewEncoder(w).Encode(result)
		return
	}

	// no target: return all rates relative to base

	// For full-list, produce base-per-1-target values for every currency:
	// rate = INR_per_target / INR_per_base
	// For full-list: for each base in bases produce pairs. Output structure:
	// if multiple bases supplied -> map[base] => (array of pairs if spot, or map[target]=>rate if not)
	baseResults := make(map[string]interface{}, len(bases))
	codes := make([]string, 0, len(inrRates))
	for c := range inrRates {
		codes = append(codes, c)
	}
	sort.Strings(codes)

	for _, b := range bases {
		baseInr, ok := inrRates[b]
		if !ok {
			baseResults[b] = map[string]string{"error": "base not found"}
			continue
		}
		if req.Spot {
			type PairRate struct {
				Pair string  `json:"pair"`
				Rate float64 `json:"rate"`
			}
			pairs := make([]PairRate, 0, len(codes))
			for _, c := range codes {
				r := inrRates[c]
				// BASE/TARGET = INR_per_BASE / INR_per_TARGET
				rate := baseInr / r
				rate = math.Round(rate*10000) / 10000
				pairs = append(pairs, PairRate{Pair: fmt.Sprintf("%s/%s", b, c), Rate: rate})
			}
			baseResults[b] = pairs
		} else {
			out := make(map[string]float64, len(codes))
			for _, c := range codes {
				r := inrRates[c]
				// BASE/TARGET = INR_per_BASE / INR_per_TARGET
				out[c] = math.Round(baseInr/r*10000) / 10000
			}
			baseResults[b] = out
		}
	}

	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSONUTF8)
	// If only one base requested, return its value directly to preserve previous behavior
	if len(bases) == 1 {
		json.NewEncoder(w).Encode(baseResults[bases[0]])
		return
	}
	json.NewEncoder(w).Encode(baseResults)
}
