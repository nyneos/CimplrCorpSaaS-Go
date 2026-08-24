package runtime

import (
	"strconv"
	"strings"
)

func lookupCDMVar(vars map[string]string, suffixes ...string) string {
	for _, suffix := range suffixes {
		for k, v := range vars {
			if !strings.HasSuffix(strings.ToLower(strings.TrimSpace(k)), suffix) {
				continue
			}
			if val := strings.TrimSpace(v); val != "" {
				return val
			}
		}
	}
	return ""
}

func lookupCDMVarAll(vars map[string]string, suffixes ...string) []string {
	out := make([]string, 0, len(suffixes))
	for _, suffix := range suffixes {
		for k, v := range vars {
			if !strings.HasSuffix(strings.ToLower(strings.TrimSpace(k)), suffix) {
				continue
			}
			val := strings.TrimSpace(v)
			if val == "" || containsFold(out, val) {
				continue
			}
			out = append(out, val)
		}
	}
	return out
}

func currencyFromVars(vars map[string]string) []string {
	return lookupCDMVarAll(vars,
		".currency_code", ".currency",
		".base_currency", ".quote_currency", ".local_currency")
}

func tenorBandFromVars(vars map[string]string) string {
	years := 0.0
	if v := lookupCDMVar(vars, ".tenure_years", ".tenor_years"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			years = f
		}
	}
	if years == 0 {
		if v := lookupCDMVar(vars, ".tenor_months", ".tenure_months"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
				years = f / 12
			}
		}
	}
	if years == 0 {
		if v := lookupCDMVar(vars, ".tenor_days", ".tenure_days"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
				years = f / 365
			}
		}
	}
	if years <= 0 {
		return ""
	}
	switch {
	case years <= 1:
		return "<= 1Y"
	case years <= 3:
		return "1-3Y"
	case years <= 5:
		return "3-5Y"
	default:
		return "> 5Y"
	}
}

func policyMatchesDataFilters(instrument, currency, tenor, rating string, vars map[string]string) bool {
	if f := strings.TrimSpace(currency); f != "" {
		if !strings.EqualFold(f, lookupCDMVar(vars, ".currency_code", ".currency")) {
			return false
		}
	}
	if f := strings.TrimSpace(instrument); f != "" {
		if !strings.EqualFold(f, lookupCDMVar(vars, ".instrument_type", ".instrument")) {
			return false
		}
	}
	if f := strings.TrimSpace(rating); f != "" {
		if !strings.EqualFold(f, lookupCDMVar(vars, ".credit_rating", ".rating")) {
			return false
		}
	}
	if f := strings.TrimSpace(tenor); f != "" {
		if !strings.EqualFold(f, tenorBandFromVars(vars)) {
			return false
		}
	}
	return true
}
