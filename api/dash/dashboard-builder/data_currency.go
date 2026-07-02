package dashboardbuilder

import (
	"strings"

	"CimplrCorpSaas/api/dash/ticker"
)

type currencyNormConfig struct {
	currencyField     string
	fieldCurrencyMap  map[string]string
	currencyPairField string
	amountFields      map[string]struct{}
}

// sourceCurrencyConfig maps dashboard data sources to currency normalization rules.
// Monetary fields are converted to INR before the API response is sent.
var sourceCurrencyConfig = map[string]currencyNormConfig{
	"cashPayable": {
		currencyField: "currency_code",
		amountFields:  map[string]struct{}{"amount": {}},
	},
	"cashReceivable": {
		currencyField: "currency_code",
		amountFields:  map[string]struct{}{"invoice_amount": {}},
	},
	"cashPayableReceivable": {
		currencyField: "currency_code",
		amountFields:  map[string]struct{}{"amount": {}},
	},
	"cashFundPlanSummary": {
		currencyField: "currency",
		amountFields:  map[string]struct{}{"total_amount": {}},
	},
	"cashFundPlanDetails": {
		currencyField: "currency",
		amountFields: map[string]struct{}{
			"amount":           {},
			"allocated_amount": {},
		},
	},
	"cashProjectionDetail": {
		currencyField: "currency_code",
		amountFields:  map[string]struct{}{"expected_amount": {}},
	},
	"cashFundAvailability": {
		currencyField: "currency_code",
		amountFields:  map[string]struct{}{"total_amount": {}},
	},
	"cashBankLimits": {
		currencyField: "currency_code",
		amountFields: map[string]struct{}{
			"sanctioned_amount":   {},
			"initial_utilization": {},
		},
	},
	"cashUtilizations": {
		currencyField: "currency_code",
		fieldCurrencyMap: map[string]string{
			"limit_sanctioned_amount":   "limit_currency_code",
			"limit_initial_utilization":   "limit_currency_code",
			"limit_available":             "limit_currency_code",
		},
		amountFields: map[string]struct{}{
			"utilized_amount":           {},
			"limit_sanctioned_amount":   {},
			"limit_initial_utilization": {},
			"limit_available":           {},
		},
	},
	"fxExposureHeadersLineItems": {
		currencyField: "currency",
		amountFields: map[string]struct{}{
			"total_open_amount":     {},
			"total_original_amount": {},
			"line_item_amount":      {},
		},
	},
	"fxForwardMTM": {
		currencyPairField: "currency_pair",
		amountFields: map[string]struct{}{
			"notional_amount": {},
			"mtm_value":       {},
		},
	},
	"fxForwardBookingList": {
		currencyPairField: "currency_pair",
		amountFields:      map[string]struct{}{"booking_amount": {}},
	},
	"fxEntityRelevantForwardBookings": {
		currencyPairField: "currency_pair",
		amountFields:      map[string]struct{}{"booking_amount": {}},
	},
}

func normalizeRowsToINR(source string, rows []map[string]any) []map[string]any {
	if len(rows) == 0 {
		return rows
	}

	cfg, ok := sourceCurrencyConfig[source]
	if !ok {
		detected := autoDetectCurrencyConfig(rows)
		if detected == nil {
			return rows
		}
		cfg = *detected
	}

	for _, row := range rows {
		normalizeRowToINR(row, cfg)
	}
	return rows
}

func normalizeRowToINR(row map[string]any, cfg currencyNormConfig) {
	defaultCurrency := rowCurrency(row, cfg.currencyField)
	if cfg.currencyPairField != "" && defaultCurrency == "" {
		defaultCurrency = baseCurrencyFromPair(rowString(row, cfg.currencyPairField))
	}

	for field := range cfg.amountFields {
		currencyField := cfg.currencyField
		if override, ok := cfg.fieldCurrencyMap[field]; ok && override != "" {
			currencyField = override
		}

		currency := rowCurrency(row, currencyField)
		if currency == "" {
			currency = defaultCurrency
		}
		if currency == "" {
			currency = "INR"
		}

		amount, ok := toFloat64(row[field])
		if !ok {
			continue
		}
		row[field] = ticker.ConvertAmountToINR(amount, currency)
	}

	if cfg.currencyField != "" {
		row[cfg.currencyField] = "INR"
	}
	if cfg.currencyPairField != "" {
		row[cfg.currencyPairField] = "INR/INR"
	}
	for _, currencyField := range cfg.fieldCurrencyMap {
		if currencyField != "" {
			row[currencyField] = "INR"
		}
	}
}

// ── Auto-detection of currency columns ──────────────────────────────────────

var knownCurrencyFields = map[string]bool{
	"currency_code":      true,
	"currency":           true,
	"base_currency_code": true,
}

var knownCurrencyPairFields = map[string]bool{
	"currency_pair": true,
}

var nonMonetarySuffixes = []string{
	"rate", "_pct", "percent", "_days", "_months", "_years",
	"_count", "_nav", "score",
}

var nonMonetaryPrefixes = []string{
	"sequence", "precision", "run_day", "fds_", "quantity",
}

var nonMonetaryExact = map[string]bool{
	"divisor":                true,
	"total_records":         true,
	"total_cashflow_events": true,
	"total_groups":          true,
	"horizon":               true,
	"item_count":            true,
}

func isLikelyMonetaryField(key string) bool {
	lower := strings.ToLower(key)
	if nonMonetaryExact[lower] {
		return false
	}
	if strings.Contains(lower, "units") {
		return false
	}
	for _, suffix := range nonMonetarySuffixes {
		if strings.HasSuffix(lower, suffix) {
			return false
		}
	}
	for _, prefix := range nonMonetaryPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return false
		}
	}
	return true
}

func autoDetectCurrencyConfig(rows []map[string]any) *currencyNormConfig {
	if len(rows) == 0 {
		return nil
	}
	sample := rows[0]

	var currencyField, currencyPairField string
	for key := range sample {
		if knownCurrencyFields[key] {
			currencyField = key
			break
		}
	}
	if currencyField == "" {
		for key := range sample {
			if knownCurrencyPairFields[key] {
				currencyPairField = key
				break
			}
		}
	}
	if currencyField == "" && currencyPairField == "" {
		return nil
	}

	amountFields := make(map[string]struct{})
	for key, val := range sample {
		if _, isNum := toFloat64(val); isNum && isLikelyMonetaryField(key) {
			amountFields[key] = struct{}{}
		}
	}
	if len(amountFields) == 0 {
		return nil
	}

	return &currencyNormConfig{
		currencyField:     currencyField,
		currencyPairField: currencyPairField,
		amountFields:      amountFields,
	}
}

func rowCurrency(row map[string]any, field string) string {
	if field == "" {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(rowString(row, field)))
}

func rowString(row map[string]any, field string) string {
	if field == "" || row == nil {
		return ""
	}
	v, ok := row[field]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return dashboardStr(v)
	}
}

func baseCurrencyFromPair(pair string) string {
	pair = strings.TrimSpace(pair)
	if pair == "" {
		return ""
	}
	if i := strings.Index(pair, "/"); i > 0 {
		return strings.ToUpper(strings.TrimSpace(pair[:i]))
	}
	return strings.ToUpper(pair)
}

func toFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case uint:
		return float64(t), true
	case uint32:
		return float64(t), true
	case uint64:
		return float64(t), true
	default:
		return 0, false
	}
}
