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
	cfg, ok := sourceCurrencyConfig[source]
	if !ok || len(rows) == 0 {
		return rows
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
