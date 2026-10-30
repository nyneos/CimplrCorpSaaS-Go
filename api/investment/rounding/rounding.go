// Package rounding provides unified monetary rounding for FD cashflow,
// simulator, and accrual engines (interest_rounding_decimals + rounding_method).
package rounding

import (
	"math"
	"strings"

	"github.com/shopspring/decimal"
)

// DefaultDecimals is used when bank config does not specify interest_rounding_decimals.
const DefaultDecimals = 2

// NormalizeDecimals returns a non-negative decimal count (default DefaultDecimals).
func NormalizeDecimals(decimals int) int {
	if decimals < 0 {
		return DefaultDecimals
	}
	return decimals
}

// NormalizeMethod maps bank/accrual method codes to a canonical rounding method.
func NormalizeMethod(method string) string {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case "TRUNCATE", "TRUNC":
		return "TRUNCATE"
	case "ROUND_UP", "CEIL", "CEILING":
		return "ROUND_UP"
	case "ROUND_DOWN", "FLOOR":
		return "ROUND_DOWN"
	case "BANKERS", "HALF_EVEN", "ROUND_HALF_EVEN":
		return "BANKERS"
	default:
		return "ROUND"
	}
}

// NormalizeFrequency maps rounding_frequency values to EACH_PERIOD or AT_MATURITY.
func NormalizeFrequency(frequency string) string {
	f := strings.ToUpper(strings.TrimSpace(frequency))
	switch f {
	case "AT_MATURITY", "FINAL_ONLY", "MATURITY":
		return "AT_MATURITY"
	default:
		return "EACH_PERIOD"
	}
}

// maxDecimals bounds the precision so a bad configuration cannot ask for absurd scales.
const maxDecimals = 12

// noiseDigits is how many places past the target scale are kept before rounding.
const noiseDigits = 6

// RoundByMethod rounds value to the given decimal places using method. The value is
// converted to its shortest decimal form first, so 1.15 rounds as 1.15 and not as
// the binary 1.1499999...
func RoundByMethod(value float64, decimals int, method string) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return value
	}
	decimals = NormalizeDecimals(decimals)
	if decimals > maxDecimals {
		decimals = maxDecimals
	}
	places := int32(decimals)
	// Drop binary noise well below the target scale (114.99999999999999 is 115) so the
	// directed modes do not move a whole unit at the last place.
	d := decimal.NewFromFloat(value).Round(places + noiseDigits)
	var out decimal.Decimal
	switch NormalizeMethod(method) {
	case "TRUNCATE", "ROUND_DOWN":
		out = d.RoundDown(places)
	case "ROUND_UP":
		out = d.RoundUp(places)
	case "BANKERS":
		out = d.RoundBank(places)
	default:
		out = d.Round(places)
	}
	return out.InexactFloat64()
}

// Apply rounds according to method and frequency.
// When frequency is AT_MATURITY and isFinal is false, the value is returned unrounded
// so intermediate compound chains keep full precision until the final event.
func Apply(value float64, decimals int, method, frequency string, isFinal bool) float64 {
	if NormalizeFrequency(frequency) == "AT_MATURITY" && !isFinal {
		return value
	}
	return RoundByMethod(value, decimals, method)
}

// Config holds bank rounding settings shared across engines.
type Config struct {
	Decimals  int
	Method    string
	Frequency string
}

// FromBankConfig builds Config from typical bank master fields.
func FromBankConfig(decimals int, method, frequency string) Config {
	if decimals < 0 {
		decimals = DefaultDecimals
	}
	// decimals == 0 → whole rupees (workbook parity); positive values pass through.
	m := method
	if m == "" {
		m = "ROUND"
	}
	f := frequency
	if f == "" {
		f = "EACH_PERIOD"
	}
	return Config{
		Decimals:  decimals,
		Method:    m,
		Frequency: f,
	}
}

// RoundInterest is shorthand for per-period interest row rounding (not final).
func (c Config) RoundInterest(raw float64) float64 {
	return Apply(raw, c.Decimals, c.Method, c.Frequency, false)
}

// RoundFinal is shorthand for payout/maturity/total rounding.
func (c Config) RoundFinal(raw float64) float64 {
	return Apply(raw, c.Decimals, c.Method, c.Frequency, true)
}

// RoundPrincipal rounds closing principal / balance amounts (always rounded for storage).
func (c Config) RoundPrincipal(raw float64) float64 {
	return RoundByMethod(raw, c.Decimals, c.Method)
}
