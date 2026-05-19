// Package rounding provides unified monetary rounding for FD cashflow,
// simulator, and accrual engines (interest_rounding_decimals + rounding_method).
package rounding

import (
	"math"
	"strings"
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

// RoundByMethod rounds value to the given decimal places using method.
func RoundByMethod(value float64, decimals int, method string) float64 {
	decimals = NormalizeDecimals(decimals)
	if decimals == 0 {
		// Whole currency units (legacy workbook / rupee rounding).
		switch NormalizeMethod(method) {
		case "TRUNCATE":
			return math.Trunc(value)
		case "ROUND_UP":
			sign := math.Copysign(1, value)
			return sign * math.Ceil(math.Abs(value))
		case "ROUND_DOWN":
			sign := math.Copysign(1, value)
			return sign * math.Floor(math.Abs(value))
		default:
			return math.Round(value)
		}
	}
	pow := math.Pow(10, float64(decimals))
	switch NormalizeMethod(method) {
	case "TRUNCATE":
		return math.Trunc(value*pow) / pow
	case "ROUND_UP":
		sign := math.Copysign(1, value)
		return sign * math.Ceil(math.Abs(value)*pow) / pow
	case "ROUND_DOWN":
		sign := math.Copysign(1, value)
		return sign * math.Floor(math.Abs(value)*pow) / pow
	default:
		return math.Round(value*pow) / pow
	}
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
