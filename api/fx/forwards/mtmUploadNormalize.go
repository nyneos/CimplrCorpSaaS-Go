package forwards

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
)

// ---------------------------------------------------------------------------
// Header normalization
// ---------------------------------------------------------------------------

// mtmDisplayHeaderAliases maps the human-readable column titles shipped in the
// downloadable MTM template (and anything a treasury user is likely to type)
// onto the canonical backend column names the reconciliation loop reads.
var mtmDisplayHeaderAliases = map[string]string{
	"internal reference id": "internal_reference_id",
	"internal ref id":       "internal_reference_id",
	"reference id":          "internal_reference_id",
	"deal date":             "deal_date",
	"maturity date":         "maturity_date",
	"currency pair":         "currency_pair",
	"buy/sell":              "buy_sell",
	"buy sell":              "buy_sell",
	"notional amount":       "notional_amount",
	"contract rate":         "contract_rate",
	"mtm rate":              "mtm_rate",
	"mtm value":             "mtm_value",
	"days to maturity":      "days_to_maturity",
	"entity":                "entity",
	"status":                "status",
}

// normalizeMTMHeader lowercases, trims, strips the UTF-8 BOM and resolves a
// display-title alias so that "Internal Reference ID", "internal_reference_id"
// and " Internal Ref ID " all land on the same key.
func normalizeMTMHeader(header string) string {
	h := strings.TrimSpace(strings.TrimPrefix(header, "\uFEFF"))
	h = strings.Trim(h, "\"")
	h = strings.ToLower(strings.TrimSpace(h))
	if mapped, ok := mtmDisplayHeaderAliases[h]; ok {
		return mapped
	}
	return strings.ReplaceAll(h, " ", "_")
}

// ---------------------------------------------------------------------------
// Value normalization
// ---------------------------------------------------------------------------

// normalizeCurrencyPair strips separators and case so that "USD/INR",
// "usd-inr" and "USDINR" all compare equal. forward_bookings stores the pair
// without a separator while the MTM template asks for "XXX/YYY".
func normalizeCurrencyPair(v interface{}) string {
	s := strings.ToUpper(strings.TrimSpace(str(v)))
	var b strings.Builder
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// normalizeSide upper-cases and trims a buy/sell or order_type value so the
// comparison is not defeated by "Buy" vs "BUY" vs " buy ".
func normalizeSide(v interface{}) string {
	return strings.ToUpper(strings.TrimSpace(str(v)))
}

// mtmDateLayouts are tried in order. Day-first layouts precede month-first
// ones because the uploads are Indian treasury files (dd-mm-yyyy / dd/mm/yyyy).
var mtmDateLayouts = []string{
	"2006-01-02",
	"2006/01/02",
	"02-01-2006",
	"02/01/2006",
	"2-1-2006",
	"2/1/2006",
	"02.01.2006",
	"02-Jan-2006",
	"2-Jan-2006",
	"02-Jan-06",
	"02 Jan 2006",
	"Jan 2, 2006",
	"January 2, 2006",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05-07",
	"02-01-2006 15:04:05",
	"02/01/2006 15:04:05",
}

// excelEpoch is the origin Excel serial date numbers count from (Excel's
// 1900 leap-year bug means day 1 is 1900-01-01 relative to 1899-12-30).
var excelEpoch = time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)

// parseMTMDate accepts any of the layouts above, a bare Excel serial number
// (what excelize hands back for unformatted date cells), or a time.Time, and
// returns the parsed day in UTC.
func parseMTMDate(v interface{}) (time.Time, bool) {
	switch t := v.(type) {
	case nil:
		return time.Time{}, false
	case time.Time:
		if t.IsZero() {
			return time.Time{}, false
		}
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), true
	case float64:
		return excelSerialToDate(t)
	case int:
		return excelSerialToDate(float64(t))
	}

	s := strings.TrimSpace(str(v))
	if s == "" {
		return time.Time{}, false
	}
	s = strings.Trim(s, "\"")

	for _, layout := range mtmDateLayouts {
		if parsed, err := time.Parse(layout, s); err == nil {
			return time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC), true
		}
	}

	// Excel serial arriving as a numeric string, e.g. "45810".
	if serial, err := strconv.ParseFloat(s, 64); err == nil {
		return excelSerialToDate(serial)
	}

	return time.Time{}, false
}

// excelSerialToDate converts an Excel/LibreOffice date serial into a date.
// The guarded range covers 1970-01-01 .. 2149-06-04, which rules out ordinary
// numeric cells (amounts, rates) being misread as dates.
func excelSerialToDate(serial float64) (time.Time, bool) {
	if serial < 25569 || serial > 90000 {
		return time.Time{}, false
	}
	d := excelEpoch.AddDate(0, 0, int(math.Trunc(serial)))
	return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC), true
}

// normalizeMTMDate renders any accepted input as the canonical YYYY-MM-DD the
// forward_mtm date columns expect. It returns "" when the value is unparseable.
func normalizeMTMDate(v interface{}) string {
	parsed, ok := parseMTMDate(v)
	if !ok {
		return ""
	}
	return parsed.Format(constants.DateFormat)
}

// normalizeMTMNumber parses a numeric cell, tolerating thousands separators,
// currency symbols, surrounding whitespace and accounting-style negatives.
func normalizeMTMNumber(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case nil:
		return 0, false
	case float64:
		return t, true
	case int:
		return float64(t), true
	}

	s := strings.TrimSpace(str(v))
	if s == "" {
		return 0, false
	}
	negative := false
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		negative = true
		s = strings.TrimSuffix(strings.TrimPrefix(s, "("), ")")
	}
	replacer := strings.NewReplacer(",", "", " ", "", " ", "", "₹", "", "$", "", "€", "", "£", "")
	s = replacer.Replace(s)
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	if negative {
		f = -f
	}
	return f, true
}

// Comparison tolerances. Amounts are compared to the paisa/cent, rates to the
// eighth decimal — enough to absorb float round-tripping through NUMERIC and
// through the CSV, without letting a genuinely different rate slip past.
const (
	mtmAmountTolerance = 0.01
	mtmRateTolerance   = 1e-8
)

func mtmNumbersEqual(a, b, tolerance float64) bool {
	return math.Abs(a-b) <= tolerance
}

// formatMTMNumber renders a float for an error message without exponent
// notation or trailing zero noise.
func formatMTMNumber(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// mtmFieldMismatch is one failed reconciliation check, carrying both sides so
// the uploader can see exactly what to correct.
type mtmFieldMismatch struct {
	Field    string
	Expected string
	Provided string
}

func (m mtmFieldMismatch) String() string {
	return fmt.Sprintf("%s (booking has %q, file has %q)", m.Field, m.Expected, m.Provided)
}

func formatMTMMismatches(mismatches []mtmFieldMismatch) string {
	parts := make([]string, 0, len(mismatches))
	for _, m := range mismatches {
		parts = append(parts, m.String())
	}
	return strings.Join(parts, "; ")
}
