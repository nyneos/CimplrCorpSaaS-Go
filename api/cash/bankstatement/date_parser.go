package bankstatement

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
)

// normalizeDateMonthAbbrev converts uppercase or lowercase 3-letter month abbreviations
// to Go's expected title-case form (e.g. "FEB" → "Feb", "feb" → "Feb").
// This lets time.Parse match formats like "01.FEB 2026" against the layout "02.Jan 2006".
func normalizeDateMonthAbbrev(s string) string {
	months := []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
	upper := strings.ToUpper(s)
	for _, m := range months {
		mu := strings.ToUpper(m)
		if idx := strings.Index(upper, mu); idx != -1 {
			// Replace the exact slice in the original string with title-case month
			s = s[:idx] + m + s[idx+3:]
			upper = strings.ToUpper(s) // recompute in case of multiple occurrences
		}
	}
	return s
}

// parseDate tries multiple date formats for CSV
func parseDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	// Prefer dd/mm/yyyy for bank statements before falling back to the broader parser set.
	if t, err := time.Parse("02/01/2006", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2/1/2006", s); err == nil {
		return t, nil
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty date string")
	}
	// Normalize uppercase/lowercase month abbreviations so Go's time.Parse (which is case-sensitive)
	// can match formats like "01.FEB 2026", "01-FEB-2026", "01/FEB/2026" → "01.Feb 2026" etc.
	s = normalizeDateMonthAbbrev(s)
	// Critical: dd/mm/yyyy formats MUST come before mm/dd/yyyy to prevent misparsing Indian bank statements
	layouts := []string{
		// dd/mm/yyyy variants (Indian/European format) - MUST BE FIRST
		"02/01/2006", "02/01/06", "2/1/2006", "2/1/06",
		"02/01/2006 03:04:05 PM", "02/01/06 03:04:05 PM", "2/1/2006 03:04:05 PM", "2/1/06 03:04:05 PM",
		"02/01/2006 3:04:05 PM", "02/01/06 3:04:05 PM", "2/1/2006 3:04:05 PM", "2/1/06 3:04:05 PM",
		"02/01/06 15:04", "02/01/06 3:04", "02/01/06 15:04:05", "02/01/06 3:04:05",
		"2/1/06 15:04", "2/1/06 3:04", "2/1/06 15:04:05", "2/1/06 3:04:05",
		// mm/dd/yyyy variants (American format) - AFTER dd/mm/yyyy
		"01/02/2006", "01/02/06", "1/2/2006", "1/2/06",
		"01/02/2006 03:04:05 PM", "01/02/2006 03:04 PM", "01/02/06 03:04:05 PM", "01/02/06 03:04 PM",
		"1/2/2006 03:04:05 PM", "1/2/2006 03:04 PM", "1/2/06 03:04:05 PM", "1/2/06 03:04 PM",
		"01/02/06 15:04", "01/02/06 3:04", "01/02/06 15:04:05", "01/02/06 3:04:05",
		"1/2/06 15:04", "1/2/06 3:04", "1/2/06 15:04:05", "1/2/06 3:04:05",
		// Named month formats
		constants.DateFormatSlash, constants.DateFormatDash, // for 29/Aug/2025 and 29-Aug-2025
		"2-Jan-2006", "1/Feb/2006",
		// dd-mm-yyyy with dash (Indian/European, e.g. BOB: "16-02-2026", "02-02-2026 19:40:13") - BEFORE mm-dd-yyyy!
		"02-01-2006", "2-1-2006", "02-01-06", "2-1-06",
		"02-01-2006 15:04:05", "2-1-2006 15:04:05", "02-01-2006 15:04", "2-1-2006 15:04",
		"02-01-2006 3:04:05", "2-1-2006 3:04:05", "02-01-2006 3:04", "2-1-2006 3:04",
		// ISO and other formats
		constants.DateFormat, "2006/01/02", "2006.01.02", "01.02.2006", "1.2.2006", "01-02-2006", "1-2-2006",
		"01-02-06", "1-2-06", "2006/1/2", "2006-1-2",
		// dd.Mon yyyy variants (Citibank and similar: "01.FEB 2026", "02.Jan 2026")
		"02.Jan 2006", "2.Jan 2006", "02.Jan 06", "2.Jan 06",
		// dd-Mon-yy and dd/Mon/yy variants
		"02-Jan-06", "02-Jan-2006", "02/Jan/06", "02/Jan/2006",
		"02-Jan-06 15:04", "02-Jan-2006 15:04", "02-Jan-06 3:04", "02-Jan-2006 3:04",
		"02-Jan-06 15:04:05", "02-Jan-2006 15:04:05", "02-Jan-06 3:04:05", "02-Jan-2006 3:04:05",
		"02/Jan/06 15:04", "02/Jan/2006 15:04", "02/Jan/06 3:04", "02/Jan/2006 3:04",
		"02/Jan/06 15:04:05", "02/Jan/2006 15:04:05", "02/Jan/06 3:04:05", "02/Jan/2006 3:04:05",
		"02-Jan-2006 03:04:05 PM", "02-Jan-06 03:04:05 PM", "02-Jan-2006 3:04:05 PM", "02-Jan-06 3:04:05 PM",
		"02/Jan/2006 03:04:05 PM", "02/Jan/06 03:04:05 PM", "02/Jan/2006 3:04:05 PM", "02/Jan/06 3:04:05 PM",
		// dd-Mon-yy variants (American style)
		"01-Feb-06", "01-Feb-2006", "01/Feb/06", "01/Feb/2006",
		"01-Feb-06 15:04", "01-Feb-2006 15:04", "01-Feb-06 3:04", "01-Feb-2006 3:04",
		"01-Feb-06 15:04:05", "01-Feb-2006 15:04:05", "01-Feb-06 3:04:05", "01-Feb-2006 3:04:05",
		"01/Feb/06 15:04", "01/Feb/2006 15:04", "01/Feb/06 3:04", "01/Feb/2006 3:04",
		"01/Feb/06 15:04:05", "01/Feb/2006 15:04:05", "01/Feb/06 3:04:05", "01/Feb/2006 3:04:05",
		// ISO-ish layouts
		constants.DateFormat, constants.DateTimeFormat, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02T15:04",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	// Try to parse with 2-digit year fallback (e.g., 13-Dec-25 as 2025)
	if len(s) == 9 && s[2] == '-' && s[6] == '-' {
		t, err := time.Parse("02-Jan-06", s)
		if err == nil {
			y := t.Year()
			if y < 100 {
				t = t.AddDate(2000, 0, 0)
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse date: %s", s)
}

// parseDateDDMMFirst is a convenience wrapper that simply delegates to parseDate,
// which already prefers dd/mm/yyyy layouts.
func parseDateDDMMFirst(s string) (time.Time, error) {
	return parseDate(s)
}

// tryParseDateWithExcelSerial first attempts normal string parsing, then falls back to
// Excel serial date numbers (days since 1899-12-30 with the Excel 1900 leap-year bug).
func tryParseDateWithExcelSerial(s string) time.Time {
	if t, err := parseDateDDMMFirst(s); err == nil && !t.IsZero() {
		return t
	}
	if t, err := parseExcelSerialDate(s); err == nil {
		return t
	}
	return time.Time{}
}

// parseExcelSerialDate converts an Excel serial date (possibly with fractional day time)
// into a time.Time. Excel counts from 1899-12-30 and includes a fake 1900-02-29 day.
func parseExcelSerialDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty excel serial")
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}, err
	}
	days := int(f)
	frac := f - float64(days)
	if days > 59 {
		days--
	}
	base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
	d := base.AddDate(0, 0, days)
	d = d.Add(time.Duration(frac * float64(24*time.Hour)))
	return d, nil
}
