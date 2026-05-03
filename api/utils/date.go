package utils

import (
	"CimplrCorpSaas/api/constants"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func NormalizeDateString(dateStr string) string {
	dateStr = strings.TrimSpace(dateStr)
	if dateStr == "" {
		return ""
	}

	// Normalize spaces
	dateStr = regexp.MustCompile(`\s+`).ReplaceAllString(dateStr, " ")

	// Try common layouts first (preserve original behavior)
	layouts := []string{
		// ISO formats
		constants.DateFormat,
		"2006/01/02",
		"2006.01.02",
		time.RFC3339,
		constants.DateTimeFormat,
		constants.DateFormatISO,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",

		// DD-MM-YYYY formats
		constants.DateFormatAlt,
		"02/01/2006",
		"02.01.2006",
		"02-01-2006 15:04:05",
		"02/01/2006 15:04:05",
		"02.01.2006 15:04:05",

		// MM-DD-YYYY formats
		"01-02-2006",
		"01/02/2006",
		"01.02.2006",
		"01-02-2006 15:04:05",
		"01/02/2006 15:04:05",
		"01.02.2006 15:04:05",

		// Text month formats
		constants.DateFormatDash,
		"02-Jan-06",
		"2-Jan-2006",
		"2-Jan-06",
		"02-Jan-2006 15:04:05",
		"02 Jan 2006",
		"2 Jan 2006",
		"02 Jan 06",
		"2 Jan 06",
		"Jan 02, 2006",
		"Jan 2, 2006",
		"January 02, 2006",
		"January 2, 2006",

		// Single digit day/month formats
		"2-1-2006",
		"2/1/2006",
		"2.1.2006",
		"1-2-2006",
		"1/2/2006",
		"1.2.2006",

		// Short year formats
		"02-01-06",
		"02/01/06",
		"02.01.06",
		"01-02-06",
		"01/02/06",
		"01.02.06",
		"2-1-06",
		"2/1/06",
		"1-2-06",
		"1/2-06",

		// compact
		"20060102",
	}

	for _, l := range layouts {
		if t, err := time.Parse(l, dateStr); err == nil {
			if t.Year() < 1900 || t.Year() > 9999 {
				continue
			}
			return t.Format(constants.DateFormat)
		}
	}

	// If the string is purely numeric try several heuristics:
	// - YYYYMMDD (8 digits)
	// - Unix timestamp (seconds / ms / us / ns)
	// - Excel serial (days since 1899-12-30)
	digits := true
	for _, r := range dateStr {
		if r < '0' || r > '9' {
			digits = false
			break
		}
	}

	if digits {
		// YYYYMMDD
		if len(dateStr) == 8 {
			if y, err := strconv.Atoi(dateStr[0:4]); err == nil {
				if m, err := strconv.Atoi(dateStr[4:6]); err == nil {
					if d, err := strconv.Atoi(dateStr[6:8]); err == nil {
						if y >= 1900 && y <= 9999 {
							return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC).Format(constants.DateFormat)
						}
					}
				}
			}
		}

		if v, err := strconv.ParseInt(dateStr, 10, 64); err == nil {
			var t time.Time
			switch {
			case v >= 1e17:
				// nanoseconds since epoch
				t = time.Unix(0, v)
			case v >= 1e14:
				// microseconds -> ns
				t = time.Unix(0, v*1000)
			case v >= 1e11:
				// milliseconds -> ns
				t = time.Unix(0, v*1000000)
			case v >= 1e9:
				// seconds
				t = time.Unix(v, 0)
			default:
				// Treat as Excel serial date (days since 1899-12-30)
				base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
				t = base.AddDate(0, 0, int(v))
			}
			if t.Year() >= 1900 && t.Year() <= 9999 {
				return t.Format(constants.DateFormat)
			}
		}
	}

	return ""
}
