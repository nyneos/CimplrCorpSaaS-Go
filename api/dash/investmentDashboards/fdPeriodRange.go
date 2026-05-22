package investmentdashboards

import (
	"regexp"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
)

var fdConcatenatedPeriodRE = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})\s*(?:→|->|–|—| to )\s*(\d{4}-\d{2}-\d{2})$`)

// fdPeriodBounds is the normalized inclusive calendar range for dashboard queries.
type fdPeriodBounds struct {
	Period       string
	Start        time.Time
	EndInclusive time.Time
	StartStr     string
	EndStr       string
}

func parseFDDate(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(constants.DateFormat, s)
	if err != nil {
		return time.Time{}, false
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), true
}

func calendarToday(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
}

// periodStartDate returns the start of the requested period label.
func periodStartDate(period string, now time.Time) time.Time {
	p := strings.TrimSpace(period)
	switch strings.ToUpper(p) {
	case "TODAY":
		return calendarToday(now)
	case "THIS WEEK":
		wd := int(now.Weekday())
		if wd == 0 {
			wd = 7
		}
		monday := now.AddDate(0, 0, -(wd - 1))
		return time.Date(monday.Year(), monday.Month(), monday.Day(), 0, 0, 0, 0, now.Location())
	case "THIS MONTH", "MTD":
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	case "QUARTER", "QTD":
		m := int(now.Month())
		var qStart int
		switch {
		case m >= 4 && m <= 6:
			qStart = 4
		case m >= 7 && m <= 9:
			qStart = 7
		case m >= 10 && m <= 12:
			qStart = 10
		default:
			qStart = 1
		}
		return time.Date(now.Year(), time.Month(qStart), 1, 0, 0, 0, 0, now.Location())
	case "YTD":
		fy := now.Year()
		if now.Month() < time.April {
			fy--
		}
		return time.Date(fy, time.April, 1, 0, 0, 0, 0, now.Location())
	default:
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	}
}

func normalizeFDPeriodLabel(period, startDate, endDate string) (label, startOut, endOut string) {
	startOut = strings.TrimSpace(startDate)
	endOut = strings.TrimSpace(endDate)
	p := strings.TrimSpace(period)

	if strings.EqualFold(p, "CUSTOM") || strings.EqualFold(p, "Custom Date Range") {
		return "CUSTOM", startOut, endOut
	}
	if m := fdConcatenatedPeriodRE.FindStringSubmatch(p); len(m) == 3 {
		return "CUSTOM", m[1], m[2]
	}
	if startOut != "" || endOut != "" {
		return "CUSTOM", startOut, endOut
	}
	switch strings.ToLower(p) {
	case "quarter":
		return "QTD", "", ""
	default:
		return strings.ToUpper(p), "", ""
	}
}

func orderFDDateRange(start, end time.Time) (time.Time, time.Time) {
	if start.After(end) {
		return end, start
	}
	return start, end
}

// resolveFDPeriodBounds normalizes period + optional custom dates.
// End is inclusive (queries that need exclusive upper bound should add +1 day).
func resolveFDPeriodBounds(period, startDate, endDate string, now time.Time) fdPeriodBounds {
	label, startS, endS := normalizeFDPeriodLabel(period, startDate, endDate)
	today := calendarToday(now)

	if label == "CUSTOM" {
		startT, hasStart := parseFDDate(startS)
		endT, hasEnd := parseFDDate(endS)
		switch {
		case hasStart && hasEnd:
			startT, endT = orderFDDateRange(startT, endT)
		case hasStart && !hasEnd:
			endT = today
		case !hasStart && hasEnd:
			startT, endT = endT, endT
		default:
			startT = periodStartDate("MTD", now)
			endT = today
		}
		return fdPeriodBounds{
			Period:       "CUSTOM",
			Start:        startT,
			EndInclusive: endT,
			StartStr:     startT.Format(constants.DateFormat),
			EndStr:       endT.Format(constants.DateFormat),
		}
	}

	if label == "" {
		label = "MTD"
	}
	startT := periodStartDate(label, now)
	endT := today
	return fdPeriodBounds{
		Period:       label,
		Start:        startT,
		EndInclusive: endT,
		StartStr:     startT.Format(constants.DateFormat),
		EndStr:       endT.Format(constants.DateFormat),
	}
}

// auditPeriodExclusiveEnd returns the exclusive upper bound for audit log queries (start <= t < end).
func auditPeriodExclusiveEnd(bounds fdPeriodBounds) string {
	return bounds.EndInclusive.AddDate(0, 0, 1).Format(constants.DateFormat)
}
