package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ICSCalendar struct {
	ProdID   string
	Name     string
	Timezone string // X-WR-TIMEZONE (no VTIMEZONE component here)
	Events   []ICSEvent
	Method   string // optional, usually omitted for feeds
	Color    string // optional (Google supports X-APPLE-CALENDAR-COLOR-ish but not official)
	CalScale string // default: GREGORIAN
}

type ICSEvent struct {
	// All-day holiday; we’ll write DTSTART;VALUE=DATE
	Date        time.Time // REQUIRED (local date)
	Summary     string    // REQUIRED
	Description string    // optional
	RRule       string    // optional (pass-through only if FREQ=YEARLY…)
	UID         string    // optional (we can derive from date+summary+calendar)
}

// Escape per RFC5545
func icsEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `;`, `\;`)
	s = strings.ReplaceAll(s, `,`, `\,`)
	s = strings.ReplaceAll(s, "\r\n", `\n`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

// Fold lines to 75 octets (simple UTF-8 rune-safe approximation by 73 chars + CRLF + space)
func icsFold(line string) string {
	const limit = 75
	if len(line) <= limit {
		return line
	}
	var b strings.Builder
	r := []rune(line)
	cur := 0
	for cur < len(r) {
		remain := len(r) - cur
		take := limit
		if remain < take {
			take = remain
		}
		chunk := string(r[cur : cur+take])
		if cur == 0 {
			b.WriteString(chunk)
		} else {
			b.WriteString("\r\n ")
			b.WriteString(chunk)
		}
		cur += take
	}
	return b.String()
}

func formatDateValue(t time.Time) string {
	// VALUE=DATE must be YYYYMMDD; ensure date in local calendar’s timezone is handled upstream
	return t.Format("20060102")
}

// conservative RRULE allow-list: only YEARLY (FR scope)
var rruleAllowed = regexp.MustCompile(`(?i)^FREQ=YEARLY(?:;.*)?$`)

// Build iCalendar text
func buildICS(cal ICSCalendar) string {
	if cal.CalScale == "" {
		cal.CalScale = "GREGORIAN"
	}
	var lines []string
	lines = append(lines, "BEGIN:VCALENDAR")
	lines = append(lines, "VERSION:2.0")
	lines = append(lines, icsFold("PRODID:"+icsEscape(defaultIfEmpty(cal.ProdID, "-//Cimplr//Holiday Master//EN"))))
	lines = append(lines, "CALSCALE:"+cal.CalScale)
	if cal.Method != "" {
		lines = append(lines, "METHOD:"+strings.ToUpper(cal.Method))
	}
	if cal.Name != "" {
		lines = append(lines, icsFold("X-WR-CALNAME:"+icsEscape(cal.Name)))
	}
	if cal.Timezone != "" {
		lines = append(lines, icsFold("X-WR-TIMEZONE:"+icsEscape(cal.Timezone)))
	}

	for _, ev := range cal.Events {
		lines = append(lines, "BEGIN:VEVENT")
		// all-day:
		lines = append(lines, "DTSTART;VALUE=DATE:"+formatDateValue(ev.Date))
		if ev.Summary != "" {
			lines = append(lines, icsFold("SUMMARY:"+icsEscape(ev.Summary)))
		}
		if ev.Description != "" {
			lines = append(lines, icsFold("DESCRIPTION:"+icsEscape(ev.Description)))
		}
		if ev.UID != "" {
			lines = append(lines, icsFold("UID:"+icsEscape(ev.UID)))
		}
		rr := strings.TrimSpace(ev.RRule)
		if rr != "" && rruleAllowed.MatchString(rr) {
			// pass-through only if yearly rrule
			lines = append(lines, icsFold("RRULE:"+rr))
		}
		lines = append(lines, "END:VEVENT")
	}
	lines = append(lines, "END:VCALENDAR")
	return strings.Join(lines, "\r\n") + "\r\n"
}

type calRow struct {
	CalendarID string
	Code       string
	Name       string
	Timezone   string
	WeekendPat string
	EffFrom    *time.Time
	EffTo      *time.Time
	Status     string
	Processing string // latest audit status
}

type holRow struct {
	ID     string
	Date   time.Time
	Name   string
	Type   string
	Notes  *string
	RRule  *string
	Status string
}

// fetch calendar with APPROVED+ACTIVE guard
func fetchApprovedActiveCalendar(ctx context.Context, db *pgxpool.Pool, calendarID string) (*calRow, error) {
	q := `
WITH latest AS (
	SELECT DISTINCT ON (calendar_id)
	       calendar_id, processing_status
	FROM investment.auditactioncalendar
	WHERE calendar_id = $1
	ORDER BY calendar_id, requested_at DESC
)
SELECT mc.calendar_id, mc.calendar_code, mc.calendar_name, mc.timezone, mc.weekend_pattern,
       mc.eff_from, mc.eff_to, mc.status,
       COALESCE(lat.processing_status,'') AS processing_status
FROM investment.mastercalendar mc
LEFT JOIN latest lat ON lat.calendar_id = mc.calendar_id
WHERE mc.calendar_id=$1
  AND COALESCE(mc.is_deleted,false)=false
LIMIT 1;`
	row := calRow{}
	err := db.QueryRow(ctx, q, calendarID).Scan(
		&row.CalendarID, &row.Code, &row.Name, &row.Timezone, &row.WeekendPat,
		&row.EffFrom, &row.EffTo, &row.Status, &row.Processing,
	)
	if err != nil {
		return nil, err
	}
	// Guards: latest audit APPROVED + status ACTIVE
	if !strings.EqualFold(row.Status, "Active") || !strings.EqualFold(row.Processing, "APPROVED") {
		return nil, fmt.Errorf("calendar not active/approved")
	}
	return &row, nil
}

// fetch holidays under the same guards (status active, not deleted, date within eff_from/eff_to if set)
func fetchApprovedActiveHolidays(ctx context.Context, db *pgxpool.Pool, calendarID string, effFrom, effTo *time.Time) ([]holRow, error) {
	args := []interface{}{calendarID}
	where := `
WHERE h.calendar_id=$1
  AND COALESCE(h.is_deleted,false)=false
  AND UPPER(h.status)='ACTIVE'`
	if effFrom != nil {
		where += " AND h.holiday_date >= $2"
		args = append(args, effFrom.Format("2006-01-02"))
	}
	if effTo != nil {
		where += fmt.Sprintf(" AND h.holiday_date <= $%d", len(args)+1)
		args = append(args, effTo.Format("2006-01-02"))
	}

	q := `
SELECT h.holiday_id, h.holiday_date, h.holiday_name, h.holiday_type,
       h.notes, h.recurrence_rule, h.status
FROM investment.masterholiday h
` + where + `
ORDER BY h.holiday_date, h.holiday_name;`

	rows, err := db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []holRow{}
	for rows.Next() {
		var r holRow
		if err := rows.Scan(&r.ID, &r.Date, &r.Name, &r.Type, &r.Notes, &r.RRule, &r.Status); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}



func ExportCalendarICS(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			CalendarID string `json:"calendar_id"`
			UserID     string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.CalendarID) == "" {
			api.RespondWithError(w, 400, "calendar_id required")
			return
		}

		user := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				user = s.Email
				break
			}
		}
		if user == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		ctx := r.Context()
		cal, err := fetchApprovedActiveCalendar(ctx, pgxPool, req.CalendarID)
		if err != nil {
			api.RespondWithError(w, 404, "calendar not found or not approved/active")
			return
		}
		hols, err := fetchApprovedActiveHolidays(ctx, pgxPool, req.CalendarID, cal.EffFrom, cal.EffTo)
		if err != nil {
			api.RespondWithError(w, 500, "fetch holidays failed: "+err.Error())
			return
		}

		ics := ICSCalendar{
			ProdID:   "-//Cimplr Corp//Holiday Calendar//EN",
			Name:     fmt.Sprintf("%s - %s", cal.Code, cal.Name),
			Timezone: cal.Timezone,
		}

		for _, h := range hols {
			ev := ICSEvent{
				Date:    h.Date,
				Summary: fmt.Sprintf("%s (%s)", h.Name, h.Type),
				UID:     fmt.Sprintf("%s-%s@cimplr", cal.CalendarID, h.ID),
			}
			if h.Notes != nil {
				ev.Description = *h.Notes
			}
			if h.RRule != nil {
				ev.RRule = strings.TrimSpace(*h.RRule)
			}
			ics.Events = append(ics.Events, ev)
		}

		text := buildICS(ics)

		filename := cal.Code
		if filename == "" {
			filename = cal.CalendarID
		}

		w.Header().Set("Content-Type", "text/calendar; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.ics"`, filename))
		_, _ = w.Write([]byte(text))
	}
}

func CalendarFeedICS(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			CalendarID string `json:"calendar_id"`
			UserID     string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.CalendarID) == "" {
			api.RespondWithError(w, 400, "calendar_id required")
			return
		}

		ctx := r.Context()
		cal, err := fetchApprovedActiveCalendar(ctx, pgxPool, req.CalendarID)
		if err != nil {
			api.RespondWithError(w, 404, "calendar not found or not approved/active")
			return
		}

		hols, err := fetchApprovedActiveHolidays(ctx, pgxPool, req.CalendarID, cal.EffFrom, cal.EffTo)
		if err != nil {
			api.RespondWithError(w, 500, "fetch holidays failed")
			return
		}

		ics := ICSCalendar{
			ProdID:   "-//Cimplr Corp//Holiday Calendar Feed//EN",
			Name:     fmt.Sprintf("%s - %s", cal.Code, cal.Name),
			Timezone: cal.Timezone,
		}
		for _, h := range hols {
			ics.Events = append(ics.Events, ICSEvent{
				Date:        h.Date,
				Summary:     fmt.Sprintf("%s (%s)", h.Name, h.Type),
				Description: stringOrEmpty(h.Notes),
				RRule:       stringOrEmpty(h.RRule),
				UID:         fmt.Sprintf("%s-%s@cimplr", cal.CalendarID, h.ID),
			})
		}

		w.Header().Set("Content-Type", "text/calendar; charset=utf-8")
		_, _ = io.WriteString(w, buildICS(ics))
	}
}

func ShareLinksCalendar(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			CalendarID string `json:"calendar_id"`
			UserID     string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.CalendarID) == "" {
			api.RespondWithError(w, 400, "calendar_id required")
			return
		}

		ctx := r.Context()
		cal, err := fetchApprovedActiveCalendar(ctx, pgxPool, req.CalendarID)
		if err != nil {
			api.RespondWithError(w, 404, "calendar not approved/active")
			return
		}

		base := guessBaseURL(r)
		feedURL := base + "/master/calendar/feed" // will be invoked by POST JSON request

		resp := map[string]string{
			"ics_download":             base + "/master/calendar/export/ics",
			"google_add_url":           "https://calendar.google.com/calendar/r?cid=" + url.QueryEscape(feedURL),
			"outlook365_subscribe_url": "https://outlook.office.com/calendar/0/addfromweb?url=" + url.QueryEscape(feedURL),
			"webcal_url":               "webcal://" + stripScheme(base) + "/master/calendar/feed",
			"calendar_name":            cal.Name,
			"calendar_code":            cal.Code,
			"latest_audit_status":      cal.Processing,
		}

		api.RespondWithPayload(w, true, "", resp)
	}
}

// Build a best-effort absolute base from request and proxies (X-Forwarded-Proto/Host)
func guessBaseURL(r *http.Request) string {
	scheme := r.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	// Ensure host contains a port only if not default
	h, p, err := net.SplitHostPort(host)
	if err == nil {
		// has port
		use := true
		if (scheme == "http" && p == "80") || (scheme == "https" && p == "443") {
			use = false
		}
		if use {
			host = net.JoinHostPort(h, p)
		} else {
			host = h
		}
	}
	return scheme + "://" + host
}

func stripScheme(base string) string {
	if strings.HasPrefix(base, "https://") {
		return strings.TrimPrefix(base, "https://")
	}
	if strings.HasPrefix(base, "http://") {
		return strings.TrimPrefix(base, "http://")
	}
	return base
}

func stringOrEmpty(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return strings.TrimSpace(*ptr)
}
