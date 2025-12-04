package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CreateCalendarReq struct {
	UserID             string `json:"user_id"`
	CalendarCode       string `json:"calendar_code"`
	CalendarName       string `json:"calendar_name"`
	Scope              string `json:"scope,omitempty"`
	Country            string `json:"country,omitempty"`
	State              string `json:"state,omitempty"`
	City               string `json:"city,omitempty"`
	Timezone           string `json:"timezone"`
	WeekendPattern     string `json:"weekend_pattern"`
	Source             string `json:"source,omitempty"`
	EffFrom            string `json:"eff_from"`
	EffTo              string `json:"eff_to,omitempty"`
	Status             string `json:"status,omitempty"`
	IngestionSource    string `json:"ingestion_source,omitempty"`
	ERPType            string `json:"erp_type,omitempty"`
	SAPFactoryCalID    string `json:"sap_factory_calendar_id,omitempty"`
	OracleCalendarName string `json:"oracle_calendar_name,omitempty"`
	TallyCalendarID    string `json:"tally_calendar_id,omitempty"`
	SageCalendarID     string `json:"sage_calendar_id,omitempty"`
}

func NormalizeDate(dateStr string) string {
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
		"02-Jan-2006",
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

func getCSVVal(r []string, pos map[string]int, col string) string {
	if i, ok := pos[col]; ok && i < len(r) {
		return strings.TrimSpace(r[i])
	}
	return ""
}

func CreateCalendarSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req CreateCalendarReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, "invalid json body")
			return
		}
		missing := []string{}
		if strings.TrimSpace(req.UserID) == "" {
			missing = append(missing, "user_id")
		}
		if strings.TrimSpace(req.CalendarCode) == "" {
			missing = append(missing, "calendar_code")
		}
		if strings.TrimSpace(req.CalendarName) == "" {
			missing = append(missing, "calendar_name")
		}
		if strings.TrimSpace(req.Timezone) == "" {
			missing = append(missing, "timezone")
		}
		if strings.TrimSpace(req.WeekendPattern) == "" {
			missing = append(missing, "weekend_pattern")
		}
		if strings.TrimSpace(req.EffFrom) == "" {
			missing = append(missing, "eff_from")
		}

		if len(missing) > 0 {
			api.RespondWithError(w, 400, "missing required fields: "+strings.Join(missing, ", "))
			return
		}
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}
		if req.Status == "" {
			req.Status = "Active"
		}
		if req.IngestionSource == "" {
			req.IngestionSource = "Manual"
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		var exists string
		err = tx.QueryRow(ctx, `
			SELECT calendar_id 
			FROM investment.mastercalendar 
			WHERE calendar_code=$1 AND COALESCE(is_deleted,false)=false 
			LIMIT 1
		`, req.CalendarCode).Scan(&exists)

		if err == nil {
			api.RespondWithError(w, 400, "calendar_code already exists")
			return
		}

		var calendarID string
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.mastercalendar 
			(calendar_code, calendar_name, scope, country, state, city, timezone, weekend_pattern, source, 
			 eff_from, eff_to, status, ingestion_source,
			 erp_type, sap_factory_calendar_id, oracle_calendar_name, tally_calendar_id, sage_calendar_id)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
			RETURNING calendar_id
		`,
			req.CalendarCode, req.CalendarName, req.Scope, req.Country, req.State, req.City,
			req.Timezone, req.WeekendPattern, req.Source,
			req.EffFrom, req.EffTo, req.Status, req.IngestionSource,
			req.ERPType, req.SAPFactoryCalID, req.OracleCalendarName, req.TallyCalendarID, req.SageCalendarID,
		).Scan(&calendarID)

		if err != nil {
			api.RespondWithError(w, 500, "calendar insert failed: "+err.Error())
			return
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.auditactioncalendar 
			(calendar_id, actiontype, processing_status, requested_by, requested_at)
			VALUES ($1, 'CREATE', 'PENDING_APPROVAL', $2, now())
		`, calendarID, userEmail)

		if err != nil {
			api.RespondWithError(w, 500, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{"calendar_id": calendarID})
	}
}

type BulkHolidayRow struct {
	HolidayDate     string `json:"holiday_date"`
	HolidayName     string `json:"holiday_name"`
	HolidayType     string `json:"holiday_type"`
	RecurrenceRule  string `json:"recurrence_rule,omitempty"`
	Notes           string `json:"notes,omitempty"`
	IngestionSource string `json:"ingestion_source,omitempty"`
}

type BulkHolidayReq struct {
	UserID     string           `json:"user_id"`
	CalendarID string           `json:"calendar_id"`
	Rows       []BulkHolidayRow `json:"rows"`
}

func CreateHolidayBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req BulkHolidayReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONShort)
			return
		}

		// Required
		if strings.TrimSpace(req.UserID) == "" ||
			strings.TrimSpace(req.CalendarID) == "" ||
			len(req.Rows) == 0 {
			api.RespondWithError(w, 400, "user_id, calendar_id and rows required")
			return
		}

		// Validate session
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		_, _ = tx.Exec(ctx, "SET LOCAL synchronous_commit TO OFF")

		// Temp table like folio upload
		_, err = tx.Exec(ctx, `
			DROP TABLE IF EXISTS tmp_holiday;
			CREATE TEMP TABLE tmp_holiday (
				calendar_id varchar(60),
				holiday_date date,
				holiday_name text,
				holiday_type varchar(50),
				recurrence_rule text,
				notes text,
				ingestion_source text
			) ON COMMIT DROP;
		`)
		if err != nil {
			api.RespondWithError(w, 500, "tmp table error: "+err.Error())
			return
		}

		// Prepare rows
		copyRows := make([][]interface{}, 0, len(req.Rows))
		for _, r := range req.Rows {

			if strings.TrimSpace(r.HolidayDate) == "" ||
				strings.TrimSpace(r.HolidayName) == "" ||
				strings.TrimSpace(r.HolidayType) == "" {
				api.RespondWithError(w, 400, "holiday_date, holiday_name & holiday_type are required")
				return
			}

			source := r.IngestionSource
			if source == "" {
				source = "Manual"
			}

			copyRows = append(copyRows, []interface{}{
				req.CalendarID,
				r.HolidayDate,
				r.HolidayName,
				r.HolidayType,
				r.RecurrenceRule,
				r.Notes,
				source,
			})
		}

		// Copy into temp
		_, err = tx.CopyFrom(
			ctx,
			pgx.Identifier{"tmp_holiday"},
			[]string{"calendar_id", "holiday_date", "holiday_name", "holiday_type", "recurrence_rule", "notes", "ingestion_source"},
			pgx.CopyFromRows(copyRows),
		)
		if err != nil {
			api.RespondWithError(w, 500, "copy error: "+err.Error())
			return
		}

		// Insert to master table (ignore duplicates) — use WHERE NOT EXISTS to avoid relying on unique indexes
		_, err = tx.Exec(ctx, `
			INSERT INTO investment.masterholiday
			(calendar_id, holiday_date, holiday_name, holiday_type, recurrence_rule, notes, ingestion_source, status)
			SELECT th.calendar_id, th.holiday_date, th.holiday_name, th.holiday_type, th.recurrence_rule, th.notes, th.ingestion_source, 'Active'
			FROM tmp_holiday th
			WHERE NOT EXISTS (
				SELECT 1 FROM investment.masterholiday mh
				WHERE mh.calendar_id = th.calendar_id
				  AND mh.holiday_date = th.holiday_date
				  AND mh.holiday_name = th.holiday_name
				  AND mh.holiday_type = th.holiday_type
				  AND COALESCE(mh.is_deleted,false)=false
			);
		`)
		if err != nil {
			api.RespondWithError(w, 500, "insert error: "+err.Error())
			return
		}

		// Create calendar audit once
		// Create calendar audit once
		_, err = tx.Exec(ctx, `
	INSERT INTO investment.auditactioncalendar
	(calendar_id, actiontype, processing_status, reason, requested_by, requested_at)
	SELECT DISTINCT calendar_id, 'EDIT', 'PENDING_EDIT_APPROVAL', 'Holidays Inserted', $1, now()
	FROM tmp_holiday
`, userEmail)

		if err != nil {
			api.RespondWithError(w, 500, "audit insert error: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, "commit error: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"calendar_id": req.CalendarID,
			"rows":        len(req.Rows),
			"status":      "PENDING_EDIT_APPROVAL",
		})
	}
}

func UploadCalendarBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// ---- Get user from form or body ----
		userID := r.FormValue("user_id")
		if userID == "" {
			var tmp struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&tmp)
			userID = tmp.UserID
		}
		if strings.TrimSpace(userID) == "" {
			api.RespondWithError(w, 400, constants.ErrUserIDRequired)
			return
		}

		// ---- Validate user session ----
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		// ---- Parse multipart ----
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, 400, "form parse: "+err.Error())
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, 400, "no file uploaded")
			return
		}

		// Allowed upload fields
		allowed := map[string]bool{
			"calendar_code": true, "calendar_name": true, "scope": true,
			"country": true, "state": true, "city": true,
			"timezone": true, "weekend_pattern": true, "source": true,
			"eff_from": true, "eff_to": true, "status": true,
			"erp_type": true, "sap_factory_calendar_id": true,
			"oracle_calendar_name": true, "tally_calendar_id": true,
			"sage_calendar_id": true, "ingestion_source": true,
		}

		batchIDs := []string{}

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, 400, "open file failed")
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, 400, "invalid csv")
				return
			}

			headers := normalizeHeader(records[0])
			dataRows := records[1:]

			validCols := []string{"calendar_code", "calendar_name", "timezone", "weekend_pattern", "eff_from"} // enforce minimum
			headerPos := map[string]int{}

			for i, h := range headers {
				headerPos[h] = i
				if allowed[h] {
					if !slices.Contains(validCols, h) {
						validCols = append(validCols, h)
					}
				}
			}

			// Required check
			reqCols := []string{"calendar_code", "calendar_name", "timezone", "weekend_pattern", "eff_from"}
			for _, c := range reqCols {
				if !slices.Contains(validCols, c) {
					api.RespondWithError(w, 400, "CSV missing mandatory columns: "+strings.Join(reqCols, ", "))
					return
				}
			}

			rowsToInsert := [][]interface{}{}
			for _, r := range dataRows {
				row := []interface{}{}
				for _, col := range validCols {
					val := ""
					if pos, ok := headerPos[col]; ok && pos < len(r) {
						val = strings.TrimSpace(r[pos])
					}
					if col == "eff_from" || col == "eff_to" {
						if d := NormalizeDate(val); d != "" {
							val = d
						}
					}
					if val == "" && col == "status" {
						val = "Active"
					}
					if val == "" && col == "ingestion_source" {
						val = "Upload"
					}
					row = append(row, val)
				}
				rowsToInsert = append(rowsToInsert, row)
			}

			// ---- DB TX ----
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, 500, "tx begin: "+err.Error())
				return
			}
			defer tx.Rollback(ctx)

			_, _ = tx.Exec(ctx, `
				CREATE TEMP TABLE tmp_calendar_upload(
					calendar_code varchar, calendar_name text, scope varchar,
					country varchar, state varchar, city varchar,
					"timezone" varchar, weekend_pattern varchar, source varchar,
					eff_from date, eff_to date, status varchar,
					erp_type varchar, sap_factory_calendar_id varchar,
					oracle_calendar_name varchar, tally_calendar_id varchar,
					sage_calendar_id varchar, ingestion_source text
				) ON COMMIT DROP
			`)

			_, err = tx.CopyFrom(ctx,
				pgx.Identifier{"tmp_calendar_upload"},
				validCols,
				pgx.CopyFromRows(rowsToInsert),
			)
			if err != nil {
				api.RespondWithError(w, 500, "copy: "+err.Error())
				return
			}

			// Insert Calendars
			_, err = tx.Exec(ctx, `
				INSERT INTO investment.mastercalendar
				(calendar_code, calendar_name, scope, country, state, city, timezone,
				 weekend_pattern, source, eff_from, eff_to, status, erp_type,
				 sap_factory_calendar_id, oracle_calendar_name, tally_calendar_id,
				 sage_calendar_id, ingestion_source)
				SELECT calendar_code, calendar_name, scope, country, state, city, timezone,
					   weekend_pattern, source, eff_from, eff_to, status, erp_type,
					   sap_factory_calendar_id, oracle_calendar_name, tally_calendar_id,
					   sage_calendar_id, ingestion_source
				FROM tmp_calendar_upload tcu
				WHERE NOT EXISTS (
					SELECT 1 FROM investment.mastercalendar mc
					WHERE mc.calendar_code = tcu.calendar_code
					  AND COALESCE(mc.is_deleted,false)=false
				)
			`)
			if err != nil {
				api.RespondWithError(w, 500, "insert: "+err.Error())
				return
			}

			// Audit for each created calendar
			_, err = tx.Exec(ctx, `
				INSERT INTO investment.auditactioncalendar
				(calendar_id, actiontype, processing_status, reason, requested_by, requested_at)
				SELECT calendar_id, 'CREATE', 'PENDING_APPROVAL', 'Calendar Uploaded (Bulk)', $1, now()
				FROM investment.mastercalendar
				WHERE calendar_code IN (SELECT calendar_code FROM tmp_calendar_upload)
				  AND is_deleted=false
			`, userEmail)
			if err != nil {
				api.RespondWithError(w, 500, "audit: "+err.Error())
				return
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, 500, "commit: "+err.Error())
				return
			}

			batchIDs = append(batchIDs, uuid.New().String())
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"success":   true,
			"batch_ids": batchIDs,
		})
	}
}

// UploadHolidayBulk handles CSV import for holidays
func UploadHolidayBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// ---- step 1: user ----
		userID := r.FormValue("user_id")
		if userID == "" {
			var tmp struct {
				UserID string `json:"user_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&tmp)
			userID = tmp.UserID
		}
		if userID == "" {
			api.RespondWithError(w, 400, constants.ErrUserIDRequired)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		// ---- step 2: parse multipart ----
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithError(w, 400, "parse form: "+err.Error())
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithError(w, 400, "no file uploaded")
			return
		}

		allowed := map[string]bool{
			"calendar_code":    true,
			"holiday_date":     true,
			"holiday_name":     true,
			"holiday_type":     true,
			"recurrence_rule":  true,
			"notes":            true,
			"ingestion_source": true,
		}

		batchIDs := []string{}

		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				api.RespondWithError(w, 400, "open file failed")
				return
			}
			records, err := parseCashFlowCategoryFile(f, getFileExt(fh.Filename))
			f.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithError(w, 400, "invalid csv")
				return
			}

			// headers
			headers := normalizeHeader(records[0])
			dataRows := records[1:]

			validCols := []string{}
			headerPos := map[string]int{}
			for i, h := range headers {
				headerPos[h] = i
				if allowed[h] {
					validCols = append(validCols, h)
				}
			}

			// required check
			reqCols := []string{"calendar_code", "holiday_date", "holiday_name", "holiday_type"}
			for _, c := range reqCols {
				if !slices.Contains(validCols, c) {
					api.RespondWithError(w, 400, "CSV must include: calendar_code, holiday_date, holiday_name, holiday_type")
					return
				}
			}

			// resolve all calendar_code to calendar_id
			codes := []string{}
			for _, r := range dataRows {
				codes = append(codes, strings.TrimSpace(r[headerPos["calendar_code"]]))
			}

			// get mapping
			calMap := map[string]string{}
			rows, _ := pgxPool.Query(ctx, `
				SELECT calendar_code, calendar_id
				FROM investment.mastercalendar
				WHERE calendar_code = ANY($1) AND is_deleted=false
			`, codes)
			for rows.Next() {
				var code, id string
				_ = rows.Scan(&code, &id)
				calMap[code] = id
			}
			rows.Close()

			// build copy rows
			tmpRows := make([][]interface{}, 0, len(dataRows))
			for _, r := range dataRows {
				cc := strings.TrimSpace(r[headerPos["calendar_code"]])
				calID := calMap[cc]
				if calID == "" {
					api.RespondWithError(w, 400, "calendar_code not found: "+cc)
					return
				}

				row := []interface{}{
					calID,
					strings.TrimSpace(r[headerPos["holiday_date"]]),
					strings.TrimSpace(r[headerPos["holiday_name"]]),
					strings.TrimSpace(r[headerPos["holiday_type"]]),
					getCSVVal(r, headerPos, "recurrence_rule"),
					getCSVVal(r, headerPos, "notes"),
					defaultIfEmpty(getCSVVal(r, headerPos, "ingestion_source"), "Upload"),
				}
				tmpRows = append(tmpRows, row)
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithError(w, 500, "tx begin: "+err.Error())
				return
			}
			defer tx.Rollback(ctx)

			// temp table
			_, _ = tx.Exec(ctx, `
				CREATE TEMP TABLE tmp_holiday_upload(
					calendar_id varchar,
					holiday_date date,
					holiday_name text,
					holiday_type varchar,
					recurrence_rule text,
					notes text,
					ingestion_source text
				) ON COMMIT DROP`)

			_, err = tx.CopyFrom(ctx,
				pgx.Identifier{"tmp_holiday_upload"},
				[]string{"calendar_id", "holiday_date", "holiday_name", "holiday_type", "recurrence_rule", "notes", "ingestion_source"},
				pgx.CopyFromRows(tmpRows),
			)
			if err != nil {
				api.RespondWithError(w, 500, "copy: "+err.Error())
				return
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO investment.masterholiday
				(calendar_id, holiday_date, holiday_name, holiday_type, recurrence_rule, notes, ingestion_source, status)
				SELECT thu.calendar_id, thu.holiday_date, thu.holiday_name, thu.holiday_type, thu.recurrence_rule, thu.notes, thu.ingestion_source, 'Active'
				FROM tmp_holiday_upload thu
				WHERE NOT EXISTS (
					SELECT 1 FROM investment.masterholiday mh
					WHERE mh.calendar_id = thu.calendar_id
					  AND mh.holiday_date = thu.holiday_date
					  AND mh.holiday_name = thu.holiday_name
					  AND mh.holiday_type = thu.holiday_type
					  AND COALESCE(mh.is_deleted,false)=false
				)
			`)
			if err != nil {
				api.RespondWithError(w, 500, "insert: "+err.Error())
				return
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO investment.auditactioncalendar
				(calendar_id, actiontype, processing_status, reason, requested_by, requested_at)
				SELECT DISTINCT calendar_id, 'EDIT','PENDING_EDIT_APPROVAL','Holidays Inserted (Bulk)', $1, now()
				FROM tmp_holiday_upload
			`, userEmail)
			if err != nil {
				api.RespondWithError(w, 500, "audit: "+err.Error())
				return
			}

			if err := tx.Commit(ctx); err != nil {
				api.RespondWithError(w, 500, "commit: "+err.Error())
				return
			}

			batchIDs = append(batchIDs, uuid.New().String())
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"success":   true,
			"batch_ids": batchIDs,
		})
	}
}

func GetCalendarsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
WITH latest_audit AS (
    SELECT DISTINCT ON (a.calendar_id)
        a.calendar_id,
        a.actiontype,
        a.processing_status,
        a.action_id,
        a.requested_by,
        a.requested_at,
        a.checker_by,
        a.checker_at,
        a.checker_comment,
        a.reason
    FROM investment.auditactioncalendar a
    ORDER BY a.calendar_id, a.requested_at DESC
),
history AS (
    SELECT 
        calendar_id,
        MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
        MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
        MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
        MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
        MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
        MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
    FROM investment.auditactioncalendar
    GROUP BY calendar_id
)
SELECT
    mc.calendar_id,
    mc.calendar_code,
    mc.old_calendar_code,
    mc.calendar_name,
    mc.old_calendar_name,
    mc.scope,
    mc.old_scope,
    mc.country,
    mc.old_country,
    mc.state,
    mc.old_state,
    mc.city,
    mc.old_city,
    mc.timezone,
    mc.old_timezone,
    mc.weekend_pattern,
    mc.old_weekend_pattern,
    mc.source,
    mc.old_source,
    mc.eff_from,
    mc.old_eff_from,
    mc.eff_to,
    mc.old_eff_to,
    mc.status,
    mc.old_status,
    mc.erp_type,
    mc.old_erp_type,
    mc.sap_factory_calendar_id,
    mc.old_sap_factory_calendar_id,
    mc.oracle_calendar_name,
    mc.old_oracle_calendar_name,
    mc.tally_calendar_id,
    mc.old_tally_calendar_id,
    mc.sage_calendar_id,
    mc.old_sage_calendar_id,
    mc.ingestion_source,
    mc.is_deleted,

    -- latest audit
    COALESCE(l.actiontype,'') AS action_type,
    COALESCE(l.processing_status,'') AS processing_status,
    COALESCE(l.action_id::text,'') AS action_id,
    COALESCE(l.requested_by,'') AS requested_by,
    TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
    COALESCE(l.checker_by,'') AS checker_by,
    TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
    COALESCE(l.checker_comment,'') AS checker_comment,
    COALESCE(l.reason,'') AS reason,

    -- history
    COALESCE(h.created_by,'') AS created_by,
    COALESCE(h.created_at,'') AS created_at,
    COALESCE(h.edited_by,'') AS edited_by,
    COALESCE(h.edited_at,'') AS edited_at,
    COALESCE(h.deleted_by,'') AS deleted_by,
    COALESCE(h.deleted_at,'') AS deleted_at

FROM investment.mastercalendar mc
LEFT JOIN latest_audit l ON l.calendar_id = mc.calendar_id
LEFT JOIN history h ON h.calendar_id = mc.calendar_id
WHERE COALESCE(mc.is_deleted,false) = false
ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC	, mc.calendar_name, mc.calendar_code;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, 500, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)

		for rows.Next() {
			vals, _ := rows.Values()
			rec := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				name := string(f.Name)
				if vals[i] == nil {
					rec[name] = ""
				} else if t, ok := vals[i].(time.Time); ok {
					rec[name] = t.Format(constants.DateTimeFormat)
				} else {
					rec[name] = vals[i]
				}
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, 500, "scan failed: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetCalendarListLite(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
WITH latest_audit AS (
    SELECT DISTINCT ON (calendar_id)
        calendar_id,
        requested_at,
        checker_at
    FROM investment.auditactioncalendar
    ORDER BY calendar_id, GREATEST(COALESCE(requested_at, '1970-01-01'::timestamp), COALESCE(checker_at, '1970-01-01'::timestamp)) DESC
)
SELECT
    mc.calendar_id,
    mc.calendar_code,
    mc.calendar_name,
    mc.eff_from,
    mc.eff_to
FROM investment.mastercalendar mc
LEFT JOIN latest_audit la ON la.calendar_id = mc.calendar_id
WHERE COALESCE(mc.is_deleted,false) = false
ORDER BY GREATEST(COALESCE(la.requested_at, '1970-01-01'::timestamp), COALESCE(la.checker_at, '1970-01-01'::timestamp)) DESC,
    mc.calendar_name;
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, 500, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var id, code, name string
			var effFrom, effTo *time.Time

			if err := rows.Scan(&id, &code, &name, &effFrom, &effTo); err != nil {
				api.RespondWithError(w, 500, "scan failed: "+err.Error())
				return
			}

			out = append(out, map[string]interface{}{
				"calendar_id":   id,
				"calendar_code": code,
				"calendar_name": name,
				"eff_from":      formatDateOrEmpty(effFrom),
				"eff_to":        formatDateOrEmpty(effTo),
			})
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func formatDateOrEmpty(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(constants.DateFormat)
}

func GetCalendarWithHolidays(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			CalendarID string `json:"calendar_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.CalendarID) == "" {
			api.RespondWithError(w, 400, "calendar_id required")
			return
		}

		ctx := r.Context()

		q := `
WITH latest_audit AS (
    SELECT DISTINCT ON (calendar_id)
        calendar_id, actiontype, processing_status, action_id,
        requested_by, requested_at, checker_by, checker_at,
        checker_comment, reason
    FROM investment.auditactioncalendar
    WHERE calendar_id = $1
    ORDER BY calendar_id, requested_at DESC
),
history AS (
    SELECT 
        calendar_id,
        MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
        MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
        MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
        MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
        MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
        MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
    FROM investment.auditactioncalendar
    WHERE calendar_id = $1
    GROUP BY calendar_id
),
hol AS (
	SELECT 
        holiday_id,
        holiday_date,
        old_holiday_date,
        holiday_name,
        old_holiday_name,
        holiday_type,
        old_holiday_type,
        recurrence_rule,
        old_recurrence_rule,
        notes,
        old_notes,
        status,
        old_status,
        ingestion_source
	FROM investment.masterholiday
	WHERE calendar_id = $1
	  AND COALESCE(is_deleted,false)=false
	ORDER BY holiday_date
)

SELECT
	jsonb_build_object(
		'calendar', jsonb_build_object(
			'calendar_id', mc.calendar_id,
			'calendar_code', mc.calendar_code,
			'old_calendar_code', mc.old_calendar_code,
			'calendar_name', mc.calendar_name,
			'old_calendar_name', mc.old_calendar_name,
			'scope', mc.scope,
			'old_scope', mc.old_scope,
			'country', mc.country,
			'old_country', mc.old_country,
			'state', mc.state,
			'old_state', mc.old_state,
			'city', mc.city,
			'old_city', mc.old_city,
			'timezone', mc.timezone,
			'old_timezone', mc.old_timezone,
			'weekend_pattern', mc.weekend_pattern,
			'old_weekend_pattern', mc.old_weekend_pattern,
			'source', mc.source,
			'old_source', mc.old_source,
			'eff_from', mc.eff_from,
			'old_eff_from', mc.old_eff_from,
			'eff_to', mc.eff_to,
			'old_eff_to', mc.old_eff_to,
			'status', mc.status,
			'old_status', mc.old_status,
			'erp_type', mc.erp_type,
			'sap_factory_calendar_id', mc.sap_factory_calendar_id,
			'oracle_calendar_name', mc.oracle_calendar_name,
			'tally_calendar_id', mc.tally_calendar_id,
			'sage_calendar_id', mc.sage_calendar_id,

			-- audit fields merged here
			'action_type', COALESCE(l.actiontype,''),
			'processing_status', COALESCE(l.processing_status,''),
			'requested_by', COALESCE(l.requested_by,''),
			'requested_at', COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),''),
			'checker_by', COALESCE(l.checker_by,''),
			'checker_at', COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),''),
			'checker_comment', COALESCE(l.checker_comment,''),
			'reason', COALESCE(l.reason,''),

			'created_by', COALESCE(h.created_by,''),
			'created_at', COALESCE(h.created_at,''),
			'edited_by', COALESCE(h.edited_by,''),
			'edited_at', COALESCE(h.edited_at,''),
			'deleted_by', COALESCE(h.deleted_by,''),
			'deleted_at', COALESCE(h.deleted_at,'')
		),
		'holidays', COALESCE((SELECT json_agg(row_to_json(hol.*)) FROM hol), '[]'::json)
	) AS data
FROM investment.mastercalendar mc
LEFT JOIN latest_audit l ON l.calendar_id = mc.calendar_id
LEFT JOIN history h ON h.calendar_id = mc.calendar_id
WHERE mc.calendar_id = $1 AND COALESCE(mc.is_deleted,false)=false;
`

		var data any
		err := pgxPool.QueryRow(ctx, q, req.CalendarID).Scan(&data)
		if err != nil {
			api.RespondWithError(w, 404, "calendar not found")
			return
		}

		api.RespondWithPayload(w, true, "", data)
	}
}

func GetApprovedActiveCalendars(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
WITH latest AS (
    SELECT DISTINCT ON (calendar_id)
        calendar_id, processing_status
    FROM investment.auditactioncalendar
    ORDER BY calendar_id, requested_at DESC
)
SELECT
    mc.calendar_id,
    mc.calendar_code,
    mc.calendar_name
FROM investment.mastercalendar mc
JOIN latest l ON l.calendar_id = mc.calendar_id
WHERE 
    UPPER(l.processing_status) = 'APPROVED'
    AND UPPER(mc.status) = 'ACTIVE'
    AND COALESCE(mc.is_deleted,false)=false
ORDER BY mc.calendar_code;
`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			vals, _ := rows.Values()
			rec := map[string]interface{}{
				"calendar_id":   vals[0],
				"calendar_code": vals[1],
				"calendar_name": vals[2],
			}
			out = append(out, rec)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "scan failed: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetCalendarWithHolidaysApproved(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			CalendarID string `json:"calendar_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.CalendarID) == "" {
			api.RespondWithError(w, 400, "calendar_id required")
			return
		}

		ctx := r.Context()

		q := `
WITH latest_audit AS (
    SELECT DISTINCT ON (calendar_id)
        calendar_id, actiontype, processing_status, action_id,
        requested_by, requested_at, checker_by, checker_at,
        checker_comment, reason
    FROM investment.auditactioncalendar
    WHERE calendar_id = $1
    ORDER BY calendar_id, requested_at DESC
),
history AS (
    SELECT 
        calendar_id,
        MAX(CASE WHEN actiontype='CREATE' THEN requested_by END) AS created_by,
        MAX(CASE WHEN actiontype='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
        MAX(CASE WHEN actiontype='EDIT' THEN requested_by END) AS edited_by,
        MAX(CASE WHEN actiontype='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
        MAX(CASE WHEN actiontype='DELETE' THEN requested_by END) AS deleted_by,
        MAX(CASE WHEN actiontype='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
    FROM investment.auditactioncalendar
    WHERE calendar_id = $1
    GROUP BY calendar_id
),
hol AS (
	SELECT 
        holiday_id,
        holiday_date,
        old_holiday_date,
        holiday_name,
        old_holiday_name,
        holiday_type,
        old_holiday_type,
        recurrence_rule,
        old_recurrence_rule,
        notes,
        old_notes,
        status,
        old_status,
        ingestion_source
	FROM investment.masterholiday
	WHERE calendar_id = $1
	  AND COALESCE(is_deleted,false)=false
	  AND UPPER(status) = 'ACTIVE'
	ORDER BY holiday_date
)

SELECT
	jsonb_build_object(
		'calendar', jsonb_build_object(
			'calendar_id', mc.calendar_id,
			'calendar_code', mc.calendar_code,
			'old_calendar_code', mc.old_calendar_code,
			'calendar_name', mc.calendar_name,
			'old_calendar_name', mc.old_calendar_name,
			'scope', mc.scope,
			'old_scope', mc.old_scope,
			'country', mc.country,
			'old_country', mc.old_country,
			'state', mc.state,
			'old_state', mc.old_state,
			'city', mc.city,
			'old_city', mc.old_city,
			'timezone', mc.timezone,
			'old_timezone', mc.old_timezone,
			'weekend_pattern', mc.weekend_pattern,
			'old_weekend_pattern', mc.old_weekend_pattern,
			'source', mc.source,
			'old_source', mc.old_source,
			'eff_from', mc.eff_from,
			'old_eff_from', mc.old_eff_from,
			'eff_to', mc.eff_to,
			'old_eff_to', mc.old_eff_to,
			'status', mc.status,
			'old_status', mc.old_status,
			'erp_type', mc.erp_type,
			'sap_factory_calendar_id', mc.sap_factory_calendar_id,
			'oracle_calendar_name', mc.oracle_calendar_name,
			'tally_calendar_id', mc.tally_calendar_id,
			'sage_calendar_id', mc.sage_calendar_id,

			'action_type', COALESCE(l.actiontype,''),
			'processing_status', COALESCE(l.processing_status,''),
			'requested_by', COALESCE(l.requested_by,''),
			'requested_at', COALESCE(TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS'),''),
			'checker_by', COALESCE(l.checker_by,''),
			'checker_at', COALESCE(TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS'),''),
			'checker_comment', COALESCE(l.checker_comment,''),
			'reason', COALESCE(l.reason,''),

			'created_by', COALESCE(h.created_by,''),
			'created_at', COALESCE(h.created_at,''),
			'edited_by', COALESCE(h.edited_by,''),
			'edited_at', COALESCE(h.edited_at,''),
			'deleted_by', COALESCE(h.deleted_by,''),
			'deleted_at', COALESCE(h.deleted_at,'')
		),
		'holidays', COALESCE((SELECT json_agg(row_to_json(hol.*)) FROM hol), '[]'::json)
	) AS data
FROM investment.mastercalendar mc
LEFT JOIN latest_audit l ON l.calendar_id = mc.calendar_id
LEFT JOIN history h ON h.calendar_id = mc.calendar_id
WHERE mc.calendar_id = $1
  AND COALESCE(mc.is_deleted,false)=false
  AND UPPER(mc.status) = 'ACTIVE'
  AND UPPER(COALESCE(l.processing_status,'')) = 'APPROVED';
`

		var data any
		err := pgxPool.QueryRow(ctx, q, req.CalendarID).Scan(&data)
		if err != nil {
			api.RespondWithError(w, 404, "calendar not found or not approved")
			return
		}

		api.RespondWithPayload(w, true, "", data)
	}
}

func DeleteCalendar(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			UserID      string   `json:"user_id"`
			CalendarIDs []string `json:"calendar_ids"`
			Reason      string   `json:"reason"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONShort)
			return
		}
		if len(req.CalendarIDs) == 0 {
			api.RespondWithError(w, 400, "calendar_ids required")
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		for _, id := range req.CalendarIDs {
			_, err := tx.Exec(ctx, `
				INSERT INTO investment.auditactioncalendar
				(calendar_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())
			`, id, req.Reason, requestedBy)

			if err != nil {
				api.RespondWithError(w, 500, "insert failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"delete_requested": req.CalendarIDs})
	}
}

func BulkApproveCalendarActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			UserID      string   `json:"user_id"`
			CalendarIDs []string `json:"calendar_ids"`
			Comment     string   `json:"comment"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONShort)
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "tx begin failed")
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (calendar_id)
				action_id, calendar_id, actiontype, processing_status
			FROM investment.auditactioncalendar
			WHERE calendar_id = ANY($1)
			ORDER BY calendar_id, requested_at DESC
		`

		rows, err := tx.Query(ctx, sel, req.CalendarIDs)
		if err != nil {
			api.RespondWithError(w, 500, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		toApprove := []string{}
		toDeleteActionIDs := []string{}
		deleteMasterIDs := []string{}

		for rows.Next() {
			var aid, cid, atype, pstatus string
			rows.Scan(&aid, &cid, &atype, &pstatus)

			ps := strings.ToUpper(strings.TrimSpace(pstatus))
			if ps == "APPROVED" {
				continue
			}

			if ps == "PENDING_DELETE_APPROVAL" {
				toDeleteActionIDs = append(toDeleteActionIDs, aid)
				deleteMasterIDs = append(deleteMasterIDs, cid)
				continue
			}

			if ps == "PENDING_APPROVAL" || ps == "PENDING_EDIT_APPROVAL" {
				toApprove = append(toApprove, aid)
			}
		}

		if len(toApprove) == 0 && len(toDeleteActionIDs) == 0 {
			api.RespondWithPayload(w, false, "No approvable actions found",
				map[string]any{"approved_action_ids": []string{}, "deleted_calendars": []string{}})
			return
		}

		if len(toApprove) > 0 {
			_, err := tx.Exec(ctx, `
				UPDATE investment.auditactioncalendar
				SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toApprove)
			if err != nil {
				api.RespondWithError(w, 500, "approve update failed: "+err.Error())
				return
			}
		}

		if len(toDeleteActionIDs) > 0 {
			_, err := tx.Exec(ctx, `
				UPDATE investment.auditactioncalendar
				SET processing_status='DELETED', checker_by=$1, checker_at=now(), checker_comment=$2
				WHERE action_id = ANY($3)
			`, checkerBy, req.Comment, toDeleteActionIDs)
			if err != nil {
				api.RespondWithError(w, 500, "mark deleted failed: "+err.Error())
				return
			}

			_, err = tx.Exec(ctx, `
				UPDATE investment.mastercalendar
				SET is_deleted=true, status='Inactive'
				WHERE calendar_id = ANY($1)
			`, deleteMasterIDs)
			if err != nil {
				api.RespondWithError(w, 500, "master soft-delete failed: "+err.Error())
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "",
			map[string]any{"approved_action_ids": toApprove, "deleted_calendars": deleteMasterIDs})
	}
}

func BulkRejectCalendarActions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			UserID      string   `json:"user_id"`
			CalendarIDs []string `json:"calendar_ids"`
			Comment     string   `json:"comment"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONShort)
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		ctx := context.Background()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "tx begin failed")
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT DISTINCT ON (calendar_id)
				action_id, calendar_id, processing_status
			FROM investment.auditactioncalendar
			WHERE calendar_id = ANY($1)
			ORDER BY calendar_id, requested_at DESC
		`
		rows, err := tx.Query(ctx, sel, req.CalendarIDs)
		if err != nil {
			api.RespondWithError(w, 500, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		var actionIDs, cannotReject, missing []string
		found := map[string]bool{}

		for rows.Next() {
			var aid, cid, ps string
			rows.Scan(&aid, &cid, &ps)

			found[cid] = true
			if strings.ToUpper(ps) == "APPROVED" {
				cannotReject = append(cannotReject, cid)
			} else {
				actionIDs = append(actionIDs, aid)
			}
		}

		for _, id := range req.CalendarIDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			api.RespondWithError(w, 400,
				fmt.Sprintf("missing: %v | cannot reject approved: %v", missing, cannotReject))
			return
		}

		_, err = tx.Exec(ctx, `
			UPDATE investment.auditactioncalendar
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE action_id = ANY($3)
		`, checkerBy, req.Comment, actionIDs)

		if err != nil {
			api.RespondWithError(w, 500, "update failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, "commit failed")
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{"rejected_action_ids": actionIDs})
	}
}

func UpdateCalendar(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			UserID     string                 `json:"user_id"`
			CalendarID string                 `json:"calendar_id"`
			Fields     map[string]interface{} `json:"fields"`
			Reason     string                 `json:"reason"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.CalendarID) == "" {
			api.RespondWithError(w, 400, "calendar_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, 400, "no fields to update")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Fetch old values
		var oldVals = make(map[string]interface{})
		sel := `
            SELECT calendar_code, calendar_name, scope, country, state, city,
                   timezone, weekend_pattern, source, eff_from, eff_to, status,
                   erp_type, sap_factory_calendar_id, oracle_calendar_name,
                   tally_calendar_id, sage_calendar_id
            FROM investment.mastercalendar
            WHERE calendar_id=$1 FOR UPDATE
        `
		cols := []string{
			"calendar_code", "calendar_name", "scope", "country", "state", "city",
			"timezone", "weekend_pattern", "source", "eff_from", "eff_to", "status",
			"erp_type", "sap_factory_calendar_id", "oracle_calendar_name",
			"tally_calendar_id", "sage_calendar_id",
		}

		scanArgs := make([]interface{}, len(cols))
		for i := range cols {
			scanArgs[i] = new(interface{})
		}

		if err := tx.QueryRow(ctx, sel, req.CalendarID).Scan(scanArgs...); err != nil {
			api.RespondWithError(w, 500, "calendar fetch failed: "+err.Error())
			return
		}
		for i, c := range cols {
			oldVals[c] = *(scanArgs[i].(*interface{}))
		}

		// Allowed updatable fields
		allowed := map[string]bool{
			"calendar_code": true, "calendar_name": true, "scope": true,
			"country": true, "state": true, "city": true,
			"timezone": true, "weekend_pattern": true, "source": true,
			"eff_from": true, "eff_to": true, "status": true,
			"erp_type": true, "sap_factory_calendar_id": true,
			"oracle_calendar_name": true, "tally_calendar_id": true,
			"sage_calendar_id": true,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			field := strings.ToLower(k)
			if !allowed[field] {
				continue
			}

			// uniqueness check for calendar_code
			if field == "calendar_code" {
				newCode := strings.TrimSpace(fmt.Sprint(v))
				oldCode := fmt.Sprint(oldVals["calendar_code"])
				if newCode != "" && newCode != oldCode {
					var exists string
					err := tx.QueryRow(ctx, `
                        SELECT calendar_id FROM investment.mastercalendar
                        WHERE calendar_code=$1 AND COALESCE(is_deleted,false)=false LIMIT 1
                    `, newCode).Scan(&exists)
					if err == nil {
						api.RespondWithError(w, 400, "calendar_code already exists")
						return
					}
				}
			}

			oldField := "old_" + field
			sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, field, pos, oldField, pos+1))
			args = append(args, v, oldVals[field])
			pos += 2
		}

		if len(sets) == 0 {
			api.RespondWithError(w, 400, "no valid updatable fields found")
			return
		}

		q := fmt.Sprintf(`UPDATE investment.mastercalendar SET %s WHERE calendar_id=$%d`,
			strings.Join(sets, ", "), pos)
		args = append(args, req.CalendarID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithError(w, 500, "update failed: "+err.Error())
			return
		}

		// Insert audit
		_, err = tx.Exec(ctx, `
            INSERT INTO investment.auditactioncalendar
            (calendar_id, actiontype, processing_status, reason, requested_by, requested_at)
            VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
        `, req.CalendarID, req.Reason, userEmail)
		if err != nil {
			api.RespondWithError(w, 500, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"calendar_id":  req.CalendarID,
			"requested_by": userEmail,
		})
	}
}

func UpdateHoliday(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			UserID    string                 `json:"user_id"`
			HolidayID string                 `json:"holiday_id"`
			Fields    map[string]interface{} `json:"fields"`
			Reason    string                 `json:"reason"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.HolidayID) == "" {
			api.RespondWithError(w, 400, "holiday_id required")
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, 400, "no fields to update")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "tx begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		var calendarID string
		oldVals := make(map[string]interface{})
		sel := `
			SELECT calendar_id,
				   holiday_date, holiday_name, holiday_type,
				   recurrence_rule, notes, status
			FROM investment.masterholiday
			WHERE holiday_id=$1 AND COALESCE(is_deleted,false)=false
			FOR UPDATE
		`

		cols := []string{"holiday_date", "holiday_name", "holiday_type",
			"recurrence_rule", "notes", "status"}

		scanArgs := make([]interface{}, len(cols)+1) // + calendar_id
		scanArgs[0] = &calendarID
		for i := range cols {
			scanArgs[i+1] = new(interface{})
		}

		if err := tx.QueryRow(ctx, sel, req.HolidayID).Scan(scanArgs...); err != nil {
			tx.Rollback(ctx)
			api.RespondWithError(w, 404, "Holiday not found")
			return
		}
		for i, c := range cols {
			oldVals[c] = *(scanArgs[i+1].(*interface{}))
		}

		allowed := map[string]bool{
			"holiday_date":    true,
			"holiday_name":    true,
			"holiday_type":    true,
			"recurrence_rule": true,
			"notes":           true,
			"status":          true,
		}

		var sets []string
		var args []interface{}
		pos := 1

		var newDateVal interface{} = oldVals["holiday_date"]
		newName := fmt.Sprint(oldVals["holiday_name"])
		newType := fmt.Sprint(oldVals["holiday_type"])

		// helper to canonicalize a date value to YYYY-MM-DD for reliable comparison
		toDateStr := func(x interface{}) string {
			if x == nil {
				return ""
			}
			switch t := x.(type) {
			case time.Time:
				return t.Format(constants.DateFormat)
			case *time.Time:
				if t == nil {
					return ""
				}
				return t.Format(constants.DateFormat)
			case string:
				return NormalizeDate(t)
			default:
				return NormalizeDate(fmt.Sprint(t))
			}
		}

		for k, v := range req.Fields {
			field := strings.ToLower(k)
			if !allowed[field] {
				continue
			}
			// Track new potential values for uniqueness check and ensure holiday_date is a safe type
			if field == "holiday_date" {
				// attempt to normalize/parse incoming date values to either YYYY-MM-DD string or time.Time
				switch tv := v.(type) {
				case string:
					s := strings.TrimSpace(tv)
					if s == "" {
						tx.Rollback(ctx)
						api.RespondWithError(w, 400, "invalid holiday_date")
						return
					}
					// try NormalizeDate to get YYYY-MM-DD
					nd := NormalizeDate(s)
					if nd != "" {
						// keep as normalized date string (Postgres accepts 'YYYY-MM-DD')
						v = nd
						newDateVal = nd
					} else {
						// try RFC3339/time.Parse fallback and convert to YYYY-MM-DD string
						if tt, err := time.Parse(time.RFC3339, s); err == nil {
							v = tt.Format(constants.DateFormat)
							newDateVal = v
						} else if tt2, err2 := time.Parse(constants.DateTimeFormat, s); err2 == nil {
							v = tt2.Format(constants.DateFormat)
							newDateVal = v
						} else {
							tx.Rollback(ctx)
							api.RespondWithError(w, 400, "invalid holiday_date format")
							return
						}
					}
				case time.Time:
					// convert to plain date string to avoid timezone formatting issues
					v = tv.Format(constants.DateFormat)
					newDateVal = v
				case *time.Time:
					if tv != nil {
						v = tv.Format(constants.DateFormat)
						newDateVal = v
					} else {
						newDateVal = nil
					}
				default:
					// try stringifying and normalizing
					s := strings.TrimSpace(fmt.Sprint(tv))
					nd := NormalizeDate(s)
					if nd != "" {
						v = nd
						newDateVal = nd
					} else {
						tx.Rollback(ctx)
						api.RespondWithError(w, 400, "invalid holiday_date format")
						return
					}
				}
			}
			if field == "holiday_name" {
				newName = fmt.Sprint(v)
			}
			if field == "holiday_type" {
				newType = fmt.Sprint(v)
			}

			oldField := "old_" + field
			sets = append(sets, fmt.Sprintf(constants.FormatSQLSetPair, field, pos, oldField, pos+1))
			args = append(args, v, oldVals[field])
			pos += 2
		}

		if len(sets) == 0 {
			// ensure transaction is rolled back before returning
			tx.Rollback(ctx)
			api.RespondWithError(w, 400, "no valid editable fields")
			return
		}
		if toDateStr(newDateVal) != toDateStr(oldVals["holiday_date"]) ||
			newName != fmt.Sprint(oldVals["holiday_name"]) ||
			newType != fmt.Sprint(oldVals["holiday_type"]) {

			var exists string
			err := tx.QueryRow(ctx, `
				SELECT holiday_id FROM investment.masterholiday
				WHERE calendar_id=$1 AND holiday_date=$2 AND holiday_name=$3 AND holiday_type=$4
				AND holiday_id <> $5 AND COALESCE(is_deleted,false)=false
				LIMIT 1
			`, calendarID, newDateVal, newName, newType, req.HolidayID).Scan(&exists)

			if err == nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, 400, "duplicate holiday date+name+type for this calendar")
				return
			}
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				tx.Rollback(ctx)
				api.RespondWithError(w, 500, "duplicate check failed: "+err.Error())
				return
			}
		}

		q := fmt.Sprintf(`UPDATE investment.masterholiday SET %s WHERE holiday_id=$%d`,
			strings.Join(sets, ", "), pos)
		args = append(args, req.HolidayID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			tx.Rollback(ctx)
			api.RespondWithError(w, 500, "update failed: "+err.Error())
			return
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.auditactioncalendar
			(calendar_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
		`, calendarID, req.Reason, userEmail)
		if err != nil {
			tx.Rollback(ctx)
			api.RespondWithError(w, 500, "audit insert failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{
			"holiday_id":   req.HolidayID,
			"calendar_id":  calendarID,
			"requested_by": userEmail,
			"status":       "PENDING_EDIT_APPROVAL",
		})
	}
}

func UpdateCalendarWithHolidays(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string                   `json:"user_id"`
			CalendarID     string                   `json:"calendar_id"`
			Reason         string                   `json:"reason"`
			CalendarFields map[string]interface{}   `json:"calendar_fields"`
			Holidays       []map[string]interface{} `json:"holidays"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, constants.ErrInvalidJSONShort)
			return
		}
		if strings.TrimSpace(req.CalendarID) == "" {
			api.RespondWithError(w, 400, "calendar_id required")
			return
		}
		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Name
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, 401, "Invalid session")
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, 500, "TX begin failed: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		allowed := map[string]bool{
			"calendar_code": true, "calendar_name": true, "scope": true, "country": true,
			"state": true, "city": true, "timezone": true, "weekend_pattern": true,
			"source": true, "eff_from": true, "eff_to": true, "status": true,
			"erp_type": true, "sap_factory_calendar_id": true, "oracle_calendar_name": true,
			"tally_calendar_id": true, "sage_calendar_id": true,
		}

		// Fetch old values
		oldVals := make(map[string]interface{})
		cols := []string{"calendar_code", "calendar_name", "scope", "country", "state", "city",
			"timezone", "weekend_pattern", "source", "eff_from", "eff_to", "status",
			"erp_type", "sap_factory_calendar_id", "oracle_calendar_name",
			"tally_calendar_id", "sage_calendar_id"}

		sel := `SELECT ` + strings.Join(cols, ",") + ` FROM investment.mastercalendar WHERE calendar_id=$1 FOR UPDATE`
		row := tx.QueryRow(ctx, sel, req.CalendarID)

		scanArgs := make([]interface{}, len(cols))
		for i := range cols {
			scanArgs[i] = new(interface{})
		}

		if err := row.Scan(scanArgs...); err != nil {
			api.RespondWithError(w, 404, "calendar not found")
			return
		}
		for i, c := range cols {
			oldVals[c] = *(scanArgs[i].(*interface{}))
		}

		var calSets []string
		var calArgs []interface{}
		pos := 1

		for k, v := range req.CalendarFields {
			f := strings.ToLower(k)
			if !allowed[f] {
				continue
			}

			oldField := "old_" + f
			calSets = append(calSets, fmt.Sprintf(constants.FormatSQLSetPair, f, pos, oldField, pos+1))
			calArgs = append(calArgs, v, oldVals[f])
			pos += 2
		}

		if len(calSets) > 0 {
			q := fmt.Sprintf(`UPDATE investment.mastercalendar SET %s WHERE calendar_id=$%d`,
				strings.Join(calSets, ", "), pos)
			calArgs = append(calArgs, req.CalendarID)

			if _, err := tx.Exec(ctx, q, calArgs...); err != nil {
				api.RespondWithError(w, 500, "Calendar update failed: "+err.Error())
				return
			}
		}

		holidayResults := []map[string]string{}

		for _, h := range req.Holidays {
			holidayID := fmt.Sprint(h["holiday_id"])
			if holidayID == "" {
				holidayResults = append(holidayResults, map[string]string{
					"id": "", "status": "failed", "error": "holiday_id missing",
				})
				continue
			}

			// fetch old
			var old map[string]interface{}
			hsel := `
				SELECT holiday_date, holiday_name, holiday_type, recurrence_rule, notes, status
				FROM investment.masterholiday 
				WHERE holiday_id=$1 AND calendar_id=$2 AND COALESCE(is_deleted,false)=false
				FOR UPDATE
			`

			var o1, o2, o3, o4, o5, o6 interface{}
			err := tx.QueryRow(ctx, hsel, holidayID, req.CalendarID).Scan(&o1, &o2, &o3, &o4, &o5, &o6)
			if err != nil {
				holidayResults = append(holidayResults, map[string]string{
					"id": holidayID, "status": "failed", "error": "holiday not found",
				})
				continue
			}
			old = map[string]interface{}{
				"holiday_date": o1, "holiday_name": o2, "holiday_type": o3,
				"recurrence_rule": o4, "notes": o5, "status": o6,
			}

			allowedH := []string{"holiday_date", "holiday_name", "holiday_type", "recurrence_rule", "notes", "status"}

			var setH []string
			var argsH []interface{}
			p := 1
			for _, f := range allowedH {
				if val, ok := h[f]; ok {
					setH = append(setH, fmt.Sprintf("%s=$%d, old_%s=$%d", f, p, f, p+1))
					argsH = append(argsH, val, old[f])
					p += 2
				}
			}
			if len(setH) == 0 {
				holidayResults = append(holidayResults, map[string]string{
					"id": holidayID, "status": "skipped", "error": "no fields changed",
				})
				continue
			}

			// Update holiday
			qh := fmt.Sprintf(`UPDATE investment.masterholiday SET %s WHERE holiday_id=$%d`,
				strings.Join(setH, ", "), p)
			argsH = append(argsH, holidayID)

			if _, err := tx.Exec(ctx, qh, argsH...); err != nil {
				holidayResults = append(holidayResults, map[string]string{
					"id": holidayID, "status": "failed", "error": err.Error(),
				})
				continue
			}

			holidayResults = append(holidayResults, map[string]string{
				"id": holidayID, "status": "updated",
			})
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO investment.auditactioncalendar
			(calendar_id, actiontype, processing_status, reason, requested_by, requested_at)
			VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())
		`, req.CalendarID, req.Reason, userEmail)
		if err != nil {
			api.RespondWithError(w, 500, "audit failed: "+err.Error())
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, 500, constants.ErrCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"calendar_id": req.CalendarID,
			"result":      holidayResults,
			"audit":       "PENDING_EDIT_APPROVAL",
		})
	}
}

func GetPastYearsHolidays(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		var req struct {
			Years  int    `json:"years"`
			UserID string `json:"user_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, 400, "invalid json")
			return
		}

		user := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				user = s.Name
				break
			}
		}
		if user == "" {
			api.RespondWithError(w, 401, "invalid session")
			return
		}

		if req.Years <= 0 {
			req.Years = 5
		}

		endYear := time.Now().Year()
		startYear := endYear - req.Years

		ctx := r.Context()

		query := `
WITH latest_audit AS (
	SELECT DISTINCT ON (calendar_id)
	       calendar_id, processing_status
	FROM investment.auditactioncalendar
	ORDER BY calendar_id, requested_at DESC
)
SELECT 
	h.holiday_id,
	h.holiday_date,
	h.holiday_name,
	h.holiday_type,
	h.recurrence_rule,
	h.notes
FROM investment.mastercalendar c
JOIN latest_audit a ON a.calendar_id = c.calendar_id
JOIN investment.masterholiday h ON h.calendar_id = c.calendar_id
WHERE 
	c.status = 'Active'
	AND UPPER(a.processing_status) = 'APPROVED'
	AND COALESCE(c.is_deleted,false) = false
	AND COALESCE(h.is_deleted,false) = false
	AND UPPER(h.status) = 'ACTIVE'
	AND EXTRACT(YEAR FROM h.holiday_date) BETWEEN $1 AND $2
ORDER BY h.holiday_date, h.holiday_name
		`

		rows, err := pgxPool.Query(ctx, query, startYear, endYear)
		if err != nil {
			api.RespondWithError(w, 500, err.Error())
			return
		}
		defer rows.Close()

		var result []map[string]interface{}

		for rows.Next() {
			var id, name, htype string
			var date time.Time
			var recRule, notes *string

			if err := rows.Scan(&id, &date, &name, &htype, &recRule, &notes); err != nil {
				api.RespondWithError(w, 500, err.Error())
				return
			}

			record := map[string]interface{}{
				"holiday_id":      id,
				"holiday_date":    date.Format(constants.DateFormat),
				"holiday_name":    name,
				"holiday_type":    htype,
				"recurrence_rule": coalesceStrPtr(recRule),
				"notes":           coalesceStrPtr(notes),
			}

			result = append(result, record)
		}

		api.RespondWithPayload(w, true, "", result)
	}
}

func coalesceStrPtr(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
