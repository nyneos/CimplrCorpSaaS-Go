package fdAccrual

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/fdMaster"
	"CimplrCorpSaas/api/investment/rounding"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Structs ──────────────────────────────────────────────────────────────────

// AccrualInput holds every field the engine needs for a single FD.
type AccrualInput struct {
	FDID             string
	FdRefNo          string // bank_fd_ref_no
	BankID           string
	BankName         string
	EntityID         string
	EntityName       string
	InterestTypeCode string // SIMPLE / COMPOUND / STEPPED
	PrincipalAmount  float64
	InterestRate     float64 // annual percentage e.g. 7.5
	DayCountCode     string  // ACT_365 / ACT_360 / 30_360
	FdStartDate      time.Time
	FdMaturityDate   time.Time
	// FD lifecycle state — used to clamp accrual at premature/maturity closure
	FdStatus           string    // ACTIVE / MATURED / PREMATURELY_CLOSED / ROLLED_OVER
	EffectiveCloseDate time.Time // closure date (premature) — interest stops here
	// Bank config fields for holiday-aware accrual day counting
	BankConfigID        string
	HolidayCalendarCode string
	WeekendAccrual      bool   // if false, exclude weekends from day count
	HolidayAccrual      bool   // if false, exclude holidays from day count
	WeekendPattern      string // e.g. "Sat,Sun"
	// Bank config: boundary and rounding conventions (STEP 3)
	AccrualStartConvention string // INCLUDE / EXCLUDE / NEXT_WD
	AccrualEndConvention   string // INCLUDE / EXCLUDE / PRECEDING_WD
	BankRoundingMethod     string // ROUND / FLOOR / CEIL (bank config level)
	BankRoundingFrequency  string // EACH_PERIOD / FINAL_ONLY
	BankRoundingDecimals   int    // interest_rounding_decimals from bank config
	CapDateAdjustment      string // PRECEDING_WD / FOLLOWING_WD / NO_ADJUST
	BrokenPeriodMethod     string // SIMPLE / COMPOUND / HYBRID / NONE
	// Compounding frequency (from fd_master.frequency_id → frequency_master)
	FrequencyID          string // e.g. "FREQ-QUARTERLY"
	CompoundingFrequency string // e.g. "QUARTERLY", "MONTHLY", "ANNUALLY", "DAILY", "AT_MATURITY"
}

// AccrualRunParams controls what the engine calculates.
type AccrualRunParams struct {
	PeriodStart        time.Time
	PeriodEnd          time.Time
	FinancialPeriod    string
	DayCountConvention string // overrides per-FD code if set
	RoundingRule       string // ROUND / TRUNCATE
	PrecisionDecimals  int
	EntityID           string // for scope
	BankIDFilter       string // optional
	FDStatusFilter     string // ACTIVE default
	FDInclusionMethod  string // ALL / SELECT_LIST
	FDInclusionList    []string
	Granularity        string // DAILY / MONTHLY / QUARTERLY / RUN
}

// AccrualPeriodResult holds the engine output for one FD x one period.
// It carries two parallel calculation views:
//   - The standard (config-authoritative) fields: bank config day count, holiday-aware days,
//     bank rounding rule — these are written to the main ledger columns.
//   - The Req* fields: calculated using run-level request params only (entity baseline,
//     no holiday exclusions, run rounding rule). Stored in req_* ledger columns.
type AccrualPeriodResult struct {
	FDID             string
	FdRefNo          string
	BankID           string
	BankName         string
	EntityID         string
	EntityName       string
	InterestTypeCode string
	PrincipalAmount  float64
	InterestRate     float64
	DayCountCode     string // config path day count code
	FdStartDate      time.Time
	FdMaturityDate   time.Time

	// Period
	AccrualPeriodStart time.Time
	AccrualPeriodEnd   time.Time
	AccrualDays        int

	// Config-path calculation inputs (bank-authoritative)
	OpeningPrincipal float64 // compound principal from cashflow schedule (COMPOUND FDs)
	DailyAccrualRate float64
	Divisor          int

	// Config-path outputs
	PeriodInterestAccrued    float64
	OpeningAccruedBalance    float64
	InterestReceivedInPeriod float64
	ClosingAccruedBalance    float64
	TDSApplicableAmount      float64
	TDSDeductedInPeriod      float64
	NetInterestInPeriod      float64

	// Config-path metadata (stored on ledger for audit)
	ConfigWeekendAccrual    bool
	ConfigHolidayAccrual    bool
	ConfigHolidaysExcluded  int    // days skipped due to bank holiday calendar
	ConfigRoundingRule      string // bank rounding method
	ConfigPrecisionDecimals int    // bank interest_rounding_decimals
	ConfigCapDateAdjustment string // PRECEDING_WD / FOLLOWING_WD / NO_ADJUST
	ConfigHolidayCalendar   string // calendar code used

	// Req-path: calculation using run-level request parameters only.
	// All calendar days counted (no holiday/weekend exclusion).
	// Opening principal always = fd.PrincipalAmount (no compounding lookup).
	ReqDayCountCode      string
	ReqDivisor           int
	ReqAccrualDays       int // raw calendar days (effectiveEnd - effectiveStart)
	ReqOpeningPrincipal  float64
	ReqDailyAccrualRate  float64
	ReqPeriodInterest    float64
	ReqClosingBalance    float64
	ReqTDSDeducted       float64
	ReqNetInterest       float64
	ReqFormulaUsed       string
	ReqRoundingRule      string
	ReqPrecisionDecimals int
	ReqWeekendAccrual    bool // always true
	ReqHolidayAccrual    bool // always true

	// Compounding metadata (for BRD IA-05 Section C)
	FrequencyID            string  // from fd_master.frequency_id
	CompoundingFrequency   string  // human-readable e.g. "QUARTERLY"
	CompoundPrincipalUsed  float64 // the opening_principal including past capitalizations
	NextCapitalizationDate string  // next cashflow CAPITALIZATION event after period_end

	// References
	CashflowRowIDs   []string // cashflow_ids used in this calc
	FormulaUsed      string   // config-path formula string
	LedgerRowStatus  string   // CALCULATED / ERROR / EXCLUDED
	CalculationError string
}

// ValidationFinding is a single validation issue.
type ValidationFinding struct {
	FDID            string
	FdRefNo         string
	BankName        string
	IssueType       string // MISSING_RATE / ZERO_PRINCIPAL / etc.
	Severity        string // BLOCKER / WARNING / INFO
	Description     string
	SuggestedAction string
}

// CreateAccrualRunInput is used by the internal scheduler to create a run.
type CreateAccrualRunInput struct {
	RunType            string
	RunMode            string
	EntityID           string
	EntityName         string
	Granularity        string // DAILY / MONTHLY / QUARTERLY / RUN
	BankIDFilter       string
	FDStatusFilter     string
	AccrualPeriodStart time.Time
	AccrualPeriodEnd   time.Time
	FinancialPeriod    string
	DayCountConvention string
	RoundingRule       string
	PrecisionDecimals  int
	FDInclusionMethod  string
	CreatedBy          string
}

// ScheduleConfigRow holds a row from fd_accrual_schedule_config.
type ScheduleConfigRow struct {
	ConfigID               string
	EntityID               string
	EntityName             string
	ScheduleFrequency      string
	RunDayOfMonth          *int
	RunTime                *time.Time
	DefaultBankIDFilter    string
	DefaultFDStatusFilter  string
	DefaultRunMode         string
	AutoSubmitForApproval  bool
	NotificationRecipients []byte // JSONB raw
}

// ─── Day-count helpers ────────────────────────────────────────────────────────

func normalizeAccrualDayCountCode(dayCountCode string) string {
	norm := strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(strings.TrimSpace(dayCountCode)))
	norm = strings.TrimPrefix(norm, "DC_")
	switch {
	case strings.Contains(norm, "30") && strings.Contains(norm, "360"):
		return "30_360"
	case strings.Contains(norm, "ACT") && strings.Contains(norm, "360") && !strings.Contains(norm, "365"):
		return "ACT_360"
	case norm == "ACT_ACT" || (strings.Contains(norm, "ACT") && strings.Contains(norm, "ACTUAL") && !strings.Contains(norm, "360") && !strings.Contains(norm, "365")):
		return "ACT_ACT"
	case strings.Contains(norm, "365"):
		return "ACT_365"
	default:
		return "ACT_365"
	}
}

func accrualIsLeapYear(year int) bool {
	return year%400 == 0 || (year%4 == 0 && year%100 != 0)
}

func accrualDaysInMonth(year int, month time.Month) int {
	switch month {
	case time.January, time.March, time.May, time.July, time.August, time.October, time.December:
		return 31
	case time.April, time.June, time.September, time.November:
		return 30
	case time.February:
		if accrualIsLeapYear(year) {
			return 29
		}
		return 28
	default:
		return 0
	}
}

func accrualCountDays30by360(start, end time.Time) int {
	y1, m1, d1 := start.Year(), start.Month(), start.Day()
	y2, m2, d2 := end.Year(), end.Month(), end.Day()
	if m1 == time.February && d1 == accrualDaysInMonth(y1, m1) {
		d1 = 30
	}
	if d1 == 31 {
		d1 = 30
	}
	if d2 == 31 && d1 >= 30 {
		d2 = 30
	}
	return (y2-y1)*360 + (int(m2)-int(m1))*30 + (d2 - d1)
}

// getDivisorForAccrual returns the annual divisor from a day count code.
// For ACT_ACT it uses the reference date's year, matching the cashflow workbook rule.
// Never returns 0.
func getDivisorForAccrual(dayCountCode string, refDate time.Time) int {
	switch normalizeAccrualDayCountCode(dayCountCode) {
	case "ACT_365":
		return 365
	case "ACT_360":
		return 360
	case "30_360":
		return 360
	case "ACT_ACT":
		if accrualIsLeapYear(refDate.Year()) {
			return 366
		}
		return 365
	default:
		return 365
	}
}

// getDivisorForPeriod is the period-aware version of getDivisorForAccrual.
// For ACT_ACT it follows the cashflow workbook rule: end-date year decides 365/366.
func getDivisorForPeriod(dayCountCode string, start, end time.Time) int {
	switch normalizeAccrualDayCountCode(dayCountCode) {
	case "ACT_365":
		return 365
	case "ACT_360":
		return 360
	case "30_360":
		return 360
	case "ACT_ACT":
		if accrualIsLeapYear(end.Year()) {
			return 366
		}
		return 365
	default:
		return 365
	}
}

func getDivisorAndDaysForAccrualPeriod(dayCountCode string, start, end time.Time, fd AccrualInput, cal accrualHolidayCalendar, holidayAware bool) (int, int) {
	normalized := normalizeAccrualDayCountCode(dayCountCode)
	if normalized == "30_360" {
		return 360, accrualCountDays30by360(start, end)
	}
	divisor := getDivisorForPeriod(normalized, start, end)
	if holidayAware {
		return divisor, accrualCountWorkingDays(start, end, fd, cal)
	}
	return divisor, int(end.Sub(start).Hours() / 24)
}

// buildAccrualPeriod returns "APR 2025" uppercase month-year string.
func buildAccrualPeriod(t time.Time) string {
	return strings.ToUpper(t.Format("Jan 2006"))
}

// ─── Scope query ──────────────────────────────────────────────────────────────

// getFDsInScope queries investment.fd_master using exact column names.
//
// Scope rules (TC-27/28/29/30/31/32):
//   - ACTIVE means active FDs only.
//   - ACTIVE_MATURED means active FDs plus all completed FD outcomes
//     (matured payout, premature closure, rollover) that overlap the run period.
//   - ALL includes every accrual-capable status except pending/cancelled activation.
//
// FDs that were closed/matured before the run period start are filtered out
// because they have no overlap window.
func getFDsInScope(ctx context.Context, pool *pgxpool.Pool, params AccrualRunParams) ([]AccrualInput, error) {
	fdStatus := strings.ToUpper(strings.TrimSpace(params.FDStatusFilter))
	if fdStatus == "" {
		fdStatus = "ACTIVE"
	}

	// Build the fd_status IN (...) filter. Keep this aligned with the user's
	// selected scope; validation should not pull closed FDs into an ACTIVE run.
	var statusValues []string
	switch fdStatus {
	case "ACTIVE":
		statusValues = []string{"ACTIVE"}
	case "ACTIVE_MATURED":
		statusValues = []string{"ACTIVE", "MATURED", "PREMATURELY_CLOSED", "ROLLED_OVER"}
	case "ALL":
		statusValues = []string{"ACTIVE", "MATURED", "PREMATURELY_CLOSED", "ROLLED_OVER"}
	default:
		statusValues = []string{fdStatus}
	}

	query := `
		SELECT
			f.fd_id,
			COALESCE(f.bank_fd_ref_no, '')                        AS fd_ref_no,
			COALESCE(f.bank_id, '')                               AS bank_id,
			COALESCE(f.bank_name, '')                             AS bank_name,
			COALESCE(f.entity_id, '')                             AS entity_id,
			COALESCE(f.entity_name, '')                           AS entity_name,
			COALESCE(f.interest_type_code, 'SIMPLE')              AS interest_type_code,
			COALESCE(f.principal_amount, 0)                       AS principal_amount,
			COALESCE(f.interest_rate, 0)                          AS interest_rate,
			COALESCE(f.day_count_code, 'ACT_365')                 AS day_count_code,
			f.start_date,
			f.maturity_date,
			COALESCE(f.fd_status, 'ACTIVE')                       AS fd_status,
			-- Effective closure date (premature) — bounds the accrual end
			COALESCE(cr.effective_closure_date, f.closed_at::date) AS effective_close_date,
			COALESCE(f.bank_config_id, '')                        AS bank_config_id,
			COALESCE(bc.holiday_calendar_code, '')                AS holiday_calendar_code,
			COALESCE(bc.weekend_accrual, true)                    AS weekend_accrual,
			COALESCE(bc.holiday_accrual, true)                    AS holiday_accrual,
			COALESCE(mc.weekend_pattern, 'Sat,Sun')               AS weekend_pattern,
			COALESCE(bc.accrual_start_convention, 'INCLUDE')      AS accrual_start_convention,
			COALESCE(bc.accrual_end_convention,   'EXCLUDE')      AS accrual_end_convention,
			COALESCE(bc.rounding_method,          'ROUND')        AS bank_rounding_method,
			COALESCE(bc.rounding_frequency,       'EACH_PERIOD')  AS bank_rounding_frequency,
			COALESCE(bc.interest_rounding_decimals, 2)            AS bank_rounding_decimals,
			COALESCE(bc.capitalization_date_adjustment, 'NO_ADJUST') AS cap_date_adjustment,
			COALESCE(bc.broken_period_method,     'SIMPLE')       AS broken_period_method,
			COALESCE(f.frequency_id, '')                          AS frequency_id,
			COALESCE(f.frequency_id, '')                          AS compounding_frequency
		FROM investment.fd_master f
			LEFT JOIN investment.fd_bank_config_master bc ON bc.config_id = f.bank_config_id
				AND COALESCE(bc.is_deleted, false) = false
			LEFT JOIN investment.mastercalendar mc ON mc.calendar_code = bc.holiday_calendar_code
				AND COALESCE(mc.is_deleted, false) = false
			LEFT JOIN investment.fd_closure_request cr ON cr.closure_request_id = f.closure_request_id
				AND cr.closure_status = 'POSTED'
		WHERE f.entity_id = $1
		  AND f.fd_status = ANY($2)
		  AND f.cashflow_generated = true
		  AND f.is_deleted = false
		  AND f.is_active = true
		  AND f.start_date <= $3
		  AND f.maturity_date >= $4
		  AND (
		  	COALESCE(cr.effective_closure_date, f.closed_at::date) IS NULL
		  	OR COALESCE(cr.effective_closure_date, f.closed_at::date) >= $4
		  )`

	args := []interface{}{params.EntityID, statusValues, params.PeriodEnd, params.PeriodStart}
	argIdx := 5

	if params.BankIDFilter != "" {
		query += fmt.Sprintf(" AND (f.bank_id = $%d OR f.bank_name ILIKE $%d)", argIdx, argIdx)
		args = append(args, params.BankIDFilter)
		argIdx++
	}
	if params.FDInclusionMethod == "SELECT_LIST" && len(params.FDInclusionList) > 0 {
		query += fmt.Sprintf(" AND f.fd_id = ANY($%d)", argIdx)
		args = append(args, params.FDInclusionList)
		argIdx++
	}
	query += " ORDER BY f.bank_name, f.start_date"

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFDsInScope: %w", err)
	}
	defer rows.Close()

	var results []AccrualInput
	for rows.Next() {
		var fd AccrualInput
		var effectiveCloseDate sql.NullTime
		if err := rows.Scan(
			&fd.FDID, &fd.FdRefNo, &fd.BankID, &fd.BankName,
			&fd.EntityID, &fd.EntityName, &fd.InterestTypeCode,
			&fd.PrincipalAmount, &fd.InterestRate, &fd.DayCountCode,
			&fd.FdStartDate, &fd.FdMaturityDate,
			&fd.FdStatus, &effectiveCloseDate,
			&fd.BankConfigID, &fd.HolidayCalendarCode,
			&fd.WeekendAccrual, &fd.HolidayAccrual, &fd.WeekendPattern,
			&fd.AccrualStartConvention, &fd.AccrualEndConvention,
			&fd.BankRoundingMethod, &fd.BankRoundingFrequency,
			&fd.BankRoundingDecimals, &fd.CapDateAdjustment,
			&fd.BrokenPeriodMethod,
			&fd.FrequencyID, &fd.CompoundingFrequency,
		); err != nil {
			return nil, fmt.Errorf("getFDsInScope scan: %w", err)
		}
		if effectiveCloseDate.Valid {
			fd.EffectiveCloseDate = effectiveCloseDate.Time
		}
		results = append(results, fd)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getFDsInScope rows.Err: %w", err)
	}
	if len(results) == 0 {
		api.LogInfo("[FDAccrual] getFDsInScope: 0 FDs matched scope (entity=%s status=%s period=%s→%s)",
			params.EntityID, params.FDStatusFilter,
			params.PeriodStart.Format(constants.DateFormat), params.PeriodEnd.Format(constants.DateFormat))
	}
	return results, nil
}

// ─── Holiday calendar helpers ────────────────────────────────────────────────

// accrualHolidayCalendar holds expanded holiday dates for accrual engine use.
type accrualHolidayCalendar struct {
	CalendarCode   string
	WeekendPattern string          // e.g. "Sat,Sun"
	HolidayDates   map[string]bool // keyed "YYYY-MM-DD"
}

// accrualIsWeekend checks whether d falls on a weekend day listed in weekendPattern.
func accrualIsWeekend(d time.Time, weekendPattern string) bool {
	if weekendPattern == "" {
		return false
	}
	dow := strings.ToLower(d.Weekday().String()[:3]) // "mon","tue",...
	for _, part := range strings.Split(weekendPattern, ",") {
		if strings.ToLower(strings.TrimSpace(part))[:3] == dow {
			return true
		}
	}
	return false
}

// reByDay matches BYDAY values like "MO", "2MO", "-1FR"
var reByDay = regexp.MustCompile(`^(-?\d*)([A-Z]{2})$`)

// accrualExpandRRule expands a recurrence rule string into concrete dates in [from, to].
// Supported: FREQ=YEARLY/MONTHLY/WEEKLY/DAILY with optional BYMONTH and BYDAY.
// Empty rrule returns just the seed date (if in range).
func accrualExpandRRule(seed time.Time, rrule string, from, to time.Time) []time.Time {
	var out []time.Time
	if rrule == "" {
		if !seed.Before(from) && !seed.After(to) {
			out = append(out, seed.Truncate(24*time.Hour))
		}
		return out
	}

	params := map[string]string{}
	for _, tok := range strings.Split(rrule, ";") {
		kv := strings.SplitN(tok, "=", 2)
		if len(kv) == 2 {
			params[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	freq := params["FREQ"]
	byMonth := 0
	if bm := params["BYMONTH"]; bm != "" {
		byMonth, _ = strconv.Atoi(bm)
	}
	byDay := params["BYDAY"] // e.g. "MO" or "2MO" or "-1FR"

	addIfInRange := func(d time.Time) {
		d = d.Truncate(24 * time.Hour)
		if !d.Before(from) && !d.After(to) {
			out = append(out, d)
		}
	}

	// Helper: nth weekday of a month (n=1 → first, n=-1 → last)
	nthWeekdayOfMonth := func(year, month int, wd time.Weekday, n int) time.Time {
		m := time.Month(month)
		if n > 0 {
			first := time.Date(year, m, 1, 0, 0, 0, 0, time.UTC)
			diff := int(wd) - int(first.Weekday())
			if diff < 0 {
				diff += 7
			}
			return first.AddDate(0, 0, diff+(n-1)*7)
		}
		// negative: count from end
		last := time.Date(year, m+1, 0, 0, 0, 0, 0, time.UTC)
		diff := int(last.Weekday()) - int(wd)
		if diff < 0 {
			diff += 7
		}
		return last.AddDate(0, 0, -diff+(n+1)*7)
	}

	weekdayFromCode := func(code string) time.Weekday {
		switch strings.ToUpper(code) {
		case "MO":
			return time.Monday
		case "TU":
			return time.Tuesday
		case "WE":
			return time.Wednesday
		case "TH":
			return time.Thursday
		case "FR":
			return time.Friday
		case "SA":
			return time.Saturday
		default:
			return time.Sunday
		}
	}

	switch strings.ToUpper(freq) {
	case "YEARLY":
		for yr := from.Year() - 1; yr <= to.Year()+1; yr++ {
			if byDay != "" && byMonth != 0 {
				// e.g. FREQ=YEARLY;BYMONTH=1;BYDAY=3MO → 3rd Monday of January each year
				m := reByDay.FindStringSubmatch(byDay)
				if len(m) == 3 {
					n := 1
					if m[1] != "" {
						n, _ = strconv.Atoi(m[1])
					}
					wd := weekdayFromCode(m[2])
					addIfInRange(nthWeekdayOfMonth(yr, byMonth, wd, n))
				}
			} else if byMonth != 0 {
				// FREQ=YEARLY;BYMONTH=8 → anniversary in that month
				addIfInRange(time.Date(yr, time.Month(byMonth), seed.Day(), 0, 0, 0, 0, time.UTC))
			} else if byDay != "" {
				// FREQ=YEARLY;BYDAY=MO → first Monday of year
				m := reByDay.FindStringSubmatch(byDay)
				if len(m) == 3 {
					n := 1
					if m[1] != "" {
						n, _ = strconv.Atoi(m[1])
					}
					wd := weekdayFromCode(m[2])
					addIfInRange(nthWeekdayOfMonth(yr, int(seed.Month()), wd, n))
				}
			} else {
				// plain FREQ=YEARLY → same date every year
				addIfInRange(time.Date(yr, seed.Month(), seed.Day(), 0, 0, 0, 0, time.UTC))
			}
		}
	case "MONTHLY":
		cur := time.Date(from.Year(), from.Month()-1, 1, 0, 0, 0, 0, time.UTC)
		for !cur.After(to.AddDate(0, 1, 0)) {
			yr, mo := cur.Year(), int(cur.Month())
			if byDay != "" {
				m := reByDay.FindStringSubmatch(byDay)
				if len(m) == 3 {
					n := 1
					if m[1] != "" {
						n, _ = strconv.Atoi(m[1])
					}
					wd := weekdayFromCode(m[2])
					addIfInRange(nthWeekdayOfMonth(yr, mo, wd, n))
				}
			} else {
				addIfInRange(time.Date(yr, time.Month(mo), seed.Day(), 0, 0, 0, 0, time.UTC))
			}
			cur = cur.AddDate(0, 1, 0)
		}
	case "WEEKLY":
		var targetWD time.Weekday
		if byDay != "" {
			m := reByDay.FindStringSubmatch(byDay)
			if len(m) == 3 {
				targetWD = weekdayFromCode(m[2])
			}
		} else {
			targetWD = seed.Weekday()
		}
		cur := from.AddDate(-1, 0, 0)
		for !cur.After(to) {
			if cur.Weekday() == targetWD {
				addIfInRange(cur)
			}
			cur = cur.AddDate(0, 0, 1)
		}
	case "DAILY":
		cur := from
		for !cur.After(to) {
			addIfInRange(cur)
			cur = cur.AddDate(0, 0, 1)
		}
	}
	return out
}

// loadHolidayCalendarForAccrual fetches and expands a named calendar for accrual use.
// calCode="" → returns empty calendar (all days count).
func loadHolidayCalendarForAccrual(ctx context.Context, pool *pgxpool.Pool,
	calCode string, from, to time.Time) accrualHolidayCalendar {

	cal := accrualHolidayCalendar{
		CalendarCode: calCode,
		HolidayDates: map[string]bool{},
	}
	if calCode == "" {
		return cal
	}

	var calendarID string
	err := pool.QueryRow(ctx, `
		SELECT calendar_id, COALESCE(weekend_pattern, 'Sat,Sun')
		FROM investment.mastercalendar
		WHERE calendar_code = $1
		  AND COALESCE(status, 'ACTIVE') = 'ACTIVE'
		  AND COALESCE(is_deleted, false) = false
		LIMIT 1`, calCode,
	).Scan(&calendarID, &cal.WeekendPattern)
	if err != nil {
		return cal
	}

	// expand window slightly so YEARLY rrules near boundaries are caught
	expandFrom := from.AddDate(-1, 0, 0)
	expandTo := to.AddDate(1, 0, 0)

	rows, err := pool.Query(ctx, `
		SELECT COALESCE(holiday_date::text, ''), COALESCE(recurrence_rule, '')
		FROM investment.masterholiday
		WHERE calendar_id = $1
		  AND COALESCE(status, 'ACTIVE') = 'ACTIVE'
		  AND COALESCE(is_deleted, false) = false`, calendarID,
	)
	if err != nil {
		return cal
	}
	defer rows.Close()

	for rows.Next() {
		var dateStr, rrule string
		if err := rows.Scan(&dateStr, &rrule); err != nil {
			continue
		}
		seed, err := time.Parse(constants.DateFormat, dateStr[:10])
		if err != nil {
			continue
		}
		for _, d := range accrualExpandRRule(seed, rrule, expandFrom, expandTo) {
			cal.HolidayDates[d.Format(constants.DateFormat)] = true
		}
	}
	return cal
}

// accrualIsNonAccrualDay returns true if d should be excluded from day-count
// based on bank config flags and the expanded calendar.
func accrualIsNonAccrualDay(d time.Time, fd AccrualInput, cal accrualHolidayCalendar) bool {
	if !fd.WeekendAccrual && accrualIsWeekend(d, fd.WeekendPattern) {
		return true
	}
	if !fd.HolidayAccrual && cal.HolidayDates[d.Format(constants.DateFormat)] {
		return true
	}
	return false
}

// accrualCountWorkingDays counts days in [start, end) excluding non-accrual days.
func accrualCountWorkingDays(start, end time.Time, fd AccrualInput, cal accrualHolidayCalendar) int {
	// If both accrual flags are true (default), all calendar days count
	if fd.WeekendAccrual && fd.HolidayAccrual {
		return int(end.Sub(start).Hours() / 24)
	}
	days := 0
	cur := start.Truncate(24 * time.Hour)
	e := end.Truncate(24 * time.Hour)
	for cur.Before(e) {
		if !accrualIsNonAccrualDay(cur, fd, cal) {
			days++
		}
		cur = cur.AddDate(0, 0, 1)
	}
	return days
}

// ─── Validation ───────────────────────────────────────────────────────────────

// validateFDsForAccrual checks each FD for blockers and warnings.
func validateFDsForAccrual(fds []AccrualInput, params AccrualRunParams) []ValidationFinding {
	var findings []ValidationFinding
	for _, fd := range fds {
		if fd.PrincipalAmount <= 0 {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType:       "ZERO_PRINCIPAL",
				Severity:        "BLOCKER",
				Description:     "Principal amount is zero or negative",
				SuggestedAction: "Update FD principal before running accrual",
			})
		}
		if fd.InterestRate <= 0 {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType:       "MISSING_RATE",
				Severity:        "BLOCKER",
				Description:     "Interest rate is zero or negative",
				SuggestedAction: "Set a positive interest rate on the FD",
			})
		}
		if fd.DayCountCode == "" {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType:       "MISSING_DAY_COUNT",
				Severity:        "BLOCKER",
				Description:     "Day count code is missing",
				SuggestedAction: "Set day_count_code on the FD (ACT_365 / ACT_360 / 30_360)",
			})
		}
		if !fd.FdMaturityDate.IsZero() && fd.FdMaturityDate.Before(params.PeriodStart) {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType: "MATURITY_BEFORE_PERIOD",
				Severity:  "BLOCKER",
				Description: fmt.Sprintf("FD matured on %s before period start %s",
					fd.FdMaturityDate.Format(constants.DateFormat), params.PeriodStart.Format(constants.DateFormat)),
				SuggestedAction: "Exclude this FD from the run or verify maturity date",
			})
		}
		if !fd.FdStartDate.IsZero() && fd.FdStartDate.After(params.PeriodEnd) {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType: "FD_NOT_STARTED_IN_PERIOD",
				Severity:  "BLOCKER",
				Description: fmt.Sprintf("FD start date %s is after period end %s",
					fd.FdStartDate.Format(constants.DateFormat), params.PeriodEnd.Format(constants.DateFormat)),
				SuggestedAction: "Exclude this FD or adjust the accrual period",
			})
		}
		// TC-27/28: Partial-period notice when FD starts mid-period — informational
		// so the user understands days_in_period < period_length.
		if !fd.FdStartDate.IsZero() && fd.FdStartDate.After(params.PeriodStart) && !fd.FdStartDate.After(params.PeriodEnd) {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType: "FD_OPENED_WITHIN_PERIOD",
				Severity:  "WARNING",
				Description: fmt.Sprintf("FD started %s — accrual will run from %s to period end (partial accrual)",
					fd.FdStartDate.Format(constants.DateFormat), fd.FdStartDate.Format(constants.DateFormat)),
				SuggestedAction: "No action needed — engine accrues only valid days from FD start.",
			})
		}
		// TC-29/30: FD closed pre-maturely within the period — interest stops at closure.
		if !fd.EffectiveCloseDate.IsZero() && !fd.EffectiveCloseDate.Before(params.PeriodStart) && fd.EffectiveCloseDate.Before(params.PeriodEnd) {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType: "FD_CLOSED_WITHIN_PERIOD",
				Severity:  "WARNING",
				Description: fmt.Sprintf("FD closed pre-maturely on %s — accrual will stop at closure (no interest after closure)",
					fd.EffectiveCloseDate.Format(constants.DateFormat)),
				SuggestedAction: "No action needed — engine clamps accrual at the closure date.",
			})
		}
		// TC-31/32: FD matures within the period — interest till maturity only.
		if !fd.FdMaturityDate.IsZero() && !fd.FdMaturityDate.Before(params.PeriodStart) && fd.FdMaturityDate.Before(params.PeriodEnd) {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType: "FD_MATURES_WITHIN_PERIOD",
				Severity:  "WARNING",
				Description: fmt.Sprintf("FD matures on %s — accrual will stop at maturity (no interest after maturity)",
					fd.FdMaturityDate.Format(constants.DateFormat)),
				SuggestedAction: "No action needed — engine clamps accrual at the maturity date.",
			})
		}
		// TC-38: FD eligibility — closed/matured before this period should never reach
		// the engine, but if the FD master and closure tables are inconsistent (race),
		// flag it loudly.
		closedStatuses := map[string]bool{"PREMATURELY_CLOSED": true, "MATURED": true, "ROLLED_OVER": true}
		if closedStatuses[strings.ToUpper(fd.FdStatus)] {
			closeOrMaturity := fd.FdMaturityDate
			if !fd.EffectiveCloseDate.IsZero() && fd.EffectiveCloseDate.Before(closeOrMaturity) {
				closeOrMaturity = fd.EffectiveCloseDate
			}
			if !closeOrMaturity.IsZero() && closeOrMaturity.Before(params.PeriodStart) {
				findings = append(findings, ValidationFinding{
					FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
					IssueType: "FD_NOT_ELIGIBLE",
					Severity:  "BLOCKER",
					Description: fmt.Sprintf("FD is %s and effectively ended on %s — not eligible for accrual in this period",
						fd.FdStatus, closeOrMaturity.Format(constants.DateFormat)),
					SuggestedAction: "Exclude this FD from the run; closed FDs are not eligible.",
				})
			}
		}
	}
	return findings
}

// hasBlockers returns true if any finding has severity BLOCKER.
func hasBlockers(findings []ValidationFinding) bool {
	for _, f := range findings {
		if f.Severity == "BLOCKER" {
			return true
		}
	}
	return false
}

// accrualPeriodGranularities are valid accrual_granularity / run_type values.
var accrualPeriodGranularities = map[string]bool{
	"DAILY": true, "MONTHLY": true, "QUARTERLY": true,
	"HALF_YEARLY": true, "YEARLY": true, "RUN": true,
}

// scheduleFrequencies are valid schedule_frequency values (when the job fires).
var scheduleFrequencies = map[string]bool{
	"DAILY": true, "MONTHLY": true, "QUARTERLY": true,
	"HALF_YEARLY": true, "YEARLY": true,
}

// periodCoverages are valid period_coverage values (the date window each run covers).
// RUN = inherit from schedule_frequency (legacy behaviour).
var periodCoverages = map[string]bool{
	"DAILY": true, "MONTHLY": true, "QUARTERLY": true,
	"HALF_YEARLY": true, "YEARLY": true, "RUN": true,
}

func isValidPeriodCoverage(p string) bool {
	return periodCoverages[strings.ToUpper(strings.TrimSpace(p))]
}

// accrualRunApprovalListStatuses — /run/all shows only submitted approval-queue runs
// (excludes DRAFT, VALIDATED, COMPUTED, IN_PROGRESS, VALIDATION_FAILED, FAILED, etc.).
var accrualRunApprovalListStatuses = map[string]bool{
	"PENDING_APPROVAL": true,
	"APPROVED":         true,
	"REJECTED":         true,
	"POSTED":           true,
	"POSTED_TO_GL":     true,
	"LOCKED":           true,
}

func isAccrualRunApprovalListStatus(status string) bool {
	return accrualRunApprovalListStatuses[strings.ToUpper(strings.TrimSpace(status))]
}

func normalizeAccrualGranularity(g string) string {
	return strings.ToUpper(strings.TrimSpace(g))
}

func normalizeScheduleFrequency(f string) string {
	return strings.ToUpper(strings.TrimSpace(f))
}

func isValidAccrualGranularity(g string) bool {
	return accrualPeriodGranularities[normalizeAccrualGranularity(g)]
}

func isValidScheduleFrequency(f string) bool {
	return scheduleFrequencies[normalizeScheduleFrequency(f)]
}

// AccrualPeriodBounds returns inclusive calendar start/end for the accrual window
// that contains `at`, based on accrual_granularity (not schedule_frequency).
//
// CURRENT BEHAVIOUR (calendar-month):
//
//	 MONTHLY → 2025-05-01 to 2025-05-31  (always 1st → last day of month)
//
//	— ALTERNATIVE: FD-date-based periods ("24 to 24")
//
// If you want the period to run from one FD-anniversary-day to the next
// (e.g. Apr 24 → May 23, so the scheduler fires ON May 24 and covers the
// preceding month), this function needs a second parameter: anchorDay int.
// Change the signature to:
//
//	func AccrualPeriodBounds(at time.Time, granularity string, anchorDay ...int) (time.Time, time.Time)
//
// Then in the MONTHLY case replace the two lines with:
//
//	 — FD-date period: fire date IS the period end + 1 (anniversary)
//	day := at.Day()                                          // e.g. 24 (run_day_of_month)
//	if len(anchorDay) > 0 && anchorDay[0] >= 1 {
//	    day = anchorDay[0]
//	}
//	periodEnd   := time.Date(at.Year(), at.Month(), day, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1) // May 23
//	periodStart := time.Date(at.Year(), at.Month()-1, day, 0, 0, 0, 0, time.UTC)                 // Apr 24
//	return periodStart, periodEnd
//
// NOTE: at.Month()-1 wraps correctly via Go's time.Date overflow (e.g. January-1 = December).
//
// You ALSO need to update the call site in fireScheduledRun (scheduler.go) to pass p.RunDay:
//
//	periodStart, periodEnd := AccrualPeriodBounds(scheduledAt, periodGranularity, p.RunDay)
//
// The manual-run API (run.go CreateAccrualRun) already accepts explicit accrual_period_start /
// accrual_period_end from the caller — no change needed there.
func AccrualPeriodBounds(at time.Time, granularity string) (time.Time, time.Time) {
	at = at.UTC()
	switch normalizeAccrualGranularity(granularity) {
	case "DAILY":
		d := time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, time.UTC)
		return d, d
	case "QUARTERLY":
		qMonth := time.Month(((int(at.Month())-1)/3)*3 + 1)
		start := time.Date(at.Year(), qMonth, 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 3, -1)
	case "HALF_YEARLY":
		if at.Month() <= 6 {
			return time.Date(at.Year(), time.January, 1, 0, 0, 0, 0, time.UTC),
				time.Date(at.Year(), time.June, 30, 0, 0, 0, 0, time.UTC)
		}
		return time.Date(at.Year(), time.July, 1, 0, 0, 0, 0, time.UTC),
			time.Date(at.Year(), time.December, 31, 0, 0, 0, 0, time.UTC)
	case "YEARLY":
		return time.Date(at.Year(), 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(at.Year(), 12, 31, 0, 0, 0, 0, time.UTC)
	default: // MONTHLY and unknown
		//  — CURRENT: calendar-month window (1st → last day).
		// To switch to FD-date-based see the comment above the function.
		start := time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 1, -1)
	}
}

// RunTypeForGranularity maps accrual_granularity to fd_accrual_run.run_type.
func RunTypeForGranularity(granularity string) string {
	switch normalizeAccrualGranularity(granularity) {
	case "DAILY":
		return "DAILY"
	case "QUARTERLY":
		return "QUARTERLY"
	case "HALF_YEARLY":
		return "HALF_YEARLY"
	case "YEARLY":
		return "YEARLY"
	default:
		return "MONTHLY"
	}
}

// periodGranularityForScheduler resolves the calendar window when granularity is RUN.
func periodGranularityForScheduler(accrualGranularity, scheduleFrequency string) string {
	g := normalizeAccrualGranularity(accrualGranularity)
	if g == "" || g == "RUN" {
		if isValidScheduleFrequency(scheduleFrequency) {
			return normalizeScheduleFrequency(scheduleFrequency)
		}
		return "MONTHLY"
	}
	return g
}

// generateSubPeriods splits [start, end] (inclusive calendar dates) into consecutive
// sub-periods of the requested granularity. RUN returns the whole range as one chunk.
// Each element is [subStart, subEnd) where subEnd is exclusive.
func generateSubPeriods(start, end time.Time, granularity string) [][2]time.Time {
	var periods [][2]time.Time
	cur := start.Truncate(24 * time.Hour)
	endDay := end.Truncate(24 * time.Hour)
	// fd_accrual_run stores inclusive end dates; treat as [start, end+1day) for slicing.
	e := endDay.AddDate(0, 0, 1)
	if !cur.Before(e) {
		return periods
	}
	switch normalizeAccrualGranularity(granularity) {
	case "DAILY":
		for cur.Before(e) {
			next := cur.AddDate(0, 0, 1)
			if next.After(e) {
				next = e
			}
			periods = append(periods, [2]time.Time{cur, next})
			cur = next
		}
	case "MONTHLY":
		for cur.Before(e) {
			next := time.Date(cur.Year(), cur.Month()+1, 1, 0, 0, 0, 0, time.UTC)
			if next.After(e) {
				next = e
			}
			periods = append(periods, [2]time.Time{cur, next})
			cur = next
		}
	case "QUARTERLY":
		for cur.Before(e) {
			next := cur.AddDate(0, 3, 0)
			qMonth := ((int(next.Month())-1)/3)*3 + 1
			next = time.Date(next.Year(), time.Month(qMonth), 1, 0, 0, 0, 0, time.UTC)
			if next.After(e) || !next.After(cur) {
				next = e
			}
			periods = append(periods, [2]time.Time{cur, next})
			cur = next
		}
	case "HALF_YEARLY":
		for cur.Before(e) {
			var next time.Time
			if cur.Month() <= 6 {
				next = time.Date(cur.Year(), time.July, 1, 0, 0, 0, 0, time.UTC)
			} else {
				next = time.Date(cur.Year()+1, time.January, 1, 0, 0, 0, 0, time.UTC)
			}
			if next.After(e) || !next.After(cur) {
				next = e
			}
			periods = append(periods, [2]time.Time{cur, next})
			cur = next
		}
	case "YEARLY":
		for cur.Before(e) {
			next := time.Date(cur.Year()+1, time.January, 1, 0, 0, 0, 0, time.UTC)
			if next.After(e) {
				next = e
			}
			periods = append(periods, [2]time.Time{cur, next})
			cur = next
		}
	default: // RUN — whole period as one chunk
		periods = append(periods, [2]time.Time{cur, e})
	}
	return periods
}

// ─── DB-backed calculation helpers ───────────────────────────────────────────

// getCompoundPrincipalAtDate returns MAX(closing_principal) from CAPITALIZATION rows.
func getCompoundPrincipalAtDate(ctx context.Context, pool *pgxpool.Pool,
	fdID string, atDate time.Time) float64 {

	var val float64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(closing_principal), 0)
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_type = 'CAPITALIZATION'
		  AND event_date <= $2
		  AND COALESCE(is_deleted, false) = false`,
		fdID, atDate,
	).Scan(&val)
	return val
}

// getNextCapitalizationDate finds the first capitalization date occurring strictly after the period end.
func getNextCapitalizationDate(ctx context.Context, pool *pgxpool.Pool, fdID string, afterDate time.Time) string {
	var nextDate time.Time
	err := pool.QueryRow(ctx, `
		SELECT MIN(event_date)
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_type = 'CAPITALIZATION'
		  AND event_date > $2
		  AND COALESCE(is_deleted, false) = false`,
		fdID, afterDate,
	).Scan(&nextDate)
	if err != nil || nextDate.IsZero() {
		return ""
	}
	return nextDate.Format(constants.DateFormat)
}

func sameAccrualDate(a, b time.Time) bool {
	if a.IsZero() || b.IsZero() {
		return false
	}
	return a.Format(constants.DateFormat) == b.Format(constants.DateFormat)
}

// getCashflowDataForPeriod sums interest receipts and TDS deductions in the period.
func getCashflowDataForPeriod(ctx context.Context, pool *pgxpool.Pool,
	fdID string, periodStart, periodEnd time.Time, includePeriodEnd bool) (interestReceived float64, tdsDeducted float64, cashflowIDs []string) {

	rows, err := pool.Query(ctx, `
		SELECT cashflow_id, event_type, event_date,
		       COALESCE(net_cash_flow, 0),
		       COALESCE(tds_amount, 0),
		       COALESCE(interest_accrued, 0)
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_date >= $2
		  AND (event_date < $3 OR ($4 AND event_date = $3))
		  AND event_type IN ('INTEREST_RECEIPT', 'MATURITY', 'TDS_DEDUCTION', 'CAPITALIZATION')
		  AND COALESCE(is_deleted, false) = false`,
		fdID, periodStart, periodEnd, includePeriodEnd,
	)
	if err != nil {
		return 0, 0, nil
	}
	defer rows.Close()

	tdsByDeductionDate := map[string]float64{}
	embeddedTDSByDate := map[string]float64{}

	for rows.Next() {
		var cashflowID, eventType string
		var eventDate time.Time
		var netCash, tdsAmt, interestAccrued float64
		if err := rows.Scan(&cashflowID, &eventType, &eventDate, &netCash, &tdsAmt, &interestAccrued); err != nil {
			continue
		}
		cashflowIDs = append(cashflowIDs, cashflowID)
		if eventType == "INTEREST_RECEIPT" || eventType == "MATURITY" {
			if interestAccrued > 0 {
				interestReceived += interestAccrued
			} else {
				interestReceived += netCash + tdsAmt
			}
		}
		if eventType == "TDS_DEDUCTION" {
			tdsByDeductionDate[eventDate.Format(constants.DateFormat)] += tdsAmt
		} else if tdsAmt > 0 {
			embeddedTDSByDate[eventDate.Format(constants.DateFormat)] += tdsAmt
		}
	}
	for _, amount := range tdsByDeductionDate {
		tdsDeducted += amount
	}
	for date, amount := range embeddedTDSByDate {
		if tdsByDeductionDate[date] == 0 {
			tdsDeducted += amount
		}
	}
	return interestReceived, tdsDeducted, cashflowIDs
}

// getPriorRunClosingBalance fetches closing_accrued_balance from the most recent
// prior FINAL+POSTED ledger row for this FD.
func getPriorRunClosingBalance(ctx context.Context, pool *pgxpool.Pool,
	entityID string, fdID string, currentPeriodStart time.Time) float64 {

	var closing float64
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(l.closing_accrued_balance, 0)
		FROM investment.fd_accrual_ledger l
		JOIN investment.fd_accrual_run r ON r.run_id = l.run_id
		WHERE l.fd_id = $1
		  AND r.entity_id = $2
		  AND r.run_mode = 'FINAL'
		  AND r.run_status = 'POSTED'
		  AND r.accrual_period_end < $3
		  AND COALESCE(l.is_deleted, false) = false
		ORDER BY r.accrual_period_end DESC
		LIMIT 1`,
		fdID, entityID, currentPeriodStart,
	).Scan(&closing)
	return closing
}

// ─── Core calculation ─────────────────────────────────────────────────────────

// calculateAccrualForFD computes the accrual for a single FD over the run period.
//
// It runs the interest formula TWICE:
//
//  1. Config path (bank-authoritative): uses fd.DayCountCode, fd.BankRoundingMethod,
//     fd.BankRoundingDecimals, holiday-aware working day count, compound principal from
//     fd_cashflow_schedule for COMPOUND FDs.
//
//  2. Req path (run-level baseline): uses params.DayCountConvention, params.RoundingRule,
//     params.PrecisionDecimals, raw calendar days (no holiday exclusion),
//     always fd.PrincipalAmount (no compounding lookup).
//
// Both results are stored on AccrualPeriodResult; the config path populates the
// main fields (DayCountCode, DailyAccrualRate, PeriodInterestAccrued, FormulaUsed...)
// and the req path populates Req* fields.  The DB layer stores both in the ledger.
func calculateAccrualForFD(ctx context.Context, pool *pgxpool.Pool,
	fd AccrualInput, params AccrualRunParams, openingBalance float64) AccrualPeriodResult {

	excluded := AccrualPeriodResult{
		FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankID: fd.BankID, BankName: fd.BankName,
		EntityID: fd.EntityID, EntityName: fd.EntityName,
		InterestTypeCode: fd.InterestTypeCode,
		PrincipalAmount:  fd.PrincipalAmount, InterestRate: fd.InterestRate,
		FdStartDate: fd.FdStartDate, FdMaturityDate: fd.FdMaturityDate,
		LedgerRowStatus: "EXCLUDED",
	}

	// TC-27/28: FD Opened Within Period — start partial accrual from the later
	// of the FD start date and the run period start.
	effectiveStart := fd.FdStartDate
	if params.PeriodStart.After(effectiveStart) {
		effectiveStart = params.PeriodStart
	}

	// TC-31/32: FD Matured / TC-29/30: FD Pre-maturely Closed —
	// stop accrual at the earliest of (period end, maturity date, closure date).
	// effective_close_date is set for PREMATURELY_CLOSED FDs (and falls back to
	// closed_at::date for matured/rolled-over FDs that lack a closure request).
	effectiveEnd := fd.FdMaturityDate
	if params.PeriodEnd.Before(effectiveEnd) {
		effectiveEnd = params.PeriodEnd
	}
	if !fd.EffectiveCloseDate.IsZero() && fd.EffectiveCloseDate.Before(effectiveEnd) {
		effectiveEnd = fd.EffectiveCloseDate
	}
	// If the FD is closed/matured, never extend accrual past those dates even
	// if no explicit closure record was found.
	switch strings.ToUpper(fd.FdStatus) {
	case "MATURED", "ROLLED_OVER":
		if !fd.FdMaturityDate.IsZero() && fd.FdMaturityDate.Before(effectiveEnd) {
			effectiveEnd = fd.FdMaturityDate
		}
	case "PREMATURELY_CLOSED":
		// effective_close_date already applied above; if missing, fall back to
		// maturity to be safe and never accrue beyond it.
		if fd.EffectiveCloseDate.IsZero() && !fd.FdMaturityDate.IsZero() && fd.FdMaturityDate.Before(effectiveEnd) {
			effectiveEnd = fd.FdMaturityDate
		}
	}

	// Apply accrual start convention (STEP 3C)
	switch strings.ToUpper(fd.AccrualStartConvention) {
	case "EXCLUDE":
		// Start date itself does not accrue — shift start forward by 1 day
		effectiveStart = effectiveStart.AddDate(0, 0, 1)
	default:
		// INCLUDE or NEXT_WD → use start date as-is
	}

	// Apply accrual end convention (STEP 3C)
	switch strings.ToUpper(fd.AccrualEndConvention) {
	case "INCLUDE":
		// End date accrues — extend by 1 day so day count includes it
		effectiveEnd = effectiveEnd.AddDate(0, 0, 1)
	default:
		// EXCLUDE or PRECEDING_WD → no change (INCL_START_EXCL_END default)
	}

	// STEP 1: FD entirely outside the period window — truly exclude
	if effectiveStart.After(effectiveEnd) {
		excluded.CalculationError = fmt.Sprintf(
			"FD period [%s, %s] does not overlap accrual window [%s, %s]",
			fd.FdStartDate.Format(constants.DateFormat),
			fd.FdMaturityDate.Format(constants.DateFormat),
			params.PeriodStart.Format(constants.DateFormat),
			params.PeriodEnd.Format(constants.DateFormat),
		)
		return excluded
	}
	// Same-day (effectiveStart == effectiveEnd): 0 accrual days — return zero-interest row, not EXCLUDED
	if !effectiveStart.Before(effectiveEnd) {
		return AccrualPeriodResult{
			FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankID: fd.BankID,
			BankName: fd.BankName, EntityID: fd.EntityID, EntityName: fd.EntityName,
			InterestTypeCode: fd.InterestTypeCode,
			PrincipalAmount:  fd.PrincipalAmount, InterestRate: fd.InterestRate,
			DayCountCode: fd.DayCountCode,
			FdStartDate:  fd.FdStartDate, FdMaturityDate: fd.FdMaturityDate,
			AccrualPeriodStart: effectiveStart,
			AccrualPeriodEnd:   effectiveEnd,
			AccrualDays:        0,
			OpeningPrincipal:   fd.PrincipalAmount,
			LedgerRowStatus:    "CALCULATED",
			FormulaUsed: fmt.Sprintf(
				"0 accrual days: FD started %s on last day of period",
				fd.FdStartDate.Format(constants.DateFormat)),
		}
	}

	// ── Load holiday calendar (config path) ───────────────────────────────
	cal := loadHolidayCalendarForAccrual(ctx, pool, fd.HolidayCalendarCode, fd.FdStartDate, fd.FdMaturityDate)

	// ─────────────────────────────────────────────────────────────────────
	// CONFIG PATH — bank-authoritative calculation
	// ─────────────────────────────────────────────────────────────────────

	// Day count from bank config (fd_master / fd_bank_config_master)
	configDayCountCode := normalizeAccrualDayCountCode(fd.DayCountCode)
	if configDayCountCode == "" {
		configDayCountCode = "ACT_365"
	}
	configDivisor, configAccrualDays := getDivisorAndDaysForAccrualPeriod(
		configDayCountCode, effectiveStart, effectiveEnd, fd, cal, true)

	// Count excluded days for reporting
	rawCalDays := int(effectiveEnd.Sub(effectiveStart).Hours() / 24)
	configHolidaysExcluded := rawCalDays - configAccrualDays
	if configDayCountCode == "30_360" {
		configHolidaysExcluded = 0
	}
	if configHolidaysExcluded < 0 {
		configHolidaysExcluded = 0
	}

	if configAccrualDays <= 0 {
		excluded.CalculationError = "config path: accrual days <= 0 after holiday/weekend exclusion"
		return excluded
	}

	// Opening principal: for COMPOUND FDs use closing_principal from last CAPITALIZATION event
	configOpeningPrincipal := fd.PrincipalAmount
	if strings.EqualFold(fd.InterestTypeCode, "COMPOUND") {
		if cp := getCompoundPrincipalAtDate(ctx, pool, fd.FDID, effectiveStart); cp > 0 {
			configOpeningPrincipal = cp
		}
	}

	// Bank rounding: bank config decimals take precedence, then run-level.
	// Zero decimals (whole rupees) is valid — do not treat 0 as "unset".
	configDecimals := fd.BankRoundingDecimals
	if configDecimals < 0 {
		configDecimals = params.PrecisionDecimals
	}
	if configDecimals < 0 {
		configDecimals = rounding.DefaultDecimals
	}
	configRoundingRule := fd.BankRoundingMethod
	if configRoundingRule == "" {
		configRoundingRule = params.RoundingRule
	}
	if configRoundingRule == "" {
		configRoundingRule = "ROUND"
	}

	configDailyRate := (fd.InterestRate / 100.0) / float64(configDivisor)
	configRaw := configOpeningPrincipal * configDailyRate * float64(configAccrualDays)

	configRoundingFreq := fd.BankRoundingFrequency
	if configRoundingFreq == "" {
		configRoundingFreq = "EACH_PERIOD"
	}

	// Prefer posted cashflow schedule (same rounded rows as simulator / cashflow engine).
	configPeriodInterest := rounding.Apply(
		configRaw, configDecimals, configRoundingRule, configRoundingFreq, true)
	interestSource := "formula"
	var schedCFIDs []string
	includePeriodEndCashflows := sameAccrualDate(effectiveEnd, fd.FdMaturityDate) ||
		sameAccrualDate(effectiveEnd, fd.EffectiveCloseDate)
	if schedInterest, ids, hasSched := fdMaster.PeriodInterestFromScheduleWithEnd(
		ctx, pool, fd.FDID, effectiveStart, effectiveEnd, fd.InterestTypeCode,
		includePeriodEndCashflows,
	); hasSched {
		configPeriodInterest = schedInterest
		schedCFIDs = ids
		interestSource = "cashflow_schedule"
	}

	// Cashflow data (same for both paths — receipts come from bank, not formula)
	interestReceived, tdsDeducted, cashflowIDs := getCashflowDataForPeriod(
		ctx, pool, fd.FDID, effectiveStart, effectiveEnd, includePeriodEndCashflows)
	if len(schedCFIDs) > 0 {
		seen := make(map[string]struct{}, len(cashflowIDs))
		for _, id := range cashflowIDs {
			seen[id] = struct{}{}
		}
		for _, id := range schedCFIDs {
			if _, ok := seen[id]; !ok {
				cashflowIDs = append(cashflowIDs, id)
			}
		}
	}

	configTDSApplicable := configPeriodInterest
	configClosingBalance := openingBalance + configPeriodInterest - interestReceived
	configNetInterest := configPeriodInterest - tdsDeducted

	configFormula := fmt.Sprintf(
		"[CONFIG] src=%s P(%.2f) × r(%.4f%%) × d(%d) / D(%d) = %.4f | rnd(%s,%d,freq=%s) = %.2f | holiday_excl=%d",
		interestSource, configOpeningPrincipal, fd.InterestRate, configAccrualDays, configDivisor,
		configRaw, configRoundingRule, configDecimals, configRoundingFreq,
		configPeriodInterest, configHolidaysExcluded)

	// ─────────────────────────────────────────────────────────────────────
	// REQ PATH — run-level baseline calculation
	// No holiday exclusions. Always fd.PrincipalAmount (no compounding lookup).
	// ─────────────────────────────────────────────────────────────────────

	reqDayCountCode := normalizeAccrualDayCountCode(params.DayCountConvention)
	if reqDayCountCode == "" {
		reqDayCountCode = "ACT_365"
	}
	// Req path: run-level day count, no holiday/weekend exclusion except 30/360 formula days.
	reqDivisor, reqAccrualDays := getDivisorAndDaysForAccrualPeriod(
		reqDayCountCode, effectiveStart, effectiveEnd, fd, cal, false)

	reqOpeningPrincipal := fd.PrincipalAmount // always original principal

	reqDecimals := params.PrecisionDecimals
	if reqDecimals < 0 {
		reqDecimals = rounding.DefaultDecimals
	}
	reqRoundingRule := params.RoundingRule
	if reqRoundingRule == "" {
		reqRoundingRule = "ROUND"
	}

	reqDailyRate := (fd.InterestRate / 100.0) / float64(reqDivisor)
	reqRaw := reqOpeningPrincipal * reqDailyRate * float64(reqAccrualDays)
	reqPeriodInterest := rounding.RoundByMethod(reqRaw, reqDecimals, reqRoundingRule)

	reqClosingBalance := openingBalance + reqPeriodInterest - interestReceived
	reqNetInterest := reqPeriodInterest - tdsDeducted

	reqFormula := fmt.Sprintf(
		"[REQ] P(%.2f) × r(%.4f%%) × d(%d) / D(%d) = %.4f | rnd(%s,%d) = %.2f",
		reqOpeningPrincipal, fd.InterestRate, reqAccrualDays, reqDivisor,
		reqRaw, reqRoundingRule, reqDecimals, reqPeriodInterest)

	// ─────────────────────────────────────────────────────────────────────
	// marshal cashflowIDs JSON for DB storage (used in run.go)
	cashflowIDsJSON, _ := json.Marshal(cashflowIDs)
	_ = cashflowIDsJSON

	return AccrualPeriodResult{
		// Identity
		FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankID: fd.BankID, BankName: fd.BankName,
		EntityID: fd.EntityID, EntityName: fd.EntityName,
		InterestTypeCode: fd.InterestTypeCode,
		PrincipalAmount:  fd.PrincipalAmount, InterestRate: fd.InterestRate,
		FdStartDate:    fd.FdStartDate,
		FdMaturityDate: fd.FdMaturityDate,

		// Period window
		AccrualPeriodStart: effectiveStart,
		AccrualPeriodEnd:   effectiveEnd,

		// Config path (main ledger fields)
		DayCountCode:             configDayCountCode,
		AccrualDays:              configAccrualDays,
		OpeningPrincipal:         configOpeningPrincipal,
		DailyAccrualRate:         configDailyRate,
		Divisor:                  configDivisor,
		PeriodInterestAccrued:    configPeriodInterest,
		OpeningAccruedBalance:    openingBalance,
		InterestReceivedInPeriod: interestReceived,
		ClosingAccruedBalance:    configClosingBalance,
		TDSApplicableAmount:      configTDSApplicable,
		TDSDeductedInPeriod:      tdsDeducted,
		NetInterestInPeriod:      configNetInterest,
		FormulaUsed:              configFormula,

		// Config metadata (stored on ledger for audit)
		ConfigWeekendAccrual:    fd.WeekendAccrual,
		ConfigHolidayAccrual:    fd.HolidayAccrual,
		ConfigHolidaysExcluded:  configHolidaysExcluded,
		ConfigRoundingRule:      configRoundingRule,
		ConfigPrecisionDecimals: configDecimals,
		ConfigCapDateAdjustment: fd.CapDateAdjustment,
		ConfigHolidayCalendar:   fd.HolidayCalendarCode,

		// Req path
		ReqDayCountCode:      reqDayCountCode,
		ReqDivisor:           reqDivisor,
		ReqAccrualDays:       reqAccrualDays,
		ReqOpeningPrincipal:  reqOpeningPrincipal,
		ReqDailyAccrualRate:  reqDailyRate,
		ReqPeriodInterest:    reqPeriodInterest,
		ReqClosingBalance:    reqClosingBalance,
		ReqTDSDeducted:       tdsDeducted,
		ReqNetInterest:       reqNetInterest,
		ReqFormulaUsed:       reqFormula,
		ReqRoundingRule:      reqRoundingRule,
		ReqPrecisionDecimals: reqDecimals,
		ReqWeekendAccrual:    true,
		ReqHolidayAccrual:    true,

		// Compounding metadata (BRD IA-05 Section C)
		FrequencyID:            fd.FrequencyID,
		CompoundingFrequency:   fd.CompoundingFrequency,
		CompoundPrincipalUsed:  configOpeningPrincipal,
		NextCapitalizationDate: getNextCapitalizationDate(ctx, pool, fd.FDID, effectiveEnd),

		// References
		CashflowRowIDs:  cashflowIDs,
		LedgerRowStatus: "CALCULATED",
	}
}

// friendlyAccrualError maps known DB constraint violations and
// common errors to human-readable messages.
func friendlyAccrualError(err error, context string) string {
	if err == nil {
		return ""
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "fd_accrual_run_granularity_check"):
		return "Invalid accrual_granularity. Allowed: DAILY, MONTHLY, QUARTERLY, HALF_YEARLY, YEARLY, RUN. Run migration 20260519_fd_accrual_run_granularity_yearly.sql if YEARLY fails."
	case strings.Contains(s, "fd_accrual_run_type_check"):
		return "Invalid run_type. Allowed: DAILY, MONTHLY, QUARTERLY, HALF_YEARLY, YEARLY, AD_HOC, MANUAL, SCHEDULED, RUN."
	case strings.Contains(s, "fd_accrual_run_status_check"):
		return "Invalid run_status transition. Check allowed statuses."
	case strings.Contains(s, "fd_accrual_run_mode_check"):
		return "Invalid run_mode. Must be SIMULATION or FINAL."
	case strings.Contains(s, "fd_accrual_ledger_status_check"):
		return "Invalid ledger_row_status. Allowed: CALCULATED, ERROR, OVERRIDDEN, EXCLUDED, PENDING_OVERRIDE, POSTED."
	case strings.Contains(s, "fd_accrual_ledger_override_status_check"):
		return "Invalid override_status. Allowed: PENDING, PROPOSED, APPROVED, REJECTED."
	case strings.Contains(s, "fd_accrual_run_fd_unique"):
		return "This FD already has a ledger entry for this run. Use /recompute to recalculate."
	case strings.Contains(s, "fd_accrual_run_period_check"):
		return "accrual_period_end must be >= accrual_period_start."
	case strings.Contains(s, "fd_accrual_ledger_fd_fkey"):
		return "FD not found in fd_master. Verify the fd_id is correct and the FD is not deleted."
	case strings.Contains(s, "fd_accrual_ledger_run_fkey"):
		return "Accrual run not found. Verify the run_id is correct."
	case strings.Contains(s, "fd_accrual_exception_ledger_fkey"):
		return "Ledger row not found. Cannot create exception — run execute first."
	case strings.Contains(s, "fd_accrual_exception_run_fkey"):
		return "Accrual run not found for this exception."
	case strings.Contains(s, "duplicate key"), strings.Contains(s, "unique constraint"):
		return "Duplicate entry: " + context + ". Record already exists."
	case strings.Contains(s, "foreign key"), strings.Contains(s, "violates foreign key"):
		return "Referenced record not found: " + context
	case strings.Contains(s, "no rows in result set"), strings.Contains(s, "no rows"):
		return "Record not found: " + context
	case strings.Contains(s, "context canceled"), strings.Contains(s, "context deadline"):
		return "Request timed out. The operation took too long. Try a smaller date range."
	case strings.Contains(s, "connection"):
		return "Database connection error. Please retry."
	default:
		return context + ": " + err.Error()
	}
}

// toFloat64 converts numeric interface{} values (from JSON decode or DB scan) to float64.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
}
