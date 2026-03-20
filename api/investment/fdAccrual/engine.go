package fdAccrual

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─── Structs ──────────────────────────────────────────────────────────────────

// AccrualInput holds every field the engine needs for a single FD.
type AccrualInput struct {
	FDID             string
	FdRefNo          string  // bank_fd_ref_no
	BankID           string
	BankName         string
	EntityID         string
	EntityName       string
	InterestTypeCode string  // SIMPLE / COMPOUND / STEPPED
	PrincipalAmount  float64
	InterestRate     float64 // annual percentage e.g. 7.5
	DayCountCode     string  // ACT_365 / ACT_360 / 30_360
	FdStartDate      time.Time
	FdMaturityDate   time.Time
	// Bank config fields for holiday-aware accrual day counting
	BankConfigID        string
	HolidayCalendarCode string
	WeekendAccrual      bool // if false, exclude weekends from day count
	HolidayAccrual      bool // if false, exclude holidays from day count
	WeekendPattern      string // e.g. "Sat,Sun"
}

// AccrualRunParams controls what the engine calculates.
type AccrualRunParams struct {
	PeriodStart        time.Time
	PeriodEnd          time.Time
	FinancialPeriod    string
	DayCountConvention string   // overrides per-FD code if set
	RoundingRule       string   // ROUND / TRUNCATE
	PrecisionDecimals  int
	EntityID           string   // for scope
	BankIDFilter       string   // optional
	FDStatusFilter     string   // ACTIVE default
	FDInclusionMethod  string   // ALL / SELECT_LIST
	FDInclusionList    []string
}

// AccrualPeriodResult holds the engine output for one FD x one period.
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
	DayCountCode     string
	FdStartDate      time.Time
	FdMaturityDate   time.Time

	// Period
	AccrualPeriodStart time.Time
	AccrualPeriodEnd   time.Time
	AccrualDays        int

	// Calculation inputs
	OpeningPrincipal float64
	DailyAccrualRate float64
	Divisor          int

	// Outputs
	PeriodInterestAccrued    float64
	OpeningAccruedBalance    float64
	InterestReceivedInPeriod float64
	ClosingAccruedBalance    float64
	TDSApplicableAmount      float64
	TDSDeductedInPeriod      float64
	NetInterestInPeriod      float64

	// References
	CashflowRowIDs   []string // cashflow_ids used in this calc
	FormulaUsed      string
	LedgerRowStatus  string // CALCULATED / ERROR / EXCLUDED
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
	BankIDFilter       string
	FDStatusFilter     string
	AccrualPeriodStart time.Time
	AccrualPeriodEnd   time.Time
	FinancialPeriod    string
	DayCountConvention string
	RoundingRule       string
	PrecisionDecimals  int
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

// getDivisorForAccrual returns the annual divisor. Never returns 0.
func getDivisorForAccrual(dayCountCode string, refDate time.Time) int {
	switch strings.ToUpper(dayCountCode) {
	case "ACT_365":
		return 365
	case "ACT_360":
		return 360
	case "30_360":
		return 360
	case "ACT_ACT":
		year := refDate.Year()
		if year%400 == 0 || (year%4 == 0 && year%100 != 0) {
			return 366
		}
		return 365
	default:
		return 365
	}
}

// roundAccrual rounds to the given decimals using the specified rule.
func roundAccrual(amount float64, decimals int, rule string) float64 {
	if decimals <= 0 {
		decimals = 2
	}
	pow := math.Pow(10, float64(decimals))
	if strings.EqualFold(rule, "TRUNCATE") {
		return math.Floor(amount*pow) / pow
	}
	return math.Round(amount*pow) / pow
}

// buildAccrualPeriod returns "APR 2025" uppercase month-year string.
func buildAccrualPeriod(t time.Time) string {
	return strings.ToUpper(t.Format("Jan 2006"))
}

// ─── Scope query ──────────────────────────────────────────────────────────────

// getFDsInScope queries investment.fd_master using exact column names.
func getFDsInScope(ctx context.Context, pool *pgxpool.Pool, params AccrualRunParams) ([]AccrualInput, error) {
	fdStatus := params.FDStatusFilter
	if fdStatus == "" {
		fdStatus = "ACTIVE"
	}

	query := `
		SELECT
			f.fd_id,
			COALESCE(f.bank_fd_ref_no, '')                   AS fd_ref_no,
			COALESCE(f.bank_id, '')                           AS bank_id,
			COALESCE(f.bank_name, '')                         AS bank_name,
			COALESCE(f.entity_id, '')                         AS entity_id,
			COALESCE(f.entity_name, '')                       AS entity_name,
			COALESCE(f.interest_type_code, 'SIMPLE')          AS interest_type_code,
			COALESCE(f.principal_amount, 0)                   AS principal_amount,
			COALESCE(f.interest_rate, 0)                      AS interest_rate,
			COALESCE(f.day_count_code, 'ACT_365')             AS day_count_code,
			f.start_date,
			f.maturity_date,
			COALESCE(f.bank_config_id, '')                    AS bank_config_id,
			COALESCE(bc.holiday_calendar_code, '')            AS holiday_calendar_code,
			COALESCE(bc.weekend_accrual, true)                AS weekend_accrual,
			COALESCE(bc.holiday_accrual, true)                AS holiday_accrual,
			COALESCE(mc.weekend_pattern, 'Sat,Sun')           AS weekend_pattern
		FROM investment.fd_master f
			LEFT JOIN investment.fd_bank_config_master bc ON bc.config_id = f.bank_config_id
				AND COALESCE(bc.is_deleted, false) = false
			LEFT JOIN investment.mastercalendar mc ON mc.calendar_code = bc.holiday_calendar_code
				AND COALESCE(mc.is_deleted, false) = false
		WHERE f.entity_id = $1
		  AND f.fd_status = $2
		  AND f.cashflow_generated = true
		  AND f.is_deleted = false
		  AND f.is_active = true
		  AND f.start_date <= $3
		  AND f.maturity_date >= $4`

	args := []interface{}{params.EntityID, fdStatus, params.PeriodEnd, params.PeriodStart}
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
		if err := rows.Scan(
			&fd.FDID, &fd.FdRefNo, &fd.BankID, &fd.BankName,
			&fd.EntityID, &fd.EntityName, &fd.InterestTypeCode,
			&fd.PrincipalAmount, &fd.InterestRate, &fd.DayCountCode,
			&fd.FdStartDate, &fd.FdMaturityDate,
			&fd.BankConfigID, &fd.HolidayCalendarCode,
			&fd.WeekendAccrual, &fd.HolidayAccrual, &fd.WeekendPattern,
		); err != nil {
			return nil, fmt.Errorf("getFDsInScope scan: %w", err)
		}
		results = append(results, fd)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getFDsInScope rows.Err: %w", err)
	}
	return results, nil
}

// ─── Holiday calendar helpers ────────────────────────────────────────────────

// accrualHolidayCalendar holds expanded holiday dates for accrual engine use.
type accrualHolidayCalendar struct {
	CalendarCode   string
	WeekendPattern string         // e.g. "Sat,Sun"
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
		seed, err := time.Parse("2006-01-02", dateStr[:10])
		if err != nil {
			continue
		}
		for _, d := range accrualExpandRRule(seed, rrule, expandFrom, expandTo) {
			cal.HolidayDates[d.Format("2006-01-02")] = true
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
	if !fd.HolidayAccrual && cal.HolidayDates[d.Format("2006-01-02")] {
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
					fd.FdMaturityDate.Format("2006-01-02"), params.PeriodStart.Format("2006-01-02")),
				SuggestedAction: "Exclude this FD from the run or verify maturity date",
			})
		}
		if !fd.FdStartDate.IsZero() && fd.FdStartDate.After(params.PeriodEnd) {
			findings = append(findings, ValidationFinding{
				FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankName: fd.BankName,
				IssueType: "FD_NOT_STARTED_IN_PERIOD",
				Severity:  "BLOCKER",
				Description: fmt.Sprintf("FD start date %s is after period end %s",
					fd.FdStartDate.Format("2006-01-02"), params.PeriodEnd.Format("2006-01-02")),
				SuggestedAction: "Exclude this FD or adjust the accrual period",
			})
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

// getCashflowDataForPeriod sums interest receipts and TDS deductions in the period.
func getCashflowDataForPeriod(ctx context.Context, pool *pgxpool.Pool,
	fdID string, periodStart, periodEnd time.Time) (interestReceived float64, tdsDeducted float64, cashflowIDs []string) {

	rows, err := pool.Query(ctx, `
		SELECT cashflow_id, event_type,
		       COALESCE(net_cash_flow, 0),
		       COALESCE(tds_amount, 0)
		FROM investment.fd_cashflow_schedule
		WHERE fd_id = $1
		  AND event_date >= $2
		  AND event_date <= $3
		  AND event_type IN ('INTEREST_RECEIPT', 'TDS_DEDUCTION')
		  AND COALESCE(is_deleted, false) = false`,
		fdID, periodStart, periodEnd,
	)
	if err != nil {
		return 0, 0, nil
	}
	defer rows.Close()

	for rows.Next() {
		var cashflowID, eventType string
		var netCash, tdsAmt float64
		if err := rows.Scan(&cashflowID, &eventType, &netCash, &tdsAmt); err != nil {
			continue
		}
		cashflowIDs = append(cashflowIDs, cashflowID)
		if eventType == "INTEREST_RECEIPT" {
			interestReceived += netCash
		}
		if eventType == "TDS_DEDUCTION" {
			tdsDeducted += tdsAmt
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

	effectiveStart := fd.FdStartDate
	if params.PeriodStart.After(effectiveStart) {
		effectiveStart = params.PeriodStart
	}
	effectiveEnd := fd.FdMaturityDate
	if params.PeriodEnd.Before(effectiveEnd) {
		effectiveEnd = params.PeriodEnd
	}

	if !effectiveStart.Before(effectiveEnd) {
		excluded.CalculationError = "effective period is zero or negative"
		return excluded
	}

	// Load holiday calendar (empty if no calendar code configured)
	cal := loadHolidayCalendarForAccrual(ctx, pool, fd.HolidayCalendarCode, fd.FdStartDate, fd.FdMaturityDate)

	dayCountCode := fd.DayCountCode
	if params.DayCountConvention != "" {
		dayCountCode = params.DayCountConvention
	}
	divisor := getDivisorForAccrual(dayCountCode, effectiveStart)

	// Use holiday-aware working day count (skips non-accrual days per bank config)
	accrualDays := accrualCountWorkingDays(effectiveStart, effectiveEnd, fd, cal)
	if accrualDays <= 0 {
		excluded.CalculationError = "accrual days <= 0"
		return excluded
	}

	openingPrincipal := fd.PrincipalAmount
	if strings.EqualFold(fd.InterestTypeCode, "COMPOUND") {
		if cp := getCompoundPrincipalAtDate(ctx, pool, fd.FDID, effectiveStart); cp > 0 {
			openingPrincipal = cp
		}
	}

	decimals := params.PrecisionDecimals
	if decimals <= 0 {
		decimals = 2
	}

	dailyRate := (fd.InterestRate / 100.0) / float64(divisor)
	rawInterest := openingPrincipal * dailyRate * float64(accrualDays)
	periodInterest := roundAccrual(rawInterest, decimals, params.RoundingRule)

	interestReceived, tdsDeducted, cashflowIDs := getCashflowDataForPeriod(
		ctx, pool, fd.FDID, effectiveStart, effectiveEnd)

	tdsApplicable := periodInterest
	closingBalance := openingBalance + periodInterest - interestReceived
	netInterest := periodInterest - tdsDeducted

	formula := fmt.Sprintf("P(%.2f) × r(%.4f%%) × d(%d) / D(%d) = %.6f",
		openingPrincipal, fd.InterestRate, accrualDays, divisor, periodInterest)

	// marshal cashflowIDs JSON for DB storage (used in run.go)
	cashflowIDsJSON, _ := json.Marshal(cashflowIDs)
	_ = cashflowIDsJSON

	return AccrualPeriodResult{
		FDID: fd.FDID, FdRefNo: fd.FdRefNo, BankID: fd.BankID, BankName: fd.BankName,
		EntityID: fd.EntityID, EntityName: fd.EntityName,
		InterestTypeCode: fd.InterestTypeCode,
		PrincipalAmount:  fd.PrincipalAmount, InterestRate: fd.InterestRate,
		DayCountCode:   dayCountCode,
		FdStartDate:    fd.FdStartDate,
		FdMaturityDate: fd.FdMaturityDate,

		AccrualPeriodStart: effectiveStart,
		AccrualPeriodEnd:   effectiveEnd,
		AccrualDays:        accrualDays,

		OpeningPrincipal: openingPrincipal,
		DailyAccrualRate: dailyRate,
		Divisor:          divisor,

		PeriodInterestAccrued:    periodInterest,
		OpeningAccruedBalance:    openingBalance,
		InterestReceivedInPeriod: interestReceived,
		ClosingAccruedBalance:    closingBalance,
		TDSApplicableAmount:      tdsApplicable,
		TDSDeductedInPeriod:      tdsDeducted,
		NetInterestInPeriod:      netInterest,

		CashflowRowIDs:  cashflowIDs,
		FormulaUsed:     formula,
		LedgerRowStatus: "CALCULATED",
	}
}
