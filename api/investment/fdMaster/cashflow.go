package fdMaster

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type FDRecord struct {
	FDID                    string
	ConfirmationID          string
	BookingID               string
	EntityID                string
	EntityName              string
	BankID                  string
	BankName                string
	BankAccountID           string // maps to source_account_id
	BankConfigID            string
	FrequencyID             string
	PrincipalAmount         float64
	InterestRate            float64
	InterestTypeCode        string // SIMPLE / COMPOUND / STEPPED
	TenorDays               int
	ValueDate               time.Time // maps to start_date
	MaturityDate            time.Time
	MaturityAmount          float64
	InterestPayoutFrequency string
	CompoundingFrequency    string
	DayCountConvention      string
	Currency                string
	TDSPlanID               string
	BankFDReference         string
	ReceiptDate             time.Time
	ConfirmationStatus      string
}

// InterestType is an alias kept for backward compat inside cashflow calculations.
func (r *FDRecord) InterestType() string { return r.InterestTypeCode }

// BankConfig holds ALL relevant fields from investment.fd_bank_config_master.
type BankConfig struct {
	ConfigID                   string
	DayCountCode               string  // DC-ACT-365 | DC-ACT-360 | DC-ACT-ACT | DC-30-360
	CapitalizationScheduleType string  // ANNIVERSARY | CALENDAR_QTR_END
	QuarterDefinition          string  // CALENDAR_QUARTER | 90_DAYS
	TDSDeductionTiming         string  // ACCRUAL_ANNUAL | MATURITY | NONE
	RoundingMethod             string  // ROUND | TRUNCATE | ROUND_UP | ROUND_DOWN
	RoundingFrequency          string  // EACH_PERIOD | AT_MATURITY
	InterestRoundingDecimals   int
	AccrualStartConvention     string  // INCLUDE | EXCLUDE
	AccrualEndConvention       string  // INCLUDE | EXCLUDE
	PeriodBoundaryDefinition   string  // INCL_START_EXCL_END | INCL_BOTH
        WeekendAccrual             bool
        HolidayAccrual             bool
        HolidayCalendarCode        string  // e.g. "Q" or "IN" — code from mastercalendar
        BrokenPeriodMethod         string  // SIMPLE | NONE
}

type CompoundingFreq struct {
	FrequencyID               string
	FrequencyCode             string
	FrequencyName             string
	FrequencyType             string
	CompoundingPeriodsPerYear int
	DaysPerPeriod             int
}

type TDSConfig struct {
	TDSPlanID       string
	TDSRate         float64
	ThresholdAmount float64
	ThresholdType   string
	DeductionTiming string // ACCRUAL_ANNUAL | MATURITY | NONE
}

// CashflowRow maps 1:1 to the fd_cashflow_schedule table columns.
type CashflowRow struct {
	// identity / sequence
	PeriodNumber    int
	EventType       string
	EventDate       time.Time
	// period window
	PeriodStartDate time.Time
	PeriodEndDate   time.Time
	PeriodDays      int
	// amounts
	OpeningPrincipal float64
	InterestAccrued  float64
	CapitalizedAmount float64
	ClosingPrincipal float64
	TDSAmount        float64
	NetCashFlow      float64
	// calculation metadata
	DayCountCode      string
	Divisor           int
	FormulaUsed       string
	AccrualRatePerDay float64
	// GL accounts (populated when known)
	DrAccountCode string
	DrAccountName string
	CrAccountCode string
	CrAccountName string
}

// ── DB loaders ─────────────────────────────────────────────────────────────

func loadFDRecord(ctx context.Context, exec queryExecutor, confirmationID string) (*FDRecord, error) {
	bookingCols, err := loadTableColumns(ctx, exec, "investment", "fd_booking_request")
	if err != nil {
		return nil, fmt.Errorf("load FD record: %w", err)
	}
	bankAccCol := pickFirstExistingColumn(bookingCols, "source_account_id", "bank_account_id", "account_id", "bank_account")
	bankAccExpr := "''::text"
	if bankAccCol != "" {
		bankAccExpr = "COALESCE(b." + bankAccCol + ", '')"
	}

	confCols, err := loadTableColumns(ctx, exec, "investment", "fd_confirmation")
	if err != nil {
		return nil, fmt.Errorf("load FD record: %w", err)
	}
	currencyExpr := "''::text"
	switch {
	case confCols["currency"]:
		currencyExpr = "COALESCE(c.currency, '')"
	case confCols["currency_code"]:
		currencyExpr = "COALESCE(c.currency_code, '')"
	case bookingCols["currency"]:
		currencyExpr = "COALESCE(b.currency, '')"
	case bookingCols["currency_code"]:
		currencyExpr = "COALESCE(b.currency_code, '')"
	}

	rec := &FDRecord{}
	q := fmt.Sprintf(`
		SELECT
			c.confirmation_id,
			COALESCE(c.booking_id, '') AS booking_id,
			COALESCE(b.entity_id, '') AS entity_id,
			COALESCE(b.bank_id, '') AS bank_id,
			%s AS bank_account_id,
			COALESCE(b.bank_config_id, '') AS bank_config_id,
			COALESCE(b.frequency_id, '') AS frequency_id,
			COALESCE(c.actual_principal, 0),
			COALESCE(c.confirmed_rate, 0),
			COALESCE(b.interest_type_code, 'SIMPLE') AS interest_type_code,
			COALESCE(b.tenure_days, 0),
			COALESCE(c.actual_start_date, b.expected_start_date),
			COALESCE(c.actual_maturity_date, b.expected_maturity_date),
			0,
			COALESCE(b.frequency_id, '') AS interest_payout_frequency,
			'' AS compounding_frequency,
			COALESCE(b.day_count_code, '') AS day_count_convention,
			%s AS currency,
			COALESCE(b.tds_plan_id, ''),
			COALESCE(c.bank_fd_ref_no, c.bank_reference_number, ''),
			COALESCE(c.confirmation_received_date, c.actual_start_date),
			COALESCE(c.confirmation_status, '')
		FROM investment.fd_confirmation c
		JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
		WHERE c.confirmation_id = $1
		  AND COALESCE(c.is_deleted, false) = false
		  AND COALESCE(b.is_deleted, false) = false
	`, bankAccExpr, currencyExpr)
	err = exec.QueryRow(ctx, q, confirmationID).Scan(
		&rec.ConfirmationID,
		&rec.BookingID,
		&rec.EntityID,
		&rec.BankID,
		&rec.BankAccountID,
		&rec.BankConfigID,
		&rec.FrequencyID,
		&rec.PrincipalAmount,
		&rec.InterestRate,
		&rec.InterestTypeCode,
		&rec.TenorDays,
		&rec.ValueDate,
		&rec.MaturityDate,
		&rec.MaturityAmount,
		&rec.InterestPayoutFrequency,
		&rec.CompoundingFrequency,
		&rec.DayCountConvention,
		&rec.Currency,
		&rec.TDSPlanID,
		&rec.BankFDReference,
		&rec.ReceiptDate,
		&rec.ConfirmationStatus,
	)
	if err != nil {
		return nil, fmt.Errorf("load FD record: %w", err)
	}
	return rec, nil
}

func loadFDRecordByFDID(ctx context.Context, exec queryExecutor, fdID string) (*FDRecord, error) {
	masterCols, err := loadTableColumns(ctx, exec, "investment", "fd_master")
	if err != nil {
		return nil, err
	}
	keyCol := pickFirstExistingColumn(masterCols, "fd_id", "master_id", "confirmation_id")
	confirmationCol := pickFirstExistingColumn(masterCols, "confirmation_id")
	if keyCol == "" || confirmationCol == "" {
		return nil, fmt.Errorf("fd_master metadata incomplete")
	}

	var confirmationID string
	if err := exec.QueryRow(ctx, fmt.Sprintf(
		"SELECT %s FROM investment.fd_master WHERE %s = $1 AND COALESCE(is_deleted,false)=false",
		confirmationCol, keyCol,
	), fdID).Scan(&confirmationID); err != nil {
		// Fallback: caller may have passed a confirmation_id — try reverse lookup.
		var fallbackFDID string
		if ferr := exec.QueryRow(ctx, fmt.Sprintf(
			"SELECT %s FROM investment.fd_master WHERE %s = $1 AND COALESCE(is_deleted,false)=false",
			keyCol, confirmationCol,
		), fdID).Scan(&fallbackFDID); ferr == nil {
			return loadFDRecordByFDID(ctx, exec, fallbackFDID)
		}
		// No fd_master row — load directly from confirmation.
		rec, recErr := loadFDRecord(ctx, exec, fdID)
		if recErr != nil {
			return nil, fmt.Errorf("load fd_master confirmation: %w", err)
		}
		return rec, nil
	}

	rec, err := loadFDRecord(ctx, exec, confirmationID)
	if err != nil {
		return nil, err
	}
	rec.FDID = fdID
	// Prefer day_count_code stored on fd_master over booking if available.
	if masterCols["day_count_code"] && rec.DayCountConvention == "" {
		var dc string
		_ = exec.QueryRow(ctx, fmt.Sprintf(
			"SELECT COALESCE(day_count_code,'') FROM investment.fd_master WHERE %s=$1", keyCol), fdID).Scan(&dc)
		if dc != "" {
			rec.DayCountConvention = dc
		}
	}
	return rec, nil
}

func loadBankConfig(ctx context.Context, exec queryExecutor, bankConfigID string) (*BankConfig, error) {
	if strings.TrimSpace(bankConfigID) == "" {
		return &BankConfig{InterestRoundingDecimals: 2}, nil
	}

	cfg := &BankConfig{}
	err := exec.QueryRow(ctx, `
		SELECT
			config_id,
			COALESCE(day_count_code, ''),
			COALESCE(capitalization_schedule_type, ''),
			COALESCE(quarter_definition, ''),
			COALESCE(tds_deduction_timing, ''),
			COALESCE(rounding_method, 'ROUND'),
			COALESCE(rounding_frequency, 'EACH_PERIOD'),
			COALESCE(interest_rounding_decimals, 2),
			COALESCE(accrual_start_convention, 'INCLUDE'),
			COALESCE(accrual_end_convention, 'EXCLUDE'),
			COALESCE(period_boundary_definition, 'INCL_START_EXCL_END'),
			COALESCE(weekend_accrual, true),
			COALESCE(holiday_accrual, true),
			COALESCE(holiday_calendar_code, ''),
			COALESCE(broken_period_method, 'SIMPLE')
		FROM investment.fd_bank_config_master
		WHERE config_id = $1
		  AND COALESCE(is_deleted, false) = false
	`, bankConfigID).Scan(
		&cfg.ConfigID,
		&cfg.DayCountCode,
		&cfg.CapitalizationScheduleType,
		&cfg.QuarterDefinition,
		&cfg.TDSDeductionTiming,
		&cfg.RoundingMethod,
		&cfg.RoundingFrequency,
		&cfg.InterestRoundingDecimals,
		&cfg.AccrualStartConvention,
		&cfg.AccrualEndConvention,
		&cfg.PeriodBoundaryDefinition,
		&cfg.WeekendAccrual,
		&cfg.HolidayAccrual,
		&cfg.HolidayCalendarCode,
		&cfg.BrokenPeriodMethod,
	)
	if err != nil {
		return &BankConfig{InterestRoundingDecimals: 2}, nil
	}
	if cfg.InterestRoundingDecimals == 0 {
		cfg.InterestRoundingDecimals = 2
	}
	return cfg, nil
}

func loadCompoundingFreq(ctx context.Context, exec queryExecutor, frequencyRef string) (*CompoundingFreq, error) {
	if strings.TrimSpace(frequencyRef) == "" {
		return &CompoundingFreq{}, nil
	}
	freq := &CompoundingFreq{}
	err := exec.QueryRow(ctx, `
		SELECT
			frequency_id,
			COALESCE(frequency_code, ''),
			COALESCE(frequency_name, ''),
			COALESCE(frequency_type, ''),
			COALESCE(compounding_periods_per_year, 0),
			COALESCE(days_per_period, 0)
		FROM investment.fd_compounding_frequency_master
		WHERE COALESCE(is_deleted, false) = false
		  AND (frequency_id = $1 OR frequency_code = $1 OR frequency_name = $1)
		LIMIT 1
	`, frequencyRef).Scan(
		&freq.FrequencyID,
		&freq.FrequencyCode,
		&freq.FrequencyName,
		&freq.FrequencyType,
		&freq.CompoundingPeriodsPerYear,
		&freq.DaysPerPeriod,
	)
	if err != nil {
		return &CompoundingFreq{}, nil
	}
	return freq, nil
}

func loadTDSConfig(ctx context.Context, exec queryExecutor, planID string) (*TDSConfig, error) {
	if strings.TrimSpace(planID) == "" {
		return &TDSConfig{}, nil
	}
	tds := &TDSConfig{}
	err := exec.QueryRow(ctx, `
		SELECT
			tds_plan_id,
			COALESCE(tds_rate, 0),
			COALESCE(threshold_amount, 0),
			COALESCE(threshold_type, ''),
			COALESCE(deduction_timing, '')
		FROM investment.fd_tds_plan_master
		WHERE tds_plan_id = $1
		  AND COALESCE(is_deleted, false) = false
	`, planID).Scan(
		&tds.TDSPlanID,
		&tds.TDSRate,
		&tds.ThresholdAmount,
		&tds.ThresholdType,
		&tds.DeductionTiming,
	)
	if err != nil {
		return &TDSConfig{}, nil
	}
	return tds, nil
}

// ── Day-count helpers ──────────────────────────────────────────────────────

// DayCountInfo holds the canonical fields resolved from fd_day_count_convention_master.
type DayCountInfo struct {
	DayCountCode    string // e.g. "DC-ACT-365"
	ConventionType  string // e.g. "ACT_365" — the canonical enum value
}

// loadDayCountConvention resolves a day_count_code (e.g. "DC-ACT-365") or a raw
// convention string (e.g. "ACT_365", "ACT/365") against investment.fd_day_count_convention_master
// and returns the canonical convention_type.
// Falls back to static normalisation when the DB lookup fails.
func loadDayCountConvention(ctx context.Context, exec queryExecutor, dayCountRef string) DayCountInfo {
	ref := strings.TrimSpace(dayCountRef)
	if ref == "" {
		return DayCountInfo{ConventionType: "ACT_365"}
	}
	var code, conv string
	err := exec.QueryRow(ctx, `
		SELECT day_count_code, COALESCE(convention_type, '')
		FROM investment.fd_day_count_convention_master
		WHERE COALESCE(is_deleted, false) = false
		  AND (day_count_code = $1
		    OR UPPER(convention_type) = UPPER($1)
		    OR UPPER(day_count_name) = UPPER($1))
		LIMIT 1
	`, ref).Scan(&code, &conv)
	if err == nil && conv != "" {
		return DayCountInfo{DayCountCode: code, ConventionType: strings.ToUpper(conv)}
	}
	// Static fallback for values like "ACT/365", "30/360" that may not be stored.
	return DayCountInfo{DayCountCode: ref, ConventionType: normConventionStatic(ref)}
}

// normConventionStatic maps any freeform string to ACT_365 | ACT_360 | ACT_ACT | 30_360.
func normConventionStatic(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	switch s {
	case "DC-ACT-360", "ACT/360", "ACT_360", "ACTUAL/360", "ACTUAL_360":
		return "ACT_360"
	case "DC-30-360", "30/360", "30_360":
		return "30_360"
	case "DC-ACT-ACT", "ACT/ACT", "ACT_ACT", "ACTUAL/ACTUAL":
		return "ACT_ACT"
	default: // DC-ACT-365, ACTUAL_365, ACT/365 etc.
		return "ACT_365"
	}
}

// InterestTypeInfo holds fields resolved from fd_interest_type_master.
type InterestTypeInfo struct {
	InterestID        string
	InterestTypeCode  string
	CalculationMethod string // SIMPLE | COMPOUND | STEPPED
}

// loadInterestType looks up interest_type_code (or interest_id) in fd_interest_type_master
// and returns the canonical calculation_method.
func loadInterestType(ctx context.Context, exec queryExecutor, interestTypeRef string) InterestTypeInfo {
	ref := strings.TrimSpace(interestTypeRef)
	if ref == "" {
		return InterestTypeInfo{CalculationMethod: "SIMPLE"}
	}
	var id, code, method string
	err := exec.QueryRow(ctx, `
		SELECT interest_id, interest_type_code, COALESCE(calculation_method, 'SIMPLE')
		FROM investment.fd_interest_type_master
		WHERE COALESCE(is_deleted, false) = false
		  AND (interest_id = $1
		    OR UPPER(interest_type_code) = UPPER($1)
		    OR UPPER(interest_type_name) = UPPER($1))
		LIMIT 1
	`, ref).Scan(&id, &code, &method)
	if err == nil && method != "" {
		return InterestTypeInfo{InterestID: id, InterestTypeCode: code, CalculationMethod: strings.ToUpper(method)}
	}
	// Static fallback: raw value may already be SIMPLE/COMPOUND/STEPPED.
	upper := strings.ToUpper(ref)
	if upper == "COMPOUND" || upper == "STEPPED" {
		return InterestTypeInfo{CalculationMethod: upper}
	}
	return InterestTypeInfo{CalculationMethod: "SIMPLE"}
}

// ── Holiday Calendar helpers ──────────────────────────────────────────────

// HolidayCalendarInfo holds the resolved calendar + expanded holiday dates.
type HolidayCalendarInfo struct {
	CalendarCode    string
	WeekendPattern  string // e.g. "Sat,Sun"
	HolidayDates    map[string]bool // keyed "YYYY-MM-DD" for O(1) lookup
}

// isWeekend returns true if d is a weekend day according to weekendPattern.
func isWeekend(d time.Time, weekendPattern string) bool {
	w := strings.ToLower(d.Weekday().String()[:3])
	patLower := strings.ToLower(weekendPattern)
	return strings.Contains(patLower, w)
}

// expandRRule expands a recurrence rule string into dates within [from, to].
// Supports:
//   FREQ=YEARLY             — same day/month every year
//   FREQ=YEARLY;BYMONTH=N;BYDAY=MO/TU/WE/TH/FR/SA/SU — Nth weekday of month, every year
//   FREQ=MONTHLY;BYDAY=MO  — every month that weekday
//   FREQ=DAILY             — every day
//   FREQ=WEEKLY;BYDAY=MO   — every week that weekday
//   (empty)                — just the seed date itself
func expandRRule(seed time.Time, rrule string, from, to time.Time) []time.Time {
	var out []time.Time
	if seed.IsZero() {
		return out
	}

	// Parse rrule parts into a map for easy lookup.
	parts := map[string]string{}
	for _, seg := range strings.Split(rrule, ";") {
		kv := strings.SplitN(seg, "=", 2)
		if len(kv) == 2 {
			parts[strings.ToUpper(strings.TrimSpace(kv[0]))] = strings.ToUpper(strings.TrimSpace(kv[1]))
		}
	}

	weekdayMap := map[string]time.Weekday{
		"MO": time.Monday, "TU": time.Tuesday, "WE": time.Wednesday,
		"TH": time.Thursday, "FR": time.Friday, "SA": time.Saturday, "SU": time.Sunday,
	}

	freq := parts["FREQ"]
	byMonth := parts["BYMONTH"]
	byDay := parts["BYDAY"]

	switch freq {
	case "YEARLY":
		if byMonth != "" && byDay != "" {
			// e.g. FREQ=YEARLY;BYMONTH=1;BYDAY=WED => first Wednesday in January each year
			wdTarget, ok := weekdayMap[byDay]
			if !ok {
				// try 3-letter
				for k, v := range weekdayMap {
					if strings.HasPrefix(byDay, k) {
						wdTarget = v
						ok = true
						break
					}
				}
			}
			// parse month number
			var monthNum time.Month
			fmt.Sscanf(byMonth, "%d", &monthNum)
			if monthNum < 1 || monthNum > 12 {
				monthNum = seed.Month()
			}
			for y := from.Year(); y <= to.Year(); y++ {
				// Find first occurrence of weekday in month
				d := time.Date(y, monthNum, 1, 0, 0, 0, 0, time.UTC)
				for d.Weekday() != wdTarget {
					d = d.AddDate(0, 0, 1)
				}
				if !d.Before(from) && !d.After(to) {
					out = append(out, d)
				}
			}
		} else {
			// Simple FREQ=YEARLY — repeat seed date each year
			for y := from.Year(); y <= to.Year(); y++ {
				d := time.Date(y, seed.Month(), seed.Day(), 0, 0, 0, 0, time.UTC)
				if !d.Before(from) && !d.After(to) {
					out = append(out, d)
				}
			}
		}
	case "MONTHLY":
		if byDay != "" {
			wdTarget, ok := weekdayMap[byDay]
			if !ok {
				for k, v := range weekdayMap {
					if strings.HasPrefix(byDay, k) {
						wdTarget = v
						ok = true
						break
					}
				}
			}
			for cur := from; !cur.After(to); cur = cur.AddDate(0, 1, 0) {
				d := time.Date(cur.Year(), cur.Month(), 1, 0, 0, 0, 0, time.UTC)
				for d.Weekday() != wdTarget {
					d = d.AddDate(0, 0, 1)
				}
				if !d.Before(from) && !d.After(to) {
					out = append(out, d)
				}
			}
		} else {
			// Same day each month
			for cur := from; !cur.After(to); cur = cur.AddDate(0, 1, 0) {
				d := time.Date(cur.Year(), cur.Month(), seed.Day(), 0, 0, 0, 0, time.UTC)
				if !d.Before(from) && !d.After(to) {
					out = append(out, d)
				}
			}
		}
	case "WEEKLY":
		wdTarget := seed.Weekday()
		if byDay != "" {
			if wd, ok := weekdayMap[byDay]; ok {
				wdTarget = wd
			}
		}
		for cur := from; !cur.After(to); cur = cur.AddDate(0, 0, 1) {
			if cur.Weekday() == wdTarget {
				out = append(out, cur)
			}
		}
	case "DAILY":
		for cur := from; !cur.After(to); cur = cur.AddDate(0, 0, 1) {
			out = append(out, cur)
		}
	default:
		// No rrule or unknown — just the seed date itself
		if !seed.Before(from) && !seed.After(to) {
			out = append(out, seed)
		}
	}
	return out
}

// loadHolidayCalendar fetches calendar + holidays from mastercalendar/masterholiday
// and expands recurrence_rule entries for the range [from-2yr, to+2yr].
// Returns an empty HolidayCalendarInfo if calCode is empty or calendar not found.
func loadHolidayCalendar(ctx context.Context, exec queryExecutor, calCode string, from, to time.Time) HolidayCalendarInfo {
	info := HolidayCalendarInfo{HolidayDates: make(map[string]bool)}
	if strings.TrimSpace(calCode) == "" {
		return info
	}

	var calendarID, weekendPattern string
	err := exec.QueryRow(ctx, `
		SELECT calendar_id, COALESCE(weekend_pattern, 'Sat,Sun')
		FROM investment.mastercalendar
		WHERE calendar_code = $1
		  AND COALESCE(is_deleted, false) = false
		  AND UPPER(status) = 'ACTIVE'
		LIMIT 1
	`, calCode).Scan(&calendarID, &weekendPattern)
	if err != nil {
		// Try matching by calendar_id directly
		err = exec.QueryRow(ctx, `
			SELECT calendar_id, COALESCE(weekend_pattern, 'Sat,Sun')
			FROM investment.mastercalendar
			WHERE calendar_id = $1
			  AND COALESCE(is_deleted, false) = false
			LIMIT 1
		`, calCode).Scan(&calendarID, &weekendPattern)
		if err != nil {
			return info
		}
	}

	info.CalendarCode = calCode
	info.WeekendPattern = weekendPattern

	// Load holidays — include a 2-year buffer around the FD period to cover recurring ones.
	windowFrom := from.AddDate(-1, 0, 0)
	windowTo := to.AddDate(1, 0, 0)

	rows, err := exec.Query(ctx, `
		SELECT holiday_date, COALESCE(recurrence_rule, '')
		FROM investment.masterholiday
		WHERE calendar_id = $1
		  AND COALESCE(is_deleted, false) = false
		  AND UPPER(status) = 'ACTIVE'
	`, calendarID)
	if err != nil {
		return info
	}
	defer rows.Close()

	for rows.Next() {
		var seed time.Time
		var rrule string
		if err := rows.Scan(&seed, &rrule); err != nil {
			continue
		}
		expanded := expandRRule(seed, rrule, windowFrom, windowTo)
		for _, d := range expanded {
			info.HolidayDates[d.Format("2006-01-02")] = true
		}
	}
	return info
}

// isNonAccrualDay returns true if the date should be excluded from accrual
// (i.e. it is a holiday or weekend and the config says not to accrue on those days).
func isNonAccrualDay(d time.Time, cfg *BankConfig, cal HolidayCalendarInfo) bool {
	if !cfg.WeekendAccrual && cal.WeekendPattern != "" && isWeekend(d, cal.WeekendPattern) {
		return true
	}
	if !cfg.HolidayAccrual && len(cal.HolidayDates) > 0 {
		if cal.HolidayDates[d.Format("2006-01-02")] {
			return true
		}
	}
	return false
}

// countAccrualDays counts actual working days between periodStart and periodEnd
// honoring the bank config's weekend_accrual and holiday_accrual flags.
// For conventions that use raw calendar days (ACT_365, ACT_360, ACT_ACT) this
// replaces simple date-subtraction when the bank excludes non-working days.
func countAccrualDays(conventionType string, periodStart, periodEnd time.Time, cfg *BankConfig, cal HolidayCalendarInfo) int {
	// If both flags are true (accrue on all days), just use calendar days.
	if cfg.WeekendAccrual && cfg.HolidayAccrual {
		return int(periodEnd.Sub(periodStart).Hours() / 24)
	}
	// 30/360 convention: never use calendar-day counting; always use 30/360 formula.
	norm := strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(strings.TrimSpace(conventionType)))
	norm = strings.TrimPrefix(norm, "DC_")
	if norm == "30_360" {
		return int(periodEnd.Sub(periodStart).Hours() / 24) // 30_360 handled separately
	}
	// Count working days only.
	count := 0
	for cur := periodStart; cur.Before(periodEnd); cur = cur.AddDate(0, 0, 1) {
		if !isNonAccrualDay(cur, cfg, cal) {
			count++
		}
	}
	return count
}

// getDivisorAndDays computes (divisor, accrualDays) for a period.
// conventionType must be the canonical value from fd_day_count_convention_master:
// ACT_365 | ACT_360 | ACT_ACT | 30_360
func getDivisorAndDays(conventionType string, periodStart, periodEnd time.Time) (divisor int, days int) {
	// Accept both normalised forms from the master (ACT_360) and legacy slash forms (ACT/360).
	norm := strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(strings.TrimSpace(conventionType)))
	// Strip DC_ prefix if someone passes raw day_count_code.
	norm = strings.TrimPrefix(norm, "DC_")
	rawDays := int(periodEnd.Sub(periodStart).Hours() / 24)

	switch norm {
	case "30_360":
		days = countDays30_360(periodStart, periodEnd)
		return 360, days
	case "ACT_360":
		return 360, rawDays
	case "ACT_ACT":
		// Determine if any leap day falls in the period.
		divisorVal := 365
		for y := periodStart.Year(); y <= periodEnd.Year(); y++ {
			if isLeapYear(y) {
				// Check if Feb 29 of this year is within the period.
				feb29 := time.Date(y, 2, 29, 0, 0, 0, 0, time.UTC)
				if !feb29.Before(periodStart) && feb29.Before(periodEnd) {
					divisorVal = 366
					break
				}
			}
		}
		return divisorVal, rawDays
	default: // ACT_365 — fixed 365 even in leap years
		return 365, rawDays
	}
}

func isLeapYear(y int) bool {
	return (y%4 == 0 && y%100 != 0) || y%400 == 0
}

func countDays30_360(start, end time.Time) int {
	y1, m1, d1 := start.Date()
	y2, m2, d2 := end.Date()
	if d1 == 31 {
		d1 = 30
	}
	if d2 == 31 && d1 >= 30 {
		d2 = 30
	}
	return (y2-y1)*360 + int(m2-m1)*30 + (d2 - d1)
}

func roundAmount(value float64, decimals int) float64 {
	if decimals < 0 {
		decimals = 2
	}
	pow := math.Pow(10, float64(decimals))
	return math.Round(value*pow) / pow
}

// getDivisorAndDaysWithCal is like getDivisorAndDays but uses countAccrualDays
// to exclude non-working days when the bank config says so.
func getDivisorAndDaysWithCal(conventionType string, periodStart, periodEnd time.Time, cfg *BankConfig, cal HolidayCalendarInfo) (divisor int, days int) {
	// For 30/360, use formula-based day count regardless of holidays.
	norm := strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(strings.TrimSpace(conventionType)))
	norm = strings.TrimPrefix(norm, "DC_")
	if norm == "30_360" {
		return 360, countDays30_360(periodStart, periodEnd)
	}
	// For all other conventions count working days if needed.
	effectiveDays := countAccrualDays(conventionType, periodStart, periodEnd, cfg, cal)
	switch norm {
	case "ACT_360":
		return 360, effectiveDays
	case "ACT_ACT":
		divisorVal := 365
		for y := periodStart.Year(); y <= periodEnd.Year(); y++ {
			if isLeapYear(y) {
				feb29 := time.Date(y, 2, 29, 0, 0, 0, 0, time.UTC)
				if !feb29.Before(periodStart) && feb29.Before(periodEnd) {
					divisorVal = 366
					break
				}
			}
		}
		return divisorVal, effectiveDays
	default: // ACT_365
		return 365, effectiveDays
	}
}

// ── Period schedule builders ───────────────────────────────────────────────

func nextCalendarQuarterEnd(date time.Time) time.Time {
	y := date.Year()
	m := date.Month()
	switch {
	case m <= 3:
		return time.Date(y, 3, 31, 0, 0, 0, 0, time.UTC)
	case m <= 6:
		return time.Date(y, 6, 30, 0, 0, 0, 0, time.UTC)
	case m <= 9:
		return time.Date(y, 9, 30, 0, 0, 0, 0, time.UTC)
	default:
		return time.Date(y, 12, 31, 0, 0, 0, 0, time.UTC)
	}
}

// buildCapitalizationDates returns the period-end dates for compound/capitalized FDs.
// These determine CAPITALIZATION event boundaries.
func buildCapitalizationDates(fd *FDRecord, cfg *BankConfig) []time.Time {
	capType := strings.ToUpper(strings.TrimSpace(cfg.CapitalizationScheduleType))
	qDef := strings.ToUpper(strings.TrimSpace(cfg.QuarterDefinition))

	var dates []time.Time
	current := fd.ValueDate

	for current.Before(fd.MaturityDate) {
		var next time.Time

		switch capType {
		case "CALENDAR_QTR_END":
			next = nextCalendarQuarterEnd(current)
			if !next.After(current) {
				next = nextCalendarQuarterEnd(current.AddDate(0, 1, 0))
			}
		case "ANNIVERSARY":
			// Use 90-day anniversary or the configured days_per_period.
			offsetDays := 91
			if qDef == "90_DAYS" {
				offsetDays = 90
			}
			next = current.AddDate(0, 0, offsetDays)
		default:
			// Fallback: quarterly anniversary.
			next = current.AddDate(0, 3, 0)
		}

		if !next.Before(fd.MaturityDate) {
			break
		}
		dates = append(dates, next)
		current = next
	}

	dates = append(dates, fd.MaturityDate)
	return deduplicateDates(dates)
}

// buildMonthlyAccrualDates returns calendar month-end dates between start and maturity,
// used for ACCRUAL (non-cash) events which run for every FD regardless of payout frequency.
func buildMonthlyAccrualDates(fd *FDRecord) []time.Time {
	var dates []time.Time

	// First accrual boundary = end of the start month.
	y, m, _ := fd.ValueDate.Date()
	current := lastDayOfMonth(y, m)

	for current.Before(fd.MaturityDate) {
		if current.After(fd.ValueDate) {
			dates = append(dates, current)
		}
		y, m, _ = current.Date()
		m++
		if m > 12 {
			m = 1
			y++
		}
		current = lastDayOfMonth(y, m)
	}

	dates = append(dates, fd.MaturityDate)
	return deduplicateDates(dates)
}

func lastDayOfMonth(year int, month time.Month) time.Time {
	// First day of next month minus one day.
	first := time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC)
	return first.AddDate(0, 0, -1)
}

func deduplicateDates(in []time.Time) []time.Time {
	sort.SliceStable(in, func(i, j int) bool { return in[i].Before(in[j]) })
	out := make([]time.Time, 0, len(in))
	var last time.Time
	for _, d := range in {
		if last.IsZero() || !d.Equal(last) {
			out = append(out, d)
			last = d
		}
	}
	return out
}

// ── Core schedule generator ────────────────────────────────────────────────

// freqTypeToMonths maps a canonical frequency_type (or common freqCode) to months-per-period.
// Returns 0 if the frequency means AT_MATURITY.
func freqTypeToMonths(freqType string) int {
	switch strings.ToUpper(strings.TrimSpace(freqType)) {
	case "MONTHLY", "MONTH":
		return 1
	case "QUARTERLY", "QUARTER", "QTR":
		return 3
	case "HALF_YEARLY", "HALF-YEARLY", "HALFYEARLY", "BI_ANNUAL", "BIANNUAL", "SEMI_ANNUAL", "SEMI-ANNUAL":
		return 6
	case "ANNUAL", "YEARLY", "YEAR":
		return 12
	default:
		return 0 // AT_MATURITY or unknown
	}
}

// buildPayoutDates returns the interest-payment boundary dates for a given
// monthly step (e.g. 3 for quarterly).  The maturity date is always included.
func buildPayoutDates(fd *FDRecord, monthsPerPeriod int) []time.Time {
	if monthsPerPeriod <= 0 {
		// AT_MATURITY — single boundary.
		return []time.Time{fd.MaturityDate}
	}
	var dates []time.Time
	cur := fd.ValueDate
	for {
		next := cur.AddDate(0, monthsPerPeriod, 0)
		if !next.Before(fd.MaturityDate) {
			break
		}
		dates = append(dates, next)
		cur = next
	}
	dates = append(dates, fd.MaturityDate)
	return deduplicateDates(dates)
}

func generateCashflowSchedule(fd *FDRecord, cfg *BankConfig, freq *CompoundingFreq, tdsCfg *TDSConfig, dcInfo DayCountInfo, itInfo InterestTypeInfo, calInfo HolidayCalendarInfo) []CashflowRow {
	if fd == nil || fd.ValueDate.IsZero() || fd.MaturityDate.IsZero() {
		return nil
	}

	// Effective day count.
	effectiveConvention := firstNonEmpty(dcInfo.ConventionType, normConventionStatic(cfg.DayCountCode), "ACT_365")
	effectiveDayCountCode := firstNonEmpty(dcInfo.DayCountCode, fd.DayCountConvention, cfg.DayCountCode, "DC-ACT-365")
	decimals := cfg.InterestRoundingDecimals
	if decimals <= 0 {
		decimals = 2
	}

	// Determine calculation method and interest payout frequency.
	calcMethod := firstNonEmpty(itInfo.CalculationMethod, strings.ToUpper(fd.InterestTypeCode), "SIMPLE")
	isCompound := calcMethod == "COMPOUND"

	// Resolve the human-readable frequency type: prefer freq.FrequencyType (from master),
	// then fall back to the raw frequency_id / payout field which may itself be a code.
	freqType := strings.ToUpper(strings.TrimSpace(firstNonEmpty(
		freq.FrequencyType,
		freq.FrequencyCode,
		fd.InterestPayoutFrequency,
		fd.FrequencyID,
	)))
	payoutMonths := freqTypeToMonths(freqType)
	isAtMaturity := payoutMonths == 0

	// Determine TDS deduction timing and TDS payout frequency.
	tdsDeductionTiming := ""
	tdsPeriodMonths := 0 // 0 = same as interest payout
	if tdsCfg != nil {
		tdsDeductionTiming = strings.ToUpper(strings.TrimSpace(tdsCfg.DeductionTiming))
	}
	if tdsDeductionTiming == "" {
		tdsDeductionTiming = strings.ToUpper(strings.TrimSpace(cfg.TDSDeductionTiming))
	}
	hasTDS := tdsCfg != nil && tdsCfg.TDSRate > 0

	// Derive TDS deduction period from its timing code.
	// Examples: "HALF_YEARLY" / "SEMI_ANNUAL" → 6 months, "ANNUAL" → 12, "MATURITY" → 0 (at maturity), "ACCRUAL_ANNUAL" → 12.
	switch {
	case strings.Contains(tdsDeductionTiming, "HALF") || strings.Contains(tdsDeductionTiming, "SEMI"):
		tdsPeriodMonths = 6
	case strings.Contains(tdsDeductionTiming, "QUARTER"):
		tdsPeriodMonths = 3
	case strings.Contains(tdsDeductionTiming, "MONTH"):
		tdsPeriodMonths = 1
	case strings.Contains(tdsDeductionTiming, "ANNUAL") || strings.Contains(tdsDeductionTiming, "YEAR"):
		tdsPeriodMonths = 12
	default:
		// MATURITY or NONE — TDS at maturity only.
		tdsPeriodMonths = 0
	}

	// ── COMPOUND FDs: use capitalization dates ─────────────────────────────
	if isCompound {
		return generateCompoundSchedule(fd, cfg, freq, tdsCfg, dcInfo, itInfo, calInfo,
			effectiveConvention, effectiveDayCountCode, decimals, hasTDS, tdsDeductionTiming)
	}

	// ── SIMPLE / AT_MATURITY FDs: payout-frequency schedule ───────────────
	// Build the interest payment boundaries.
	payoutDates := buildPayoutDates(fd, payoutMonths)

	// Build the TDS deduction dates (may differ from payout dates).
	var tdsDeductionDates map[string]bool
	if hasTDS && tdsPeriodMonths > 0 && tdsPeriodMonths != payoutMonths {
		tdsDeductionDates = make(map[string]bool)
		for _, d := range buildPayoutDates(fd, tdsPeriodMonths) {
			tdsDeductionDates[d.Format("2006-01-02")] = true
		}
	}

	openingPrincipal := fd.PrincipalAmount
	periodStart := fd.ValueDate
	seq := 0
	var rows []CashflowRow
	var cumulativeTDSInterest float64 // interest accrued since last TDS deduction

	for idx, periodEnd := range payoutDates {
		if !periodEnd.After(periodStart) {
			continue
		}

		divisor, days := getDivisorAndDaysWithCal(effectiveConvention, periodStart, periodEnd, cfg, calInfo)
		if days <= 0 {
			days = 1
		}
		if strings.EqualFold(cfg.PeriodBoundaryDefinition, "INCL_BOTH") {
			days++
		}

		ratePerDay := fd.InterestRate / (float64(divisor) * 100)
		interest := roundAmount(openingPrincipal*fd.InterestRate*float64(days)/float64(divisor)/100, decimals)
		cumulativeTDSInterest += interest

		isMaturity := idx == len(payoutDates)-1

		// ── TDS_DEDUCTION event (if separate from interest payment) ─────
		needsTDS := hasTDS
		if hasTDS && tdsPeriodMonths > 0 && tdsPeriodMonths != payoutMonths {
			// TDS only on its own schedule dates.
			needsTDS = tdsDeductionDates[periodEnd.Format("2006-01-02")]
		}
		if hasTDS && tdsDeductionTiming == "MATURITY" {
			needsTDS = isMaturity
		}

		var tdsAmount float64
		if needsTDS && cumulativeTDSInterest > 0 {
			tdsAmount = roundAmount(cumulativeTDSInterest*tdsCfg.TDSRate/100, decimals)
		}

		// ── Determine event type ─────────────────────────────────────────
		eventType := "INTEREST_RECEIPT"
		if isAtMaturity || isMaturity {
			eventType = "MATURITY"
		}

		// Net cash flow for INTEREST_RECEIPT: interest paid out minus TDS withheld.
		var netCashflow float64
		if eventType == "MATURITY" {
			// Return principal + accumulated interest − TDS.
			netCashflow = roundAmount(openingPrincipal+interest-tdsAmount, decimals)
		} else {
			// Periodic interest payout.
			netCashflow = roundAmount(interest-tdsAmount, decimals)
		}

		seq++
		rows = append(rows, CashflowRow{
			PeriodNumber:      seq,
			EventType:         eventType,
			EventDate:         periodEnd,
			PeriodStartDate:   periodStart,
			PeriodEndDate:     periodEnd,
			PeriodDays:        days,
			OpeningPrincipal:  openingPrincipal,
			InterestAccrued:   interest,
			CapitalizedAmount: 0,
			ClosingPrincipal: func() float64 {
				if isMaturity {
					return 0
				}
				return openingPrincipal
			}(),
			TDSAmount:         tdsAmount,
			NetCashFlow:       netCashflow,
			DayCountCode:      effectiveDayCountCode,
			Divisor:           divisor,
			FormulaUsed:       fmt.Sprintf("P(%.2f) × r(%.4f%%) × d(%d) / D(%d) [%s]", openingPrincipal, fd.InterestRate, days, divisor, effectiveConvention),
			AccrualRatePerDay: ratePerDay,
		})

		// If TDS deduction is on a separate schedule (different period from payout)
		// and this is a TDS date, insert a dedicated TDS_DEDUCTION row too.
		if needsTDS && tdsAmount > 0 && tdsPeriodMonths > 0 && tdsPeriodMonths != payoutMonths && !isMaturity {
			seq++
			rows = append(rows, CashflowRow{
				PeriodNumber:     seq,
				EventType:        "TDS_DEDUCTION",
				EventDate:        periodEnd,
				PeriodStartDate:  periodStart,
				PeriodEndDate:    periodEnd,
				PeriodDays:       days,
				OpeningPrincipal: openingPrincipal,
				TDSAmount:        tdsAmount,
				NetCashFlow:      -tdsAmount,
				ClosingPrincipal: openingPrincipal,
				DayCountCode:     effectiveDayCountCode,
				Divisor:          divisor,
				FormulaUsed:      fmt.Sprintf("TDS = CumulativeInterest(%.2f) × %.4f%%", cumulativeTDSInterest, tdsCfg.TDSRate),
			})
			cumulativeTDSInterest = 0
		} else if needsTDS {
			cumulativeTDSInterest = 0
		}

		periodStart = periodEnd
	}

	// Sort by event date then sequence.
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].EventDate.Equal(rows[j].EventDate) {
			return rows[i].PeriodNumber < rows[j].PeriodNumber
		}
		return rows[i].EventDate.Before(rows[j].EventDate)
	})
	for i := range rows {
		rows[i].PeriodNumber = i + 1
	}
	return rows
}

// generateCompoundSchedule handles COMPOUND FDs — interest is capitalized each period.
func generateCompoundSchedule(
	fd *FDRecord,
	cfg *BankConfig,
	_ *CompoundingFreq,
	tdsCfg *TDSConfig,
	_ DayCountInfo,
	_ InterestTypeInfo,
	calInfo HolidayCalendarInfo,
	effectiveConvention, effectiveDayCountCode string,
	decimals int,
	hasTDS bool,
	tdsDeductionTiming string,
) []CashflowRow {
	periodEnds := buildCapitalizationDates(fd, cfg)

	openingPrincipal := fd.PrincipalAmount
	periodStart := fd.ValueDate
	seq := 0
	var rows []CashflowRow
	var cumulativeInterest float64
	lastTDSYear := fd.ValueDate.Year()

	for _, periodEnd := range periodEnds {
		if !periodEnd.After(periodStart) {
			continue
		}

		divisor, days := getDivisorAndDaysWithCal(effectiveConvention, periodStart, periodEnd, cfg, calInfo)
		if days <= 0 {
			days = 1
		}
		if strings.EqualFold(cfg.PeriodBoundaryDefinition, "INCL_BOTH") {
			days++
		}

		ratePerDay := fd.InterestRate / (float64(divisor) * 100)
		interest := roundAmount(openingPrincipal*fd.InterestRate*float64(days)/float64(divisor)/100, decimals)
		cumulativeInterest += interest

		isCapBoundary := periodEnd.Before(fd.MaturityDate)
		isMaturity := periodEnd.Equal(fd.MaturityDate)

		eventType := "CAPITALIZATION"
		if isMaturity {
			eventType = "MATURITY"
		}

		// Annual TDS injection.
		if hasTDS && strings.Contains(tdsDeductionTiming, "ANNUAL") {
			if periodEnd.Year() > lastTDSYear || isMaturity {
				tdsOnPeriod := roundAmount(cumulativeInterest*tdsCfg.TDSRate/100, decimals)
				if tdsOnPeriod > 0 {
					seq++
					rows = append(rows, CashflowRow{
						PeriodNumber:     seq,
						EventType:        "TDS_DEDUCTION",
						EventDate:        time.Date(lastTDSYear, 3, 31, 0, 0, 0, 0, time.UTC),
						PeriodStartDate:  time.Date(lastTDSYear-1, 4, 1, 0, 0, 0, 0, time.UTC),
						PeriodEndDate:    time.Date(lastTDSYear, 3, 31, 0, 0, 0, 0, time.UTC),
						PeriodDays:       365,
						OpeningPrincipal: openingPrincipal,
						TDSAmount:        tdsOnPeriod,
						NetCashFlow:      -tdsOnPeriod,
						ClosingPrincipal: openingPrincipal,
						DayCountCode:     effectiveDayCountCode,
						Divisor:          divisor,
						FormulaUsed:      fmt.Sprintf("TDS = CumulativeInterest(%.2f) × %.4f%%", cumulativeInterest, tdsCfg.TDSRate),
					})
					cumulativeInterest = 0
					lastTDSYear = periodEnd.Year()
				}
			}
		}

		var tdsThisPeriod float64
		var capitalized float64
		netCashflow := 0.0
		closingPrincipal := openingPrincipal

		if isCapBoundary {
			if hasTDS && tdsDeductionTiming == "MATURITY" {
				tdsThisPeriod = 0
			} else if hasTDS && !strings.Contains(tdsDeductionTiming, "ANNUAL") {
				tdsThisPeriod = roundAmount(interest*tdsCfg.TDSRate/100, decimals)
			}
			capitalized = roundAmount(interest-tdsThisPeriod, decimals)
			closingPrincipal = roundAmount(openingPrincipal+capitalized, decimals)
			netCashflow = 0
		} else if isMaturity {
			if hasTDS && tdsDeductionTiming == "MATURITY" {
				tdsThisPeriod = roundAmount(cumulativeInterest*tdsCfg.TDSRate/100, decimals)
			} else if hasTDS && !strings.Contains(tdsDeductionTiming, "ANNUAL") {
				tdsThisPeriod = roundAmount(interest*tdsCfg.TDSRate/100, decimals)
			}
			netCashflow = roundAmount(openingPrincipal+interest-tdsThisPeriod, decimals)
			closingPrincipal = 0
		}

		seq++
		rows = append(rows, CashflowRow{
			PeriodNumber:      seq,
			EventType:         eventType,
			EventDate:         periodEnd,
			PeriodStartDate:   periodStart,
			PeriodEndDate:     periodEnd,
			PeriodDays:        days,
			OpeningPrincipal:  openingPrincipal,
			InterestAccrued:   interest,
			CapitalizedAmount: capitalized,
			ClosingPrincipal:  closingPrincipal,
			TDSAmount:         tdsThisPeriod,
			NetCashFlow:       netCashflow,
			DayCountCode:      effectiveDayCountCode,
			Divisor:           divisor,
			FormulaUsed:       fmt.Sprintf("P(%.2f) × r(%.4f%%) × d(%d) / D(%d) [%s]", openingPrincipal, fd.InterestRate, days, divisor, effectiveConvention),
			AccrualRatePerDay: ratePerDay,
		})

		if isCapBoundary {
			openingPrincipal = closingPrincipal
		}
		periodStart = periodEnd
	}

	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].EventDate.Equal(rows[j].EventDate) {
			return rows[i].PeriodNumber < rows[j].PeriodNumber
		}
		return rows[i].EventDate.Before(rows[j].EventDate)
	})
	for i := range rows {
		rows[i].PeriodNumber = i + 1
	}
	return rows
}

func GenerateCashflowForFD(ctx context.Context, exec queryExecutor, fdID string, confirmationID string) ([]CashflowRow, *FDRecord, error) {
	var fd *FDRecord
	var err error
	if strings.TrimSpace(fdID) != "" {
		fd, err = loadFDRecordByFDID(ctx, exec, fdID)
	} else {
		fd, err = loadFDRecord(ctx, exec, confirmationID)
	}
	if err != nil {
		return nil, nil, err
	}

	cfg, _ := loadBankConfig(ctx, exec, fd.BankConfigID)
	freq, _ := loadCompoundingFreq(ctx, exec, firstNonEmpty(fd.CompoundingFrequency, fd.InterestPayoutFrequency, fd.FrequencyID))
	tds, _ := loadTDSConfig(ctx, exec, fd.TDSPlanID)

	// Resolve day count convention from master — prefer fd's day_count_code, fall back to bank config's.
	dcRef := firstNonEmpty(fd.DayCountConvention, cfg.DayCountCode)
	dcInfo := loadDayCountConvention(ctx, exec, dcRef)

	// Resolve interest type calculation method from master.
	itInfo := loadInterestType(ctx, exec, fd.InterestTypeCode)

	// Load holiday calendar from bank config's holiday_calendar_code.
	calInfo := loadHolidayCalendar(ctx, exec, cfg.HolidayCalendarCode, fd.ValueDate, fd.MaturityDate)

	return generateCashflowSchedule(fd, cfg, freq, tds, dcInfo, itInfo, calInfo), fd, nil
}

func SaveCashflowSchedule(ctx context.Context, exec queryExecutor, fdID string, rows []CashflowRow) error {
	return SaveCashflowScheduleWithCreator(ctx, exec, fdID, rows, "system")
}

func SaveCashflowScheduleWithCreator(ctx context.Context, exec queryExecutor, fdID string, rows []CashflowRow, createdBy string) error {
	if strings.TrimSpace(fdID) == "" || len(rows) == 0 {
		return nil
	}
	if createdBy == "" {
		createdBy = "system"
	}

	table := resolveFirstExistingTable(ctx, exec, []string{
		"investment.fd_cashflow_schedule",
		"investment.fd_cashflow",
		"investment.fd_master_cashflow_schedule",
	})
	if table == "" {
		return nil
	}

	schemaName, tableName := splitQualifiedTable(table)
	cols, err := loadTableColumns(ctx, exec, schemaName, tableName)
	if err != nil {
		return err
	}
	fdCol := pickFirstExistingColumn(cols, "fd_id", "master_id")
	if fdCol == "" {
		return nil
	}

	// Delete existing rows first (clean regeneration).
	_, _ = exec.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, fdCol), fdID)

	for _, row := range rows {
		valueMap := map[string]interface{}{
			fdCol:                     fdID,
			"sequence_number":         row.PeriodNumber,
			"period_number":           row.PeriodNumber,
			"event_type":              row.EventType,
			"event_date":              row.EventDate,
			"cashflow_date":           row.EventDate,
			"period_start_date":       nilIfZero(row.PeriodStartDate),
			"period_end_date":         nilIfZero(row.PeriodEndDate),
			"period_days":             nilIfZero(row.PeriodDays),
			"opening_principal":       row.OpeningPrincipal,
			"interest_accrued":        row.InterestAccrued,
			"interest_amount":         row.InterestAccrued,
			"capitalized_amount":      row.CapitalizedAmount,
			"closing_principal":       row.ClosingPrincipal,
			"tds_amount":              row.TDSAmount,
			"net_cash_flow":           row.NetCashFlow,
			"net_cashflow":            row.NetCashFlow,
			"net_amount":              row.NetCashFlow,
			"day_count_code":          nilIfEmpty(row.DayCountCode),
			"divisor":                 nilIfZero(row.Divisor),
			"formula_used":            nilIfEmpty(row.FormulaUsed),
			"accrual_rate_per_day":    row.AccrualRatePerDay,
			"dr_account_code":         nilIfEmpty(row.DrAccountCode),
			"dr_account_name":         nilIfEmpty(row.DrAccountName),
			"cr_account_code":         nilIfEmpty(row.CrAccountCode),
			"cr_account_name":         nilIfEmpty(row.CrAccountName),
			"created_by":              createdBy,
			"created_at":              time.Now(),
		}
		preferredCols := []string{
			fdCol,
			"sequence_number", "period_number",
			"event_type",
			"event_date", "cashflow_date",
			"period_start_date", "period_end_date", "period_days",
			"opening_principal",
			"interest_accrued", "interest_amount",
			"capitalized_amount", "closing_principal",
			"tds_amount",
			"net_cash_flow", "net_cashflow", "net_amount",
			"day_count_code", "divisor", "formula_used", "accrual_rate_per_day",
			"dr_account_code", "dr_account_name",
			"cr_account_code", "cr_account_name",
			"created_by", "created_at",
		}
		insertSQL, args, _, ok := buildDynamicInsert(table, cols, preferredCols, valueMap, nil)
		if !ok {
			continue
		}
		if _, err := exec.Exec(ctx, insertSQL, args...); err != nil {
			return fmt.Errorf("save cashflow schedule: %w", err)
		}
	}

	return nil
}

// nilIfZero returns nil for time.Time zero values (so DB stores NULL not 0001-01-01).
func nilIfZero(v interface{}) interface{} {
	switch t := v.(type) {
	case time.Time:
		if t.IsZero() {
			return nil
		}
		return t
	case int:
		if t == 0 {
			return nil
		}
		return t
	}
	return v
}

// nilIfEmpty returns nil for empty strings.
func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
