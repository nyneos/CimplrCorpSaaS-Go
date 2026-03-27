package fdMaster

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
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
		CapitalizationDateAdjustment string  // NO_ADJUST / PRECEDING_WD / FOLLOWING_WD
		BrokenPeriodLocation         string  // FIRST / LAST / BOTH
		GracePeriodDays              int
		GracePeriodRateType          string  // SAME_RATE / SAVINGS_RATE / NO_INTEREST
		MinimumCompoundingPeriodDays int
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
		return &BankConfig{
			InterestRoundingDecimals: 2,
			RoundingMethod: "ROUND",
			RoundingFrequency: "EACH_PERIOD",
			WeekendAccrual: true,
			HolidayAccrual: true,
		}, nil
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
			COALESCE(broken_period_method, 'SIMPLE'),
			COALESCE(capitalization_date_adjustment, 'NO_ADJUST'),
			COALESCE(broken_period_location, 'LAST'),
			COALESCE(grace_period_days, 0),
			COALESCE(grace_period_rate_type, 'NO_INTEREST'),
			COALESCE(minimum_compounding_period_days, 0)
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
		&cfg.CapitalizationDateAdjustment,
		&cfg.BrokenPeriodLocation,
		&cfg.GracePeriodDays,
		&cfg.GracePeriodRateType,
		&cfg.MinimumCompoundingPeriodDays,
	)
	if err != nil {
		return &BankConfig{
			InterestRoundingDecimals: 2,
			RoundingMethod: "ROUND",
			RoundingFrequency: "EACH_PERIOD",
			WeekendAccrual: true,
			HolidayAccrual: true,
		}, nil
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

// adjustToWorkingDay moves a date to the preceding or following working day
// according to the bank config and holiday calendar. If adjustment is
// "NO_ADJUST" or calendar is empty, the original date is returned.
func adjustToWorkingDay(d time.Time, adjustment string, cfg *BankConfig, cal HolidayCalendarInfo) time.Time {
	adj := strings.ToUpper(strings.TrimSpace(adjustment))
	if adj == "" || adj == "NO_ADJUST" {
		return d
	}
	isNonWorking := func(t time.Time) bool {
		if !cfg.WeekendAccrual && cal.WeekendPattern != "" && isWeekend(t, cal.WeekendPattern) {
			return true
		}
		if !cfg.HolidayAccrual && len(cal.HolidayDates) > 0 {
			if cal.HolidayDates[t.Format("2006-01-02")] {
				return true
			}
		}
		return false
	}
	switch adj {
	case "FOLLOWING_WD":
		for isNonWorking(d) {
			d = d.AddDate(0, 0, 1)
		}
		return d
	case "PRECEDING_WD":
		for isNonWorking(d) {
			d = d.AddDate(0, 0, -1)
		}
		return d
	default:
		return d
	}
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
	// Respect accrual start/end conventions by adjusting the window.
	start := periodStart
	end := periodEnd
	if strings.EqualFold(cfg.AccrualStartConvention, "EXCLUDE") {
		start = start.AddDate(0, 0, 1)
	}
	if strings.EqualFold(cfg.AccrualEndConvention, "EXCLUDE") {
		end = end.AddDate(0, 0, -1)
	}
	if !end.After(start) {
		return 0
	}

	// If both flags are true (accrue on all days), just use calendar days.
	if cfg.WeekendAccrual && cfg.HolidayAccrual {
		return int(end.Sub(start).Hours() / 24)
	}
	// 30/360 convention: never use calendar-day counting; always use 30/360 formula.
	norm := strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(strings.TrimSpace(conventionType)))
	norm = strings.TrimPrefix(norm, "DC_")
	if norm == "30_360" {
		return int(periodEnd.Sub(periodStart).Hours() / 24) // 30_360 handled separately
	}
	// Count working days only.
	count := 0
	for cur := start; cur.Before(end); cur = cur.AddDate(0, 0, 1) {
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

// toFloat64 tries to convert an interface{} value to float64.
// Handles float64, float32, int, int32, int64, and string (parsed via strconv).
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case string:
		f, err := strconv.ParseFloat(val, 64)
		return f, err == nil
	}
	return 0, false
}

// roundByMethod applies the requested rounding method to value.
func roundByMethod(value float64, decimals int, method string) float64 {
	if decimals < 0 {
		decimals = 2
	}
	pow := math.Pow(10, float64(decimals))
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case "TRUNCATE":
		return math.Trunc(value*pow) / pow
	case "ROUND_UP":
		sign := math.Copysign(1, value)
		return sign * math.Ceil(math.Abs(value)*pow) / pow
	case "ROUND_DOWN":
		sign := math.Copysign(1, value)
		return sign * math.Floor(math.Abs(value)*pow) / pow
	default: // ROUND
		return math.Round(value*pow) / pow
	}
}

// applyRounding applies rounding according to method and frequency.
// If frequency is AT_MATURITY and this is not the final rounding, the value is returned unrounded.
func applyRounding(value float64, decimals int, method string, frequency string, isFinal bool) float64 {
	if strings.ToUpper(strings.TrimSpace(frequency)) == "AT_MATURITY" && !isFinal {
		return value
	}
	return roundByMethod(value, decimals, method)
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
func buildCapitalizationDates(fd *FDRecord, cfg *BankConfig, freq *CompoundingFreq, cal HolidayCalendarInfo) []time.Time {
	capType := strings.ToUpper(strings.TrimSpace(cfg.CapitalizationScheduleType))
	qDef := strings.ToUpper(strings.TrimSpace(cfg.QuarterDefinition))

	var dates []time.Time
	current := fd.ValueDate
	const maxPeriods = 1200 // 100 years of monthly — hard guard against infinite loops

	for iter := 0; iter < maxPeriods && current.Before(fd.MaturityDate); iter++ {
		var next time.Time

		switch capType {
		case "CALENDAR_QTR_END":
			// Find the next unadjusted quarter-end strictly after current.
			raw := nextCalendarQuarterEnd(current)
			if !raw.After(current) {
				raw = nextCalendarQuarterEnd(current.AddDate(0, 1, 0))
			}
			next = adjustToWorkingDay(raw, cfg.CapitalizationDateAdjustment, cfg, cal)
			// Guard: if adjustment moved next backwards to <= current (e.g. PRECEDING on a
			// weekend quarter-end), bump past the raw quarter-end and try the next one.
			if !next.After(current) {
				raw = nextCalendarQuarterEnd(raw.AddDate(0, 1, 0))
				next = adjustToWorkingDay(raw, cfg.CapitalizationDateAdjustment, cfg, cal)
			}
			// Last-resort safety — if still stuck, skip forward one quarter.
			if !next.After(current) {
				next = current.AddDate(0, 3, 1)
			}
		case "ANNIVERSARY":
			// Use DaysPerPeriod from freq when available, otherwise fall back to
			// configured 90-day behaviour or compute from periods-per-year.
			offsetDays := 91 // default quarterly fallback
			if freq != nil && freq.DaysPerPeriod > 0 {
				offsetDays = freq.DaysPerPeriod
			} else if qDef == "90_DAYS" {
				offsetDays = 90
			} else if freq != nil && freq.CompoundingPeriodsPerYear > 0 {
				offsetDays = 365 / freq.CompoundingPeriodsPerYear
			}
			next = current.AddDate(0, 0, offsetDays)
			next = adjustToWorkingDay(next, cfg.CapitalizationDateAdjustment, cfg, cal)
			if !next.After(current) {
				next = current.AddDate(0, 0, offsetDays+1)
			}
		case "MONTH_END":
			// Next month-end strictly after current.
			y, m, _ := current.Date()
			next = lastDayOfMonth(y, m)
			if !next.After(current) {
				m++
				if m > 12 {
					m = 1
					y++
				}
				next = lastDayOfMonth(y, m)
			}
			next = adjustToWorkingDay(next, cfg.CapitalizationDateAdjustment, cfg, cal)
			if !next.After(current) {
				next = current.AddDate(0, 1, 1)
			}
		case "FIXED_DAY":
			// Not implemented in v1 — treat same as ANNIVERSARY (quarterly)
			next = current.AddDate(0, 3, 0)
			next = adjustToWorkingDay(next, cfg.CapitalizationDateAdjustment, cfg, cal)
			if !next.After(current) {
				next = current.AddDate(0, 3, 1)
			}
		default:
			// Fallback: quarterly anniversary.
			next = current.AddDate(0, 3, 0)
			if !next.After(current) {
				next = current.AddDate(0, 3, 1)
			}
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

	// Resolve the human-readable frequency type if needed by callers.
	// Determine TDS deduction timing and TDS payout frequency.
	tdsDeductionTiming := ""
	if tdsCfg != nil {
		tdsDeductionTiming = strings.ToUpper(strings.TrimSpace(tdsCfg.DeductionTiming))
	}
	if tdsDeductionTiming == "" {
		tdsDeductionTiming = strings.ToUpper(strings.TrimSpace(cfg.TDSDeductionTiming))
	}
	hasTDS := tdsCfg != nil && tdsCfg.TDSRate > 0

	// tdsDeductionTiming is left for downstream logic (compound handler).

	// ── COMPOUND FDs: use capitalization dates ─────────────────────────────
	if isCompound {
		return generateCompoundSchedule(fd, cfg, freq, tdsCfg, dcInfo, itInfo, calInfo,
			effectiveConvention, effectiveDayCountCode, decimals, hasTDS, tdsDeductionTiming)
	}

	// BRD: Build ACCRUAL rows first (monthly), then CAPITALIZATION/TDS/MATURITY.
	var rows []CashflowRow

	// Build monthly accrual boundaries.
	accrualDates := buildMonthlyAccrualDates(fd)
	type accrualEntry struct {
		PeriodStart time.Time
		PeriodEnd   time.Time
		PeriodDays  int
		OpeningP    float64
		Interest    float64
		Divisor     int
	}

	accrualRows := make([]*accrualEntry, 0, len(accrualDates))
	// initial opening principal is fd.PrincipalAmount; for compound we'll update per cap window.
	for i, end := range accrualDates {
		var start time.Time
		if i == 0 {
			start = fd.ValueDate
		} else {
			start = accrualDates[i-1]
		}
		divisor, days := getDivisorAndDaysWithCal(effectiveConvention, start, end, cfg, calInfo)
		if days <= 0 {
			days = 1
		}
		if strings.EqualFold(cfg.PeriodBoundaryDefinition, "INCL_BOTH") {
			days++
		}
		ae := &accrualEntry{
			PeriodStart: start,
			PeriodEnd:   end,
			PeriodDays:  days,
			OpeningP:    fd.PrincipalAmount,
			Divisor:     divisor,
		}
		// interest with current OpeningP (base principal for now)
		raw := ae.OpeningP * fd.InterestRate * float64(ae.PeriodDays) / float64(ae.Divisor) / 100
		ae.Interest = applyRounding(raw, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, false)
		accrualRows = append(accrualRows, ae)
	}

	// If SIMPLE or AT_MATURITY — no capitalization. Build accrual rows + maturity + TDS.
	if !isCompound {
		// Append accrual rows
		for _, ae := range accrualRows {
			rows = append(rows, CashflowRow{
				EventType:         "ACCRUAL",
				EventDate:         ae.PeriodEnd,
				PeriodStartDate:   ae.PeriodStart,
				PeriodEndDate:     ae.PeriodEnd,
				PeriodDays:        ae.PeriodDays,
				OpeningPrincipal:  ae.OpeningP,
				InterestAccrued:   ae.Interest,
				CapitalizedAmount: 0,
				ClosingPrincipal:  ae.OpeningP,
				TDSAmount:         0,
				NetCashFlow:       0,
				DayCountCode:      effectiveDayCountCode,
				Divisor:           ae.Divisor,
				FormulaUsed:       fmt.Sprintf("P(%.2f) × r(%.4f%%) × d(%d) / D(%d) [%s]", ae.OpeningP, fd.InterestRate, ae.PeriodDays, ae.Divisor, effectiveConvention),
				AccrualRatePerDay: fd.InterestRate / (float64(ae.Divisor) * 100),
			})
		}

		// Determine payout frequency — if payouts are periodic, create INTEREST_RECEIPT events (non-cumulative)
		freqType := strings.ToUpper(strings.TrimSpace(firstNonEmpty(freq.FrequencyType, freq.FrequencyCode, fd.InterestPayoutFrequency, fd.FrequencyID)))
		payoutMonths := freqTypeToMonths(freqType)
		isNonCumulative := payoutMonths > 0

		if isNonCumulative {
			// Build payout boundaries and emit INTEREST_RECEIPT + per-payout TDS (if threshold crossed)
			payoutDates := buildPayoutDates(fd, payoutMonths)
			lastPayout := fd.ValueDate
			for _, payoutDate := range payoutDates {
				payoutInterest := 0.0
				for _, a := range accrualRows {
					if a.PeriodEnd.After(lastPayout) && !a.PeriodEnd.After(payoutDate) {
						payoutInterest += a.Interest
					}
				}
				if payoutInterest > 0 {
					tdsAmt := 0.0
					if hasTDS && payoutInterest >= tdsCfg.ThresholdAmount {
						tdsAmt = applyRounding(payoutInterest*tdsCfg.TDSRate/100, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, payoutDate.Equal(fd.MaturityDate))
					}
					rows = append(rows, CashflowRow{
						EventType: "INTEREST_RECEIPT",
						EventDate: payoutDate,
						PeriodStartDate: lastPayout,
						PeriodEndDate: payoutDate,
						PeriodDays: int(payoutDate.Sub(lastPayout).Hours()/24),
						OpeningPrincipal: fd.PrincipalAmount,
						InterestAccrued: payoutInterest,
						CapitalizedAmount: 0,
						ClosingPrincipal: fd.PrincipalAmount,
						TDSAmount: tdsAmt,
						NetCashFlow: applyRounding(payoutInterest-tdsAmt, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, payoutDate.Equal(fd.MaturityDate)),
						DayCountCode: effectiveDayCountCode,
					})
					if tdsAmt > 0 {
						rows = append(rows, CashflowRow{
							EventType: "TDS_DEDUCTION",
							EventDate: payoutDate,
							PeriodStartDate: lastPayout,
							PeriodEndDate: payoutDate,
							OpeningPrincipal: fd.PrincipalAmount,
							TDSAmount: tdsAmt,
							NetCashFlow: -tdsAmt,
							ClosingPrincipal: fd.PrincipalAmount,
							DayCountCode: effectiveDayCountCode,
						})
					}
				}
				lastPayout = payoutDate
			}

			// Maturity: return principal only (interest already paid out)
			rows = append(rows, CashflowRow{
				EventType: "MATURITY",
				EventDate: fd.MaturityDate,
				PeriodStartDate: lastPayout,
				PeriodEndDate: fd.MaturityDate,
				PeriodDays: int(fd.MaturityDate.Sub(lastPayout).Hours()/24),
				OpeningPrincipal: fd.PrincipalAmount,
				InterestAccrued: 0,
				CapitalizedAmount: 0,
				ClosingPrincipal: 0,
				TDSAmount: 0,
				NetCashFlow: fd.PrincipalAmount,
				DayCountCode: effectiveDayCountCode,
			})
		} else {
			// AT_MATURITY cumulative behaviour — TDS per FY if threshold crossed
			if hasTDS {
				// Map accruals into FY buckets
				fyMap := map[int]float64{}
				for _, a := range accrualRows {
					y := a.PeriodEnd.Year()
					fy := y
					if a.PeriodEnd.Month() >= time.April {
						fy = y + 1
					}
					fyMap[fy] += a.Interest
				}
				for fy, interestSum := range fyMap {
					if interestSum < tdsCfg.ThresholdAmount {
						continue
					}
					tdsAmt := applyRounding(interestSum*tdsCfg.TDSRate/100, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, true)
					// FY end date is Mar 31 of fy
					ed := time.Date(fy, time.March, 31, 0, 0, 0, 0, time.UTC)
					rows = append(rows, CashflowRow{
						EventType:       "TDS_DEDUCTION",
						EventDate:       ed,
						PeriodStartDate: time.Date(fy-1, time.April, 1, 0, 0, 0, 0, time.UTC),
						PeriodEndDate:   ed,
						PeriodDays:      365,
						OpeningPrincipal: fd.PrincipalAmount,
						TDSAmount:        tdsAmt,
						NetCashFlow:      -tdsAmt,
						ClosingPrincipal: fd.PrincipalAmount,
						DayCountCode:     effectiveDayCountCode,
					})
				}
			}

			// Maturity row
			var totalInterest float64
			for _, a := range accrualRows {
				totalInterest += a.Interest
			}
			tdsAtMaturity := 0.0
			if hasTDS && (strings.ToUpper(cfg.TDSDeductionTiming) == "MATURITY" || strings.ToUpper(tdsDeductionTiming) == "MATURITY") {
				if totalInterest >= tdsCfg.ThresholdAmount {
					tdsAtMaturity = applyRounding(totalInterest*tdsCfg.TDSRate/100, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, true)
				}
			}
			net := applyRounding(fd.PrincipalAmount+totalInterest-tdsAtMaturity, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, true)
			rows = append(rows, CashflowRow{
				EventType:         "MATURITY",
				EventDate:         fd.MaturityDate,
				PeriodStartDate:   accrualDates[len(accrualDates)-1],
				PeriodEndDate:     fd.MaturityDate,
				PeriodDays:        int(fd.MaturityDate.Sub(accrualDates[len(accrualDates)-1]).Hours()/24),
				OpeningPrincipal:  fd.PrincipalAmount,
				InterestAccrued:   totalInterest,
				CapitalizedAmount: 0,
				ClosingPrincipal:  0,
				TDSAmount:         tdsAtMaturity,
				NetCashFlow:       net,
				DayCountCode:      effectiveDayCountCode,
			})
		}

			// Sort and renumber per BRD ordering
			sort.SliceStable(rows, func(i, j int) bool {
				if rows[i].EventDate.Equal(rows[j].EventDate) {
					order := map[string]int{"ACCRUAL": 1, "INTEREST_RECEIPT": 2, "CAPITALIZATION": 3, "TDS_DEDUCTION": 4, "MATURITY": 5}
					return order[rows[i].EventType] < order[rows[j].EventType]
				}
				return rows[i].EventDate.Before(rows[j].EventDate)
			})
		for i := range rows {
			rows[i].PeriodNumber = i + 1
		}
		return rows
	}
    // Fallback return to satisfy compiler (should be unreachable).
    return nil
}

// generateCompoundSchedule handles COMPOUND FDs — interest is capitalized each period.
func generateCompoundSchedule(
	fd *FDRecord,
	cfg *BankConfig,
	freq *CompoundingFreq,
	tdsCfg *TDSConfig,
	_ DayCountInfo,
	_ InterestTypeInfo,
	calInfo HolidayCalendarInfo,
	effectiveConvention, effectiveDayCountCode string,
	decimals int,
	hasTDS bool,
	tdsDeductionTiming string,
) []CashflowRow {
	// Build monthly accrual entries first
	accrualDates := buildMonthlyAccrualDates(fd)
	type aEntry struct{
		PeriodStart time.Time
		PeriodEnd time.Time
		PeriodDays int
		Divisor int
		OpeningP float64
		Interest float64
	}
	accrualRows := make([]*aEntry, 0, len(accrualDates))
	for i, end := range accrualDates {
		var start time.Time
		if i == 0 { start = fd.ValueDate } else { start = accrualDates[i-1] }
		divisor, days := getDivisorAndDaysWithCal(effectiveConvention, start, end, cfg, calInfo)
		if days <= 0 { days = 1 }
		if strings.EqualFold(cfg.PeriodBoundaryDefinition, "INCL_BOTH") { days++ }
		accrualRows = append(accrualRows, &aEntry{PeriodStart:start, PeriodEnd:end, PeriodDays:days, Divisor:divisor})
	}

	periodEnds := buildCapitalizationDates(fd, cfg, freq, calInfo)
	openingPrincipal := fd.PrincipalAmount
	seq := 0
	var rows []CashflowRow
	var cumulativeInterest float64

	// Initialize FY TDS end
	fyEndYear := fd.ValueDate.Year()
	if fd.ValueDate.Month() >= time.April { fyEndYear++ }
	lastTDSFYEnd := time.Date(fyEndYear, time.March, 31,0,0,0,0,time.UTC)

	lastCapDate := fd.ValueDate
	for _, capEnd := range periodEnds {
		if !capEnd.After(lastCapDate) { continue }

		// collect accruals within (lastCapDate <= a.PeriodStart) && (a.PeriodEnd <= capEnd)
		var capInterest float64
		accrualCountInWindow := 0
		capDivisor := 0
		capAccrualRate := 0.0
		for _, a := range accrualRows {
			if (a.PeriodStart.Equal(lastCapDate) || a.PeriodStart.After(lastCapDate)) && !a.PeriodEnd.After(capEnd) {
				a.OpeningP = openingPrincipal
				raw := a.OpeningP * fd.InterestRate * float64(a.PeriodDays) / float64(a.Divisor) / 100
				a.Interest = applyRounding(raw, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, false)
				capInterest += a.Interest
				cumulativeInterest += a.Interest
				accrualCountInWindow++
				if capDivisor == 0 && a.Divisor > 0 {
					capDivisor = a.Divisor
					capAccrualRate = fd.InterestRate / (float64(capDivisor) * 100)
				}
			}
		}

		isCapBoundary := capEnd.Before(fd.MaturityDate)
		isMaturity := capEnd.Equal(fd.MaturityDate)

		// FIX 3: compute TDS first, then capitalized = interest - TDS (not just interest)
		tdsThisPeriod := 0.0
		tdsTiming := strings.ToUpper(strings.TrimSpace(tdsDeductionTiming))
		if hasTDS {
			switch {
			case strings.Contains(tdsTiming, "ANNUAL"):
				if (capEnd.After(lastTDSFYEnd) || isMaturity) && cumulativeInterest >= tdsCfg.ThresholdAmount {
					tdsThisPeriod = applyRounding(cumulativeInterest*tdsCfg.TDSRate/100, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, isMaturity)
				}
			case strings.Contains(tdsTiming, "MATURITY"):
				if isMaturity && cumulativeInterest >= tdsCfg.ThresholdAmount {
					tdsThisPeriod = applyRounding(cumulativeInterest*tdsCfg.TDSRate/100, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, true)
				}
			default: // RECEIPT / EACH_PERIOD — TDS every cap period
				if capInterest >= tdsCfg.ThresholdAmount {
					tdsThisPeriod = applyRounding(capInterest*tdsCfg.TDSRate/100, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, isMaturity)
				}
			}
		}

		// FIX 3: capitalizedAmount = interest - TDS (what actually joins the principal)
		capitalized := applyRounding(capInterest-tdsThisPeriod, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, isMaturity)
		closingPrincipal := openingPrincipal
		if isCapBoundary {
			closingPrincipal = applyRounding(openingPrincipal+capitalized, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, isMaturity)
		}

		eventType := "CAPITALIZATION"
		if isMaturity {
			eventType = "MATURITY"
		}
		capDays := int(capEnd.Sub(lastCapDate).Hours() / 24)

		// FIX 2 & 4: CAPITALIZATION/MATURITY row — TDSAmount always 0; separate TDS row below.
		seq++
		maturityNetCF := 0.0
		if isMaturity {
			maturityNetCF = applyRounding(closingPrincipal, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, true)
		}
		rows = append(rows, CashflowRow{
			PeriodNumber:      seq,
			EventType:         eventType,
			EventDate:         capEnd,
			PeriodStartDate:   lastCapDate,
			PeriodEndDate:     capEnd,
			PeriodDays:        capDays,
			OpeningPrincipal:  openingPrincipal,
			InterestAccrued:   capInterest,
			CapitalizedAmount: capitalized,
			ClosingPrincipal:  func() float64 { if isMaturity { return 0 }; return closingPrincipal }(),
			TDSAmount:         0,
			NetCashFlow:       maturityNetCF,
			DayCountCode:      effectiveDayCountCode,
			Divisor:           capDivisor,
			FormulaUsed:       fmt.Sprintf("CAP_SUM: P(%.2f), %d accruals=%.2f, tds=%.2f, cap=%.2f", openingPrincipal, accrualCountInWindow, capInterest, tdsThisPeriod, capitalized),
			AccrualRatePerDay: capAccrualRate,
		})

		// FIX 2: emit separate TDS_DEDUCTION row
		if tdsThisPeriod > 0 {
			seq++
			tdsEventDate := capEnd
			if strings.Contains(tdsTiming, "ANNUAL") {
				tdsEventDate = lastTDSFYEnd
			}
			rows = append(rows, CashflowRow{
				PeriodNumber:     seq,
				EventType:        "TDS_DEDUCTION",
				EventDate:        tdsEventDate,
				PeriodStartDate:  lastCapDate,
				PeriodEndDate:    capEnd,
				PeriodDays:       capDays,
				OpeningPrincipal: openingPrincipal,
				TDSAmount:        tdsThisPeriod,
				NetCashFlow:      -tdsThisPeriod,
				ClosingPrincipal: closingPrincipal,
				DayCountCode:     effectiveDayCountCode,
				FormulaUsed:      fmt.Sprintf("TDS = %.2f × %.4f%%", capInterest, tdsCfg.TDSRate),
			})
			if strings.Contains(tdsTiming, "ANNUAL") {
				cumulativeInterest = 0
				lastTDSFYEnd = lastTDSFYEnd.AddDate(1, 0, 0)
			} else if isMaturity {
				cumulativeInterest = 0
			}
		}

		// advance
		openingPrincipal = closingPrincipal
		lastCapDate = capEnd
	}

	// FIX 1: fix up accruals that didn't get principal set (post-last-cap rows), then append ALL
	for _, a := range accrualRows {
		if a.OpeningP == 0 {
			a.OpeningP = openingPrincipal
			raw := a.OpeningP * fd.InterestRate * float64(a.PeriodDays) / float64(a.Divisor) / 100
			a.Interest = applyRounding(raw, decimals, cfg.RoundingMethod, cfg.RoundingFrequency, false)
		}
	}
	for _, a := range accrualRows {
		seq++
		rows = append(rows, CashflowRow{
			PeriodNumber:      seq,
			EventType:         "ACCRUAL",
			EventDate:         a.PeriodEnd,
			PeriodStartDate:   a.PeriodStart,
			PeriodEndDate:     a.PeriodEnd,
			PeriodDays:        a.PeriodDays,
			OpeningPrincipal:  a.OpeningP,
			InterestAccrued:   a.Interest,
			CapitalizedAmount: 0,
			ClosingPrincipal:  a.OpeningP,
			TDSAmount:         0,
			NetCashFlow:       0,
			DayCountCode:      effectiveDayCountCode,
			Divisor:           a.Divisor,
			FormulaUsed:       fmt.Sprintf("P(%.2f) × r(%.4f%%) × d(%d) / D(%d) [%s]", a.OpeningP, fd.InterestRate, a.PeriodDays, a.Divisor, effectiveConvention),
			AccrualRatePerDay: fd.InterestRate / (float64(a.Divisor) * 100),
		})
	}

	// Final sort and renumber
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].EventDate.Equal(rows[j].EventDate) {
			order := map[string]int{"ACCRUAL": 1, "INTEREST_RECEIPT": 2, "CAPITALIZATION": 3, "TDS_DEDUCTION": 4, "MATURITY": 5}
			return order[rows[i].EventType] < order[rows[j].EventType]
		}
		return rows[i].EventDate.Before(rows[j].EventDate)
	})
	for i := range rows { rows[i].PeriodNumber = i+1 }
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
	return GenerateCashflowFromRecord(ctx, exec, fd)
}

// GenerateCashflowFromRecord generates cashflow rows from an already-loaded FDRecord,
// skipping the DB load step. Use this when the record is already in memory (e.g. ActivateFD)
// to avoid a redundant DB round-trip inside an open transaction.
func GenerateCashflowFromRecord(ctx context.Context, exec queryExecutor, fd *FDRecord) ([]CashflowRow, *FDRecord, error) {
	if fd == nil {
		return nil, nil, fmt.Errorf("GenerateCashflowFromRecord: nil FDRecord")
	}

	tg := time.Now()
	log.Printf("[CFGEN][%s] ► start bankConfigID=%q tdsID=%q freqID=%q itCode=%q dcCode=%q",
		fd.ConfirmationID, fd.BankConfigID, fd.TDSPlanID,
		firstNonEmpty(fd.CompoundingFrequency, fd.InterestPayoutFrequency, fd.FrequencyID),
		fd.InterestTypeCode, fd.DayCountConvention)

	cfg, _ := loadBankConfig(ctx, exec, fd.BankConfigID)
	log.Printf("[CFGEN][%s] ✓ loadBankConfig (+%s) calCode=%q dcCode=%q", fd.ConfirmationID, time.Since(tg).Round(time.Millisecond), cfg.HolidayCalendarCode, cfg.DayCountCode)

	t1 := time.Now()
	freq, _ := loadCompoundingFreq(ctx, exec, firstNonEmpty(fd.CompoundingFrequency, fd.InterestPayoutFrequency, fd.FrequencyID))
	log.Printf("[CFGEN][%s] ✓ loadCompoundingFreq (+%s)", fd.ConfirmationID, time.Since(t1).Round(time.Millisecond))

	t2 := time.Now()
	tds, _ := loadTDSConfig(ctx, exec, fd.TDSPlanID)
	log.Printf("[CFGEN][%s] ✓ loadTDSConfig (+%s)", fd.ConfirmationID, time.Since(t2).Round(time.Millisecond))

	// Resolve day count convention from master — prefer fd's day_count_code, fall back to bank config's.
	dcRef := firstNonEmpty(fd.DayCountConvention, cfg.DayCountCode)
	t3 := time.Now()
	dcInfo := loadDayCountConvention(ctx, exec, dcRef)
	log.Printf("[CFGEN][%s] ✓ loadDayCountConvention (+%s)", fd.ConfirmationID, time.Since(t3).Round(time.Millisecond))

	// Resolve interest type calculation method from master.
	t4 := time.Now()
	itInfo := loadInterestType(ctx, exec, fd.InterestTypeCode)
	log.Printf("[CFGEN][%s] ✓ loadInterestType (+%s)", fd.ConfirmationID, time.Since(t4).Round(time.Millisecond))

	// Load holiday calendar from bank config's holiday_calendar_code.
	t5 := time.Now()
	calInfo := loadHolidayCalendar(ctx, exec, cfg.HolidayCalendarCode, fd.ValueDate, fd.MaturityDate)
	log.Printf("[CFGEN][%s] ✓ loadHolidayCalendar (+%s) holidays=%d", fd.ConfirmationID, time.Since(t5).Round(time.Millisecond), len(calInfo.HolidayDates))

	rows := generateCashflowSchedule(fd, cfg, freq, tds, dcInfo, itInfo, calInfo)
	log.Printf("[CFGEN][%s] ✓ generateCashflowSchedule → %d rows TOTAL (+%s)", fd.ConfirmationID, len(rows), time.Since(tg).Round(time.Millisecond))
	return rows, fd, nil
}

func SaveCashflowSchedule(ctx context.Context, exec queryExecutor, fdID string, rows []CashflowRow) error {
	return SaveCashflowScheduleWithCreator(ctx, exec, fdID, rows, "system")
}

// SaveCashflowScheduleWithRecord is like SaveCashflowScheduleWithCreator but accepts
// an already-loaded FDRecord so it can skip the two extra in-tx DB queries
// (FOR UPDATE lock + SELECT master fields) that would otherwise run inside the transaction.
// Use this from ActivateFD where rec is already in memory and the FD was just inserted
// (no concurrent regeneration possible).
func SaveCashflowScheduleWithRecord(ctx context.Context, exec queryExecutor, fdID string, rows []CashflowRow, createdBy string, rec *FDRecord) error {
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

	// Use master fields from the pre-loaded record — no extra DB query needed.
	var masterEntityID, masterEntityName, masterBankID, masterBankName, masterSourceAccount string
	if rec != nil {
		masterEntityID = rec.EntityID
		masterBankID = rec.BankID
		masterSourceAccount = rec.BankAccountID
	}

	return saveCashflowBatch(ctx, exec, table, fdCol, cols, fdID, rows, createdBy,
		masterEntityID, masterEntityName, masterBankID, masterBankName, masterSourceAccount)
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

	// Row-level lock: prevent concurrent cashflow regeneration for the same FD.
	// This is only effective when exec is a transaction (tx); pool-based callers accept the no-op.
	var lockCheck string
	if lockErr := exec.QueryRow(ctx,
		`SELECT fd_id FROM investment.fd_master WHERE fd_id = $1 FOR UPDATE NOWAIT`,
		fdID,
	).Scan(&lockCheck); lockErr != nil {
		return fmt.Errorf("cashflow regeneration lock failed for fd %s — already in progress or fd not found: %w", fdID, lockErr)
	}

	// Fetch FD master details to populate cosmetic fields on cashflow rows when available.
	var masterEntityID, masterEntityName, masterBankID, masterBankName, masterSourceAccount string
	_ = exec.QueryRow(ctx, `SELECT COALESCE(entity_id,''), COALESCE(entity_name,''), COALESCE(bank_id,''), COALESCE(bank_name,''), COALESCE(source_account_id,'') FROM investment.fd_master WHERE fd_id = $1 LIMIT 1`, fdID).Scan(&masterEntityID, &masterEntityName, &masterBankID, &masterBankName, &masterSourceAccount)

	return saveCashflowBatch(ctx, exec, table, fdCol, cols, fdID, rows, createdBy,
		masterEntityID, masterEntityName, masterBankID, masterBankName, masterSourceAccount)
}

// saveCashflowBatch is the shared batch-INSERT engine used by both
// SaveCashflowScheduleWithCreator and SaveCashflowScheduleWithRecord.
func saveCashflowBatch(ctx context.Context, exec queryExecutor, table, fdCol string, cols map[string]bool, fdID string, rows []CashflowRow, createdBy string,
	masterEntityID, masterEntityName, masterBankID, masterBankName, masterSourceAccount string,
) error {
	_, _ = exec.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, fdCol), fdID)

	// ── Determine the fixed column list for the batch INSERT ───────────────
	// We resolve which columns exist ONCE here, then reuse for every row.
	// This avoids the old per-row pattern of N separate INSERT round-trips.
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
	// Optional cosmetic columns — only include if they exist in the schema.
	optionalCols := []string{"bank_id", "bank_name", "entity_id", "entity_name", "source_account_id", "posting_status", "bank_confirmed", "bank_reference"}
	for _, oc := range optionalCols {
		if cols[oc] {
			preferredCols = append(preferredCols, oc)
		}
	}

	// Build the final ordered insert column list using the first row as a probe.
	// We call buildDynamicInsert on row[0] just to get the canonical insertCols
	// order, then reuse that order for all subsequent rows in the batch.
	now := time.Now()
	makeValueMap := func(row CashflowRow) map[string]interface{} {
		vm := map[string]interface{}{
			fdCol:                  fdID,
			"sequence_number":     row.PeriodNumber,
			"period_number":       row.PeriodNumber,
			"event_type":         row.EventType,
			"event_date":         row.EventDate,
			"cashflow_date":      row.EventDate,
			"period_start_date":  nilIfZero(row.PeriodStartDate),
			"period_end_date":    nilIfZero(row.PeriodEndDate),
			"period_days":        nilIfZero(row.PeriodDays),
			"opening_principal":  row.OpeningPrincipal,
			"interest_accrued":   row.InterestAccrued,
			"interest_amount":    row.InterestAccrued,
			"capitalized_amount": row.CapitalizedAmount,
			"closing_principal":  row.ClosingPrincipal,
			"tds_amount":         row.TDSAmount,
			"net_cash_flow":      row.NetCashFlow,
			"net_cashflow":       row.NetCashFlow,
			"net_amount":         row.NetCashFlow,
			"day_count_code":     nilIfEmpty(row.DayCountCode),
			"divisor":            nilIfZero(row.Divisor),
			"formula_used":       nilIfEmpty(row.FormulaUsed),
			"accrual_rate_per_day": row.AccrualRatePerDay,
			"dr_account_code":    nilIfEmpty(row.DrAccountCode),
			"dr_account_name":    nilIfEmpty(row.DrAccountName),
			"cr_account_code":    nilIfEmpty(row.CrAccountCode),
			"cr_account_name":    nilIfEmpty(row.CrAccountName),
			"created_by":         createdBy,
			"created_at":         now,
		}
		if cols["bank_id"] { vm["bank_id"] = masterBankID }
		if cols["bank_name"] { vm["bank_name"] = masterBankName }
		if cols["entity_id"] { vm["entity_id"] = masterEntityID }
		if cols["entity_name"] { vm["entity_name"] = masterEntityName }
		if cols["source_account_id"] { vm["source_account_id"] = masterSourceAccount }
		if cols["posting_status"] { vm["posting_status"] = "NOT_POSTED" }
		if cols["bank_confirmed"] { vm["bank_confirmed"] = false }
		if cols["bank_reference"] { vm["bank_reference"] = "" }
		return vm
	}

	// ── Batch INSERT: collect all row value-sets and fire ONE query ────────
	// Postgres supports: INSERT INTO t (c1,c2,...) VALUES ($1,$2,...),($3,$4,...)
	// We build the column list from the first row, then append value-tuples for each.
	// Postgres limit is ~65535 bind parameters; for safety batch at 500 params / insert.
	const maxParams = 60000

	// Determine column list once.
	var insertCols []string
	for _, col := range preferredCols {
		if cols[col] {
			insertCols = append(insertCols, col)
		}
	}
	if len(insertCols) == 0 {
		return fmt.Errorf("save cashflow schedule: no valid columns found")
	}
	colsPerRow := len(insertCols)
	maxRowsPerBatch := maxParams / colsPerRow
	if maxRowsPerBatch < 1 {
		maxRowsPerBatch = 1
	}

	for batchStart := 0; batchStart < len(rows); batchStart += maxRowsPerBatch {
		batchEnd := batchStart + maxRowsPerBatch
		if batchEnd > len(rows) {
			batchEnd = len(rows)
		}
		batch := rows[batchStart:batchEnd]

		var valueTuples []string
		var batchArgs []interface{}
		paramIdx := 1

		for _, row := range batch {
			vm := makeValueMap(row)
			placeholders := make([]string, 0, colsPerRow)
			for _, col := range insertCols {
				placeholders = append(placeholders, fmt.Sprintf("$%d", paramIdx))
				batchArgs = append(batchArgs, vm[col])
				paramIdx++
			}
			valueTuples = append(valueTuples, "("+strings.Join(placeholders, ",")+")")
		}

		batchSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s",
			table,
			strings.Join(insertCols, ","),
			strings.Join(valueTuples, ","),
		)
		if _, err := exec.Exec(ctx, batchSQL, batchArgs...); err != nil {
			return fmt.Errorf("save cashflow schedule batch [%d-%d]: %w", batchStart, batchEnd-1, err)
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
