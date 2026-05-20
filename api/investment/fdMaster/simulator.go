package fdMaster

// simulator.go — Cashflow Simulation Engine
//
// Provides two reusable entry points:
//
//  1. GetHolidayListForRange(ctx, exec, calCode, from, to)
//     Returns every non-working date (holidays + weekends) for a calendar
//     and date range, expanding rrule recurrences from masterholiday.
//     Can be called by any handler; also exposed as HTTP handler
//     GET/POST /investment/fd/simulator/holidays
//
//  2. IsWorkingDay(date, calInfo)
//     Pure in-memory check — given a pre-loaded HolidayCalendarInfo returns
//     true when the date is neither a weekend nor a holiday.
//
//  3. SimulateCashflow — HTTP handler POST /investment/fd/simulator/cashflow
//     Accepts a full FD description (principal, rate, tenor, bank_config_id,
//     tds_plan_id, frequency_id, interest_type_code, day_count_code, start_date)
//     and returns a computed cashflow schedule without touching fd_booking_request
//     or fd_master.  Nothing is persisted.
//
// All the heavy lifting (holiday expansion, day-count, broken periods,
// capitalisation dates, TDS, grace period) is delegated to the existing
// generateCashflowSchedule / loadXxx helpers in cashflow.go so this file
// stays thin and does not duplicate logic.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/rounding"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Public helpers — usable from any package via fdMaster.Xxx
// ─────────────────────────────────────────────────────────────────────────────

// HolidayEntry is a single non-working date returned by GetHolidayListForRange.
type HolidayEntry struct {
	Date        string `json:"date"`        // YYYY-MM-DD
	Type        string `json:"type"`        // "HOLIDAY" | "WEEKEND"
	Description string `json:"description"` // holiday name or weekday name
}

// GetHolidayListForRange expands the full list of non-working dates for a
// named calendar between from and to (inclusive).
//
// Holidays are loaded from investment.masterholiday (with rrule expansion);
// weekends are derived from the calendar's weekend_pattern.
//
// Returns the sorted slice.  Safe to call concurrently.
func GetHolidayListForRange(
	ctx context.Context,
	exec queryExecutor,
	calCode string,
	from, to time.Time,
) ([]HolidayEntry, error) {
	if from.IsZero() || to.IsZero() || !to.After(from) {
		return nil, fmt.Errorf("invalid date range: from=%s to=%s", from.Format(constants.DateFormat), to.Format(constants.DateFormat))
	}

	cal := loadHolidayCalendar(ctx, exec, calCode, from, to)

	// Build a set of holiday date strings for description lookup.
	// We need the holiday name, so re-query just for the display names.
	holidayNames := map[string]string{} // date → holiday_name
	{
		// Load calendar_id first (same logic as loadHolidayCalendar).
		var calendarID string
		var weekendPattern string
		err := exec.QueryRow(ctx, `
			SELECT calendar_id, COALESCE(weekend_pattern, 'Sat,Sun')
			FROM investment.mastercalendar
			WHERE calendar_code = $1
			  AND COALESCE(is_deleted, false) = false
			  AND UPPER(status) = 'ACTIVE'
			LIMIT 1
		`, calCode).Scan(&calendarID, &weekendPattern)
		if err != nil {
			// try by id
			_ = exec.QueryRow(ctx, `
				SELECT calendar_id, COALESCE(weekend_pattern, 'Sat,Sun')
				FROM investment.mastercalendar
				WHERE calendar_id = $1
				  AND COALESCE(is_deleted, false) = false
				LIMIT 1
			`, calCode).Scan(&calendarID, &weekendPattern)
		}

		if calendarID != "" {
			windowFrom := from.AddDate(-1, 0, 0)
			windowTo := to.AddDate(1, 0, 0)
			rows, err := exec.Query(ctx, `
				SELECT holiday_date,
				       COALESCE(recurrence_rule, ''),
				       COALESCE(holiday_name, '')
				FROM investment.masterholiday
				WHERE calendar_id = $1
				  AND COALESCE(is_deleted, false) = false
				  AND UPPER(status) = 'ACTIVE'
			`, calendarID)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var seed time.Time
					var rrule, name string
					if rows.Scan(&seed, &rrule, &name) == nil {
						expanded := expandRRule(seed, rrule, windowFrom, windowTo)
						for _, d := range expanded {
							key := d.Format(constants.DateFormat)
							if holidayNames[key] == "" {
								if name != "" {
									holidayNames[key] = name
								} else {
									holidayNames[key] = "Holiday"
								}
							}
						}
					}
				}
			}
		}
	}

	var out []HolidayEntry

	for cur := from; !cur.After(to); cur = cur.AddDate(0, 0, 1) {
		key := cur.Format(constants.DateFormat)

		if name, ok := holidayNames[key]; ok {
			out = append(out, HolidayEntry{Date: key, Type: "HOLIDAY", Description: name})
			continue
		}

		if cal.WeekendPattern != "" && isWeekend(cur, cal.WeekendPattern) {
			out = append(out, HolidayEntry{Date: key, Type: "WEEKEND", Description: cur.Weekday().String()})
		}
	}

	return out, nil
}

// IsWorkingDay returns true when date is neither a weekend (per cal.WeekendPattern)
// nor a listed holiday (per cal.HolidayDates).
// The HolidayCalendarInfo must already be loaded via loadHolidayCalendar.
func IsWorkingDay(date time.Time, cal HolidayCalendarInfo) bool {
	key := date.Format(constants.DateFormat)
	if cal.HolidayDates[key] {
		return false
	}
	if cal.WeekendPattern != "" && isWeekend(date, cal.WeekendPattern) {
		return false
	}
	return true
}

// NextWorkingDay returns the next working day on or after date.
func NextWorkingDay(date time.Time, cal HolidayCalendarInfo) time.Time {
	d := date
	for i := 0; i < 30; i++ { // guard: max 30 days forward
		if IsWorkingDay(d, cal) {
			return d
		}
		d = d.AddDate(0, 0, 1)
	}
	return date // fallback: return original if stuck
}

// PrevWorkingDay returns the nearest working day on or before date.
func PrevWorkingDay(date time.Time, cal HolidayCalendarInfo) time.Time {
	d := date
	for i := 0; i < 30; i++ {
		if IsWorkingDay(d, cal) {
			return d
		}
		d = d.AddDate(0, 0, -1)
	}
	return date
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP handler — GET/POST /investment/fd/simulator/holidays
// ─────────────────────────────────────────────────────────────────────────────

// HolidayListRequest is the JSON/query body for the holidays endpoint.
//
// Two modes:
//
//  1. Range mode  — provide from + to → returns the full list of non-working days.
//  2. Single-date mode — provide date alone (omit from/to) → returns a status
//     object for that one date: is_holiday, is_weekend, is_working_day,
//     next_working_day, prev_working_day.
type HolidayListRequest struct {
	CalendarCode string `json:"calendar_code"` // required always
	Date         string `json:"date"`          // YYYY-MM-DD — single-date mode
	From         string `json:"from"`          // YYYY-MM-DD — range mode
	To           string `json:"to"`            // YYYY-MM-DD — range mode
}

// GetHolidayListHandler handles both modes based on which fields are provided.
//
// Range mode:
//
//	POST /investment/fd/simulator/holidays
//	{"calendar_code":"TESTING_CALENDAR","from":"2025-01-01","to":"2025-12-31"}
//	→ {calendar_code, from, to, total_days, working_days, holiday_count, holidays:[...]}
//
// Single-date mode:
//
//	POST /investment/fd/simulator/holidays
//	{"calendar_code":"TESTING_CALENDAR","date":"2025-01-26"}
//	→ {date, is_holiday, is_weekend, is_working_day, holiday_name, next_working_day, prev_working_day}
func GetHolidayListHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req HolidayListRequest

		// Support both GET querystring and POST JSON body.
		if r.Method == http.MethodGet {
			req.CalendarCode = r.URL.Query().Get("calendar_code")
			req.Date = r.URL.Query().Get("date")
			req.From = r.URL.Query().Get("from")
			req.To = r.URL.Query().Get("to")
		} else {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
				return
			}
		}

		req.CalendarCode = strings.TrimSpace(req.CalendarCode)
		if req.CalendarCode == "" {
			api.RespondWithError(w, http.StatusBadRequest, "calendar_code is required")
			return
		}

		ctx := r.Context()
		conn, err := pool.Acquire(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusServiceUnavailable, constants.ErrDBConnection)
			return
		}
		defer conn.Release()

		// ── Single-date mode ──────────────────────────────────────────────
		if dateStr := strings.TrimSpace(req.Date); dateStr != "" && req.From == "" && req.To == "" {
			checkDate, err := time.Parse(constants.DateFormat, dateStr)
			if err != nil {
				api.RespondWithError(w, http.StatusBadRequest, "date must be YYYY-MM-DD: "+dateStr)
				return
			}

			// Load calendar with a narrow 3-year window around the check date.
			calInfo := loadHolidayCalendar(ctx, conn, req.CalendarCode,
				checkDate.AddDate(-1, 0, 0), checkDate.AddDate(1, 0, 0))

			isHol := calInfo.HolidayDates[checkDate.Format(constants.DateFormat)]
			isWknd := calInfo.WeekendPattern != "" && isWeekend(checkDate, calInfo.WeekendPattern)
			isWorking := !isHol && !isWknd

			// Resolve holiday name (re-query for display).
			holName := ""
			if isHol {
				entries, _ := GetHolidayListForRange(ctx, conn, req.CalendarCode, checkDate, checkDate)
				for _, e := range entries {
					if e.Type == "HOLIDAY" {
						holName = e.Description
						break
					}
				}
			}

			next := NextWorkingDay(checkDate.AddDate(0, 0, 1), calInfo)
			prev := PrevWorkingDay(checkDate.AddDate(0, 0, -1), calInfo)

			api.RespondWithPayload(w, true, "", map[string]interface{}{
				"calendar_code":    req.CalendarCode,
				"date":             dateStr,
				"is_holiday":       isHol,
				"is_weekend":       isWknd,
				"is_working_day":   isWorking,
				"holiday_name":     holName,
				"next_working_day": next.Format(constants.DateFormat),
				"prev_working_day": prev.Format(constants.DateFormat),
			})
			return
		}

		// ── Range mode ────────────────────────────────────────────────────
		from, err := time.Parse(constants.DateFormat, strings.TrimSpace(req.From))
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "from must be YYYY-MM-DD: "+req.From)
			return
		}
		to, err := time.Parse(constants.DateFormat, strings.TrimSpace(req.To))
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "to must be YYYY-MM-DD: "+req.To)
			return
		}
		if !to.After(from) {
			api.RespondWithError(w, http.StatusBadRequest, "'to' must be after 'from'")
			return
		}
		if to.Sub(from).Hours() > 24*366*10 {
			api.RespondWithError(w, http.StatusBadRequest, "date range cannot exceed 10 years")
			return
		}

		holidays, err := GetHolidayListForRange(ctx, conn, req.CalendarCode, from, to)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		totalDays := int(to.Sub(from).Hours()/24) + 1
		workingDays := totalDays - len(holidays)

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"calendar_code": req.CalendarCode,
			"from":          req.From,
			"to":            req.To,
			"total_days":    totalDays,
			"working_days":  workingDays,
			"holiday_count": len(holidays),
			"holidays":      holidays,
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Maturity Date Calculator
// POST /investment/fd/simulator/maturity-date
// ─────────────────────────────────────────────────────────────────────────────

// MaturityDateRequest is the input for the maturity date calculator.
type MaturityDateRequest struct {
	// ── Tenor input — provide exactly one ────────────────────────────────
	StartDate   string `json:"start_date"`   // YYYY-MM-DD — required
	TenorDays   int    `json:"tenor_days"`   // raw calendar days
	TenorMonths int    `json:"tenor_months"` // months (expanded using calendar months)
	TenorYears  int    `json:"tenor_years"`  // years

	// ── Calendar & adjustment ─────────────────────────────────────────────
	// At least one of BankConfigID or CalendarCode should be provided for
	// holiday-aware adjustment.
	BankConfigID   string `json:"bank_config_id"`  // loads holiday calendar + adjustment rule
	CalendarCode   string `json:"calendar_code"`   // direct calendar override
	DateAdjustment string `json:"date_adjustment"` // FOLLOWING_WD | PRECEDING_WD | NO_ADJUST (overrides bank config)
}

// UnmarshalJSON accepts float-typed tenor fields (e.g. 10.98) and truncates to int.
func (r *MaturityDateRequest) UnmarshalJSON(data []byte) error {
	type Alias MaturityDateRequest
	var aux struct {
		Alias
		TenorDays   *json.Number `json:"tenor_days"`
		TenorMonths *json.Number `json:"tenor_months"`
		TenorYears  *json.Number `json:"tenor_years"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*r = MaturityDateRequest(aux.Alias)
	if aux.TenorDays != nil {
		if f, err := aux.TenorDays.Float64(); err == nil {
			r.TenorDays = int(f)
		}
	}
	if aux.TenorMonths != nil {
		if f, err := aux.TenorMonths.Float64(); err == nil {
			r.TenorMonths = int(f)
		}
	}
	if aux.TenorYears != nil {
		if f, err := aux.TenorYears.Float64(); err == nil {
			r.TenorYears = int(f)
		}
	}
	return nil
}

// MaturityDateResponse is what the endpoint returns.
type MaturityDateResponse struct {
	StartDate            string `json:"start_date"`
	TenorDays            int    `json:"tenor_days"`         // resolved calendar days
	RawMaturityDate      string `json:"raw_maturity_date"`  // start + tenor, unadjusted
	MaturityDate         string `json:"maturity_date"`      // adjusted for holidays/weekends
	IsAdjusted           bool   `json:"is_adjusted"`        // true when raw != adjusted
	AdjustmentApplied    string `json:"adjustment_applied"` // FOLLOWING_WD / PRECEDING_WD / NO_ADJUST
	CalendarCode         string `json:"calendar_code"`
	IsRawMaturityHoliday bool   `json:"is_raw_maturity_holiday"`
	IsRawMaturityWeekend bool   `json:"is_raw_maturity_weekend"`
	NextWorkingDay       string `json:"next_working_day"`
	PrevWorkingDay       string `json:"prev_working_day"`
}

// MaturityDateHandler calculates the maturity date from a start date + tenor,
// applying holiday and weekend adjustments from the bank config or calendar.
//
// POST /investment/fd/simulator/maturity-date
//
//	{
//	  "start_date": "2025-04-01",
//	  "tenor_months": 12,
//	  "bank_config_id": "bcfg-xxx"
//	}
//
// or with direct calendar:
//
//	{
//	  "start_date": "2025-04-01",
//	  "tenor_days": 365,
//	  "calendar_code": "TESTING_CALENDAR",
//	  "date_adjustment": "FOLLOWING_WD"
//	}
func MaturityDateHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req MaturityDateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		startDate, err := time.Parse(constants.DateFormat, strings.TrimSpace(req.StartDate))
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "start_date must be YYYY-MM-DD")
			return
		}

		// ── Resolve tenor → exact calendar date ───────────────────────────
		var rawMaturity time.Time
		tenorDays := 0
		switch {
		case req.TenorDays > 0:
			rawMaturity = startDate.AddDate(0, 0, req.TenorDays)
			tenorDays = req.TenorDays
		case req.TenorMonths > 0:
			rawMaturity = startDate.AddDate(0, req.TenorMonths, 0)
			tenorDays = int(rawMaturity.Sub(startDate).Hours() / 24)
		case req.TenorYears > 0:
			rawMaturity = startDate.AddDate(req.TenorYears, 0, 0)
			tenorDays = int(rawMaturity.Sub(startDate).Hours() / 24)
		default:
			api.RespondWithError(w, http.StatusBadRequest, "provide one of tenor_days, tenor_months, or tenor_years (> 0)")
			return
		}

		ctx := r.Context()
		conn, err := pool.Acquire(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusServiceUnavailable, constants.ErrDBConnection)
			return
		}
		defer conn.Release()

		// ── Load bank config to get calendar + adjustment rule ─────────────
		cfg, _ := loadBankConfig(ctx, conn, req.BankConfigID)

		// Direct calendar_code or date_adjustment overrides bank config.
		calCode := strings.TrimSpace(req.CalendarCode)
		if calCode == "" {
			calCode = strings.TrimSpace(cfg.HolidayCalendarCode)
		}
		adjustment := strings.TrimSpace(req.DateAdjustment)
		if adjustment == "" {
			adjustment = strings.TrimSpace(cfg.CapitalizationDateAdjustment)
		}
		if adjustment == "" {
			adjustment = "NO_ADJUST"
		}

		// ── Load holiday calendar ─────────────────────────────────────────
		calInfo := loadHolidayCalendar(ctx, conn, calCode, startDate, rawMaturity.AddDate(0, 0, 30))

		// ── Classify raw maturity day ─────────────────────────────────────
		rawKey := rawMaturity.Format(constants.DateFormat)
		isRawHoliday := calInfo.HolidayDates[rawKey]
		isRawWeekend := calInfo.WeekendPattern != "" && isWeekend(rawMaturity, calInfo.WeekendPattern)

		// ── Adjust maturity date ──────────────────────────────────────────
		adjustedMaturity := adjustToWorkingDay(rawMaturity, adjustment, cfg, calInfo)

		// If NO_ADJUST but raw is a non-working day, still compute next/prev for informational purposes.
		nextWD := NextWorkingDay(rawMaturity, calInfo)
		prevWD := PrevWorkingDay(rawMaturity, calInfo)
		// Adjust next/prev so they don't equal rawMaturity itself when it IS a working day.
		if nextWD.Equal(rawMaturity) && !isRawHoliday && !isRawWeekend {
			// rawMaturity is a working day — next is the day after
			nextWD = NextWorkingDay(rawMaturity.AddDate(0, 0, 1), calInfo)
			prevWD = PrevWorkingDay(rawMaturity.AddDate(0, 0, -1), calInfo)
		}

		api.RespondWithPayload(w, true, "", MaturityDateResponse{
			StartDate:            startDate.Format(constants.DateFormat),
			TenorDays:            tenorDays,
			RawMaturityDate:      rawMaturity.Format(constants.DateFormat),
			MaturityDate:         adjustedMaturity.Format(constants.DateFormat),
			IsAdjusted:           !adjustedMaturity.Equal(rawMaturity),
			AdjustmentApplied:    adjustment,
			CalendarCode:         calCode,
			IsRawMaturityHoliday: isRawHoliday,
			IsRawMaturityWeekend: isRawWeekend,
			NextWorkingDay:       nextWD.Format(constants.DateFormat),
			PrevWorkingDay:       prevWD.Format(constants.DateFormat),
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Cashflow Simulation — request / response types
// ─────────────────────────────────────────────────────────────────────────────

// SimulateCashflowRequest is the full input accepted by SimulateCashflowHandler.
// Nothing is stored — the engine runs purely in-memory using DB master lookups.
type SimulateCashflowRequest struct {
	// ── Required ─────────────────────────────────────────────────────────
	PrincipalAmount float64 `json:"principal_amount"` // e.g. 1000000
	InterestRate    float64 `json:"interest_rate"`    // annual % e.g. 7.5
	StartDate       string  `json:"start_date"`       // YYYY-MM-DD

	// Tenor — provide exactly one of:
	TenorDays   int `json:"tenor_days"`   // calendar days
	TenorMonths int `json:"tenor_months"` // months (converted → days)
	TenorYears  int `json:"tenor_years"`  // years  (converted → days)

	// ── Config refs — at least bank_config_id is recommended ─────────────
	BankConfigID      string `json:"bank_config_id"`      // resolved from investment.fd_bank_config_master
	DayCountCode      string `json:"day_count_code"`      // override / standalone e.g. "DC-ACT-365"
	InterestType      string `json:"interest_type"`       // code or id from fd_interest_type_master
	FrequencyID       string `json:"frequency_id"`        // compounding / payout frequency id or code
	FrequencyCode     string `json:"frequency_code"`      // alias for frequency_id (frontend compat)
	PayoutFrequencyID string `json:"payout_frequency_id"` // separate cash-payout freq for COMPOUND FDs (e.g. half-yearly payout on quarterly compounding)
	TDSPlanID         string `json:"tds_plan_id"`         // id from fd_tds_plan_master (optional)

	// ── Explicit maturity date override (overrides tenor calculation) ─────
	// When provided together with tenor fields, maturity_date wins.
	MaturityDate string `json:"maturity_date"` // YYYY-MM-DD — explicit maturity override

	// ── Inline overrides (applied on top of bank config) ─────────────────
	// These let the frontend test "what-if" scenarios without creating a config.
	HolidayCalendarCode          *string `json:"holiday_calendar_code"`
	CapitalizationScheduleType   *string `json:"capitalization_schedule_type"`
	CapitalizationDateAdjustment *string `json:"capitalization_date_adjustment"`
	AccrualStartConvention       *string `json:"accrual_start_convention"`
	AccrualEndConvention         *string `json:"accrual_end_convention"`
	PeriodBoundaryDefinition     *string `json:"period_boundary_definition"`
	RoundingMethod               *string `json:"rounding_method"`
	RoundingFrequency            *string `json:"rounding_frequency"`
	InterestRoundingDecimals     *int    `json:"interest_rounding_decimals"`
	BrokenPeriodMethod           *string `json:"broken_period_method"`
	BrokenPeriodLocation         *string `json:"broken_period_location"`
	GracePeriodDays              *int    `json:"grace_period_days"`
	GracePeriodRateType          *string `json:"grace_period_rate_type"`
	WeekendAccrual               *bool   `json:"weekend_accrual"`
	HolidayAccrual               *bool   `json:"holiday_accrual"`
	TDSDeductionTiming           *string `json:"tds_deduction_timing"`
	QuarterDefinition            *string `json:"quarter_definition"`
	// User-overridable first cap / payout event dates (YYYY-MM-DD).
	// First event lands on this date; later cap/payout events step by frequency.
	// applyFirstPayoutDateOverride may still nudge value_date when it differs from event_date.
	FirstPayoutDate         *string `json:"first_payout_date,omitempty"`
	FirstCapitalizationDate *string `json:"first_capitalization_date,omitempty"`
	// ResetType controls how the interest base principal resets after each payout.
	// Applies to both SIMPLE and COMPOUND FDs.
	// "AT_MATURITY" (default): accumulate / compound until maturity.
	// "AT_EACH_PAYOUT": reset the accrual/compounding base after each payout.
	ResetType *string `json:"reset_type,omitempty"`
	// AccrualFrequencyCode sets the accrual granularity independently of payout/cap frequency.
	// E.g. "M" for monthly, "Q" for quarterly, "H" for half-yearly, "Y" for yearly.
	// Defaults to "M" (monthly) when empty.
	AccrualFrequencyCode string `json:"accrual_frequency_code,omitempty"`
}

// UnmarshalJSON accepts float-typed tenor fields (e.g. 10.98) and truncates to int.
// This tolerates frontend payloads where tenor_months/tenor_years arrive as floats.
func (r *SimulateCashflowRequest) UnmarshalJSON(data []byte) error {
	type Alias SimulateCashflowRequest
	var aux struct {
		Alias
		TenorDays   *json.Number `json:"tenor_days"`
		TenorMonths *json.Number `json:"tenor_months"`
		TenorYears  *json.Number `json:"tenor_years"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*r = SimulateCashflowRequest(aux.Alias)
	if aux.TenorDays != nil {
		if f, err := aux.TenorDays.Float64(); err == nil {
			r.TenorDays = int(f)
		}
	}
	if aux.TenorMonths != nil {
		if f, err := aux.TenorMonths.Float64(); err == nil {
			r.TenorMonths = int(f)
		}
	}
	if aux.TenorYears != nil {
		if f, err := aux.TenorYears.Float64(); err == nil {
			r.TenorYears = int(f)
		}
	}
	return nil
}

// SimulateCashflowResponse is what the handler returns.
type SimulateCashflowResponse struct {
	// Resolved inputs echoed back so the caller can verify what was used.
	ResolvedInput SimulatedFDSummary `json:"resolved_input"`

	// The computed cashflow schedule — same structure as the persisted schedule.
	Schedule []SimulatedCashflowRow `json:"schedule"`

	// Derived summary metrics.
	Summary SimulateSummary `json:"summary"`

	// Non-working days that fell within the FD period.
	Holidays []HolidayEntry `json:"holidays,omitempty"`

	// Interest payout dates (INTEREST_RECEIPT boundaries) as YYYY-MM-DD array.
	PayoutDates *json.RawMessage `json:"payout_dates,omitempty"`

	// Compounding / capitalisation dates as YYYY-MM-DD array.
	CompoundingDates *json.RawMessage `json:"compounding_dates,omitempty"`
}

// SimulatedFDSummary echoes back what the engine used — all resolved masters.
type SimulatedFDSummary struct {
	// ── Core FD parameters ───────────────────────────────────────────────
	PrincipalAmount     float64 `json:"principal_amount"`
	InterestRate        float64 `json:"interest_rate"`
	StartDate           string  `json:"start_date"`
	MaturityDate        string  `json:"maturity_date"`
	TenorDays           int     `json:"tenor_days"`
	InterestType        string  `json:"interest_type"`
	DayCountConvention  string  `json:"day_count_convention"`
	BankConfigID        string  `json:"bank_config_id"`
	HolidayCalendarCode string  `json:"holiday_calendar_code"`

	// ── Resolved frequency (compounding / capitalization) ────────────────
	FrequencyID   string `json:"frequency_id,omitempty"`
	FrequencyCode string `json:"frequency_code,omitempty"`
	FrequencyName string `json:"frequency_name,omitempty"`
	FrequencyType string `json:"frequency_type,omitempty"`
	PayoutMonths  int    `json:"payout_months_per_period,omitempty"` // 0 = AT_MATURITY

	// ── Resolved payout frequency (separate cash payout for COMPOUND FDs) ─
	PayoutFrequencyID   string `json:"payout_frequency_id,omitempty"`
	PayoutFrequencyCode string `json:"payout_frequency_code,omitempty"`
	PayoutFrequencyName string `json:"payout_frequency_name,omitempty"`
	PayoutFrequencyType string `json:"payout_frequency_type,omitempty"`

	// ── Resolved TDS ──────────────────────────────────────────────────────
	TDSPlanID          string  `json:"tds_plan_id,omitempty"`
	TDSPlanCode        string  `json:"tds_plan_code,omitempty"`
	TDSPlanName        string  `json:"tds_plan_name,omitempty"`
	TDSRate            float64 `json:"tds_rate_pct,omitempty"` // % e.g. 10.00
	TDSThreshold       float64 `json:"tds_threshold_amount,omitempty"`
	TDSDeductionTiming string  `json:"tds_deduction_timing,omitempty"` // ACCRUAL_ANNUAL|MATURITY|RECEIPT
}

// SimulatedCashflowRow mirrors CashflowRow but with JSON-friendly date strings.
type SimulatedCashflowRow struct {
	PeriodNumber      int     `json:"period_number"`
	EventType         string  `json:"event_type"`
	EventDate         string  `json:"event_date"`
	ValueDate         string  `json:"value_date,omitempty"`    // settlement/processed date (next working day for cash events)
	CashflowType      string  `json:"cashflow_type,omitempty"` // OUTFLOW|INFLOW|CAP|NA
	PeriodStartDate   string  `json:"period_start_date"`
	PeriodEndDate     string  `json:"period_end_date"`
	PeriodDays        int     `json:"period_days"`
	OpeningPrincipal  float64 `json:"opening_principal"`
	InterestAccrued   float64 `json:"interest_accrued"`
	CapitalizedAmount float64 `json:"capitalized_amount"`
	ClosingPrincipal  float64 `json:"closing_principal"`
	TDSAmount         float64 `json:"tds_amount"`
	NetCashFlow       float64 `json:"net_cash_flow"`
	DueNotAccrued     float64 `json:"due_not_accrued"`             // last month's accrual folded into payout (suppressed as standalone row)
	AccrRevK          float64 `json:"accr_rev_k"`                  // cumulative prior accruals reversed into payout
	TDSRevL           float64 `json:"tds_rev_l"`                   // TDS on AccrRevK
	ProvisionalTDS    float64 `json:"provisional_tds"`             // indicative TDS on this period's accrual (= InterestAccrued × TDSRate/100)
	NetAmount         float64 `json:"net_amount,omitempty"`        // CO: J(latestCap) for cap/maturity rows; G for payout rows
	AccrualFrequency  string  `json:"accrual_frequency,omitempty"` // interest_payout_frequency / compounding_frequency
	DayCountCode      string  `json:"day_count_code,omitempty"`
	Divisor           int     `json:"divisor,omitempty"`
	FormulaUsed       string  `json:"formula_used,omitempty"`
	AccrualRatePerDay float64 `json:"accrual_rate_per_day,omitempty"`
	HolidaysInPeriod  int     `json:"holidays_in_period"`
	// Cumulative / snapshot fields
	InterestRate            float64 `json:"interest_rate"`
	TDSRate                 float64 `json:"tds_rate"`
	FinancialYear           string  `json:"financial_year"`
	CumulativeInterestFY    float64 `json:"cumulative_interest_fy"`
	CumulativeTDSFY         float64 `json:"cumulative_tds_fy"`
	CumulativeInterestTotal float64 `json:"cumulative_interest_total"`
}

// SimulateSummary holds aggregated metrics across the schedule.
type SimulateSummary struct {
	TotalInterestAccrued float64 `json:"total_interest_accrued"`
	TotalTDSDeducted     float64 `json:"total_tds_deducted"`
	TotalCapitalized     float64 `json:"total_capitalized"`
	MaturityAmount       float64 `json:"maturity_amount"`
	EffectiveYield       float64 `json:"effective_yield_pct"` // % post-TDS
	AccrualPeriodCount   int     `json:"accrual_period_count"`
	CapitalizationCount  int     `json:"capitalization_count"`
	InterestReceiptCount int     `json:"interest_receipt_count"`
	// Workbook header cells (FD_Scenarios_v7.xlsx C17/C16) — compound formula totals.
	// Differs from total_interest_accrued which is the sum of schedule CAP rows (ACT/365 path).
	WorkbookTotalInterest float64 `json:"workbook_total_interest,omitempty"`
	// WorkbookTotalTDS equals Σ TDSAmount from CAPITALIZATION rows (per-period rounded TDS,
	// i.e. Σ TDS Rev L from the schedule), matching what the bank actually deducts and
	// what appears on Form 16A. For SIMPLE FDs it is computed from WorkbookTotalInterest.
	WorkbookTotalTDS float64 `json:"workbook_total_tds,omitempty"`
	// Maturity gross interest (CO MATURITY row G = closing J − original principal).
	MaturityGrossInterest float64 `json:"maturity_gross_interest,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared simulation runner — reused by both cashflow and diff handlers
// ─────────────────────────────────────────────────────────────────────────────

// simulationResult holds everything produced by runSimulationForRequest.
type simulationResult struct {
	ResolvedInput    SimulatedFDSummary
	Schedule         []SimulatedCashflowRow
	RawRows          []CashflowRow // before conversion — needed for summary
	Summary          SimulateSummary
	Holidays         []HolidayEntry
	FD               *FDRecord        // in-memory FDRecord used for the run
	PayoutDates      *json.RawMessage // INTEREST_RECEIPT boundary dates
	CompoundingDates *json.RawMessage // CAPITALIZATION boundary dates
}

// runSimulationForRequest validates the request, loads all required DB masters,
// runs generateCashflowSchedule and returns a fully populated simulationResult.
// It does NOT write any HTTP response; that is left to the caller.
//
// exec must be a live *pgxpool.Conn (or pgx.Tx).
func runSimulationForRequest(ctx context.Context, exec queryExecutor, req SimulateCashflowRequest) (*simulationResult, error) {
	// ── Validate required fields ──────────────────────────────────────────
	if req.PrincipalAmount <= 0 {
		return nil, fmt.Errorf("principal_amount must be > 0")
	}
	if req.InterestRate <= 0 {
		return nil, fmt.Errorf("interest_rate must be > 0")
	}

	startDate, err := time.Parse(constants.DateFormat, strings.TrimSpace(req.StartDate))
	if err != nil {
		return nil, fmt.Errorf("start_date must be YYYY-MM-DD, got: %q", req.StartDate)
	}

	// ── Resolve frequency ref (frequency_id OR frequency_code alias) ─────
	freqRef := strings.TrimSpace(firstNonEmpty(req.FrequencyID, req.FrequencyCode))

	// ── Resolve tenor → maturity date ────────────────────────────────────
	// Explicit maturity_date in request wins over tenor calculation.
	tenorDays := req.TenorDays
	tenorFromMonthsOrYears := false // tracks whether maturity needs holiday adjustment

	var maturityDate time.Time
	if d := strings.TrimSpace(req.MaturityDate); d != "" {
		maturityDate, err = time.Parse(constants.DateFormat, d)
		if err != nil {
			return nil, fmt.Errorf("maturity_date must be YYYY-MM-DD, got: %q", d)
		}
		// Back-compute tenor days from explicit dates if not supplied
		if tenorDays <= 0 {
			tenorDays = int(maturityDate.Sub(startDate).Hours() / 24)
		}
	} else {
		// No explicit maturity_date — derive from tenor.
		// Use proper calendar math (AddDate) rather than the approximation
		// tenor_months*30, so the computed maturity matches how the booking
		// side stores it (booking also uses AddDate(0, months, 0)).
		if tenorDays <= 0 && req.TenorMonths > 0 {
			maturityDate = startDate.AddDate(0, req.TenorMonths, 0)
			tenorDays = int(maturityDate.Sub(startDate).Hours() / 24)
			tenorFromMonthsOrYears = true
		} else if tenorDays <= 0 && req.TenorYears > 0 {
			maturityDate = startDate.AddDate(req.TenorYears, 0, 0)
			tenorDays = int(maturityDate.Sub(startDate).Hours() / 24)
			tenorFromMonthsOrYears = true
		} else if tenorDays > 0 {
			maturityDate = startDate.AddDate(0, 0, tenorDays)
		} else {
			return nil, fmt.Errorf("provide one of tenor_days, tenor_months, tenor_years, or maturity_date (> 0)")
		}
	}

	if tenorDays <= 0 {
		return nil, fmt.Errorf("cannot determine tenor: maturity_date must be after start_date")
	}
	_ = tenorFromMonthsOrYears // holiday adjustment applied below after cfg+cal are loaded

	// ── Load bank config (if provided) ────────────────────────────────────
	cfg, _ := loadBankConfig(ctx, exec, req.BankConfigID)
	// cfg is always non-nil — loadBankConfig returns &BankConfig{} on any error

	// Apply inline overrides on top of loaded config.
	applyConfigOverrides(cfg, &req)

	// ── Load TDS config ───────────────────────────────────────────────────
	tds, _ := loadTDSConfig(ctx, exec, req.TDSPlanID)

	// ── Resolve day count convention ──────────────────────────────────────
	dcRef := firstNonEmpty(req.DayCountCode, cfg.DayCountCode)
	dcInfo := loadDayCountConvention(ctx, exec, dcRef)

	// ── Resolve interest type ─────────────────────────────────────────────
	itInfo := loadInterestType(ctx, exec, req.InterestType)
	isCompound := firstNonEmpty(itInfo.CalculationMethod, strings.ToUpper(req.InterestType), "SIMPLE") == "COMPOUND"

	// ── Load cap + payout frequencies (engine / workbook mapping) ─────────
	// COMPOUND: frequency_id = cap/compounding freq, payout_frequency_id = cash payout.
	// SIMPLE:   frequency_id = payout freq (accrual via accrual_frequency_code).
	capFreqRef, payoutFreqRef := resolveCapAndPayoutFreqRefs(isCompound, freqRef, req.PayoutFrequencyID)
	capFreq, _ := loadCompoundingFreq(ctx, exec, capFreqRef)
	var payoutFreq *CompoundingFreq
	if payoutFreqRef != "" && !strings.EqualFold(payoutFreqRef, capFreqRef) {
		payoutFreq, _ = loadCompoundingFreq(ctx, exec, payoutFreqRef)
	} else {
		payoutFreq = capFreq
	}
	payoutFreq = ensureAtMaturityPayoutFreq(payoutFreqRef, payoutFreq)
	if !isCompound {
		capFreq = payoutFreq
	}

	// ── Load holiday calendar ─────────────────────────────────────────────
	calCode := strings.TrimSpace(cfg.HolidayCalendarCode)
	calInfo := loadHolidayCalendar(ctx, exec, calCode, startDate, maturityDate)

	// ── Apply holiday adjustment to month/year-derived maturity ──────────
	// When maturity was computed from tenor_months or tenor_years (not an explicit
	// maturity_date or tenor_days), adjust for non-working days using the same
	// CapitalizationDateAdjustment rule as the booking path.
	if tenorFromMonthsOrYears && cfg.CapitalizationDateAdjustment != "" && cfg.CapitalizationDateAdjustment != "NO_ADJUST" {
		adjusted := adjustToWorkingDay(maturityDate, cfg.CapitalizationDateAdjustment, cfg, calInfo)
		if !adjusted.Equal(maturityDate) {
			maturityDate = adjusted
			tenorDays = int(maturityDate.Sub(startDate).Hours() / 24)
		}
	}

	// ── Build FDRecord in memory (no DB) ──────────────────────────────────
	fd := &FDRecord{
		PrincipalAmount:         req.PrincipalAmount,
		InterestRate:            req.InterestRate,
		InterestTypeCode:        req.InterestType,
		TenorDays:               tenorDays,
		ValueDate:               startDate,
		MaturityDate:            maturityDate,
		BankConfigID:            req.BankConfigID,
		FrequencyID:             freqRef,
		InterestPayoutFrequency: payoutFreqRef,
		TDSPlanID:               req.TDSPlanID,
		DayCountConvention:      dcRef,
	}

	// ── Apply user-supplied first payout / capitalization date and reset-type overrides ──
	// Must be set BEFORE generateCashflowSchedule runs so the engine sees them.
	if req.FirstPayoutDate != nil && *req.FirstPayoutDate != "" {
		if pd, perr := time.Parse(constants.DateFormat, *req.FirstPayoutDate); perr == nil {
			fd.FirstPayoutDate = pd
		}
	}
	if req.FirstCapitalizationDate != nil && *req.FirstCapitalizationDate != "" {
		if cd, cerr := time.Parse(constants.DateFormat, *req.FirstCapitalizationDate); cerr == nil {
			fd.FirstCapitalizationDate = cd
		}
	}
	if req.ResetType != nil && *req.ResetType != "" {
		fd.ResetType = strings.ToUpper(strings.TrimSpace(*req.ResetType))
	}

	// ── Parse accrual frequency ───────────────────────────────────────────
	accrualFreqMonths := 1
	if code := strings.ToUpper(strings.TrimSpace(req.AccrualFrequencyCode)); code != "" {
		if m := freqTypeToMonths(code); m > 0 {
			accrualFreqMonths = m
		}
	}

	// ── Run the engine ────────────────────────────────────────────────────
	rawRows := generateCashflowSchedule(CashflowScheduleParams{
		FD: fd, Cfg: cfg, Freq: capFreq, TDSCfg: tds,
		DCInfo: dcInfo, ITInfo: itInfo, CalInfo: calInfo,
		PayoutFreqOverride: payoutFreq,
		AccrualFreqMonths:  accrualFreqMonths,
	})

	// ── Apply grace period extension ──────────────────────────────────────
	rawRows = applyGracePeriod(fd, cfg, rawRows, dcInfo, calInfo)

	// ── Stamp cumulative + FY fields ─────────────────────────────────────
	simTDSRate := 0.0
	if tds != nil {
		simTDSRate = tds.TDSRate
	}
	rawRows = stampCumulativeFields(rawRows, fd, simTDSRate, isCompound)

	rawRows = applyFirstPayoutDateOverride(rawRows, fd)

	// ── Convert to sim rows ───────────────────────────────────────────────
	simRows := make([]SimulatedCashflowRow, 0, len(rawRows))
	for _, row := range rawRows {
		simRows = append(simRows, cashflowRowToSim(row))
	}
	sortSimulatedCashflowRows(simRows)

	// ── Holidays for display ──────────────────────────────────────────────
	holidays, _ := GetHolidayListForRange(ctx, exec, calCode, startDate, maturityDate)

	// ── Build effective TDS fields for echo ───────────────────────────────
	effectiveTDSPlanID := ""
	effectiveTDSCode := ""
	effectiveTDSName := ""
	effectiveTDSRate := 0.0
	effectiveTDSThreshold := 0.0
	effectiveTDSTiming := ""
	if tds != nil && tds.TDSPlanID != "" {
		effectiveTDSPlanID = tds.TDSPlanID
		effectiveTDSCode = tds.TDSPlanCode
		effectiveTDSName = tds.TDSPlanName
		effectiveTDSRate = tds.TDSRate
		effectiveTDSThreshold = tds.ThresholdAmount
		// TDS timing comes ONLY from tds_plan_master — no bank config fallback
		effectiveTDSTiming = normTDSTiming(tds.DeductionTiming)
	}

	// ── Build effective frequency fields for echo ─────────────────────────
	payoutMonths := freqTypeToMonths(firstNonEmpty(payoutFreq.FrequencyType, payoutFreq.FrequencyCode))

	// ── Extract payout and compounding date arrays from schedule ─────────
	payoutDatesJSON := extractEventDates(simRows, "INTEREST_RECEIPT")
	compoundingDatesJSON := extractEventDates(simRows, "CAPITALIZATION")

	return &simulationResult{
		ResolvedInput: SimulatedFDSummary{
			PrincipalAmount:     req.PrincipalAmount,
			InterestRate:        req.InterestRate,
			StartDate:           startDate.Format(constants.DateFormat),
			MaturityDate:        maturityDate.Format(constants.DateFormat),
			TenorDays:           tenorDays,
			InterestType:        itInfo.CalculationMethod,
			DayCountConvention:  dcInfo.ConventionType,
			BankConfigID:        req.BankConfigID,
			HolidayCalendarCode: calCode,
			// Compounding / capitalization frequency
			FrequencyID:   capFreq.FrequencyID,
			FrequencyCode: capFreq.FrequencyCode,
			FrequencyName: capFreq.FrequencyName,
			FrequencyType: capFreq.FrequencyType,
			PayoutMonths:  payoutMonths,
			// Cash-payout frequency (same as comp freq when payout_frequency_id not supplied)
			PayoutFrequencyID:   payoutFreq.FrequencyID,
			PayoutFrequencyCode: payoutFreq.FrequencyCode,
			PayoutFrequencyName: payoutFreq.FrequencyName,
			PayoutFrequencyType: payoutFreq.FrequencyType,
			// TDS
			TDSPlanID:          effectiveTDSPlanID,
			TDSPlanCode:        effectiveTDSCode,
			TDSPlanName:        effectiveTDSName,
			TDSRate:            effectiveTDSRate,
			TDSThreshold:       effectiveTDSThreshold,
			TDSDeductionTiming: effectiveTDSTiming,
		},
		Schedule:         simRows,
		RawRows:          rawRows,
		Summary: func() SimulateSummary {
			sum := buildSimulateSummary(rawRows, fd)
			tdsPct := 0.0
			if tds != nil {
				tdsPct = tds.TDSRate
			}
			if isCompound {
				enrichCompoundWorkbookSummary(&sum, fd, cfg, firstNonEmpty(capFreq.FrequencyType, capFreq.FrequencyCode), tdsPct, rawRows)
			} else {
				enrichSimpleWorkbookSummary(&sum, fd, cfg, tdsPct)
			}
			return sum
		}(),
		Holidays:         holidays,
		FD:               fd,
		PayoutDates:      payoutDatesJSON,
		CompoundingDates: compoundingDatesJSON,
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP handler — POST /investment/fd/simulator/cashflow
// ─────────────────────────────────────────────────────────────────────────────

// SimulateCashflowHandler runs a full cashflow simulation and returns the
// schedule without persisting anything.
//
// POST /investment/fd/simulator/cashflow
//
//	{
//	  "principal_amount": 1000000,
//	  "interest_rate": 7.5,
//	  "start_date": "2025-04-01",
//	  "tenor_months": 12,
//	  "bank_config_id": "bcfg-xxx",
//	  "interest_type": "COMPOUND",
//	  "frequency_id": "freq-quarterly",
//	  "tds_plan_id": "tds-plan-abc"
//	}
func SimulateCashflowHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SimulateCashflowRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		ctx := r.Context()
		conn, err := pool.Acquire(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusServiceUnavailable, constants.ErrDBConnection)
			return
		}
		defer conn.Release()

		result, err := runSimulationForRequest(ctx, conn, req)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		api.RespondWithPayload(w, true, "", SimulateCashflowResponse{
			ResolvedInput:    result.ResolvedInput,
			Schedule:         result.Schedule,
			Summary:          result.Summary,
			Holidays:         result.Holidays,
			PayoutDates:      result.PayoutDates,
			CompoundingDates: result.CompoundingDates,
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

// resolveCapAndPayoutFreqRefs maps HTTP frequency fields to engine inputs.
// COMPOUND: frequency_id = cap/compounding, payout_frequency_id = cash payout.
// SIMPLE:   frequency_id = payout; payout_frequency_id is an optional override.
func resolveCapAndPayoutFreqRefs(isCompound bool, freqRef, payoutFreqRef string) (capRef, payoutRef string) {
	freqRef = strings.TrimSpace(freqRef)
	payoutFreqRef = strings.TrimSpace(payoutFreqRef)
	if isCompound {
		capRef = freqRef
		payoutRef = payoutFreqRef
		if payoutRef == "" {
			payoutRef = freqRef
		}
		return capRef, payoutRef
	}
	capRef = freqRef
	payoutRef = payoutFreqRef
	if payoutRef == "" {
		payoutRef = freqRef
	}
	return capRef, payoutRef
}

// ensureAtMaturityPayoutFreq guarantees a non-empty FrequencyID for AT_MATURITY
// payout refs so engCOSchedule honours payout-at-maturity (workbook CO-*-MAT-*).
func ensureAtMaturityPayoutFreq(payoutFreqRef string, loaded *CompoundingFreq) *CompoundingFreq {
	ref := strings.ToUpper(strings.TrimSpace(payoutFreqRef))
	if ref != "AT_MATURITY" && ref != "MAT" && ref != "AT-MATURITY" {
		if loaded == nil {
			return &CompoundingFreq{}
		}
		return loaded
	}
	ft := firstNonEmpty(loaded.FrequencyType, "AT_MATURITY")
	return &CompoundingFreq{
		FrequencyID:   firstNonEmpty(loaded.FrequencyID, ref, "AT_MATURITY"),
		FrequencyCode: firstNonEmpty(loaded.FrequencyCode, "AT_MATURITY"),
		FrequencyName: loaded.FrequencyName,
		FrequencyType: ft,
	}
}

// applyConfigOverrides merges the request's inline override fields into a
// loaded (or empty) BankConfig so callers can test what-if scenarios.
func applyConfigOverrides(cfg *BankConfig, req *SimulateCashflowRequest) {
	if req.HolidayCalendarCode != nil {
		cfg.HolidayCalendarCode = *req.HolidayCalendarCode
	}
	if req.CapitalizationScheduleType != nil {
		cfg.CapitalizationScheduleType = *req.CapitalizationScheduleType
	}
	if req.CapitalizationDateAdjustment != nil {
		cfg.CapitalizationDateAdjustment = *req.CapitalizationDateAdjustment
	}
	if req.AccrualStartConvention != nil {
		cfg.AccrualStartConvention = *req.AccrualStartConvention
	}
	if req.AccrualEndConvention != nil {
		cfg.AccrualEndConvention = *req.AccrualEndConvention
	}
	if req.PeriodBoundaryDefinition != nil {
		cfg.PeriodBoundaryDefinition = *req.PeriodBoundaryDefinition
	}
	if req.RoundingMethod != nil {
		cfg.RoundingMethod = *req.RoundingMethod
	}
	if req.RoundingFrequency != nil {
		cfg.RoundingFrequency = *req.RoundingFrequency
	}
	if req.InterestRoundingDecimals != nil {
		cfg.InterestRoundingDecimals = *req.InterestRoundingDecimals
	}
	if req.BrokenPeriodMethod != nil {
		cfg.BrokenPeriodMethod = *req.BrokenPeriodMethod
	}
	if req.BrokenPeriodLocation != nil {
		cfg.BrokenPeriodLocation = *req.BrokenPeriodLocation
	}
	if req.GracePeriodDays != nil {
		cfg.GracePeriodDays = *req.GracePeriodDays
	}
	if req.GracePeriodRateType != nil {
		cfg.GracePeriodRateType = *req.GracePeriodRateType
	}
	if req.WeekendAccrual != nil {
		cfg.WeekendAccrual = *req.WeekendAccrual
	}
	if req.HolidayAccrual != nil {
		cfg.HolidayAccrual = *req.HolidayAccrual
	}
	if req.TDSDeductionTiming != nil {
		cfg.TDSDeductionTiming = *req.TDSDeductionTiming
	}
	if req.QuarterDefinition != nil {
		cfg.QuarterDefinition = *req.QuarterDefinition
	}
}

// applyGracePeriod appends an optional GRACE_PERIOD cashflow row when
// BankConfig.GracePeriodDays > 0 and GracePeriodRateType is not "NO_INTEREST".
//
// The grace row covers maturityDate+1 through maturityDate+graceDays using
// the same (or a savings) rate so the caller can see accruals after maturity.
func applyGracePeriod(fd *FDRecord, cfg *BankConfig, rows []CashflowRow, dcInfo DayCountInfo, cal HolidayCalendarInfo) []CashflowRow {
	if cfg.GracePeriodDays <= 0 {
		return rows
	}
	if strings.ToUpper(strings.TrimSpace(cfg.GracePeriodRateType)) == "NO_INTEREST" {
		return rows
	}

	graceRate := fd.InterestRate
	if strings.ToUpper(strings.TrimSpace(cfg.GracePeriodRateType)) == "SAVINGS_RATE" {
		graceRate = 3.5 // canonical savings rate fallback; real code would load from master
	}

	graceStart := fd.MaturityDate
	graceEnd := graceStart.AddDate(0, 0, cfg.GracePeriodDays)
	// Adjust grace end to working day if needed.
	graceEnd = adjustToWorkingDay(graceEnd, cfg.CapitalizationDateAdjustment, cfg, cal)

	decimals := cfg.InterestRoundingDecimals
	if decimals < 0 {
		decimals = rounding.DefaultDecimals
	}
	effectiveConvention := firstNonEmpty(dcInfo.ConventionType, "ACT_365")
	divisor, days := getDivisorAndDaysWithCal(effectiveConvention, graceStart, graceEnd, cfg, cal)
	if days <= 0 {
		days = cfg.GracePeriodDays
	}
	raw := fd.PrincipalAmount * graceRate * float64(days) / float64(divisor) / 100
	interest := roundByMethod(raw, decimals, cfg.RoundingMethod)

	rows = append(rows, CashflowRow{
		PeriodNumber:     len(rows) + 1,
		EventType:        "GRACE_PERIOD",
		EventDate:        graceEnd,
		ValueDate:        resolveValueDate("GRACE_PERIOD", graceEnd, cfg, cal),
		CashflowType:     "NA",
		PeriodStartDate:  graceStart,
		PeriodEndDate:    graceEnd,
		PeriodDays:       days,
		OpeningPrincipal: fd.PrincipalAmount,
		InterestAccrued:  interest,
		ClosingPrincipal: fd.PrincipalAmount,
		NetCashFlow:      interest,
		DayCountCode:     dcInfo.DayCountCode,
		Divisor:          divisor,
		FormulaUsed: fmt.Sprintf(
			"GRACE: P(%.2f) × r(%.4f%%) × d(%d) / D(%d) [%s]",
			fd.PrincipalAmount, graceRate, days, divisor, effectiveConvention,
		),
		AccrualRatePerDay: graceRate / (float64(divisor) * 100),
	})
	return rows
}

// cashflowRowToSim converts an internal CashflowRow to the JSON-friendly sim type.
func cashflowRowToSim(row CashflowRow) SimulatedCashflowRow {
	formatDate := func(t time.Time) string {
		if t.IsZero() {
			return ""
		}
		return t.Format(constants.DateFormat)
	}
	return SimulatedCashflowRow{
		PeriodNumber:      row.PeriodNumber,
		EventType:         row.EventType,
		EventDate:         formatDate(row.EventDate),
		ValueDate:         formatDate(row.ValueDate),
		CashflowType:      row.CashflowType,
		PeriodStartDate:   formatDate(row.PeriodStartDate),
		PeriodEndDate:     formatDate(row.PeriodEndDate),
		PeriodDays:        row.PeriodDays,
		OpeningPrincipal:  row.OpeningPrincipal,
		InterestAccrued:   row.InterestAccrued,
		CapitalizedAmount: row.CapitalizedAmount,
		ClosingPrincipal:  row.ClosingPrincipal,
		TDSAmount:         row.TDSAmount,
		NetCashFlow:       row.NetCashFlow,
		DueNotAccrued:     row.DueNotAccrued,
		AccrRevK:          row.AccrRevK,
		TDSRevL:           row.TDSRevL,
		ProvisionalTDS:    row.ProvisionalTDS,
		AccrualFrequency:  row.AccrualFrequency,
		DayCountCode:      row.DayCountCode,
		Divisor:           row.Divisor,
		FormulaUsed:       row.FormulaUsed,
		AccrualRatePerDay: row.AccrualRatePerDay,
		HolidaysInPeriod:  row.HolidaysInPeriod,
		// cumulative / snapshot
		InterestRate:            row.InterestRate,
		TDSRate:                 row.TDSRate,
		FinancialYear:           row.FinancialYear,
		CumulativeInterestFY:    row.CumulativeInterestFY,
		CumulativeTDSFY:         row.CumulativeTDSFY,
		CumulativeInterestTotal: row.CumulativeInterestTotal,
		NetAmount:               row.NetAmount,
	}
}

// extractEventDates collects event_date strings for all rows matching eventType,
// marshals them to a json.RawMessage array, and returns nil if none found.
func extractEventDates(rows []SimulatedCashflowRow, eventType string) *json.RawMessage {
	var dates []string
	for _, r := range rows {
		if r.EventType == eventType && r.EventDate != "" {
			dates = append(dates, r.EventDate)
		}
	}
	if len(dates) == 0 {
		return nil
	}
	b, err := json.Marshal(dates)
	if err != nil {
		return nil
	}
	raw := json.RawMessage(b)
	return &raw
}

// capPeriodsPerYear returns compoundings per year for workbook C17 (Q→4, H→2, Y→1).
func capPeriodsPerYear(freqType string) int {
	switch strings.ToUpper(strings.TrimSpace(freqType)) {
	case "QUARTERLY", "QUARTER", "QTR", "Q":
		return 4
	case "HALF_YEARLY", "HALF-YEARLY", "HALFYEARLY", "BI_ANNUAL", "BIANNUAL", "SEMI_ANNUAL", "SEMI-ANNUAL", "H":
		return 2
	case "ANNUAL", "YEARLY", "YEAR", "Y":
		return 1
	case "MONTHLY", "MONTH", "M":
		return 12
	default:
		return 4
	}
}

// simpleFormulaInterest implements workbook C16: P * r * tenorDays / 365 with bank rounding.
func simpleFormulaInterest(principal, annualRatePct float64, tenorDays int, cfg *BankConfig) float64 {
	if principal <= 0 || annualRatePct <= 0 || tenorDays <= 0 {
		return 0
	}
	raw := principal * (annualRatePct / 100.0) * float64(tenorDays) / 365.0
	rnd := engRoundingFromCfg(cfg)
	return rnd.RoundFinal(raw)
}

// compoundFormulaInterest implements workbook C17 with bank rounding.
func compoundFormulaInterest(principal, annualRatePct float64, capPeriodsPerYear, tenorDays int, cfg *BankConfig) float64 {
	if principal <= 0 || annualRatePct <= 0 || tenorDays <= 0 || capPeriodsPerYear <= 0 {
		return 0
	}
	r := annualRatePct / 100.0
	n := float64(capPeriodsPerYear)
	tenorYears := float64(tenorDays) / 365.0
	raw := principal * (math.Pow(1+r/n, n*tenorYears) - 1)
	rnd := engRoundingFromCfg(cfg)
	return rnd.RoundFinal(raw)
}

// buildSimulateSummary aggregates totals across the simulated schedule.
//
// Source-of-truth rules for TotalInterestAccrued:
//
//	SIMPLE payout FD (INTEREST_RECEIPT rows present):
//	  The schedule emits 2 ACCRUAL sub-rows per quarter (e.g. Jan + Feb) and one
//	  INTEREST_RECEIPT row for the full quarter (Jan+Feb+Mar).  The ACCRUAL rows
//	  only cover the months that fall BEFORE the payout date; the final month's
//	  interest is embedded in INTEREST_RECEIPT.  Summing only ACCRUAL rows would
//	  therefore under-count by the "due_not_accrued" slice of every quarter.
//	  → Use INTEREST_RECEIPT rows as the canonical interest source.
//
//	SIMPLE AT_MATURITY FD (no INTEREST_RECEIPT, no CAPITALIZATION):
//	  All interest is in ACCRUAL rows.  Sum those.
//
//	COMPOUND FD (CAPITALIZATION rows present):
//	  Each CAPITALIZATION row's InterestAccrued = sum of its ACCRUAL sub-rows.
//	  → Use CAPITALIZATION rows; ignore ACCRUAL row interest to avoid double-count.
//
// TDS is always sourced exclusively from TDS_DEDUCTION rows.
func buildSimulateSummary(rows []CashflowRow, fd *FDRecord) SimulateSummary {
	var s SimulateSummary

	// First pass: detect schedule shape and build set of dates that have a TDS_DEDUCTION row.
	// Same-date TDS_DEDUCTION rows take precedence over CAP/MATURITY tds_amount (avoids double-count).
	isCompound := false
	hasInterestReceipt := false
	tdsDeductionDates := make(map[string]bool) // YYYY-MM-DD keys
	for _, row := range rows {
		switch row.EventType {
		case "CAPITALIZATION":
			isCompound = true
		case "INTEREST_RECEIPT":
			hasInterestReceipt = true
		case "TDS_DEDUCTION":
			tdsDeductionDates[row.EventDate.Format(constants.DateFormat)] = true
		}
	}

	for _, row := range rows {
		switch row.EventType {
		case "ACCRUAL":
			s.AccrualPeriodCount++
			if !isCompound && !hasInterestReceipt {
				s.TotalInterestAccrued += row.InterestAccrued
			}
			// ACCRUAL.TDSAmount is ProvisionalTDS — NEVER include in TotalTDSDeducted.
		case "CAPITALIZATION":
			s.TotalCapitalized += row.CapitalizedAmount
			s.CapitalizationCount++
			if isCompound {
				s.TotalInterestAccrued += row.InterestAccrued
			}
			// CO+RECEIPT/ACCRUAL: TDS sits on cap row; include unless paired TDS_DEDUCTION exists.
			if row.TDSAmount > 0 && !tdsDeductionDates[row.EventDate.Format(constants.DateFormat)] {
				s.TotalTDSDeducted += row.TDSAmount
			}
		case "INTEREST_RECEIPT":
			s.InterestReceiptCount++
			if !isCompound && hasInterestReceipt {
				s.TotalInterestAccrued += row.InterestAccrued
			}
			// SI+RECEIPT: TDS sits on receipt row AND a paired TDS_DEDUCTION row exists.
			// If no paired TDS_DEDUCTION row, include here.
			if row.TDSAmount > 0 && !tdsDeductionDates[row.EventDate.Format(constants.DateFormat)] {
				s.TotalTDSDeducted += row.TDSAmount
			}
		case "TDS_DEDUCTION":
			s.TotalTDSDeducted += row.TDSAmount
		case "MATURITY":
			s.MaturityAmount = row.NetCashFlow
			if isCompound && row.InterestAccrued > 0 {
				s.MaturityGrossInterest = row.InterestAccrued
			}
			if !isCompound && row.InterestAccrued > 0 {
				s.TotalInterestAccrued += row.InterestAccrued
				s.InterestReceiptCount++
			}
			if row.TDSAmount > 0 && !tdsDeductionDates[row.EventDate.Format(constants.DateFormat)] {
				s.TotalTDSDeducted += row.TDSAmount
			}
		case "GRACE_PERIOD":
			s.TotalInterestAccrued += row.InterestAccrued
		}
	}

	if fd.PrincipalAmount > 0 && fd.TenorDays > 0 {
		netInterest := s.TotalInterestAccrued - s.TotalTDSDeducted
		if isCompound && s.MaturityGrossInterest > 0 {
			// Post-TDS economic yield uses maturity cash interest (G − I), not sum of cap rows.
			netInterest = s.MaturityAmount
		}
		s.EffectiveYield = netInterest / fd.PrincipalAmount * (365.0 / float64(fd.TenorDays)) * 100
		s.EffectiveYield = roundByMethod(s.EffectiveYield, 4, "ROUND")
	}
	return s
}

// enrichSimpleWorkbookSummary fills workbook C16-style header totals (ACT/365 simple interest).
// Schedule total_interest_accrued is the sum of per-period rounded rows; with unified rounding
// it should match workbook totals when decimals and method align with bank config.
func enrichSimpleWorkbookSummary(s *SimulateSummary, fd *FDRecord, cfg *BankConfig, tdsRatePct float64) {
	if fd == nil || s == nil {
		return
	}
	s.WorkbookTotalInterest = simpleFormulaInterest(fd.PrincipalAmount, fd.InterestRate, fd.TenorDays, cfg)
	if tdsRatePct > 0 && s.WorkbookTotalInterest > 0 {
		rnd := engRoundingFromCfg(cfg)
		s.WorkbookTotalTDS = rnd.RoundFinal(s.WorkbookTotalInterest * tdsRatePct / 100.0)
	}
}

// enrichCompoundWorkbookSummary fills workbook C17-style header totals on a summary.
// WorkbookTotalInterest uses the closed-form compound formula for Excel parity.
// WorkbookTotalTDS is sourced from the schedule: Σ TDSAmount across CAPITALIZATION rows,
// matching the per-period rounded TDS the bank actually deducts (Σ TDS Rev L on Form 16A).
func enrichCompoundWorkbookSummary(s *SimulateSummary, fd *FDRecord, cfg *BankConfig, capFreqType string, tdsRatePct float64, rawRows []CashflowRow) {
	if fd == nil || s == nil {
		return
	}
	n := capPeriodsPerYear(capFreqType)
	s.WorkbookTotalInterest = compoundFormulaInterest(fd.PrincipalAmount, fd.InterestRate, n, fd.TenorDays, cfg)
	if tdsRatePct > 0 {
		var tdsSum float64
		for _, row := range rawRows {
			if row.EventType == "CAPITALIZATION" {
				tdsSum += row.TDSAmount
			}
		}
		s.WorkbookTotalTDS = tdsSum
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Diff Simulator — types, matching logic, and HTTP handler
// ─────────────────────────────────────────────────────────────────────────────

// SimulateDiffRequest wraps two independent simulation requests: the original
// booking parameters and the updated confirmation parameters.  Both are run
// through the same engine and their schedules are diff-ed row-by-row.
type SimulateDiffRequest struct {
	Booking      SimulateCashflowRequest `json:"booking"`      // original / old baseline
	Confirmation SimulateCashflowRequest `json:"confirmation"` // updated / new values
}

// DiffChangeType classifies how a row changed between booking and confirmation.
type DiffChangeType = string

const (
	// DiffNew — the row exists only in the confirmation schedule.
	DiffNew DiffChangeType = "NEW"
	// DiffChanged — the row exists in both schedules but has different values.
	DiffChanged DiffChangeType = "CHANGED"
	// DiffUnchanged — the row exists in both schedules with identical values.
	DiffUnchanged DiffChangeType = "UNCHANGED"
	// DiffRemoved — the row exists only in the booking schedule (dropped in confirmation).
	DiffRemoved DiffChangeType = "REMOVED"
)

// DiffCashflowRow represents one row of the merged diff schedule.
//
// Matching key: EventType + "|" + EventDate (YYYY-MM-DD).
//
//   - new_* fields are ALWAYS present (from the confirmation schedule).
//   - old_* pointer fields are nil when the row is NEW (not in booking schedule).
//   - For REMOVED rows the new_* fields are zero / empty.
//   - change_type is one of: "NEW" | "CHANGED" | "UNCHANGED" | "REMOVED".
//   - has_change is true whenever old_* and new_* differ (or one side is absent).
type DiffCashflowRow struct {
	// ── Matching key ─────────────────────────────────────────────────────
	EventType string `json:"event_type"`
	EventDate string `json:"event_date"` // YYYY-MM-DD (matched on this)

	// ── Period boundaries ─────────────────────────────────────────────────
	PeriodStartDate string `json:"period_start_date"`
	PeriodEndDate   string `json:"period_end_date"`

	// ── Old schedule values (from booking) — nil when change_type == "NEW" ─
	OldPeriodNumber            *int     `json:"old_period_number,omitempty"`
	OldPeriodDays              *int     `json:"old_period_days,omitempty"`
	OldOpeningPrincipal        *float64 `json:"old_opening_principal,omitempty"`
	OldInterestAccrued         *float64 `json:"old_interest_accrued,omitempty"`
	OldCapitalizedAmount       *float64 `json:"old_capitalized_amount,omitempty"`
	OldClosingPrincipal        *float64 `json:"old_closing_principal,omitempty"`
	OldTDSAmount               *float64 `json:"old_tds_amount,omitempty"`
	OldNetCashFlow             *float64 `json:"old_net_cash_flow,omitempty"`
	OldDayCountCode            *string  `json:"old_day_count_code,omitempty"`
	OldDivisor                 *int     `json:"old_divisor,omitempty"`
	OldFormulaUsed             *string  `json:"old_formula_used,omitempty"`
	OldAccrualRatePerDay       *float64 `json:"old_accrual_rate_per_day,omitempty"`
	OldDueNotAccrued           *float64 `json:"old_due_not_accrued,omitempty"`
	OldAccrRevK                *float64 `json:"old_accr_rev_k,omitempty"`
	OldTDSRevL                 *float64 `json:"old_tds_rev_l,omitempty"`
	OldProvisionalTDS          *float64 `json:"old_provisional_tds,omitempty"`
	OldAccrualFrequency        *string  `json:"old_accrual_frequency,omitempty"`
	OldValueDate               *string  `json:"old_value_date,omitempty"`
	OldInterestRate            *float64 `json:"old_interest_rate,omitempty"`
	OldTDSRate                 *float64 `json:"old_tds_rate,omitempty"`
	OldFinancialYear           *string  `json:"old_financial_year,omitempty"`
	OldCumulativeInterestFY    *float64 `json:"old_cumulative_interest_fy,omitempty"`
	OldCumulativeTDSFY         *float64 `json:"old_cumulative_tds_fy,omitempty"`
	OldCumulativeInterestTotal *float64 `json:"old_cumulative_interest_total,omitempty"`
	OldHolidaysInPeriod        *int     `json:"old_holidays_in_period,omitempty"`
	OldCashflowType            *string  `json:"old_cashflow_type,omitempty"`

	// ── New schedule values (from confirmation) — always present unless REMOVED ─
	NewPeriodNumber            int     `json:"new_period_number"`
	NewPeriodDays              int     `json:"new_period_days"`
	NewOpeningPrincipal        float64 `json:"new_opening_principal"`
	NewInterestAccrued         float64 `json:"new_interest_accrued"`
	NewCapitalizedAmount       float64 `json:"new_capitalized_amount"`
	NewClosingPrincipal        float64 `json:"new_closing_principal"`
	NewTDSAmount               float64 `json:"new_tds_amount"`
	NewNetCashFlow             float64 `json:"new_net_cash_flow"`
	NewDayCountCode            string  `json:"new_day_count_code"`
	NewDivisor                 int     `json:"new_divisor"`
	NewFormulaUsed             string  `json:"new_formula_used"`
	NewAccrualRatePerDay       float64 `json:"new_accrual_rate_per_day"`
	NewDueNotAccrued           float64 `json:"new_due_not_accrued"`
	NewAccrRevK                float64 `json:"new_accr_rev_k"`
	NewTDSRevL                 float64 `json:"new_tds_rev_l"`
	NewProvisionalTDS          float64 `json:"new_provisional_tds"`
	NewAccrualFrequency        string  `json:"new_accrual_frequency,omitempty"`
	NewValueDate               string  `json:"new_value_date,omitempty"`
	NewInterestRate            float64 `json:"new_interest_rate"`
	NewTDSRate                 float64 `json:"new_tds_rate"`
	NewFinancialYear           string  `json:"new_financial_year,omitempty"`
	NewCumulativeInterestFY    float64 `json:"new_cumulative_interest_fy"`
	NewCumulativeTDSFY         float64 `json:"new_cumulative_tds_fy"`
	NewCumulativeInterestTotal float64 `json:"new_cumulative_interest_total"`
	NewHolidaysInPeriod        int     `json:"new_holidays_in_period"`
	NewCashflowType            string  `json:"new_cashflow_type,omitempty"`

	// ── Diff metadata ─────────────────────────────────────────────────────
	HasChange  bool           `json:"has_change"`
	ChangeType DiffChangeType `json:"change_type"` // "NEW" | "CHANGED" | "UNCHANGED" | "REMOVED"
}

// SimulateDiffResponse is returned by SimulateDiffHandler.
type SimulateDiffResponse struct {
	// Booking-side resolved inputs.
	BookingInput SimulatedFDSummary `json:"booking_input"`
	// Confirmation-side resolved inputs.
	ConfirmationInput SimulatedFDSummary `json:"confirmation_input"`

	// Merged diff schedule — every row from either side (sorted by event_date).
	DiffSchedule []DiffCashflowRow `json:"diff_schedule"`

	// Full schedules for side-by-side UI (same sort as /simulator/cashflow).
	BookingSchedule      []SimulatedCashflowRow `json:"booking_schedule,omitempty"`
	ConfirmationSchedule []SimulatedCashflowRow `json:"confirmation_schedule,omitempty"`

	// Summary metrics for each side.
	BookingSummary      SimulateSummary `json:"booking_summary"`
	ConfirmationSummary SimulateSummary `json:"confirmation_summary"`

	// Convenience counts.
	TotalRows     int `json:"total_rows"`
	NewRows       int `json:"new_rows"`
	ChangedRows   int `json:"changed_rows"`
	RemovedRows   int `json:"removed_rows"`
	UnchangedRows int `json:"unchanged_rows"`

	// Holidays from the confirmation schedule's calendar.
	Holidays []HolidayEntry `json:"holidays,omitempty"`

	// Booking payout/compounding date arrays.
	BookingPayoutDates      *json.RawMessage `json:"booking_payout_dates,omitempty"`
	BookingCompoundingDates *json.RawMessage `json:"booking_compounding_dates,omitempty"`
	// Confirmation payout/compounding date arrays.
	ConfirmationPayoutDates      *json.RawMessage `json:"confirmation_payout_dates,omitempty"`
	ConfirmationCompoundingDates *json.RawMessage `json:"confirmation_compounding_dates,omitempty"`
}

// diffKey produces the matching key for a SimulatedCashflowRow.
// We match on EventType + "|" + EventDate so that, e.g., two ACCRUAL rows
// for the same date are treated as the same logical row.
func diffKey(row SimulatedCashflowRow) string {
	return row.EventType + "|" + row.EventDate
}

// numPtr returns a pointer to a float64 (helper to fill old_* fields).
func numPtr(f float64) *float64 { v := f; return &v }

// intPtr returns a pointer to an int (helper to fill old_* fields).
func intPtr(i int) *int { v := i; return &v }

// strPtr returns a pointer to a string (helper to fill old_* fields).
func strPtr(s string) *string { v := s; return &v }

// diffFloat64 returns true when two float64 values differ by more than epsilon.
// We use an absolute epsilon of 0.0001 (sub-paisa precision) which is generous
// enough to absorb floating-point noise but tight enough to catch real changes.
const diffEpsilon = 0.0001

func diffFloat64(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d > diffEpsilon
}

// cashflowEventTypeRank orders rows that share the same event_date (BRD / workbook).
func cashflowEventTypeRank(eventType string) int {
	switch strings.ToUpper(strings.TrimSpace(eventType)) {
	case "INITIAL_INVESTMENT":
		return 0
	case "ACCRUAL":
		return 1
	case "CAPITALIZATION":
		return 2
	case "INTEREST_RECEIPT":
		return 3
	case "TDS_DEDUCTION":
		return 4
	case "MATURITY":
		return 5
	case "PRINCIPAL_RETURN":
		return 6
	default:
		return 99
	}
}

func sortSimulatedCashflowRows(rows []SimulatedCashflowRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].EventDate != rows[j].EventDate {
			return rows[i].EventDate < rows[j].EventDate
		}
		ri := cashflowEventTypeRank(rows[i].EventType)
		rj := cashflowEventTypeRank(rows[j].EventType)
		if ri != rj {
			return ri < rj
		}
		return rows[i].PeriodNumber < rows[j].PeriodNumber
	})
}

func sortDiffCashflowRows(rows []DiffCashflowRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].EventDate != rows[j].EventDate {
			return rows[i].EventDate < rows[j].EventDate
		}
		ri := cashflowEventTypeRank(rows[i].EventType)
		rj := cashflowEventTypeRank(rows[j].EventType)
		if ri != rj {
			return ri < rj
		}
		// UNCHANGED/CHANGED before REMOVED/NEW at same slot for readability
		return diffChangeTypeRank(rows[i].ChangeType) < diffChangeTypeRank(rows[j].ChangeType)
	})
}

func diffChangeTypeRank(ct DiffChangeType) int {
	switch ct {
	case DiffUnchanged:
		return 0
	case DiffChanged:
		return 1
	case DiffNew:
		return 2
	case DiffRemoved:
		return 3
	default:
		return 9
	}
}

// diffSchedules merges two schedules (old = booking, new = confirmation)
// and produces a DiffCashflowRow slice sorted by event_date.
//
// Matching strategy:
//   - Build a map from diffKey → old row index.
//   - Walk new rows: if matching old key exists → CHANGED or UNCHANGED; else → NEW.
//   - Walk old rows that were never matched → REMOVED.
//   - Output order: chronological by event_date, then event_type rank.
func diffSchedules(oldRows, newRows []SimulatedCashflowRow) []DiffCashflowRow {
	oldRows = append([]SimulatedCashflowRow(nil), oldRows...)
	newRows = append([]SimulatedCashflowRow(nil), newRows...)
	sortSimulatedCashflowRows(oldRows)
	sortSimulatedCashflowRows(newRows)
	// Index old rows by key.  If the same key appears more than once in the
	// old schedule (rare but possible, e.g. two ACCRUAL rows on the same date)
	// we keep a slice per key and consume them in order.
	type indexedRow struct {
		row     SimulatedCashflowRow
		matched bool
	}
	oldIndex := make(map[string][]*indexedRow, len(oldRows))
	oldOrder := make([]*indexedRow, 0, len(oldRows)) // preserve insertion order for REMOVED walk
	for i := range oldRows {
		ir := &indexedRow{row: oldRows[i]}
		k := diffKey(oldRows[i])
		oldIndex[k] = append(oldIndex[k], ir)
		oldOrder = append(oldOrder, ir)
	}

	out := make([]DiffCashflowRow, 0, len(newRows)+len(oldRows)/4)

	for _, nr := range newRows {
		k := diffKey(nr)

		// Try to consume the next unmatched old row with the same key.
		var oldRow *SimulatedCashflowRow
		if bucket := oldIndex[k]; len(bucket) > 0 {
			// Find first unmatched in bucket.
			for _, ir := range bucket {
				if !ir.matched {
					ir.matched = true
					oldRow = &ir.row
					break
				}
			}
		}

		dr := DiffCashflowRow{
			EventType:       nr.EventType,
			EventDate:       nr.EventDate,
			PeriodStartDate: nr.PeriodStartDate,
			PeriodEndDate:   nr.PeriodEndDate,
			// New values — always filled.
			NewPeriodNumber:            nr.PeriodNumber,
			NewPeriodDays:              nr.PeriodDays,
			NewOpeningPrincipal:        nr.OpeningPrincipal,
			NewInterestAccrued:         nr.InterestAccrued,
			NewCapitalizedAmount:       nr.CapitalizedAmount,
			NewClosingPrincipal:        nr.ClosingPrincipal,
			NewTDSAmount:               nr.TDSAmount,
			NewNetCashFlow:             nr.NetCashFlow,
			NewDayCountCode:            nr.DayCountCode,
			NewDivisor:                 nr.Divisor,
			NewFormulaUsed:             nr.FormulaUsed,
			NewAccrualRatePerDay:       nr.AccrualRatePerDay,
			NewDueNotAccrued:           nr.DueNotAccrued,
			NewAccrRevK:                nr.AccrRevK,
			NewTDSRevL:                 nr.TDSRevL,
			NewProvisionalTDS:          nr.ProvisionalTDS,
			NewAccrualFrequency:        nr.AccrualFrequency,
			NewValueDate:               nr.ValueDate,
			NewInterestRate:            nr.InterestRate,
			NewTDSRate:                 nr.TDSRate,
			NewFinancialYear:           nr.FinancialYear,
			NewCumulativeInterestFY:    nr.CumulativeInterestFY,
			NewCumulativeTDSFY:         nr.CumulativeTDSFY,
			NewCumulativeInterestTotal: nr.CumulativeInterestTotal,
			NewHolidaysInPeriod:        nr.HolidaysInPeriod,
			NewCashflowType:            nr.CashflowType,
		}

		if oldRow == nil {
			// Row only in new schedule.
			dr.ChangeType = DiffNew
			dr.HasChange = true
		} else {
			// Fill old_* fields.
			dr.OldPeriodNumber = intPtr(oldRow.PeriodNumber)
			dr.OldPeriodDays = intPtr(oldRow.PeriodDays)
			dr.OldOpeningPrincipal = numPtr(oldRow.OpeningPrincipal)
			dr.OldInterestAccrued = numPtr(oldRow.InterestAccrued)
			dr.OldCapitalizedAmount = numPtr(oldRow.CapitalizedAmount)
			dr.OldClosingPrincipal = numPtr(oldRow.ClosingPrincipal)
			dr.OldTDSAmount = numPtr(oldRow.TDSAmount)
			dr.OldNetCashFlow = numPtr(oldRow.NetCashFlow)
			dr.OldDayCountCode = strPtr(oldRow.DayCountCode)
			dr.OldDivisor = intPtr(oldRow.Divisor)
			dr.OldFormulaUsed = strPtr(oldRow.FormulaUsed)
			dr.OldAccrualRatePerDay = numPtr(oldRow.AccrualRatePerDay)
			dr.OldDueNotAccrued = numPtr(oldRow.DueNotAccrued)
			dr.OldAccrRevK = numPtr(oldRow.AccrRevK)
			dr.OldTDSRevL = numPtr(oldRow.TDSRevL)
			dr.OldProvisionalTDS = numPtr(oldRow.ProvisionalTDS)
			dr.OldAccrualFrequency = strPtr(oldRow.AccrualFrequency)
			dr.OldValueDate = strPtr(oldRow.ValueDate)
			dr.OldInterestRate = numPtr(oldRow.InterestRate)
			dr.OldTDSRate = numPtr(oldRow.TDSRate)
			dr.OldFinancialYear = strPtr(oldRow.FinancialYear)
			dr.OldCumulativeInterestFY = numPtr(oldRow.CumulativeInterestFY)
			dr.OldCumulativeTDSFY = numPtr(oldRow.CumulativeTDSFY)
			dr.OldCumulativeInterestTotal = numPtr(oldRow.CumulativeInterestTotal)
			dr.OldHolidaysInPeriod = intPtr(oldRow.HolidaysInPeriod)
			dr.OldCashflowType = strPtr(oldRow.CashflowType)

			// Detect if anything changed.
			changed := diffFloat64(oldRow.OpeningPrincipal, nr.OpeningPrincipal) ||
				diffFloat64(oldRow.InterestAccrued, nr.InterestAccrued) ||
				diffFloat64(oldRow.CapitalizedAmount, nr.CapitalizedAmount) ||
				diffFloat64(oldRow.ClosingPrincipal, nr.ClosingPrincipal) ||
				diffFloat64(oldRow.TDSAmount, nr.TDSAmount) ||
				diffFloat64(oldRow.NetCashFlow, nr.NetCashFlow) ||
				diffFloat64(oldRow.AccrualRatePerDay, nr.AccrualRatePerDay) ||
				diffFloat64(oldRow.DueNotAccrued, nr.DueNotAccrued) ||
				diffFloat64(oldRow.AccrRevK, nr.AccrRevK) ||
				diffFloat64(oldRow.TDSRevL, nr.TDSRevL) ||
				diffFloat64(oldRow.ProvisionalTDS, nr.ProvisionalTDS) ||
				oldRow.PeriodDays != nr.PeriodDays ||
				oldRow.DayCountCode != nr.DayCountCode ||
				oldRow.AccrualFrequency != nr.AccrualFrequency ||
				oldRow.ValueDate != nr.ValueDate

			if changed {
				dr.ChangeType = DiffChanged
				dr.HasChange = true
			} else {
				dr.ChangeType = DiffUnchanged
				dr.HasChange = false
			}
		}

		out = append(out, dr)
	}

	// Append REMOVED rows — old rows that were never matched by any new row.
	for _, ir := range oldOrder {
		if ir.matched {
			continue
		}
		or := ir.row
		out = append(out, DiffCashflowRow{
			EventType:       or.EventType,
			EventDate:       or.EventDate,
			PeriodStartDate: or.PeriodStartDate,
			PeriodEndDate:   or.PeriodEndDate,
			// Old values present.
			OldPeriodNumber:            intPtr(or.PeriodNumber),
			OldPeriodDays:              intPtr(or.PeriodDays),
			OldOpeningPrincipal:        numPtr(or.OpeningPrincipal),
			OldInterestAccrued:         numPtr(or.InterestAccrued),
			OldCapitalizedAmount:       numPtr(or.CapitalizedAmount),
			OldClosingPrincipal:        numPtr(or.ClosingPrincipal),
			OldTDSAmount:               numPtr(or.TDSAmount),
			OldNetCashFlow:             numPtr(or.NetCashFlow),
			OldDayCountCode:            strPtr(or.DayCountCode),
			OldDivisor:                 intPtr(or.Divisor),
			OldFormulaUsed:             strPtr(or.FormulaUsed),
			OldAccrualRatePerDay:       numPtr(or.AccrualRatePerDay),
			OldDueNotAccrued:           numPtr(or.DueNotAccrued),
			OldAccrRevK:                numPtr(or.AccrRevK),
			OldTDSRevL:                 numPtr(or.TDSRevL),
			OldProvisionalTDS:          numPtr(or.ProvisionalTDS),
			OldAccrualFrequency:        strPtr(or.AccrualFrequency),
			OldValueDate:               strPtr(or.ValueDate),
			OldInterestRate:            numPtr(or.InterestRate),
			OldTDSRate:                 numPtr(or.TDSRate),
			OldFinancialYear:           strPtr(or.FinancialYear),
			OldCumulativeInterestFY:    numPtr(or.CumulativeInterestFY),
			OldCumulativeTDSFY:         numPtr(or.CumulativeTDSFY),
			OldCumulativeInterestTotal: numPtr(or.CumulativeInterestTotal),
			OldHolidaysInPeriod:        intPtr(or.HolidaysInPeriod),
			OldCashflowType:            strPtr(or.CashflowType),
			// New values are zero / empty (row was removed).
			ChangeType: DiffRemoved,
			HasChange:  true,
		})
	}

	sortDiffCashflowRows(out)
	return out
}

// countDiffTypes tallies the four change types in a diff schedule.
func countDiffTypes(rows []DiffCashflowRow) (newRows, changed, removed, unchanged int) {
	for _, r := range rows {
		switch r.ChangeType {
		case DiffNew:
			newRows++
		case DiffChanged:
			changed++
		case DiffRemoved:
			removed++
		case DiffUnchanged:
			unchanged++
		}
	}
	return
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP handler — POST /investment/fd/simulator/cashflow/diff
// ─────────────────────────────────────────────────────────────────────────────

// SimulateDiffHandler runs two independent cashflow simulations (booking vs
// confirmation) and returns a merged diff schedule showing what changed.
//
// POST /investment/fd/simulator/cashflow/diff
//
//	{
//	  "booking": {
//	    "principal_amount": 1000000,
//	    "interest_rate": 7.5,
//	    "start_date": "2025-04-01",
//	    "tenor_months": 12,
//	    "bank_config_id": "bcfg-xxx",
//	    "interest_type": "COMPOUND",
//	    "frequency_id": "freq-quarterly"
//	  },
//	  "confirmation": {
//	    "principal_amount": 1000000,
//	    "interest_rate": 7.75,       ← rate changed
//	    "start_date": "2025-04-01",
//	    "tenor_months": 12,
//	    "bank_config_id": "bcfg-xxx",
//	    "interest_type": "COMPOUND",
//	    "frequency_id": "freq-quarterly"
//	  }
//	}
//
// Response shape:
//
//	{
//	  "booking_input":      {...},
//	  "confirmation_input": {...},
//	  "booking_summary":    {...},
//	  "confirmation_summary": {...},
//	  "diff_schedule": [
//	    {
//	      "event_type": "CAPITALIZATION",
//	      "event_date": "2025-07-01",
//	      "old_interest_accrued": 18750.00,   ← booking
//	      "new_interest_accrued": 19375.00,   ← confirmation
//	      "change_type": "CHANGED",
//	      "has_change": true
//	    },
//	    ...
//	  ],
//	  "total_rows": 5, "new_rows": 0, "changed_rows": 4, "removed_rows": 0, "unchanged_rows": 1
//	}
func SimulateDiffHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SimulateDiffRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		ctx := r.Context()
		conn, err := pool.Acquire(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusServiceUnavailable, constants.ErrDBConnection)
			return
		}
		defer conn.Release()

		// ── Run booking simulation ────────────────────────────────────────
		bookingResult, err := runSimulationForRequest(ctx, conn, req.Booking)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "booking simulation error: "+err.Error())
			return
		}

		// ── Run confirmation simulation ───────────────────────────────────
		confirmResult, err := runSimulationForRequest(ctx, conn, req.Confirmation)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "confirmation simulation error: "+err.Error())
			return
		}

		// ── Diff the two schedules ────────────────────────────────────────
		diffRows := diffSchedules(bookingResult.Schedule, confirmResult.Schedule)

		newCnt, changedCnt, removedCnt, unchangedCnt := countDiffTypes(diffRows)

		api.RespondWithPayload(w, true, "", SimulateDiffResponse{
			BookingInput:                 bookingResult.ResolvedInput,
			ConfirmationInput:            confirmResult.ResolvedInput,
			DiffSchedule:                 diffRows,
			BookingSchedule:              bookingResult.Schedule,
			ConfirmationSchedule:         confirmResult.Schedule,
			BookingSummary:               bookingResult.Summary,
			ConfirmationSummary:          confirmResult.Summary,
			TotalRows:                    len(diffRows),
			NewRows:                      newCnt,
			ChangedRows:                  changedCnt,
			RemovedRows:                  removedCnt,
			UnchangedRows:                unchangedCnt,
			Holidays:                     confirmResult.Holidays,
			BookingPayoutDates:           bookingResult.PayoutDates,
			BookingCompoundingDates:      bookingResult.CompoundingDates,
			ConfirmationPayoutDates:      confirmResult.PayoutDates,
			ConfirmationCompoundingDates: confirmResult.CompoundingDates,
		})
	}
}
