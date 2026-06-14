// cashflow_engine.go — Clean rewrite of the FD cashflow schedule generator.
//
// Implements the formulas from FD_Scenarios_v7.xlsx exactly as described in
// "FD Cashflow Engine Rewrite.md". This file replaces the logic in the legacy
// buildMonthlyAccrualDates / buildAccrualDates / buildSIAccrualDates /
// buildCapitalizationDates / buildPayoutDates / generateCompoundSchedule
// functions.  It is called from generateCashflowSchedule (cashflow.go) via
// the new engSchedule* entry points.
//
// Key rules (all from the spec):
//  1. Event dates = addMonthsRollback(start, k*months) — calendar month-end, NOT month_end-1.
//  2. Accrual tenor = D - prev_row_D (simple subtraction).
//  3. Rounding: bank config interest_rounding_decimals + rounding_method (shared rounding package).
//  4. SI: per payout-window — accruals first (date ≤ payout_date), then payout/maturity row.
//  5. CO: per cap-window  — accruals first (date ≤ cap_date), then cap row, then optional payout.
//  6. Same-date ordering: INITIAL_INVESTMENT → ACCRUAL → CAPITALIZATION → INTEREST_PAYOUT/MATURITY → PRINCIPAL_RETURN.
//  7. ValueDate: D for everything except INTEREST_PAYOUT (D+2) and CO MATURITY (D+2).
//  8. SI MATURITY valueDate = D (no +2).

package fdMaster

import (
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/investment/rounding"
)

// ── Date helpers ──────────────────────────────────────────────────────────────

// engAddMonths shifts d forward by months, clamping to month-end when the
// day-of-month doesn't exist in the target month.
// E.g. Jan 31 + 1 month → Feb 28 (non-leap) or Feb 29 (leap).
func engAddMonths(d time.Time, months int) time.Time {
	y, m, day := d.Date()
	target := time.Date(y, m+time.Month(months), 1, 0, 0, 0, 0, time.UTC)
	lastDay := time.Date(target.Year(), target.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
	if day > lastDay {
		day = lastDay
	}
	return time.Date(target.Year(), target.Month(), day, 0, 0, 0, 0, time.UTC)
}

// engEventDatesMaxIter caps the stepping loop (~100 years at monthly frequency).
const engEventDatesMaxIter = 1200

// engEventDates returns dates from start (exclusive) to maturity (inclusive)
// stepping by monthsPerStep using calendar month-end roll-back.
// The returned slice always ends exactly at maturity.
func engEventDates(start, maturity time.Time, monthsPerStep int) []time.Time {
	// monthsPerStep <= 0: AT_MATURITY — single boundary at maturity (never loop).
	if monthsPerStep <= 0 {
		return []time.Time{maturity}
	}
	var out []time.Time
	for k := 1; k <= engEventDatesMaxIter; k++ {
		d := engAddMonths(start, monthsPerStep*k)
		if !d.Before(maturity) {
			out = append(out, maturity)
			return out
		}
		out = append(out, d)
	}
	// Defense in depth: abnormal tenor/frequency combo — still terminate at maturity.
	out = append(out, maturity)
	return out
}

// engEventDatesAnchored steps from firstAnchor when set (first cap/payout on that calendar day),
// then adds monthsPerStep from each prior boundary. Without anchor, delegates to engEventDates.
func engEventDatesAnchored(start, maturity time.Time, monthsPerStep int, anchor time.Time) []time.Time {
	if monthsPerStep <= 0 {
		return []time.Time{maturity}
	}
	if anchor.IsZero() {
		return engEventDates(start, maturity, monthsPerStep)
	}
	first := anchor
	if !first.After(start) {
		first = engAddMonths(start, monthsPerStep)
	}
	var out []time.Time
	cur := first
	for range engEventDatesMaxIter {
		if !cur.Before(maturity) {
			break
		}
		out = append(out, cur)
		cur = engAddMonths(cur, monthsPerStep)
	}
	if len(out) == 0 || !out[len(out)-1].Equal(maturity) {
		out = append(out, maturity)
	}
	return out
}

// engResolveAccrualMonths picks accrual step months: explicit param, then payout/cap, then monthly.
func engResolveAccrualMonths(explicit, payoutOrCapMonths int) int {
	m := explicit
	if m <= 0 {
		m = payoutOrCapMonths
	}
	if m <= 0 {
		m = 1
	}
	return m
}

// engDivisorAndDays returns (divisor, days) for an ACCRUAL row.
// Applies accrual_start/end_convention and weekend/holiday flags from the bank config.
// days is clamped to >= 1.
func engDivisorAndDays(conventionType string, start, end time.Time, cfg *BankConfig, cal HolidayCalendarInfo) (divisor, days int) {
	divisor, days = getDivisorAndDaysWithCal(conventionType, start, end, cfg, cal)
	if days <= 0 {
		days = 1
	}
	return
}

// engCapDivisorAndDays returns (divisor, days) for a CAPITALIZATION or PAYOUT period.
// Cap/payout boundaries are governed by period_boundary_definition only:
//   INCL_BOTH        → raw (end−start) + 1
//   EXCL_BOTH        → raw (end−start) − 1
//   INCL_START_EXCL_END / EXCL_START_INCL_END / default → raw (end−start)
//
// accrual_start/end_convention is intentionally NOT applied here — those fields
// are accrual-row controls (like weekend_accrual / holiday_accrual).
func engCapDivisorAndDays(conventionType string, start, end time.Time, cfg *BankConfig) (divisor, days int) {
	norm := strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(strings.TrimSpace(conventionType)))
	norm = strings.TrimPrefix(norm, "DC_")

	if norm == "30_360" {
		return 360, countDays30by360(start, end)
	}

	days = int(end.Sub(start).Hours() / 24)

	if cfg != nil {
		switch strings.ToUpper(strings.TrimSpace(cfg.PeriodBoundaryDefinition)) {
		case "INCL_BOTH":
			days++
		case "EXCL_BOTH":
			if days > 0 {
				days--
			}
		}
	}
	if days <= 0 {
		days = 1
	}

	switch norm {
	case "ACT_360":
		return 360, days
	case "ACT_ACT":
		divisorVal := 365
		if isLeapYear(end.Year()) {
			divisorVal = 366
		}
		return divisorVal, days
	default: // ACT_365
		return 365, days
	}
}

// engRoundingFromCfg builds unified rounding settings from bank config.
func engRoundingFromCfg(cfg *BankConfig) rounding.Config {
	if cfg == nil {
		return rounding.FromBankConfig(rounding.DefaultDecimals, "ROUND", "EACH_PERIOD")
	}
	return rounding.FromBankConfig(cfg.InterestRoundingDecimals, cfg.RoundingMethod, cfg.RoundingFrequency)
}

// engDaysBetween returns the integer number of days from a to b.
func engDaysBetween(a, b time.Time) int {
	return int(b.Sub(a).Hours() / 24)
}

// ── Config-aware capitalization boundary builder (BRD: Bank FD Configuration) ─
//
// Honors the bank config fields that drive period boundaries:
//
//	capitalization_schedule_type   ANNIVERSARY | CALENDAR_QTR_END | MONTH_END | FIXED_DAY
//	quarter_definition             ACTUAL_QTR_END (calendar) | 90_DAYS (fixed 90-day quarters)
//	capitalization_date_adjustment NO_ADJUST | PRECEDING_WD | FOLLOWING_WD
//	broken_period_location         FIRST | LAST | BOTH (grid anchoring for anniversary mode)
//	minimum_compounding_period_days  interior windows shorter than this are merged forward
//
// A "stub" (broken period) is any window shorter than one full step. StubKeys
// marks the event-date keys of broken windows so engCOSchedule can apply
// broken_period_method (SIMPLE / COMPOUND / HYBRID / NONE).

// engCapSchedule holds capitalization boundaries plus broken-period metadata.
type engCapSchedule struct {
	Dates    []time.Time
	StubKeys map[string]bool // keyed YYYY-MM-DD on the window-end event date
}

// engNormalizeCapScheduleType maps BRD/legacy tokens to one of three modes.
func engNormalizeCapScheduleType(s string) string {
	v := strings.ToUpper(strings.NewReplacer("-", "_", " ", "_", "/", "_").Replace(strings.TrimSpace(s)))
	switch v {
	case "CALENDAR_QTR_END", "CALENDAR_QUARTER_END", "QUARTER_END", "QTR_END", "ACTUAL_QTR_END":
		return "QTR_END"
	case "MONTH_END":
		return "MONTH_END"
	default: // ANNIVERSARY | FIXED_DAY | "" | unknown
		return "ANNIVERSARY"
	}
}

func engIsCalendarQuarterEnd(d time.Time) bool {
	return nextCalendarQuarterEnd(d).Equal(d)
}

func engIsMonthEnd(d time.Time) bool {
	return lastDayOfMonth(d.Year(), d.Month()).Equal(d)
}

// engAdjustWorkingDay applies capitalization_date_adjustment to one boundary.
// Unlike the legacy adjustToWorkingDay it derives working days purely from the
// holiday calendar — the weekend/holiday ACCRUAL flags control day counting,
// not event-date placement (BRD: "Holiday Calendar must adjust event_date").
func engAdjustWorkingDay(d time.Time, adjustment string, cal HolidayCalendarInfo) time.Time {
	if cal.WeekendPattern == "" && len(cal.HolidayDates) == 0 {
		return d
	}
	switch strings.ToUpper(strings.TrimSpace(adjustment)) {
	case "FOLLOWING_WD", "FOLLOWING", "NEXT_WD":
		return NextWorkingDay(d, cal)
	case "PRECEDING_WD", "PRECEDING", "PREV_WD":
		return PrevWorkingDay(d, cal)
	default: // NO_ADJUST | ""
		return d
	}
}

// engCapDatesFromConfig builds the capitalization boundaries for a compound FD
// from the bank configuration. An explicit first_capitalization_date anchor
// always wins (bank-confirmed schedule) and keeps the legacy anchored behavior.
func engCapDatesFromConfig(fd *FDRecord, cfg *BankConfig, capMonths int, cal HolidayCalendarInfo) engCapSchedule {
	out := engCapSchedule{StubKeys: map[string]bool{}}
	start, maturity := fd.ValueDate, fd.MaturityDate

	if !fd.FirstCapitalizationDate.IsZero() || cfg == nil {
		out.Dates = engEventDatesAnchored(start, maturity, capMonths, fd.FirstCapitalizationDate)
		return out
	}

	type boundary struct {
		date time.Time
		stub bool
	}
	var bounds []boundary

	mode := engNormalizeCapScheduleType(cfg.CapitalizationScheduleType)
	qdef := strings.ToUpper(strings.NewReplacer("-", "_", " ", "_").Replace(strings.TrimSpace(cfg.QuarterDefinition)))
	loc := strings.ToUpper(strings.TrimSpace(cfg.BrokenPeriodLocation))

	switch mode {
	case "QTR_END":
		// Fixed calendar quarter-end grid (Mar 31 / Jun 30 / Sep 30 / Dec 31).
		quarters := capMonths / 3
		if quarters < 1 {
			quarters = 1
		}
		cur := start
		for range engEventDatesMaxIter {
			next := nextCalendarQuarterEnd(cur)
			if !next.After(cur) {
				next = nextCalendarQuarterEnd(cur.AddDate(0, 1, 0))
			}
			for q := 1; q < quarters; q++ {
				next = nextCalendarQuarterEnd(next.AddDate(0, 1, 0))
			}
			if !next.Before(maturity) {
				break
			}
			bounds = append(bounds, boundary{date: next})
			cur = next
		}
		// Mid-quarter start → broken first period; off-grid maturity → broken last period.
		if len(bounds) > 0 && !engIsCalendarQuarterEnd(start) {
			bounds[0].stub = true
		}
		bounds = append(bounds, boundary{date: maturity, stub: !engIsCalendarQuarterEnd(maturity)})

	case "MONTH_END":
		step := capMonths
		if step < 1 {
			step = 1
		}
		idx := start.Year()*12 + int(start.Month()) - 1
		if !lastDayOfMonth(idx/12, time.Month(idx%12+1)).After(start) {
			idx++
		}
		for range engEventDatesMaxIter {
			me := lastDayOfMonth(idx/12, time.Month(idx%12+1))
			if !me.Before(maturity) {
				break
			}
			bounds = append(bounds, boundary{date: me})
			idx += step
		}
		if len(bounds) > 0 && !engIsMonthEnd(start) {
			bounds[0].stub = true
		}
		bounds = append(bounds, boundary{date: maturity, stub: !engIsMonthEnd(maturity)})

	default: // ANNIVERSARY / FIXED_DAY
		stepDays := 0
		if qdef == "90_DAYS" && capMonths == 3 {
			stepDays = 90 // fixed 90-day quarters instead of calendar-month anniversaries
		}
		stepFwd := func(d time.Time) time.Time {
			if stepDays > 0 {
				return d.AddDate(0, 0, stepDays)
			}
			return engAddMonths(d, capMonths)
		}
		stepBack := func(d time.Time) time.Time {
			if stepDays > 0 {
				return d.AddDate(0, 0, -stepDays)
			}
			return engAddMonths(d, -capMonths)
		}
		if loc == "FIRST" {
			// Regular grid anchored at maturity — the broken period sits at the start.
			var rev []time.Time
			cur := maturity
			for range engEventDatesMaxIter {
				prev := stepBack(cur)
				if !prev.After(start) {
					break
				}
				rev = append(rev, prev)
				cur = prev
			}
			stubFirst := !stepBack(cur).Equal(start)
			for i := len(rev) - 1; i >= 0; i-- {
				bounds = append(bounds, boundary{date: rev[i], stub: i == len(rev)-1 && stubFirst})
			}
			bounds = append(bounds, boundary{date: maturity, stub: len(rev) == 0 && stubFirst})
		} else {
			// LAST | BOTH | "" — forward stepping; the stub (if any) lands at maturity.
			cur := start
			aligned := false
			for range engEventDatesMaxIter {
				next := stepFwd(cur)
				if !next.Before(maturity) {
					aligned = next.Equal(maturity)
					break
				}
				bounds = append(bounds, boundary{date: next})
				cur = next
			}
			bounds = append(bounds, boundary{date: maturity, stub: !aligned})
		}
	}

	// minimum_compounding_period_days: merge interior windows that are too short
	// into the following window (the maturity boundary is never dropped).
	if minDays := cfg.MinimumCompoundingPeriodDays; minDays > 0 && len(bounds) > 1 {
		merged := make([]boundary, 0, len(bounds))
		prev := start
		carryStub := false
		for i, b := range bounds {
			if i < len(bounds)-1 && engDaysBetween(prev, b.date) < minDays {
				carryStub = carryStub || b.stub // a merged window is still broken
				continue
			}
			if carryStub {
				b.stub = true
				carryStub = false
			}
			merged = append(merged, b)
			prev = b.date
		}
		bounds = merged
	}

	// capitalization_date_adjustment on interior boundaries. The maturity
	// boundary is owned by the booking/simulator layer and is never moved here.
	adj := cfg.CapitalizationDateAdjustment
	for i, b := range bounds {
		d := b.date
		if i < len(bounds)-1 {
			d = engAdjustWorkingDay(d, adj, cal)
			if !d.After(start) || !d.Before(maturity) {
				continue // adjustment pushed the boundary out of range — drop it
			}
		}
		if len(out.Dates) > 0 && !d.After(out.Dates[len(out.Dates)-1]) {
			continue // keep boundaries strictly increasing
		}
		out.Dates = append(out.Dates, d)
		if b.stub {
			out.StubKeys[d.Format(constants.DateFormat)] = true
		}
	}
	if len(out.Dates) == 0 || !out.Dates[len(out.Dates)-1].Equal(maturity) {
		out.Dates = append(out.Dates, maturity)
	}
	return out
}

type engFormulaParams struct {
	Principal float64
	Rate      float64
	Days      int
	Divisor   int
	Raw       float64
	Rounded   float64
	Rnd       rounding.Config
}

func engFormula(label string, p engFormulaParams) string {
	return fmt.Sprintf("%s P(%.2f) * r(%.4f%%) * d(%d) / D(%d) = %.4f | rnd(%s,%d,freq=%s) = %.2f",
		label, p.Principal, p.Rate*100, p.Days, p.Divisor, p.Raw, p.Rnd.Method, p.Rnd.Decimals, p.Rnd.Frequency, p.Rounded)
}

// ── SI schedule ───────────────────────────────────────────────────────────────

// engSISchedule generates the full cashflow schedule for a Simple Interest FD.
func engSISchedule(p CashflowScheduleParams) []CashflowRow {
	fd := p.FD
	tdsCfg := p.TDSCfg
	dcCode := firstNonEmpty(p.DCInfo.DayCountCode, fd.DayCountConvention, p.Cfg.DayCountCode, "DC-ACT-365")

	P := fd.PrincipalAmount
	r := fd.InterestRate / 100.0 // convert % to decimal
	var tRate float64
	hasTDS := tdsCfg != nil && tdsCfg.TDSRate > 0
	if hasTDS {
		tRate = tdsCfg.TDSRate / 100.0
	}
	convention := firstNonEmpty(p.DCInfo.ConventionType, normConventionStatic(p.Cfg.DayCountCode), "ACT_365")
	rnd := engRoundingFromCfg(p.Cfg)

	// Payout and accrual frequencies
	payoutMonths := p.AccrualFreqMonths // for SI freq is stored in Freq
	if p.Freq != nil {
		if m := freqTypeToMonths(firstNonEmpty(p.Freq.FrequencyType, p.Freq.FrequencyCode)); m > 0 {
			payoutMonths = m
		}
	}
	if p.PayoutFreqOverride != nil && p.PayoutFreqOverride.FrequencyID != "" {
		if m := freqTypeToMonths(firstNonEmpty(p.PayoutFreqOverride.FrequencyType, p.PayoutFreqOverride.FrequencyCode)); m > 0 {
			payoutMonths = m
		}
	}

	if strings.ToUpper(strings.TrimSpace(fd.ResetType)) == "AT_MATURITY" {
		payoutMonths = 0 // single payout at maturity — do not pass 0 into engEventDates for payouts
	}

	accrualMonths := engResolveAccrualMonths(p.AccrualFreqMonths, payoutMonths)

	var payoutDates []time.Time
	if payoutMonths > 0 {
		payoutDates = engEventDatesAnchored(fd.ValueDate, fd.MaturityDate, payoutMonths, fd.FirstPayoutDate)
	} else {
		payoutDates = []time.Time{fd.MaturityDate}
	}
	accrualDates := engEventDates(fd.ValueDate, fd.MaturityDate, accrualMonths)

	var rows []CashflowRow

	// INITIAL_INVESTMENT
	rows = append(rows, CashflowRow{
		EventType:        "INITIAL_INVESTMENT",
		EventDate:        fd.ValueDate,
		ValueDate:        fd.ValueDate,
		CashflowType:     "OUTFLOW",
		PeriodDays:       0,
		OpeningPrincipal: P,
		ClosingPrincipal: P,
		NetCashFlow:      -P,
		DayCountCode:     dcCode,
	})

	prevRowDate := fd.ValueDate
	lastPayoutDate := fd.ValueDate

	for _, pd := range payoutDates {
		isMaturity := pd.Equal(fd.MaturityDate)

		// Emit all accrual rows in (lastPayoutDate, pd]
		for _, ad := range accrualDates {
			if ad.After(lastPayoutDate) && !ad.After(pd) {
				periodDivisor, periodDays := engDivisorAndDays(convention, prevRowDate, ad, p.Cfg, p.CalInfo)
				rawGross := P * r * float64(periodDays) / float64(periodDivisor)
				gross := rnd.RoundInterest(rawGross)
				if gross < 0 {
					gross = 0
				}
				var provTDS float64
				if hasTDS {
					provTDS = rnd.RoundInterest(gross * tRate)
				}
				rows = append(rows, CashflowRow{
					EventType:         "ACCRUAL",
					EventDate:         ad,
					ValueDate:         ad,
					CashflowType:      "NA",
					PeriodStartDate:   prevRowDate,
					PeriodEndDate:     ad,
					PeriodDays:        periodDays,
					OpeningPrincipal:  P,
					InterestAccrued:   gross,
					ClosingPrincipal:  P,
					TDSAmount:         provTDS, // provisional TDS — not actually deducted on SI accrual
					NetCashFlow:       gross,   // net = gross for SI accrual
					DayCountCode:      dcCode,
					Divisor:           periodDivisor,
					FormulaUsed:       engFormula("[SI ACCRUAL]", engFormulaParams{Principal: P, Rate: r, Days: periodDays, Divisor: periodDivisor, Raw: rawGross, Rounded: gross, Rnd: rnd}),
					AccrualRatePerDay: r / float64(periodDivisor),
				})
				prevRowDate = ad
			}
		}

		// Compute payout row (Interest Payout or Maturity)
		payoutDivisor, payoutDays := engCapDivisorAndDays(convention, lastPayoutDate, pd, p.Cfg)
		payoutRaw := P * r * float64(payoutDays) / float64(payoutDivisor)
		var payoutGross float64
		if isMaturity {
			payoutGross = rnd.RoundFinal(payoutRaw)
		} else {
			payoutGross = rnd.RoundInterest(payoutRaw)
		}
		if payoutGross < 0 {
			payoutGross = 0
		}
		var payoutTDS float64
		if hasTDS {
			if isMaturity {
				payoutTDS = rnd.RoundFinal(payoutGross * tRate)
			} else {
				payoutTDS = rnd.RoundInterest(payoutGross * tRate)
			}
		}
		payoutNet := payoutGross - payoutTDS

		// K = sum of gross from accrual rows in (lastPayoutDate, pd]
		var accrRevK, tdsRevL float64
		for _, r2 := range rows {
			if r2.EventType == "ACCRUAL" && r2.EventDate.After(lastPayoutDate) && !r2.EventDate.After(pd) {
				accrRevK += r2.InterestAccrued
				tdsRevL += r2.TDSAmount
			}
		}
		dueNotAccr := payoutGross - accrRevK
		if dueNotAccr < 0 {
			dueNotAccr = 0
		}

		if isMaturity {
			// SI Maturity: valueDate = eventDate (no T+2)
			rows = append(rows, CashflowRow{
				EventType:         "MATURITY",
				EventDate:         pd,
				ValueDate:         pd,
				CashflowType:      "INFLOW",
				PeriodStartDate:   lastPayoutDate,
				PeriodEndDate:     pd,
				PeriodDays:        payoutDays,
				OpeningPrincipal:  P,
				InterestAccrued:   payoutGross,
				ClosingPrincipal:  P,
				TDSAmount:         payoutTDS,
				NetCashFlow:       payoutNet,
				AccrRevK:          accrRevK,
				TDSRevL:           tdsRevL,
				DueNotAccrued:     dueNotAccr,
				DayCountCode:      dcCode,
				Divisor:           payoutDivisor,
				FormulaUsed:       engFormula("[SI MATURITY]", engFormulaParams{Principal: P, Rate: r, Days: payoutDays, Divisor: payoutDivisor, Raw: payoutRaw, Rounded: payoutGross, Rnd: rnd}),
				AccrualRatePerDay: r / float64(payoutDivisor),
			})
			// Principal Return
			rows = append(rows, CashflowRow{
				EventType:        "PRINCIPAL_RETURN",
				EventDate:        pd,
				ValueDate:        pd,
				CashflowType:     "INFLOW",
				PeriodStartDate:  pd,
				PeriodEndDate:    pd,
				OpeningPrincipal: P,
				InterestAccrued:  0,
				ClosingPrincipal: 0,
				NetCashFlow:      P,
				DayCountCode:     dcCode,
			})
		} else {
			// Interest Payout: valueDate = eventDate + 2
			rows = append(rows, CashflowRow{
				EventType:         "INTEREST_RECEIPT",
				EventDate:         pd,
				ValueDate:         pd.AddDate(0, 0, 2),
				CashflowType:      "INFLOW",
				PeriodStartDate:   lastPayoutDate,
				PeriodEndDate:     pd,
				PeriodDays:        payoutDays,
				OpeningPrincipal:  P,
				InterestAccrued:   payoutGross,
				ClosingPrincipal:  P,
				TDSAmount:         payoutTDS,
				NetCashFlow:       payoutNet,
				AccrRevK:          accrRevK,
				TDSRevL:           tdsRevL,
				DueNotAccrued:     dueNotAccr,
				DayCountCode:      dcCode,
				Divisor:           payoutDivisor,
				FormulaUsed:       engFormula("[SI PAYOUT]", engFormulaParams{Principal: P, Rate: r, Days: payoutDays, Divisor: payoutDivisor, Raw: payoutRaw, Rounded: payoutGross, Rnd: rnd}),
				AccrualRatePerDay: r / float64(payoutDivisor),
			})
		}

		prevRowDate = pd
		lastPayoutDate = pd
	}

	return rows
}

// ── CO schedule ───────────────────────────────────────────────────────────────

// engCOSchedule generates the full cashflow schedule for a Compound Interest FD.
func engCOSchedule(p CashflowScheduleParams) []CashflowRow {
	fd := p.FD
	tdsCfg := p.TDSCfg
	dcCode := firstNonEmpty(p.DCInfo.DayCountCode, fd.DayCountConvention, p.Cfg.DayCountCode, "DC-ACT-365")

	P := fd.PrincipalAmount
	r := fd.InterestRate / 100.0
	var tRate float64
	hasTDS := tdsCfg != nil && tdsCfg.TDSRate > 0
	if hasTDS {
		tRate = tdsCfg.TDSRate / 100.0
	}
	convention := firstNonEmpty(p.DCInfo.ConventionType, normConventionStatic(p.Cfg.DayCountCode), "ACT_365")
	rnd := engRoundingFromCfg(p.Cfg)

	// Cap frequency (from Freq)
	capMonths := 3 // default quarterly
	if p.Freq != nil {
		if m := freqTypeToMonths(firstNonEmpty(p.Freq.FrequencyType, p.Freq.FrequencyCode)); m > 0 {
			capMonths = m
		}
	}

	// Payout frequency (from PayoutFreqOverride or Freq)
	payoutMonths := 0 // 0 = AT_MATURITY
	if p.PayoutFreqOverride != nil && p.PayoutFreqOverride.FrequencyID != "" {
		if m := freqTypeToMonths(firstNonEmpty(p.PayoutFreqOverride.FrequencyType, p.PayoutFreqOverride.FrequencyCode)); m > 0 {
			payoutMonths = m
		}
		// AT_MATURITY payout code
		pt := firstNonEmpty(p.PayoutFreqOverride.FrequencyType, p.PayoutFreqOverride.FrequencyCode)
		if freqTypeToMonths(pt) == 0 {
			payoutMonths = 0
		}
	}

	if strings.ToUpper(strings.TrimSpace(fd.ResetType)) == "AT_MATURITY" {
		payoutMonths = 0
	}

	if capMonths <= 0 {
		capMonths = 3 // compound cap default when master freq is unknown
	}
	accrualMonths := engResolveAccrualMonths(p.AccrualFreqMonths, capMonths)

	// Capitalization boundaries honor capitalization_schedule_type, quarter_definition,
	// capitalization_date_adjustment, broken_period_location and min compounding days.
	capSched := engCapDatesFromConfig(fd, p.Cfg, capMonths, p.CalInfo)
	capDates := capSched.Dates
	brokenMethod := ""
	if p.Cfg != nil {
		brokenMethod = strings.ToUpper(strings.TrimSpace(p.Cfg.BrokenPeriodMethod))
	}
	accrualDates := engEventDates(fd.ValueDate, fd.MaturityDate, accrualMonths)

	// Payout dates: explicit AT_MATURITY (maturity only) vs stepped schedule.
	var payoutDateSlice []time.Time
	if payoutMonths > 0 {
		payoutDateSlice = engEventDatesAnchored(fd.ValueDate, fd.MaturityDate, payoutMonths, fd.FirstPayoutDate)
	} else {
		payoutDateSlice = []time.Time{fd.MaturityDate}
	}
	payoutDateSet := make(map[string]bool, len(payoutDateSlice))
	for _, d := range payoutDateSlice {
		payoutDateSet[d.Format(constants.DateFormat)] = true
	}

	resetType := fd.ResetType // "AT_EACH_PAYOUT" or "AT_MATURITY"

	var rows []CashflowRow

	// INITIAL_INVESTMENT
	rows = append(rows, CashflowRow{
		EventType:        "INITIAL_INVESTMENT",
		EventDate:        fd.ValueDate,
		ValueDate:        fd.ValueDate,
		CashflowType:     "OUTFLOW",
		PeriodDays:       0,
		OpeningPrincipal: P,
		ClosingPrincipal: P,
		NetCashFlow:      -P,
		NetAmount:        P,
		DayCountCode:     dcCode,
	})

	prevRowDate := fd.ValueDate
	lastCapDate := fd.ValueDate
	lastPayoutDate := fd.ValueDate
	prevCapJRaw := P         // unrounded J of last cap row (matches Excel float chain)
	prevCapJRnd := P         // rounded J for display in CashflowRow
	postPayoutReset := false // true right after a payout row in AT_EACH_PAYOUT mode
	carriedSimple := 0.0     // broken-period interest kept simple (not capitalized), settled at maturity

	for _, cd := range capDates {
		isMaturity := cd.Equal(fd.MaturityDate)
		isPayoutDate := payoutDateSet[cd.Format(constants.DateFormat)]

		// F source for this cap window.
		// Use the unrounded raw J to propagate full precision (Excel does not round
		// cell-reference intermediate values — it only rounds what is displayed).
		var F float64
		var Frnd float64 // rounded F for display in OpeningPrincipal
		if lastCapDate.Equal(fd.ValueDate) || postPayoutReset {
			F = P
			Frnd = P
		} else {
			F = prevCapJRaw
			Frnd = prevCapJRnd
		}

		// ── Emit ACCRUAL rows in (lastCapDate, cd] ──────────────────────────
		for _, ad := range accrualDates {
			if ad.After(lastCapDate) && !ad.After(cd) {
				periodDivisor, periodDays := engDivisorAndDays(convention, prevRowDate, ad, p.Cfg, p.CalInfo)
				rawGross := F * r * float64(periodDays) / float64(periodDivisor)
				gross := rnd.RoundInterest(rawGross)
				if gross < 0 {
					gross = 0
				}
				var provTDS float64
				if hasTDS {
					provTDS = rnd.RoundInterest(gross * tRate)
				}
				rows = append(rows, CashflowRow{
					EventType:         "ACCRUAL",
					EventDate:         ad,
					ValueDate:         ad,
					CashflowType:      "NA",
					PeriodStartDate:   prevRowDate,
					PeriodEndDate:     ad,
					PeriodDays:        periodDays,
					OpeningPrincipal:  Frnd, // display rounded F
					InterestAccrued:   gross,
					ClosingPrincipal:  Frnd,
					TDSAmount:         provTDS,
					NetCashFlow:       0,
					NetAmount:         gross - provTDS,
					DayCountCode:      dcCode,
					Divisor:           periodDivisor,
					FormulaUsed:       engFormula("[CO ACCRUAL]", engFormulaParams{Principal: Frnd, Rate: r, Days: periodDays, Divisor: periodDivisor, Raw: rawGross, Rounded: gross, Rnd: rnd}),
					AccrualRatePerDay: r / float64(periodDivisor),
				})
				prevRowDate = ad
			}
		}

		// ── Emit CAPITALIZATION row at cd ────────────────────────────────────
		capDivisor, capDays := engCapDivisorAndDays(convention, lastCapDate, cd, p.Cfg)
		capRaw := F * r * float64(capDays) / float64(capDivisor)
		capGross := rnd.RoundInterest(capRaw)
		if capGross < 0 {
			capGross = 0
		}

		// broken_period_method on stub (broken-period) windows.
		// The final window is excluded: capitalize-then-pay at maturity is
		// numerically identical to simple settlement, so the normal path holds.
		capLabel := "[CO CAPITALIZATION]"
		brokenSimple := false
		if capSched.StubKeys[cd.Format(constants.DateFormat)] && !isMaturity {
			switch brokenMethod {
			case "NONE":
				// Broken period earns no interest.
				capRaw, capGross = 0, 0
				capLabel = "[CO BROKEN NONE]"
			case "SIMPLE", "HYBRID":
				// Broken-period interest stays simple: earned but NOT capitalized;
				// carried outside the principal chain and settled at maturity.
				brokenSimple = true
				capLabel = "[CO BROKEN SIMPLE]"
			}
		}

		var capTDS float64
		if hasTDS {
			capTDS = rnd.RoundInterest(capGross * tRate)
		}
		capJRaw := F + capGross - capTDS
		capAmt := capGross - capTDS
		if brokenSimple {
			carriedSimple += capGross - capTDS
			capJRaw = F
			capAmt = 0
		}
		capJRnd := rnd.RoundPrincipal(capJRaw)

		rows = append(rows, CashflowRow{
			EventType:         "CAPITALIZATION",
			EventDate:         cd,
			ValueDate:         cd,
			CashflowType:      "CAP",
			PeriodStartDate:   lastCapDate,
			PeriodEndDate:     cd,
			PeriodDays:        capDays,
			OpeningPrincipal:  Frnd, // display rounded F
			InterestAccrued:   capGross,
			CapitalizedAmount: capAmt,
			ClosingPrincipal:  capJRnd,
			TDSAmount:         capTDS,
			NetCashFlow:       0,
			NetAmount:         capJRnd,
			DayCountCode:      dcCode,
			Divisor:           capDivisor,
			FormulaUsed:       engFormula(capLabel, engFormulaParams{Principal: Frnd, Rate: r, Days: capDays, Divisor: capDivisor, Raw: capRaw, Rounded: capGross, Rnd: rnd}),
			AccrualRatePerDay: r / float64(capDivisor),
		})
		prevCapJRaw = capJRaw
		prevCapJRnd = capJRnd
		prevRowDate = cd
		postPayoutReset = false

		// ── Payout / Maturity row ────────────────────────────────────────────
		if isPayoutDate {
			// Collect accruals in (lastPayoutDate, cd] for K and L
			var accrRevK, tdsRevL float64
			for _, r2 := range rows {
				if r2.EventType == "ACCRUAL" && r2.EventDate.After(lastPayoutDate) && !r2.EventDate.After(cd) {
					accrRevK += r2.InterestAccrued
					tdsRevL += r2.TDSAmount
				}
			}

			payoutDivisor, payoutDays := engCapDivisorAndDays(convention, lastPayoutDate, cd, p.Cfg)
			payoutRaw := capJRaw - P
			if isMaturity {
				// Settle broken-period simple interest carried outside the principal chain.
				payoutRaw += carriedSimple
			}
			var payoutGross float64
			if isMaturity {
				payoutGross = rnd.RoundFinal(payoutRaw)
			} else {
				payoutGross = rnd.RoundInterest(payoutRaw)
			}
			if payoutGross < 0 {
				payoutGross = 0
			}
			dueNotAccr := payoutGross - accrRevK
			if dueNotAccr < 0 {
				dueNotAccr = 0
			}

			if isMaturity {
				// CO Maturity: E = D + 2
				var matTDS float64
				if hasTDS {
					matTDS = rnd.RoundFinal(payoutGross * tRate)
				}
				// Full maturity amount = compound chain + carried broken-period simple interest.
				matTotalRnd := rnd.RoundPrincipal(capJRaw + carriedSimple)
				rows = append(rows, CashflowRow{
					EventType:       "MATURITY",
					EventDate:       cd,
					ValueDate:       cd.AddDate(0, 0, 2),
					CashflowType:    "INFLOW",
					PeriodStartDate: lastPayoutDate,
					PeriodEndDate:   cd,
					PeriodDays:      payoutDays,
					InterestAccrued: payoutGross,
					TDSAmount:       matTDS,
					// NetCashFlow:       payoutGross - matTDS,
					NetAmount:     payoutGross - matTDS,
					AccrRevK:      accrRevK,
					TDSRevL:       tdsRevL,
					DueNotAccrued: dueNotAccr,
					// NetAmount:         capJRnd,
					NetCashFlow:       matTotalRnd, // override net cash flow to show full maturity amount including principal
					DayCountCode:      dcCode,
					Divisor:           payoutDivisor,
					FormulaUsed:       engFormula("[CO MATURITY]", engFormulaParams{Principal: P, Rate: r, Days: payoutDays, Divisor: payoutDivisor, Raw: payoutRaw, Rounded: payoutGross, Rnd: rnd}),
					AccrualRatePerDay: r / float64(payoutDivisor),
					OpeningPrincipal:  P,
					ClosingPrincipal:  matTotalRnd,
				})
				rows = append(rows, CashflowRow{
					EventType:        "PRINCIPAL_RETURN",
					EventDate:        cd,
					ValueDate:        cd,
					CashflowType:     "INFLOW",
					PeriodStartDate:  cd,
					PeriodEndDate:    cd,
					OpeningPrincipal: P,
					ClosingPrincipal: 0,
					NetCashFlow:      P,
					DayCountCode:     dcCode,
				})
			} else {
				// Intermediate CO payout: TDS = 0 (already taken at cap rows)
				rows = append(rows, CashflowRow{
					EventType:         "INTEREST_RECEIPT",
					EventDate:         cd,
					ValueDate:         cd.AddDate(0, 0, 2),
					CashflowType:      "INFLOW",
					PeriodStartDate:   lastPayoutDate,
					PeriodEndDate:     cd,
					PeriodDays:        payoutDays,
					InterestAccrued:   payoutGross,
					TDSAmount:         0,
					NetCashFlow:       payoutGross,
					AccrRevK:          accrRevK,
					TDSRevL:           tdsRevL,
					DueNotAccrued:     dueNotAccr,
					NetAmount:         payoutGross,
					DayCountCode:      dcCode,
					Divisor:           payoutDivisor,
					FormulaUsed:       engFormula("[CO PAYOUT]", engFormulaParams{Principal: P, Rate: r, Days: payoutDays, Divisor: payoutDivisor, Raw: payoutRaw, Rounded: payoutGross, Rnd: rnd}),
					AccrualRatePerDay: r / float64(payoutDivisor),
					OpeningPrincipal:  P,
					ClosingPrincipal:  capJRnd,
				})
				lastPayoutDate = cd
				if resetType == "AT_EACH_PAYOUT" {
					postPayoutReset = true
				}
			}
		}

		lastCapDate = cd
	}

	return rows
}
