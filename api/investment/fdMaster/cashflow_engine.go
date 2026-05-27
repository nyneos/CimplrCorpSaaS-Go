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

// engDivisorAndDays returns the (divisor, days) pair for one accrual/cap/payout
// period, honoring the day-count convention AND the bank config's weekend/holiday
// accrual flags. It delegates to getDivisorAndDaysWithCal in cashflow.go so all
// four conventions (ACT_365, ACT_360, ACT_ACT, 30_360) are handled consistently.
// days is clamped to >= 1 so a zero-length period never produces zero interest.
func engDivisorAndDays(conventionType string, start, end time.Time, cfg *BankConfig, cal HolidayCalendarInfo) (divisor, days int) {
	divisor, days = getDivisorAndDaysWithCal(conventionType, start, end, cfg, cal)
	if days <= 0 {
		days = 1
	}
	return
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
		payoutDivisor, payoutDays := engDivisorAndDays(convention, lastPayoutDate, pd, p.Cfg, p.CalInfo)
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

	capDates := engEventDatesAnchored(fd.ValueDate, fd.MaturityDate, capMonths, fd.FirstCapitalizationDate)
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
		capDivisor, capDays := engDivisorAndDays(convention, lastCapDate, cd, p.Cfg, p.CalInfo)
		capRaw := F * r * float64(capDays) / float64(capDivisor)
		capGross := rnd.RoundInterest(capRaw)
		if capGross < 0 {
			capGross = 0
		}
		var capTDS float64
		if hasTDS {
			capTDS = rnd.RoundInterest(capGross * tRate)
		}
		capJRaw := F + capGross - capTDS
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
			CapitalizedAmount: capGross - capTDS,
			ClosingPrincipal:  capJRnd,
			TDSAmount:         capTDS,
			NetCashFlow:       0,
			NetAmount:         capJRnd,
			DayCountCode:      dcCode,
			Divisor:           capDivisor,
			FormulaUsed:       engFormula("[CO CAPITALIZATION]", engFormulaParams{Principal: Frnd, Rate: r, Days: capDays, Divisor: capDivisor, Raw: capRaw, Rounded: capGross, Rnd: rnd}),
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

			payoutDivisor, payoutDays := engDivisorAndDays(convention, lastPayoutDate, cd, p.Cfg, p.CalInfo)
			payoutRaw := capJRaw - P
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
				rows = append(rows, CashflowRow{
					EventType:         "MATURITY",
					EventDate:         cd,
					ValueDate:         cd.AddDate(0, 0, 2),
					CashflowType:      "INFLOW",
					PeriodStartDate:   lastPayoutDate,
					PeriodEndDate:     cd,
					PeriodDays:        payoutDays,
					InterestAccrued:   payoutGross,
					TDSAmount:         matTDS,
					NetCashFlow:       payoutGross - matTDS,
					AccrRevK:          accrRevK,
					TDSRevL:           tdsRevL,
					DueNotAccrued:     dueNotAccr,
					NetAmount:         capJRnd,
					DayCountCode:      dcCode,
					Divisor:           payoutDivisor,
					FormulaUsed:       engFormula("[CO MATURITY]", engFormulaParams{Principal: P, Rate: r, Days: payoutDays, Divisor: payoutDivisor, Raw: payoutRaw, Rounded: payoutGross, Rnd: rnd}),
					AccrualRatePerDay: r / float64(payoutDivisor),
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
