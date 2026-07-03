// cashflow_engine_config_test.go — Scenario checks for the BRD bank-config
// fields that drive cashflow generation:
//
//	capitalization_schedule_type, quarter_definition, broken_period_method,
//	broken_period_location, capitalization_date_adjustment,
//	accrual_start_convention, accrual_end_convention,
//	period_boundary_definition, minimum_compounding_period_days.
//
// Pure unit tests — no DB required.
package fdMaster

import (
	"testing"
	"time"
)

func cfDate(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func cfFmt(d time.Time) string { return d.Format("2006-01-02") }

func assertDates(t *testing.T, got []time.Time, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %d boundaries %v, got %d: %v", len(want), want, len(got), cfFmtAll(got))
	}
	for i, w := range want {
		if cfFmt(got[i]) != w {
			t.Fatalf("boundary[%d]: expected %s, got %s (all: %v)", i, w, cfFmt(got[i]), cfFmtAll(got))
		}
	}
}

func cfFmtAll(ds []time.Time) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = cfFmt(d)
	}
	return out
}

func baseTestCfg() *BankConfig {
	return &BankConfig{
		ConfigID:                 "CFG-TEST",
		DayCountCode:             "DC-ACT-365",
		RoundingMethod:           "ROUND",
		RoundingFrequency:        "EACH_PERIOD",
		InterestRoundingDecimals: 2,
		AccrualStartConvention:   "INCLUDE",
		AccrualEndConvention:     "INCLUDE",
		WeekendAccrual:           true,
		HolidayAccrual:           true,
	}
}

func baseTestFD(start, maturity time.Time) *FDRecord {
	return &FDRecord{
		FDID:             "FD-TEST",
		PrincipalAmount:  1000000,
		InterestRate:     7,
		InterestTypeCode: "COMPOUND",
		ValueDate:        start,
		MaturityDate:     maturity,
		ResetType:        "AT_MATURITY",
	}
}

// ── capitalization_schedule_type ──────────────────────────────────────────────

// BRD: CALENDAR_QTR_END — periods align with Mar 31 / Jun 30 / Sep 30 / Dec 31.
// A mid-quarter start produces a broken FIRST period (the reported bug).
func TestCapDatesCalendarQuarterEndBrokenFirst(t *testing.T) {
	fd := baseTestFD(cfDate(2026, 1, 15), cfDate(2027, 1, 15))
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "CALENDAR_QTR_END"

	sched := engCapDatesFromConfig(fd, cfg, 3, HolidayCalendarInfo{})
	assertDates(t, sched.Dates, "2026-03-31", "2026-06-30", "2026-09-30", "2026-12-31", "2027-01-15")

	if !sched.StubKeys["2026-03-31"] {
		t.Errorf("first window Jan 15 → Mar 31 must be flagged as a broken period")
	}
	if !sched.StubKeys["2027-01-15"] {
		t.Errorf("last window Dec 31 → Jan 15 must be flagged as a broken period")
	}
	if sched.StubKeys["2026-06-30"] || sched.StubKeys["2026-09-30"] || sched.StubKeys["2026-12-31"] {
		t.Errorf("full calendar quarters must not be flagged as broken: %v", sched.StubKeys)
	}
}

// BRD: ANNIVERSARY — periods step from the booking date; a 1-year quarterly FD
// lands exactly on maturity, so there is no broken period.
func TestCapDatesAnniversaryDefault(t *testing.T) {
	fd := baseTestFD(cfDate(2026, 1, 15), cfDate(2027, 1, 15))
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "ANNIVERSARY"

	sched := engCapDatesFromConfig(fd, cfg, 3, HolidayCalendarInfo{})
	assertDates(t, sched.Dates, "2026-04-15", "2026-07-15", "2026-10-15", "2027-01-15")
	if len(sched.StubKeys) != 0 {
		t.Errorf("anniversary-aligned schedule must have no broken periods, got %v", sched.StubKeys)
	}
}

// BRD: QuarterDefinition = 90_DAYS — fixed 90-day quarters instead of
// calendar-month anniversaries (ICICI-style fixed-day stepping).
func TestCapDatesAnniversary90Days(t *testing.T) {
	fd := baseTestFD(cfDate(2026, 1, 15), cfDate(2027, 1, 15))
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "ANNIVERSARY"
	cfg.QuarterDefinition = "90_DAYS"

	sched := engCapDatesFromConfig(fd, cfg, 3, HolidayCalendarInfo{})
	assertDates(t, sched.Dates, "2026-04-15", "2026-07-14", "2026-10-12", "2027-01-10", "2027-01-15")
	if !sched.StubKeys["2027-01-15"] {
		t.Errorf("5-day tail Jan 10 → Jan 15 must be flagged as a broken period")
	}
}

// BRD: MONTH_END — boundaries at calendar month ends.
func TestCapDatesMonthEnd(t *testing.T) {
	fd := baseTestFD(cfDate(2026, 1, 15), cfDate(2026, 4, 15))
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "MONTH_END"

	sched := engCapDatesFromConfig(fd, cfg, 1, HolidayCalendarInfo{})
	assertDates(t, sched.Dates, "2026-01-31", "2026-02-28", "2026-03-31", "2026-04-15")
	if !sched.StubKeys["2026-01-31"] || !sched.StubKeys["2026-04-15"] {
		t.Errorf("mid-month start and off-grid maturity must both be broken periods, got %v", sched.StubKeys)
	}
}

// ── capitalization_date_adjustment ────────────────────────────────────────────

// BRD: PRECEDING_WD / FOLLOWING_WD move a capitalization date that falls on a
// non-working day; NO_ADJUST keeps it. The maturity boundary is never moved.
func TestCapDateAdjustment(t *testing.T) {
	cal := HolidayCalendarInfo{
		WeekendPattern: "Sat,Sun",
		HolidayDates:   map[string]bool{"2026-06-30": true}, // Tue, declared holiday
	}
	fd := baseTestFD(cfDate(2026, 1, 15), cfDate(2027, 1, 15))

	cfgFollow := baseTestCfg()
	cfgFollow.CapitalizationScheduleType = "CALENDAR_QTR_END"
	cfgFollow.CapitalizationDateAdjustment = "FOLLOWING_WD"
	schedF := engCapDatesFromConfig(fd, cfgFollow, 3, cal)
	assertDates(t, schedF.Dates, "2026-03-31", "2026-07-01", "2026-09-30", "2026-12-31", "2027-01-15")

	cfgPrec := baseTestCfg()
	cfgPrec.CapitalizationScheduleType = "CALENDAR_QTR_END"
	cfgPrec.CapitalizationDateAdjustment = "PRECEDING_WD"
	schedP := engCapDatesFromConfig(fd, cfgPrec, 3, cal)
	assertDates(t, schedP.Dates, "2026-03-31", "2026-06-29", "2026-09-30", "2026-12-31", "2027-01-15")

	cfgNo := baseTestCfg()
	cfgNo.CapitalizationScheduleType = "CALENDAR_QTR_END"
	cfgNo.CapitalizationDateAdjustment = "NO_ADJUST"
	schedN := engCapDatesFromConfig(fd, cfgNo, 3, cal)
	assertDates(t, schedN.Dates, "2026-03-31", "2026-06-30", "2026-09-30", "2026-12-31", "2027-01-15")
}

// ── broken_period_location ────────────────────────────────────────────────────

// BRD: FIRST — the regular grid anchors at maturity and the stub sits at the
// start of the tenor (anniversary mode steps backwards from maturity).
func TestCapDatesBrokenLocationFirst(t *testing.T) {
	fd := baseTestFD(cfDate(2026, 1, 15), cfDate(2027, 3, 31))
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "ANNIVERSARY"
	cfg.BrokenPeriodLocation = "FIRST"

	sched := engCapDatesFromConfig(fd, cfg, 3, HolidayCalendarInfo{})
	assertDates(t, sched.Dates, "2026-03-30", "2026-06-30", "2026-09-30", "2026-12-31", "2027-03-31")
	if !sched.StubKeys["2026-03-30"] {
		t.Errorf("the first window Jan 15 → Mar 30 must be the broken period, got %v", sched.StubKeys)
	}
	if sched.StubKeys["2027-03-31"] {
		t.Errorf("maturity window is grid-aligned and must not be broken")
	}
}

// ── minimum_compounding_period_days ───────────────────────────────────────────

// BRD: stubs shorter than the minimum are merged into the following window.
func TestMinimumCompoundingPeriodDays(t *testing.T) {
	fd := baseTestFD(cfDate(2026, 6, 20), cfDate(2026, 12, 31))
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "CALENDAR_QTR_END"
	cfg.MinimumCompoundingPeriodDays = 45

	sched := engCapDatesFromConfig(fd, cfg, 3, HolidayCalendarInfo{})
	// Jun 30 (10-day stub) is below the 45-day minimum → merged into Sep 30.
	assertDates(t, sched.Dates, "2026-09-30", "2026-12-31")
	if !sched.StubKeys["2026-09-30"] {
		t.Errorf("the merged window Jun 20 → Sep 30 must remain flagged as broken")
	}
}

// ── broken_period_method ──────────────────────────────────────────────────────

func findRows(rows []CashflowRow, eventType string) []CashflowRow {
	var out []CashflowRow
	for _, r := range rows {
		if r.EventType == eventType {
			out = append(out, r)
		}
	}
	return out
}

// BRD: SIMPLE — broken-period interest is earned but NOT compounded into the
// principal; COMPOUND — the stub capitalizes like any full period.
func TestBrokenPeriodMethodSimpleVsCompound(t *testing.T) {
	start, maturity := cfDate(2026, 1, 15), cfDate(2027, 1, 15)

	run := func(method string) []CashflowRow {
		cfg := baseTestCfg()
		cfg.CapitalizationScheduleType = "CALENDAR_QTR_END"
		cfg.BrokenPeriodMethod = method
		return engCOSchedule(CashflowScheduleParams{
			FD:     baseTestFD(start, maturity),
			Cfg:    cfg,
			Freq:   &CompoundingFreq{FrequencyType: "QUARTERLY"},
			DCInfo: DayCountInfo{DayCountCode: "DC-ACT-365", ConventionType: "ACT_365"},
		})
	}

	compound := run("COMPOUND")
	simple := run("SIMPLE")

	capC := findRows(compound, "CAPITALIZATION")
	capS := findRows(simple, "CAPITALIZATION")
	if len(capC) < 2 || len(capS) < 2 {
		t.Fatalf("expected at least 2 capitalization rows, got %d / %d", len(capC), len(capS))
	}

	// COMPOUND: the broken first period (Jan 15 → Mar 31) capitalizes.
	if capC[0].CapitalizedAmount <= 0 || capC[0].ClosingPrincipal <= 1000000 {
		t.Errorf("COMPOUND: stub must capitalize (cap=%.2f closing=%.2f)", capC[0].CapitalizedAmount, capC[0].ClosingPrincipal)
	}
	if capC[1].OpeningPrincipal <= 1000000 {
		t.Errorf("COMPOUND: Q2 must open above principal, got %.2f", capC[1].OpeningPrincipal)
	}

	// SIMPLE: stub interest is earned but kept outside the principal chain.
	if capS[0].InterestAccrued <= 0 {
		t.Errorf("SIMPLE: stub must still earn interest, got %.2f", capS[0].InterestAccrued)
	}
	if capS[0].CapitalizedAmount != 0 || capS[0].ClosingPrincipal != 1000000 {
		t.Errorf("SIMPLE: stub must not capitalize (cap=%.2f closing=%.2f)", capS[0].CapitalizedAmount, capS[0].ClosingPrincipal)
	}
	if capS[1].OpeningPrincipal != 1000000 {
		t.Errorf("SIMPLE: Q2 must open at the original principal, got %.2f", capS[1].OpeningPrincipal)
	}

	matC := findRows(compound, "MATURITY")
	matS := findRows(simple, "MATURITY")
	if len(matC) != 1 || len(matS) != 1 {
		t.Fatalf("expected exactly one maturity row each, got %d / %d", len(matC), len(matS))
	}
	// The carried simple interest settles at maturity, but it never compounded —
	// so the SIMPLE maturity total must be positive yet below the COMPOUND total.
	if matS[0].InterestAccrued <= 0 {
		t.Errorf("SIMPLE: maturity must include the carried stub interest, got %.2f", matS[0].InterestAccrued)
	}
	if matS[0].InterestAccrued >= matC[0].InterestAccrued {
		t.Errorf("SIMPLE total interest (%.2f) must be below COMPOUND (%.2f)", matS[0].InterestAccrued, matC[0].InterestAccrued)
	}
	// Sanity: difference is the lost compounding on ~₹14.6k for 9.5 months (small).
	if diff := matC[0].InterestAccrued - matS[0].InterestAccrued; diff > 2000 {
		t.Errorf("SIMPLE vs COMPOUND difference too large: %.2f", diff)
	}
}

// BRD: NONE — the broken period earns no interest at all.
func TestBrokenPeriodMethodNone(t *testing.T) {
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "CALENDAR_QTR_END"
	cfg.BrokenPeriodMethod = "NONE"

	rows := engCOSchedule(CashflowScheduleParams{
		FD:     baseTestFD(cfDate(2026, 1, 15), cfDate(2027, 1, 15)),
		Cfg:    cfg,
		Freq:   &CompoundingFreq{FrequencyType: "QUARTERLY"},
		DCInfo: DayCountInfo{DayCountCode: "DC-ACT-365", ConventionType: "ACT_365"},
	})
	caps := findRows(rows, "CAPITALIZATION")
	if len(caps) == 0 {
		t.Fatal("expected capitalization rows")
	}
	if caps[0].InterestAccrued != 0 || caps[0].ClosingPrincipal != 1000000 {
		t.Errorf("NONE: stub must earn zero interest (interest=%.2f closing=%.2f)",
			caps[0].InterestAccrued, caps[0].ClosingPrincipal)
	}
	if caps[1].InterestAccrued <= 0 {
		t.Errorf("NONE: full quarters must still earn interest, got %.2f", caps[1].InterestAccrued)
	}
}

// ── accrual conventions + period_boundary_definition ─────────────────────────

func TestAccrualConventionsAndBoundaryDefs(t *testing.T) {
	cal := HolidayCalendarInfo{WeekendPattern: "Sat,Sun", HolidayDates: map[string]bool{}}

	mk := func(mutate func(c *BankConfig)) *BankConfig {
		c := baseTestCfg()
		mutate(c)
		return c
	}

	cases := []struct {
		name       string
		cfg        *BankConfig
		start, end time.Time
		wantDays   int
	}{
		{"default INCLUDE/INCLUDE span", mk(func(c *BankConfig) {}),
			cfDate(2026, 6, 1), cfDate(2026, 6, 30), 29},
		{"start EXCLUDE drops one day", mk(func(c *BankConfig) { c.AccrualStartConvention = "EXCLUDE" }),
			cfDate(2026, 6, 1), cfDate(2026, 6, 30), 28},
		{"end EXCLUDE drops one day", mk(func(c *BankConfig) { c.AccrualEndConvention = "EXCLUDE" }),
			cfDate(2026, 6, 1), cfDate(2026, 6, 30), 28},
		// Jun 20 2026 is a Saturday → NEXT_WD snaps the start to Mon Jun 22.
		{"start NEXT_WD on weekend", mk(func(c *BankConfig) { c.AccrualStartConvention = "NEXT_WD" }),
			cfDate(2026, 6, 20), cfDate(2026, 6, 30), 8},
		// Jun 28 2026 is a Sunday → PRECEDING_WD snaps the end to Fri Jun 26.
		{"end PRECEDING_WD on weekend", mk(func(c *BankConfig) { c.AccrualEndConvention = "PRECEDING_WD" }),
			cfDate(2026, 6, 1), cfDate(2026, 6, 28), 25},
		{"INCL_BOTH adds a day", mk(func(c *BankConfig) { c.PeriodBoundaryDefinition = "INCL_BOTH" }),
			cfDate(2026, 6, 1), cfDate(2026, 6, 30), 30},
		{"EXCL_BOTH removes a day", mk(func(c *BankConfig) { c.PeriodBoundaryDefinition = "EXCL_BOTH" }),
			cfDate(2026, 6, 1), cfDate(2026, 6, 30), 28},
		{"EXCL_START_INCL_END keeps span", mk(func(c *BankConfig) { c.PeriodBoundaryDefinition = "EXCL_START_INCL_END" }),
			cfDate(2026, 6, 1), cfDate(2026, 6, 30), 29},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			divisor, days := getDivisorAndDaysWithCal("ACT_365", tc.start, tc.end, tc.cfg, cal)
			if divisor != 365 {
				t.Errorf("expected divisor 365, got %d", divisor)
			}
			if days != tc.wantDays {
				t.Errorf("expected %d days, got %d", tc.wantDays, days)
			}
		})
	}
}

// ── day count: ACT/ACT leap-year divisor ──────────────────────────────────────

func TestActActLeapYearDivisor(t *testing.T) {
	cfg := baseTestCfg()
	cal := HolidayCalendarInfo{}

	divLeap, _ := getDivisorAndDaysWithCal("ACT_ACT", cfDate(2024, 1, 1), cfDate(2024, 12, 31), cfg, cal)
	if divLeap != 366 {
		t.Errorf("ACT_ACT period ending in leap year must use divisor 366, got %d", divLeap)
	}
	divNorm, _ := getDivisorAndDaysWithCal("ACT_ACT", cfDate(2026, 1, 1), cfDate(2026, 12, 31), cfg, cal)
	if divNorm != 365 {
		t.Errorf("ACT_ACT period ending in non-leap year must use divisor 365, got %d", divNorm)
	}
}

// ── first_capitalization_date anchor wins over schedule type ──────────────────

func TestAnchorOverridesScheduleType(t *testing.T) {
	fd := baseTestFD(cfDate(2026, 1, 15), cfDate(2027, 1, 15))
	fd.FirstCapitalizationDate = cfDate(2026, 2, 28) // bank-confirmed anchor
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "CALENDAR_QTR_END" // must be ignored

	sched := engCapDatesFromConfig(fd, cfg, 3, HolidayCalendarInfo{})
	if cfFmt(sched.Dates[0]) != "2026-02-28" {
		t.Errorf("explicit first_capitalization_date must win, got first boundary %s", cfFmt(sched.Dates[0]))
	}
}

// ── Jun 12 mid-quarter: live-demo reproduction scenario ───────────────────────

// TestJun12CalendarQtrEndBrokenFirst confirms that with minimum_compounding_period_days=0
// a CALENDAR_QTR_END FD booked Jun 12 shows Jun 30 as the first (18-day) broken boundary.
// This is the scenario that was broken in production: CONFIG-75839A4 had
// minimum_compounding_period_days ≥ 19, which merged Jun 30 into Sep 30.
func TestJun12CalendarQtrEndBrokenFirst(t *testing.T) {
	fd := baseTestFD(cfDate(2026, 6, 12), cfDate(2027, 6, 12))
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "CALENDAR_QTR_END"
	cfg.MinimumCompoundingPeriodDays = 0

	sched := engCapDatesFromConfig(fd, cfg, 3, HolidayCalendarInfo{})

	t.Log("─── Cap boundaries (min_compounding_period_days=0) ───")
	for _, d := range sched.Dates {
		broken := ""
		if sched.StubKeys[cfFmt(d)] {
			broken = "  ← BROKEN (stub)"
		}
		t.Logf("  %s%s", cfFmt(d), broken)
	}

	assertDates(t, sched.Dates,
		"2026-06-30", "2026-09-30", "2026-12-31", "2027-03-31", "2027-06-12")

	if !sched.StubKeys["2026-06-30"] {
		t.Errorf("Jun 30 (18-day stub) must be flagged as broken first period")
	}
	for _, d := range []string{"2026-09-30", "2026-12-31", "2027-03-31"} {
		if sched.StubKeys[d] {
			t.Errorf("full quarter %s must not be broken, stubs: %v", d, sched.StubKeys)
		}
	}
	if !sched.StubKeys["2027-06-12"] {
		t.Errorf("maturity Jun 12 (off-grid tail) must also be flagged as broken last period")
	}
	t.Log("PASS: Jun 30 is the broken first period; full quarters are clean")
}

// TestJun12MinDaysMergesBreak is the root-cause regression test:
// minimum_compounding_period_days ≥ 19 drops the 18-day Jun 30 stub and
// transfers its broken flag to Sep 30, producing a misleading 109-day "[CO BROKEN SIMPLE]".
func TestJun12MinDaysMergesBreak(t *testing.T) {
	fd := baseTestFD(cfDate(2026, 6, 12), cfDate(2027, 6, 12))
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "CALENDAR_QTR_END"
	cfg.MinimumCompoundingPeriodDays = 30 // 18-day stub < 30 → merge into Sep 30

	sched := engCapDatesFromConfig(fd, cfg, 3, HolidayCalendarInfo{})

	t.Log("─── Cap boundaries (min_compounding_period_days=30, reproduces the bug) ───")
	t.Log("  NOTE: Jun 30 is dropped because 18 days < 30 min; its stub flag moves to Sep 30")
	for _, d := range sched.Dates {
		broken := ""
		if sched.StubKeys[cfFmt(d)] {
			broken = "  ← BROKEN (stub carried from merged Jun 30)"
		}
		t.Logf("  %s%s", cfFmt(d), broken)
	}

	assertDates(t, sched.Dates,
		"2026-09-30", "2026-12-31", "2027-03-31", "2027-06-12")

	if !sched.StubKeys["2026-09-30"] {
		t.Errorf("Sep 30 must inherit the broken flag from the merged Jun 30 window, stubs: %v", sched.StubKeys)
	}
	t.Log("PASS: Sep 30 carries the broken flag; Jun 30 boundary is absent (merged)")
}

// TestAccrualPatternAfterBrokenCap verifies the 11/30/30 accrual day pattern
// visible in the UI after a CALENDAR_QTR_END broken-period capitalization.
//
// Cap grid (QTR-END): Jun 30, Sep 30, Dec 31, Mar 31
// Accrual grid (monthly from start): Jul 12, Aug 12, Sep 12, Oct 12, …
//
// These grids don't share a date, so every quarter-end cap produces a short
// bridging accrual back to the next "12th":
//
//	Jun 30 CAP  → 17 days  (Jun 12→Jun 30, EXCLUDE end: 18 raw − 1)
//	ACCRUAL Jul 12 → 11 days  (Jun 30→Jul 12, EXCLUDE end: 12 raw − 1)
//	ACCRUAL Aug 12 → 30 days  (Jul 12→Aug 12, EXCLUDE end: 31 raw − 1)
//	ACCRUAL Sep 12 → 30 days  (Aug 12→Sep 12, EXCLUDE end: 31 raw − 1)
//	Sep 30 CAP  → 91 days  (Jun 30→Sep 30, EXCLUDE end: 92 raw − 1)
//	ACCRUAL Oct 12 → 11 days  (Sep 30→Oct 12, same short bridge)
func TestAccrualPatternAfterBrokenCap(t *testing.T) {
	start, maturity := cfDate(2026, 6, 12), cfDate(2027, 6, 12)
	cfg := baseTestCfg()
	cfg.CapitalizationScheduleType = "CALENDAR_QTR_END"
	cfg.AccrualEndConvention = "EXCLUDE"
	cfg.MinimumCompoundingPeriodDays = 0

	rows := engCOSchedule(CashflowScheduleParams{
		FD:                baseTestFD(start, maturity),
		Cfg:               cfg,
		Freq:              &CompoundingFreq{FrequencyType: "QUARTERLY"},
		DCInfo:            DayCountInfo{DayCountCode: "DC-ACT-365", ConventionType: "ACT_365"},
		AccrualFreqMonths: 1, // monthly accruals — matches simulator default (accrualFreqMonths=1)
	})

	caps := findRows(rows, "CAPITALIZATION")
	accruals := findRows(rows, "ACCRUAL")

	// Print full schedule so the output is visible in go test -v
	t.Log("─── Full cashflow schedule (CAPITALIZATION + ACCRUAL) ───")
	t.Logf("  %-12s  %-14s  %8s  %16s  %16s  %s",
		"EVENT", "DATE", "DAYS", "GROSS_INTEREST", "CLOSING_PRINC", "NOTE")
	t.Logf("  %s", "─────────────────────────────────────────────────────────────────────────────────")
	for _, r := range rows {
		if r.EventType != "CAPITALIZATION" && r.EventType != "ACCRUAL" && r.EventType != "INITIAL_INVESTMENT" && r.EventType != "MATURITY" {
			continue
		}
		note := ""
		if r.EventType == "CAPITALIZATION" {
			note = "[CAP]"
			if r.InterestAccrued > 0 && r.CapitalizedAmount == 0 {
				note = "[BROKEN SIMPLE — interest earned, NOT compounded]"
			} else if r.CapitalizedAmount > 0 {
				note = "[COMPOUNDED into principal]"
			}
		}
		if r.EventType == "ACCRUAL" {
			note = "[journal entry only, no cash]"
		}
		t.Logf("  %-12s  %-14s  %8d  %16.2f  %16.2f  %s",
			r.EventType, cfFmt(r.EventDate), r.PeriodDays,
			r.InterestAccrued, r.ClosingPrincipal, note)
	}
	t.Log("")

	// First cap: Jun 30, broken period, 17 days
	if len(caps) == 0 {
		t.Fatal("no capitalization rows")
	}
	if cfFmt(caps[0].EventDate) != "2026-06-30" {
		t.Fatalf("first cap must be Jun 30, got %s", cfFmt(caps[0].EventDate))
	}
	if caps[0].PeriodDays != 17 {
		t.Errorf("Jun 30 broken cap: want 17 days (18 raw − 1 EXCLUDE), got %d", caps[0].PeriodDays)
	}

	// Second cap: Sep 30, full quarter, 91 days
	if len(caps) < 2 || cfFmt(caps[1].EventDate) != "2026-09-30" {
		t.Fatalf("second cap must be Sep 30, got %v", cfFmtAll(func() []time.Time {
			var ds []time.Time
			for _, c := range caps {
				ds = append(ds, c.EventDate)
			}
			return ds
		}()))
	}
	if caps[1].PeriodDays != 91 {
		t.Errorf("Sep 30 full cap: want 91 days (92 raw − 1 EXCLUDE), got %d", caps[1].PeriodDays)
	}

	// First three accruals after the Jun 30 broken cap
	t.Log("─── Accrual day pattern after broken Jun 30 cap ───")
	t.Logf("  %-14s  %5s  %s", "DATE", "DAYS", "EXPLANATION")
	for i, r := range accruals {
		var why string
		switch i {
		case 0:
			why = "Jun 30→Jul 12: 12 raw − 1 EXCLUDE end = 11  (short bridge from QTR-end to next 12th)"
		case 1:
			why = "Jul 12→Aug 12: 31 raw − 1 EXCLUDE end = 30"
		case 2:
			why = "Aug 12→Sep 12: 31 raw − 1 EXCLUDE end = 30"
		default:
			why = ""
		}
		if i < 4 {
			t.Logf("  %-14s  %5d  %s", cfFmt(r.EventDate), r.PeriodDays, why)
		}
	}

	wantAccr := []struct {
		date string
		days int
	}{
		{"2026-07-12", 11}, // Jun 30 → Jul 12: 12 raw − 1 EXCLUDE = 11
		{"2026-08-12", 30}, // Jul 12 → Aug 12: 31 raw − 1 EXCLUDE = 30
		{"2026-09-12", 30}, // Aug 12 → Sep 12: 31 raw − 1 EXCLUDE = 30
	}
	if len(accruals) < 3 {
		t.Fatalf("expected ≥3 accrual rows, got %d", len(accruals))
	}
	for i, want := range wantAccr {
		if cfFmt(accruals[i].EventDate) != want.date {
			t.Errorf("accrual[%d]: want %s, got %s", i, want.date, cfFmt(accruals[i].EventDate))
		}
		if accruals[i].PeriodDays != want.days {
			t.Errorf("accrual[%d] %s: want %d days, got %d",
				i, want.date, want.days, accruals[i].PeriodDays)
		}
	}

	// The same 11-day bridge appears after every cap — verify Oct 12 (after Sep 30 cap)
	var oct12Days int
	for _, r := range accruals {
		if cfFmt(r.EventDate) == "2026-10-12" {
			oct12Days = r.PeriodDays
			break
		}
	}
	if oct12Days != 11 {
		t.Errorf("accrual after Sep 30 cap (Oct 12): want 11 days, got %d", oct12Days)
	}
	t.Logf("  Oct 12 (after Sep 30 cap): %d days — same 11-day bridge pattern repeats", oct12Days)
	t.Log("PASS: 11/30/30 pattern confirmed")
}
