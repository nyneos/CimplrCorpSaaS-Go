package fdMaster

import (
	"strings"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers — build SimulatedCashflowRow slices by hand
// ─────────────────────────────────────────────────────────────────────────────

func row(eventType, eventDate string, period int, interest, tds, netCF float64) SimulatedCashflowRow {
	return SimulatedCashflowRow{
		PeriodNumber:     period,
		EventType:        eventType,
		EventDate:        eventDate,
		PeriodStartDate:  eventDate, // simplified
		PeriodEndDate:    eventDate,
		InterestAccrued:  interest,
		TDSAmount:        tds,
		NetCashFlow:      netCF,
		ClosingPrincipal: 1000000, // arbitrary fixed
	}
}

func countByChangeType(rows []DiffCashflowRow) map[string]int {
	out := map[string]int{}
	for _, r := range rows {
		out[r.ChangeType]++
	}
	return out
}

func findRow(rows []DiffCashflowRow, eventType, eventDate string) *DiffCashflowRow {
	for i := range rows {
		if rows[i].EventType == eventType && rows[i].EventDate == eventDate {
			return &rows[i]
		}
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 1 — identical schedules → all UNCHANGED
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_Identical_AllUnchanged(t *testing.T) {
	schedule := []SimulatedCashflowRow{
		row("INITIAL_INVESTMENT", "2025-04-01", 1, 0, 0, -1000000),
		row("CAPITALIZATION", "2025-06-30", 2, 18750, 1875, 0),
		row("CAPITALIZATION", "2025-09-30", 3, 19102, 1910, 0),
		row("MATURITY", "2025-12-31", 4, 19463, 1946, 17517),
		row("PRINCIPAL_RETURN", "2025-12-31", 5, 0, 0, 1000000),
	}

	diff := diffSchedules(schedule, schedule)
	counts := countByChangeType(diff)

	if counts[DiffUnchanged] != len(schedule) {
		t.Errorf("expected all %d rows UNCHANGED, got %+v", len(schedule), counts)
	}
	if counts[DiffChanged]+counts[DiffNew]+counts[DiffRemoved] != 0 {
		t.Errorf("expected 0 changes, got %+v", counts)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 2 — interest rate bump → all CAPs CHANGED, no NEW/REMOVED
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_RateChanged_AllCapsChanged(t *testing.T) {
	booking := []SimulatedCashflowRow{
		row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0),
		row("CAPITALIZATION", "2025-09-30", 2, 19102, 1910, 0),
		row("MATURITY", "2025-12-31", 3, 19463, 1946, 17517),
	}
	confirmation := []SimulatedCashflowRow{
		row("CAPITALIZATION", "2025-06-30", 1, 19375, 1938, 0),
		row("CAPITALIZATION", "2025-09-30", 2, 19751, 1975, 0),
		row("MATURITY", "2025-12-31", 3, 20136, 2014, 18122),
	}

	diff := diffSchedules(booking, confirmation)
	counts := countByChangeType(diff)

	if counts[DiffChanged] != 3 {
		t.Errorf("expected 3 CHANGED, got %+v", counts)
	}
	if counts[DiffNew] != 0 || counts[DiffRemoved] != 0 {
		t.Errorf("expected 0 NEW/REMOVED, got %+v", counts)
	}

	// Spot-check a known row
	r := findRow(diff, "CAPITALIZATION", "2025-06-30")
	if r == nil {
		t.Fatal("expected CAPITALIZATION 2025-06-30 in diff")
	}
	if r.OldInterestAccrued == nil || *r.OldInterestAccrued != 18750 {
		t.Errorf("OldInterestAccrued: want 18750, got %v", r.OldInterestAccrued)
	}
	if r.NewInterestAccrued != 19375 {
		t.Errorf("NewInterestAccrued: want 19375, got %v", r.NewInterestAccrued)
	}
	if r.DateShifted {
		t.Error("DateShifted should be false (dates identical)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 3 — tenor extended → NEW rows at the end, no REMOVED
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_TenorExtended_NewRowsAppended(t *testing.T) {
	booking := []SimulatedCashflowRow{
		row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0),
		row("MATURITY", "2025-09-30", 2, 18750, 1875, 16875),
		row("PRINCIPAL_RETURN", "2025-09-30", 3, 0, 0, 1000000),
	}
	confirmation := []SimulatedCashflowRow{
		row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0),
		row("CAPITALIZATION", "2025-09-30", 2, 19102, 1910, 0),
		row("CAPITALIZATION", "2025-12-31", 3, 19463, 1946, 0),
		row("MATURITY", "2026-03-31", 4, 19834, 1983, 17851),
		row("PRINCIPAL_RETURN", "2026-03-31", 5, 0, 0, 1000000),
	}

	diff := diffSchedules(booking, confirmation)
	counts := countByChangeType(diff)

	// CAPITALIZATION 2025-06-30 → UNCHANGED (or paired via fallback)
	// CAPITALIZATION 2025-09-30 → NEW (booking has MATURITY here, not CAP)
	// CAPITALIZATION 2025-12-31 → NEW
	// MATURITY 2025-09-30 → REMOVED (moved to 2026-03-31)
	// MATURITY 2026-03-31 → NEW (MATURITY excluded from fallback pairing)
	// PRINCIPAL_RETURN 2025-09-30 → REMOVED, 2026-03-31 → NEW

	if counts[DiffNew] < 2 {
		t.Errorf("expected ≥2 NEW rows, got %+v", counts)
	}
	if counts[DiffRemoved] < 1 {
		t.Errorf("expected ≥1 REMOVED row (booking MATURITY), got %+v", counts)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 4 — first_payout_date shifted by +5 days → all CAPs DateShifted
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_DateShift_FallbackPairsByPosition(t *testing.T) {
	booking := []SimulatedCashflowRow{
		row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0),
		row("CAPITALIZATION", "2025-09-30", 2, 19102, 1910, 0),
		row("CAPITALIZATION", "2025-12-31", 3, 19463, 1946, 0),
	}
	confirmation := []SimulatedCashflowRow{
		row("CAPITALIZATION", "2025-07-05", 1, 18750, 1875, 0), // +5d
		row("CAPITALIZATION", "2025-10-05", 2, 19102, 1910, 0), // +5d
		row("CAPITALIZATION", "2026-01-05", 3, 19463, 1946, 0), // +5d
	}

	diff := diffSchedules(booking, confirmation)
	counts := countByChangeType(diff)

	// All 3 should pair via fallback → CHANGED with DateShifted=true
	if counts[DiffChanged] != 3 {
		t.Errorf("expected 3 CHANGED (fallback paired), got %+v", counts)
	}
	if counts[DiffNew] != 0 || counts[DiffRemoved] != 0 {
		t.Errorf("expected 0 NEW/REMOVED after fallback, got %+v", counts)
	}

	for i, r := range diff {
		if !r.DateShifted {
			t.Errorf("row %d (%s): DateShifted should be true", i, r.EventDate)
		}
		if r.OldEventDate == "" {
			t.Errorf("row %d: OldEventDate should be populated", i)
		}
		if r.OldEventDate == r.EventDate {
			t.Errorf("row %d: OldEventDate (%s) should differ from EventDate (%s)",
				i, r.OldEventDate, r.EventDate)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 5 — MATURITY shift does NOT trigger fallback (excluded)
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_MaturityShift_NotPaired(t *testing.T) {
	booking := []SimulatedCashflowRow{
		row("MATURITY", "2025-12-31", 1, 18750, 1875, 16875),
	}
	confirmation := []SimulatedCashflowRow{
		row("MATURITY", "2026-01-05", 1, 18750, 1875, 16875),
	}

	diff := diffSchedules(booking, confirmation)
	counts := countByChangeType(diff)

	if counts[DiffNew] != 1 || counts[DiffRemoved] != 1 {
		t.Errorf("MATURITY should NOT fallback-pair (structural change), got %+v", counts)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 6 — mixed: 2 of 3 same dates, 1 shifted
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_MixedSameAndShifted(t *testing.T) {
	booking := []SimulatedCashflowRow{
		row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0),
		row("CAPITALIZATION", "2025-09-30", 2, 19102, 1910, 0),
		row("CAPITALIZATION", "2025-12-31", 3, 19463, 1946, 0),
	}
	confirmation := []SimulatedCashflowRow{
		row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0), // same
		row("CAPITALIZATION", "2025-09-30", 2, 19102, 1910, 0), // same
		row("CAPITALIZATION", "2026-01-05", 3, 19463, 1946, 0), // shifted
	}

	diff := diffSchedules(booking, confirmation)
	counts := countByChangeType(diff)

	if counts[DiffUnchanged] != 2 {
		t.Errorf("expected 2 UNCHANGED, got %+v", counts)
	}
	if counts[DiffChanged] != 1 {
		t.Errorf("expected 1 CHANGED (fallback-paired shift), got %+v", counts)
	}

	shifted := findRow(diff, "CAPITALIZATION", "2026-01-05")
	if shifted == nil || !shifted.DateShifted {
		t.Error("expected the 2026-01-05 row to be CHANGED + DateShifted=true")
	}
	if shifted.OldEventDate != "2025-12-31" {
		t.Errorf("OldEventDate: want 2025-12-31, got %q", shifted.OldEventDate)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 7 — REMOVED row has empty new_* fields
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_RemovedRow_NewFieldsZero(t *testing.T) {
	booking := []SimulatedCashflowRow{
		row("MATURITY", "2025-06-30", 1, 5000, 500, 4500),
	}
	confirmation := []SimulatedCashflowRow{}

	diff := diffSchedules(booking, confirmation)
	if len(diff) != 1 {
		t.Fatalf("expected 1 row, got %d", len(diff))
	}
	if diff[0].ChangeType != DiffRemoved {
		t.Errorf("expected REMOVED, got %s", diff[0].ChangeType)
	}
	if diff[0].NewInterestAccrued != 0 {
		t.Errorf("REMOVED row's new_* should be zero, got %v", diff[0].NewInterestAccrued)
	}
	if diff[0].OldInterestAccrued == nil || *diff[0].OldInterestAccrued != 5000 {
		t.Error("REMOVED row's old_interest_accrued should be 5000")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 8 — sort order: chronological then event_type rank
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_SortOrder(t *testing.T) {
	// Same-date INITIAL_INVESTMENT (0) should come before ACCRUAL (1)
	booking := []SimulatedCashflowRow{
		row("ACCRUAL", "2025-04-01", 2, 100, 0, 100),
		row("INITIAL_INVESTMENT", "2025-04-01", 1, 0, 0, -1000000),
	}
	confirmation := booking

	diff := diffSchedules(booking, confirmation)
	if diff[0].EventType != "INITIAL_INVESTMENT" {
		t.Errorf("expected INITIAL_INVESTMENT first, got %s", diff[0].EventType)
	}
	if diff[1].EventType != "ACCRUAL" {
		t.Errorf("expected ACCRUAL second, got %s", diff[1].EventType)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 9 — change-detection covers all fields (Issue 3 regression)
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_DetectsInterestRateChange(t *testing.T) {
	// Same amounts but different InterestRate → should still be CHANGED
	b := row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0)
	b.InterestRate = 7.5
	c := row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0)
	c.InterestRate = 7.75

	diff := diffSchedules([]SimulatedCashflowRow{b}, []SimulatedCashflowRow{c})
	if diff[0].ChangeType != DiffChanged {
		t.Errorf("InterestRate-only change: expected CHANGED, got %s", diff[0].ChangeType)
	}
}

func TestDiff_DetectsCumulativeChange(t *testing.T) {
	b := row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0)
	b.CumulativeInterestTotal = 18750
	c := row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0)
	c.CumulativeInterestTotal = 20000 // schedule context changed

	diff := diffSchedules([]SimulatedCashflowRow{b}, []SimulatedCashflowRow{c})
	if diff[0].ChangeType != DiffChanged {
		t.Errorf("CumulativeInterestTotal change: expected CHANGED, got %s", diff[0].ChangeType)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 10 — GRACE_PERIOD rank
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_GracePeriodRank(t *testing.T) {
	if cashflowEventTypeRank("GRACE_PERIOD") != 7 {
		t.Errorf("GRACE_PERIOD rank: want 7, got %d", cashflowEventTypeRank("GRACE_PERIOD"))
	}
	if cashflowEventTypeRank("PRINCIPAL_RETURN") >= cashflowEventTypeRank("GRACE_PERIOD") {
		t.Error("GRACE_PERIOD should rank after PRINCIPAL_RETURN")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 11 — countDiffTypes helper
// ─────────────────────────────────────────────────────────────────────────────

func TestCountDiffTypes(t *testing.T) {
	rows := []DiffCashflowRow{
		{ChangeType: DiffNew},
		{ChangeType: DiffNew},
		{ChangeType: DiffChanged},
		{ChangeType: DiffRemoved},
		{ChangeType: DiffUnchanged},
		{ChangeType: DiffUnchanged},
	}
	n, c, r, u := countDiffTypes(rows)
	if n != 2 || c != 1 || r != 1 || u != 2 {
		t.Errorf("counts: want (2,1,1,2), got (%d,%d,%d,%d)", n, c, r, u)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 12 — period dates surfaced separately (Issue 1)
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_PeriodDatesSplit(t *testing.T) {
	b := row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0)
	b.PeriodStartDate = "2025-04-01"
	b.PeriodEndDate = "2025-06-30"
	c := row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0)
	c.PeriodStartDate = "2025-04-01"
	c.PeriodEndDate = "2025-07-15" // period boundary shifted

	diff := diffSchedules([]SimulatedCashflowRow{b}, []SimulatedCashflowRow{c})
	if diff[0].ChangeType != DiffChanged {
		t.Errorf("period end shift: expected CHANGED, got %s", diff[0].ChangeType)
	}
	if diff[0].OldPeriodEndDate == nil || *diff[0].OldPeriodEndDate != "2025-06-30" {
		t.Error("OldPeriodEndDate not surfaced")
	}
	if diff[0].NewPeriodEndDate != "2025-07-15" {
		t.Error("NewPeriodEndDate not surfaced")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 13 — sort stability: confirm we don't mutate caller's slices
// ─────────────────────────────────────────────────────────────────────────────

func TestDiff_DoesNotMutateInputs(t *testing.T) {
	booking := []SimulatedCashflowRow{
		row("CAPITALIZATION", "2025-09-30", 2, 19102, 1910, 0),
		row("CAPITALIZATION", "2025-06-30", 1, 18750, 1875, 0), // out of order
	}
	bookingCopy := append([]SimulatedCashflowRow(nil), booking...)

	_ = diffSchedules(booking, booking)

	for i := range booking {
		if booking[i].EventDate != bookingCopy[i].EventDate {
			t.Errorf("diffSchedules mutated input order at index %d", i)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 14 — buildSummaryDelta (Issue 5)
// ─────────────────────────────────────────────────────────────────────────────

func TestSummaryDelta_RoundsToTwoDecimals(t *testing.T) {
	booking := SimulateSummary{
		TotalInterestAccrued: 18750.123,
		TotalTDSDeducted:     1875.045,
		MaturityAmount:       16875.078,
		EffectiveYield:       7.4567,
	}
	confirmation := SimulateSummary{
		TotalInterestAccrued: 19375.456,
		TotalTDSDeducted:     1937.567,
		MaturityAmount:       17437.889,
		EffectiveYield:       7.7234,
	}

	delta := buildSummaryDelta(booking, confirmation)
	// Adjust expected values to your rounding scheme
	if delta.TotalInterestAccruedDelta != 625.33 {
		t.Errorf("TotalInterestAccruedDelta: want 625.33, got %v", delta.TotalInterestAccruedDelta)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 15 — buildDiffWarnings (Issue 6)
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Scenario 15 — buildDiffWarnings (Issue 6)
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildDiffWarnings_PrincipalMismatch(t *testing.T) {
	bk := SimulateCashflowRequest{
		PrincipalAmount: 1000000,
		StartDate:       "2025-04-01",
		InterestType:    "COMPOUND",
		BankConfigID:    "bcfg-x",
	}
	cf := SimulateCashflowRequest{
		PrincipalAmount: 2000000, // ← differs
		StartDate:       "2025-04-01",
		InterestType:    "COMPOUND",
		BankConfigID:    "bcfg-x",
	}

	warns := buildDiffWarnings(bk, cf)
	if len(warns) == 0 {
		t.Fatal("expected principal mismatch warning, got none")
	}
	found := false
	for _, w := range warns {
		if strings.Contains(strings.ToLower(w), "principal") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("warnings should mention principal, got %v", warns)
	}
}

func TestBuildDiffWarnings_NoMismatch_NoWarnings(t *testing.T) {
	r := SimulateCashflowRequest{
		PrincipalAmount: 1000000,
		StartDate:       "2025-04-01",
		InterestType:    "COMPOUND",
		BankConfigID:    "bcfg-x",
	}
	warns := buildDiffWarnings(r, r)
	if len(warns) != 0 {
		t.Errorf("expected no warnings, got %v", warns)
	}
}
