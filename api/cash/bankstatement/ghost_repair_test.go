package bankstatement

import "testing"

// TestRepairGhostBalances reproduces the BOB-statement screenshot case: the model invented the
// intermediate balance 837751.25 (reading +6 credit / -130 debit as -6 / -118), which never
// appears in the statement. The real balance 837763.25 and the amounts 6.00 / 130.00 do appear,
// so the repair must snap the ghost back to 837763.25.
func TestRepairGhostBalances(t *testing.T) {
	opening := 842757.25
	views := []amtRow{
		{bal: 837757.25}, // prev anchor (valid)
		{bal: 837751.25}, // GHOST — should become 837763.25
		{bal: 837633.25}, // next anchor (valid)
	}
	// Source numbers actually present in the statement.
	srcVals := []float64{842757.25, 837757.25, 837763.25, 837633.25, 5000.00, 6.00, 130.00}
	src := make(map[int64]struct{})
	for _, v := range srcVals {
		src[int64(v*100+0.5)] = struct{}{}
	}

	fixed, unfixed := repairGhostBalances(opening, views, src)
	if fixed != 1 || unfixed != 0 {
		t.Fatalf("expected fixed=1 unfixed=0, got fixed=%d unfixed=%d", fixed, unfixed)
	}
	if absf(views[1].bal-837763.25) > 0.001 {
		t.Fatalf("ghost not repaired: got %.2f want 837763.25", views[1].bal)
	}

	// After repair, deriving amounts from deltas must yield +6 deposit then -130 withdrawal.
	prev := views[0].bal
	d := views[1].bal - prev
	if absf(d-6.00) > 0.001 {
		t.Fatalf("expected +6.00 deposit after repair, got %.2f", d)
	}
	w := views[1].bal - views[2].bal
	if absf(w-130.00) > 0.001 {
		t.Fatalf("expected 130.00 withdrawal after repair, got %.2f", w)
	}
}
