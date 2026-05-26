package rounding

import "testing"

func TestRoundByMethodTwoDecimals(t *testing.T) {
	got := RoundByMethod(1234.567, 2, "ROUND")
	if got != 1234.57 {
		t.Fatalf("ROUND 2dp: got %v want 1234.57", got)
	}
	got = RoundByMethod(1234.567, 2, "TRUNCATE")
	if got != 1234.56 {
		t.Fatalf("TRUNCATE 2dp: got %v want 1234.56", got)
	}
}

func TestRoundByMethodZeroDecimals(t *testing.T) {
	got := RoundByMethod(1234.567, 0, "ROUND")
	if got != 1235 {
		t.Fatalf("ROUND 0dp: got %v want 1235", got)
	}
}

func TestApplyATMaturityDefersIntermediate(t *testing.T) {
	raw := 1234.567
	got := Apply(raw, 2, "ROUND", "AT_MATURITY", false)
	if got != raw {
		t.Fatalf("expected unrounded intermediate, got %v", got)
	}
	got = Apply(raw, 2, "ROUND", "AT_MATURITY", true)
	if got != 1234.57 {
		t.Fatalf("expected final round, got %v", got)
	}
}

func TestNormalizeMethodFloorCeil(t *testing.T) {
	if NormalizeMethod("FLOOR") != "ROUND_DOWN" {
		t.Fatal("FLOOR mapping")
	}
	if NormalizeMethod("CEIL") != "ROUND_UP" {
		t.Fatal("CEIL mapping")
	}
}
