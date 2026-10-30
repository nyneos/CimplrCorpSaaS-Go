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

func TestRoundByMethodDecimalEdges(t *testing.T) {
	cases := []struct {
		v      float64
		d      int
		method string
		want   float64
	}{
		{1.15, 2, "TRUNCATE", 1.15},
		{1.005, 2, "ROUND", 1.01},
		{100.00000000000001, 2, "ROUND_UP", 100},
		{114.99999999999999, 2, "TRUNCATE", 115},
		{100.004, 2, "ROUND_UP", 100.01},
		{-1.234, 2, "ROUND_UP", -1.24},
		{-1.236, 2, "ROUND_DOWN", -1.23},
		{2.345, 2, "BANKERS", 2.34},
		{2.355, 2, "BANKERS", 2.36},
	}
	for _, c := range cases {
		if got := RoundByMethod(c.v, c.d, c.method); got != c.want {
			t.Errorf("RoundByMethod(%v, %d, %s) = %v, want %v", c.v, c.d, c.method, got, c.want)
		}
	}
}
