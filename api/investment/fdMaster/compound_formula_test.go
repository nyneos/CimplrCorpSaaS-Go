package fdMaster

import "testing"

func TestCompoundFormulaInterest_CO_Q_MAT_M(t *testing.T) {
	got := compoundFormulaInterest(1_000_000, 10, 4, 730)
	want := float64(218403)
	if got != want {
		t.Fatalf("compoundFormulaInterest = %.0f, want %.0f", got, want)
	}
}

func TestCapPeriodsPerYear(t *testing.T) {
	if capPeriodsPerYear("QUARTERLY") != 4 {
		t.Fatalf("QUARTERLY = %d", capPeriodsPerYear("QUARTERLY"))
	}
}
