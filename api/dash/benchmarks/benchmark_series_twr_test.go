package benchmarks

import "testing"

func TestBuildPortfolioIndexedSeriesTWR(t *testing.T) {
	// Apr: 200k holding, May: 205k (+2.5% market), Jun: 5.8M AUM after 5.6M inflow
	aum := map[int]float64{1: 200_000, 2: 205_000, 3: 5_800_000}
	cf := map[int]float64{1: 0, 2: 0, 3: 5_600_000}
	out := BuildPortfolioIndexedSeriesTWR(aum, cf, 3)
	if len(out) != 3 {
		t.Fatalf("expected 3 points, got %d", len(out))
	}
	if out[0] != 100 {
		t.Fatalf("apr base want 100, got %v", out[0])
	}
	if out[1] < 102 || out[1] > 103 {
		t.Fatalf("may want ~102.5, got %v", out[1])
	}
	// Jun: small market loss on existing book after large inflow stripped out
	wantJun := RoundIndexed(out[1] * (1 + (5_800_000.0-205_000-5_600_000)/205_000))
	if out[2] != wantJun {
		t.Fatalf("jun want %v, got %v", wantJun, out[2])
	}
	if out[2] >= out[1] {
		t.Fatalf("jun should reflect market move not inflow, got %v", out)
	}
}

func TestBuildPortfolioIndexedSeriesTWR_NoHoldingsUntilJune(t *testing.T) {
	aum := map[int]float64{1: 0, 2: 0, 3: 500_000}
	cf := map[int]float64{3: 500_000}
	out := BuildPortfolioIndexedSeriesTWR(aum, cf, 3)
	if out[0] != 100 || out[1] != 100 || out[2] != 100 {
		t.Fatalf("expected flat 100 before meaningful prior AUM, got %v", out)
	}
}
