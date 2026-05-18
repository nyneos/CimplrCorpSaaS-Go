package fdMaster

import "testing"

func TestResolveCapAndPayoutFreqRefs_Compound(t *testing.T) {
	capRef, payoutRef := resolveCapAndPayoutFreqRefs(true, "FREQ-Q", "FREQ-Y")
	if capRef != "FREQ-Q" {
		t.Fatalf("capRef = %q, want FREQ-Q", capRef)
	}
	if payoutRef != "FREQ-Y" {
		t.Fatalf("payoutRef = %q, want FREQ-Y", payoutRef)
	}

	capRef, payoutRef = resolveCapAndPayoutFreqRefs(true, "FREQ-Q", "")
	if capRef != "FREQ-Q" || payoutRef != "FREQ-Q" {
		t.Fatalf("default payout: cap=%q payout=%q", capRef, payoutRef)
	}
}

func TestResolveCapAndPayoutFreqRefs_Simple(t *testing.T) {
	capRef, payoutRef := resolveCapAndPayoutFreqRefs(false, "FREQ-M", "")
	if capRef != "FREQ-M" || payoutRef != "FREQ-M" {
		t.Fatalf("simple default: cap=%q payout=%q", capRef, payoutRef)
	}
}

func TestEnsureAtMaturityPayoutFreq(t *testing.T) {
	f := ensureAtMaturityPayoutFreq("AT_MATURITY", &CompoundingFreq{})
	if f.FrequencyID == "" || f.FrequencyType != "AT_MATURITY" {
		t.Fatalf("AT_MATURITY freq: %+v", f)
	}
	f2 := ensureAtMaturityPayoutFreq("FREQ-Q", &CompoundingFreq{FrequencyID: "FREQ-Q", FrequencyType: "QUARTERLY"})
	if f2.FrequencyID != "FREQ-Q" {
		t.Fatalf("quarterly unchanged: %+v", f2)
	}
}
