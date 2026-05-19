package fdAccrual

import (
	"testing"
	"time"
)

func TestAccrualPeriodBounds_Monthly(t *testing.T) {
	at := time.Date(2026, 5, 18, 10, 0, 0, 0, time.UTC)
	start, end := AccrualPeriodBounds(at, "MONTHLY")
	if start.Format("2006-01-02") != "2026-05-01" || end.Format("2006-01-02") != "2026-05-31" {
		t.Fatalf("monthly bounds: got %s → %s", start, end)
	}
}

func TestAccrualPeriodBounds_Quarterly(t *testing.T) {
	at := time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC)
	start, end := AccrualPeriodBounds(at, "QUARTERLY")
	if start.Format("2006-01-02") != "2026-04-01" || end.Format("2006-01-02") != "2026-06-30" {
		t.Fatalf("quarterly bounds: got %s → %s", start, end)
	}
}

func TestPeriodGranularityForScheduler_RunUsesSchedule(t *testing.T) {
	g := periodGranularityForScheduler("RUN", "QUARTERLY")
	if g != "QUARTERLY" {
		t.Fatalf("expected QUARTERLY, got %s", g)
	}
}

func TestGenerateSubPeriods_DailyInclusiveEnd(t *testing.T) {
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	subs := generateSubPeriods(start, end, "DAILY")
	if len(subs) != 3 {
		t.Fatalf("expected 3 daily sub-periods, got %d", len(subs))
	}
}
