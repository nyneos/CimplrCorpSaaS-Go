package investmentdashboards

import (
	"testing"
	"time"
)

func TestTrimMonthsByPeriodFlag(t *testing.T) {
	names := []string{"Apr", "May", "Jun"}
	dates := []string{"2026-04-30", "2026-05-31", "2026-06-24"}
	ends := []time.Time{
		time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 31, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 24, 0, 0, 0, 0, time.UTC),
	}

	n1, d1, _ := trimMonthsByPeriodFlag("1M", names, dates, ends)
	if len(n1) != 2 || n1[0] != "May" || n1[1] != "Jun" {
		t.Fatalf("1M want May+Jun, got names=%v dates=%v", n1, d1)
	}

	n3, _, _ := trimMonthsByPeriodFlag("3M", names, dates, ends)
	if len(n3) != 3 {
		t.Fatalf("3M want 3 months, got %v", n3)
	}

	n6, _, _ := trimMonthsByPeriodFlag("6M", names, dates, ends)
	if len(n6) != 3 {
		t.Fatalf("6M with only 3 FY months want all 3, got %v", n6)
	}
}
