// regen_golden_test.go — regenerates testdata/golden/*.csv from CanonicalScenarios
// (which are extracted directly from FD_Scenarios_v7.xlsx via gen_fixtures.py).
//
// Run once to update all golden files:
//   go test ./api/investment/fdMaster/... -run TestRegenGolden -v -count=1
//
// After running, the golden CSVs will reflect the exact workbook values.
// Commit the updated CSVs — they become the new regression baseline.
package fdMaster

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRegenGolden(t *testing.T) {
	if err := os.MkdirAll(goldenDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", goldenDir, err)
	}

	for _, sc := range CanonicalScenarios {
		sc := sc
		t.Run(sc.SheetName, func(t *testing.T) {
			params := buildParamsFromScenario(sc)
			isCompound := sc.InterestType == "COMPOUND"

			got := generateCashflowSchedule(params)

			// Filter out internal-only rows
			var rows []CashflowRow
			for _, r := range got {
				if r.EventType != "TDS_DEDUCTION" {
					rows = append(rows, r)
				}
			}

			path := filepath.Join(goldenDir, sc.SheetName+".csv")
			f, err := os.Create(path)
			if err != nil {
				t.Fatalf("create %s: %v", path, err)
			}
			defer f.Close()

			w := csv.NewWriter(f)
			// Header
			_ = w.Write(goldenColNames[:])
			for _, r := range rows {
				fields := rowToFields(r, isCompound)
				_ = w.Write(fields[:])
			}
			w.Flush()
			t.Logf("wrote %d rows → %s", len(rows), path)

			// Verify: run the same comparison used by TestGoldenWorkbookParity
			// to confirm the CSV round-trips correctly.
			golden := readGoldenCSV(t, path)
			assertRows(t, sc.SheetName, golden, rows, isCompound)
		})
	}
}

// fmtF0Regen formats a float to 0 decimal places, or "" if zero.
// Kept here to avoid conflict with golden_test helpers (same function name).
func fmtF0Regen(v float64) string {
	if v == 0 {
		return "0"
	}
	return fmt.Sprintf("%.0f", v)
}
