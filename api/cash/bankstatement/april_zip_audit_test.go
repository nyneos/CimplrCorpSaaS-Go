package bankstatement

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var acctFromFilename = regexp.MustCompile(`(\d{8,12})`)

// countGXLSMRows applies the same keep/skip rules as preview (post-fix).
func countGXLSMRows(rows [][]string) (kept int, skippedSavli int, skippedByNonTxn int, lastBal float64, err error) {
	hdr := -1
	for i, row := range rows {
		j := strings.ToLower(strings.Join(row, "|"))
		if strings.Contains(j, "date") && strings.Contains(j, "debit") {
			hdr = i
			break
		}
	}
	if hdr < 0 {
		return 0, 0, 0, 0, fmt.Errorf("header not found")
	}
	rb := 0.0
	for i := hdr + 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) == 0 || strings.TrimSpace(strings.Join(row, "")) == "" {
			continue
		}
		desc := ""
		if len(row) > 1 {
			desc = row[1]
		}
		cells := []string{row[0], desc, strings.Join(row, " ")}
		if IsNonTransactionRow(cells...) {
			skippedByNonTxn++
			if strings.Contains(strings.ToUpper(desc), "SAVLI") {
				skippedSavli++
			}
			continue
		}
		if IsStatementOpeningCarryRow(desc) {
			if len(row) > 3 {
				if v, e := parseAmount(cleanAmount(row[3])); e == nil {
					rb = v
				}
			}
			continue
		}
		if _, e := parseDate(strings.TrimSpace(row[0])); e != nil {
			continue
		}
		w, d := 0.0, 0.0
		if len(row) > 2 {
			if v, ok := parseAmountNonZero(row[2]); ok {
				w = v
			}
		}
		if len(row) > 3 {
			if v, ok := parseAmountNonZero(row[3]); ok {
				d = v
			}
		}
		rb = math.Round((rb+d-w)*100) / 100
		kept++
	}
	return kept, skippedSavli, skippedByNonTxn, rb, nil
}

// countGXLSMRowsLegacy simulates pre-fix IsNonTransactionRow (bare "avl" substring).
func countGXLSMRowsLegacy(rows [][]string) (kept int, droppedSavli int) {
	hdr := -1
	for i, row := range rows {
		j := strings.ToLower(strings.Join(row, "|"))
		if strings.Contains(j, "date") && strings.Contains(j, "debit") {
			hdr = i
			break
		}
	}
	if hdr < 0 {
		return 0, 0
	}
	legacyNonTxn := func(cells ...string) bool {
		for _, cell := range cells {
			lc := strings.ToLower(strings.TrimSpace(cell))
			if lc == "" {
				continue
			}
			for _, kw := range nonTxnKeywords {
				if strings.Contains(lc, kw) {
					return true
				}
			}
			// old bare avl/avil that we removed
			if strings.Contains(lc, "avl") || strings.Contains(lc, "avil") {
				return true
			}
			if strings.Contains(lc, "brought forward") {
				return true
			}
		}
		return false
	}
	for i := hdr + 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) == 0 || strings.TrimSpace(strings.Join(row, "")) == "" {
			continue
		}
		desc := ""
		if len(row) > 1 {
			desc = row[1]
		}
		cells := []string{row[0], desc, strings.Join(row, " ")}
		if legacyNonTxn(cells...) {
			if strings.Contains(strings.ToUpper(desc), "SAVLI") {
				droppedSavli++
			}
			continue
		}
		if IsStatementOpeningCarryRow(desc) {
			continue
		}
		if _, e := parseDate(strings.TrimSpace(row[0])); e != nil {
			continue
		}
		kept++
	}
	return kept, droppedSavli
}

func extractAccountFromFilename(name string) string {
	m := acctFromFilename.FindStringSubmatch(name)
	if len(m) < 2 {
		return ""
	}
	return strings.TrimLeft(m[1], "0")
}

// TestApril2026ZipAllAccounts audits every XLS in April 2026.zip for row-count / SAVLI issues.
// unzip to /tmp/april2026 first.
func TestApril2026ZipAllAccounts(t *testing.T) {
	dir := "/tmp/april2026"
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Skip("extract April 2026.zip to /tmp/april2026:", err)
	}

	type row struct {
		file, acct   string
		kept, legacy int
		savliDrop    int
		lastBal      float64
		rawRows      int
		issue        string
	}
	var results []row

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".xls") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Logf("SKIP read %s: %v", e.Name(), err)
			continue
		}
		rows, err := parseXLSFile(data)
		if err != nil {
			t.Logf("SKIP parse %s: %v", e.Name(), err)
			continue
		}
		acct := extractAccountFromFilename(e.Name())
		kept, _, _, lastBal, err := countGXLSMRows(rows)
		if err != nil {
			t.Logf("SKIP count %s: %v", e.Name(), err)
			continue
		}
		legacy, savliDrop := countGXLSMRowsLegacy(rows)
		issue := ""
		if savliDrop > 0 {
			issue = fmt.Sprintf("SAVLI_DROP=%d", savliDrop)
		}
		if legacy != kept {
			issue += fmt.Sprintf(" LEGACY_GAP=%d", kept-legacy)
		}
		results = append(results, row{
			file: e.Name(), acct: acct, kept: kept, legacy: legacy,
			savliDrop: savliDrop, lastBal: lastBal, rawRows: len(rows), issue: strings.TrimSpace(issue),
		})
	}

	t.Logf("=== April 2026 ZIP audit (%d files) ===", len(results))
	t.Logf("%-45s %10s %6s %6s %12s %s", "FILE", "ACCOUNT", "KEPT", "OLD", "LAST_BAL", "ISSUE")
	for _, r := range results {
		flag := ""
		if r.issue != "" {
			flag = r.issue
		}
		if r.acct == "36013133" || r.acct == "36013893" {
			flag = "** " + flag
		}
		t.Logf("%-45s %10s %6d %6d %12.2f %s", truncName(r.file, 45), r.acct, r.kept, r.legacy, r.lastBal, flag)
	}

	// Highlight accounts where legacy dropped SAVLI rows
	var affected int
	for _, r := range results {
		if r.savliDrop > 0 {
			affected++
		}
	}
	if affected == 0 {
		t.Log("No SAVLI false-positive drops with current parser")
	} else {
		t.Logf("%d file(s) had SAVLI rows wrongly dropped by OLD parser (fixed now)", affected)
	}
}

func truncName(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

// TestApril2026Account36013133 deep-check BOM file for 36013133.
func TestApril2026Account36013133(t *testing.T) {
	path := "/tmp/april2026/95201994180_GXLSM_0036013133_BOM_01052026_M.xls"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skip(err)
	}
	rows, err := parseXLSFile(data)
	if err != nil {
		t.Fatal(err)
	}
	kept, savliSkipped, nonTxnSkipped, lastBal, err := countGXLSMRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	legacy, savliDrop := countGXLSMRowsLegacy(rows)

	t.Logf("36013133: kept=%d legacy=%d savli_wrongly_dropped=%d non_txn_skipped=%d last_bal=%.2f",
		kept, legacy, savliDrop, nonTxnSkipped, lastBal)

	if savliDrop > 0 {
		t.Logf("OLD bug would drop %d HEATING-SAVLI rows; NEW parser recovers them (skippedSavli in nonTxn=%d)", savliDrop, savliSkipped)
		if kept != legacy+savliDrop {
			t.Errorf("expected kept=%d = legacy(%d)+savliDrop(%d)", kept, legacy, savliDrop)
		}
	} else if legacy != kept {
		t.Errorf("unexpected row count change: legacy=%d current=%d (delta=%d)", legacy, kept, kept-legacy)
	}

	// ZBA sweep accounts should end at ~0
	if kept > 0 && abs(lastBal) > 0.02 {
		t.Logf("WARNING: last running balance %.2f (expected ~0 for sweep ZBA — verify credits/debits in file)", lastBal)
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
