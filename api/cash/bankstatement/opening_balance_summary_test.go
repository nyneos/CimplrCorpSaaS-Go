package bankstatement

import "testing"

// TestExtractLabelledBalanceSummaryBox reproduces the IDFC FIRST statement layout where the
// opening/closing balances live in a summary box as column HEADERS with the figures in the row
// BELOW (and carry a "CR" suffix). The old same-row-only scan returned nil for both.
func TestExtractLabelledBalanceSummaryBox(t *testing.T) {
	csv := "" +
		"Opening Balance,Total Debit,Total Credit,Closing Balance\n" +
		"17.91 CR,\"1,47,103.00\",\"1,47,090.00\",4.91 CR\n"

	if ob := extractOpeningBalanceFromCSV([]byte(csv)); ob == nil {
		t.Fatalf("opening balance not detected from summary box")
	} else if absf(*ob-17.91) > 0.001 {
		t.Fatalf("opening balance = %.2f, want 17.91", *ob)
	}

	if cb := extractClosingBalanceFromCSV([]byte(csv)); cb == nil {
		t.Fatalf("closing balance not detected from summary box")
	} else if absf(*cb-4.91) > 0.001 {
		t.Fatalf("closing balance = %.2f, want 4.91", *cb)
	}
}

// TestCollectSourceNumbersHandlesCRSuffix ensures balances carrying an uppercase "CR" suffix (with a
// space) are recognised. A case-sensitive TrimSuffix("Cr") previously dropped them all, causing the
// ghost-balance cross-check to discard otherwise-correct LLM extractions.
func TestCollectSourceNumbersHandlesCRSuffix(t *testing.T) {
	rows := [][]string{
		{"30-Jun-2021", "QUARTERLY INTEREST", "", "5.00", "22.91 CR"},
		{"03-Jul-2021", "UPI", "", "9,000.00", "9,022.91 CR"},
	}
	set := collectSourceNumbers(rows)
	for _, want := range []float64{22.91, 9022.91, 9000.00, 5.00} {
		if !inSourceSet(set, want) {
			t.Fatalf("source set missing %.2f (CR-suffixed/comma value not parsed)", want)
		}
	}
}

// TestScanOpeningBalanceSkipsEmptyCell ensures an "Opening Balance" label with no figure beside it
// does not lock in a spurious opening balance of 0 (which suppressed the correct first-row derivation).
func TestScanOpeningBalanceSkipsEmptyCell(t *testing.T) {
	rows := [][]string{
		{"Opening Balance", "", "", ""},
		{"Some other header", "x"},
	}
	if ob := scanOpeningBalanceFromRows(rows); ob != nil {
		t.Fatalf("expected nil opening balance for empty cell, got %.2f", *ob)
	}
}

// TestSummaryRowBalance covers the in-table "Opening Balance" row whose figure sits in the mapped
// Balance column (with a "CR" suffix) and whose Debit/Credit cells are empty.
func TestSummaryRowBalance(t *testing.T) {
	// Transaction Date | Value Date | Particulars | Cheque No | Debit | Credit | Balance
	row := []string{"", "", "Opening Balance", "", "", "", "17.91 CR"}
	balIdx := 6
	if v, ok := summaryRowBalance(row, balIdx); !ok {
		t.Fatalf("balance not extracted from opening balance row")
	} else if absf(v-17.91) > 0.001 {
		t.Fatalf("balance = %.2f, want 17.91", v)
	}
}
