package bankstatement

import (
	"os"
	"strings"
	"testing"
)

func TestCitiGXLSM36013893NonTxnFilter(t *testing.T) {
	if IsNonTransactionRow("BOOK TRANSFER||FROM THERMAX||LIMITED-FCOE DIVISION 04||HEATING-SAVLI 109791") {
		t.Fatal("must not treat HEATING-SAVLI book transfers as footer rows")
	}
	if !IsNonTransactionRow("AVL BALANCE || 30.APR 2026") {
		t.Fatal("must still skip AVL BALANCE footer rows")
	}
}

func TestCitiGXLSM36013893RowCount(t *testing.T) {
	path := "/tmp/april2026/95201994180_GXLSM_0036013893_PUN_01052026_M.xls"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skip("extract April 2026.zip to /tmp/april2026:", err)
	}
	rows, err := parseXLSFile(data)
	if err != nil {
		t.Fatal(err)
	}
	hdr := -1
	for i, row := range rows {
		j := strings.ToLower(strings.Join(row, "|"))
		if strings.Contains(j, "date") && strings.Contains(j, "debit") {
			hdr = i
			break
		}
	}
	if hdr < 0 {
		t.Fatal("header not found")
	}
	kept, skippedSavli := 0, 0
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
			continue
		}
		if IsStatementOpeningCarryRow(desc) {
			continue
		}
		if _, err := parseDate(strings.TrimSpace(row[0])); err != nil {
			continue
		}
		kept++
		if strings.Contains(strings.ToUpper(desc), "SAVLI") {
			skippedSavli++
		}
	}
	if kept != 72 {
		t.Fatalf("expected 72 transaction rows, got %d", kept)
	}
	if skippedSavli != 5 {
		t.Fatalf("expected 5 HEATING-SAVLI rows to be kept, got %d", skippedSavli)
	}
}
