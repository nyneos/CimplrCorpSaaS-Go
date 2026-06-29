package bankstatement

import "testing"

// TestClosingBalanceReanchorRecoversFinalWithdrawal reproduces the ICICI screenshot case: the
// account is swept to a 0.00 closing balance, but the LLM lost the final balance and copied the
// previous row's 9000. Because amounts are reconstructed from balance deltas, the unchanged balance
// zeroes the genuine ₹9,000 withdrawal on the last row — and the ghost-balance repair can't catch
// it (9000 is a real balance earlier in the statement, and the last row has no following anchor).
// The "Page Total ... 0.00 Cr" row is the independent closing anchor that must restore it.
func TestClosingBalanceReanchorRecoversFinalWithdrawal(t *testing.T) {
	ob := flexFloat(0)
	resp := &llmTxnResponse{
		OpeningBalance: &ob,
		Transactions: []llmExtractedTxn{
			{Date: "03-02-2026", Description: "NEFT-CITIN26616782256", Deposit: 100000, Balance: 100000},
			{Date: "16-02-2026", Description: "NEFT-CITIN26621312615", Deposit: 18000, Balance: 118000},
			{Date: "16-02-2026", Description: "COMMERCIAL PAPER CHARGES", Withdrawal: 100000, Balance: 18000},
			{Date: "16-02-2026", Description: "SGST202602166798447682", Withdrawal: 9000, Balance: 9000},
			// WRONG as extracted: should be withdrawal 9000, balance 0.00.
			{Date: "16-02-2026", Description: "CGST202602166798447685", Balance: 9000},
		},
	}
	sourceRows := [][]string{
		{"Date", "Particulars", "Withdrawals", "Deposits", "Balance(INR)"},
		{"03-02-2026", "NEFT-CITIN26616782256", "0.00", "1,00,000.00", "1,00,000.00 Cr"},
		{"16-02-2026", "NEFT-CITIN26621312615", "0.00", "18,000.00", "1,18,000.00 Cr"},
		{"16-02-2026", "COMMERCIAL PAPER CHARGES", "1,00,000.00", "0.00", "18,000.00 Cr"},
		{"16-02-2026", "SGST202602166798447682", "9,000.00", "0.00", "9,000.00 Cr"},
		{"16-02-2026", "CGST202602166798447685", "9,000.00", "0.00", "0.00 Cr"},
		{"", "Page Total:", "1,18,000.00", "1,18,000.00", "0.00 Cr"},
	}

	out, ok := buildTxnMapsFromLLM(resp, llmTxnContext{batchID: "TEST00", sourceRows: sourceRows})
	if !ok {
		t.Fatalf("buildTxnMapsFromLLM returned ok=false; expected a usable extraction")
	}
	if len(out) != 5 {
		t.Fatalf("expected 5 transactions, got %d", len(out))
	}
	last := out[len(out)-1]
	if w, _ := last["withdrawal_amount"].(float64); absf(w-9000) > 0.01 {
		t.Fatalf("final withdrawal not recovered: got %.2f want 9000.00", w)
	}
	if b, _ := last["balance"].(float64); absf(b) > 0.01 {
		t.Fatalf("final balance not re-anchored: got %.2f want 0.00", b)
	}
	if d, _ := last["deposit_amount"].(float64); absf(d) > 0.01 {
		t.Fatalf("final deposit should be 0, got %.2f", d)
	}
}

// TestScanDeclaredClosingBalance covers the two location strategies plus the not-found case.
func TestScanDeclaredClosingBalance(t *testing.T) {
	cases := []struct {
		name string
		rows [][]string
		want float64
		ok   bool
	}{
		{
			name: "page total row, zero closing",
			rows: [][]string{
				{"Date", "Particulars", "Withdrawals", "Deposits", "Balance(INR)"},
				{"", "Page Total:", "1,18,000.00", "1,18,000.00", "0.00 Cr"},
			},
			want: 0, ok: true,
		},
		{
			name: "explicit closing balance label",
			rows: [][]string{
				{"Closing Balance", "12,345.67 Cr"},
			},
			want: 12345.67, ok: true,
		},
		{
			name: "overdrawn closing carries Dr sign",
			rows: [][]string{
				{"Total", "500.00", "0.00", "250.00 Dr"},
			},
			want: -250, ok: true,
		},
		{
			name: "no totals or closing row",
			rows: [][]string{
				{"Date", "Particulars", "Balance"},
				{"03-02-2026", "NEFT", "100.00"},
			},
			want: 0, ok: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := scanDeclaredClosingBalance(tc.rows)
			if ok != tc.ok {
				t.Fatalf("ok mismatch: got %v want %v", ok, tc.ok)
			}
			if ok && absf(got-tc.want) > 0.01 {
				t.Fatalf("value mismatch: got %.2f want %.2f", got, tc.want)
			}
		})
	}
}
