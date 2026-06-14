package bankstatement

import (
	"CimplrCorpSaas/api/constants"
	"testing"
)

func TestScoreTransactionRowAlignmentKeepsNormalRows(t *testing.T) {
	colIdx := hdfcPreviewColIdxForTest()
	header := hdfcPreviewHeaderForTest()
	row := []string{"02/06/26", "UPI-PATEL NAGAR", "", "0000651987325096", "02/06/26", "200.00", "", "3,880.11"}

	alignment := scoreTransactionRowAlignment(row, header, colIdx, 4080.11, true)

	if alignment.offset != 0 || alignment.start != -1 {
		t.Fatalf("expected normal alignment, got offset=%d start=%d score=%d", alignment.offset, alignment.start, alignment.score)
	}
	if got := alignment.at(row, colIdx[constants.WithdrawalAmountINR]); got != "200.00" {
		t.Fatalf("withdrawal read mismatch: got %q", got)
	}
	if got := alignment.at(row, colIdx[constants.BalanceINR]); got != "3,880.11" {
		t.Fatalf("balance read mismatch: got %q", got)
	}
}

func TestScoreTransactionRowAlignmentDetectsCompactMinusShift(t *testing.T) {
	colIdx := hdfcPreviewColIdxForTest()
	header := hdfcPreviewHeaderForTest()
	// Converter omitted the blank Chq./Ref.No. spacer on this page, so fields
	// from Tran ID onward are one physical column to the left.
	row := []string{"02/06/26", "UPI-METRO WALKERS", "0000651987595115", "02/06/26", "70.00", "", "3,810.11", ""}

	alignment := scoreTransactionRowAlignment(row, header, colIdx, 3880.11, true)

	if alignment.offset != -1 || alignment.start != colIdx[constants.TranID] {
		t.Fatalf("expected compact -1 alignment from tran id, got offset=%d start=%d score=%d", alignment.offset, alignment.start, alignment.score)
	}
	if got := alignment.at(row, colIdx[constants.ValueDateAlt]); got != "02/06/26" {
		t.Fatalf("value date read mismatch: got %q", got)
	}
	if got := alignment.at(row, colIdx[constants.WithdrawalAmountINR]); got != "70.00" {
		t.Fatalf("withdrawal read mismatch: got %q", got)
	}
	if got := alignment.at(row, colIdx[constants.BalanceINR]); got != "3,810.11" {
		t.Fatalf("balance read mismatch: got %q", got)
	}
}

func hdfcPreviewHeaderForTest() []string {
	return []string{"Date", "", "Narration", "Chq./Ref.No.", "Value Dt", "Withdrawal Amt.", "Deposit Amt.", "Closing Balance"}
}

func hdfcPreviewColIdxForTest() map[string]int {
	return map[string]int{
		constants.TransactionDateAlt:  0,
		constants.TransactionRemarks:  1,
		"Description":                 1,
		constants.TranID:              3,
		constants.ValueDateAlt:        4,
		constants.WithdrawalAmountINR: 5,
		constants.DepositAmountINR:    6,
		constants.BalanceINR:          7,
	}
}
