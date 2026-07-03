package portfolio

import (
	"math"
	"strings"
	"time"
)

// IsPortfolioTxInflow is true when cash returns to the investor (redemptions / sells).
func IsPortfolioTxInflow(row PortfolioTxRow) bool {
	switch row.TxType {
	case "Redemption":
		return true
	case "Onboard":
		return row.TransactionType == TxTypeRedemption
	default:
		return false
	}
}

// CashFlowAmountFromTx returns the signed cash movement for XIRR:
// negative = outflow (purchase), positive = inflow (redemption).
func CashFlowAmountFromTx(row PortfolioTxRow) (float64, bool) {
	amt := math.Abs(row.Amount)
	if amt == 0 {
		amt = math.Abs(row.NetAmount)
	}
	if amt == 0 {
		return 0, false
	}
	if IsPortfolioTxInflow(row) {
		return amt, true
	}
	if row.TxType == "Investment" || row.TxType == "Onboard" {
		return -amt, true
	}
	return 0, false
}

// SummarizeYTDFlows returns total buy and sell amounts from unified portfolio rows.
func SummarizeYTDFlows(rows []PortfolioTxRow) (buys, sells float64) {
	for _, row := range rows {
		amt := math.Abs(row.Amount)
		if amt == 0 {
			amt = math.Abs(row.NetAmount)
		}
		if amt == 0 {
			continue
		}
		if IsPortfolioTxInflow(row) {
			sells += amt
		} else if row.TxType == "Investment" || row.TxType == "Onboard" {
			buys += amt
		}
	}
	return buys, sells
}

// ParsePortfolioTxDate parses YYYY-MM-DD transaction dates from portfolio rows.
func ParsePortfolioTxDate(row PortfolioTxRow) (time.Time, bool) {
	d := strings.TrimSpace(row.TransactionDate)
	if d == "" {
		return time.Time{}, false
	}
	t, err := time.Parse("2006-01-02", d)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
