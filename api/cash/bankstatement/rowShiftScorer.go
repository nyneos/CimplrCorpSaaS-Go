package bankstatement

import (
	"CimplrCorpSaas/api/constants"
	"math"
	"strings"
)

const rowAlignmentEpsilon = 0.01

type rowColumnAlignment struct {
	offset int
	start  int
	score  int
}

func (a rowColumnAlignment) at(row []string, logicalIdx int) string {
	physicalIdx := logicalIdx
	if a.offset != 0 && (a.start < 0 || logicalIdx >= a.start) {
		physicalIdx = logicalIdx + a.offset
	}
	if physicalIdx >= 0 && physicalIdx < len(row) {
		return row[physicalIdx]
	}
	return ""
}

func (a rowColumnAlignment) shifted() bool {
	return a.offset != 0
}

func scoreTransactionRowAlignment(row []string, headerRow []string, colIdx map[string]int, prevBalance float64, prevBalanceOK bool) rowColumnAlignment {
	candidates := rowAlignmentCandidates(colIdx)
	best := candidates[0]
	best.score = scoreAlignmentCandidate(best, row, colIdx, prevBalance, prevBalanceOK)

	for _, candidate := range candidates[1:] {
		candidate.score = scoreAlignmentCandidate(candidate, row, colIdx, prevBalance, prevBalanceOK)
		if candidate.score > best.score {
			best = candidate
		}
	}

	// Preserve legacy behaviour for converter outputs that add a trailing numeric balance column.
	if len(row) > len(headerRow) {
		if idxBal, ok := colIdx[constants.BalanceINR]; ok && idxBal+1 < len(row) {
			if _, ok := parseNonEmptyAmount(row[idxBal+1]); ok && best.score < 80 {
				return rowColumnAlignment{offset: 1, start: -1, score: best.score}
			}
		}
	}

	return best
}

func rowAlignmentCandidates(colIdx map[string]int) []rowColumnAlignment {
	candidates := []rowColumnAlignment{{offset: 0, start: -1}}
	seen := map[[2]int]bool{{0, -1}: true}

	add := func(offset, start int) {
		key := [2]int{offset, start}
		if seen[key] {
			return
		}
		seen[key] = true
		candidates = append(candidates, rowColumnAlignment{offset: offset, start: start})
	}

	add(1, -1)
	add(-1, -1)

	for _, key := range []string{
		constants.TranID,
		constants.ValueDateAlt,
		constants.WithdrawalAmountINR,
		constants.DepositAmountINR,
		constants.BalanceINR,
	} {
		if idx, ok := colIdx[key]; ok && idx > 0 {
			add(1, idx)
			add(-1, idx)
		}
	}

	return candidates
}

func scoreAlignmentCandidate(candidate rowColumnAlignment, row []string, colIdx map[string]int, prevBalance float64, prevBalanceOK bool) int {
	score := 0

	transactionDateOK := false
	if idx, ok := colIdx[constants.TransactionDateAlt]; ok {
		_, transactionDateOK = parseDateCell(candidate.at(row, idx))
	}
	if !transactionDateOK {
		if idx, ok := colIdx["Date"]; ok {
			_, transactionDateOK = parseDateCell(candidate.at(row, idx))
		}
	}
	if transactionDateOK {
		score += 30
	} else {
		score -= 40
	}

	if idx, ok := colIdx[constants.ValueDateAlt]; ok {
		if _, ok := parseDateCell(candidate.at(row, idx)); ok {
			score += 20
		} else {
			score -= 15
		}
	}

	balance, balanceOK := parseAlignedBalance(candidate, row, colIdx)
	if balanceOK {
		score += 35
	} else {
		score -= 35
	}

	withdrawal, deposit, amountOK, bothSides := parseAlignedAmounts(candidate, row, colIdx)
	switch {
	case bothSides:
		score -= 45
	case amountOK:
		score += 25
	default:
		score -= 25
	}

	if prevBalanceOK && balanceOK {
		if amountOK && balanceMatches(prevBalance, withdrawal, deposit, balance) {
			score += 120
		} else if !amountOK && math.Abs(prevBalance-balance) <= rowAlignmentEpsilon {
			score += 25
		} else {
			score -= 80
		}
	}

	return score
}

func parseAlignedBalance(candidate rowColumnAlignment, row []string, colIdx map[string]int) (float64, bool) {
	idxBal, ok := colIdx[constants.BalanceINR]
	if !ok {
		return 0, false
	}
	return parseNonEmptyAmount(candidate.at(row, idxBal))
}

func parseAlignedAmounts(candidate rowColumnAlignment, row []string, colIdx map[string]int) (withdrawal float64, deposit float64, amountOK bool, bothSides bool) {
	idxW, okW := colIdx[constants.WithdrawalAmountINR]
	idxD, okD := colIdx[constants.DepositAmountINR]
	idxCrDr, okCrDr := colIdx["CrDr"]

	singleAmtIdx := -1
	if okW && okD && idxW == idxD {
		singleAmtIdx = idxW
	} else if okW && !okD {
		singleAmtIdx = idxW
	} else if okD && !okW {
		singleAmtIdx = idxD
	}

	if singleAmtIdx >= 0 {
		amount, ok := parseNonEmptyAmount(candidate.at(row, singleAmtIdx))
		if !ok || amount <= 0 {
			return 0, 0, false, false
		}
		if okCrDr {
			crdr := strings.ToLower(strings.TrimSpace(candidate.at(row, idxCrDr)))
			if strings.HasPrefix(crdr, "cr") || strings.Contains(crdr, "credit") {
				return 0, amount, true, false
			}
			if strings.HasPrefix(crdr, "dr") || strings.Contains(crdr, "debit") {
				return amount, 0, true, false
			}
		}
		return amount, 0, true, false
	}

	if okW {
		if value, ok := parseNonEmptyAmount(candidate.at(row, idxW)); ok && value > 0 {
			withdrawal = value
		}
	}
	if okD {
		if value, ok := parseNonEmptyAmount(candidate.at(row, idxD)); ok && value > 0 {
			deposit = value
		}
	}

	bothSides = withdrawal > 0 && deposit > 0
	amountOK = withdrawal > 0 || deposit > 0
	return withdrawal, deposit, amountOK, bothSides
}

func parseDateCell(value string) (string, bool) {
	if dt, err := parseDate(strings.TrimSpace(value)); err == nil && !dt.IsZero() {
		return dt.Format(constants.DateFormat), true
	}
	return "", false
}

func parseNonEmptyAmount(value string) (float64, bool) {
	cleaned := strings.TrimSpace(cleanAmount(value))
	if cleaned == "" || cleaned == "-" {
		return 0, false
	}
	amount, err := parseAmount(cleaned)
	if err != nil {
		return 0, false
	}
	return amount, true
}

func balanceMatches(prevBalance, withdrawal, deposit, currentBalance float64) bool {
	expected := prevBalance - withdrawal + deposit
	return math.Abs(expected-currentBalance) <= rowAlignmentEpsilon
}
