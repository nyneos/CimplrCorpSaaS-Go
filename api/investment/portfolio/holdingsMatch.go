package portfolio

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"CimplrCorpSaas/api/constants"
)

// attachTransactionsToHoldings maps transaction rows onto holdings using the same
// position identity as the live holdings engine (entity + folio + demat + scheme).
func attachTransactionsToHoldings(holdings []PortfolioHoldingsRow, txs []PortfolioTxRow) {
	if len(holdings) == 0 || len(txs) == 0 {
		return
	}

	byKey := make(map[holdingsPositionKey][]PortfolioTxRow, len(txs))
	unkeyed := make([]PortfolioTxRow, 0)

	for _, tx := range txs {
		key := holdingsPositionKeyFromTx(tx)
		if key.entity == "" || key.scheme == "" {
			unkeyed = append(unkeyed, tx)
			continue
		}
		byKey[key] = append(byKey[key], tx)
	}

	for i := range holdings {
		h := &holdings[i]
		h.Transactions = []map[string]interface{}{}

		key := holdingsPositionKeyFromRow(*h)
		matched := byKey[key]

		if len(matched) == 0 {
			for _, tx := range txs {
				if txMatchesHolding(tx, *h) {
					matched = append(matched, tx)
				}
			}
		}
		if len(matched) == 0 {
			for _, tx := range unkeyed {
				if txMatchesHolding(tx, *h) {
					matched = append(matched, tx)
				}
			}
		}

		for _, row := range matched {
			txType := row.TxType
			if row.TransactionType != "" {
				txType = row.TxType + " · " + row.TransactionType
			}
			h.Transactions = append(h.Transactions, map[string]interface{}{
				"transaction_date": row.TransactionDate,
				"transaction_type": txType,
				"amount":           row.Amount,
				"units":            row.Units,
				"nav":              row.Nav,
				"folio_number":     row.FolioNumber,
				"demat_acc_number": row.DematNumber,
				"source":           row.Source,
			})
		}
	}
}

func holdingsPositionKeyFromTx(tx PortfolioTxRow) holdingsPositionKey {
	scheme := canonicalSchemeKey(
		tx.AmfiSchemeCode,
		tx.SchemeID,
		tx.SchemeCode,
		tx.ISIN,
		tx.SchemeName,
	)
	return holdingsPositionKey{
		entity: strings.ToLower(strings.TrimSpace(tx.EntityName)),
		folio:  strings.TrimSpace(tx.FolioNumber),
		demat:  strings.TrimSpace(tx.DematNumber),
		scheme: scheme,
	}
}

func canonicalSchemeKey(ids ...string) string {
	for _, id := range ids {
		v := strings.TrimSpace(id)
		if v != "" {
			return strings.ToLower(v)
		}
	}
	return ""
}

func txMatchesHolding(tx PortfolioTxRow, h PortfolioHoldingsRow) bool {
	if !strings.EqualFold(strings.TrimSpace(tx.EntityName), strings.TrimSpace(h.EntityName)) {
		return false
	}
	if !folioDematCompatible(tx.FolioNumber, h.FolioNumber) {
		return false
	}
	if !folioDematCompatible(tx.DematNumber, h.DematAccountNumber) {
		return false
	}
	return schemeIdentifiersMatch(tx, h)
}

func folioDematCompatible(txVal, hVal string) bool {
	txVal = strings.TrimSpace(txVal)
	hVal = strings.TrimSpace(hVal)
	if txVal == "" || hVal == "" {
		return true
	}
	return txVal == hVal
}

func schemeIdentifiersMatch(tx PortfolioTxRow, h PortfolioHoldingsRow) bool {
	txKeys := schemeKeySet(
		tx.AmfiSchemeCode,
		tx.SchemeID,
		tx.SchemeCode,
		tx.ISIN,
		tx.SchemeName,
	)
	hKeys := schemeKeySet(
		h.AmfiSchemeCode,
		h.SchemeID,
		h.ISIN,
		h.SchemeName,
	)
	for k := range txKeys {
		if hKeys[k] {
			return true
		}
	}

	txName := normalizeSchemeName(tx.SchemeName)
	hName := normalizeSchemeName(h.SchemeName)
	return txName != "" && txName == hName
}

func schemeKeySet(ids ...string) map[string]bool {
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		v := strings.TrimSpace(id)
		if v == "" {
			continue
		}
		out[strings.ToLower(v)] = true
	}
	return out
}

func normalizeSchemeName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func portfolioTxSchemeMatch(a, b PortfolioTxRow) bool {
	aKeys := schemeKeySet(a.AmfiSchemeCode, a.SchemeID, a.SchemeCode, a.ISIN, a.SchemeName)
	bKeys := schemeKeySet(b.AmfiSchemeCode, b.SchemeID, b.SchemeCode, b.ISIN, b.SchemeName)
	for k := range aKeys {
		if bKeys[k] {
			return true
		}
	}
	aName := normalizeSchemeName(a.SchemeName)
	bName := normalizeSchemeName(b.SchemeName)
	return aName != "" && aName == bName
}

func onboardMirrorsSuiteRow(onboard PortfolioTxRow, suites []PortfolioTxRow) bool {
	onboardInflow := IsPortfolioTxInflow(onboard)
	for _, suite := range suites {
		if suite.Source != constants.InvestmentSuite && suite.Source != constants.RedemptionSuite {
			continue
		}
		if IsPortfolioTxInflow(suite) != onboardInflow {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(suite.EntityName), strings.TrimSpace(onboard.EntityName)) {
			continue
		}
		if !portfolioTxDatesClose(suite.TransactionDate, onboard.TransactionDate, 14) {
			continue
		}
		if !portfolioTxSchemeMatch(onboard, suite) {
			continue
		}
		if amountsSimilar(onboard.Amount, suite.Amount) {
			return true
		}
	}
	return false
}

func portfolioTxDatesClose(a, b string, maxDays int) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" || b == "" {
		return a == b
	}
	ta, errA := time.Parse("2006-01-02", a)
	tb, errB := time.Parse("2006-01-02", b)
	if errA != nil || errB != nil {
		return a == b
	}
	diff := ta.Sub(tb)
	if diff < 0 {
		diff = -diff
	}
	return diff <= time.Duration(maxDays)*24*time.Hour
}

// dedupePortfolioTxRowsPreferSuite drops onboard/workbench rows only when a matching
// suite row is present in the same result set (safe for display; holdings math unchanged).
func dedupePortfolioTxRowsPreferSuite(rows []PortfolioTxRow) []PortfolioTxRow {
	if len(rows) < 2 {
		return rows
	}
	suiteRows := make([]PortfolioTxRow, 0, len(rows))
	for _, row := range rows {
		if row.Source == constants.InvestmentSuite || row.Source == constants.RedemptionSuite {
			suiteRows = append(suiteRows, row)
		}
	}
	if len(suiteRows) == 0 {
		return dedupePortfolioTxRows(rows)
	}
	out := make([]PortfolioTxRow, 0, len(rows))
	for _, row := range rows {
		if row.TxType == "Onboard" && onboardMirrorsSuiteRow(row, suiteRows) {
			continue
		}
		out = append(out, row)
	}
	return dedupePortfolioTxRows(out)
}

func dedupePortfolioTxRows(rows []PortfolioTxRow) []PortfolioTxRow {
	if len(rows) < 2 {
		return rows
	}
	seen := make(map[string]int, len(rows))
	out := make([]PortfolioTxRow, 0, len(rows))
	for _, row := range rows {
		key := strings.ToLower(strings.TrimSpace(row.EntityName)) + "|" +
			strings.TrimSpace(row.TransactionDate) + "|" +
			strings.ToLower(strings.TrimSpace(row.TransactionType)) + "|" +
			strings.TrimSpace(row.FolioNumber) + "|" +
			canonicalSchemeKey(row.AmfiSchemeCode, row.SchemeID, row.ISIN, row.SchemeName) + "|" +
			strings.TrimSpace(fmt.Sprintf("%.4f|%.4f", row.Amount, row.Units))
		if idx, ok := seen[key]; ok {
			if txSourceRank(row.Source) > txSourceRank(out[idx].Source) {
				out[idx] = row
			}
			continue
		}
		seen[key] = len(out)
		out = append(out, row)
	}
	return out
}

func txSourceRank(source string) int {
	switch strings.TrimSpace(source) {
	case constants.RedemptionSuite:
		return 4
	case constants.InvestmentSuite:
		return 3
	case "Workbench":
		return 2
	case "Holdings Engine":
		return 1
	default:
		return 0
	}
}
