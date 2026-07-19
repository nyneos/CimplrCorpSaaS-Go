package bankstatement

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
)

var (
	filenameBranchDateRe = regexp.MustCompile(`_[A-Z]{3}_(\d{8})_M`)
	filenameEightDigitRe = regexp.MustCompile(`(\d{8})`)
)

// StatementBalanceSummary holds balance/as-of metadata from footer or summary rows.
type StatementBalanceSummary struct {
	Balance  float64
	AsOfDate time.Time
	Found    bool
}

var balanceSummaryPhrases = []string{
	"avl balance", "available balance",
	"closing balance", "closing avail",
	"ledger balance", "book balance",
	"new balance", "new val",
	"balance as on", "balance as at",
	"statement balance",
}

func isBalanceSummaryRow(text string) bool {
	lc := strings.ToLower(strings.TrimSpace(text))
	if lc == "" {
		return false
	}
	for _, kw := range balanceSummaryPhrases {
		if strings.Contains(lc, kw) {
			return true
		}
	}
	return false
}

func rowSummaryText(row []string) string {
	parts := make([]string, 0, len(row))
	for _, cell := range row {
		if t := strings.TrimSpace(cell); t != "" {
			parts = append(parts, t)
		}
	}
	return strings.Join(parts, " | ")
}

func rowSummaryAmount(row []string) (float64, bool) {
	for _, cell := range row {
		if v, ok := parseAmountNonZero(cell); ok {
			return v, true
		}
		if v, err := parseAmount(cleanAmount(cell)); err == nil && v != 0 {
			return v, true
		}
	}
	return 0, false
}

func parseDateFromSummaryText(text string) (time.Time, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return time.Time{}, false
	}
	for _, part := range strings.Split(text, "||") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if t, err := parseDate(part); err == nil && !t.IsZero() {
			return t, true
		}
	}
	for _, part := range strings.FieldsFunc(text, func(r rune) bool {
		return r == '|' || r == ','
	}) {
		part = strings.TrimSpace(part)
		if t, err := parseDate(part); err == nil && !t.IsZero() {
			return t, true
		}
	}
	if t, err := parseDate(text); err == nil && !t.IsZero() {
		return t, true
	}
	return time.Time{}, false
}

func parseDDMMYYYY(s string) (time.Time, bool) {
	if len(s) != 8 {
		return time.Time{}, false
	}
	t, err := time.Parse("02012006", s)
	if err != nil {
		return time.Time{}, false
	}
	if t.Year() < 1990 || t.Year() > 2100 {
		return time.Time{}, false
	}
	return t, true
}

// parseFilenameStatementDate extracts DDMMYYYY from common bank export filenames.
func parseFilenameStatementDate(filename string) (time.Time, bool) {
	upper := strings.ToUpper(filepath.Base(filename))
	if m := filenameBranchDateRe.FindStringSubmatch(upper); len(m) >= 2 {
		if t, ok := parseDDMMYYYY(m[1]); ok {
			return t, true
		}
	}
	matches := filenameEightDigitRe.FindAllString(upper, -1)
	for i := len(matches) - 1; i >= 0; i-- {
		if t, ok := parseDDMMYYYY(matches[i]); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

func findTransactionHeaderRow(rows [][]string) int {
	for i, row := range rows {
		hasDate := false
		hasDesc := false
		hasAmountCols := false
		for _, cell := range row {
			lc := strings.ToLower(strings.TrimSpace(cell))
			if headerContainsAny(lc, bankStmtDateHeaderAliases) {
				hasDate = true
			}
			if headerContainsAny(lc, bankStmtDescriptionHeaderAliases) {
				hasDesc = true
			}
			if headerContainsAny(lc, bankStmtDebitHeaderAliases) || headerContainsAny(lc, bankStmtCreditHeaderAliases) {
				hasAmountCols = true
			}
		}
		if hasDate && (hasDesc || hasAmountCols) {
			return i
		}
	}
	return -1
}

func parseUploadRowsFromBytes(fileBytes []byte, filename string) ([][]string, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".xlsx":
		return parseExcelFile(fileBytes)
	case ".xls":
		return parseXLSFile(fileBytes)
	case ".csv":
		return parseCSVFile(fileBytes)
	default:
		if rows, err := parseCSVFile(fileBytes); err == nil && len(rows) >= 2 {
			return rows, nil
		}
		if rows, err := parseXLSFile(fileBytes); err == nil && len(rows) >= 2 {
			return rows, nil
		}
		if rows, err := parseExcelFile(fileBytes); err == nil && len(rows) >= 2 {
			return rows, nil
		}
		return nil, fmt.Errorf("unsupported file format: %s", ext)
	}
}

// extractStatementBalanceSummary reads footer/summary rows when a file has no transactions.
func extractStatementBalanceSummary(rows [][]string, headerIdx int, filename string) StatementBalanceSummary {
	if headerIdx < 0 || headerIdx >= len(rows) {
		return StatementBalanceSummary{}
	}

	var summary StatementBalanceSummary
	for i := headerIdx + 1; i < len(rows); i++ {
		row := rows[i]
		if len(row) == 0 || strings.TrimSpace(strings.Join(row, "")) == "" {
			continue
		}
		text := rowSummaryText(row)
		if !isBalanceSummaryRow(text) {
			continue
		}
		if amt, ok := rowSummaryAmount(row); ok {
			summary.Balance = amt
			summary.Found = true
		}
		if t, ok := parseDateFromSummaryText(text); ok {
			summary.AsOfDate = t
			summary.Found = true
		}
	}

	if summary.AsOfDate.IsZero() {
		if t, ok := parseFilenameStatementDate(filename); ok {
			summary.AsOfDate = t
			summary.Found = true
		}
	}

	return summary
}

func applyStatementBalanceSummary(summary StatementBalanceSummary, openingBalance, closingBalance float64, periodStart, periodEnd time.Time) (float64, float64, time.Time, time.Time) {
	if !summary.Found {
		return openingBalance, closingBalance, periodStart, periodEnd
	}
	if summary.Balance != 0 {
		openingBalance = summary.Balance
		closingBalance = summary.Balance
	}
	if !summary.AsOfDate.IsZero() {
		periodEnd = summary.AsOfDate
		periodStart = time.Date(summary.AsOfDate.Year(), summary.AsOfDate.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	return openingBalance, closingBalance, periodStart, periodEnd
}

func isSummaryOnlyCleanData(clean RecalculateCleanData) bool {
	if len(clean.Transactions) > 0 {
		return false
	}
	hasPeriod := clean.Metadata.PeriodStart != nil && strings.TrimSpace(*clean.Metadata.PeriodStart) != "" &&
		clean.Metadata.PeriodEnd != nil && strings.TrimSpace(*clean.Metadata.PeriodEnd) != ""
	hasBalance := (clean.OpeningBalance != nil && *clean.OpeningBalance != 0) ||
		(clean.Metadata.ClosingBalance != nil && *clean.Metadata.ClosingBalance != 0) ||
		(clean.Metadata.OpeningBalance != nil && *clean.Metadata.OpeningBalance != 0)
	return hasPeriod && hasBalance
}

func buildPreviewResponseFromSummary(summary StatementBalanceSummary, fileBytes []byte, filename, accountOverride string) map[string]interface{} {
	opening, closing, start, end := applyStatementBalanceSummary(summary, 0, 0, time.Time{}, time.Time{})

	accountNumber := strings.TrimSpace(accountOverride)
	if accountNumber == "" {
		if rows, err := parseUploadRowsFromBytes(fileBytes, filename); err == nil {
			accountNumber = extractAccountFromRows(rows, filename)
		}
	}

	periodStartStr, periodEndStr := "", ""
	if !start.IsZero() {
		periodStartStr = start.Format(constants.DateFormat)
	}
	if !end.IsZero() {
		periodEndStr = end.Format(constants.DateFormat)
	}

	openingPtr := &opening
	closingPtr := &closing
	meta := Metadata{
		AccountNumber:  strPtr(accountNumber),
		PeriodStart:    strPtr(periodStartStr),
		PeriodEnd:      strPtr(periodEndStr),
		OpeningBalance: openingPtr,
		ClosingBalance: closingPtr,
	}

	return map[string]interface{}{
		"clean": RecalculateCleanData{
			Metadata:       meta,
			OpeningBalance: openingPtr,
			Transactions:   []RecalculateTransaction{},
		},
		"status":            "complete",
		"summary_only":      true,
		"transaction_count": 0,
	}
}

func tryBuildSummaryOnlyPreview(fileBytes []byte, filename, accountOverride string) (map[string]interface{}, bool, error) {
	rows, err := parseUploadRowsFromBytes(fileBytes, filename)
	if err != nil {
		return nil, false, err
	}
	if len(rows) < 2 {
		return nil, false, nil
	}
	hdr := findTransactionHeaderRow(rows)
	if hdr < 0 {
		return nil, false, nil
	}
	summary := extractStatementBalanceSummary(rows, hdr, filename)
	if !summary.Found {
		return nil, false, nil
	}
	return buildPreviewResponseFromSummary(summary, fileBytes, filename, accountOverride), true, nil
}

func resolveEmptyUploadFromSummary(rows [][]string, headerIdx int, filename string, openingBalance, closingBalance float64, periodStart, periodEnd time.Time) (float64, float64, time.Time, time.Time, error) {
	summary := extractStatementBalanceSummary(rows, headerIdx, filename)
	if !summary.Found {
		return openingBalance, closingBalance, periodStart, periodEnd, ErrStatementSummaryOnly
	}
	openingBalance, closingBalance, periodStart, periodEnd = applyStatementBalanceSummary(
		summary, openingBalance, closingBalance, periodStart, periodEnd,
	)
	return openingBalance, closingBalance, periodStart, periodEnd, nil
}

// extractAccountFromRows is a lightweight account resolver for summary-only previews.
func extractAccountFromRows(rows [][]string, filename string) string {
	for i := 0; i < len(rows) && i < 25; i++ {
		for j, cell := range rows[i] {
			nc := normalizeCell(cell)
			if strings.Contains(strings.ToLower(nc), "account") && j+1 < len(rows[i]) {
				if v := extractAccountFromCell(rows[i][j+1]); v != "" {
					return v
				}
			}
		}
	}
	if cands := extractAccountCandidatesFromFilename(filename); len(cands) > 0 {
		return cands[0]
	}
	return ""
}
