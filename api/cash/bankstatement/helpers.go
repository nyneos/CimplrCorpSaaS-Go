package bankstatement

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/lib/pq"
)

func pqUserFriendlyMessage(err error) string {
	if err == nil {
		return ""
	}
	pqErr, ok := err.(*pq.Error)
	if !ok {
		return err.Error()
	}
	switch pqErr.Code {
	case "23505":
		switch pqErr.Constraint {
		case "uniq_file_hash", "bank_statements_uniq_file_hash", "uniq_file_hash_key":
			return constants.ErrBankStatementFileAlreadyUploaded
		case "uniq_stmt":
			return "A statement for this period is already uploaded for this account."
		default:
			return "A record with the same unique value already exists."
		}
	case "23503":
		return "Some referenced data was not found (please refresh and try again)."
	case "23514":
		return "Some fields have invalid values. Please check and try again."
	default:
		return "Database error while processing the request. Please try again."
	}
}

func ctxHasApprovedBankAccount(ctx context.Context, accountNumber string) bool {
	if strings.TrimSpace(accountNumber) == "" {
		return false
	}
	v := ctx.Value("ApprovedBankAccounts")
	if v == nil {
		return true
	}
	accounts, ok := v.([]map[string]string)
	if !ok {
		return true
	}
	for _, a := range accounts {
		if strings.EqualFold(strings.TrimSpace(a["account_number"]), strings.TrimSpace(accountNumber)) {
			return true
		}
	}
	return false
}

// ctxApprovedCurrencies returns list of allowed currency codes from context (case-insensitive stored as upper)
func ctxApprovedCurrencies(ctx context.Context) []string {
	v := ctx.Value("CurrencyInfo")
	if v == nil {
		v = ctx.Value("ApprovedCurrencies")
	}
	if v == nil {
		return nil
	}
	arr, ok := v.([]map[string]string)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, m := range arr {
		if c, ok := m["currency_code"]; ok {
			c = strings.TrimSpace(c)
			if c != "" {
				out = append(out, strings.ToUpper(c))
			}
		}
	}
	return out
}

// ctxHasApprovedCurrency returns true when no currency restriction is present or currency is allowed
func ctxHasApprovedCurrency(ctx context.Context, currency string) bool {
	currency = strings.TrimSpace(currency)
	if currency == "" {
		return true
	}
	codes := ctxApprovedCurrencies(ctx)
	if len(codes) == 0 {
		return true
	}
	up := strings.ToUpper(currency)
	for _, c := range codes {
		if strings.ToUpper(c) == up {
			return true
		}
	}
	return false
}

// normalizeCell trims, removes non-breaking spaces and collapses whitespace
func normalizeCell(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, constants.NBSP, " ")
	return strings.Join(strings.Fields(s), " ")
}

// sanitizeForPostgres escapes backslashes to prevent PostgreSQL Unicode escape errors
func sanitizeForPostgres(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.ReplaceAll(s, "\\", "/")
	s = strings.ReplaceAll(s, "\x00", "")
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r == 0 {
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

// extractAccountFromCell tries to extract a conservative account-number candidate
// from a single cell string. It returns the digits (no spaces/dashes) or empty.
func extractAccountFromCell(s string) string {
	v := normalizeCell(s)
	acctRe := regexp.MustCompile(`\d{6,}`)
	if m := acctRe.FindString(v); m != "" {
		out := strings.ReplaceAll(m, " ", "")
		out = strings.ReplaceAll(out, "-", "")
		return out
	}
	return ""
}

// allEmptyRow returns true when every cell in the row is empty or whitespace
func allEmptyRow(row []string) bool {
	for _, c := range row {
		if strings.TrimSpace(c) != "" {
			return false
		}
	}
	return true
}

func cleanAmount(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "₹", "")
	s = strings.ReplaceAll(s, "$", "")
	// PDF->CSV providers frequently emit currency codes in amount cells.
	// Remove the common wrappers so strict numeric parsing still works.
	s = strings.ReplaceAll(s, "INR", "")
	s = strings.ReplaceAll(s, "inr", "")
	s = strings.ReplaceAll(s, "Rs.", "")
	s = strings.ReplaceAll(s, "rs.", "")
	s = strings.ReplaceAll(s, "Rs", "")
	s = strings.ReplaceAll(s, "rs", "")
	s = strings.TrimSpace(s)
	// Strip trailing Cr/Dr indicator used by some banks (e.g. ICICI: "53,90,593.75 Cr").
	// For balance columns: "Dr" means overdraft (caller handles sign if needed).
	// For debit/credit amount columns: the indicator is redundant — value is already positive.
	lc := strings.ToLower(s)
	switch {
	case strings.HasSuffix(lc, " cr"):
		s = strings.TrimSpace(s[:len(s)-3])
	case strings.HasSuffix(lc, " dr"):
		s = strings.TrimSpace(s[:len(s)-3])
	case len(s) > 2 && strings.HasSuffix(lc, "cr") && (lc[len(lc)-3] >= '0' && lc[len(lc)-3] <= '9'):
		s = strings.TrimSpace(s[:len(s)-2])
	case len(s) > 2 && strings.HasSuffix(lc, "dr") && (lc[len(lc)-3] >= '0' && lc[len(lc)-3] <= '9'):
		s = strings.TrimSpace(s[:len(s)-2])
	}
	return s
}

// parseStrictAmount parses a cleaned amount string strictly.
// Unlike fmt.Sscanf("%f"), it does NOT accept partial numeric prefixes.
// Returns (value, true) only when the entire cleaned string is numeric.
func parseStrictAmount(raw string) (float64, bool) {
	s := cleanAmount(raw)
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	// Accept only full numeric strings like "-123", "123.45"
	// (no trailing text, no embedded spaces).
	numRe := regexp.MustCompile(`^-?\d+(\.\d+)?$`)
	if !numRe.MatchString(s) {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || !isFiniteNumber(v) {
		return 0, false
	}
	return v, true
}

func isFiniteNumber(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// buildTxnKey creates a stable key used to detect whether a transaction from
// the uploaded file already exists in the database.
func buildTxnKey(accountNumber string, transactionDate time.Time, description string, withdrawal, deposit sql.NullFloat64) string {
	var wStr, dStr string
	if withdrawal.Valid {
		wStr = fmt.Sprintf("%.2f", withdrawal.Float64)
	}
	if deposit.Valid {
		dStr = fmt.Sprintf("%.2f", deposit.Float64)
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s",
		strings.TrimSpace(strings.ToUpper(accountNumber)),
		transactionDate.Format(constants.DateFormat),
		strings.TrimSpace(strings.ToLower(description)),
		wStr,
		dStr,
	)
}

// Broad header synonym lists for heterogeneous bank exports across CSV/XLS/XLSX.
// These are substring tokens checked against normalized lower-case header labels.
var (
	bankStmtDescriptionHeaderAliases = []string{
		"description", "details", "detail", "narration", "narrative", "remarks", "remark",
		"particular", "particulars", "transaction particulars", "transaction details",
		"transaction description", "narr", "memo", "note", "purpose", "payment details",
		"transaction summary", "details of transaction", "event details",

		// Real-world additions
		"txn remarks", "txn narration", "transaction remarks",
		"payment narration", "payment remarks",
		"remittance info", "remittance details",
		"transfer details", "transfer remarks",
		"merchant name", "beneficiary details", "payer details",
		"payee", "beneficiary", "sender", "receiver",
		"info", "additional info", "transaction info",
		"reference details", "instruction details",
		"statement details",
		"transaction text", "booking text", // EU banks
		"posting description",
		"entry description",
	}

	bankStmtDateHeaderAliases = []string{
		"date", "txn date", "trans date", "transaction date", "posting date", "posted date",
		"value date", "val date", "entry date", "book date", "effective date",

		// Additional real-world
		"transaction day", "process date", "processing date",
		"clearing date", "settlement date",
		"execution date", "order date",
		"accounting date", "business date",
		"timestamp", "txn timestamp",
		"date/time", "datetime",
		"operation date",
	}

	bankStmtDebitHeaderAliases = []string{
		"withdrawal", "withdrawals", "debit", "debits", "dr", "debit amount", "withdrawal amount",
		"debit amt", "paid out", "outflow", "money out", "debit value",

		// More variants
		"debit (inr)", "withdrawal (inr)",
		"amount debited", "amount withdrawn",
		"payment", "payments",
		"sent", "transfer out",
		"expense", "expenses",
		"charges", "fees", "bank charges",
		"dr amount", "dr amt",
		"outgoing", "outward",
		"debit txn", "debit transaction",
	}

	bankStmtCreditHeaderAliases = []string{
		"deposit", "deposits", "credit", "credits", "cr", "credit amount", "deposit amount",
		"credit amt", "paid in", "inflow", "money in", "credit value",

		// More variants
		"credit (inr)", "deposit (inr)",
		"amount credited", "amount deposited",
		"received", "received amount",
		"incoming", "inward",
		"transfer in",
		"income", "receipts",
		"cr amount", "cr amt",
		"credit txn", "credit transaction",
	}

	bankStmtBalanceHeaderAliases = []string{
		"balance", "closing balance", "running balance", "available balance", "book balance",
		"new balance", "current balance", "ledger balance", "bal",

		// Additional
		"account balance", "final balance",
		"ending balance", "closing bal",
		"available bal",
		"balance (inr)", "balance amount",
		"post balance", "after transaction balance",
		"remaining balance",
	}

	bankStmtReferenceHeaderAliases = []string{
		"tran id", "transaction id", "txn id", "reference", "reference no", "reference number",
		"ref no", "ref number", "rrn", "utr", "cheque", "chq", "instrument no",

		// More variants
		"cheque no", "check no", "check number",
		"instrument number",
		"transaction reference",
		"payment reference",
		"remittance id",
		"order id",
		"auth code", "authorization code",
		"approval code",
		"trace number",
		"confirmation number",
		"bank reference",
		"external reference",
		"internal reference",
		"voucher number",
		"doc number", "document number",
	}
)

func headerContainsAny(lowerHeader string, tokens []string) bool {
	for _, t := range tokens {
		kwLc := strings.ToLower(t)
		if len(kwLc) <= 3 {
			if lowerHeader == kwLc || strings.HasPrefix(lowerHeader, kwLc+" ") || strings.HasSuffix(lowerHeader, " "+kwLc) || strings.Contains(lowerHeader, " "+kwLc+" ") {
				return true
			}
		} else if strings.Contains(lowerHeader, kwLc) {
			return true
		}
	}
	return false
}

func rowMatrixMaxCols(rows [][]string) int {
	m := 0
	for _, r := range rows {
		if len(r) > m {
			m = len(r)
		}
	}
	return m
}

// parseDelimitedTextRows parses UTF-8 CSV or tab-separated (TSV) bank extracts into [][]string.
//
// Upload/preview use encoding/csv with the default comma. Citibank-style XLS exports and some
// converter outputs are tab-separated; comma parsing then yields one column per row and breaks
// header / debit / credit detection even though the same sheet opens correctly in parseXLSFile.
//
// Heuristic: if the file contains tabs and comma-parsed rows have fewer columns than tab-parsed
// rows (and tab has at least 3 columns), use the tab parse.
func parseDelimitedTextRows(fileBytes []byte) ([][]string, error) {
	read := func(comma rune) ([][]string, error) {
		r := csv.NewReader(bytes.NewReader(fileBytes))
		r.Comma = comma
		r.FieldsPerRecord = -1
		r.LazyQuotes = true
		r.TrimLeadingSpace = true
		return r.ReadAll()
	}

	commaRows, cErr := read(',')
	if cErr != nil {
		tabRows, tErr := read('\t')
		if tErr != nil {
			return nil, cErr
		}
		return tabRows, nil
	}
	if len(commaRows) == 0 {
		return nil, errors.New("file contains no rows")
	}

	if !bytes.Contains(fileBytes, []byte{'\t'}) {
		return commaRows, nil
	}
	commaMax := rowMatrixMaxCols(commaRows)
	// Already looks like a multi-column comma file
	if commaMax >= 4 {
		return commaRows, nil
	}

	tabRows, tErr := read('\t')
	if tErr != nil {
		return commaRows, nil
	}
	tabMax := rowMatrixMaxCols(tabRows)
	if tabMax > commaMax && tabMax >= 3 {
		return tabRows, nil
	}
	return commaRows, nil
}

// nonTxnKeywords lists substrings (lower-case) that mark a row as a footer,
// summary, or informational row — never a real ledger transaction.
// Checked against first cell + description column(s) of each row.
var nonTxnKeywords = []string{
	// General bank footer / informational lines
	"call 1800", "write to us", "toll free", "customer care",
	"please do not share", "computer generated", "does not require",
	"power of attorney",

	// Balance summary rows
	"closing balance", "opening balance",
	"new balance", "new val",
	"avl balance", "available balance", "avl", "avil",

	// Page / statement totals (skipped as they are summation rows, not transactions)
	"page total", "statement total", "grand total",
	"total debit", "total credit", "total withdrawals", "total deposits",
	"cumulative total", "cumulative totals",

	// Statement summary section (SBI / UBI / other banks)
	"statement summary", "brought forward",

	// SBI-specific
	"page no.", "page no ", "powappsrv4",
	"account summary", "statement of account",
	"welcome:", "as on",

	// UBI-specific
	"unless constituent", "discrepancy found",
	"fastest mode", "ifsc/micr",
	"please visit your branch", "inconvenience to",
	"legal heirs", "cancelled by you", "finacle.ubi.com",
	"transaction details", "page :", "page: ",

	// ICICI / generic legal & legend blocks (PDF→CSV often appends these after the txn table)
	"legends for",
	"authenticated intimation", "category of service",
	"regd address", "registered address",
	"team icici", "sincerely",
	"customers are requested", "notify the bank of any discrepancy",
	"miv/st/bank", "banking &financial services",
}

// IsNonTransactionRow checks whether any of the given candidate cell values
// (lower-cased, trimmed) contain a non-transaction keyword. This covers footer,
// summary, informational, and page-break rows across all bank formats.
func IsNonTransactionRow(candidateCells ...string) bool {
	for _, cell := range candidateCells {
		lc := strings.ToLower(strings.TrimSpace(cell))
		if lc == "" {
			continue
		}
		for _, kw := range nonTxnKeywords {
			if strings.Contains(lc, kw) {
				return true
			}
		}
	}
	return false
}

// IsSeparatorRow returns true when the first cell is a line of dashes or
// equals signs (common in UBI / PDF-to-CSV output as visual separators).
func IsSeparatorRow(row []string) bool {
	if len(row) == 0 {
		return false
	}
	first := strings.TrimSpace(row[0])
	if len(first) < 4 {
		return false
	}
	for _, r := range first {
		if r != '-' && r != '=' && r != '_' {
			return false
		}
	}
	return true
}

// IsPageBreakRow detects SBI-style page-break rows where the only meaningful
// content is "Page no." or "Balance" repeated as a column header, and
// mid-file repeated column headers (Date + Transaction/Particulars) from PDF→CSV.
func IsPageBreakRow(row []string) bool {
	if len(row) >= 2 {
		c0 := strings.ToLower(strings.TrimSpace(row[0]))
		c1 := strings.ToLower(strings.TrimSpace(row[1]))
		if c0 == "date" && (strings.Contains(c1, "transaction") || strings.Contains(c1, "particulars") ||
			strings.Contains(c1, "details") || strings.Contains(c1, "narration")) {
			return true
		}
	}
	for _, cell := range row {
		lc := strings.ToLower(strings.TrimSpace(cell))
		if lc == "" {
			continue
		}
		if lc == "page no." || lc == "balance" || strings.HasPrefix(lc, "page ") || lc == "particulars" || lc == "withdrawals" || lc == "deposits" {
			return true
		}
	}
	return false
}

// MergeMultiLineDescriptions collapses continuation rows into their parent
// transaction row. This handles SBI/UBI PDF-to-CSV formats where a single
// transaction's description is split across 2–3 CSV rows.
//
// Detection heuristic:
//   - A "transaction row" has a non-empty cell at dateColIdx that looks like a date.
//   - A "continuation row" has no date and only text in descColIdx.
//   - Continuation text is appended to the preceding transaction row's descColIdx with " ".
//   - Rows that are separators, page breaks, or non-transaction are passed through as-is
//     (the caller will skip them).
//
// The function returns a new slice — the original is not mutated.
func MergeMultiLineDescriptions(rows [][]string, dateColIdx, descColIdx int) [][]string {
	if len(rows) == 0 || dateColIdx < 0 || descColIdx < 0 {
		return rows
	}

	result := make([][]string, 0, len(rows))
	var lastTxnIdx int = -1 // index in result of the last transaction row

	isDateLike := func(s string) bool {
		s = strings.TrimSpace(s)
		if s == "" {
			return false
		}
		// Quick check: contains at least one digit and a separator (/, -, .)
		hasDigit := false
		hasSep := false
		for _, r := range s {
			if r >= '0' && r <= '9' {
				hasDigit = true
			}
			if r == '/' || r == '-' || r == '.' || r == '\u2013' || r == '\u2014' || r == '\u2212' {
				hasSep = true
			}
		}
		return hasDigit && hasSep
	}

	for _, row := range rows {
		// Determine if this row has a date (transaction row) or is a continuation
		hasDate := dateColIdx < len(row) && isDateLike(row[dateColIdx])

		if hasDate {
			// This is a transaction row — copy and append
			rowCopy := make([]string, len(row))
			copy(rowCopy, row)
			result = append(result, rowCopy)
			lastTxnIdx = len(result) - 1
		} else if lastTxnIdx >= 0 {
			// Continuation row — check if there is any text in ANY column
			hasText := false
			isNonTxn := false
			for _, cell := range row {
				txt := strings.TrimSpace(cell)
				if txt != "" && txt != "-" {
					hasText = true
					if IsNonTransactionRow(txt) {
						isNonTxn = true
						break
					}
				}
			}
			joined := strings.TrimSpace(strings.Join(row, " "))
			if joined != "" && IsNonTransactionRow(joined) {
				isNonTxn = true
			}

			if hasText && !IsSeparatorRow(row) && !isNonTxn && !IsPageBreakRow(row) {
				// Merge each column into the corresponding parent column
				for ci := 0; ci < len(row); ci++ {
					if ci == dateColIdx {
						continue // never merge into the date column
					}
					extra := strings.TrimSpace(row[ci])
					if extra != "" && extra != "-" {
						for len(result[lastTxnIdx]) <= ci {
							result[lastTxnIdx] = append(result[lastTxnIdx], "")
						}
						existing := strings.TrimSpace(result[lastTxnIdx][ci])
						if existing != "" {
							result[lastTxnIdx][ci] = existing + " " + extra
						} else {
							result[lastTxnIdx][ci] = extra
						}
					}
				}
			} else if IsPageBreakRow(row) {
				// Repeated column header mid-file — omit so following continuation lines
				// still merge into the prior real transaction (see Indian Bank PDF→CSV).
				continue
			} else {
				// Empty continuation, non-txn, or separator — pass through for caller to skip
				result = append(result, row)
			}
		} else {
			// Non-continuation, non-date row (headers, footers, etc.)
			result = append(result, row)
		}
	}
	return result
}

// IsStatementOpeningCarryRow identifies narration that marks opening/closing carry-over
// (yesterday's closing = today's opening). Banks emit this as a non-transaction line;
// it must seed opening_balance but must not be inserted as a ledger transaction.
func IsStatementOpeningCarryRow(desc string) bool {
	d := strings.ToLower(strings.TrimSpace(desc))
	if d == "" {
		return false
	}
	if strings.Contains(d, "balance brought forward") ||
		strings.Contains(d, "balance carried forward") ||
		strings.Contains(d, "brought forward balance") ||
		strings.Contains(d, "carried forward balance") {
		return true
	}
	if strings.Contains(d, "balance b/f") || strings.Contains(d, "balance c/f") ||
		strings.Contains(d, "bal b/f") || strings.Contains(d, "bal c/f") ||
		strings.Contains(d, "bal. b/f") || strings.Contains(d, "bal. c/f") {
		return true
	}
	if strings.Contains(d, "brought forward") || strings.Contains(d, "carried forward") {
		return true
	}
	// Short labels (often first line of statement body)
	if d == "b/f" || d == "b.f." || d == "bf" || d == "c/f" || d == "c.f." || d == "cf" {
		return true
	}
	if strings.HasPrefix(d, "b/f ") || strings.HasPrefix(d, "c/f ") ||
		strings.HasPrefix(d, "b.f ") || strings.HasPrefix(d, "c.f ") {
		return true
	}
	if strings.Contains(d, "bbf") || strings.Contains(d, "b.b.f") {
		return true
	}
	return false
}

// userFriendlyUploadError converts internal/SQL/Go errors into messages that are safe and easy
// for end users to understand.
func userFriendlyUploadError(err error) string {
	if err == nil {
		return ""
	}

	if errors.Is(err, ErrFileAlreadyUploaded) {
		return constants.ErrBankStatementFileAlreadyUploaded
	}
	if errors.Is(err, ErrAccountNumberMissing) {
		return "Could not find the bank account number in the uploaded statement. Please upload the original bank statement downloaded from the bank."
	}
	if errors.Is(err, ErrAccountNotFound) {
		return "Bank account not found in the system for this statement. Please check the account number in master data."
	}
	if errors.Is(err, ErrStatementPeriodExists) {
		return "A statement for this period is already uploaded for this account."
	}
	if errors.Is(err, ErrAllTransactionsDuplicate) {
		return "This statement has already been uploaded. All transactions in this statement already exist in the system."
	}

	msg := err.Error()

	if strings.Contains(msg, "not found in master data") {
		return "Bank account not found in the system for this statement. Please check the account number in master data."
	}
	if strings.Contains(msg, "convert:") || strings.Contains(msg, "decode response:") {
		return "PDF conversion service returned an error. Please try again in a few minutes, or contact support if this keeps happening."
	}
	if strings.Contains(msg, "transaction header row not found") {
		return "Could not detect the transactions table in the statement. Please upload the original bank statement in the supported format (Excel/XLS/CSV)."
	}
	if strings.Contains(msg, "must have at least one data row") {
		return "The uploaded statement does not contain any transactions."
	}
	if strings.Contains(msg, "failed to parse excel, xls, or csv") || strings.Contains(msg, "failed to parse excel") {
		return "We could not read this file as a valid Excel/XLS/CSV bank statement. Please check the file format and try again."
	}
	if strings.Contains(msg, "failed to get rows") {
		return "We could not read rows from the uploaded statement. The file may be corrupted or in an unsupported format."
	}
	if strings.Contains(msg, "could not parse date") {
		return "One or more transaction dates in the statement could not be understood. Please verify the dates in the statement and try again."
	}
	// Also catch pq uniq_stmt wrapped inside fmt.Errorf chains
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		switch pqErr.Constraint {
		case "uniq_stmt":
			return "A statement for this account and period is already uploaded. Use force_override=true to re-upload."
		case "uniq_file_hash", "bank_statements_uniq_file_hash", "uniq_file_hash_key":
			return constants.ErrBankStatementFileAlreadyUploaded
		}
	}
	// 22003: numeric_value_out_of_range (e.g. "numeric field overflow")
	if errors.As(err, &pqErr) && pqErr.Code == "22003" {
		return "Some transaction amounts/balances in this statement are too large to be saved. Please try uploading the Excel version, or contact support with this file."
	}
	if strings.Contains(msg, "failed to begin db transaction") ||
		strings.Contains(msg, "failed to insert bank statement") ||
		strings.Contains(msg, "failed to upsert bank_balances_manual") ||
		strings.Contains(msg, "failed to bulk insert transactions") ||
		strings.Contains(msg, "failed to insert audit action") ||
		strings.Contains(msg, constants.ErrTxCommitFailed) {
		return "Something went wrong while saving the uploaded statement. Please try again !!"
	}

	if strings.Contains(msg, "pq:") || strings.Contains(msg, "SQLSTATE") {
		return "Database error while processing the bank statement. Please try again !!"
	}

	// Generic staging/preview wrapper errors — keep last so more specific matchers win.
	if strings.Contains(msg, "build preview:") || strings.Contains(msg, "stage statement:") {
		return "We could not finish preparing this PDF for review. Please try again with the original bank PDF, or upload CSV/Excel if the problem continues."
	}

	return msg
}

// isTransientPQProtocolError returns true for lib/pq protocol desync errors that are usually
// fixed by retrying on a fresh connection from the pool.
func isTransientPQProtocolError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Observed in logs: "pq: unexpected message 'E'; expected ReadyForQuery"
	if strings.Contains(msg, "unexpected message") && strings.Contains(msg, "readyforquery") {
		return true
	}
	return false
}

// joinStrings is a helper for bulk insert value string joining
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	out := strs[0]
	for _, s := range strs[1:] {
		out += sep + s
	}
	return out
}

// generateBatchID returns a random 6-character uppercase alphanumeric string that acts as a
// unique prefix for all synthetic tran_ids in one statement upload.  Using crypto/rand keeps
// the prefix collision probability negligible even across millions of re-uploads.
// Character space: A-Z + 0-9 (36 chars) → 36^6 ≈ 2.18 billion unique prefixes.
func generateBatchID() string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	raw := make([]byte, 6)
	if _, err := rand.Read(raw); err != nil {
		// Extremely unlikely; produce a time-seeded fallback so we never return ""
		t := time.Now().UnixNano()
		for i := range raw {
			raw[i] = chars[int(t>>uint(i*8))%len(chars)]
		}
		return string(raw)
	}
	out := make([]byte, 6)
	for i, v := range raw {
		out[i] = chars[int(v)%len(chars)]
	}
	return string(out)
}

// buildSyntheticTranID creates a compact, sequential transaction identifier.
//
// Format: {batchID}{seq7}  (total 13 characters)
// Examples:
//   - "A3B9X20000001" — first row of a batch with prefix A3B9X2
//   - "A3B9X20000008" — eighth row of the same batch
//
// batchID is generated once per upload via generateBatchID().
// seq is the raw row number within the statement — supports up to 9,999,999 rows (10M+).
func buildSyntheticTranID(batchID string, seq int) string {
	return fmt.Sprintf("%s%07d", batchID, seq)
}
