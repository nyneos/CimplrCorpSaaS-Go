package bankstatement

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
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
			return "This bank statement file was already uploaded earlier. Please upload a different file."
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
	s = strings.ReplaceAll(s, ",", "")
	return strings.TrimSpace(s)
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

// userFriendlyUploadError converts internal/SQL/Go errors into messages that are safe and easy
// for end users to understand.
func userFriendlyUploadError(err error) string {
	if err == nil {
		return ""
	}

	if errors.Is(err, ErrFileAlreadyUploaded) {
		return "This bank statement file was already uploaded earlier. Please upload a different file."
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
			return "This bank statement file was already uploaded earlier. Please upload a different file."
		}
	}
	log.Println("Debug raw mesage", msg)
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

	return msg
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
