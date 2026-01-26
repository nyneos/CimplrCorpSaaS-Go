package bankstatement

import (
	apictx "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"
	"bytes"
	"context"

	// "regexp"
	"log"
	"path/filepath"

	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
    "sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lib/pq"
	"github.com/shakinm/xlsReader/xls"
	"github.com/xuri/excelize/v2"
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
		// uniq_file_hash / uniq_stmt / other unique constraints
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
	// PostgreSQL standard_conforming_strings=on mode requires backslashes to be escaped.
	// Replace actual newlines, tabs, carriage returns, and backslashes with safe characters.
	s = strings.ReplaceAll(s, "\n", " ") // actual newline character (LF)
	s = strings.ReplaceAll(s, "\r", " ") // actual carriage return (CR)
	s = strings.ReplaceAll(s, "\t", " ") // actual tab character
	s = strings.ReplaceAll(s, "\\", "/") // backslash to forward slash
	// Remove NUL bytes which cause PostgreSQL UTF8 errors (0x00)
	s = strings.ReplaceAll(s, "\x00", "")
	// As an extra safeguard remove any remaining rune 0 characters
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
	// conservative digit run detector: at least 6 digits
	acctNumberRe := regexp.MustCompile(`\d{6,}`)
	if m := acctNumberRe.FindString(v); m != "" {
		out := strings.ReplaceAll(m, " ", "")
		out = strings.ReplaceAll(out, "-", "")
		return out
	}
	return ""
}

// Sentinel errors used for mapping to user-friendly messages
var (
	ErrFileAlreadyUploaded      = errors.New("bank statement file already uploaded")
	ErrAccountNotFound          = errors.New("bank account not found in master data")
	ErrAccountNumberMissing     = errors.New("account number not found in file header")
	ErrStatementPeriodExists    = errors.New("a statement for this period already exists for this account")
	ErrAllTransactionsDuplicate = errors.New("all transactions in this statement already exist")
)

var (
	// precompiled regexes for fallback extraction
	acctLabelRe  = regexp.MustCompile(`(?i)\b(?:a[/\\]?c|acct|account)[\s\.:#-]*(?:no|number)?\b`)
	acctNumberRe = regexp.MustCompile(`\d{7,}`) // prefer >=7 digits to avoid postal-code false positives
)
var (
	acNoHeader                      = "A/C No:"
	slNoHeader                      = "Sl. No."
	tranIDHeader                    = "Tran. Id"
	valueDateHeader                 = "Value Date"
	transactionDateHeader           = "Transaction Date"
	transactionRemarksHeader        = "Transaction Remarks"
	withdrawalAmtHeader             = "Withdrawal Amt (INR)"
	depositAmtHeader                = "Deposit Amt (INR)"
	balanceHeader                   = "Balance (INR)"
	missingUserIDOrBankStatementIDs = "Missing user_id or bank_statement_ids"
)

const (
	bankStmtDefaultBucket  = "cimplr"
	bankStmtPrefix         = "bankstatements/"
	bankStmtDefaultRegion  = "ap-south-1"
	bankStmtDefaultBaseURL = "https://cimplr.s3.ap-south-1.amazonaws.com/"
)

func bankStmtBucket() string {
	if b := strings.TrimSpace(os.Getenv("BANK_STMT_S3_BUCKET")); b != "" {
		return b
	}
	return bankStmtDefaultBucket
}

func bankStmtRegion() string {
	if r := strings.TrimSpace(os.Getenv("BANK_STMT_S3_REGION")); r != "" {
		return r
	}
	return bankStmtDefaultRegion
}

func bankStmtBaseURL() string {
	if u := strings.TrimSpace(os.Getenv("BANK_STMT_S3_BASE_URL")); u != "" {
		u = strings.TrimSuffix(u, "/")
		return u + "/"
	}
	return bankStmtDefaultBaseURL
}

func sanitizePathSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(" ", "_", "/", "_", "\\", "_")
	return replacer.Replace(s)
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

func buildBankStatementS3Key(accountNumber, fileHash, fileExt string) string {
	ext := strings.TrimSpace(fileExt)
	if ext == "" {
		ext = ".bin"
	}
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	acct := sanitizePathSegment(accountNumber)
	return fmt.Sprintf("%s%s/%s%s", bankStmtPrefix, acct, fileHash, ext)
}

// isS3Enabled reads env var BANK_STMT_S3_ENABLED to determine whether to
// upload original bank-statement files to S3. Defaults to true when unset.
func isS3Enabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("BANK_STMT_S3_ENABLED")))
	if v == "" {
		return true
	}
	return v == "1" || v == "true" || v == "yes"
}

func detectContentType(data []byte) string {
	if len(data) == 0 {
		return "application/octet-stream"
	}
	if len(data) > 512 {
		return http.DetectContentType(data[:512])
	}
	return http.DetectContentType(data)
}

func uploadBankStatementToS3(ctx context.Context, key string, body []byte, contentType string) (string, error) {
	bucket := bankStmtBucket()
	region := bankStmtRegion()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return "", fmt.Errorf("load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return "", fmt.Errorf("upload to s3 (bucket %s, key %s): %w", bucket, key, err)
	}
	return bankStmtBaseURL() + key, nil
}

// categoryRuleComponent represents a single rule component used for
// categorizing transactions. This is shared between the upload and recompute
// flows so that both use identical matching logic.
type categoryRuleComponent struct {
	RuleID         int64
	Priority       int
	CategoryID     int64
	CategoryName   string
	CategoryType   string
	ComponentType  string
	MatchType      sql.NullString
	MatchValue     sql.NullString
	AmountOperator sql.NullString
	AmountValue    sql.NullFloat64
	TxnFlow        sql.NullString
	CurrencyCode   sql.NullString
}

// loadCategoryRuleComponents fetches all active rule components for a given
// account/entity/bank/currency/global scope, ordered by rule priority. This mirrors the
// logic originally present in the upload handler so that recompute can reuse
// the same rules.
func loadCategoryRuleComponents(ctx context.Context, db *sql.DB, accountNumber, entityID, currencyCode string) ([]categoryRuleComponent, error) {
	q := `
	       SELECT r.rule_id, r.priority, r.category_id, c.category_name, c.category_type, comp.component_type, comp.match_type, comp.match_value, comp.amount_operator, comp.amount_value, comp.txn_flow, comp.currency_code
	       FROM cimplrcorpsaas.category_rules r
	       JOIN cimplrcorpsaas.transaction_categories c ON r.category_id = c.category_id
	       JOIN cimplrcorpsaas.category_rule_components comp ON r.rule_id = comp.rule_id AND comp.is_active = true
	       JOIN cimplrcorpsaas.rule_scope s ON r.scope_id = s.scope_id
	       WHERE r.is_active = true
		 AND (
		       (s.scope_type = 'ACCOUNT' AND s.account_number = $1)
		       OR (s.scope_type = 'ENTITY' AND s.entity_id = $2)
		       OR (s.scope_type = 'BANK' AND s.bank_code IS NOT NULL)
		       OR (s.scope_type = 'CURRENCY' AND s.currency = $3)
		       OR (s.scope_type = 'GLOBAL')
		 )
	       ORDER BY r.priority ASC, r.rule_id ASC, comp.component_id ASC
	   `

	rowsRule, err := db.QueryContext(ctx, q, accountNumber, entityID, currencyCode)
	if err != nil {
		return nil, err
	}
	defer rowsRule.Close()

	rules := []categoryRuleComponent{}
	for rowsRule.Next() {
		var rc categoryRuleComponent
		if err := rowsRule.Scan(&rc.RuleID, &rc.Priority, &rc.CategoryID, &rc.CategoryName, &rc.CategoryType, &rc.ComponentType, &rc.MatchType, &rc.MatchValue, &rc.AmountOperator, &rc.AmountValue, &rc.TxnFlow, &rc.CurrencyCode); err != nil {
			return nil, err
		}
		rules = append(rules, rc)
	}
	if err := rowsRule.Err(); err != nil {
		return nil, err
	}
	return rules, nil
}

// matchCategoryForTransaction applies the rule components to a single
// transaction and returns the matched category_id, if any. The logic here is
// exactly the same as what is used during upload so that recompute produces
// consistent results when new rules are added.
func matchCategoryForTransaction(rules []categoryRuleComponent, description string, withdrawal, deposit sql.NullFloat64) sql.NullInt64 {
	matchedCategoryID := sql.NullInt64{Valid: false}
	descLower := strings.ToLower(description)
	for _, rule := range rules {
		// NARRATION LOGIC
		if rule.ComponentType == "NARRATION_LOGIC" && rule.MatchType.Valid && rule.MatchValue.Valid {
			val := strings.ToLower(rule.MatchValue.String)
			switch rule.MatchType.String {
			case "CONTAINS", "ILIKE":
				if strings.Contains(descLower, val) {
					matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
				}
			case "EQUALS":
				if descLower == val {
					matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
				}
			case "STARTS_WITH":
				if strings.HasPrefix(descLower, val) {
					matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
				}
			case "ENDS_WITH":
				if strings.HasSuffix(descLower, val) {
					matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
				}

			case "REGEX":
				// Regex not implemented in original logic
			}
		}
		// AMOUNT LOGIC (applies to both withdrawal and deposit)
		if !matchedCategoryID.Valid && rule.ComponentType == "AMOUNT_LOGIC" && rule.AmountOperator.Valid && rule.AmountValue.Valid {
			amounts := []float64{}
			if withdrawal.Valid {
				amounts = append(amounts, withdrawal.Float64)
			}
			if deposit.Valid {
				amounts = append(amounts, deposit.Float64)
			}
			for _, amt := range amounts {
				switch rule.AmountOperator.String {
				case ">":
					if amt > rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
					}
				case ">=":
					if amt >= rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
					}
				case "=":
					if amt == rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
					}
				case "<=":
					if amt <= rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
					}
				case "<":
					if amt < rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
					}
				}
			}
		}
		// TRANSACTION LOGIC (DEBIT/CREDIT)
		if !matchedCategoryID.Valid && rule.ComponentType == "TRANSACTION_LOGIC" && rule.TxnFlow.Valid {
			if rule.TxnFlow.String == "DEBIT" && withdrawal.Valid && withdrawal.Float64 > 0 {
				matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
			}
			if rule.TxnFlow.String == "CREDIT" && deposit.Valid && deposit.Float64 > 0 {
				matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
			}
		}
		// CURRENCY_CONDITION and other component types are ignored here because
		// the original upload logic didn't implement them either.
		if matchedCategoryID.Valid {
			break
		}
	}
	return matchedCategoryID
}

// parseDate tries multiple date formats for CSV
func parseDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	// Prefer dd/mm/yyyy for bank statements before falling back to the broader parser set.
	if t, err := time.Parse("02/01/2006", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2/1/2006", s); err == nil {
		return t, nil
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty date string")
	}
	// Critical: dd/mm/yyyy formats MUST come before mm/dd/yyyy to prevent misparsing Indian bank statements
	layouts := []string{
		// dd/mm/yyyy variants (Indian/European format) - MUST BE FIRST
		"02/01/2006", "02/01/06", "2/1/2006", "2/1/06",
		"02/01/2006 03:04:05 PM", "02/01/06 03:04:05 PM", "2/1/2006 03:04:05 PM", "2/1/06 03:04:05 PM",
		"02/01/2006 3:04:05 PM", "02/01/06 3:04:05 PM", "2/1/2006 3:04:05 PM", "2/1/06 3:04:05 PM",
		"02/01/06 15:04", "02/01/06 3:04", "02/01/06 15:04:05", "02/01/06 3:04:05",
		"2/1/06 15:04", "2/1/06 3:04", "2/1/06 15:04:05", "2/1/06 3:04:05",
		// mm/dd/yyyy variants (American format) - AFTER dd/mm/yyyy
		"01/02/2006", "01/02/06", "1/2/2006", "1/2/06",
		"01/02/2006 03:04:05 PM", "01/02/2006 03:04 PM", "01/02/06 03:04:05 PM", "01/02/06 03:04 PM",
		"1/2/2006 03:04:05 PM", "1/2/2006 03:04 PM", "1/2/06 03:04:05 PM", "1/2/06 03:04 PM",
		"01/02/06 15:04", "01/02/06 3:04", "01/02/06 15:04:05", "01/02/06 3:04:05",
		"1/2/06 15:04", "1/2/06 3:04", "1/2/06 15:04:05", "1/2/06 3:04:05",
		// Named month formats
		constants.DateFormatSlash, constants.DateFormatDash, // for 29/Aug/2025 and 29-Aug-2025
		"2-Jan-2006", "1/Feb/2006",
		// ISO and other formats
		constants.DateFormat, "2006/01/02", "2006.01.02", "01.02.2006", "1.2.2006", "01-02-2006", "1-2-2006",
		"01-02-06", "1-2-06", "2006/1/2", "2006-1-2",
		// dd-Mon-yy and dd/Mon/yy variants
		"02-Jan-06", "02-Jan-2006", "02/Jan/06", "02/Jan/2006",
		"02-Jan-06 15:04", "02-Jan-2006 15:04", "02-Jan-06 3:04", "02-Jan-2006 3:04",
		"02-Jan-06 15:04:05", "02-Jan-2006 15:04:05", "02-Jan-06 3:04:05", "02-Jan-2006 3:04:05",
		"02/Jan/06 15:04", "02/Jan/2006 15:04", "02/Jan/06 3:04", "02/Jan/2006 3:04",
		"02/Jan/06 15:04:05", "02/Jan/2006 15:04:05", "02/Jan/06 3:04:05", "02/Jan/2006 3:04:05",
		"02-Jan-2006 03:04:05 PM", "02-Jan-06 03:04:05 PM", "02-Jan-2006 3:04:05 PM", "02-Jan-06 3:04:05 PM",
		"02/Jan/2006 03:04:05 PM", "02/Jan/06 03:04:05 PM", "02/Jan/2006 3:04:05 PM", "02/Jan/06 3:04:05 PM",
		// dd-Mon-yy variants (American style)
		"01-Feb-06", "01-Feb-2006", "01/Feb/06", "01/Feb/2006",
		"01-Feb-06 15:04", "01-Feb-2006 15:04", "01-Feb-06 3:04", "01-Feb-2006 3:04",
		"01-Feb-06 15:04:05", "01-Feb-2006 15:04:05", "01-Feb-06 3:04:05", "01-Feb-2006 3:04:05",
		"01/Feb/06 15:04", "01/Feb/2006 15:04", "01/Feb/06 3:04", "01/Feb/2006 3:04",
		"01/Feb/06 15:04:05", "01/Feb/2006 15:04:05", "01/Feb/06 3:04:05", "01/Feb/2006 3:04:05",
		// ISO-ish layouts to catch Excel exports that already render as 2026-01-15 or RFC3339 strings
		constants.DateFormat, "2006-01-02 15:04:05", time.RFC3339, "2006-01-02T15:04:05", "2006-01-02T15:04",
	}
	// Try all layouts
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	// Try to parse with 2-digit year fallback (e.g., 13-Dec-25 as 2025)
	if len(s) == 9 && s[2] == '-' && s[6] == '-' { // e.g., 13-Dec-25
		t, err := time.Parse("02-Jan-06", s)
		if err == nil {
			// If year < 100, add 2000
			y := t.Year()
			if y < 100 {
				t = t.AddDate(2000, 0, 0)
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse date: %s", s)
}

// parseDateDDMMFirst is a convenience wrapper that simply delegates to parseDate,
// which already prefers dd/mm/yyyy layouts.
func parseDateDDMMFirst(s string) (time.Time, error) {
	return parseDate(s)
}

// tryParseDateWithExcelSerial first attempts normal string parsing, then falls back to
// Excel serial date numbers (days since 1899-12-30 with the Excel 1900 leap-year bug).
func tryParseDateWithExcelSerial(s string) time.Time {
	if t, err := parseDateDDMMFirst(s); err == nil && !t.IsZero() {
		return t
	}
	if t, err := parseExcelSerialDate(s); err == nil {
		return t
	}
	return time.Time{}
}

// parseExcelSerialDate converts an Excel serial date (possibly with fractional day time)
// into a time.Time. Excel counts from 1899-12-30 and includes a fake 1900-02-29 day.
func parseExcelSerialDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty excel serial")
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}, err
	}
	days := int(f)
	frac := f - float64(days)
	if days > 59 { // Excel leap-year bug adjustment (skips nonexistent 1900-02-29)
		days--
	}
	base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
	d := base.AddDate(0, 0, days)
	d = d.Add(time.Duration(frac * float64(24*time.Hour)))
	return d, nil
}

// UploadBankStatementV2WithCategorization wraps UploadBankStatementV2 and adds category intelligence and KPIs to the response.

func UploadBankStatementV2WithCategorization(ctx context.Context, db *sql.DB, file multipart.File, fileHash string) (map[string]interface{}, error) {
	// 1. Idempotency: Check if file hash already exists
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM cimplrcorpsaas.bank_statements WHERE file_hash = $1)`, fileHash).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check file hash: %w", err)
	}
	if exists {
		return nil, ErrFileAlreadyUploaded
	}

	// 2. Parse Excel, XLS, or CSV file
	tmpFile, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	contentType := detectContentType(tmpFile)

	var rows [][]string
	var isCSV bool
	fileExt := ".xlsx"

	//var isXLS bool Try Excel first
	xl, xlErr := excelize.OpenReader(bytes.NewReader(tmpFile))
	if xlErr == nil {
		defer xl.Close()
		sheetName := xl.GetSheetName(0)
		// Use GetCellValue instead of GetRows to properly handle formulas and formatting
		rawRows, err := xl.GetRows(sheetName)
		if err != nil {
			return nil, fmt.Errorf("failed to get rows: %w", err)
		}
		if len(rawRows) < 2 {
			return nil, errors.New("excel must have at least one data row")
		}

		// Re-read each cell using GetCellValue to evaluate formulas and handle special formatting
		rows = make([][]string, len(rawRows))
		for i, rawRow := range rawRows {
			rows[i] = make([]string, len(rawRow))
			for j := range rawRow {
				// Convert column index to Excel column name (0->A, 1->B, etc.)
				colName, _ := excelize.ColumnNumberToName(j + 1)
				cellRef := fmt.Sprintf("%s%d", colName, i+1)
				cellValue, cellErr := xl.GetCellValue(sheetName, cellRef)
				if cellErr == nil && cellValue != "" {
					rows[i][j] = cellValue
				} else {
					// Fallback to raw row value if GetCellValue fails
					rows[i][j] = rawRow[j]
				}
			}
		}

		isCSV = false
		fileExt = ".xlsx"

	} else {
		// Try XLS (legacy Excel) using shakinm/xlsReader which properly handles formulas and formatting
		// Write to temp file since xlsReader works with file paths
		var xlsErr error
		tmpXlsFile, tmpErr := os.CreateTemp("", "bankstmt-*.xls")
		if tmpErr == nil {
			defer os.Remove(tmpXlsFile.Name())
			defer tmpXlsFile.Close()
			_, writeErr := tmpXlsFile.Write(tmpFile)
			if writeErr == nil {
				tmpXlsFile.Close() // Close before reading
				xlsBook, xlsErr := xls.OpenFile(tmpXlsFile.Name())
				if xlsErr == nil {
					sheet, sheetErr := xlsBook.GetSheet(0)
					if sheetErr == nil && sheet != nil {
						xlsRows := sheet.GetRows()
						for _, xlsRow := range xlsRows {
							var rowVals []string
							cols := xlsRow.GetCols()
							for _, col := range cols {
								rowVals = append(rowVals, col.GetString())
							}
							rows = append(rows, rowVals)
						}
						if len(rows) < 2 {
							return nil, errors.New("xls must have at least one data row")
						}
						isCSV = false
						fileExt = ".xls"
					} else {
						xlsErr = errors.New("failed to get xls sheet")
					}
				}
			} else {
				xlsErr = writeErr
			}
		} else {
			xlsErr = tmpErr
		}

		// If XLS failed, try CSV
		if xlsErr != nil {
			r := csv.NewReader(bytes.NewReader(tmpFile))
			r.FieldsPerRecord = -1
			rows, err = r.ReadAll()
			if err != nil {
				return nil, fmt.Errorf("failed to parse excel, xls, or csv: %w", xlErr)
			}
			if len(rows) < 2 {
				return nil, errors.New("csv must have at least one data row")
			}
			isCSV = true
			fileExt = ".csv"
		}
	}

	// 3. Extract account number, entity id, and name from header
	var accountNumber, entityID, accountName string
	if isCSV {
		// For CSV, try to find account number in the first 20 rows, look for acNoHeader or "Account Number"
		for i := 0; i < 20 && i < len(rows); i++ {
			for j, cell := range rows[i] {
				if (cell == acNoHeader || strings.EqualFold(cell, "Account Number") || strings.EqualFold(cell, "Account No.")) && j+1 < len(rows[i]) {
					accountNumber = rows[i][j+1]
				}
				if (cell == "Name:" || strings.EqualFold(cell, "Account Name")) && j+1 < len(rows[i]) {
					accountName = rows[i][j+1]
				}
			}
		}
		// If not found, try to infer from data rows (look for column header)
		if accountNumber == "" {
			// Find header row
			for i, row := range rows {
				for _, cell := range row {
					if cell == slNoHeader {
						// Data starts after this
						if i+1 < len(rows) && len(rows[i+1]) > 0 {
							// Try to get account number from a known column if present
							// But usually not present in data, so skip
						}
						break
					}
				}
			}
		}
	} else {
		for i := 0; i < 20 && i < len(rows); i++ {
			for j, cell := range rows[i] {
				if cell == acNoHeader && j+1 < len(rows[i]) {
					accountNumber = rows[i][j+1]
				}
				if cell == "Name:" && j+1 < len(rows[i]) {
					accountName = rows[i][j+1]
				}
			}
		}
	}
	if accountNumber == "" {
		// Fallback: try to extract account number from the first 20 header rows
		// Handles cases like "Account Number: 36013001" in a single cell,
		// label in one cell and value in adjacent or next-row cell, or merged cells.
		acctLabelRe := regexp.MustCompile(`(?i)\b(?:a[/\\]?c|acct|account)[\s\.:#-]*(?:no|number)?\b`)
		acctNumberRe := regexp.MustCompile(`\d{6,}`)
		for i := 0; i < 20 && i < len(rows); i++ {
			for j, cell := range rows[i] {
				v := normalizeCell(cell)
				// If cell contains a label, try to extract digits from same cell
				if acctLabelRe.MatchString(v) {
					if m := acctNumberRe.FindString(v); m != "" {
						accountNumber = strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
						break
					}
					// try adjacent cell on same row
					if j+1 < len(rows[i]) {
						cand := normalizeCell(rows[i][j+1])
						if m := acctNumberRe.FindString(cand); m != "" {
							accountNumber = strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
							break
						}
					}
					// try same column next row (merged/stacked label)
					if i+1 < len(rows) && j < len(rows[i+1]) {
						cand := normalizeCell(rows[i+1][j])
						if m := acctNumberRe.FindString(cand); m != "" {
							accountNumber = strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
							break
						}
					}
				}
				// Do NOT accept arbitrary digit runs here without an explicit label –
				// this often picks up postal codes (e.g. 411019). If the cell contains
				// an account label we already handled same-cell / adjacent / below
				// checks above. Any header-only digit-run fallback is handled later
				// using a stricter regexp (>=7 digits) in the second-pass.
			}
			if accountNumber != "" {
				break
			}
		}
		if accountNumber == "" {
			// Additional fallback: some bank XLS exports include account number in a title line like
			// "Transactions List - <entity> - <division> (INR) - <account>".
			// Scan the first 40 rows for such a pattern and pick the longest digit run (>=7 digits).
			titleDigitRe := regexp.MustCompile(`\d{7,}`)
			for i := 0; i < 40 && i < len(rows); i++ {
				for _, cell := range rows[i] {
					v := strings.ToLower(normalizeCell(cell))
					if strings.Contains(v, "transactions list") {
						if m := titleDigitRe.FindString(v); m != "" {
							accountNumber = strings.ReplaceAll(m, " ", "")
							log.Printf("[BANK-UPLOAD-DEBUG] Title-line extracted accountNumber=%q", accountNumber)
							break
						}
					}
				}
				if accountNumber != "" {
					break
				}
			}

			// Fallback extraction: search first 20 rows for label variants and numeric candidates.
			// Normalize cells and prefer candidates adjacent to label. Avoid 6-digit matches (likely postal codes).
			normalize := func(s string) string {
				s = strings.TrimSpace(s)
				s = strings.ReplaceAll(s, constants.NBSP, " ")
				return strings.Join(strings.Fields(s), " ")
			}

			found := ""
			// First pass: look for label in cell, then try same-cell, right-cell, then below-cell
			for i := 0; i < 20 && i < len(rows); i++ {
				for j, cell := range rows[i] {
					v := normalize(cell)
					if acctLabelRe.MatchString(v) {
						// 1) same cell has digits
						if m := acctNumberRe.FindString(v); m != "" {
							found = m
							break
						}
						// 2) right neighbor
						if j+1 < len(rows[i]) {
							cand := normalize(rows[i][j+1])
							if m := acctNumberRe.FindString(cand); m != "" {
								found = m
								break
							}
						}
						// 3) below same column
						if i+1 < len(rows) && j < len(rows[i+1]) {
							cand := normalize(rows[i+1][j])
							if m := acctNumberRe.FindString(cand); m != "" {
								found = m
								break
							}
						}
					}
				}
				if found != "" {
					break
				}
			}

			// Second pass: no label found — search any header cell for >=7-digit sequences
			if found == "" {
				digitsRe := acctNumberRe
				for i := 0; i < 20 && i < len(rows); i++ {
					for _, cell := range rows[i] {
						v := normalize(cell)
						if m := digitsRe.FindString(v); m != "" {
							found = m
							break
						}
					}
					if found != "" {
						break
					}
				}
			}

			if found != "" {
				accountNumber = strings.ReplaceAll(strings.ReplaceAll(found, " ", ""), "-", "")
				log.Printf("[BANK-UPLOAD-DEBUG] Fallback extracted accountNumber=%q", accountNumber)
			} else {
				// Dump header rows for debugging when no candidate found
				log.Printf("[BANK-UPLOAD-DEBUG] Account number not found in header; dumping first 20 rows for inspection")
				for i := 0; i < 20 && i < len(rows); i++ {
					log.Printf("[BANK-UPLOAD-DEBUG] row[%d]=%q", i, rows[i])
				}
				return nil, ErrAccountNumberMissing
			}
		}
	}

	// Lookup entity_id and bank_name from masterbankaccount/masterbank
	var bankName string
	log.Printf("[BANK-UPLOAD-DEBUG] Looking up account in masterbankaccount for accountNumber=%q", accountNumber)
	err = db.QueryRowContext(ctx, `
		SELECT mba.entity_id, mb.bank_name, COALESCE(mba.account_nickname, mb.bank_name)
		FROM public.masterbankaccount mba
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		WHERE mba.account_number = $1 AND mba.is_deleted = false
	`, accountNumber).Scan(&entityID, &bankName, &accountName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Primary candidate not found. Try scanning header rows for other
			// long digit runs and attempt DB verification for each candidate
			// in order. This prevents accidentally picking transaction refs
			// or postal codes that appear earlier in the header.
			log.Printf("[BANK-UPLOAD-DEBUG] Primary account %q not found; scanning header for verified candidates", accountNumber)
			candidates := []string{}
			seen := map[string]bool{}
			// use the stricter acctNumberRe (>=7 digits) to collect candidates
			for i := 0; i < 20 && i < len(rows); i++ {
				for _, cell := range rows[i] {
					v := normalizeCell(cell)
					for _, m := range acctNumberRe.FindAllString(v, -1) {
						cand := strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
						if cand == "" {
							continue
						}
						if !seen[cand] {
							seen[cand] = true
							candidates = append(candidates, cand)
						}
					}
				}
			}
			matched := false
			for _, cand := range candidates {
				log.Printf("[BANK-UPLOAD-DEBUG] Trying candidate accountNumber=%q", cand)
				var eID, bName, aName string
				qErr := db.QueryRowContext(ctx, `
					SELECT mba.entity_id, mb.bank_name, COALESCE(mba.account_nickname, mb.bank_name)
					FROM public.masterbankaccount mba
					LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
					WHERE mba.account_number = $1 AND mba.is_deleted = false
				`, cand).Scan(&eID, &bName, &aName)
				if qErr == nil {
					accountNumber = cand
					entityID = eID
					bankName = bName
					accountName = aName
					matched = true
					log.Printf("[BANK-UPLOAD-DEBUG] Candidate matched accountNumber=%q", cand)
					break
				}
				if qErr != nil && !errors.Is(qErr, sql.ErrNoRows) {
					log.Printf("[BANK-UPLOAD-DEBUG] DB error while trying candidate %q: %v", cand, qErr)
					return nil, fmt.Errorf(constants.ErrDBMasterBankAccountLookup, qErr)
				}
			}
			if !matched {
				log.Printf("[BANK-UPLOAD-DEBUG] Account not found in masterbankaccount after candidate scan: %v", candidates)
				return nil, ErrAccountNotFound
			}
		} else {
			log.Printf("[BANK-UPLOAD-DEBUG] DB error while looking up account %q: %v", accountNumber, err)
			return nil, fmt.Errorf(constants.ErrDBMasterBankAccountLookup, err)
		}
	}

	// Context validations (entity scope + approved bank/account lists)
	if ids := apictx.GetEntityIDsFromCtx(ctx); len(ids) > 0 {
		if !apictx.IsEntityAllowed(ctx, entityID) {
			return nil, errors.New("no access to this entity")
		}
	}
	if bankName != "" {
		if names := apictx.GetBankNamesFromCtx(ctx); len(names) > 0 {
			if !apictx.IsBankAllowed(ctx, bankName) {
				return nil, errors.New(constants.ErrBankNotAllowed)
			}
		}
	}
	if ctx.Value("ApprovedBankAccounts") != nil {
		if !ctxHasApprovedBankAccount(ctx, accountNumber) {
			return nil, errors.New("bank account not approved")
		}
	}

	// Currency enforcement (entity -> bank -> account -> currency)
	var acctCurrency sql.NullString
	if curCodes := ctxApprovedCurrencies(ctx); len(curCodes) > 0 {
		_ = db.QueryRowContext(ctx, `SELECT mba.currency FROM public.masterbankaccount mba WHERE mba.account_number = $1 LIMIT 1`, accountNumber).Scan(&acctCurrency)
		if acctCurrency.Valid && strings.TrimSpace(acctCurrency.String) != "" {
			if !ctxHasApprovedCurrency(ctx, acctCurrency.String) {
				return nil, errors.New(constants.ErrCurrencyNotAllowed)
			}
		}
	}

	s3Key := buildBankStatementS3Key(accountNumber, fileHash, fileExt)
	var s3URL string
	if isS3Enabled() {
		s3URL, err = uploadBankStatementToS3(ctx, s3Key, tmpFile, contentType)
		if err != nil {
			return nil, fmt.Errorf("failed to store original file to s3: %w", err)
		}
	} else {
		log.Printf("[BANK-UPLOAD-DEBUG] S3 upload disabled by BANK_STMT_S3_ENABLED=false; skipping storage for account=%q key=%q", accountNumber, s3Key)
		s3URL = ""
	}

	// 4. Fetch all active rules/components for this account/entity/bank/currency/global
	var currencyCode string
	if acctCurrency.Valid {
		currencyCode = acctCurrency.String
	}
	rules, err := loadCategoryRuleComponents(ctx, db, accountNumber, entityID, currencyCode)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch category rules: %w", err)
	}

	// 5. Parse transactions, categorize, and collect KPIs
	// Find the header row for transactions
	var txnHeaderIdx int = -1
	var headerRow []string
	var colIdx map[string]int
	// helpers that need to be visible both during header-mapping and later parsing
	var sampleNumericColumn func(int) bool
	var findAmountLikeCSV func() int
	var findDebitCreditCols func() (int, int)
	if isCSV {
		// For CSV, header often contains Date + Description (or Detail Description)
		// and Debit/Credit (or Withdrawal/Deposit) columns. Try to find a row
		// that looks like the transaction header instead of relying solely on
		// a "Sl. No." column which many banks omit.
		for i, row := range rows {
			foundSl := false
			for _, cell := range row {
				if strings.EqualFold(strings.TrimSpace(cell), slNoHeader) {
					txnHeaderIdx = i
					foundSl = true
					break
				}
			}
			if foundSl {
				break
			}
		}
		// If not found, look for a row that has Date and either Description or Debit/Credit columns
		if txnHeaderIdx == -1 {
			for i, row := range rows {
				hasDate := false
				hasDesc := false
				hasAmountCols := false
				for _, cell := range row {
					c := strings.TrimSpace(cell)
					lc := strings.ToLower(c)
					// Accept "Date", "Value Date", "Transaction Date", etc.
					if strings.EqualFold(c, "Date") || strings.Contains(lc, "date") {
						hasDate = true
					}
					if strings.Contains(lc, "description") || strings.Contains(lc, "remarks") || strings.Contains(lc, "narration") || strings.Contains(lc, constants.ErrDebitCreditReference2) {
						hasDesc = true
					}
					if strings.EqualFold(c, "Withdrawal") || strings.EqualFold(c, "Deposit") || strings.EqualFold(c, "Debit") || strings.EqualFold(c, "Credit") || strings.Contains(lc, "debit") || strings.Contains(lc, "credit") || strings.Contains(lc, "amount") {
						hasAmountCols = true
					}
				}
				if hasDate && (hasDesc || hasAmountCols) {
					txnHeaderIdx = i
					log.Printf("[BANK-UPLOAD-DEBUG] CSV header found at row %d: %q", i, row)
					break
				}
			}
		}
		if txnHeaderIdx == -1 {
			log.Printf("[BANK-UPLOAD-DEBUG] CSV header detection FAILED. Dumping first 30 rows:")
			for i := 0; i < 30 && i < len(rows); i++ {
				log.Printf("[BANK-UPLOAD-DEBUG] CSV row[%d]=%q", i, rows[i])
			}
			return nil, errors.New("transaction header row not found in CSV file")
		}
		headerRow = rows[txnHeaderIdx]
		colIdx = map[string]int{}
		for idx, col := range headerRow {
			colIdx[strings.TrimSpace(col)] = idx
		}

		// Verbose header/column dump for Excel branch as well: index, header,
		// normalized token, numeric-sample and first few sample cells.
		sampleValuesLimit := 5
		for idx, col := range headerRow {
			norm := strings.ToLower(strings.TrimSpace(col))
			norm = strings.ReplaceAll(norm, ",", "")
			norm = strings.ReplaceAll(norm, ".", "")
			norm = strings.ReplaceAll(norm, "/", " ")
			norm = strings.Join(strings.Fields(norm), " ")
			// do a quick numeric sample across the next 20 rows
			countSamples := 0
			numCount := 0
			for r := txnHeaderIdx + 1; r < len(rows) && countSamples < 20; r++ {
				row := rows[r]
				if idx >= len(row) {
					countSamples++
					continue
				}
				cell := strings.TrimSpace(row[idx])
				if cell == "" {
					countSamples++
					continue
				}
				clean := cleanAmount(cell)
				if _, err := strconv.ParseFloat(clean, 64); err == nil {
					numCount++
				}
				countSamples++
			}
			numericSample := false
			if countSamples > 0 && numCount*2 >= countSamples {
				numericSample = true
			}
			// collect sample cells
			samples := []string{}
			for r := txnHeaderIdx + 1; r < len(rows) && len(samples) < sampleValuesLimit; r++ {
				row := rows[r]
				if idx < len(row) {
					samples = append(samples, row[idx])
				} else {
					samples = append(samples, "")
				}
			}
			log.Printf("[BANK-UPLOAD-DEBUG] excel-header-col idx=%d header=%q norm=%q numericSample=%v samples=%q", idx, col, norm, numericSample, samples)
		}

		// Add flexible column name mappings for CSV as well
		findColContaining := func(keywords ...string) int {
			for colName, idx := range colIdx {
				lcName := strings.ToLower(colName)
				if strings.Contains(lcName, "ref") {
					continue
				}
				for _, kw := range keywords {
					if strings.Contains(lcName, strings.ToLower(kw)) {
						return idx
					}
				}
			}
			return -1
		}

		// helper to check if a column looks numeric by sampling a few data rows
		sampleNumericColumn = func(colIdxNum int) bool {
			if colIdxNum < 0 {
				return false
			}
			samples := 0
			numericCount := 0
			for r := txnHeaderIdx + 1; r < len(rows) && samples < 100; r++ {
				row := rows[r]
				if colIdxNum >= len(row) {
					samples++
					continue
				}
				cell := strings.TrimSpace(row[colIdxNum])
				if cell == "" {
					samples++
					continue
				}
				clean := cleanAmount(cell)
				if _, err := strconv.ParseFloat(clean, 64); err == nil {
					numericCount++
				}
				samples++
			}
			// consider numeric if at least one sampled cell looks numeric
			return samples > 0 && numericCount > 0
		}

		// find amount-like column for CSV: avoid 'ref' columns and prefer numeric columns
		findAmountLikeCSV = func() int {
			best := -1
			// prefer explicit Debit/Credit headers that are numeric
			for colName, idx := range colIdx {
				lcName := strings.ToLower(strings.TrimSpace(colName))
				if lcName == "debit" || lcName == "credit" || strings.Contains(lcName, "withdrawal") || strings.Contains(lcName, "deposit") {
					if strings.Contains(lcName, "ref") || strings.Contains(lcName, "reference") || strings.Contains(lcName, constants.ErrReferenceRequired) {
						continue
					}
					if sampleNumericColumn(idx) {
						return idx
					}
					best = idx
				}
			}
			// fallback: any header containing 'amount' that is numeric
			for colName, idx := range colIdx {
				lcName := strings.ToLower(colName)
				if strings.Contains(lcName, "amount") {
					if sampleNumericColumn(idx) {
						return idx
					}
					if best == -1 {
						best = idx
					}
				}
			}
			return best
		}
		// Force remap Description column: remove any auto-mapped description columns
		// and explicitly set to Debit/Credit Ref if present
		delete(colIdx, "Description")
		delete(colIdx, "Detail Description")

		// Special finder that INCLUDES ref columns (override the default findColContaining)
		findDescriptionCol := func(keywords ...string) int {
			for colName, idx := range colIdx {
				lcName := strings.ToLower(colName)
				for _, kw := range keywords {
					if strings.Contains(lcName, strings.ToLower(kw)) {
						return idx
					}
				}
			}
			return -1
		}

		if idx := findDescriptionCol(constants.ErrDebitCreditReference2, "debit / credit ref", constants.ErrDebitCreditReferenceShort, constants.ErrDebitCreditReferenceAlt, constants.ErrDebitCreditReference, "debitcreditref"); idx >= 0 {
			colIdx["Description"] = idx
			log.Printf("[BANK-UPLOAD-DEBUG] Using Debit/Credit Ref column %d for Description", idx)
		} else if idx := findColContaining("description", "remarks", "narration", "particulars"); idx >= 0 {
			colIdx["Description"] = idx
			log.Printf("[BANK-UPLOAD-DEBUG] Using Description-like column %d for Description", idx)
		}
		if _, exists := colIdx["Date"]; !exists {
			if idx := findColContaining("date"); idx >= 0 && !strings.Contains(strings.ToLower(headerRow[idx]), "value") {
				colIdx["Date"] = idx
			}
		}
		if _, exists := colIdx["Balance"]; !exists {
			if idx := findColContaining("balance"); idx >= 0 {
				colIdx["Balance"] = idx
				colIdx[balanceHeader] = idx // Also set balanceHeader for CSV path compatibility
			}
		} else {
			// Balance exists, also map it to balanceHeader
			colIdx[balanceHeader] = colIdx["Balance"]
		}
		// If Value Date is absent in CSV headers, fall back to the primary Date column
		if _, exists := colIdx[valueDateHeader]; !exists {
			if idx, ok := colIdx["Date"]; ok {
				colIdx[valueDateHeader] = idx
			}
		}
		// Choose Debit and Credit columns robustly by combining header keywords
		// and numeric sampling. Prefer distinct numeric columns and skip
		// reference columns like "Debit/Credit Ref".
		findDebitCreditCols = func() (int, int) {
			// scores: headerScore (higher if header explicitly mentions debit/credit), numericScore (1 if column looks numeric)
			type cand struct {
				idx         int
				headerScore int
				numeric     bool
			}
			cands := map[int]*cand{}
			for colName, idx := range colIdx {
				lc := strings.ToLower(strings.TrimSpace(colName))
				// skip obvious reference columns
				if strings.Contains(lc, "ref") || strings.Contains(lc, "reference") || strings.Contains(lc, constants.ErrReferenceRequired) {
					continue
				}
				hdrScore := 0
				if strings.Contains(lc, "withdrawal") || strings.Contains(lc, "debit") || strings.Contains(lc, "dr") {
					hdrScore += 3
				}
				if strings.Contains(lc, "deposit") || strings.Contains(lc, "credit") || strings.Contains(lc, "cr") {
					hdrScore += 3
				}
				if strings.Contains(lc, "amount") {
					hdrScore += 1
				}
				// record candidate only if header suggests amounts or numeric sampling passes
				numeric := sampleNumericColumn(idx)
				if hdrScore > 0 || numeric {
					cands[idx] = &cand{idx: idx, headerScore: hdrScore, numeric: numeric}
				}
			}
			// pick withdrawal (debit) candidate
			bestW, bestD := -1, -1
			bestWScore := -1
			for _, cc := range cands {
				score := cc.headerScore
				if cc.numeric {
					score += 2
				}
				hdr := strings.ToLower(strings.TrimSpace(headerRow[cc.idx]))
				// prefer explicit debit/withdrawal headers
				if score > bestWScore && (hdr == "debit" || hdr == "withdrawal" || strings.Contains(hdr, "withdrawal") || (strings.Contains(hdr, "debit") && !strings.Contains(hdr, "credit"))) {
					bestW = cc.idx
					bestWScore = score
				}
			}
			// pick deposit (credit) candidate
			bestDScore := -1
			for _, cc := range cands {
				if cc.idx == bestW {
					continue
				}
				score := cc.headerScore
				if cc.numeric {
					score += 2
				}
				hdr := strings.ToLower(strings.TrimSpace(headerRow[cc.idx]))
				if score > bestDScore && (hdr == "credit" || hdr == "deposit" || strings.Contains(hdr, "deposit") || (strings.Contains(hdr, "credit") && !strings.Contains(hdr, "debit"))) {
					bestD = cc.idx
					bestDScore = score
				}
			}
			// If still missing, try to fallback to any numeric candidates
			if bestW == -1 {
				for _, cc := range cands {
					if cc.numeric {
						bestW = cc.idx
						break
					}
				}
			}
			if bestD == -1 {
				for _, cc := range cands {
					if cc.idx != bestW && cc.numeric {
						bestD = cc.idx
						break
					}
				}
			}
			return bestW, bestD
		}

		wIdx, dIdx := findDebitCreditCols()
		log.Printf("[BANK-UPLOAD-DEBUG] findDebitCreditCols returned: Withdrawal=%d, Deposit=%d", wIdx, dIdx)
		log.Printf("[BANK-UPLOAD-DEBUG] Headers: %v", headerRow)
		if wIdx >= 0 {
			log.Printf("[BANK-UPLOAD-DEBUG] Setting Withdrawal to column %d (header: %s)", wIdx, headerRow[wIdx])
			colIdx["Withdrawal"] = wIdx
			colIdx[withdrawalAmtHeader] = wIdx
		}
		if dIdx >= 0 {
			log.Printf("[BANK-UPLOAD-DEBUG] Setting Deposit to column %d (header: %s)", dIdx, headerRow[dIdx])
			colIdx["Deposit"] = dIdx
			colIdx[depositAmtHeader] = dIdx
		}
		log.Printf("[BANK-UPLOAD-DEBUG] CSV Column mapping: Date=%d, Description=%d, Withdrawal=%d, Deposit=%d, Balance=%d",
			colIdx["Date"], colIdx["Description"], colIdx["Withdrawal"], colIdx["Deposit"], colIdx["Balance"])
		// Flexible required columns: ensure we have Date, a description column, and at least one amount column
		hasDate := false
		hasDesc := false
		hasAmount := false
		for col := range colIdx {
			lc := strings.ToLower(col)
			// accept any date-like header (Date, Value Date, Transaction Date, Posted Date)
			if strings.Contains(lc, "date") {
				hasDate = true
			}
			if strings.Contains(lc, "description") || strings.Contains(lc, "remarks") {
				hasDesc = true
			}
			if lc == "withdrawal" || lc == "deposit" || lc == "debit" || lc == "credit" || strings.Contains(lc, "debit") || strings.Contains(lc, "credit") || strings.Contains(lc, "amount") {
				hasAmount = true
			}
		}
		if !hasDate || (!hasDesc && !hasAmount) {
			return nil, errors.New("transaction header row not found in CSV file")
		}
	} else {
		// For Excel/XLS, some statements don't include a "Tran. Id" column.
		// Try the original Tran Id detection first, then fall back to a
		// flexible detection similar to CSV: find a row that has Date and
		// (Description-like column OR amount columns).
		for i, row := range rows {
			for _, cell := range row {
				if cell == tranIDHeader || strings.EqualFold(strings.TrimSpace(cell), "Tran Id") {
					txnHeaderIdx = i
					break
				}
			}
			if txnHeaderIdx != -1 {
				break
			}
		}
		if txnHeaderIdx == -1 {
			for i, row := range rows {
				hasDate := false
				hasDesc := false
				hasAmountCols := false
				for _, cell := range row {
					c := strings.TrimSpace(cell)
					lc := strings.ToLower(c)
					// Accept "Date", "Value Date", "Transaction Date", "Txn Posted Date", etc.
					if strings.EqualFold(c, "Date") || strings.Contains(lc, "date") {
						hasDate = true
					}
					if strings.Contains(lc, "description") || strings.Contains(lc, "remarks") || strings.Contains(lc, "narration") {
						hasDesc = true
					}
					if strings.EqualFold(c, "Withdrawal") || strings.EqualFold(c, "Deposit") || strings.EqualFold(c, "Debit") || strings.EqualFold(c, "Credit") || strings.Contains(lc, "debit") || strings.Contains(lc, "credit") || strings.Contains(lc, "amount") {
						hasAmountCols = true
					}
				}
				if hasDate && (hasDesc || hasAmountCols) {
					txnHeaderIdx = i
					log.Printf("[BANK-UPLOAD-DEBUG] Excel header found at row %d: %q", i, row)
					break
				}
			}
		}
		if txnHeaderIdx == -1 {
			log.Printf("[BANK-UPLOAD-DEBUG] Excel header detection FAILED. Dumping first 30 rows:")
			for i := 0; i < 30 && i < len(rows); i++ {
				log.Printf("[BANK-UPLOAD-DEBUG] Excel row[%d]=%q", i, rows[i])
			}
			return nil, errors.New(constants.ErrTransactionHeaderRowNotFound)
		}
		headerRow = rows[txnHeaderIdx]
		colIdx = map[string]int{}
		for idx, col := range headerRow {
			colIdx[strings.TrimSpace(col)] = idx
		}
		// Add flexible column name mappings for common variations
		// This allows matching "Detail Description" when code looks for "Description"
		findColContaining := func(keywords ...string) int {
			for colName, idx := range colIdx {
				lcName := strings.ToLower(colName)
				for _, kw := range keywords {
					if strings.Contains(lcName, strings.ToLower(kw)) {
						return idx
					}
				}
			}
			return -1
		}

		// Normalize common variants so downstream parsing does not miss dates.
		if _, ok := colIdx[transactionDateHeader]; !ok {
			if idx := findColContaining("transaction date", constants.TransactionPostedDate, constants.TransactionPostedDateAlt, constants.TransactionPostingDate); idx >= 0 {
				colIdx[transactionDateHeader] = idx
			}
		}
		if _, ok := colIdx["PostedDate"]; !ok {
			if idx := findColContaining(constants.TransactionPostedDate, constants.TransactionPostedDateAlt, constants.TransactionPostingDate); idx >= 0 {
				colIdx["PostedDate"] = idx
			}
		}
		if _, ok := colIdx[valueDateHeader]; !ok {
			if idx := findColContaining("value date", "value-date", "val date"); idx >= 0 {
				colIdx[valueDateHeader] = idx
			}
		}

		// detect possible cr/dr indicator and amount column separately
		crDrIdx := -1
		amountIdx := -1

		// helper to find amount-like columns (prefer explicit "amount" or exact "Debit"/"Credit")
		findAmountLike := func() int {
			// prefer any header containing "amount"
			for colName, idx := range colIdx {
				lcName := strings.ToLower(colName)
				if strings.Contains(lcName, "amount") {
					return idx
				}
			}
			// then prefer exact matches for Debit or Credit
			for colName, idx := range colIdx {
				lcName := strings.ToLower(strings.TrimSpace(colName))
				if lcName == "debit" || lcName == "credit" || lcName == constants.DebitAmount || lcName == constants.CreditAmount {
					return idx
				}
			}
			// otherwise, pick a debit/credit-like column only if it is NOT a reference column
			for colName, idx := range colIdx {
				lcName := strings.ToLower(colName)
				if (strings.Contains(lcName, "debit") || strings.Contains(lcName, "credit") || strings.Contains(lcName, "withdrawal") || strings.Contains(lcName, "deposit")) && !strings.Contains(lcName, "ref") && !strings.Contains(lcName, "reference") && !strings.Contains(lcName, constants.ErrReferenceRequired) {
					return idx
				}
			}
			return -1
		}

		// helper to find Cr/Dr indicator columns
		findCrDrLike := func() int {
			for colName, idx := range colIdx {
				lcName := strings.ToLower(colName)
				if strings.Contains(lcName, "cr/dr") || strings.Contains(lcName, "crdr") || strings.Contains(lcName, "cr / dr") || strings.TrimSpace(lcName) == "cr" || strings.TrimSpace(lcName) == "dr" || strings.Contains(lcName, "credit/debit") || strings.Contains(lcName, "debit/credit") {
					return idx
				}
			}
			return -1
		}

		// Map standard names to flexible column indices
		if _, exists := colIdx["Description"]; !exists {
			// Prefer explicit description column when present
			if idx := findColContaining("description", "remarks", "narration", "particulars"); idx >= 0 {
				colIdx["Description"] = idx
			} else if idx := findColContaining(constants.ErrDebitCreditReference2, constants.ErrDebitCreditReferenceShort, constants.ErrDebitCreditReferenceAlt, constants.ErrDebitCreditReference, "debitcreditref"); idx >= 0 {
				colIdx["Description"] = idx
			}
		}
		if _, exists := colIdx[transactionRemarksHeader]; !exists {
			// Prefer Debit/Credit Ref header if present for transaction remarks
			// Try variations with slash and space
			if idx := findColContaining(constants.ErrDebitCreditReference2, "debit / credit ref", constants.ErrDebitCreditReferenceShort, constants.ErrDebitCreditReferenceAlt, constants.ErrDebitCreditReference, "debitcreditref"); idx >= 0 {
				colIdx[transactionRemarksHeader] = idx
				log.Printf("[BANK-UPLOAD-DEBUG] Using Debit/Credit Ref column %d for Transaction Remarks", idx)
			} else if idx := findColContaining("description", "remarks", "narration", "particulars"); idx >= 0 {
				colIdx[transactionRemarksHeader] = idx
				log.Printf("[BANK-UPLOAD-DEBUG] Using Description-like column %d for Transaction Remarks", idx)
			}
		}
		if _, exists := colIdx["Date"]; !exists {
			if idx := findColContaining("date"); idx >= 0 && !strings.Contains(strings.ToLower(headerRow[idx]), "value") && !strings.Contains(strings.ToLower(headerRow[idx]), "posted") {
				colIdx["Date"] = idx
			}
		}
		if _, exists := colIdx[valueDateHeader]; !exists {
			// Try "Value Date", "Txn Posted Date", or any date column
			if idx := findColContaining("value date", constants.TransactionPostedDate); idx >= 0 {
				colIdx[valueDateHeader] = idx
			} else if idx := findColContaining("date"); idx >= 0 {
				colIdx[valueDateHeader] = idx
			}
		}
		if _, exists := colIdx[transactionDateHeader]; !exists {
			// Try "Transaction Date", "Txn Posted Date", or fallback to "Date"
			if idx := findColContaining("transaction date", constants.TransactionPostedDate, constants.TransactionPostedDateAlt); idx >= 0 {
				colIdx[transactionDateHeader] = idx
			} else if idx := findColContaining("date"); idx >= 0 {
				colIdx[transactionDateHeader] = idx
			}
		}
		// Ensure we have a generic Date mapping; prefer value/transaction date when explicit Date is absent.
		if _, exists := colIdx["Date"]; !exists {
			if idx := findColContaining("date"); idx >= 0 && !strings.Contains(strings.ToLower(headerRow[idx]), "value") && !strings.Contains(strings.ToLower(headerRow[idx]), "posted") {
				colIdx["Date"] = idx
			} else if idx, ok := colIdx[valueDateHeader]; ok {
				colIdx["Date"] = idx
			} else if idx, ok := colIdx[transactionDateHeader]; ok {
				colIdx["Date"] = idx
			}
		}
		// Normalize posted date column
		if _, exists := colIdx["PostedDate"]; !exists {
			if idx := findColContaining(constants.TransactionPostedDate, constants.TransactionPostedDateAlt, constants.TransactionPostingDate); idx >= 0 {
				colIdx["PostedDate"] = idx
			}
		}
		if _, exists := colIdx[tranIDHeader]; !exists {
			// Prefer explicit "Transaction ID" over generic "No." by ordering keywords
			if idx := findColContaining("transaction id", "tran id", "txn id", "reference"); idx >= 0 {
				colIdx[tranIDHeader] = idx
			} else if idx := findColContaining("no.", "sl.", "sl no"); idx >= 0 {
				colIdx[tranIDHeader] = idx
			}
		}

		// Prefer explicit amount-like column, but only if we don't have separate debit/credit columns
		amountIdx = findAmountLike()

		// Detect Cr/Dr indicator column (if present)
		if crDrIdx = findCrDrLike(); crDrIdx >= 0 {
			colIdx["CrDr"] = crDrIdx
		}

		// First try to find separate withdrawal/deposit columns
		if _, exists := colIdx[withdrawalAmtHeader]; !exists {
			// Look for exact "Debit" or "Withdrawal" column (exclude "Debit/Credit Ref")
			for colName, idx := range colIdx {
				lcName := strings.ToLower(strings.TrimSpace(colName))
				// Skip reference columns
				if strings.Contains(lcName, "ref") || strings.Contains(lcName, "reference") {
					continue
				}
				// Exact match for debit or withdrawal
				if lcName == "debit" || lcName == "withdrawal" || lcName == constants.DebitAmount || lcName == "withdrawal amt" {
					colIdx[withdrawalAmtHeader] = idx
					break
				}
			}
		}
		if _, exists := colIdx[depositAmtHeader]; !exists {
			// Look for exact "Credit" or "Deposit" column (exclude "Debit/Credit Ref")
			for colName, idx := range colIdx {
				lcName := strings.ToLower(strings.TrimSpace(colName))
				// Skip reference columns
				if strings.Contains(lcName, "ref") || strings.Contains(lcName, "reference") {
					continue
				}
				// Exact match for credit or deposit
				if lcName == "credit" || lcName == "deposit" || lcName == constants.CreditAmount || lcName == "deposit amt" {
					colIdx[depositAmtHeader] = idx
					break
				}
			}
		}

		// If we still don't have both withdrawal and deposit columns, and we have an amount column with Cr/Dr indicator,
		// then map both to the amount column (sign will be determined by Cr/Dr)
		if amountIdx >= 0 && crDrIdx >= 0 {
			if _, exists := colIdx[withdrawalAmtHeader]; !exists {
				colIdx[withdrawalAmtHeader] = amountIdx
			}
			if _, exists := colIdx[depositAmtHeader]; !exists {
				colIdx[depositAmtHeader] = amountIdx
			}
		}
		if _, exists := colIdx[balanceHeader]; !exists {
			if idx := findColContaining("balance", "available balance"); idx >= 0 {
				colIdx[balanceHeader] = idx
			}
		}
		if _, exists := colIdx["Balance"]; !exists {
			if idx := findColContaining("balance", "available balance"); idx >= 0 {
				colIdx["Balance"] = idx
			}
		}
		if _, exists := colIdx["Withdrawal"]; !exists {
			if idx := findColContaining("withdrawal", "debit", "dr"); idx >= 0 {
				colIdx["Withdrawal"] = idx
			}
		}
		if _, exists := colIdx["Deposit"]; !exists {
			if idx := findColContaining("deposit", "credit", "cr"); idx >= 0 {
				colIdx["Deposit"] = idx
			}
		}
		// If we still don't have a dedicated remarks column, mirror Description so parsing works for title-style layouts
		if _, exists := colIdx[transactionRemarksHeader]; !exists {
			if idx, ok := colIdx["Description"]; ok {
				colIdx[transactionRemarksHeader] = idx
			}
		}
		// Helper to safely get a column index or -1 when missing
		getIdx := func(key string) int {
			if v, ok := colIdx[key]; ok {
				return v
			}
			return -1
		}
		log.Printf("[BANK-UPLOAD-DEBUG] Column mapping: Date=%d, ValueDate=%d, TranID=%d, Description=%d, Withdrawal=%d, Deposit=%d, Balance=%d, CrDr=%d, Amount=%d",
			getIdx("Date"), getIdx(valueDateHeader), getIdx(tranIDHeader), getIdx(transactionRemarksHeader), getIdx(withdrawalAmtHeader), getIdx(depositAmtHeader), getIdx(balanceHeader), getIdx("CrDr"), amountIdx)
		// Flexible required columns for Excel as well: need Date and (Description or amount column)
		hasDate := false
		hasDesc := false
		hasAmount := false
		for col := range colIdx {
			lc := strings.ToLower(col)
			if strings.Contains(lc, "date") {
				hasDate = true
			}
			if strings.Contains(lc, "description") || strings.Contains(lc, "remarks") {
				hasDesc = true
			}
			if lc == "withdrawal" || lc == "deposit" || lc == "debit" || lc == "credit" || strings.Contains(lc, "debit") || strings.Contains(lc, "credit") || strings.Contains(lc, "amount") {
				hasAmount = true
			}
		}
		if !hasDate || (!hasDesc && !hasAmount) {
			return nil, errors.New(constants.ErrTransactionHeaderRowNotFound)
		}
	}

	// KPI maps
	categoryCount := map[int64]int{}
	debitSum := map[int64]float64{}
	creditSum := map[int64]float64{}
	uncategorized := []map[string]interface{}{}
	transactions := []BankStatementTransaction{}
	// Will be filled later after DB insert to indicate which
	// transactions from this file were newly inserted vs already present.
	reviewTransactions := []map[string]interface{}{}
	uploadedCount := 0
	var lastValidValueDate time.Time
	var statementPeriodStart, statementPeriodEnd time.Time
	var openingBalance, closingBalance float64
	totalRows := 0
	skippedEmptyRows := 0
	skippedNonTxnRows := 0
	skippedMissingDateRows := 0
	skippedOpeningBalanceRows := 0
	skippedClosingBalanceRows := 0
	keptRows := 0
	var firstValidRow = true
	var cumulative float64
	// For CSV: try to extract period from a row like 'Period From 01/07/2025 To 30/09/2025'
	var csvPeriodStart, csvPeriodEnd time.Time
	for _, row := range rows {
		if len(row) > 1 && strings.Contains(strings.ToLower(row[0]), "period") && strings.Contains(strings.ToLower(row[1]), "from") {
			// Example: row[1] = "From 01/07/2025 To 30/09/2025"
			parts := strings.Fields(row[1])
			var fromIdx, toIdx int
			for i, p := range parts {
				if strings.ToLower(p) == "from" {
					fromIdx = i
				}
				if strings.ToLower(p) == "to" {
					toIdx = i
				}
			}
			if fromIdx+1 < len(parts) && toIdx+1 <= len(parts) && toIdx > fromIdx {
				startStr := parts[fromIdx+1]
				endStr := parts[toIdx+1]
				t1, err1 := parseDate(startStr)
				t2, err2 := parseDate(endStr)
				if err1 == nil && !t1.IsZero() {
					csvPeriodStart = t1
				}
				if err2 == nil && !t2.IsZero() {
					csvPeriodEnd = t2
				}
			}
		}
	}
	// Data rows start after header
	debugParse := strings.ToLower(strings.TrimSpace(os.Getenv("BANK_STMT_DEBUG_PARSE"))) == "1" || strings.ToLower(strings.TrimSpace(os.Getenv("BANK_STMT_DEBUG_PARSE"))) == "true"
	debugCount := 0
	// iterate with index so we can log original row number
	for ri, row := range rows[txnHeaderIdx+1:] {
		rowNum := txnHeaderIdx + 1 + ri
		totalRows++
		// Filter out non-transaction rows by checking for known non-transaction keywords in the first column
		if len(row) > 0 {
			firstCell := strings.ToLower(strings.TrimSpace(row[0]))
			if strings.Contains(firstCell, "call 1800") ||
				strings.Contains(firstCell, "write to us") ||
				strings.Contains(firstCell, constants.ClosingBalance) ||
				strings.Contains(firstCell, "opening balance") ||
				strings.Contains(firstCell, "toll free") {
				skippedNonTxnRows++
				continue
			}
		}
		// Defensive: fill missing columns with empty string
		for len(row) < len(headerRow) {
			row = append(row, "")
		}
		var tranID sql.NullString
		var valueDate, transactionDate, postedDate time.Time
		var description string
		var withdrawal, deposit sql.NullFloat64
		var balance sql.NullFloat64

		// Try to locate cheque/ref and serial-no columns for fallback IDs or description enrichment
		chequeRefIdx := -1
		serialIdx := -1
		for h, idx := range colIdx {
			lh := strings.ToLower(h)
			if strings.Contains(lh, "cheque") || strings.Contains(lh, "chq") || strings.Contains(lh, "ref") {
				chequeRefIdx = idx
			}
			if serialIdx == -1 && (lh == "no." || strings.Contains(lh, "sl") || strings.Contains(lh, "serial")) {
				serialIdx = idx
			}
		}

		if isCSV {
			// CSV: Sl. No., Date, Description, Chq / Ref number, Value Date, Withdrawal, Deposit, Balance, CR/DR
			// First check for opening balance row BEFORE filtering empty tranID
			var tmpDesc string
			if colIdx["Description"] < len(row) {
				tmpDesc = strings.ToLower(strings.TrimSpace(row[colIdx["Description"]]))
			}
			balanceIdx, hasBalance := colIdx[balanceHeader]
			if strings.Contains(tmpDesc, constants.BalanceCarriedForward) {
				// Extract opening balance from Balance column
				if hasBalance && balanceIdx >= 0 && balanceIdx < len(row) {
					balanceStr := cleanAmount(row[balanceIdx])
					if balanceStr != "" {
						fmt.Sscanf(balanceStr, "%f", &openingBalance)
						log.Printf("[BANK-UPLOAD-DEBUG] OPENING BALANCE detected: %.2f (cumulative stays at 0, will be added via openingBalance + cumulative)", openingBalance)
					}
				}
				skippedOpeningBalanceRows++
				continue
			}
			if len(row) == 0 {
				skippedEmptyRows++
				continue
			}
			valueDateStr := row[colIdx[valueDateHeader]]
			transactionDateStr := row[colIdx["Date"]]
			valueDate, _ = parseDateDDMMFirst(valueDateStr)
			transactionDate, _ = parseDateDDMMFirst(transactionDateStr)
			// If only one of the two dates parsed, mirror it to the other to avoid skipping
			if valueDate.IsZero() && !transactionDate.IsZero() {
				valueDate = transactionDate
			}
			if transactionDate.IsZero() && !valueDate.IsZero() {
				transactionDate = valueDate
			}
			if colIdx[tranIDHeader] < len(row) {
				tranID = sql.NullString{String: row[colIdx[tranIDHeader]], Valid: strings.TrimSpace(row[colIdx[tranIDHeader]]) != ""}
			} else {
				tranID = sql.NullString{Valid: false}
			}
			// Fallback: use cheque/ref number when Tran Id cell is empty
			if (!tranID.Valid || strings.TrimSpace(tranID.String) == "") && chequeRefIdx >= 0 && chequeRefIdx < len(row) {
				val := strings.TrimSpace(row[chequeRefIdx])
				if val != "" {
					tranID = sql.NullString{String: val, Valid: true}
				}
			}
			// If still empty, use serial number with an M-prefix to keep IDs stable
			if (!tranID.Valid || strings.TrimSpace(tranID.String) == "") && serialIdx >= 0 && serialIdx < len(row) {
				val := strings.TrimSpace(row[serialIdx])
				if val != "" {
					tranID = sql.NullString{String: "M" + val, Valid: true}
				}
			}
			description = row[colIdx["Description"]]
			// sanitize early so any NULs/newlines are removed before matching/JSON
			description = sanitizeForPostgres(description)
			withdrawalStr := cleanAmount(row[colIdx[withdrawalAmtHeader]])
			depositStr := cleanAmount(row[colIdx[depositAmtHeader]])
			if withdrawalStr != "" && depositStr == "" {
				withdrawal.Valid = true
				fmt.Sscanf(withdrawalStr, "%f", &withdrawal.Float64)
				deposit.Valid = false
			} else if depositStr != "" && withdrawalStr == "" {
				deposit.Valid = true
				fmt.Sscanf(depositStr, "%f", &deposit.Float64)
				withdrawal.Valid = false
			} else {
				withdrawal.Valid = false
				deposit.Valid = false
				// Fallback: try to find a generic amount-like column and use Cr/Dr
				if amtIdx := findAmountLikeCSV(); amtIdx >= 0 && amtIdx < len(row) {
					amtStr := cleanAmount(row[amtIdx])
					if amtStr != "" {
						var amt float64
						fmt.Sscanf(amtStr, "%f", &amt)
						// Try to find a Cr/Dr indicator column name in the header map
						crIdx := -1
						for k, v := range colIdx {
							lk := strings.ToLower(k)
							if strings.Contains(lk, "cr/dr") || strings.Contains(lk, "crdr") || strings.Contains(lk, "cr / dr") || strings.TrimSpace(lk) == "cr" || strings.TrimSpace(lk) == "dr" || strings.Contains(lk, "credit/debit") || strings.Contains(lk, "debit/credit") {
								crIdx = v
								break
							}
							// also accept short headers like 'cr' or 'dr' or 'type'
							if crIdx == -1 && (strings.Contains(lk, "type") || strings.Contains(lk, "txn flow") || strings.Contains(lk, "dr/cr")) {
								crIdx = v
							}
						}
						if crIdx >= 0 && crIdx < len(row) {
							crdr := strings.ToLower(strings.TrimSpace(row[crIdx]))
							if strings.HasPrefix(crdr, "cr") || strings.Contains(crdr, "credit") || strings.HasPrefix(crdr, "c") {
								deposit.Valid = true
								deposit.Float64 = amt
							} else if strings.HasPrefix(crdr, "dr") || strings.Contains(crdr, "debit") || strings.HasPrefix(crdr, "d") {
								withdrawal.Valid = true
								withdrawal.Float64 = amt
							} else {
								// Unknown indicator: assume positive => deposit
								if strings.HasPrefix(amtStr, "-") {
									withdrawal.Valid = true
									withdrawal.Float64 = -amt
								} else {
									deposit.Valid = true
									deposit.Float64 = amt
								}
							}
						} else {
							// No Cr/Dr column: assume positive => deposit, negative => withdrawal
							if strings.HasPrefix(amtStr, "-") {
								withdrawal.Valid = true
								withdrawal.Float64 = -amt
							} else {
								deposit.Valid = true
								deposit.Float64 = amt
							}
						}
					}
				}
			}
			if hasBalance && balanceIdx >= 0 && balanceIdx < len(row) {
				balance = sql.NullFloat64{Valid: strings.TrimSpace(row[balanceIdx]) != ""}
				if balance.Valid {
					balanceStr := cleanAmount(row[balanceIdx])
					fmt.Sscanf(balanceStr, "%f", &balance.Float64)
				}
			} else {
				balance = sql.NullFloat64{Valid: false}
			}
			// Check for opening balance row
			if strings.Contains(strings.ToLower(description), constants.BalanceCarriedForward) {
				if balance.Valid {
					openingBalance = balance.Float64
					cumulative = openingBalance
					log.Printf("[BANK-UPLOAD-DEBUG] OPENING BALANCE detected: %.2f (initial cumulative set)", openingBalance)
				}
				continue
			}
			// Filter out rows only when both dates are invalid or explicitly marked as custom placeholders
			// If both dates are missing, fall back to the last seen valid value date before skipping.
			if valueDate.IsZero() && transactionDate.IsZero() && !lastValidValueDate.IsZero() {
				valueDate = lastValidValueDate
				transactionDate = lastValidValueDate
				if debugParse {
					log.Printf("[BANK-UPLOAD-DEBUG] Row %d: Filled missing dates with last valid date %s", rowNum, lastValidValueDate.Format(constants.DateFormat))
				}
			}

			if (valueDate.IsZero() && transactionDate.IsZero()) || (valueDateStr == constants.DateFormatCustom && transactionDateStr == constants.DateFormatCustom) {
				// Check if this is a closing balance row before skipping
				if strings.Contains(strings.ToLower(description), constants.ClosingBalance) {
					if balance.Valid {
						log.Printf("[BANK-UPLOAD-DEBUG] CLOSING BALANCE detected: %.2f (skipping from transactions)", balance.Float64)
					} else {
						log.Printf("[BANK-UPLOAD-DEBUG] CLOSING BALANCE row detected (no balance value)")
					}
					skippedClosingBalanceRows++
				}
				if debugParse {
					log.Printf("[BANK-UPLOAD-DEBUG] Skipping row %d due to missing dates (valueDate=%q transactionDate=%q) raw=%q", rowNum, valueDateStr, transactionDateStr, row)
				}
				skippedMissingDateRows++
				continue
			}
		} else {
			// Excel/XLS: Tran. Id, Value Date, Transaction Date, Transaction Remarks, Withdrawal Amt (INR), Deposit Amt (INR), Balance (INR)
			// if len(row) == 0 || (colIdx[tranIDHeader] >= len(row)) || strings.TrimSpace(row[colIdx[tranIDHeader]]) == "" {
			// 	continue
			// }
			if len(row) == 0 {
				skippedEmptyRows++
				continue
			}

			valueDateStr := ""
			if idx, ok := colIdx[valueDateHeader]; ok && idx < len(row) {
				valueDateStr = row[idx]
			}
			transactionDateStr := ""
			if idx, ok := colIdx[transactionDateHeader]; ok && idx < len(row) {
				transactionDateStr = row[idx]
			}
			postedDateStr := ""
			if idx, ok := colIdx["PostedDate"]; ok && idx < len(row) {
				postedDateStr = row[idx]
			}
			// Pre-read description text so we can use it for closing-balance detection even if we need to fill dates.
			tempDescLower := ""
			if descIdx, ok := colIdx[transactionRemarksHeader]; ok && descIdx < len(row) {
				tempDescLower = strings.ToLower(strings.TrimSpace(row[descIdx]))
			}
			sanitizeDateStr := func(s string) string {
				s = strings.ReplaceAll(s, "\x00", "")
				s = strings.ReplaceAll(s, constants.NBSP, " ")
				s = strings.Join(strings.Fields(s), " ")
				return strings.TrimSpace(s)
			}
			valueDateStr = sanitizeDateStr(valueDateStr)
			transactionDateStr = sanitizeDateStr(transactionDateStr)
			valueDate = tryParseDateWithExcelSerial(valueDateStr)
			transactionDate = tryParseDateWithExcelSerial(transactionDateStr)
			postedDate = tryParseDateWithExcelSerial(postedDateStr)
			// Prefer posted date when one of the dates is missing but posted date exists
			if valueDate.IsZero() && !postedDate.IsZero() {
				valueDate = postedDate
			}
			if transactionDate.IsZero() && !postedDate.IsZero() {
				transactionDate = postedDate
			}
			if valueDate.IsZero() && !transactionDate.IsZero() {
				valueDate = transactionDate
			}
			if transactionDate.IsZero() && !valueDate.IsZero() {
				transactionDate = valueDate
			}
			// If both are still zero but posted date exists, use posted date.
			if valueDate.IsZero() && transactionDate.IsZero() && !postedDate.IsZero() {
				valueDate = postedDate
				transactionDate = postedDate
				// postedDate is already populated; no further assignment required
			}

			// Extract TranID early so we can use it in fallback logic
			originalTranIDStr := ""
			if idx, ok := colIdx[tranIDHeader]; ok && idx >= 0 && idx < len(row) {
				originalTranIDStr = row[idx]
				tranID = sql.NullString{String: row[idx], Valid: strings.TrimSpace(row[idx]) != ""}
			} else {
				tranID = sql.NullString{Valid: false}
			}
			// Fallback: use cheque/ref number when Tran Id cell is empty
			if (!tranID.Valid || strings.TrimSpace(tranID.String) == "") && chequeRefIdx >= 0 && chequeRefIdx < len(row) {
				val := strings.TrimSpace(row[chequeRefIdx])
				if val != "" {
					tranID = sql.NullString{String: val, Valid: true}
				}
			}

			// If the Tran ID cell looks like a date/time and the Value Date cell is actually an ID (non-date), swap them.
			if tranID.Valid {
				if _, err := parseDate(tranID.String); err == nil {
					if valueDateStr != "" {
						if _, vErr := parseDate(valueDateStr); vErr != nil {
							tranID = sql.NullString{String: valueDateStr, Valid: true}
							if debugParse {
								log.Printf("[BANK-UPLOAD-DEBUG] Row %d: TranID looked like date, using ValueDate cell as tran_id instead: %q", rowNum, valueDateStr)
							}
						}
					}
				}
			}

			// If both dates failed to parse but the original Tran ID cell is date-like, use that as the date (keep tran_id swap intact).
			if valueDate.IsZero() && transactionDate.IsZero() && strings.TrimSpace(originalTranIDStr) != "" {
				if t, err := parseDate(originalTranIDStr); err == nil {
					valueDate = t
					transactionDate = t
					if debugParse {
						log.Printf("[BANK-UPLOAD-DEBUG] Row %d: Using original TranID cell for dates: %q", rowNum, originalTranIDStr)
					}
				}
			}
			// Last-resort OR option: if still no dates, scan all cells and pick the first parsable date string
			if valueDate.IsZero() && transactionDate.IsZero() {
				for _, cell := range row {
					c := sanitizeDateStr(cell)
					if c == "" {
						continue
					}
					if t, err := parseDate(c); err == nil {
						valueDate = t
						transactionDate = t
						if debugParse {
							log.Printf("[BANK-UPLOAD-DEBUG] Row %d: Fallback picked date from cell value %q", rowNum, c)
						}
						break
					}
				}
			}

			// If both dates are blank but we already saw a valid date earlier, carry it forward for statements that omit the date on subsequent rows.
			if valueDate.IsZero() && transactionDate.IsZero() && !lastValidValueDate.IsZero() {
				hasAmount := false
				if idx, ok := colIdx[withdrawalAmtHeader]; ok && idx < len(row) && strings.TrimSpace(row[idx]) != "" {
					hasAmount = true
				}
				if idx, ok := colIdx[depositAmtHeader]; ok && idx < len(row) && strings.TrimSpace(row[idx]) != "" {
					hasAmount = true
				}
				// Try to detect any amount-like column (contains "amount" or exact debit/credit labels)
				amountIdxLocal := -1
				for colName, idx := range colIdx {
					lc := strings.ToLower(strings.TrimSpace(colName))
					if strings.Contains(lc, "amount") {
						amountIdxLocal = idx
						break
					}
					if lc == "debit" || lc == "credit" || lc == constants.DebitAmount || lc == constants.CreditAmount {
						amountIdxLocal = idx
						break
					}
				}
				if amountIdxLocal >= 0 && amountIdxLocal < len(row) && strings.TrimSpace(row[amountIdxLocal]) != "" {
					hasAmount = true
				}
				hasDesc := tempDescLower != ""
				if hasAmount || hasDesc {
					valueDate = lastValidValueDate
					transactionDate = lastValidValueDate
					if postedDate.IsZero() {
						postedDate = lastValidValueDate
					}
					if debugParse {
						log.Printf("[BANK-UPLOAD-DEBUG] Row %d: Carried forward last valid date %s for blank date cells", rowNum, lastValidValueDate.Format(constants.DateFormat))
					}
				}
			}

			// Check for opening balance row
			if strings.Contains(tempDescLower, constants.BalanceCarriedForward) {
				// Extract opening balance from balance column
				if bIdx, ok := colIdx[balanceHeader]; ok && bIdx >= 0 && bIdx < len(row) {
					balanceStr := cleanAmount(row[bIdx])
					if balanceStr != "" {
						fmt.Sscanf(balanceStr, "%f", &openingBalance)
						cumulative = openingBalance
						log.Printf("[BANK-UPLOAD-DEBUG] OPENING BALANCE detected (Excel): %.2f", openingBalance)
						skippedOpeningBalanceRows++
						continue
					}
				}
				// Fallback: scan from the end for a numeric-looking cell
				for i := len(row) - 1; i >= 0; i-- {
					cand := cleanAmount(row[i])
					if cand != "" {
						fmt.Sscanf(cand, "%f", &openingBalance)
						cumulative = openingBalance
						log.Printf("[BANK-UPLOAD-DEBUG] OPENING BALANCE detected (Excel scan): %.2f", openingBalance)
						skippedOpeningBalanceRows++
						continue
					}
				}
				// If nothing numeric found, still skip as opening balance row
				skippedOpeningBalanceRows++
				continue
			}

			// Fallback: scan entire row for 'balance carried forward' in any cell
			for _, cell := range row {
				if strings.Contains(strings.ToLower(strings.TrimSpace(cell)), constants.BalanceCarriedForward) {
					// Prefer balance column
					if bIdx, ok := colIdx[balanceHeader]; ok && bIdx >= 0 && bIdx < len(row) {
						balanceStr := cleanAmount(row[bIdx])
						if balanceStr != "" {
							fmt.Sscanf(balanceStr, "%f", &openingBalance)
							cumulative = openingBalance
							log.Printf("[BANK-UPLOAD-DEBUG] OPENING BALANCE detected (Excel fallback): %.2f", openingBalance)
							skippedOpeningBalanceRows++
							goto continueRowExcel
						}
					}
					// Scan for last numeric
					for i := len(row) - 1; i >= 0; i-- {
						cand := cleanAmount(row[i])
						if cand != "" {
							fmt.Sscanf(cand, "%f", &openingBalance)
							cumulative = openingBalance
							log.Printf("[BANK-UPLOAD-DEBUG] OPENING BALANCE detected (Excel scan fallback): %.2f", openingBalance)
							skippedOpeningBalanceRows++
							goto continueRowExcel
						}
					}
					skippedOpeningBalanceRows++
					goto continueRowExcel
				}
			}

		continueRowExcel:
			_ = 0

			// NOW check if we should skip - AFTER all fallback attempts
			if (valueDate.IsZero() && transactionDate.IsZero()) || (valueDateStr == constants.DateFormatCustom && transactionDateStr == constants.DateFormatCustom) {
				// Check if this is a closing balance row before skipping
				if strings.Contains(tempDescLower, constants.ClosingBalance) || tempDescLower == constants.ClosingBalance {
					// Extract closing balance value
					if bIdx, ok := colIdx[balanceHeader]; ok && bIdx >= 0 && bIdx < len(row) {
						balanceStr := cleanAmount(row[bIdx])
						var closingBal float64
						if balanceStr != "" {
							fmt.Sscanf(balanceStr, "%f", &closingBal)
							log.Printf("[BANK-UPLOAD-DEBUG] CLOSING BALANCE detected: %.2f (skipping from transactions)", closingBal)
						}
					}
					skippedClosingBalanceRows++
				}
				if debugParse {
					log.Printf("[BANK-UPLOAD-DEBUG] Skipping row %d due to missing dates (valueDate=%q transactionDate=%q) raw=%q", rowNum, valueDateStr, transactionDateStr, row)
				}
				skippedMissingDateRows++
				continue
			}
			if descIdx, ok := colIdx[transactionRemarksHeader]; ok && descIdx >= 0 && descIdx < len(row) {
				description = row[descIdx]
			} else {
				description = ""
			}
			// If remarks cell is empty but a Description column exists, fall back to it to avoid losing narration.
			if strings.TrimSpace(description) == "" {
				if descIdx, ok := colIdx["Description"]; ok && descIdx >= 0 && descIdx < len(row) {
					description = row[descIdx]
				}
			}
			// sanitize early so any NULs/newlines are removed before matching/JSON
			description = sanitizeForPostgres(description)

			// Debug: Check for empty/whitespace-only descriptions
			if debugParse && debugCount < 30 {
				trimmedDesc := strings.TrimSpace(description)
				if trimmedDesc == "" {
					log.Printf("[BANK-UPLOAD-DEBUG] Row %d: Description is EMPTY (or whitespace only). Raw: '%s'", debugCount, description)
				} else {
					log.Printf("[BANK-UPLOAD-DEBUG] Row %d: Description='%s'", debugCount, trimmedDesc)
				}
			}
			// Determine amount using available columns. Some statements use separate
			// Withdrawal/Deposit columns; others use a single Amount column with a
			// Cr/Dr indicator. Handle both cases.
			idxW, okW := colIdx[withdrawalAmtHeader]
			idxD, okD := colIdx[depositAmtHeader]
			idxCr, okCr := colIdx["CrDr"]

			// If both withdrawal and deposit map to same index (single amount column)
			// and a Cr/Dr column exists, use the Cr/Dr indicator to decide sign.
			if okW && okD && idxW == idxD && okCr && idxCr >= 0 && idxCr < len(row) {
				amtStr := cleanAmount(row[idxW])
				crdr := strings.ToLower(strings.TrimSpace(row[idxCr]))
				if amtStr != "" {
					var amt float64
					fmt.Sscanf(amtStr, "%f", &amt)
					if strings.HasPrefix(crdr, "cr") || strings.Contains(crdr, "credit") || strings.HasPrefix(crdr, "c") {
						deposit.Valid = true
						deposit.Float64 = amt
					} else if strings.HasPrefix(crdr, "dr") || strings.Contains(crdr, "debit") || strings.HasPrefix(crdr, "d") {
						withdrawal.Valid = true
						withdrawal.Float64 = amt
					} else {
						// Unknown indicator: treat positive as deposit, negative as withdrawal
						if strings.HasPrefix(amtStr, "-") {
							withdrawal.Valid = true
							withdrawal.Float64 = -amt
						} else {
							deposit.Valid = true
							deposit.Float64 = amt
						}
					}
				}
			} else {
				// Default behaviour: separate Withdrawal and Deposit columns
				var withdrawalStr, depositStr string
				if okW && idxW < len(row) {
					withdrawalStr = cleanAmount(row[idxW])
				}
				if okD && idxD < len(row) {
					depositStr = cleanAmount(row[idxD])
				}
				if withdrawalStr != "" && depositStr == "" {
					withdrawal.Valid = true
					fmt.Sscanf(withdrawalStr, "%f", &withdrawal.Float64)
					deposit.Valid = false
				} else if depositStr != "" && withdrawalStr == "" {
					deposit.Valid = true
					fmt.Sscanf(depositStr, "%f", &deposit.Float64)
					withdrawal.Valid = false
				} else {
					withdrawal.Valid = false
					deposit.Valid = false
				}
			}
			if bIdx, ok := colIdx[balanceHeader]; ok && bIdx >= 0 && bIdx < len(row) {
				balance = sql.NullFloat64{Valid: strings.TrimSpace(row[bIdx]) != ""}
				if balance.Valid {
					balanceStr := cleanAmount(row[bIdx])
					fmt.Sscanf(balanceStr, "%f", &balance.Float64)
				}
			} else {
				balance = sql.NullFloat64{Valid: false}
			}
		}
		rowJSON, _ := json.Marshal(row)

		// Detailed debug logging: show what we read and what we filled
		if debugParse && (debugCount < 200 || (!withdrawal.Valid && !deposit.Valid)) {
			// helper to safe-get header name at index
			// safeHeader := func(idx int) string {
			// 	if idx >= 0 && idx < len(headerRow) {
			// 		return headerRow[idx]
			// 	}
			// 	return "-"
			// }
			wIdx := -1
			dIdx := -1
			bIdx := -1
			if v, ok := colIdx[withdrawalAmtHeader]; ok {
				wIdx = v
			}
			if v, ok := colIdx[depositAmtHeader]; ok {
				dIdx = v
			}
			if v, ok := colIdx[balanceHeader]; ok {
				bIdx = v
			}
			// raw strings (safe indices)
			rawW := ""
			rawD := ""
			rawB := ""
			if wIdx >= 0 && wIdx < len(row) {
				rawW = row[wIdx]
			}
			if dIdx >= 0 && dIdx < len(row) {
				rawD = row[dIdx]
			}
			if bIdx >= 0 && bIdx < len(row) {
				rawB = row[bIdx]
			}
			log.Printf("[BANK-UPLOAD-DEBUG] parsed-row num=%d headerRow=%q mapping(Date=%d Description=%d Withdrawal=%d Deposit=%d Balance=%d TranID=%d) rawRow=%q\n  rawWithdrawal=%q rawDeposit=%q rawBalance=%q parsedWithdrawal.Valid=%v parsedWithdrawal=%v parsedDeposit.Valid=%v parsedDeposit=%v description=%q sanitizedDescription=%q",
				rowNum,
				headerRow,
				colIdx["Date"], colIdx["Description"], wIdx, dIdx, bIdx, colIdx[tranIDHeader],
				row,
				rawW, rawD, rawB,
				withdrawal.Valid, withdrawal.Float64, deposit.Valid, deposit.Float64,
				row[colIdx["Description"]], sanitizeForPostgres(row[colIdx["Description"]]),
			)
			debugCount++
		}

		// --- CATEGORY MATCHING ---
		matchedCategoryID := matchCategoryForTransaction(rules, description, withdrawal, deposit)
		if matchedCategoryID.Valid {
			categoryCount[matchedCategoryID.Int64]++
			if withdrawal.Valid {
				debitSum[matchedCategoryID.Int64] += withdrawal.Float64
			}
			if deposit.Valid {
				creditSum[matchedCategoryID.Int64] += deposit.Float64
			}
		} else {
			uncategorized = append(uncategorized, map[string]interface{}{
				"tran_id":     tranID.String,
				"description": description,
				"value_date":  valueDate,
				"amount":      map[string]interface{}{"withdrawal": withdrawal.Float64, "deposit": deposit.Float64},
			})
		}
		// Recalculate balance using a running cumulative that starts from the first available balance.
		// This prevents double-counting the first row amounts.
		origBalance := balance

		if firstValidRow {
			// Initialize cumulative from the first valid balance if present; otherwise from opening balance or zero.
			if origBalance.Valid {
				cumulative = origBalance.Float64
				if openingBalance == 0 {
					openingBalance = origBalance.Float64
				}
			} else {
				// No balance provided on the first row; seed cumulative from openingBalance if known.
				cumulative = openingBalance
			}
			firstValidRow = false
			effectiveStart := valueDate
			if effectiveStart.IsZero() {
				effectiveStart = transactionDate
			}
			if effectiveStart.IsZero() {
				effectiveStart = postedDate
			}
			if effectiveStart.IsZero() {
				effectiveStart = lastValidValueDate
			}
			if !effectiveStart.IsZero() {
				statementPeriodStart = effectiveStart
			}
		} else {
			// Apply this row's amounts to the running cumulative.
			if deposit.Valid {
				cumulative += deposit.Float64
			}
			if withdrawal.Valid {
				cumulative -= withdrawal.Float64
			}
		}

		// If the file provides a balance for this row, trust it to keep in sync with bank statement; otherwise use computed cumulative.
		if origBalance.Valid {
			cumulative = origBalance.Float64
			balance = origBalance
		} else {
			balance = sql.NullFloat64{Valid: true, Float64: cumulative}
		}
		// Ensure a non-empty tran_id so downstream consumers see stable identifiers even when the file omits them.
		if !tranID.Valid || strings.TrimSpace(tranID.String) == "" {
			tranID = sql.NullString{Valid: true, String: fmt.Sprintf("M%s", strings.TrimSpace(fmt.Sprintf("%d", rowNum)))}
		}

		keptRows++
		transactions = append(transactions, BankStatementTransaction{
			AccountNumber:    accountNumber,
			TranID:           tranID,
			ValueDate:        valueDate,
			TransactionDate:  transactionDate,
			PostedDate:       sql.NullTime{Valid: !postedDate.IsZero(), Time: postedDate},
			Description:      description,
			WithdrawalAmount: withdrawal,
			DepositAmount:    deposit,
			Balance:          balance,
			RawJSON:          rowJSON,
			CategoryID:       matchedCategoryID,
		})
		// Track the latest date seen (files are not always sorted chronologically).
		// Prefer value_date as the authoritative transaction date; fall back to transaction_date,
		// then posted_date only if value_date is missing. This prevents posted timestamps from
		// incorrectly extending the statement period.
		candidateDate := valueDate
		if candidateDate.IsZero() {
			candidateDate = transactionDate
		}
		if candidateDate.IsZero() {
			candidateDate = postedDate
		}
		if !candidateDate.IsZero() && (lastValidValueDate.IsZero() || candidateDate.After(lastValidValueDate)) {
			lastValidValueDate = candidateDate
		}
		if balance.Valid && !strings.Contains(description, "000551000101") {
			closingBalance = balance.Float64
		}
	}
	statementPeriodEnd = lastValidValueDate
	// If period not found by old logic, use CSV period if available
	if statementPeriodStart.IsZero() && !csvPeriodStart.IsZero() {
		statementPeriodStart = csvPeriodStart
	}
	if statementPeriodEnd.IsZero() && !csvPeriodEnd.IsZero() {
		statementPeriodEnd = csvPeriodEnd
	}
	// Fallback: derive start/end from any date fields on transactions if still zero
	if len(transactions) > 0 {
		var firstTxnDate, lastTxnDate time.Time
		updateBounds := func(t time.Time) {
			if t.IsZero() {
				return
			}
			if firstTxnDate.IsZero() || t.Before(firstTxnDate) {
				firstTxnDate = t
			}
			if lastTxnDate.IsZero() || t.After(lastTxnDate) {
				lastTxnDate = t
			}
		}
		for _, txn := range transactions {
			updateBounds(txn.ValueDate)
			updateBounds(txn.TransactionDate)
			if txn.PostedDate.Valid {
				updateBounds(txn.PostedDate.Time)
			}
		}
		if statementPeriodStart.IsZero() && !firstTxnDate.IsZero() {
			statementPeriodStart = firstTxnDate
		}
		if statementPeriodEnd.IsZero() && !lastTxnDate.IsZero() {
			statementPeriodEnd = lastTxnDate
		}
	}
	log.Printf("[BANK-UPLOAD-DEBUG] Parse summary: rows_after_header=%d kept=%d skipped_empty=%d skipped_non_txn=%d skipped_missing_dates=%d skipped_opening_balance=%d skipped_closing_balance=%d",
		totalRows, keptRows, skippedEmptyRows, skippedNonTxnRows, skippedMissingDateRows, skippedOpeningBalanceRows, skippedClosingBalanceRows)

	// If opening balance was detected but first transaction has balance 0, recalculate all balances starting from opening balance
	if openingBalance != 0 && len(transactions) > 0 && (!transactions[0].Balance.Valid || transactions[0].Balance.Float64 == 0) {
		cumulative = openingBalance
		for i := 0; i < len(transactions); i++ {
			transactions[i].Balance = sql.NullFloat64{Valid: true, Float64: cumulative}
			if transactions[i].DepositAmount.Valid {
				cumulative += transactions[i].DepositAmount.Float64
			}
			if transactions[i].WithdrawalAmount.Valid {
				cumulative -= transactions[i].WithdrawalAmount.Float64
			}
		}
		log.Printf("[BANK-UPLOAD-DEBUG] Recalculated balances starting from opening balance %.2f", openingBalance)
	}

	// Preload existing transactions for this account so we can identify which
	// rows from the uploaded file are truly new vs already present.
	existingTxnKeys := make(map[string]bool)
	rowsExisting, err := db.QueryContext(ctx, `
			SELECT account_number, transaction_date, description, withdrawal_amount, deposit_amount
			FROM cimplrcorpsaas.bank_statement_transactions
			WHERE account_number = $1
		`, accountNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing transactions: %w", err)
	}
	defer rowsExisting.Close()
	for rowsExisting.Next() {
		var acc string
		var txDate time.Time
		var desc string
		var wAmt, dAmt sql.NullFloat64
		if scanErr := rowsExisting.Scan(&acc, &txDate, &desc, &wAmt, &dAmt); scanErr != nil {
			continue
		}
		k := buildTxnKey(acc, txDate, desc, wAmt, dAmt)
		existingTxnKeys[k] = true
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin db transaction: %w", err)
	}
	defer tx.Rollback()

	var bankStatementID string
	err = tx.QueryRowContext(ctx, `
		      INSERT INTO cimplrcorpsaas.bank_statements (
			      entity_id, account_number, statement_period_start, statement_period_end, file_hash, opening_balance, closing_balance
		      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
		      ON CONFLICT ON CONSTRAINT uniq_stmt
		      DO UPDATE SET
			      file_hash = EXCLUDED.file_hash,
			      closing_balance = EXCLUDED.closing_balance
		      RETURNING bank_statement_id
		  `, entityID, accountNumber, statementPeriodStart, statementPeriodEnd, fileHash, openingBalance, closingBalance).Scan(&bankStatementID)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf(constants.ErrFailedToInsertBankStatement, err)
	}

	// Bulk insert transactions for speed, skip duplicates
	if len(transactions) > 0 {
		// Classify transactions into new vs already existing using DATABASE state
		// only. We assume each row in a single uploaded file is already unique
		// with respect to the uniq_transaction constraint, so we do NOT
		// deduplicate within this file here.
		newTransactions := make([]BankStatementTransaction, 0, len(transactions))
		for _, t := range transactions {
			k := buildTxnKey(t.AccountNumber, t.TransactionDate, t.Description, t.WithdrawalAmount, t.DepositAmount)
			if existingTxnKeys[k] {
				// Already present in DB from earlier statements -> under review
				reviewTransactions = append(reviewTransactions, map[string]interface{}{
					"account_number":    t.AccountNumber,
					"tran_id":           t.TranID.String,
					"value_date":        t.ValueDate,
					"transaction_date":  t.TransactionDate,
					"description":       t.Description,
					"withdrawal_amount": t.WithdrawalAmount.Float64,
					"deposit_amount":    t.DepositAmount.Float64,
					"balance":           t.Balance.Float64,
					"category_id":       t.CategoryID.Int64,
				})
				continue
			}
			// Not present in DB -> treat as a new transaction to insert.
			newTransactions = append(newTransactions, t)
		}

		existingInDBCount := len(reviewTransactions)
		newInsertCount := len(newTransactions)
		log.Printf("[BANK-UPLOAD-DEBUG] Dedup summary: parsed=%d existing_in_db=%d new_to_insert=%d", len(transactions), existingInDBCount, newInsertCount)

		if len(newTransactions) > 0 {
			valueStrings := make([]string, 0, len(newTransactions))
			valueArgs := make([]interface{}, 0, len(newTransactions)*11)
			for i, t := range newTransactions {
				valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
					i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11))
				valueArgs = append(valueArgs,
					bankStatementID,
					// account_number
					t.AccountNumber,
					sanitizeForPostgres(t.TranID.String),
					t.ValueDate,
					t.TransactionDate,
					sanitizeForPostgres(t.Description),
					t.WithdrawalAmount,
					t.DepositAmount,
					t.Balance,
					sanitizeForPostgres(string(t.RawJSON)),
					t.CategoryID,
				)
			}
			stmt := `INSERT INTO cimplrcorpsaas.bank_statement_transactions (
				       bank_statement_id, account_number, tran_id, value_date, transaction_date, description, withdrawal_amount, deposit_amount, balance, raw_json, category_id
			       ) VALUES ` +
				joinStrings(valueStrings, ",") +
				` ON CONFLICT (account_number, transaction_date, description, withdrawal_amount, deposit_amount) DO NOTHING`
			log.Printf("[BANK-UPLOAD-DEBUG] Attempting to insert %d new transactions", len(newTransactions))
			if _, err := tx.ExecContext(ctx, stmt, valueArgs...); err != nil {
				tx.Rollback()
				log.Printf("[BANK-UPLOAD-DEBUG] Bulk insert FAILED. First 3 descriptions:")
				for i := 0; i < 3 && i < len(newTransactions); i++ {
					log.Printf("[BANK-UPLOAD-DEBUG]   txn[%d] desc=%q (sanitized=%q)", i, newTransactions[i].Description, sanitizeForPostgres(newTransactions[i].Description))
				}
				return nil, fmt.Errorf("failed to bulk insert transactions: %w", err)
			}
		}
	}
	// Report the number of transactions present in the uploaded file (after
	// parsing), regardless of whether some were duplicates within the same
	// file. This matches the original behaviour/expectation that
	// transactions_uploaded_count reflects all rows processed from the upload.
	uploadedCount = len(transactions)

	// Insert audit action for this bank statement
	requestedBy := "system"
	if s := middlewares.GetSessionFromContext(ctx); s != nil {
		if strings.TrimSpace(s.Name) != "" {
			requestedBy = s.Name
		} else if strings.TrimSpace(s.UserID) != "" {
			requestedBy = s.UserID
		}
	}
	_, err = tx.ExecContext(ctx, `
			       INSERT INTO cimplrcorpsaas.auditactionbankstatement (
				       bankstatementid, actiontype, processing_status, requested_by, requested_at
			       ) VALUES ($1, $2, $3, $4, $5)
		       `, bankStatementID, "CREATE", "PENDING_APPROVAL", requestedBy, time.Now())
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf(constants.ErrFailedToInsertAuditAction, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	// KPIs and category details
	kpiCats := []map[string]interface{}{}
	foundCategories := []map[string]interface{}{}
	foundCategoryIDs := map[int64]bool{}
	totalTxns := len(transactions)
	groupedTxns := 0
	ungroupedTxns := 0

	categoryTxns := map[int64][]map[string]interface{}{}

	for i, t := range transactions {
		if t.CategoryID.Valid {
			groupedTxns++
			catID := t.CategoryID.Int64
			categoryTxns[catID] = append(categoryTxns[catID], map[string]interface{}{
				"index":             i,
				"tran_id":           t.TranID.String,
				"value_date":        t.ValueDate,
				"transaction_date":  t.TransactionDate,
				"description":       t.Description,
				"withdrawal_amount": t.WithdrawalAmount.Float64,
				"deposit_amount":    t.DepositAmount.Float64,
				"balance":           t.Balance.Float64,
				"category_id":       catID,
			})
		} else {
			ungroupedTxns++
		}
	}

	for catID, count := range categoryCount {
		var catName string
		for _, rule := range rules {
			if rule.CategoryID == catID {
				catName = rule.CategoryName
				break
			}
		}
		kpiCats = append(kpiCats, map[string]interface{}{
			"category_id":   catID,
			"category_name": catName,
			"count":         count,
			"debit_sum":     debitSum[catID],
			"credit_sum":    creditSum[catID],
			"transactions":  categoryTxns[catID],
		})
		foundCategoryIDs[catID] = true
	}

	groupedPct := 0.0
	ungroupedPct := 0.0
	if totalTxns > 0 {
		groupedPct = float64(groupedTxns) * 100.0 / float64(totalTxns)
		ungroupedPct = float64(ungroupedTxns) * 100.0 / float64(totalTxns)
	}

	// Add category names/types for found categories
	for _, rule := range rules {
		if foundCategoryIDs[rule.CategoryID] {
			foundCategories = append(foundCategories, map[string]interface{}{
				"category_id":   rule.CategoryID,
				"category_name": rule.CategoryName,
				"category_type": rule.CategoryType,
			})
			delete(foundCategoryIDs, rule.CategoryID)
		}
	}
	result := map[string]interface{}{
		"pages_processed":                 1, // Excel = 1 sheet
		"bank_wise_status":                []map[string]interface{}{{"account_number": accountNumber, "status": "SUCCESS"}},
		"statement_date_coverage":         map[string]interface{}{"start": transactions[0].ValueDate, "end": statementPeriodEnd},
		"category_kpis":                   kpiCats,
		"categories_found":                foundCategories,
		"uncategorized":                   uncategorized,
		"bank_statement_id":               bankStatementID,
		"transactions_uploaded_count":     uploadedCount,
		"transactions_under_review_count": len(reviewTransactions),
		"transactions_under_review":       reviewTransactions,
		"grouped_transaction_count":       groupedTxns,
		"ungrouped_transaction_count":     ungroupedTxns,
		"grouped_transaction_percent":     groupedPct,
		"ungrouped_transaction_percent":   ungroupedPct,
		"file_storage_key":                s3Key,
		"file_storage_url":                s3URL,
	}
	return result, nil
}

// UploadMultiAccountBankStatementHandler parses a single CSV file containing rows
// for multiple accounts, groups rows by account_number and invokes the existing
// categorization pipeline for each account independently.
//
// Behaviour:
// - Expects a multipart form upload with a single CSV file field (commonly `file`).
// - Requires `multi=true` form field to be set; UploadBankStatementV2Handler will
//   delegate to this handler when present.
// - Parses CSV (robust to header name variations), groups rows by account number,
//   builds a per-account CSV, computes idempotent `fileHash = sha256(csvBytes + account_number)`,
//   and calls UploadBankStatementV2WithCategorization for each account.
// - Aggregates results per-account and isolates failures to the account level.
func UploadMultiAccountBankStatementHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		if r.Method != http.MethodPost {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Only POST method is allowed for this endpoint.",
			})
			return
		}

		// Ensure multipart form is parsed
		if r.MultipartForm == nil {
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Unable to read the uploaded file. Please try again."})
				return
			}
		}

		// Find the uploaded file (same tolerant logic as single-file handler)
		var file multipart.File
		var fhHeader *multipart.FileHeader
		var err error
		file, fhHeader, err = r.FormFile("file")
		if err != nil || file == nil {
			// try alternative field names
			if file == nil {
				file, fhHeader, err = r.FormFile("statement")
			}
			if err != nil || file == nil {
				file, fhHeader, err = r.FormFile("bankStatement")
			}
			if err != nil || file == nil {
				// try first available file field
				if r.MultipartForm != nil && len(r.MultipartForm.File) > 0 {
					for _, files := range r.MultipartForm.File {
						if len(files) > 0 {
							fhHeader = files[0]
							file, err = fhHeader.Open()
							break
						}
					}
				}
			}
		}
		if err != nil || file == nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "File not found in request. Please attach a CSV file using the 'file' field in form-data."})
			return
		}
		defer file.Close()

		// Read file bytes
		fileBytes, err := io.ReadAll(file)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Failed to read the uploaded file. Please try again."})
			return
		}

		// Parse CSV
		rdr := csv.NewReader(bytes.NewReader(fileBytes))
		rdr.FieldsPerRecord = -1
		rows, err := rdr.ReadAll()
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Unable to parse CSV file. Please ensure it is a valid CSV."})
			return
		}
		if len(rows) < 1 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "CSV contains no rows."})
			return
		}

		header := rows[0]
		// helper to find header indices (case-insensitive, substring match)
		findIdx := func(keywords ...string) int {
			for i, h := range header {
				lc := strings.ToLower(strings.TrimSpace(h))
				for _, kw := range keywords {
					if strings.Contains(lc, strings.ToLower(kw)) {
						return i
					}
				}
			}
			return -1
		}

		accIdx := findIdx("account number", "account_no", "account")
		dateIdx := findIdx("transaction date", "statement date", "date")
		valDateIdx := findIdx("value date", "value-date", "value")
		descIdx := findIdx("transaction description", "description", "remarks", "narration")
		// prefer explicit Extra Information column when available
		extraInfoIdx := findIdx("extra information", "extra", "extra-info", "extra_info")
		debitIdx := findIdx("debit", "debit amount", "withdrawal")
		creditIdx := findIdx("credit", "credit amount", "deposit")
		openingIdx := findIdx("opening", "opening available", "opening available balance")
		closingIdx := findIdx("closing", "closing available", "current / closing")

		if accIdx == -1 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "CSV must contain an account number column."})
			return
		}

		// Group rows by account number
		groups := make(map[string][][]string)
		for ri := 1; ri < len(rows); ri++ {
			row := rows[ri]
			// guard length
			if accIdx >= len(row) {
				continue
			}
			acc := strings.TrimSpace(row[accIdx])
			if acc == "" {
				continue
			}
			groups[acc] = append(groups[acc], row)
		}

		ctx := r.Context()
		results := make(map[string]interface{})

		// Process each account independently
		for account, grecs := range groups {
			// Build structured input per-account by normalizing rows into transactions.
			stm := StructuredBankStatement{
				AccountNumber: account,
				BankName:      "",
				Transactions:  []StructuredTransaction{},
			}

			var rowsTxns []struct {
				dt        time.Time
				valueDate time.Time
				desc      string
				withdrawal float64
				deposit   float64
				rawClose  string
			}
			var minDate, maxDate time.Time
			var openingBalance, closingBalance float64
			openingFound := false

			// First pass: collect rows into temporary slice and capture opening/closing if present
			for _, rrow := range grecs {
				get := func(idx int) string {
					if idx >= 0 && idx < len(rrow) {
						return strings.TrimSpace(rrow[idx])
					}
					return ""
				}
				// Prefer value date when available
				dateCell := get(dateIdx)
				if valDateIdx >= 0 {
					if v := get(valDateIdx); v != "" {
						dateCell = v
					}
				}
				dt := tryParseDateWithExcelSerial(dateCell)
				if dt.IsZero() {
					// skip unparsable rows
					continue
				}

				// description: prefer Extra Information column if present
				descCell := ""
				if extraInfoIdx >= 0 {
					descCell = get(extraInfoIdx)
				}
				if descCell == "" {
					descCell = get(descIdx)
				}
				descCell = sanitizeForPostgres(descCell)

				debitStr := cleanAmount(get(debitIdx))
				creditStr := cleanAmount(get(creditIdx))
				closeStr := cleanAmount(get(closingIdx))

				var w, d float64
				if debitStr != "" {
					if v, err := strconv.ParseFloat(debitStr, 64); err == nil {
						w = v
					}
				}
				if creditStr != "" {
					if v, err := strconv.ParseFloat(creditStr, 64); err == nil {
						d = v
					}
				}

				rowsTxns = append(rowsTxns, struct {
					dt        time.Time
					valueDate time.Time
					desc      string
					withdrawal float64
					deposit   float64
					rawClose  string
				}{dt: dt, valueDate: dt, desc: descCell, withdrawal: w, deposit: d, rawClose: closeStr})

				if minDate.IsZero() || dt.Before(minDate) {
					minDate = dt
				}
				if maxDate.IsZero() || dt.After(maxDate) {
					maxDate = dt
				}

				// opening balance: read from first non-empty Opening column if available
				if !openingFound && openingIdx >= 0 {
					opStr := cleanAmount(get(openingIdx))
					if opStr != "" {
						if v, err := strconv.ParseFloat(opStr, 64); err == nil {
							openingBalance = v
							openingFound = true
						}
					}
				}
				// closing balance: capture last non-empty closing column
				if closeStr != "" {
					if v, err := strconv.ParseFloat(closeStr, 64); err == nil {
						closingBalance = v
					}
				}
			}

			if len(rowsTxns) == 0 {
				results[account] = map[string]interface{}{"success": false, "message": "No parseable transactions for account"}
				continue
			}

			// Sort transactions by date ascending to compute running balance
			sort.Slice(rowsTxns, func(i, j int) bool { return rowsTxns[i].dt.Before(rowsTxns[j].dt) })

			// If opening balance wasn't found in opening column, try to derive from first row's rawClose and deltas
			running := 0.0
			if openingFound {
				running = openingBalance
			} else {
				// start from 0 and compute; we'll set opening to first computed running later
				running = 0.0
			}

			// Build structured transactions with cumulative balance
			for _, rt := range rowsTxns {
				running = running - rt.withdrawal + rt.deposit
				txn := StructuredTransaction{
					TransactionDate: rt.dt.Format(constants.DateFormat),
					ValueDate:       rt.valueDate.Format(constants.DateFormat),
					Description:     rt.desc,
					Withdrawal:      rt.withdrawal,
					Deposit:         rt.deposit,
					Balance:         running,
				}
				stm.Transactions = append(stm.Transactions, txn)
			}

			// finalize period and balances
			stm.PeriodStart = minDate.Format(constants.DateFormat)
			stm.PeriodEnd = maxDate.Format(constants.DateFormat)
			if openingFound {
				stm.OpeningBalance = openingBalance
			} else {
				// when opening not provided, set to running minus sum of transactions
				// compute opening as first running minus first txn effect
				if len(stm.Transactions) > 0 {
					first := stm.Transactions[0]
					stm.OpeningBalance = first.Balance - (first.Deposit - first.Withdrawal)
				}
			}
			// prefer explicit captured closingBalance if present, else use running
			if closingBalance != 0 {
				stm.ClosingBalance = closingBalance
			} else if len(stm.Transactions) > 0 {
				stm.ClosingBalance = stm.Transactions[len(stm.Transactions)-1].Balance
			}

			// compute idempotent fileHash = sha256(serialized_transactions + account_number)
			// Use the original file bytes for idempotency surface too (stable)
			var txnBuf bytes.Buffer
			enc := json.NewEncoder(&txnBuf)
			_ = enc.Encode(stm.Transactions)
			h := sha256.New()
			h.Write(txnBuf.Bytes())
			h.Write([]byte(account))
			fileHash := fmt.Sprintf("%x", h.Sum(nil))

			// Call structured ingestion
			res, upErr := ProcessBankStatementFromStructuredInput(ctx, db, stm, fileHash)
			if upErr != nil {
				results[account] = map[string]interface{}{"success": false, "message": userFriendlyUploadError(upErr)}
				continue
			}
			results[account] = map[string]interface{}{"success": true, "data": res}
		}

		// Return aggregated results
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    results,
		})
	})
}

// 1. Get all bank statements (POST, req: user_id)
func GetAllBankStatementsHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			http.Error(w, "Missing or invalid user_id in body", http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		entityIDs := apictx.GetEntityIDsFromCtx(ctx)
		if len(entityIDs) == 0 {
			http.Error(w, "No accessible business units found", http.StatusUnauthorized)
			return
		}
		rows, err := db.QueryContext(ctx, `
										WITH latest_audit AS (
											SELECT a.*
											FROM cimplrcorpsaas.auditactionbankstatement a
											INNER JOIN (
												SELECT bankstatementid, MAX(action_id) AS max_action_id
												FROM cimplrcorpsaas.auditactionbankstatement
												GROUP BY bankstatementid
											) b ON a.bankstatementid = b.bankstatementid AND a.action_id = b.max_action_id
										)
										SELECT s.bank_statement_id, e.entity_name, s.account_number, s.statement_period_start, s.statement_period_end, s.opening_balance, s.closing_balance, s.uploaded_at,
													 la.actiontype, la.processing_status, la.action_id, la.requested_by, la.requested_at, la.checker_by, la.checker_at, la.checker_comment, la.reason,
														COALESCE(mb.bank_name, '') AS bank_name,
														mba.account_nickname AS account_nickname
										FROM cimplrcorpsaas.bank_statements s
										JOIN public.masterentitycash e ON s.entity_id = e.entity_id
										LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND mba.is_deleted = false
										LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
										LEFT JOIN latest_audit la ON la.bankstatementid = s.bank_statement_id
										WHERE s.entity_id = ANY($1)
										ORDER BY s.uploaded_at DESC
						`, pq.Array(entityIDs))
		if err != nil {
			http.Error(w, pqUserFriendlyMessage(err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		resp := []map[string]interface{}{}
		for rows.Next() {
			var id, entityName, acc string
			var start, end, uploaded time.Time
			var open, close float64
			var actionType, processingStatus, actionID, requestedBy, checkerBy, checkerComment, reason sql.NullString
			var bankName sql.NullString
			var accountNickname sql.NullString
			var requestedAt, checkerAt sql.NullTime
			if err := rows.Scan(&id, &entityName, &acc, &start, &end, &open, &close, &uploaded,
				&actionType, &processingStatus, &actionID, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason, &bankName, &accountNickname); err != nil {
				continue
			}
			// Add computed field for delete pending approval
			isDeletePending := false
			if actionType.String == "DELETE" && processingStatus.String == "DELETE_PENDING_APPROVAL" {
				isDeletePending = true
			}
			resp = append(resp, map[string]interface{}{
				"bank_statement_id":          id,
				"entity_name":                entityName,
				"account_number":             acc,
				"statement_period_start":     start,
				"statement_period_end":       end,
				"opening_balance":            open,
				"closing_balance":            close,
				"uploaded_at":                uploaded,
				"action_type":                actionType.String,
				"processing_status":          processingStatus.String,
				"action_id":                  actionID.String,
				"requested_by":               requestedBy.String,
				"requested_at":               requestedAt.Time,
				"checker_by":                 checkerBy.String,
				"checker_at":                 checkerAt.Time,
				"checker_comment":            checkerComment.String,
				"reason":                     reason.String,
				"bank_name":                  bankName.String,
				"account_nickname":           accountNickname.String,
				"is_delete_pending_approval": isDeletePending,
			})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    resp,
		})
	})

	// 2. Get all transactions for a bank statement (POST, req: user_id, bank_statement_id)
}

func GetBankStatementTransactionsHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			UserID          string `json:"user_id"`
			BankStatementID string `json:"bank_statement_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
			body.UserID == "" || body.BankStatementID == "" {
			http.Error(w, "Missing user_id or bank_statement_id", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		entityIDs := apictx.GetEntityIDsFromCtx(ctx)
		if len(entityIDs) == 0 {
			http.Error(w, "No accessible business units found", http.StatusUnauthorized)
			return
		}
		rows, err := db.QueryContext(ctx, `
			SELECT
				t.transaction_id,
				e.entity_name,
				t.tran_id,
				t.value_date,
				t.transaction_date,
				t.description,
				t.withdrawal_amount,
				t.deposit_amount,
				t.balance,
				c.category_name,
				COALESCE(t.misclassified_flag, false) AS misclassified_flag
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements s
				ON t.bank_statement_id = s.bank_statement_id
			JOIN public.masterentitycash e
				ON s.entity_id = e.entity_id
			LEFT JOIN cimplrcorpsaas.transaction_categories c
				ON t.category_id = c.category_id
			WHERE t.bank_statement_id = $1
			  AND s.entity_id = ANY($2)
			ORDER BY t.value_date
		`, body.BankStatementID, pq.Array(entityIDs))

		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		resp := []map[string]interface{}{}

		for rows.Next() {
			var (
				tid           int64
				entityName    string
				tranID        sql.NullString
				desc          string
				category      sql.NullString
				vdate         time.Time
				tdate         time.Time
				withdrawal    sql.NullFloat64
				deposit       sql.NullFloat64
				balance       sql.NullFloat64
				misclassified bool
			)

			if err := rows.Scan(
				&tid,
				&entityName,
				&tranID,
				&vdate,
				&tdate,
				&desc,
				&withdrawal,
				&deposit,
				&balance,
				&category,
				&misclassified,
			); err != nil {
				continue
			}

			categoryName := category.String
			if !category.Valid || categoryName == "" {
				categoryName = "Uncategorized"
			}

			resp = append(resp, map[string]interface{}{
				"transaction_id":     tid,
				"entity_name":        entityName,
				"tran_id":            tranID.String,
				"value_date":         vdate,
				"transaction_date":   tdate,
				"description":        desc,
				"withdrawal_amount":  withdrawal.Float64,
				"deposit_amount":     deposit.Float64,
				"balance":            balance.Float64,
				"category_name":      categoryName,
				"misclassified_flag": misclassified,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    resp,
		})
	})
}

// MarkBankStatementTransactionsMisclassifiedHandler sets misclassified_flag = true
// for the provided list of transaction IDs.
func MarkBankStatementTransactionsMisclassifiedHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			UserID         string  `json:"user_id"`
			TransactionIDs []int64 `json:"transaction_ids"`
		}

		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.TransactionIDs) == 0 {
			http.Error(w, "Missing user_id or transaction_ids", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		res, err := db.ExecContext(ctx, `
			UPDATE cimplrcorpsaas.bank_statement_transactions
			SET misclassified_flag = true
			WHERE transaction_id = ANY($1)
		`, pq.Array(body.TransactionIDs))
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		rowsAffected, _ := res.RowsAffected()

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":       true,
			"updated_count": rowsAffected,
		})
	})
}

// 2b. Recompute KPIs and uncategorized data for an existing bank statement
// (POST, req: user_id, bank_statement_id). This endpoint returns a response
// shaped like the upload endpoint so that the frontend can refresh KPIs and
// the list of uncategorized transactions without re-uploading the file.
func RecomputeBankStatementSummaryHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID          string `json:"user_id"`
			BankStatementID string `json:"bank_statement_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.BankStatementID == "" {
			http.Error(w, "Missing bank_statement_id in body", http.StatusBadRequest)
			return
		}

		ctx := r.Context()

		// Fetch statement meta to know entity/account and period
		var entityID, accountNumber string
		var statementPeriodStart, statementPeriodEnd time.Time
		err := db.QueryRowContext(ctx, `
				SELECT entity_id, account_number, statement_period_start, statement_period_end
				FROM cimplrcorpsaas.bank_statements
				WHERE bank_statement_id = $1
			`, body.BankStatementID).Scan(&entityID, &accountNumber, &statementPeriodStart, &statementPeriodEnd)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				http.Error(w, "bank_statement_id not found", http.StatusNotFound)
				return
			}
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}

		// Fetch account currency for rule loading
		var acctCurrency sql.NullString
		err = db.QueryRowContext(ctx, `SELECT mba.currency FROM public.masterbankaccount mba WHERE mba.account_number = $1 LIMIT 1`, accountNumber).Scan(&acctCurrency)
		if err != nil {
			http.Error(w, "failed to fetch account currency: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Load all rules/components for this account/entity/bank/currency/global so we can
		// both re-evaluate categories and attach category metadata (names/types)
		// to the KPI response, mirroring the upload behaviour.
		var currencyCode string
		if acctCurrency.Valid {
			currencyCode = acctCurrency.String
		}
		rules, err := loadCategoryRuleComponents(ctx, db, accountNumber, entityID, currencyCode)
		if err != nil {
			http.Error(w, "failed to fetch category rules: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Load all transactions for this bank statement
		rows, err := db.QueryContext(ctx, `
			SELECT transaction_id, tran_id, value_date, transaction_date, description,
			       withdrawal_amount, deposit_amount, balance, raw_json, category_id
			FROM cimplrcorpsaas.bank_statement_transactions
			WHERE bank_statement_id = $1
			ORDER BY value_date, transaction_date, transaction_id
		`, body.BankStatementID)
		if err != nil {
			http.Error(w, constants.ErrDBPrefix+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		categoryCount := map[int64]int{}
		debitSum := map[int64]float64{}
		creditSum := map[int64]float64{}
		uncategorized := []map[string]interface{}{}
		transactionsCount := 0
		totalTxns := 0
		groupedTxns := 0
		ungroupedTxns := 0
		categoryTxns := map[int64][]map[string]interface{}{}
		for rows.Next() {
			var transactionID int64
			var tranID sql.NullString
			var valueDate, transactionDate time.Time
			var description string
			var withdrawal, deposit, balance sql.NullFloat64
			var rawJSON json.RawMessage
			var existingCategoryID sql.NullInt64
			if err := rows.Scan(&transactionID, &tranID, &valueDate, &transactionDate, &description,
				&withdrawal, &deposit, &balance, &rawJSON, &existingCategoryID); err != nil {
				continue
			}
			transactionsCount++
			totalTxns++

			// Re-evaluate category for this transaction using the same logic as
			// the upload flow so that new/updated rules are applied.
			newCategoryID := matchCategoryForTransaction(rules, description, withdrawal, deposit)

			// If the category has changed (including from/to NULL), persist the
			// new category_id back to the database.
			if (newCategoryID.Valid != existingCategoryID.Valid) || (newCategoryID.Valid && existingCategoryID.Valid && newCategoryID.Int64 != existingCategoryID.Int64) {
				if _, err := db.ExecContext(ctx, `
					UPDATE cimplrcorpsaas.bank_statement_transactions
					SET category_id = $1
					WHERE transaction_id = $2
				`, newCategoryID, transactionID); err != nil {
					log.Printf("failed to update category_id for transaction_id %d: %v", transactionID, err)
				}
			}

			// Use the (potentially updated) category for KPI aggregation and
			// uncategorized list.
			if newCategoryID.Valid {
				categoryCount[newCategoryID.Int64]++
				if withdrawal.Valid {
					debitSum[newCategoryID.Int64] += withdrawal.Float64
				}
				if deposit.Valid {
					creditSum[newCategoryID.Int64] += deposit.Float64
				}
				groupedTxns++
				catID := newCategoryID.Int64
				categoryTxns[catID] = append(categoryTxns[catID], map[string]interface{}{
					"transaction_id":    transactionID,
					"tran_id":           tranID.String,
					"value_date":        valueDate,
					"transaction_date":  transactionDate,
					"description":       description,
					"withdrawal_amount": withdrawal.Float64,
					"deposit_amount":    deposit.Float64,
					"balance":           balance.Float64,
					"category_id":       catID,
				})
			} else {
				uncategorized = append(uncategorized, map[string]interface{}{
					"tran_id":     tranID.String,
					"description": description,
					"value_date":  valueDate,
					"amount":      map[string]interface{}{"withdrawal": withdrawal.Float64, "deposit": deposit.Float64},
				})
				ungroupedTxns++
			}
		}

		// Build KPI and category metadata, mirroring the upload response
		kpiCats := []map[string]interface{}{}
		foundCategories := []map[string]interface{}{}
		foundCategoryIDs := map[int64]bool{}
		for catID, count := range categoryCount {
			var catName string
			for _, rule := range rules {
				if rule.CategoryID == catID {
					catName = rule.CategoryName
					break
				}
			}
			kpiCats = append(kpiCats, map[string]interface{}{
				"category_id":   catID,
				"category_name": catName,
				"count":         count,
				"debit_sum":     debitSum[catID],
				"credit_sum":    creditSum[catID],
				"transactions":  categoryTxns[catID],
			})
			foundCategoryIDs[catID] = true
		}
		for _, rule := range rules {
			if foundCategoryIDs[rule.CategoryID] {
				foundCategories = append(foundCategories, map[string]interface{}{
					"category_id":   rule.CategoryID,
					"category_name": rule.CategoryName,
					"category_type": rule.CategoryType,
				})
				delete(foundCategoryIDs, rule.CategoryID)
			}
		}

		groupedPct := 0.0
		ungroupedPct := 0.0
		if totalTxns > 0 {
			groupedPct = float64(groupedTxns) * 100.0 / float64(totalTxns)
			ungroupedPct = float64(ungroupedTxns) * 100.0 / float64(totalTxns)
		}
		result := map[string]interface{}{
			"pages_processed":                 1,
			"bank_wise_status":                []map[string]interface{}{{"account_number": accountNumber, "status": "SUCCESS"}},
			"statement_date_coverage":         map[string]interface{}{"start": statementPeriodStart, "end": statementPeriodEnd},
			"category_kpis":                   kpiCats,
			"categories_found":                foundCategories,
			"uncategorized":                   uncategorized,
			"bank_statement_id":               body.BankStatementID,
			"transactions_uploaded_count":     transactionsCount,
			"transactions_under_review_count": 0,
			"transactions_under_review":       []map[string]interface{}{},
			"grouped_transaction_count":       groupedTxns,
			"ungrouped_transaction_count":     ungroupedTxns,
			"grouped_transaction_percent":     groupedPct,
			"ungrouped_transaction_percent":   ungroupedPct,
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Bank statement summary recomputed successfully",
			"data":    result,
		})
	})
}

// 3. Approve a bank statement (POST, req: user_id, bank_statement_id)
func ApproveBankStatementHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bank_statement_ids"`
			Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.BankStatementIDs) == 0 {
			http.Error(w, missingUserIDOrBankStatementIDs, http.StatusBadRequest)
			return
		}
		results := make([]map[string]interface{}, 0)
		ctx := r.Context()
		for _, bsid := range body.BankStatementIDs {
			// entity access check
			var entityID string
			if err := db.QueryRowContext(ctx, `SELECT entity_id FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bsid).Scan(&entityID); err == nil {
				if ids := apictx.GetEntityIDsFromCtx(ctx); len(ids) > 0 {
					if !apictx.IsEntityAllowed(ctx, entityID) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             constants.ErrNoAccessToBankStatement,
						})
						continue
					}
				}
			}
			// Check if the latest audit action is DELETE_PENDING_APPROVAL
			var actionType, processingStatus string
			err := db.QueryRowContext(ctx, `
				       SELECT actiontype, processing_status FROM cimplrcorpsaas.auditactionbankstatement
				       WHERE bankstatementid = $1
				       ORDER BY action_id DESC LIMIT 1
			       `, bsid).Scan(&actionType, &processingStatus)
			if err != nil {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             err.Error(),
				})
				continue
			}
			if actionType == "DELETE" && processingStatus == "DELETE_PENDING_APPROVAL" {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				defer tx.Rollback()
				_, err = tx.Exec(`DELETE FROM cimplrcorpsaas.bank_statement_transactions WHERE bank_statement_id = $1`, bsid)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				_, err = tx.Exec(`DELETE FROM public.bank_balances_manual WHERE balance_id = $1`, bsid)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				_, err = tx.Exec(`DELETE FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bsid)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				_, err = tx.Exec(`INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at, checker_comment) VALUES ($1, $2, $3, $4, $5, $6)`, bsid, "DELETE", "DELETED", body.UserID, time.Now(), body.Comment)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				if err := tx.Commit(); err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           true,
					"message":           "Bank statement and related data deleted after approval",
				})
			} else {
				// On normal approval, create/update the corresponding bank balance
				// entry and its audit record, based on the already-stored statement
				// and transactions.
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}
				defer tx.Rollback()

				var accountNumber string
				var statementPeriodEnd time.Time
				var openingBalance, closingBalance float64
				err = tx.QueryRowContext(ctx, `
				       SELECT account_number, statement_period_end, opening_balance, closing_balance
				       FROM cimplrcorpsaas.bank_statements
				       WHERE bank_statement_id = $1
			       `, bsid).Scan(&accountNumber, &statementPeriodEnd, &openingBalance, &closingBalance)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				// Lookup account/bank details for balances_manual
				var bankName, currencyCode, nickname, country string
				err = tx.QueryRowContext(ctx, `
				       SELECT mb.bank_name, mba.currency, COALESCE(mba.account_nickname, mb.bank_name), mba.country
				       FROM public.masterbankaccount mba
				       JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
				       WHERE mba.account_number = $1 AND mba.is_deleted = false
			       `, accountNumber).Scan(&bankName, &currencyCode, &nickname, &country)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				// Enforce bank/account/currency checks (entity already checked above)
				if names := apictx.GetBankNamesFromCtx(ctx); len(names) > 0 {
					if bankName != "" && !apictx.IsBankAllowed(ctx, bankName) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             constants.ErrBankNotAllowed,
						})
						continue
					}
				}
				if ctx.Value("ApprovedBankAccounts") != nil {
					if !ctxHasApprovedBankAccount(ctx, accountNumber) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             "bank account not approved",
						})
						continue
					}
				}
				if !ctxHasApprovedCurrency(ctx, currencyCode) {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             constants.ErrCurrencyNotAllowed,
					})
					continue
				}

				// Calculate total credits and debits from stored transactions
				var totalCredits, totalDebits float64
				err = tx.QueryRowContext(ctx, `
				       SELECT COALESCE(SUM(deposit_amount), 0), COALESCE(SUM(withdrawal_amount), 0)
				       FROM cimplrcorpsaas.bank_statement_transactions
				       WHERE bank_statement_id = $1
			       `, bsid).Scan(&totalCredits, &totalDebits)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				// Upsert into public.bank_balances_manual for dashboard matching
				_, err = tx.Exec(`
				       INSERT INTO public.bank_balances_manual (
					       balance_id, bank_name, account_no, currency_code, nickname, country, as_of_date, balance_type, balance_amount, opening_balance, total_credits, total_debits, closing_balance, statement_type, source_channel
				       ) VALUES (
					       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
				       )
				       ON CONFLICT (balance_id) DO UPDATE SET
					       bank_name = EXCLUDED.bank_name,
					       account_no = EXCLUDED.account_no,
					       currency_code = EXCLUDED.currency_code,
					       nickname = EXCLUDED.nickname,
					       country = EXCLUDED.country,
					       as_of_date = EXCLUDED.as_of_date,
					       balance_type = EXCLUDED.balance_type,
					       balance_amount = EXCLUDED.balance_amount,
					       opening_balance = EXCLUDED.opening_balance,
					       total_credits = EXCLUDED.total_credits,
					       total_debits = EXCLUDED.total_debits,
					       closing_balance = EXCLUDED.closing_balance,
					       statement_type = EXCLUDED.statement_type,
					       source_channel = EXCLUDED.source_channel
			       `,
					bsid,
					bankName,
					accountNumber,
					currencyCode,
					nickname,
					country,
					statementPeriodEnd,
					"CLOSING",
					closingBalance,
					openingBalance,
					totalCredits,
					totalDebits,
					closingBalance,
					"BANK_STATEMENT_V2",
					"UPLOAD_V2",
				)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				// Insert audit action for bank_balances_manual marking it pending approval
				_, err = tx.Exec(`
				       INSERT INTO auditactionbankbalances (
					       balance_id, actiontype, processing_status, requested_by, requested_at
				       ) VALUES ($1, $2, $3, $4, $5)
			       `,
					bsid,
					"CREATE",
					"PENDING_APPROVAL",
					body.UserID,
					time.Now(),
				)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				// Finally, record the statement approval itself
				_, err = tx.Exec(`INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at, checker_comment) VALUES ($1, $2, $3, $4, $5, $6)`, bsid, "APPROVE", "APPROVED", body.UserID, time.Now(), body.Comment)
				if err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				if err := tx.Commit(); err != nil {
					results = append(results, map[string]interface{}{
						"bank_statement_id": bsid,
						"success":           false,
						"error":             err.Error(),
					})
					continue
				}

				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           true,
					"message":           "Bank statement approved and bank balance created",
				})
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	})
}

// 4. Reject a bank statement (POST, req: user_id, bank_statement_id)
func RejectBankStatementHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bank_statement_ids"`
			Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.BankStatementIDs) == 0 {
			http.Error(w, missingUserIDOrBankStatementIDs, http.StatusBadRequest)
			return
		}
		results := make([]map[string]interface{}, 0)
		ctx := r.Context()
		for _, bsid := range body.BankStatementIDs {
			var entityID string
			if err := db.QueryRowContext(ctx, `SELECT entity_id FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bsid).Scan(&entityID); err == nil {
				if ids := apictx.GetEntityIDsFromCtx(ctx); len(ids) > 0 {
					if !apictx.IsEntityAllowed(ctx, entityID) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             constants.ErrNoAccessToBankStatement,
						})
						continue
					}
				}
			}
			_, err := db.ExecContext(ctx, `INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at, checker_comment) VALUES ($1, $2, $3, $4, $5, $6)`, bsid, "REJECT", "REJECTED", body.UserID, time.Now(), body.Comment)
			if err != nil {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             err.Error(),
				})
				continue
			}
			results = append(results, map[string]interface{}{
				"bank_statement_id": bsid,
				"success":           true,
				"message":           "Bank statement rejected",
			})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	})
}

// 5. Delete bank statement, its transactions, and its balance (POST, req: user_id, bank_statement_id)
func DeleteBankStatementHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bank_statement_ids"`
			Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.BankStatementIDs) == 0 {
			http.Error(w, missingUserIDOrBankStatementIDs, http.StatusBadRequest)
			return
		}
		results := make([]map[string]interface{}, 0)
		ctx := r.Context()
		for _, bsid := range body.BankStatementIDs {
			var entityID string
			if err := db.QueryRowContext(ctx, `SELECT entity_id FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bsid).Scan(&entityID); err == nil {
				if ids := apictx.GetEntityIDsFromCtx(ctx); len(ids) > 0 {
					if !apictx.IsEntityAllowed(ctx, entityID) {
						results = append(results, map[string]interface{}{
							"bank_statement_id": bsid,
							"success":           false,
							"error":             constants.ErrNoAccessToBankStatement,
						})
						continue
					}
				}
			}
			_, err := db.ExecContext(ctx, `
				       INSERT INTO cimplrcorpsaas.auditactionbankstatement (
					       bankstatementid, actiontype, processing_status, requested_by, requested_at, checker_comment
				       ) VALUES ($1, $2, $3, $4, $5, $6)
			       `, bsid, "DELETE", "DELETE_PENDING_APPROVAL", body.UserID, time.Now(), body.Comment)
			if err != nil {
				results = append(results, map[string]interface{}{
					"bank_statement_id": bsid,
					"success":           false,
					"error":             err.Error(),
				})
				continue
			}
			results = append(results, map[string]interface{}{
				"bank_statement_id": bsid,
				"success":           true,
				"message":           "Delete request submitted for approval",
			})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	})

}

func UploadBankStatementV2Handler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)

		if r.Method != http.MethodPost {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Only POST method is allowed for this endpoint.",
			})
			return
		}

		// Check if is_pdf flag is set in query params first (no multipart parsing needed)
		isPDF := r.URL.Query().Get("is_pdf") == "true"

		if isPDF {
			// PDF processing: read bank.json and process it (NO FILE UPLOAD NEEDED)
			log.Println("[BANK_STATEMENT] PDF flag detected, processing from bank.json")
			result, err := ProcessBankStatementFromJSON(r.Context(), db)
			if err != nil {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"message": userFriendlyUploadError(err),
				})
				return
			}

			msg := "Bank statement from PDF uploaded successfully"
			if rc, ok := result["transactions_under_review_count"].(int); ok && rc > 0 {
				msg = fmt.Sprintf("Bank statement from PDF uploaded successfully. %d transactions are under review.", rc)
			}

			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"message": msg,
				"data":    result,
			})
			return
		}

		// Regular Excel/XLS/CSV processing - requires file upload
		if err := r.ParseMultipartForm(32 << 20); err != nil { // 32MB
			log.Printf("[BANK-UPLOAD-ERROR] Failed to parse multipart form: %v", err)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Unable to read the uploaded file. Please try again.",
			})
			return
		}

		// If client requested multi-account CSV processing, delegate to the
		// dedicated handler which groups rows by account_number and calls the
		// existing categorization pipeline per-account.
		if r.FormValue("multi") == "true" {
			UploadMultiAccountBankStatementHandler(db).ServeHTTP(w, r)
			return
		}

		// Log available form fields for debugging
		var fileFieldsAvailable []string
		if r.MultipartForm != nil && r.MultipartForm.File != nil {
			for fieldName := range r.MultipartForm.File {
				fileFieldsAvailable = append(fileFieldsAvailable, fieldName)
			}
			log.Printf("[BANK-UPLOAD-DEBUG] Available file form fields: %v", fileFieldsAvailable)
		} else {
			log.Printf("[BANK-UPLOAD-ERROR] No file fields found in multipart form - request may not include a file attachment")
		}

		file, _, err := r.FormFile("file")
		if err != nil {
			log.Printf("[BANK-UPLOAD-ERROR] FormFile('file') error: %v", err)
			// Try alternative field names commonly used in file uploads
			if file == nil {
				file, _, err = r.FormFile("statement")
			}
			if err != nil && file == nil {
				file, _, err = r.FormFile("bankStatement")
			}
			if err != nil && file == nil {
				// Try to use any available file field
				if r.MultipartForm != nil && len(r.MultipartForm.File) > 0 {
					log.Printf("[BANK-UPLOAD-DEBUG] Trying first available file field from: %v", fileFieldsAvailable)
					for fieldName, files := range r.MultipartForm.File {
						if len(files) > 0 {
							log.Printf("[BANK-UPLOAD-DEBUG] Using field: %s", fieldName)
							file, err = files[0].Open()
							break
						}
					}
				}
			}
			if err != nil || file == nil {
				errorMsg := "File not found in request. Please attach a bank statement file using the 'file' field in form-data."
				if len(fileFieldsAvailable) > 0 {
					errorMsg = fmt.Sprintf("No 'file' field found. Available fields: %v. Please use 'file' as the field name.", fileFieldsAvailable)
				}
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"message": errorMsg,
				})
				return
			}
		}
		defer file.Close()

		// Use a hash of the file contents for idempotency
		fileBytes, err := io.ReadAll(file)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Failed to read the uploaded file. Please try again.",
			})
			return
		}
		hash := sha256.Sum256(fileBytes)
		fileHash := fmt.Sprintf("%x", hash[:])

		// Re-create file reader for actual processing
		fileReader := bytes.NewReader(fileBytes)
		mf := &bytesFile{Reader: fileReader}

		result, err := UploadBankStatementV2WithCategorization(r.Context(), db, mf, fileHash)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": userFriendlyUploadError(err),
			})
			return
		}

		// Build a user-friendly success message that also highlights any
		// transactions that were detected as already present and put under review.
		msg := "Bank statement uploaded successfully"
		if rc, ok := result["transactions_under_review_count"].(int); ok && rc > 0 {
			msg = fmt.Sprintf("Bank statement uploaded successfully. %d transactions are under review.", rc)
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": msg,
			"data":    result,
		})
	})
}

// bytesFile implements multipart.File for a bytes.Reader
type bytesFile struct {
	*bytes.Reader
}

func (b *bytesFile) Close() error { return nil }

// PDFBankStatementJSON represents the JSON structure from PDF parsing
type PDFBankStatementJSON struct {
	AccountNumber  string               `json:"account_number"`
	AccountName    string               `json:"account_name"`
	BankName       string               `json:"bank_name"`
	IFSC           string               `json:"ifsc"`
	MICR           string               `json:"micr"`
	PeriodStart    string               `json:"period_start"`
	PeriodEnd      string               `json:"period_end"`
	OpeningBalance float64              `json:"opening_balance"`
	ClosingBalance float64              `json:"closing_balance"`
	Transactions   []PDFTransactionJSON `json:"transactions"`
}

type PDFTransactionJSON struct {
	TranID          *string `json:"tran_id"`
	TransactionDate string  `json:"transaction_date"`
	ValueDate       string  `json:"value_date"`
	Description     string  `json:"description"`
	Withdrawal      float64 `json:"withdrawal"`
	Deposit         float64 `json:"deposit"`
	Balance         float64 `json:"balance"`
}

type BankStatement struct {
	BankStatementID      string     `db:"bank_statement_id"`
	EntityID             string     `db:"entity_id"`
	AccountNumber        string     `db:"account_number"`
	StatementPeriodStart time.Time  `db:"statement_period_start"`
	StatementPeriodEnd   time.Time  `db:"statement_period_end"`
	StatementRequestDate *time.Time `db:"statement_request_date"`
	FileHash             string     `db:"file_hash"`
	UploadedAt           time.Time  `db:"uploaded_at"`
	OpeningBalance       float64    `db:"opening_balance"`
	ClosingBalance       float64    `db:"closing_balance"`
}

type BankStatementTransaction struct {
	TransactionID    int64           `db:"transaction_id"`
	BankStatementID  string          `db:"bank_statement_id"`
	AccountNumber    string          `db:"account_number"`
	TranID           sql.NullString  `db:"tran_id"`
	ValueDate        time.Time       `db:"value_date"`
	TransactionDate  time.Time       `db:"transaction_date"`
	PostedDate       sql.NullTime    `db:"posted_date"`
	ChequeNo         sql.NullString  `db:"cheque_no"`
	Description      string          `db:"description"`
	WithdrawalAmount sql.NullFloat64 `db:"withdrawal_amount"`
	DepositAmount    sql.NullFloat64 `db:"deposit_amount"`
	Balance          sql.NullFloat64 `db:"balance"`
	RawJSON          json.RawMessage `db:"raw_json"`
	CategoryID       sql.NullInt64   `db:"category_id"`
	CreatedAt        time.Time       `db:"created_at"`
}

// UploadBankStatementV2 handles the upload of a bank statement Excel file with idempotency and duplicate data checks.
func UploadBankStatementV2(ctx context.Context, db *sql.DB, file multipart.File, fileHash string) error {
	defer file.Close()

	// 1. Idempotency: Check if file hash already exists
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM cimplrcorpsaas.bank_statements WHERE file_hash = $1)`, fileHash).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check file hash: %w", err)
	}
	if exists {
		return ErrFileAlreadyUploaded
	}

	// 2. Parse Excel file
	tmpFile, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	contentType := detectContentType(tmpFile)
	fileExt := ".xlsx"
	xl, err := excelize.OpenReader(bytes.NewReader(tmpFile))
	if err != nil {
		return fmt.Errorf("failed to parse excel: %w", err)
	}
	defer xl.Close()

	sheetName := xl.GetSheetName(0)
	rows, err := xl.GetRows(sheetName)
	if err != nil {
		return fmt.Errorf("failed to get rows: %w", err)
	}
	if len(rows) < 2 {
		return errors.New("excel must have at least one data row")
	}

	// 3. Extract account number, entity id, and name from header
	var accountNumber, entityID, accountName string
	for i := 0; i < 20 && i < len(rows); i++ {
		for j, cell := range rows[i] {
			if cell == acNoHeader && j+1 < len(rows[i]) {
				accountNumber = rows[i][j+1]
			}
			if cell == "Name:" && j+1 < len(rows[i]) {
				accountName = rows[i][j+1]
			}
		}
	}
	if accountNumber == "" {
		return ErrAccountNumberMissing
	}

	// Lookup entity_id and name from masterbankaccount
	err = db.QueryRowContext(ctx, `SELECT entity_id, COALESCE(account_nickname, bank_name) FROM public.masterbankaccount WHERE account_number = $1 AND is_deleted = false`, accountNumber).Scan(&entityID, &accountName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrAccountNotFound
		}
		return fmt.Errorf(constants.ErrDBMasterBankAccountLookup, err)
	}

	// Print extracted data
	fmt.Printf("Extracted Account Number: %s\n", accountNumber)
	fmt.Printf("Entity ID: %s\n", entityID)
	fmt.Printf("Account Name: %s\n", accountName)

	// Context validations precedence: entity -> bank (later) -> account -> currency
	if ids := apictx.GetEntityIDsFromCtx(ctx); len(ids) > 0 {
		if !apictx.IsEntityAllowed(ctx, entityID) {
			return ErrAccountNotFound
		}
	}

	s3Key := buildBankStatementS3Key(accountNumber, fileHash, fileExt)
	if _, err := uploadBankStatementToS3(ctx, s3Key, tmpFile, contentType); err != nil {
		return fmt.Errorf("failed to store original file to s3: %w", err)
	}

	// 4. Extract statement period and balances from the file (assume first/last row for balances)
	var (
		statementPeriodStart, statementPeriodEnd time.Time
		openingBalance, closingBalance           float64
		transactions                             []BankStatementTransaction
	)
	// Find the header row for transactions. Prefer Tran Id detection but
	// fall back to a flexible detection: a row that contains a Date column
	// and either a description-like column or amount columns (Debit/Credit/Withdrawal/Deposit).
	var txnHeaderIdx int = -1
	for i, row := range rows {
		for _, cell := range row {
			if cell == tranIDHeader || strings.EqualFold(strings.TrimSpace(cell), "Tran Id") {
				txnHeaderIdx = i
				break
			}
		}
		if txnHeaderIdx != -1 {
			break
		}
	}
	if txnHeaderIdx == -1 {
		for i, row := range rows {
			hasDate := false
			hasDesc := false
			hasAmountCols := false
			for _, cell := range row {
				c := strings.TrimSpace(cell)
				lc := strings.ToLower(c)
				if strings.EqualFold(c, "Date") {
					hasDate = true
				}
				if strings.Contains(lc, "description") || strings.Contains(lc, "detail description") || strings.Contains(lc, "remarks") {
					hasDesc = true
				}
				if strings.EqualFold(c, "Withdrawal") || strings.EqualFold(c, "Deposit") || strings.EqualFold(c, "Debit") || strings.EqualFold(c, "Credit") || strings.Contains(lc, "debit") || strings.Contains(lc, "credit") {
					hasAmountCols = true
				}
			}
			if hasDate && (hasDesc || hasAmountCols) {
				txnHeaderIdx = i
				break
			}
		}
	}
	if txnHeaderIdx == -1 {
		return errors.New(constants.ErrTransactionHeaderRowNotFound)
	}

	// Map columns by trimmed header name for flexibility
	headerRow := rows[txnHeaderIdx]
	colIdx := map[string]int{}
	for idx, col := range headerRow {
		colIdx[strings.TrimSpace(col)] = idx
	}

	// Flexible required columns: need Date and (Description or amount column)
	hasDate := false
	hasDesc := false
	hasAmount := false
	for col := range colIdx {
		lc := strings.ToLower(col)
		if lc == "date" {
			hasDate = true
		}
		if strings.Contains(lc, "description") || strings.Contains(lc, "remarks") {
			hasDesc = true
		}
		if lc == "withdrawal" || lc == "deposit" || lc == "debit" || lc == "credit" || strings.Contains(lc, "debit") || strings.Contains(lc, "credit") {
			hasAmount = true
		}
	}
	if !hasDate || (!hasDesc && !hasAmount) {
		return errors.New(constants.ErrTransactionHeaderRowNotFound)
	}

	// Parse transactions, skip header
	var lastValidBalance sql.NullFloat64
	var lastValidValueDate time.Time
	debugParse := strings.ToLower(strings.TrimSpace(os.Getenv("BANK_STMT_DEBUG_PARSE"))) == "1" || strings.ToLower(strings.TrimSpace(os.Getenv("BANK_STMT_DEBUG_PARSE"))) == "true"
	debugCount := 0
	for i, row := range rows[txnHeaderIdx+1:] {
		// Skip rows that are completely empty
		if len(row) == 0 || allEmptyRow(row) {
			continue
		}
		// Defensive: fill missing columns with empty string
		for len(row) < len(headerRow) {
			row = append(row, "")
		}
		// Safe tranID lookup: only use it if header mapping exists
		var tranID sql.NullString
		if idx, ok := colIdx[tranIDHeader]; ok {
			if idx < len(row) && strings.TrimSpace(row[idx]) != "" {
				tranID = sql.NullString{String: row[idx], Valid: true}
			} else {
				tranID = sql.NullString{Valid: false}
			}
		} else {
			// no TranID header mapped; leave empty but continue parsing
			tranID = sql.NullString{Valid: false}
		}
		// Parse dates safely with checks
		var valueDateStr, transactionDateStr string
		var valueDate, transactionDate time.Time
		if idx, ok := colIdx[valueDateHeader]; ok && idx < len(row) {
			valueDateStr = row[idx]
			valueDate, _ = parseDate(valueDateStr)
		}
		if idx, ok := colIdx[transactionDateHeader]; ok && idx < len(row) {
			transactionDateStr = row[idx]
			transactionDate, _ = parseDate(transactionDateStr)
		}
		if valueDate.IsZero() && !transactionDate.IsZero() {
			valueDate = transactionDate
		}
		if transactionDate.IsZero() && !valueDate.IsZero() {
			transactionDate = valueDate
		}
		// Safe description lookup
		var description string
		if idx, ok := colIdx[transactionRemarksHeader]; ok && idx < len(row) {
			description = row[idx]
		}
		var withdrawal, deposit sql.NullFloat64
		// Support single amount column + Cr/Dr indicator
		idxW, okW := colIdx[withdrawalAmtHeader]
		idxD, okD := colIdx[depositAmtHeader]
		idxCr, okCr := colIdx["CrDr"]
		if okW && okD && idxW == idxD && okCr && idxCr >= 0 && idxCr < len(row) {
			amtStr := cleanAmount(row[idxW])
			crdr := strings.ToLower(strings.TrimSpace(row[idxCr]))
			if amtStr != "" {
				var amt float64
				fmt.Sscanf(amtStr, "%f", &amt)
				if strings.HasPrefix(crdr, "cr") || strings.Contains(crdr, "credit") || strings.HasPrefix(crdr, "c") {
					deposit.Valid = true
					deposit.Float64 = amt
				} else if strings.HasPrefix(crdr, "dr") || strings.Contains(crdr, "debit") || strings.HasPrefix(crdr, "d") {
					withdrawal.Valid = true
					withdrawal.Float64 = amt
				} else {
					if strings.HasPrefix(amtStr, "-") {
						withdrawal.Valid = true
						withdrawal.Float64 = -amt
					} else {
						deposit.Valid = true
						deposit.Float64 = amt
					}
				}
			}
		} else {
			var withdrawalStr, depositStr string
			if okW && idxW < len(row) {
				withdrawalStr = cleanAmount(row[idxW])
			}
			if okD && idxD < len(row) {
				depositStr = cleanAmount(row[idxD])
			}
			if withdrawalStr != "" && depositStr == "" {
				withdrawal.Valid = true
				fmt.Sscanf(withdrawalStr, "%f", &withdrawal.Float64)
				deposit.Valid = false
			} else if depositStr != "" && withdrawalStr == "" {
				deposit.Valid = true
				fmt.Sscanf(depositStr, "%f", &deposit.Float64)
				withdrawal.Valid = false
			} else {
				withdrawal.Valid = false
				deposit.Valid = false
			}
		}
		balance := sql.NullFloat64{Valid: row[colIdx[balanceHeader]] != ""}
		if balance.Valid {
			balanceStr := cleanAmount(row[colIdx[balanceHeader]])
			fmt.Sscanf(balanceStr, "%f", &balance.Float64)
		}
		if i == 0 {
			statementPeriodStart = valueDate
			openingBalance = balance.Float64
		}
		// Track last valid balance for closing
		if balance.Valid {
			lastValidBalance = balance
			lastValidValueDate = valueDate
		}
		rowJSON, _ := json.Marshal(row)
		transactions = append(transactions, BankStatementTransaction{
			AccountNumber:    accountNumber,
			TranID:           tranID,
			ValueDate:        valueDate,
			TransactionDate:  transactionDate,
			Description:      description,
			WithdrawalAmount: withdrawal,
			DepositAmount:    deposit,
			Balance:          balance,
			RawJSON:          rowJSON,
		})
		// Optional debug: log raw amount strings and parsed floats for first N rows
		if debugParse && debugCount < 50 {
			debugCount++
			var wStr, dStr, amtShared, crdrRaw string
			if okW && idxW < len(row) {
				wStr = row[idxW]
			}
			if okD && idxD < len(row) {
				dStr = row[idxD]
			}
			if okW && okD && idxW == idxD {
				amtShared = row[idxW]
			}
			if okCr && idxCr < len(row) {
				crdrRaw = row[idxCr]
			}
			log.Printf("[BANK-UPLOAD-DEBUG] parse row[%d]: tran=%q date=%q desc=%q withdrawal_raw=%q deposit_raw=%q sharedAmt=%q crdr=%q parsed_withdrawal=%v parsed_deposit=%v", i+txnHeaderIdx+1, tranID.String, valueDate.Format(constants.DateFormatSlash), sanitizeForPostgres(description), wStr, dStr, amtShared, crdrRaw, withdrawal, deposit)
		}
	}
	// Set closing balance to last valid
	closingBalance = lastValidBalance.Float64
	statementPeriodEnd = lastValidValueDate

	// 5. Duplicate period check
	var periodExists bool
	err = db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM cimplrcorpsaas.bank_statements WHERE entity_id = $1 AND account_number = $2 AND statement_period_start = $3 AND statement_period_end = $4)`, entityID, accountNumber, statementPeriodStart, statementPeriodEnd).Scan(&periodExists)
	if err != nil {
		return fmt.Errorf("failed to check period: %w", err)
	}
	if periodExists {
		return ErrStatementPeriodExists
	}

	// 6. Insert bank statement and transactions in a transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin db transaction: %w", err)
	}
	defer tx.Rollback()

	var bankStatementID string
	err = tx.QueryRowContext(ctx, `
		      INSERT INTO cimplrcorpsaas.bank_statements (
			      entity_id, account_number, statement_period_start, statement_period_end, file_hash, opening_balance, closing_balance
		      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
		      ON CONFLICT ON CONSTRAINT uniq_stmt
		      DO UPDATE SET
			      file_hash = EXCLUDED.file_hash,
			      closing_balance = EXCLUDED.closing_balance
		      RETURNING bank_statement_id
	      `, entityID, accountNumber, statementPeriodStart, statementPeriodEnd, fileHash, openingBalance, closingBalance).Scan(&bankStatementID)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf(constants.ErrFailedToInsertBankStatement, err)
	}

	// Upsert into public.bank_balances_manual for dashboard matching
	// Use bankStatementID as balance_id for uniqueness
	// Lookup additional info from masterbankaccount and masterbank for bank_name, currency_code, nickname, country
	var bankName, currencyCode, nickname, country string
	err = tx.QueryRowContext(ctx, `
		       SELECT mb.bank_name, mba.currency, COALESCE(mba.account_nickname, mb.bank_name), mba.country
		       FROM public.masterbankaccount mba
		       JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
		       WHERE mba.account_number = $1 AND mba.is_deleted = false
	       `, accountNumber).Scan(&bankName, &currencyCode, &nickname, &country)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to lookup account info for balances_manual: %w", err)
	}

	// Enforce bank/account/currency checks (entity already validated earlier)
	if names := apictx.GetBankNamesFromCtx(ctx); len(names) > 0 {
		if bankName != "" && !apictx.IsBankAllowed(ctx, bankName) {
			return errors.New(constants.ErrBankNotAllowed)
		}
	}
	if ctx.Value("ApprovedBankAccounts") != nil {
		if !ctxHasApprovedBankAccount(ctx, accountNumber) {
			return ErrAccountNotFound
		}
	}
	if !ctxHasApprovedCurrency(ctx, currencyCode) {
		return errors.New(constants.ErrCurrencyNotAllowed)
	}
	// Calculate total credits and debits from transactions
	var totalCredits, totalDebits float64
	for _, t := range transactions {
		if t.DepositAmount.Valid {
			totalCredits += t.DepositAmount.Float64
		}
		if t.WithdrawalAmount.Valid {
			totalDebits += t.WithdrawalAmount.Float64
		}
	}
	_, err = tx.ExecContext(ctx, `
			       INSERT INTO public.bank_balances_manual (
				       balance_id, bank_name, account_no, currency_code, nickname, country, as_of_date, balance_type, balance_amount, opening_balance, total_credits, total_debits, closing_balance, statement_type, source_channel
			       ) VALUES (
				       $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
			       )
			       ON CONFLICT (balance_id) DO UPDATE SET
				       bank_name = EXCLUDED.bank_name,
				       account_no = EXCLUDED.account_no,
				       currency_code = EXCLUDED.currency_code,
				       nickname = EXCLUDED.nickname,
				       country = EXCLUDED.country,
				       as_of_date = EXCLUDED.as_of_date,
				       balance_type = EXCLUDED.balance_type,
				       balance_amount = EXCLUDED.balance_amount,
				       opening_balance = EXCLUDED.opening_balance,
				       total_credits = EXCLUDED.total_credits,
				       total_debits = EXCLUDED.total_debits,
				       closing_balance = EXCLUDED.closing_balance,
				       statement_type = EXCLUDED.statement_type,
				       source_channel = EXCLUDED.source_channel
		       `,
		bankStatementID,
		bankName,
		accountNumber,
		currencyCode,
		nickname,
		country,
		statementPeriodEnd,
		"CLOSING",
		closingBalance,
		openingBalance,
		totalCredits,
		totalDebits,
		closingBalance,
		"BANK_STATEMENT_V2",
		"UPLOAD_V2",
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to upsert bank_balances_manual: %w", err)
	}

	// Insert audit action for bank_balances_manual after upsert, using correct transaction and variables
	_, err = tx.ExecContext(ctx, `
			       INSERT INTO auditactionbankbalances (
				       balance_id, actiontype, processing_status, requested_by, requested_at
			       ) VALUES ($1, $2, $3, $4, $5)
		       `,
		bankStatementID,
		"CREATE",
		"PENDING_APPROVAL",
		"system",
		time.Now(),
	)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to insert audit action for bank_balances_manual: %w", err)
	}

	// Bulk insert transactions for speed, skip duplicates
	if len(transactions) > 0 {
		valueStrings := make([]string, 0, len(transactions))
		valueArgs := make([]interface{}, 0, len(transactions)*10)
		for i, t := range transactions {
			valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				i*10+1, i*10+2, i*10+3, i*10+4, i*10+5, i*10+6, i*10+7, i*10+8, i*10+9, i*10+10))
			valueArgs = append(valueArgs,
				bankStatementID,
				t.AccountNumber,
				t.TranID,
				t.ValueDate,
				t.TransactionDate,
				t.Description,
				t.WithdrawalAmount,
				t.DepositAmount,
				t.Balance,
				t.RawJSON,
			)
		}
		stmt := `INSERT INTO cimplrcorpsaas.bank_statement_transactions (
				       bank_statement_id, account_number, tran_id, value_date, transaction_date, description, withdrawal_amount, deposit_amount, balance, raw_json
			       ) VALUES ` +
			joinStrings(valueStrings, ",") +
			` ON CONFLICT (account_number, tran_id, transaction_date) DO NOTHING`
		_, err := tx.ExecContext(ctx, stmt, valueArgs...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to bulk insert transactions: %w", err)
		}
	}

	// cleanAmount removes commas and trims whitespace for Indian number format

	// Insert audit action for this bank statement
	_, err = tx.ExecContext(ctx, `
		       INSERT INTO cimplrcorpsaas.auditactionbankstatement (
			       bankstatementid, actiontype, processing_status, requested_by, requested_at
		       ) VALUES ($1, $2, $3, $4, $5)
	       `, bankStatementID, "CREATE", "PENDING_APPROVAL", "system", time.Now())
	if err != nil {
		tx.Rollback()
		return fmt.Errorf(constants.ErrFailedToInsertAuditAction, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	return nil
}

// ProcessBankStatementFromJSON reads bank.json and processes it like Excel upload
// This is a placeholder for future PDF OCR integration
func ProcessBankStatementFromJSON(ctx context.Context, db *sql.DB) (map[string]interface{}, error) {
	log.Println("[PDF_PROCESSOR] Reading bank.json file")

	// Try multiple possible paths for bank.json
	possiblePaths := []string{
		"api/cash/bankstatement/bank.json",                               // From project root
		"../api/cash/bankstatement/bank.json",                            // From cmd directory
		filepath.Join("..", "api", "cash", "bankstatement", "bank.json"), // Explicit relative
		"/Users/hardikmishra/Documents/CimplrCorpSaaS.   CI work ing/CimplrCorpSaaS-Go/api/cash/bankstatement/bank.json", // Absolute fallback
	}

	var jsonBytes []byte
	var err error
	var foundPath string

	for _, jsonPath := range possiblePaths {
		jsonBytes, err = os.ReadFile(jsonPath)
		if err == nil {
			foundPath = jsonPath
			log.Printf("[PDF_PROCESSOR] Found bank.json at: %s", foundPath)
			break
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to read bank.json (tried multiple paths): %w", err)
	}

	var pdfData PDFBankStatementJSON
	if err := json.Unmarshal(jsonBytes, &pdfData); err != nil {
		return nil, fmt.Errorf("failed to parse bank.json: %w", err)
	}

	log.Printf("[PDF_PROCESSOR] Parsed JSON: Account=%s, Bank=%s, Period=%s to %s, Transactions=%d",
		pdfData.AccountNumber, pdfData.BankName, pdfData.PeriodStart, pdfData.PeriodEnd, len(pdfData.Transactions))

	// Delegate to generic structured processor
	var structured StructuredBankStatement
	structured.AccountNumber = pdfData.AccountNumber
	structured.BankName = pdfData.BankName
	structured.PeriodStart = pdfData.PeriodStart
	structured.PeriodEnd = pdfData.PeriodEnd
	structured.OpeningBalance = pdfData.OpeningBalance
	structured.ClosingBalance = pdfData.ClosingBalance
	for _, t := range pdfData.Transactions {
		st := StructuredTransaction{
			TranID:          t.TranID,
			TransactionDate: t.TransactionDate,
			ValueDate:       t.ValueDate,
			Description:     t.Description,
			Withdrawal:      t.Withdrawal,
			Deposit:         t.Deposit,
			Balance:         t.Balance,
		}
		structured.Transactions = append(structured.Transactions, st)
	}
	hash := sha256.Sum256(jsonBytes)
	fileHash := fmt.Sprintf("%x", hash[:])
	return ProcessBankStatementFromStructuredInput(ctx, db, structured, fileHash)
}

// StructuredBankStatement and StructuredTransaction are canonical input types
// for structured ingestion (PDF JSON or pre-parsed CSV JSON).
type StructuredBankStatement struct {
	AccountNumber  string
	BankName       string
	PeriodStart    string
	PeriodEnd      string
	OpeningBalance float64
	ClosingBalance float64
	Transactions   []StructuredTransaction
}

type StructuredTransaction struct {
	TranID          *string
	TransactionDate string
	ValueDate       string
	Description     string
	Withdrawal      float64
	Deposit         float64
	Balance         float64
}

// ProcessBankStatementFromStructuredInput performs the same ingestion, categorization,
// dedup and KPI computation as the PDF JSON processor but accepts structured input
// directly (no file I/O or header detection).
func ProcessBankStatementFromStructuredInput(ctx context.Context, db *sql.DB, input StructuredBankStatement, fileHash string) (map[string]interface{}, error) {
	// Lookup entity_id from masterbankaccount
	var entityID, accountName, bankName string
	err := db.QueryRowContext(ctx, `
		SELECT mba.entity_id, mb.bank_name, COALESCE(mba.account_nickname, mb.bank_name)
		FROM public.masterbankaccount mba
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		WHERE mba.account_number = $1 AND mba.is_deleted = false
	`, input.AccountNumber).Scan(&entityID, &bankName, &accountName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("account %s not found in master data", input.AccountNumber)
		}
		return nil, fmt.Errorf("failed to lookup account: %w", err)
	}

	// Parse period dates (optional: input may already be ISO; enforce format)
	periodStart, err := time.Parse(constants.DateFormat, input.PeriodStart)
	if err != nil {
		return nil, fmt.Errorf("invalid period_start date: %w", err)
	}
	periodEnd, err := time.Parse(constants.DateFormat, input.PeriodEnd)
	if err != nil {
		return nil, fmt.Errorf("invalid period_end date: %w", err)
	}

	// Load category rules for this account
	var acctCurrency sql.NullString
	db.QueryRowContext(ctx, `SELECT currency FROM public.masterbankaccount WHERE account_number = $1`, input.AccountNumber).Scan(&acctCurrency)

	var currencyCode string
	if acctCurrency.Valid {
		currencyCode = acctCurrency.String
	}
	rules, err := loadCategoryRuleComponents(ctx, db, input.AccountNumber, entityID, currencyCode)
	if err != nil {
		return nil, fmt.Errorf("failed to load category rules: %w", err)
	}

	// Convert structured transactions to BankStatementTransaction and build
	// per-category transaction lists for KPI output (matching V2 format).
	transactions := []BankStatementTransaction{}
	categoryCount := map[int64]int{}
	debitSum := map[int64]float64{}
	creditSum := map[int64]float64{}
	uncategorized := []map[string]interface{}{}
	categoryTxns := map[int64][]map[string]interface{}{}

	for i, txn := range input.Transactions {
		txnDate, err := time.Parse(constants.DateFormat, txn.TransactionDate)
		if err != nil {
			log.Printf("[STRUCTURED_PROCESSOR] Warning: failed to parse transaction_date %s, skipping", txn.TransactionDate)
			continue
		}
		valDate, err := time.Parse(constants.DateFormat, txn.ValueDate)
		if err != nil {
			log.Printf("[STRUCTURED_PROCESSOR] Warning: failed to parse value_date %s, skipping", txn.ValueDate)
			continue
		}

		var withdrawal, deposit sql.NullFloat64
		if txn.Withdrawal > 0 {
			withdrawal = sql.NullFloat64{Float64: txn.Withdrawal, Valid: true}
		}
		if txn.Deposit > 0 {
			deposit = sql.NullFloat64{Float64: txn.Deposit, Valid: true}
		}

		// Match category
		categoryID := matchCategoryForTransaction(rules, txn.Description, withdrawal, deposit)

		// Build transaction
		t := BankStatementTransaction{
			AccountNumber:    input.AccountNumber,
			ValueDate:        valDate,
			TransactionDate:  txnDate,
			Description:      txn.Description,
			WithdrawalAmount: withdrawal,
			DepositAmount:    deposit,
			Balance:          sql.NullFloat64{Float64: txn.Balance, Valid: true},
			CategoryID:       categoryID,
		}
		if txn.TranID != nil && *txn.TranID != "" {
			t.TranID = sql.NullString{String: *txn.TranID, Valid: true}
		}

		transactions = append(transactions, t)

		// Update KPIs and category transaction lists
		if categoryID.Valid {
			catID := categoryID.Int64
			categoryCount[catID]++
			if withdrawal.Valid {
				debitSum[catID] += withdrawal.Float64
			}
			if deposit.Valid {
				creditSum[catID] += deposit.Float64
			}
			txMap := map[string]interface{}{
				"index":             i,
				"tran_id":           t.TranID.String,
				"value_date":        t.ValueDate,
				"transaction_date":  t.TransactionDate,
				"description":       t.Description,
				"withdrawal_amount": t.WithdrawalAmount.Float64,
				"deposit_amount":    t.DepositAmount.Float64,
				"balance":           t.Balance.Float64,
				"category_id":       catID,
			}
			categoryTxns[catID] = append(categoryTxns[catID], txMap)
		} else {
			uncategorized = append(uncategorized, map[string]interface{}{
				"index":            i,
				"transaction_date": txnDate.Format(constants.DateFormat),
				"value_date":       valDate.Format(constants.DateFormat),
				"description":      txn.Description,
				"withdrawal":       txn.Withdrawal,
				"deposit":          txn.Deposit,
				"balance":          txn.Balance,
			})
		}
	}

	// Check for existing transactions
	existingTxnKeys := make(map[string]bool)
	rowsExisting, err := db.QueryContext(ctx, `
		SELECT account_number, transaction_date, description, withdrawal_amount, deposit_amount
		FROM cimplrcorpsaas.bank_statement_transactions
		WHERE account_number = $1
	`, input.AccountNumber)
	if err == nil {
		defer rowsExisting.Close()
		for rowsExisting.Next() {
			var acc, desc string
			var txnDate time.Time
			var w, d sql.NullFloat64
			if err := rowsExisting.Scan(&acc, &txnDate, &desc, &w, &d); err == nil {
				key := buildTxnKey(acc, txnDate, desc, w, d)
				existingTxnKeys[key] = true
			}
		}
	}

	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert bank statement
	var bankStatementID string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO cimplrcorpsaas.bank_statements (
			entity_id, account_number, statement_period_start, statement_period_end,
			file_hash, opening_balance, closing_balance
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT ON CONSTRAINT uniq_stmt
		DO UPDATE SET file_hash = EXCLUDED.file_hash, closing_balance = EXCLUDED.closing_balance
		RETURNING bank_statement_id
	`, entityID, input.AccountNumber, periodStart, periodEnd, fileHash,
		input.OpeningBalance, input.ClosingBalance).Scan(&bankStatementID)
	if err != nil {
		return nil, fmt.Errorf(constants.ErrFailedToInsertBankStatement, err)
	}

	// Insert transactions
	newTxns := []BankStatementTransaction{}
	reviewTxns := []map[string]interface{}{}

	for _, t := range transactions {
		key := buildTxnKey(t.AccountNumber, t.TransactionDate, t.Description, t.WithdrawalAmount, t.DepositAmount)
		if existingTxnKeys[key] {
			reviewTxns = append(reviewTxns, map[string]interface{}{
				"transaction_date": t.TransactionDate.Format(constants.DateFormat),
				"description":      t.Description,
				"withdrawal":       t.WithdrawalAmount.Float64,
				"deposit":          t.DepositAmount.Float64,
			})
			continue
		}
		newTxns = append(newTxns, t)
	}

	// Check if all transactions are duplicates
	if len(newTxns) == 0 && len(reviewTxns) > 0 {
		return nil, ErrAllTransactionsDuplicate
	}

	// Bulk insert new transactions
	if len(newTxns) > 0 {
		valueStrings := []string{}
		valueArgs := []interface{}{}
		argIdx := 1

		for _, t := range newTxns {
			valueStrings = append(valueStrings, fmt.Sprintf(
				"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				argIdx, argIdx+1, argIdx+2, argIdx+3, argIdx+4, argIdx+5, argIdx+6, argIdx+7, argIdx+8, argIdx+9,
			))
			valueArgs = append(valueArgs, bankStatementID, t.AccountNumber, t.TranID, t.ValueDate,
				t.TransactionDate, t.Description, t.WithdrawalAmount, t.DepositAmount, t.Balance, t.CategoryID)
			argIdx += 10
		}

		insertQuery := fmt.Sprintf(`
			INSERT INTO cimplrcorpsaas.bank_statement_transactions (
				bank_statement_id, account_number, tran_id, value_date, transaction_date,
				description, withdrawal_amount, deposit_amount, balance, category_id
			) VALUES %s
			ON CONFLICT ON CONSTRAINT uniq_transaction DO NOTHING
		`, strings.Join(valueStrings, ","))

		if _, err := tx.Exec(insertQuery, valueArgs...); err != nil {
			return nil, fmt.Errorf("failed to insert transactions: %w", err)
		}
	}

	// Insert audit action
	requestedBy := "system"
	if userID := ctx.Value("user_id"); userID != nil {
		if uid, ok := userID.(string); ok {
			requestedBy = uid
		}
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO cimplrcorpsaas.auditactionbankstatement (
			bankstatementid, actiontype, processing_status, requested_by, requested_at
		) VALUES ($1, $2, $3, $4, $5)
	`, bankStatementID, "CREATE", "PENDING_APPROVAL", requestedBy, time.Now())
	if err != nil {
		return nil, fmt.Errorf(constants.ErrFailedToInsertAuditAction, err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Build KPI response
	kpiCats := []map[string]interface{}{}
	foundCategories := []map[string]interface{}{}
	foundCategoryIDs := map[int64]bool{}
	totalTxns := len(transactions)
	groupedTxns := 0
	for catID, count := range categoryCount {
		// find category name/type from rules
		catName := ""
		for _, rule := range rules {
			if rule.CategoryID == catID {
				catName = rule.CategoryName
				break
			}
		}
		kpiCats = append(kpiCats, map[string]interface{}{
			"category_id":   catID,
			"category_name": catName,
			"count":         count,
			"debit_sum":     debitSum[catID],
			"credit_sum":    creditSum[catID],
			"transactions":  categoryTxns[catID],
		})
		foundCategoryIDs[catID] = true
		groupedTxns += count
	}
	ungroupedTxns := totalTxns - groupedTxns

	groupedPct := 0.0
	ungroupedPct := 0.0
	if totalTxns > 0 {
		groupedPct = (float64(groupedTxns) / float64(totalTxns)) * 100
		ungroupedPct = (float64(ungroupedTxns) / float64(totalTxns)) * 100
	}

	// Add category names from rules
	for _, rule := range rules {
		if foundCategoryIDs[rule.CategoryID] {
			foundCategories = append(foundCategories, map[string]interface{}{
				"category_id":   rule.CategoryID,
				"category_name": rule.CategoryName,
				"category_type": rule.CategoryType,
			})
			delete(foundCategoryIDs, rule.CategoryID)
		}
	}

	result := map[string]interface{}{
		"pages_processed":                 1,
		"bank_wise_status":                []map[string]interface{}{{"account_number": input.AccountNumber, "status": "SUCCESS"}},
		"statement_date_coverage":         map[string]interface{}{"start": periodStart, "end": periodEnd},
		"category_kpis":                   kpiCats,
		"categories_found":                foundCategories,
		"uncategorized":                   uncategorized,
		"bank_statement_id":               bankStatementID,
		"transactions_uploaded_count":     len(transactions),
		"transactions_under_review_count": len(reviewTxns),
		"transactions_under_review":       reviewTxns,
		"grouped_transaction_count":       groupedTxns,
		"ungrouped_transaction_count":     ungroupedTxns,
		"grouped_transaction_percent":     groupedPct,
		"ungrouped_transaction_percent":   ungroupedPct,
		"source":                          "STRUCTURED_INPUT",
	}

	return result, nil
}
func cleanAmount(s string) string {
	s = strings.ReplaceAll(s, ",", "")
	return strings.TrimSpace(s)
}

// buildTxnKey creates a stable key used to detect whether a transaction from
// the uploaded file already exists in the database. It mirrors the UNIQUE
// constraint used in the ON CONFLICT clause: (account_number, transaction_date,
// description, withdrawal_amount, deposit_amount).
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
// for end users to understand. It intentionally hides low-level details like pq/SQLSTATE codes.
func userFriendlyUploadError(err error) string {
	if err == nil {
		return ""
	}

	// Handle known sentinel errors first
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

	// Map common parsing/format issues to user friendly text
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
	log.Println("Debug raw mesage", msg)
	// Map DB write/transaction issues to a generic safe message
	if strings.Contains(msg, "failed to begin db transaction") ||
		strings.Contains(msg, "failed to insert bank statement") ||
		strings.Contains(msg, "failed to upsert bank_balances_manual") ||
		strings.Contains(msg, "failed to bulk insert transactions") ||
		strings.Contains(msg, "failed to insert audit action") ||
		strings.Contains(msg, "failed to commit") {
		return "Something went wrong while saving the uploaded statement. Please try again !!"
	}

	// Hide raw pq / SQL / driver details from end user
	if strings.Contains(msg, "pq:") || strings.Contains(msg, "SQLSTATE") {
		return "Database error while processing the bank statement. Please try again !!"
	}

	// Fallback: return the existing message (most of them are already human readable)
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
