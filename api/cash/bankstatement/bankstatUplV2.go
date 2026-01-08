package bankstatement

import (
	apictx "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"
	"bytes"
	"context"
	"log"

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
	"path/filepath"
	"strings"
	"time"

	"github.com/extrame/xls"
	"github.com/lib/pq"
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

// Sentinel errors used for mapping to user-friendly messages
var (
	ErrFileAlreadyUploaded   = errors.New("bank statement file already uploaded")
	ErrAccountNotFound       = errors.New("bank account not found in master data")
	ErrAccountNumberMissing  = errors.New("account number not found in file header")
	ErrStatementPeriodExists = errors.New("a statement for this period already exists for this account")
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
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty date string")
	}
	layouts := []string{
		"02/01/2006", constants.DateFormatDash, constants.DateFormatSlash, "02/01/06 15:04", "02/01/06 3:04", "02/01/06 15:04:05", "02/01/06 3:04:05",
		constants.DateFormatSlash, constants.DateFormatDash, // for 29/Aug/2025 and 29-Aug-2025
		"2/1/2006", "2-Jan-2006", "2/Jan/2006", "2/1/06", "2/1/06 15:04", "2/1/06 3:04", "2/1/06 15:04:05", "2/1/06 3:04:05",
		constants.DateFormat, "2006/01/02", "2006.01.02", "02.01.2006", "2.1.2006", "02-01-2006", "2-1-2006",
		"02/01/06", "2/1/06", "02-01-06", "2-1-06", "2006/1/2", "2006-1-2",
		"02-Jan-06", constants.DateFormatDash, "02-Jan-06 15:04", "02-Jan-2006 15:04", "02-Jan-06 3:04", "02-Jan-2006 3:04",
		"02-Jan-06 15:04:05", "02-Jan-2006 15:04:05", "02-Jan-06 3:04:05", "02-Jan-2006 3:04:05",
		"02/Jan/06", constants.DateFormatSlash, "02/Jan/06 15:04", "02/Jan/2006 15:04", "02/Jan/06 3:04", "02/Jan/2006 3:04",
		"02/Jan/06 15:04:05", "02/Jan/2006 15:04:05", "02/Jan/06 3:04:05", "02/Jan/2006 3:04:05",
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

	var rows [][]string
	var isCSV bool

	//var isXLS bool Try Excel first
	xl, xlErr := excelize.OpenReader(bytes.NewReader(tmpFile))
	if xlErr == nil {
		defer xl.Close()
		sheetName := xl.GetSheetName(0)
		rows, err = xl.GetRows(sheetName)
		if err != nil {
			return nil, fmt.Errorf("failed to get rows: %w", err)
		}
		if len(rows) < 2 {
			return nil, errors.New("excel must have at least one data row")
		}
		isCSV = false

	} else {
		// Try XLS (legacy Excel)
		xlsBook, xlsErr := xls.OpenReader(bytes.NewReader(tmpFile), "utf-8")
		if xlsErr == nil && xlsBook != nil && xlsBook.NumSheets() > 0 {
			sheet := xlsBook.GetSheet(0)
			if sheet == nil {
				return nil, errors.New("xls sheet is nil")
			}
			for i := 0; i <= int(sheet.MaxRow); i++ {
				row := sheet.Row(i)
				var rowVals []string
				for j := 0; j < row.LastCol(); j++ {
					rowVals = append(rowVals, row.Col(j))
				}
				rows = append(rows, rowVals)
			}
			if len(rows) < 2 {
				return nil, errors.New("xls must have at least one data row")
			}
			isCSV = false

		} else {
			// Try CSV
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
		return nil, ErrAccountNumberMissing
	}

	// Lookup entity_id and bank_name from masterbankaccount/masterbank
	var bankName string
	err = db.QueryRowContext(ctx, `
		SELECT mba.entity_id, mb.bank_name, COALESCE(mba.account_nickname, mb.bank_name)
		FROM public.masterbankaccount mba
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		WHERE mba.account_number = $1 AND mba.is_deleted = false
	`, accountNumber).Scan(&entityID, &bankName, &accountName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAccountNotFound
		}
		return nil, fmt.Errorf("db error while looking up account in masterbankaccount: %w", err)
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
				return nil, errors.New("bank not allowed")
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
				return nil, errors.New("currency not allowed")
			}
		}
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
	if isCSV {
		// For CSV, header is usually: Sl. No., Date, Description, Chq / Ref number, Value Date, Withdrawal, Deposit, Balance, CR/DR
		for i, row := range rows {
			for _, cell := range row {
				if cell == slNoHeader {
					txnHeaderIdx = i
					break
				}
			}
			if txnHeaderIdx != -1 {
				break
			}
		}
		if txnHeaderIdx == -1 {
			return nil, errors.New("transaction header row not found in CSV file")
		}
		headerRow = rows[txnHeaderIdx]
		colIdx = map[string]int{}
		for idx, col := range headerRow {
			colIdx[col] = idx
		}
		// Required columns for CSV
		required := []string{slNoHeader, "Date", "Description", valueDateHeader, "Withdrawal", "Deposit", "Balance"}
		for _, col := range required {
			if _, ok := colIdx[col]; !ok {
				return nil, fmt.Errorf(constants.ErrRequiredColumnNotFound, col)
			}
		}
	} else {
		for i, row := range rows {
			for _, cell := range row {
				if cell == tranIDHeader || cell == "Tran Id" {
					txnHeaderIdx = i
					break
				}
			}
			if txnHeaderIdx != -1 {
				break
			}
		}
		if txnHeaderIdx == -1 {
			return nil, errors.New("transaction header row not found in Excel file")
		}
		headerRow = rows[txnHeaderIdx]
		colIdx = map[string]int{}
		for idx, col := range headerRow {
			colIdx[col] = idx
		}
		required := []string{tranIDHeader, valueDateHeader, transactionDateHeader, transactionRemarksHeader, withdrawalAmtHeader, depositAmtHeader, balanceHeader}
		for _, col := range required {
			if _, ok := colIdx[col]; !ok {
				return nil, fmt.Errorf(constants.ErrRequiredColumnNotFound, col)
			}
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
	var firstValidRow = true
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
	for _, row := range rows[txnHeaderIdx+1:] {
		// Filter out non-transaction rows by checking for known non-transaction keywords in the first column
		if len(row) > 0 {
			firstCell := strings.ToLower(strings.TrimSpace(row[0]))
			if strings.Contains(firstCell, "call 1800") ||
				strings.Contains(firstCell, "write to us") ||
				strings.Contains(firstCell, "closing balance") ||
				strings.Contains(firstCell, "opening balance") ||
				strings.Contains(firstCell, "toll free") {
				continue
			}
		}
		// Defensive: fill missing columns with empty string
		for len(row) < len(headerRow) {
			row = append(row, "")
		}
		var tranID sql.NullString
		var valueDate, transactionDate time.Time
		var description string
		var withdrawal, deposit sql.NullFloat64
		var balance sql.NullFloat64

		if isCSV {
			// CSV: Sl. No., Date, Description, Chq / Ref number, Value Date, Withdrawal, Deposit, Balance, CR/DR
			if len(row) == 0 || (colIdx[slNoHeader] >= len(row)) || strings.TrimSpace(row[colIdx[slNoHeader]]) == "" {
				continue
			}
			valueDate, _ = parseDate(row[colIdx[valueDateHeader]])
			transactionDate, _ = parseDate(row[colIdx["Date"]])
			// Filter out rows with invalid dates (e.g., 01-01-1 or zero date)
			if valueDate.IsZero() || transactionDate.IsZero() || row[colIdx[valueDateHeader]] == constants.DateFormatCustom || row[colIdx["Date"]] == constants.DateFormatCustom {
				continue
			}
			tranID = sql.NullString{String: row[colIdx[slNoHeader]], Valid: row[colIdx[slNoHeader]] != ""}
			description = row[colIdx["Description"]]
			withdrawalStr := cleanAmount(row[colIdx["Withdrawal"]])
			depositStr := cleanAmount(row[colIdx["Deposit"]])
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
			balance = sql.NullFloat64{Valid: row[colIdx["Balance"]] != ""}
			if balance.Valid {
				balanceStr := cleanAmount(row[colIdx["Balance"]])
				fmt.Sscanf(balanceStr, "%f", &balance.Float64)
			}
		} else {
			// Excel/XLS: Tran. Id, Value Date, Transaction Date, Transaction Remarks, Withdrawal Amt (INR), Deposit Amt (INR), Balance (INR)
			if len(row) == 0 || (colIdx[tranIDHeader] >= len(row)) || strings.TrimSpace(row[colIdx[tranIDHeader]]) == "" {
				continue
			}
			valueDate, _ = parseDate(row[colIdx[valueDateHeader]])
			transactionDate, _ = parseDate(row[colIdx[transactionDateHeader]])
			// If either date is zero, try to parse with fallback logic
			if valueDate.IsZero() && row[colIdx[valueDateHeader]] != "" {
				valueDate, _ = parseDate(strings.Split(row[colIdx[valueDateHeader]], " ")[0])
			}
			if transactionDate.IsZero() && row[colIdx[transactionDateHeader]] != "" {
				transactionDate, _ = parseDate(strings.Split(row[colIdx[transactionDateHeader]], " ")[0])
			}
			// Filter out rows with invalid dates (e.g., 01-01-1 or zero date)
			if valueDate.IsZero() || transactionDate.IsZero() || row[colIdx[valueDateHeader]] == constants.DateFormatCustom || row[colIdx[transactionDateHeader]] == constants.DateFormatCustom {
				continue
			}
			tranID = sql.NullString{String: row[colIdx[tranIDHeader]], Valid: row[colIdx[tranIDHeader]] != ""}
			description = row[colIdx[transactionRemarksHeader]]
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
			}
			balance = sql.NullFloat64{Valid: row[colIdx[balanceHeader]] != ""}
			if balance.Valid {
				balanceStr := cleanAmount(row[colIdx[balanceHeader]])
				fmt.Sscanf(balanceStr, "%f", &balance.Float64)
			}
		}
		rowJSON, _ := json.Marshal(row)

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
			CategoryID:       matchedCategoryID,
		})
		if balance.Valid {
			lastValidValueDate = valueDate
			closingBalance = balance.Float64
		}
		if firstValidRow && balance.Valid {
			statementPeriodStart = valueDate
			openingBalance = balance.Float64
			firstValidRow = false
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
	// Fallback: use first and last transaction dates if still zero
	if len(transactions) > 0 {
		// Find first and last non-zero ValueDate
		var firstTxnDate, lastTxnDate time.Time
		for _, t := range transactions {
			if !t.ValueDate.IsZero() {
				if firstTxnDate.IsZero() {
					firstTxnDate = t.ValueDate
				}
				lastTxnDate = t.ValueDate
			}
		}
		if statementPeriodStart.IsZero() && !firstTxnDate.IsZero() {
			statementPeriodStart = firstTxnDate
		}
		if statementPeriodEnd.IsZero() && !lastTxnDate.IsZero() {
			statementPeriodEnd = lastTxnDate
		}
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
		return nil, fmt.Errorf("failed to insert bank statement: %w", err)
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
					t.TranID,
					t.ValueDate,
					t.TransactionDate,
					t.Description,
					t.WithdrawalAmount,
					t.DepositAmount,
					t.Balance,
					t.RawJSON,
					t.CategoryID,
				)
			}
			stmt := `INSERT INTO cimplrcorpsaas.bank_statement_transactions (
				       bank_statement_id, account_number, tran_id, value_date, transaction_date, description, withdrawal_amount, deposit_amount, balance, raw_json, category_id
			       ) VALUES ` +
				joinStrings(valueStrings, ",") +
				` ON CONFLICT (account_number, transaction_date, description, withdrawal_amount, deposit_amount) DO NOTHING`
			if _, err := tx.ExecContext(ctx, stmt, valueArgs...); err != nil {
				tx.Rollback()
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
		return nil, fmt.Errorf("failed to insert audit action: %w", err)
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
	}
	return result, nil
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
													 la.actiontype, la.processing_status, la.action_id, la.requested_by, la.requested_at, la.checker_by, la.checker_at, la.checker_comment, la.reason
										FROM cimplrcorpsaas.bank_statements s
										JOIN public.masterentitycash e ON s.entity_id = e.entity_id
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
			var requestedAt, checkerAt sql.NullTime
			if err := rows.Scan(&id, &entityName, &acc, &start, &end, &open, &close, &uploaded,
				&actionType, &processingStatus, &actionID, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
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
							"error":             "No access to this bank statement",
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
							"error":             "bank not allowed",
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
						"error":             "currency not allowed",
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
							"error":             "No access to this bank statement",
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
							"error":             "No access to this bank statement",
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
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Unable to read the uploaded file. Please try again.",
			})
			return
		}

		file, _, err := r.FormFile("file")
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "File not found in request. Please attach a bank statement file.",
			})
			return
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
	AccountNumber  string                     `json:"account_number"`
	AccountName    string                     `json:"account_name"`
	BankName       string                     `json:"bank_name"`
	IFSC           string                     `json:"ifsc"`
	MICR           string                     `json:"micr"`
	PeriodStart    string                     `json:"period_start"`
	PeriodEnd      string                     `json:"period_end"`
	OpeningBalance float64                    `json:"opening_balance"`
	ClosingBalance float64                    `json:"closing_balance"`
	Transactions   []PDFTransactionJSON       `json:"transactions"`
}

type PDFTransactionJSON struct {
	TranID          *string  `json:"tran_id"`
	TransactionDate string   `json:"transaction_date"`
	ValueDate       string   `json:"value_date"`
	Description     string   `json:"description"`
	Withdrawal      float64  `json:"withdrawal"`
	Deposit         float64  `json:"deposit"`
	Balance         float64  `json:"balance"`
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
		return fmt.Errorf("db error while looking up account in masterbankaccount: %w", err)
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

	// 4. Extract statement period and balances from the file (assume first/last row for balances)
	var (
		statementPeriodStart, statementPeriodEnd time.Time
		openingBalance, closingBalance           float64
		transactions                             []BankStatementTransaction
	)
	// Find the header row for transactions
	var txnHeaderIdx int = -1
	for i, row := range rows {
		for _, cell := range row {
			if cell == tranIDHeader || cell == "Tran Id" {
				txnHeaderIdx = i
				goto foundTxnHeader
			}
		}
	}
foundTxnHeader:
	if txnHeaderIdx == -1 {
		return errors.New("transaction header row not found in Excel file")
	}

	// Map columns by header name for flexibility
	headerRow := rows[txnHeaderIdx]
	colIdx := map[string]int{}
	for idx, col := range headerRow {
		colIdx[col] = idx
	}

	// Required columns
	required := []string{tranIDHeader, valueDateHeader, transactionDateHeader, transactionRemarksHeader, withdrawalAmtHeader, depositAmtHeader, balanceHeader}
	for _, col := range required {
		if _, ok := colIdx[col]; !ok {
			return fmt.Errorf(constants.ErrRequiredColumnNotFound, col)
		}
	}

	// Parse transactions, skip header
	var lastValidBalance sql.NullFloat64
	var lastValidValueDate time.Time
	for i, row := range rows[txnHeaderIdx+1:] {
		// Skip rows with no transaction ID or all columns empty
		if len(row) == 0 || (colIdx[tranIDHeader] >= len(row)) || strings.TrimSpace(row[colIdx[tranIDHeader]]) == "" {
			continue
		}
		// Defensive: fill missing columns with empty string
		for len(row) < len(headerRow) {
			row = append(row, "")
		}
		tranID := sql.NullString{String: row[colIdx[tranIDHeader]], Valid: row[colIdx[tranIDHeader]] != ""}
		valueDate, _ := time.Parse(constants.DateFormatSlash, row[colIdx[valueDateHeader]])
		transactionDate, _ := time.Parse(constants.DateFormatSlash, row[colIdx[transactionDateHeader]])
		description := row[colIdx[transactionRemarksHeader]]
		var withdrawal, deposit sql.NullFloat64
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
		return fmt.Errorf("failed to insert bank statement: %w", err)
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
			return errors.New("bank not allowed")
		}
	}
	if ctx.Value("ApprovedBankAccounts") != nil {
		if !ctxHasApprovedBankAccount(ctx, accountNumber) {
			return ErrAccountNotFound
		}
	}
	if !ctxHasApprovedCurrency(ctx, currencyCode) {
		return errors.New("currency not allowed")
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
		return fmt.Errorf("failed to insert audit action: %w", err)
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
		"api/cash/bankstatement/bank.json",                                    // From project root
		"../api/cash/bankstatement/bank.json",                                 // From cmd directory
		filepath.Join("..", "api", "cash", "bankstatement", "bank.json"),      // Explicit relative
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

	// Generate file hash from JSON content for idempotency
	hash := sha256.Sum256(jsonBytes)
	fileHash := fmt.Sprintf("%x", hash[:])

	// Lookup entity_id from masterbankaccount
	var entityID, accountName, bankName string
	err = db.QueryRowContext(ctx, `
		SELECT mba.entity_id, mb.bank_name, COALESCE(mba.account_nickname, mb.bank_name)
		FROM public.masterbankaccount mba
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		WHERE mba.account_number = $1 AND mba.is_deleted = false
	`, pdfData.AccountNumber).Scan(&entityID, &bankName, &accountName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("account %s not found in master data", pdfData.AccountNumber)
		}
		return nil, fmt.Errorf("failed to lookup account: %w", err)
	}

	// Parse period dates
	periodStart, err := time.Parse("2006-01-02", pdfData.PeriodStart)
	if err != nil {
		return nil, fmt.Errorf("invalid period_start date: %w", err)
	}
	periodEnd, err := time.Parse("2006-01-02", pdfData.PeriodEnd)
	if err != nil {
		return nil, fmt.Errorf("invalid period_end date: %w", err)
	}

	// Load category rules for this account
	var acctCurrency sql.NullString
	db.QueryRowContext(ctx, `SELECT currency FROM public.masterbankaccount WHERE account_number = $1`, pdfData.AccountNumber).Scan(&acctCurrency)
	
	var currencyCode string
	if acctCurrency.Valid {
		currencyCode = acctCurrency.String
	}
	rules, err := loadCategoryRuleComponents(ctx, db, pdfData.AccountNumber, entityID, currencyCode)
	if err != nil {
		return nil, fmt.Errorf("failed to load category rules: %w", err)
	}

	// Convert JSON transactions to BankStatementTransaction
	transactions := []BankStatementTransaction{}
	categoryCount := map[int64]int{}
	debitSum := map[int64]float64{}
	creditSum := map[int64]float64{}
	uncategorized := []map[string]interface{}{}

	for _, txn := range pdfData.Transactions {
		txnDate, err := time.Parse("2006-01-02", txn.TransactionDate)
		if err != nil {
			log.Printf("[PDF_PROCESSOR] Warning: failed to parse transaction_date %s, skipping", txn.TransactionDate)
			continue
		}
		valDate, err := time.Parse("2006-01-02", txn.ValueDate)
		if err != nil {
			log.Printf("[PDF_PROCESSOR] Warning: failed to parse value_date %s, skipping", txn.ValueDate)
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
			AccountNumber:    pdfData.AccountNumber,
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

		// Update KPIs
		if categoryID.Valid {
			categoryCount[categoryID.Int64]++
			if withdrawal.Valid {
				debitSum[categoryID.Int64] += withdrawal.Float64
			}
			if deposit.Valid {
				creditSum[categoryID.Int64] += deposit.Float64
			}
		} else {
			uncategorized = append(uncategorized, map[string]interface{}{
				"transaction_date": txnDate.Format("2006-01-02"),
				"description":      txn.Description,
				"withdrawal":       txn.Withdrawal,
				"deposit":          txn.Deposit,
			})
		}
	}

	log.Printf("[PDF_PROCESSOR] Processed %d transactions, %d categorized, %d uncategorized",
		len(transactions), len(transactions)-len(uncategorized), len(uncategorized))

	// Check for existing transactions
	existingTxnKeys := make(map[string]bool)
	rowsExisting, err := db.QueryContext(ctx, `
		SELECT account_number, transaction_date, description, withdrawal_amount, deposit_amount
		FROM cimplrcorpsaas.bank_statement_transactions
		WHERE account_number = $1
	`, pdfData.AccountNumber)
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
	`, entityID, pdfData.AccountNumber, periodStart, periodEnd, fileHash, 
		pdfData.OpeningBalance, pdfData.ClosingBalance).Scan(&bankStatementID)
	if err != nil {
		return nil, fmt.Errorf("failed to insert bank statement: %w", err)
	}

	log.Printf("[PDF_PROCESSOR] Created bank_statement_id: %s", bankStatementID)

	// Insert transactions
	newTxns := []BankStatementTransaction{}
	reviewTxns := []map[string]interface{}{}

	for _, t := range transactions {
		key := buildTxnKey(t.AccountNumber, t.TransactionDate, t.Description, t.WithdrawalAmount, t.DepositAmount)
		if existingTxnKeys[key] {
			reviewTxns = append(reviewTxns, map[string]interface{}{
				"transaction_date": t.TransactionDate.Format("2006-01-02"),
				"description":      t.Description,
				"withdrawal":       t.WithdrawalAmount.Float64,
				"deposit":          t.DepositAmount.Float64,
			})
			continue
		}
		newTxns = append(newTxns, t)
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

	log.Printf("[PDF_PROCESSOR] Inserted %d new transactions, %d under review", len(newTxns), len(reviewTxns))

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
		return nil, fmt.Errorf("failed to insert audit action: %w", err)
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
	ungroupedTxns := 0

	for catID, count := range categoryCount {
		kpiCats = append(kpiCats, map[string]interface{}{
			"category_id": catID,
			"count":       count,
			"debit_sum":   debitSum[catID],
			"credit_sum":  creditSum[catID],
		})
		foundCategoryIDs[catID] = true
		groupedTxns += count
	}
	ungroupedTxns = totalTxns - groupedTxns

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
		"bank_wise_status":                []map[string]interface{}{{"account_number": pdfData.AccountNumber, "status": "SUCCESS"}},
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
		"source":                          "PDF_JSON",
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
