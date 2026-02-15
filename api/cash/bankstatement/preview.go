package bankstatement

import (
	"CimplrCorpSaas/api/constants"
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/shakinm/xlsReader/xls"
	"github.com/xuri/excelize/v2"
)

// PreviewBankStatementHandler parses uploaded file(s), categorizes transactions, returns FLAT transaction list WITHOUT DB insertion
// Uses EXACT same parsing logic as UploadBankStatementV2WithCategorization but NO database writes
// Supports: XLSX, XLS, CSV, multi-account CSV (multi=true), PDF/DOCX (external AI), ZIP (containing any of above)
func PreviewBankStatementHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := r.Context()

		// Parse multipart form
		err := r.ParseMultipartForm(100 << 20) // 100MB max
		if err != nil {
			http.Error(w, "Failed to parse multipart form: "+err.Error(), http.StatusBadRequest)
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "Missing 'file' field: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Check for custom mappings (same as upload handler)
		useMapping := r.FormValue("useMapping") == "true"
		var mappings *ColumnMappings
		if useMapping {
			mappingsJSON := r.FormValue("mappings")
			if mappingsJSON != "" {
				mappings = &ColumnMappings{}
				if err := json.Unmarshal([]byte(mappingsJSON), mappings); err != nil {
					http.Error(w, "Invalid mappings JSON: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
		}

		// Check for multi-account CSV flag
		isMultiAccount := r.FormValue("multi") == "true"

		// Read file into memory
		fileBytes, err := io.ReadAll(file)
		if err != nil {
			http.Error(w, "Failed to read file: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ext := strings.ToLower(filepath.Ext(header.Filename))

		var allTransactions []map[string]interface{}

		// Route based on file type
		if ext == ".zip" {
			// ZIP with multiple files
			allTransactions, err = processZipPreviewFlat(ctx, db, fileBytes, useMapping, mappings)
			if err != nil {
				http.Error(w, "ZIP processing failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
		} else if ext == ".pdf" || ext == ".docx" {
			// PDF/DOCX - call external AI parser (NO DB writes)
			allTransactions, err = processPDFPreviewFlat(ctx, db, fileBytes, header.Filename)
			if err != nil {
				http.Error(w, "PDF/DOCX processing failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
		} else if ext == ".csv" && isMultiAccount {
			// Multi-account CSV
			allTransactions, err = processMultiAccountCSVPreviewFlat(ctx, db, fileBytes)
			if err != nil {
				http.Error(w, "Multi-account CSV processing failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
		} else {
			// Single file processing (XLSX, XLS, CSV)
			transactions, err := processSingleFilePreviewFlat(ctx, db, fileBytes, header.Filename, useMapping, mappings)
			if err != nil {
				http.Error(w, "File processing failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			allTransactions = transactions
		}

		response := map[string]interface{}{
			"success": true,
			"data":    allTransactions,
			"count":   len(allTransactions),
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(response)
	})
}

// processZipPreviewFlat extracts all files from ZIP and returns flat transaction list
func processZipPreviewFlat(ctx context.Context, db *sql.DB, zipBytes []byte, useMapping bool, mappings *ColumnMappings) ([]map[string]interface{}, error) {
	zipReader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("invalid ZIP file: %w", err)
	}

	allTransactions := []map[string]interface{}{}

	for _, f := range zipReader.File {
		if f.FileInfo().IsDir() {
			continue
		}

		// Skip macOS metadata and hidden files
		if isJunkFile(f.Name) {
			continue
		}

		ext := strings.ToLower(filepath.Ext(f.Name))
		if ext != ".xlsx" && ext != ".xls" && ext != ".csv" {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			continue
		}

		fileBytes, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			continue
		}

		transactions, err := processSingleFilePreviewFlat(ctx, db, fileBytes, f.Name, useMapping, mappings)
		if err != nil {
			// Skip files with errors, continue processing others
			continue
		}

		allTransactions = append(allTransactions, transactions...)
	}

	return allTransactions, nil
}

// processSingleFilePreviewFlat parses file and categorizes WITHOUT any DB writes
// Uses EXACT same parsing logic as UploadBankStatementV2WithCategorization but ONLY reads DB for account lookup and rules
func processSingleFilePreviewFlat(ctx context.Context, db *sql.DB, fileBytes []byte, filename string, useMapping bool, mappings *ColumnMappings) ([]map[string]interface{}, error) {

	ext := strings.ToLower(filepath.Ext(filename))
	var rows [][]string
	var isCSV bool

	// Parse file with EXACT same logic as upload
	switch ext {
	case ".xlsx":
		rows, _ = parseExcelFile(fileBytes)
		isCSV = false
	case ".xls":
		rows, _ = parseXLSFile(fileBytes)
		isCSV = false
	case ".csv":
		rows, _ = parseCSVFile(fileBytes)
		isCSV = true
	default:
		return nil, fmt.Errorf("unsupported file format: %s", ext)
	}

	if len(rows) < 2 {
		return nil, fmt.Errorf("file must have at least one data row")
	}

	// Extract account number using EXACT upload logic with all fallbacks
	var accountNumber string
	acNoHeader := "A/C No:"

	// Custom mapping first
	if mappings != nil && mappings.AccountNumber != "" {
		for i := 0; i < len(rows) && i < 20; i++ {
			for j, cell := range rows[i] {
				normCell := normalizeCell(cell)
				if strings.EqualFold(normCell, mappings.AccountNumber) {
					if j+1 < len(rows[i]) {
						match := regexp.MustCompile(`\d{6,}`).FindString(rows[i][j+1])
						if match != "" {
							accountNumber = strings.ReplaceAll(strings.ReplaceAll(match, " ", ""), "-", "")
						}
					}
					if accountNumber == "" && i+1 < len(rows) && j < len(rows[i+1]) {
						match := regexp.MustCompile(`\d{6,}`).FindString(rows[i+1][j])
						if match != "" {
							accountNumber = strings.ReplaceAll(strings.ReplaceAll(match, " ", ""), "-", "")
						}
					}
				}
			}
		}
	}

	// Default extraction if custom mapping failed
	if accountNumber == "" {
		if isCSV {
			for i := 0; i < 20 && i < len(rows); i++ {
				for j, cell := range rows[i] {
					if (cell == acNoHeader || strings.EqualFold(cell, "Account Number") || strings.EqualFold(cell, "Account No.")) && j+1 < len(rows[i]) {
						accountNumber = rows[i][j+1]
					}
				}
			}
		} else {
			for i := 0; i < 20 && i < len(rows); i++ {
				for j, cell := range rows[i] {
					if cell == acNoHeader && j+1 < len(rows[i]) {
						accountNumber = rows[i][j+1]
					}
				}
			}
		}
	}

	// Fallback: regex-based extraction with label detection
	if accountNumber == "" {
		acctLabelRe := regexp.MustCompile(`(?i)\b(?:a[/\\]?c|acct|account)[\s\.:#-]*(?:no|number)?\b`)
		acctNumberRe := regexp.MustCompile(`\d{6,}`)

		// First: look for title line with "transactions list"
		titleDigitRe := regexp.MustCompile(`\d{7,}`)
		for i := 0; i < 40 && i < len(rows); i++ {
			for _, cell := range rows[i] {
				v := strings.ToLower(normalizeCell(cell))
				if strings.Contains(v, "transactions list") {
					if m := titleDigitRe.FindString(v); m != "" {
						accountNumber = strings.ReplaceAll(m, " ", "")
						break
					}
				}
			}
			if accountNumber != "" {
				break
			}
		}

		// Second: label-based extraction (same cell, adjacent, below)
		if accountNumber == "" {
			for i := 0; i < 20 && i < len(rows); i++ {
				for j, cell := range rows[i] {
					v := normalizeCell(cell)
					if acctLabelRe.MatchString(v) {
						// Same cell
						if m := acctNumberRe.FindString(v); m != "" {
							accountNumber = strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
							break
						}
						// Adjacent right
						if j+1 < len(rows[i]) {
							cand := normalizeCell(rows[i][j+1])
							if m := acctNumberRe.FindString(cand); m != "" {
								accountNumber = strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
								break
							}
						}
						// Below same column
						if i+1 < len(rows) && j < len(rows[i+1]) {
							cand := normalizeCell(rows[i+1][j])
							if m := acctNumberRe.FindString(cand); m != "" {
								accountNumber = strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
								break
							}
						}
					}
				}
				if accountNumber != "" {
					break
				}
			}
		}

		// Third: scan all header cells for 7+ digit sequences
		if accountNumber == "" {
			for i := 0; i < 20 && i < len(rows); i++ {
				for _, cell := range rows[i] {
					v := normalizeCell(cell)
					if m := titleDigitRe.FindString(v); m != "" {
						accountNumber = strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
						break
					}
				}
				if accountNumber != "" {
					break
				}
			}
		}
	}

	if accountNumber == "" {
		return nil, fmt.Errorf("account number not found in file header")
	}

	// DB READ ONLY: Lookup account metadata
	var entityID, bankName, currency string
	err := db.QueryRowContext(ctx, `
		SELECT mba.entity_id, COALESCE(mb.bank_name, ''), COALESCE(mba.currency, 'INR')
		FROM public.masterbankaccount mba
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		WHERE mba.account_number = $1 AND COALESCE(mba.is_deleted, false) = false
	`, accountNumber).Scan(&entityID, &bankName, &currency)

	if err != nil {
		if err == sql.ErrNoRows {
			// Try candidate verification like upload does
			candidates := []string{}
			seen := map[string]bool{}
			acctNumberRe := regexp.MustCompile(`\d{7,}`)
			for i := 0; i < 20 && i < len(rows); i++ {
				for _, cell := range rows[i] {
					v := normalizeCell(cell)
					for _, m := range acctNumberRe.FindAllString(v, -1) {
						cand := strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
						if cand != "" && !seen[cand] {
							seen[cand] = true
							candidates = append(candidates, cand)
						}
					}
				}
			}

			matched := false
			for _, cand := range candidates {
				qErr := db.QueryRowContext(ctx, `
					SELECT mba.entity_id, COALESCE(mb.bank_name, ''), COALESCE(mba.currency, 'INR')
					FROM public.masterbankaccount mba
					LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
					WHERE mba.account_number = $1 AND COALESCE(mba.is_deleted, false) = false
				`, cand).Scan(&entityID, &bankName, &currency)
				if qErr == nil {
					accountNumber = cand
					matched = true
					break
				}
			}
			if !matched {
				return nil, fmt.Errorf("account %s not found in master data", accountNumber)
			}
		} else {
			return nil, fmt.Errorf("database lookup failed: %w", err)
		}
	}

	// DB READ ONLY: Load category rules
	rules, err := loadCategoryRuleComponents(ctx, db, accountNumber, entityID, currency)
	if err != nil {
		return nil, fmt.Errorf("failed to load category rules: %w", err)
	}

	// Find transaction header row (EXACT upload logic)
	var txnHeaderIdx int = -1
	if isCSV {
		slNoHeader := "Sl. No."
		for i, row := range rows {
			for _, cell := range row {
				if strings.EqualFold(strings.TrimSpace(cell), slNoHeader) {
					txnHeaderIdx = i
					break
				}
			}
			if txnHeaderIdx != -1 {
				break
			}
		}
		// Fallback: look for Date + Description/Amount columns
		if txnHeaderIdx == -1 {
			for i, row := range rows {
				hasDate := false
				hasDesc := false
				hasAmount := false
				for _, cell := range row {
					lc := strings.ToLower(strings.TrimSpace(cell))
					if strings.Contains(lc, "date") {
						hasDate = true
					}
					if strings.Contains(lc, "description") || strings.Contains(lc, "remarks") || strings.Contains(lc, "narration") {
						hasDesc = true
					}
					if strings.Contains(lc, "withdrawal") || strings.Contains(lc, "deposit") || strings.Contains(lc, "debit") || strings.Contains(lc, "credit") || strings.Contains(lc, "amount") {
						hasAmount = true
					}
				}
				if hasDate && (hasDesc || hasAmount) {
					txnHeaderIdx = i
					break
				}
			}
		}
	} else {
		// Excel: look for row with Date + Amount columns
		for i, row := range rows {
			hasDate := false
			hasAmount := false
			for _, cell := range row {
				lc := strings.ToLower(strings.TrimSpace(cell))
				if strings.Contains(lc, "date") {
					hasDate = true
				}
				if strings.Contains(lc, "withdrawal") || strings.Contains(lc, "deposit") || strings.Contains(lc, "debit") || strings.Contains(lc, "credit") || strings.Contains(lc, "amount") || strings.Contains(lc, "balance") {
					hasAmount = true
				}
			}
			if hasDate && hasAmount {
				txnHeaderIdx = i
				break
			}
		}
	}

	if txnHeaderIdx == -1 {
		return nil, fmt.Errorf("transaction header row not found")
	}

	headerRow := rows[txnHeaderIdx]

	// Build column mapping (simplified from upload)
	colIdx := make(map[string]int)
	for idx, col := range headerRow {
		colIdx[strings.TrimSpace(col)] = idx
	}

	// Apply custom mappings if provided
	if mappings != nil {
		if mappings.TranID != "" {
			for idx, col := range headerRow {
				if strings.EqualFold(strings.TrimSpace(col), mappings.TranID) {
					colIdx[constants.TranID] = idx
					break
				}
			}
		}
		if mappings.ValueDate != "" {
			for idx, col := range headerRow {
				if strings.EqualFold(strings.TrimSpace(col), mappings.ValueDate) {
					colIdx[constants.ValueDateAlt] = idx
					break
				}
			}
		}
		if mappings.Description != "" {
			for idx, col := range headerRow {
				if strings.EqualFold(strings.TrimSpace(col), mappings.Description) {
					colIdx[constants.TransactionRemarks] = idx
					colIdx["Description"] = idx
					break
				}
			}
		}
		if mappings.WithdrawalAmount != "" {
			for idx, col := range headerRow {
				if strings.EqualFold(strings.TrimSpace(col), mappings.WithdrawalAmount) {
					colIdx[constants.WithdrawalAmountINR] = idx
					break
				}
			}
		}
		if mappings.DepositAmount != "" {
			for idx, col := range headerRow {
				if strings.EqualFold(strings.TrimSpace(col), mappings.DepositAmount) {
					colIdx[constants.DepositAmountINR] = idx
					break
				}
			}
		}
		if mappings.Balance != "" {
			for idx, col := range headerRow {
				if strings.EqualFold(strings.TrimSpace(col), mappings.Balance) {
					colIdx[constants.BalanceINR] = idx
					break
				}
			}
		}
	}

	// Auto-detect common columns
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

	if _, ok := colIdx[constants.TransactionDateAlt]; !ok {
		if idx := findColContaining("transaction date", "txn date", "posted date"); idx >= 0 {
			colIdx[constants.TransactionDateAlt] = idx
		}
	}
	if _, ok := colIdx[constants.ValueDateAlt]; !ok {
		if idx := findColContaining("value date", "val date"); idx >= 0 {
			colIdx[constants.ValueDateAlt] = idx
		}
	}
	if _, ok := colIdx["Description"]; !ok {
		if idx := findColContaining("description", "remarks", "narration", "particulars"); idx >= 0 {
			colIdx["Description"] = idx
		}
	}
	if _, ok := colIdx[constants.TransactionRemarks]; !ok {
		if idx := findColContaining("transaction remarks", "description", "remarks"); idx >= 0 {
			colIdx[constants.TransactionRemarks] = idx
		}
	}
	if _, ok := colIdx[constants.WithdrawalAmountINR]; !ok {
		if idx := findColContaining("withdrawal", "debit"); idx >= 0 {
			colIdx[constants.WithdrawalAmountINR] = idx
		}
	}
	if _, ok := colIdx[constants.DepositAmountINR]; !ok {
		if idx := findColContaining("deposit", "credit"); idx >= 0 {
			colIdx[constants.DepositAmountINR] = idx
		}
	}
	if _, ok := colIdx[constants.BalanceINR]; !ok {
		if idx := findColContaining("balance"); idx >= 0 {
			colIdx[constants.BalanceINR] = idx
		}
	}
	if _, ok := colIdx[constants.TranID]; !ok {
		if idx := findColContaining("tran id", "transaction id", "txn id"); idx >= 0 {
			colIdx[constants.TranID] = idx
		}
	}

	// Parse transactions
	transactions := []map[string]interface{}{}

	for i := txnHeaderIdx + 1; i < len(rows); i++ {
		row := rows[i]

		if isEmptyRow(row) {
			continue
		}

		// Skip non-transaction rows
		if len(row) > 0 {
			firstCell := strings.ToLower(strings.TrimSpace(row[0]))
			if strings.Contains(firstCell, "call 1800") ||
				strings.Contains(firstCell, "write to us") ||
				strings.Contains(firstCell, "closing balance") ||
				strings.Contains(firstCell, "opening balance") ||
				strings.Contains(firstCell, "balance carried forward") ||
				strings.Contains(firstCell, "toll free") {
				continue
			}
		}

		// Pad row
		for len(row) < len(headerRow) {
			row = append(row, "")
		}

		txn := map[string]interface{}{
			"account_number":     accountNumber,
			"entity_id":          entityID,
			"entity_name":        entityID,
			"bank_name":          bankName,
			"currency":           currency,
			"misclassified_flag": false,
		}

		// Extract fields
		if idx, ok := colIdx[constants.TranID]; ok && idx < len(row) {
			txn["tran_id"] = strings.TrimSpace(row[idx])
		}

		// Parse dates
		var transactionDate, valueDate time.Time
		if idx, ok := colIdx[constants.TransactionDateAlt]; ok && idx < len(row) {
			if dt, err := parseDate(row[idx]); err == nil {
				transactionDate = dt
			}
		}
		if idx, ok := colIdx[constants.ValueDateAlt]; ok && idx < len(row) {
			if dt, err := parseDate(row[idx]); err == nil {
				valueDate = dt
			}
		}
		if idx, ok := colIdx["Date"]; ok && idx < len(row) && transactionDate.IsZero() && valueDate.IsZero() {
			if dt, err := parseDate(row[idx]); err == nil {
				transactionDate = dt
				valueDate = dt
			}
		}

		// Mirror dates if one is missing
		if transactionDate.IsZero() && !valueDate.IsZero() {
			transactionDate = valueDate
		}
		if valueDate.IsZero() && !transactionDate.IsZero() {
			valueDate = transactionDate
		}

		// Skip if no dates
		if transactionDate.IsZero() {
			continue
		}

		txn["transaction_date"] = transactionDate.Format(time.RFC3339)
		txn["value_date"] = valueDate.Format(time.RFC3339)

		// Description
		description := ""
		if idx, ok := colIdx[constants.TransactionRemarks]; ok && idx < len(row) {
			description = sanitizeForPostgres(normalizeCell(row[idx]))
		} else if idx, ok := colIdx["Description"]; ok && idx < len(row) {
			description = sanitizeForPostgres(normalizeCell(row[idx]))
		}
		txn["description"] = description

		// Amounts
		var withdrawal, deposit sql.NullFloat64

		if idx, ok := colIdx[constants.WithdrawalAmountINR]; ok && idx < len(row) {
			if val, err := parseAmount(row[idx]); err == nil && val > 0 {
				withdrawal = sql.NullFloat64{Float64: val, Valid: true}
				txn["withdrawal_amount"] = val
			} else {
				txn["withdrawal_amount"] = 0
			}
		} else {
			txn["withdrawal_amount"] = 0
		}

		if idx, ok := colIdx[constants.DepositAmountINR]; ok && idx < len(row) {
			if val, err := parseAmount(row[idx]); err == nil && val > 0 {
				deposit = sql.NullFloat64{Float64: val, Valid: true}
				txn["deposit_amount"] = val
			} else {
				txn["deposit_amount"] = 0
			}
		} else {
			txn["deposit_amount"] = 0
		}

		if idx, ok := colIdx[constants.BalanceINR]; ok && idx < len(row) {
			if val, err := parseAmount(row[idx]); err == nil {
				txn["balance"] = val
			}
		}

		// Apply categorization
		categoryID := matchCategoryForTransaction(rules, description, withdrawal, deposit)
		if categoryID.Valid && categoryID.String != "" {
			txn["category_id"] = categoryID.String
			for _, rule := range rules {
				if rule.CategoryID == categoryID.String {
					txn["category_name"] = rule.CategoryName
					break
				}
			}
		} else {
			txn["category_name"] = "Uncategorized"
		}

		transactions = append(transactions, txn)
	}

	return transactions, nil
}

// isJunkFile returns true for macOS metadata files and other files to skip
func isJunkFile(filename string) bool {
	base := filepath.Base(filename)
	dir := filepath.Dir(filename)

	// Skip hidden files (starting with .)
	if strings.HasPrefix(base, ".") {
		return true
	}

	// Skip macOS resource fork files (starting with ._)
	if strings.HasPrefix(base, "._") {
		return true
	}

	// Skip __MACOSX directory and its contents
	if strings.Contains(dir, "__MACOSX") || strings.HasPrefix(dir, "__MACOSX") {
		return true
	}

	// Skip .DS_Store files
	if base == ".DS_Store" {
		return true
	}

	return false
}

// parseExcelFile parses XLSX file (EXACT same logic as upload)
func parseExcelFile(data []byte) ([][]string, error) {
	xl, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer xl.Close()

	sheetName := xl.GetSheetName(0)
	rawRows, err := xl.GetRows(sheetName)
	if err != nil {
		return nil, err
	}
	if len(rawRows) < 2 {
		return nil, fmt.Errorf("excel must have at least one data row")
	}

	rows := make([][]string, len(rawRows))
	for i, rawRow := range rawRows {
		rows[i] = make([]string, len(rawRow))
		for j := range rawRow {
			colName, _ := excelize.ColumnNumberToName(j + 1)
			cellRef := fmt.Sprintf("%s%d", colName, i+1)
			cellValue, cellErr := xl.GetCellValue(sheetName, cellRef)
			if cellErr == nil && cellValue != "" {
				rows[i][j] = cellValue
			} else {
				rows[i][j] = rawRow[j]
			}
		}
	}

	return rows, nil
}

// parseXLSFile parses legacy XLS file (EXACT same logic as upload)
func parseXLSFile(data []byte) ([][]string, error) {
	tmpFile, err := os.CreateTemp("", "preview-*.xls")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	_, err = tmpFile.Write(data)
	if err != nil {
		return nil, err
	}
	tmpFile.Close()

	xlsBook, err := xls.OpenFile(tmpFile.Name())
	if err != nil {
		return nil, err
	}

	sheet, err := xlsBook.GetSheet(0)
	if err != nil || sheet == nil {
		return nil, fmt.Errorf("no sheets found")
	}

	rows := [][]string{}
	xlsRows := sheet.GetRows()
	for _, xlsRow := range xlsRows {
		rowData := []string{}
		cols := xlsRow.GetCols()
		for _, col := range cols {
			rowData = append(rowData, col.GetString())
		}
		rows = append(rows, rowData)
	}

	if len(rows) < 2 {
		return nil, fmt.Errorf("xls must have at least one data row")
	}

	return rows, nil
}

// parseCSVFile parses CSV file (EXACT same logic as upload)
func parseCSVFile(data []byte) ([][]string, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("csv must have at least one data row")
	}
	return rows, nil
}

// parseAmount converts string to float64
func parseAmount(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return 0, nil
	}

	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "₹", "")
	s = strings.ReplaceAll(s, "$", "")
	s = strings.TrimSpace(s)

	return strconv.ParseFloat(s, 64)
}

// isEmptyRow checks if all cells are empty
func isEmptyRow(row []string) bool {
	for _, cell := range row {
		if strings.TrimSpace(cell) != "" {
			return false
		}
	}
	return true
}

// processMultiAccountCSVPreviewFlat processes CSV with multiple account numbers (multi=true)
// Each row can have different account number in the account column
func processMultiAccountCSVPreviewFlat(ctx context.Context, db *sql.DB, fileBytes []byte) ([]map[string]interface{}, error) {
	// Parse CSV
	rdr := csv.NewReader(bytes.NewReader(fileBytes))
	rdr.FieldsPerRecord = -1
	rdr.LazyQuotes = true
	rdr.TrimLeadingSpace = true
	rows, err := rdr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("CSV must have at least one data row")
	}

	header := rows[0]
	fmt.Printf("[PREVIEW-MULTI] CSV has %d rows, header: %v\n", len(rows), header)

	// Find column indices
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

	accIdx := findIdx("account number", "account_no", "account", "a/c no", "acc no")
	dateIdx := findIdx("transaction date", "date", "txn date", "statement date")
	valDateIdx := findIdx("value date", "val date")
	descIdx := findIdx("description", "transaction description", "remarks", "narration", "particulars")
	debitIdx := findIdx("debit", "withdrawal", "debit amount", "withdrawal amount")
	creditIdx := findIdx("credit", "deposit", "credit amount", "deposit amount")
	balanceIdx := findIdx("balance", "available balance", "closing balance", "current / closing")
	tranIDIdx := findIdx("transaction id", "tran id", "txn id", "reference", "ref no", "bank reference")

	fmt.Printf("[PREVIEW-MULTI] Column indices - acc:%d date:%d desc:%d debit:%d credit:%d balance:%d\n",
		accIdx, dateIdx, descIdx, debitIdx, creditIdx, balanceIdx)

	if accIdx == -1 {
		return nil, fmt.Errorf("CSV must contain an account number column")
	}

	allTransactions := []map[string]interface{}{}

	// OPTIMIZATION: Load ALL accounts in ONE query to avoid per-row DB calls with context timeout
	fmt.Printf("[PREVIEW-MULTI] Loading all accounts from database...\n")
	accountCache := make(map[string]struct {
		entityID string
		bankName string
		currency string
		rules    []categoryRuleComponent
	})

	// Use background context for initial load (not tied to HTTP request timeout)
	loadCtx := context.Background()
	accountRows, err := db.QueryContext(loadCtx, `
		SELECT mba.account_number, mba.entity_id, COALESCE(mb.bank_name, ''), COALESCE(mba.currency, 'INR')
		FROM public.masterbankaccount mba
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		WHERE COALESCE(mba.is_deleted, false) = false
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to load accounts: %w", err)
	}
	defer accountRows.Close()

	for accountRows.Next() {
		var accNum, entityID, bankName, currency string
		if err := accountRows.Scan(&accNum, &entityID, &bankName, &currency); err != nil {
			continue
		}

		// Also pre-load rules for this account
		rules, err := loadCategoryRuleComponents(loadCtx, db, accNum, entityID, currency)
		if err != nil {
			rules = []categoryRuleComponent{}
		}

		accountCache[accNum] = struct {
			entityID string
			bankName string
			currency string
			rules    []categoryRuleComponent
		}{entityID, bankName, currency, rules}
	}
	fmt.Printf("[PREVIEW-MULTI] Pre-loaded %d accounts from database\n", len(accountCache))

	skippedNoAccount := 0
	skippedNoDate := 0
	skippedUnknownAccount := 0
	processed := 0

	// Process each data row (NO MORE DB QUERIES - just map lookups)
	for ri := 1; ri < len(rows); ri++ {
		row := rows[ri]

		// Get account number from this row
		if accIdx >= len(row) {
			skippedNoAccount++
			continue
		}
		accNum := strings.TrimSpace(row[accIdx])
		if accNum == "" {
			skippedNoAccount++
			continue
		}

		// Lookup from pre-loaded cache
		accData, exists := accountCache[accNum]
		if !exists {
			// Account not in database
			fmt.Printf("[PREVIEW-MULTI] Unknown account: %s (not found in pre-loaded accounts)\n", accNum)
			skippedUnknownAccount++
			continue
		}

		// Parse transaction fields
		txn := map[string]interface{}{
			"account_number":     accNum,
			"entity_id":          accData.entityID,
			"entity_name":        accData.entityID,
			"bank_name":          accData.bankName,
			"currency":           accData.currency,
			"misclassified_flag": false,
		}

		// Transaction ID
		if tranIDIdx >= 0 && tranIDIdx < len(row) {
			txn["tran_id"] = strings.TrimSpace(row[tranIDIdx])
		}

		// Parse dates
		var transactionDate, valueDate time.Time
		if dateIdx >= 0 && dateIdx < len(row) {
			if dt, err := parseDate(row[dateIdx]); err == nil {
				transactionDate = dt
			}
		}
		if valDateIdx >= 0 && valDateIdx < len(row) {
			if dt, err := parseDate(row[valDateIdx]); err == nil {
				valueDate = dt
			}
		}

		// Mirror dates if one is missing
		if transactionDate.IsZero() && !valueDate.IsZero() {
			transactionDate = valueDate
		}
		if valueDate.IsZero() && !transactionDate.IsZero() {
			valueDate = transactionDate
		}

		// Skip if no valid date
		if transactionDate.IsZero() {
			skippedNoDate++
			continue
		}

		txn["transaction_date"] = transactionDate.Format(time.RFC3339)
		txn["value_date"] = valueDate.Format(time.RFC3339)

		// Description
		description := ""
		if descIdx >= 0 && descIdx < len(row) {
			description = sanitizeForPostgres(normalizeCell(row[descIdx]))
		}
		txn["description"] = description

		// Amounts
		var withdrawal, deposit sql.NullFloat64

		if debitIdx >= 0 && debitIdx < len(row) {
			if val, err := parseAmount(row[debitIdx]); err == nil && val > 0 {
				withdrawal = sql.NullFloat64{Float64: val, Valid: true}
				txn["withdrawal_amount"] = val
			} else {
				txn["withdrawal_amount"] = 0
			}
		} else {
			txn["withdrawal_amount"] = 0
		}

		if creditIdx >= 0 && creditIdx < len(row) {
			if val, err := parseAmount(row[creditIdx]); err == nil && val > 0 {
				deposit = sql.NullFloat64{Float64: val, Valid: true}
				txn["deposit_amount"] = val
			} else {
				txn["deposit_amount"] = 0
			}
		} else {
			txn["deposit_amount"] = 0
		}

		if balanceIdx >= 0 && balanceIdx < len(row) {
			if val, err := parseAmount(row[balanceIdx]); err == nil {
				txn["balance"] = val
			}
		}

		// Apply categorization
		categoryID := matchCategoryForTransaction(accData.rules, description, withdrawal, deposit)
		if categoryID.Valid && categoryID.String != "" {
			txn["category_id"] = categoryID.String
			for _, rule := range accData.rules {
				if rule.CategoryID == categoryID.String {
					txn["category_name"] = rule.CategoryName
					break
				}
			}
		} else {
			txn["category_name"] = "Uncategorized"
		}

		allTransactions = append(allTransactions, txn)
		processed++
	}

	fmt.Printf("[PREVIEW-MULTI] Results - processed:%d skipped(no_account:%d no_date:%d unknown_account:%d) unique_accounts:%d\n",
		processed, skippedNoAccount, skippedNoDate, skippedUnknownAccount, len(accountCache))

	return allTransactions, nil
}

// processPDFPreviewFlat calls external AI parser for PDF/DOCX files
// Returns parsed transactions WITHOUT any database writes
// Uses the EXACT same logic as UploadBankStatementV3Handler
func processPDFPreviewFlat(ctx context.Context, db *sql.DB, fileBytes []byte, filename string) ([]map[string]interface{}, error) {
	// Get external parser URL using the EXACT same logic as upload handler
	v := q8()
	v = attachStreamKey(v)
	if v[0] != 'h' {
		v = z4()
	}

	fmt.Printf("[PREVIEW-PDF] Calling external parser: %s\n", v)

	// Build multipart request (exact same as upload handler)
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	fw, err := mw.CreateFormFile("pdf", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err := fw.Write(fileBytes); err != nil {
		return nil, fmt.Errorf("failed to write file: %w", err)
	}
	if err := mw.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	// Call external parser with NO timeout (same as upload)
	req, err := http.NewRequestWithContext(ctx, "POST", v, &b)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set(constants.ContentTypeText, mw.FormDataContentType())

	client := &http.Client{Timeout: 0} // NO timeout, same as upload
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call external parser: %w", err)
	}
	defer resp.Body.Close()

	// Read AI response
	aiResponseBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read parser response: %w", err)
	}

	// Parse AI response
	var aiResponse map[string]interface{}
	if err := json.Unmarshal(aiResponseBytes, &aiResponse); err != nil {
		return nil, fmt.Errorf("failed to parse AI response: %w", err)
	}

	fmt.Printf("[PREVIEW-PDF] AI response keys: %v\n", getMapKeys(aiResponse))

	// Check for error response from AI parser
	if status, ok := aiResponse["status"].(string); ok && status == "error" {
		errorMsg := "Unknown error from PDF parser"
		if errVal, ok := aiResponse["error"].(string); ok {
			errorMsg = errVal
		}
		return nil, fmt.Errorf("PDF parser error: %s", errorMsg)
	}

	// Extract transactions from AI response
	transactions := []map[string]interface{}{}

	// Check if AI response has transactions array
	if txnData, ok := aiResponse["transactions"].([]interface{}); ok {
		accountNumber := ""
		if acc, ok := aiResponse["account_number"].(string); ok {
			accountNumber = strings.TrimSpace(acc)
		}

		entityID := ""
		bankName := ""
		currency := "INR"

		// DB READ: Lookup account metadata if we have account number
		if accountNumber != "" {
			err := db.QueryRowContext(ctx, `
				SELECT mba.entity_id, COALESCE(mb.bank_name, ''), COALESCE(mba.currency, 'INR')
				FROM public.masterbankaccount mba
				LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
				WHERE mba.account_number = $1 AND COALESCE(mba.is_deleted, false) = false
			`, accountNumber).Scan(&entityID, &bankName, &currency)

			if err != nil && err != sql.ErrNoRows {
				return nil, fmt.Errorf("failed to lookup account: %w", err)
			}

			// DB READ: Load category rules if account found
			var rules []categoryRuleComponent
			if entityID != "" {
				rules, _ = loadCategoryRuleComponents(ctx, db, accountNumber, entityID, currency)
			}

			// Process each transaction from AI
			for _, txnItem := range txnData {
				txnMap, ok := txnItem.(map[string]interface{})
				if !ok {
					continue
				}

				// Build transaction object
				txn := map[string]interface{}{
					"account_number":     accountNumber,
					"entity_id":          entityID,
					"entity_name":        entityID,
					"bank_name":          bankName,
					"currency":           currency,
					"misclassified_flag": false,
				}

				// Copy fields from AI response
				if val, ok := txnMap["tran_id"]; ok {
					txn["tran_id"] = val
				}
				if val, ok := txnMap["transaction_date"]; ok {
					txn["transaction_date"] = val
				}
				if val, ok := txnMap["value_date"]; ok {
					txn["value_date"] = val
				}
				if val, ok := txnMap["withdrawal"]; ok {
					txn["withdrawal_amount"] = val
				} else {
					txn["withdrawal_amount"] = 0
				}
				if val, ok := txnMap["deposit"]; ok {
					txn["deposit_amount"] = val
				} else {
					txn["deposit_amount"] = 0
				}
				if val, ok := txnMap["balance"]; ok {
					txn["balance"] = val
				}

				// Description and categorization
				if val, ok := txnMap["description"]; ok {
					if desc, ok := val.(string); ok {
						txn["description"] = desc

						// Apply categorization if we have rules
						if len(rules) > 0 {
							var withdrawal, deposit sql.NullFloat64
							if w, ok := txnMap["withdrawal"].(float64); ok && w > 0 {
								withdrawal = sql.NullFloat64{Float64: w, Valid: true}
							}
							if d, ok := txnMap["deposit"].(float64); ok && d > 0 {
								deposit = sql.NullFloat64{Float64: d, Valid: true}
							}

							categoryID := matchCategoryForTransaction(rules, desc, withdrawal, deposit)
							if categoryID.Valid && categoryID.String != "" {
								txn["category_id"] = categoryID.String
								for _, rule := range rules {
									if rule.CategoryID == categoryID.String {
										txn["category_name"] = rule.CategoryName
										break
									}
								}
							} else {
								txn["category_name"] = "Uncategorized"
							}
						} else {
							txn["category_name"] = "Uncategorized"
						}
					}
				} else {
					txn["description"] = ""
					txn["category_name"] = "Uncategorized"
				}

				transactions = append(transactions, txn)
			}
		} else {
			// No account number - return AI data as-is with "Uncategorized"
			for _, txnItem := range txnData {
				txnMap, ok := txnItem.(map[string]interface{})
				if !ok {
					continue
				}
				txnMap["category_name"] = "Uncategorized"

				// Ensure amount fields exist
				if _, ok := txnMap["withdrawal_amount"]; !ok {
					if w, ok := txnMap["withdrawal"]; ok {
						txnMap["withdrawal_amount"] = w
					} else {
						txnMap["withdrawal_amount"] = 0
					}
				}
				if _, ok := txnMap["deposit_amount"]; !ok {
					if d, ok := txnMap["deposit"]; ok {
						txnMap["deposit_amount"] = d
					} else {
						txnMap["deposit_amount"] = 0
					}
				}

				transactions = append(transactions, txnMap)
			}
		}
	}

	fmt.Printf("[PREVIEW-PDF] Extracted %d transactions\n", len(transactions))
	return transactions, nil
}

func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Note: Helper functions z4(), q8(), and attachStreamKey() are defined in stream_handlers.go
// They are already available in this package, so we don't redefine them here
