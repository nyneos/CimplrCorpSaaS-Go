package bankstatement

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/bindref"
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shakinm/xlsReader/xls"
	"github.com/xuri/excelize/v2"
)

// PreviewBankStatementHandler parses uploaded file(s), categorizes transactions, returns FLAT transaction list WITHOUT DB insertion
// Uses EXACT same parsing logic as UploadBankStatementV2WithCategorization but NO database writes
// Supports: XLSX, XLS, CSV, multi-account CSV (multi=true), PDF/DOCX (external AI), ZIP (containing any of above)
func PreviewBankStatementHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
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

		extEarly := strings.ToLower(filepath.Ext(header.Filename))

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

		forceOverride := r.FormValue("force_override") == "true"
		accountNums := parseAccountNumbers(r.MultipartForm.Value)
		var accountOverride string
		if forceOverride {
			if len(accountNums) == 0 {
				http.Error(w, "force_override=true requires at least one account number in account_numbers", http.StatusBadRequest)
				return
			}
			if len(accountNums) != 1 {
				http.Error(w, "force_override=true with a single file requires exactly 1 account number", http.StatusBadRequest)
				return
			}
			accountOverride = accountNums[0]
		}

		// Read file into memory (zip uploads capped at MaxBankStatementZipBytes)
		var fileBytes []byte
		if extEarly == ".zip" {
			fileBytes, err = readBankStatementZipBytes(file, header)
			if err != nil {
				if strings.Contains(err.Error(), "exceeds the maximum size") {
					http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
					return
				}
				http.Error(w, constants.ErrFailedToReadFile+err.Error(), http.StatusInternalServerError)
				return
			}
		} else {
			fileBytes, err = io.ReadAll(file)
			if err != nil {
				http.Error(w, constants.ErrFailedToReadFile+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		ext := extEarly

		var allTransactions []map[string]interface{}

		// Route based on file type
		if ext == ".zip" {
			// ZIP with multiple files
			allTransactions, err = processZipPreviewFlat(ctx, pool, fileBytes, useMapping, mappings)
			if err != nil {
				http.Error(w, "ZIP processing failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
		} else if ext == ".pdf" || ext == ".docx" {
			// PDF/DOCX - call external AI parser (NO DB writes)
			allTransactions, err = processPDFPreviewFlat(ctx, pool, fileBytes, header.Filename)
			if err != nil {
				http.Error(w, "PDF/DOCX processing failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
		} else if ext == ".csv" && isMultiAccount {
			// Multi-account CSV
			allTransactions, err = processMultiAccountCSVPreviewFlat(ctx, pool, fileBytes)
			if err != nil {
				http.Error(w, "Multi-account CSV processing failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
		} else {
			// Single file processing (XLSX, XLS, CSV)
			transactions, err := processSingleFilePreviewFlat(ctx, pool, fileBytes, header.Filename, useMapping, mappings, accountOverride)
			if err != nil {
				http.Error(w, "File processing failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			allTransactions = transactions
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"transactions": allTransactions,
			"count":        len(allTransactions),
		})
	})
}

// processZipPreviewFlat extracts all files from ZIP and returns flat transaction list
func processZipPreviewFlat(ctx context.Context, pool *pgxpool.Pool, zipBytes []byte, useMapping bool, mappings *ColumnMappings) ([]map[string]interface{}, error) {
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

		transactions, err := processSingleFilePreviewFlat(ctx, pool, fileBytes, f.Name, useMapping, mappings, "")
		if err != nil {
			// Skip files with errors, continue processing others
			continue
		}

		allTransactions = append(allTransactions, transactions...)
	}

	return allTransactions, nil
}

// resolveMasterBankAccountForPreview mirrors upload V2 candidate expansion: dash/space strip,
// leading-zero strip, and filename digit runs — so preview matches master rows stored as
// "505002795" when the statement shows "000505002795".
func resolveMasterBankAccountForPreview(ctx context.Context, pool *pgxpool.Pool, primary string, uploadFileName string, rows [][]string) (matchedAccount string, entityID string, bankName string, currency string, err error) {
	primary = strings.TrimSpace(primary)
	if primary == "" {
		return "", "", "", "", fmt.Errorf("account number could not be found anywhere in the file name or file contents")
	}

	candidates := []string{}
	seen := map[string]bool{}

	addCand := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" {
			return
		}
		if !seen[s] {
			seen[s] = true
			candidates = append(candidates, s)
		}
		stripped := strings.ReplaceAll(strings.ReplaceAll(s, " ", ""), "-", "")
		if stripped != s && stripped != "" && !seen[stripped] {
			seen[stripped] = true
			candidates = append(candidates, stripped)
		}
		noLeadZero := strings.TrimLeft(stripped, "0")
		if noLeadZero != stripped && noLeadZero != "" && !seen[noLeadZero] {
			seen[noLeadZero] = true
			candidates = append(candidates, noLeadZero)
		}
	}

	addCand(primary)

	// Filename digit runs (same idea as bankstatUplV2)
	filenameDigitRe := regexp.MustCompile(`\d{7,}`)
	baseName := filepath.Base(uploadFileName)
	dashAcctSegRe := regexp.MustCompile(`^\d[\d\-]{5,}\d$`)
	for _, part := range regexp.MustCompile(`[_\s\.]+`).Split(baseName, -1) {
		part = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(part, ".xls"), ".xlsx"))
		if filenameDigitRe.MatchString(part) || (len(part) >= 7 && dashAcctSegRe.MatchString(part)) {
			addCand(part)
		}
	}
	for _, part := range regexp.MustCompile(`[_\-\s\.]+`).Split(baseName, -1) {
		part = strings.TrimSpace(part)
		if filenameDigitRe.MatchString(part) {
			addCand(part)
		}
	}
	for _, m := range filenameDigitRe.FindAllString(baseName, -1) {
		addCand(m)
	}

	// Header-area digit runs (first 20 rows)
	acctNumberRe := regexp.MustCompile(`\d{7,}`)
	for i := 0; i < 20 && i < len(rows); i++ {
		for _, cell := range rows[i] {
			v := normalizeCell(cell)
			for _, m := range acctNumberRe.FindAllString(v, -1) {
				addCand(strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", ""))
			}
		}
	}

	for _, cand := range candidates {
		qErr := pool.QueryRow(ctx, `
			SELECT mba.account_number, mba.entity_id, COALESCE(mb.bank_name, ''), COALESCE(mba.currency, 'INR')
			FROM public.masterbankaccount mba
			LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
			WHERE mba.account_number = $1 AND COALESCE(mba.is_deleted, false) = false
		`, cand).Scan(&matchedAccount, &entityID, &bankName, &currency)
		if qErr == nil {
			return matchedAccount, entityID, bankName, currency, nil
		}
		if !(errors.Is(qErr, pgx.ErrNoRows) || errors.Is(qErr, sql.ErrNoRows)) {
			return "", "", "", "", fmt.Errorf("database lookup failed: %w", qErr)
		}
	}

	return "", "", "", "", fmt.Errorf("account %s not found in master data", primary)
}

// resolveAccountForPreviewWithLLMFallback tries master lookup; when it fails and LLM is
// enabled, asks the model for the account number and retries once (ZIP/single preview path).
func resolveAccountForPreviewWithLLMFallback(ctx context.Context, pool *pgxpool.Pool, accountNumber, filename string, rows [][]string, accountOverride string) (matchedAccount, entityID, bankName, currency string, err error) {
	matchedAccount, entityID, bankName, currency, err = resolveMasterBankAccountForPreview(ctx, pool, accountNumber, filename, rows)
	if err == nil {
		return matchedAccount, entityID, bankName, currency, nil
	}
	if accountOverride != "" || !bindref.BrOn() {
		return "", "", "", "", err
	}
	info, llmErr := extractAccountInfoWithLLM(ctx, rows)
	if llmErr != nil {
		logger.LogInfo("[PREVIEW-DEBUG] LLM account retry after master miss skipped: %v", llmErr)
		return "", "", "", "", err
	}
	if info.AccountNumber == "" || info.AccountNumber == accountNumber {
		return "", "", "", "", err
	}
	logger.LogInfo("[LLM-ACCT] preview retry after master miss manual=%q llm=%q", accountNumber, info.AccountNumber)
	return resolveMasterBankAccountForPreview(ctx, pool, info.AccountNumber, filename, rows)
}

// processSingleFilePreviewFlat parses file and categorizes WITHOUT any DB writes
// Uses EXACT same parsing logic as UploadBankStatementV2WithCategorization but ONLY reads DB for account lookup and rules.
// When accountOverride is non-empty (force_override + single account from the client), it is used for master lookup instead of relying only on the file header.
func processSingleFilePreviewFlat(ctx context.Context, pool *pgxpool.Pool, fileBytes []byte, filename string, useMapping bool, mappings *ColumnMappings, accountOverride string) ([]map[string]interface{}, error) {

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
					nc := normalizeCell(cell)
					isAcctLabel := cell == acNoHeader ||
						strings.EqualFold(nc, "Account Number") ||
						strings.EqualFold(nc, "Account No.") ||
						strings.EqualFold(nc, "Account No") ||
						strings.EqualFold(nc, "Acc No") ||
						strings.EqualFold(nc, "Acc No.") ||
						strings.EqualFold(nc, "A/C Number") ||
						strings.EqualFold(nc, "A/C No") ||
						strings.EqualFold(nc, "A/C No.") ||
						strings.EqualFold(nc, "Account:") ||
						strings.EqualFold(nc, "Account #") ||
						strings.EqualFold(nc, "Acct Number") ||
						strings.EqualFold(nc, "Acct No") ||
						strings.EqualFold(nc, "Acct No.")
					if isAcctLabel {
						if j+1 < len(rows[i]) {
							if v := extractAccountFromCell(rows[i][j+1]); v != "" {
								accountNumber = v
							}
						}
						if accountNumber == "" && i+1 < len(rows) && j < len(rows[i+1]) {
							if v := extractAccountFromCell(rows[i+1][j]); v != "" {
								accountNumber = v
							}
						}
					}
				}
			}
			// ICICI-style PDF→CSV: summary row has "Account Number" next to another column header
			// ("Balance (INR )") — only accept digit-derived account numbers from that path.
			if accountNumber == "" {
				stmtAcctRe := regexp.MustCompile(`(?i)account\s*number\s*[:#]?\s*([0-9]{6,})`)
				for i := 0; i < 30 && i < len(rows); i++ {
					for _, cell := range rows[i] {
						v := normalizeCell(cell)
						if m := stmtAcctRe.FindStringSubmatch(v); len(m) > 1 {
							accountNumber = strings.ReplaceAll(strings.ReplaceAll(m[1], " ", ""), "-", "")
							break
						}
					}
					if accountNumber != "" {
						break
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

	accountOverride = strings.TrimSpace(accountOverride)
	if accountOverride != "" {
		accountNumber = strings.ReplaceAll(strings.ReplaceAll(accountOverride, " ", ""), "-", "")
	}

	// LLM account extraction when manual strategies found nothing.
	if accountNumber == "" && bindref.BrOn() {
		if info, llmErr := extractAccountInfoWithLLM(ctx, rows); llmErr == nil && info.AccountNumber != "" {
			accountNumber = info.AccountNumber
			logger.LogInfo("[LLM-ACCT] preview extracted accountNumber=%q accountName=%q", accountNumber, info.AccountName)
		} else if llmErr != nil {
			logger.LogInfo("[PREVIEW-DEBUG] LLM account extraction skipped or failed: %v", llmErr)
		}
	}

	if accountNumber == "" {
		return nil, fmt.Errorf("account number could not be found anywhere in the file name or file contents")
	}

	// DB READ ONLY: Lookup account metadata (same candidate expansion as upload V2).
	// If manual account is wrong (not in master), retry with LLM before failing.
	matchedAcct, entityID, bankName, currency, err := resolveAccountForPreviewWithLLMFallback(ctx, pool, accountNumber, filename, rows, accountOverride)
	if err != nil {
		// Log error but DO NOT fail if this is a staging/preview operation
		// This saves conversion credits for PDFs by allowing them to stage even if unmapped.
		logger.LogInfo("[PREVIEW-DEBUG] Account not found in master data: %v. Proceeding with staging unmapped.", err)
		matchedAcct = accountNumber // Keep the raw parsed number
		entityID = ""
		bankName = ""
		currency = ""
	}
	accountNumber = matchedAcct

	// DB READ ONLY: Load category rules
	rules, err := loadCategoryRuleComponentsPgx(ctx, pool, accountNumber, entityID, currency)
	if err != nil {
		return nil, fmt.Errorf("failed to load category rules: %w", err)
	}

	// LLM-first extraction: the model reads the converted CSV semantically (a running total is a
	// balance, a debit lowers it), which is far more robust to the converter scrambling columns or
	// splitting one transaction across rows than position-based parsing. The result is validated
	// against the running-balance identity; on low confidence (or when AI is disabled / fails) we
	// fall through to the deterministic parser below.
	if bindref.BrOn() {
		if resp, lerr := extractTransactionsWithLLM(ctx, rows); lerr != nil {
			logger.LogInfo("[LLM-TXN] extraction unavailable (%v) — using deterministic parser", lerr)
		} else if maps, ok := buildTxnMapsFromLLM(resp, llmTxnContext{
			accountNumber: accountNumber,
			entityID:      entityID,
			bankName:      bankName,
			currency:      currency,
			rules:         rules,
			batchID:       generateBatchID(),
			sourceRows:    rows,
		}); ok {
			logger.LogInfo("[LLM-TXN] using LLM-extracted transactions as primary path (count=%d)", len(maps))
			return maps, nil
		}
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

				// Create a combined row using current row and next row (if exists)
				// to handle split headers commonly found in bank statements (e.g. BOB)
				combinedRow := make([]string, len(row))
				copy(combinedRow, row)
				if i+1 < len(rows) {
					for j := 0; j < len(combinedRow) && j < len(rows[i+1]); j++ {
						if strings.TrimSpace(rows[i+1][j]) != "" {
							combinedRow[j] = strings.TrimSpace(combinedRow[j]) + " " + strings.TrimSpace(rows[i+1][j])
						}
					}
				}

				for _, cell := range combinedRow {
					lc := strings.ToLower(strings.TrimSpace(cell))
					if strings.Contains(lc, "date") {
						hasDate = true
					}
					if strings.Contains(lc, "description") || strings.Contains(lc, "remarks") || strings.Contains(lc, "narration") || strings.Contains(lc, "particulars") {
						hasDesc = true
					}
					if strings.Contains(lc, "withdrawal") || strings.Contains(lc, "deposit") || strings.Contains(lc, "debit") || strings.Contains(lc, "credit") || strings.Contains(lc, "amount") || strings.Contains(lc, "amount subtracted") || strings.Contains(lc, "amount added") || strings.EqualFold(lc, "dr") || strings.EqualFold(lc, "cr") {
						hasAmount = true
					}
				}
				if hasDate && (hasDesc || hasAmount) {
					txnHeaderIdx = i
					rows[i] = combinedRow // use combined row for column mapping
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

	// Fallback for headless PDF-to-CSV exports (e.g. SBI)
	if txnHeaderIdx == -1 {
		for i, row := range rows {
			if len(row) >= 14 && len(row[0]) >= 8 && len(row[2]) >= 8 {
				if strings.ContainsAny(row[0], "/-.\u2013\u2014\u2212") && strings.ContainsAny(row[2], "/-.\u2013\u2014\u2212") {
					// Likely a headless transaction row
					syntheticHeader := make([]string, len(row))
					syntheticHeader[0] = "Date"
					syntheticHeader[2] = "Value Date"
					syntheticHeader[4] = "Description"
					syntheticHeader[9] = "Withdrawal"
					syntheticHeader[12] = "Deposit"
					syntheticHeader[13] = "Balance"

					newRows := make([][]string, 0, len(rows)+1)
					newRows = append(newRows, rows[:i]...)
					newRows = append(newRows, syntheticHeader)
					newRows = append(newRows, rows[i:]...)
					rows = newRows
					txnHeaderIdx = i

					// Fix shifted columns in the data rows for this headless table
					for r := txnHeaderIdx + 1; r < len(rows); r++ {
						if len(rows[r]) >= 14 {
							val12 := strings.TrimSpace(rows[r][12])
							if val12 == "" || val12 == "-" {
								val11 := strings.TrimSpace(rows[r][11])
								if val11 != "" && val11 != "-" {
									rows[r][12] = rows[r][11]
									rows[r][11] = ""
								}
							}
							val9 := strings.TrimSpace(rows[r][9])
							if val9 == "" || val9 == "-" {
								val8 := strings.TrimSpace(rows[r][8])
								if val8 != "" && val8 != "-" {
									rows[r][9] = rows[r][8]
									rows[r][8] = ""
								} else {
									val10 := strings.TrimSpace(rows[r][10])
									if val10 != "" && val10 != "-" {
										rows[r][9] = rows[r][10]
										rows[r][10] = ""
									}
								}
							}
						}
					}
					break
				}
			}
		}
	}

	// Build column mapping from heuristic if found
	colIdx := make(map[string]int)
	if txnHeaderIdx != -1 {
		headerRow := rows[txnHeaderIdx]
		for idx, col := range headerRow {
			colIdx[strings.TrimSpace(col)] = idx
		}
	}

	// LLM column layout — only when enabled and manual mapping is incomplete (or completely missing).
	txnHeaderIdx, colIdx = applyLLMColumnLayoutIfNeeded(ctx, rows, txnHeaderIdx, colIdx)

	if txnHeaderIdx == -1 {
		return nil, fmt.Errorf("transaction header row not found even after LLM fallback")
	}

	headerRow := rows[txnHeaderIdx]
	logger.LogInfo("[PREVIEW-DEBUG] txnHeaderIdx=%d total_rows=%d data_rows_to_parse=%d", txnHeaderIdx, len(rows), len(rows)-txnHeaderIdx-1)

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
		for idx, colName := range headerRow {
			lcName := strings.ToLower(colName)
			for _, kw := range keywords {
				if strings.Contains(lcName, strings.ToLower(kw)) {
					return idx
				}
			}
		}
		return -1
	}

	// Use broad alias lists for auto-detection (same coverage as the upload handler)
	findColByAliases := func(aliases []string) int {
		for idx, colName := range headerRow {
			if headerContainsAny(strings.ToLower(colName), aliases) {
				return idx
			}
		}
		return -1
	}

	if _, ok := colIdx[constants.TransactionDateAlt]; !ok {
		if idx := findColByAliases(bankStmtDateHeaderAliases); idx >= 0 {
			colIdx[constants.TransactionDateAlt] = idx
		}
	}
	if _, ok := colIdx[constants.ValueDateAlt]; !ok {
		if idx := findColContaining("value date", "val date"); idx >= 0 {
			colIdx[constants.ValueDateAlt] = idx
		}
	}
	if _, ok := colIdx["Description"]; !ok {
		if idx := findColByAliases(bankStmtDescriptionHeaderAliases); idx >= 0 {
			colIdx["Description"] = idx
		}
	}
	if _, ok := colIdx[constants.TransactionRemarks]; !ok {
		if idx := findColByAliases(bankStmtDescriptionHeaderAliases); idx >= 0 {
			colIdx[constants.TransactionRemarks] = idx
		}
	}
	if _, ok := colIdx[constants.WithdrawalAmountINR]; !ok {
		if idx := findColByAliases(bankStmtDebitHeaderAliases); idx >= 0 {
			colIdx[constants.WithdrawalAmountINR] = idx
		}
	}
	if _, ok := colIdx[constants.DepositAmountINR]; !ok {
		if idx := findColByAliases(bankStmtCreditHeaderAliases); idx >= 0 {
			colIdx[constants.DepositAmountINR] = idx
		}
	}
	if _, ok := colIdx[constants.BalanceINR]; !ok {
		if idx := findColByAliases(bankStmtBalanceHeaderAliases); idx >= 0 {
			colIdx[constants.BalanceINR] = idx
		}
	}
	// Citi GXLSM native XLS: DATE | DESCRIPTION | DEBIT AMT | CREDIT AMT | TIME STAMP
	for idx, col := range headerRow {
		lc := strings.ToLower(strings.TrimSpace(col))
		switch {
		case lc == "date":
			colIdx[constants.TransactionDateAlt] = idx
			colIdx["Date"] = idx
		case strings.Contains(lc, "description"):
			colIdx["Description"] = idx
			colIdx[constants.TransactionRemarks] = idx
		case strings.Contains(lc, "debit"):
			colIdx[constants.WithdrawalAmountINR] = idx
		case strings.Contains(lc, "credit"):
			colIdx[constants.DepositAmountINR] = idx
		case strings.Contains(lc, "time stamp") || lc == "timestamp":
			colIdx["TimeStamp"] = idx
		}
	}
	if _, ok := colIdx[constants.TranID]; !ok {
		if idx := findColByAliases(bankStmtReferenceHeaderAliases); idx >= 0 {
			colIdx[constants.TranID] = idx
		}
	}
	// CrDr indicator column — used when statement has a single Amount column
	if _, ok := colIdx["CrDr"]; !ok {
		for idx, colName := range headerRow {
			lc := strings.ToLower(strings.TrimSpace(colName))
			if strings.Contains(lc, "cr/dr") || strings.Contains(lc, "crdr") ||
				strings.Contains(lc, "dr/cr") || lc == "cr" || lc == "dr" ||
				strings.Contains(lc, "credit/debit") || strings.Contains(lc, "debit/credit") {
				colIdx["CrDr"] = idx
				break
			}
		}
	}
	// Parse transactions
	transactions := []map[string]interface{}{}
	previewRowNum := txnHeaderIdx
	// Unique 6-char prefix shared across all synthetic tran_ids in this preview batch
	previewBatchID := generateBatchID()

	// Determine column indices for merging multi-line descriptions
	mergeDateIdx := -1
	if idx, ok := colIdx["Date"]; ok {
		mergeDateIdx = idx
	} else if idx, ok := colIdx[constants.TransactionDateAlt]; ok {
		mergeDateIdx = idx
	} else if idx, ok := colIdx[constants.ValueDateAlt]; ok {
		mergeDateIdx = idx
	}

	mergeDescIdx := -1
	if idx, ok := colIdx[constants.TransactionRemarks]; ok {
		mergeDescIdx = idx
	} else if idx, ok := colIdx["Description"]; ok {
		mergeDescIdx = idx
	}

	dataRows := rows[txnHeaderIdx+1:]
	dataRows = MergeMultiLineDescriptions(dataRows, mergeDateIdx, mergeDescIdx)

	// Detect newest-first ordering (e.g. PNB) and reverse so processing is always oldest-first.
	if mergeDateIdx >= 0 {
		var first, last time.Time
		for _, r := range dataRows {
			if mergeDateIdx < len(r) {
				if t, err := parseDate(strings.TrimSpace(r[mergeDateIdx])); err == nil && !t.IsZero() {
					first = t
					break
				}
			}
		}
		for i := len(dataRows) - 1; i >= 0; i-- {
			r := dataRows[i]
			if mergeDateIdx < len(r) {
				if t, err := parseDate(strings.TrimSpace(r[mergeDateIdx])); err == nil && !t.IsZero() {
					last = t
					break
				}
			}
		}
		if !first.IsZero() && !last.IsZero() && first.After(last) {
			logger.LogInfo("[PREVIEW] detected reverse row order (first=%s last=%s) — reversing", first.Format("2006-01-02"), last.Format("2006-01-02"))
			for i, j := 0, len(dataRows)-1; i < j; i, j = i+1, j-1 {
				dataRows[i], dataRows[j] = dataRows[j], dataRows[i]
			}
		}
	}

	var prevStmtBalance float64
	var prevStmtBalanceOK bool
	var openingBalance float64
	var openingBalanceKnown bool
	var cumulative float64
	var firstValidRow = true

	if ob := scanOpeningBalanceFromRows(rows[:txnHeaderIdx+1]); ob != nil {
		openingBalance = *ob
		openingBalanceKnown = true
		cumulative = *ob
		logger.LogInfo("[PREVIEW] header opening balance=%.2f", openingBalance)
	}

	for _, row := range dataRows {
		previewRowNum++

		if isEmptyRow(row) {
			continue
		}

		// Defensive: fix collapsed CSV columns where empty cells were omitted (common in PDF-to-CSV)
		if len(row) < len(headerRow) && len(row) > 3 {
			lastCell := strings.TrimSpace(row[len(row)-1])
			lcLast := strings.ToLower(lastCell)
			if strings.HasSuffix(lcLast, "cr") || strings.HasSuffix(lcLast, "dr") || strings.HasSuffix(lcLast, "cr.") || strings.HasSuffix(lcLast, "dr.") {
				balIdx := -1
				if idx, ok := colIdx[constants.BalanceINR]; ok {
					balIdx = idx
				} else if idx, ok := colIdx["Balance"]; ok {
					balIdx = idx
				}
				if balIdx == len(headerRow)-1 {
					oldLen := len(row)
					// Pad the row
					for len(row) < len(headerRow) {
						row = append(row, "")
					}
					// Shift last cell to balance column
					row[balIdx] = lastCell
					// Empty the old position
					if oldLen-1 != balIdx {
						row[oldLen-1] = ""
					}
				}
			}
		}

		// Pad row before any column access
		for len(row) < len(headerRow) {
			row = append(row, "")
		}

		// Skip separator lines (UBI: "----...----")
		if IsSeparatorRow(row) {
			continue
		}
		if IsPageBreakRow(row) {
			continue
		}

		// Skip non-transaction rows — check first cell AND the description column.
		// Opening carry lines (B/F, balance brought forward, etc.) establish opening balance only;
		// they are skipped from the preview list (same as V2 ingestion).
		candidateCells := []string{strings.TrimSpace(row[0])}
		if descIdx, ok := colIdx["Description"]; ok && descIdx < len(row) {
			candidateCells = append(candidateCells, strings.TrimSpace(row[descIdx]))
		}
		if descIdx, ok := colIdx[constants.TransactionRemarks]; ok && descIdx < len(row) {
			candidateCells = append(candidateCells, strings.TrimSpace(row[descIdx]))
		}
		candidateCells = append(candidateCells, strings.TrimSpace(strings.Join(row, " ")))
		if IsNonTransactionRow(candidateCells...) {
			// An "Opening Balance" summary line is a non-transaction row, but it still carries the
			// statement's opening balance in its Balance column (e.g. IDFC: "Opening Balance ... 17.91 CR").
			// Seed it before skipping — otherwise the ledger starts from 0, every running balance is off
			// by the opening amount, and the derived opening/closing balances come out as 0.
			if !openingBalanceKnown {
				joined := strings.ToLower(strings.Join(candidateCells, " "))
				if strings.Contains(joined, "opening balance") || strings.Contains(joined, "opening bal") {
					balIdx := -1
					if idx, ok := colIdx[constants.BalanceINR]; ok {
						balIdx = idx
					}
					if v, ok := summaryRowBalance(row, balIdx); ok {
						openingBalance = v
						openingBalanceKnown = true
						cumulative = v
						logger.LogInfo("[PREVIEW] opening balance summary row: opening_balance=%.2f", v)
					}
				}
			}
			continue
		}

		alignment := scoreTransactionRowAlignment(row, headerRow, colIdx, prevStmtBalance, prevStmtBalanceOK)
		if alignment.shifted() {
			logger.LogInfo("[col-shift] row=%d len=%d headerLen=%d offset=%d start_col=%d score=%d",
				previewRowNum, len(row), len(headerRow), alignment.offset, alignment.start, alignment.score)
		}

		// rowAt reads a logical column index with the scored per-row alignment applied.
		rowAt := func(idx int) string {
			return alignment.at(row, idx)
		}

		txn := map[string]interface{}{
			"account_number":     accountNumber,
			"entity_id":          entityID,
			"entity_name":        entityID,
			"bank_name":          bankName,
			"currency":           currency,
			"misclassified_flag": false,
		}

		// Extract tran_id — prefer explicit column; fall back to batchID+seq synthetic ID
		tranIDStr := ""
		if idx, ok := colIdx[constants.TranID]; ok && idx < len(row) {
			tranIDStr = strings.TrimSpace(rowAt(idx))
		}

		// Parse dates
		var transactionDate, valueDate time.Time
		if idx, ok := colIdx[constants.TransactionDateAlt]; ok && idx < len(row) {
			if dt, err := parseDate(rowAt(idx)); err == nil {
				transactionDate = dt
			}
		}
		if idx, ok := colIdx[constants.ValueDateAlt]; ok && idx < len(row) {
			if dt, err := parseDate(rowAt(idx)); err == nil {
				valueDate = dt
			}
		}
		if idx, ok := colIdx["Date"]; ok && idx < len(row) && transactionDate.IsZero() && valueDate.IsZero() {
			if dt, err := parseDate(rowAt(idx)); err == nil {
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
			description = sanitizeForPostgres(normalizeCell(rowAt(idx)))
		} else if idx, ok := colIdx["Description"]; ok && idx < len(row) {
			description = sanitizeForPostgres(normalizeCell(rowAt(idx)))
		}
		txn["description"] = description

		// Resolve tran_id: file column → cheque/ref → date+timestamp+seq synthetic ID
		if tranIDStr == "" {
			// Cheque/ref column fallback
			for _, hdr := range []string{"Cheque", "Chq", "Reference", "Ref No"} {
				if idx, ok := colIdx[hdr]; ok && idx < len(row) {
					if v := strings.TrimSpace(rowAt(idx)); v != "" {
						tranIDStr = v
						break
					}
				}
			}
		}
		if tranIDStr == "" {
			tranIDStr = buildSyntheticTranID(previewBatchID, previewRowNum)
		}
		txn["tran_id"] = tranIDStr

		// Amounts — handle both separate Withdrawal/Deposit columns and single Amount+CrDr format
		var withdrawal, deposit sql.NullFloat64
		txn["withdrawal_amount"] = 0
		txn["deposit_amount"] = 0

		idxW, okW := colIdx[constants.WithdrawalAmountINR]
		idxD, okD := colIdx[constants.DepositAmountINR]
		idxCrDr, okCrDr := colIdx["CrDr"]

		// Resolve single-amount column index: same col for both, or only one side detected.
		singleAmtIdx := -1
		if okW && okD && idxW == idxD {
			singleAmtIdx = idxW
		} else if okW && !okD {
			singleAmtIdx = idxW
		} else if okD && !okW {
			singleAmtIdx = idxD
		}

		if singleAmtIdx >= 0 && okCrDr && idxCrDr >= 0 {
			// Single amount column with Dr/Cr indicator
			crdr := strings.ToLower(strings.TrimSpace(rowAt(idxCrDr)))
			if val, ok := parseAmountNonZero(rowAt(singleAmtIdx)); ok {
				if strings.HasPrefix(crdr, "cr") || strings.Contains(crdr, "credit") {
					deposit = sql.NullFloat64{Float64: val, Valid: true}
					txn["deposit_amount"] = val
				} else if strings.HasPrefix(crdr, "dr") || strings.Contains(crdr, "debit") {
					withdrawal = sql.NullFloat64{Float64: val, Valid: true}
					txn["withdrawal_amount"] = val
				}
			}
		} else if singleAmtIdx >= 0 {
			// Single amount column, no CrDr indicator — store amount temporarily as withdrawal;
			// direction will be corrected below once the balance column is parsed.
			if val, ok := parseAmountNonZero(rowAt(singleAmtIdx)); ok {
				withdrawal = sql.NullFloat64{Float64: val, Valid: true}
				txn["withdrawal_amount"] = val
			}
		} else {
			rawW, rawD, wOK, dOK := applySeparateDebitCreditColumns(rowAt, idxW, idxD, okW, okD)
			if wOK {
				withdrawal = sql.NullFloat64{Float64: rawW, Valid: true}
				txn["withdrawal_amount"] = rawW
			}
			if dOK {
				deposit = sql.NullFloat64{Float64: rawD, Valid: true}
				txn["deposit_amount"] = rawD
			}
		}

		var curBal float64
		var curBalOK bool
		if idxBal, ok := colIdx[constants.BalanceINR]; ok {
			if val, err := parseAmount(cleanAmount(rowAt(idxBal))); err == nil {
				txn["balance"] = val
				curBal = val
				curBalOK = true
			}
		}

		// Temporary per-row debug log — remove once Canara Bank parsing is confirmed correct
		logger.LogInfo("[ROW-DBG] row=%d len=%d shift=%d start=%d score=%d rawW=%q rawD=%q rawBal=%q w=%.2f d=%.2f bal=%.2f",
			previewRowNum, len(row), alignment.offset, alignment.start, alignment.score,
			func() string {
				if idxW, ok := colIdx[constants.WithdrawalAmountINR]; ok {
					return rowAt(idxW)
				}
				return ""
			}(),
			func() string {
				if idxD, ok := colIdx[constants.DepositAmountINR]; ok {
					return rowAt(idxD)
				}
				return ""
			}(),
			func() string {
				if idxB, ok := colIdx[constants.BalanceINR]; ok {
					return rowAt(idxB)
				}
				return ""
			}(),
			func() float64 { v, _ := txn["withdrawal_amount"].(float64); return v }(),
			func() float64 { v, _ := txn["deposit_amount"].(float64); return v }(),
			curBal,
		)

		// Balance-direction flip: for single-amount columns without a CrDr indicator,
		// use the running balance change to determine whether the transaction is a debit or credit.
		// prevStmtBalance is 0 when not yet set, which is correct for statements starting at 0.
		if singleAmtIdx >= 0 && !okCrDr && curBalOK {
			w, _ := txn["withdrawal_amount"].(float64)
			if w > 0 && curBal > prevStmtBalance+0.005 {
				// Balance went UP — flip to deposit
				txn["deposit_amount"] = w
				txn["withdrawal_amount"] = 0
				deposit = sql.NullFloat64{Float64: w, Valid: true}
				withdrawal = sql.NullFloat64{}
			}
		}

		// Match V2: merged footer can break amount parsing on last sweep row; infer debit from prior balance.
		if prevStmtBalanceOK && curBalOK {
			const sweepEps = 0.01
			lowDesc := strings.ToLower(strings.TrimSpace(description))
			wAmt, _ := txn["withdrawal_amount"].(float64)
			dAmt, _ := txn["deposit_amount"].(float64)
			if wAmt < sweepEps && dAmt < sweepEps && math.Abs(curBal) < sweepEps && prevStmtBalance > sweepEps &&
				(strings.Contains(lowDesc, "transfer to") || strings.Contains(lowDesc, "transfer-out")) {
				txn["withdrawal_amount"] = prevStmtBalance
				withdrawal = sql.NullFloat64{Valid: true, Float64: prevStmtBalance}
			}
		}

		// B/F and opening carry rows seed opening_balance only — skip from transaction list (V2 parity).
		lowDesc := strings.ToLower(strings.TrimSpace(description))
		if IsStatementOpeningCarryRow(description) || strings.Contains(lowDesc, constants.BalanceCarriedForward) {
			if curBalOK {
				openingBalance = curBal
				openingBalanceKnown = true
				cumulative = curBal
			} else if deposit.Valid {
				openingBalance = deposit.Float64
				openingBalanceKnown = true
				cumulative = deposit.Float64
			}
			logger.LogInfo("[PREVIEW] opening carry row: opening_balance=%.2f desc=%q", openingBalance, description)
			continue
		}

		// Running balance — mirror V2 upload when balance column is missing or all zeros.
		origBal := curBal
		origBalOK := curBalOK
		if firstValidRow {
			if origBalOK {
				cumulative = origBal
				if !openingBalanceKnown {
					derived := origBal
					if withdrawal.Valid {
						derived += withdrawal.Float64
					}
					if deposit.Valid {
						derived -= deposit.Float64
					}
					openingBalance = derived
					openingBalanceKnown = true
				}
			} else {
				cumulative = openingBalance
				if deposit.Valid {
					cumulative += deposit.Float64
				}
				if withdrawal.Valid {
					cumulative -= withdrawal.Float64
				}
			}
			firstValidRow = false
		} else if origBalOK {
			cumulative = origBal
		} else {
			if deposit.Valid {
				cumulative += deposit.Float64
			}
			if withdrawal.Valid {
				cumulative -= withdrawal.Float64
			}
		}
		effBal := math.Round(cumulative*100) / 100
		if origBalOK && origBal != 0 {
			effBal = origBal
		}
		txn["balance"] = effBal
		prevStmtBalance = effBal
		prevStmtBalanceOK = true

		// Apply categorization (use value_date when available for effective_date checks)
		categoryID := matchCategoryForTransaction(rules, description, withdrawal, deposit, sql.NullTime{Time: valueDate, Valid: !valueDate.IsZero()})
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

	if len(transactions) > 0 {
		transactions[0]["_parser_opening_balance"] = openingBalance
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

// parseCSVFile parses CSV or TSV (same path as upload via parseDelimitedTextRows).
func parseCSVFile(data []byte) ([][]string, error) {
	rows, err := parseDelimitedTextRows(data)
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("csv must have at least one data row")
	}
	return rows, nil
}

// parseAmountNonZero parses a cell amount and returns its absolute value when non-zero.
// Citi and several other banks store debits as negative numbers in the debit column (V2 upload handles this).
func parseAmountNonZero(s string) (float64, bool) {
	val, err := parseAmount(cleanAmount(s))
	if err != nil || val == 0 {
		return 0, false
	}
	if val < 0 {
		val = -val
	}
	return val, true
}

// applySeparateDebitCreditColumns mirrors V2 logic for split debit/credit amount columns.
func applySeparateDebitCreditColumns(rowAt func(int) string, idxW, idxD int, okW, okD bool) (withdrawal, deposit float64, hasW, hasD bool) {
	if okW {
		withdrawal, hasW = parseAmountNonZero(rowAt(idxW))
	}
	if okD {
		deposit, hasD = parseAmountNonZero(rowAt(idxD))
	}
	if hasW && hasD {
		if withdrawal == 0 && deposit != 0 {
			hasW = false
		} else if deposit == 0 && withdrawal != 0 {
			hasD = false
		} else if withdrawal == 0 && deposit == 0 {
			hasW, hasD = false, false
		}
	}
	return withdrawal, deposit, hasW, hasD
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
	// Strip trailing Cr/Dr indicator (e.g. ICICI: "53,90,593.75 Cr")
	lc := strings.ToLower(s)
	switch {
	case strings.HasSuffix(lc, " cr"):
		s = strings.TrimSpace(s[:len(s)-3])
	case strings.HasSuffix(lc, " dr"):
		s = strings.TrimSpace(s[:len(s)-3])
	case len(s) > 2 && strings.HasSuffix(lc, "cr") && lc[len(lc)-3] >= '0' && lc[len(lc)-3] <= '9':
		s = strings.TrimSpace(s[:len(s)-2])
	case len(s) > 2 && strings.HasSuffix(lc, "dr") && lc[len(lc)-3] >= '0' && lc[len(lc)-3] <= '9':
		s = strings.TrimSpace(s[:len(s)-2])
	}
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
func processMultiAccountCSVPreviewFlat(ctx context.Context, pool *pgxpool.Pool, fileBytes []byte) ([]map[string]interface{}, error) {
	rows, err := parseDelimitedTextRows(fileBytes)
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
	debitIdx := findIdx("debit", "withdrawal", "debit amount", "withdrawal amount", "amount subtracted")
	creditIdx := findIdx("credit", "deposit", "credit amount", "deposit amount", "amount added")
	balanceIdx := findIdx("balance", constants.QuerryAvailableBalance, "closing balance", "current / closing")
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
	accountRows, err := pool.Query(loadCtx, `
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
		rules, err := loadCategoryRuleComponentsPgx(loadCtx, pool, accNum, entityID, currency)
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
			if val, ok := parseAmountNonZero(row[debitIdx]); ok {
				withdrawal = sql.NullFloat64{Float64: val, Valid: true}
				txn["withdrawal_amount"] = val
			} else {
				txn["withdrawal_amount"] = 0
			}
		} else {
			txn["withdrawal_amount"] = 0
		}

		if creditIdx >= 0 && creditIdx < len(row) {
			if val, ok := parseAmountNonZero(row[creditIdx]); ok {
				deposit = sql.NullFloat64{Float64: val, Valid: true}
				txn["deposit_amount"] = val
			} else {
				txn["deposit_amount"] = 0
			}
		} else {
			txn["deposit_amount"] = 0
		}

		if balanceIdx >= 0 && balanceIdx < len(row) {
			if val, err := parseAmount(cleanAmount(row[balanceIdx])); err == nil {
				txn["balance"] = val
			}
		}

		// Apply categorization (use value_date when available for effective_date checks)
		categoryID := matchCategoryForTransaction(accData.rules, description, withdrawal, deposit, sql.NullTime{Time: valueDate, Valid: !valueDate.IsZero()})
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
func processPDFPreviewFlat(ctx context.Context, pool *pgxpool.Pool, fileBytes []byte, filename string) ([]map[string]interface{}, error) {
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
			err := pool.QueryRow(ctx, `
				SELECT mba.entity_id, COALESCE(mb.bank_name, ''), COALESCE(mba.currency, 'INR')
				FROM public.masterbankaccount mba
				LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
				WHERE mba.account_number = $1 AND COALESCE(mba.is_deleted, false) = false
			`, accountNumber).Scan(&entityID, &bankName, &currency)

			if err != nil && !(errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows)) {
				return nil, fmt.Errorf("failed to lookup account: %w", err)
			}

			// DB READ: Load category rules if account found
			var rules []categoryRuleComponent
			if entityID != "" {
				rules, _ = loadCategoryRuleComponentsPgx(ctx, pool, accountNumber, entityID, currency)
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

							// Parse value_date (if present) for effective_date comparisons
							var parsedValDate sql.NullTime
							if v, ok := txnMap["value_date"]; ok {
								switch t := v.(type) {
								case time.Time:
									parsedValDate = sql.NullTime{Time: t, Valid: true}
								case string:
									if pd, err := time.Parse(constants.DateFormat, t); err == nil {
										parsedValDate = sql.NullTime{Time: pd, Valid: true}
									} else if pd2, err2 := time.Parse(time.RFC3339, t); err2 == nil {
										parsedValDate = sql.NullTime{Time: pd2, Valid: true}
									}
								}
							}

							categoryID := matchCategoryForTransaction(rules, desc, withdrawal, deposit, parsedValDate)
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

func previewHasCoreColumns(colIdx map[string]int) bool {
	hasDate, hasDesc := false, false
	for k, v := range colIdx {
		if v < 0 {
			continue
		}
		kl := strings.ToLower(strings.TrimSpace(k))
		if strings.Contains(kl, "date") {
			hasDate = true
		}
		if strings.Contains(kl, "description") || strings.Contains(kl, "narration") ||
			strings.Contains(kl, "remark") || strings.Contains(kl, "particular") {
			hasDesc = true
		}
	}
	return hasDate && hasDesc
}

// applyLLMColumnLayoutIfNeeded runs LLM column detection only when inference is enabled
// and manual header/column mapping is incomplete — avoids slow LLM calls on well-formed files.
func applyLLMColumnLayoutIfNeeded(ctx context.Context, rows [][]string, txnHeaderIdx int, colIdx map[string]int) (int, map[string]int) {
	if !bindref.BrOn() {
		return txnHeaderIdx, colIdx
	}
	if txnHeaderIdx >= 0 && previewHasCoreColumns(colIdx) {
		return txnHeaderIdx, colIdx
	}
	layout, llmErr := extractColumnLayoutWithLLM(ctx, rows)
	if llmErr != nil {
		logger.LogInfo("[PREVIEW-DEBUG] LLM column layout skipped or failed: %v", llmErr)
		return txnHeaderIdx, colIdx
	}
	if layout.HeaderRowIndex != txnHeaderIdx && layout.HeaderRowIndex >= 0 {
		logger.LogInfo("[PREVIEW-DEBUG] manual txnHeaderIdx=%d but LLM says header_row=%d — using LLM row", txnHeaderIdx, layout.HeaderRowIndex)
		txnHeaderIdx = layout.HeaderRowIndex
	}
	for k, v := range layout.toColIdx() {
		if v >= 0 {
			colIdx[k] = v
		}
	}
	return txnHeaderIdx, colIdx
}
