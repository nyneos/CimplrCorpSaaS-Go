package bankstatement

import (
	apictx "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	middlewares "CimplrCorpSaas/api/middlewares"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/ctxutil"
	cashjobs "CimplrCorpSaas/internal/jobs/cash"
	"CimplrCorpSaas/internal/logger"
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shakinm/xlsReader/xls"
	"github.com/xuri/excelize/v2"
)

const bankStatementUploadModule = "bankstatement"

// tryExtractNumbersAsXLSX attempts to extract the embedded XLSX from an Apple Numbers (.numbers) file.
// Apple Numbers files are ZIP archives containing Sheets/Sheet.xlsx (or similar).
// Returns the XLSX bytes if successful, otherwise returns nil.
func tryExtractNumbersAsXLSX(data []byte) []byte {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	// Apple Numbers embeds the spreadsheet data in a few possible paths
	prefixes := []string{"Sheets/Sheet", "preview-micro.jpg", "buildVersionHistory.plist"}
	_ = prefixes
	for _, f := range zr.File {
		name := strings.ToLower(f.Name)
		// Look for any xlsx inside the zip (Numbers puts it under Sheets/)
		if strings.HasSuffix(name, ".xlsx") {
			rc, openErr := f.Open()
			if openErr != nil {
				continue
			}
			xlsxBytes, readErr := io.ReadAll(rc)
			rc.Close()
			if readErr == nil && len(xlsxBytes) > 0 {
				logger.LogInfo("[BANK-PARSE] extracted embedded xlsx '%s' from .numbers file (%d bytes)", f.Name, len(xlsxBytes))
				return xlsxBytes
			}
		}
	}
	return nil
}

// UploadBankStatementV2WithCategorization wraps UploadBankStatementV2 and adds category intelligence and KPIs to the response.

func UploadBankStatementV2WithCategorization(ctx context.Context, pool *pgxpool.Pool, file multipart.File, fileHash string, opts UploadOpts) (map[string]interface{}, error) {
	useMapping := opts.UseMapping
	mappings := opts.Mappings
	accountNumberOverride := opts.AccountNumberOverride
	uploadFileName := opts.UploadFileName
	uploadedBy := opts.UploadedBy

	// 1. Idempotency: Check if file hash already exists
	var exists bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM cimplrcorpsaas.bank_statements WHERE file_hash = $1 AND COALESCE(is_deleted, false) = false)`, fileHash).Scan(&exists)
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
	contentType := s3storage.DetectContentType(tmpFile)

	// If this is an Apple Numbers file, extract the embedded XLSX first
	if extracted := tryExtractNumbersAsXLSX(tmpFile); extracted != nil {
		logger.LogInfo("[BANK-PARSE] .numbers file detected — using embedded XLSX (%d bytes)", len(extracted))
		tmpFile = extracted
	}

	var rows [][]string
	var isCSV bool

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
				var xlsBook xls.Workbook
				xlsBook, xlsErr = xls.OpenFile(tmpXlsFile.Name())
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

		// If XLS failed, try CSV / TSV
		if xlsErr != nil {
			rows, err = parseDelimitedTextRows(tmpFile)
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

	// If caller supplied an explicit account number override, skip all file-based extraction.
	if strings.TrimSpace(accountNumberOverride) != "" {
		accountNumber = strings.TrimSpace(accountNumberOverride)
		logger.LogInfo("[BANK-UPLOAD-DEBUG] Using caller-supplied account_number override=%q", accountNumber)
	}

	// If custom mapping is provided, attempt to extract account number and name using the mapped header names.
	if accountNumber == "" && mappings != nil {
		// Search for custom mapped headers in first 20 rows
		for i := 0; i < len(rows) && i < 20; i++ {
			for j, cell := range rows[i] {
				normCell := normalizeCell(cell)

				// Check for AccountNumber mapping
				if mappings.AccountNumber != "" && strings.EqualFold(normCell, mappings.AccountNumber) {
					if j+1 < len(rows[i]) {
						accountNumber = extractAccountFromCell(rows[i][j+1])
					}
					if accountNumber == "" && i+1 < len(rows) && j < len(rows[i+1]) {
						accountNumber = extractAccountFromCell(rows[i+1][j])
					}
				}

				// Check for AccountName mapping
				if mappings.AccountName != "" && strings.EqualFold(normCell, mappings.AccountName) {
					if j+1 < len(rows[i]) {
						accountName = normalizeCell(rows[i][j+1])
					}
					if accountName == "" && i+1 < len(rows) && j < len(rows[i+1]) {
						accountName = normalizeCell(rows[i+1][j])
					}
				}
			}
		}
	}

	// Strategy 3: LLM extraction — try before falling through to manual label/regex scan.
	// Fails silently (logs and moves on) if the inference service is not configured or unavailable.
	if accountNumber == "" {
		if info, llmErr := extractAccountInfoWithLLM(ctx, rows); llmErr == nil && info.AccountNumber != "" {
			accountNumber = info.AccountNumber
			if accountName == "" {
				accountName = info.AccountName
			}
			logger.LogInfo("[BANK-UPLOAD-DEBUG] LLM extracted accountNumber=%q accountName=%q", accountNumber, accountName)
		} else if llmErr != nil {
			logger.LogInfo("[BANK-UPLOAD-DEBUG] LLM account extraction skipped or failed: %v", llmErr)
		}
	}

	// If account number wasn't found via mapping or override, proceed with the regular extraction logic.
	if accountNumber == "" {
		// Use default extraction logic when not using custom mapping or when mapping failed
		// Unified label scan for both CSV and XLS/XLSX — same variants, same adjacent-cell logic.
		for i := 0; i < 20 && i < len(rows); i++ {
			for j, cell := range rows[i] {
				nc := normalizeCell(cell)
				isAcctLabel := nc == acNoHeader ||
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
					// Try right-neighbor first (label | value on same row)
					if j+1 < len(rows[i]) {
						if v := extractAccountFromCell(rows[i][j+1]); v != "" {
							accountNumber = v
						}
					}
					// Try below (label on one row, value on next row same column)
					if accountNumber == "" && i+1 < len(rows) && j < len(rows[i+1]) {
						if v := extractAccountFromCell(rows[i+1][j]); v != "" {
							accountNumber = v
						}
					}
				}
				isNameLabel := cell == "Name:" ||
					strings.EqualFold(nc, "Account Name") ||
					strings.EqualFold(nc, "Name")
				if isNameLabel && j+1 < len(rows[i]) {
					accountName = normalizeCell(rows[i][j+1])
				}
			}
			if accountNumber != "" {
				break
			}
		}
	}

	// If account number still not found via mapping/header scan, run fallback extraction
	if accountNumber == "" {
		// Fallback: try to extract account number from the first 20 header rows
		// Handles cases like "Account Number: 36013001" in a single cell,
		// label in one cell and value in adjacent or next-row cell, or merged cells.
		acctLabelRe := regexp.MustCompile(`(?i)\b(?:a[/\\]?c|acct|account)[\s\.:#-]*(?:no|number)?\b`)
		acctNumberRe := regexp.MustCompile(`\d{6,}`)
		acctAlphaNumRe := regexp.MustCompile(`[A-Za-z0-9]{6,}`) // for alphanumeric account numbers
		extractCandFromCell := func(cell string) string {
			v := normalizeCell(cell)
			if m := acctNumberRe.FindString(v); m != "" {
				return strings.ReplaceAll(strings.ReplaceAll(m, " ", ""), "-", "")
			}
			if m := acctAlphaNumRe.FindString(v); m != "" {
				return strings.TrimSpace(m)
			}
			return ""
		}
		for i := 0; i < 20 && i < len(rows); i++ {
			for j, cell := range rows[i] {
				v := normalizeCell(cell)
				// If cell contains a label, try to extract digits from same cell
				if acctLabelRe.MatchString(v) {
					if m := extractCandFromCell(v); m != "" {
						accountNumber = m
						break
					}
					// try adjacent cell on same row
					if j+1 < len(rows[i]) {
						if m := extractCandFromCell(rows[i][j+1]); m != "" {
							accountNumber = m
							break
						}
					}
					// try same column next row (merged/stacked label)
					if i+1 < len(rows) && j < len(rows[i+1]) {
						if m := extractCandFromCell(rows[i+1][j]); m != "" {
							accountNumber = m
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
							logger.LogInfo("[BANK-UPLOAD-DEBUG] Title-line extracted accountNumber=%q", accountNumber)
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

			// Second pass: no label found — search any header cell for >=7-digit sequences.
			// Only scan rows 0..19 (true header area); do NOT scan transaction data rows to
			// avoid picking up reference numbers from narrative/cheque columns.
			if found == "" {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Label scan found nothing; dumping first 20 rows for inspection")
				for i := 0; i < 20 && i < len(rows); i++ {
					logger.LogInfo("[BANK-UPLOAD-DEBUG] header-row[%d]=%q", i, rows[i])
				}
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
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Fallback extracted accountNumber=%q", accountNumber)
			} else {
				// Dump header rows for debugging when no candidate found
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Account number not found in header; dumping first 20 rows for inspection")
				for i := 0; i < 20 && i < len(rows); i++ {
					logger.LogInfo("[BANK-UPLOAD-DEBUG] row[%d]=%q", i, rows[i])
				}
				return nil, ErrAccountNumberMissing
			}
		}
	}

	// If account number is still empty after extraction/fallback, return error
	// (this catches both custom mapping failures and default extraction failures)
	if accountNumber == "" {
		if useMapping && mappings != nil {
			return nil, fmt.Errorf("account number not found using custom mapping for column '%s'. Please verify the column name in your file matches exactly", mappings.AccountNumber)
		}
		return nil, ErrAccountNumberMissing
	}

	// Lookup entity_id and bank_name from masterbankaccount/masterbank
	var bankName string
	logger.LogInfo("[BANK-UPLOAD-DEBUG] Looking up account in masterbankaccount for accountNumber=%q", accountNumber)
	err = pool.QueryRow(ctx, `
		SELECT mba.entity_id, mb.bank_name, COALESCE(mba.account_nickname, mb.bank_name)
		FROM public.masterbankaccount mba
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		WHERE mba.account_number = $1 AND mba.is_deleted = false
	`, accountNumber).Scan(&entityID, &bankName, &accountName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Primary candidate not found.
			//
			// Strategy (in priority order):
			//  1. Digit runs (>=7) extracted from the original filename — most reliable for
			//     Citibank-style filenames like "95201994179_GXLSM_0036013656_PUN_28022026_M.xls"
			//     where the real account number is embedded directly in the name.
			//  2. Digit runs from true header rows only (rows BEFORE the data header row, i.e.
			//     rows that do NOT look like a column-header line). This prevents picking up
			//     counterparty account numbers that appear inside transaction description cells.
			//  3. Alphanumeric runs from header rows (for BG/NEFT reference-style accounts).
			//
			// All matching is done with a SINGLE batch DB query (WHERE account_number = ANY($1))
			// to avoid N serial round-trips. We then pick the match whose candidate appeared
			// earliest in the priority-ordered list.
			logger.LogInfo("[BANK-UPLOAD-DEBUG] Primary account %q not found; scanning filename+header for verified candidates", accountNumber)

			candidates := []string{}
			seen := map[string]bool{}

			// addCand keeps the original segment (with dashes, e.g. "0-456789-678") as priority candidate
			// and also adds the dash/space-stripped version for banks that store without dashes.
			addCand := func(s string) {
				s = strings.TrimSpace(s)
				if s == "" {
					return
				}
				// Add original first (exact match wins)
				if !seen[s] {
					seen[s] = true
					candidates = append(candidates, s)
				}
				// Also add dash/space-stripped version if different (e.g. "0-456789-678" → "0456789678")
				stripped := strings.ReplaceAll(strings.ReplaceAll(s, " ", ""), "-", "")
				if stripped != s && stripped != "" && !seen[stripped] {
					seen[stripped] = true
					candidates = append(candidates, stripped)
				}
				// Also add leading-zero-stripped version (e.g. "0714447177" → "714447177")
				// Some banks store account numbers without leading zeros in their DB.
				noLeadZero := strings.TrimLeft(stripped, "0")
				if noLeadZero != stripped && noLeadZero != "" && !seen[noLeadZero] {
					seen[noLeadZero] = true
					candidates = append(candidates, noLeadZero)
				}
			}

			// --- Priority 1: filename digit/account runs ---
			filenameDigitRe := regexp.MustCompile(`\d{7,}`)
			baseName := filepath.Base(uploadFileName)
			// 1a: Split on _ space dot (NOT dash) so "0-456789-678" is preserved as a single segment.
			// Dash-containing segments are valid account numbers at some banks.
			dashAcctSegRe := regexp.MustCompile(`^\d[\d\-]{5,}\d$`) // starts+ends with digit, interior has digits+dashes, >=7 chars total
			for _, part := range regexp.MustCompile(`[_\s\.]+`).Split(baseName, -1) {
				part = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(part, ".xls"), ".xlsx"))
				if filenameDigitRe.MatchString(part) || (len(part) >= 7 && dashAcctSegRe.MatchString(part)) {
					addCand(part) // adds original (with dashes) + stripped version
				}
			}
			// 1b: Also split on all separators including dash (catches plain digit segments)
			for _, part := range regexp.MustCompile(`[_\-\s\.]+`).Split(baseName, -1) {
				part = strings.TrimSpace(part)
				if filenameDigitRe.MatchString(part) {
					addCand(part)
				}
			}
			// 1c: raw digit runs anywhere in filename
			for _, m := range filenameDigitRe.FindAllString(baseName, -1) {
				addCand(m)
			}

			// --- Priority 2 & 3: header rows only (before the data header row) ---
			// Detect where the data header row starts: first row containing >= 2 cells that look
			// like column header words (DATE, DESCRIPTION, DEBIT, CREDIT, BALANCE, AMOUNT, etc.)
			colHeaderRe := regexp.MustCompile(`(?i)^(date|value\s*date|description|narration|particulars|debit|credit|withdrawal|deposit|balance|amount|dr|cr|time)$`)
			headerEnd := 20 // default: scan all first 20 rows
			for i := 0; i < 20 && i < len(rows); i++ {
				hits := 0
				for _, cell := range rows[i] {
					if colHeaderRe.MatchString(strings.TrimSpace(cell)) {
						hits++
					}
				}
				if hits >= 2 {
					headerEnd = i // do NOT scan this row or beyond — transaction data follows
					break
				}
			}

			alphaNumCandRe := regexp.MustCompile(`[A-Za-z0-9]{7,}`)
			for i := 0; i < headerEnd && i < len(rows); i++ {
				for _, cell := range rows[i] {
					v := normalizeCell(cell)
					for _, m := range acctNumberRe.FindAllString(v, -1) {
						addCand(m)
					}
					for _, m := range alphaNumCandRe.FindAllString(v, -1) {
						cand := strings.TrimSpace(m)
						if cand == "" || seen[cand] || acctNumberRe.MatchString(cand) {
							continue
						}
						addCand(cand)
					}
				}
			}

			logger.LogInfo("[BANK-UPLOAD-DEBUG] Candidate scan: %d candidates (filename+header rows 0..%d)", len(candidates), headerEnd-1)
			for _, c := range candidates {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Trying candidate accountNumber=%q", c)
			}

			// Single batch DB query instead of N serial round-trips
			matched := false
			if len(candidates) > 0 {
				rows2, qErr := pool.Query(ctx, `
    SELECT mba.account_number, mba.entity_id, mb.bank_name, COALESCE(mba.account_nickname, mb.bank_name)
    FROM public.masterbankaccount mba
    LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
    WHERE mba.account_number = ANY($1) AND mba.is_deleted = false
`, candidates)
				if qErr != nil {
					return nil, fmt.Errorf(constants.ErrDBMasterBankAccountLookup, qErr)
				}
				// Build a map of accountNumber → (entityID, bankName, accountName) for all DB matches
				type acctRow struct{ eID, bName, aName string }
				dbMatches := map[string]acctRow{}
				for rows2.Next() {
					var acNum string
					var ar acctRow
					if sErr := rows2.Scan(&acNum, &ar.eID, &ar.bName, &ar.aName); sErr == nil {
						dbMatches[acNum] = ar
					}
				}
				rows2.Close()
				// Pick the first candidate (in priority order) that the DB confirmed
				for _, cand := range candidates {
					if ar, ok := dbMatches[cand]; ok {
						accountNumber = cand
						entityID = ar.eID
						bankName = ar.bName
						accountName = ar.aName
						matched = true
						logger.LogInfo("[BANK-UPLOAD-DEBUG] Candidate matched accountNumber=%q", cand)
						break
					}
				}
			}

			if !matched {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Account not found in masterbankaccount after candidate scan: %v", candidates)
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Dumping first 20 rows for inspection:")
				for i := 0; i < 20 && i < len(rows); i++ {
					logger.LogInfo("[BANK-UPLOAD-DEBUG] row[%d]=%q", i, rows[i])
				}
				return nil, ErrAccountNotFound
			}
		} else {
			logger.LogInfo("[BANK-UPLOAD-DEBUG] DB error while looking up account %q: %v", accountNumber, err)
			return nil, fmt.Errorf(constants.ErrDBMasterBankAccountLookup, err)
		}
	}

	// Context validations (entity scope + approved bank/account lists)
	if ids := ctxutil.FromContext(ctx).EntityIDs; len(ids) > 0 {
		if !ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
			return nil, errors.New("no access to this entity")
		}
	}
	if bankName != "" {
		if names := apictx.GetBankNamesFromCtx(ctx); len(names) > 0 {
			if !ctxutil.FromContext(ctx).HasApprovedBank(bankName) {
				return nil, errors.New(constants.ErrBankNotAllowed)
			}
		}
	}
	if len(ctxutil.FromContext(ctx).BankAccounts) > 0 {
		if !ctxutil.FromContext(ctx).HasApprovedBankAccount(accountNumber) {
			return nil, errors.New("bank account not approved")
		}
	}

	// Currency enforcement (entity -> bank -> account -> currency)
	var acctCurrency sql.NullString
	if curCodes := ctxApprovedCurrencies(ctx); len(curCodes) > 0 {
		_ = pool.QueryRow(ctx, `SELECT mba.currency FROM public.masterbankaccount mba WHERE mba.account_number = $1 LIMIT 1`, accountNumber).Scan(&acctCurrency)
		if acctCurrency.Valid && strings.TrimSpace(acctCurrency.String) != "" {
			if !ctxutil.FromContext(ctx).HasApprovedCurrency(acctCurrency.String) {
				return nil, errors.New(constants.ErrCurrencyNotAllowed)
			}
		}
	}

	storedFileName := s3storage.BuildUploadedFilename(uploadFileName, uploadedBy, time.Now().UTC())
	folder := s3storage.GetStoragePrefix(bankStatementUploadModule)
	s3Key := s3storage.BuildNamedS3Key(folder, "", storedFileName)
	var uploadS3Key sql.NullString
	var s3URL string
	if s3storage.IsS3UploadEnabled() {
		if err = s3storage.PutObjectToS3(ctx, s3Key, tmpFile, contentType); err != nil {
			return nil, fmt.Errorf("failed to store original file to s3: %w", err)
		}
		uploadS3Key = sql.NullString{String: s3Key, Valid: true}
		if s3URL, err = s3storage.GetDownloadPresignedURL(ctx, s3Key, 0); err != nil {
			logger.LogInfo("[BANK-UPLOAD-DEBUG] failed to presign upload response url for key=%q: %v", s3Key, err)
			s3URL = ""
		}
	} else {
		logger.LogInfo("[BANK-UPLOAD-DEBUG] S3 upload disabled by BANK_STMT_S3_ENABLED=false; skipping storage for account=%q key=%q", accountNumber, s3Key)
		s3URL = ""
	}

	// 4. Fetch all active rules/components for this account/entity/bank/currency/global
	var currencyCode string
	if acctCurrency.Valid {
		currencyCode = acctCurrency.String
	}
	rules, err := loadCategoryRuleComponentsPgx(ctx, pool, accountNumber, entityID, currencyCode)
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

	// LLM column layout detection — seeds txnHeaderIdx and semantic colIdx keys.
	// Manual detection below runs only when LLM fails or is not configured.
	llmHeaderIdx := -1
	llmColMap := map[string]int{}
	if layout, llmErr := extractColumnLayoutWithLLM(ctx, rows); llmErr == nil {
		llmHeaderIdx = layout.HeaderRowIndex
		llmColMap = layout.toColIdx()
		logger.LogInfo("[BANK-UPLOAD-DEBUG] LLM column layout: header_row=%d cols=%v", llmHeaderIdx, llmColMap)
	} else {
		logger.LogInfo("[BANK-UPLOAD-DEBUG] LLM column layout skipped or failed: %v", llmErr)
	}

	if isCSV {
		// Seed from LLM result if available; manual scan only runs when LLM failed.
		if llmHeaderIdx >= 0 {
			txnHeaderIdx = llmHeaderIdx
		}
		// For CSV, header often contains Date + Description (or Detail Description)
		// and Debit/Credit (or Withdrawal/Deposit) columns. Try to find a row
		// that looks like the transaction header instead of relying solely on
		// a "Sl. No." column which many banks omit.
		if txnHeaderIdx == -1 {
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
		}
		// If not found, look for a row that has Date and either Description or Debit/Credit columns
		if txnHeaderIdx == -1 {
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
					txnHeaderIdx = i
					logger.LogInfo("[BANK-UPLOAD-DEBUG] CSV header found at row %d: %q", i, row)
					break
				}
			}
		}
		// Fallback for headless PDF-to-CSV exports (e.g. SBI)
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
						logger.LogInfo("[BANK-UPLOAD-DEBUG] Headless SBI table detected. Inserted synthetic header at row %d", txnHeaderIdx)

						// Fix shifted columns in the data rows for this headless table
						for r := txnHeaderIdx + 1; r < len(rows); r++ {
							if len(rows[r]) >= 14 {
								// Move stray deposit amounts from index 11 to 12
								val12 := strings.TrimSpace(rows[r][12])
								if val12 == "" || val12 == "-" {
									val11 := strings.TrimSpace(rows[r][11])
									if val11 != "" && val11 != "-" {
										rows[r][12] = rows[r][11]
										rows[r][11] = ""
									}
								}
								// Move stray withdrawal amounts from 8 or 10 to 9
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

		if txnHeaderIdx == -1 {
			logger.LogInfo("[BANK-UPLOAD-DEBUG] CSV header detection FAILED. Dumping first 100 rows:")
			for i := 0; i < 100 && i < len(rows); i++ {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] CSV row[%d]=%q", i, rows[i])
			}
			return nil, errors.New("transaction header row not found in CSV file")
		}
		headerRow = rows[txnHeaderIdx]
		colIdx = map[string]int{}

		// Base mapping: index header names by their trimmed text
		for idx, col := range headerRow {
			colIdx[strings.TrimSpace(col)] = idx
		}

		// LLM-detected semantic columns take priority — overwrite base mapping unconditionally.
		// Alias expansion below only fills keys LLM left unset (returned as -1).
		maps.Copy(colIdx, llmColMap)

		// If custom mapping provided, apply mapping fields as overrides (partial mappings allowed)
		if mappings != nil {
			for idx, col := range headerRow {
				normCol := strings.TrimSpace(col)

				// Map custom names to standard internal names only when provided
				if mappings.TranID != "" && strings.EqualFold(normCol, mappings.TranID) {
					colIdx[tranIDHeader] = idx
				}
				if mappings.ValueDate != "" && strings.EqualFold(normCol, mappings.ValueDate) {
					colIdx[valueDateHeader] = idx
					colIdx["Date"] = idx // Also map to "Date" for compatibility
				}
				if mappings.Description != "" && strings.EqualFold(normCol, mappings.Description) {
					colIdx["Description"] = idx
				}
				if mappings.DepositAmount != "" && strings.EqualFold(normCol, mappings.DepositAmount) {
					colIdx["Deposit"] = idx
					colIdx[depositAmtHeader] = idx
				}
				if mappings.WithdrawalAmount != "" && strings.EqualFold(normCol, mappings.WithdrawalAmount) {
					colIdx["Withdrawal"] = idx
					colIdx[withdrawalAmtHeader] = idx
				}
				if mappings.Balance != "" && strings.EqualFold(normCol, mappings.Balance) {
					colIdx["Balance"] = idx
					colIdx[balanceHeader] = idx
				}
			}
			logger.LogInfo("[BANK-UPLOAD-DEBUG] Custom mapping overrides applied: %v", colIdx)
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
			logger.LogInfo("[BANK-UPLOAD-DEBUG] excel-header-col idx=%d header=%q norm=%q numericSample=%v samples=%q", idx, col, norm, numericSample, samples)
		}

		// Add flexible column name mappings for CSV as well
		findColContaining := func(keywords ...string) int {
			for idx, colName := range headerRow {
				lcName := strings.ToLower(strings.TrimSpace(colName))
				if strings.Contains(lcName, "ref") {
					continue
				}
				for _, kw := range keywords {
					kwLc := strings.ToLower(kw)
					if len(kwLc) <= 3 {
						if lcName == kwLc || strings.HasPrefix(lcName, kwLc+" ") || strings.HasSuffix(lcName, " "+kwLc) || strings.Contains(lcName, " "+kwLc+" ") {
							return idx
						}
					} else if strings.Contains(lcName, kwLc) {
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
		// Force remap Description column only when LLM did not already identify it.
		if _, hasLLMDesc := llmColMap["Description"]; !hasLLMDesc {
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
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Using Debit/Credit Ref column %d for Description", idx)
			} else if idx := findColContaining("description", "remarks", "narration", "narrative", "particulars"); idx >= 0 {
				colIdx["Description"] = idx
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Using Description-like column %d for Description", idx)
			}
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
		logger.LogInfo("[BANK-UPLOAD-DEBUG] findDebitCreditCols returned: Withdrawal=%d, Deposit=%d", wIdx, dIdx)
		logger.LogInfo("[BANK-UPLOAD-DEBUG] Headers: %v", headerRow)
		// Only apply findDebitCreditCols result for columns LLM did not already identify.
		if wIdx >= 0 {
			if _, ok := llmColMap["Withdrawal"]; !ok {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Setting Withdrawal to column %d (header: %s)", wIdx, headerRow[wIdx])
				colIdx["Withdrawal"] = wIdx
				colIdx[withdrawalAmtHeader] = wIdx
			}
		}
		if dIdx >= 0 {
			if _, ok := llmColMap["Deposit"]; !ok {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Setting Deposit to column %d (header: %s)", dIdx, headerRow[dIdx])
				colIdx["Deposit"] = dIdx
				colIdx[depositAmtHeader] = dIdx
			}
		}
		logger.LogInfo("[BANK-UPLOAD-DEBUG] CSV Column mapping: Date=%d, Description=%d, Withdrawal=%d, Deposit=%d, Balance=%d",
			colIdx["Date"], colIdx["Description"], colIdx["Withdrawal"], colIdx["Deposit"], colIdx["Balance"])

		// Flexible required columns: ensure we have Date, a description column, and at least one amount column
		hasDate := false
		hasDesc := false
		hasAmount := false
		for col := range colIdx {
			lc := strings.ToLower(col)
			if headerContainsAny(lc, bankStmtDateHeaderAliases) {
				hasDate = true
			}
			if headerContainsAny(lc, bankStmtDescriptionHeaderAliases) {
				hasDesc = true
			}
			if headerContainsAny(lc, bankStmtDebitHeaderAliases) || headerContainsAny(lc, bankStmtCreditHeaderAliases) {
				hasAmount = true
			}
		}
		if !hasDate || (!hasDesc && !hasAmount) {
			return nil, errors.New("transaction header row not found in CSV file")
		}
	} else {
		// Seed from LLM result if available; manual scan only runs when LLM failed.
		if llmHeaderIdx >= 0 {
			txnHeaderIdx = llmHeaderIdx
		}
		// For Excel/XLS, some statements don't include a constants.TranID column.
		// Try the original Tran Id detection first, then fall back to a
		// flexible detection similar to CSV: find a row that has Date and
		// (Description-like column OR amount columns).
		if txnHeaderIdx == -1 {
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
		}
		if txnHeaderIdx == -1 {
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
					txnHeaderIdx = i
					logger.LogInfo("[BANK-UPLOAD-DEBUG] Excel header found at row %d: %q", i, row)
					break
				}
			}
		}
		if txnHeaderIdx == -1 {
			logger.LogInfo("[BANK-UPLOAD-DEBUG] Excel header detection FAILED. Dumping first 30 rows:")
			for i := 0; i < 30 && i < len(rows); i++ {
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Excel row[%d]=%q", i, rows[i])
			}
			return nil, errors.New(constants.ErrTransactionHeaderRowNotFound)
		}
		headerRow = rows[txnHeaderIdx]
		colIdx = map[string]int{}

		// Base mapping: index header names by their trimmed text
		for idx, col := range headerRow {
			colIdx[strings.TrimSpace(col)] = idx
		}

		// LLM-detected semantic columns take priority — overwrite base mapping unconditionally.
		// Alias expansion below only fills keys LLM left unset (returned as -1).
		maps.Copy(colIdx, llmColMap)

		// If custom mapping provided, apply mapping fields as overrides (partial mappings allowed)
		if mappings != nil {
			for idx, col := range headerRow {
				normCol := strings.TrimSpace(col)

				// Map custom names to standard internal names only when provided
				if mappings.TranID != "" && strings.EqualFold(normCol, mappings.TranID) {
					colIdx[tranIDHeader] = idx
				}
				if mappings.ValueDate != "" && strings.EqualFold(normCol, mappings.ValueDate) {
					colIdx[valueDateHeader] = idx
					colIdx[transactionDateHeader] = idx
				}
				if mappings.Description != "" && strings.EqualFold(normCol, mappings.Description) {
					colIdx["Description"] = idx
					colIdx[transactionRemarksHeader] = idx
				}
				if mappings.DepositAmount != "" && strings.EqualFold(normCol, mappings.DepositAmount) {
					colIdx[depositAmtHeader] = idx
				}
				if mappings.WithdrawalAmount != "" && strings.EqualFold(normCol, mappings.WithdrawalAmount) {
					colIdx[withdrawalAmtHeader] = idx
				}
				if mappings.Balance != "" && strings.EqualFold(normCol, mappings.Balance) {
					colIdx[balanceHeader] = idx
				}
			}
			logger.LogInfo("[BANK-UPLOAD-DEBUG] Custom mapping overrides applied for Excel: %v", colIdx)
		}
		// Add flexible column name mappings for common variations
		// This allows matching "Detail Description" when code looks for "Description"
		findColContaining := func(keywords ...string) int {
			for idx, colName := range headerRow {
				lcName := strings.ToLower(strings.TrimSpace(colName))
				if strings.Contains(lcName, "ref") {
					continue
				}
				for _, kw := range keywords {
					kwLc := strings.ToLower(kw)
					if len(kwLc) <= 3 {
						if lcName == kwLc || strings.HasPrefix(lcName, kwLc+" ") || strings.HasSuffix(lcName, " "+kwLc) || strings.Contains(lcName, " "+kwLc+" ") {
							return idx
						}
					} else if strings.Contains(lcName, kwLc) {
						return idx
					}
				}
			}
			return -1
		}

		// Normalize common variants so downstream parsing does not miss dates.
		if _, ok := colIdx[transactionDateHeader]; !ok {
			if idx := findColContaining(constants.TransactionDate, constants.TransactionPostedDate, constants.TransactionPostedDateAlt, constants.TransactionPostingDate); idx >= 0 {
				colIdx[transactionDateHeader] = idx
			}
		}
		if _, ok := colIdx["PostedDate"]; !ok {
			if idx := findColContaining(constants.TransactionPostedDate, constants.TransactionPostedDateAlt, constants.TransactionPostingDate); idx >= 0 {
				colIdx["PostedDate"] = idx
			}
		}
		if _, ok := colIdx[valueDateHeader]; !ok {
			if idx := findColContaining(constants.ValueDate, "value-date", "val date"); idx >= 0 {
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
				lcName := strings.ToLower(strings.TrimSpace(colName))
				if strings.Contains(lcName, "cr/dr") || strings.Contains(lcName, "crdr") ||
					strings.Contains(lcName, "cr / dr") ||
					strings.Contains(lcName, "dr/cr") || strings.Contains(lcName, "dr / cr") ||
					strings.Contains(lcName, "dr|cr") || strings.Contains(lcName, "cr|dr") ||
					strings.Contains(lcName, "dr | cr") || strings.Contains(lcName, "cr | dr") ||
					lcName == "cr" || lcName == "dr" ||
					strings.Contains(lcName, "credit/debit") || strings.Contains(lcName, "debit/credit") {
					return idx
				}
			}
			return -1
		}

		// Map standard names to flexible column indices
		if _, exists := colIdx["Description"]; !exists {
			// Prefer explicit description column when present (uses full alias list)
			if idx := findColContaining(bankStmtDescriptionHeaderAliases...); idx >= 0 {
				colIdx["Description"] = idx
			} else if idx := findColContaining(constants.ErrDebitCreditReference2, constants.ErrDebitCreditReferenceShort, constants.ErrDebitCreditReferenceAlt, constants.ErrDebitCreditReference, "debitcreditref"); idx >= 0 {
				colIdx["Description"] = idx
			}
		}
		if _, exists := colIdx[transactionRemarksHeader]; !exists {
			// Prefer Debit/Credit Ref header if present for transaction remarks
			if idx := findColContaining(constants.ErrDebitCreditReference2, "debit / credit ref", constants.ErrDebitCreditReferenceShort, constants.ErrDebitCreditReferenceAlt, constants.ErrDebitCreditReference, "debitcreditref"); idx >= 0 {
				colIdx[transactionRemarksHeader] = idx
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Using Debit/Credit Ref column %d for Transaction Remarks", idx)
			} else if idx := findColContaining(bankStmtDescriptionHeaderAliases...); idx >= 0 {
				colIdx[transactionRemarksHeader] = idx
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Using Description-like column %d for Transaction Remarks", idx)
			}
		}
		if _, exists := colIdx["Date"]; !exists {
			if idx := findColContaining("date"); idx >= 0 && !strings.Contains(strings.ToLower(headerRow[idx]), "value") && !strings.Contains(strings.ToLower(headerRow[idx]), "posted") {
				colIdx["Date"] = idx
			}
		}
		if _, exists := colIdx[valueDateHeader]; !exists {
			// Try constants.ValueDateAlt, "Txn Posted Date", or any date column
			if idx := findColContaining(constants.ValueDate, constants.TransactionPostedDate); idx >= 0 {
				colIdx[valueDateHeader] = idx
			} else if idx := findColContaining("date"); idx >= 0 {
				colIdx[valueDateHeader] = idx
			}
		}
		if _, exists := colIdx[transactionDateHeader]; !exists {
			// Try constants.TransactionDateAlt, "Txn Posted Date", or fallback to "Date"
			if idx := findColContaining(constants.TransactionDate, constants.TransactionPostedDate, constants.TransactionPostedDateAlt); idx >= 0 {
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
		// Detect TIME STAMP / Time column (used by some banks e.g. Citi GXLSM statements).
		// Stored under "TimeStamp" key so downstream row-parsing can sort within the same date.
		if _, exists := colIdx["TimeStamp"]; !exists {
			if idx := findColContaining("time stamp", "timestamp", "time"); idx >= 0 {
				colIdx["TimeStamp"] = idx
				logger.LogInfo("[BANK-UPLOAD-DEBUG] TIME STAMP column detected at index %d", idx)
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

		// Detect Cr/Dr indicator column only when LLM did not already identify it.
		if crDrIdx = findCrDrLike(); crDrIdx >= 0 {
			if _, ok := llmColMap["CrDr"]; !ok {
				colIdx["CrDr"] = crDrIdx
			}
		}

		// First try to find separate withdrawal/deposit columns.
		// IMPORTANT: iterate headerRow cells (original file headers), NOT colIdx keys —
		// colIdx contains synthesized keys like "Description" whose text can accidentally
		// match short credit aliases (e.g. "cr" inside "description").
		if _, exists := colIdx[withdrawalAmtHeader]; !exists {
			for ci, cell := range headerRow {
				lcCell := strings.ToLower(strings.TrimSpace(cell))
				if strings.Contains(lcCell, "ref") || strings.Contains(lcCell, "reference") {
					continue
				}
				if headerContainsAny(lcCell, bankStmtDebitHeaderAliases) && !headerContainsAny(lcCell, bankStmtCreditHeaderAliases) {
					colIdx[withdrawalAmtHeader] = ci
					break
				}
			}
		}
		if _, exists := colIdx[depositAmtHeader]; !exists {
			for ci, cell := range headerRow {
				lcCell := strings.ToLower(strings.TrimSpace(cell))
				if strings.Contains(lcCell, "ref") || strings.Contains(lcCell, "reference") {
					continue
				}
				if headerContainsAny(lcCell, bankStmtCreditHeaderAliases) && !headerContainsAny(lcCell, bankStmtDebitHeaderAliases) {
					colIdx[depositAmtHeader] = ci
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
			if idx := findColContaining(bankStmtBalanceHeaderAliases...); idx >= 0 {
				colIdx[balanceHeader] = idx
			}
		}
		if _, exists := colIdx["Balance"]; !exists {
			if idx := findColContaining(bankStmtBalanceHeaderAliases...); idx >= 0 {
				colIdx["Balance"] = idx
			}
		}
		if _, exists := colIdx["Withdrawal"]; !exists {
			if idx := findColContaining(bankStmtDebitHeaderAliases...); idx >= 0 {
				colIdx["Withdrawal"] = idx
			}
		}
		if _, exists := colIdx["Deposit"]; !exists {
			if idx := findColContaining(bankStmtCreditHeaderAliases...); idx >= 0 {
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
		logger.LogInfo("[BANK-UPLOAD-DEBUG] Column mapping: Date=%d, ValueDate=%d, TranID=%d, Description=%d, Withdrawal=%d, Deposit=%d, Balance=%d, CrDr=%d, Amount=%d",
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
	categoryCount := map[string]int{}
	debitSum := map[string]float64{}
	creditSum := map[string]float64{}
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
	// Unique 6-char prefix for all synthetic tran_ids in this upload batch.
	// Combined with a 7-digit sequence it supports up to 10M transactions per statement.
	stmtBatchID := generateBatchID()
	debugParse := strings.ToLower(strings.TrimSpace(os.Getenv("BANK_STMT_DEBUG_PARSE"))) == "1" || strings.ToLower(strings.TrimSpace(os.Getenv("BANK_STMT_DEBUG_PARSE"))) == "true"
	debugCount := 0
	// Determine column indices for merging multi-line descriptions
	mergeDateIdx := -1
	if idx, ok := colIdx["Date"]; ok {
		mergeDateIdx = idx
	} else if idx, ok := colIdx[constants.TransactionDateAlt]; ok {
		mergeDateIdx = idx
	} else if idx, ok := colIdx[valueDateHeader]; ok {
		mergeDateIdx = idx
	}

	mergeDescIdx := -1
	if idx, ok := colIdx[transactionRemarksHeader]; ok {
		mergeDescIdx = idx
	} else if idx, ok := colIdx["Description"]; ok {
		mergeDescIdx = idx
	}

	dataRows := rows[txnHeaderIdx+1:]
	dataRows = MergeMultiLineDescriptions(dataRows, mergeDateIdx, mergeDescIdx)

	// iterate with index so we can log original row number
	for ri, row := range dataRows {
		rowNum := txnHeaderIdx + 1 + ri
		totalRows++
		// TIME STAMP field (e.g. Citi GXLSM): populated below if the column exists.
		rowTimeStr := ""
		// Filter out non-transaction rows by checking for known non-transaction keywords in the first column
		if len(row) > 0 {
			// Skip separator lines (UBI: "----...----")
			if IsSeparatorRow(row) {
				skippedNonTxnRows++
				continue
			}
			if IsPageBreakRow(row) {
				skippedNonTxnRows++
				continue
			}
			firstCell := strings.TrimSpace(row[0])
			// Also check the description column for footer/summary rows
			descCell := firstCell
			if descIdx, ok := colIdx[transactionRemarksHeader]; ok && descIdx >= 0 && descIdx < len(row) {
				descCell = strings.TrimSpace(row[descIdx])
			} else if descIdx, ok := colIdx["Description"]; ok && descIdx >= 0 && descIdx < len(row) {
				descCell = strings.TrimSpace(row[descIdx])
			}

			// Use shared non-transaction keyword list covering SBI/UBI/all banks.
			// Also scan the full row: ICICI PDF→CSV often leaves Date/Particulars blank and puts
			// legend/footer text in Chq.No. or other columns (see "Legends for transactions…").
			rowJoined := strings.TrimSpace(strings.Join(row, " "))
			if IsNonTransactionRow(firstCell, descCell, rowJoined) {
				skippedNonTxnRows++
				continue
			}
		}
		// Defensive: fix collapsed CSV columns where empty cells were omitted (common in PDF-to-CSV)
		if len(row) < len(headerRow) && len(row) > 3 {
			lastCell := strings.TrimSpace(row[len(row)-1])
			lcLast := strings.ToLower(lastCell)
			if strings.HasSuffix(lcLast, "cr") || strings.HasSuffix(lcLast, "dr") || strings.HasSuffix(lcLast, "cr.") || strings.HasSuffix(lcLast, "dr.") {
				balIdx := -1
				if idx, ok := colIdx[balanceHeader]; ok {
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
						logger.LogInfo("[BANK-UPLOAD-DEBUG] OPENING BALANCE detected: %.2f (cumulative stays at 0, will be added via openingBalance + cumulative)", openingBalance)
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
			if tranIDIdx, ok := colIdx[tranIDHeader]; ok && tranIDIdx >= 0 && tranIDIdx < len(row) {
				tranID = sql.NullString{String: row[tranIDIdx], Valid: strings.TrimSpace(row[tranIDIdx]) != ""}
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
			// Parse both amounts upfront so we can handle the case where one is "0.00" (non-empty but zero).
			// Use parseStrictAmount (strict regex ^-?\d+(\.\d+)?$) instead of fmt.Sscanf because Sscanf
			// parses "INF" as +Inf with no error — causing pq numeric field overflow when a description
			// like "INF/INFT/..." is accidentally read from a wrong column.
			var rawWithdrawal, rawDeposit float64
			var withdrawalParsed, depositParsed bool
			if withdrawalStr != "" {
				if v, ok := parseStrictAmount(withdrawalStr); ok {
					rawWithdrawal = v
					withdrawalParsed = true
				}
			}
			if depositStr != "" {
				if v, ok := parseStrictAmount(depositStr); ok {
					rawDeposit = v
					depositParsed = true
				}
			}
			// When both columns are non-empty but one is zero (e.g. BOB: Debit=14516.57, Credit=0.00),
			// treat the zero-value column as absent so the non-zero value is picked correctly.
			if withdrawalParsed && depositParsed {
				if rawWithdrawal == 0 && rawDeposit != 0 {
					withdrawalParsed = false
				} else if rawDeposit == 0 && rawWithdrawal != 0 {
					depositParsed = false
				} else if rawWithdrawal == 0 && rawDeposit == 0 {
					// Both columns are literally zero — treat as no amounts so fallbacks / inference can run.
					withdrawalParsed = false
					depositParsed = false
				}
			}
			if withdrawalParsed && !depositParsed {
				withdrawal.Valid = true
				withdrawal.Float64 = rawWithdrawal
				// Some banks (e.g. Citibank) store debits as negative values in the debit column
				if withdrawal.Float64 < 0 {
					withdrawal.Float64 = -withdrawal.Float64
				}
				deposit.Valid = false
			} else if depositParsed && !withdrawalParsed {
				deposit.Valid = true
				deposit.Float64 = rawDeposit
				// Some banks store credits as negative values in the credit column — take abs
				if deposit.Float64 < 0 {
					deposit.Float64 = -deposit.Float64
				}
				withdrawal.Valid = false
			} else if withdrawalParsed && depositParsed {
				// Both columns have non-zero values: set both
				withdrawal.Valid = true
				withdrawal.Float64 = rawWithdrawal
				if withdrawal.Float64 < 0 {
					withdrawal.Float64 = -withdrawal.Float64
				}
				deposit.Valid = true
				deposit.Float64 = rawDeposit
				if deposit.Float64 < 0 {
					deposit.Float64 = -deposit.Float64
				}
			}

			if hasBalance && balanceIdx >= 0 && balanceIdx < len(row) {
				balanceStr := cleanAmount(row[balanceIdx])
				balance = sql.NullFloat64{Valid: false}
				if strings.TrimSpace(balanceStr) != "" {
					var parsedBal float64
					if n, err := fmt.Sscanf(balanceStr, "%f", &parsedBal); n == 1 && err == nil && isFiniteNumber(parsedBal) {
						balance = sql.NullFloat64{Valid: true, Float64: parsedBal}
					}
				}
			} else {
				balance = sql.NullFloat64{Valid: false}
			}

			// Fallback amount parsing requires balance for inference
			if !withdrawal.Valid && !deposit.Valid {
				if amtIdx := findAmountLikeCSV(); amtIdx >= 0 && amtIdx < len(row) {
					amtStr := cleanAmount(row[amtIdx])
					if amtStr != "" {
						var amt float64
						fmt.Sscanf(amtStr, "%f", &amt)

						crIdx := -1
						for k, v := range colIdx {
							lk := strings.ToLower(k)
							if strings.Contains(lk, "cr/dr") || strings.Contains(lk, "crdr") || strings.Contains(lk, "cr / dr") || strings.TrimSpace(lk) == "cr" || strings.TrimSpace(lk) == "dr" || strings.Contains(lk, "credit/debit") || strings.Contains(lk, "debit/credit") {
								crIdx = v
								break
							}
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
								if strings.HasPrefix(amtStr, "-") {
									withdrawal.Valid = true
									withdrawal.Float64 = -amt
								} else {
									deposit.Valid = true
									deposit.Float64 = amt
								}
							}
						} else {
							// No Cr/Dr column. If we have previous running balance, infer mathematically.
							if !firstValidRow && balance.Valid {
								if math.Abs((cumulative-amt)-balance.Float64) < 0.05 {
									withdrawal.Valid = true
									withdrawal.Float64 = amt
								} else if math.Abs((cumulative+amt)-balance.Float64) < 0.05 {
									deposit.Valid = true
									deposit.Float64 = amt
								} else {
									// Fallback if math doesn't match
									if strings.HasPrefix(amtStr, "-") {
										withdrawal.Valid = true
										withdrawal.Float64 = -amt
									} else {
										deposit.Valid = true
										deposit.Float64 = amt
									}
								}
							} else {
								// First row or no balance available
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
			}
			// Check for opening balance / BBF row — skip but extract opening balance
			if IsStatementOpeningCarryRow(description) {
				if balance.Valid {
					openingBalance = balance.Float64
					cumulative = openingBalance
					logger.LogInfo("[BANK-UPLOAD-DEBUG] OPENING BALANCE (BBF) detected in CSV: %.2f (initial cumulative set)", openingBalance)
				}
				skippedOpeningBalanceRows++
				continue
			}
			// Filter out rows only when both dates are invalid or explicitly marked as custom placeholders
			// If both dates are missing, fall back to the last seen valid value date before skipping.
			if valueDate.IsZero() && transactionDate.IsZero() && !lastValidValueDate.IsZero() {
				valueDate = lastValidValueDate
				transactionDate = lastValidValueDate
				if debugParse {
					logger.LogInfo("[BANK-UPLOAD-DEBUG] Row %d: Filled missing dates with last valid date %s", rowNum, lastValidValueDate.Format(constants.DateFormat))
				}
			}

			if (valueDate.IsZero() && transactionDate.IsZero()) || (valueDateStr == constants.DateFormatCustom && transactionDateStr == constants.DateFormatCustom) {
				// Check if this is a closing balance row before skipping
				if strings.Contains(strings.ToLower(description), constants.ClosingBalance) {
					if balance.Valid {
						logger.LogInfo("[BANK-UPLOAD-DEBUG] CLOSING BALANCE detected: %.2f (skipping from transactions)", balance.Float64)
					} else {
						logger.LogInfo("[BANK-UPLOAD-DEBUG] CLOSING BALANCE row detected (no balance value)")
					}
					skippedClosingBalanceRows++
				}
				if debugParse {
					logger.LogInfo("[BANK-UPLOAD-DEBUG] Skipping row %d due to missing dates (valueDate=%q transactionDate=%q) raw=%q", rowNum, valueDateStr, transactionDateStr, row)
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

			// Capture TIME STAMP column (e.g. Citi GXLSM "TIME STAMP", value "05:47:32").
			if tsIdx, ok := colIdx["TimeStamp"]; ok && tsIdx >= 0 && tsIdx < len(row) {
				rowTimeStr = strings.TrimSpace(row[tsIdx])
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
								logger.LogInfo("[BANK-UPLOAD-DEBUG] Row %d: TranID looked like date, using ValueDate cell as tran_id instead: %q", rowNum, valueDateStr)
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
						logger.LogInfo("[BANK-UPLOAD-DEBUG] Row %d: Using original TranID cell for dates: %q", rowNum, originalTranIDStr)
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
							logger.LogInfo("[BANK-UPLOAD-DEBUG] Row %d: Fallback picked date from cell value %q", rowNum, c)
						}
						break
					}
				}
			}

			// If both dates are blank but we already saw a valid date earlier, carry it forward for statements that omit the date on subsequent rows.
			if valueDate.IsZero() && transactionDate.IsZero() && !lastValidValueDate.IsZero() {
				hasAmount := false
				if idx, ok := colIdx[withdrawalAmtHeader]; ok && idx < len(row) {
					w := cleanAmount(row[idx])
					var wf float64
					if w != "" {
						if n, err := fmt.Sscanf(w, "%f", &wf); n == 1 && err == nil && isFiniteNumber(wf) {
							hasAmount = true
						}
					}
				}
				if idx, ok := colIdx[depositAmtHeader]; ok && idx < len(row) {
					d := cleanAmount(row[idx])
					var df float64
					if d != "" {
						if n, err := fmt.Sscanf(d, "%f", &df); n == 1 && err == nil && isFiniteNumber(df) {
							hasAmount = true
						}
					}
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
				if amountIdxLocal >= 0 && amountIdxLocal < len(row) {
					amountVal := cleanAmount(row[amountIdxLocal])
					var af float64
					if amountVal != "" {
						if n, err := fmt.Sscanf(amountVal, "%f", &af); n == 1 && err == nil && isFiniteNumber(af) {
							hasAmount = true
						}
					}
				}
				if hasAmount {
					valueDate = lastValidValueDate
					transactionDate = lastValidValueDate
					if postedDate.IsZero() {
						postedDate = lastValidValueDate
					}
					if debugParse {
						logger.LogInfo("[BANK-UPLOAD-DEBUG] Row %d: Carried forward last valid date %s for blank date cells", rowNum, lastValidValueDate.Format(constants.DateFormat))
					}
				}
			}

			// BBF / "Balance Brought Forward" / "Balance Carried Forward" rows are NOT real transactions.
			// They represent the bank's way of telling you the opening balance (last day's closing).
			// Extract the balance value as openingBalance and skip the row from transactions.
			if IsStatementOpeningCarryRow(tempDescLower) {
				// Prefer balance column
				if bIdx, ok := colIdx[balanceHeader]; ok && bIdx >= 0 && bIdx < len(row) {
					balanceStr := cleanAmount(row[bIdx])
					if balanceStr != "" {
						var bal float64
						if n, err := fmt.Sscanf(balanceStr, "%f", &bal); n == 1 && err == nil && isFiniteNumber(bal) {
							openingBalance = bal
							cumulative = bal
						}
					}
				}
				// Fallback: scan from the end for any numeric-looking cell
				if openingBalance == 0 {
					for i := len(row) - 1; i >= 0; i-- {
						cand := cleanAmount(row[i])
						if cand != "" {
							var bal float64
							if n, err := fmt.Sscanf(cand, "%f", &bal); n == 1 && err == nil && isFiniteNumber(bal) {
								openingBalance = bal
								cumulative = bal
								break
							}
						}
					}
				}
				logger.LogInfo("[BANK-UPLOAD-DEBUG] BBF row detected (Excel): desc=%q openingBalance=%.2f — skipping from transactions", tempDescLower, openingBalance)
				skippedOpeningBalanceRows++
				continue
			}

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
							logger.LogInfo("[BANK-UPLOAD-DEBUG] CLOSING BALANCE detected: %.2f (skipping from transactions)", closingBal)
						}
					}
					skippedClosingBalanceRows++
				}
				if debugParse {
					logger.LogInfo("[BANK-UPLOAD-DEBUG] Skipping row %d due to missing dates (valueDate=%q transactionDate=%q) raw=%q", rowNum, valueDateStr, transactionDateStr, row)
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
					logger.LogInfo("[BANK-UPLOAD-DEBUG] Row %d: Description is EMPTY (or whitespace only). Raw: '%s'", debugCount, description)
				} else {
					logger.LogInfo("[BANK-UPLOAD-DEBUG] Row %d: Description='%s'", debugCount, trimmedDesc)
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
					if n, err := fmt.Sscanf(amtStr, "%f", &amt); n != 1 || err != nil || !isFiniteNumber(amt) {
						amt = 0
					}
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
				// Parse both amounts upfront so we can handle the case where one is "0.00" (non-empty but zero)
				var rawW, rawD float64
				var wParsed, dParsed bool
				if withdrawalStr != "" {
					if n, err := fmt.Sscanf(withdrawalStr, "%f", &rawW); n == 1 && err == nil && isFiniteNumber(rawW) {
						wParsed = true
					}
				}
				if depositStr != "" {
					if n, err := fmt.Sscanf(depositStr, "%f", &rawD); n == 1 && err == nil && isFiniteNumber(rawD) {
						dParsed = true
					}
				}
				// When both columns are non-empty but one is zero (e.g. BOB), treat zero column as absent
				if wParsed && dParsed {
					if rawW == 0 && rawD != 0 {
						wParsed = false
					} else if rawD == 0 && rawW != 0 {
						dParsed = false
					} else if rawW == 0 && rawD == 0 {
						wParsed = false
						dParsed = false
					}
				}
				if wParsed && !dParsed {
					withdrawal.Valid = true
					withdrawal.Float64 = rawW
					// Some banks (e.g. Citibank) store debits as negative values in the debit column
					if withdrawal.Float64 < 0 {
						withdrawal.Float64 = -withdrawal.Float64
					}
					deposit.Valid = false
				} else if dParsed && !wParsed {
					deposit.Valid = true
					deposit.Float64 = rawD
					// Some banks store credits as negative values in the credit column — take abs
					if deposit.Float64 < 0 {
						deposit.Float64 = -deposit.Float64
					}
					withdrawal.Valid = false
				} else if wParsed && dParsed {
					// Both non-zero: set both
					withdrawal.Valid = true
					withdrawal.Float64 = rawW
					if withdrawal.Float64 < 0 {
						withdrawal.Float64 = -withdrawal.Float64
					}
					deposit.Valid = true
					deposit.Float64 = rawD
					if deposit.Float64 < 0 {
						deposit.Float64 = -deposit.Float64
					}
				} else {
					withdrawal.Valid = false
					deposit.Valid = false
				}
			}
			if bIdx, ok := colIdx[balanceHeader]; ok && bIdx >= 0 && bIdx < len(row) {
				balanceStr := cleanAmount(row[bIdx])
				balance = sql.NullFloat64{Valid: false}
				if strings.TrimSpace(balanceStr) != "" {
					var parsedBal float64
					if n, err := fmt.Sscanf(balanceStr, "%f", &parsedBal); n == 1 && err == nil && isFiniteNumber(parsedBal) {
						balance = sql.NullFloat64{Valid: true, Float64: parsedBal}
					}
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
			logger.LogInfo("[BANK-UPLOAD-DEBUG] parsed-row num=%d headerRow=%q mapping(Date=%d Description=%d Withdrawal=%d Deposit=%d Balance=%d TranID=%d) rawRow=%q\n  rawWithdrawal=%q rawDeposit=%q rawBalance=%q parsedWithdrawal.Valid=%v parsedWithdrawal=%v parsedDeposit.Valid=%v parsedDeposit=%v description=%q sanitizedDescription=%q",
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

		// Infer missing debit when PDF→CSV merge appends footer totals into amount cells (parse fails or reads 0)
		// but the balance column correctly shows a full sweep to zero (ICICI "TRANSFER TO" / sweep-out).
		if !firstValidRow {
			const sweepEps = 0.01
			lowDesc := strings.ToLower(strings.TrimSpace(description))
			wEmpty := !withdrawal.Valid || math.Abs(withdrawal.Float64) < sweepEps
			dEmpty := !deposit.Valid || math.Abs(deposit.Float64) < sweepEps
			if wEmpty && dEmpty && balance.Valid && math.Abs(balance.Float64) < sweepEps &&
				cumulative > sweepEps && isFiniteNumber(cumulative) &&
				(strings.Contains(lowDesc, "transfer to") || strings.Contains(lowDesc, "transfer-out")) {
				withdrawal = sql.NullFloat64{Valid: true, Float64: cumulative}
				deposit = sql.NullFloat64{Valid: false}
				if debugParse {
					logger.LogInfo("[BANK-UPLOAD-DEBUG] Inferred sweep withdrawal=%.2f from prior balance (desc=%q)", cumulative, description)
				}
			}
		}

		// --- COMPUTE RUNNING BALANCE (before category matching so balance is correct in uncategorized list) ---
		// Recalculate balance using a running cumulative that starts from the first available balance.
		origBalance := balance

		if firstValidRow {
			// Check if this first row is a BBF/opening carry row — extract opening balance and skip.
			if IsStatementOpeningCarryRow(description) {
				if origBalance.Valid {
					openingBalance = origBalance.Float64
					cumulative = origBalance.Float64
				} else if deposit.Valid {
					// BBF with no balance column: deposit amount is the opening balance.
					openingBalance = deposit.Float64
					cumulative = deposit.Float64
				}
				logger.LogInfo("[BANK-UPLOAD-DEBUG] BBF detected on firstValidRow: openingBalance=%.2f — skipping from transactions", openingBalance)
				// Keep firstValidRow = true so the NEXT real row correctly initialises period start.
				// But we need to track last valid date for period end.
				candidateDate := valueDate
				if candidateDate.IsZero() {
					candidateDate = transactionDate
				}
				if !candidateDate.IsZero() && (lastValidValueDate.IsZero() || candidateDate.After(lastValidValueDate)) {
					lastValidValueDate = candidateDate
				}
				skippedOpeningBalanceRows++
				continue
			}

			// Initialize cumulative from the first valid balance if present; otherwise from opening balance or zero.
			if origBalance.Valid {
				cumulative = origBalance.Float64
				if openingBalance == 0 {
					// Regular first row: back-calculate the balance BEFORE this transaction.
					//   opening = balance_after_first_txn + withdrawal - deposit
					derivedOpening := origBalance.Float64
					if withdrawal.Valid {
						derivedOpening += withdrawal.Float64
					}
					if deposit.Valid {
						derivedOpening -= deposit.Float64
					}
					openingBalance = derivedOpening
				}
			} else {
				// No Balance column: seed from opening balance, then immediately apply this row's
				// own amounts so the stored balance represents the state AFTER this transaction.
				cumulative = openingBalance
				if deposit.Valid {
					cumulative += deposit.Float64
				}
				if withdrawal.Valid {
					cumulative -= withdrawal.Float64
				}
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

		// --- CATEGORY MATCHING ---
		matchedCategoryID := matchCategoryForTransaction(rules, description, withdrawal, deposit, sql.NullTime{Time: valueDate, Valid: !valueDate.IsZero()})
		if matchedCategoryID.Valid {
			categoryCount[matchedCategoryID.String]++
			if withdrawal.Valid {
				debitSum[matchedCategoryID.String] += withdrawal.Float64
			}
			if deposit.Valid {
				creditSum[matchedCategoryID.String] += deposit.Float64
			}
		} else {
			uncategorized = append(uncategorized, map[string]interface{}{
				"index":            rowNum,
				"tran_id":          tranID.String,
				"tran_date":        transactionDate,
				"transaction_date": transactionDate,
				"description":      description,
				"value_date":       valueDate,
				"amount":           map[string]interface{}{"withdrawal": withdrawal.Float64, "deposit": deposit.Float64},
				"balance":          balance.Float64,
			})
		}
		// Use a synthetic tran_id for every row to keep IDs consistent and sequential
		// across banks regardless of cheque/reference column quality.
		tranID = sql.NullString{Valid: true, String: buildSyntheticTranID(stmtBatchID, rowNum)}

		// Append TIME STAMP to description when present so intra-day order is auditable.
		finalDesc := description
		if rowTimeStr != "" {
			finalDesc = description + " [" + rowTimeStr + "]"
		}

		keptRows++
		transactions = append(transactions, BankStatementTransaction{
			AccountNumber:    accountNumber,
			TranID:           tranID,
			ValueDate:        valueDate,
			TransactionDate:  transactionDate,
			PostedDate:       sql.NullTime{Valid: !postedDate.IsZero(), Time: postedDate},
			Description:      finalDesc,
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
		// Closing balance should reflect the last valid transaction row's balance.
		// Do not hardcode description-based exclusions (e.g. "000551000101"),
		// because in ICICI statements that can be a legitimate final transaction.
		if balance.Valid && isFiniteNumber(balance.Float64) {
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
	logger.LogInfo("[BANK-UPLOAD-DEBUG] Parse summary: rows_after_header=%d kept=%d skipped_empty=%d skipped_non_txn=%d skipped_missing_dates=%d skipped_opening_balance=%d skipped_closing_balance=%d",
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
		logger.LogInfo("[BANK-UPLOAD-DEBUG] Recalculated balances starting from opening balance %.2f", openingBalance)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin db transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Preload existing transactions for this account so we can identify which
	// rows from the uploaded file are truly new vs already present.
	existingTxnKeys := make(map[string]bool)
	rowsExisting, err := tx.Query(ctx, `
			SELECT t.account_number, t.transaction_date, t.description, t.withdrawal_amount, t.deposit_amount
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements s ON s.bank_statement_id = t.bank_statement_id
			WHERE t.account_number = $1
			  AND COALESCE(s.is_deleted, false) = false
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

	var bankStatementID string
	if !isFiniteNumber(openingBalance) {
		logger.LogInfo("[BANK-UPLOAD-DEBUG] openingBalance became non-finite (%v); resetting to 0", openingBalance)
		openingBalance = 0
	}
	if !isFiniteNumber(closingBalance) {
		logger.LogInfo("[BANK-UPLOAD-DEBUG] closingBalance became non-finite (%v); resetting to 0", closingBalance)
		closingBalance = 0
	}
	logger.LogInfo("[BANK-UPLOAD-DEBUG] Inserting bank_statements: account=%q period=%s→%s openingBalance=%.6f closingBalance=%.6f",
		accountNumber, statementPeriodStart.Format(constants.DateFormat), statementPeriodEnd.Format(constants.DateFormat), openingBalance, closingBalance)
	err = tx.QueryRow(ctx, `
		     INSERT INTO cimplrcorpsaas.bank_statements (
    entity_id,
    account_number,
    statement_period_start,
    statement_period_end,
    file_hash,
    opening_balance,
    closing_balance,
    upload_s3_key
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)

RETURNING bank_statement_id
		  `, entityID, accountNumber, statementPeriodStart, statementPeriodEnd, fileHash, openingBalance, closingBalance, uploadS3Key).Scan(&bankStatementID)
	if err != nil {
		tx.Rollback(ctx)
		// Detect uniq_stmt (entity+account+period composite) constraint violation → clear user error
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" && pgErr.ConstraintName == "uniq_stmt" {
			return nil, fmt.Errorf("%w: account %s already has a statement for this period", ErrStatementPeriodExists, accountNumber)
		}
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
				// normalize dates to RFC3339 strings
				vDate := ""
				if !t.ValueDate.IsZero() {
					vDate = t.ValueDate.Format(time.RFC3339)
				}
				trxDate := ""
				if !t.TransactionDate.IsZero() {
					trxDate = t.TransactionDate.Format(time.RFC3339)
				}
				reviewTransactions = append(reviewTransactions, map[string]interface{}{
					"account_number":    t.AccountNumber,
					"tran_id":           t.TranID.String,
					"value_date":        vDate,
					"transaction_date":  trxDate,
					"description":       t.Description,
					"withdrawal_amount": t.WithdrawalAmount.Float64,
					"deposit_amount":    t.DepositAmount.Float64,
					"balance":           t.Balance.Float64,
					"category_id":       t.CategoryID.String,
				})
				continue
			}
			// Not present in DB -> treat as a new transaction to insert.
			newTransactions = append(newTransactions, t)
		}

		existingInDBCount := len(reviewTransactions)
		newInsertCount := len(newTransactions)
		logger.LogInfo("[BANK-UPLOAD-DEBUG] Dedup summary: parsed=%d existing_in_db=%d new_to_insert=%d", len(transactions), existingInDBCount, newInsertCount)

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
			logger.LogInfo("[BANK-UPLOAD-DEBUG] Attempting to insert %d new transactions", len(newTransactions))
			if _, err := tx.Exec(ctx, stmt, valueArgs...); err != nil {
				tx.Rollback(ctx)
				logger.LogInfo("[BANK-UPLOAD-DEBUG] Bulk insert FAILED. First 5 transactions with amounts:")
				for i := 0; i < 5 && i < len(newTransactions); i++ {
					t := newTransactions[i]
					logger.LogInfo("[BANK-UPLOAD-DEBUG]   txn[%d] date=%s desc=%q withdrawal=%.4f deposit=%.4f balance=%.4f",
						i, t.TransactionDate.Format(constants.DateFormat), t.Description, t.WithdrawalAmount.Float64, t.DepositAmount.Float64, t.Balance.Float64)
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
	_, err = tx.Exec(ctx, `
			       INSERT INTO cimplrcorpsaas.auditactionbankstatement (
				       bankstatementid, actiontype, processing_status, requested_by, requested_at, requested_ip
			       ) VALUES ($1, $2, $3, $4, $5, $6)
		       `, bankStatementID, "CREATE", constants.StatusPendingApproval, requestedBy, time.Now().UTC(), nullIfBlank(opts.RequestedIP))
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf(constants.ErrFailedToInsertAuditAction, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	// M7: Trigger full smart-categorization waterfall after commit so newly
	// uploaded transactions are classified via the 10-step engine (rules,
	// counterparty defaults, corrections, similarity, account defaults, AI).
	if opts.PgxPool != nil {
		pool := opts.PgxPool
		bsID := bankStatementID // capture for goroutine
		go func() {
			if err := cashjobs.ProcessUncategorizedTransactions(pool, 500, bsID); err != nil {
				logger.LogInfo("[SMART-CAT] post-upload trigger failed: %v", err)
			}
		}()
	}

	// KPIs and category details
	kpiCats := []map[string]interface{}{}
	foundCategories := []map[string]interface{}{}
	foundCategoryIDs := map[string]bool{}
	totalTxns := len(transactions)
	groupedTxns := 0
	ungroupedTxns := 0

	categoryTxns := map[string][]map[string]interface{}{}

	for i, t := range transactions {
		if t.CategoryID.Valid {
			groupedTxns++
			catID := t.CategoryID.String
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
	// Prepare statement date coverage as RFC3339 strings when possible
	startStr := ""
	endStr := ""
	if len(transactions) > 0 && !transactions[0].ValueDate.IsZero() {
		startStr = transactions[0].ValueDate.Format(time.RFC3339)
	}
	if !statementPeriodEnd.IsZero() {
		endStr = statementPeriodEnd.Format(time.RFC3339)
	}

	result := map[string]interface{}{
		"pages_processed":                 1, // Excel = 1 sheet
		"bank_wise_status":                []map[string]interface{}{{"account_number": accountNumber, "status": "SUCCESS"}},
		"statement_date_coverage":         map[string]interface{}{"start": startStr, "end": endStr},
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
		"file_storage_key":                uploadS3Key.String,
		"file_storage_url":                s3URL,
	}
	return result, nil
}

// parseFileToRows converts raw file bytes (CSV, XLSX, or XLS) into a 2-D string slice.
// All three formats are normalised to the same [][]string output so callers do not need
// to know which format was uploaded. Returns an error when the content cannot be parsed
// as any of the three supported types.
func parseFileToRows(fileBytes []byte) ([][]string, error) {
	// 0. If Apple Numbers file, extract embedded XLSX first
	if extracted := tryExtractNumbersAsXLSX(fileBytes); extracted != nil {
		logger.LogInfo("[BANK-PARSE] parseFileToRows: .numbers detected — using embedded XLSX")
		fileBytes = extracted
	}

	// 1. Try XLSX / modern Excel first (excelize handles .xlsx and some .xlsm)
	if xl, xlErr := excelize.OpenReader(bytes.NewReader(fileBytes)); xlErr == nil {
		defer xl.Close()
		sheetName := xl.GetSheetName(0)
		rawRows, err := xl.GetRows(sheetName)
		if err != nil {
			return nil, fmt.Errorf("failed to read xlsx sheet: %w", err)
		}
		if len(rawRows) == 0 {
			return nil, errors.New("xlsx file contains no rows")
		}
		// Re-read via GetCellValue so formulas are evaluated
		out := make([][]string, len(rawRows))
		for i, rawRow := range rawRows {
			out[i] = make([]string, len(rawRow))
			for j := range rawRow {
				colName, _ := excelize.ColumnNumberToName(j + 1)
				cellRef := fmt.Sprintf("%s%d", colName, i+1)
				if v, cErr := xl.GetCellValue(sheetName, cellRef); cErr == nil && v != "" {
					out[i][j] = v
				} else {
					out[i][j] = rawRow[j]
				}
			}
		}
		return out, nil
	}

	// 2. Try legacy XLS (BIFF8 / Excel 97-2003) via shakinm/xlsReader
	tmpXlsFile, tmpErr := os.CreateTemp("", "bankstmt-multi-*.xls")
	if tmpErr == nil {
		defer os.Remove(tmpXlsFile.Name())
		defer tmpXlsFile.Close()
		if _, wErr := tmpXlsFile.Write(fileBytes); wErr == nil {
			tmpXlsFile.Close()
			if xlsBook, xlsErr := xls.OpenFile(tmpXlsFile.Name()); xlsErr == nil {
				if sheet, sheetErr := xlsBook.GetSheet(0); sheetErr == nil && sheet != nil {
					var out [][]string
					for _, xlsRow := range sheet.GetRows() {
						var rowVals []string
						for _, col := range xlsRow.GetCols() {
							rowVals = append(rowVals, col.GetString())
						}
						out = append(out, rowVals)
					}
					if len(out) > 0 {
						return out, nil
					}
				}
			}
		}
	}

	// 3. Fall back to CSV / TSV
	out, err := parseDelimitedTextRows(fileBytes)
	if err != nil {
		return nil, fmt.Errorf("unable to parse file as xlsx, xls, or csv: %w", err)
	}
	if len(out) == 0 {
		return nil, errors.New("file contains no rows")
	}
	return out, nil
}

// UploadMultiAccountBankStatementHandler parses a single CSV file containing rows
// for multiple accounts, groups rows by account_number and invokes the existing
// categorization pipeline for each account independently.
//
// Behaviour:
//   - Expects a multipart form upload with a single CSV file field (commonly `file`).
//   - Requires `multi=true` form field to be set; UploadBankStatementV2Handler will
//     delegate to this handler when present.
//   - Parses CSV (robust to header name variations), groups rows by account number,
//     builds a per-account CSV, computes idempotent `fileHash = sha256(csvBytes + account_number)`,
//     and calls UploadBankStatementV2WithCategorization for each account.
//   - Aggregates results per-account and isolates failures to the account level.
func UploadMultiAccountBankStatementHandler(pool *pgxpool.Pool) http.Handler {
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
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "File not found in request. Please attach a CSV, XLSX, or XLS file using the 'file' field in form-data."})
			return
		}
		defer file.Close()

		// Read file bytes
		fileBytes, err := io.ReadAll(file)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Failed to read the uploaded file. Please try again."})
			return
		}

		// Parse CSV / XLSX / XLS → uniform [][]string
		rows, parseErr := parseFileToRows(fileBytes)
		if parseErr != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Unable to parse file. Please upload a valid CSV, XLSX, or XLS file."})
			return
		}
		if len(rows) < 1 {
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "File contains no rows."})
			return
		}

		// Auto-detect the actual column-header row by scoring the first 30 rows.
		// Banks like Axis Bank embed metadata (name, address, customer ID, etc.) above
		// the real column header row. We pick the row with the most matches against
		// a set of known column keywords; that row becomes `header` and only rows
		// after it are treated as data.
		headerRowIdx := 0
		{
			knownCols := []string{
				"date", "debit", "credit", "balance", "particulars", "description",
				"narrative", "narration", "withdrawal", "deposit", "amount", "cheque",
				"account", "value", "remarks", "branch", "transaction",
			}
			bestScore := 0
			limit := 30
			if limit > len(rows) {
				limit = len(rows)
			}
			for ri := 0; ri < limit; ri++ {
				score := 0
				for _, cell := range rows[ri] {
					lc := strings.ToLower(strings.TrimSpace(cell))
					for _, kw := range knownCols {
						if strings.Contains(lc, kw) {
							score++
							break
						}
					}
				}
				if score > bestScore {
					bestScore = score
					headerRowIdx = ri
				}
			}
		}
		header := make([]string, len(rows[headerRowIdx]))
		copy(header, rows[headerRowIdx])
		if headerRowIdx+1 < len(rows) {
			for j := 0; j < len(header) && j < len(rows[headerRowIdx+1]); j++ {
				if strings.TrimSpace(rows[headerRowIdx+1][j]) != "" {
					header[j] = strings.TrimSpace(header[j]) + " " + strings.TrimSpace(rows[headerRowIdx+1][j])
				}
			}
		}
		// Data rows are everything after the detected header row.
		dataRows := rows[headerRowIdx+1:]

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
		// findIdxExclude finds the first header that contains one of the keywords
		// but does NOT contain any of the excludeWords. Used to disambiguate columns
		// like "Account number" vs "Account name" when searching for "account".
		findIdxExclude := func(excludeWords []string, keywords ...string) int {
			for i, h := range header {
				lc := strings.ToLower(strings.TrimSpace(h))
				excluded := false
				for _, ex := range excludeWords {
					if strings.Contains(lc, strings.ToLower(ex)) {
						excluded = true
						break
					}
				}
				if excluded {
					continue
				}
				for _, kw := range keywords {
					if strings.Contains(lc, strings.ToLower(kw)) {
						return i
					}
				}
			}
			return -1
		}

		// Account number column: prefer explicit "account number" / "account_no" / "account no",
		// but fall back to any "account" column that does NOT also contain "name".
		accIdx := findIdx("account number", "account_no", "account no", "acct_no", "acct no")
		if accIdx == -1 {
			accIdx = findIdxExclude([]string{"name"}, "account")
		}
		// Account name column (optional — used for display / result key labeling)
		accNameIdx := findIdxExclude([]string{"number", "no", "_no"}, "account name", "account_name", "accountname")
		if accNameIdx == -1 {
			accNameIdx = findIdx("account name", "account_name", "accountname")
		}
		// Bank name column (optional)
		bankNameIdx := findIdx("bank name", "bank_name", "bankname")
		dateIdx := findIdx(constants.TransactionDate, "statement date", "post date", "date")
		valDateIdx := findIdx(constants.ValueDate, "value-date", "value date", "value")
		// Description: also match "narrative" (HSBC) and "particulars"
		descIdx := findIdx("transaction description", "description", "narrative", "narration", "particulars", "remarks")
		// prefer explicit Extra Information column when available
		extraInfoIdx := findIdx("extra information", "extra", "extra-info", "extra_info")
		// Debug: log the detected header row and all column cells
		logger.LogInfo("[MULTI-UPLOAD] headerRowIdx=%d, header cells: %v", headerRowIdx, header)

		// Debit / credit: search for explicit column names first to avoid matching
		// "Closing ledger balance" etc. that happen to contain "credit"/"debit".
		debitIdx := findIdx("debit amount", "debit_amount", "withdrawal amount", "withdrawal_amount", "debit", "amount subtracted")
		creditIdx := findIdx("credit amount", "credit_amount", "deposit amount", "deposit_amount", "amount added")
		if creditIdx == -1 {
			// Only fall back to "credit" / "deposit" when no explicit column found
			creditIdx = findIdxExclude([]string{"ledger", "available", "brought", "closing", "opening", "current"}, "credit", "deposit")
		}
		logger.LogInfo("[MULTI-UPLOAD] accIdx=%d dateIdx=%d descIdx=%d debitIdx=%d creditIdx=%d balanceIdx(pre)=%d", accIdx, dateIdx, descIdx, debitIdx, creditIdx, -1)
		openingIdx := findIdx("opening balance", "opening available balance", "opening available", "opening")
		// Per-row running balance column: prefer an exact "balance" column over "closing balance" etc.
		// Strategy: first try to find a header whose trimmed lowercase is exactly "balance",
		// then fall back to columns containing "balance" but not "closing" or "opening".
		balanceIdx := -1
		for i, h := range header {
			if strings.ToLower(strings.TrimSpace(h)) == "balance" {
				balanceIdx = i
				break
			}
		}
		if balanceIdx == -1 {
			balanceIdx = findIdxExclude([]string{"closing", "opening", "ledger", "available", "brought", "current"}, "balance")
		}
		// closingIdx is used for the overall statement closing balance (header row value, not per-row)
		closingIdx := findIdx("closing available balance", "closing ledger balance", "closing balance", "closing available", "closing")

		// When there is no dedicated account-number column (single-account format like Axis Bank),
		// scan all rows for a long digit run that looks like an account number.
		// The file will be treated as one group under that number.
		singleAccNum := ""
		if accIdx == -1 {
			for _, row := range rows {
				for _, cell := range row {
					if candidate := extractAccountFromCell(cell); candidate != "" {
						singleAccNum = candidate
						break
					}
					// Also try the regex directly on the raw cell (catches "Statement of Account No - 923030007788535")
					if m := acctNumberRe.FindString(cell); m != "" && len(m) >= 9 {
						singleAccNum = m
						break
					}
				}
				if singleAccNum != "" {
					break
				}
			}
		}

		// Group rows by account number; also track account name and bank name per group for display.
		groups := make(map[string][][]string)
		groupAccName := make(map[string]string)  // accountNumber → account name (display label)
		groupBankName := make(map[string]string) // accountNumber → bank name
		for _, row := range dataRows {
			var accNum string
			if accIdx == -1 {
				// Single-account format: use the number extracted from header rows.
				// Only include rows that look like data rows (have a date or debit/credit value).
				hasData := false
				if dateIdx >= 0 && dateIdx < len(row) && strings.TrimSpace(row[dateIdx]) != "" {
					hasData = true
				}
				if !hasData && debitIdx >= 0 && debitIdx < len(row) && strings.TrimSpace(row[debitIdx]) != "" {
					hasData = true
				}
				if !hasData && creditIdx >= 0 && creditIdx < len(row) && strings.TrimSpace(row[creditIdx]) != "" {
					hasData = true
				}
				if !hasData {
					continue
				}
				accNum = singleAccNum
			} else {
				// guard length
				if accIdx >= len(row) {
					continue
				}
				accNum = strings.TrimSpace(row[accIdx])
			}
			if accNum == "" {
				continue
			}
			groups[accNum] = append(groups[accNum], row)
			// capture account name and bank name from first row of each group
			if _, seen := groupAccName[accNum]; !seen {
				if accNameIdx >= 0 && accNameIdx < len(row) {
					groupAccName[accNum] = strings.TrimSpace(row[accNameIdx])
				}
				if bankNameIdx >= 0 && bankNameIdx < len(row) {
					groupBankName[accNum] = strings.TrimSpace(row[bankNameIdx])
				}
			}
		}

		ctx := r.Context()
		results := make(map[string]interface{})

		// Process each account independently
		for account, grecs := range groups {
			// Use account name as result key when available (more human-readable), else account number.
			resultKey := account
			if name := groupAccName[account]; name != "" {
				resultKey = name + " (" + account + ")"
			}
			// Build structured input per-account by normalizing rows into transactions.
			stm := StructuredBankStatement{
				AccountNumber: account, // always the actual account number for DB lookup
				BankName:      groupBankName[account],
				Transactions:  []StructuredTransaction{},
			}

			var rowsTxns []struct {
				dt          time.Time
				valueDate   time.Time
				desc        string
				withdrawal  float64
				deposit     float64
				rawClose    string
				rowBal      float64
				rowBalValid bool
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
				if len(stm.Transactions) == 0 {
					// Log first row details to debug column index resolution
					logger.LogInfo("[MULTI-UPLOAD] FIRST ROW: debitIdx=%d debitStr=%q creditIdx=%d creditStr=%q balanceIdx=%d row=%v", debitIdx, debitStr, creditIdx, creditStr, balanceIdx, rrow)
				}
				// Per-row balance: prefer dedicated balanceIdx; fall back to closingIdx
				rowBalStr := ""
				if balanceIdx >= 0 {
					rowBalStr = cleanAmount(get(balanceIdx))
				}
				if rowBalStr == "" && closingIdx >= 0 {
					rowBalStr = cleanAmount(get(closingIdx))
				}

				var w, d float64
				if debitStr != "" {
					if v, pErr := strconv.ParseFloat(debitStr, 64); pErr == nil {
						// Some banks (e.g. HSBC) store debits as negative in the debit column
						if v < 0 {
							v = -v
						}
						w = v
					}
				}
				if creditStr != "" {
					if v, pErr := strconv.ParseFloat(creditStr, 64); pErr == nil {
						if v < 0 {
							v = -v
						}
						d = v
					}
				}

				// Parse the per-row running balance when available
				var rowBal float64
				rowBalValid := false
				if rowBalStr != "" {
					if v, pErr := strconv.ParseFloat(rowBalStr, 64); pErr == nil {
						rowBal = v
						rowBalValid = true
					}
				}

				rowsTxns = append(rowsTxns, struct {
					dt          time.Time
					valueDate   time.Time
					desc        string
					withdrawal  float64
					deposit     float64
					rawClose    string
					rowBal      float64
					rowBalValid bool
				}{dt: dt, valueDate: dt, desc: descCell, withdrawal: w, deposit: d, rawClose: rowBalStr, rowBal: rowBal, rowBalValid: rowBalValid})

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
				// closing balance: capture last non-empty per-row balance (last row = statement closing balance)
				if rowBalValid {
					closingBalance = rowBal
				}
			}

			if len(rowsTxns) == 0 {
				results[resultKey] = map[string]interface{}{"success": false, "message": "No parseable transactions for account"}
				continue
			}

			// Build structured transactions: use per-row balance when available (more accurate),
			// otherwise compute a running balance from opening.
			sort.Slice(rowsTxns, func(i, j int) bool { return rowsTxns[i].dt.Before(rowsTxns[j].dt) })
			useRowBal := len(rowsTxns) > 0 && rowsTxns[0].rowBalValid
			running := 0.0
			if !useRowBal {
				if openingFound {
					running = openingBalance
				}
			}

			// Build structured transactions with cumulative balance
			for _, rt := range rowsTxns {
				var bal float64
				if rt.rowBalValid {
					bal = rt.rowBal
				} else {
					running = running - rt.withdrawal + rt.deposit
					bal = running
				}
				txn := StructuredTransaction{
					TransactionDate: rt.dt.Format(constants.DateFormat),
					ValueDate:       rt.valueDate.Format(constants.DateFormat),
					Description:     rt.desc,
					Withdrawal:      rt.withdrawal,
					Deposit:         rt.deposit,
					Balance:         bal,
				}
				stm.Transactions = append(stm.Transactions, txn)
			}

			// finalize period and balances
			stm.PeriodStart = minDate.Format(constants.DateFormat)
			stm.PeriodEnd = maxDate.Format(constants.DateFormat)
			if openingFound {
				stm.OpeningBalance = openingBalance
			} else if len(stm.Transactions) > 0 {
				first := stm.Transactions[0]
				if IsStatementOpeningCarryRow(first.Description) {
					// BBF row: its balance IS the statement opening balance — remove from transactions
					stm.OpeningBalance = first.Balance
					stm.Transactions = stm.Transactions[1:]
				} else {
					stm.OpeningBalance = first.Balance + first.Withdrawal - first.Deposit
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
			res, upErr := ProcessBankStatementFromStructuredInput(ctx, pool, stm, fileHash, "")
			if upErr != nil {
				results[resultKey] = map[string]interface{}{"success": false, "message": userFriendlyUploadError(upErr)}
				continue
			}
			results[resultKey] = map[string]interface{}{"success": true, "data": res}
		}

		// Return aggregated results
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    results,
		})
	})
}

// 1. Get all bank statements (POST, req: user_id)
