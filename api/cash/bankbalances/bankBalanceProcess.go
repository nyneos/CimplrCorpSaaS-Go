package bankbalances

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"mime/multipart"
	"strconv"
	"strings"
	"time"

	s3storage "CimplrCorpSaas/api/utils/s3storage"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrFileAlreadyUploaded = errors.New("bank balance file already uploaded")

const duplicateBankBalanceUploadMessage = "This bank balance file was already uploaded earlier. Please upload a different file."

func nullableString(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return &s
}

func existingBankBalanceUploadKeys(ctx context.Context, pgxPool *pgxpool.Pool) ([]string, error) {
	rows, err := pgxPool.Query(ctx, `
		SELECT DISTINCT upload_s3_key
		FROM bank_balances_manual
		WHERE COALESCE(TRIM(upload_s3_key), '') <> ''
		  AND COALESCE(is_deleted, false) = false
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make([]string, 0)
	for rows.Next() {
		var key string
		if scanErr := rows.Scan(&key); scanErr != nil {
			return nil, scanErr
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func ensureUniqueBankBalanceUpload(ctx context.Context, pgxPool *pgxpool.Pool, fileBytes []byte) error {
	keys, err := existingBankBalanceUploadKeys(ctx, pgxPool)
	if err != nil {
		return fmt.Errorf("failed to check duplicate bank balance upload: %w", err)
	}
	duplicateKey, err := s3storage.FindDuplicateObjectKey(ctx, fileBytes, keys)
	if err != nil {
		return fmt.Errorf("failed to compare bank balance upload content: %w", err)
	}
	if duplicateKey != "" {
		return ErrFileAlreadyUploaded
	}
	return nil
}

func UploadBankBalancesProcess(
	ctx context.Context,
	pgxPool *pgxpool.Pool,
	fileHeader *multipart.FileHeader,
	userName string,
	entityIDs []string,
	requestedIP string,
) (string, error) {

	// 1. Open file
	file, err := fileHeader.Open()
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// 2. Read bytes for S3 + hash
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("failed to read file: %w", err)
	}
	if s3storage.IsS3UploadEnabled() {
		if err := ensureUniqueBankBalanceUpload(ctx, pgxPool, fileBytes); err != nil {
			return "", err
		}
	}
	contentType := s3storage.DetectContentType(fileBytes)

	// 3. Parse file
	ext := ubGetFileExt(fileHeader.Filename)
	records, err := ubParseUploadFile(bytes.NewReader(fileBytes), ext)
	if err != nil || len(records) < 2 {
		return "", fmt.Errorf("failed to parse file: %w", err)
	}

	// 4. Normalize headers
	headerRow := records[0]
	colCount := len(headerRow)
	headersNorm := make([]string, colCount)
	for i, h := range headerRow {
		hn := strings.TrimSpace(h)
		hn = strings.Trim(hn, ", \t\n\r")
		hn = strings.Trim(hn, "'\"`")
		hn = strings.ToLower(hn)
		hn = strings.ReplaceAll(hn, " ", "_")
		headersNorm[i] = hn
	}

	// 5. Prepare rows
	dataRows := records[1:]
	batchID := uuid.New().String()
	copyRows := make([][]interface{}, len(dataRows))
	for i, row := range dataRows {
		vals := make([]interface{}, 0, colCount+1)
		vals = append(vals, batchID)
		numericFields := map[string]bool{
			"balance_amount":  true,
			"opening_balance": true,
			"total_credits":   true,
			"total_debits":    true,
			"closing_balance": true,
		}
		for j := 0; j < colCount; j++ {
			cell := ""
			if j < len(row) {
				cell = strings.TrimSpace(row[j])
			}
			key := headersNorm[j]
			if key == "as_of_date" {
				cell = normalizeDate(cell)
			}
			if key == "as_of_time" {
				cell = normalizeTime(cell)
			}
			if key == "account_no" && cell != "" {
				if strings.ContainsAny(cell, "Ee") {
					if f, err := strconv.ParseFloat(cell, 64); err == nil {
						s := strconv.FormatFloat(f, 'f', -1, 64)
						s = strings.TrimSuffix(s, ".0")
						cell = s
					}
				}
			}
			if numericFields[key] {
				if cell == "" {
					vals = append(vals, nil)
				} else {
					clean := strings.ReplaceAll(cell, ",", "")
					if f, err := strconv.ParseFloat(clean, 64); err == nil {
						vals = append(vals, f)
					} else {
						vals = append(vals, cell)
					}
				}
			} else {
				if cell == "" {
					vals = append(vals, nil)
				} else {
					vals = append(vals, cell)
				}
			}
		}
		copyRows[i] = vals
	}

	fmt.Println("S3 ENABLED:", s3storage.IsS3UploadEnabled())

	// 6. Begin transaction for staging + validation + final insert.
	tx, err := pgxPool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to start db transaction: %w", err)
	}
	committed := false
	s3Key := ""
	s3Uploaded := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
			if s3Uploaded && s3Key != "" {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					fmt.Printf("[BANK-BALANCE-UPLOAD] cleanup failed for key=%s: %v\n", s3Key, cleanupErr)
				}
			}
		}
	}()

	// 7. S3 upload must succeed before any DB write is persisted for this batch.
	var s3URL string
	folder := s3storage.GetStoragePrefix("bankbalance")
	storedFileName := s3storage.BuildUploadedFilename(fileHeader.Filename, userName, time.Now().UTC())
	s3Key = s3storage.BuildNamedS3Key(folder, "", storedFileName)
	if s3storage.IsS3UploadEnabled() {
		if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
			fmt.Printf("[BANK-BALANCE-UPLOAD] S3 upload failed: %v", err)
			return "", fmt.Errorf("failed to upload file to s3: %w", err)
		}
		s3Uploaded = true
		if s3URL, err = s3storage.GetDownloadPresignedURL(ctx, s3Key, 0); err != nil {
			s3URL = ""
		}
		fmt.Printf("[BANK-BALANCE-UPLOAD] S3 upload success key=%s url=%s", s3Key, s3URL)
	} else {
		fmt.Printf("[BANK-BALANCE-UPLOAD] S3 upload disabled, skipping for batch=%s", batchID)
	}

	// 8. Staging insert stays inside the same transaction.
	columns := append([]string{"upload_batch_id"}, headersNorm...)
	if _, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"input_bank_balance_table"},
		columns,
		pgx.CopyFromRows(copyRows),
	); err != nil {
		return "", fmt.Errorf("failed to stage rows: %w", err)
	}

	// 9. Read column mapping - correct column names from upload_mapping_bank_balance
	mapRows, err := tx.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_bank_balance`)
	if err != nil {
		return "", fmt.Errorf("mapping read error: %w", err)
	}
	mapping := make(map[string]string)
	for mapRows.Next() {
		var src, tgt string
		if err := mapRows.Scan(&src, &tgt); err == nil {
			key := strings.TrimSpace(strings.ToLower(src))
			key = strings.ReplaceAll(key, " ", "_")
			mapping[key] = tgt
		}
	}
	mapRows.Close()

	// TEMP DEBUG - remove after testing
	fmt.Printf("[BANK-BALANCE-DEBUG] headersNorm=%v", headersNorm)
	fmt.Printf("[BANK-BALANCE-DEBUG] mapping=%v", mapping)

	var selectExprs []string
	var tgtCols []string
	for _, hn := range headersNorm {
		if tgt, ok := mapping[hn]; ok && tgt != "" {
			selectExprs = append(selectExprs, fmt.Sprintf("s.%s AS %s", hn, tgt))
			tgtCols = append(tgtCols, tgt)
		}
	}
	if len(tgtCols) == 0 || len(selectExprs) == 0 {
		return "", fmt.Errorf("no mapped columns found for upload file")
	}

	// 10. Count staged rows (identical to original)
	var stagedCount int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM input_bank_balance_table WHERE upload_batch_id = $1`, batchID).Scan(&stagedCount); err != nil {
		return "", fmt.Errorf("failed to count staged rows: %w", err)
	}
	if stagedCount == 0 {
		return "", fmt.Errorf("no staged rows found for batch %s", batchID)
	}

	// 11. Validate bank/account/currency (identical to original)
	var bankExpr, acctExpr, currencyExpr string
	for i, c := range tgtCols {
		switch c {
		case "bank_name":
			bankExpr = selectExprs[i]
		case "account_no":
			acctExpr = selectExprs[i]
		case "currency_code":
			currencyExpr = selectExprs[i]
		}
	}
	if bankExpr != "" || acctExpr != "" || currencyExpr != "" {
		cols := make([]string, 0, 3)
		if bankExpr != "" {
			cols = append(cols, bankExpr)
		}
		if acctExpr != "" {
			cols = append(cols, acctExpr)
		}
		if currencyExpr != "" {
			cols = append(cols, currencyExpr)
		}
		valSQL := fmt.Sprintf(`SELECT DISTINCT %s FROM input_bank_balance_table s WHERE s.upload_batch_id = $1`, strings.Join(cols, ", "))
		// fmt.Println("VALIDATION SQL:", valSQL)
		vrows, verr := tx.Query(ctx, valSQL, batchID)
		if verr != nil {
			return "", fmt.Errorf("%s", pgUserFriendlyMessage(verr))
		}
		for vrows.Next() {
			var bankName, accountNo, currency *string
			if bankExpr != "" && acctExpr != "" && currencyExpr != "" {
				_ = vrows.Scan(&bankName, &accountNo, &currency)
			} else if bankExpr != "" && acctExpr != "" {
				_ = vrows.Scan(&bankName, &accountNo)
			} else if bankExpr != "" && currencyExpr != "" {
				_ = vrows.Scan(&bankName, &currency)
			} else if acctExpr != "" && currencyExpr != "" {
				_ = vrows.Scan(&accountNo, &currency)
			} else if bankExpr != "" {
				_ = vrows.Scan(&bankName)
			} else if acctExpr != "" {
				_ = vrows.Scan(&accountNo)
			} else {
				_ = vrows.Scan(&currency)
			}

			if bankName != nil {
				fmt.Println("BANK:", *bankName)
			}

			if accountNo != nil {
				fmt.Println("ACCOUNT:", *accountNo)
			}

			if currency != nil {
				fmt.Println("CURRENCY:", *currency)
			}
			if bankName != nil && strings.TrimSpace(*bankName) != "" && !ctxHasApprovedBankName(ctx, *bankName) {
				vrows.Close()
				return "", fmt.Errorf("invalid or inactive bank")
			}
			if accountNo != nil && strings.TrimSpace(*accountNo) != "" {
				if !ctxHasApprovedBankAccount(ctx, *accountNo) {
					vrows.Close()
					return "", fmt.Errorf("invalid account")
				}
				// if len(entityIDs) > 0 {
				// 	acct := strings.TrimSpace(*accountNo)
				// 	var mbaEntityID *string
				// 	queryErr := pgxPool.QueryRow(ctx, `SELECT entity_id FROM masterbankaccount WHERE account_number = $1 LIMIT 1`, acct).Scan(&mbaEntityID)
				// 	if queryErr == nil && mbaEntityID != nil {
				// 		isNameList := true
				// 		for _, e := range entityIDs {
				// 			ee := strings.TrimSpace(e)
				// 			if ee == "" {
				// 				continue
				// 			}
				// 			if strings.HasPrefix(strings.ToUpper(ee), "EC-") || strings.Contains(ee, "-") {
				// 				isNameList = false
				// 				break
				// 			}
				// 		}
				// 		if isNameList {
				// 			lowerNames := make([]string, 0, len(entityIDs))
				// 			for _, e := range entityIDs {
				// 				if s := strings.TrimSpace(e); s != "" {
				// 					lowerNames = append(lowerNames, strings.ToLower(s))
				// 				}
				// 			}
				// 			rowsEnt, err := pgxPool.Query(ctx, `SELECT entity_id FROM public.masterentity WHERE LOWER(entity_name) = ANY($1)`, lowerNames)
				// 			allowed := map[string]bool{}
				// 			if err == nil {
				// 				defer rowsEnt.Close()
				// 				for rowsEnt.Next() {
				// 					var id string
				// 					if err := rowsEnt.Scan(&id); err == nil {
				// 						allowed[id] = true
				// 					}
				// 				}
				// 			}
				// 			if !allowed[*mbaEntityID] {
				// 				vrows.Close()
				// 				return "", fmt.Errorf("account's entity not allowed")
				// 			}
				// 		} else {
				// 			allowed := false
				// 			for _, e := range entityIDs {
				// 				if strings.TrimSpace(e) == *mbaEntityID {
				// 					allowed = true
				// 					break
				// 				}
				// 			}
				// 			if !allowed {
				// 				vrows.Close()
				// 				return "", fmt.Errorf("account's entity not allowed")
				// 			}
				// 		}
				// 	} else {
				// 		vrows.Close()
				// 		return "", fmt.Errorf("invalid account")
				// 	}
				// }
			}
			if currency != nil && strings.TrimSpace(*currency) != "" && !ctxHasApprovedCurrency(ctx, *currency) {
				vrows.Close()
				return "", fmt.Errorf("invalid currency")
			}
		}
		vrows.Close()
	}

	// 12. Final insert with key-based storage for new rows.
	tgtColsStr := strings.Join(tgtCols, ", ")
	srcColsStr := strings.Join(selectExprs, ", ")
	insertSQL := fmt.Sprintf(`
		INSERT INTO bank_balances_manual (%s, upload_s3_key)
		SELECT %s, $2
		FROM input_bank_balance_table s
		WHERE s.upload_batch_id = $1
		RETURNING balance_id
	`, tgtColsStr, srcColsStr)
	// fmt.Println("TARGET COLS:", tgtColsStr)
	// fmt.Println("SOURCE COLS:", srcColsStr)
	// fmt.Println("FINAL SQL:", insertSQL)

	rows, err := tx.Query(ctx, insertSQL, batchID, nullableString(s3Key))
	if err != nil {
		return "", err
	}
	var insertedIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			insertedIDs = append(insertedIDs, id)
		}
	}
	rows.Close()
	if len(insertedIDs) == 0 {
		return "", fmt.Errorf("staged rows present for batch %s but no rows inserted into final table (staged=%d)", batchID, stagedCount)
	}

	// 13. Audit insert (identical to original)
	for _, bid := range insertedIDs {
		if _, err := tx.Exec(ctx, `INSERT INTO auditactionbankbalances (balance_id, actiontype, processing_status, requested_by, requested_at, requested_ip) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now(),$3)`, bid, userName, nullableString(requestedIP)); err != nil {
			return "", fmt.Errorf("failed to create audit action: %w", err)
		}
	}

	// 14. Commit
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit failed: %w", err)
	}
	committed = true

	fmt.Printf("[BANK-BALANCE-UPLOAD] batch=%s committed s3_key=%s inserted=%d", batchID, s3Key, len(insertedIDs))
	return batchID, nil
}
