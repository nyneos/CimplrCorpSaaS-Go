package bankstatement

import (
	"CimplrCorpSaas/api/constants"
	cashjobs "CimplrCorpSaas/internal/jobs/cash"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// ProcessBankStatementFromJSON reads bank.json and processes it like Excel upload.
// This is a placeholder for future PDF OCR integration.
func ProcessBankStatementFromJSON(ctx context.Context, db *sql.DB) (map[string]interface{}, error) {
	logger.LogInfo("[PDF_PROCESSOR] Reading bank.json file")

	possiblePaths := []string{
		"api/cash/bankstatement/bank.json",
		"../api/cash/bankstatement/bank.json",
		filepath.Join("..", "api", "cash", "bankstatement", "bank.json"),
	}

	var jsonBytes []byte
	var err error
	var foundPath string

	for _, jsonPath := range possiblePaths {
		jsonBytes, err = os.ReadFile(jsonPath)
		if err == nil {
			foundPath = jsonPath
			logger.LogInfo("[PDF_PROCESSOR] Found bank.json at: %s", foundPath)
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

	logger.LogInfo("[PDF_PROCESSOR] Parsed JSON: Account=%s, Bank=%s, Period=%s to %s, Transactions=%d",
		pdfData.AccountNumber, pdfData.BankName, pdfData.PeriodStart, pdfData.PeriodEnd, len(pdfData.Transactions))

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
	return ProcessBankStatementFromStructuredInput(ctx, db, structured, fileHash, nil)
}

// ProcessBankStatementFromStructuredInput performs ingestion, categorization,
// dedup and KPI computation for structured input (no file I/O or header detection).
// pgxPool is optional: when non-nil, the smart categorization engine is triggered
// asynchronously after the commit so that uploads use the full 10-step waterfall.
func ProcessBankStatementFromStructuredInput(ctx context.Context, db *sql.DB, input StructuredBankStatement, fileHash string, pgxPool *pgxpool.Pool) (map[string]interface{}, error) {
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
	_ = accountName // used for logging if needed

	periodStart, err := time.Parse(constants.DateFormat, input.PeriodStart)
	if err != nil {
		return nil, fmt.Errorf("invalid period_start date: %w", err)
	}
	periodEnd, err := time.Parse(constants.DateFormat, input.PeriodEnd)
	if err != nil {
		return nil, fmt.Errorf("invalid period_end date: %w", err)
	}

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

	transactions := []BankStatementTransaction{}
	categoryCount := map[string]int{}
	debitSum := map[string]float64{}
	creditSum := map[string]float64{}
	uncategorized := []map[string]interface{}{}
	categoryTxns := map[string][]map[string]interface{}{}

	for i, txn := range input.Transactions {
		txnDate, err := time.Parse(constants.DateFormat, txn.TransactionDate)
		if err != nil {
			logger.LogError("[STRUCTURED_PROCESSOR] Warning: failed to parse transaction_date %s, skipping", txn.TransactionDate)
			continue
		}
		valDate, err := time.Parse(constants.DateFormat, txn.ValueDate)
		if err != nil {
			logger.LogError("[STRUCTURED_PROCESSOR] Warning: failed to parse value_date %s, skipping", txn.ValueDate)
			continue
		}
		if IsStatementOpeningCarryRow(txn.Description) {
			continue
		}

		var withdrawal, deposit sql.NullFloat64
		if txn.Withdrawal > 0 {
			withdrawal = sql.NullFloat64{Float64: txn.Withdrawal, Valid: true}
		}
		if txn.Deposit > 0 {
			deposit = sql.NullFloat64{Float64: txn.Deposit, Valid: true}
		}

		categoryID := matchCategoryForTransaction(rules, txn.Description, withdrawal, deposit, sql.NullTime{Time: valDate, Valid: true})

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

		if categoryID.Valid {
			catID := categoryID.String
			categoryCount[catID]++
			if withdrawal.Valid {
				debitSum[catID] += withdrawal.Float64
			}
			if deposit.Valid {
				creditSum[catID] += deposit.Float64
			}
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
			uncategorized = append(uncategorized, map[string]interface{}{
				"index":            i,
				"tran_id":          t.TranID.String,
				"description":      t.Description,
				"transaction_date": t.TransactionDate,
				"value_date":       t.ValueDate,
				"amount":           map[string]interface{}{"withdrawal": t.WithdrawalAmount.Float64, "deposit": t.DepositAmount.Float64},
				"balance":          t.Balance.Float64,
			})
		}
	}

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

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var bankStatementID string
	err = tx.QueryRowContext(ctx, `
		INSERT INTO cimplrcorpsaas.bank_statements (
			entity_id, account_number, statement_period_start, statement_period_end,
			file_hash, opening_balance, closing_balance, upload_s3_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT ON CONSTRAINT uniq_stmt
		DO UPDATE SET file_hash = EXCLUDED.file_hash, closing_balance = EXCLUDED.closing_balance, upload_s3_key = EXCLUDED.upload_s3_key, upload_link = NULL
		RETURNING bank_statement_id
	`, entityID, input.AccountNumber, periodStart, periodEnd, fileHash,
		input.OpeningBalance, input.ClosingBalance, nil).Scan(&bankStatementID)
	if err != nil {
		return nil, fmt.Errorf(constants.ErrFailedToInsertBankStatement, err)
	}

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

	if len(newTxns) == 0 && len(reviewTxns) > 0 {
		return nil, ErrAllTransactionsDuplicate
	}

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

	// M7: After persisting new transactions, trigger the smart categorization
	// engine so the upload path uses the full 10-step waterfall instead of the
	// legacy matchCategoryForTransaction engine.
	if pgxPool != nil && len(newTxns) > 0 {
		go func(bsID string) {
			if err := cashjobs.ProcessUncategorizedTransactions(pgxPool, 500); err != nil {
				log.Printf("[SMART-CAT] post-upload categorization error (bs_id=%s): %v", bsID, err)
			}
		}(bankStatementID)
	}

	kpiCats := []map[string]interface{}{}
	foundCategories := []map[string]interface{}{}
	foundCategoryIDs := map[string]bool{}
	totalTxns := len(transactions)
	groupedTxns := 0
	for catID, count := range categoryCount {
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
