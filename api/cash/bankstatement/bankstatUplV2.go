package bankstatement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// UploadBankStatementV2WithCategorization wraps UploadBankStatementV2 and adds category intelligence and KPIs to the response.
func UploadBankStatementV2WithCategorization(ctx context.Context, db *sql.DB, file multipart.File, fileHash string) (map[string]interface{}, error) {
	// We'll reuse most of the logic from UploadBankStatementV2, but add category rule matching and KPI collection.
	// 1. Idempotency: Check if file hash already exists
	var exists bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM cimplrcorpsaas.bank_statements WHERE file_hash = $1)`, fileHash).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check file hash: %w", err)
	}
	if exists {
		return nil, errors.New("this file has already been uploaded (idempotency check failed)")
	}

	// 2. Parse Excel file
	tmpFile, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	xl, err := excelize.OpenReader(bytes.NewReader(tmpFile))
	if err != nil {
		return nil, fmt.Errorf("failed to parse excel: %w", err)
	}
	defer xl.Close()

	sheetName := xl.GetSheetName(0)
	rows, err := xl.GetRows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}
	if len(rows) < 2 {
		return nil, errors.New("excel must have at least one data row")
	}

	// 3. Extract account number, entity id, and name from header
	var accountNumber, entityID, accountName string
	for i := 0; i < 20 && i < len(rows); i++ {
		for j, cell := range rows[i] {
			if cell == "A/C No:" && j+1 < len(rows[i]) {
				accountNumber = rows[i][j+1]
			}
			if cell == "Name:" && j+1 < len(rows[i]) {
				accountName = rows[i][j+1]
			}
		}
	}
	if accountNumber == "" {
		return nil, errors.New("account number not found in file header")
	}

	// Lookup entity_id and name from masterbankaccount
	err = db.QueryRowContext(ctx, `SELECT entity_id, COALESCE(account_nickname, bank_name) FROM public.masterbankaccount WHERE account_number = $1 AND is_deleted = false`, accountNumber).Scan(&entityID, &accountName)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup account in masterbankaccount: %w", err)
	}

	// 4. Fetch all active rules/components for this account/entity/bank/global
	type ruleComponent struct {
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
	// Get all rules/components for this account, entity, bank, or global (ordered by priority)
	rules := []ruleComponent{}
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
		       OR (s.scope_type = 'BANK' AND s.bank_code IS NOT NULL) -- TODO: add bank_code logic if needed
		       OR (s.scope_type = 'GLOBAL')
		 )
	       ORDER BY r.priority ASC, r.rule_id ASC, comp.component_id ASC
       `
	rowsRule, err := db.QueryContext(ctx, q, accountNumber, entityID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch category rules: %w", err)
	}
	for rowsRule.Next() {
		var rc ruleComponent
		err := rowsRule.Scan(&rc.RuleID, &rc.Priority, &rc.CategoryID, &rc.CategoryName, &rc.CategoryType, &rc.ComponentType, &rc.MatchType, &rc.MatchValue, &rc.AmountOperator, &rc.AmountValue, &rc.TxnFlow, &rc.CurrencyCode)
		if err == nil {
			rules = append(rules, rc)
		}
	}
	rowsRule.Close()

	// 5. Parse transactions, categorize, and collect KPIs
	// Find the header row for transactions
	var txnHeaderIdx int = -1
	for i, row := range rows {
		for _, cell := range row {
			if cell == "Tran. Id" || cell == "Tran Id" {
				txnHeaderIdx = i
				goto foundTxnHeader
			}
		}
	}
foundTxnHeader:
	if txnHeaderIdx == -1 {
		return nil, errors.New("transaction header row not found in Excel file")
	}
	headerRow := rows[txnHeaderIdx]
	colIdx := map[string]int{}
	for idx, col := range headerRow {
		colIdx[col] = idx
	}
	required := []string{"Tran. Id", "Value Date", "Transaction Date", "Transaction Remarks", "Withdrawal Amt (INR)", "Deposit Amt (INR)", "Balance (INR)"}
	for _, col := range required {
		if _, ok := colIdx[col]; !ok {
			return nil, fmt.Errorf("required column '%s' not found in header", col)
		}
	}

	// KPI maps
	categoryCount := map[int64]int{}
	debitSum := map[int64]float64{}
	creditSum := map[int64]float64{}
	uncategorized := []map[string]interface{}{}
	transactions := []BankStatementTransaction{}
	// var lastValidBalance sql.NullFloat64
	var lastValidValueDate time.Time
	for i, row := range rows[txnHeaderIdx+1:] {
		if len(row) == 0 || (colIdx["Tran. Id"] >= len(row)) || strings.TrimSpace(row[colIdx["Tran. Id"]]) == "" {
			continue
		}
		for len(row) < len(headerRow) {
			row = append(row, "")
		}
		tranID := sql.NullString{String: row[colIdx["Tran. Id"]], Valid: row[colIdx["Tran. Id"]] != ""}
		valueDate, _ := time.Parse("02/Jan/2006", row[colIdx["Value Date"]])
		transactionDate, _ := time.Parse("02/Jan/2006", row[colIdx["Transaction Date"]])
		description := row[colIdx["Transaction Remarks"]]
		var withdrawal, deposit sql.NullFloat64
		withdrawalStr := cleanAmount(row[colIdx["Withdrawal Amt (INR)"]])
		depositStr := cleanAmount(row[colIdx["Deposit Amt (INR)"]])
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
		balance := sql.NullFloat64{Valid: row[colIdx["Balance (INR)"]] != ""}
		if balance.Valid {
			balanceStr := cleanAmount(row[colIdx["Balance (INR)"]])
			fmt.Sscanf(balanceStr, "%f", &balance.Float64)
		}
		if i == 0 {
			// ...existing code...
		}
		if balance.Valid {
			lastValidValueDate = valueDate
		}
		rowJSON, _ := json.Marshal(row)

		// --- CATEGORY MATCHING ---
		matchedCategoryID := sql.NullInt64{Valid: false}
		for _, rule := range rules {
			// Only NARRATION_LOGIC for now, can expand
			if rule.ComponentType == "NARRATION_LOGIC" && rule.MatchType.Valid && rule.MatchValue.Valid {
				desc := strings.ToLower(description)
				val := strings.ToLower(rule.MatchValue.String)
				switch rule.MatchType.String {
				case "CONTAINS":
					if strings.Contains(desc, val) {
						matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
						break
					}
				case "EQUALS":
					if desc == val {
						matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
						break
					}
				case "STARTS_WITH":
					if strings.HasPrefix(desc, val) {
						matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
						break
					}
				case "ENDS_WITH":
					if strings.HasSuffix(desc, val) {
						matchedCategoryID = sql.NullInt64{Int64: rule.CategoryID, Valid: true}
						break
					}
				}
			}
			// TODO: Add more logic for AMOUNT, TXN_FLOW, etc.
			if matchedCategoryID.Valid {
				break
			}
		}
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
	}
	// closingBalance := lastValidBalance.Float64
	statementPeriodEnd := lastValidValueDate

	// 6. Insert bank statement and transactions in a transaction (from UploadBankStatementV2)
	// Calculate opening/closing balance, period start/end
	var openingBalance, closingBalance float64
	var statementPeriodStart time.Time
	if len(transactions) > 0 {
		openingBalance = transactions[0].Balance.Float64
		statementPeriodStart = transactions[0].ValueDate
		// Find last non-zero balance for closing balance
		closingBalance = 0
		for i := len(transactions) - 1; i >= 0; i-- {
			if transactions[i].Balance.Valid && transactions[i].Balance.Float64 != 0 {
				closingBalance = transactions[i].Balance.Float64
				break
			}
		}
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
		      RETURNING bank_statement_id
		  `, entityID, accountNumber, statementPeriodStart, statementPeriodEnd, fileHash, openingBalance, closingBalance).Scan(&bankStatementID)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to insert bank statement: %w", err)
	}

	// Upsert into public.bank_balances_manual for dashboard matching
	var bankName, currencyCode, nickname, country string
	err = tx.QueryRowContext(ctx, `
			       SELECT mb.bank_name, mba.currency, COALESCE(mba.account_nickname, mb.bank_name), mba.country
			       FROM public.masterbankaccount mba
			       JOIN public.masterbank mb ON mba.bank_id = mb.bank_id
			       WHERE mba.account_number = $1 AND mba.is_deleted = false
		       `, accountNumber).Scan(&bankName, &currencyCode, &nickname, &country)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("failed to lookup account info for balances_manual: %w", err)
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
		return nil, fmt.Errorf("failed to upsert bank_balances_manual: %w", err)
	}

	// Insert audit action for bank_balances_manual after upsert
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
		return nil, fmt.Errorf("failed to insert audit action for bank_balances_manual: %w", err)
	}

	// Bulk insert transactions for speed, skip duplicates
	if len(transactions) > 0 {
		valueStrings := make([]string, 0, len(transactions))
		valueArgs := make([]interface{}, 0, len(transactions)*11)
		for i, t := range transactions {
			valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11))
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
				t.CategoryID,
			)
		}
		stmt := `INSERT INTO cimplrcorpsaas.bank_statement_transactions (
					       bank_statement_id, account_number, tran_id, value_date, transaction_date, description, withdrawal_amount, deposit_amount, balance, raw_json, category_id
				       ) VALUES ` +
			joinStrings(valueStrings, ",") +
			` ON CONFLICT (account_number, tran_id, transaction_date) DO NOTHING`
		_, err := tx.ExecContext(ctx, stmt, valueArgs...)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to bulk insert transactions: %w", err)
		}
	}

	// Insert audit action for this bank statement
	_, err = tx.ExecContext(ctx, `
			       INSERT INTO cimplrcorpsaas.auditactionbankstatement (
				       bankstatementid, actiontype, processing_status, requested_by, requested_at
			       ) VALUES ($1, $2, $3, $4, $5)
		       `, bankStatementID, "CREATE", "PENDING_APPROVAL", "system", time.Now())
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
	for catID, count := range categoryCount {
		kpiCats = append(kpiCats, map[string]interface{}{
			"category_id": catID,
			"count":       count,
			"debit_sum":   debitSum[catID],
			"credit_sum":  creditSum[catID],
		})
		foundCategoryIDs[catID] = true
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
		"pages_processed":         1, // Excel = 1 sheet
		"bank_wise_status":        []map[string]interface{}{{"account_number": accountNumber, "status": "SUCCESS"}},
		"statement_date_coverage": map[string]interface{}{"start": transactions[0].ValueDate, "end": statementPeriodEnd},
		"category_kpis":           kpiCats,
		"categories_found":        foundCategories,
		"uncategorized":           uncategorized,
	}
	return result, nil
}

// 1. Get all bank statements (POST, req: user_id)
func GetAllBankStatementsHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
			http.Error(w, "Missing or invalid user_id in body", http.StatusBadRequest)
			return
		}
		rows, err := db.Query(`
			       WITH latest_audit AS (
				       SELECT DISTINCT ON (a.bankstatementid)
					       a.bankstatementid,
					       a.actiontype,
					       a.processing_status,
					       a.action_id,
					       a.requested_by,
					       a.requested_at,
					       a.checker_by,
					       a.checker_at,
					       a.checker_comment,
					       a.reason
				       FROM cimplrcorpsaas.auditactionbankstatement a
				       ORDER BY a.bankstatementid, a.requested_at DESC
			       )
			       SELECT s.bank_statement_id, e.entity_name, s.account_number, s.statement_period_start, s.statement_period_end, s.opening_balance, s.closing_balance, s.uploaded_at,
				      la.actiontype, la.processing_status, la.action_id, la.requested_by, la.requested_at, la.checker_by, la.checker_at, la.checker_comment, la.reason
			       FROM cimplrcorpsaas.bank_statements s
			       JOIN public.masterentitycash e ON s.entity_id = e.entity_id
			       LEFT JOIN latest_audit la ON la.bankstatementid = s.bank_statement_id
			       ORDER BY s.uploaded_at DESC
		       `)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
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
			resp = append(resp, map[string]interface{}{
				"bank_statement_id":      id,
				"entity_name":            entityName,
				"account_number":         acc,
				"statement_period_start": start,
				"statement_period_end":   end,
				"opening_balance":        open,
				"closing_balance":        close,
				"uploaded_at":            uploaded,
				"action_type":            actionType.String,
				"processing_status":      processingStatus.String,
				"action_id":              actionID.String,
				"requested_by":           requestedBy.String,
				"requested_at":           requestedAt.Time,
				"checker_by":             checkerBy.String,
				"checker_at":             checkerAt.Time,
				"checker_comment":        checkerComment.String,
				"reason":                 reason.String,
			})
		}
		w.Header().Set("Content-Type", "application/json")
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
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || body.BankStatementID == "" {
			http.Error(w, "Missing user_id or bank_statement_id", http.StatusBadRequest)
			return
		}
		rows, err := db.Query(`
			SELECT t.transaction_id, e.entity_name, t.tran_id, t.value_date, t.transaction_date, t.description, t.withdrawal_amount, t.deposit_amount, t.balance
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements s ON t.bank_statement_id = s.bank_statement_id
			JOIN public.masterentitycash e ON s.entity_id = e.entity_id
			WHERE t.bank_statement_id = $1
			ORDER BY t.value_date
		`, body.BankStatementID)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		resp := []map[string]interface{}{}
		for rows.Next() {
			var tid int64
			var entityName, tranID, desc string
			var vdate, tdate time.Time
			var withdrawal, deposit, balance sql.NullFloat64
			if err := rows.Scan(&tid, &entityName, &tranID, &vdate, &tdate, &desc, &withdrawal, &deposit, &balance); err != nil {
				continue
			}
			resp = append(resp, map[string]interface{}{
				"transaction_id":    tid,
				"entity_name":       entityName,
				"tran_id":           tranID,
				"value_date":        vdate,
				"transaction_date":  tdate,
				"description":       desc,
				"withdrawal_amount": withdrawal.Float64,
				"deposit_amount":    deposit.Float64,
				"balance":           balance.Float64,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    resp,
		})
	})
}

// 3. Approve a bank statement (POST, req: user_id, bank_statement_id)
func ApproveBankStatementHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID          string `json:"user_id"`
			BankStatementID string `json:"bank_statement_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || body.BankStatementID == "" {
			http.Error(w, "Missing user_id or bank_statement_id", http.StatusBadRequest)
			return
		}
		// Check if the latest audit action is DELETE_PENDING_APPROVAL
		var actionType, processingStatus string
		err := db.QueryRow(`
			SELECT actiontype, processing_status FROM cimplrcorpsaas.auditactionbankstatement
			WHERE bankstatementid = $1
			ORDER BY requested_at DESC LIMIT 1
		`, body.BankStatementID).Scan(&actionType, &processingStatus)
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if actionType == "DELETE" && processingStatus == "DELETE_PENDING_APPROVAL" {
			// Perform actual deletion
			tx, err := db.Begin()
			if err != nil {
				http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
				return
			}
			defer tx.Rollback()
			_, err = tx.Exec(`DELETE FROM cimplrcorpsaas.bank_statement_transactions WHERE bank_statement_id = $1`, body.BankStatementID)
			if err != nil {
				http.Error(w, "Failed to delete transactions: "+err.Error(), http.StatusInternalServerError)
				return
			}
			_, err = tx.Exec(`DELETE FROM public.bank_balances_manual WHERE balance_id = $1`, body.BankStatementID)
			if err != nil {
				http.Error(w, "Failed to delete manual balance: "+err.Error(), http.StatusInternalServerError)
				return
			}
			_, err = tx.Exec(`DELETE FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, body.BankStatementID)
			if err != nil {
				http.Error(w, "Failed to delete bank statement: "+err.Error(), http.StatusInternalServerError)
				return
			}
			// Insert audit action for delete approval
			_, err = tx.Exec(`INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at) VALUES ($1, $2, $3, $4, $5)`, body.BankStatementID, "DELETE", "DELETED", body.UserID, time.Now())
			if err != nil {
				http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if err := tx.Commit(); err != nil {
				http.Error(w, "Failed to commit: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"message": "Bank statement and related data deleted after approval",
			})
			return
		} else {
			// Normal approval
			_, err := db.Exec(`INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at) VALUES ($1, $2, $3, $4, $5)`, body.BankStatementID, "APPROVE", "APPROVED", body.UserID, time.Now())
			if err != nil {
				http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"message": "Bank statement approved",
			})
		}
	})
}

// 4. Reject a bank statement (POST, req: user_id, bank_statement_id)
func RejectBankStatementHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID          string `json:"user_id"`
			BankStatementID string `json:"bank_statement_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || body.BankStatementID == "" {
			http.Error(w, "Missing user_id or bank_statement_id", http.StatusBadRequest)
			return
		}
		_, err := db.Exec(`INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at) VALUES ($1, $2, $3, $4, $5)`, body.BankStatementID, "REJECT", "REJECTED", body.UserID, time.Now())
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Bank statement rejected",
		})
	})
}

// 5. Delete bank statement, its transactions, and its balance (POST, req: user_id, bank_statement_id)
func DeleteBankStatementHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			UserID          string `json:"user_id"`
			BankStatementID string `json:"bank_statement_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || body.BankStatementID == "" {
			http.Error(w, "Missing user_id or bank_statement_id", http.StatusBadRequest)
			return
		}
		// Instead of direct delete, insert audit action for delete approval
		_, err := db.Exec(`
			INSERT INTO cimplrcorpsaas.auditactionbankstatement (
				bankstatementid, actiontype, processing_status, requested_by, requested_at
			) VALUES ($1, $2, $3, $4, $5)
		`, body.BankStatementID, "DELETE", "DELETE_PENDING_APPROVAL", body.UserID, time.Now())
		if err != nil {
			http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Delete request submitted for approval",
		})
	})

}

func UploadBankStatementV2Handler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		err := r.ParseMultipartForm(32 << 20) // 32MB
		if err != nil {
			http.Error(w, "Failed to parse form: "+err.Error(), http.StatusBadRequest)
			return
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "File not found in request: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Use a hash of the file contents for idempotency
		fileBytes, err := io.ReadAll(file)
		if err != nil {
			http.Error(w, "Failed to read file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		hash := sha256.Sum256(fileBytes)
		fileHash := fmt.Sprintf("%x", hash[:])

		// Re-create file reader for actual processing
		fileReader := bytes.NewReader(fileBytes)
		mf := &bytesFile{Reader: fileReader}

		result, err := UploadBankStatementV2WithCategorization(r.Context(), db, mf, fileHash)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": err.Error(),
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": "Bank statement uploaded successfully",
			"data":    result,
		})
	})
}

// bytesFile implements multipart.File for a bytes.Reader
type bytesFile struct {
	*bytes.Reader
}

func (b *bytesFile) Close() error { return nil }

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
		return errors.New("this file has already been uploaded (idempotency check failed)")
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
			if cell == "A/C No:" && j+1 < len(rows[i]) {
				accountNumber = rows[i][j+1]
			}
			if cell == "Name:" && j+1 < len(rows[i]) {
				accountName = rows[i][j+1]
			}
		}
	}
	if accountNumber == "" {
		return errors.New("account number not found in file header")
	}

	// Lookup entity_id and name from masterbankaccount
	err = db.QueryRowContext(ctx, `SELECT entity_id, COALESCE(account_nickname, bank_name) FROM public.masterbankaccount WHERE account_number = $1 AND is_deleted = false`, accountNumber).Scan(&entityID, &accountName)
	if err != nil {
		return fmt.Errorf("failed to lookup account in masterbankaccount: %w", err)
	}

	// Print extracted data
	fmt.Printf("Extracted Account Number: %s\n", accountNumber)
	fmt.Printf("Entity ID: %s\n", entityID)
	fmt.Printf("Account Name: %s\n", accountName)

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
			if cell == "Tran. Id" || cell == "Tran Id" {
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
	required := []string{"Tran. Id", "Value Date", "Transaction Date", "Transaction Remarks", "Withdrawal Amt (INR)", "Deposit Amt (INR)", "Balance (INR)"}
	for _, col := range required {
		if _, ok := colIdx[col]; !ok {
			return fmt.Errorf("required column '%s' not found in header", col)
		}
	}

	// Parse transactions, skip header
	var lastValidBalance sql.NullFloat64
	var lastValidValueDate time.Time
	for i, row := range rows[txnHeaderIdx+1:] {
		// Skip rows with no transaction ID or all columns empty
		if len(row) == 0 || (colIdx["Tran. Id"] >= len(row)) || strings.TrimSpace(row[colIdx["Tran. Id"]]) == "" {
			continue
		}
		// Defensive: fill missing columns with empty string
		for len(row) < len(headerRow) {
			row = append(row, "")
		}
		tranID := sql.NullString{String: row[colIdx["Tran. Id"]], Valid: row[colIdx["Tran. Id"]] != ""}
		valueDate, _ := time.Parse("02/Jan/2006", row[colIdx["Value Date"]])
		transactionDate, _ := time.Parse("02/Jan/2006", row[colIdx["Transaction Date"]])
		description := row[colIdx["Transaction Remarks"]]
		var withdrawal, deposit sql.NullFloat64
		withdrawalStr := cleanAmount(row[colIdx["Withdrawal Amt (INR)"]])
		depositStr := cleanAmount(row[colIdx["Deposit Amt (INR)"]])
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
		balance := sql.NullFloat64{Valid: row[colIdx["Balance (INR)"]] != ""}
		if balance.Valid {
			balanceStr := cleanAmount(row[colIdx["Balance (INR)"]])
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
		return errors.New("a statement for this period already exists for this account")
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

func cleanAmount(s string) string {
	s = strings.ReplaceAll(s, ",", "")
	return strings.TrimSpace(s)
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
