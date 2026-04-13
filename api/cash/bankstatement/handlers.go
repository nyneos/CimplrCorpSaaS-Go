package bankstatement

import (
	apictx "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/notification/catalog"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
)

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
														mba.account_nickname AS account_nickname,
														s.upload_s3_key
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
			var uploadS3Key sql.NullString
			var requestedAt, checkerAt sql.NullTime
			if err := rows.Scan(&id, &entityName, &acc, &start, &end, &open, &close, &uploaded,
				&actionType, &processingStatus, &actionID, &requestedBy, &requestedAt, &checkerBy, &checkerAt, &checkerComment, &reason, &bankName, &accountNickname, &uploadS3Key); err != nil {
				continue
			}
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
				"upload_s3_key":              uploadS3Key.String,
				"is_delete_pending_approval": isDeletePending,
			})
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    resp,
		})
	})
}

// 2. Get all transactions for a bank statement (POST, req: user_id, bank_statement_id)
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
			LEFT JOIN public.mastercashflowcategory c
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

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    resp,
		})
	})
}

func GetBankStatementDownloadURLHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			BankStatementID string `json:"bank_statement_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.BankStatementID) == "" {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "bank_statement_id is required"})
			return
		}

		ctx := r.Context()
		var uploadS3Key sql.NullString
		err := db.QueryRowContext(ctx, `
			SELECT upload_s3_key
			FROM cimplrcorpsaas.bank_statements
			WHERE bank_statement_id = $1
		`, body.BankStatementID).Scan(&uploadS3Key)
		if err != nil {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			if errors.Is(err, sql.ErrNoRows) {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "bank_statement_id not found"})
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": pqUserFriendlyMessage(err)})
			return
		}

		if !uploadS3Key.Valid || strings.TrimSpace(uploadS3Key.String) == "" {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "no file available"})
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, uploadS3Key.String, 15*time.Minute)
		if err != nil {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "Failed to generate download URL"})
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"download_url": downloadURL,
			},
		})
	})
}

func GetBankStatementBulkDownloadURLHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			BankStatementIDs []string `json:"bank_statement_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.BankStatementIDs) == 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": "bank_statement_ids is required"})
			return
		}

		ctx := r.Context()
		files := make([]map[string]string, 0, len(body.BankStatementIDs))
		failedIDs := make([]string, 0)

		for _, rawID := range body.BankStatementIDs {
			bankStatementID := strings.TrimSpace(rawID)
			if bankStatementID == "" {
				continue
			}

			var uploadS3Key sql.NullString
			err := db.QueryRowContext(ctx, `
				SELECT upload_s3_key
				FROM cimplrcorpsaas.bank_statements
				WHERE bank_statement_id = $1
			`, bankStatementID).Scan(&uploadS3Key)
			if err != nil {
				failedIDs = append(failedIDs, bankStatementID)
				continue
			}

			key := strings.TrimSpace(uploadS3Key.String)
			if !uploadS3Key.Valid || key == "" {
				failedIDs = append(failedIDs, bankStatementID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, bankStatementID)
				continue
			}

			files = append(files, map[string]string{
				"bank_statement_id": bankStatementID,
				"download_url":      downloadURL,
			})
		}

		if len(files) == 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "no downloadable files found",
				"data": map[string]interface{}{
					"files":      []map[string]string{},
					"failed_ids": failedIDs,
				},
			})
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"files":      files,
				"failed_ids": failedIDs,
			},
		})
	})
}

// 2a. Mark transactions as misclassified
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

		var acctCurrency sql.NullString
		err = db.QueryRowContext(ctx, `SELECT mba.currency FROM public.masterbankaccount mba WHERE mba.account_number = $1 LIMIT 1`, accountNumber).Scan(&acctCurrency)
		if err != nil {
			http.Error(w, "failed to fetch account currency: "+err.Error(), http.StatusInternalServerError)
			return
		}

		var currencyCode string
		if acctCurrency.Valid {
			currencyCode = acctCurrency.String
		}
		rules, err := loadCategoryRuleComponents(ctx, db, accountNumber, entityID, currencyCode)
		if err != nil {
			http.Error(w, "failed to fetch category rules: "+err.Error(), http.StatusInternalServerError)
			return
		}

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

		categoryCount := map[string]int{}
		debitSum := map[string]float64{}
		creditSum := map[string]float64{}
		uncategorized := []map[string]interface{}{}
		transactionsCount := 0
		totalTxns := 0
		groupedTxns := 0
		ungroupedTxns := 0
		categoryTxns := map[string][]map[string]interface{}{}
		for rows.Next() {
			var transactionID int64
			var tranID sql.NullString
			var valueDate, transactionDate time.Time
			var description string
			var withdrawal, deposit, balance sql.NullFloat64
			var rawJSON json.RawMessage
			var existingCategoryID sql.NullString
			if err := rows.Scan(&transactionID, &tranID, &valueDate, &transactionDate, &description,
				&withdrawal, &deposit, &balance, &rawJSON, &existingCategoryID); err != nil {
				continue
			}
			transactionsCount++
			totalTxns++

			newCategoryID := matchCategoryForTransaction(rules, description, withdrawal, deposit, sql.NullTime{Time: valueDate, Valid: true})

			if (newCategoryID.Valid != existingCategoryID.Valid) || (newCategoryID.Valid && existingCategoryID.Valid && newCategoryID.String != existingCategoryID.String) {
				var catParam interface{}
				if newCategoryID.Valid {
					catParam = newCategoryID.String
				} else {
					catParam = nil
				}
				if _, err := db.ExecContext(ctx, `
					UPDATE cimplrcorpsaas.bank_statement_transactions
					SET category_id = $1
					WHERE transaction_id = $2
				`, catParam, transactionID); err != nil {
					log.Printf("failed to update category_id for transaction_id %d: %v", transactionID, err)
				}
			}

			if newCategoryID.Valid {
				categoryCount[newCategoryID.String]++
				if withdrawal.Valid {
					debitSum[newCategoryID.String] += withdrawal.Float64
				}
				if deposit.Valid {
					creditSum[newCategoryID.String] += deposit.Float64
				}
				groupedTxns++
				catID := newCategoryID.String
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
					"tran_id":          tranID.String,
					"transaction_id":   transactionID,
					"tran_date":        transactionDate,
					"transaction_date": transactionDate,
					"description":      description,
					"value_date":       valueDate,
					"amount":           map[string]interface{}{"withdrawal": withdrawal.Float64, "deposit": deposit.Float64},
					"balance":          balance.Float64,
				})
				ungroupedTxns++
			}
		}

		kpiCats := []map[string]interface{}{}
		foundCategories := []map[string]interface{}{}
		foundCategoryIDs := map[string]bool{}
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
		startStr := ""
		endStr := ""
		if !statementPeriodStart.IsZero() {
			startStr = statementPeriodStart.Format(time.RFC3339)
		}
		if !statementPeriodEnd.IsZero() {
			endStr = statementPeriodEnd.Format(time.RFC3339)
		}
		result := map[string]interface{}{
			"pages_processed":                 1,
			"bank_wise_status":                []map[string]interface{}{{"account_number": accountNumber, "status": "SUCCESS"}},
			"statement_date_coverage":         map[string]interface{}{"start": startStr, "end": endStr},
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
func ApproveBankStatementHandler(db *sql.DB, pgxPool *pgxpool.Pool) http.Handler {
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

				capturedID := bsid
				capturedUser := body.UserID
				payload := BuildBankStatementNotifPayload(context.Background(), pgxPool, []string{capturedID}, "APPROVE", capturedUser)
				go catalog.TriggerNotification(
					context.Background(), pgxPool,
					"/cash/bank-statements/v2/approve",
					fmt.Sprintf("BSAPPROVE/%s/%d", bsid, time.Now().UnixMilli()),
					payload,
				)

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
func RejectBankStatementHandler(db *sql.DB, pgxPool *pgxpool.Pool) http.Handler {
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
		if pgxPool != nil {
			capturedIDs := body.BankStatementIDs
			capturedUser := body.UserID
			capturedComment := body.Comment
			payload := BuildBankStatementNotifPayload(context.Background(), pgxPool, capturedIDs, "REJECT", capturedUser)
			payload["Comment"] = capturedComment
			go catalog.TriggerNotification(
				context.Background(), pgxPool,
				"/cash/bank-statements/v2/reject",
				fmt.Sprintf("BSREJECT/%s/%d", capturedUser, time.Now().UnixMilli()),
				payload,
			)
		}
	})
}

// 5. Delete bank statement (POST, req: user_id, bank_statement_id)
func DeleteBankStatementHandler(db *sql.DB, pgxPool *pgxpool.Pool) http.Handler {
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
		if pgxPool != nil {
			capturedIDs := body.BankStatementIDs
			capturedUser := body.UserID
			capturedComment := body.Comment
			go catalog.TriggerNotification(
				context.Background(), pgxPool,
				"/cash/bank-statements/v2/delete",
				fmt.Sprintf("BSDELETE/%s/%d", capturedUser, time.Now().UnixMilli()),
				map[string]interface{}{
					"BankStatementIDs": capturedIDs,
					"Count":            len(capturedIDs),
					"UserID":           capturedUser,
					"Comment":          capturedComment,
					"Action":           "DELETE_PENDING_APPROVAL",
					"ActionAt":         time.Now().Format(time.RFC3339),
				},
			)
		}
	})
}

func UploadBankStatementV2Handler(db *sql.DB, pgxPool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)

		if r.Method != http.MethodPost {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Only POST method is allowed for this endpoint.",
			})
			return
		}

		isPDF := r.URL.Query().Get("is_pdf") == "true"

		if isPDF {
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

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			log.Printf("[BANK-UPLOAD-ERROR] Failed to parse multipart form: %v", err)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Unable to read the uploaded file. Please try again.",
			})
			return
		}

		if r.FormValue("multi") == "true" {
			UploadMultiAccountBankStatementHandler(db).ServeHTTP(w, r)
			return
		}

		useMappingFlag := strings.ToLower(strings.TrimSpace(r.FormValue("mapping")))
		useMapping := useMappingFlag == "true" || useMappingFlag == "1"

		var mappings *ColumnMappings
		if useMapping {
			mappingsJSON := r.FormValue("column_mappings")
			if mappingsJSON != "" {
				mappings = &ColumnMappings{}
				if err := json.Unmarshal([]byte(mappingsJSON), mappings); err != nil {
					log.Printf("[BANK-UPLOAD-ERROR] Invalid column_mappings JSON: %v", err)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"success": false,
						"message": "Invalid column_mappings JSON: " + err.Error(),
					})
					return
				}
				log.Printf("[BANK-UPLOAD-DEBUG] Custom column mappings provided: %+v", mappings)
			} else {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"message": "mapping flag is true but column_mappings not provided",
				})
				return
			}

			if mappings != nil && strings.TrimSpace(mappings.AccountNumber) == "" {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"message": "Column-Mappings must include 'Account Number' when mapping is enabled",
				})
				return
			}
		}

		var fileFieldsAvailable []string
		if r.MultipartForm != nil && r.MultipartForm.File != nil {
			for fieldName := range r.MultipartForm.File {
				fileFieldsAvailable = append(fileFieldsAvailable, fieldName)
			}
			log.Printf("[BANK-UPLOAD-DEBUG] Available file form fields: %v", fileFieldsAvailable)
		} else {
			log.Printf("[BANK-UPLOAD-ERROR] No file fields found in multipart form - request may not include a file attachment")
		}

		file, fileHeader, err := r.FormFile("file")
		uploadFileName := ""
		if fileHeader != nil {
			uploadFileName = fileHeader.Filename
		}
		if err != nil {
			log.Printf("[BANK-UPLOAD-ERROR] FormFile('file') error: %v", err)
			if file == nil {
				file, fileHeader, err = r.FormFile("statement")
				if fileHeader != nil {
					uploadFileName = fileHeader.Filename
				}
			}
			if err != nil && file == nil {
				file, fileHeader, err = r.FormFile("bankStatement")
				if fileHeader != nil {
					uploadFileName = fileHeader.Filename
				}
			}
			if err != nil && file == nil {
				if r.MultipartForm != nil && len(r.MultipartForm.File) > 0 {
					log.Printf("[BANK-UPLOAD-DEBUG] Trying first available file field from: %v", fileFieldsAvailable)
					for fieldName, files := range r.MultipartForm.File {
						if len(files) > 0 {
							log.Printf("[BANK-UPLOAD-DEBUG] Using field: %s", fieldName)
							file, err = files[0].Open()
							uploadFileName = files[0].Filename
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

		fileReader := bytes.NewReader(fileBytes)
		mf := &bytesFile{Reader: fileReader}

		result, err := UploadBankStatementV2WithCategorization(r.Context(), db, mf, fileHash, useMapping, mappings)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": userFriendlyUploadError(err),
			})
			return
		}

		msg := "Bank statement uploaded successfully"
		if rc, ok := result["transactions_under_review_count"].(int); ok && rc > 0 {
			msg = fmt.Sprintf("Bank statement uploaded successfully. %d transactions are under review.", rc)
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"message": msg,
			"data":    result,
		})

		if pgxPool != nil {
			capturedResult := result
			capturedUser := r.FormValue("user_id")
			capturedFile := uploadFileName
			go func() {
				notifPayload := BuildBankStatementPayloadFromV2Result(
					capturedResult,
					capturedUser,
					capturedFile,
					"PREVIEW",
				)
				bsID := notifPayload.BankStatementID
				if bsID == "" {
					if v, ok := capturedResult["bank_statement_id"].(string); ok {
						bsID = v
					}
				}
				catalog.TriggerNotification(
					context.Background(), pgxPool,
					"/cash/preview",
					fmt.Sprintf("BSUPLOAD/%s/%d", bsID, time.Now().UnixMilli()),
					notifPayload.ToMap(),
				)
			}()
		}
	})
}

// UploadZippedBankStatementsHandler accepts a zip file containing multiple bank statement files,
// unzips them, processes each file one by one, and returns aggregated results.
func UploadZippedBankStatementsHandler(db *sql.DB, pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		if err := r.ParseMultipartForm(100 << 20); err != nil {
			http.Error(w, fmt.Sprintf("Failed to parse multipart form: %v", err), http.StatusBadRequest)
			return
		}

		userID := r.FormValue("user_id")
		if userID == "" {
			http.Error(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}

		zipFile, zipHeader, err := r.FormFile("file")
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get zip file: %v", err), http.StatusBadRequest)
			return
		}
		defer zipFile.Close()

		useMapping := r.FormValue("useMapping") == "true"
		var mappings *ColumnMappings
		if useMapping {
			mappingsJSON := r.FormValue("mappings")
			if mappingsJSON != "" {
				mappings = &ColumnMappings{}
				if err := json.Unmarshal([]byte(mappingsJSON), mappings); err != nil {
					http.Error(w, fmt.Sprintf("Invalid mappings JSON: %v", err), http.StatusBadRequest)
					return
				}
			}
		}

		zipData, err := io.ReadAll(zipFile)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read zip file: %v", err), http.StatusInternalServerError)
			return
		}

		zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to open zip file: %v", err), http.StatusBadRequest)
			return
		}

		type FileResult struct {
			FileName string                 `json:"file_name"`
			Success  bool                   `json:"success"`
			Result   map[string]interface{} `json:"result,omitempty"`
			Error    string                 `json:"error,omitempty"`
		}

		results := []FileResult{}
		successCount := 0
		failureCount := 0

		for _, zipFileEntry := range zipReader.File {
			if zipFileEntry.FileInfo().IsDir() {
				continue
			}

			base := filepath.Base(zipFileEntry.Name)
			dir := filepath.Dir(zipFileEntry.Name)
			if strings.HasPrefix(base, ".") || strings.HasPrefix(base, "._") ||
				strings.Contains(dir, "__MACOSX") || base == ".DS_Store" {
				continue
			}

			ext := strings.ToLower(filepath.Ext(zipFileEntry.Name))
			if ext != ".xlsx" && ext != ".xls" && ext != ".csv" {
				results = append(results, FileResult{
					FileName: zipFileEntry.Name,
					Success:  false,
					Error:    fmt.Sprintf("Unsupported file type: %s (only .xlsx, .xls, .csv allowed)", ext),
				})
				failureCount++
				continue
			}

			fileReader, err := zipFileEntry.Open()
			if err != nil {
				results = append(results, FileResult{
					FileName: zipFileEntry.Name,
					Success:  false,
					Error:    fmt.Sprintf("Failed to open file from zip: %v", err),
				})
				failureCount++
				continue
			}

			fileData, err := io.ReadAll(fileReader)
			fileReader.Close()
			if err != nil {
				results = append(results, FileResult{
					FileName: zipFileEntry.Name,
					Success:  false,
					Error:    fmt.Sprintf("Failed to read file contents: %v", err),
				})
				failureCount++
				continue
			}

			hash := sha256.New()
			hash.Write(fileData)
			fileHash := fmt.Sprintf("%x", hash.Sum(nil))

			bytesReader := bytes.NewReader(fileData)
			file := &bytesFile{Reader: bytesReader}

			result, err := UploadBankStatementV2WithCategorization(ctx, db, file, fileHash, useMapping, mappings)
			if err != nil {
				results = append(results, FileResult{
					FileName: zipFileEntry.Name,
					Success:  false,
					Error:    userFriendlyUploadError(err),
				})
				failureCount++
				continue
			}

			results = append(results, FileResult{
				FileName: zipFileEntry.Name,
				Success:  true,
				Result:   result,
			})
			successCount++
		}

		response := map[string]interface{}{
			"message":       fmt.Sprintf("Processed %d files from zip", len(results)),
			"zip_file_name": zipHeader.Filename,
			"total_files":   len(results),
			"success_count": successCount,
			"failure_count": failureCount,
			"results":       results,
			"uploaded_by":   userID,
			"upload_time":   time.Now().Format(time.RFC3339),
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)

		if pool != nil {
			for _, fres := range results {
				if fres.Success && fres.Result != nil {
					if rid, ok := fres.Result["id"].(string); ok && rid == "" {
						if bsid, ok2 := fres.Result["bank_statement_id"].(string); ok2 {
							rid = bsid
						}
					}
					capturedResult := fres.Result
					capturedZipFile := zipHeader.Filename
					go func() {
						notifPayload := BuildBankStatementPayloadFromV2Result(
							capturedResult,
							userID,
							capturedZipFile,
							"PENDING_APPROVAL",
						)
						catalog.TriggerNotification(context.Background(), pool,
							"/cash/upload-bank-statement-zip",
							fmt.Sprintf("BSUPLOAD/%s/%d", notifPayload.BankStatementID, time.Now().UnixMilli()),
							notifPayload.ToMap(),
						)
					}()
				}
			}
		}
	})
}
