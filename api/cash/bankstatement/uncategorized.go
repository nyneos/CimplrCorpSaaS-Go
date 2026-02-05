package bankstatement

import (
	"database/sql"
	"encoding/json"
	"net/http"
)

// GetUncategorizedTransactionsHandler returns all uncategorized transactions across all bank statements
// Supports pagination with limit/offset, if no limit provided returns all data
func GetUncategorizedTransactionsHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			UserID string `json:"user_id"`
			Limit  *int   `json:"limit,omitempty"`  // Optional: if not provided, return all
			Offset *int   `json:"offset,omitempty"` // Optional: defaults to 0
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		if req.UserID == "" {
			http.Error(w, "user_id is required", http.StatusBadRequest)
			return
		}

		// Set defaults
		offset := 0
		if req.Offset != nil {
			offset = *req.Offset
		}

		// Build query with optional limit
		query := `
			SELECT 
				t.transaction_id,
				t.bank_statement_id,
				t.account_number,
				t.tran_id,
				t.value_date,
				t.transaction_date,
				t.posted_date,
				t.cheque_no,
				t.description,
				t.withdrawal_amount,
				t.deposit_amount,
				t.balance,
				t.category_id,
				t.created_at,
				bs.entity_id,
				bs.statement_period_start,
				bs.statement_period_end,
				mba.bank_name,
				mba.entity_id as entity_name,
				mba.currency,
				COALESCE(mc.category_name, 'Uncategorized') as category_name
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
			LEFT JOIN public.masterbankaccount mba ON t.account_number = mba.account_number
			LEFT JOIN public.mastercashflowcategory mc ON t.category_id = mc.category_id
			WHERE (t.category_id IS NULL OR t.category_id = '')
			ORDER BY t.transaction_date DESC, t.transaction_id DESC
		`

		args := []interface{}{}
		if req.Limit != nil && *req.Limit > 0 {
			query += " LIMIT $1 OFFSET $2"
			args = append(args, *req.Limit, offset)
		}

		rows, err := db.Query(query, args...)
		if err != nil {
			http.Error(w, "Database query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		transactions := []map[string]interface{}{}
		for rows.Next() {
			var (
				transactionID                                      int64
				bankStatementID, accountNumber                     string
				tranID, chequeNo                                   sql.NullString
				valueDate, transactionDate                         sql.NullTime
				postedDate                                         sql.NullTime
				description                                        string
				withdrawalAmount, depositAmount, balance           sql.NullFloat64
				categoryID                                         sql.NullString
				createdAt                                          sql.NullTime
				entityID                                           sql.NullString
				periodStart, periodEnd                             sql.NullTime
				bankName, entityName                               sql.NullString
				currency                                           sql.NullString
				categoryName                                       string
			)

			err := rows.Scan(
				&transactionID, &bankStatementID, &accountNumber, &tranID,
				&valueDate, &transactionDate, &postedDate, &chequeNo,
				&description, &withdrawalAmount, &depositAmount, &balance,
				&categoryID, &createdAt, &entityID, &periodStart, &periodEnd,
				&bankName, &entityName, &currency, &categoryName,
			)
			if err != nil {
				continue
			}

			txn := map[string]interface{}{
				"transaction_id":     transactionID,
				"bank_statement_id":  bankStatementID,
				"account_number":     accountNumber,
				"description":        description,
				"withdrawal_amount":  nil,
				"deposit_amount":     nil,
				"balance":            nil,
				"category_id":        nil,
				"category_name":      categoryName,
				"tran_id":            nil,
				"cheque_no":          nil,
				"value_date":         nil,
				"transaction_date":   nil,
				"posted_date":        nil,
				"created_at":         nil,
				"entity_id":          nil,
				"period_start":       nil,
				"period_end":         nil,
				"bank_name":          nil,
				"entity_name":        nil,
				"currency":           nil,
			}

			if tranID.Valid {
				txn["tran_id"] = tranID.String
			}
			if chequeNo.Valid {
				txn["cheque_no"] = chequeNo.String
			}
			if valueDate.Valid {
				txn["value_date"] = valueDate.Time.Format("2006-01-02")
			}
			if transactionDate.Valid {
				txn["transaction_date"] = transactionDate.Time.Format("2006-01-02")
			}
			if postedDate.Valid {
				txn["posted_date"] = postedDate.Time.Format("2006-01-02")
			}
			if withdrawalAmount.Valid {
				txn["withdrawal_amount"] = withdrawalAmount.Float64
			}
			if depositAmount.Valid {
				txn["deposit_amount"] = depositAmount.Float64
			}
			if balance.Valid {
				txn["balance"] = balance.Float64
			}
			if categoryID.Valid {
				txn["category_id"] = categoryID.String
			}
			if createdAt.Valid {
				txn["created_at"] = createdAt.Time.Format("2006-01-02 15:04:05")
			}
			if entityID.Valid {
				txn["entity_id"] = entityID.String
			}
			if periodStart.Valid {
				txn["period_start"] = periodStart.Time.Format("2006-01-02")
			}
			if periodEnd.Valid {
				txn["period_end"] = periodEnd.Time.Format("2006-01-02")
			}
			if bankName.Valid {
				txn["bank_name"] = bankName.String
			}
			if entityName.Valid {
				txn["entity_name"] = entityName.String
			}
			if currency.Valid {
				txn["currency"] = currency.String
			}

			transactions = append(transactions, txn)
		}

		// Get total count for pagination metadata
		var totalCount int
		countQuery := `
			SELECT COUNT(*) 
			FROM cimplrcorpsaas.bank_statement_transactions t
			WHERE (t.category_id IS NULL OR t.category_id = '')
		`
		db.QueryRow(countQuery).Scan(&totalCount)

		response := map[string]interface{}{
			"success":      true,
			"transactions": transactions,
			"count":        len(transactions),
			"total_count":  totalCount,
		}

		if req.Limit != nil {
			response["limit"] = *req.Limit
			response["offset"] = offset
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})
}
