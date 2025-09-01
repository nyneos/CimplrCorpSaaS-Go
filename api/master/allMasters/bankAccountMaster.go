package allMaster

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/lib/pq"
)

type ClearingCode struct {
	CodeType  string `json:"code_type"`
	CodeValue string `json:"code_value"`
}

type BankAccountMasterRequest struct {
	BankID          string         `json:"bank_id"`
	EntityID        string         `json:"entity_id"`
	AccountNumber   string         `json:"account_number"`
	AccountNickname string         `json:"account_nickname"`
	AccountType     string         `json:"account_type"`
	CreditLimit     float64        `json:"credit_limit"`
	AccountCurrency string         `json:"account_currency"`
	IBAN            string         `json:"iban"`
	BranchName      string         `json:"branch_name"`
	BranchAddress   string         `json:"branch_address"`
	AccountStatus   string         `json:"account_status"`
	ClearingCodes   []ClearingCode `json:"clearing_codes"`
	UserID          string         `json:"user_id"`
}

func CreateBankAccountMaster(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BankAccountMasterRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Invalid JSON"})
			return
		}
		userID := req.UserID
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Missing user_id in body")
			return
		}
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				createdBy = s.Email
				break
			}
		}
		if createdBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "User session not found or email missing")
			return
		}
		if req.BankID == "" || req.EntityID == "" || req.AccountNumber == "" || req.AccountType == "" || req.AccountCurrency == "" || req.AccountStatus == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Missing required fields"})
			return
		}
		tx, err := db.Begin()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Failed to start transaction: " + err.Error()})
			return
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Panic: " + p.(string)})
			}
		}()
		var accountID string
		query := `INSERT INTO masterbankaccount (
			bank_id, entity_id, account_number, account_nickname, account_type, credit_limit, account_currency, iban, branch_name, branch_address, account_status
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		) RETURNING account_id`
		err = tx.QueryRow(query,
			req.BankID, req.EntityID, req.AccountNumber, req.AccountNickname, req.AccountType, req.CreditLimit, req.AccountCurrency, req.IBAN, req.BranchName, req.BranchAddress, req.AccountStatus,
		).Scan(&accountID)
		if err != nil {
			tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		auditQuery := `INSERT INTO auditactionbankaccount (
			account_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, $2, $3, $4, $5, now()) RETURNING action_id`
		var auditActionID string
		auditErr := tx.QueryRow(auditQuery,
			accountID,
			"CREATE",
			"PENDING_APPROVAL",
			nil,
			createdBy,
		).Scan(&auditActionID)
		if auditErr != nil {
			tx.Rollback()
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Account created but audit log failed: " + auditErr.Error(), "account_id": accountID})
			return
		}
		var clearingResults []map[string]interface{}
		for _, cc := range req.ClearingCodes {
			if cc.CodeType == "" || cc.CodeValue == "" {
				continue
			}
			ccQuery := `INSERT INTO masterclearingcode (account_id, code_type, code_value) VALUES ($1, $2, $3) RETURNING clearing_id`
			var clearingID string
			err := tx.QueryRow(ccQuery, accountID, cc.CodeType, cc.CodeValue).Scan(&clearingID)
			if err != nil {
				tx.Rollback()
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": err.Error(), "code_type": cc.CodeType})
				return
			}
			clearingResults = append(clearingResults, map[string]interface{}{"success": true, "clearing_id": clearingID, "code_type": cc.CodeType})
		}
		if err := tx.Commit(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Failed to commit transaction: " + err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":         true,
			"account_id":      accountID,
			"audit_action_id": auditActionID,
			"clearing_codes":  clearingResults,
		})
	}
}

func GetAllBankAccountMaster(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Query(`
			SELECT 
				a.account_id,
				a.account_number,
				a.account_type,
				a.account_status,
				a.account_currency,
				a.account_nickname,
				a.credit_limit,
				a.iban,
				a.branch_name,
				a.branch_address,
					a.old_bank_id,
					a.old_entity_id,
					a.old_account_number,
					a.old_account_nickname,
					a.old_account_type,
					a.old_credit_limit,
					a.old_account_currency,
					a.old_iban,
					a.old_branch_name,
					a.old_branch_address,
					a.old_account_status,
				b.bank_id,
				b.bank_name,
				e.entity_id,
				e.entity_name,
				COALESCE(
					(
						SELECT json_agg(json_build_object(
							'code_type', c.code_type,
							'code_value', c.code_value,
							'old_code_type', c.old_code_type,
							'old_code_value', c.old_code_value
						))
						FROM masterclearingcode c
						WHERE c.account_id = a.account_id
					), '[]'::json
				) AS clearing_codes
			FROM masterbankaccount a
			LEFT JOIN masterbank b ON a.bank_id = b.bank_id
			LEFT JOIN masterentity e ON e.entity_id::text = a.entity_id
		`)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type AccountRow struct {
			AccountID, AccountNumber, AccountType, AccountStatus, AccountCurrency string
			AccountNickname, IBAN, BranchName, BranchAddress                      sql.NullString
			OldBankID, OldEntityID, OldAccountNumber, OldAccountNickname          sql.NullString
			OldAccountType                                                        sql.NullString
			CreditLimit, OldCreditLimit                                           sql.NullFloat64
			OldAccountCurrency, OldIBAN, OldBranchName, OldBranchAddress          sql.NullString
			OldAccountStatus                                                      sql.NullString
			BankID, BankName, EntityID, EntityName                                sql.NullString
			ClearingCodes                                                         []byte
		}

		var accountIDs []string
		var accountRows []AccountRow

		for rows.Next() {
			var row AccountRow
			if err := rows.Scan(
				&row.AccountID, &row.AccountNumber, &row.AccountType, &row.AccountStatus,
				&row.AccountCurrency, &row.AccountNickname, &row.CreditLimit, &row.IBAN,
				&row.BranchName, &row.BranchAddress,
				&row.OldBankID, &row.OldEntityID, &row.OldAccountNumber, &row.OldAccountNickname, &row.OldAccountType, &row.OldCreditLimit, &row.OldAccountCurrency, &row.OldIBAN, &row.OldBranchName, &row.OldBranchAddress, &row.OldAccountStatus,
				&row.BankID, &row.BankName,
				&row.EntityID, &row.EntityName,
				&row.ClearingCodes,
			); err == nil {
				accountIDs = append(accountIDs, row.AccountID)
				accountRows = append(accountRows, row)
			}
		}

		// Fetch audit info (same as before)
		auditMap := make(map[string]map[string]interface{})
		if len(accountIDs) > 0 {
			auditQuery := `
				SELECT DISTINCT ON (account_id) account_id, processing_status, requested_by, requested_at,
				       actiontype, action_id, checker_by, checker_at, checker_comment, reason
				FROM auditactionbankaccount
				WHERE account_id = ANY($1::uuid[])
				ORDER BY account_id, requested_at DESC`
			auditRows, err := db.Query(auditQuery, pq.Array(accountIDs))
			if err == nil {
				defer auditRows.Close()
				for auditRows.Next() {
					var accID string
					var processingStatus, requestedBy, actionType, actionID, checkerBy, checkerComment, reason sql.NullString
					var requestedAt, checkerAt sql.NullTime
					if err := auditRows.Scan(&accID, &processingStatus, &requestedBy, &requestedAt,
						&actionType, &actionID, &checkerBy, &checkerAt, &checkerComment, &reason); err == nil {
						auditMap[accID] = map[string]interface{}{
							"processing_status": getNullString(processingStatus),
							"action_type":       getNullString(actionType),
							"action_id":         getNullString(actionID),
							"requested_by":      getNullString(requestedBy),
							"requested_at":      getNullTime(requestedAt),
							"checker_by":        getNullString(checkerBy),
							"checker_at":        getNullTime(checkerAt),
							"checker_comment":   getNullString(checkerComment),
							"reason":            getNullString(reason),
						}
					}
				}
			}
		}

		// Fetch audit details for CREATE/EDIT/DELETE
		auditDetailMap := make(map[string]api.ActionAuditInfo)
		if len(accountIDs) > 0 {
			adQuery := `SELECT account_id, actiontype, requested_by, requested_at
			            FROM auditactionbankaccount
			            WHERE account_id = ANY($1::uuid[])
			            AND actiontype IN ('CREATE','EDIT','DELETE')
			            ORDER BY account_id, requested_at DESC`
			adRows, err := db.Query(adQuery, pq.Array(accountIDs))
			if err == nil {
				defer adRows.Close()
				for adRows.Next() {
					var accID, adType string
					var adBy sql.NullString
					var adAt sql.NullTime
					if err := adRows.Scan(&accID, &adType, &adBy, &adAt); err == nil {
						info := api.GetAuditInfo(adType, adBy, adAt)
						audit := auditDetailMap[accID]
						if info.CreatedBy != "" {
							audit.CreatedBy = info.CreatedBy
							audit.CreatedAt = info.CreatedAt
						}
						if info.EditedBy != "" {
							audit.EditedBy = info.EditedBy
							audit.EditedAt = info.EditedAt
						}
						if info.DeletedBy != "" {
							audit.DeletedBy = info.DeletedBy
							audit.DeletedAt = info.DeletedAt
						}
						auditDetailMap[accID] = audit
					}
				}
			}
		}

		// Build response
		var accounts []map[string]interface{}
		for _, row := range accountRows {
			var clearingCodes []map[string]interface{}
			_ = json.Unmarshal(row.ClearingCodes, &clearingCodes)

			audit := auditMap[row.AccountID]
			auditInfo := auditDetailMap[row.AccountID]

			accounts = append(accounts, map[string]interface{}{
				"account_id":       row.AccountID,
				"account_number":   row.AccountNumber,
				"account_type":     row.AccountType,
				"account_status":   row.AccountStatus,
				"account_currency": row.AccountCurrency,
				"account_nickname": getNullString(row.AccountNickname),
				"credit_limit": func() float64 {
					if row.CreditLimit.Valid {
						return row.CreditLimit.Float64
					}
					return 0
				}(),
				"iban":                 getNullString(row.IBAN),
				"branch_name":          getNullString(row.BranchName),
				"branch_address":       getNullString(row.BranchAddress),
				"old_bank_id":          getNullString(row.OldBankID),
				"old_entity_id":        getNullString(row.OldEntityID),
				"old_account_number":   getNullString(row.OldAccountNumber),
				"old_account_nickname": getNullString(row.OldAccountNickname),
				"old_account_type":     getNullString(row.OldAccountType),
				"old_credit_limit": func() float64 {
					if row.OldCreditLimit.Valid {
						return row.OldCreditLimit.Float64
					}
					return 0
				}(),
				"old_account_currency": getNullString(row.OldAccountCurrency),
				"old_iban":             getNullString(row.OldIBAN),
				"old_branch_name":      getNullString(row.OldBranchName),
				"old_branch_address":   getNullString(row.OldBranchAddress),
				"old_account_status":   getNullString(row.OldAccountStatus),
				"bank_id":              getNullString(row.BankID),
				"bank_name":            getNullString(row.BankName),
				"entity_id":            getNullString(row.EntityID),
				"entity_name":          getNullString(row.EntityName),
				"clearing_codes":       clearingCodes,
				"processing_status": func() interface{} {
					if audit != nil {
						return audit["processing_status"]
					}
					return ""
				}(),
				"action_type": func() interface{} {
					if audit != nil {
						return audit["action_type"]
					}
					return ""
				}(),
				"action_id": func() interface{} {
					if audit != nil {
						return audit["action_id"]
					}
					return ""
				}(),
				"requested_by": func() interface{} {
					if audit != nil {
						return audit["requested_by"]
					}
					return ""
				}(),
				"requested_at": func() interface{} {
					if audit != nil {
						return audit["requested_at"]
					}
					return ""
				}(),
				"checker_by": func() interface{} {
					if audit != nil {
						return audit["checker_by"]
					}
					return ""
				}(),
				"checker_at": func() interface{} {
					if audit != nil {
						return audit["checker_at"]
					}
					return ""
				}(),
				"checker_comment": func() interface{} {
					if audit != nil {
						return audit["checker_comment"]
					}
					return ""
				}(),
				"reason": func() interface{} {
					if audit != nil {
						return audit["reason"]
					}
					return ""
				}(),
				"created_by": auditInfo.CreatedBy,
				"created_at": auditInfo.CreatedAt,
				"edited_by":  auditInfo.EditedBy,
				"edited_at":  auditInfo.EditedAt,
				"deleted_by": auditInfo.DeletedBy,
				"deleted_at": auditInfo.DeletedAt,
			})
		}

		if accounts == nil {
			accounts = make([]map[string]interface{}, 0)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"data":    accounts,
		})
	}
}

// Bulk update handler for bank account master
func UpdateBankAccountMasterBulk(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string `json:"user_id"`
			Accounts []struct {
				AccountID string                 `json:"account_id"`
				Fields    map[string]interface{} `json:"fields"`
			} `json:"accounts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Invalid JSON"})
			return
		}
		userID := req.UserID
		if userID == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "Missing user_id"})
			return
		}
		updatedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				updatedBy = s.Email
				break
			}
		}
		if updatedBy == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "User session not found"})
			return
		}

		var results []map[string]interface{}
		for _, acc := range req.Accounts {
			tx, err := db.Begin()
			if err != nil {
				results = append(results, map[string]interface{}{"success": false, "error": "Failed to start transaction: " + err.Error(), "account_id": acc.AccountID})
				continue
			}
			committed := false
			func() {
				defer func() {
					if !committed {
						tx.Rollback()
					}
					if p := recover(); p != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "panic: " + fmt.Sprint(p), "account_id": acc.AccountID})
					}
				}()

				// fetch current values for columns we care about
				var exBankID, exEntityID, exAccountNumber, exAccountNickname, exAccountType, exAccountCurrency, exIBAN, exBranchName, exBranchAddress, exAccountStatus sql.NullString
				var exCreditLimit sql.NullFloat64
				sel := `SELECT bank_id, entity_id, account_number, account_nickname, account_type, credit_limit, account_currency, iban, branch_name, branch_address, account_status FROM masterbankaccount WHERE account_id=$1 FOR UPDATE`
				if err := tx.QueryRow(sel, acc.AccountID).Scan(&exBankID, &exEntityID, &exAccountNumber, &exAccountNickname, &exAccountType, &exCreditLimit, &exAccountCurrency, &exIBAN, &exBranchName, &exBranchAddress, &exAccountStatus); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Failed to fetch existing account: " + err.Error(), "account_id": acc.AccountID})
					return
				}

				// build dynamic update based on provided fields
				var sets []string
				var args []interface{}
				pos := 1
				clearingProvided := false
				var clearingPayload interface{}

				for k, v := range acc.Fields {
					switch k {
					case "bank_id":
						sets = append(sets, fmt.Sprintf("bank_id=$%d, old_bank_id=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exBankID.String)
						pos += 2
					case "entity_id":
						sets = append(sets, fmt.Sprintf("entity_id=$%d, old_entity_id=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exEntityID.String)
						pos += 2
					case "account_number":
						sets = append(sets, fmt.Sprintf("account_number=$%d, old_account_number=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exAccountNumber.String)
						pos += 2
					case "account_nickname":
						sets = append(sets, fmt.Sprintf("account_nickname=$%d, old_account_nickname=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exAccountNickname.String)
						pos += 2
					case "account_type":
						sets = append(sets, fmt.Sprintf("account_type=$%d, old_account_type=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exAccountType.String)
						pos += 2
					case "credit_limit":
						// JSON numbers decode to float64
						if num, ok := v.(float64); ok {
							sets = append(sets, fmt.Sprintf("credit_limit=$%d, old_credit_limit=$%d", pos, pos+1))
							args = append(args, num, exCreditLimit.Float64)
							pos += 2
						}
					case "account_currency":
						sets = append(sets, fmt.Sprintf("account_currency=$%d, old_account_currency=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exAccountCurrency.String)
						pos += 2
					case "iban":
						sets = append(sets, fmt.Sprintf("iban=$%d, old_iban=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exIBAN.String)
						pos += 2
					case "branch_name":
						sets = append(sets, fmt.Sprintf("branch_name=$%d, old_branch_name=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exBranchName.String)
						pos += 2
					case "branch_address":
						sets = append(sets, fmt.Sprintf("branch_address=$%d, old_branch_address=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exBranchAddress.String)
						pos += 2
					case "account_status":
						sets = append(sets, fmt.Sprintf("account_status=$%d, old_account_status=$%d", pos, pos+1))
						args = append(args, fmt.Sprint(v), exAccountStatus.String)
						pos += 2
					case "clearing_codes":
						// handle after update; capture payload
						clearingProvided = true
						clearingPayload = v
					default:
						// ignore unknown fields
					}
				}

				var updatedAccountID string
				if len(sets) > 0 {
					// build query
					query := "UPDATE masterbankaccount SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE account_id=$%d RETURNING account_id", pos)
					args = append(args, acc.AccountID)
					if err := tx.QueryRow(query, args...).Scan(&updatedAccountID); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": err.Error(), "account_id": acc.AccountID})
						return
					}
				} else {
					// nothing to update in masterbankaccount
					updatedAccountID = acc.AccountID
				}

				var clearingResults []map[string]interface{}
				if clearingProvided {
					// parse clearingPayload to []ClearingCode
					var codes []ClearingCode
					// marshal then unmarshal to our struct for safety
					b, _ := json.Marshal(clearingPayload)
					if err := json.Unmarshal(b, &codes); err != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "invalid clearing_codes payload: " + err.Error(), "account_id": updatedAccountID})
						return
					}
					if _, delErr := tx.Exec(`DELETE FROM masterclearingcode WHERE account_id=$1`, updatedAccountID); delErr != nil {
						results = append(results, map[string]interface{}{"success": false, "error": "Failed to delete old clearing codes: " + delErr.Error(), "account_id": updatedAccountID})
						return
					}
					for _, cc := range codes {
						if cc.CodeType == "" || cc.CodeValue == "" {
							continue
						}
						ccQuery := `INSERT INTO masterclearingcode (account_id, code_type, code_value) VALUES ($1, $2, $3) RETURNING clearing_id`
						var clearingID string
						if err := tx.QueryRow(ccQuery, updatedAccountID, cc.CodeType, cc.CodeValue).Scan(&clearingID); err != nil {
							results = append(results, map[string]interface{}{"success": false, "error": err.Error(), "account_id": updatedAccountID, "code_type": cc.CodeType})
							return
						}
						clearingResults = append(clearingResults, map[string]interface{}{"success": true, "clearing_id": clearingID, "code_type": cc.CodeType})
					}
				}

				// insert audit action
				auditQuery := `INSERT INTO auditactionbankaccount (
					account_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, $2, $3, $4, $5, now()) RETURNING action_id`
				var auditActionID string
				if err := tx.QueryRow(auditQuery, updatedAccountID, "EDIT", "PENDING_EDIT_APPROVAL", nil, updatedBy).Scan(&auditActionID); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Account updated but audit log failed: " + err.Error(), "account_id": updatedAccountID, "clearing_codes": clearingResults})
					return
				}

				if err := tx.Commit(); err != nil {
					results = append(results, map[string]interface{}{"success": false, "error": "Failed to commit transaction: " + err.Error(), "account_id": updatedAccountID})
					return
				}
				committed = true
				results = append(results, map[string]interface{}{"success": true, "account_id": updatedAccountID, "audit_action_id": auditActionID, "clearing_codes": clearingResults})
			}()
		}
		w.Header().Set("Content-Type", "application/json")
		finalSuccess := api.IsBulkSuccess(results)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": finalSuccess, "results": results})
	}
}

// Bulk delete handler for bank account audit actions
func BulkDeleteBankAccountAudit(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			AccountIDs []string `json:"account_ids"`
			Reason     string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.AccountIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		sessions := auth.GetActiveSessions()
		requestedBy := ""
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		var results []string
		for _, accountID := range req.AccountIDs {
			query := `INSERT INTO auditactionbankaccount (
				account_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, 'DELETE', 'PENDING_DELETE_APPROVAL', $2, $3, now()) RETURNING action_id`
			var actionID string
			err := db.QueryRow(query, accountID, req.Reason, requestedBy).Scan(&actionID)
			if err == nil {
				results = append(results, actionID)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"created": results,
		})
	}
}

// Bulk reject audit actions for bank account master
func BulkRejectBankAccountAuditActions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ActionIDs []string `json:"action_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ActionIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		sessions := auth.GetActiveSessions()
		checkerBy := ""
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		query := `UPDATE auditactionbankaccount SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id,account_id`
		rows, err := db.Query(query, checkerBy, req.Comment, pq.Array(req.ActionIDs))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, accountID string
			rows.Scan(&id, &accountID)
			updated = append(updated, id, accountID)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"updated": updated,
		})
	}
}

// Bulk approve audit actions for bank account master
func BulkApproveBankAccountAuditActions(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string   `json:"user_id"`
			ActionIDs []string `json:"action_ids"`
			Comment   string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.ActionIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON or missing fields")
			return
		}
		sessions := auth.GetActiveSessions()
		checkerBy := ""
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid user_id or session")
			return
		}
		// First, delete audit rows that requested DELETE (pending delete approval)
		delQuery := `DELETE FROM auditactionbankaccount WHERE action_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL' RETURNING action_id, account_id`
		delRows, delErr := db.Query(delQuery, pq.Array(req.ActionIDs))
		var deleted []string
		var accountIDsToDelete []string
		if delErr == nil {
			defer delRows.Close()
			for delRows.Next() {
				var id, accountID string
				delRows.Scan(&id, &accountID)
				deleted = append(deleted, id, accountID)
				accountIDsToDelete = append(accountIDsToDelete, accountID)
			}
		}

		// Delete associated clearing codes and master account records
		if len(accountIDsToDelete) > 0 {
			// delete clearing codes first to avoid FK issues
			_, _ = db.Exec(`DELETE FROM masterclearingcode WHERE account_id = ANY($1)`, pq.Array(accountIDsToDelete))
			_, _ = db.Exec(`DELETE FROM masterbankaccount WHERE account_id = ANY($1)`, pq.Array(accountIDsToDelete))
		}

		// Approve remaining audit actions (exclude those that were pending delete)
		query := `UPDATE auditactionbankaccount SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) AND processing_status != 'PENDING_DELETE_APPROVAL' RETURNING action_id,account_id`
		rows, err := db.Query(query, checkerBy, req.Comment, pq.Array(req.ActionIDs))
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()
		var updated []string
		for rows.Next() {
			var id, accountID string
			rows.Scan(&id, &accountID)
			updated = append(updated, id, accountID)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"updated": updated,
			"deleted": deleted,
		})
	}
}

// GET handler to fetch all bank_id, bank_name (bank_short_name) for banks used by accounts
func GetBankNamesWithIDForAccount(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Invalid JSON")
			return
		}
		query := `
			SELECT DISTINCT m.bank_id, m.bank_name, m.bank_short_name
			FROM masterbank m
			JOIN masterbankaccount a ON a.bank_id = m.bank_id
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionbank a2
				WHERE a2.bank_id = m.bank_id
				ORDER BY requested_at DESC
				LIMIT 1
			) astatus ON TRUE
			WHERE m.active_status = 'Active' AND astatus.processing_status = 'APPROVED'
		`
		rows, err := db.Query(query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		var anyError error
		for rows.Next() {
			var bankID, bankName, bankShortName string
			if err := rows.Scan(&bankID, &bankName, &bankShortName); err != nil {
				anyError = err
				break
			}
			results = append(results, map[string]interface{}{
				"bank_id":         bankID,
				"bank_name":       bankName,
				"bank_short_name": getNullString(sql.NullString{String: bankShortName, Valid: bankShortName != ""}),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		if anyError != nil {
			api.RespondWithError(w, http.StatusInternalServerError, anyError.Error())
			return
		}
		if results == nil {
			results = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	}
}

// GET handler to fetch approved accounts with bank and entity names
func GetApprovedBankAccountsWithBankEntity(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		// accept optional user_id in body for parity with other handlers
		_ = json.NewDecoder(r.Body).Decode(&req)

		query := `
			SELECT
				a.account_id,
				a.account_number,
				a.account_nickname,
				b.bank_id,
				b.bank_name,
				e.entity_id,
				e.entity_name
			FROM masterbankaccount a
			LEFT JOIN masterbank b ON a.bank_id = b.bank_id
			LEFT JOIN masterentity e ON e.entity_id::text = a.entity_id
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionbankaccount aa
				WHERE aa.account_id = a.account_id
				ORDER BY requested_at DESC
				LIMIT 1
			) astatus ON TRUE
			WHERE astatus.processing_status = 'APPROVED' AND a.account_status = 'Active'
		`
		rows, err := db.Query(query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		var anyErr error
		for rows.Next() {
			var accountID, accountNumber string
			var accountNickname sql.NullString
			var bankID, bankName, entityID, entityName sql.NullString
			if err := rows.Scan(&accountID, &accountNumber, &accountNickname, &bankID, &bankName, &entityID, &entityName); err != nil {
				anyErr = err
				break
			}
			results = append(results, map[string]interface{}{
				"account_id":       accountID,
				"account_number":   accountNumber,
				"account_nickname": getNullString(accountNickname),
				"bank_id":          getNullString(bankID),
				"bank_name":        getNullString(bankName),
				"entity_id":        getNullString(entityID),
				"entity_name":      getNullString(entityName),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		if anyErr != nil {
			api.RespondWithError(w, http.StatusInternalServerError, anyErr.Error())
			return
		}
		if results == nil {
			results = make([]map[string]interface{}, 0)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"results": results,
		})
	}
}
