package bankstatement

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xuri/excelize/v2"
)

func ifaceToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case *string:
		if t == nil {
			return ""
		}
		return *t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

func ifaceToTimeString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case time.Time:
		return t.Format("2006-01-02 15:04:05")
	case *time.Time:
		if t == nil {
			return ""
		}
		return t.Format("2006-01-02 15:04:05")
	case string:
		return t
	case *string:
		if t == nil {
			return ""
		}
		return *t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

func getFileExt(filename string) string {
	return strings.ToLower(filepath.Ext(filename))
}

func parseBankStatementFile(file multipart.File, ext string) ([][]string, error) {
	if ext == ".csv" {
		r := csv.NewReader(file)
		return r.ReadAll()
	}
	if ext == ".xlsx" || ext == ".xls" {
		f, err := excelize.OpenReader(file)
		if err != nil {
			return nil, err
		}
		sheet := f.GetSheetName(0)
		rows, err := f.GetRows(sheet)
		if err != nil {
			return nil, err
		}
		return rows, nil
	}
	return nil, errors.New("unsupported file type")
}

func UploadBankStatement(pgxPool *pgxpool.Pool) http.HandlerFunc {
	// Helper to normalize date strings to YYYY-MM-DD
	normalizeDate := func(dateStr string) string {
		dateStr = strings.TrimSpace(dateStr)
		if dateStr == "" {
			return ""
		}
		layouts := []string{
			"2006-01-02",
			"02/01/06",
			"02/01/2006",
			"02-01-2006",
			"01/02/2006",
			"01-02-2006",
			"2006/01/02",
			"2006.01.02",
			"02.01.2006",
			"01.02.2006",
			"02-Jan-2006",
			"02-Jan-06",
			"2006/1/2",
			"2/1/2006",
			"2-1-2006",
			"1/2/2006",
			"1-2-2006",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, dateStr); err == nil {
				return t.Format("2006-01-02")
			}
		}
		// Try to handle 2-digit year (e.g., 10/12/25, 10-12-25, 10.12.25)
		var day, month, year string
		var sep string
		if strings.Count(dateStr, "/") == 2 {
			sep = "/"
		} else if strings.Count(dateStr, "-") == 2 {
			sep = "-"
		} else if strings.Count(dateStr, ".") == 2 {
			sep = "."
		}
		if sep != "" {
			parts := strings.Split(dateStr, sep)
			if len(parts) == 3 {
				day, month, year = parts[0], parts[1], parts[2]
				if len(year) == 2 {
					// Assume 20xx for years 00-49, 19xx for 50-99
					if year >= "00" && year <= "49" {
						year = "20" + year
					} else {
						year = "19" + year
					}
				}
				try := year + "-" + month + "-" + day
				if t, err := time.Parse("2006-01-02", try); err == nil {
					return t.Format("2006-01-02")
				}
			}
		}
		return "" // fallback: return empty string for invalid date
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		userID := ""
		if r.Header.Get("Content-Type") == "application/json" {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				api.RespondWithPayload(w, false, "user_id required in body", nil)
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue("user_id")
			if userID == "" {
				api.RespondWithPayload(w, false, "user_id required in form", nil)
				return
			}
		}

		userName := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithPayload(w, false, "User not found in active sessions", nil)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			api.RespondWithPayload(w, false, "Failed to parse multipart form", nil)
			return
		}
		files := r.MultipartForm.File["file"]
		if len(files) == 0 {
			api.RespondWithPayload(w, false, "No files uploaded", nil)
			return
		}
		batchIDs := make([]string, 0, len(files))
		for _, fileHeader := range files {
			file, err := fileHeader.Open()
			if err != nil {
				api.RespondWithPayload(w, false, "Failed to open file: "+fileHeader.Filename, nil)
				return
			}
			ext := getFileExt(fileHeader.Filename)
			records, err := parseBankStatementFile(file, ext)
			file.Close()
			if err != nil || len(records) < 2 {
				api.RespondWithPayload(w, false, "Invalid or empty file: "+fileHeader.Filename, nil)
				return
			}
			headerRow := records[0]
			dataRows := records[1:]
			batchID := uuid.New().String()
			batchIDs = append(batchIDs, batchID)
			colCount := len(headerRow)
			// Identify date columns
			dateCols := map[string]bool{
				"transactiondate":     true,
				"statementdate":       true,
				"old_statementdate":   true,
				"old_transactiondate": true,
			}
			copyRows := make([][]interface{}, len(dataRows))
			for i, row := range dataRows {
				vals := make([]interface{}, colCount+1)
				vals[0] = batchID
				for j := 0; j < colCount; j++ {
					val := ""
					if j < len(row) {
						val = row[j]
					}
					colName := strings.ToLower(headerRow[j])
					if dateCols[colName] {
						vals[j+1] = normalizeDate(val)
					} else {
						vals[j+1] = val
					}
				}
				copyRows[i] = vals
			}
			columns := append([]string{"upload_batch_id"}, headerRow...)
			_, err = pgxPool.CopyFrom(
				ctx,
				pgx.Identifier{"input_bank_statement_table"},
				columns,
				pgx.CopyFromRows(copyRows),
			)
			if err != nil {
				api.RespondWithPayload(w, false, "Failed to stage data: "+err.Error(), nil)
				return
			}
			mapRows, err := pgxPool.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_bank_statement`)
			if err != nil {
				api.RespondWithPayload(w, false, "Mapping error", nil)
				return
			}
			mapping := make(map[string]string)
			for mapRows.Next() {
				var src, tgt string
				if err := mapRows.Scan(&src, &tgt); err == nil {
					mapping[src] = tgt
				}
			}
			mapRows.Close()
			var srcCols, tgtCols []string
			for src, tgt := range mapping {
				srcCols = append(srcCols, src)
				tgtCols = append(tgtCols, tgt)
			}
			tgtCols = append(tgtCols, "createdby")
			srcColsStr := strings.Join(srcCols, ", ")
			tgtColsStr := strings.Join(tgtCols, ", ")

			// Insert rows and return generated bankstatement ids so we can create audit entries
			rows2, err := pgxPool.Query(ctx, fmt.Sprintf(`
		       INSERT INTO bank_statement (%s)
		       SELECT %s, $1
		       FROM input_bank_statement_table s
		       WHERE s.upload_batch_id = $2
		       RETURNING bankstatementid
		   `, tgtColsStr, srcColsStr), userName, batchID)
			if err != nil {
				api.RespondWithPayload(w, false, "Final insert error: "+err.Error(), nil)
				return
			}
			var newIDs []string
			for rows2.Next() {
				var id string
				if err := rows2.Scan(&id); err == nil {
					newIDs = append(newIDs, id)
				}
			}
			rows2.Close()
			// Insert audit action rows for created bank statements
			for _, bsid := range newIDs {
				if _, aerr := pgxPool.Exec(ctx, `INSERT INTO auditactionbankstatement (bankstatementid, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',NULL,$2,now())`, bsid, userName); aerr != nil {
					// log but don't fail the entire upload for audit insert error
					// (could collect and return these later if desired)
					fmt.Println("audit insert error:", aerr)
				}
			}
			// --- Begin: Insert transactions and update balances ---
			// Build header index for easy lookup
			headerIndex := make(map[string]int)
			for idx, col := range headerRow {
				headerIndex[strings.ToLower(col)] = idx
			}
			var latestClosingBalance string
			var accountNo string
			for i, row := range dataRows {
				// Defensive: check length
				if len(row) < colCount {
					continue
				}
				txnDate := ""
				if idx, ok := headerIndex["transactiondate"]; ok && idx < len(row) {
					txnDate = row[idx]
				}
				desc := ""
				if idx, ok := headerIndex["description"]; ok && idx < len(row) {
					desc = row[idx]
				}
				debit := ""
				if idx, ok := headerIndex["debitamount"]; ok && idx < len(row) {
					debit = row[idx]
				}
				credit := ""
				if idx, ok := headerIndex["creditamount"]; ok && idx < len(row) {
					credit = row[idx]
				}
				bal := ""
				if idx, ok := headerIndex["balanceaftertxn"]; ok && idx < len(row) {
					bal = row[idx]
				}
				accNo := ""
				if idx, ok := headerIndex["account_number"]; ok && idx < len(row) {
					accNo = row[idx]
				}
				// Save last closing balance and account number for update
				if i == len(dataRows)-1 {
					latestClosingBalance = bal
					accountNo = accNo
				}
				// Insert transaction with idempotency
				_, err := pgxPool.Exec(ctx, `
                    INSERT INTO bank_statement_transaction (
                        bankstatementid, account_number, transactiondate, description, debitamount, creditamount, balanceaftertxn
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (account_number, transactiondate, description, debitamount, creditamount) DO NOTHING
                `, batchID, accNo, txnDate, desc, debit, credit, bal)
				if err != nil {
					fmt.Println("transaction insert error:", err)
				}
			}
			// Update bank balance for the account
			if accountNo != "" && latestClosingBalance != "" {
				_, err := pgxPool.Exec(ctx, `
                    UPDATE bank_balances_manual
                    SET closing_balance = $1
                    WHERE account_no = $2
                `, latestClosingBalance, accountNo)
				if err != nil {
					fmt.Println("bank balance update error:", err)
				}
			}
			// --- End: Insert transactions and update balances ---
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"batch_ids": batchIDs, "message": "All bank statements uploaded and processed"})
	}
}

func GetBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		query := `
			SELECT 
				s.bankstatementid,
				s.entityid,
				s.account_number,
				s.statementdate,
				s.openingbalance,
				s.closingbalance,
				s.currencycode,
				s.transactiondate,
				s.description,
				s.debitamount,
				s.creditamount,
				s.balanceaftertxn,
				s.createdby,
				s.createdat,
				s.status,
				s.is_deleted,
				s.old_entityid,
				s.old_account_number,
				s.old_statementdate,
				s.old_openingbalance,
				s.old_closingbalance,
				s.old_transactiondate,
				s.old_description,
				s.old_status,
				s.old_accountholdername,
				s.old_branchname,
				s.old_ifsccode,
				s.old_statement_period,
				s.old_chequerefno,
				s.old_withdrawalamount,
				s.old_depositamount,
				s.old_modeoftransaction,
				e.entity_name,
				b.bank_name
			FROM bank_statement s
			LEFT JOIN LATERAL (
				SELECT reason FROM auditactionbankstatement ab WHERE ab.bankstatementid = s.bankstatementid ORDER BY requested_at DESC LIMIT 1
			) la ON TRUE
			JOIN masterbankaccount mba ON s.account_number = mba.account_number
			JOIN masterentity e ON mba.entity_id = e.entity_id
			JOIN masterbank b ON mba.bank_id = b.bank_id
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()

		type Item struct {
			BankStatementID       string     `json:"bankstatementid"`
			EntityID              string     `json:"entityid"`
			AccountNumber         string     `json:"account_number"`
			StatementDate         time.Time  `json:"statementdate"`
			OpeningBalance        *float64   `json:"openingbalance"`
			ClosingBalance        *float64   `json:"closingbalance"`
			CurrencyCode          string     `json:"currencycode"`
			TransactionDate       time.Time  `json:"transactiondate"`
			Description           string     `json:"description"`
			DebitAmount           *float64   `json:"debitamount"`
			CreditAmount          *float64   `json:"creditamount"`
			BalanceAfterTxn       *float64   `json:"balanceaftertxn"`
			CreatedBy             string     `json:"createdby"`
			CreatedAt             time.Time  `json:"createdat"`
			Status                string     `json:"status"`
			IsDeleted             bool       `json:"is_deleted"`
			EntityName            string     `json:"entity_name"`
			BankName              string     `json:"bank_name"`
			OldEntityIDDB         string     `json:"old_entityid_db"`
			OldAccountNoDB        string     `json:"old_account_number_db"`
			OldStatementDateDB    *time.Time `json:"old_statementdate_db"`
			OldOpeningBalanceDB   *float64   `json:"old_openingbalance_db"`
			OldClosingBalanceDB   *float64   `json:"old_closingbalance_db"`
			OldTransactionDateDB  *time.Time `json:"old_transactiondate_db"`
			OldDescriptionDB      string     `json:"old_description_db"`
			OldStatusDB           string     `json:"old_status_db"`
			OldAccountholderDB    string     `json:"old_accountholdername_db"`
			OldBranchNameDB       string     `json:"old_branchname_db"`
			OldIFSCDB             string     `json:"old_ifsccode_db"`
			OldStatementPeriodDB  string     `json:"old_statement_period_db"`
			OldChequeRefNoDB      string     `json:"old_chequerefno_db"`
			OldWithdrawalAmountDB *float64   `json:"old_withdrawalamount_db"`
			OldDepositAmountDB    *float64   `json:"old_depositamount_db"`
			OldModeOfTxnDB        string     `json:"old_modeoftransaction_db"`
			OldEntityID           string     `json:"old_entityid"`
			OldAccountNo          string     `json:"old_account_number"`
			OldDescription        string     `json:"old_description"`
			CreatedByAudit        string     `json:"created_by"`
			CreatedAtAudit        string     `json:"created_at"`
			EditedByAudit         string     `json:"edited_by"`
			EditedAtAudit         string     `json:"edited_at"`
			DeletedByAudit        string     `json:"deleted_by"`
			DeletedAtAudit        string     `json:"deleted_at"`
		}

		var stmtIDs []string
		tempItems := make([]Item, 0)
		for rows.Next() {
			var it Item
			var desc *string
			var isDeleted *bool
			var createdBy *string
			var reasonRaw *string
			var oldEntity, oldAccount *string
			var oldStmtDate *time.Time
			var oldOpenBal, oldCloseBal *float64
			var oldTxnDate *time.Time
			var oldDesc *string
			var oldStatus *string
			var oldAccHolder *string
			var oldBranch *string
			var oldIfsc *string
			var oldStmtPeriod *string
			var oldCheque *string
			var oldWithdraw, oldDeposit *float64
			var oldMode *string
			if err := rows.Scan(
				&it.BankStatementID,
				&it.EntityID,
				&it.AccountNumber,
				&it.StatementDate,
				&it.OpeningBalance,
				&it.ClosingBalance,
				&it.CurrencyCode,
				&it.TransactionDate,
				&desc,
				&it.DebitAmount,
				&it.CreditAmount,
				&it.BalanceAfterTxn,
				&createdBy,
				&it.CreatedAt,
				&it.Status,
				&isDeleted,
				&oldEntity,
				&oldAccount,
				&oldStmtDate,
				&oldOpenBal,
				&oldCloseBal,
				&oldTxnDate,
				&oldDesc,
				&oldStatus,
				&oldAccHolder,
				&oldBranch,
				&oldIfsc,
				&oldStmtPeriod,
				&oldCheque,
				&oldWithdraw,
				&oldDeposit,
				&oldMode,
				&it.EntityName,
				&it.BankName,
				&reasonRaw,
			); err != nil {
				api.RespondWithPayload(w, false, err.Error(), nil)
				return
			}
			if desc != nil {
				it.Description = *desc
			} else {
				it.Description = ""
			}
			if createdBy != nil {
				it.CreatedBy = *createdBy
			} else {
				it.CreatedBy = ""
			}
			if isDeleted != nil {
				it.IsDeleted = *isDeleted
			} else {
				it.IsDeleted = false
			}
			it.OldEntityID = ""
			it.OldAccountNo = ""
			it.OldDescription = ""
			// populate DB-backed old_* fields into the Item
			if oldEntity != nil {
				it.OldEntityIDDB = *oldEntity
			} else {
				it.OldEntityIDDB = ""
			}
			if oldAccount != nil {
				it.OldAccountNoDB = *oldAccount
			} else {
				it.OldAccountNoDB = ""
			}
			if oldStmtDate != nil {
				it.OldStatementDateDB = oldStmtDate
			} else {
				it.OldStatementDateDB = nil
			}
			if oldOpenBal != nil {
				it.OldOpeningBalanceDB = oldOpenBal
			} else {
				it.OldOpeningBalanceDB = nil
			}
			if oldCloseBal != nil {
				it.OldClosingBalanceDB = oldCloseBal
			} else {
				it.OldClosingBalanceDB = nil
			}
			if oldTxnDate != nil {
				it.OldTransactionDateDB = oldTxnDate
			} else {
				it.OldTransactionDateDB = nil
			}
			if oldDesc != nil {
				it.OldDescriptionDB = *oldDesc
			} else {
				it.OldDescriptionDB = ""
			}
			if oldStatus != nil {
				it.OldStatusDB = *oldStatus
			} else {
				it.OldStatusDB = ""
			}
			if oldAccHolder != nil {
				it.OldAccountholderDB = *oldAccHolder
			} else {
				it.OldAccountholderDB = ""
			}
			if oldBranch != nil {
				it.OldBranchNameDB = *oldBranch
			} else {
				it.OldBranchNameDB = ""
			}
			if oldIfsc != nil {
				it.OldIFSCDB = *oldIfsc
			} else {
				it.OldIFSCDB = ""
			}
			if oldStmtPeriod != nil {
				it.OldStatementPeriodDB = *oldStmtPeriod
			} else {
				it.OldStatementPeriodDB = ""
			}
			if oldCheque != nil {
				it.OldChequeRefNoDB = *oldCheque
			} else {
				it.OldChequeRefNoDB = ""
			}
			if oldWithdraw != nil {
				it.OldWithdrawalAmountDB = oldWithdraw
			} else {
				it.OldWithdrawalAmountDB = nil
			}
			if oldDeposit != nil {
				it.OldDepositAmountDB = oldDeposit
			} else {
				it.OldDepositAmountDB = nil
			}
			if oldMode != nil {
				it.OldModeOfTxnDB = *oldMode
			} else {
				it.OldModeOfTxnDB = ""
			}
			if reasonRaw != nil && *reasonRaw != "" {
				var m map[string]interface{}
				if err := json.Unmarshal([]byte(*reasonRaw), &m); err == nil {
					if v, ok := m["old_entityid"]; ok {
						if s, ok2 := v.(string); ok2 {
							it.OldEntityID = s
						}
					}
					if v, ok := m["old_account_number"]; ok {
						if s, ok2 := v.(string); ok2 {
							it.OldAccountNo = s
						}
					}
					if v, ok := m["old_description"]; ok {
						if s, ok2 := v.(string); ok2 {
							it.OldDescription = s
						}
					}
				}
			}
			tempItems = append(tempItems, it)
			stmtIDs = append(stmtIDs, it.BankStatementID)
		}
		if len(stmtIDs) > 0 {
			auditQ := `SELECT bankstatementid, actiontype, requested_by, requested_at FROM auditactionbankstatement WHERE bankstatementid = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC`
			arows, aerr := pgxPool.Query(ctx, auditQ, stmtIDs)
			if aerr == nil {
				defer arows.Close()
				type AuditInfo struct {
					CreatedBy string
					CreatedAt string
					EditedBy  string
					EditedAt  string
					DeletedBy string
					DeletedAt string
				}
				auditMap := make(map[string]*AuditInfo)
				for arows.Next() {
					var bid, atype string
					var rby interface{}
					var rat interface{}
					if err := arows.Scan(&bid, &atype, &rby, &rat); err == nil {
						if auditMap[bid] == nil {
							auditMap[bid] = &AuditInfo{}
						}
						switch atype {
						case "CREATE":
							if auditMap[bid].CreatedBy == "" {
								auditMap[bid].CreatedBy = ifaceToString(rby)
								auditMap[bid].CreatedAt = ifaceToTimeString(rat)
							}
						case "EDIT":
							if auditMap[bid].EditedBy == "" {
								auditMap[bid].EditedBy = ifaceToString(rby)
								auditMap[bid].EditedAt = ifaceToTimeString(rat)
							}
						case "DELETE":
							if auditMap[bid].DeletedBy == "" {
								auditMap[bid].DeletedBy = ifaceToString(rby)
								auditMap[bid].DeletedAt = ifaceToTimeString(rat)
							}
						}
					}
				}
				for i := range tempItems {
					bid := tempItems[i].BankStatementID
					if a, ok := auditMap[bid]; ok {
						tempItems[i].CreatedByAudit = a.CreatedBy
						tempItems[i].CreatedAtAudit = a.CreatedAt
						tempItems[i].EditedByAudit = a.EditedBy
						tempItems[i].EditedAtAudit = a.EditedAt
						tempItems[i].DeletedByAudit = a.DeletedBy
						tempItems[i].DeletedAtAudit = a.DeletedAt
					}
				}
			}
		}
		if rows.Err() != nil {
			api.RespondWithPayload(w, false, rows.Err().Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", tempItems)
	}
}

func BulkApproveBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string   `json:"user_id"`
			BankStatementIDs []string `json:"bankstatement_ids"`
			Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "Invalid JSON", nil)
			return
		}
		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithPayload(w, false, "Invalid user_id or session", nil)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, "failed to start transaction: "+err.Error(), nil)
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()
		sel := `SELECT DISTINCT ON (bankstatementid) action_id, bankstatementid, actiontype, processing_status FROM auditactionbankstatement WHERE bankstatementid = ANY($1) ORDER BY bankstatementid, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.BankStatementIDs)
		if err != nil {
			api.RespondWithPayload(w, false, "failed to fetch audit rows: "+err.Error(), nil)
			return
		}
		defer rows.Close()
		actionIDs := make([]string, 0)
		found := map[string]bool{}
		cannotApprove := []string{}
		actionTypeBy := map[string]string{}
		for rows.Next() {
			var aid, bid, atype, pstatus string
			if err := rows.Scan(&aid, &bid, &atype, &pstatus); err != nil {
				api.RespondWithPayload(w, false, "failed to scan audit rows: "+err.Error(), nil)
				return
			}
			found[bid] = true
			actionIDs = append(actionIDs, aid)
			actionTypeBy[bid] = atype
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotApprove = append(cannotApprove, bid)
			}
		}
		missing := []string{}
		for _, bid := range req.BankStatementIDs {
			if !found[bid] {
				missing = append(missing, bid)
			}
		}
		if len(missing) > 0 || len(cannotApprove) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for bankstatement_ids: %v. ", missing)
			}
			if len(cannotApprove) > 0 {
				msg += fmt.Sprintf("cannot approve already approved bankstatement_ids: %v", cannotApprove)
			}
			api.RespondWithPayload(w, false, msg, nil)
			return
		}
		upd := `UPDATE auditactionbankstatement SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, bankstatementid, actiontype`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithPayload(w, false, "failed to update audit rows: "+err.Error(), nil)
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		deleteIDs := make([]string, 0)
		for urows.Next() {
			var aid, bid, atype string
			if err := urows.Scan(&aid, &bid, &atype); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "bankstatementid": bid, "action_type": atype})
				if strings.ToUpper(strings.TrimSpace(atype)) == "DELETE" {
					deleteIDs = append(deleteIDs, bid)
				}
			}
		}
		if len(updated) != len(actionIDs) {
			api.RespondWithPayload(w, false, fmt.Sprintf("updated %d of %d actions, aborting", len(updated), len(actionIDs)), nil)
			return
		}
		if len(deleteIDs) > 0 {
			sd := `UPDATE bank_statement SET is_deleted = true WHERE bankstatementid = ANY($1)`
			if _, err := tx.Exec(ctx, sd, deleteIDs); err != nil {
				api.RespondWithPayload(w, false, "failed to soft-delete bank_statement rows: "+err.Error(), nil)
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, "commit failed: "+err.Error(), nil)
			return
		}
		committed = true
		api.RespondWithPayload(w, true, "", map[string]interface{}{"updated": updated})
	}
}
func BulkRejectBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			IDs     []string `json:"bankstatement_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "Invalid JSON", nil)
			return
		}
		checkerBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				checkerBy = s.Email
				break
			}
		}
		if checkerBy == "" {
			api.RespondWithPayload(w, false, "Invalid user_id or session", nil)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, "failed to start transaction: "+err.Error(), nil)
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()
		sel := `SELECT DISTINCT ON (bankstatementid) action_id, bankstatementid, processing_status FROM auditactionbankstatement WHERE bankstatementid = ANY($1) ORDER BY bankstatementid, requested_at DESC`
		rows, err := tx.Query(ctx, sel, req.IDs)
		if err != nil {
			api.RespondWithPayload(w, false, "failed to fetch audit rows: "+err.Error(), nil)
			return
		}
		defer rows.Close()
		actionIDs := make([]string, 0)
		found := map[string]bool{}
		cannotReject := []string{}
		for rows.Next() {
			var aid, bid, pstatus string
			if err := rows.Scan(&aid, &bid, &pstatus); err != nil {
				api.RespondWithPayload(w, false, "failed to scan audit rows: "+err.Error(), nil)
				return
			}
			found[bid] = true
			actionIDs = append(actionIDs, aid)
			if strings.ToUpper(strings.TrimSpace(pstatus)) == "APPROVED" {
				cannotReject = append(cannotReject, bid)
			}
		}
		missing := []string{}
		for _, bid := range req.IDs {
			if !found[bid] {
				missing = append(missing, bid)
			}
		}
		if len(missing) > 0 || len(cannotReject) > 0 {
			msg := ""
			if len(missing) > 0 {
				msg += fmt.Sprintf("no audit action found for bankstatement_ids: %v. ", missing)
			}
			if len(cannotReject) > 0 {
				msg += fmt.Sprintf("cannot reject already approved bankstatement_ids: %v", cannotReject)
			}
			api.RespondWithPayload(w, false, msg, nil)
			return
		}
		upd := `UPDATE auditactionbankstatement SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE action_id = ANY($3) RETURNING action_id, bankstatementid`
		urows, err := tx.Query(ctx, upd, checkerBy, req.Comment, actionIDs)
		if err != nil {
			api.RespondWithPayload(w, false, "failed to update audit rows: "+err.Error(), nil)
			return
		}
		defer urows.Close()
		updated := []map[string]interface{}{}
		for urows.Next() {
			var aid, bid string
			if err := urows.Scan(&aid, &bid); err == nil {
				updated = append(updated, map[string]interface{}{"action_id": aid, "bankstatementid": bid})
			}
		}
		if len(updated) != len(actionIDs) {
			api.RespondWithPayload(w, false, fmt.Sprintf("updated %d of %d actions, aborting", len(updated), len(actionIDs)), nil)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, "commit failed: "+err.Error(), nil)
			return
		}
		committed = true
		api.RespondWithPayload(w, true, "", map[string]interface{}{"updated": updated})
	}
}

func BulkDeleteBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			IDs    []string `json:"bankstatement_ids"`
			Reason string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "Invalid JSON", nil)
			return
		}
		requestedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Email
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithPayload(w, false, "Invalid user_id or session", nil)
			return
		}
		if len(req.IDs) == 0 {
			api.RespondWithPayload(w, false, "bankstatement_ids required", nil)
			return
		}
		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, "failed to start transaction: "+err.Error(), nil)
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()
		q := `INSERT INTO auditactionbankstatement (bankstatementid, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`
		for _, id := range req.IDs {
			if strings.TrimSpace(id) == "" {
				tx.Rollback(ctx)
				api.RespondWithPayload(w, false, "empty bankstatement id provided", nil)
				return
			}
			if _, err := tx.Exec(ctx, q, id, req.Reason, requestedBy); err != nil {
				tx.Rollback(ctx)
				api.RespondWithPayload(w, false, err.Error(), nil)
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, "commit failed: "+err.Error(), nil)
			return
		}
		committed = true
		api.RespondWithPayload(w, true, "", map[string]interface{}{"success": true})
	}
}

func GetAllBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			SELECT
				s.bankstatementid,
				s.entityid,
				s.account_number,
				s.statementdate,
				s.openingbalance,
				s.closingbalance,
				s.transactiondate,
				s.description,
				s.status,
				s.is_deleted,
				s.old_entityid,
				s.old_account_number,
				s.old_statementdate,
				s.old_openingbalance,
				s.old_closingbalance,
				s.old_transactiondate,
				s.old_description,
				s.old_status,
				s.old_accountholdername,
				s.old_branchname,
				s.old_ifsccode,
				s.old_statement_period,
				s.old_chequerefno,
				s.old_withdrawalamount,
				s.old_depositamount,
				s.old_modeoftransaction,
				s.accountholdername,
				s.branchname,
				s.ifsccode,
				s.statement_period,
				s.chequerefno,
				s.withdrawalamount,
				s.depositamount,
				s.modeoftransaction,
				e.entity_name,
				mb.bank_name,
				a.processing_status,
				a.requested_by,
				a.requested_at,
				a.checker_by,
				a.checker_at,
				a.reason
			FROM bank_statement s
			LEFT JOIN masterbankaccount mba ON s.account_number = mba.account_number
			LEFT JOIN masterentity e ON s.entityid = e.entity_id
			LEFT JOIN masterbank mb ON mba.bank_id = mb.bank_id
			LEFT JOIN LATERAL (
				SELECT processing_status, requested_by, requested_at, checker_by, checker_at ,reason
				FROM auditactionbankstatement ab
				WHERE ab.bankstatementid = s.bankstatementid
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON TRUE
			
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithPayload(w, false, err.Error(), nil)
			return
		}
		defer rows.Close()

		type Row struct {
			BankStatementID  string     `json:"bankstatementid"`
			EntityID         string     `json:"entityid"`
			AccountNumber    string     `json:"account_number"`
			StatementDate    time.Time  `json:"statementdate"`
			OpeningBalance   *float64   `json:"openingbalance"`
			ClosingBalance   *float64   `json:"closingbalance"`
			TransactionDate  time.Time  `json:"transactiondate"`
			Description      string     `json:"description"`
			Status           string     `json:"status"`
			IsDeleted        bool       `json:"is_deleted"`
			Accountholder    string     `json:"accountholdername"`
			BranchName       string     `json:"branchname"`
			IFSC             string     `json:"ifsccode"`
			StatementPeriod  string     `json:"statement_period"`
			ChequeRefNo      string     `json:"chequerefno"`
			WithdrawalAmount *float64   `json:"withdrawalamount"`
			DepositAmount    *float64   `json:"depositamount"`
			ModeOfTxn        string     `json:"modeoftransaction"`
			EntityName       string     `json:"entity_name"`
			BankName         string     `json:"bank_name"`
			ProcessingStatus string     `json:"processing_status"`
			RequestedBy      string     `json:"requested_by"`
			RequestedAt      *time.Time `json:"requested_at"`
			CheckerBy        string     `json:"checker_by"`
			CheckerAt        *time.Time `json:"checker_at"`
			// DB-backed old_* columns
			OldEntityIDDB         string     `json:"old_entityid_db"`
			OldAccountNoDB        string     `json:"old_account_number_db"`
			OldStatementDateDB    *time.Time `json:"old_statementdate_db"`
			OldOpeningBalanceDB   *float64   `json:"old_openingbalance_db"`
			OldClosingBalanceDB   *float64   `json:"old_closingbalance_db"`
			OldTransactionDateDB  *time.Time `json:"old_transactiondate_db"`
			OldDescriptionDB      string     `json:"old_description_db"`
			OldStatusDB           string     `json:"old_status_db"`
			OldAccountholderDB    string     `json:"old_accountholdername_db"`
			OldBranchNameDB       string     `json:"old_branchname_db"`
			OldIFSCDB             string     `json:"old_ifsccode_db"`
			OldStatementPeriodDB  string     `json:"old_statement_period_db"`
			OldChequeRefNoDB      string     `json:"old_chequerefno_db"`
			OldWithdrawalAmountDB *float64   `json:"old_withdrawalamount_db"`
			OldDepositAmountDB    *float64   `json:"old_depositamount_db"`
			OldModeOfTxnDB        string     `json:"old_modeoftransaction_db"`
			// audit-created/edited/deleted info
			CreatedBy string `json:"created_by"`
			CreatedAt string `json:"created_at"`
			EditedBy  string `json:"edited_by"`
			EditedAt  string `json:"edited_at"`
			DeletedBy string `json:"deleted_by"`
			DeletedAt string `json:"deleted_at"`
			// raw reason from latest audit (used to parse old_* fields)
			ReasonRaw *string `json:"-"`
			// old_* placeholders
			OldEntityID    string `json:"old_entityid"`
			OldAccountNo   string `json:"old_account_number"`
			OldDescription string `json:"old_description"`
		}

		out := make([]Row, 0)
		for rows.Next() {
			var rrow Row
			// scan nullable strings into pointers
			var desc, branch, ifsc, stmtPeriod, chequeRef, modeOfTxn, entName, bankName, procStatus, reqBy, chkBy *string
			var isDeleted *bool
			var reasonRaw *string
			// DB-backed old_* scan targets
			var oldEntity, oldAccount *string
			var oldStmtDate *time.Time
			var oldOpenBal, oldCloseBal *float64
			var oldTxnDate *time.Time
			var oldDesc *string
			var oldStatus *string
			var oldAccHolder *string
			var oldBranch *string
			var oldIfsc *string
			var oldStmtPeriod *string
			var oldCheque *string
			var oldWithdraw, oldDeposit *float64
			var oldMode *string
			if err := rows.Scan(
				&rrow.BankStatementID,
				&rrow.EntityID,
				&rrow.AccountNumber,
				&rrow.StatementDate,
				&rrow.OpeningBalance,
				&rrow.ClosingBalance,
				&rrow.TransactionDate,
				&desc,
				&rrow.Status,
				&isDeleted,
				&oldEntity,
				&oldAccount,
				&oldStmtDate,
				&oldOpenBal,
				&oldCloseBal,
				&oldTxnDate,
				&oldDesc,
				&oldStatus,
				&oldAccHolder,
				&oldBranch,
				&oldIfsc,
				&oldStmtPeriod,
				&oldCheque,
				&oldWithdraw,
				&oldDeposit,
				&oldMode,
				&rrow.Accountholder,
				&branch,
				&ifsc,
				&stmtPeriod,
				&chequeRef,
				&rrow.WithdrawalAmount,
				&rrow.DepositAmount,
				&modeOfTxn,
				&entName,
				&bankName,
				&procStatus,
				&reqBy,
				&rrow.RequestedAt,
				&chkBy,
				&rrow.CheckerAt,
				&reasonRaw,
			); err != nil {
				api.RespondWithPayload(w, false, err.Error(), nil)
				return
			}
			// normalize nullable strings to empty
			if desc != nil {
				rrow.Description = *desc
			} else {
				rrow.Description = ""
			}
			if branch != nil {
				rrow.BranchName = *branch
			} else {
				rrow.BranchName = ""
			}
			if ifsc != nil {
				rrow.IFSC = *ifsc
			} else {
				rrow.IFSC = ""
			}
			if stmtPeriod != nil {
				rrow.StatementPeriod = *stmtPeriod
			} else {
				rrow.StatementPeriod = ""
			}
			if chequeRef != nil {
				rrow.ChequeRefNo = *chequeRef
			} else {
				rrow.ChequeRefNo = ""
			}
			if modeOfTxn != nil {
				rrow.ModeOfTxn = *modeOfTxn
			} else {
				rrow.ModeOfTxn = ""
			}
			if entName != nil {
				rrow.EntityName = *entName
			} else {
				rrow.EntityName = ""
			}
			if bankName != nil {
				rrow.BankName = *bankName
			} else {
				rrow.BankName = ""
			}
			if procStatus != nil {
				rrow.ProcessingStatus = *procStatus
			} else {
				rrow.ProcessingStatus = ""
			}
			if isDeleted != nil {
				rrow.IsDeleted = *isDeleted
			} else {
				rrow.IsDeleted = false
			}
			if reqBy != nil {
				rrow.RequestedBy = *reqBy
			} else {
				rrow.RequestedBy = ""
			}
			if chkBy != nil {
				rrow.CheckerBy = *chkBy
			} else {
				rrow.CheckerBy = ""
			}
			rrow.ReasonRaw = reasonRaw
			// old_* defaults
			rrow.OldEntityID = ""
			rrow.OldAccountNo = ""
			rrow.OldDescription = ""
			// populate DB-backed old_* fields into the Row
			if oldEntity != nil {
				rrow.OldEntityIDDB = *oldEntity
			} else {
				rrow.OldEntityIDDB = ""
			}
			if oldAccount != nil {
				rrow.OldAccountNoDB = *oldAccount
			} else {
				rrow.OldAccountNoDB = ""
			}
			if oldStmtDate != nil {
				rrow.OldStatementDateDB = oldStmtDate
			} else {
				rrow.OldStatementDateDB = nil
			}
			if oldOpenBal != nil {
				rrow.OldOpeningBalanceDB = oldOpenBal
			} else {
				rrow.OldOpeningBalanceDB = nil
			}
			if oldCloseBal != nil {
				rrow.OldClosingBalanceDB = oldCloseBal
			} else {
				rrow.OldClosingBalanceDB = nil
			}
			if oldTxnDate != nil {
				rrow.OldTransactionDateDB = oldTxnDate
			} else {
				rrow.OldTransactionDateDB = nil
			}
			if oldDesc != nil {
				rrow.OldDescriptionDB = *oldDesc
			} else {
				rrow.OldDescriptionDB = ""
			}
			if oldStatus != nil {
				rrow.OldStatusDB = *oldStatus
			} else {
				rrow.OldStatusDB = ""
			}
			if oldAccHolder != nil {
				rrow.OldAccountholderDB = *oldAccHolder
			} else {
				rrow.OldAccountholderDB = ""
			}
			if oldBranch != nil {
				rrow.OldBranchNameDB = *oldBranch
			} else {
				rrow.OldBranchNameDB = ""
			}
			if oldIfsc != nil {
				rrow.OldIFSCDB = *oldIfsc
			} else {
				rrow.OldIFSCDB = ""
			}
			if oldStmtPeriod != nil {
				rrow.OldStatementPeriodDB = *oldStmtPeriod
			} else {
				rrow.OldStatementPeriodDB = ""
			}
			if oldCheque != nil {
				rrow.OldChequeRefNoDB = *oldCheque
			} else {
				rrow.OldChequeRefNoDB = ""
			}
			if oldWithdraw != nil {
				rrow.OldWithdrawalAmountDB = oldWithdraw
			} else {
				rrow.OldWithdrawalAmountDB = nil
			}
			if oldDeposit != nil {
				rrow.OldDepositAmountDB = oldDeposit
			} else {
				rrow.OldDepositAmountDB = nil
			}
			if oldMode != nil {
				rrow.OldModeOfTxnDB = *oldMode
			} else {
				rrow.OldModeOfTxnDB = ""
			}

			// If DB-backed old values are present, prefer them for the public old_* fields
			if rrow.OldEntityIDDB != "" {
				rrow.OldEntityID = rrow.OldEntityIDDB
			}
			if rrow.OldAccountNoDB != "" {
				rrow.OldAccountNo = rrow.OldAccountNoDB
			}
			if rrow.OldDescriptionDB != "" {
				rrow.OldDescription = rrow.OldDescriptionDB
			}

			// try parsing reason JSON for old_* values if present and DB did not provide them
			if (rrow.OldEntityID == "" || rrow.OldAccountNo == "" || rrow.OldDescription == "") && reasonRaw != nil && *reasonRaw != "" {
				var m map[string]interface{}
				if err := json.Unmarshal([]byte(*reasonRaw), &m); err == nil {
					if rrow.OldEntityID == "" {
						if v, ok := m["old_entityid"]; ok {
							if s, ok2 := v.(string); ok2 {
								rrow.OldEntityID = s
							}
						}
					}
					if rrow.OldAccountNo == "" {
						if v, ok := m["old_account_number"]; ok {
							if s, ok2 := v.(string); ok2 {
								rrow.OldAccountNo = s
							}
						}
					}
					if rrow.OldDescription == "" {
						if v, ok := m["old_description"]; ok {
							if s, ok2 := v.(string); ok2 {
								rrow.OldDescription = s
							}
						}
					}
				}
			}

			out = append(out, rrow)
		}
		if rows.Err() != nil {
			api.RespondWithPayload(w, false, rows.Err().Error(), nil)
			return
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// CreateBankStatements inserts one or more bank_statement rows and creates
// corresponding auditactionbankstatement rows with PENDING_APPROVAL status.
func CreateBankStatements(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
			Reason string `json:"reason,omitempty"`
			Rows   []struct {
				EntityID         string   `json:"entityid"`
				AccountNumber    string   `json:"account_number"`
				StatementDate    string   `json:"statementdate"`
				TransactionDate  string   `json:"transactiondate"`
				OpeningBalance   *float64 `json:"openingbalance,omitempty"`
				ClosingBalance   *float64 `json:"closingbalance,omitempty"`
				Description      *string  `json:"description,omitempty"`
				Status           *string  `json:"status,omitempty"`
				Accountholder    *string  `json:"accountholdername,omitempty"`
				BranchName       *string  `json:"branchname,omitempty"`
				IFSC             *string  `json:"ifsccode,omitempty"`
				StatementPeriod  *string  `json:"statement_period,omitempty"`
				ChequeRefNo      *string  `json:"chequerefno,omitempty"`
				WithdrawalAmount *float64 `json:"withdrawalamount,omitempty"`
				DepositAmount    *float64 `json:"depositamount,omitempty"`
				ModeOfTxn        *string  `json:"modeoftransaction,omitempty"`
			} `json:"rows"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.Rows) == 0 {
			api.RespondWithPayload(w, false, "Invalid JSON or missing user_id/rows", nil)
			return
		}

		// validate session / get createdBy
		createdBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				createdBy = s.Name
				break
			}
		}
		if createdBy == "" {
			api.RespondWithPayload(w, false, "Invalid user_id or session", nil)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, "Failed to begin transaction: "+err.Error(), nil)
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		created := make([]string, 0, len(req.Rows))

		insertSQL := `INSERT INTO bank_statement (
			entityid, account_number, statementdate, openingbalance, closingbalance,
			transactiondate, description, status, accountholdername, branchname,
			ifsccode, statement_period, chequerefno, withdrawalamount, depositamount, modeoftransaction
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING bankstatementid`

		auditQ := `INSERT INTO auditactionbankstatement (bankstatementid, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,$3,now())`

		for _, row := range req.Rows {
			// basic validation
			missing := []string{}
			if strings.TrimSpace(row.EntityID) == "" {
				missing = append(missing, "entityid")
			}
			if strings.TrimSpace(row.AccountNumber) == "" {
				missing = append(missing, "account_number")
			}
			if strings.TrimSpace(row.StatementDate) == "" {
				missing = append(missing, "statementdate")
			}
			if strings.TrimSpace(row.TransactionDate) == "" {
				missing = append(missing, "transactiondate")
			}
			if len(missing) > 0 {
				api.RespondWithPayload(w, false, "Missing required fields: "+strings.Join(missing, ","), nil)
				return
			}

			var id string
			// normalize nil string pointers to empty strings for DB insert
			desc := ""
			if row.Description != nil {
				desc = *row.Description
			}
			status := ""
			if row.Status != nil {
				status = *row.Status
			}
			accHolder := ""
			if row.Accountholder != nil {
				accHolder = *row.Accountholder
			}
			branch := ""
			if row.BranchName != nil {
				branch = *row.BranchName
			}
			ifsc := ""
			if row.IFSC != nil {
				ifsc = *row.IFSC
			}
			stmtPeriod := ""
			if row.StatementPeriod != nil {
				stmtPeriod = *row.StatementPeriod
			}
			cheque := ""
			if row.ChequeRefNo != nil {
				cheque = *row.ChequeRefNo
			}
			mode := ""
			if row.ModeOfTxn != nil {
				mode = *row.ModeOfTxn
			}

			if err := tx.QueryRow(ctx, insertSQL,
				row.EntityID,
				row.AccountNumber,
				row.StatementDate,
				row.OpeningBalance,
				row.ClosingBalance,
				row.TransactionDate,
				desc,
				status,
				accHolder,
				branch,
				ifsc,
				stmtPeriod,
				cheque,
				row.WithdrawalAmount,
				row.DepositAmount,
				mode,
			).Scan(&id); err != nil {
				api.RespondWithPayload(w, false, "Insert error: "+err.Error(), nil)
				return
			}

			// Use top-level reason if provided by frontend; otherwise build placeholder JSON
			reasonJSON := req.Reason
			if strings.TrimSpace(reasonJSON) == "" {
				// reasonJSON = fmt.Sprintf(`{"old_entityid":"%s","old_account_number":"%s","old_description":"%s"}`, "", "", "")
				reasonJSON = ""
			}
			if _, err := tx.Exec(ctx, auditQ, id, reasonJSON, createdBy); err != nil {
				api.RespondWithPayload(w, false, "Audit insert error: "+err.Error(), nil)
				return
			}

			created = append(created, id)
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, "Commit error: "+err.Error(), nil)
			return
		}
		committed = true

		// respond with created ids and overall success
		api.RespondWithPayload(w, true, "", map[string]interface{}{"created": created})
	}
}

// CreateBankStatement is a convenience wrapper for creating a single bank statement
func CreateBankStatement(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// reuse bulk handler by wrapping single row
		var req struct {
			UserID string                 `json:"user_id"`
			Row    map[string]interface{} `json:"row"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			api.RespondWithPayload(w, false, "Invalid JSON or missing user_id/row", nil)
			return
		}
		// If single-row included a 'reason', lift it to top-level; otherwise omit
		topReason := ""
		if v, ok := req.Row["reason"]; ok {
			if s, ok2 := v.(string); ok2 {
				topReason = s
			}
		}
		// convert map to expected bulk shape and set user id in body
		combined := map[string]interface{}{"user_id": req.UserID, "reason": topReason, "rows": []map[string]interface{}{req.Row}}
		cb, _ := json.Marshal(combined)
		r.Body = io.NopCloser(strings.NewReader(string(cb)))
		CreateBankStatements(pgxPool)(w, r)
	}
}

// UpdateBankStatement updates fields on an existing bank_statement row,
// copies existing values into corresponding old_* columns and creates
// an EDIT audit action with processing_status = 'PENDING_EDIT_APPROVAL'.
func UpdateBankStatement(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID          string                 `json:"user_id"`
			BankStatementID string                 `json:"bankstatementid"`
			Fields          map[string]interface{} `json:"fields"`
			Reason          string                 `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithPayload(w, false, "invalid JSON", nil)
			return
		}
		if strings.TrimSpace(req.UserID) == "" || strings.TrimSpace(req.BankStatementID) == "" {
			api.RespondWithPayload(w, false, "user_id and bankstatementid required", nil)
			return
		}

		// resolve requested_by name
		requestedBy := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithPayload(w, false, "invalid user_id or session", nil)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithPayload(w, false, "failed to begin tx: "+err.Error(), nil)
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// fetch current row FOR UPDATE
		sel := `SELECT entityid, account_number, statementdate, openingbalance, closingbalance, transactiondate, description, status, accountholdername, branchname, ifsccode, statement_period, chequerefno, withdrawalamount, depositamount, modeoftransaction FROM bank_statement WHERE bankstatementid=$1 FOR UPDATE`

		var (
			curEntity, curAccount, curDescription, curStatus, curAccHolder, curBranch, curIFSC, curStmtPeriod, curCheque, curMode *string
			curStatementDate, curTransactionDate                                                                                  *time.Time
			curOpenBal, curCloseBal, curWithdraw, curDeposit                                                                      *float64
		)

		if err := tx.QueryRow(ctx, sel, req.BankStatementID).Scan(
			&curEntity,
			&curAccount,
			&curStatementDate,
			&curOpenBal,
			&curCloseBal,
			&curTransactionDate,
			&curDescription,
			&curStatus,
			&curAccHolder,
			&curBranch,
			&curIFSC,
			&curStmtPeriod,
			&curCheque,
			&curWithdraw,
			&curDeposit,
			&curMode,
		); err != nil {
			api.RespondWithPayload(w, false, "failed to fetch existing bank_statement: "+err.Error(), nil)
			return
		}

		// helpers to convert current values to interface{} for args
		curStr := func(p *string) interface{} {
			if p == nil {
				return nil
			}
			return *p
		}
		curTime := func(p *time.Time) interface{} {
			if p == nil {
				return nil
			}
			return *p
		}
		curF := func(p *float64) interface{} {
			if p == nil {
				return nil
			}
			return *p
		}

		// build update sets and args; when updating a field, also set old_<field>=current_value
		sets := []string{}
		args := []interface{}{}
		pos := 1

		addStrField := func(col, oldcol string, val interface{}, cur *string) {
			sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", col, pos, oldcol, pos+1))
			if val == nil {
				args = append(args, nil)
			} else {
				args = append(args, fmt.Sprint(val))
			}
			args = append(args, curStr(cur))
			pos += 2
		}
		addTimeField := func(col, oldcol string, val interface{}, cur *time.Time) {
			sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", col, pos, oldcol, pos+1))
			if val == nil {
				args = append(args, nil)
			} else {
				args = append(args, fmt.Sprint(val))
			}
			args = append(args, curTime(cur))
			pos += 2
		}
		addFloatField := func(col, oldcol string, val interface{}, cur *float64) {
			sets = append(sets, fmt.Sprintf("%s=$%d, %s=$%d", col, pos, oldcol, pos+1))
			if val == nil {
				args = append(args, nil)
			} else {
				args = append(args, val)
			}
			args = append(args, curF(cur))
			pos += 2
		}

		for k, v := range req.Fields {
			switch k {
			case "entityid":
				addStrField("entityid", "old_entityid", v, curEntity)
			case "account_number":
				addStrField("account_number", "old_account_number", v, curAccount)
			case "statementdate":
				addTimeField("statementdate", "old_statementdate", v, curStatementDate)
			case "openingbalance":
				addFloatField("openingbalance", "old_openingbalance", v, curOpenBal)
			case "closingbalance":
				addFloatField("closingbalance", "old_closingbalance", v, curCloseBal)
			case "transactiondate":
				addTimeField("transactiondate", "old_transactiondate", v, curTransactionDate)
			case "description":
				addStrField("description", "old_description", v, curDescription)
			case "status":
				addStrField("status", "old_status", v, curStatus)
			case "accountholdername":
				addStrField("accountholdername", "old_accountholdername", v, curAccHolder)
			case "branchname":
				addStrField("branchname", "old_branchname", v, curBranch)
			case "ifsccode":
				addStrField("ifsccode", "old_ifsccode", v, curIFSC)
			case "statement_period":
				addStrField("statement_period", "old_statement_period", v, curStmtPeriod)
			case "chequerefno":
				addStrField("chequerefno", "old_chequerefno", v, curCheque)
			case "withdrawalamount":
				addFloatField("withdrawalamount", "old_withdrawalamount", v, curWithdraw)
			case "depositamount":
				addFloatField("depositamount", "old_depositamount", v, curDeposit)
			case "modeoftransaction":
				addStrField("modeoftransaction", "old_modeoftransaction", v, curMode)
			default:
				// ignore unknown fields
			}
		}

		if len(sets) == 0 {
			api.RespondWithPayload(w, false, "no valid fields to update", nil)
			return
		}

		q := "UPDATE bank_statement SET " + strings.Join(sets, ", ") + fmt.Sprintf(" WHERE bankstatementid=$%d", pos)
		args = append(args, req.BankStatementID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			api.RespondWithPayload(w, false, "failed to update bank_statement: "+err.Error(), nil)
			return
		}

		// create audit action
		auditQ := `INSERT INTO auditactionbankstatement (bankstatementid, actiontype, processing_status, reason, requested_by, requested_at) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now())`

		// Use frontend-provided reason if present; otherwise build a placeholder JSON with old_* values
		reasonJSON := req.Reason
		if strings.TrimSpace(reasonJSON) == "" {
			// reasonJSON = fmt.Sprintf(`{"old_entityid":"%s","old_account_number":"%s","old_description":"%s"}`,
			// 	curStr(curEntity), curStr(curAccount), curStr(curDescription))
			reasonJSON = ""
		}

		if _, err := tx.Exec(ctx, auditQ, req.BankStatementID, reasonJSON, requestedBy); err != nil {
			api.RespondWithPayload(w, false, "failed to create audit action: "+err.Error(), nil)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithPayload(w, false, "failed to commit: "+err.Error(), nil)
			return
		}
		committed = true

		api.RespondWithPayload(w, true, "", map[string]interface{}{"bankstatementid": req.BankStatementID})
	}
}
