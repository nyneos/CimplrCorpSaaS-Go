package bankbalances

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type bankBalanceAuditRequest struct {
	UserID    string `json:"user_id"`
	BalanceID string `json:"balance_id"`
}

func GetBankBalanceAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.Error(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req bankBalanceAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.BalanceID) == "" {
			api.Error(w, http.StatusBadRequest, "Missing balance_id")
			return
		}

		ctx := r.Context()
		if code, msg := ensureBalanceIDsAccessible(ctx, pgxPool, []string{req.BalanceID}); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pgxPool.Query(ctx, `
			SELECT
				balance_id,
				actiontype,
				processing_status,
				requested_by,
				requested_at,
				checker_by,
				checker_at,
				checker_comment,
				reason
			FROM auditactionbankbalances
			WHERE balance_id = $1
			ORDER BY requested_at ASC, action_id ASC
		`, req.BalanceID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pgUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var entityID string
			var action, status, performedBy, checkerBy, comment, reason sql.NullString
			var performedAt, checkerAt sql.NullTime
			if err := rows.Scan(&entityID, &action, &status, &performedBy, &performedAt, &checkerBy, &checkerAt, &comment, &reason); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read bank balance audit history")
				return
			}

			entry := map[string]interface{}{
				"entity_id":         entityID,
				"action_type":       auditString(action),
				"processing_status": auditString(status),
				"requested_by":      auditString(performedBy),
				"requested_at":      auditTime(performedAt),
				"checker_by":        auditString(checkerBy),
				"checker_at":        auditTime(checkerAt),
				"checker_comment":   auditString(comment),
				"reason":            auditString(reason),
			}
			if strings.EqualFold(auditString(action), "EDIT") {
				if changes := buildBankBalanceChangeSummary(ctx, pgxPool, req.BalanceID); len(changes) > 0 {
					entry["change_summary"] = changes
				}
			}

			payload = append(payload, entry)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read bank balance audit history")
			return
		}

		downloadRows, err := pgxPool.Query(ctx, `
			SELECT balance_id, requested_by, requested_at, file_name, upload_s3_key
			FROM auditactionbankbalance_downloads
			WHERE balance_id = $1
			ORDER BY requested_at ASC, download_audit_id ASC
		`, req.BalanceID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read bank balance download audit history")
			return
		}
		defer downloadRows.Close()

		for downloadRows.Next() {
			var entityID, requestedBy string
			var requestedAt sql.NullTime
			var fileName, uploadKey sql.NullString
			if err := downloadRows.Scan(&entityID, &requestedBy, &requestedAt, &fileName, &uploadKey); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read bank balance download audit history")
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":         entityID,
				"action_type":       "DOWNLOAD",
				"processing_status": "COMPLETED",
				"requested_by":      strings.TrimSpace(requestedBy),
				"requested_at":      auditTime(requestedAt),
				"checker_by":        "",
				"checker_at":        nil,
				"checker_comment":   "",
				"reason":            "",
				"file_name":         auditString(fileName),
				"upload_s3_key":     auditString(uploadKey),
				"source":            "BANK_BALANCE",
			})
		}
		if err := downloadRows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read bank balance download audit history")
			return
		}

		api.Success(w, http.StatusOK, payload, "")
	}
}

func auditString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func auditTime(value sql.NullTime) interface{} {
	if !value.Valid {
		return nil
	}
	return value.Time
}

func insertBankBalanceDownloadAudit(ctx context.Context, pgxPool *pgxpool.Pool, balanceID, requestedBy, uploadS3Key string) {
	balanceID = strings.TrimSpace(balanceID)
	requestedBy = strings.TrimSpace(requestedBy)
	uploadS3Key = strings.TrimSpace(uploadS3Key)
	if balanceID == "" {
		return
	}
	if requestedBy == "" {
		if userID, ok := ctx.Value("user_id").(string); ok {
			requestedBy = strings.TrimSpace(userID)
		}
	}
	if requestedBy == "" {
		return
	}

	_, err := pgxPool.Exec(ctx, `
		INSERT INTO auditactionbankbalance_downloads (balance_id, requested_by, requested_at, file_name, upload_s3_key)
		VALUES ($1, $2, now(), $3, $4)
	`, balanceID, requestedBy, extractAuditFileName(uploadS3Key), nullIfEmpty(uploadS3Key))
	if err != nil {
		log.Printf("failed to insert bank balance download audit for %s: %v", balanceID, err)
	}
}

func nullIfEmpty(value string) interface{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func extractAuditFileName(uploadS3Key string) interface{} {
	uploadS3Key = strings.TrimSpace(uploadS3Key)
	if uploadS3Key == "" {
		return nil
	}

	parts := strings.Split(uploadS3Key, "/")
	name := strings.TrimSpace(parts[len(parts)-1])
	if name == "" {
		return nil
	}
	return name
}

func buildBankBalanceChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, balanceID string) []map[string]interface{} {
	var (
		bankName, oldBankName             string
		accountNo, oldAccountNo           string
		iban, oldIBAN                     string
		currencyCode, oldCurrencyCode     string
		nickname, oldNickname             string
		asOfDate, oldAsOfDate             string
		asOfTime, oldAsOfTime             string
		balanceType, oldBalanceType       string
		balanceAmount, oldBalanceAmount   string
		statementType, oldStatementType   string
		sourceChannel, oldSourceChannel   string
		openingBalance, oldOpeningBalance string
		totalCredits, oldTotalCredits     string
		totalDebits, oldTotalDebits       string
		closingBalance, oldClosingBalance string
	)

	err := pgxPool.QueryRow(ctx, `
		SELECT
			COALESCE(bank_name, ''),
			COALESCE(old_bank_name, ''),
			COALESCE(account_no, ''),
			COALESCE(old_account_no, ''),
			COALESCE(iban, ''),
			COALESCE(old_iban, ''),
			COALESCE(currency_code, ''),
			COALESCE(old_currency_code, ''),
			COALESCE(nickname, ''),
			COALESCE(old_nickname, ''),
			COALESCE(TO_CHAR(as_of_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_as_of_date, 'YYYY-MM-DD'), ''),
			COALESCE(CAST(as_of_time AS text), ''),
			COALESCE(CAST(old_as_of_time AS text), ''),
			COALESCE(balance_type, ''),
			COALESCE(old_balance_type, ''),
			COALESCE(CAST(balance_amount AS text), ''),
			COALESCE(CAST(old_balance_amount AS text), ''),
			COALESCE(statement_type, ''),
			COALESCE(old_statement_type, ''),
			COALESCE(source_channel, ''),
			COALESCE(old_source_channel, ''),
			COALESCE(CAST(opening_balance AS text), ''),
			COALESCE(CAST(old_opening_balance AS text), ''),
			COALESCE(CAST(total_credits AS text), ''),
			COALESCE(CAST(old_total_credits AS text), ''),
			COALESCE(CAST(total_debits AS text), ''),
			COALESCE(CAST(old_total_debits AS text), ''),
			COALESCE(CAST(closing_balance AS text), ''),
			COALESCE(CAST(old_closing_balance AS text), '')
		FROM bank_balances_manual
		WHERE balance_id = $1
	`, balanceID).Scan(
		&bankName, &oldBankName,
		&accountNo, &oldAccountNo,
		&iban, &oldIBAN,
		&currencyCode, &oldCurrencyCode,
		&nickname, &oldNickname,
		&asOfDate, &oldAsOfDate,
		&asOfTime, &oldAsOfTime,
		&balanceType, &oldBalanceType,
		&balanceAmount, &oldBalanceAmount,
		&statementType, &oldStatementType,
		&sourceChannel, &oldSourceChannel,
		&openingBalance, &oldOpeningBalance,
		&totalCredits, &oldTotalCredits,
		&totalDebits, &oldTotalDebits,
		&closingBalance, &oldClosingBalance,
	)
	if err != nil {
		return nil
	}

	changes := make([]map[string]interface{}, 0)
	appendAuditChange(&changes, "Bank Name", oldBankName, bankName)
	appendAuditChange(&changes, "Account Number", oldAccountNo, accountNo)
	appendAuditChange(&changes, "IBAN", oldIBAN, iban)
	appendAuditChange(&changes, "Currency Code", oldCurrencyCode, currencyCode)
	appendAuditChange(&changes, "Nickname", oldNickname, nickname)
	appendAuditChange(&changes, "As Of Date", oldAsOfDate, asOfDate)
	appendAuditChange(&changes, "As Of Time", oldAsOfTime, asOfTime)
	appendAuditChange(&changes, "Balance Type", oldBalanceType, balanceType)
	appendAuditChange(&changes, "Balance Amount", oldBalanceAmount, balanceAmount)
	appendAuditChange(&changes, "Statement Type", oldStatementType, statementType)
	appendAuditChange(&changes, "Source Channel", oldSourceChannel, sourceChannel)
	appendAuditChange(&changes, "Opening Balance", oldOpeningBalance, openingBalance)
	appendAuditChange(&changes, "Total Credits", oldTotalCredits, totalCredits)
	appendAuditChange(&changes, "Total Debits", oldTotalDebits, totalDebits)
	appendAuditChange(&changes, "Closing Balance", oldClosingBalance, closingBalance)
	return changes
}

func appendAuditChange(changes *[]map[string]interface{}, fieldName, oldValue, newValue string) {
	if strings.TrimSpace(oldValue) == strings.TrimSpace(newValue) {
		return
	}

	*changes = append(*changes, map[string]interface{}{
		"field":     fieldName,
		"old_value": oldValue,
		"new_value": newValue,
	})
}
