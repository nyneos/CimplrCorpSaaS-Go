package bankstatement

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type bankStatementAuditRequest struct {
	UserID          string `json:"user_id"`
	BankStatementID string `json:"bank_statement_id"`
}

type bankStatementDownloadAuditRequest struct {
	UserID          string `json:"user_id"`
	BankStatementID string `json:"bank_statement_id"`
}

func GetBankStatementAuditHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var body bankStatementAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.BankStatementID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrBankStatementIDRequired)
			return
		}

		ctx := r.Context()
		if code, msg := validateBankStatementAuditAccess(ctx, pool, body.BankStatementID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pool.Query(ctx, `
			SELECT
				action_id,
				bankstatementid,
				actiontype,
				processing_status,
				requested_by,
				requested_at,
				requested_ip,
				checker_by,
				checker_at,
				checker_ip,
				checker_comment,
				reason
			FROM cimplrcorpsaas.auditactionbankstatement
			WHERE bankstatementid = $1
			ORDER BY requested_at ASC, action_id ASC
		`, body.BankStatementID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pqUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var auditID, entityID string
			var action, status, performedBy, requestedIP, checkerBy, checkerIP, comment, reason sql.NullString
			var performedAt, checkerAt sql.NullTime
			if err := rows.Scan(&auditID, &entityID, &action, &status, &performedBy, &performedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &comment, &reason); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read audit history")
				return
			}

			entry := map[string]interface{}{
				"audit_id":          auditID,
				"entity_id":         entityID,
				"action_type":       action.String,
				"processing_status": status.String,
				"requested_by":      performedBy.String,
				"requested_at":      nullableTime(performedAt),
				"requested_ip":      requestedIP.String,
				"checker_by":        checkerBy.String,
				"checker_at":        nullableTime(checkerAt),
				"checker_ip":        checkerIP.String,
				"checker_comment":   comment.String,
				"reason":            reason.String,
			}
			if strings.EqualFold(action.String, "EDIT") {
				if changes := buildBankStatementChangeSummary(ctx, pool, body.BankStatementID); len(changes) > 0 {
					entry["change_summary"] = changes
				}
			}

			payload = append(payload, entry)
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read audit history")
			return
		}

		// Standardize: always return 'rows' as the array field
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"audit_logs": payload,
		})
	})
}

func GetBankStatementDownloadAuditHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var body bankStatementDownloadAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.BankStatementID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrBankStatementIDRequired)
			return
		}

		ctx := r.Context()
		if code, msg := validateBankStatementAuditAccess(ctx, pool, body.BankStatementID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pool.Query(ctx, `
			SELECT
				file_id,
				bankstatementid,
				user_id,
				NOW() AS performed_at,
				ip,
				entity_name
			FROM cimplrcorpsaas.bank_pdf_download_audits
			WHERE bankstatementid = $1
		`, body.BankStatementID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pqUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var fileID, bankStatementID, performedBy, ip, entityName sql.NullString
			var performedAt time.Time
			if err := rows.Scan(&fileID, &bankStatementID, &performedBy, &performedAt, &ip, &entityName); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read download audit history")
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":         bankStatementID.String,
				"action_type":       "DOWNLOAD",
				"processing_status": "COMPLETED",
				"requested_by":      performedBy.String,
				"requested_at":      api.FormatAuditTimestampIST(performedAt),
				"requested_ip":      ip.String,
				"checker_by":        "",
				"checker_at":        nil,
				"checker_ip":        "",
				"checker_comment":   "",
				"reason":            "",
				"file_id":           fileID.String,
				"bank_statement_id": bankStatementID.String,
				"ip":                ip.String,
				"entity_name":       entityName.String,
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read download audit history")
			return
		}

		// Standardize: always return 'rows' as the array field
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"audit_logs": payload,
		})
	})
}

func GetBankStatementBalanceImpactAuditHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var body bankStatementAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.BankStatementID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrBankStatementIDRequired)
			return
		}

		ctx := r.Context()
		if code, msg := validateBankStatementAuditAccess(ctx, pool, body.BankStatementID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pool.Query(ctx, `
			SELECT
				balance_id,
				actiontype,
				processing_status,
				requested_by,
				requested_at,
				requested_ip,
				checker_by,
				checker_at,
				checker_ip,
				checker_comment
			FROM public.auditactionbankbalances
			WHERE balance_id = $1
			ORDER BY requested_at ASC, action_id ASC
		`, body.BankStatementID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pqUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var balanceID string
			var action, status, performedBy, requestedIP, checkerBy, checkerIP, checkerComment sql.NullString
			var performedAt, checkerAt sql.NullTime
			if err := rows.Scan(&balanceID, &action, &status, &performedBy, &performedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read balance impact audit")
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":         balanceID,
				"balance_id":        balanceID,
				"action_type":       action.String,
				"processing_status": status.String,
				"requested_by":      performedBy.String,
				"requested_at":      nullableTime(performedAt),
				"requested_ip":      requestedIP.String,
				"checker_by":        checkerBy.String,
				"checker_at":        nullableTime(checkerAt),
				"checker_ip":        checkerIP.String,
				"checker_comment":   checkerComment.String,
				"reason":            "",
				"bank_statement_id": body.BankStatementID,
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read balance impact audit")
			return
		}

		// Standardize: always return 'rows' as the array field
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"audit_logs": payload,
		})
	})
}

func GetBankStatementTransactionAuditHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var body bankStatementAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.BankStatementID) == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrBankStatementIDRequired)
			return
		}

		ctx := r.Context()
		if code, msg := validateBankStatementAuditAccess(ctx, pool, body.BankStatementID); code != 0 {
			api.RespondWithError(w, code, msg)
			return
		}

		rows, err := pool.Query(ctx, `
			SELECT
				transaction_id,
				field_name,
				old_value,
				new_value,
				performed_by,
				performed_at,
				requested_ip,
				checker_ip
			FROM cimplrcorpsaas.auditactionbankstatementtxn
			WHERE bankstatementid = $1
			ORDER BY performed_at DESC, transaction_id ASC
		`, body.BankStatementID)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, pqUserFriendlyMessage(err))
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var transactionID, fieldName, oldValue, newValue, performedBy, requestedIP, checkerIP sql.NullString
			var performedAt sql.NullTime
			if err := rows.Scan(&transactionID, &fieldName, &oldValue, &newValue, &performedBy, &performedAt, &requestedIP, &checkerIP); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "failed to read transaction audit history")
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":         transactionID.String,
				"action_type":       "EDIT",
				"processing_status": "COMPLETED",
				"requested_by":      performedBy.String,
				"requested_at":      nullableTime(performedAt),
				"requested_ip":      requestedIP.String,
				"checker_by":        "",
				"checker_at":        nil,
				"checker_ip":        checkerIP.String,
				"checker_comment":   "",
				"reason":            "",
				"transaction_id":    transactionID.String,
				"change_summary": []map[string]interface{}{{
					"field":     fieldName.String,
					"old_value": oldValue.String,
					"new_value": newValue.String,
				}},
			})
		}
		if err := rows.Err(); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "failed to read transaction audit history")
			return
		}

		// Standardize: always return 'rows' as the array field
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"audit_logs": payload,
		})
	})
}

func validateBankStatementAuditAccess(ctx context.Context, pool *pgxpool.Pool, bankStatementID string) (int, string) {
	var entityID string
	if err := pool.QueryRow(ctx, `SELECT entity_id FROM cimplrcorpsaas.bank_statements WHERE bank_statement_id = $1`, bankStatementID).Scan(&entityID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return http.StatusNotFound, "bank statement not found"
		}
		return http.StatusInternalServerError, pqUserFriendlyMessage(err)
	}
	if ids := ctxutil.FromContext(ctx).EntityIDs; len(ids) > 0 && !ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
		return http.StatusForbidden, constants.ErrNoAccessToBankStatement
	}
	return 0, ""
}

func validateEntityAuditAccess(ctx context.Context, pool *pgxpool.Pool, entityName string) (int, string) {
	entityIDs := ctxutil.FromContext(ctx).EntityIDs
	if len(entityIDs) == 0 {
		return http.StatusUnauthorized, constants.ErrNoAccessibleBusinessUnit
	}

	var entityID string
	if err := pool.QueryRow(ctx, `
		SELECT entity_id
		FROM public.masterentitycash
		WHERE entity_name = $1
		LIMIT 1
	`, entityName).Scan(&entityID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return http.StatusNotFound, "entity not found"
		}
		return http.StatusInternalServerError, pqUserFriendlyMessage(err)
	}
	if !ctxutil.FromContext(ctx).HasEntityAccess(entityID) {
		return http.StatusForbidden, constants.ErrNoAccessToBankStatement
	}
	return 0, ""
}

func nullableTime(value sql.NullTime) interface{} {
	return api.FormatAuditTimestampNullIST(value)
}

func nullIfBlank(value string) interface{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func respondAuditPayload(w http.ResponseWriter, payload interface{}) {
	api.RespondEnvelopeSuccess(w, "Success", payload)
}

func buildBankStatementChangeSummary(ctx context.Context, pool *pgxpool.Pool, bankStatementID string) []map[string]interface{} {
	var (
		entityID, oldEntityID                   string
		accountNumber, oldAccountNumber         string
		statementDate, oldStatementDate         string
		openingBalance, oldOpeningBalance       string
		closingBalance, oldClosingBalance       string
		transactionDate, oldTransactionDate     string
		description, oldDescription             string
		status, oldStatus                       string
		accountHolderName, oldAccountHolderName string
		branchName, oldBranchName               string
		ifscCode, oldIFSCCode                   string
		statementPeriod, oldStatementPeriod     string
		chequeRefNo, oldChequeRefNo             string
		withdrawalAmount, oldWithdrawalAmount   string
		depositAmount, oldDepositAmount         string
		modeOfTransaction, oldModeOfTransaction string
	)

	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(entityid, ''),
			COALESCE(old_entityid, ''),
			COALESCE(account_number, ''),
			COALESCE(old_account_number, ''),
			COALESCE(TO_CHAR(statementdate, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_statementdate, 'YYYY-MM-DD'), ''),
			COALESCE(CAST(openingbalance AS text), ''),
			COALESCE(CAST(old_openingbalance AS text), ''),
			COALESCE(CAST(closingbalance AS text), ''),
			COALESCE(CAST(old_closingbalance AS text), ''),
			COALESCE(TO_CHAR(transactiondate, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_transactiondate, 'YYYY-MM-DD'), ''),
			COALESCE(description, ''),
			COALESCE(old_description, ''),
			COALESCE(status, ''),
			COALESCE(old_status, ''),
			COALESCE(accountholdername, ''),
			COALESCE(old_accountholdername, ''),
			COALESCE(branchname, ''),
			COALESCE(old_branchname, ''),
			COALESCE(ifsccode, ''),
			COALESCE(old_ifsccode, ''),
			COALESCE(statement_period, ''),
			COALESCE(old_statement_period, ''),
			COALESCE(chequerefno, ''),
			COALESCE(old_chequerefno, ''),
			COALESCE(CAST(withdrawalamount AS text), ''),
			COALESCE(CAST(old_withdrawalamount AS text), ''),
			COALESCE(CAST(depositamount AS text), ''),
			COALESCE(CAST(old_depositamount AS text), ''),
			COALESCE(modeoftransaction, ''),
			COALESCE(old_modeoftransaction, '')
		FROM bank_statement
		WHERE bankstatementid = $1
	`, bankStatementID).Scan(
		&entityID, &oldEntityID,
		&accountNumber, &oldAccountNumber,
		&statementDate, &oldStatementDate,
		&openingBalance, &oldOpeningBalance,
		&closingBalance, &oldClosingBalance,
		&transactionDate, &oldTransactionDate,
		&description, &oldDescription,
		&status, &oldStatus,
		&accountHolderName, &oldAccountHolderName,
		&branchName, &oldBranchName,
		&ifscCode, &oldIFSCCode,
		&statementPeriod, &oldStatementPeriod,
		&chequeRefNo, &oldChequeRefNo,
		&withdrawalAmount, &oldWithdrawalAmount,
		&depositAmount, &oldDepositAmount,
		&modeOfTransaction, &oldModeOfTransaction,
	)
	if err != nil {
		return nil
	}

	changes := make([]map[string]interface{}, 0)
	appendBankStatementChange(&changes, "Entity ID", oldEntityID, entityID)
	appendBankStatementChange(&changes, "Account Number", oldAccountNumber, accountNumber)
	appendBankStatementChange(&changes, "Statement Date", oldStatementDate, statementDate)
	appendBankStatementChange(&changes, "Opening Balance", oldOpeningBalance, openingBalance)
	appendBankStatementChange(&changes, "Closing Balance", oldClosingBalance, closingBalance)
	appendBankStatementChange(&changes, "Transaction Date", oldTransactionDate, transactionDate)
	appendBankStatementChange(&changes, "Description", oldDescription, description)
	appendBankStatementChange(&changes, "Status", oldStatus, status)
	appendBankStatementChange(&changes, "Account Holder Name", oldAccountHolderName, accountHolderName)
	appendBankStatementChange(&changes, "Branch Name", oldBranchName, branchName)
	appendBankStatementChange(&changes, "IFSC Code", oldIFSCCode, ifscCode)
	appendBankStatementChange(&changes, "Statement Period", oldStatementPeriod, statementPeriod)
	appendBankStatementChange(&changes, "Cheque Ref No", oldChequeRefNo, chequeRefNo)
	appendBankStatementChange(&changes, "Withdrawal Amount", oldWithdrawalAmount, withdrawalAmount)
	appendBankStatementChange(&changes, "Deposit Amount", oldDepositAmount, depositAmount)
	appendBankStatementChange(&changes, "Mode Of Transaction", oldModeOfTransaction, modeOfTransaction)
	return changes
}

func appendBankStatementChange(changes *[]map[string]interface{}, fieldName, oldValue, newValue string) {
	if strings.TrimSpace(oldValue) == strings.TrimSpace(newValue) {
		return
	}

	*changes = append(*changes, map[string]interface{}{
		"field":     fieldName,
		"old_value": oldValue,
		"new_value": newValue,
	})
}
