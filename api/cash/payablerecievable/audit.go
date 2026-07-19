package payablerecievable

import (
	api "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

type transactionAuditRequest struct {
	UserID          string `json:"user_id"`
	TransactionType string `json:"transaction_type"`
	TransactionID   string `json:"transaction_id"`
}

func GetTransactionAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			api.RespondWithError(w, http.StatusMethodNotAllowed, constants.ErrMethodNotAllowed)
			return
		}

		var req transactionAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.TransactionID) == "" || strings.TrimSpace(req.TransactionType) == "" {
			respondTransactionAuditError(w, http.StatusBadRequest, "transaction_type and transaction_id are required")
			return
		}

		txType := strings.ToUpper(strings.TrimSpace(req.TransactionType))
		tableName := ""
		idColumn := ""
		switch txType {
		case "PAYABLE":
			tableName = "auditactionpayable"
			idColumn = "payable_id"
		case "RECEIVABLE":
			tableName = "auditactionreceivable"
			idColumn = "receivable_id"
		default:
			respondTransactionAuditError(w, http.StatusBadRequest, "transaction_type must be PAYABLE or RECEIVABLE")
			return
		}

		ctx := r.Context()
		if msg := validateTransactionRecordScope(ctx, pgxPool, txType, req.TransactionID); msg != "" {
			api.RespondWithError(w, http.StatusForbidden, msg)
			return
		}
		rows, err := pgxPool.Query(ctx, `
			SELECT
				action_id,
				`+idColumn+`,
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
			FROM `+tableName+`
			WHERE `+idColumn+` = $1
			ORDER BY requested_at ASC, action_id ASC
		`, req.TransactionID)
		if err != nil {
			respondTransactionAuditError(w, http.StatusInternalServerError, err.Error())
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var actionID, entityID, action, status, performedBy string
			var performedAt time.Time
			var requestedIP, checkerIP *string
			var checkerBy, checkerComment, reason *string
			var checkerAt *time.Time
			if err := rows.Scan(&actionID, &entityID, &action, &status, &performedBy, &performedAt, &requestedIP, &checkerBy, &checkerAt, &checkerIP, &checkerComment, &reason); err != nil {
				respondTransactionAuditError(w, http.StatusInternalServerError, "failed to read transaction audit history")
				return
			}

			payload = append(payload, map[string]interface{}{
				"action_id":         actionID,
				"entity_id":         entityID,
				"action_type":       action,
				"processing_status": status,
				"requested_by":      performedBy,
				"requested_at":      api.FormatAuditTimestampIST(performedAt),
				"requested_ip":      stringPointerValue(requestedIP),
				"checker_by":        stringPointerValue(checkerBy),
				"checker_at":        timePointerValue(checkerAt),
				"checker_ip":        stringPointerValue(checkerIP),
				"checker_comment":   stringPointerValue(checkerComment),
				"reason":            stringPointerValue(reason),
				"change_summary":    buildTransactionChangeSummary(ctx, pgxPool, txType, req.TransactionID, action, actionID),
			})
		}
		if err := rows.Err(); err != nil {
			respondTransactionAuditError(w, http.StatusInternalServerError, "failed to read transaction audit history")
			return
		}

		downloadRows, err := pgxPool.Query(ctx, `
			SELECT transaction_id, requested_by, requested_at, requested_ip, file_name, upload_s3_key
			FROM auditactiontransactiondownloads
			WHERE transaction_type = $1 AND transaction_id = $2
			ORDER BY requested_at ASC, download_audit_id ASC
		`, txType, req.TransactionID)
		if err != nil {
			respondTransactionAuditError(w, http.StatusInternalServerError, constants.ErrFailedToReadTransactionDownloadAuditHistory)
			return
		}
		defer downloadRows.Close()

		for downloadRows.Next() {
			var entityID, requestedBy string
			var requestedAt sql.NullTime
			var requestedIP sql.NullString
			var fileName, uploadKey sql.NullString
			if err := downloadRows.Scan(&entityID, &requestedBy, &requestedAt, &requestedIP, &fileName, &uploadKey); err != nil {
				respondTransactionAuditError(w, http.StatusInternalServerError, constants.ErrFailedToReadTransactionDownloadAuditHistory)
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":         entityID,
				"action_type":       "DOWNLOAD",
				"processing_status": "COMPLETED",
				"requested_by":      strings.TrimSpace(requestedBy),
				"requested_at":      api.FormatAuditTimestampNullIST(requestedAt),
				"requested_ip":      strings.TrimSpace(requestedIP.String),
				"checker_by":        "",
				"checker_at":        nil,
				"checker_ip":        "",
				"checker_comment":   "",
				"reason":            "",
				"file_name":         fileName.String,
				"upload_s3_key":     uploadKey.String,
				"source":            txType,
			})
		}
		if err := downloadRows.Err(); err != nil {
			respondTransactionAuditError(w, http.StatusInternalServerError, constants.ErrFailedToReadTransactionDownloadAuditHistory)
			return
		}

		// Standardize: always return 'rows' as the array field
		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"audit_logs": payload,
		})
	}
}

func validateTransactionRecordScope(ctx context.Context, pgxPool *pgxpool.Pool, transactionType, transactionID string) string {
	var entityName, counterpartyName, currencyCode string
	switch strings.ToUpper(strings.TrimSpace(transactionType)) {
	case "PAYABLE":
		if err := pgxPool.QueryRow(ctx, `
			SELECT COALESCE(entity_name, ''), COALESCE(counterparty_name, ''), COALESCE(currency_code, '')
			FROM tr_payables
			WHERE payable_id = $1 AND is_deleted != TRUE
		`, transactionID).Scan(&entityName, &counterpartyName, &currencyCode); err != nil {
			return "payable not found"
		}
	case "RECEIVABLE":
		if err := pgxPool.QueryRow(ctx, `
			SELECT COALESCE(entity_name, ''), COALESCE(counterparty_name, ''), COALESCE(currency_code, '')
			FROM tr_receivables
			WHERE receivable_id = $1 AND is_deleted != TRUE
		`, transactionID).Scan(&entityName, &counterpartyName, &currencyCode); err != nil {
			return "receivable not found"
		}
	default:
		return "transaction_type must be PAYABLE or RECEIVABLE"
	}
	return validatePayRecScope(ctx, entityName, counterpartyName, currencyCode)
}

func respondTransactionAuditError(w http.ResponseWriter, status int, message string) {
	api.RespondEnvelopeError(w, status, message, "")
}

func buildTransactionChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, transactionType, transactionID, action, actionID string) []map[string]interface{} {
	if !strings.EqualFold(strings.TrimSpace(action), "EDIT") {
		return nil
	}

	switch strings.ToUpper(strings.TrimSpace(transactionType)) {
	case "PAYABLE":
		return buildPayableChangeSummary(ctx, pgxPool, transactionID, actionID)
	case "RECEIVABLE":
		return buildReceivableChangeSummary(ctx, pgxPool, transactionID, actionID)
	default:
		return nil
	}
}

func buildPayableChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, payableID, actionID string) []map[string]interface{} {
	var (
		oldEntity, newEntity     sql.NullString
		oldCounter, newCounter   sql.NullString
		oldInvoice, newInvoice   sql.NullString
		oldInvDate, newInvDate   sql.NullString
		oldDueDate, newDueDate   sql.NullString
		oldAmount, newAmount     sql.NullString
		oldCurrency, newCurrency sql.NullString
	)

	err := pgxPool.QueryRow(ctx, `
		SELECT
			aa.old_entity_name,
			aa.new_entity_name,
			aa.old_counterparty_name,
			aa.new_counterparty_name,
			aa.old_invoice_number,
			aa.new_invoice_number,
			CAST(aa.old_invoice_date AS text),
			CAST(aa.new_invoice_date AS text),
			CAST(aa.old_due_date AS text),
			CAST(aa.new_due_date AS text),
			CAST(aa.old_amount AS text),
			CAST(aa.new_amount AS text),
			aa.old_currency_code,
			aa.new_currency_code
		FROM auditactionpayable aa
		WHERE aa.action_id = $1 AND aa.payable_id = $2
	`, actionID, payableID).Scan(
		&oldEntity, &newEntity,
		&oldCounter, &newCounter,
		&oldInvoice, &newInvoice,
		&oldInvDate, &newInvDate,
		&oldDueDate, &newDueDate,
		&oldAmount, &newAmount,
		&oldCurrency, &newCurrency,
	)
	if err != nil {
		return nil
	}

	changes := make([]map[string]interface{}, 0)
	appendTransactionChange(&changes, "Entity Name", oldEntity, newEntity)
	appendTransactionChange(&changes, "Counterparty Name", oldCounter, newCounter)
	appendTransactionChange(&changes, "Invoice Number", oldInvoice, newInvoice)
	appendTransactionChange(&changes, "Invoice Date", oldInvDate, newInvDate)
	appendTransactionChange(&changes, "Due Date", oldDueDate, newDueDate)
	appendTransactionChange(&changes, "Amount", oldAmount, newAmount)
	appendTransactionChange(&changes, "Currency Code", oldCurrency, newCurrency)
	return changes
}

func buildReceivableChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, receivableID, actionID string) []map[string]interface{} {
	var (
		oldEntity, newEntity     sql.NullString
		oldCounter, newCounter   sql.NullString
		oldInvoice, newInvoice   sql.NullString
		oldInvDate, newInvDate   sql.NullString
		oldDueDate, newDueDate   sql.NullString
		oldAmount, newAmount     sql.NullString
		oldCurrency, newCurrency sql.NullString
	)

	err := pgxPool.QueryRow(ctx, `
		SELECT
			aa.old_entity_name,
			aa.new_entity_name,
			aa.old_counterparty_name,
			aa.new_counterparty_name,
			aa.old_invoice_number,
			aa.new_invoice_number,
			CAST(aa.old_invoice_date AS text),
			CAST(aa.new_invoice_date AS text),
			CAST(aa.old_due_date AS text),
			CAST(aa.new_due_date AS text),
			CAST(aa.old_amount AS text),
			CAST(aa.new_amount AS text),
			aa.old_currency_code,
			aa.new_currency_code
		FROM auditactionreceivable aa
		WHERE aa.action_id = $1 AND aa.receivable_id = $2
	`, actionID, receivableID).Scan(
		&oldEntity, &newEntity,
		&oldCounter, &newCounter,
		&oldInvoice, &newInvoice,
		&oldInvDate, &newInvDate,
		&oldDueDate, &newDueDate,
		&oldAmount, &newAmount,
		&oldCurrency, &newCurrency,
	)
	if err != nil {
		return nil
	}

	changes := make([]map[string]interface{}, 0)
	appendTransactionChange(&changes, "Entity Name", oldEntity, newEntity)
	appendTransactionChange(&changes, "Counterparty Name", oldCounter, newCounter)
	appendTransactionChange(&changes, "Invoice Number", oldInvoice, newInvoice)
	appendTransactionChange(&changes, "Invoice Date", oldInvDate, newInvDate)
	appendTransactionChange(&changes, "Due Date", oldDueDate, newDueDate)
	appendTransactionChange(&changes, "Amount", oldAmount, newAmount)
	appendTransactionChange(&changes, "Currency Code", oldCurrency, newCurrency)
	return changes
}

func appendTransactionChange(changes *[]map[string]interface{}, fieldName string, oldValue, newValue sql.NullString) {
	if !newValue.Valid {
		return
	}
	if oldValue.Valid && strings.TrimSpace(oldValue.String) == strings.TrimSpace(newValue.String) {
		return
	}

	var oldOut interface{}
	if oldValue.Valid {
		oldOut = oldValue.String
	}

	*changes = append(*changes, map[string]interface{}{
		"field":     fieldName,
		"old_value": oldOut,
		"new_value": newValue.String,
	})
}

func stringPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func timePointerValue(value *time.Time) interface{} {
	return api.FormatAuditTimestampPtrIST(value)
}

func transactionRequestedBy(userID string) string {
	for _, s := range auth.GetActiveSessions() {
		if s.UserID == userID {
			if strings.TrimSpace(s.Name) != "" {
				return strings.TrimSpace(s.Name)
			}
			break
		}
	}
	return strings.TrimSpace(userID)
}

func insertTransactionDownloadAudit(ctx context.Context, pgxPool *pgxpool.Pool, transactionType, transactionID, requestedBy, uploadS3Key, requestedIP string) {
	transactionID = strings.TrimSpace(transactionID)
	requestedBy = strings.TrimSpace(requestedBy)
	transactionType = strings.ToUpper(strings.TrimSpace(transactionType))
	uploadS3Key = strings.TrimSpace(uploadS3Key)
	if transactionID == "" {
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

	switch transactionType {
	case "PAYABLE":
	case "RECEIVABLE":
	default:
		return
	}

	if _, err := pgxPool.Exec(ctx, `
		INSERT INTO auditactiontransactiondownloads (transaction_type, transaction_id, requested_by, requested_at, requested_ip, file_name, upload_s3_key)
		VALUES ($1, $2, $3, now(), $4, $5, $6)
	`, transactionType, transactionID, requestedBy, transactionNullIfEmpty(requestedIP), transactionExtractAuditFileName(uploadS3Key), transactionNullIfEmpty(uploadS3Key)); err != nil {
		logger.LogError("failed to insert %s download audit for %s: %v", strings.ToLower(transactionType), transactionID, err)
	}
}

func transactionNullIfEmpty(value string) interface{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func transactionExtractAuditFileName(uploadS3Key string) interface{} {
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
