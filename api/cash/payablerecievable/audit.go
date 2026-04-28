package payablerecievable

import (
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type transactionAuditRequest struct {
	UserID          string `json:"user_id"`
	TransactionType string `json:"transaction_type"`
	TransactionID   string `json:"transaction_id"`
}

func GetTransactionAuditHandler(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req transactionAuditRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" || strings.TrimSpace(req.TransactionID) == "" || strings.TrimSpace(req.TransactionType) == "" {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "user_id, transaction_type and transaction_id are required"})
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
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "transaction_type must be PAYABLE or RECEIVABLE"})
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				`+idColumn+`,
				actiontype,
				processing_status,
				requested_by,
				requested_at,
				checker_by,
				checker_at,
				checker_comment,
				reason
			FROM `+tableName+`
			WHERE `+idColumn+` = $1
			ORDER BY requested_at ASC, action_id ASC
		`, req.TransactionID)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": err.Error()})
			return
		}
		defer rows.Close()

		payload := make([]map[string]interface{}, 0)
		for rows.Next() {
			var entityID, action, status, performedBy string
			var performedAt time.Time
			var checkerBy, checkerComment, reason *string
			var checkerAt *time.Time
			if err := rows.Scan(&entityID, &action, &status, &performedBy, &performedAt, &checkerBy, &checkerAt, &checkerComment, &reason); err != nil {
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to read transaction audit history"})
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":      entityID,
				"action":         action,
				"status":         status,
				"performed_by":   performedBy,
				"performed_at":   performedAt,
				"checker_by":     stringPointerValue(checkerBy),
				"checker_at":     timePointerValue(checkerAt),
				"comment":        stringPointerValue(checkerComment),
				"reason":         stringPointerValue(reason),
				"change_summary": buildTransactionChangeSummary(ctx, pgxPool, txType, req.TransactionID, action),
			})
		}
		if err := rows.Err(); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to read transaction audit history"})
			return
		}

		downloadRows, err := pgxPool.Query(ctx, `
			SELECT transaction_id, requested_by, requested_at, file_name, upload_s3_key
			FROM auditactiontransactiondownloads
			WHERE transaction_type = $1 AND transaction_id = $2
			ORDER BY requested_at ASC, download_audit_id ASC
		`, txType, req.TransactionID)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to read transaction download audit history"})
			return
		}
		defer downloadRows.Close()

		for downloadRows.Next() {
			var entityID, requestedBy string
			var requestedAt sql.NullTime
			var fileName, uploadKey sql.NullString
			if err := downloadRows.Scan(&entityID, &requestedBy, &requestedAt, &fileName, &uploadKey); err != nil {
				json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to read transaction download audit history"})
				return
			}

			payload = append(payload, map[string]interface{}{
				"entity_id":     entityID,
				"action":        "DOWNLOAD",
				"status":        "COMPLETED",
				"performed_by":  strings.TrimSpace(requestedBy),
				"performed_at":  timePointerValue(&requestedAt.Time),
				"checker_by":    "",
				"checker_at":    nil,
				"comment":       "",
				"reason":        "",
				"file_name":     fileName.String,
				"upload_s3_key": uploadKey.String,
				"source":        txType,
			})
		}
		if err := downloadRows.Err(); err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{constants.ValueSuccess: false, "message": "failed to read transaction download audit history"})
			return
		}

		// Standardize: always return 'rows' as the array field
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"rows":    payload,
		})
	}
}

func buildTransactionChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, transactionType, transactionID, action string) []map[string]interface{} {
	if !strings.EqualFold(strings.TrimSpace(action), "EDIT") {
		return nil
	}

	switch strings.ToUpper(strings.TrimSpace(transactionType)) {
	case "PAYABLE":
		return buildPayableChangeSummary(ctx, pgxPool, transactionID)
	case "RECEIVABLE":
		return buildReceivableChangeSummary(ctx, pgxPool, transactionID)
	default:
		return nil
	}
}

func buildPayableChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, payableID string) []map[string]interface{} {
	var (
		entityName, oldEntityName             string
		counterpartyName, oldCounterpartyName string
		invoiceNumber, oldInvoiceNumber       string
		invoiceDate, oldInvoiceDate           string
		dueDate, oldDueDate                   string
		amount, oldAmount                     string
		currencyCode, oldCurrencyCode         string
	)

	err := pgxPool.QueryRow(ctx, `
		SELECT
			COALESCE(entity_name, ''),
			COALESCE(old_entity_name, ''),
			COALESCE(counterparty_name, ''),
			COALESCE(old_counterparty_name, ''),
			COALESCE(invoice_number, ''),
			COALESCE(old_invoice_number, ''),
			COALESCE(TO_CHAR(invoice_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_invoice_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(due_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_due_date, 'YYYY-MM-DD'), ''),
			COALESCE(CAST(amount AS text), ''),
			COALESCE(CAST(old_amount AS text), ''),
			COALESCE(currency_code, ''),
			COALESCE(old_currency_code, '')
		FROM tr_payables
		WHERE payable_id = $1
	`, payableID).Scan(
		&entityName, &oldEntityName,
		&counterpartyName, &oldCounterpartyName,
		&invoiceNumber, &oldInvoiceNumber,
		&invoiceDate, &oldInvoiceDate,
		&dueDate, &oldDueDate,
		&amount, &oldAmount,
		&currencyCode, &oldCurrencyCode,
	)
	if err != nil {
		return nil
	}

	changes := make([]map[string]interface{}, 0)
	appendTransactionChange(&changes, "Entity Name", oldEntityName, entityName)
	appendTransactionChange(&changes, "Counterparty Name", oldCounterpartyName, counterpartyName)
	appendTransactionChange(&changes, "Invoice Number", oldInvoiceNumber, invoiceNumber)
	appendTransactionChange(&changes, "Invoice Date", oldInvoiceDate, invoiceDate)
	appendTransactionChange(&changes, "Due Date", oldDueDate, dueDate)
	appendTransactionChange(&changes, "Amount", oldAmount, amount)
	appendTransactionChange(&changes, "Currency Code", oldCurrencyCode, currencyCode)
	return changes
}

func buildReceivableChangeSummary(ctx context.Context, pgxPool *pgxpool.Pool, receivableID string) []map[string]interface{} {
	var (
		entityName, oldEntityName             string
		counterpartyName, oldCounterpartyName string
		invoiceNumber, oldInvoiceNumber       string
		invoiceDate, oldInvoiceDate           string
		dueDate, oldDueDate                   string
		amount, oldAmount                     string
		currencyCode, oldCurrencyCode         string
	)

	err := pgxPool.QueryRow(ctx, `
		SELECT
			COALESCE(entity_name, ''),
			COALESCE(old_entity_name, ''),
			COALESCE(counterparty_name, ''),
			COALESCE(old_counterparty_name, ''),
			COALESCE(invoice_number, ''),
			COALESCE(old_invoice_number, ''),
			COALESCE(TO_CHAR(invoice_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_invoice_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(due_date, 'YYYY-MM-DD'), ''),
			COALESCE(TO_CHAR(old_due_date, 'YYYY-MM-DD'), ''),
			COALESCE(CAST(invoice_amount AS text), ''),
			COALESCE(CAST(old_invoice_amount AS text), ''),
			COALESCE(currency_code, ''),
			COALESCE(old_currency_code, '')
		FROM tr_receivables
		WHERE receivable_id = $1
	`, receivableID).Scan(
		&entityName, &oldEntityName,
		&counterpartyName, &oldCounterpartyName,
		&invoiceNumber, &oldInvoiceNumber,
		&invoiceDate, &oldInvoiceDate,
		&dueDate, &oldDueDate,
		&amount, &oldAmount,
		&currencyCode, &oldCurrencyCode,
	)
	if err != nil {
		return nil
	}

	changes := make([]map[string]interface{}, 0)
	appendTransactionChange(&changes, "Entity Name", oldEntityName, entityName)
	appendTransactionChange(&changes, "Counterparty Name", oldCounterpartyName, counterpartyName)
	appendTransactionChange(&changes, "Invoice Number", oldInvoiceNumber, invoiceNumber)
	appendTransactionChange(&changes, "Invoice Date", oldInvoiceDate, invoiceDate)
	appendTransactionChange(&changes, "Due Date", oldDueDate, dueDate)
	appendTransactionChange(&changes, "Amount", oldAmount, amount)
	appendTransactionChange(&changes, "Currency Code", oldCurrencyCode, currencyCode)
	return changes
}

func appendTransactionChange(changes *[]map[string]interface{}, fieldName, oldValue, newValue string) {
	if strings.TrimSpace(oldValue) == strings.TrimSpace(newValue) {
		return
	}

	*changes = append(*changes, map[string]interface{}{
		"field":     fieldName,
		"old_value": oldValue,
		"new_value": newValue,
	})
}

func stringPointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func timePointerValue(value *time.Time) interface{} {
	if value == nil {
		return nil
	}
	return *value
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

func insertTransactionDownloadAudit(ctx context.Context, pgxPool *pgxpool.Pool, transactionType, transactionID, requestedBy, uploadS3Key string) {
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
		INSERT INTO auditactiontransactiondownloads (transaction_type, transaction_id, requested_by, requested_at, file_name, upload_s3_key)
		VALUES ($1, $2, $3, now(), $4, $5)
	`, transactionType, transactionID, requestedBy, transactionExtractAuditFileName(uploadS3Key), transactionNullIfEmpty(uploadS3Key)); err != nil {
		log.Printf("failed to insert %s download audit for %s: %v", strings.ToLower(transactionType), transactionID, err)
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
