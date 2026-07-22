package payablerecievable

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/validation"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

func validatePayRecScope(ctx context.Context, entityName, counterpartyName, currencyCode string) string {
	return validation.ValidateCashMasterReferences(ctx, map[string]interface{}{
		"entity_name":       entityName,
		"counterparty_name": counterpartyName,
		"currency_code":     currencyCode,
	})
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func GetTransactionDownloadURL(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string `json:"user_id"`
			TransactionType string `json:"transaction_type"`
			TransactionID   string `json:"transaction_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.TransactionID) == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "transaction_id is required", "")
			return
		}

		ctx := r.Context()
		txType := strings.ToUpper(strings.TrimSpace(req.TransactionType))
		requestedBy := transactionRequestedBy(req.UserID)
		if txType == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "transaction_type is required", "")
			return
		}

		var uploadS3Key *string
		var entityName, counterpartyName, currencyCode string
		switch txType {
		case "PAYABLE":
			err := pgxPool.QueryRow(ctx, `SELECT upload_s3_key, COALESCE(entity_name,''), COALESCE(counterparty_name,''), COALESCE(currency_code,'') FROM tr_payables WHERE payable_id = $1 AND is_deleted != TRUE`, req.TransactionID).Scan(&uploadS3Key, &entityName, &counterpartyName, &currencyCode)
			if err != nil {
				api.RespondEnvelopeError(w, http.StatusNotFound, "payable not found", "")
				return
			}
		case "RECEIVABLE":
			err := pgxPool.QueryRow(ctx, `SELECT upload_s3_key, COALESCE(entity_name,''), COALESCE(counterparty_name,''), COALESCE(currency_code,'') FROM tr_receivables WHERE receivable_id = $1 AND is_deleted != TRUE`, req.TransactionID).Scan(&uploadS3Key, &entityName, &counterpartyName, &currencyCode)
			if err != nil {
				api.RespondEnvelopeError(w, http.StatusNotFound, "receivable not found", "")
				return
			}
		default:
			api.RespondEnvelopeError(w, http.StatusBadRequest, "transaction_type must be PAYABLE or RECEIVABLE", "")
			return
		}
		if msg := validatePayRecScope(ctx, entityName, counterpartyName, currencyCode); msg != "" {
			api.RespondEnvelopeError(w, http.StatusForbidden, msg, "")
			return
		}

		if uploadS3Key == nil || strings.TrimSpace(*uploadS3Key) == "" {
			api.RespondEnvelopeError(w, http.StatusNotFound, "no file available", "")
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, strings.TrimSpace(*uploadS3Key), 15*time.Minute)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to generate download url", "")
			return
		}

		insertTransactionDownloadAudit(ctx, pgxPool, txType, req.TransactionID, requestedBy, strings.TrimSpace(*uploadS3Key), api.ClientIPFromRequest(r))

		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"download_url": downloadURL,
		})
	}
}

func GetTransactionBulkDownloadURL(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			TransactionType string   `json:"transaction_type"`
			TransactionIDs  []string `json:"transaction_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.TransactionIDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "transaction_ids are required", "")
			return
		}

		ctx := r.Context()
		txType := strings.ToUpper(strings.TrimSpace(req.TransactionType))
		requestedBy := transactionRequestedBy(req.UserID)
		if txType == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "transaction_type is required", "")
			return
		}

		files := make([]map[string]string, 0, len(req.TransactionIDs))
		failedIDs := make([]string, 0)

		for _, rawID := range req.TransactionIDs {
			transactionID := strings.TrimSpace(rawID)
			if transactionID == "" {
				continue
			}

			var uploadS3Key *string
			var entityName, counterpartyName, currencyCode string
			switch txType {
			case "PAYABLE":
				err := pgxPool.QueryRow(ctx, `SELECT upload_s3_key, COALESCE(entity_name,''), COALESCE(counterparty_name,''), COALESCE(currency_code,'') FROM tr_payables WHERE payable_id = $1 AND is_deleted != TRUE`, transactionID).Scan(&uploadS3Key, &entityName, &counterpartyName, &currencyCode)
				if err != nil {
					failedIDs = append(failedIDs, transactionID)
					continue
				}
			case "RECEIVABLE":
				err := pgxPool.QueryRow(ctx, `SELECT upload_s3_key, COALESCE(entity_name,''), COALESCE(counterparty_name,''), COALESCE(currency_code,'') FROM tr_receivables WHERE receivable_id = $1 AND is_deleted != TRUE`, transactionID).Scan(&uploadS3Key, &entityName, &counterpartyName, &currencyCode)
				if err != nil {
					failedIDs = append(failedIDs, transactionID)
					continue
				}
			default:
				api.RespondEnvelopeError(w, http.StatusBadRequest, "transaction_type must be PAYABLE or RECEIVABLE", "")
				return
			}
			if msg := validatePayRecScope(ctx, entityName, counterpartyName, currencyCode); msg != "" {
				failedIDs = append(failedIDs, transactionID)
				continue
			}

			if uploadS3Key == nil || strings.TrimSpace(*uploadS3Key) == "" {
				failedIDs = append(failedIDs, transactionID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, strings.TrimSpace(*uploadS3Key), 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, transactionID)
				continue
			}

			files = append(files, map[string]string{
				"transaction_id": transactionID,
				"download_url":   downloadURL,
			})
			insertTransactionDownloadAudit(ctx, pgxPool, txType, transactionID, requestedBy, strings.TrimSpace(*uploadS3Key), api.ClientIPFromRequest(r))
		}

		if len(files) == 0 {
			api.RespondEnvelopeFailureWithData(w, http.StatusNotFound, "no downloadable files found", "", map[string]interface{}{
				"files":      []map[string]string{},
				"failed_ids": failedIDs,
			})
			return
		}

		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"files":      files,
			"failed_ids": failedIDs,
		})
	}
}

// helpers used by bulk update flow
func nullifyEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

type sqlNullString struct {
	Valid bool
	S     string
}

func (n *sqlNullString) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		n.S = ""
		return nil
	}
	switch t := v.(type) {
	case string:
		n.Valid = true
		n.S = t
	case []byte:
		n.Valid = true
		n.S = string(t)
	default:
		n.Valid = true
		n.S = fmt.Sprint(v)
	}
	return nil
}

func (n sqlNullString) ValueOrZero() interface{} {
	if n.Valid {
		return n.S
	}
	return ""
}

type sqlNullFloat struct {
	Valid bool
	F     float64
}

func (n *sqlNullFloat) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		n.F = 0
		return nil
	}
	switch t := v.(type) {
	case float64:
		n.Valid = true
		n.F = t
	case int64:
		n.Valid = true
		n.F = float64(t)
	case []byte:
		s := string(t)
		if s == "" {
			n.Valid = false
			n.F = 0
			return nil
		}
		var err error
		n.F, err = strconv.ParseFloat(s, 64)
		if err != nil {
			n.Valid = false
			n.F = 0
			return nil
		}
		n.Valid = true
	case string:
		if t == "" {
			n.Valid = false
			n.F = 0
			return nil
		}
		var err error
		n.F, err = strconv.ParseFloat(t, 64)
		if err != nil {
			n.Valid = false
			n.F = 0
			return nil
		}
		n.Valid = true
	default:
		n.Valid = true
		n.F = 0
	}
	return nil
}

func (n sqlNullFloat) ValueOrZero() interface{} {
	if n.Valid {
		return n.F
	}
	return nil
}

type sqlNullTime struct {
	Valid bool
	T     time.Time
}

func (n *sqlNullTime) Scan(v interface{}) error {
	if v == nil {
		n.Valid = false
		return nil
	}
	switch t := v.(type) {
	case time.Time:
		n.Valid = true
		n.T = t
	case []byte:
		s := string(t)
		if s == "" {
			n.Valid = false
			return nil
		}
		if parsed, err := time.Parse(constants.DateFormat, s); err == nil {
			n.Valid = true
			n.T = parsed
		} else {
			n.Valid = false
		}
	case string:
		if t == "" {
			n.Valid = false
			return nil
		}
		if parsed, err := time.Parse(constants.DateFormat, t); err == nil {
			n.Valid = true
			n.T = parsed
		} else {
			n.Valid = false
		}
	default:
		n.Valid = false
	}
	return nil
}

func (n sqlNullTime) ValueOrZero() interface{} {
	if n.Valid {
		return n.T
	}
	return nil
}
func getAuditInfoPayable(ctx context.Context, pgxPool *pgxpool.Pool, payableID string) (createdBy, createdAt, createdStatus, editedBy, editedAt, editedStatus, deletedBy, deletedAt, deletedStatus string) {
	auditDetailsQuery := `SELECT actiontype, requested_by, requested_at, processing_status FROM auditactionpayable WHERE payable_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC, action_id DESC`
	auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, payableID)
	if auditErr == nil {
		defer auditRows.Close()
		for auditRows.Next() {
			var atype, status string
			var rbyPtr *string
			var ratPtr *time.Time
			if err := auditRows.Scan(&atype, &rbyPtr, &ratPtr, &status); err == nil {
				by := ""
				at := ""
				if rbyPtr != nil {
					by = *rbyPtr
				}
				if ratPtr != nil {
					at = api.FormatAuditTimestampIST(*ratPtr)
				}
				if atype == "CREATE" && createdBy == "" {
					createdBy = by
					createdAt = at
					createdStatus = status
				} else if atype == constants.AuditActionEdit && editedBy == "" {
					editedBy = by
					editedAt = at
					editedStatus = status
				} else if atype == constants.AuditActionDelete && deletedBy == "" {
					deletedBy = by
					deletedAt = at
					deletedStatus = status
				}
			}
		}
	}
	return
}

func getAuditInfoReceivable(ctx context.Context, pgxPool *pgxpool.Pool, receivableID string) (createdBy, createdAt, createdStatus, editedBy, editedAt, editedStatus, deletedBy, deletedAt, deletedStatus string) {
	auditDetailsQuery := `SELECT actiontype, requested_by, requested_at, processing_status FROM auditactionreceivable WHERE receivable_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC, action_id DESC`
	auditRows, auditErr := pgxPool.Query(ctx, auditDetailsQuery, receivableID)
	if auditErr == nil {
		defer auditRows.Close()
		for auditRows.Next() {
			var atype, status string
			var rbyPtr *string
			var ratPtr *time.Time
			if err := auditRows.Scan(&atype, &rbyPtr, &ratPtr, &status); err == nil {
				by := ""
				at := ""
				if rbyPtr != nil {
					by = *rbyPtr
				}
				if ratPtr != nil {
					at = api.FormatAuditTimestampIST(*ratPtr)
				}
				if atype == "CREATE" && createdBy == "" {
					createdBy = by
					createdAt = at
					createdStatus = status
				} else if atype == constants.AuditActionEdit && editedBy == "" {
					editedBy = by
					editedAt = at
					editedStatus = status
				} else if atype == constants.AuditActionDelete && deletedBy == "" {
					deletedBy = by
					deletedAt = at
					deletedStatus = status
				}
			}
		}
	}
	return
}

// Helper: get file extension
func getFileExt(filename string) string {
	return strings.ToLower(filepath.Ext(filename))
}

// Helper: parse uploaded file into [][]string
func parseUploadFile(file multipart.File, ext string) ([][]string, error) {
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
	return nil, errors.New(constants.ErrUnsupportedFileType)
}

// Helper: normalize date string to YYYY-MM-DD
func normalizeDate(dateStr string) string {
	layouts := []string{constants.DateFormat, constants.DateFormatAlt, "01/02/2006", "2 Jan 2006", "2006/01/02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t.Format(constants.DateFormat)
		}
	}
	return dateStr // fallback, let DB error if invalid
}

func validateLegacyPayRecUploadScope(ctx context.Context, pgxPool *pgxpool.Pool, batchID string) error {
	rows, err := pgxPool.Query(ctx, `
		SELECT transaction_type,
		       COALESCE(entity_name, entity_id, ''),
		       COALESCE(counterparty_name, vendor_id, customer_id, ''),
		       COALESCE(currency_code, '')
		FROM input_transactions
		WHERE upload_batch_id = $1
	`, batchID)
	if err != nil {
		return fmt.Errorf("failed to validate upload scope: %w", err)
	}
	defer rows.Close()

	rowNo := 0
	for rows.Next() {
		rowNo++
		var txType, entityName, counterpartyName, currencyCode string
		if err := rows.Scan(&txType, &entityName, &counterpartyName, &currencyCode); err != nil {
			return fmt.Errorf("failed to validate upload row %d: %w", rowNo, err)
		}
		if msg := validatePayRecScope(ctx, entityName, counterpartyName, currencyCode); msg != "" {
			return fmt.Errorf("%s row %d failed scope validation: %s", strings.ToLower(txType), rowNo, msg)
		}
	}
	return rows.Err()
}

// Handler: UploadPayRec (for payables/receivables)
func UploadPayRec(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		userID := ""
		if r.Header.Get(constants.ContentTypeText) == constants.ContentTypeJSON {
			var req struct {
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
				http.Error(w, "user_id required in body", http.StatusBadRequest)
				return
			}
			userID = req.UserID
		} else {
			userID = r.FormValue(constants.KeyUserID)
			if userID == "" {
				http.Error(w, constants.ErrUserIDRequired, http.StatusBadRequest)
				return
			}
		}

		// Fetch user name from active sessions
		userName := ""
		sessions := auth.GetActiveSessions()
		for _, s := range sessions {
			if s.UserID == userID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			http.Error(w, constants.ErrInvalidSession, http.StatusUnauthorized)
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, constants.ErrFailedToParseMultipartForm, http.StatusBadRequest)
			return
		}
		if len(r.MultipartForm.File) == 0 {
			http.Error(w, constants.ErrNoFilesUploaded, http.StatusBadRequest)
			return
		}

		// Read mapping
		mapRows, err := pgxPool.Query(ctx, `SELECT source_column_name, target_field_name FROM upload_mapping_input_transactions`)
		if err != nil {
			http.Error(w, "Mapping error", http.StatusInternalServerError)
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

		if !runtime.Enforce(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreUpload,
			ModuleCode:          common.ModuleCash,
			SubModule:           "PAYABLE_RECEIVABLE",
			ActorUserID:         userID,
			HandlerName:         "UploadPayRec",
			APIPath:             "/cash/upload-payrec",
			DefaultBlockMessage: "Payable/receivable upload blocked by policy",
			Fields:              map[string]interface{}{"file_count": len(r.MultipartForm.File)},
		}) {
			return
		}

		batchIDs := make([]string, 0)
		for txType, files := range r.MultipartForm.File {
			txTypeUpper := strings.ToUpper(txType)
			for _, fileHeader := range files {
				file, err := fileHeader.Open()
				if err != nil {
					http.Error(w, "Failed to open file: "+fileHeader.Filename, http.StatusBadRequest)
					return
				}
				ext := getFileExt(fileHeader.Filename)
				records, err := parseUploadFile(file, ext)
				file.Close()
				if err != nil || len(records) < 2 {
					http.Error(w, "Invalid or empty file: "+fileHeader.Filename, http.StatusBadRequest)
					return
				}
				headerRow := records[0]
				dataRows := records[1:]
				batchID := uuid.New().String()
				batchIDs = append(batchIDs, batchID)
				colCount := len(headerRow)
				copyRows := make([][]interface{}, len(dataRows))
				for i, row := range dataRows {
					vals := make([]interface{}, colCount+2) // +2 for batchID and transaction_type
					vals[0] = batchID
					vals[1] = txTypeUpper
					for j := 0; j < colCount; j++ {
						val := ""
						if j < len(row) {
							val = row[j]
						}
						// Normalize date columns
						if mapping[headerRow[j]] == "invoice_date" || mapping[headerRow[j]] == "due_date" {
							val = normalizeDate(val)
						}
						vals[j+2] = val
					}
					copyRows[i] = vals
				}
				columns := append([]string{"upload_batch_id", "transaction_type"}, headerRow...)
				_, err = pgxPool.CopyFrom(
					ctx,
					pgx.Identifier{"input_transactions"},
					columns,
					pgx.CopyFromRows(copyRows),
				)
				if err != nil {
					http.Error(w, "Failed to stage data: "+err.Error(), http.StatusInternalServerError)
					return
				}
				if err := validateLegacyPayRecUploadScope(ctx, pgxPool, batchID); err != nil {
					http.Error(w, err.Error(), http.StatusForbidden)
					return
				}

				// Move to final table(s)
				if txTypeUpper == "PAYABLE" {
					// Insert into payables and get payable_ids
					rows, err := pgxPool.Query(ctx, `
						INSERT INTO payables (entity_id, vendor_id, invoice_number, invoice_date, due_date, amount, currency_code)
						SELECT entity_id, vendor_id, invoice_number, invoice_date::date, due_date::date, amount::numeric, currency_code
						FROM input_transactions WHERE upload_batch_id = $1
						RETURNING payable_id
					`, batchID)
					if err != nil {
						http.Error(w, "Final insert error (payables): "+err.Error(), http.StatusInternalServerError)
						return
					}
					var payableIDs []string
					for rows.Next() {
						var payableID string
						if err := rows.Scan(&payableID); err == nil {
							payableIDs = append(payableIDs, payableID)
						}
					}
					rows.Close()
					if len(payableIDs) > 0 {
						_, auditErr := pgxPool.Exec(ctx, `
							INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
							SELECT unnest($1::text[]), 'CREATE', 'PENDING_APPROVAL', NULL, $2, now(), $3
						`, payableIDs, userName, transactionNullIfEmpty(api.ClientIPFromRequest(r)))
						if auditErr != nil {
							http.Error(w, "Audit log error (payables): "+auditErr.Error(), http.StatusInternalServerError)
							return
						}
					}
				} else if txTypeUpper == "RECEIVABLE" {
					// Insert into receivables and get receivable_ids
					rows, err := pgxPool.Query(ctx, `
						INSERT INTO receivables (entity_id, customer_id, invoice_number, invoice_date, due_date, invoice_amount, currency_code)
						SELECT entity_id, customer_id, invoice_number, invoice_date::date, due_date::date, invoice_amount::numeric, currency_code
						FROM input_transactions WHERE upload_batch_id = $1
						RETURNING receivable_id
					`, batchID)
					if err != nil {
						http.Error(w, "Final insert error (receivables): "+err.Error(), http.StatusInternalServerError)
						return
					}
					var receivableIDs []string
					for rows.Next() {
						var receivableID string
						if err := rows.Scan(&receivableID); err == nil {
							receivableIDs = append(receivableIDs, receivableID)
						}
					}
					rows.Close()
					if len(receivableIDs) > 0 {
						_, auditErr := pgxPool.Exec(ctx, `
							INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
							SELECT unnest($1::text[]), 'CREATE', 'PENDING_APPROVAL', NULL, $2, now(), $3
						`, receivableIDs, userName, transactionNullIfEmpty(api.ClientIPFromRequest(r)))
						if auditErr != nil {
							http.Error(w, "Audit log error (receivables): "+auditErr.Error(), http.StatusInternalServerError)
							return
						}
					}
				} else {
					http.Error(w, "Unknown transaction_type: "+txTypeUpper, http.StatusBadRequest)
					return
				}
			}
		}
		api.RespondEnvelopeSuccess(w, "All transactions uploaded and processed", nil)
	}
}

func GetAllPayableReceivable(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		idMaps := LoadPayRecMasterIDMaps(ctx, pgxPool)
		type Payable struct {
			PayableID        string  `json:"payable_id"`
			EntityID         string  `json:"entity_id"`
			EntityName       string  `json:"entity_name"`
			CounterpartyID   string  `json:"counterparty_id"`
			CounterpartyName string  `json:"counterparty_name"`
			InvoiceNo        string  `json:"invoice_number"`
			InvoiceDate      string  `json:"invoice_date"`
			DueDate          string  `json:"due_date"`
			Amount           float64 `json:"amount"`
			CurrencyCode     string  `json:"currency_code"`
			UploadS3Key      string  `json:"upload_s3_key"`
			// old values
			OldEntityName   string  `json:"old_entity_name"`
			OldCounterparty string  `json:"old_counterparty_name"`
			OldInvoiceNo    string  `json:"old_invoice_number"`
			OldInvoiceDate  string  `json:"old_invoice_date"`
			OldDueDate      string  `json:"old_due_date"`
			OldAmount       float64 `json:"old_amount"`
			OldCurrencyCode string  `json:"old_currency_code"`
			Status          string  `json:"status"`
			CreatedBy       string  `json:"created_by"`
			CreatedAt       string  `json:"created_at"`
			EditedBy        string  `json:"edited_by"`
			EditedAt        string  `json:"edited_at"`
			DeletedBy       string  `json:"deleted_by"`
			DeletedAt       string  `json:"deleted_at"`
		}
		type Receivable struct {
			ReceivableID     string  `json:"receivable_id"`
			EntityID         string  `json:"entity_id"`
			EntityName       string  `json:"entity_name"`
			CounterpartyID   string  `json:"counterparty_id"`
			CounterpartyName string  `json:"counterparty_name"`
			InvoiceNo        string  `json:"invoice_number"`
			InvoiceDate      string  `json:"invoice_date"`
			DueDate          string  `json:"due_date"`
			Amount           float64 `json:"invoice_amount"`
			CurrencyCode     string  `json:"currency_code"`
			UploadS3Key      string  `json:"upload_s3_key"`
			// old values
			OldEntityName   string  `json:"old_entity_name"`
			OldCounterparty string  `json:"old_counterparty_name"`
			OldInvoiceNo    string  `json:"old_invoice_number"`
			OldInvoiceDate  string  `json:"old_invoice_date"`
			OldDueDate      string  `json:"old_due_date"`
			OldAmount       float64 `json:"old_invoice_amount"`
			OldCurrencyCode string  `json:"old_currency_code"`
			Status          string  `json:"status"`
			CreatedBy       string  `json:"created_by"`
			CreatedAt       string  `json:"created_at"`
			EditedBy        string  `json:"edited_by"`
			EditedAt        string  `json:"edited_at"`
			DeletedBy       string  `json:"deleted_by"`
			DeletedAt       string  `json:"deleted_at"`
		}

		// 1. Fetch all payables (new table tr_payables)
		payableRows, err := pgxPool.Query(ctx, `
			SELECT
				p.payable_id,
				COALESCE(e.entity_id::text, ''),
				COALESCE(p.entity_name, ''),
				COALESCE(c.counterparty_id::text, ''),
				COALESCE(p.counterparty_name, ''),
				p.invoice_number,
				p.invoice_date,
				p.due_date,
				p.amount,
				p.currency_code,
				p.upload_s3_key,
				p.old_entity_name,
				p.old_counterparty_name,
				p.old_invoice_number,
				p.old_invoice_date,
				p.old_due_date,
				p.old_amount,
				p.old_currency_code
			FROM tr_payables p
			LEFT JOIN masterentitycash e
				ON LOWER(TRIM(e.entity_name)) = LOWER(TRIM(p.entity_name))
				AND COALESCE(e.is_deleted, false) = false
			LEFT JOIN mastercounterparty c
				ON LOWER(TRIM(c.counterparty_name)) = LOWER(TRIM(p.counterparty_name))
				AND COALESCE(c.is_deleted, false) = false
			WHERE COALESCE(p.is_deleted, false) != TRUE`)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, err.Error(), "")
			return
		}
		defer payableRows.Close()
		var payables []Payable
		var payableIDs []string
		for payableRows.Next() {
			var p Payable
			var invoiceDate, dueDate *time.Time
			var uploadS3Key *string
			var oldEntityPtr, oldCounterPtr, oldInvoicePtr *string
			var oldInvoiceDate, oldDueDate *time.Time
			var oldAmountPtr *float64
			var oldCurrencyPtr *string
			if err := payableRows.Scan(&p.PayableID, &p.EntityID, &p.EntityName, &p.CounterpartyID, &p.CounterpartyName, &p.InvoiceNo, &invoiceDate, &dueDate, &p.Amount, &p.CurrencyCode, &uploadS3Key, &oldEntityPtr, &oldCounterPtr, &oldInvoicePtr, &oldInvoiceDate, &oldDueDate, &oldAmountPtr, &oldCurrencyPtr); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, err.Error(), "")
				return
			}
			p.EntityID, p.CounterpartyID = EnrichPayRecRowIDs(idMaps, p.EntityName, p.CounterpartyName, p.EntityID, p.CounterpartyID)
			if uploadS3Key != nil {
				p.UploadS3Key = *uploadS3Key
			} else {
				p.UploadS3Key = ""
			}
			// populate old fields safely
			if oldEntityPtr != nil {
				p.OldEntityName = *oldEntityPtr
			} else {
				p.OldEntityName = ""
			}
			if oldCounterPtr != nil {
				p.OldCounterparty = *oldCounterPtr
			} else {
				p.OldCounterparty = ""
			}
			if oldInvoicePtr != nil {
				p.OldInvoiceNo = *oldInvoicePtr
			} else {
				p.OldInvoiceNo = ""
			}
			if oldInvoiceDate != nil {
				p.OldInvoiceDate = oldInvoiceDate.Format(constants.DateFormat)
			} else {
				p.OldInvoiceDate = ""
			}
			if oldDueDate != nil {
				p.OldDueDate = oldDueDate.Format(constants.DateFormat)
			} else {
				p.OldDueDate = ""
			}
			if oldAmountPtr != nil {
				p.OldAmount = *oldAmountPtr
			} else {
				p.OldAmount = 0
			}
			if oldCurrencyPtr != nil {
				p.OldCurrencyCode = *oldCurrencyPtr
			} else {
				p.OldCurrencyCode = ""
			}
			p.InvoiceDate = ""
			if invoiceDate != nil {
				p.InvoiceDate = invoiceDate.Format(constants.DateFormat)
			}
			p.DueDate = ""
			if dueDate != nil {
				p.DueDate = dueDate.Format(constants.DateFormat)
			}
			if msg := validatePayRecScope(ctx, p.EntityName, p.CounterpartyName, p.CurrencyCode); msg != "" {
				continue
			}
			payables = append(payables, p)
			payableIDs = append(payableIDs, p.PayableID)
		}

		// 2. Fetch all receivables (new table tr_receivables)
		receivableRows, err := pgxPool.Query(ctx, `
			SELECT
				r.receivable_id,
				COALESCE(e.entity_id::text, ''),
				COALESCE(r.entity_name, ''),
				COALESCE(c.counterparty_id::text, ''),
				COALESCE(r.counterparty_name, ''),
				r.invoice_number,
				r.invoice_date,
				r.due_date,
				r.invoice_amount,
				r.currency_code,
				r.upload_s3_key,
				r.old_entity_name,
				r.old_counterparty_name,
				r.old_invoice_number,
				r.old_invoice_date,
				r.old_due_date,
				r.old_invoice_amount,
				r.old_currency_code
			FROM tr_receivables r
			LEFT JOIN masterentitycash e
				ON LOWER(TRIM(e.entity_name)) = LOWER(TRIM(r.entity_name))
				AND COALESCE(e.is_deleted, false) = false
			LEFT JOIN mastercounterparty c
				ON LOWER(TRIM(c.counterparty_name)) = LOWER(TRIM(r.counterparty_name))
				AND COALESCE(c.is_deleted, false) = false
			WHERE COALESCE(r.is_deleted, false) != TRUE`)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, err.Error(), "")
			return
		}
		defer receivableRows.Close()
		var receivables []Receivable
		var receivableIDs []string
		for receivableRows.Next() {
			var rcv Receivable
			var invoiceDate, dueDate *time.Time
			var uploadS3Key *string
			var oldEntityPtr, oldCounterPtr, oldInvoicePtr *string
			var oldInvoiceDate, oldDueDate *time.Time
			var oldAmountPtr *float64
			var oldCurrencyPtr *string
			if err := receivableRows.Scan(&rcv.ReceivableID, &rcv.EntityID, &rcv.EntityName, &rcv.CounterpartyID, &rcv.CounterpartyName, &rcv.InvoiceNo, &invoiceDate, &dueDate, &rcv.Amount, &rcv.CurrencyCode, &uploadS3Key, &oldEntityPtr, &oldCounterPtr, &oldInvoicePtr, &oldInvoiceDate, &oldDueDate, &oldAmountPtr, &oldCurrencyPtr); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, err.Error(), "")
				return
			}
			rcv.EntityID, rcv.CounterpartyID = EnrichPayRecRowIDs(idMaps, rcv.EntityName, rcv.CounterpartyName, rcv.EntityID, rcv.CounterpartyID)
			if uploadS3Key != nil {
				rcv.UploadS3Key = *uploadS3Key
			} else {
				rcv.UploadS3Key = ""
			}
			if oldEntityPtr != nil {
				rcv.OldEntityName = *oldEntityPtr
			} else {
				rcv.OldEntityName = ""
			}
			if oldCounterPtr != nil {
				rcv.OldCounterparty = *oldCounterPtr
			} else {
				rcv.OldCounterparty = ""
			}
			if oldInvoicePtr != nil {
				rcv.OldInvoiceNo = *oldInvoicePtr
			} else {
				rcv.OldInvoiceNo = ""
			}
			if oldInvoiceDate != nil {
				rcv.OldInvoiceDate = oldInvoiceDate.Format(constants.DateFormat)
			} else {
				rcv.OldInvoiceDate = ""
			}
			if oldDueDate != nil {
				rcv.OldDueDate = oldDueDate.Format(constants.DateFormat)
			} else {
				rcv.OldDueDate = ""
			}
			if oldAmountPtr != nil {
				rcv.OldAmount = *oldAmountPtr
			} else {
				rcv.OldAmount = 0
			}
			if oldCurrencyPtr != nil {
				rcv.OldCurrencyCode = *oldCurrencyPtr
			} else {
				rcv.OldCurrencyCode = ""
			}
			rcv.InvoiceDate = ""
			if invoiceDate != nil {
				rcv.InvoiceDate = invoiceDate.Format(constants.DateFormat)
			}
			rcv.DueDate = ""
			if dueDate != nil {
				rcv.DueDate = dueDate.Format(constants.DateFormat)
			}
			if msg := validatePayRecScope(ctx, rcv.EntityName, rcv.CounterpartyName, rcv.CurrencyCode); msg != "" {
				continue
			}
			receivables = append(receivables, rcv)
			receivableIDs = append(receivableIDs, rcv.ReceivableID)
		}

		// 3. Batch fetch audit logs for payables
		auditPayableMap := make(map[string]map[string]string)
		latestPayableAuditMap := make(map[string]string)
		if len(payableIDs) > 0 {
			query := `SELECT payable_id, actiontype, requested_by, requested_at, processing_status FROM auditactionpayable WHERE payable_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY payable_id, requested_at DESC, action_id DESC`
			rows, err := pgxPool.Query(ctx, query, payableIDs)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var pid, atype, requestedBy, status string
					var requestedAt *time.Time
					_ = rows.Scan(&pid, &atype, &requestedBy, &requestedAt, &status)
					if _, ok := auditPayableMap[pid]; !ok {
						auditPayableMap[pid] = make(map[string]string)
					}
					if _, ok := latestPayableAuditMap[pid]; !ok {
						latestPayableAuditMap[pid] = status
					}
					if atype == "CREATE" {
						auditPayableMap[pid]["created_by"] = requestedBy
						if requestedAt != nil {
							auditPayableMap[pid]["created_at"] = api.FormatAuditTimestampIST(*requestedAt)
						}
						auditPayableMap[pid]["created_status"] = status
					} else if atype == constants.AuditActionEdit {
						auditPayableMap[pid]["edited_by"] = requestedBy
						if requestedAt != nil {
							auditPayableMap[pid]["edited_at"] = api.FormatAuditTimestampIST(*requestedAt)
						}
						auditPayableMap[pid]["edited_status"] = status
					} else if atype == constants.AuditActionDelete {
						auditPayableMap[pid]["deleted_by"] = requestedBy
						if requestedAt != nil {
							auditPayableMap[pid]["deleted_at"] = api.FormatAuditTimestampIST(*requestedAt)
						}
						auditPayableMap[pid]["deleted_status"] = status
					}
				}
			}
		}
		for i := range payables {
			audit := auditPayableMap[payables[i].PayableID]
			if audit != nil {
				payables[i].CreatedBy = audit["created_by"]
				payables[i].CreatedAt = audit["created_at"]
				payables[i].EditedBy = audit["edited_by"]
				payables[i].EditedAt = audit["edited_at"]
				payables[i].DeletedBy = audit["deleted_by"]
				payables[i].DeletedAt = audit["deleted_at"]
			}
			if latestStatus, ok := latestPayableAuditMap[payables[i].PayableID]; ok {
				payables[i].Status = latestStatus
			}
		}

		// 4. Batch fetch audit logs for receivables
		auditReceivableMap := make(map[string]map[string]string)
		latestReceivableAuditMap := make(map[string]string)
		if len(receivableIDs) > 0 {
			query := `SELECT receivable_id, actiontype, requested_by, requested_at, processing_status FROM auditactionreceivable WHERE receivable_id = ANY($1) AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY receivable_id, requested_at DESC, action_id DESC`
			rows, err := pgxPool.Query(ctx, query, receivableIDs)
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					var rid, atype, requestedBy, status string
					var requestedAt *time.Time
					_ = rows.Scan(&rid, &atype, &requestedBy, &requestedAt, &status)
					if _, ok := auditReceivableMap[rid]; !ok {
						auditReceivableMap[rid] = make(map[string]string)
					}
					if _, ok := latestReceivableAuditMap[rid]; !ok {
						latestReceivableAuditMap[rid] = status
					}
					if atype == "CREATE" {
						auditReceivableMap[rid]["created_by"] = requestedBy
						if requestedAt != nil {
							auditReceivableMap[rid]["created_at"] = api.FormatAuditTimestampIST(*requestedAt)
						}
						auditReceivableMap[rid]["created_status"] = status
					} else if atype == constants.AuditActionEdit {
						auditReceivableMap[rid]["edited_by"] = requestedBy
						if requestedAt != nil {
							auditReceivableMap[rid]["edited_at"] = api.FormatAuditTimestampIST(*requestedAt)
						}
						auditReceivableMap[rid]["edited_status"] = status
					} else if atype == constants.AuditActionDelete {
						auditReceivableMap[rid]["deleted_by"] = requestedBy
						if requestedAt != nil {
							auditReceivableMap[rid]["deleted_at"] = api.FormatAuditTimestampIST(*requestedAt)
						}
						auditReceivableMap[rid]["deleted_status"] = status
					}
				}
			}
		}
		for i := range receivables {
			audit := auditReceivableMap[receivables[i].ReceivableID]
			if audit != nil {
				receivables[i].CreatedBy = audit["created_by"]
				receivables[i].CreatedAt = audit["created_at"]
				receivables[i].EditedBy = audit["edited_by"]
				receivables[i].EditedAt = audit["edited_at"]
				receivables[i].DeletedBy = audit["deleted_by"]
				receivables[i].DeletedAt = audit["deleted_at"]
			}
			if latestStatus, ok := latestReceivableAuditMap[receivables[i].ReceivableID]; ok {
				receivables[i].Status = latestStatus
			}
		}

		// Merge payables and receivables into a single array if you want a flat list, or return two arrays as separate fields
		// Here, we return both as separate top-level arrays for clarity
		api.RespondEnvelopeSuccess(w, "Success", map[string]interface{}{
			"payables":    payables,
			"receivables": receivables,
		})
	}
}

// BulkRequestDeleteTransactions inserts DELETE audit actions for mixed transaction ids (payable or receivable)
func BulkRequestDeleteTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string   `json:"user_id"`
			TransactionIDs []string `json:"transaction_ids"`
			Reason         string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.TransactionIDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidJSON, "")
			return
		}

		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondEnvelopeError(w, http.StatusUnauthorized, constants.ErrInvalidSession, "")
			return
		}
		requestedIP := api.ClientIPFromRequest(r)

		txIDsPay := []string{}
		txIDsRec := []string{}
		for _, id := range req.TransactionIDs {
			if strings.HasPrefix(id, constants.ErrPrefixPayable) {
				txIDsPay = append(txIDsPay, id)
			} else if strings.HasPrefix(id, constants.ErrPrefixReceivable) {
				txIDsRec = append(txIDsRec, id)
			}
		}

		ctx := r.Context()
		for _, id := range req.TransactionIDs {
			if ok, msg := runtime.EnforceInline(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreDelete,
				ModuleCode:          common.ModuleCash,
				SubModule:           "PAYABLE_RECEIVABLE",
				ActorUserID:         req.UserID,
				HandlerName:         "BulkRequestDeleteTransactions",
				APIPath:             "/cash/transactions/bulk-delete",
				DefaultBlockMessage: "Payable/receivable delete blocked by policy",
				Fields:              map[string]interface{}{"transaction_id": id},
			}); !ok {
				api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, msg, "")
				return
			}
		}
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, constants.ErrFailedToBeginTransaction, "")
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		insPay := `INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now(),$4) RETURNING action_id`
		reasonArg := interface{}(nil)
		if strings.TrimSpace(req.Reason) != "" {
			reasonArg = req.Reason
		}
		for _, id := range txIDsPay {
			var latestActionType, latestStatus string
			latestErr := tx.QueryRow(ctx, `
				SELECT actiontype, processing_status
				FROM auditactionpayable
				WHERE payable_id = $1
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			`, id).Scan(&latestActionType, &latestStatus)
			if latestErr == nil && latestActionType == constants.AuditActionDelete && latestStatus == constants.StatusPendingDeleteApproval {
				api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, "delete request already pending for transaction: " + id, "")
				return
			}
			var actionID string
			if err := tx.QueryRow(ctx, insPay, id, reasonArg, requestedBy, requestedIP).Scan(&actionID); err == nil {
				// nop: collected if needed
			}
		}

		insRec := `INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip) VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now(),$4) RETURNING action_id`
		for _, id := range txIDsRec {
			var latestActionType, latestStatus string
			latestErr := tx.QueryRow(ctx, `
				SELECT actiontype, processing_status
				FROM auditactionreceivable
				WHERE receivable_id = $1
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			`, id).Scan(&latestActionType, &latestStatus)
			if latestErr == nil && latestActionType == constants.AuditActionDelete && latestStatus == constants.StatusPendingDeleteApproval {
				api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, "delete request already pending for transaction: " + id, "")
				return
			}
			var actionID string
			if err := tx.QueryRow(ctx, insRec, id, reasonArg, requestedBy, requestedIP).Scan(&actionID); err == nil {
				// nop
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed, "")
			return
		}
		committed = true

		api.RespondEnvelopeSuccess(w, "delete requests created", nil)
	}
}

// BulkRejectTransactions rejects latest audit actions for mixed transaction ids (payable or receivable)
func BulkRejectTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string   `json:"user_id"`
			TransactionIDs []string `json:"transaction_ids"`
			Comment        string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.TransactionIDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidJSON, "")
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondEnvelopeError(w, http.StatusUnauthorized, constants.ErrInvalidSession, "")
			return
		}
		checkerIP := api.ClientIPFromRequest(r)

		// separate ids
		payIDs := []string{}
		recIDs := []string{}
		for _, id := range req.TransactionIDs {
			if strings.HasPrefix(id, constants.ErrPrefixPayable) {
				payIDs = append(payIDs, id)
			} else if strings.HasPrefix(id, constants.ErrPrefixReceivable) {
				recIDs = append(recIDs, id)
			}
		}

		ctx := r.Context()
		payActionIDs := make([]string, 0, len(payIDs))
		recActionIDs := make([]string, 0, len(recIDs))
		payEditActionIDs := make([]string, 0)
		recEditActionIDs := make([]string, 0)

		for _, id := range req.TransactionIDs {
			if ok, msg := runtime.EnforceInline(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreReject,
				ModuleCode:          common.ModuleCash,
				SubModule:           "PAYABLE_RECEIVABLE",
				ActorUserID:         req.UserID,
				HandlerName:         "BulkRejectTransactions",
				APIPath:             "/cash/transactions/bulk-reject",
				DefaultBlockMessage: "Payable/receivable reject blocked by policy",
				Fields:              map[string]interface{}{"transaction_id": id},
			}); !ok {
				api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, msg, "")
				return
			}
		}
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, constants.ErrFailedToBeginTransaction, "")
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		// For payables: find latest action_id per payable and add to list
		if len(payIDs) > 0 {
			for _, pid := range payIDs {
				var aid, atype, status string
				if err := tx.QueryRow(ctx, `SELECT action_id, actiontype, processing_status FROM auditactionpayable WHERE payable_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC, action_id DESC LIMIT 1`, pid).Scan(&aid, &atype, &status); err != nil {
					api.RespondEnvelopeError(w, http.StatusNotFound, constants.ErrMissingLatestAuditForTransaction + pid, "")
					return
				}
				if aid == "" {
					api.RespondEnvelopeError(w, http.StatusNotFound, constants.ErrMissingLatestAuditForTransaction + pid, "")
					return
				}
				if status != constants.StatusPendingApproval && status != constants.StatusPendingEditApproval && status != constants.StatusPendingDeleteApproval {
					api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, "cannot reject non-pending transaction: " + pid, "")
					return
				}
				payActionIDs = append(payActionIDs, aid)
				if atype == constants.AuditActionEdit {
					payEditActionIDs = append(payEditActionIDs, aid)
				}
			}
		}

		// For receivables
		if len(recIDs) > 0 {
			for _, rid := range recIDs {
				var aid, atype, status string
				if err := tx.QueryRow(ctx, `SELECT action_id, actiontype, processing_status FROM auditactionreceivable WHERE receivable_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE') ORDER BY requested_at DESC, action_id DESC LIMIT 1`, rid).Scan(&aid, &atype, &status); err != nil {
					api.RespondEnvelopeError(w, http.StatusNotFound, constants.ErrMissingLatestAuditForTransaction + rid, "")
					return
				}
				if aid == "" {
					api.RespondEnvelopeError(w, http.StatusNotFound, constants.ErrMissingLatestAuditForTransaction + rid, "")
					return
				}
				if status != constants.StatusPendingApproval && status != constants.StatusPendingEditApproval && status != constants.StatusPendingDeleteApproval {
					api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, "cannot reject non-pending transaction: " + rid, "")
					return
				}
				recActionIDs = append(recActionIDs, aid)
				if atype == constants.AuditActionEdit {
					recEditActionIDs = append(recEditActionIDs, aid)
				}
			}
		}

		if len(payActionIDs) == 0 && len(recActionIDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, "no valid actions found for provided ids", "")
			return
		}
		commentArg := interface{}(nil)
		if strings.TrimSpace(req.Comment) != "" {
			commentArg = req.Comment
		}
		if len(payEditActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE auditactionpayable aa
				SET
					old_entity_name       = p.entity_name,
					old_counterparty_name = p.counterparty_name,
					old_invoice_number    = p.invoice_number,
					old_invoice_date      = p.invoice_date,
					old_due_date          = p.due_date,
					old_amount            = p.amount,
					old_currency_code     = p.currency_code
				FROM tr_payables p
				WHERE aa.action_id = ANY($1) AND aa.payable_id = p.payable_id
			`, payEditActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to capture old payable values on rejection", "")
				return
			}
		}
		if len(recEditActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE auditactionreceivable aa
				SET
					old_entity_name       = r.entity_name,
					old_counterparty_name = r.counterparty_name,
					old_invoice_number    = r.invoice_number,
					old_invoice_date      = r.invoice_date,
					old_due_date          = r.due_date,
					old_amount            = r.invoice_amount,
					old_currency_code     = r.currency_code
				FROM tr_receivables r
				WHERE aa.action_id = ANY($1) AND aa.receivable_id = r.receivable_id
			`, recEditActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to capture old receivable values on rejection", "")
				return
			}
		}
		if len(payActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE auditactionpayable SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3 WHERE action_id = ANY($4)`, checkerBy, commentArg, checkerIP, payActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject payable actions", "")
				return
			}
		}
		if len(recActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE auditactionreceivable SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3 WHERE action_id = ANY($4)`, checkerBy, commentArg, checkerIP, recActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to reject receivable actions", "")
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed, "")
			return
		}
		committed = true

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"rejected_count": len(payActionIDs) + len(recActionIDs)})
	}
}

// BulkApproveTransactions approves latest audit actions for mixed transaction ids (payable or receivable)
func BulkApproveTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string   `json:"user_id"`
			TransactionIDs []string `json:"transaction_ids"`
			Comment        string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.TransactionIDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidJSON, "")
			return
		}

		checkerBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerBy = s.Name
				break
			}
		}
		if checkerBy == "" {
			api.RespondEnvelopeError(w, http.StatusUnauthorized, constants.ErrInvalidSession, "")
			return
		}
		checkerIP := api.ClientIPFromRequest(r)

		// separate ids
		payIDs := []string{}
		recIDs := []string{}
		for _, id := range req.TransactionIDs {
			if strings.HasPrefix(id, constants.ErrPrefixPayable) {
				payIDs = append(payIDs, id)
			} else if strings.HasPrefix(id, constants.ErrPrefixReceivable) {
				recIDs = append(recIDs, id)
			}
		}

		ctx := r.Context()
		payActionIDs := make([]string, 0, len(payIDs))
		recActionIDs := make([]string, 0, len(recIDs))
		payDeleteActionIDs := make([]string, 0)
		recDeleteActionIDs := make([]string, 0)
		payEditActionIDs := make([]string, 0)
		recEditActionIDs := make([]string, 0)
		for _, pid := range payIDs {
			var aid, atype, status string
			if err := pgxPool.QueryRow(ctx, `
				SELECT action_id, actiontype, processing_status
				FROM auditactionpayable
				WHERE payable_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE')
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			`, pid).Scan(&aid, &atype, &status); err != nil {
				api.RespondEnvelopeError(w, http.StatusNotFound, constants.ErrMissingLatestAuditForTransaction + pid, "")
				return
			}
			if aid == "" {
				api.RespondEnvelopeError(w, http.StatusNotFound, constants.ErrMissingLatestAuditForTransaction + pid, "")
				return
			}
			if status != constants.StatusPendingApproval && status != constants.StatusPendingEditApproval && status != constants.StatusPendingDeleteApproval {
				api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, "cannot approve non-pending transaction: " + pid, "")
				return
			}
			payActionIDs = append(payActionIDs, aid)
			if atype == constants.AuditActionDelete && status == constants.StatusPendingDeleteApproval {
				payDeleteActionIDs = append(payDeleteActionIDs, aid)
			} else if atype == constants.AuditActionEdit {
				payEditActionIDs = append(payEditActionIDs, aid)
			}
		}
		for _, rid := range recIDs {
			var aid, atype, status string
			if err := pgxPool.QueryRow(ctx, `
				SELECT action_id, actiontype, processing_status
				FROM auditactionreceivable
				WHERE receivable_id = $1 AND actiontype IN ('CREATE','EDIT','DELETE')
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			`, rid).Scan(&aid, &atype, &status); err != nil {
				api.RespondEnvelopeError(w, http.StatusNotFound, constants.ErrMissingLatestAuditForTransaction + rid, "")
				return
			}
			if aid == "" {
				api.RespondEnvelopeError(w, http.StatusNotFound, constants.ErrMissingLatestAuditForTransaction + rid, "")
				return
			}
			if status != constants.StatusPendingApproval && status != constants.StatusPendingEditApproval && status != constants.StatusPendingDeleteApproval {
				api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, "cannot approve non-pending transaction: " + rid, "")
				return
			}
			recActionIDs = append(recActionIDs, aid)
			if atype == constants.AuditActionDelete && status == constants.StatusPendingDeleteApproval {
				recDeleteActionIDs = append(recDeleteActionIDs, aid)
			} else if atype == constants.AuditActionEdit {
				recEditActionIDs = append(recEditActionIDs, aid)
			}
		}

		if len(payActionIDs) == 0 && len(recActionIDs) == 0 {
			api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, "no valid actions found for provided ids", "")
			return
		}
		for _, id := range req.TransactionIDs {
			if ok, msg := runtime.EnforceInline(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreApprove,
				ModuleCode:          common.ModuleCash,
				SubModule:           "PAYABLE_RECEIVABLE",
				ActorUserID:         req.UserID,
				HandlerName:         "BulkApproveTransactions",
				APIPath:             "/cash/transactions/bulk-approve",
				DefaultBlockMessage: "Payable/receivable approve blocked by policy",
				Fields:              map[string]interface{}{"transaction_id": id},
			}); !ok {
				api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, msg, "")
				return
			}
		}
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, constants.ErrFailedToBeginTransaction, "")
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()
		// delPayQuery := `DELETE FROM auditactionpayable WHERE action_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL'`
		// delRecQuery := `DELETE FROM auditactionreceivable WHERE action_id = ANY($1) AND processing_status = 'PENDING_DELETE_APPROVAL'`
		commentArg := interface{}(nil)
		if strings.TrimSpace(req.Comment) != "" {
			commentArg = req.Comment
		}
		if len(payActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE auditactionpayable SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3 WHERE action_id = ANY($4)`, checkerBy, commentArg, checkerIP, payActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to approve payable audits", "")
				return
			}
		}
		if len(recActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE auditactionreceivable SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2, checker_ip=$3 WHERE action_id = ANY($4)`, checkerBy, commentArg, checkerIP, recActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to approve receivable audits", "")
				return
			}
		}

		if len(payEditActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE auditactionpayable aa
				SET
					old_entity_name       = p.entity_name,
					old_counterparty_name = p.counterparty_name,
					old_invoice_number    = p.invoice_number,
					old_invoice_date      = p.invoice_date,
					old_due_date          = p.due_date,
					old_amount            = p.amount,
					old_currency_code     = p.currency_code
				FROM tr_payables p
				WHERE aa.action_id = ANY($1) AND aa.payable_id = p.payable_id
			`, payEditActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to capture old payable values: " + err.Error(), "")
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE tr_payables p
				SET
					entity_name       = COALESCE(NULLIF(aa.new_entity_name, ''), p.entity_name),
					counterparty_name = COALESCE(NULLIF(aa.new_counterparty_name, ''), p.counterparty_name),
					invoice_number    = COALESCE(NULLIF(aa.new_invoice_number, ''), p.invoice_number),
					invoice_date      = COALESCE(NULLIF(aa.new_invoice_date::text, '')::date, p.invoice_date),
					due_date          = COALESCE(NULLIF(aa.new_due_date::text, '')::date, p.due_date),
					amount            = COALESCE(NULLIF(aa.new_amount::text, '')::numeric, p.amount),
					currency_code     = COALESCE(NULLIF(aa.new_currency_code, ''), p.currency_code)
				FROM auditactionpayable aa
				WHERE aa.action_id = ANY($1) AND aa.payable_id = p.payable_id
			`, payEditActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to apply new payable values: " + err.Error(), "")
				return
			}
		}

		if len(recEditActionIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE auditactionreceivable aa
				SET
					old_entity_name       = r.entity_name,
					old_counterparty_name = r.counterparty_name,
					old_invoice_number    = r.invoice_number,
					old_invoice_date      = r.invoice_date,
					old_due_date          = r.due_date,
					old_amount            = r.invoice_amount,
					old_currency_code     = r.currency_code
				FROM tr_receivables r
				WHERE aa.action_id = ANY($1) AND aa.receivable_id = r.receivable_id
			`, recEditActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to capture old receivable values: " + err.Error(), "")
				return
			}
			if _, err := tx.Exec(ctx, `
				UPDATE tr_receivables r
				SET
					entity_name       = COALESCE(NULLIF(aa.new_entity_name, ''), r.entity_name),
					counterparty_name = COALESCE(NULLIF(aa.new_counterparty_name, ''), r.counterparty_name),
					invoice_number    = COALESCE(NULLIF(aa.new_invoice_number, ''), r.invoice_number),
					invoice_date      = COALESCE(NULLIF(aa.new_invoice_date::text, '')::date, r.invoice_date),
					due_date          = COALESCE(NULLIF(aa.new_due_date::text, '')::date, r.due_date),
					invoice_amount    = COALESCE(NULLIF(aa.new_amount::text, '')::numeric, r.invoice_amount),
					currency_code     = COALESCE(NULLIF(aa.new_currency_code, ''), r.currency_code)
				FROM auditactionreceivable aa
				WHERE aa.action_id = ANY($1) AND aa.receivable_id = r.receivable_id
			`, recEditActionIDs); err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to apply new receivable values: " + err.Error(), "")
				return
			}
		}

		if len(payDeleteActionIDs) > 0 {
			payDeleteTag, err := tx.Exec(ctx, `
				UPDATE tr_payables p
				SET is_deleted = TRUE,
					deleted_at = now(),
					deleted_by = aa.requested_by
				FROM auditactionpayable aa
				WHERE aa.action_id = ANY($1)
				  AND aa.actiontype = 'DELETE'
				  AND aa.processing_status = 'APPROVED'
				  AND aa.payable_id = p.payable_id
			`, payDeleteActionIDs)
			if err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to soft delete payables: " + err.Error(), "")
				return
			}
			if payDeleteTag.RowsAffected() != int64(len(payDeleteActionIDs)) {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, fmt.Sprintf("soft deleted %d of %d payable delete approvals", payDeleteTag.RowsAffected(), len(payDeleteActionIDs)), "")
				return
			}
		}
		if len(recDeleteActionIDs) > 0 {
			recDeleteTag, err := tx.Exec(ctx, `
				UPDATE tr_receivables r
				SET is_deleted = TRUE,
					deleted_at = now(),
					deleted_by = aa.requested_by
				FROM auditactionreceivable aa
				WHERE aa.action_id = ANY($1)
				  AND aa.actiontype = 'DELETE'
				  AND aa.processing_status = 'APPROVED'
				  AND aa.receivable_id = r.receivable_id
			`, recDeleteActionIDs)
			if err != nil {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, "failed to soft delete receivables: " + err.Error(), "")
				return
			}
			if recDeleteTag.RowsAffected() != int64(len(recDeleteActionIDs)) {
				api.RespondEnvelopeError(w, http.StatusInternalServerError, fmt.Sprintf("soft deleted %d of %d receivable delete approvals", recDeleteTag.RowsAffected(), len(recDeleteActionIDs)), "")
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed, "")
			return
		}
		committed = true

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{"approved_count": len(payActionIDs) + len(recActionIDs)})
	}
}

func BulkCreateTransactions(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string                   `json:"user_id"`
			Items  []map[string]interface{} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondEnvelopeError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort, "")
			return
		}
		if req.UserID == "" || len(req.Items) == 0 {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "user_id and items are required", "")
			return
		}

		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondEnvelopeError(w, http.StatusUnauthorized, constants.ErrInvalidSession, "")
			return
		}

		for idx, itm := range req.Items {
			entityName := fmt.Sprint(itm["entity_name"])
			if ok, msg := runtime.EnforceInline(ctx, r, pgxPool, runtime.EnforceInput{
				EventCode:           common.TriggerPreCreate,
				ModuleCode:          common.ModuleCash,
				SubModule:           "PAYABLE_RECEIVABLE",
				EntityCode:          entityName,
				ActorUserID:         req.UserID,
				HandlerName:         "BulkCreateTransactions",
				APIPath:             "/cash/transactions/create",
				DefaultBlockMessage: "Payable/receivable create blocked by policy",
				Fields: map[string]interface{}{
					"transaction_type": itm["transaction_type"],
					"entity_name":      entityName,
					"index":            idx,
				},
			}); !ok {
				api.RespondEnvelopeError(w, http.StatusUnprocessableEntity, msg, "")
				return
			}
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, constants.ErrFailedToBeginTransaction, "")
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		var createdPayables []string
		var createdReceivables []string
		var payableActionIDs []string
		var receivableActionIDs []string

		for idx, itm := range req.Items {
			tRaw, ok := itm["transaction_type"]
			if !ok {
				tx.Rollback(ctx)
				api.RespondEnvelopeError(w, http.StatusBadRequest, fmt.Sprintf("item %d missing transaction_type", idx), "")
				return
			}
			txType := strings.ToUpper(fmt.Sprint(tRaw))

			entityName := fmt.Sprint(itm["entity_name"])         // required
			counterparty := fmt.Sprint(itm["counterparty_name"]) // required
			invoiceNumber := fmt.Sprint(itm["invoice_number"])   // required
			invDateStr := fmt.Sprint(itm["invoice_date"])        // optional
			dueDateStr := fmt.Sprint(itm["due_date"])            // optional
			currency := fmt.Sprint(itm["currency_code"])         // required

			amountF := 0.0
			if v, ok := itm["amount"]; ok {
				switch vv := v.(type) {
				case float64:
					amountF = vv
				case float32:
					amountF = float64(vv)
				case int:
					amountF = float64(vv)
				case int64:
					amountF = float64(vv)
				case string:
					fmt.Sscan(vv, &amountF)
				}
			}
			if entityName == "" || counterparty == "" || invoiceNumber == "" || currency == "" {
				tx.Rollback(ctx)
				api.RespondEnvelopeError(w, http.StatusBadRequest, fmt.Sprintf("item %d missing required fields", idx), "")
				return
			}
			if msg := validatePayRecScope(ctx, entityName, counterparty, currency); msg != "" {
				tx.Rollback(ctx)
				api.RespondEnvelopeError(w, http.StatusForbidden, fmt.Sprintf("item %d: %s", idx, msg), "")
				return
			}

			invDate := normalizeDate(invDateStr)
			dueDate := normalizeDate(dueDateStr)
			var invDateVal, dueDateVal interface{}
			if strings.TrimSpace(invDate) != "" {
				if t, e := time.Parse(constants.DateFormat, invDate); e == nil {
					invDateVal = t
				}
			}
			if strings.TrimSpace(dueDate) != "" {
				if t, e := time.Parse(constants.DateFormat, dueDate); e == nil {
					dueDateVal = t
				}
			}

			if txType == "PAYABLE" {
				var pid string
				q := `INSERT INTO tr_payables (entity_name, counterparty_name, invoice_number, invoice_date, due_date, amount, currency_code) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING payable_id`
				if err := tx.QueryRow(ctx, q, entityName, counterparty, invoiceNumber, invDateVal, dueDateVal, amountF, currency).Scan(&pid); err != nil {
					tx.Rollback(ctx)
					api.RespondEnvelopeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to insert payable item %d: %v", idx, err), "")
					return
				}
				createdPayables = append(createdPayables, pid)
				var actionID string
				auditQ := `INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip) VALUES ($1,'CREATE','PENDING_APPROVAL',NULL,$2,now(),$3) RETURNING action_id`
				if err := tx.QueryRow(ctx, auditQ, pid, userName, transactionNullIfEmpty(api.ClientIPFromRequest(r))).Scan(&actionID); err != nil {
					tx.Rollback(ctx)
					api.RespondEnvelopeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create audit for payable %s: %v", pid, err), "")
					return
				}
				payableActionIDs = append(payableActionIDs, actionID)

			} else if txType == "RECEIVABLE" {
				var rid string
				q := `INSERT INTO tr_receivables (entity_name, counterparty_name, invoice_number, invoice_date, due_date, invoice_amount, currency_code) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING receivable_id`
				if err := tx.QueryRow(ctx, q, entityName, counterparty, invoiceNumber, invDateVal, dueDateVal, amountF, currency).Scan(&rid); err != nil {
					tx.Rollback(ctx)
					api.RespondEnvelopeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to insert receivable item %d: %v", idx, err), "")
					return
				}
				createdReceivables = append(createdReceivables, rid)
				var actionID string
				auditQ := `INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip) VALUES ($1,'CREATE','PENDING_APPROVAL',NULL,$2,now(),$3) RETURNING action_id`
				if err := tx.QueryRow(ctx, auditQ, rid, userName, transactionNullIfEmpty(api.ClientIPFromRequest(r))).Scan(&actionID); err != nil {
					tx.Rollback(ctx)
					api.RespondEnvelopeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create audit for receivable %s: %v", rid, err), "")
					return
				}
				receivableActionIDs = append(receivableActionIDs, actionID)

			} else {
				tx.Rollback(ctx)
				api.RespondEnvelopeError(w, http.StatusBadRequest, fmt.Sprintf("item %d unknown transaction_type: %s", idx, txType), "")
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, constants.ErrTxCommitFailed, "")
			return
		}
		committed = true

		resp := map[string]interface{}{}
		if len(createdPayables) > 0 {
			resp["created_payables"] = createdPayables
			resp["payable_action_ids"] = payableActionIDs
		}
		if len(createdReceivables) > 0 {
			resp["created_receivables"] = createdReceivables
			resp["receivable_action_ids"] = receivableActionIDs
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", resp)
	}
}

func UpdateTransaction(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string                 `json:"user_id"`
			ID     string                 `json:"id"`
			Fields map[string]interface{} `json:"fields"`
			Reason string                 `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}
		if req.UserID == "" || req.ID == "" || len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "user_id, id and fields are required")
			return
		}

		userName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userName = s.Name
				break
			}
		}
		if userName == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidSession)
			return
		}

		id := strings.TrimSpace(req.ID)
		if !runtime.Enforce(ctx, w, r, pgxPool, runtime.EnforceInput{
			EventCode:           common.TriggerPreEdit,
			ModuleCode:          common.ModuleCash,
			SubModule:           "PAYABLE_RECEIVABLE",
			ActorUserID:         req.UserID,
			HandlerName:         "UpdateTransaction",
			APIPath:             "/cash/transactions/update",
			DefaultBlockMessage: "Payable/receivable update blocked by policy",
			Fields: map[string]interface{}{
				"transaction_id": id,
				"fields":         req.Fields,
			},
		}) {
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		var actionID string

		if strings.HasPrefix(id, constants.ErrPrefixPayable) {
			extractStr := func(key string) *string {
				if v, ok := req.Fields[key]; ok {
					s := fmt.Sprint(v)
					return &s
				}
				return nil
			}
			extractDate := func(key string) *time.Time {
				if v, ok := req.Fields[key]; ok {
					if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
						if t, e := time.Parse(constants.DateFormat, normalizeDate(s)); e == nil {
							return &t
						}
					}
				}
				return nil
			}
			extractFloat := func(key string) *float64 {
				if v, ok := req.Fields[key]; ok {
					switch vv := v.(type) {
					case float64:
						return &vv
					case float32:
						f := float64(vv)
						return &f
					case int:
						f := float64(vv)
						return &f
					case int64:
						f := float64(vv)
						return &f
					case string:
						var out float64
						fmt.Sscan(vv, &out)
						return &out
					}
				}
				return nil
			}

			newEntity := extractStr("entity_name")
			newCounter := extractStr("counterparty_name")
			newInvoice := extractStr("invoice_number")
			newCurrency := extractStr("currency_code")
			newInvDate := extractDate("invoice_date")
			newDueDate := extractDate("due_date")
			newAmount := extractFloat("amount")

			if newEntity == nil && newCounter == nil && newInvoice == nil && newCurrency == nil &&
				newInvDate == nil && newDueDate == nil && newAmount == nil {
				api.RespondWithResult(w, false, "no valid fields to update")
				tx.Rollback(ctx)
				return
			}

			reasonArg := interface{}(nil)
			if strings.TrimSpace(req.Reason) != "" {
				reasonArg = req.Reason
			}

			var (
				oldEntity, oldCounter, oldInvoice, oldCurrency *string
				oldInvDate, oldDueDate                         *time.Time
				oldAmount                                      *float64
			)
			_ = tx.QueryRow(ctx, `
				SELECT entity_name, counterparty_name, invoice_number, invoice_date, due_date, amount, currency_code
				FROM tr_payables WHERE payable_id = $1 AND is_deleted != TRUE
			`, id).Scan(&oldEntity, &oldCounter, &oldInvoice, &oldInvDate, &oldDueDate, &oldAmount, &oldCurrency)
			effectiveEntity := stringPtrValue(oldEntity)
			effectiveCounterparty := stringPtrValue(oldCounter)
			effectiveCurrency := stringPtrValue(oldCurrency)
			if newEntity != nil {
				effectiveEntity = *newEntity
			}
			if newCounter != nil {
				effectiveCounterparty = *newCounter
			}
			if newCurrency != nil {
				effectiveCurrency = *newCurrency
			}
			if msg := validatePayRecScope(ctx, effectiveEntity, effectiveCounterparty, effectiveCurrency); msg != "" {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusForbidden, msg)
				return
			}

			auditQ := `
				INSERT INTO auditactionpayable
				  (payable_id, actiontype, processing_status, reason, requested_by, requested_at,
				   requested_ip,
				   old_entity_name, old_counterparty_name, old_invoice_number, old_invoice_date,
				   old_due_date, old_amount, old_currency_code,
				   new_entity_name, new_counterparty_name, new_invoice_number, new_invoice_date,
				   new_due_date, new_amount, new_currency_code)
				VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),
					$4,
					$5,$6,$7,$8,$9,$10,$11,
					$12,$13,$14,$15,$16,$17,$18)
				RETURNING action_id`
			if err := tx.QueryRow(ctx, auditQ, id, reasonArg, userName,
				transactionNullIfEmpty(api.ClientIPFromRequest(r)),
				oldEntity, oldCounter, oldInvoice, oldInvDate, oldDueDate, oldAmount, oldCurrency,
				newEntity, newCounter, newInvoice, newInvDate, newDueDate, newAmount, newCurrency).Scan(&actionID); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to create audit for payable: "+err.Error())
				return
			}

		} else if strings.HasPrefix(id, constants.ErrPrefixReceivable) {
			extractStr := func(key string) *string {
				if v, ok := req.Fields[key]; ok {
					s := fmt.Sprint(v)
					return &s
				}
				return nil
			}
			extractDate := func(key string) *time.Time {
				if v, ok := req.Fields[key]; ok {
					if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
						if t, e := time.Parse(constants.DateFormat, normalizeDate(s)); e == nil {
							return &t
						}
					}
				}
				return nil
			}
			extractFloat := func(key string) *float64 {
				if v, ok := req.Fields[key]; ok {
					switch vv := v.(type) {
					case float64:
						return &vv
					case float32:
						f := float64(vv)
						return &f
					case int:
						f := float64(vv)
						return &f
					case int64:
						f := float64(vv)
						return &f
					case string:
						var out float64
						fmt.Sscan(vv, &out)
						return &out
					}
				}
				return nil
			}

			newEntity := extractStr("entity_name")
			newCounter := extractStr("counterparty_name")
			newInvoice := extractStr("invoice_number")
			newCurrency := extractStr("currency_code")
			newInvDate := extractDate("invoice_date")
			newDueDate := extractDate("due_date")
			newAmount := extractFloat("invoice_amount")

			if newEntity == nil && newCounter == nil && newInvoice == nil && newCurrency == nil &&
				newInvDate == nil && newDueDate == nil && newAmount == nil {
				api.RespondWithResult(w, false, "no valid fields to update")
				tx.Rollback(ctx)
				return
			}

			reasonArg := interface{}(nil)
			if strings.TrimSpace(req.Reason) != "" {
				reasonArg = req.Reason
			}

			var (
				oldEntity, oldCounter, oldInvoice, oldCurrency *string
				oldInvDate, oldDueDate                         *time.Time
				oldAmount                                      *float64
			)
			_ = tx.QueryRow(ctx, `
				SELECT entity_name, counterparty_name, invoice_number, invoice_date, due_date, invoice_amount, currency_code
				FROM tr_receivables WHERE receivable_id = $1 AND is_deleted != TRUE
			`, id).Scan(&oldEntity, &oldCounter, &oldInvoice, &oldInvDate, &oldDueDate, &oldAmount, &oldCurrency)
			effectiveEntity := stringPtrValue(oldEntity)
			effectiveCounterparty := stringPtrValue(oldCounter)
			effectiveCurrency := stringPtrValue(oldCurrency)
			if newEntity != nil {
				effectiveEntity = *newEntity
			}
			if newCounter != nil {
				effectiveCounterparty = *newCounter
			}
			if newCurrency != nil {
				effectiveCurrency = *newCurrency
			}
			if msg := validatePayRecScope(ctx, effectiveEntity, effectiveCounterparty, effectiveCurrency); msg != "" {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusForbidden, msg)
				return
			}

			auditQ := `
				INSERT INTO auditactionreceivable
				  (receivable_id, actiontype, processing_status, reason, requested_by, requested_at,
				   requested_ip,
				   old_entity_name, old_counterparty_name, old_invoice_number, old_invoice_date,
				   old_due_date, old_amount, old_currency_code,
				   new_entity_name, new_counterparty_name, new_invoice_number, new_invoice_date,
				   new_due_date, new_amount, new_currency_code)
				VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),
					$4,
					$5,$6,$7,$8,$9,$10,$11,
					$12,$13,$14,$15,$16,$17,$18)
				RETURNING action_id`
			if err := tx.QueryRow(ctx, auditQ, id, reasonArg, userName,
				transactionNullIfEmpty(api.ClientIPFromRequest(r)),
				oldEntity, oldCounter, oldInvoice, oldInvDate, oldDueDate, oldAmount, oldCurrency,
				newEntity, newCounter, newInvoice, newInvDate, newDueDate, newAmount, newCurrency).Scan(&actionID); err != nil {
				tx.Rollback(ctx)
				api.RespondWithError(w, http.StatusInternalServerError, "failed to create audit for receivable: "+err.Error())
				return
			}

		} else {
			api.RespondWithError(w, http.StatusBadRequest, "unknown id prefix")
			tx.Rollback(ctx)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		committed = true

		api.RespondWithPayload(w, true, "", map[string]string{"id": id, "action_id": actionID})
	}
}
