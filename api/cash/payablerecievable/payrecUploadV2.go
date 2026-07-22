package payablerecievable

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/policyengine/common"
	"CimplrCorpSaas/api/policyengine/runtime"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"

	"CimplrCorpSaas/internal/logger"
)

// V2 Data structures for new JSONB staging architecture
type StagingBatchTransaction struct {
	BatchID            uuid.UUID `json:"batch_id" db:"batch_id"`
	IngestionSource    string    `json:"ingestion_source" db:"ingestion_source"`
	IngestionTimestamp time.Time `json:"ingestion_timestamp" db:"ingestion_timestamp"`
	Status             string    `json:"status" db:"status"`
	ErrorMessage       *string   `json:"error_message" db:"error_message"`
	TotalRecords       int       `json:"total_records" db:"total_records"`
	ProcessedRecords   int       `json:"processed_records" db:"processed_records"`
	FailedRecords      int       `json:"failed_records" db:"failed_records"`
	FileHash           *string   `json:"file_hash" db:"file_hash"`
	FileName           *string   `json:"file_name" db:"file_name"`
}

type StagingTransaction struct {
	StagingID          uuid.UUID       `json:"staging_id" db:"staging_id"`
	BatchID            uuid.UUID       `json:"batch_id" db:"batch_id"`
	TransactionType    string          `json:"transaction_type" db:"transaction_type"`
	RawPayload         json.RawMessage `json:"raw_payload" db:"raw_payload"`
	IngestionTimestamp time.Time       `json:"ingestion_timestamp" db:"ingestion_timestamp"`
	Status             string          `json:"status" db:"status"`
	ErrorMessage       *string         `json:"error_message" db:"error_message"`
}

type PayableV2 struct {
	PayableID        string     `json:"payable_id" db:"payable_id"`
	EntityName       string     `json:"entity_name" db:"entity_name"`
	CounterpartyName string     `json:"counterparty_name" db:"counterparty_name"`
	InvoiceNumber    string     `json:"invoice_number" db:"invoice_number"`
	InvoiceDate      time.Time  `json:"invoice_date" db:"invoice_date"`
	DueDate          time.Time  `json:"due_date" db:"due_date"`
	Amount           float64    `json:"amount" db:"amount"`
	CurrencyCode     string     `json:"currency_code" db:"currency_code"`
	OldEntityName    *string    `json:"old_entity_name,omitempty" db:"old_entity_name"`
	OldCounterparty  *string    `json:"old_counterparty_name,omitempty" db:"old_counterparty_name"`
	OldInvoiceNumber *string    `json:"old_invoice_number,omitempty" db:"old_invoice_number"`
	OldInvoiceDate   *time.Time `json:"old_invoice_date,omitempty" db:"old_invoice_date"`
	OldDueDate       *time.Time `json:"old_due_date,omitempty" db:"old_due_date"`
	OldAmount        *float64   `json:"old_amount,omitempty" db:"old_amount"`
	OldCurrencyCode  *string    `json:"old_currency_code,omitempty" db:"old_currency_code"`
}

type ReceivableV2 struct {
	ReceivableID     string     `json:"receivable_id" db:"receivable_id"`
	EntityName       string     `json:"entity_name" db:"entity_name"`
	CounterpartyName string     `json:"counterparty_name" db:"counterparty_name"`
	InvoiceNumber    string     `json:"invoice_number" db:"invoice_number"`
	InvoiceDate      time.Time  `json:"invoice_date" db:"invoice_date"`
	DueDate          time.Time  `json:"due_date" db:"due_date"`
	InvoiceAmount    float64    `json:"invoice_amount" db:"invoice_amount"`
	CurrencyCode     string     `json:"currency_code" db:"currency_code"`
	OldEntityName    *string    `json:"old_entity_name,omitempty" db:"old_entity_name"`
	OldCounterparty  *string    `json:"old_counterparty_name,omitempty" db:"old_counterparty_name"`
	OldInvoiceNumber *string    `json:"old_invoice_number,omitempty" db:"old_invoice_number"`
	OldInvoiceDate   *time.Time `json:"old_invoice_date,omitempty" db:"old_invoice_date"`
	OldDueDate       *time.Time `json:"old_due_date,omitempty" db:"old_due_date"`
	OldInvoiceAmount *float64   `json:"old_invoice_amount,omitempty" db:"old_invoice_amount"`
	OldCurrencyCode  *string    `json:"old_currency_code,omitempty" db:"old_currency_code"`
}

// Constants for consistent error handling
const (
	InvalidRequestFormat       = "Invalid request format"
	BusinessUnitsNotFound      = "Business units not found in context"
	ContentTypeJSON            = constants.ContentTypeText
	ApplicationJSON            = constants.ContentTypeJSON
	UnauthorizedInvalidSession = "Unauthorized: invalid session"
)

// Helper function for consistent error responses
func respondWithErrorTransactionV2(w http.ResponseWriter, status int, errMsg string) {
	logger.LogError("%s", errMsg)
	api.RespondEnvelopeError(w, status, errMsg, "")
}

// Batch processor for transactions
type TransactionBatchProcessor struct {
	pool *pgxpool.Pool
}

func NewTransactionBatchProcessor(pool *pgxpool.Pool) *TransactionBatchProcessor {
	return &TransactionBatchProcessor{pool: pool}
}

func (bp *TransactionBatchProcessor) BatchInsertStagingTransactions(ctx context.Context, transactions []StagingTransaction) error {
	if len(transactions) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `INSERT INTO public.staging_transactions 
		(staging_id, batch_id, transaction_type, raw_payload, ingestion_timestamp, status) 
		VALUES ($1, $2, $3, $4, $5, $6)`

	for _, trans := range transactions {
		batch.Queue(query, trans.StagingID, trans.BatchID, trans.TransactionType, trans.RawPayload, trans.IngestionTimestamp, trans.Status)
	}

	br := bp.pool.SendBatch(ctx, batch)
	defer br.Close()

	var errors []string
	for i := 0; i < len(transactions); i++ {
		_, err := br.Exec()
		if err != nil {
			errors = append(errors, fmt.Sprintf("Record %d: %v", i+1, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to insert %d out of %d staging transactions: %s", len(errors), len(transactions), strings.Join(errors, "; "))
	}

	return nil
}

// BatchInsertStagingTransactionsWithTx - Transaction-aware version with improved error handling
func (bp *TransactionBatchProcessor) BatchInsertStagingTransactionsWithTx(ctx context.Context, tx pgx.Tx, transactions []StagingTransaction) error {
	if len(transactions) == 0 {
		return nil
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled: %w", ctx.Err())
	default:
	}

	batch := &pgx.Batch{}
	query := `INSERT INTO public.staging_transactions 
		(staging_id, batch_id, transaction_type, raw_payload, ingestion_timestamp, status) 
		VALUES ($1, $2, $3, $4, $5, $6)`

	for _, trans := range transactions {
		batch.Queue(query, trans.StagingID, trans.BatchID, trans.TransactionType, trans.RawPayload, trans.IngestionTimestamp, trans.Status)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	var errors []string
	var successCount int

	for i := 0; i < len(transactions); i++ {
		// Check for cancellation during batch processing
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled during batch processing at record %d: %w", i+1, ctx.Err())
		default:
		}

		_, err := br.Exec()
		if err != nil {
			// Collect detailed error information for human-friendly reporting
			transType := transactions[i].TransactionType
			errorMsg := fmt.Sprintf("Record %d (%s): %v", i+1, transType, err)
			errors = append(errors, errorMsg)
			logger.LogError("ERROR: Failed to insert staging transaction: %s", errorMsg)
		} else {
			successCount++
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("batch insert failed: %d successful, %d failed. Errors: %s",
			successCount, len(errors), strings.Join(errors, "; "))
	}

	logger.LogInfo("Successfully inserted %d staging transactions", successCount)
	return nil
}

// High-performance batch insert for payables with improved error handling
func (bp *TransactionBatchProcessor) BatchInsertPayables(ctx context.Context, payables []PayableV2, userName string) error {
	if len(payables) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `INSERT INTO public.tr_payables 
		(entity_name, counterparty_name, invoice_number, invoice_date, due_date, 
		amount, currency_code, old_entity_name, old_counterparty_name, old_invoice_number, 
		old_invoice_date, old_due_date, old_amount, old_currency_code) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING payable_id`

	for _, payable := range payables {
		if msg := validatePayRecScope(ctx, payable.EntityName, payable.CounterpartyName, payable.CurrencyCode); msg != "" {
			return fmt.Errorf("payable invoice %s failed scope validation: %s", payable.InvoiceNumber, msg)
		}
		batch.Queue(query, payable.EntityName, payable.CounterpartyName, payable.InvoiceNumber,
			payable.InvoiceDate, payable.DueDate, payable.Amount, payable.CurrencyCode,
			payable.OldEntityName, payable.OldCounterparty, payable.OldInvoiceNumber,
			payable.OldInvoiceDate, payable.OldDueDate, payable.OldAmount, payable.OldCurrencyCode)
	}

	br := bp.pool.SendBatch(ctx, batch)
	defer br.Close()

	var errors []string
	var successCount int
	var payableIDs []string

	for i := 0; i < len(payables); i++ {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("payables insertion cancelled at record %d: %w", i+1, ctx.Err())
		default:
		}

		var payableID string
		err := br.QueryRow().Scan(&payableID)
		if err != nil {
			invoiceNum := payables[i].InvoiceNumber
			entity := payables[i].EntityName
			errorMsg := fmt.Sprintf("Payable %d (Invoice: %s, Entity: %s): %v", i+1, invoiceNum, entity, err)
			errors = append(errors, errorMsg)
			logger.LogError("ERROR: Failed to insert payable: %s", errorMsg)
		} else {
			successCount++
			payableIDs = append(payableIDs, payableID)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("payables batch insert failed: %d successful, %d failed. Errors: %s",
			successCount, len(errors), strings.Join(errors, "; "))
	}

	// Insert audit actions for successful inserts
	if len(payableIDs) > 0 {
		_, auditErr := bp.pool.Exec(ctx, `
			INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
			SELECT unnest($1::text[]), 'CREATE', 'PENDING_APPROVAL', NULL, $2, now(), $3
		`, payableIDs, userName, transactionNullIfEmpty(api.ClientIPFromContext(ctx)))
		if auditErr != nil {
			return fmt.Errorf("audit log insert failed for payables: %w", auditErr)
		}
	}

	return nil
}

// High-performance batch insert for receivables with improved error handling
func (bp *TransactionBatchProcessor) BatchInsertReceivables(ctx context.Context, receivables []ReceivableV2, userName string) error {
	if len(receivables) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `INSERT INTO public.tr_receivables 
		(entity_name, counterparty_name, invoice_number, invoice_date, due_date, 
		invoice_amount, currency_code, old_entity_name, old_counterparty_name, old_invoice_number, 
		old_invoice_date, old_due_date, old_invoice_amount, old_currency_code) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING receivable_id`

	for _, receivable := range receivables {
		if msg := validatePayRecScope(ctx, receivable.EntityName, receivable.CounterpartyName, receivable.CurrencyCode); msg != "" {
			return fmt.Errorf("receivable invoice %s failed scope validation: %s", receivable.InvoiceNumber, msg)
		}
		batch.Queue(query, receivable.EntityName, receivable.CounterpartyName, receivable.InvoiceNumber,
			receivable.InvoiceDate, receivable.DueDate, receivable.InvoiceAmount, receivable.CurrencyCode,
			receivable.OldEntityName, receivable.OldCounterparty, receivable.OldInvoiceNumber,
			receivable.OldInvoiceDate, receivable.OldDueDate, receivable.OldInvoiceAmount, receivable.OldCurrencyCode)
	}

	br := bp.pool.SendBatch(ctx, batch)
	defer br.Close()

	var errors []string
	var successCount int
	var receivableIDs []string

	for i := 0; i < len(receivables); i++ {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("receivables insertion cancelled at record %d: %w", i+1, ctx.Err())
		default:
		}

		var receivableID string
		err := br.QueryRow().Scan(&receivableID)
		if err != nil {
			invoiceNum := receivables[i].InvoiceNumber
			entity := receivables[i].EntityName
			errorMsg := fmt.Sprintf("Receivable %d (Invoice: %s, Entity: %s): %v", i+1, invoiceNum, entity, err)
			errors = append(errors, errorMsg)
			logger.LogError("ERROR: Failed to insert receivable: %s", errorMsg)
		} else {
			successCount++
			receivableIDs = append(receivableIDs, receivableID)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("receivables batch insert failed: %d successful, %d failed. Errors: %s",
			successCount, len(errors), strings.Join(errors, "; "))
	}

	// Insert audit actions for successful inserts
	if len(receivableIDs) > 0 {
		_, auditErr := bp.pool.Exec(ctx, `
			INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip)
			SELECT unnest($1::text[]), 'CREATE', 'PENDING_APPROVAL', NULL, $2, now(), $3
		`, receivableIDs, userName, transactionNullIfEmpty(api.ClientIPFromContext(ctx)))
		if auditErr != nil {
			return fmt.Errorf("audit log insert failed for receivables: %w", auditErr)
		}
	}

	return nil
}

func BatchUploadTransactionsV2(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Parse multipart form (32MB max)
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			respondWithErrorTransactionV2(w, http.StatusBadRequest, "Failed to parse form data")
			return
		}

		// Get user session from form or body
		userID := r.FormValue(constants.KeyUserID)
		userName := ""
		if userID != "" {
			// Validate session and get userName
			activeSessions := auth.GetActiveSessions()
			found := false
			for _, session := range activeSessions {
				if session.UserID == userID {
					found = true
					userName = session.Name // Assuming Name is the username
					break
				}
			}
			if !found {
				respondWithErrorTransactionV2(w, http.StatusUnauthorized, UnauthorizedInvalidSession)
				return
			}
		}

		fileField, fileHeader, err := findTransactionUploadFile(r)
		if err != nil {

			respondWithErrorTransactionV2(w, http.StatusBadRequest, err.Error())
			return
		}

		if !runtime.Enforce(ctx, w, r, pool, runtime.EnforceInput{
			EventCode:           common.TriggerPreUpload,
			ModuleCode:          common.ModuleCash,
			SubModule:           "PAYABLE_RECEIVABLE",
			ActorUserID:         userID,
			HandlerName:         "BatchUploadTransactionsV2",
			APIPath:             "/cash/transactions/upload-payrec-batch",
			DefaultBlockMessage: "Payable/receivable batch upload blocked by policy",
			Fields: map[string]interface{}{
				"file_field": fileField,
				"filename":   fileHeader.Filename,
			},
		}) {
			return
		}

		service := NewTransactionUploadService(pool)
		result, err := service.UploadTransactionBatch(ctx, fileHeader, fileField, userName)
		if err != nil {
			if errors.Is(err, ErrFileAlreadyUploaded) {
				respondWithErrorTransactionV2(w, http.StatusBadRequest, "This transaction file was already uploaded earlier. Please upload a different file.")
				return
			}
			respondWithErrorTransactionV2(w, http.StatusInternalServerError, err.Error())
			return
		}

		msg := fmt.Sprintf("Successfully uploaded %d transaction records, %d processed to canonical tables", result.TotalRecords, result.ProcessedRecords)
		api.RespondEnvelopeSuccessCompat(w, msg, map[string]interface{}{
			"batch_id":          result.BatchID,
			"total_records":     result.TotalRecords,
			"processed_records": result.ProcessedRecords,
			"processed_files":   result.ProcessedFiles,
			"processing_time":   result.ProcessingTime.String(),
			"file_storage_key":  result.FileStorageKey,
			"file_storage_url":  result.FileStorageURL,
		})
	}
}

func findTransactionUploadFile(r *http.Request) (string, *multipart.FileHeader, error) {
	for _, fileField := range []string{"payables", "receivables"} {
		if r.MultipartForm == nil || r.MultipartForm.File == nil {
			break
		}
		files := r.MultipartForm.File[fileField]
		if len(files) > 0 {
			return fileField, files[0], nil
		}
	}
	return "", nil, fmt.Errorf("no valid files provided")
}

// Helper function to process uploaded transaction files
func processUploadedTransactionFiles(r *http.Request, batchID uuid.UUID) ([]StagingTransaction, []string, int) {
	var allStagingTransactions []StagingTransaction
	var processedFiles []string
	var totalRecords int

	// Process specific file type uploads for transactions
	fileTypes := map[string]string{
		"payables":    "PAYABLE",
		"receivables": "RECEIVABLE",
	}

	for fileField, transactionType := range fileTypes {
		file, header, err := r.FormFile(fileField)
		if err != nil {
			continue // Skip if file not provided
		}
		defer file.Close()

		rawPayloads := processTransactionFileData(file, header.Filename, transactionType)
		if len(rawPayloads) == 0 {
			continue
		}

		// Create staging transaction records
		for _, payload := range rawPayloads {
			stagingTrans := StagingTransaction{
				StagingID:          uuid.New(),
				BatchID:            batchID,
				TransactionType:    transactionType,
				RawPayload:         payload,
				IngestionTimestamp: time.Now(),
				Status:             "pending",
			}
			allStagingTransactions = append(allStagingTransactions, stagingTrans)
		}

		totalRecords += len(rawPayloads)
		processedFiles = append(processedFiles, header.Filename)
	}

	return allStagingTransactions, processedFiles, totalRecords
}

// Helper function to process individual transaction file data
func processTransactionFileData(file multipart.File, filename, transactionType string) []json.RawMessage {
	var rawPayloads []json.RawMessage
	var err error

	if strings.HasSuffix(strings.ToLower(filename), ".csv") {
		rawPayloads, err = processTransactionCSVFile(file, transactionType)
	} else if strings.HasSuffix(strings.ToLower(filename), ".xlsx") || strings.HasSuffix(strings.ToLower(filename), ".xls") {
		rawPayloads, err = processTransactionExcelFile(file, transactionType)
	} else {
		logger.LogInfo("Unsupported file type: %s", filename)
		return nil
	}

	if err != nil {
		logger.LogError("Failed to process transaction file %s: %v", filename, err)
		return nil
	}

	return rawPayloads
}

func processTransactionCSVFile(file io.Reader, transactionType string) ([]json.RawMessage, error) {
	reader := csv.NewReader(file)
	records, err := reader.ReadAll() // MEMORY RISK: Loads entire file into memory
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) < 2 {
		return nil, fmt.Errorf("CSV file must have at least header and one data row")
	}

	headers := records[0]
	var rawPayloads []json.RawMessage

	for i, record := range records[1:] {
		if len(record) != len(headers) {
			continue // Skip malformed rows
		}

		rowData := make(map[string]interface{})
		for j, value := range record {
			rowData[strings.TrimSpace(headers[j])] = strings.TrimSpace(value)
		}

		jsonData, err := json.Marshal(rowData)
		if err != nil {
			logger.LogError("Failed to marshal transaction row %d: %v", i+1, err)
			continue
		}

		rawPayloads = append(rawPayloads, json.RawMessage(jsonData))
	}

	return rawPayloads, nil
}

// Process Excel files for transactions
func processTransactionExcelFile(file io.Reader, transactionType string) ([]json.RawMessage, error) {
	tmpFile, err := excelize.OpenReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to open Excel file: %w", err)
	}
	defer tmpFile.Close()

	sheets := tmpFile.GetSheetList()
	if len(sheets) == 0 {
		return nil, fmt.Errorf("no sheets found in Excel file")
	}

	sheetName := sheets[0] // Use first sheet
	rows, err := tmpFile.GetRows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}

	if len(rows) < 2 {
		return nil, fmt.Errorf("Excel file must have at least header and one data row")
	}

	headers := rows[0]
	var rawPayloads []json.RawMessage

	for i, row := range rows[1:] {
		if len(row) == 0 {
			continue // Skip empty rows
		}

		rowData := make(map[string]interface{})
		for j, value := range row {
			if j < len(headers) {
				rowData[strings.TrimSpace(headers[j])] = strings.TrimSpace(value)
			}
		}

		jsonData, err := json.Marshal(rowData)
		if err != nil {
			logger.LogError("Failed to marshal transaction row %d: %v", i+1, err)
			continue
		}

		rawPayloads = append(rawPayloads, json.RawMessage(jsonData))
	}

	return rawPayloads, nil
}

// calculateTransactionFileHashes calculates SHA256 hashes for uploaded transaction files to enable idempotent uploads
func calculateTransactionFileHashes(r *http.Request) ([]string, error) {
	var hashes []string

	// Check for specific file type uploads
	fileTypes := []string{"payables", "receivables"}
	for _, fileField := range fileTypes {
		file, _, err := r.FormFile(fileField)
		if err != nil {
			continue // Skip if file not provided
		}
		defer file.Close()

		hash, err := calculateSingleTransactionFileHash(file)
		if err != nil {
			return nil, fmt.Errorf("failed to hash transaction file %s: %w", fileField, err)
		}
		hashes = append(hashes, hash)
	}

	if len(hashes) == 0 {
		return nil, fmt.Errorf("no transaction files found for hash calculation")
	}

	return hashes, nil
}

// calculateSingleTransactionFileHash calculates SHA256 hash for a single transaction file
func calculateSingleTransactionFileHash(file multipart.File) (string, error) {
	// Reset file pointer to beginning
	file.Seek(0, io.SeekStart)
	defer file.Seek(0, io.SeekStart) // Reset for subsequent use

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("failed to calculate file hash: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
