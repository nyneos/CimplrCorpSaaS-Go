package payablerecievable

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"time"

	bankstatement "CimplrCorpSaas/api/cash/bankstatement"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shakinm/xlsReader/xls"
)

type TransactionUploadResult struct {
	BatchID          uuid.UUID
	TotalRecords     int
	ProcessedRecords int
	ProcessedFiles   []string
	ProcessingTime   time.Duration
	FileStorageKey   string
	FileStorageURL   string
	Duplicate        bool
}

type TransactionUploadService struct {
	pool           *pgxpool.Pool
	batchProcessor *TransactionBatchProcessor
}

func NewTransactionUploadService(pool *pgxpool.Pool) *TransactionUploadService {
	return &TransactionUploadService{
		pool:           pool,
		batchProcessor: NewTransactionBatchProcessor(pool),
	}
}

func (s *TransactionUploadService) UploadTransactionBatch(
	ctx context.Context,
	fileHeader *multipart.FileHeader,
	fileField string,
	userName string,
) (*TransactionUploadResult, error) {
	startTime := time.Now()

	file, err := fileHeader.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open upload file: %w", err)
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read upload file: %w", err)
	}

	fileHash := sha256.Sum256(fileBytes)
	fileHashHex := hex.EncodeToString(fileHash[:])
	transactionType, err := transactionTypeFromField(fileField)
	if err != nil {
		return nil, err
	}

	rawPayloads, err := processTransactionFileBytes(fileBytes, fileHeader.Filename, transactionType)
	if err != nil {
		return nil, err
	}
	if len(rawPayloads) == 0 {
		return nil, fmt.Errorf("no valid rows found in uploaded file")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	committed := false
	s3Key := ""
	s3Uploaded := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && rollbackErr != pgx.ErrTxClosed {
				fmt.Printf("[TRANSACTION-UPLOAD] rollback error: %v\n", rollbackErr)
			}
			if s3Uploaded && s3Key != "" {
				if cleanupErr := bankstatement.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					fmt.Printf("[TRANSACTION-UPLOAD] cleanup after failed transaction failed for key=%s: %v\n", s3Key, cleanupErr)
				}
			}
		}
	}()

	var duplicateBatchID uuid.UUID
	var existingUploadLink *string
	err = tx.QueryRow(ctx, `
		SELECT batch_id, upload_link
		FROM public.staging_batches_transactions
		WHERE file_hash = $1 AND status IN ('completed', 'processing')
		ORDER BY ingestion_timestamp DESC
		LIMIT 1
	`, fileHashHex).Scan(&duplicateBatchID, &existingUploadLink)
	if err == nil {
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return nil, fmt.Errorf("failed to finalize duplicate lookup: %w", commitErr)
		}
		committed = true
		dupURL := ""
		if existingUploadLink != nil {
			dupURL = strings.TrimSpace(*existingUploadLink)
		}
		return &TransactionUploadResult{
			BatchID:        duplicateBatchID,
			ProcessedFiles: []string{fileHeader.Filename},
			FileStorageURL: dupURL,
			Duplicate:      true,
			ProcessingTime: time.Since(startTime),
		}, nil
	}
	if err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("failed to check for duplicate transaction upload: %w", err)
	}

	batchID := uuid.New()
	batch := StagingBatchTransaction{
		BatchID:            batchID,
		IngestionSource:    "UPLOAD",
		IngestionTimestamp: time.Now(),
		Status:             "pending",
		TotalRecords:       0,
		ProcessedRecords:   0,
		FailedRecords:      0,
		FileHash:           stringPtr(fileHashHex),
		FileName:           stringPtr(fileHeader.Filename),
	}

	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(fileHeader.Filename)))
	contentType := bankstatement.DetectContentType(fileBytes)
	folder := bankstatement.GetStoragePrefix(fileField)
	s3Key = bankstatement.BuildS3Key(folder, fileField, fileHashHex, ext)
	s3URL := ""
	if bankstatement.IsS3UploadEnabled() {
		s3URL, err = bankstatement.UploadToS3(ctx, s3Key, fileBytes, contentType)
		if err != nil {
			return nil, fmt.Errorf("failed to upload file to s3: %w", err)
		}
		s3Uploaded = true
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO public.staging_batches_transactions
		(batch_id, ingestion_source, status, total_records, file_hash, file_name, upload_link)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`,
		batch.BatchID,
		batch.IngestionSource,
		batch.Status,
		batch.TotalRecords,
		batch.FileHash,
		batch.FileName,
		nullableString(s3URL),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create batch: %w", err)
	}

	var uploadLink string
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(upload_link, '')
		FROM public.staging_batches_transactions
		WHERE batch_id = $1
	`, batchID).Scan(&uploadLink); err != nil {
		return nil, fmt.Errorf("failed to load batch upload_link: %w", err)
	}

	allStagingTransactions := buildTransactionStagingRecords(batchID, transactionType, rawPayloads)
	if len(allStagingTransactions) == 0 {
		return nil, fmt.Errorf("no valid staging transactions created from upload")
	}

	if err := s.batchProcessor.BatchInsertStagingTransactionsWithTx(ctx, tx, allStagingTransactions); err != nil {
		return nil, fmt.Errorf("failed to insert staging data: %w", err)
	}

	if err := ProcessStagingTransactionsToCanonicalV2WithTx(ctx, tx, batchID, userName, uploadLink); err != nil {
		return nil, fmt.Errorf("failed to process staging data to canonical tables: %w", err)
	}

	totalRecords := len(allStagingTransactions)
	processedRecords := totalRecords
	_, err = tx.Exec(ctx, `
		UPDATE public.staging_batches_transactions
		SET total_records = $1,
		    processed_records = $2,
		    status = 'completed',
		    upload_link = $3
		WHERE batch_id = $4
	`, totalRecords, processedRecords, nullableString(s3URL), batchID)
	if err != nil {
		return nil, fmt.Errorf("failed to update batch totals: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	committed = true

	return &TransactionUploadResult{
		BatchID:          batchID,
		TotalRecords:     totalRecords,
		ProcessedRecords: processedRecords,
		ProcessedFiles:   []string{fileHeader.Filename},
		ProcessingTime:   time.Since(startTime),
		FileStorageKey:   s3Key,
		FileStorageURL:   s3URL,
	}, nil
}

func transactionTypeFromField(fileField string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(fileField)) {
	case "payables":
		return "PAYABLE", nil
	case "receivables":
		return "RECEIVABLE", nil
	default:
		return "", fmt.Errorf("unsupported transaction upload field: %s", fileField)
	}
}

func buildTransactionStagingRecords(batchID uuid.UUID, transactionType string, rawPayloads []json.RawMessage) []StagingTransaction {
	stagingTransactions := make([]StagingTransaction, 0, len(rawPayloads))
	for _, payload := range rawPayloads {
		stagingTransactions = append(stagingTransactions, StagingTransaction{
			StagingID:          uuid.New(),
			BatchID:            batchID,
			TransactionType:    transactionType,
			RawPayload:         payload,
			IngestionTimestamp: time.Now(),
			Status:             "pending",
		})
	}
	return stagingTransactions
}

func processTransactionFileBytes(fileBytes []byte, filename, transactionType string) ([]json.RawMessage, error) {
	switch strings.ToLower(strings.TrimSpace(filepath.Ext(filename))) {
	case ".csv":
		return processTransactionCSVFile(bytes.NewReader(fileBytes), transactionType)
	case ".xlsx":
		return processTransactionExcelFile(bytes.NewReader(fileBytes), transactionType)
	case ".xls":
		return processLegacyTransactionExcelFile(fileBytes)
	default:
		return nil, fmt.Errorf("unsupported file type: %s", filename)
	}
}

func processLegacyTransactionExcelFile(fileBytes []byte) ([]json.RawMessage, error) {
	tmpFile, err := os.CreateTemp("", "payrec-*.xls")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp xls file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if _, err := tmpFile.Write(fileBytes); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("failed to write temp xls file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temp xls file: %w", err)
	}

	book, err := xls.OpenFile(tmpName)
	if err != nil {
		return nil, fmt.Errorf("failed to open legacy xls file: %w", err)
	}
	sheet, err := book.GetSheet(0)
	if err != nil || sheet == nil {
		if err == nil {
			err = fmt.Errorf("legacy xls sheet not found")
		}
		return nil, fmt.Errorf("failed to access legacy xls sheet: %w", err)
	}

	xlsRows := sheet.GetRows()
	if len(xlsRows) < 2 {
		return nil, fmt.Errorf("legacy xls file must have at least header and one data row")
	}

	headers := make([]string, 0)
	for _, col := range xlsRows[0].GetCols() {
		headers = append(headers, strings.TrimSpace(col.GetString()))
	}
	if len(headers) == 0 {
		return nil, fmt.Errorf("legacy xls header row is empty")
	}

	rawPayloads := make([]json.RawMessage, 0, len(xlsRows)-1)
	for i, row := range xlsRows[1:] {
		cols := row.GetCols()
		if len(cols) == 0 {
			continue
		}

		rowData := make(map[string]interface{})
		for j, col := range cols {
			if j < len(headers) {
				rowData[headers[j]] = strings.TrimSpace(col.GetString())
			}
		}
		if len(rowData) == 0 {
			continue
		}

		jsonData, err := json.Marshal(rowData)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal legacy xls row %d: %w", i+1, err)
		}
		rawPayloads = append(rawPayloads, json.RawMessage(jsonData))
	}

	return rawPayloads, nil
}

func stringPtr(s string) *string {
	return &s
}

func nullableString(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return &s
}
