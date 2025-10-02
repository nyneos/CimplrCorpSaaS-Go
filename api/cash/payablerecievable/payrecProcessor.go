package payablerecievable

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ProcessStagingTransactionsToCanonicalV2(pool *pgxpool.Pool, batchID uuid.UUID, userName string) error {
	ctx := context.Background()
	startTime := time.Now()

	mappingRows, err := pool.Query(ctx, `
		SELECT transaction_type, source_column_name, target_table_name, target_field_name 
		FROM public.upload_mappings_transactions 
		ORDER BY transaction_type, target_table_name, target_field_name`)
	if err != nil {
		return fmt.Errorf("failed to load upload mappings: %w", err)
	}
	defer mappingRows.Close()

	// Pre-load all mappings into memory
	allMappings := make(map[string]map[string][]struct {
		SourceCol   string
		TargetField string
	})

	for mappingRows.Next() {
		var transactionType, sourceCol, targetTable, targetField string
		if err := mappingRows.Scan(&transactionType, &sourceCol, &targetTable, &targetField); err != nil {
			continue
		}

		if allMappings[transactionType] == nil {
			allMappings[transactionType] = make(map[string][]struct {
				SourceCol   string
				TargetField string
			})
		}

		if allMappings[transactionType][targetTable] == nil {
			allMappings[transactionType][targetTable] = []struct {
				SourceCol   string
				TargetField string
			}{}
		}

		allMappings[transactionType][targetTable] = append(allMappings[transactionType][targetTable], struct {
			SourceCol   string
			TargetField string
		}{SourceCol: sourceCol, TargetField: targetField})
	}

	rows, err := pool.Query(ctx, `
		SELECT staging_id, transaction_type, raw_payload 
		FROM public.staging_transactions 
		WHERE batch_id = $1 AND status = 'pending'`, batchID)
	if err != nil {
		return fmt.Errorf("failed to load staging records: %w", err)
	}
	defer rows.Close()

	var allRecords []struct {
		StagingID       uuid.UUID
		TransactionType string
		RawData         map[string]interface{}
	}
	var scanErrors []string

	for rows.Next() {

		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled during record loading: %w", ctx.Err())
		default:
		}

		var stagingID uuid.UUID
		var transactionType string
		var rawPayload json.RawMessage

		if err := rows.Scan(&stagingID, &transactionType, &rawPayload); err != nil {
			errorMsg := fmt.Sprintf("Failed to scan staging record ID %s: %v", stagingID, err)
			scanErrors = append(scanErrors, errorMsg)
			log.Printf("ERROR: %s", errorMsg)
			continue // Continue processing other records but track errors
		}

		var rawData map[string]interface{}
		if err := json.Unmarshal(rawPayload, &rawData); err != nil {
			errorMsg := fmt.Sprintf("Failed to unmarshal payload for staging ID %s: %v", stagingID, err)
			scanErrors = append(scanErrors, errorMsg)
			log.Printf("ERROR: %s", errorMsg)
			continue
		}

		allRecords = append(allRecords, struct {
			StagingID       uuid.UUID
			TransactionType string
			RawData         map[string]interface{}
		}{
			StagingID:       stagingID,
			TransactionType: transactionType,
			RawData:         rawData,
		})
	}

	if len(scanErrors) > 0 {
		return fmt.Errorf("failed to process %d staging records due to scan/unmarshal errors: %s", len(scanErrors), strings.Join(scanErrors, "; "))
	}

	if len(allRecords) == 0 {
		log.Printf("No pending staging records found for batch %s", batchID)
		return nil
	}
	log.Printf("PERFORMANCE: Loaded %d staging transaction records for processing", len(allRecords))

	// OPTIMIZATION 3: Prepare batch inserts
	processedIDs := []uuid.UUID{}
	var payableIDs []string
	var receivableIDs []string

	// Use pgx batch for maximum performance
	batch := &pgx.Batch{}

	for _, record := range allRecords {
		mappings, exists := allMappings[record.TransactionType]
		if !exists {
			log.Printf("No mappings found for transaction type %s", record.TransactionType)
			continue
		}

		// Process payables mappings
		if record.TransactionType == "PAYABLE" {
			if payableMappings, exists := mappings["tr_payables"]; exists {
				payableData := make(map[string]interface{})

				for _, mapping := range payableMappings {
					value := getValueFromTransactionRawData(record.RawData, mapping.SourceCol)
					if value != nil {
						payableData[mapping.TargetField] = convertTransactionValueByDataType(value, mapping.TargetField)
					}
				}

				if len(payableData) > 0 {
					// Build dynamic insert query for payables
					var fields []string
					var placeholders []string
					var values []interface{}
					i := 1

					for field, value := range payableData {
						fields = append(fields, field)
						placeholders = append(placeholders, fmt.Sprintf("$%d", i))
						values = append(values, value)
						i++
					}

					query := fmt.Sprintf(`
						INSERT INTO public.tr_payables (%s) 
						VALUES (%s) RETURNING payable_id`,
						strings.Join(fields, ", "),
						strings.Join(placeholders, ", "))

					batch.Queue(query, values...)
					processedIDs = append(processedIDs, record.StagingID)
				}
			}
		}

		if record.TransactionType == "RECEIVABLE" {
			if receivableMappings, exists := mappings["tr_receivables"]; exists {
				receivableData := make(map[string]interface{})

				for _, mapping := range receivableMappings {
					value := getValueFromTransactionRawData(record.RawData, mapping.SourceCol)
					if value != nil {
						receivableData[mapping.TargetField] = convertTransactionValueByDataType(value, mapping.TargetField)
					}
				}

				if len(receivableData) > 0 {
					// Build dynamic insert query for receivables
					var fields []string
					var placeholders []string
					var values []interface{}
					i := 1

					for field, value := range receivableData {
						fields = append(fields, field)
						placeholders = append(placeholders, fmt.Sprintf("$%d", i))
						values = append(values, value)
						i++
					}

					query := fmt.Sprintf(`
						INSERT INTO public.tr_receivables (%s) 
						VALUES (%s) RETURNING receivable_id`,
						strings.Join(fields, ", "),
						strings.Join(placeholders, ", "))

					batch.Queue(query, values...)
					processedIDs = append(processedIDs, record.StagingID)
				}
			}
		}
	}

	batchResults := pool.SendBatch(ctx, batch)
	defer batchResults.Close()

	// Process batch results and collect IDs
	insertCount := 0
	for i := 0; i < batch.Len(); i++ {
		if strings.Contains(batch.QueuedQueries[i].SQL, "tr_payables") {
			var payableID string
			err := batchResults.QueryRow().Scan(&payableID)
			if err != nil {
				log.Printf("Error executing payables batch item %d: %v", i, err)
			} else {
				insertCount++
				payableIDs = append(payableIDs, payableID)
			}
		} else if strings.Contains(batch.QueuedQueries[i].SQL, "tr_receivables") {
			var receivableID string
			err := batchResults.QueryRow().Scan(&receivableID)
			if err != nil {
				log.Printf("Error executing receivables batch item %d: %v", i, err)
			} else {
				insertCount++
				receivableIDs = append(receivableIDs, receivableID)
			}
		} else {
			_, err := batchResults.Exec()
			if err != nil {
				log.Printf("Error executing batch item %d: %v", i, err)
			} else {
				insertCount++
			}
		}
	}

	// Insert audit actions for payables
	if len(payableIDs) > 0 {
		var auditValues []string
		for _, pid := range payableIDs {
			auditValues = append(auditValues, fmt.Sprintf("('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())", pid, userName))
		}
		auditSQL := "INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES " + strings.Join(auditValues, ",")
		_, auditErr := pool.Exec(ctx, auditSQL)
		if auditErr != nil {
			log.Printf("Error inserting audit actions for payables: %v", auditErr)
		}
	}

	// Insert audit actions for receivables
	if len(receivableIDs) > 0 {
		var auditValues []string
		for _, rid := range receivableIDs {
			auditValues = append(auditValues, fmt.Sprintf("('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())", rid, userName))
		}
		auditSQL := "INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES " + strings.Join(auditValues, ",")
		_, auditErr := pool.Exec(ctx, auditSQL)
		if auditErr != nil {
			log.Printf("Error inserting audit actions for receivables: %v", auditErr)
		}
	}

	if len(processedIDs) > 0 {
		// Convert UUIDs to strings for PostgreSQL compatibility
		processedIDStrings := make([]string, len(processedIDs))
		for i, id := range processedIDs {
			processedIDStrings[i] = id.String()
		}

		_, err = pool.Exec(ctx, `
			UPDATE public.staging_transactions 
			SET status = 'processed' 
			WHERE staging_id = ANY($1)`, processedIDStrings)
		if err != nil {
			log.Printf("Error updating staging record status: %v", err)
		}
	}

	processingTime := time.Since(startTime)
	recordsPerSecond := float64(len(processedIDs)) / processingTime.Seconds()

	log.Printf("PERFORMANCE COMPLETE: Processed %d transaction records in %v (%.2f records/sec)",
		len(processedIDs), processingTime, recordsPerSecond)
	log.Printf("PERFORMANCE TARGET: %v for 1000 records (Target: 5 seconds)",
		time.Duration(float64(time.Second)*1000.0/recordsPerSecond))

	return nil
}

func getValueFromTransactionRawData(rawData map[string]interface{}, sourceCol string) interface{} {
	// Try exact match first
	if val, exists := rawData[sourceCol]; exists {
		return val
	}

	// Try case-insensitive match
	lowerSourceCol := strings.ToLower(sourceCol)
	for key, val := range rawData {
		if strings.ToLower(key) == lowerSourceCol {
			return val
		}
	}

	// Try with spaces removed
	sourceColNoSpaces := strings.ReplaceAll(sourceCol, " ", "")
	for key, val := range rawData {
		keyNoSpaces := strings.ReplaceAll(key, " ", "")
		if strings.EqualFold(keyNoSpaces, sourceColNoSpaces) {
			return val
		}
	}

	return nil
}

func convertTransactionValueByDataType(value interface{}, fieldName string) interface{} {
	strVal := fmt.Sprintf("%v", value)
	strVal = strings.TrimSpace(strVal)

	// Convert based on field name patterns for transactions
	switch strings.ToLower(fieldName) {
	case "amount", "invoice_amount", "old_amount", "old_invoice_amount":
		if f, err := strconv.ParseFloat(strVal, 64); err == nil {
			return f
		}
		return 0.0
	case "invoice_date", "due_date", "old_invoice_date", "old_due_date":
		// Try to parse various date formats
		layouts := []string{
			"2006-01-02",
			"02-01-2006",
			"01/02/2006",
			"2 Jan 2006",
			"2006/01/02",
			"2006-01-02 15:04:05",
			"02-01-2006 15:04:05",
		}

		for _, layout := range layouts {
			if t, err := time.Parse(layout, strVal); err == nil {
				return t.Format("2006-01-02") // Return as date string
			}
		}
		// If no date format matched, return current date as fallback
		return time.Now().Format("2006-01-02")
	case "entity_name", "counterparty_name", "invoice_number", "currency_code",
		"old_entity_name", "old_counterparty_name", "old_invoice_number", "old_currency_code":
		return strVal
	default:
		return strVal
	}
}

// Helper functions for data transformation
func transformToPayableV2(rawPayload json.RawMessage) (PayableV2, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(rawPayload, &data); err != nil {
		return PayableV2{}, err
	}

	payable := PayableV2{}

	payable.EntityName = getStringFromTransactionData(data, []string{"entity_name", "Entity Name", "Entity", "Company"})
	payable.CounterpartyName = getStringFromTransactionData(data, []string{"counterparty_name", "Counterparty Name", "Vendor Name", "Supplier"})
	payable.InvoiceNumber = getStringFromTransactionData(data, []string{"invoice_number", "Invoice Number", "Invoice No", "Bill Number"})
	payable.CurrencyCode = getStringFromTransactionData(data, []string{"currency_code", "Currency Code", "Currency", "CCY"})

	// Numeric fields
	if amount := getFloatFromTransactionData(data, []string{"amount", "Amount", "Invoice Amount", "Total Amount"}); amount != nil {
		payable.Amount = *amount
	}

	// Date fields
	if invoiceDate := getDateFromTransactionData(data, []string{"invoice_date", "Invoice Date", "Bill Date"}); invoiceDate != nil {
		payable.InvoiceDate = *invoiceDate
	}
	if dueDate := getDateFromTransactionData(data, []string{"due_date", "Due Date", "Payment Date"}); dueDate != nil {
		payable.DueDate = *dueDate
	}

	return payable, nil
}

func transformToReceivableV2(rawPayload json.RawMessage) (ReceivableV2, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(rawPayload, &data); err != nil {
		return ReceivableV2{}, err
	}

	receivable := ReceivableV2{}

	// Map fields from raw payload with type conversion and multiple field name variations
	receivable.EntityName = getStringFromTransactionData(data, []string{"entity_name", "Entity Name", "Entity", "Company"})
	receivable.CounterpartyName = getStringFromTransactionData(data, []string{"counterparty_name", "Counterparty Name", "Customer Name", "Client"})
	receivable.InvoiceNumber = getStringFromTransactionData(data, []string{"invoice_number", "Invoice Number", "Invoice No", "Bill Number"})
	receivable.CurrencyCode = getStringFromTransactionData(data, []string{"currency_code", "Currency Code", "Currency", "CCY"})

	// Numeric fields
	if amount := getFloatFromTransactionData(data, []string{"invoice_amount", "Invoice Amount", "Amount", "Total Amount"}); amount != nil {
		receivable.InvoiceAmount = *amount
	}

	// Date fields
	if invoiceDate := getDateFromTransactionData(data, []string{"invoice_date", "Invoice Date", "Bill Date"}); invoiceDate != nil {
		receivable.InvoiceDate = *invoiceDate
	}
	if dueDate := getDateFromTransactionData(data, []string{"due_date", "Due Date", "Payment Date"}); dueDate != nil {
		receivable.DueDate = *dueDate
	}

	return receivable, nil
}

// Helper functions for flexible field mapping
func getStringFromTransactionData(data map[string]interface{}, fieldNames []string) string {
	for _, fieldName := range fieldNames {
		if val, exists := data[fieldName]; exists {
			if strVal, ok := val.(string); ok && strVal != "" {
				return strings.TrimSpace(strVal)
			}
		}
	}
	return ""
}

func getFloatFromTransactionData(data map[string]interface{}, fieldNames []string) *float64 {
	for _, fieldName := range fieldNames {
		if val, exists := data[fieldName]; exists {
			switch v := val.(type) {
			case float64:
				return &v
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
					return &f
				}
			}
		}
	}
	return nil
}

func getDateFromTransactionData(data map[string]interface{}, fieldNames []string) *time.Time {
	for _, fieldName := range fieldNames {
		if val, exists := data[fieldName]; exists {
			if strVal, ok := val.(string); ok && strVal != "" {
				layouts := []string{
					"2006-01-02",
					"02-01-2006",
					"01/02/2006",
					"2 Jan 2006",
					"2006/01/02",
					"2006-01-02 15:04:05",
				}

				for _, layout := range layouts {
					if t, err := time.Parse(layout, strings.TrimSpace(strVal)); err == nil {
						return &t
					}
				}
			}
		}
	}
	return nil
}

// ProcessStagingTransactionsToCanonicalV2WithTx - Transaction-aware version for atomic operations
func ProcessStagingTransactionsToCanonicalV2WithTx(ctx context.Context, tx pgx.Tx, batchID uuid.UUID, userName string) error {
	log.Printf("PERFORMANCE: Starting transaction-aware batch processing for batch %s", batchID)

	// Check for context cancellation before starting
	select {
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled before processing: %w", ctx.Err())
	default:
	}

	// Load upload mappings
	log.Printf("PERFORMANCE: Loading transaction upload mappings...")
	mappingRows, err := tx.Query(ctx, `
		SELECT transaction_type, source_column_name, target_table_name, target_field_name 
		FROM public.upload_mappings_transactions 
		ORDER BY transaction_type, target_table_name, target_field_name`)
	if err != nil {
		return fmt.Errorf("failed to load upload mappings: %w", err)
	}
	defer mappingRows.Close()

	// Pre-load all mappings into memory
	allMappings := make(map[string]map[string][]struct {
		SourceCol   string
		TargetField string
	})

	for mappingRows.Next() {
		var transactionType, sourceCol, targetTable, targetField string
		if err := mappingRows.Scan(&transactionType, &sourceCol, &targetTable, &targetField); err != nil {
			return fmt.Errorf("failed to scan mapping: %w", err)
		}

		if allMappings[transactionType] == nil {
			allMappings[transactionType] = make(map[string][]struct {
				SourceCol   string
				TargetField string
			})
		}

		if allMappings[transactionType][targetTable] == nil {
			allMappings[transactionType][targetTable] = []struct {
				SourceCol   string
				TargetField string
			}{}
		}

		allMappings[transactionType][targetTable] = append(allMappings[transactionType][targetTable], struct {
			SourceCol   string
			TargetField string
		}{SourceCol: sourceCol, TargetField: targetField})
	}

	// Load staging records
	log.Printf("PERFORMANCE: Loading all staging transaction records...")
	rows, err := tx.Query(ctx, `
		SELECT staging_id, transaction_type, raw_payload 
		FROM public.staging_transactions 
		WHERE batch_id = $1 AND status = 'pending'`, batchID)
	if err != nil {
		return fmt.Errorf("failed to load staging records: %w", err)
	}
	defer rows.Close()

	// Process records with proper error handling
	var allRecords []struct {
		StagingID       uuid.UUID
		TransactionType string
		RawData         map[string]interface{}
	}
	var processingErrors []string

	for rows.Next() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled during record loading: %w", ctx.Err())
		default:
		}

		var stagingID uuid.UUID
		var transactionType string
		var rawPayload json.RawMessage

		if err := rows.Scan(&stagingID, &transactionType, &rawPayload); err != nil {
			errorMsg := fmt.Sprintf("Failed to scan staging record %s: %v", stagingID, err)
			processingErrors = append(processingErrors, errorMsg)
			log.Printf("ERROR: %s", errorMsg)
			continue
		}

		var rawData map[string]interface{}
		if err := json.Unmarshal(rawPayload, &rawData); err != nil {
			errorMsg := fmt.Sprintf("Failed to unmarshal payload for staging ID %s: %v", stagingID, err)
			processingErrors = append(processingErrors, errorMsg)
			log.Printf("ERROR: %s", errorMsg)
			continue
		}

		allRecords = append(allRecords, struct {
			StagingID       uuid.UUID
			TransactionType string
			RawData         map[string]interface{}
		}{
			StagingID:       stagingID,
			TransactionType: transactionType,
			RawData:         rawData,
		})
	}

	// Fail fast if there were critical processing errors
	if len(processingErrors) > 0 {
		return fmt.Errorf("failed to process %d staging records: %s", len(processingErrors), strings.Join(processingErrors, "; "))
	}

	if len(allRecords) == 0 {
		log.Printf("No pending staging records found for batch %s", batchID)
		return nil
	}

	// Collect processed IDs for final update
	var processedIDs []uuid.UUID
	var payableIDs []string
	var receivableIDs []string
	for _, record := range allRecords {
		processedIDs = append(processedIDs, record.StagingID)
	}

	// Use proper pgx.Batch for FAST batch processing within transaction
	batch := &pgx.Batch{}
	insertCount := 0

	for _, record := range allRecords {
		mappings, exists := allMappings[record.TransactionType]
		if !exists {
			continue
		}

		// Process payables mappings
		if record.TransactionType == "PAYABLE" {
			if payableMappings, exists := mappings["tr_payables"]; exists {
				payableData := make(map[string]interface{})

				for _, mapping := range payableMappings {
					value := getValueFromTransactionRawData(record.RawData, mapping.SourceCol)
					if value != nil {
						payableData[mapping.TargetField] = convertTransactionValueByDataType(value, mapping.TargetField)
					}
				}

				if len(payableData) > 0 {
					var fields []string
					var placeholders []string
					var values []interface{}
					i := 1

					for field, value := range payableData {
						fields = append(fields, field)
						placeholders = append(placeholders, fmt.Sprintf("$%d", i))
						values = append(values, value)
						i++
					}

					query := fmt.Sprintf("INSERT INTO public.tr_payables (%s) VALUES (%s) RETURNING payable_id",
						strings.Join(fields, ", "), strings.Join(placeholders, ", "))

					batch.Queue(query, values...)
				}
			}
		}

		// Process receivables mappings
		if record.TransactionType == "RECEIVABLE" {
			if receivableMappings, exists := mappings["tr_receivables"]; exists {
				receivableData := make(map[string]interface{})

				for _, mapping := range receivableMappings {
					value := getValueFromTransactionRawData(record.RawData, mapping.SourceCol)
					if value != nil {
						receivableData[mapping.TargetField] = convertTransactionValueByDataType(value, mapping.TargetField)
					}
				}

				if len(receivableData) > 0 {
					var fields []string
					var placeholders []string
					var values []interface{}
					i := 1

					for field, value := range receivableData {
						fields = append(fields, field)
						placeholders = append(placeholders, fmt.Sprintf("$%d", i))
						values = append(values, value)
						i++
					}

					query := fmt.Sprintf("INSERT INTO public.tr_receivables (%s) VALUES (%s) RETURNING receivable_id",
						strings.Join(fields, ", "), strings.Join(placeholders, ", "))

					batch.Queue(query, values...)
				}
			}
		}
	}

	br := tx.SendBatch(ctx, batch)

	var insertErrors []string
	for i := 0; i < batch.Len(); i++ {
		if strings.Contains(batch.QueuedQueries[i].SQL, "tr_payables") {
			var payableID string
			err := br.QueryRow().Scan(&payableID)
			if err != nil {
				insertErrors = append(insertErrors, fmt.Sprintf("Payable record %d: %v", i+1, err))
			} else {
				insertCount++
				payableIDs = append(payableIDs, payableID)
			}
		} else if strings.Contains(batch.QueuedQueries[i].SQL, "tr_receivables") {
			var receivableID string
			err := br.QueryRow().Scan(&receivableID)
			if err != nil {
				insertErrors = append(insertErrors, fmt.Sprintf("Receivable record %d: %v", i+1, err))
			} else {
				insertCount++
				receivableIDs = append(receivableIDs, receivableID)
			}
		} else {
			_, err := br.Exec()
			if err != nil {
				insertErrors = append(insertErrors, fmt.Sprintf("Record %d: %v", i+1, err))
			} else {
				insertCount++
			}
		}
	}

	// CRITICAL: Close batch result BEFORE any other tx operations
	br.Close()

	// Fail if any inserts failed
	if len(insertErrors) > 0 {
		return fmt.Errorf("canonical table insertion failed: %d successful, %d failed. Errors: %s",
			insertCount, len(insertErrors), strings.Join(insertErrors, "; "))
	}

	// Insert audit actions for payables
	if len(payableIDs) > 0 {
		var auditValues []string
		for _, pid := range payableIDs {
			auditValues = append(auditValues, fmt.Sprintf("('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())", pid, userName))
		}
		auditSQL := "INSERT INTO auditactionpayable (payable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES " + strings.Join(auditValues, ",")
		_, auditErr := tx.Exec(ctx, auditSQL)
		if auditErr != nil {
			return fmt.Errorf("failed to insert audit actions for payables: %w", auditErr)
		}
	}

	// Insert audit actions for receivables
	if len(receivableIDs) > 0 {
		var auditValues []string
		for _, rid := range receivableIDs {
			auditValues = append(auditValues, fmt.Sprintf("('%s','CREATE','PENDING_APPROVAL',NULL,'%s',now())", rid, userName))
		}
		auditSQL := "INSERT INTO auditactionreceivable (receivable_id, actiontype, processing_status, reason, requested_by, requested_at) VALUES " + strings.Join(auditValues, ",")
		_, auditErr := tx.Exec(ctx, auditSQL)
		if auditErr != nil {
			return fmt.Errorf("failed to insert audit actions for receivables: %w", auditErr)
		}
	}

	_, err = tx.Exec(ctx, "UPDATE public.staging_transactions SET status = 'processed' WHERE batch_id = $1", batchID)
	if err != nil {
		return fmt.Errorf("failed to update staging record status: %v", err)
	}

	return nil
}
