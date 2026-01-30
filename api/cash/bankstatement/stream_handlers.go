package bankstatement

import (
	"CimplrCorpSaas/api/constants"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type BankPDFUpload struct {
	ID               string         `json:"id" db:"id"`
	UserID           sql.NullString `json:"user_id" db:"user_id"`
	OriginalFilename string         `json:"original_filename" db:"original_filename"`
	StoragePath      string         `json:"storage_path" db:"storage_path"`
	ChecksumSHA256   string         `json:"checksum_sha256" db:"checksum_sha256"`
	CreatedAt        time.Time      `json:"created_at" db:"created_at"`
	Status           string         `json:"status" db:"status"`
}

// helper: compute sha256 checksum for bytes
func computeSHA256(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// uploadToSupabase uploads fileBytes to Supabase storage and returns storage path or error
func uploadToSupabase(ctx context.Context, fileBytes []byte, objectPath string) error {
	supaURL := os.Getenv("SUPABASE_URL")
	supaServiceKey := os.Getenv("SUPABASE_SERVICE_ROLE_KEY")
	supaAnonKey := os.Getenv("SUPABASE_ANON_KEY")
	bucketName := os.Getenv("SUPABASE_BUCKET")

	// Trim accidental quoting from .env values (some loaders may leave quotes)
	supaURL = strings.Trim(supaURL, "\"")
	supaServiceKey = strings.Trim(supaServiceKey, "\"")
	supaAnonKey = strings.Trim(supaAnonKey, "\"")
	bucketName = strings.Trim(bucketName, "\"")

	// Require URL, bucket and at least one key (service role preferred)
	if supaURL == "" || bucketName == "" || (supaServiceKey == "" && supaAnonKey == "") {
		return fmt.Errorf("supabase configuration missing; set SUPABASE_URL, SUPABASE_BUCKET and at least one of SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY")
	}

	// Supabase Storage REST upload: PUT to /storage/v1/object/{bucket}/{path}
	// Ensure objectPath is URL-encoded
	// Build URL
	u := fmt.Sprintf("%s/storage/v1/object/%s/%s", strings.TrimRight(supaURL, "/"), bucketName, url.PathEscape(objectPath))
	req, err := http.NewRequestWithContext(ctx, "PUT", u, bytes.NewReader(fileBytes))
	if err != nil {
		return err
	}
	// Use only the project API key in `apikey` header for storage requests
	// Prefer using the service role key for both Authorization and apikey headers when available.
	if supaServiceKey != "" {
		req.Header.Set("Authorization", "Bearer "+supaServiceKey)
		req.Header.Set("apikey", supaServiceKey)
	} else if supaAnonKey != "" {
		req.Header.Set("apikey", supaAnonKey)
	}
	// Set content type based on file extension
	contentType := "application/pdf"
	if strings.HasSuffix(strings.ToLower(objectPath), ".docx") {
		contentType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	}
	req.Header.Set(constants.ContentTypeText, contentType)

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("supabase upload failed: %d %s", resp.StatusCode, string(b))
}

// deleteFromSupabase removes the given objectPath from the configured
// Supabase storage bucket. Used for cleanup if a later DB commit fails.
func deleteFromSupabase(ctx context.Context, objectPath string) error {
	supaURL := strings.Trim(os.Getenv("SUPABASE_URL"), "\"")
	supaServiceKey := strings.Trim(os.Getenv("SUPABASE_SERVICE_ROLE_KEY"), "\"")
	bucketName := strings.Trim(os.Getenv("SUPABASE_BUCKET"), "\"")

	if supaURL == "" || bucketName == "" || supaServiceKey == "" {
		return fmt.Errorf("supabase not configured for delete")
	}

	u := fmt.Sprintf("%s/storage/v1/object/%s/%s", strings.TrimRight(supaURL, "/"), bucketName, url.PathEscape(objectPath))
	req, err := http.NewRequestWithContext(ctx, "DELETE", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+supaServiceKey)
	req.Header.Set("apikey", supaServiceKey)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	b, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("supabase delete failed: %d %s", resp.StatusCode, string(b))
}

// respondWithError logs the internal error and returns a standardized JSON error
func respondWithError(w http.ResponseWriter, err error, userMsg string, code int) {
	if err != nil {
		log.Printf("[bankstatement] internal error: %v", err)
	}
	if userMsg == "" && err != nil {
		userMsg = userFriendlyUploadError(err)
		if userMsg == "" {
			userMsg = "Internal server error"
		}
	}
	if userMsg == "" {
		userMsg = "Internal server error"
	}
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": userMsg})
}

// insertUploadRow inserts a metadata row and returns the generated id
func insertUploadRow(ctx context.Context, db *sql.DB, filename, storagePath, checksum string) (string, error) {
	var id string
	// If storage uploads are disabled we pass an empty string for storage_path
	// The DB in this project requires a non-NULL storage_path column, so do
	// not convert empty string to NULL here. Store $2 as-is.
	q := `INSERT INTO cimplrcorpsaas.bank_pdf_uploads (original_filename, storage_path, checksum_sha256, status) VALUES ($1, $2, $3, 'uploaded') RETURNING id`
	err := db.QueryRowContext(ctx, q, filename, storagePath, checksum).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

// insertUploadRowTx inserts using an explicit transaction so callers can roll
// back if downstream processing fails.
func insertUploadRowTx(ctx context.Context, tx *sql.Tx, filename, storagePath, checksum string) (string, error) {
	var id string
	q := `INSERT INTO cimplrcorpsaas.bank_pdf_uploads (original_filename, storage_path, checksum_sha256, status) VALUES ($1, $2, $3, 'uploaded') RETURNING id`
	if err := tx.QueryRowContext(ctx, q, filename, storagePath, checksum).Scan(&id); err != nil {
		return "", err
	}
	return id, nil
}

// checkExistingByChecksum returns true and id if checksum exists
func checkExistingByChecksum(ctx context.Context, db *sql.DB, checksum string) (bool, string, error) {
	var id string
	q := `SELECT id FROM cimplrcorpsaas.bank_pdf_uploads WHERE checksum_sha256 = $1 LIMIT 1`
	err := db.QueryRowContext(ctx, q, checksum).Scan(&id)
	if err == sql.ErrNoRows {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, id, nil
}

// proxyStreamToFinPDF sends PDF bytes to external fin-pdf-upload streaming endpoint and proxies the response to client
func proxyStreamToFinPDF(w http.ResponseWriter, r *http.Request, fileBytes []byte, filename string) error {
	// v := z4(0x61)
	// v := q9()
	v := q8()
	if v[0] != 'h' {
		v = z4()
	}

	// Build multipart/form-data body with field name `pdf` (file)
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	fw, err := mw.CreateFormFile("pdf", filename)
	if err != nil {
		return err
	}
	if _, err := fw.Write(fileBytes); err != nil {
		return err
	}
	// close writer to set terminating boundary
	if err := mw.Close(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(r.Context(), "POST", v, &b)
	if err != nil {
		return err
	}
	// set content type to multipart with boundary
	req.Header.Set(constants.ContentTypeText, mw.FormDataContentType())

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Stream response headers and body to original client
	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	// ensure chunked streaming
	w.Header().Set(constants.ContentTypeText, "application/json; charset=utf-8")
	w.WriteHeader(resp.StatusCode)

	flusher, ok := w.(http.Flusher)
	if !ok {
		// not able to flush, just copy
		_, err = io.Copy(w, resp.Body)
		return err
	}

	buf := make([]byte, 4096)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			flusher.Flush()
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return readErr
		}
	}
	return nil
}

// UploadBankStatementV3Handler returns http.Handler that accepts file upload and streams preview
func UploadBankStatementV3Handler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		log.Printf("[BANK-PREVIEW] /cash/preview start: remote=%s method=%s", r.RemoteAddr, r.Method)

		// parse multipart form (support file field named "file")
		if err := r.ParseMultipartForm(50 << 20); err != nil {
			http.Error(w, "failed to parse multipart form: "+err.Error(), http.StatusBadRequest)
			return
		}

		if r.MultipartForm == nil || r.MultipartForm.File == nil || len(r.MultipartForm.File["file"]) == 0 {
			http.Error(w, "file is required", http.StatusBadRequest)
			return
		}

		// Detect file type by extension and route to appropriate handler
		fh := r.MultipartForm.File["file"][0]
		ext := strings.ToLower(filepath.Ext(fh.Filename))
		log.Printf("[BANK-PREVIEW] uploaded filename=%s ext=%s", fh.Filename, ext)
		// try to log user_id field if present
		if vals := r.MultipartForm.Value["user_id"]; len(vals) > 0 {
			log.Printf("[BANK-PREVIEW] user_id=%s", vals[0])
		}
		// Accept PDF and DOCX for streaming to external AI parser; others go to V2
		if ext != ".pdf" && ext != ".docx" {
			// delegate to existing V2 handler for Excel/CSV
			log.Printf("[BANK-PREVIEW] delegating to V2 handler for extension=%s", ext)
			h := UploadBankStatementV2Handler(db)
			h.ServeHTTP(w, r)
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "file is required: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		fileBytes, err := io.ReadAll(file)
		if err != nil {
			respondWithError(w, err, "Failed to read uploaded file", http.StatusInternalServerError)
			return
		}

		checksum := computeSHA256(fileBytes)

		// Prepare object path if storage upload is enabled, but do not perform
		// the external upload yet. We will upload only after parsing succeeds so
		// we can keep the whole operation logically atomic (attempt cleanup on failure).
		objectPath := ""
		uploadEnabled := strings.ToLower(strings.TrimSpace(os.Getenv("UPLOAD_TO_STORAGE"))) == "true"
		if uploadEnabled {
			objectPath = fmt.Sprintf("uploads/%s/%s", time.Now().Format("2006/01/02"), header.Filename)
		}

		// Begin a DB transaction so we can check for existing checksum and
		// commit the metadata only after parsing and storage upload succeed.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			respondWithError(w, err, "Failed to start DB transaction", http.StatusInternalServerError)
			return
		}
		// Ensure rollback if we return before explicit commit.
		defer func() {
			if tx != nil {
				_ = tx.Rollback()
			}
		}()

		// Check for existing checksum inside the transaction to avoid races.
		var existingID sql.NullString
		err = tx.QueryRowContext(ctx, `SELECT id FROM cimplrcorpsaas.bank_pdf_uploads WHERE checksum_sha256 = $1 LIMIT 1`, checksum).Scan(&existingID)
		if err == nil && existingID.Valid {
			// already exists — rollback and return
			if rerr := tx.Rollback(); rerr != nil {
				log.Printf("failed to rollback tx after existing-check: %v", rerr)
			}
			tx = nil
			resp := map[string]interface{}{"status": "exists", "id": existingID.String}
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(resp)
			return
		} else if err != nil && err != sql.ErrNoRows {
			respondWithError(w, err, "Failed to check existing uploads", http.StatusInternalServerError)
			return
		}
		// v := z4()
		// v := q9()
		v := q8()
		if v[0] != 'h' {
			v = z4()
		}

		log.Printf("[BANK-PREVIEW] proxying PDF/DOCX to parsing service: fin_url=%s", v)
		// Build multipart/form-data body with field name `pdf` (file)
		var b bytes.Buffer
		mw := multipart.NewWriter(&b)
		fw, err := mw.CreateFormFile("pdf", header.Filename)
		if err != nil {
			respondWithError(w, err, constants.ErrFailedToPrepareFile, http.StatusInternalServerError)
			return
		}
		if _, err := fw.Write(fileBytes); err != nil {
			respondWithError(w, err, constants.ErrFailedToPrepareFile, http.StatusInternalServerError)
			return
		}
		if err := mw.Close(); err != nil {
			respondWithError(w, err, constants.ErrFailedToPrepareFile, http.StatusInternalServerError)
			return
		}

		req, err := http.NewRequestWithContext(ctx, "POST", v, &b)
		if err != nil {
			respondWithError(w, err, "Failed to create parsing request", http.StatusInternalServerError)
			return
		}
		req.Header.Set(constants.ContentTypeText, mw.FormDataContentType())

		client := &http.Client{Timeout: 0}
		resp, err := client.Do(req)
		if err != nil {
			respondWithError(w, err, "Failed to connect to parsing service", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Read the complete AI response
		aiResponseBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			respondWithError(w, err, "Failed to read parsing response", http.StatusInternalServerError)
			return
		}

		// Parse the AI response to merge with our upload data
		var aiResponse map[string]interface{}
		if err := json.Unmarshal(aiResponseBytes, &aiResponse); err != nil {
			log.Printf("AI response parsing error: %v, raw: %s", err, string(aiResponseBytes))
			// rollback the transaction because parsing failed
			if tx != nil {
				if rerr := tx.Rollback(); rerr != nil {
					log.Printf("failed to rollback tx after parse error: %v", rerr)
				}
				tx = nil
			}
			respondWithError(w, err, "Failed to parse AI response", http.StatusInternalServerError)
			return
		}

		// After successful parsing, upload to storage (if enabled) and
		// insert the metadata row inside the transaction. If any of these
		// steps fail we rollback and attempt cleanup so we don't leave
		// dangling state.
		if uploadEnabled {
			if upErr := uploadToSupabase(ctx, fileBytes, objectPath); upErr != nil {
				// rollback transaction
				if tx != nil {
					if rerr := tx.Rollback(); rerr != nil {
						log.Printf("failed to rollback tx after supabase upload failure: %v", rerr)
					}
					tx = nil
				}
				log.Printf("supabase upload failed: %v", upErr)
				respondWithError(w, upErr, "Failed to upload file to storage", http.StatusInternalServerError)
				return
			}
		}

		// Insert metadata row now that parsing (and optional storage upload)
		// have succeeded.
		id, err := insertUploadRowTx(ctx, tx, header.Filename, objectPath, checksum)
		if err != nil {
			// attempt to delete uploaded object if we uploaded earlier
			if uploadEnabled {
				if derr := deleteFromSupabase(ctx, objectPath); derr != nil {
					log.Printf("failed to delete uploaded object after insert failure: %v", derr)
				}
			}
			respondWithError(w, err, "Failed to persist upload metadata", http.StatusInternalServerError)
			return
		}

		// Merge upload metadata with AI response
		combinedResponse := map[string]interface{}{
			"id":     id,
			"status": "uploaded",
		}

		// Copy all fields from AI response
		for key, value := range aiResponse {
			combinedResponse[key] = value
		}

		// Commit the metadata insert now that parsing succeeded.
		if tx != nil {
			if cerr := tx.Commit(); cerr != nil {
				log.Printf("failed to commit upload metadata: %v", cerr)
				respondWithError(w, cerr, "Failed to persist upload metadata", http.StatusInternalServerError)
				return
			}
			tx = nil
		}

		// Send the single combined JSON response
		w.Header().Set(constants.ContentTypeText, "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(combinedResponse); err != nil {
			log.Printf("failed to write response: %v", err)
		}
	})
}

// RecalculateHandler accepts bank statement transactions and validates running balances
func RecalculateHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input RecalculateInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			respondWithError(w, err, "Invalid request format. Please check your data and try again.", http.StatusBadRequest)
			return
		}

		// No top-level metadata pointer in types; validate required metadata fields below

		if len(input.Clean.Transactions) == 0 {
			respondWithError(w, nil, "No transactions found in the statement. Please upload a statement with transaction data.", http.StatusBadRequest)
			return
		}

		// Validate account number
		if input.Clean.Metadata.AccountNumber == nil || strings.TrimSpace(*input.Clean.Metadata.AccountNumber) == "" {
			respondWithError(w, nil, "Account number is missing from the statement metadata. Please upload the original bank statement.", http.StatusBadRequest)
			return
		}

		// Validate transaction dates are not empty
		for i, tx := range input.Clean.Transactions {
			if tx.TranDate == nil || strings.TrimSpace(*tx.TranDate) == "" {
				respondWithError(w, nil, fmt.Sprintf("Transaction date is missing for transaction %d. All transactions must have valid dates.", i+1), http.StatusBadRequest)
				return
			}
			// Validate date format
			if _, err := time.Parse(constants.DateFormat, *tx.TranDate); err != nil {
				respondWithError(w, nil, fmt.Sprintf("Invalid date format for transaction %d. Expected format: YYYY-MM-DD.", i+1), http.StatusBadRequest)
				return
			}
		}

		// Validate opening balance if provided
		if input.Clean.OpeningBalance == nil {
			respondWithError(w, nil, "Opening balance is required for balance validation.", http.StatusBadRequest)
			return
		}

		// Validate and process transactions
		var outputTxs []RecalculateTransactionOutput
		var issues []ValidationIssue

		// Start with opening balance
		var runningBalance *float64
		if input.Clean.OpeningBalance != nil {
			rb := *input.Clean.OpeningBalance
			runningBalance = &rb
		}

		for _, tx := range input.Clean.Transactions {
			// Normalize null amounts to 0
			withdrawal := float64(0)
			if tx.Withdrawal != nil {
				withdrawal = *tx.Withdrawal
			}
			deposit := float64(0)
			if tx.Deposit != nil {
				deposit = *tx.Deposit
			}

			// Normalize narration
			narration := ""
			if tx.Narration != nil {
				narration = *tx.Narration
			}

			// Compute running balance
			if runningBalance != nil {
				rb := *runningBalance
				rb = rb - withdrawal + deposit
				runningBalance = &rb
			} else if deposit > 0 || withdrawal > 0 {
				// If no opening balance, start from first transaction
				rb := deposit - withdrawal
				runningBalance = &rb
			}

			// Check balance mismatch if provided - add to issues
			if tx.Balance != nil && runningBalance != nil {
				diff := *runningBalance - *tx.Balance
				if diff > 0.01 || diff < -0.01 { // tolerance for rounding
					issues = append(issues, ValidationIssue{
						Transaction:     tx,
						ExpectedBalance: runningBalance,
						ActualBalance:   tx.Balance,
					})
				}
			}

			// Build output transaction
			outputTxs = append(outputTxs, RecalculateTransactionOutput{
				TranDate:       tx.TranDate,
				ValueDate:      tx.ValueDate,
				Narration:      narration,
				Withdrawal:     withdrawal,
				Deposit:        deposit,
				Balance:        tx.Balance,
				RunningBalance: runningBalance,
			})
		}

		// Compute validation result
		isValid := len(issues) == 0

		// Get actual closing balance from metadata
		var actualClosing *float64
		if input.Clean.Metadata.ClosingBalance != nil {
			actualClosing = input.Clean.Metadata.ClosingBalance
		}

		// Determine validation status
		status := "valid"
		if !isValid {
			status = "invalid"
		}

		output := RecalculateOutput{
			Success:         isValid,
			Status:          status,
			ComputedClosing: runningBalance,
			ActualClosing:   actualClosing,
			IsValid:         isValid,
			Clean: RecalculateCleanDataOutput{
				Metadata:       input.Clean.Metadata,
				OpeningBalance: input.Clean.OpeningBalance,
				Transactions:   outputTxs,
			},
			Validation: ValidationWrapper{
				Status: status,
				Issues: issues,
			},
		}
		json.NewEncoder(w).Encode(output)
	})
}

// CommitHandler persists clean JSON into bank_pdf_uploads.committed_json by id
func CommitHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			ID    string               `json:"user_id"`
			Clean RecalculateCleanData `json:"clean"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			respondWithError(w, err, "Invalid request format. Please check your data and try again.", http.StatusBadRequest)
			return
		}

		// No top-level metadata pointer in types; validate required metadata fields below

		if len(payload.Clean.Transactions) == 0 {
			respondWithError(w, nil, "No transactions found in the statement. Cannot process empty statement.", http.StatusBadRequest)
			return
		}

		// Validate account number
		accountNumber := ""
		if payload.Clean.Metadata.AccountNumber != nil {
			accountNumber = strings.TrimSpace(*payload.Clean.Metadata.AccountNumber)
		}
		if accountNumber == "" {
			respondWithError(w, nil, "Account number is missing from the statement metadata. Please upload the original bank statement.", http.StatusBadRequest)
			return
		}

		// Validate transaction dates and data integrity
		for i, tx := range payload.Clean.Transactions {
			// Check transaction date
			if tx.TranDate == nil || strings.TrimSpace(*tx.TranDate) == "" {
				respondWithError(w, nil, fmt.Sprintf("Transaction date is missing for transaction %d. All transactions must have valid dates.", i+1), http.StatusBadRequest)
				return
			}
			// Validate date format
			if _, err := time.Parse(constants.DateFormat, *tx.TranDate); err != nil {
				respondWithError(w, nil, fmt.Sprintf("Invalid date format for transaction %d (%s). Expected format: YYYY-MM-DD.", i+1, *tx.TranDate), http.StatusBadRequest)
				return
			}

			// Check that at least one of withdrawal or deposit has a value
			hasWithdrawal := tx.Withdrawal != nil && *tx.Withdrawal != 0
			hasDeposit := tx.Deposit != nil && *tx.Deposit != 0
			if !hasWithdrawal && !hasDeposit {
				respondWithError(w, nil, fmt.Sprintf("Transaction %d has no withdrawal or deposit amount. Invalid transaction data.", i+1), http.StatusBadRequest)
				return
			}

			// Validate that withdrawal and deposit are not both non-zero
			if hasWithdrawal && hasDeposit {
				respondWithError(w, nil, fmt.Sprintf("Transaction %d has both withdrawal and deposit amounts. This is not allowed.", i+1), http.StatusBadRequest)
				return
			}
		}

		// Validate opening balance
		if payload.Clean.OpeningBalance == nil {
			respondWithError(w, nil, "Opening balance is required for statement processing.", http.StatusBadRequest)
			return
		}
		// persist committed JSON and also insert normalized transactions + category matching
		ctx := r.Context()
		// cleanJSON, err := json.Marshal(payload.Clean)
		// if err != nil {
		// 	http.Error(w, "failed to marshal clean json: "+err.Error(), http.StatusInternalServerError)
		// 	return
		// }

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			respondWithError(w, err, "Failed to start database transaction", http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				panic(p)
			}
		}()

		// update committed_json and status
		// if _, err := tx.ExecContext(ctx, `UPDATE cimplrcorpsaas.bank_pdf_uploads SET committed_json = $1, status='committed' WHERE user_id = $2`, cleanJSON, payload.ID); err != nil {
		// 	tx.Rollback()
		// 	respondWithError(w, err, "Failed to persist committed data", http.StatusInternalServerError)
		// 	return
		// }

		// lookup entity id from masterbankaccount (public schema)
		var entityID sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT entity_id FROM public.masterbankaccount WHERE account_number=$1 LIMIT 1`, accountNumber).Scan(&entityID); err != nil {
			if err == sql.ErrNoRows {
				tx.Rollback()
				respondWithError(w, nil, "Bank account not found in master data. Please add this account to the system before uploading statements.", http.StatusBadRequest)
				return
			} else {
				tx.Rollback()
				respondWithError(w, err, "Failed to lookup master bank account", http.StatusInternalServerError)
				return
			}
		}

		// Parse period dates from metadata
		var periodStart, periodEnd time.Time
		if payload.Clean.Metadata.PeriodStart != nil && strings.TrimSpace(*payload.Clean.Metadata.PeriodStart) != "" {
			if t, err := time.Parse(constants.DateFormat, *payload.Clean.Metadata.PeriodStart); err == nil {
				periodStart = t
			}
		}
		if payload.Clean.Metadata.PeriodEnd != nil && strings.TrimSpace(*payload.Clean.Metadata.PeriodEnd) != "" {
			if t, err := time.Parse(constants.DateFormat, *payload.Clean.Metadata.PeriodEnd); err == nil {
				periodEnd = t
			}
		}

		// Use first and last transaction dates as fallback
		if periodStart.IsZero() && len(payload.Clean.Transactions) > 0 {
			if payload.Clean.Transactions[0].TranDate != nil && strings.TrimSpace(*payload.Clean.Transactions[0].TranDate) != "" {
				if t, err := time.Parse(constants.DateFormat, *payload.Clean.Transactions[0].TranDate); err == nil {
					periodStart = t
				}
			}
		}
		if periodEnd.IsZero() && len(payload.Clean.Transactions) > 0 {
			lastTx := payload.Clean.Transactions[len(payload.Clean.Transactions)-1]
			if lastTx.TranDate != nil && strings.TrimSpace(*lastTx.TranDate) != "" {
				if t, err := time.Parse(constants.DateFormat, *lastTx.TranDate); err == nil {
					periodEnd = t
				}
			}
		}

		// Validate we have valid period dates
		if periodStart.IsZero() {
			tx.Rollback()
			respondWithError(w, nil, "Statement period start date is missing or invalid. Please ensure the statement contains valid period information.", http.StatusBadRequest)
			return
		}
		if periodEnd.IsZero() {
			tx.Rollback()
			respondWithError(w, nil, "Statement period end date is missing or invalid. Please ensure the statement contains valid period information.", http.StatusBadRequest)
			return
		}

		// Validate period dates make sense
		if periodEnd.Before(periodStart) {
			tx.Rollback()
			respondWithError(w, nil, "Statement period end date cannot be before start date. Please check the statement dates.", http.StatusBadRequest)
			return
		}

		openingBalance := 0.0
		if payload.Clean.OpeningBalance != nil {
			openingBalance = *payload.Clean.OpeningBalance
		}
		closingBalance := 0.0
		if payload.Clean.Metadata.ClosingBalance != nil {
			closingBalance = *payload.Clean.Metadata.ClosingBalance
		}

		// Generate file hash for deduplication
		fileHash := fmt.Sprintf("%s_%s_%s", accountNumber, periodStart.Format(constants.DateFormat), periodEnd.Format(constants.DateFormat))

		// Insert parent bank_statements row (matching V2 structure)
		var bankStatementID string
		err = tx.QueryRowContext(ctx, `
			INSERT INTO cimplrcorpsaas.bank_statements (
				entity_id, account_number, statement_period_start, statement_period_end, 
				file_hash, opening_balance, closing_balance
			) VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT ON CONSTRAINT uniq_stmt
			DO UPDATE SET file_hash = EXCLUDED.file_hash, closing_balance = EXCLUDED.closing_balance
			RETURNING bank_statement_id
		`, entityID, accountNumber, periodStart, periodEnd, fileHash, openingBalance, closingBalance).Scan(&bankStatementID)
		if err != nil {
			tx.Rollback()
			respondWithError(w, err, "Failed to insert bank statement", http.StatusInternalServerError)
			return
		}

		// Skip category matching during commit (category rules need *sql.DB not *sql.Tx)
		// Category matching can be done in recompute/approve flow
		var rules []categoryRuleComponent

		// Bulk insert transactions into cimplrcorpsaas.bank_statement_transactions
		txs := payload.Clean.Transactions
		if len(txs) > 0 {
			valueStrings := make([]string, 0, len(txs))
			valueArgs := make([]interface{}, 0, len(txs)*11)
			for i, t := range txs {
				// Parse dates
				var tranDate, valueDate time.Time
				if t.TranDate != nil && strings.TrimSpace(*t.TranDate) != "" {
					if parsed, err := time.Parse(constants.DateFormat, *t.TranDate); err == nil {
						tranDate = parsed
					}
				}
				if t.ValueDate != nil && strings.TrimSpace(*t.ValueDate) != "" {
					if parsed, err := time.Parse(constants.DateFormat, *t.ValueDate); err == nil {
						valueDate = parsed
					}
				}

				// Get narration
				narration := ""
				if t.Narration != nil {
					narration = *t.Narration
				}

				// Get withdrawal/deposit amounts
				var wd sql.NullFloat64
				var dep sql.NullFloat64
				if t.Withdrawal != nil && *t.Withdrawal > 0 {
					wd = sql.NullFloat64{Float64: *t.Withdrawal, Valid: true}
				}
				if t.Deposit != nil && *t.Deposit > 0 {
					dep = sql.NullFloat64{Float64: *t.Deposit, Valid: true}
				}

				// Get balance
				var balance sql.NullFloat64
				if t.Balance != nil {
					balance = sql.NullFloat64{Float64: *t.Balance, Valid: true}
				}

				// match category
				matched := matchCategoryForTransaction(rules, narration, wd, dep)

				raw, _ := json.Marshal(t)

				valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11))
				valueArgs = append(valueArgs,
					bankStatementID,
					accountNumber,
					nil,       // tran_id
					valueDate, // value_date
					tranDate,  // transaction_date
					narration,
					wd,
					dep,
					balance,
					raw,
					matched,
				)
			}
			stmt := `INSERT INTO cimplrcorpsaas.bank_statement_transactions (bank_statement_id, account_number, tran_id, value_date, transaction_date, description, withdrawal_amount, deposit_amount, balance, raw_json, category_id) VALUES ` + strings.Join(valueStrings, ",") + ` ON CONFLICT (account_number, transaction_date, description, withdrawal_amount, deposit_amount) DO NOTHING`
			if _, err := tx.ExecContext(ctx, stmt, valueArgs...); err != nil {
				tx.Rollback()
				respondWithError(w, err, "Failed to insert transactions", http.StatusInternalServerError)
				return
			}
		}

		// insert audit action
		requestedBy := "system"
		if s := r.Context().Value("UserID"); s != nil {
			if us, ok := s.(string); ok && us != "" {
				requestedBy = us
			}
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO cimplrcorpsaas.auditactionbankstatement (
				bankstatementid, actiontype, processing_status, requested_by, requested_at
			) VALUES ($1, $2, $3, $4, $5)
		`, bankStatementID, "CREATE", "PENDING_APPROVAL", requestedBy, time.Now())
		if err != nil {
			tx.Rollback()
			respondWithError(w, err, "Failed to record audit action", http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(); err != nil {
			respondWithError(w, err, "Failed to commit transaction", http.StatusInternalServerError)
			return
		}

		// Now build categorization KPIs matching V2 output format
		// Reload category rules with db (not tx) for proper categorization
		entityIDStr := ""
		if entityID.Valid {
			entityIDStr = entityID.String
		}
		rules, _ = loadCategoryRuleComponents(ctx, db, accountNumber, entityIDStr, "INR")

		// Compute KPIs
		kpiCats := []map[string]interface{}{}
		foundCategories := []map[string]interface{}{}
		foundCategoryIDs := map[int64]bool{}
		categoryCount := map[int64]int{}
		debitSum := map[int64]float64{}
		creditSum := map[int64]float64{}
		categoryTxns := map[int64][]map[string]interface{}{}
		uncategorized := []map[string]interface{}{}

		totalTxns := len(txs)
		groupedTxns := 0
		ungroupedTxns := 0

		for i, t := range txs {
			// Get withdrawal/deposit
			var wd, dep float64
			if t.Withdrawal != nil {
				wd = *t.Withdrawal
			}
			if t.Deposit != nil {
				dep = *t.Deposit
			}

			narration := ""
			if t.Narration != nil {
				narration = *t.Narration
			}

			// Parse dates into time.Time when possible so JSON uses RFC3339 timestamps
			var tranDateVal interface{}
			var valueDateVal interface{}
			if t.TranDate != nil && strings.TrimSpace(*t.TranDate) != "" {
				if pd, err := time.Parse(constants.DateFormat, *t.TranDate); err == nil {
					tranDateVal = pd
				} else {
					tranDateVal = *t.TranDate
				}
			}
			if t.ValueDate != nil && strings.TrimSpace(*t.ValueDate) != "" {
				if pd, err := time.Parse(constants.DateFormat, *t.ValueDate); err == nil {
					valueDateVal = pd
				} else {
					valueDateVal = *t.ValueDate
				}
			}

			// Match category
			wdNull := sql.NullFloat64{Valid: wd > 0, Float64: wd}
			depNull := sql.NullFloat64{Valid: dep > 0, Float64: dep}
			matched := matchCategoryForTransaction(rules, narration, wdNull, depNull)

			if matched.Valid {
				groupedTxns++
				catID := matched.Int64
				categoryCount[catID]++
				debitSum[catID] += wd
				creditSum[catID] += dep

				txMap := map[string]interface{}{
					"index":       i,
					"tran_date":   tranDateVal,
					"value_date":  valueDateVal,
					"narration":   narration,
					"amount":       map[string]interface{}{"withdrawal": wd, "deposit": dep},
					"balance":     t.Balance,
					"category_id": catID,
				}
				categoryTxns[catID] = append(categoryTxns[catID], txMap)
			} else {
				ungroupedTxns++
				uncategorized = append(uncategorized, map[string]interface{}{
					"index":      i,
					"tran_date":  tranDateVal,
					"value_date": valueDateVal,
					"narration":  narration,
					"amount":      map[string]interface{}{"withdrawal": wd, "deposit": dep},
					"balance":    t.Balance,
				})
			}
		}

		// Build category KPIs
		for catID, count := range categoryCount {
			var catName string
			for _, rule := range rules {
				if rule.CategoryID == catID {
					catName = rule.CategoryName
					break
				}
			}
			kpiCats = append(kpiCats, map[string]interface{}{
				"category_id":   catID,
				"category_name": catName,
				"count":         count,
				"debit_sum":     debitSum[catID],
				"credit_sum":    creditSum[catID],
				"transactions":  categoryTxns[catID],
			})
			foundCategoryIDs[catID] = true
		}

		// Compute percentages
		groupedPct := 0.0
		ungroupedPct := 0.0
		if totalTxns > 0 {
			groupedPct = float64(groupedTxns) * 100.0 / float64(totalTxns)
			ungroupedPct = float64(ungroupedTxns) * 100.0 / float64(totalTxns)
		}

		// Add found categories
		for _, rule := range rules {
			if foundCategoryIDs[rule.CategoryID] {
				foundCategories = append(foundCategories, map[string]interface{}{
					"category_id":   rule.CategoryID,
					"category_name": rule.CategoryName,
					"category_type": rule.CategoryType,
				})
				delete(foundCategoryIDs, rule.CategoryID)
			}
		}

		// Get page_count from metadata, fallback to 1
		pagesProcessed := 1
		if payload.Clean.Metadata.PageCount != nil {
			pagesProcessed = *payload.Clean.Metadata.PageCount
		}

		// transactions_under_review are those with balance mismatches (from uncategorized)
		reviewTransactions := []map[string]interface{}{}
		for _, unc := range uncategorized {
			// Check if this transaction has a balance mismatch
			// We can identify review-worthy transactions as those in uncategorized list
			reviewTransactions = append(reviewTransactions, unc)
		}

		// Build response matching V2 format
		data := map[string]interface{}{
			"bank_statement_id":               bankStatementID,
			"transactions_uploaded_count":     totalTxns,
			"category_kpis":                   kpiCats,
			"categories_found":                foundCategories,
			"uncategorized":                   uncategorized,
			"grouped_transaction_count":       groupedTxns,
			"ungrouped_transaction_count":     ungroupedTxns,
			"grouped_transaction_percent":     groupedPct,
			"ungrouped_transaction_percent":   ungroupedPct,
			"statement_date_coverage":         map[string]interface{}{"start": periodStart.Format(constants.DateFormat), "end": periodEnd.Format(constants.DateFormat)},
			"bank_wise_status":                []map[string]interface{}{{"account_number": accountNumber, "status": "SUCCESS"}},
			"pages_processed":                 pagesProcessed,
			"transactions_under_review_count": len(reviewTransactions),
			"transactions_under_review":       reviewTransactions,
		}

		// Wrap in standardized response format
		result := map[string]interface{}{
			"data":    data,
			"message": "Bank statement uploaded successfully",
			"success": true,
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(result)
	})
}

// GetPDFMetadataHandler fetches metadata by id
func GetPDFMetadataHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id string
		// support GET ?id=... or POST {"id":"...","user_id":"..."}
		if r.Method == http.MethodGet {
			id = r.URL.Query().Get("id")
		} else if r.Method == http.MethodPost {
			var body struct {
				ID     string `json:"id"`
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid payload: "+err.Error(), http.StatusBadRequest)
				return
			}
			id = body.ID
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if id == "" {
			http.Error(w, "id required", http.StatusBadRequest)
			return
		}
		var row struct {
			ID               string
			OriginalFilename string
			StoragePath      string
			ChecksumSHA256   string
			Status           string
			CreatedAt        time.Time
		}
		q := `SELECT id, original_filename, storage_path, checksum_sha256, status, created_at FROM cimplrcorpsaas.bank_pdf_uploads WHERE id=$1`
		if err := db.QueryRowContext(r.Context(), q, id).Scan(&row.ID, &row.OriginalFilename, &row.StoragePath, &row.ChecksumSHA256, &row.Status, &row.CreatedAt); err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			respondWithError(w, err, "Failed to fetch metadata", http.StatusInternalServerError)
			return
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(row)
	})
}

// DownloadPDFHandler downloads file from supabase and streams to client
func DownloadPDFHandler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id string
		var providedUserID string
		if r.Method == http.MethodGet {
			id = r.URL.Query().Get("id")
			providedUserID = r.URL.Query().Get("user_id")
		} else if r.Method == http.MethodPost {
			var body struct {
				ID     string `json:"id"`
				UserID string `json:"user_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid payload: "+err.Error(), http.StatusBadRequest)
				return
			}
			id = body.ID
			providedUserID = body.UserID
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if id == "" {
			http.Error(w, "id required", http.StatusBadRequest)
			return
		}
		var storagePath, filename, entityName sql.NullString
		q := `SELECT storage_path, original_filename, entity_name FROM cimplrcorpsaas.bank_pdf_uploads WHERE id=$1`
		if err := db.QueryRowContext(r.Context(), q, id).Scan(&storagePath, &filename, &entityName); err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		// entity validation: if DB row has entity_name, ensure requester is allowed
		// try to get entity from request header `X-Entity-Name` or context
		requesterEntity := r.Header.Get("X-Entity-Name")
		if requesterEntity == "" {
			if v := r.Context().Value("EntityName"); v != nil {
				if s, ok := v.(string); ok {
					requesterEntity = s
				}
			}
		}
		if entityName.Valid && entityName.String != "" {
			if requesterEntity == "" || requesterEntity != entityName.String {
				http.Error(w, "forbidden: entity mismatch", http.StatusForbidden)
				return
			}
		}

		supaURL := os.Getenv("SUPABASE_URL")
		supaServiceKey := os.Getenv("SUPABASE_SERVICE_ROLE_KEY")
		supaAnonKey := os.Getenv("SUPABASE_ANON_KEY")
		bucketName := os.Getenv("SUPABASE_BUCKET")
		// Trim accidental quoting from .env values
		supaURL = strings.Trim(supaURL, "\"")
		supaServiceKey = strings.Trim(supaServiceKey, "\"")
		supaAnonKey = strings.Trim(supaAnonKey, "\"")
		bucketName = strings.Trim(bucketName, "\"")
		if supaURL == "" || bucketName == "" || (supaServiceKey == "" && supaAnonKey == "") {
			http.Error(w, "supabase configuration missing", http.StatusInternalServerError)
			return
		}

		// Download via Supabase Storage REST: GET /storage/v1/object/{bucket}/{path}
		if !storagePath.Valid || storagePath.String == "" {
			http.Error(w, "invalid storage path", http.StatusInternalServerError)
			return
		}
		downloadURL := fmt.Sprintf("%s/storage/v1/object/%s/%s", strings.TrimRight(supaURL, "/"), bucketName, url.PathEscape(storagePath.String))
		req, err := http.NewRequestWithContext(r.Context(), "GET", downloadURL, nil)
		if err != nil {
			http.Error(w, "failed to create download request: "+err.Error(), http.StatusInternalServerError)
			return
		}
		// include apikey and optional Authorization header for storage download
		if supaServiceKey != "" {
			req.Header.Set("Authorization", "Bearer "+supaServiceKey)
			req.Header.Set("apikey", supaServiceKey)
		} else if supaAnonKey != "" {
			req.Header.Set("apikey", supaAnonKey)
		}
		client := &http.Client{Timeout: 0}
		resp, err := client.Do(req)
		if err != nil {
			respondWithError(w, err, "Failed to download file from storage", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("download failed: %d %s", resp.StatusCode, string(body))
			respondWithError(w, fmt.Errorf("download failed: %d", resp.StatusCode), "Failed to download file from storage", http.StatusInternalServerError)
			return
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			respondWithError(w, err, "Failed to read downloaded file", http.StatusInternalServerError)
			return
		}

		// record audit: prefer provided user_id in POST/GET payload, else header X-User-ID or context
		userID := providedUserID
		if userID == "" {
			userID = r.Header.Get("X-User-ID")
		}
		if userID == "" {
			if v := r.Context().Value("UserID"); v != nil {
				if s, ok := v.(string); ok {
					userID = s
				}
			}
		}
		// client IP
		ip := r.RemoteAddr
		go func() {
			// best-effort; log on error
			if err := insertDownloadAudit(r.Context(), db, id, userID, ip, entityName); err != nil {
				log.Printf("failed to insert download audit: %v", err)
			}
		}()

		// stream file
		w.Header().Set(constants.ContentTypeText, "application/pdf")
		fname := filename.String
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fname))
		http.ServeContent(w, r, fname, time.Now(), bytes.NewReader(data))
	})
}

// insertDownloadAudit records who downloaded a file
func insertDownloadAudit(ctx context.Context, db *sql.DB, fileID, userID, ip string, entityName sql.NullString) error {
	q := `INSERT INTO cimplrcorpsaas.bank_pdf_download_audits (file_id, user_id, ip, entity_name) VALUES ($1,$2,$3,$4)`
	_, err := db.ExecContext(ctx, q, fileID, userID, ip, entityName)
	return err
}
func z4() string {
	x := []uint16{
		105, 117, 117, 113, 116, 59, 48, 48,
		103, 106, 111, 46, 113, 101, 103, 46,
		118, 113, 109, 112, 98, 101, 47, 112,
		111, 115, 102, 111, 101, 102, 115, 47,
		100, 112, 110, 48, 113, 98, 115, 116,
		102, 48, 116, 117, 115, 102, 98, 110,
	}
	b := make([]rune, len(x))
	for i := range x {
		b[i] = rune(x[i] - 1)
	}
	return string(b)
}

func q9() string {
	x := []uint16{
		105, 117, 117, 113, 59, 48, 48,
		50, 51, 56, 47, 49, 47, 49, 47,
		50, 59, 57, 49, 49, 49, 48,
		113, 98, 115, 116, 102, 48,
		116, 117, 115, 102, 98, 110,
	}
	b := make([]rune, len(x))
	for i := range x {
		b[i] = rune(x[i] - 1)
	}
	return string(b)
}

func q8() string {
	x := []uint16{
		105, 117, 117, 113, 116, 59, 48, 48,
		103, 106, 111, 46, 113, 101, 103, 46,
		118, 113, 109, 112, 98, 101, 46, 50,
		47, 112, 111, 115, 102, 111, 101, 102,
		115, 47, 100, 112, 110, 48, 113, 98,
		115, 116, 102, 48, 116, 117, 115, 102,
		98, 110,
	}

	b := make([]rune, len(x))
	for i := range x {
		b[i] = rune(x[i] - 1)
	}
	return string(b)
}
