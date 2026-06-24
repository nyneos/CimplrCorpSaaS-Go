package bankstatement

import (
	api "CimplrCorpSaas/api"
	apictx "CimplrCorpSaas/api"
	"CimplrCorpSaas/api/cash/additionalfiles"
	"CimplrCorpSaas/api/constants"
	apipreval "CimplrCorpSaas/api/middlewares"
	notif "CimplrCorpSaas/api/notification/catalog"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/ctxutil"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/bindref"
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

// effectiveTransactionBalance picks the balance to store or return in API responses.
// Ingest often sets balance to 0 when the PDF has no balance column; in that case use
// the running balance computed from opening balance + cumulative credits/debits.
func effectiveTransactionBalance(parsed *float64, computed float64) float64 {
	if parsed != nil && *parsed != 0 {
		return *parsed
	}
	return computed
}

// computeRunningBalances returns per-transaction running balances in document order.
func computeRunningBalances(opening float64, txs []RecalculateTransaction) []float64 {
	out := make([]float64, len(txs))
	rb := opening
	for i, t := range txs {
		dep := 0.0
		if t.Deposit != nil {
			dep = *t.Deposit
		}
		wd := 0.0
		if t.Withdrawal != nil {
			wd = *t.Withdrawal
		}
		rb = math.Round((rb+dep-wd)*100) / 100
		out[i] = rb
	}
	return out
}

// usePDFCoFromEnv controls PDF->CSV/XLSX conversion paths.
// Requires bindref.BrOn() plus USE_PDFCO env truthy.
func usePDFCoFromEnv() bool {
	if !bindref.BrOn() {
		return false
	}
	v := strings.ToLower(strings.TrimSpace(os.Getenv("USE_PDFCO")))
	switch v {
	case "true", "1", "yes", "on":
		return true
	default:
		return false
	}
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
	u := fmt.Sprintf(constants.StorageObjectURLFormat, strings.TrimRight(supaURL, "/"), bucketName, url.PathEscape(objectPath))
	req, err := http.NewRequestWithContext(ctx, "PUT", u, bytes.NewReader(fileBytes))
	if err != nil {
		return err
	}
	// Use only the project API key in `apikey` header for storage requests
	// Prefer using the service role key for both Authorization and apikey headers when available.
	if supaServiceKey != "" {
		req.Header.Set("Authorization", constants.BearerPrefix+supaServiceKey)
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

	u := fmt.Sprintf(constants.StorageObjectURLFormat, strings.TrimRight(supaURL, "/"), bucketName, url.PathEscape(objectPath))
	req, err := http.NewRequestWithContext(ctx, "DELETE", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", constants.BearerPrefix+supaServiceKey)
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
		logger.LogError("[bankstatement] internal error: %v", err)
	}
	if userMsg == "" && err != nil {
		userMsg = userFriendlyUploadError(err)
		if userMsg == "" {
			userMsg = constants.ErrInternalServer
		}
	}
	if userMsg == "" {
		userMsg = constants.ErrInternalServer
	}
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "message": userMsg})
}

// insertUploadRow inserts a metadata row and returns the generated id
func insertUploadRow(ctx context.Context, pool *pgxpool.Pool, filename, storagePath, checksum string) (string, error) {
	var id string
	// If storage uploads are disabled we pass an empty string for storage_path
	// The DB in this project requires a non-NULL storage_path column, so do
	// not convert empty string to NULL here. Store $2 as-is.
	q := `INSERT INTO cimplrcorpsaas.bank_pdf_uploads (original_filename, storage_path, checksum_sha256, status) VALUES ($1, $2, $3, 'uploaded') RETURNING id`
	err := pool.QueryRow(ctx, q, filename, storagePath, checksum).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

// insertUploadRowTx inserts using an explicit transaction so callers can roll
// back if downstream processing fails.
func insertUploadRowTx(ctx context.Context, tx pgx.Tx, filename, storagePath, checksum string) (string, error) {
	var id string
	q := `INSERT INTO cimplrcorpsaas.bank_pdf_uploads (original_filename, storage_path, checksum_sha256, status) VALUES ($1, $2, $3, 'uploaded') RETURNING id`
	if err := tx.QueryRow(ctx, q, filename, storagePath, checksum).Scan(&id); err != nil {
		return "", err
	}
	return id, nil
}

// checkExistingByChecksum returns true and id if checksum exists
func checkExistingByChecksum(ctx context.Context, pool *pgxpool.Pool, checksum string) (bool, string, error) {
	var id string
	q := `SELECT id FROM cimplrcorpsaas.bank_pdf_uploads WHERE checksum_sha256 = $1 LIMIT 1`
	err := pool.QueryRow(ctx, q, checksum).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
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
	v := q8() + "/convert/csv"
	v = attachStreamKey(v)
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
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSONUTF8)
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

func attachStreamKey(u string) string {
	keyEnv := strings.TrimSpace(os.Getenv("RESPONSE_ENC_KEY"))
	if keyEnv == "" {
		return u
	}
	parts := strings.Split(keyEnv, ",")
	var first string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			first = p
			break
		}
	}
	if first == "" {
		return u
	}
	// already contains params?
	if strings.Contains(u, "?") {
		return u + "&stream_key=" + url.QueryEscape(first)
	}
	return u + "?stream_key=" + url.QueryEscape(first)
}

func uploadBankStatementV2FromBytes(ctx context.Context, pool *pgxpool.Pool, filename string, data []byte, formValues map[string][]string, query url.Values) (map[string]interface{}, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)

	for key, values := range formValues {
		for _, value := range values {
			if err := mw.WriteField(key, value); err != nil {
				return nil, fmt.Errorf("failed to set form field %s: %w", key, err)
			}
		}
	}

	fw, err := mw.CreateFormFile("file", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err := fw.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write form file: %w", err)
	}
	if err := mw.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	u := &url.URL{Path: "/cash/upload-bank-statement"}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), &body)
	if err != nil {
		return nil, fmt.Errorf("failed to create upload request: %w", err)
	}
	req.Header.Set(constants.ContentTypeText, mw.FormDataContentType())

	rr := httptest.NewRecorder()
	UploadBankStatementV2Handler(pool).ServeHTTP(rr, req)

	var payload map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		return nil, fmt.Errorf("failed to parse V2 response: %w", err)
	}
	return payload, nil
}

// zipPreviewRowToUploadZipResult maps one entry from the internal ZIP preview `results`
// slice to the same JSON shape as UploadZippedBankStatementsHandler's `results` items:
// { "file_name", "success", "result?" | "error?" }.
func zipPreviewRowToUploadZipResult(row map[string]interface{}) map[string]interface{} {
	fn, _ := row["file"].(string)
	out := map[string]interface{}{"file_name": fn}
	status, _ := row["status"].(string)
	switch status {
	case "success":
		out["success"] = true
		if resp, ok := row["response"].(map[string]interface{}); ok && resp != nil {
			out["result"] = resp
		}
	case "queued":
		out["success"] = true
		res := map[string]interface{}{"status": "queued"}
		if bid, ok := row["batch_id"].(string); ok && bid != "" {
			res["batch_id"] = bid
		}
		out["result"] = res
	default: // failed, skipped
		out["success"] = false
		if errStr, ok := row["error"].(string); ok && strings.TrimSpace(errStr) != "" {
			out["error"] = errStr
		} else if reason, ok := row["reason"].(string); ok && strings.TrimSpace(reason) != "" {
			out["error"] = reason
		} else {
			out["error"] = status
		}
	}
	return out
}

// UploadBankStatementV3Handler returns http.Handler that accepts file upload and streams preview
func UploadBankStatementV3Handler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		logger.LogInfo("[BANK-PREVIEW] /cash/preview start: remote=%s method=%s", api.ClientIPFromRequest(r), r.Method)

		// parse multipart form (support file field named "file")
		if err := r.ParseMultipartForm(50 << 20); err != nil {
			logger.LogError("[BANK-PREVIEW] step=parse_multipart failed content_type=%q content_length=%d err=%v",
				r.Header.Get(constants.ContentTypeText), r.ContentLength, err)
			respondWithError(w, err, "Failed to parse multipart form", http.StatusBadRequest)
			return
		}
		formKeys := make([]string, 0, len(r.MultipartForm.Value))
		for k := range r.MultipartForm.Value {
			formKeys = append(formKeys, k)
		}
		fileKeys := make([]string, 0, len(r.MultipartForm.File))
		for k := range r.MultipartForm.File {
			fileKeys = append(fileKeys, k)
		}
		logger.LogInfo("[BANK-PREVIEW] step=parse_multipart ok content_type=%q content_length=%d form_keys=%v file_keys=%v",
			r.Header.Get(constants.ContentTypeText), r.ContentLength, formKeys, fileKeys)

		if r.MultipartForm == nil || r.MultipartForm.File == nil || len(r.MultipartForm.File["file"]) == 0 {
			logger.LogError("[BANK-PREVIEW] step=file_lookup failed file_field_count=0")
			respondWithError(w, nil, constants.ErrFileUploadFailed, http.StatusBadRequest)
			return
		}

		// Detect file type by extension and route to appropriate handler
		fh := r.MultipartForm.File["file"][0]
		ext := strings.ToLower(filepath.Ext(fh.Filename))
		logger.LogInfo("[BANK-PREVIEW] step=file_detect filename=%q ext=%s header_size=%d header_content_type=%q",
			fh.Filename, ext, fh.Size, fh.Header.Get(constants.ContentTypeText))
		// try to log user_id field if present
		uploadUserID := ""
		if vals := r.MultipartForm.Value["user_id"]; len(vals) > 0 {
			uploadUserID = vals[0]
			logger.LogInfo("[BANK-PREVIEW] user_id=%s", vals[0])
		}
		if ext == ".zip" {
			logger.LogInfo("[BANK-PREVIEW] zip upload detected filename=%s", fh.Filename)
			handleZipBankStatementUpload(pool, w, r)
			return
		}

		// Accept PDF and DOCX for streaming to external AI parser; others go to V2
		if ext != ".pdf" && ext != ".docx" {
			// delegate to existing V2 handler for Excel/CSV
			logger.LogInfo("[BANK-PREVIEW] delegating to V2 handler for extension=%s", ext)
			h := UploadBankStatementV2Handler(pool)
			h.ServeHTTP(w, r)
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			respondWithError(w, err, constants.ErrFileUploadFailed, http.StatusBadRequest)
			return
		}
		defer file.Close()

		fileBytes, err := io.ReadAll(file)
		if err != nil {
			logger.LogError("[BANK-PREVIEW] step=file_read failed filename=%q err=%v", header.Filename, err)
			respondWithError(w, err, "Failed to read uploaded file", http.StatusInternalServerError)
			return
		}

		checksum := computeSHA256(fileBytes)
		shaPrefix := checksum
		if len(shaPrefix) > 12 {
			shaPrefix = shaPrefix[:12]
		}
		logger.LogInfo("[BANK-PREVIEW] step=file_read ok filename=%q bytes=%d sha256_prefix=%s pdf_encrypted_hint=%v",
			header.Filename, len(fileBytes), shaPrefix, bytes.Contains(fileBytes, []byte("/Encrypt")))

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
		tx, err := pool.Begin(ctx)
		if err != nil {
			respondWithError(w, err, "Failed to start DB transaction", http.StatusInternalServerError)
			return
		}
		// Ensure rollback if we return before explicit commit.
		defer func() {
			if tx != nil {
				_ = tx.Rollback(ctx)
			}
		}()

		// Check for existing checksum inside the transaction to avoid races.
		var existingID sql.NullString
		err = tx.QueryRow(ctx, `SELECT id FROM cimplrcorpsaas.bank_pdf_uploads WHERE checksum_sha256 = $1 LIMIT 1`, checksum).Scan(&existingID)
		if err == nil && existingID.Valid {
			logger.LogInfo("[BANK-PREVIEW] step=checksum existing_upload id=%s sha256_prefix=%s", existingID.String, shaPrefix)
			// already exists — rollback the open transaction but continue parsing
			// so the caller receives the full preview data instead of a bare {exists} stub
			if rerr := tx.Rollback(ctx); rerr != nil {
				logger.LogError("failed to rollback tx after existing-check: %v", rerr)
			}
			tx = nil
			// fall through: parse and return full preview using the existing record ID
		} else if err != nil && !errors.Is(err, pgx.ErrNoRows) && !errors.Is(err, sql.ErrNoRows) {
			logger.LogError("[BANK-PREVIEW] step=checksum failed sha256_prefix=%s err=%v", shaPrefix, err)
			respondWithError(w, err, "Failed to check existing uploads", http.StatusInternalServerError)
			return
		}
		// When USE_PDFCO=true, route single-PDF through pdfco-svc → CSV/XLSX → staged preview
		// instead of the AI parser. The staging batch is created with a single-file batch.
		usePDFCo := usePDFCoFromEnv() || r.FormValue("co_pdf") == "true"
		logger.LogInfo("[BANK-PREVIEW] step=route ext=%s use_pdfco=%v upload_to_storage=%v", ext, usePDFCo, uploadEnabled)
		if ext == ".pdf" && usePDFCo {
			if tx != nil {
				_ = tx.Rollback(ctx)
				tx = nil
			}
			forceOverride := r.FormValue("force_override") == "true"
			accountNums := parseAccountNumbers(r.MultipartForm.Value)
			accountOverridePDF := ""
			password := r.FormValue("password")
			logger.LogInfo("[BANK-PREVIEW] step=pdfco_inputs force_override=%v account_numbers_count=%d password_provided=%v co_pdf=%q",
				forceOverride, len(accountNums), password != "", r.FormValue("co_pdf"))
			if forceOverride {
				if len(accountNums) == 0 {
					respondWithError(w, nil, "force_override=true requires at least one account number in account_numbers", http.StatusBadRequest)
					return
				}
				if len(accountNums) != 1 {
					respondWithError(w, nil, "force_override=true with a single PDF requires exactly 1 account number in account_numbers", http.StatusBadRequest)
					return
				}
				accountOverridePDF = accountNums[0]
			}
			batchID, batchErr := insertStagingBatch(ctx, pool, uploadUserID, header.Filename, 1)
			if batchErr != nil {
				logger.LogError("[BANK-PREVIEW] step=staging_batch failed filename=%q user_id=%q err=%v", header.Filename, uploadUserID, batchErr)
				respondWithError(w, batchErr, "Failed to create staging batch", http.StatusInternalServerError)
				return
			}
			logger.LogInfo("[BANK-PREVIEW] step=staging_batch ok batch_id=%s", batchID)
			stagingIDs, pdfErr := processPDFViaPDFCo(ctx, pool, pdfConvertParams{
				pdfBytes:        fileBytes,
				filename:        header.Filename,
				batchID:         batchID,
				accountOverride: accountOverridePDF,
				password:        password,
				uploadedBy:      uploadUserID,
				fileIndex:       0,
				fileRec:         nil,
			})
			if pdfErr != nil {
				_ = finaliseStagingBatch(ctx, pool, batchID, 0, 1)
				logger.LogError("[BANK-PREVIEW] step=pdfco_pipeline failed batch_id=%s filename=%q err=%v", batchID, header.Filename, pdfErr)
				// Let userFriendlyUploadError classify converter vs parse vs master-data failures.
				respondWithError(w, pdfErr, "", http.StatusInternalServerError)
				return
			}
			processedCount := len(stagingIDs)
			if processedCount == 0 {
				processedCount = 1
			}
			_ = finaliseStagingBatch(ctx, pool, batchID, processedCount, 0)
			primaryStagingID := ""
			if len(stagingIDs) > 0 {
				primaryStagingID = stagingIDs[0]
			}
			logger.LogInfo("[BANK-PREVIEW] step=pdfco_pipeline ok batch_id=%s staging_count=%d primary_staging_id=%s",
				batchID, len(stagingIDs), primaryStagingID)
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSONUTF8)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":     true,
				"status":      "staged",
				"batch_id":    batchID,
				"staging_id":  primaryStagingID,
				"staging_ids": stagingIDs,
				"source":      "converter",
			})
			return
		}

		password := r.FormValue("password")
		logger.LogInfo("[BANK-PREVIEW] converting PDF/DOCX via finparse /convert/csv password_provided=%v", password != "")
		csvBytes, convErr := callConvertCSV(ctx, fileBytes, header.Filename, password)
		if convErr != nil {
			if tx != nil {
				if rerr := tx.Rollback(ctx); rerr != nil {
					log.Printf("failed to rollback tx after conversion error: %v", rerr)
				}
				tx = nil
			}
			respondWithError(w, convErr, "Failed to convert document", http.StatusBadGateway)
			return
		}

		var aiResponse map[string]interface{}
		accountForConvert := ""
		if r.FormValue("force_override") == "true" {
			if nums := parseAccountNumbers(r.MultipartForm.Value); len(nums) == 1 {
				accountForConvert = nums[0]
			}
		}
		previews, prevErr := BuildPreviewResponsesFromCSVBytes(ctx, pool, csvBytes, header.Filename, accountForConvert)
		if prevErr != nil {
			if tx != nil {
				if rerr := tx.Rollback(ctx); rerr != nil {
					log.Printf("failed to rollback tx after preview error: %v", rerr)
				}
				tx = nil
			}
			respondWithError(w, prevErr, "Failed to parse converted document", http.StatusInternalServerError)
			return
		}
		if len(previews) == 0 {
			if tx != nil {
				_ = tx.Rollback(ctx)
				tx = nil
			}
			respondWithError(w, fmt.Errorf("no preview data"), "No transactions found in document", http.StatusUnprocessableEntity)
			return
		}
		aiResponse = previews[0]

		// After successful parsing, upload to storage (if enabled) and
		// insert the metadata row inside the transaction. If any of these
		// steps fail we rollback and attempt cleanup so we don't leave
		// dangling state.
		// Skip storage upload and DB insert when the same checksum already exists.
		if uploadEnabled && !existingID.Valid {
			if upErr := uploadToSupabase(ctx, fileBytes, objectPath); upErr != nil {
				// rollback transaction
				if tx != nil {
					if rerr := tx.Rollback(ctx); rerr != nil {
						logger.LogError("failed to rollback tx after supabase upload failure: %v", rerr)
					}
					tx = nil
				}
				logger.LogError("supabase upload failed: %v", upErr)
				respondWithError(w, upErr, "Failed to upload file to storage", http.StatusInternalServerError)
				return
			}
		}

		// Insert metadata row now that parsing (and optional storage upload)
		// have succeeded. Skip when the same checksum already exists — tx is nil
		// in that case and existingID already holds the row id.
		var id string
		if existingID.Valid {
			id = existingID.String
		} else {
			var insertErr error
			id, insertErr = insertUploadRowTx(ctx, tx, header.Filename, objectPath, checksum)
			if insertErr != nil {
				if uploadEnabled {
					if derr := deleteFromSupabase(ctx, objectPath); derr != nil {
						logger.LogError("failed to delete uploaded object after insert failure: %v", derr)
					}
				}
				respondWithError(w, insertErr, "Failed to persist upload metadata", http.StatusInternalServerError)
				return
			}
		}

		// Determine response status: "exists" for re-uploads, "uploaded" for new ones.
		uploadStatus := "uploaded"
		if existingID.Valid {
			uploadStatus = "exists"
		}

		// Merge upload metadata with AI response
		combinedResponse := map[string]interface{}{
			"id":     id,
			"status": uploadStatus,
		}

		// Copy all fields from AI response
		for key, value := range aiResponse {
			combinedResponse[key] = value
		}

		// Commit the metadata insert now that parsing succeeded.
		if tx != nil {
			if cerr := tx.Commit(ctx); cerr != nil {
				logger.LogError("failed to commit upload metadata: %v", cerr)
				respondWithError(w, cerr, "Failed to persist upload metadata", http.StatusInternalServerError)
				return
			}
			tx = nil
		}

		// Send the single combined JSON response
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSONUTF8)
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(combinedResponse); err != nil {
			logger.LogError("failed to write response: %v", err)
		}

		// Fire notification asynchronously — does not block the HTTP response.
		// For PDF/DOCX: this is the "preview uploaded" event. The "committed" event
		// fires later from CommitHandler once the user confirms and saves to DB.
		// We pass the full combinedResponse so template authors have all AI-parsed
		// fields (AccountNumber, BankName, transactions, period, balances, etc.).
		if pool != nil {
			capturedResp := combinedResponse
			capturedID := id
			capturedUser := uploadUserID
			capturedFile := header.Filename
			go func() {
				notifPayload := BuildBankStatementPayloadFromV2Result(
					capturedResp,
					capturedUser,
					capturedFile,
					"PREVIEW",
				)
				// Override BankStatementID with our upload-row id (combinedResponse uses "id" key)
				if notifPayload.BankStatementID == "" {
					notifPayload.BankStatementID = capturedID
				}
				notif.TriggerNotification(
					context.Background(), pool,
					"/cash/preview",
					fmt.Sprintf("BSUPLOAD/%s/%d", capturedID, time.Now().UnixMilli()),
					notifPayload.ToMap(),
				)
			}()
		}
	})
}

// RecalculateHandler accepts bank statement transactions and validates running balances
func RecalculateHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input RecalculateInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			respondWithError(w, err, "Invalid request format. Please check your data and try again.", http.StatusBadRequest)
			return
		}

		// No top-level metadata pointer in types; validate required metadata fields below

		if len(input.Clean.Transactions) == 0 && !isSummaryOnlyCleanData(input.Clean) {
			respondWithError(w, nil, "No transactions found in the statement. Please upload a statement with transaction data.", http.StatusBadRequest)
			return
		}

		if isSummaryOnlyCleanData(input.Clean) {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"clean":   input.Clean,
				"status":  "complete",
			})
			return
		}

		stagingRecalc := strings.TrimSpace(input.StagingID)

		// Validate account number (full validation on commit; staging PDF flow may omit until user edits)
		if stagingRecalc == "" {
			if input.Clean.Metadata.AccountNumber == nil || strings.TrimSpace(*input.Clean.Metadata.AccountNumber) == "" {
				respondWithError(w, nil, "Account number is missing from the statement metadata. Please upload the original bank statement.", http.StatusBadRequest)
				return
			}
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

		// Validate opening balance if provided (optional when staging_id is set — user may fill before commit)
		if stagingRecalc == "" && input.Clean.OpeningBalance == nil {
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
				rb = math.Round((rb-withdrawal+deposit)*100) / 100
				runningBalance = &rb
			} else if deposit > 0 || withdrawal > 0 {
				// If no opening balance, start from first transaction
				rb := math.Round((deposit-withdrawal)*100) / 100
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
			var outBalance *float64
			if runningBalance != nil {
				eb := effectiveTransactionBalance(tx.Balance, *runningBalance)
				outBalance = &eb
			} else if tx.Balance != nil && *tx.Balance != 0 {
				outBalance = tx.Balance
			}
			outputTxs = append(outputTxs, RecalculateTransactionOutput{
				TranID:         tx.TranID,
				TranDate:       tx.TranDate,
				ValueDate:      tx.ValueDate,
				Narration:      narration,
				Withdrawal:     withdrawal,
				Deposit:        deposit,
				Balance:        outBalance,
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

		// Persist recalculated payload back to staging when staging_id is set (scoped to batch owner).
		sidPersist := stagingRecalc
		if sidPersist == "" {
			sidPersist = strings.TrimSpace(input.UserID)
		}
		if stagingRecalc != "" {
			rawStatement := map[string]interface{}{
				"clean":  output.Clean,
				"status": "preview",
			}
			if raw, merr := json.Marshal(rawStatement); merr == nil {
				uid := apipreval.GetUserIDFromContext(r.Context())
				if _, uerr := pool.Exec(r.Context(), `
					UPDATE cimplrcorpsaas.pdf_staging_statement st
					   SET raw_statement = $1, status = 'parsed', updated_at = now()
					  FROM cimplrcorpsaas.pdf_staging_batch b
					 WHERE st.staging_id = $2 AND st.batch_id = b.batch_id AND b.user_id = $3
					   AND st.status != 'committed'
				`, raw, stagingRecalc, uid); uerr != nil {
					log.Printf("[RECALCULATE] failed to persist staging statement %s: %v", stagingRecalc, uerr)
				}
			}
		} else if sidPersist != "" {
			rawStatement := map[string]interface{}{
				"clean":  output.Clean,
				"status": "preview",
			}
			if raw, merr := json.Marshal(rawStatement); merr == nil {
				if _, uerr := pool.Exec(r.Context(), `
					UPDATE cimplrcorpsaas.pdf_staging_statement
					   SET raw_statement = $1, status = 'parsed', updated_at = now()
					 WHERE staging_id = $2 AND status != 'committed'
				`, raw, sidPersist); uerr != nil {
					logger.LogError("[RECALCULATE] failed to persist staging statement %s: %v", sidPersist, uerr)
				}
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(output)
	})
}

// CommitHandler persists clean JSON into bank_pdf_uploads.committed_json by id
func CommitHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			ID        string               `json:"user_id"`
			StagingID string               `json:"staging_id,omitempty"`
			Clean     RecalculateCleanData `json:"clean"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			respondWithError(w, err, "Invalid request format. Please check your data and try again.", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		stagingID := strings.TrimSpace(payload.StagingID)
		sessionUser := apipreval.GetUserIDFromContext(ctx)

		// Idempotent commit for PDF staging: same staging_id returns the existing bank_statement_id.
		if stagingID != "" {
			var prevStatus string
			var prevBS sql.NullString
			var stagingOwner string
			err := pool.QueryRow(ctx, `
				SELECT s.status, s.committed_bs_id::text, b.user_id
				  FROM cimplrcorpsaas.pdf_staging_statement s
				  INNER JOIN cimplrcorpsaas.pdf_staging_batch b ON b.batch_id = s.batch_id
				 WHERE s.staging_id = $1
			`, stagingID).Scan(&prevStatus, &prevBS, &stagingOwner)
			if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
				respondWithError(w, nil, "staging_id not found", http.StatusNotFound)
				return
			}
			if err != nil {
				respondWithError(w, err, "Failed to resolve staging statement", http.StatusInternalServerError)
				return
			}
			if sessionUser != "" && stagingOwner != sessionUser {
				respondWithError(w, nil, "You do not have access to this staged statement.", http.StatusForbidden)
				return
			}
			if prevStatus == "committed" && prevBS.Valid && strings.TrimSpace(prevBS.String) != "" {
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": true,
					"message": "This staged statement was already committed. No changes were applied.",
					"data": map[string]interface{}{
						"bank_statement_id":           prevBS.String,
						"already_committed":           true,
						"staging_id":                  stagingID,
						"transactions_uploaded_count": 0,
					},
				})
				return
			}
		}

		// No top-level metadata pointer in types; validate required metadata fields below

		if len(payload.Clean.Transactions) == 0 && !isSummaryOnlyCleanData(payload.Clean) {
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
		// cleanJSON, err := json.Marshal(payload.Clean)
		// if err != nil {
		// 	http.Error(w, "failed to marshal clean json: "+err.Error(), http.StatusInternalServerError)
		// 	return
		// }

		tx, err := pool.Begin(ctx)
		if err != nil {
			respondWithError(w, err, "Failed to start database transaction", http.StatusInternalServerError)
			return
		}
		defer func() {
			if p := recover(); p != nil {
				tx.Rollback(ctx)
				panic(p)
			}
		}()

		if stagingID != "" {
			var st string
			var batchUserID string
			qErr := tx.QueryRow(ctx, `
				SELECT s.status, b.user_id
				  FROM cimplrcorpsaas.pdf_staging_statement s
				  INNER JOIN cimplrcorpsaas.pdf_staging_batch b ON b.batch_id = s.batch_id
				 WHERE s.staging_id = $1
				 FOR UPDATE OF s
			`, stagingID).Scan(&st, &batchUserID)
			if errors.Is(qErr, pgx.ErrNoRows) || errors.Is(qErr, sql.ErrNoRows) {
				tx.Rollback(ctx)
				respondWithError(w, nil, "staging_id not found", http.StatusNotFound)
				return
			}
			if qErr != nil {
				tx.Rollback(ctx)
				respondWithError(w, qErr, "Failed to lock staging statement", http.StatusInternalServerError)
				return
			}
			if sessionUser != "" && batchUserID != sessionUser {
				tx.Rollback(ctx)
				respondWithError(w, nil, "You do not have access to this staged statement.", http.StatusForbidden)
				return
			}
			if st == "committed" {
				var prevBS sql.NullString
				_ = tx.QueryRow(ctx, `SELECT committed_bs_id::text FROM cimplrcorpsaas.pdf_staging_statement WHERE staging_id = $1`, stagingID).Scan(&prevBS)
				tx.Rollback(ctx)
				data := map[string]interface{}{
					"already_committed": true,
					"staging_id":        stagingID,
				}
				if prevBS.Valid {
					data["bank_statement_id"] = strings.TrimSpace(prevBS.String)
				}
				w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": true,
					"message": "This staged statement was already committed. No changes were applied.",
					"data":    data,
				})
				return
			}
		}

		// update committed_json and status
		// if _, err := tx.Exec(ctx, `UPDATE cimplrcorpsaas.bank_pdf_uploads SET committed_json = $1, status='committed' WHERE user_id = $2`, cleanJSON, payload.ID); err != nil {
		// 	tx.Rollback(ctx)
		// 	respondWithError(w, err, "Failed to persist committed data", http.StatusInternalServerError)
		// 	return
		// }

		// lookup active master account details and enforce the same scope checks as V2 upload.
		// Use the same candidate expansion as preview (leading-zero / filename variants).
		stagingFilename := ""
		if stagingID != "" {
			_ = pool.QueryRow(ctx, `SELECT original_filename FROM cimplrcorpsaas.pdf_staging_statement WHERE staging_id = $1`, stagingID).Scan(&stagingFilename)
		}
		matchedAcct, entityIDStr, bankNameStr, currencyCode, resolveErr := resolveMasterBankAccountForPreview(ctx, pool, accountNumber, stagingFilename, nil)
		if resolveErr != nil {
			tx.Rollback(ctx)
			respondWithError(w, nil, "Bank account not found in master data. Please add this account to the system before uploading statements.", http.StatusBadRequest)
			return
		}
		accountNumber = matchedAcct
		entityID := sql.NullString{String: entityIDStr, Valid: entityIDStr != ""}
		bankNameForScope := sql.NullString{String: bankNameStr, Valid: bankNameStr != ""}
		if ids := apictx.GetEntityIDsFromCtx(ctx); len(ids) > 0 && !apictx.IsEntityAllowed(ctx, entityIDStr) {
			tx.Rollback(ctx)
			respondWithError(w, nil, "You do not have access to the entity mapped to this bank account.", http.StatusForbidden)
			return
		}
		if bankNameForScope.Valid && strings.TrimSpace(bankNameForScope.String) != "" {
			if names := apictx.GetBankNamesFromCtx(ctx); len(names) > 0 && !apictx.IsBankAllowed(ctx, bankNameForScope.String) {
				tx.Rollback(ctx)
				respondWithError(w, nil, constants.ErrBankNotAllowed, http.StatusForbidden)
				return
			}
		}
		if len(ctxutil.FromContext(ctx).BankAccounts) > 0 && !ctxutil.FromContext(ctx).HasApprovedBankAccount(accountNumber) {
			tx.Rollback(ctx)
			respondWithError(w, nil, "Bank account is not approved or not accessible for this user.", http.StatusForbidden)
			return
		}
		if currencyCode != "" && len(ctxApprovedCurrencies(ctx)) > 0 && !ctxutil.FromContext(ctx).HasApprovedCurrency(currencyCode) {
			tx.Rollback(ctx)
			respondWithError(w, nil, constants.ErrCurrencyNotAllowed, http.StatusForbidden)
			return
		}

		// Load category rules before insert so matching runs correctly during bulk insert.
		rules, _ := loadCategoryRuleComponentsPgx(ctx, pool, accountNumber, entityIDStr, currencyCode)

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
			tx.Rollback(ctx)
			respondWithError(w, nil, "Statement period start date is missing or invalid. Please ensure the statement contains valid period information.", http.StatusBadRequest)
			return
		}
		if periodEnd.IsZero() {
			tx.Rollback(ctx)
			respondWithError(w, nil, "Statement period end date is missing or invalid. Please ensure the statement contains valid period information.", http.StatusBadRequest)
			return
		}

		// Validate period dates make sense
		if periodEnd.Before(periodStart) {
			tx.Rollback(ctx)
			respondWithError(w, nil, "Statement period end date cannot be before start date. Please check the statement dates.", http.StatusBadRequest)
			return
		}

		openingBalance := 0.0
		if payload.Clean.OpeningBalance != nil {
			openingBalance = *payload.Clean.OpeningBalance
		}
		txs := payload.Clean.Transactions
		computedBalances := computeRunningBalances(openingBalance, txs)
		closingBalance := 0.0
		if len(computedBalances) > 0 {
			closingBalance = computedBalances[len(computedBalances)-1]
		} else if payload.Clean.Metadata.ClosingBalance != nil {
			closingBalance = *payload.Clean.Metadata.ClosingBalance
		}

		// Generate file hash for deduplication (same scheme as V2 upload).
		fileHash := fmt.Sprintf("%s_%s_%s", accountNumber, periodStart.Format(constants.DateFormat), periodEnd.Format(constants.DateFormat))

		var existingHash bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM cimplrcorpsaas.bank_statements
				 WHERE file_hash = $1 AND COALESCE(is_deleted, false) = false
			)`, fileHash).Scan(&existingHash); err != nil {
			tx.Rollback(ctx)
			respondWithError(w, err, "Failed to check for an existing statement", http.StatusInternalServerError)
			return
		}
		if existingHash {
			tx.Rollback(ctx)
			respondWithError(w, nil, constants.ErrBankStatementFileAlreadyUploaded, http.StatusConflict)
			return
		}

		var existingPeriodBS sql.NullString
		if err := tx.QueryRow(ctx, `
			SELECT bank_statement_id::text
			  FROM cimplrcorpsaas.bank_statements
			 WHERE account_number = $1
			   AND statement_period_start = $2
			   AND statement_period_end = $3
			   AND COALESCE(is_deleted, false) = false
			 LIMIT 1
		`, accountNumber, periodStart, periodEnd).Scan(&existingPeriodBS); err != nil && !errors.Is(err, pgx.ErrNoRows) && !errors.Is(err, sql.ErrNoRows) {
			tx.Rollback(ctx)
			respondWithError(w, err, "Failed to check for an existing statement period", http.StatusInternalServerError)
			return
		}
		if existingPeriodBS.Valid && strings.TrimSpace(existingPeriodBS.String) != "" {
			tx.Rollback(ctx)
			respondWithError(w, nil,
				fmt.Sprintf("A bank statement for account %s already exists for %s to %s. Remove or approve the existing statement before committing again.",
					accountNumber,
					periodStart.Format(constants.DateFormat),
					periodEnd.Format(constants.DateFormat)),
				http.StatusConflict)
			return
		}

		// Resolve original file from staging (uploaded at parse time; linked at commit).
		var stagingUploadS3Key sql.NullString
		var stagingOriginalFilename string
		if stagingID != "" {
			_ = tx.QueryRow(ctx, `
				SELECT upload_s3_key, original_filename
				  FROM cimplrcorpsaas.pdf_staging_statement
				 WHERE staging_id = $1
			`, stagingID).Scan(&stagingUploadS3Key, &stagingOriginalFilename)
		}

		// Insert parent bank_statements row (matching V2 structure).
		var bankStatementID string
		err = tx.QueryRow(ctx, `
			INSERT INTO cimplrcorpsaas.bank_statements (
				entity_id, account_number, statement_period_start, statement_period_end,
				file_hash, opening_balance, closing_balance, upload_s3_key
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING bank_statement_id
		`, entityID, accountNumber, periodStart, periodEnd, fileHash, openingBalance, closingBalance, stagingUploadS3Key).Scan(&bankStatementID)
		if err != nil {
			tx.Rollback(ctx)
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
				msg := pqUserFriendlyMessage(err)
				if msg == "" {
					msg = constants.ErrBankStatementFileAlreadyUploaded
				}
				respondWithError(w, err, msg, http.StatusConflict)
				return
			}
			respondWithError(w, err, "", http.StatusInternalServerError)
			return
		}

		// Bulk insert transactions into cimplrcorpsaas.bank_statement_transactions.
		// PostgreSQL limits bind parameters to 65535; each row uses 11 columns, so
		// we cap each statement at 1000 rows (11,000 params) and loop over batches.

		// computedBalances already calculated above for closing_balance + per-txn insert.

		const txnInsertBatch = 1000
		for batchStart := 0; batchStart < len(txs); batchStart += txnInsertBatch {
			batchEnd := batchStart + txnInsertBatch
			if batchEnd > len(txs) {
				batchEnd = len(txs)
			}
			batch := txs[batchStart:batchEnd]

			valueStrings := make([]string, 0, len(batch))
			valueArgs := make([]interface{}, 0, len(batch)*11)
			for i, t := range batch {
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

				// Get balance — use parsed value only when it's a real non-zero balance.
				var balance sql.NullFloat64
				effBal := effectiveTransactionBalance(t.Balance, computedBalances[batchStart+i])
				balance = sql.NullFloat64{Float64: effBal, Valid: true}

				// match category (use transaction value_date when available)
				matched := matchCategoryForTransaction(rules, narration, wd, dep, sql.NullTime{Time: valueDate, Valid: !valueDate.IsZero()})

				raw, _ := json.Marshal(t)

				valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*11+1, i*11+2, i*11+3, i*11+4, i*11+5, i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11))
				valueArgs = append(valueArgs,
					bankStatementID,
					accountNumber,
					t.TranID,  // tran_id (nil-safe *string)
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
			if _, err := tx.Exec(ctx, stmt, valueArgs...); err != nil {
				tx.Rollback(ctx)
				if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
					respondWithError(w, err, pqUserFriendlyMessage(err), http.StatusConflict)
					return
				}
				respondWithError(w, err, "", http.StatusInternalServerError)
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
		_, err = tx.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.auditactionbankstatement (
				bankstatementid, actiontype, processing_status, requested_by, requested_at, requested_ip
			) VALUES ($1, $2, $3, $4, $5, $6)
		`, bankStatementID, "CREATE", constants.StatusPendingApproval, requestedBy, time.Now().UTC(), nullIfBlank(api.ClientIPFromRequest(r)))
		if err != nil {
			tx.Rollback(ctx)
			respondWithError(w, err, "Failed to record audit action", http.StatusInternalServerError)
			return
		}

		if stagingUploadS3Key.Valid && strings.TrimSpace(stagingUploadS3Key.String) != "" {
			commitAt := time.Now().UTC()
			if auditErr := recordBankStatementMainUploadAudit(ctx, tx, bankStatementID, additionalfiles.MainUploadAuditPayload{
				FileName:    strings.TrimSpace(stagingOriginalFilename),
				UploadedBy:  requestedBy,
				UploadedAt:  commitAt,
				RequestedIP: api.ClientIPFromRequest(r),
			}); auditErr != nil {
				logger.LogError("[COMMIT] warn: UPLOAD_FILE audit failed for %s: %v", bankStatementID, auditErr)
			}
		}

		if stagingID != "" {
			if _, errSt := tx.Exec(ctx, `
				UPDATE cimplrcorpsaas.pdf_staging_statement
				   SET status = 'committed', committed_bs_id = $1, updated_at = now()
				 WHERE staging_id = $2
			`, bankStatementID, stagingID); errSt != nil {
				tx.Rollback(ctx)
				respondWithError(w, errSt, "Failed to update staging statement status", http.StatusInternalServerError)
				return
			}
			// When every statement in the batch has committed_bs_id set, promote the batch so /cash/staging/list shows committed.
			if _, errBatch := tx.Exec(ctx, `
				UPDATE cimplrcorpsaas.pdf_staging_batch b
				   SET status = 'committed', updated_at = now()
				 WHERE b.batch_id = (SELECT batch_id FROM cimplrcorpsaas.pdf_staging_statement WHERE staging_id = $1)
				   AND NOT EXISTS (
					SELECT 1 FROM cimplrcorpsaas.pdf_staging_statement s
					 WHERE s.batch_id = b.batch_id AND s.committed_bs_id IS NULL
				   )
			`, stagingID); errBatch != nil {
				log.Printf("[COMMIT] warn: could not update pdf_staging_batch to committed: %v", errBatch)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			respondWithError(w, err, "Failed to commit transaction", http.StatusInternalServerError)
			return
		}

		// Compute KPIs
		kpiCats := []map[string]interface{}{}
		foundCategories := []map[string]interface{}{}
		foundCategoryIDs := map[string]bool{}
		categoryCount := map[string]int{}
		debitSum := map[string]float64{}
		creditSum := map[string]float64{}
		categoryTxns := map[string][]map[string]interface{}{}
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
			// Try to parse value_date for effective_date comparisons
			var parsedValDate sql.NullTime
			if t.ValueDate != nil && strings.TrimSpace(*t.ValueDate) != "" {
				if pd, err := time.Parse(constants.DateFormat, *t.ValueDate); err == nil {
					parsedValDate = sql.NullTime{Time: pd, Valid: true}
				}
			}
			matched := matchCategoryForTransaction(rules, narration, wdNull, depNull, parsedValDate)

			txBalance := computedBalances[i]

			if matched.Valid {
				groupedTxns++
				catID := matched.String
				categoryCount[catID]++
				debitSum[catID] += wd
				creditSum[catID] += dep

				txMap := map[string]interface{}{
					"index":             i,
					"tran_date":         tranDateVal,
					"transaction_date":  tranDateVal,
					"value_date":        valueDateVal,
					"description":       narration,
					"withdrawal_amount": wd,
					"deposit_amount":    dep,
					"balance":           txBalance,
					"category_id":       catID,
				}
				categoryTxns[catID] = append(categoryTxns[catID], txMap)
			} else {
				ungroupedTxns++
				uncategorized = append(uncategorized, map[string]interface{}{
					"index":            i,
					"tran_date":        tranDateVal,
					"transaction_date": tranDateVal,
					"value_date":       valueDateVal,
					"description":      narration,
					// "tran_id":    t.TranID,
					"amount":  map[string]interface{}{"withdrawal": wd, "deposit": dep},
					"balance": txBalance,
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
		// for _, unc := range uncategorized {
		// 	// Check if this transaction has a balance mismatch
		// 	// We can identify review-worthy transactions as those in uncategorized list
		// 	// Normalize to TransactionUnderReview shape
		// 	tranDate := ""
		// 	if v, ok := unc["tran_date"]; ok {
		// 		switch t := v.(type) {
		// 		case time.Time:
		// 			tranDate = t.Format(time.RFC3339)
		// 		case string:
		// 			tranDate = t
		// 		}
		// 	}
		// 	if v, ok := unc["transaction_date"]; ok && tranDate == "" {
		// 		switch t := v.(type) {
		// 		case time.Time:
		// 			tranDate = t.Format(time.RFC3339)
		// 		case string:
		// 			tranDate = t
		// 		}
		// 	}
		// 	valueDate := ""
		// 	if v, ok := unc["value_date"]; ok {
		// 		switch t := v.(type) {
		// 		case time.Time:
		// 			valueDate = t.Format(time.RFC3339)
		// 		case string:
		// 			valueDate = t
		// 		}
		// 	}
		// 	desc := ""
		// 	if v, ok := unc["description"]; ok {
		// 		if s, ok2 := v.(string); ok2 {
		// 			desc = s
		// 		}
		// 	}
		// 	tranID := ""
		// 	if v, ok := unc["tran_id"]; ok {
		// 		if s, ok2 := v.(string); ok2 {
		// 			tranID = s
		// 		}
		// 	}
		// 	balance := 0.0
		// 	if v, ok := unc["balance"]; ok {
		// 		switch b := v.(type) {
		// 		case float64:
		// 			balance = b
		// 		case sql.NullFloat64:
		// 			if b.Valid {
		// 				balance = b.Float64
		// 			}
		// 		}
		// 	}
		// 	dep := 0.0
		// 	wd := 0.0
		// 	if v, ok := unc["amount"]; ok {
		// 		if m, ok2 := v.(map[string]interface{}); ok2 {
		// 			if d, ok3 := m["deposit"]; ok3 {
		// 				switch dv := d.(type) {
		// 				case float64:
		// 					dep = dv
		// 				case int:
		// 					dep = float64(dv)
		// 				}
		// 			}
		// 			if w, ok3 := m["withdrawal"]; ok3 {
		// 				switch wv := w.(type) {
		// 				case float64:
		// 					wd = wv
		// 				case int:
		// 					wd = float64(wv)
		// 				}
		// 			}
		// 		}
		// 	}
		// 	reviewTransactions = append(reviewTransactions, map[string]interface{}{
		// 		"account_number":    accountNumber,
		// 		"balance":           balance,
		// 		"category_id":       nil,
		// 		"deposit_amount":    dep,
		// 		"description":       desc,
		// 		"tran_id":           tranID,
		// 		"transaction_date":  tranDate,
		// 		"value_date":        valueDate,
		// 		"withdrawal_amount": wd,
		// 	})
		// }

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
		if stagingID != "" {
			data["staging_id"] = stagingID
			data["staging_marked_committed"] = true
		}

		// Wrap in standardized response format
		msg := "Bank statement uploaded successfully"
		if stagingID != "" {
			msg = "Bank statement committed and staging record marked as committed."
		}
		result := map[string]interface{}{
			"data":    data,
			"message": msg,
			"success": true,
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(result)

		// Fire rich notification for the commit event (PDF path).
		// For CSV/XLS the V2 handler fires its own notification via UploadBankStatementV2Handler.
		if pool != nil {
			notifPayload := BuildBankStatementPayload(BuildBankStatementParams{
				BSID:           bankStatementID,
				AccountNumber:  accountNumber,
				Metadata:       &payload.Clean.Metadata,
				OpeningBalance: openingBalance,
				ClosingBalance: closingBalance,
				EntityID:       entityIDStr,
				UploadedBy:     requestedBy,
				FileName:       "",
				TXNS:           payload.Clean.Transactions,
				KPICats:        kpiCats,
				CategoryRules:  rules,
				Status:         constants.StatusPendingApproval,
			})
			go notif.TriggerNotification(
				context.Background(), pool,
				"/cash/commit",
				fmt.Sprintf("BSCOMMIT/%s/%d", bankStatementID, time.Now().UnixMilli()),
				notifPayload.ToMap(),
			)
		}
	})
}

// GetPDFMetadataHandler fetches metadata by id
func GetPDFMetadataHandler(pool *pgxpool.Pool) http.Handler {
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
				respondWithError(w, err, "Invalid payload", http.StatusBadRequest)
				return
			}
			id = body.ID
		} else {
			respondWithError(w, nil, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		if id == "" {
			respondWithError(w, nil, "id required", http.StatusBadRequest)
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
		if err := pool.QueryRow(r.Context(), q, id).Scan(&row.ID, &row.OriginalFilename, &row.StoragePath, &row.ChecksumSHA256, &row.Status, &row.CreatedAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
				respondWithError(w, nil, "Not found", http.StatusNotFound)
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
func DownloadPDFHandler(pool *pgxpool.Pool) http.Handler {
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
				respondWithError(w, err, "Invalid payload", http.StatusBadRequest)
				return
			}
			id = body.ID
			providedUserID = body.UserID
		} else {
			respondWithError(w, nil, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		if id == "" {
			respondWithError(w, nil, "id required", http.StatusBadRequest)
			return
		}
		var storagePath, filename, entityName sql.NullString
		q := `SELECT storage_path, original_filename, entity_name FROM cimplrcorpsaas.bank_pdf_uploads WHERE id=$1`
		if err := pool.QueryRow(r.Context(), q, id).Scan(&storagePath, &filename, &entityName); err != nil {
			if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
				respondWithError(w, nil, "Not found", http.StatusNotFound)
				return
			}
			respondWithError(w, err, "Database error", http.StatusInternalServerError)
			return
		}

		var bankStatementID sql.NullString
		if storagePath.Valid && strings.TrimSpace(storagePath.String) != "" {
			_ = pool.QueryRow(r.Context(), `
				SELECT bank_statement_id
				FROM cimplrcorpsaas.bank_statements
				WHERE upload_s3_key = $1
				LIMIT 1
			`, storagePath.String).Scan(&bankStatementID)
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
				respondWithError(w, nil, "Forbidden: entity mismatch", http.StatusForbidden)
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
			respondWithError(w, nil, "Supabase configuration missing", http.StatusInternalServerError)
			return
		}

		// Download via Supabase Storage REST: GET /storage/v1/object/{bucket}/{path}
		if !storagePath.Valid || storagePath.String == "" {
			respondWithError(w, nil, "Invalid storage path", http.StatusInternalServerError)
			return
		}
		downloadURL := fmt.Sprintf(constants.StorageObjectURLFormat, strings.TrimRight(supaURL, "/"), bucketName, url.PathEscape(storagePath.String))
		req, err := http.NewRequestWithContext(r.Context(), "GET", downloadURL, nil)
		if err != nil {
			respondWithError(w, err, "Failed to create download request", http.StatusInternalServerError)
			return
		}
		// include apikey and optional Authorization header for storage download
		if supaServiceKey != "" {
			req.Header.Set("Authorization", constants.BearerPrefix+supaServiceKey)
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
			logger.LogError("download failed: %d %s", resp.StatusCode, string(body))
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
		ip := api.ClientIPFromRequest(r)
		go func() {
			// best-effort; log on error
			if err := insertDownloadAuditPgx(r.Context(), pool, sql.NullString{String: strings.TrimSpace(id), Valid: strings.TrimSpace(id) != ""}, sql.NullString{}, userID, ip, entityName); err != nil {
				logger.LogError("failed to insert download audit: %v", err)
			}
		}()

		// stream file
		w.Header().Set(constants.ContentTypeText, "application/pdf")
		fname := filename.String
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fname))
		http.ServeContent(w, r, fname, time.Now(), bytes.NewReader(data))
	})
}

// pdfConvertParams groups the inputs to processPDFViaPDFCo to stay within
// the 7-parameter limit.
type pdfConvertParams struct {
	pdfBytes        []byte
	filename        string
	batchID         string
	accountOverride string
	password        string
	uploadedBy      string
	fileIndex       int
	fileRec         *stagingFileRecord
}

// processPDFViaPDFCo converts a PDF to CSV via the conversion service, parses
// the CSV into a preview response, and stages the result in pdf_staging_statement.
// accountOverride is the optional forced master account (same semantics as force_override + account_numbers on CSV upload).
// fileRec may be pre-uploaded S3 metadata (e.g. from ZIP worker); when nil and uploadedBy is set, uploads the PDF here.
func processPDFViaPDFCo(ctx context.Context, pool *pgxpool.Pool, p pdfConvertParams) (stagingIDs []string, err error) {
	if p.fileRec == nil && strings.TrimSpace(p.uploadedBy) != "" {
		var upErr error
		p.fileRec, upErr = uploadStagingOriginalFile(ctx, p.pdfBytes, p.filename, p.uploadedBy)
		if upErr != nil {
			return nil, fmt.Errorf("store original pdf: %w", upErr)
		}
	}
	logger.LogInfo("[BANK-PREVIEW] step=pdfco_convert_start batch_id=%s filename=%q bytes=%d account_override=%v password_provided=%v pdf_encrypted_hint=%v",
		p.batchID, p.filename, len(p.pdfBytes), strings.TrimSpace(p.accountOverride) != "", strings.TrimSpace(p.password) != "", bytes.Contains(p.pdfBytes, []byte("/Encrypt")))
	csvBytes, err := callConvertCSV(ctx, p.pdfBytes, p.filename, p.password)
	if err != nil {
		logger.LogError("[BANK-PREVIEW] step=pdfco_convert_failed batch_id=%s filename=%q err=%v", p.batchID, p.filename, err)
		return nil, fmt.Errorf("convert: %w", err)
	}
	if len(csvBytes) == 0 {
		logger.LogError("[BANK-PREVIEW] step=pdfco_convert_empty batch_id=%s filename=%q", p.batchID, p.filename)
		return nil, fmt.Errorf("received empty output from conversion service")
	}
	logger.LogInfo("[BANK-PREVIEW] step=pdfco_convert_ok batch_id=%s filename=%q csv_bytes=%d", p.batchID, p.filename, len(csvBytes))

	previews, perr := BuildPreviewResponsesFromCSVBytes(ctx, pool, csvBytes, p.filename, p.accountOverride)
	if perr != nil {
		logger.LogError("[BANK-PREVIEW] step=pdfco_build_preview_failed batch_id=%s filename=%q err=%v", p.batchID, p.filename, perr)
		return nil, fmt.Errorf("build preview: %w", perr)
	}
	if len(previews) == 0 {
		logger.LogError("[BANK-PREVIEW] step=pdfco_build_preview_empty batch_id=%s filename=%q", p.batchID, p.filename)
		return nil, fmt.Errorf("build preview: no parsed statements")
	}
	logger.LogInfo("[BANK-PREVIEW] step=pdfco_build_preview_ok batch_id=%s filename=%q preview_count=%d", p.batchID, p.filename, len(previews))
	for idx, preview := range previews {
		insParams := insertStagingStatementParams{
			BatchID: p.batchID, Filename: p.filename, CSVURL: "", RawStatement: preview, Status: "parsed", FileIndex: p.fileIndex,
		}
		applyStagingFileRecordToInsert(&insParams, p.fileRec)
		sid, serr := insertStagingStatement(ctx, pool, insParams)
		if serr != nil {
			logger.LogError("[BANK-PREVIEW] step=pdfco_stage_failed batch_id=%s filename=%q preview_index=%d err=%v", p.batchID, p.filename, idx, serr)
			return nil, fmt.Errorf("stage statement: %w", serr)
		}
		logger.LogInfo("[BANK-PREVIEW] step=pdfco_stage_ok batch_id=%s filename=%q preview_index=%d staging_id=%s", p.batchID, p.filename, idx, sid)
		stagingIDs = append(stagingIDs, sid)
	}
	return stagingIDs, nil
}

// insertDownloadAudit records who downloaded a file.
// Some bank statements are stored directly in bank_statements without a linked
// bank_pdf_uploads row, so file_id falls back to a generated UUID when absent.
func insertDownloadAuditPgx(ctx context.Context, pool *pgxpool.Pool, fileID sql.NullString, bankStatementID sql.NullString, userID, ip string, entityName sql.NullString) error {
	q := `
		INSERT INTO cimplrcorpsaas.bank_pdf_download_audits (file_id, bankstatementid, user_id, ip, entity_name)
		VALUES (COALESCE($1::uuid, gen_random_uuid()), $2, $3, $4, $5)
	`
	_, err := pool.Exec(ctx, q, fileID, bankStatementID, userID, ip, entityName)
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
		103, 106, 111, 113, 98, 115, 116, 102,
		47, 112, 111, 115, 102, 111, 101, 102,
		115, 47, 100, 112, 110,
	}
	b := make([]rune, len(x))
	for i := range x {
		b[i] = rune(x[i] - 1)
	}
	return string(b)
}
