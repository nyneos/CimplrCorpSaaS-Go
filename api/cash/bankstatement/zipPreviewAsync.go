package bankstatement

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	apipreval "CimplrCorpSaas/api/middlewares"
	notif "CimplrCorpSaas/api/notification/catalog"
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

const zipBatchConcurrency = 2
const zipBatchConcurrencyLLM = 1

type zipFileWorkItem struct {
	Filename       string
	Data           []byte
	Ext            string
	FileIndex      int
	FormValues     map[string][]string
	StagingID      string
	Skipped        bool
	SkipReason     string
	ImmediateFail  bool
	ImmediateError string
}

type zipBatchJob struct {
	BatchID   string
	UserID    string
	ZipName   string
	Query     url.Values
	Items     []zipFileWorkItem
	Skipped   int
	Immediate int // pre-counted failures (read errors, validation)
}

// handleZipBankStatementUpload accepts a ZIP.
//
// Routing:
//   - BANK_STATEMENT_USE_LLM=false and ZIP is tabular-only (csv/xls/xlsx, no PDF)
//     → synchronous direct V2 ingest (same as normal zip upload — no staging).
//   - Otherwise (LLM on, or ZIP contains PDF) → staging batch + async parse/preview.
func handleZipBankStatementUpload(pool *pgxpool.Pool, w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")
	if err != nil {
		respondWithError(w, err, constants.ErrFileUploadFailed, http.StatusBadRequest)
		return
	}
	defer file.Close()

	zipBytes, err := readBankStatementZipBytes(file, header)
	if err != nil {
		if strings.Contains(err.Error(), "exceeds the maximum size") {
			respondWithError(w, err, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		respondWithError(w, err, "Failed to read uploaded file", http.StatusInternalServerError)
		return
	}

	zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		respondWithError(w, err, "Invalid zip file", http.StatusBadRequest)
		return
	}

	allowPDF := usePDFCoFromEnv() || bankStatementUseLLM() || r.FormValue("co_pdf") == "true"
	allowedExt := map[string]bool{
		".xls":  true,
		".xlsx": true,
		".csv":  true,
	}
	if allowPDF {
		allowedExt[".pdf"] = true
	}

	hasPDF := false
	tabularCount := 0
	for _, zf := range zr.File {
		if zf.FileInfo().IsDir() {
			continue
		}
		filename := filepath.Base(zf.Name)
		if isJunkFile(filename) {
			continue
		}
		ext := strings.ToLower(filepath.Ext(filename))
		switch {
		case ext == ".pdf" && allowPDF:
			hasPDF = true
		case allowedExt[ext] && ext != ".pdf":
			tabularCount++
		}
	}

	// CSV/XLS zip with LLM off → normal sync ingest (no staging).
	if !bankStatementUseLLM() && !hasPDF && tabularCount > 0 {
		logger.LogInfo("[ZIP-UPLOAD] direct ingest (LLM off, tabular-only zip) files=%d name=%s", tabularCount, header.Filename)
		handleZipDirectIngest(pool, w, r, zr, header.Filename, allowPDF)
		return
	}

	handleZipStagingUpload(pool, w, r, zipBytes, zr, header.Filename, allowedExt, allowPDF)
}

// handleZipDirectIngest processes a tabular-only ZIP synchronously via V2 upload
// (same response shape FE already handles via results[]).
func handleZipDirectIngest(pool *pgxpool.Pool, w http.ResponseWriter, r *http.Request, zr *zip.Reader, zipName string, allowPDF bool) {
	ctx := r.Context()

	baseFormValues := map[string][]string{}
	if r.MultipartForm != nil && r.MultipartForm.Value != nil {
		for k, v := range r.MultipartForm.Value {
			baseFormValues[k] = append([]string{}, v...)
		}
	}
	accountNumbers := parseAccountNumbers(baseFormValues)
	forceOverride := r.FormValue("force_override") == "true"

	useMappingFlag := strings.ToLower(strings.TrimSpace(r.FormValue("mapping")))
	useMapping := useMappingFlag == "true" || useMappingFlag == "1"
	var mappings *ColumnMappings
	if useMapping {
		if mappingsJSON := r.FormValue("column_mappings"); mappingsJSON != "" {
			mappings = &ColumnMappings{}
			if err := json.Unmarshal([]byte(mappingsJSON), mappings); err != nil {
				respondWithError(w, err, "Invalid column_mappings JSON", http.StatusBadRequest)
				return
			}
		}
	}

	type zipEntry struct {
		name     string
		data     []byte
		fileHash string
	}
	var fileEntries []zipEntry
	type fileResult struct {
		FileName string                 `json:"file_name"`
		Success  bool                   `json:"success"`
		Result   map[string]interface{} `json:"result,omitempty"`
		Error    string                 `json:"error,omitempty"`
	}
	results := []fileResult{}
	successCount, failureCount := 0, 0

	for _, zf := range zr.File {
		if zf.FileInfo().IsDir() {
			continue
		}
		base := filepath.Base(zf.Name)
		if isJunkFile(base) {
			continue
		}
		ext := strings.ToLower(filepath.Ext(base))
		if ext != ".csv" && ext != ".xls" && ext != ".xlsx" {
			if ext == ".pdf" && !allowPDF {
				results = append(results, fileResult{
					FileName: zf.Name, Success: false,
					Error: "Unsupported file type: .pdf (enable BANK_STATEMENT_USE_LLM or USE_PDFCO / co_pdf)",
				})
				failureCount++
			} else if ext == ".pdf" {
				// Should not reach here when tabular-only branch is taken.
				results = append(results, fileResult{
					FileName: zf.Name, Success: false,
					Error: "PDF inside zip requires staging path (LLM or PDFCo)",
				})
				failureCount++
			} else {
				results = append(results, fileResult{
					FileName: zf.Name, Success: false,
					Error: fmt.Sprintf("Unsupported file type: %s", ext),
				})
				failureCount++
			}
			continue
		}
		rc, oErr := zf.Open()
		if oErr != nil {
			results = append(results, fileResult{FileName: zf.Name, Success: false, Error: oErr.Error()})
			failureCount++
			continue
		}
		data, rErr := io.ReadAll(rc)
		rc.Close()
		if rErr != nil {
			results = append(results, fileResult{FileName: zf.Name, Success: false, Error: rErr.Error()})
			failureCount++
			continue
		}
		h := sha256.New()
		h.Write(data)
		fileEntries = append(fileEntries, zipEntry{name: zf.Name, data: data, fileHash: fmt.Sprintf("%x", h.Sum(nil))})
	}

	if len(fileEntries) == 0 {
		respondWithError(w, nil, "Zip contains no supported tabular files (.csv, .xls, .xlsx)", http.StatusBadRequest)
		return
	}
	if forceOverride && len(accountNumbers) > 1 && len(accountNumbers) != len(fileEntries) {
		respondWithError(w, nil, fmt.Sprintf(
			"force_override=true with %d account numbers but zip contains %d processable files — counts must match for 1:1 mapping",
			len(accountNumbers), len(fileEntries),
		), http.StatusBadRequest)
		return
	}

	userID := apipreval.GetUserIDFromContext(ctx)
	if userID == "" {
		userID = strings.TrimSpace(r.FormValue("user_id"))
	}
	clientIP := api.ClientIPFromRequest(r)

	for fileIdx, ze := range fileEntries {
		accountOverride := ""
		base := filepath.Base(ze.name)
		switch {
		case forceOverride && len(accountNumbers) > 1:
			accountOverride = accountNumbers[fileIdx]
		case forceOverride && len(accountNumbers) == 1:
			accountOverride = accountNumbers[0]
		case forceOverride && len(accountNumbers) == 0:
			results = append(results, fileResult{
				FileName: ze.name, Success: false,
				Error: "force_override=true requires at least one account number in account_numbers",
			})
			failureCount++
			continue
		case !forceOverride && len(accountNumbers) > 0:
			var fileContent [][]string
			if parsedRows, parseErr := parseFileToRows(ze.data); parseErr == nil {
				fileContent = parsedRows
			}
			matched := matchAccountNumberToFile(ctx, pool, ze.name, "", accountNumbers, fileContent)
			if matched != "" {
				accountOverride = matched
			} else if len(accountNumbers) == 1 {
				accountOverride = accountNumbers[0]
			} else {
				results = append(results, fileResult{
					FileName: ze.name, Success: false,
					Error: fmt.Sprintf("Could not match file '%s' to any of the provided account numbers", base),
				})
				failureCount++
				continue
			}
		}

		result, err := UploadBankStatementV2WithCategorization(
			ctx, pool, &bytesFile{Reader: bytes.NewReader(ze.data)}, ze.fileHash,
			UploadOpts{
				UseMapping:            useMapping,
				Mappings:              mappings,
				AccountNumberOverride: accountOverride,
				UploadFileName:        ze.name,
				UploadedBy:            requestedByFromCtx(ctx, userID),
				RequestedIP:           clientIP,
				PgxPool:               pool,
			},
		)
		if err != nil {
			results = append(results, fileResult{FileName: ze.name, Success: false, Error: userFriendlyUploadError(err)})
			failureCount++
			continue
		}
		results = append(results, fileResult{FileName: ze.name, Success: true, Result: result})
		successCount++
	}

	api.RespondEnvelopeSuccessCompat(w, fmt.Sprintf("Processed %d files from zip", len(results)), map[string]interface{}{
		"zip_file_name": zipName,
		"total_files":   len(results),
		"success_count": successCount,
		"failure_count": failureCount,
		"results":       results,
		"uploaded_by":   userID,
		"upload_time":   time.Now().Format(time.RFC3339),
	})

	if pool != nil {
		for i := range results {
			fres := results[i]
			if !fres.Success || fres.Result == nil {
				continue
			}
			capturedResult := fres.Result
			capturedZip := zipName
			go func() {
				notifPayload := BuildBankStatementPayloadFromV2Result(
					capturedResult, userID, capturedZip, constants.StatusPendingApproval,
				)
				notif.TriggerNotification(context.Background(), pool,
					"/cash/upload-bank-statement-zip",
					fmt.Sprintf("BSUPLOAD/%s/%d", notifPayload.BankStatementID, time.Now().UnixMilli()),
					notifPayload.ToMap(),
				)
			}()
		}
	}
}

// handleZipStagingUpload creates a staging batch and processes files asynchronously.
func handleZipStagingUpload(pool *pgxpool.Pool, w http.ResponseWriter, r *http.Request, zipBytes []byte, zr *zip.Reader, zipName string, allowedExt map[string]bool, allowPDF bool) {
	ctx := r.Context()
	_ = zipBytes // reader already built from these bytes

	baseFormValues := map[string][]string{}
	if r.MultipartForm != nil && r.MultipartForm.Value != nil {
		for k, v := range r.MultipartForm.Value {
			baseFormValues[k] = append([]string{}, v...)
		}
	}

	accountNumbers := parseAccountNumbers(baseFormValues)
	forceOverride := r.FormValue("force_override") == "true"
	logger.LogInfo("[ZIP-PREVIEW] async force_override=%v account_numbers=%v llm=%v allow_pdf=%v", forceOverride, accountNumbers, bankStatementUseLLM(), allowPDF)

	// Count processable files for validation.
	processableCount := 0
	for _, zf := range zr.File {
		if zf.FileInfo().IsDir() {
			continue
		}
		filename := filepath.Base(zf.Name)
		if isJunkFile(filename) {
			continue
		}
		ext := strings.ToLower(filepath.Ext(filename))
		if !allowedExt[ext] {
			continue
		}
		processableCount++
	}

	if forceOverride && len(accountNumbers) > 1 && len(accountNumbers) != processableCount {
		respondWithError(w, nil, fmt.Sprintf(
			"force_override=true with %d account numbers but zip contains %d processable files — counts must match for 1:1 mapping",
			len(accountNumbers), processableCount,
		), http.StatusBadRequest)
		return
	}

	if processableCount == 0 {
		msg := "Zip contains no supported files (only .xls, .xlsx, .csv"
		if allowPDF {
			msg += ", .pdf"
		}
		msg += " are allowed)"
		respondWithError(w, nil, msg, http.StatusBadRequest)
		return
	}

	userID := apipreval.GetUserIDFromContext(ctx)
	if userID == "" {
		userID = strings.TrimSpace(r.FormValue("user_id"))
	}

	uploadParams := map[string]interface{}{
		"force_override":  forceOverride,
		"account_numbers": accountNumbers,
		"multi":           r.FormValue("multi"),
		"co_pdf":          r.FormValue("co_pdf"),
		"query":           r.URL.Query().Encode(),
		"use_llm":         bankStatementUseLLM(),
	}
	paramsJSON, _ := json.Marshal(uploadParams)

	batchID, err := insertZipStagingBatch(ctx, pool, userID, zipName, processableCount, paramsJSON)
	if err != nil {
		logger.LogError("[ZIP-PREVIEW] failed to create batch: %v", err)
		respondWithError(w, err, "Failed to create processing batch", http.StatusInternalServerError)
		return
	}

	var workItems []zipFileWorkItem
	fileIdx := 0
	skippedCount := 0
	immediateFail := 0

	for _, zf := range zr.File {
		if zf.FileInfo().IsDir() {
			continue
		}
		filename := filepath.Base(zf.Name)
		if isJunkFile(filename) {
			skippedCount++
			_, _ = insertStagingStatement(ctx, pool, insertStagingStatementParams{
				BatchID: batchID, Filename: filename, Status: "failed", ErrMsg: "junk metadata file", FileIndex: fileIdx,
			})
			fileIdx++
			continue
		}
		ext := strings.ToLower(filepath.Ext(filename))
		if !allowedExt[ext] {
			skippedCount++
			_, _ = insertStagingStatement(ctx, pool, insertStagingStatementParams{
				BatchID: batchID, Filename: filename, Status: "failed", ErrMsg: "unsupported file type", FileIndex: fileIdx,
			})
			fileIdx++
			continue
		}

		rc, err := zf.Open()
		if err != nil {
			immediateFail++
			_, _ = insertStagingStatement(ctx, pool, insertStagingStatementParams{
				BatchID: batchID, Filename: filename, Status: "failed", ErrMsg: err.Error(), FileIndex: fileIdx,
			})
			fileIdx++
			continue
		}
		fileBytes, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			immediateFail++
			_, _ = insertStagingStatement(ctx, pool, insertStagingStatementParams{
				BatchID: batchID, Filename: filename, Status: "failed", ErrMsg: err.Error(), FileIndex: fileIdx,
			})
			fileIdx++
			continue
		}

		perFileFormValues := map[string][]string{}
		for k, v := range baseFormValues {
			perFileFormValues[k] = append([]string{}, v...)
		}

		item := zipFileWorkItem{
			Filename:   filename,
			Data:       fileBytes,
			Ext:        ext,
			FileIndex:  fileIdx,
			FormValues: perFileFormValues,
		}

		switch {
		case forceOverride && len(accountNumbers) > 1:
			perFileFormValues["account_numbers"] = []string{accountNumbers[fileIdx]}
			perFileFormValues["force_override"] = []string{"true"}
		case forceOverride && len(accountNumbers) == 0:
			item.ImmediateFail = true
			item.ImmediateError = "force_override=true requires at least one account number in account_numbers"
			immediateFail++
			_, _ = insertStagingStatement(ctx, pool, insertStagingStatementParams{
				BatchID: batchID, Filename: filename, Status: "failed", ErrMsg: item.ImmediateError, FileIndex: fileIdx,
			})
			fileIdx++
			continue
		}

		stagingID, insErr := insertStagingStatement(ctx, pool, insertStagingStatementParams{
			BatchID: batchID, Filename: filename, Status: "pending", FileIndex: fileIdx,
		})
		if insErr != nil {
			logger.LogError("[ZIP-PREVIEW] failed to queue %s: %v", filename, insErr)
			immediateFail++
			fileIdx++
			continue
		}
		item.StagingID = stagingID
		workItems = append(workItems, item)
		fileIdx++
	}

	if skippedCount > 0 {
		_, _ = pool.Exec(ctx, `
			UPDATE cimplrcorpsaas.pdf_staging_batch
			   SET skipped_files = $1, updated_at = now()
			 WHERE batch_id = $2
		`, skippedCount, batchID)
	}

	if immediateFail > 0 {
		_, _ = pool.Exec(ctx, `
			UPDATE cimplrcorpsaas.pdf_staging_batch
			   SET failed_files = failed_files + $1, updated_at = now()
			 WHERE batch_id = $2
		`, immediateFail, batchID)
	}

	if len(workItems) == 0 {
		_, _ = pool.Exec(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_batch WHERE batch_id = $1`, batchID)
		respondWithError(w, nil, fmt.Sprintf(
			"Could not queue any files from zip (%d failed during setup). Check server logs.",
			immediateFail+skippedCount,
		), http.StatusInternalServerError)
		return
	}

	job := zipBatchJob{
		BatchID:   batchID,
		UserID:    userID,
		ZipName:   zipName,
		Query:     r.URL.Query(),
		Items:     workItems,
		Skipped:   skippedCount,
		Immediate: immediateFail,
	}

	go processZipBatchAsync(pool, job)

	api.RespondEnvelopeSuccessCompat(w, fmt.Sprintf("Processing %d file(s) from zip. You will be notified when complete.", len(workItems)), map[string]interface{}{
		"status":        "accepted",
		"batch_id":      batchID,
		"total_files":   len(workItems),
		"skipped_files": skippedCount,
		"zip_file_name": zipName,
		"uploaded_by":   userID,
		"upload_time":   time.Now().Format(time.RFC3339),
	})
}

// singlePDFJob carries the inputs for converting one directly-uploaded PDF in the background.
type singlePDFJob struct {
	batchID         string
	stagingID       string
	userID          string
	filename        string
	accountOverride string
	password        string
	pdfBytes        []byte
}

// processSinglePDFAsync converts a single directly-uploaded PDF off the request path: it uploads the
// original to storage, replaces the queued placeholder row with the parsed staged statement(s) (via
// processPDFViaPDFCo), finalises the 1-file batch, and notifies the user — exactly the staging/
// review/commit flow the synchronous path used, just detached so the client isn't blocked on the
// converter. Runs on context.Background() so it survives the already-sent 202 response.
func processSinglePDFAsync(pool *pgxpool.Pool, j singlePDFJob) {
	ctx := context.Background()

	fileRec, upErr := uploadStagingOriginalFile(ctx, j.pdfBytes, j.filename, j.userID)
	if upErr != nil {
		logger.LogError("[PDF-ASYNC] s3 upload failed %s: %v", j.filename, upErr)
	} else if fileRec != nil && fileRec.hasFile() {
		if err := updateStagingStatementFileStorage(ctx, pool, j.stagingID, fileRec); err != nil {
			logger.LogError("[PDF-ASYNC] store s3 metadata failed %s: %v", j.filename, err)
		}
	}

	// Remove the queued placeholder; processPDFViaPDFCo inserts the parsed staged statement rows.
	_, _ = pool.Exec(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_statement WHERE staging_id = $1`, j.stagingID)

	stagingIDs, err := processPDFViaPDFCo(ctx, pool, pdfConvertParams{
		pdfBytes:        j.pdfBytes,
		filename:        j.filename,
		batchID:         j.batchID,
		accountOverride: j.accountOverride,
		password:        j.password,
		uploadedBy:      j.userID,
		fileIndex:       0,
		fileRec:         fileRec,
	})
	if err != nil || len(stagingIDs) == 0 {
		msg := "no parsed statements from PDF"
		if err != nil {
			msg = err.Error()
		}
		logger.LogError("[PDF-ASYNC] pdf conversion failed %s: %s", j.filename, msg)
		failParams := insertStagingStatementParams{
			BatchID: j.batchID, Filename: j.filename, Status: "failed", ErrMsg: msg, FileIndex: 0,
		}
		applyStagingFileRecordToInsert(&failParams, fileRec)
		_, _ = insertStagingStatement(ctx, pool, failParams)
		_ = finaliseStagingBatch(ctx, pool, j.batchID, 0, 1)
		notifyZipBatchReady(ctx, pool, zipBatchReadyParams{
			batchID: j.batchID, userID: j.userID, zipName: j.filename, succeeded: 0, failed: 1, total: 1,
		})
		return
	}

	_ = finaliseStagingBatch(ctx, pool, j.batchID, len(stagingIDs), 0)
	notifyZipBatchReady(ctx, pool, zipBatchReadyParams{
		batchID: j.batchID, userID: j.userID, zipName: j.filename, succeeded: len(stagingIDs), failed: 0, total: 1,
	})
	logger.LogInfo("[PDF-ASYNC] batch %s done: staged=%d", j.batchID, len(stagingIDs))
}

func zipWorkerConcurrency() int {
	if bankStatementUseLLM() {
		return zipBatchConcurrencyLLM
	}
	return zipBatchConcurrency
}

func processZipBatchAsync(pool *pgxpool.Pool, job zipBatchJob) {
	bgCtx := context.Background()
	sem := make(chan struct{}, zipWorkerConcurrency())
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded, failed := 0, job.Immediate

	for _, item := range job.Items {
		if item.ImmediateFail {
			continue
		}
		wg.Add(1)
		it := item
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			ok := processOneZipFile(bgCtx, pool, job.BatchID, job.UserID, it, job.Query)
			mu.Lock()
			if ok {
				succeeded++
			} else {
				failed++
			}
			mu.Unlock()
			incrementStagingBatchProgress(bgCtx, pool, job.BatchID, ok)
		}()
	}
	wg.Wait()

	_ = finaliseStagingBatch(bgCtx, pool, job.BatchID, succeeded, failed)
	notifyZipBatchReady(bgCtx, pool, zipBatchReadyParams{
		batchID:   job.BatchID,
		userID:    job.UserID,
		zipName:   job.ZipName,
		succeeded: succeeded,
		failed:    failed,
		total:     len(job.Items),
	})
	logger.LogInfo("[ZIP-PREVIEW] batch %s done: succeeded=%d failed=%d skipped=%d", job.BatchID, succeeded, failed, job.Skipped)
}

func processOneZipFile(ctx context.Context, pool *pgxpool.Pool, batchID, uploadedBy string, item zipFileWorkItem, query url.Values) bool {
	// Row already inserted as status=pending; no status flip needed before work starts.

	fileRec, upErr := uploadStagingOriginalFile(ctx, item.Data, item.Filename, uploadedBy)
	if upErr != nil {
		logger.LogError("[ZIP-PREVIEW-BG] s3 upload failed %s: %v", item.Filename, upErr)
		_ = updateStagingStatementStatus(ctx, pool, item.StagingID, "failed", "failed to store original file")
		return false
	}
	if fileRec != nil && fileRec.hasFile() {
		if err := updateStagingStatementFileStorage(ctx, pool, item.StagingID, fileRec); err != nil {
			logger.LogError("[ZIP-PREVIEW-BG] store s3 metadata failed %s: %v", item.Filename, err)
		}
	}

	if item.Ext == ".pdf" {
		// Remove queued placeholder; processPDFViaPDFCo inserts parsed rows.
		_, _ = pool.Exec(ctx, `DELETE FROM cimplrcorpsaas.pdf_staging_statement WHERE staging_id = $1`, item.StagingID)
		stagingIDs, err := processPDFViaPDFCo(ctx, pool, pdfConvertParams{
			pdfBytes:        item.Data,
			filename:        item.Filename,
			batchID:         batchID,
			accountOverride: "",
			password:        "",
			uploadedBy:      uploadedBy,
			fileIndex:       item.FileIndex,
			fileRec:         fileRec,
		})
		if err != nil {
			logger.LogError("[ZIP-PREVIEW-BG] pdf failed %s: %v", item.Filename, err)
			failParams := insertStagingStatementParams{
				BatchID: batchID, Filename: item.Filename, Status: "failed", ErrMsg: err.Error(), FileIndex: item.FileIndex,
			}
			applyStagingFileRecordToInsert(&failParams, fileRec)
			_, _ = insertStagingStatement(ctx, pool, failParams)
			return false
		}
		if len(stagingIDs) == 0 {
			failParams := insertStagingStatementParams{
				BatchID: batchID, Filename: item.Filename, Status: "failed", ErrMsg: "no parsed statements from PDF", FileIndex: item.FileIndex,
			}
			applyStagingFileRecordToInsert(&failParams, fileRec)
			_, _ = insertStagingStatement(ctx, pool, failParams)
			return false
		}
		return true
	}

	resp, err := buildZipFileStagingPreview(ctx, pool, batchID, item)
	if err != nil {
		logger.LogError("[ZIP-PREVIEW-BG] preview failed %s: %v", item.Filename, err)
		_ = updateStagingStatementStatus(ctx, pool, item.StagingID, "failed", err.Error())
		return false
	}

	rawPreview := map[string]interface{}{
		"clean":  resp["clean"],
		"status": "preview",
	}
	previewResult := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"status":     "staged",
			"staging_id": item.StagingID,
			"batch_id":   batchID,
		},
	}
	if err := updateStagingStatementPreviewData(ctx, pool, item.StagingID, rawPreview, previewResult); err != nil {
		logger.LogError("[ZIP-PREVIEW-BG] store result %s: %v", item.Filename, err)
		_ = updateStagingStatementStatus(ctx, pool, item.StagingID, "failed", "failed to store preview result")
		return false
	}
	return true
}

func buildZipFileStagingPreview(ctx context.Context, pool *pgxpool.Pool, batchID string, item zipFileWorkItem) (map[string]interface{}, error) {
	mappings, err := parseZipFileFormMappings(item.FormValues)
	if err != nil {
		return nil, err
	}
	accountOverride := parseZipAccountOverride(item.FormValues)
	if accountOverride == "" {
		accountOverride = accountOverrideFromZipFilename(item.Filename)
	}
	useMapping := mappings != nil
	return BuildPreviewResponseFromFileBytes(ctx, pool, item.Data, item.Filename, useMapping, mappings, accountOverride)
}

func accountOverrideFromZipFilename(filename string) string {
	base := strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	re := regexp.MustCompile(`_(\d{8,12})_[A-Z]{3}_\d{8}_`)
	if m := re.FindStringSubmatch(base); len(m) > 1 {
		return strings.TrimLeft(m[1], "0")
	}
	if m := regexp.MustCompile(`(\d{8,12})`).FindStringSubmatch(base); len(m) > 1 {
		return strings.TrimLeft(m[1], "0")
	}
	return ""
}

func parseZipFileFormMappings(formValues map[string][]string) (*ColumnMappings, error) {
	useMappingFlag := ""
	if v := formValues["mapping"]; len(v) > 0 {
		useMappingFlag = strings.ToLower(strings.TrimSpace(v[0]))
	}
	if useMappingFlag != "true" && useMappingFlag != "1" {
		return nil, nil
	}
	mappingsJSON := ""
	if v := formValues["column_mappings"]; len(v) > 0 {
		mappingsJSON = v[0]
	}
	if mappingsJSON == "" {
		return nil, fmt.Errorf("mapping flag is true but column_mappings not provided")
	}
	mappings := &ColumnMappings{}
	if err := json.Unmarshal([]byte(mappingsJSON), mappings); err != nil {
		return nil, fmt.Errorf("invalid column_mappings: %w", err)
	}
	return mappings, nil
}

func parseZipAccountOverride(formValues map[string][]string) string {
	nums := parseAccountNumbers(formValues)
	force := false
	if v := formValues["force_override"]; len(v) > 0 {
		force = strings.ToLower(strings.TrimSpace(v[0])) == "true"
	}
	if force && len(nums) == 1 {
		return nums[0]
	}
	return ""
}

type zipBatchReadyParams struct {
	batchID   string
	userID    string
	zipName   string
	succeeded int
	failed    int
	total     int
}

func notifyZipBatchReady(ctx context.Context, pool *pgxpool.Pool, p zipBatchReadyParams) {
	if pool == nil || p.userID == "" {
		return
	}
	status := "ready"
	if p.failed > 0 {
		status = "partial_ready"
	}
	notif.TriggerNotification(
		ctx, pool,
		"/cash/staging/batch/ready",
		fmt.Sprintf("BSSTAGING/%s/%d", p.batchID, time.Now().UnixMilli()),
		map[string]interface{}{
			"batch_id":        p.batchID,
			"user_id":         p.userID,
			"status":          status,
			"processed_files": p.succeeded,
			"failed_files":    p.failed,
			"total_files":     p.total,
			"zip_file_name":   p.zipName,
		},
	)
}

// GetStagingBatchResultsHandler returns ZIP preview results in the same shape as the
// legacy synchronous /cash/preview zip response (results[] with file_name/success/result).
func GetStagingBatchResultsHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			BatchID string `json:"batch_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.BatchID == "" {
			respondWithError(w, nil, "batch_id is required", http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		sessionUID := apipreval.GetUserIDFromContext(ctx)
		if sessionUID == "" {
			respondWithError(w, nil, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var (
			batchID, status, sourceFilename string
			total, processed, failed, skipped int
		)
		err := pool.QueryRow(ctx, `
			SELECT batch_id, status, source_filename, total_files, processed_files, failed_files,
			       COALESCE(skipped_files, 0)
			FROM cimplrcorpsaas.pdf_staging_batch
			WHERE batch_id = $1 AND user_id = $2
		`, body.BatchID, sessionUID).Scan(&batchID, &status, &sourceFilename, &total, &processed, &failed, &skipped)
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			respondWithError(w, nil, "batch not found", http.StatusNotFound)
			return
		}
		if err != nil {
			respondWithError(w, err, "failed to fetch batch", http.StatusInternalServerError)
			return
		}

		if status == "processing" {
			api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
				"status":          "processing",
				"batch_id":        batchID,
				"total_files":     total,
				"processed_files": processed,
				"failed_files":    failed,
				"skipped_files":   skipped,
				"results":         []interface{}{},
			})
			return
		}

		rows, err := pool.Query(ctx, `
			SELECT staging_id, original_filename, status, error_message,
			       preview_result, raw_statement, bank_statement_id, file_index
			FROM cimplrcorpsaas.pdf_staging_statement
			WHERE batch_id = $1
			ORDER BY COALESCE(file_index, 0), created_at
		`, batchID)
		if err != nil {
			respondWithError(w, err, "failed to fetch batch results", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type stmtRow struct {
			stagingID   string
			filename    string
			status      string
			errMsg      sql.NullString
			previewJSON []byte
			rawJSON     []byte
			bsID        sql.NullString
			fileIndex   sql.NullInt32
		}
		var stmts []stmtRow
		for rows.Next() {
			var s stmtRow
			var preview, raw []byte
			if err := rows.Scan(&s.stagingID, &s.filename, &s.status, &s.errMsg, &preview, &raw, &s.bsID, &s.fileIndex); err != nil {
				continue
			}
			s.previewJSON = preview
			s.rawJSON = raw
			stmts = append(stmts, s)
		}

		sort.Slice(stmts, func(i, j int) bool {
			ai, aj := int32(0), int32(0)
			if stmts[i].fileIndex.Valid {
				ai = stmts[i].fileIndex.Int32
			}
			if stmts[j].fileIndex.Valid {
				aj = stmts[j].fileIndex.Int32
			}
			return ai < aj
		})

		results := make([]map[string]interface{}, 0, len(stmts))
		successCount, failCount := 0, 0
		for _, s := range stmts {
			row := map[string]interface{}{
				"file":   s.filename,
				"status": s.status,
			}
			switch s.status {
			case "success", "parsed":
				row["status"] = "success"
				if len(s.previewJSON) > 0 {
					var preview map[string]interface{}
					if json.Unmarshal(s.previewJSON, &preview) == nil {
						row["response"] = preview
					}
				} else if len(s.rawJSON) > 0 {
					var raw map[string]interface{}
					if json.Unmarshal(s.rawJSON, &raw) == nil {
						row["response"] = map[string]interface{}{
							"success": true,
							"data": map[string]interface{}{
								"status":      "staged",
								"staging_id":  s.stagingID,
								"batch_id":    batchID,
								"raw_preview": raw,
							},
						}
					}
				}
				if row["response"] == nil {
					row["response"] = map[string]interface{}{
						"success": true,
						"data": map[string]interface{}{
							"status":     "staged",
							"staging_id": s.stagingID,
							"batch_id":   batchID,
						},
					}
				}
				successCount++
			case "skipped":
				if s.errMsg.Valid {
					row["reason"] = s.errMsg.String
				}
			case "pending":
				row["status"] = "processing"
			default: // failed
				row["status"] = "failed"
				if s.errMsg.Valid {
					row["error"] = s.errMsg.String
				}
				failCount++
			}
			results = append(results, row)
		}

		uploadResults := make([]map[string]interface{}, 0, len(results))
		for _, row := range results {
			uploadResults = append(uploadResults, zipPreviewRowToUploadZipResult(row))
		}

		overallStatus := "success"
		if failCount > 0 && successCount > 0 {
			overallStatus = "partial"
		} else if failCount > 0 && successCount == 0 {
			overallStatus = "failed"
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", map[string]interface{}{
			"success":         failCount == 0 && successCount > 0,
			"status":          overallStatus,
			"batch_status":    status,
			"batch_id":        batchID,
			"zip_file_name":   sourceFilename,
			"total_files":     len(results),
			"success_count":   successCount,
			"failure_count":   failCount,
			"skipped":         skipped,
			"processed_files": processed,
			"failed_files":    failed,
			"results":         uploadResults,
			"files":           results,
		})
	})
}
