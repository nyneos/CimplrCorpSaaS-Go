package forwards

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/fx/auditutil"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/xuri/excelize/v2"

	"CimplrCorpSaas/internal/logger"
)

var ErrMTMFileAlreadyUploaded = errors.New("mtm file already uploaded")

const duplicateMTMUploadMessage = "This MTM file was already uploaded earlier. Please upload a different file."

func forwardMTMHasIsDeletedColumn(ctx context.Context, db *sql.DB) bool {
	return forwardMTMHasColumn(ctx, db, "is_deleted")
}

func forwardMTMHasProcessingStatusColumn(ctx context.Context, db *sql.DB) bool {
	return forwardMTMHasColumn(ctx, db, "processing_status")
}

func forwardMTMHasColumn(ctx context.Context, db *sql.DB, columnName string) bool {
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'forward_mtm'
			  AND column_name = $1
		)
	`, columnName).Scan(&exists); err != nil {
		return false
	}
	return exists
}

func existingMTMUploadKeys(ctx context.Context, db *sql.DB) ([]string, error) {
	query := `
		SELECT DISTINCT upload_s3_key
		FROM forward_mtm
		WHERE COALESCE(TRIM(upload_s3_key), '') <> ''
	`
	if forwardMTMHasIsDeletedColumn(ctx, db) {
		query += ` AND COALESCE(is_deleted, false) = false`
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make([]string, 0)
	for rows.Next() {
		var key string
		if scanErr := rows.Scan(&key); scanErr != nil {
			return nil, scanErr
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func ensureUniqueMTMUpload(ctx context.Context, db *sql.DB, fileBytes []byte) error {
	keys, err := existingMTMUploadKeys(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to check duplicate mtm upload: %w", err)
	}
	duplicateKey, err := s3storage.FindDuplicateObjectKey(ctx, fileBytes, keys)
	if err != nil {
		return fmt.Errorf("failed to compare mtm upload content: %w", err)
	}
	if duplicateKey != "" {
		return ErrMTMFileAlreadyUploaded
	}
	return nil
}

func mtmApprovalStatusExpr(ctx context.Context, db *sql.DB) string {
	if forwardMTMHasProcessingStatusColumn(ctx, db) {
		return "COALESCE(NULLIF(TRIM(processing_status), ''), NULLIF(TRIM(status), ''), '')"
	}
	return "COALESCE(NULLIF(TRIM(status), ''), '')"
}

func mtmPendingStatuses() []string {
	return []string{"PENDING", constants.StatusPendingApproval, constants.StatusPendingEditApproval, constants.StatusPendingDeleteApproval}
}

func mtmActiveFilterClause(ctx context.Context, db *sql.DB, tableAlias string) string {
	if !forwardMTMHasIsDeletedColumn(ctx, db) {
		return ""
	}
	if strings.TrimSpace(tableAlias) != "" {
		return fmt.Sprintf(" AND COALESCE(%s.is_deleted, false) = false", tableAlias)
	}
	return " AND COALESCE(is_deleted, false) = false"
}

// Helper: send JSON error response
func respondWithError(w http.ResponseWriter, status int, errMsg string) {
	w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		constants.ValueSuccess: false,
		constants.ValueError:   errMsg,
	})
}

func UploadMTMFiles(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var userID string
		ct := r.Header.Get(constants.ContentTypeText)
		if strings.HasPrefix(ct, constants.ContentTypeMultipart) {
			err := r.ParseMultipartForm(32 << 20)
			if err != nil {
				respondWithError(w, http.StatusBadRequest, "Failed to parse form-data")
				return
			}
			userID = r.FormValue(constants.KeyUserID)
			// } else if strings.HasPrefix(ct, constants.ContentTypeJSON) {
			// 	var bodyMap map[string]interface{}
			// 	_ = json.NewDecoder(r.Body).Decode(&bodyMap)
			// 	if uid, ok := bodyMap[constants.KeyUserID].(string); ok {
			// 		userID = uid
			// 	}
		}
		if userID == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrMissingUserID)
			return
		}

		// Get business units from middleware context
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// Get files from multipart form
		form := r.MultipartForm
		files := form.File["files"]
		if len(files) == 0 {
			respondWithError(w, http.StatusBadRequest, constants.ErrNoFilesUploaded)
			return
		}

		results, err := processUploadMTMFiles(r.Context(), db, r, buNames)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}

		hasErrors := false
		for _, r := range results {
			if r[constants.ValueError] != nil {
				hasErrors = true
				break
			}
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: !hasErrors,
			"results":              results,
		})
	}
}

func processUploadMTMFiles(ctx context.Context, db *sql.DB, r *http.Request, buNames []string) ([]map[string]interface{}, error) {
	files := r.MultipartForm.File["files"]
	results := []map[string]interface{}{}
	skipDuplicates := strings.EqualFold(strings.TrimSpace(r.FormValue("skipDuplicates")), "true")
	uploadedBy := forwardUploadUserName(r.FormValue(constants.KeyUserID))
	for _, fileHeader := range files {
		file, err := fileHeader.Open()
		if err != nil {
			results = append(results, map[string]interface{}{
				"filename":           fileHeader.Filename,
				constants.ValueError: constants.ErrFailedToOpenFile,
			})
			continue
		}

		fileBytes, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			results = append(results, map[string]interface{}{
				"filename":           fileHeader.Filename,
				constants.ValueError: "Failed to read file",
			})
			continue
		}

		ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
		s3Key := s3storage.BuildUploadedS3Key("fx/mtm", "", fileHeader.Filename, uploadedBy, time.Now().UTC())

		if err := ensureUniqueMTMUpload(ctx, db, fileBytes); err != nil {
			message := err.Error()
			if errors.Is(err, ErrMTMFileAlreadyUploaded) {
				message = duplicateMTMUploadMessage
			}
			results = append(results, map[string]interface{}{
				"filename":           fileHeader.Filename,
				constants.ValueError: message,
			})
			continue
		}

		var rowsData []map[string]interface{}
		if ext == ".csv" {
			reader := csv.NewReader(bytes.NewReader(fileBytes))
			headers, err := reader.Read()
			if err != nil {
				results = append(results, map[string]interface{}{
					"filename":           fileHeader.Filename,
					constants.ValueError: constants.ErrFailedToReadCSVHeaders,
				})
				continue
			}
			for {
				row, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					continue
				}
				obj := map[string]interface{}{}
				for i, h := range headers {
					obj[h] = row[i]
				}
				rowsData = append(rowsData, obj)
			}
		} else if ext == ".xls" || ext == ".xlsx" {
			xl, err := excelize.OpenReader(bytes.NewReader(fileBytes))
			if err != nil {
				results = append(results, map[string]interface{}{
					"filename":           fileHeader.Filename,
					constants.ValueError: "Failed to read Excel file",
				})
				continue
			}
			sheet := xl.GetSheetName(0)
			xRows, err := xl.GetRows(sheet)
			if err != nil || len(xRows) < 1 {
				xl.Close()
				results = append(results, map[string]interface{}{
					"filename":           fileHeader.Filename,
					constants.ValueError: "No data in Excel file",
				})
				continue
			}
			xl.Close()
			headers := xRows[0]
			for _, row := range xRows[1:] {
				obj := map[string]interface{}{}
				for i, h := range headers {
					if i < len(row) {
						obj[h] = row[i]
					} else {
						obj[h] = nil
					}
				}
				rowsData = append(rowsData, obj)
			}
		} else {
			results = append(results, map[string]interface{}{
				"filename":           fileHeader.Filename,
				constants.ValueError: constants.ErrUnsupportedFileType,
			})
			continue
		}

		if len(rowsData) == 0 {
			results = append(results, map[string]interface{}{
				"filename":           fileHeader.Filename,
				constants.ValueError: constants.ErrNoDataToUpload,
			})
			continue
		}

		var fileError error
		validRows := [][]interface{}{}
		skippedDuplicates := 0
		refIds := []string{}
		for _, row := range rowsData {
			if v, ok := row["internal_reference_id"].(string); ok && v != "" {
				refIds = append(refIds, v)
			}
		}
		bookingMap := map[string]string{}
		bookingDetailsMap := map[string]map[string]interface{}{}
		bookingIdList := []string{}
		if len(refIds) > 0 {
			query := `SELECT system_transaction_id, internal_reference_id, order_type, booking_amount, maturity_date, total_rate, currency_pair FROM forward_bookings WHERE internal_reference_id = ANY($1)`
			rows, err := db.Query(query, pq.Array(refIds))
			if err == nil {
				for rows.Next() {
					var systemTransactionId, internal_reference_id, order_type, currency_pair string
					var bookingAmount, total_rate float64
					var maturityDate string
					rows.Scan(&systemTransactionId, &internal_reference_id, &order_type, &bookingAmount, &maturityDate, &total_rate, &currency_pair)
					bookingMap[internal_reference_id] = systemTransactionId
					bookingDetailsMap[internal_reference_id] = map[string]interface{}{
						"order_type":     order_type,
						"booking_amount": bookingAmount,
						"maturity_date":  maturityDate,
						"total_rate":     total_rate,
						"currency_pair":  currency_pair,
					}
					bookingIdList = append(bookingIdList, systemTransactionId)
				}
				rows.Close()
			}
		}
		existingMTMRefs := map[string]struct{}{}
		if len(refIds) > 0 {
			query := `SELECT internal_reference_id FROM forward_mtm WHERE internal_reference_id = ANY($1)`
			if forwardMTMHasIsDeletedColumn(ctx, db) {
				query += ` AND COALESCE(is_deleted, false) = false`
			}
			rows, err := db.Query(query, pq.Array(refIds))
			if err == nil {
				for rows.Next() {
					var internalReferenceID string
					if scanErr := rows.Scan(&internalReferenceID); scanErr == nil {
						existingMTMRefs[internalReferenceID] = struct{}{}
					}
				}
				rows.Close()
			}
		}
		ledgerMap := map[string]map[string]interface{}{}
		if len(bookingIdList) > 0 {
			query := `SELECT booking_id, running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = ANY($1)`
			rows, err := db.Query(query, pq.Array(bookingIdList))
			if err == nil {
				for rows.Next() {
					var bookingId string
					var runningOpenAmount float64
					var ledgerSequence int
					rows.Scan(&bookingId, &runningOpenAmount, &ledgerSequence)
					if lm, ok := ledgerMap[bookingId]; !ok || ledgerSequence > lm["ledger_sequence"].(int) {
						ledgerMap[bookingId] = map[string]interface{}{
							"running_open_amount": runningOpenAmount,
							"ledger_sequence":     ledgerSequence,
						}
					}
				}
				rows.Close()
			}
		}
		fileSeenRefs := map[string]struct{}{}
		for i, row := range rowsData {
			entity, _ := row["entity"].(string)
			if !containsString(buNames, entity) {
				fileError = fmt.Errorf("business unit not allowed: %s (row %d)", entity, i+1)
				break
			}
			internalRef, _ := row["internal_reference_id"].(string)
			if _, seenInFile := fileSeenRefs[internalRef]; seenInFile {
				if skipDuplicates {
					skippedDuplicates++
					continue
				}
				fileError = fmt.Errorf("duplicate internal_reference_id in upload file: %s (row %d)", internalRef, i+1)
				break
			}
			fileSeenRefs[internalRef] = struct{}{}
			if _, exists := existingMTMRefs[internalRef]; exists {
				if skipDuplicates {
					skippedDuplicates++
					continue
				}
				fileError = fmt.Errorf("mtm already exists for internal_reference_id: %s (row %d)", internalRef, i+1)
				break
			}
			bookingId := bookingMap[internalRef]
			if bookingId == "" {
				fileError = fmt.Errorf("booking not found for internal_reference_id: %s (row %d)", internalRef, i+1)
				break
			}
			booking := bookingDetailsMap[internalRef]
			if booking == nil {
				fileError = fmt.Errorf("booking details not found for internal_reference_id: %s (row %d)", internalRef, i+1)
				break
			}
			openAmount := booking["booking_amount"].(float64)
			if lm, ok := ledgerMap[bookingId]; ok {
				openAmount = lm["running_open_amount"].(float64)
			}
			mismatchFields := []string{}
			if str(row["buy_sell"]) != str(booking["order_type"]) {
				mismatchFields = append(mismatchFields, "buy_sell/order_type")
			}
			if num(row["notional_amount"]) != openAmount {
				mismatchFields = append(mismatchFields, "notional_amount/open_amount")
			}
			if num(row["contract_rate"]) != booking["total_rate"].(float64) {
				mismatchFields = append(mismatchFields, "contract_rate/total_rate")
			}
			if str(row["currency_pair"]) != str(booking["currency_pair"]) {
				mismatchFields = append(mismatchFields, "currency_pair")
			}
			if len(mismatchFields) > 0 {
				fileError = fmt.Errorf("reconciliation failed for internal_reference_id: %s (row %d). Mismatched fields: %s", internalRef, i+1, strings.Join(mismatchFields, ", "))
				break
			}
			mtmRate := num(row["mtm_rate"])
			contractRate := num(row["contract_rate"])
			notionalAmount := num(row["notional_amount"])
			mtmValue := (mtmRate - contractRate) * notionalAmount
			dealDate := str(row["deal_date"])
			maturityDate := str(row["maturity_date"])
			daysToMaturity := calcDaysToMaturity(dealDate, maturityDate, row["days_to_maturity"])
			status := str(row[constants.KeyStatus])
			if status == "" {
				status = "pending"
			}
			validRows = append(validRows, []interface{}{
				uuid.New().String(),
				bookingId,
				dealDate,
				maturityDate,
				str(row["currency_pair"]),
				str(row["buy_sell"]),
				notionalAmount,
				contractRate,
				mtmRate,
				mtmValue,
				daysToMaturity,
				status,
				internalRef,
				entity,
			})
		}
		if fileError != nil {
			results = append(results, map[string]interface{}{
				"filename":           fileHeader.Filename,
				constants.ValueError: fileError.Error(),
			})
			continue
		}
		if len(validRows) == 0 {
			results = append(results, map[string]interface{}{
				"filename": fileHeader.Filename,
				"inserted": 0,
				"skipped":  skippedDuplicates,
			})
			continue
		}

		tx, err := db.Begin()
		if err != nil {
			results = append(results, map[string]interface{}{
				"filename":           fileHeader.Filename,
				constants.ValueError: constants.ErrFailedToStartDBTransaction,
			})
			continue
		}

		s3Uploaded := false
		if s3storage.IsS3UploadEnabled() {
			contentType := s3storage.DetectContentType(fileBytes)
			if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
				_ = tx.Rollback()
				results = append(results, map[string]interface{}{
					"filename":           fileHeader.Filename,
					constants.ValueError: "Failed to upload file to S3: " + err.Error(),
				})
				continue
			}
			s3Uploaded = true
		}

		hasProcessingStatus := forwardMTMHasProcessingStatusColumn(ctx, db)
		if len(validRows) > 0 {
			valueStrings := []string{}
			valueArgs := []interface{}{}
			for i, row := range validRows {
				argCount := 15
				if hasProcessingStatus {
					argCount = 16
				}
				offset := i*argCount + 1
				if hasProcessingStatus {
					valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", offset, offset+1, offset+2, offset+3, offset+4, offset+5, offset+6, offset+7, offset+8, offset+9, offset+10, offset+11, offset+12, offset+13, offset+14, offset+15))
				} else {
					valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", offset, offset+1, offset+2, offset+3, offset+4, offset+5, offset+6, offset+7, offset+8, offset+9, offset+10, offset+11, offset+12, offset+13, offset+14))
				}
				valueArgs = append(valueArgs, row...)
				valueArgs = append(valueArgs, s3Key)
				if hasProcessingStatus {
					valueArgs = append(valueArgs, constants.StatusPendingApproval)
				}
			}
			insertCols := "mtm_id, booking_id, deal_date, maturity_date, currency_pair, buy_sell, notional_amount, contract_rate, mtm_rate, mtm_value, days_to_maturity, status, internal_reference_id, entity, upload_s3_key"
			if hasProcessingStatus {
				insertCols += ", processing_status"
			}
			insertQuery := "INSERT INTO forward_mtm (" + insertCols + ") VALUES " + strings.Join(valueStrings, ",")
			_, err := tx.Exec(insertQuery, valueArgs...)
			if err != nil {
				_ = tx.Rollback()
				if s3Uploaded {
					if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
						logger.LogError("[mtm-upload] failed to cleanup S3 object after insert failure for %s: %v", fileHeader.Filename, cleanupErr)
					}
				}
				results = append(results, map[string]interface{}{
					"filename":           fileHeader.Filename,
					constants.ValueError: "Failed to insert data: " + err.Error(),
				})
				continue
			}
		}
		err = tx.Commit()
		if err != nil {
			if s3Uploaded {
				if cleanupErr := s3storage.DeleteFromS3(ctx, s3Key); cleanupErr != nil {
					logger.LogError("[mtm-upload] failed to cleanup S3 object after commit failure for %s: %v", fileHeader.Filename, cleanupErr)
				}
			}
			results = append(results, map[string]interface{}{
				"filename":           fileHeader.Filename,
				constants.ValueError: constants.ErrTxCommitFailed + err.Error(),
			})
			continue
		}
		results = append(results, map[string]interface{}{
			"filename": fileHeader.Filename,
			"inserted": len(validRows),
			"skipped":  skippedDuplicates,
		})
		for _, row := range validRows {
			if len(row) > 0 {
				auditutil.RecordAction(ctx, db, auditutil.ActionParams{TableName: auditutil.TableForwardMTM, ParentColumn: "mtm_id", ParentID: fmt.Sprint(row[0]), ActionType: "CREATE", Status: constants.StatusPendingApproval, Reason: "", RequestedBy: uploadedBy, OldValues: nil, NewValues: map[string]interface{}{"upload_s3_key": s3Key}})
			}
		}
	}

	return results, nil
}

func GetMTMDownloadURL(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(); err != nil {
			log.Printf("[ERROR] GetMTMDownloadURL: database connection issue: %v", err)
			respondWithError(w, http.StatusInternalServerError, "database connection unavailable")
			return
		}

		var req struct {
			MTMID    string `json:"mtm_id"`
			RecordID string `json:"record_id"`
			UserID   string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		mtmID := strings.TrimSpace(req.MTMID)
		if mtmID == "" {
			mtmID = strings.TrimSpace(req.RecordID)
		}
		if mtmID == "" {
			respondWithError(w, http.StatusBadRequest, "mtm_id is required")
			return
		}

		// Handle base64-encoded mtm_id (sent by frontend)
		if decoded, err := base64.StdEncoding.DecodeString(mtmID); err == nil {
			mtmID = string(decoded)
		}

		log.Printf("[DEBUG] GetMTMDownloadURL: attempting to fetch mtm_id=%s", mtmID)

		var uploadS3Key sql.NullString
		query := `
			SELECT upload_s3_key
			FROM forward_mtm
			WHERE mtm_id = $1`
		if forwardMTMHasIsDeletedColumn(r.Context(), db) {
			query += `
			  AND COALESCE(is_deleted, false) = false`
		}
		query += `
			LIMIT 1`
		err := db.QueryRowContext(r.Context(), query, mtmID).Scan(&uploadS3Key)
		if err != nil {
			if err == sql.ErrNoRows {
				respondWithError(w, http.StatusNotFound, "mtm record not found")
				return
			}
			log.Printf("[ERROR] GetMTMDownloadURL: failed to fetch mtm record for mtm_id=%s, error=%v", mtmID, err)
			respondWithError(w, http.StatusInternalServerError, "failed to fetch mtm record: "+err.Error())
			return
		}

		s3Key := strings.TrimSpace(uploadS3Key.String)
		if !uploadS3Key.Valid || s3Key == "" {
			respondWithError(w, http.StatusNotFound, "no file available for download")
			return
		}

		downloadURL, err := s3storage.GetDownloadPresignedURL(r.Context(), s3Key, 15*time.Minute)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to generate download url")
			return
		}
		requestedBy := strings.TrimSpace(auditutil.ActorFromContext(r.Context()))
		if requestedBy == "" {
			requestedBy = strings.TrimSpace(auditutil.Actor(req.UserID))
		}
		auditutil.RecordDownload(r.Context(), db, auditutil.DownloadParams{TableName: auditutil.TableForwardMTMDownloads, ParentColumn: "mtm_id", ParentID: mtmID, RequestedBy: requestedBy, UploadS3Key: s3Key, ExtraColumns: nil})

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data": map[string]interface{}{
				"download_url": downloadURL,
			},
		})
	}
}

func GetMTMBulkDownloadURL(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			MTMIDs []string `json:"mtm_ids"`
			UserID string   `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		ids := normalizeBulkIDs(req.MTMIDs)
		if len(ids) == 0 {
			respondWithError(w, http.StatusBadRequest, constants.MTMIDsRequired)
			return
		}

		ctx := r.Context()
		requestedBy := strings.TrimSpace(auditutil.ActorFromContext(ctx))
		if requestedBy == "" {
			requestedBy = strings.TrimSpace(auditutil.Actor(req.UserID))
		}
		files := make([]map[string]string, 0, len(ids))
		failedIDs := make([]string, 0)

		for _, mtmID := range ids {
			var uploadS3Key sql.NullString
			query := `
				SELECT upload_s3_key
				FROM forward_mtm
				WHERE mtm_id = $1`
			if forwardMTMHasIsDeletedColumn(ctx, db) {
				query += `
				  AND COALESCE(is_deleted, false) = false`
			}
			query += `
				LIMIT 1`
			err := db.QueryRowContext(ctx, query, mtmID).Scan(&uploadS3Key)
			if err != nil {
				failedIDs = append(failedIDs, mtmID)
				continue
			}

			s3Key := strings.TrimSpace(uploadS3Key.String)
			if !uploadS3Key.Valid || s3Key == "" {
				failedIDs = append(failedIDs, mtmID)
				continue
			}

			downloadURL, err := s3storage.GetDownloadPresignedURL(ctx, s3Key, 15*time.Minute)
			if err != nil {
				failedIDs = append(failedIDs, mtmID)
				continue
			}

			files = append(files, map[string]string{
				"mtm_id":       mtmID,
				"download_url": downloadURL,
			})
			auditutil.RecordDownload(ctx, db, auditutil.DownloadParams{TableName: auditutil.TableForwardMTMDownloads, ParentColumn: "mtm_id", ParentID: mtmID, RequestedBy: requestedBy, UploadS3Key: s3Key, ExtraColumns: nil})
		}

		writeBulkDownloadResponse(w, files, failedIDs)
	}
}

// Helper functions
func containsString(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}
func str(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
func num(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	default:
		return 0
	}
}
func calcDaysToMaturity(dealDateStr, maturityDateStr string, fallback interface{}) int {
	layout := constants.DateFormat
	dealDate, err1 := time.Parse(layout, dealDateStr)
	maturityDate, err2 := time.Parse(layout, maturityDateStr)
	if err1 == nil && err2 == nil {
		days := int(maturityDate.Sub(dealDate).Hours() / 24)
		return days
	}
	if fallback != nil {
		switch t := fallback.(type) {
		case int:
			return t
		case float64:
			return int(t)
		case string:
			v, _ := strconv.Atoi(t)
			return v
		}
	}
	return 0
}

func normalizeMTMRowValue(v interface{}) interface{} {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return v
	}
}

func collectMTMRows(rows *sql.Rows) []map[string]interface{} {
	cols, _ := rows.Columns()
	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		if err := rows.Scan(valPtrs...); err != nil {
			continue
		}
		rowMap := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			rowMap[col] = normalizeMTMRowValue(vals[i])
		}
		if _, ok := rowMap["processing_status"]; !ok || strings.TrimSpace(fmt.Sprint(rowMap["processing_status"])) == "" {
			rowMap["processing_status"] = normalizeMTMRowValue(rowMap["status"])
		}
		out = append(out, rowMap)
	}
	return out
}

// Handler: GetMTMData - returns MTM data for allowed business units from middleware
func GetMTMData(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
		}
		ct := r.Header.Get(constants.ContentTypeText)
		if strings.HasPrefix(ct, constants.ContentTypeJSON) {
			_ = json.NewDecoder(r.Body).Decode(&req)
		}

		// Get business units from middleware context
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		// Fetch MTM data for allowed business units
		query := `SELECT * FROM forward_mtm WHERE entity = ANY($1)`
		if forwardMTMHasIsDeletedColumn(r.Context(), db) {
			query += ` AND COALESCE(is_deleted, false) = false`
		}
		rows, err := db.Query(query, pq.Array(buNames))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "Failed to fetch MTM data")
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		data := []map[string]interface{}{}
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if err := rows.Scan(valPtrs...); err != nil {
				continue
			}
			rowMap := map[string]interface{}{}
			for i, col := range cols {
				rowMap[col] = normalizeMTMRowValue(vals[i])
			}
			if _, ok := rowMap["processing_status"]; !ok || strings.TrimSpace(fmt.Sprint(rowMap["processing_status"])) == "" {
				rowMap["processing_status"] = normalizeMTMRowValue(rowMap["status"])
			}
			delete(rowMap, "upload_link")
			data = append(data, rowMap)
		}
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data":                 data,
		})
	}
}

func RequestDeleteMTMRecords(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			MTMIDs []string `json:"mtm_ids"`
			Reason string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		if len(req.MTMIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, constants.MTMIDsRequired)
			return
		}
		if !forwardMTMHasIsDeletedColumn(r.Context(), db) {
			respondWithError(w, http.StatusInternalServerError, "forward_mtm soft delete columns are missing")
			return
		}

		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		normalizedIDs := normalizeBulkIDs(req.MTMIDs)
		if len(normalizedIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, constants.MTMIDsRequired)
			return
		}

		statusColumn := mtmApprovalStatusExpr(r.Context(), db)
		rows, err := db.QueryContext(r.Context(), fmt.Sprintf(`
			SELECT mtm_id, entity, COALESCE(%s, '')
			FROM forward_mtm
			WHERE mtm_id = ANY($1)%s
		`, statusColumn, mtmActiveFilterClause(r.Context(), db, "")), pq.Array(normalizedIDs))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to validate mtm rows")
			return
		}

		eligibleIDs := make([]string, 0, len(normalizedIDs))
		oldSnapshots := make(map[string]map[string]interface{})
		for rows.Next() {
			var mtmID, entity, approvalStatus string
			if scanErr := rows.Scan(&mtmID, &entity, &approvalStatus); scanErr != nil {
				continue
			}
			if !containsString(buNames, entity) {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(approvalStatus), constants.StatusPendingDeleteApproval) {
				continue
			}
			eligibleIDs = append(eligibleIDs, mtmID)
			oldSnapshots[mtmID] = auditutil.FetchRowSnapshot(r.Context(), db, "public.forward_mtm", "mtm_id", mtmID)
		}
		rows.Close()

		if len(eligibleIDs) == 0 {
			respondWithError(w, http.StatusNotFound, "No matching active MTM records found")
			return
		}

		setClauses := []string{"status = 'Pending Delete Approval'"}
		if forwardMTMHasProcessingStatusColumn(r.Context(), db) {
			setClauses = append(setClauses, "processing_status = 'PENDING_DELETE_APPROVAL'")
		}
		resultRows, err := db.QueryContext(r.Context(), fmt.Sprintf(`
			UPDATE forward_mtm
			SET %s
			WHERE mtm_id = ANY($1)%s
			RETURNING *
		`, strings.Join(setClauses, ", "), mtmActiveFilterClause(r.Context(), db, "")), pq.Array(eligibleIDs))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to submit mtm delete request")
			return
		}

		cols, _ := resultRows.Columns()
		updated := make([]map[string]interface{}, 0, len(eligibleIDs))
		requestedBy := auditutil.Actor(req.UserID)
		for resultRows.Next() {
			vals := make([]interface{}, len(cols))
			valPtrs := make([]interface{}, len(cols))
			for i := range vals {
				valPtrs[i] = &vals[i]
			}
			if scanErr := resultRows.Scan(valPtrs...); scanErr != nil {
				continue
			}

			rowMap := make(map[string]interface{}, len(cols))
			for i, col := range cols {
				rowMap[col] = normalizeMTMRowValue(vals[i])
			}
			updated = append(updated, rowMap)

			mtmID := strings.TrimSpace(fmt.Sprint(rowMap["mtm_id"]))
			if mtmID == "" || mtmID == "<nil>" {
				continue
			}
			auditutil.RecordAction(r.Context(), db, auditutil.ActionParams{TableName: auditutil.TableForwardMTM, ParentColumn: "mtm_id", ParentID: mtmID, ActionType: "DELETE", Status: constants.StatusPendingDeleteApproval, Reason: req.Reason, RequestedBy: requestedBy, OldValues: oldSnapshots[mtmID], NewValues: rowMap})
		}
		resultRows.Close()

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"updated":              updated,
		})
	}
}

func BulkUpdateMTMProcessingStatus(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID           string   `json:"user_id"`
			MTMIDs           []string `json:"mtm_ids"`
			ProcessingStatus string   `json:"processing_status"`
			ApprovalComment  string   `json:"approval_comment"`
			RejectionComment string   `json:"rejection_comment"`
			Comment          string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.UserID) == "" {
			respondWithError(w, http.StatusBadRequest, constants.ErrUserIDRequired)
			return
		}
		if len(req.MTMIDs) == 0 || (req.ProcessingStatus != "Approved" && req.ProcessingStatus != "Rejected") {
			respondWithError(w, http.StatusBadRequest, "mtm_ids and valid processing_status (Approved/Rejected) are required")
			return
		}

		normalizedIDs := normalizeBulkIDs(req.MTMIDs)
		if len(normalizedIDs) == 0 {
			respondWithError(w, http.StatusBadRequest, constants.MTMIDsRequired)
			return
		}
		buNames, ok := r.Context().Value(api.BusinessUnitsKey).([]string)
		if !ok || len(buNames) == 0 {
			respondWithError(w, http.StatusForbidden, constants.ErrNoAccessibleBusinessUnit)
			return
		}

		statusColumn := mtmApprovalStatusExpr(r.Context(), db)
		rows, err := db.QueryContext(r.Context(), fmt.Sprintf(`
			SELECT mtm_id, entity, COALESCE(%s, '')
			FROM forward_mtm
			WHERE mtm_id = ANY($1)%s
		`, statusColumn, mtmActiveFilterClause(r.Context(), db, "")), pq.Array(normalizedIDs))
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, "failed to validate mtm rows")
			return
		}
		defer rows.Close()

		deletePendingIDs := make([]string, 0)
		regularPendingIDs := make([]string, 0)
		for rows.Next() {
			var mtmID, entity, approvalStatus string
			if scanErr := rows.Scan(&mtmID, &entity, &approvalStatus); scanErr != nil {
				continue
			}
			if !containsString(buNames, entity) {
				continue
			}
			normalizedStatus := strings.ToUpper(strings.TrimSpace(approvalStatus))
			switch normalizedStatus {
			case constants.StatusPendingDeleteApproval:
				deletePendingIDs = append(deletePendingIDs, mtmID)
			case "PENDING", constants.StatusPendingApproval, constants.StatusPendingEditApproval:
				regularPendingIDs = append(regularPendingIDs, mtmID)
			}
		}

		updated := make([]map[string]interface{}, 0, len(normalizedIDs))
		decisionComment := strings.TrimSpace(req.Comment)
		if decisionComment == "" && req.ProcessingStatus == "Approved" {
			decisionComment = strings.TrimSpace(req.ApprovalComment)
		}
		if decisionComment == "" && req.ProcessingStatus == "Rejected" {
			decisionComment = strings.TrimSpace(req.RejectionComment)
		}
		checker := auditutil.Actor(req.UserID)

		if len(deletePendingIDs) > 0 {
			if req.ProcessingStatus == "Approved" {
				if !forwardMTMHasColumn(r.Context(), db, "deleted_by") || !forwardMTMHasColumn(r.Context(), db, "deleted_at") {
					respondWithError(w, http.StatusInternalServerError, "forward_mtm delete audit columns are missing")
					return
				}
				setClauses := []string{
					"is_deleted = TRUE",
					"deleted_by = $2",
					"deleted_at = NOW()",
					"status = 'Deleted'",
				}
				if forwardMTMHasProcessingStatusColumn(r.Context(), db) {
					setClauses = append(setClauses, "processing_status = 'APPROVED'")
				}
				resultRows, updateErr := db.QueryContext(r.Context(), fmt.Sprintf(`
					UPDATE forward_mtm
					SET %s
					WHERE mtm_id = ANY($1)
					  AND COALESCE(is_deleted, false) = false
					RETURNING *
				`, strings.Join(setClauses, ", ")), pq.Array(deletePendingIDs), strings.TrimSpace(req.UserID))
				if updateErr != nil {
					respondWithError(w, http.StatusInternalServerError, "failed to approve mtm delete request")
					return
				}
				updated = append(updated, collectMTMRows(resultRows)...)
				resultRows.Close()
			} else {
				setClauses := []string{"status = 'Rejected'"}
				if forwardMTMHasProcessingStatusColumn(r.Context(), db) {
					setClauses = append(setClauses, "processing_status = 'REJECTED'")
				}
				resultRows, updateErr := db.QueryContext(r.Context(), fmt.Sprintf(`
					UPDATE forward_mtm
					SET %s
					WHERE mtm_id = ANY($1)
					  AND COALESCE(is_deleted, false) = false
					RETURNING *
				`, strings.Join(setClauses, ", ")), pq.Array(deletePendingIDs))
				if updateErr != nil {
					respondWithError(w, http.StatusInternalServerError, "failed to reject mtm delete request")
					return
				}
				updated = append(updated, collectMTMRows(resultRows)...)
				resultRows.Close()
			}

			for _, id := range deletePendingIDs {
				auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableForwardMTM, ParentColumn: "mtm_id", ParentID: id, Status: strings.ToUpper(req.ProcessingStatus), CheckerBy: checker, Comment: decisionComment})
			}
		}

		if len(regularPendingIDs) > 0 {
			setClauses := []string{}
			switch req.ProcessingStatus {
			case "Approved":
				setClauses = append(setClauses, "status = 'Approved'")
				if forwardMTMHasProcessingStatusColumn(r.Context(), db) {
					setClauses = append(setClauses, "processing_status = 'APPROVED'")
				}
			case "Rejected":
				setClauses = append(setClauses, "status = 'Rejected'")
				if forwardMTMHasProcessingStatusColumn(r.Context(), db) {
					setClauses = append(setClauses, "processing_status = 'REJECTED'")
				}
			}
			resultRows, updateErr := db.QueryContext(r.Context(), fmt.Sprintf(`
				UPDATE forward_mtm
				SET %s
				WHERE mtm_id = ANY($1)
				  AND COALESCE(is_deleted, false) = false
				RETURNING *
			`, strings.Join(setClauses, ", ")), pq.Array(regularPendingIDs))
			if updateErr != nil {
				respondWithError(w, http.StatusInternalServerError, "failed to update mtm approval status")
				return
			}
			updated = append(updated, collectMTMRows(resultRows)...)
			resultRows.Close()

			for _, id := range regularPendingIDs {
				auditutil.RecordDecision(r.Context(), db, auditutil.DecisionParams{TableName: auditutil.TableForwardMTM, ParentColumn: "mtm_id", ParentID: id, Status: strings.ToUpper(req.ProcessingStatus), CheckerBy: checker, Comment: decisionComment})
			}
		}

		if len(updated) == 0 {
			respondWithError(w, http.StatusNotFound, "No matching pending MTM rows found")
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"updated":              updated,
		})
	}
}
