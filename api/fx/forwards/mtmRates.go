package forwards

import (
	"CimplrCorpSaas/api"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/xuri/excelize/v2"

	"CimplrCorpSaas/internal/logger")

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
				results = append(results, map[string]interface{}{
					"filename":           fileHeader.Filename,
					constants.ValueError: "No data in Excel file",
				})
				continue
			}
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
			rows, err := db.Query(`SELECT internal_reference_id FROM forward_mtm WHERE internal_reference_id = ANY($1)`, pq.Array(refIds))
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

		if len(validRows) > 0 {
			valueStrings := []string{}
			valueArgs := []interface{}{}
			for i, row := range validRows {
				offset := i*15 + 1
				valueStrings = append(valueStrings, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", offset, offset+1, offset+2, offset+3, offset+4, offset+5, offset+6, offset+7, offset+8, offset+9, offset+10, offset+11, offset+12, offset+13, offset+14))
				valueArgs = append(valueArgs, row...)
				valueArgs = append(valueArgs, s3Key)
			}
			insertQuery := "INSERT INTO forward_mtm (mtm_id, booking_id, deal_date, maturity_date, currency_pair, buy_sell, notional_amount, contract_rate, mtm_rate, mtm_value, days_to_maturity, status, internal_reference_id, entity, upload_s3_key) VALUES " + strings.Join(valueStrings, ",")
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
	}

	return results, nil
}

func GetMTMDownloadURL(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			MTMID    string `json:"mtm_id"`
			RecordID string `json:"record_id"`
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

		var uploadS3Key sql.NullString
		err := db.QueryRowContext(r.Context(), `
			SELECT upload_s3_key
			FROM forward_mtm
			WHERE mtm_id = $1
			LIMIT 1
		`, mtmID).Scan(&uploadS3Key)
		if err != nil {
			if err == sql.ErrNoRows {
				respondWithError(w, http.StatusNotFound, "mtm record not found")
				return
			}
			respondWithError(w, http.StatusInternalServerError, "failed to fetch mtm record")
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
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			respondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONShort)
			return
		}

		ids := normalizeBulkIDs(req.MTMIDs)
		if len(ids) == 0 {
			respondWithError(w, http.StatusBadRequest, "mtm_ids is required")
			return
		}

		ctx := r.Context()
		files := make([]map[string]string, 0, len(ids))
		failedIDs := make([]string, 0)

		for _, mtmID := range ids {
			var uploadS3Key sql.NullString
			err := db.QueryRowContext(ctx, `
				SELECT upload_s3_key
				FROM forward_mtm
				WHERE mtm_id = $1
				LIMIT 1
			`, mtmID).Scan(&uploadS3Key)
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
		rows, err := db.Query(`SELECT * FROM forward_mtm WHERE entity = ANY($1)`, pq.Array(buNames))
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
				rowMap[col] = vals[i]
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
