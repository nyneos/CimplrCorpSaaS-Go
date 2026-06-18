package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/master/mastererrors"
	"CimplrCorpSaas/api/master/bulkuploadaudit"
	"CimplrCorpSaas/api/utils/s3storage"
	dependency "CimplrCorpSaas/internal/dependency"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyDayCountError converts database errors to user-friendly messages
func getUserFriendlyDayCountError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	if msg, ok := mastererrors.TryUniqueViolation(err); ok {
		return msg, http.StatusOK
	}
	errStr := strings.ToLower(err.Error())
	dupKeySubstr := strings.ToLower(constants.ErrDuplicateKeyC)

	if strings.Contains(errStr, strings.ToLower(constants.ErrTxBeginFailed)) ||
		strings.Contains(errStr, strings.ToLower(constants.ErrAuditInsertFailed)) {
		return err.Error(), http.StatusOK
	}

	if strings.Contains(errStr, "uniq_day_count_name_active") {
		return "Duplicate day_count_name: that display name is already used by an active (non-deleted) convention. Change the name or amend the existing one.", http.StatusOK
	}

	if strings.Contains(errStr, "inv_day_count_convention_type_chk") ||
		(strings.Contains(errStr, constants.CheckConstraint) && strings.Contains(errStr, "convention_type")) {
		return "Invalid convention_type. Must be one of: ACT_365, ACT_360, ACT_ACT, 30_360.", http.StatusBadRequest
	}

	if strings.Contains(errStr, "23502") && strings.Contains(errStr, "day_count_code") {
		return "day_count_code is required for each row; include a non-empty code matching DC-[A-Z0-9-]+ in the upload file.", http.StatusBadRequest
	}

	if strings.Contains(errStr, dupKeySubstr) {
		// PostgreSQL includes constraint name and often "Key (column)=(value) already exists" in the detail.
		switch {
		case strings.Contains(errStr, "fd_day_count_convention_master_pkey") ||
			strings.Contains(errStr, "(day_count_code)="):
			return "Duplicate day_count_code: this code is already the primary key of another row. Use a different day_count_code or edit the existing convention.", http.StatusOK
		case strings.Contains(errStr, "uniq_day_count_name_active") ||
			strings.Contains(errStr, "(day_count_name)="):
			return "Duplicate day_count_name: that display name is already used by an active (non-deleted) convention. Change the name or reuse after the other record is removed.", http.StatusOK
		case strings.Contains(errStr, "uniq_day_count_id") || strings.Contains(errStr, "(day_count_id)="):
			return "Duplicate day_count_id: generated ID collision (very rare). Retry the request; if it persists, contact support.", http.StatusOK
		default:
			return "Duplicate key: this row conflicts with a unique constraint on the day count master table. Check day_count_code and day_count_name against existing records.", http.StatusOK
		}
	}

	if strings.Contains(errStr, "foreign key constraint") || strings.Contains(errStr, "violates foreign key") {
		return "Referenced record not found or invalid.", http.StatusOK
	}

	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue", http.StatusServiceUnavailable
	}

	// Commit / tx end failures: surface driver message so uploads aren't opaque "Commit failed".
	if strings.Contains(strings.ToLower(context), "commit") {
		log.Printf("[DAYCOUNT-DB] commit_error context=%q err=%v", context, err)
		return context + ": " + err.Error(), http.StatusInternalServerError
	}

	// Unmapped DB/driver error: log full detail for debugging (client still gets safe message).
	log.Printf("[DAYCOUNT-DB] unmapped_error context=%q err=%v", context, err)
	return "Internal server error: " + context, http.StatusInternalServerError
}

// --- Request Types ---
type DayCountConventionInput struct {
	DayCountCode   string  `json:"day_count_code"`
	DayCountName   string  `json:"day_count_name"`
	ConventionType string  `json:"convention_type"` // ACT_365|ACT_360|ACT_ACT|30_360
	Description    *string `json:"description"`
	FormulaExample *string `json:"formula_example"`
	IsActive       *bool   `json:"is_active"`
}

type CreateDayCountConventionSingleRequest struct {
	UserID string `json:"user_id"`
	DayCountConventionInput
}

type CreateDayCountConventionRequest struct {
	UserID string                    `json:"user_id"`
	Rows   []DayCountConventionInput `json:"rows"`
}

type UpdateDayCountConventionRequest struct {
	UserID       string                 `json:"user_id"`
	DayCountCode string                 `json:"day_count_code"`
	Fields       map[string]interface{} `json:"fields"`
	UsedByBanks  *[]string              `json:"used_by_banks"` // nil = no change; empty slice = clear all
	Reason       string                 `json:"reason"`
}

var validConventionTypes = map[string]bool{
	"ACT_365": true,
	"ACT_360": true,
	"ACT_ACT": true,
	"30_360":  true,
}

var dayCountCodePattern = regexp.MustCompile(`^DC-[A-Z0-9-]+$`)

// normalizeDayCountCode uppercases and validates day_count_code (create + CSV upload).
func normalizeDayCountCode(raw string) (string, error) {
	code := strings.ToUpper(strings.TrimSpace(raw))
	if code == "" {
		return "", errors.New(constants.ErrDayCountCodeRequired)
	}
	if len(code) > 50 {
		return "", errors.New("day_count_code too long")
	}
	if !dayCountCodePattern.MatchString(code) {
		return "", errors.New("day_count_code must match pattern DC-[A-Z0-9-]+")
	}
	return code, nil
}

func validateDayCountFields(input DayCountConventionInput) error {
	if strings.TrimSpace(input.DayCountName) == "" {
		return errors.New("day_count_name is required")
	}
	ct := strings.ToUpper(strings.TrimSpace(input.ConventionType))
	if ct == "" {
		return errors.New("convention_type is required")
	}
	if !validConventionTypes[ct] {
		return errors.New("convention_type must be one of: ACT_365, ACT_360, ACT_ACT, 30_360")
	}
	return nil
}

// bank map table removed from schema; no-op placeholder removed.

// UploadDayCountConventionSimple handles CSV/XLSX upload
func UploadDayCountConventionSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse form: "+err.Error())
			return
		}

		userID := r.FormValue("user_id")
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}

		file, handler, err := r.FormFile("file")
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to get file: "+err.Error())
			return
		}
		fileBytes, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToReadFile+err.Error())
			return
		}
		contentType := s3storage.DetectContentType(fileBytes)

		ext := strings.ToLower(filepath.Ext(handler.Filename))
		if ext != ".csv" && ext != ".xlsx" {
			api.RespondWithError(w, http.StatusBadRequest, "Only .csv and .xlsx files are supported")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		var data [][]string
		if ext == ".csv" {
			data, err = parseCSVFile(newBytesMultipartFile(fileBytes))
		} else {
			data, err = parseXLSXFile(newBytesMultipartFile(fileBytes))
		}
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse file: "+err.Error())
			return
		}

		if len(data) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, "File must have a header row and at least one data row")
			return
		}

		// Normalize header keys (remove non-alphanumeric, lowercase)
		normalize := func(s string) string {
			s = strings.ToLower(strings.TrimSpace(s))
			re := regexp.MustCompile(`[^a-z0-9]`)
			return re.ReplaceAllString(s, "")
		}

		// Build column index map from header row using normalized keys
		header := data[0]
		colMap := make(map[string]int)
		for i, col := range header {
			colMap[normalize(col)] = i
		}

		requiredCols := []string{"day_count_code", "day_count_name", "convention_type"}
		for _, col := range requiredCols {
			if _, ok := colMap[normalize(col)]; !ok {
				api.RespondWithError(w, http.StatusBadRequest, "Missing required column: "+col)
				return
			}
		}

		ctx := r.Context()

		// Pre-validate rows (fail-fast on first error)
		var validInputs []DayCountConventionInput
		var validSourceRows []int
		seenCodesInFile := make(map[string]int) // code -> first data row number (1-based sheet row)
		seenNamesInFile := make(map[string]int) // name -> first data row number

		// helper to fetch by normalized name
		get := func(row []string, col string) string { return getColumnValue(row, colMap, normalize(col)) }

		// helper to send concise fail-fast response
		sendFail := func(row int, errMsg string) {
			summary := fmt.Sprintf("Day count upload aborted: row %d failed validation: %s", row, errMsg)
			api.RespondWithPayload(w, false, summary, nil)
		}

		for i, row := range data[1:] {
			if len(row) == 0 {
				continue
			}
			sheetRow := i + 2
			normCode, codeErr := normalizeDayCountCode(get(row, "day_count_code"))
			if codeErr != nil {
				sendFail(sheetRow, codeErr.Error())
				return
			}
			if prevRow, dup := seenCodesInFile[normCode]; dup {
				sendFail(sheetRow, fmt.Sprintf("duplicate data in file: row %d conflicts with row %d (matching day_count_code: %s)", sheetRow, prevRow, normCode))
				return
			}
			seenCodesInFile[normCode] = sheetRow

			normName := strings.ToUpper(strings.TrimSpace(get(row, "day_count_name")))
			if prevRow, dup := seenNamesInFile[normName]; dup && normName != "" {
				sendFail(sheetRow, fmt.Sprintf("duplicate data in file: row %d conflicts with row %d (matching day_count_name: %s)", sheetRow, prevRow, get(row, "day_count_name")))
				return
			}
			seenNamesInFile[normName] = sheetRow

			input := DayCountConventionInput{
				DayCountCode:   normCode,
				DayCountName:   get(row, "day_count_name"),
				ConventionType: strings.ToUpper(get(row, "convention_type")),
			}

			if desc := get(row, "description"); desc != "" {
				input.Description = &desc
			}
			if fe := get(row, "formula_example"); fe != "" {
				input.FormulaExample = &fe
			}
			if isActiveStr := get(row, "is_active"); isActiveStr != "" {
				bv, bErr := parseBoolPtr(isActiveStr)
				if bErr != nil {
					sendFail(i+2, "Invalid is_active value: "+isActiveStr)
					return
				}
				input.IsActive = bv
			}

			if err := validateDayCountFields(input); err != nil {
				sendFail(sheetRow, err.Error())
				return
			}
			validInputs = append(validInputs, input)
			validSourceRows = append(validSourceRows, sheetRow)
		}

		if len(validInputs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, nil)
			return
		}

		s3Key, storedFileName := "", ""
		if s3storage.IsS3UploadEnabled() {
			folder := s3storage.GetStoragePrefix("master-day-count-convention")
			storedFileName = s3storage.BuildUploadedFilename(handler.Filename, userEmail, time.Now().UTC())
			s3Key = s3storage.BuildNamedS3Key(folder, "", storedFileName)
			if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, constants.ErrFailedToStoreFile+err.Error())
				return
			}
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			if s3Key != "" {
				_ = s3storage.DeleteFromS3(ctx, s3Key)
			}
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
				if s3Key != "" {
					_ = s3storage.DeleteFromS3(ctx, s3Key)
				}
			}
		}()

		// Batch duplicate check by name
		names := make([]string, len(validInputs))
		for i, input := range validInputs {
			names[i] = input.DayCountName
		}
		dupRows, err := tx.Query(ctx, `
			SELECT day_count_name FROM investment.fd_day_count_convention_master
			WHERE day_count_name = ANY($1::text[]) AND COALESCE(is_deleted,false) = false
		`, names)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Duplicate check failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer dupRows.Close()

		existingNames := make(map[string]bool)
		for dupRows.Next() {
			var nm string
			if err := dupRows.Scan(&nm); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Duplicate scan failed")
				return
			}
			existingNames[nm] = true
		}

		codes := make([]string, len(validInputs))
		for i, input := range validInputs {
			codes[i] = input.DayCountCode
		}
		dupCodeRows, err := tx.Query(ctx, `
			SELECT day_count_code FROM investment.fd_day_count_convention_master
			WHERE day_count_code = ANY($1::text[]) AND COALESCE(is_deleted,false) = false
		`, codes)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Duplicate code check failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer dupCodeRows.Close()

		existingCodes := make(map[string]bool)
		for dupCodeRows.Next() {
			var c string
			if err := dupCodeRows.Scan(&c); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Duplicate code scan failed")
				return
			}
			existingCodes[c] = true
		}

		// Fail-fast on first duplicate name or duplicate code in DB
		var finalValid []DayCountConventionInput
		for i, input := range validInputs {
			src := 0
			if i < len(validSourceRows) {
				src = validSourceRows[i]
			}
			if existingNames[input.DayCountName] {
				api.RespondWithPayload(w, false, fmt.Sprintf("Day count upload aborted: row %d failed validation: Day count convention name already exists and is active.", src), nil)
				return
			}
			if existingCodes[input.DayCountCode] {
				api.RespondWithPayload(w, false, fmt.Sprintf("Day count upload aborted: row %d failed validation: day_count_code already exists.", src), nil)
				return
			}
			finalValid = append(finalValid, input)
		}

		// Batch insert (day_count_code is NOT NULL in DB; must match single-create INSERT shape)
		valueStrings := make([]string, len(finalValid))
		valueArgs := make([]interface{}, 0, len(finalValid)*6)

		for i, input := range finalValid {
			isActive := true
			if input.IsActive != nil {
				isActive = *input.IsActive
			}
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", i*6+1, i*6+2, i*6+3, i*6+4, i*6+5, i*6+6)
			valueArgs = append(valueArgs, input.DayCountCode, input.DayCountName, input.ConventionType, input.Description, input.FormulaExample, isActive)
		}

		batchInsertQuery := fmt.Sprintf(`
			INSERT INTO investment.fd_day_count_convention_master
				(day_count_code, day_count_name, convention_type, description, formula_example, is_active)
			VALUES %s
		`, strings.Join(valueStrings, ","))

		if _, err := tx.Exec(ctx, batchInsertQuery, valueArgs...); err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		insertedCodes := make([]string, len(finalValid))
		var insertedRecords []map[string]interface{}
		for i, input := range finalValid {
			insertedCodes[i] = input.DayCountCode
			insertedRecords = append(insertedRecords, map[string]interface{}{
				constants.ValueSuccess: true,
				"day_count_code":       input.DayCountCode,
			})
		}

		// Batch audit insert
		if len(insertedCodes) > 0 {
			auditValues := make([]string, len(insertedCodes))
			auditArgs := make([]interface{}, 0, len(insertedCodes)*2)
			for i, code := range insertedCodes {
				auditValues[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, code, userEmail)
			}
			auditQuery := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_day_count_convention
					(day_count_code, action_type, processing_status, requested_by, requested_at)
				VALUES %s
			`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQuery, auditArgs...); err != nil {
				msg, status := getUserFriendlyDayCountError(err, "Audit insert failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if s3Key != "" && len(insertedCodes) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE investment.fd_day_count_convention_master SET upload_s3_key = $1 WHERE day_count_code = ANY($2)`, s3Key, insertedCodes); err != nil {
				log.Printf("[DAYCOUNT-UPLOAD] failed to store upload_s3_key: %v", err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			log.Printf("[DAYCOUNT-UPLOAD] COMMIT failed file=%q rows_inserted=%d err=%v", handler.Filename, len(insertedCodes), err)
			msg, status := getUserFriendlyDayCountError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}
		committed = true
		bulkuploadaudit.Record(ctx, pgxPool, bulkuploadaudit.Entry{
			ModuleKey:        "master-day-count-convention",
			OriginalFileName: handler.Filename,
			StoredFileName:   storedFileName,
			UploadS3Key:      s3Key,
			ContentType:      contentType,
			FileSize:         int64(len(fileBytes)),
			TotalRows:        len(data) - 1,
			InsertedCount:    len(insertedRecords),
			ErrorCount:       (len(data) - 1) - len(insertedRecords),
			Status:           bulkuploadaudit.StatusFor(len(insertedRecords), (len(data)-1)-len(insertedRecords)),
			UploadedBy:       userEmail,
			UploadedAt:       time.Now().UTC(),
		})

		if len(insertedRecords) > 0 {
			msg := fmt.Sprintf("%d day count conventions created successfully", len(insertedRecords))
			api.RespondWithPayload(w, true, msg, nil)
			api.LogInfo("DayCount upload: %d inserted from file %s", len(insertedRecords), handler.Filename)
			return
		}
		api.RespondWithPayload(w, false, "No records were created", nil)
		api.LogInfo("DayCount upload: 0 inserted from file %s", handler.Filename)
	}
}

// CreateDayCountConventionSingle creates a single day count convention record
func CreateDayCountConventionSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateDayCountConventionSingleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		req.ConventionType = strings.ToUpper(strings.TrimSpace(req.ConventionType))

		normCode, err := normalizeDayCountCode(req.DayCountCode)
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}
		req.DayCountCode = normCode

		if err := validateDayCountFields(req.DayCountConventionInput); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			return
		}

		if req.IsActive == nil {
			t := true
			req.IsActive = &t
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		var dayCountCode string
		var dayCountID interface{}
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_day_count_convention_master
				(day_count_code, day_count_name, convention_type, description, formula_example, is_active)
			VALUES ($1,$2,$3,$4,$5,$6)
			RETURNING day_count_code, day_count_id
		`, req.DayCountCode, req.DayCountName, req.ConventionType, req.Description, req.FormulaExample, req.IsActive).Scan(&dayCountCode, &dayCountID)
		if err != nil {
			log.Printf("[DAYCOUNT-CREATE] master INSERT failed user_id=%q code=%q name=%q convention_type=%q is_active=%v desc_nil=%v formula_nil=%v err=%v",
				req.UserID, req.DayCountCode, req.DayCountName, req.ConventionType, req.IsActive,
				req.Description == nil, req.FormulaExample == nil, err)
			msg, status := getUserFriendlyDayCountError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// bank map removed from schema; no-op

		// Audit record
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_day_count_convention
				(day_count_code, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, dayCountCode, userEmail); err != nil {
			log.Printf("[DAYCOUNT-CREATE] audit INSERT failed day_count_code=%q requested_by=%q err=%v", dayCountCode, userEmail, err)
			msg, status := getUserFriendlyDayCountError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			log.Printf("[DAYCOUNT-CREATE] COMMIT failed day_count_code=%q err=%v", dayCountCode, err)
			msg, status := getUserFriendlyDayCountError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"day_count_code":       dayCountCode,
			"day_count_id":         dayCountID,
			"day_count_name":       req.DayCountName,
			"requested_by":         userEmail,
		})
		api.LogInfo("DayCount created: code=%s", dayCountCode)
	}
}

// CreateDayCountConvention handles bulk creation
func CreateDayCountConvention(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateDayCountConventionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		var validRows []DayCountConventionInput
		var errResults []map[string]interface{}
		codeRe := regexp.MustCompile(`^DC-[A-Z0-9-]+$`)
		batchCodes := make(map[string]bool)

		for i, row := range req.Rows {
			row.DayCountCode = strings.ToUpper(strings.TrimSpace(row.DayCountCode))
			row.ConventionType = strings.ToUpper(strings.TrimSpace(row.ConventionType))
			if row.IsActive == nil {
				t := true
				row.IsActive = &t
			}
			if row.DayCountCode == "" {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   "Missing day_count_code",
				})
				continue
			}
			if len(row.DayCountCode) > 50 || !codeRe.MatchString(row.DayCountCode) {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   "Invalid day_count_code",
				})
				continue
			}
			if batchCodes[row.DayCountCode] {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   "Duplicate day_count_code in payload",
				})
				continue
			}
			batchCodes[row.DayCountCode] = true

			if err := validateDayCountFields(row); err != nil {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   err.Error(),
				})
			} else {
				validRows = append(validRows, row)
			}
		}

		if len(validRows) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errResults)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		codes := make([]string, len(validRows))
		for i, r := range validRows {
			codes[i] = r.DayCountCode
		}
		dupRows, err := tx.Query(ctx, `
			SELECT day_count_code FROM investment.fd_day_count_convention_master
			WHERE day_count_code = ANY($1::text[]) AND COALESCE(is_deleted,false) = false
		`, codes)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Duplicate check failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer dupRows.Close()

		existingCodes := make(map[string]bool)
		for dupRows.Next() {
			var nm string
			if err := dupRows.Scan(&nm); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Duplicate scan failed")
				return
			}
			existingCodes[nm] = true
		}
		dupRows.Close()

		var finalValid []DayCountConventionInput
		for i, input := range validRows {
			if existingCodes[input.DayCountCode] {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					"day_count_code":       input.DayCountCode,
					constants.ValueSuccess: false,
					constants.ValueError:   "Day count code already exists.",
				})
			} else {
				finalValid = append(finalValid, input)
			}
		}

		if len(finalValid) == 0 {
			api.RespondWithPayload(w, false, "All rows are duplicates", errResults)
			return
		}

		valueStrings := make([]string, len(finalValid))
		valueArgs := make([]interface{}, 0, len(finalValid)*6)
		for i, input := range finalValid {
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d)", i*6+1, i*6+2, i*6+3, i*6+4, i*6+5, i*6+6)
			valueArgs = append(valueArgs, input.DayCountCode, input.DayCountName, input.ConventionType, input.Description, input.FormulaExample, input.IsActive)
		}

		batchInsertQuery := fmt.Sprintf(`
			INSERT INTO investment.fd_day_count_convention_master
				(day_count_code, day_count_name, convention_type, description, formula_example, is_active)
			VALUES %s RETURNING day_count_code, day_count_id
		`, strings.Join(valueStrings, ","))

		insertRows, err := tx.Query(ctx, batchInsertQuery, valueArgs...)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer insertRows.Close()

		var insertedCodes []string
		var insertedRecords []map[string]interface{}

		for insertRows.Next() {
			var code string
			var id interface{}
			if err := insertRows.Scan(&code, &id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				return
			}
			insertedCodes = append(insertedCodes, code)
			insertedRecords = append(insertedRecords, map[string]interface{}{
				constants.ValueSuccess: true,
				"day_count_code":       code,
				"day_count_id":         id,
			})
		}
		insertRows.Close()

		if len(insertedCodes) > 0 {
			auditValues := make([]string, len(insertedCodes))
			auditArgs := make([]interface{}, 0, len(insertedCodes)*2)
			for i, code := range insertedCodes {
				auditValues[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, code, userEmail)
			}
			auditQ := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_day_count_convention
					(day_count_code, action_type, processing_status, requested_by, requested_at)
				VALUES %s
			`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyDayCountError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		allResults := append(insertedRecords, errResults...)
		api.RespondWithPayload(w, len(insertedRecords) > 0, "", allResults)
		api.LogInfo("DayCount bulk create: %d inserted, %d errors", len(insertedRecords), len(errResults))
	}
}

// UpdateDayCountConvention handles single-record update
func UpdateDayCountConvention(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateDayCountConventionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.DayCountCode == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrDayCountCodeRequired)
			return
		}
		if len(req.Fields) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No fields provided for update")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrTxStartFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Fetch existing row
		var oldName, oldType string
		var oldDesc, oldFormula *string
		var oldIsActive bool
		err = tx.QueryRow(ctx, `
			SELECT day_count_name, convention_type, description, formula_example, is_active
			FROM investment.fd_day_count_convention_master
			WHERE day_count_code=$1 FOR UPDATE
		`, req.DayCountCode).Scan(&oldName, &oldType, &oldDesc, &oldFormula, &oldIsActive)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		fieldPairs := map[string]int{
			"day_count_name":  0,
			"convention_type": 1,
			"description":     2,
			"formula_example": 3,
			"is_active":       4,
		}
		oldVals := []interface{}{oldName, oldType, oldDesc, oldFormula, oldIsActive}

		if len(req.Fields) > 0 {
			var sets []string
			var args []interface{}
			pos := 1
			for k, v := range req.Fields {
				k = strings.ToLower(k)
				if _, ok := fieldPairs[k]; ok {
					sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, k, pos))
					args = append(args, v)
					pos++
				}
			}
			if len(sets) > 0 {
				q := fmt.Sprintf("UPDATE investment.fd_day_count_convention_master SET %s WHERE day_count_code=$%d",
					strings.Join(sets, ", "), pos)
				args = append(args, req.DayCountCode)
				if _, err := tx.Exec(ctx, q, args...); err != nil {
					msg, status := getUserFriendlyDayCountError(err, constants.ErrUpdateFailed)
					api.RespondWithError(w, status, msg)
					return
				}
			}
		}

		// bank map removed from schema; skipping bank map sync

		// Build audit record
		auditCols := []string{"day_count_code", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
		auditVals := []interface{}{req.DayCountCode, "EDIT", constants.StatusPendingEditApproval, req.Reason, userEmail}
		auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
		paramPos := 6

		for k := range req.Fields {
			k = strings.ToLower(k)
			if idx, ok := fieldPairs[k]; ok {
				auditCols = append(auditCols, "old_"+k)
				auditVals = append(auditVals, oldVals[idx])
				auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
				paramPos++
			}
		}

		auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_day_count_convention (%s) VALUES (%s)",
			strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
		if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"day_count_code":       req.DayCountCode,
			"requested_by":         userEmail,
		})
		api.LogInfo("DayCount updated: code=%s", req.DayCountCode)
	}
}

// UpdateDayCountConventionBulk handles bulk update
func UpdateDayCountConventionBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				DayCountCode string                 `json:"day_count_code"`
				Fields       map[string]interface{} `json:"fields"`
				Reason       string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.Rows) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No rows provided")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()

		type validUpdate struct {
			DayCountCode string
			Fields       map[string]interface{}
			Reason       string
		}

		var validUpdates []validUpdate
		var errResults []map[string]interface{}
		allCodes := make([]string, 0, len(req.Rows))

		for i, row := range req.Rows {
			if row.DayCountCode == "" {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   "Missing day_count_code",
				})
				continue
			}
			if len(row.Fields) == 0 {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					"day_count_code":       row.DayCountCode,
					constants.ValueSuccess: false,
					constants.ValueError:   "No fields to update",
				})
				continue
			}
			validUpdates = append(validUpdates, validUpdate{row.DayCountCode, row.Fields, row.Reason})
			allCodes = append(allCodes, row.DayCountCode)
		}

		if len(validUpdates) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errResults)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Fetch all old values
		oldRows, err := tx.Query(ctx, `
			SELECT day_count_code, day_count_name, convention_type, description, formula_example, is_active
			FROM investment.fd_day_count_convention_master
			WHERE day_count_code = ANY($1::text[])
		`, allCodes)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Fetch old values failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer oldRows.Close()

		type oldRecord struct {
			Name, Type    string
			Desc, Formula *string
			IsActive      bool
		}
		oldMap := make(map[string]oldRecord)
		for oldRows.Next() {
			var code string
			var rec oldRecord
			if err := oldRows.Scan(&code, &rec.Name, &rec.Type, &rec.Desc, &rec.Formula, &rec.IsActive); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Old values scan failed: "+err.Error())
				return
			}
			oldMap[code] = rec
		}
		oldRows.Close()

		fieldPairs := map[string]int{
			"day_count_name":  0,
			"convention_type": 1,
			"description":     2,
			"formula_example": 3,
			"is_active":       4,
		}

		var successResults []map[string]interface{}

		for _, update := range validUpdates {
			old, exists := oldMap[update.DayCountCode]
			if !exists {
				errResults = append(errResults, map[string]interface{}{
					"day_count_code":       update.DayCountCode,
					constants.ValueSuccess: false,
					constants.ValueError:   "Record not found",
				})
				continue
			}

			oldVals := []interface{}{old.Name, old.Type, old.Desc, old.Formula, old.IsActive}

			if len(update.Fields) > 0 {
				var sets []string
				var args []interface{}
				pos := 1
				for k, v := range update.Fields {
					k = strings.ToLower(k)
					if _, ok := fieldPairs[k]; ok {
						sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, k, pos))
						args = append(args, v)
						pos++
					}
				}
				if len(sets) > 0 {
					q := fmt.Sprintf("UPDATE investment.fd_day_count_convention_master SET %s WHERE day_count_code=$%d",
						strings.Join(sets, ", "), pos)
					args = append(args, update.DayCountCode)
					if _, err := tx.Exec(ctx, q, args...); err != nil {
						errResults = append(errResults, map[string]interface{}{
							"day_count_code":       update.DayCountCode,
							constants.ValueSuccess: false,
							constants.ValueError:   constants.ErrUpdateFailed + err.Error(),
						})
						continue
					}
				}
			}

			// bank map removed from schema; skipping bank map updates

			// Audit
			auditCols := []string{"day_count_code", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
			auditVals := []interface{}{update.DayCountCode, "EDIT", constants.StatusPendingEditApproval, update.Reason, userEmail}
			auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
			paramPos := 6
			for k := range update.Fields {
				k = strings.ToLower(k)
				if idx, ok := fieldPairs[k]; ok {
					auditCols = append(auditCols, "old_"+k)
					auditVals = append(auditVals, oldVals[idx])
					auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
					paramPos++
				}
			}
			auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_day_count_convention (%s) VALUES (%s)",
				strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
			if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
				errResults = append(errResults, map[string]interface{}{
					"day_count_code":       update.DayCountCode,
					constants.ValueSuccess: false,
					constants.ValueError:   "Audit failed: " + err.Error(),
				})
				continue
			}

			successResults = append(successResults, map[string]interface{}{
				constants.ValueSuccess: true,
				"day_count_code":       update.DayCountCode,
			})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		allResults := append(successResults, errResults...)
		api.RespondWithPayload(w, len(successResults) > 0, "", allResults)
		api.LogInfo("DayCount bulk update: %d updated, %d errors", len(successResults), len(errResults))
	}
}

// DeleteDayCountConvention creates delete requests (pending approval)
func DeleteDayCountConvention(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			DayCountCodes []string `json:"day_count_codes"`
			Reason        string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.DayCountCodes) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoDayCountCodesProvided)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		verifyRows, err := tx.Query(ctx, `
			SELECT day_count_code FROM investment.fd_day_count_convention_master
			WHERE day_count_code = ANY($1::text[]) AND COALESCE(is_deleted,false) = false
		`, req.DayCountCodes)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Verification failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer verifyRows.Close()

		var validCodes []string
		for verifyRows.Next() {
			var code string
			if err := verifyRows.Scan(&code); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				return
			}
			validCodes = append(validCodes, code)
		}
		verifyRows.Close()

		// Block delete request if any day count convention is referenced by live FD bookings
		var deleteBlockers []map[string]interface{}
		for _, code := range validCodes {
			blockers, _ := dependency.HasCoreBelow(ctx, pgxPool, "fd_day_count_convention_master", code)
			if len(blockers) > 0 {
				deleteBlockers = append(deleteBlockers, map[string]interface{}{
					"day_count_code": code,
					"blocked_by":     dependency.BlockersSummary(blockers),
				})
			}
		}
		if len(deleteBlockers) > 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   "Cannot request deletion: one or more items are referenced by live FD bookings.",
				"blocked": deleteBlockers,
			})
			return
		}

		if len(validCodes) > 0 {
			auditValues := make([]string, len(validCodes))
			auditArgs := make([]interface{}, 0, len(validCodes)*3)
			for i, code := range validCodes {
				auditValues[i] = fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", i*3+1, i*3+2, i*3+3)
				auditArgs = append(auditArgs, code, userEmail, req.Reason)
			}
			auditQ := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_day_count_convention
					(day_count_code, action_type, processing_status, requested_by, reason, requested_at)
				VALUES %s
			`, strings.Join(auditValues, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyDayCountError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		validSet := make(map[string]bool)
		for _, c := range validCodes {
			validSet[c] = true
		}
		var results []map[string]interface{}
		for _, c := range req.DayCountCodes {
			if validSet[c] {
				results = append(results, map[string]interface{}{constants.ValueSuccess: true, "day_count_code": c})
			} else {
				results = append(results, map[string]interface{}{constants.ValueSuccess: false, "day_count_code": c, constants.ValueError: "Not found or already deleted"})
			}
		}
		api.RespondWithPayload(w, len(validCodes) > 0, "", results)
		api.LogInfo("DayCount delete requested: %d valid, %d total", len(validCodes), len(req.DayCountCodes))
	}
}

// BulkApproveDayCountConvention approves pending day count conventions
func BulkApproveDayCountConvention(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			DayCountCodes []string `json:"day_count_codes"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.DayCountCodes) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoDayCountCodesProvided)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_day_count_convention
			SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE day_count_code = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.DayCountCodes)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Approval failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Dependency check: revert DELETE approval for day counts referenced by live FD bookings.
		var blockedItems []map[string]interface{}
		{
			checkRows, _ := tx.Query(ctx, `SELECT DISTINCT day_count_code FROM investment.fd_audit_day_count_convention WHERE day_count_code = ANY($1::text[]) AND action_type='DELETE' AND processing_status='APPROVED'`, req.DayCountCodes)
			var deleteApprovedCodes []string
			if checkRows != nil {
				for checkRows.Next() {
					var code string
					if checkRows.Scan(&code) == nil {
						deleteApprovedCodes = append(deleteApprovedCodes, code)
					}
				}
				checkRows.Close()
			}
			for _, code := range deleteApprovedCodes {
				blockers, _ := dependency.HasCoreBelow(ctx, pgxPool, "fd_day_count_convention_master", code)
				if len(blockers) > 0 {
					_, _ = tx.Exec(ctx, `UPDATE investment.fd_audit_day_count_convention SET processing_status='PENDING_DELETE_APPROVAL', checker_by=NULL, checker_at=NULL, checker_comment=NULL WHERE day_count_code=$1 AND action_type='DELETE' AND processing_status='APPROVED'`, code)
					blockedItems = append(blockedItems, map[string]interface{}{"day_count_code": code, "blocked_by": dependency.BlockersSummary(blockers)})
				}
			}
		}

		// Handle DELETE approvals - set is_deleted=true (blocked ones were reverted above)
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_day_count_convention_master
			SET is_deleted=true
			WHERE day_count_code IN (
				SELECT DISTINCT a.day_count_code
				FROM investment.fd_audit_day_count_convention a
				WHERE a.day_count_code = ANY($1::text[])
				  AND a.action_type='DELETE'
				  AND a.processing_status='APPROVED'
			)
		`, req.DayCountCodes)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Delete execution failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"approved_count":       len(req.DayCountCodes) - len(blockedItems),
			"checker":              userEmail,
			"blocked":              blockedItems,
		})
		api.LogInfo("DayCount bulk approve: %d approved by %s", len(req.DayCountCodes)-len(blockedItems), userEmail)
	}
}

// BulkRejectDayCountConvention rejects pending day count conventions
func BulkRejectDayCountConvention(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID        string   `json:"user_id"`
			DayCountCodes []string `json:"day_count_codes"`
			Comment       string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.DayCountCodes) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoDayCountCodesProvided)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_day_count_convention
			SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2
			WHERE day_count_code = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.DayCountCodes)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Rejection failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			constants.ValueSuccess: true,
			"rejected_count":       len(req.DayCountCodes),
			"checker":              userEmail,
		})
		api.LogInfo("DayCount bulk reject: %d rejected by %s", len(req.DayCountCodes), userEmail)
	}
}

// GetDayCountConventionsApprovedActive returns active approved day count conventions
func GetDayCountConventionsApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT m.day_count_id, m.day_count_code, m.day_count_name, m.convention_type,
				   COALESCE(m.description,'') AS description,
				   COALESCE(m.formula_example,'') AS formula_example,
				   m.is_active
			FROM investment.fd_day_count_convention_master m
			INNER JOIN investment.fd_audit_day_count_convention a ON a.day_count_code = m.day_count_code
			WHERE a.processing_status='APPROVED' AND a.action_type IN ('CREATE','EDIT','DELETE') AND m.is_active=true AND COALESCE(m.is_deleted,false)=false
			ORDER BY m.day_count_name
		`)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)
		for rows.Next() {
			var id interface{}
			var code, name, convType, desc, formula string
			var isActive bool

			if err := rows.Scan(&id, &code, &name, &convType, &desc, &formula, &isActive); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				return
			}
			out = append(out, map[string]interface{}{
				"day_count_id":    id,
				"day_count_code":  code,
				"day_count_name":  name,
				"convention_type": convType,
				"description":     desc,
				"formula_example": formula,
				"is_active":       isActive,
			})
		}
		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("DayCount approved-active: returned %d records", len(out))
	}
}

// GetDayCountConventionsWithAudit returns all non-deleted day count conventions with latest audit info
func GetDayCountConventionsWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.day_count_code)
					a.day_count_code,
					a.processing_status,
					a.action_type,
					a.audit_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason,
					a.old_day_count_name,
					a.old_convention_type,
					a.old_description,
					a.old_formula_example,
					a.old_is_active
				FROM investment.fd_audit_day_count_convention a
				WHERE a.action_type IN ('CREATE','EDIT','DELETE')
				ORDER BY a.day_count_code,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT
					day_count_code,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS deleted_at
				FROM investment.fd_audit_day_count_convention
				GROUP BY day_count_code
			)
			SELECT
				m.day_count_id,
				m.day_count_code,
				COALESCE(m.upload_s3_key,'')          AS upload_s3_key,
				COALESCE(m.day_count_name,'')         AS day_count_name,
				COALESCE(l.old_day_count_name,'')     AS old_day_count_name,
				COALESCE(m.convention_type,'')        AS convention_type,
				COALESCE(l.old_convention_type,'')    AS old_convention_type,
				COALESCE(m.description,'')            AS description,
				COALESCE(l.old_description,'')        AS old_description,
				COALESCE(m.formula_example,'')        AS formula_example,
				COALESCE(l.old_formula_example,'')    AS old_formula_example,
				COALESCE(m.is_active,false)           AS is_active,
				COALESCE(l.old_is_active,false)       AS old_is_active,
				COALESCE(m.is_deleted,false)          AS is_deleted,
				COALESCE(m.day_count_id::text,'')     AS day_count_id_text,

				COALESCE(l.processing_status,'')      AS processing_status,
				COALESCE(l.action_type,'')            AS action_type,
				COALESCE(l.audit_id::text,'')         AS audit_id,
				COALESCE(l.requested_by,'')           AS requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(l.checker_by,'')             AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"')   AS checker_at,
				COALESCE(l.checker_comment,'')        AS checker_comment,
				COALESCE(l.reason,'')                 AS reason,

				COALESCE(h.created_by,'')             AS created_by,
				COALESCE(h.created_at,'')             AS created_at,
				COALESCE(h.edited_by,'')              AS edited_by,
				COALESCE(h.edited_at,'')              AS edited_at,
				COALESCE(h.deleted_by,'')             AS deleted_by,
				COALESCE(h.deleted_at,'')             AS deleted_at

			FROM investment.fd_day_count_convention_master m
			LEFT JOIN latest_audit l ON l.day_count_code = m.day_count_code
			LEFT JOIN history h      ON h.day_count_code = m.day_count_code
			WHERE COALESCE(m.is_deleted,false) = false
			GROUP BY
				m.day_count_code, m.day_count_name, l.old_day_count_name,
				m.convention_type, l.old_convention_type,
				m.description, l.old_description,
				m.formula_example, l.old_formula_example,
				m.is_active, l.old_is_active, m.is_deleted, m.upload_s3_key,
				l.processing_status, l.action_type, l.audit_id,
				l.requested_by, l.requested_at, l.checker_by, l.checker_at,
				l.checker_comment, l.reason,
				h.created_by, h.created_at, h.edited_by, h.edited_at,
				h.deleted_by, h.deleted_at
			ORDER BY GREATEST(COALESCE(l.requested_at,'1970-01-01'::timestamp), COALESCE(l.checker_at,'1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)

		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Row value error: "+err.Error())
				return
			}
			row := make(map[string]interface{}, len(fields))
			for i, f := range fields {
				if vals[i] == nil {
					row[string(f.Name)] = ""
				} else {
					row[string(f.Name)] = vals[i]
				}
			}
			out = append(out, row)
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row scan error: "+rows.Err().Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"rows":                 out,
		})
		api.LogInfo("DayCount WithAudit: returned %d records", len(out))
	}
}

// GetDayCountConvention returns a single day count convention by code
func GetDayCountConvention(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string `json:"user_id"`
			DayCountCode string `json:"day_count_code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if req.DayCountCode == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrDayCountCodeRequired)
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT m.day_count_id, m.day_count_code, m.day_count_name, m.convention_type,
				   COALESCE(m.description,'') AS description,
				   COALESCE(m.formula_example,'') AS formula_example,
				   m.is_active, COALESCE(m.is_deleted,false) AS is_deleted
			FROM investment.fd_day_count_convention_master m
			WHERE m.day_count_code = $1
		`, req.DayCountCode)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Get failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		if !rows.Next() {
			api.RespondWithError(w, http.StatusNotFound, "Day count convention not found")
			return
		}

		var id interface{}
		var code, name, convType, desc, formula string
		var isActive, isDeleted bool

		if err := rows.Scan(&id, &code, &name, &convType, &desc, &formula, &isActive, &isDeleted); err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Scan failed")
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"day_count_id":    id,
			"day_count_code":  code,
			"day_count_name":  name,
			"convention_type": convType,
			"description":     desc,
			"formula_example": formula,
			"is_active":       isActive,
			"is_deleted":      isDeleted,
		})
		api.LogInfo("GetDayCountConvention: code=%s by %s", code, userEmail)
	}
}
