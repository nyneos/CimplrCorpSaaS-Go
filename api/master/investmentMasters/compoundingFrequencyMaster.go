package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/master/bulkuploadaudit"
	"CimplrCorpSaas/api/utils/s3storage"
	dependency "CimplrCorpSaas/internal/dependency"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyCompoundingFrequencyError converts database errors to user-friendly messages for compounding frequencies
func getUserFriendlyCompoundingFrequencyError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	errStr := strings.ToLower(err.Error())

	// Known errors - Business logic error (200 OK)
	if strings.Contains(errStr, constants.ErrTxBeginFailed) ||
		strings.Contains(errStr, constants.ErrCommitFailedCapitalized) ||
		strings.Contains(errStr, constants.ErrAuditInsertFailed) {
		return err.Error(), http.StatusOK
	}

	// Compounding Frequency-specific constraint violations - Business logic error (200 OK)
	if strings.Contains(errStr, "uniq_frequency_code_active") {
		return "Frequency code already exists. Please use a different code.", http.StatusOK
	}

	if strings.Contains(errStr, "uniq_frequency_name_active") {
		return "Frequency name already exists. Please use a different name.", http.StatusOK
	}

	if strings.Contains(errStr, "fd_frequency_periods_chk") {
		return "Compounding periods per year must be greater than 0.", http.StatusOK
	}

	if strings.Contains(errStr, "fd_frequency_type_chk") {
		return "Invalid frequency type. Must be DAILY, MONTHLY, QUARTERLY, HALF_YEARLY, ANNUAL, or MATURITY.", http.StatusOK
	}

	// Standard database constraint errors
	if strings.Contains(errStr, "unique constraint") || strings.Contains(errStr, "duplicate key") {
		return "Duplicate entry detected. Please check for existing records.", http.StatusOK
	}
	if strings.Contains(errStr, "foreign key constraint") || strings.Contains(errStr, "violates foreign key") {
		return "Referenced record not found or is invalid.", http.StatusOK
	}
	if strings.Contains(errStr, constants.CheckConstraint) {
		return "Invalid data format or value.", http.StatusOK
	}
	if strings.Contains(errStr, "not-null constraint") || strings.Contains(errStr, "null value") {
		return "Required field is missing.", http.StatusOK
	}

	// Connection/timeout errors
	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "no response") || strings.Contains(errStr, "network") {
		return "Database connection issue", http.StatusServiceUnavailable
	}

	// Query errors
	if strings.Contains(errStr, "no rows") || strings.Contains(errStr, "not found") {
		return "Record not found", http.StatusOK
	}

	// Fallback for unexpected errors
	return "Internal server error: " + context, http.StatusInternalServerError
}

// --- Compounding Frequency: Active Approved ---
func GetCompoundingFrequenciesApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			SELECT
				m.frequency_id,
				m.frequency_code,
				m.frequency_name,
				m.frequency_type,
				m.compounding_periods_per_year,
				m.days_per_period,
				m.description,
				m.is_active
			FROM investment.fd_compounding_frequency_master m
			INNER JOIN investment.fd_audit_compounding_frequency a ON a.frequency_id = m.frequency_id
			WHERE a.processing_status = 'APPROVED'
			  AND a.action_type IN ('CREATE','EDIT','DELETE')
				AND m.is_active = true
				AND COALESCE(m.is_deleted, false) = false
			ORDER BY m.frequency_name
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)

		for rows.Next() {
			var id, code, name, ftype, desc string
			var periods int
			var days *int
			var isActive bool

			if err := rows.Scan(&id, &code, &name, &ftype, &periods, &days, &desc, &isActive); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				api.LogError("Database scan error in GetCompoundingFrequenciesApprovedActive: %v", err)
				return
			}

			out = append(out, map[string]interface{}{
				"frequency_id":                 id,
				"frequency_code":               code,
				"frequency_name":               name,
				"frequency_type":               ftype,
				"compounding_periods_per_year": periods,
				"days_per_period":              days,
				"description":                  desc,
				"is_active":                    isActive,
			})
		}

		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("Retrieved %d active approved compounding frequencies", len(out))
	}
}

// validateCompoundingFrequencyMap validates fields in a generic input map and returns
// an error message string (empty if valid). This is used by bulk and upload flows.
func validateCompoundingFrequencyMap(m map[string]interface{}) string {
	// code
	code, _ := m["frequency_code"].(string)
	name, _ := m["frequency_name"].(string)
	ftype, _ := m["frequency_type"].(string)

	// frequency_code: required, pattern ^FREQ-[A-Za-z-]+$, max 50
	if strings.TrimSpace(code) == "" {
		return "frequency_code is required"
	}
	if len(code) > 50 {
		return "frequency_code max length is 50"
	}
	codeRe := regexp.MustCompile(`^FREQ-[A-Za-z-]+$`)
	if !codeRe.MatchString(code) {
		return "frequency_code must match pattern FREQ-[A-Za-z-]+"
	}

	// frequency_name: required, max 100
	if strings.TrimSpace(name) == "" {
		return "frequency_name is required"
	}
	if len(name) > 100 {
		return "frequency_name max length is 100"
	}

	// frequency_type: required, enum
	if strings.TrimSpace(ftype) == "" {
		return "frequency_type is required"
	}
	ftype = strings.ToUpper(ftype)
	allowed := map[string]bool{"DAILY": true, "MONTHLY": true, "QUARTERLY": true, "HALF_YEARLY": true, "ANNUAL": true, "CUSTOM": true}
	if !allowed[ftype] {
		return "frequency_type must be one of DAILY, MONTHLY, QUARTERLY, HALF_YEARLY, ANNUAL, CUSTOM"
	}

	// compounding_periods_per_year: required if CUSTOM, if present must be >=1
	if ftype == "CUSTOM" {
		if v, ok := m["compounding_periods_per_year"]; !ok {
			return "compounding_periods_per_year is required for CUSTOM frequency_type"
		} else {
			// value may be int or float64 depending on JSON decoding
			switch val := v.(type) {
			case int:
				if val < 1 {
					return constants.ErrCompoundingPeriodsPerYear
				}
			case float64:
				if int(val) < 1 {
					return constants.ErrCompoundingPeriodsPerYear
				}
			default:
				return "compounding_periods_per_year must be a number"
			}
		}
	} else {
		if v, ok := m["compounding_periods_per_year"]; ok && v != nil {
			switch val := v.(type) {
			case int:
				if val < 1 {
					return constants.ErrCompoundingPeriodsPerYear
				}
			case float64:
				if int(val) < 1 {
					return constants.ErrCompoundingPeriodsPerYear
				}
			}
		}
	}

	// days_per_period: if present must be >=1
	if v, ok := m["days_per_period"]; ok && v != nil {
		switch val := v.(type) {
		case int:
			if val < 1 {
				return "days_per_period must be >= 1"
			}
		case float64:
			if int(val) < 1 {
				return "days_per_period must be >= 1"
			}
		}
	}

	// description: max 500
	if v, ok := m["description"].(string); ok && len(v) > 500 {
		return "description max length is 500"
	}

	return ""
}

// --- Create Single Handler ---
func CreateCompoundingFrequencySingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID                    string `json:"user_id"`
			FrequencyCode             string `json:"frequency_code"`
			FrequencyName             string `json:"frequency_name"`
			FrequencyType             string `json:"frequency_type"`
			CompoundingPeriodsPerYear int    `json:"compounding_periods_per_year"`
			DaysPerPeriod             *int   `json:"days_per_period"`
			Description               string `json:"description"`
			IsActive                  *bool  `json:"is_active"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		// Validate input per rules: compounding_periods_per_year required only for CUSTOM
		inputMap := map[string]interface{}{
			"frequency_code":               req.FrequencyCode,
			"frequency_name":               req.FrequencyName,
			"frequency_type":               req.FrequencyType,
			"compounding_periods_per_year": req.CompoundingPeriodsPerYear,
			"days_per_period":              req.DaysPerPeriod,
			"description":                  req.Description,
		}
		if errMsg := validateCompoundingFrequencyMap(inputMap); errMsg != "" {
			api.RespondWithError(w, http.StatusBadRequest, errMsg)
			return
		}

		if req.IsActive == nil {
			dv := true
			req.IsActive = &dv
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
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `
			INSERT INTO investment.fd_compounding_frequency_master (
				frequency_code, frequency_name, frequency_type, compounding_periods_per_year, days_per_period, description, is_active
			) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING frequency_id
		`
		var id string
		if err := tx.QueryRow(ctx, insertQ, req.FrequencyCode, req.FrequencyName, strings.ToUpper(req.FrequencyType), req.CompoundingPeriodsPerYear, req.DaysPerPeriod, req.Description, req.IsActive).Scan(&id); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		auditQ := `
			INSERT INTO investment.fd_audit_compounding_frequency (frequency_id, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`
		if _, err := tx.Exec(ctx, auditQ, id, userEmail); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "frequency_id": id, "requested": userEmail})
		api.LogInfo("Compounding frequency created: ID=%s, Code=%s", id, req.FrequencyCode)
	}
}

// --- Bulk Create Handler ---
func CreateCompoundingFrequency(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				FrequencyCode             string `json:"frequency_code"`
				FrequencyName             string `json:"frequency_name"`
				FrequencyType             string `json:"frequency_type"`
				CompoundingPeriodsPerYear int    `json:"compounding_periods_per_year"`
				DaysPerPeriod             *int   `json:"days_per_period"`
				Description               string `json:"description"`
				IsActive                  *bool  `json:"is_active"`
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
		var validRows []map[string]interface{}
		var errorsList []map[string]interface{}

		for i, row := range req.Rows {
			if row.FrequencyCode == "" || row.FrequencyName == "" || row.FrequencyType == "" || row.CompoundingPeriodsPerYear <= 0 {
				errorsList = append(errorsList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: "Missing required fields"})
				continue
			}
			if row.IsActive == nil {
				dv := true
				row.IsActive = &dv
			}
			validRows = append(validRows, map[string]interface{}{
				"frequency_code":               row.FrequencyCode,
				"frequency_name":               row.FrequencyName,
				"frequency_type":               strings.ToUpper(row.FrequencyType),
				"compounding_periods_per_year": row.CompoundingPeriodsPerYear,
				"days_per_period":              row.DaysPerPeriod,
				"description":                  row.Description,
				"is_active":                    row.IsActive,
			})
		}

		if len(validRows) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errorsList)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			api.LogError("Bulk create transaction begin failed: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		// EFFICIENT: Batch check for existing codes/names
		codes := make([]string, len(validRows))
		names := make([]string, len(validRows))
		for i, input := range validRows {
			codes[i] = input["frequency_code"].(string)
			names[i] = input["frequency_name"].(string)
		}

		// Single query to check ALL potential duplicates at once
		duplicateQuery := `
			SELECT frequency_code, frequency_name 
			FROM investment.fd_compounding_frequency_master 
			WHERE (frequency_code = ANY($1::text[]) OR frequency_name = ANY($2::text[]))
			  AND is_active = true AND COALESCE(is_deleted, false) = false
		`
		duplicateRows, err := tx.Query(ctx, duplicateQuery, codes, names)
		if err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Duplicate check failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer duplicateRows.Close()

		// Build map of existing codes/names
		existingCodes := make(map[string]bool)
		existingNames := make(map[string]bool)
		for duplicateRows.Next() {
			var code, name string
			if err := duplicateRows.Scan(&code, &name); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Duplicate check scan failed")
				return
			}
			existingCodes[code] = true
			existingNames[name] = true
		}

		// Filter out duplicates and create error reports
		var finalValidInputs []map[string]interface{}
		for i, input := range validRows {
			code := input["frequency_code"].(string)
			name := input["frequency_name"].(string)
			if existingCodes[code] {
				errorsList = append(errorsList, map[string]interface{}{
					"row_index":            i,
					"frequency_code":       code,
					constants.ValueSuccess: false,
					constants.ValueError:   "Frequency code already exists and is active.",
				})
			} else if existingNames[name] {
				errorsList = append(errorsList, map[string]interface{}{
					"row_index":            i,
					"frequency_code":       code,
					constants.ValueSuccess: false,
					constants.ValueError:   "Frequency name already exists and is active.",
				})
			} else {
				finalValidInputs = append(finalValidInputs, input)
			}
		}

		if len(finalValidInputs) == 0 {
			api.RespondWithPayload(w, false, "All rows are duplicates", errorsList)
			return
		}

		valueStrings := make([]string, len(finalValidInputs))
		valueArgs := make([]interface{}, 0, len(finalValidInputs)*7)
		for i, v := range finalValidInputs {
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7)
			valueArgs = append(valueArgs, v["frequency_code"], v["frequency_name"], v["frequency_type"], v["compounding_periods_per_year"], v["days_per_period"], v["description"], v["is_active"])
		}

		batchInsert := fmt.Sprintf(`
			INSERT INTO investment.fd_compounding_frequency_master (
				frequency_code, frequency_name, frequency_type, compounding_periods_per_year, days_per_period, description, is_active
			) VALUES %s RETURNING frequency_id, frequency_code
		`, strings.Join(valueStrings, ","))

		rows, err := tx.Query(ctx, batchInsert, valueArgs...)
		if err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Batch insert failed: %v", err)
			return
		}
		defer rows.Close()

		var inserted []map[string]interface{}
		var ids []string
		for rows.Next() {
			var id, code string
			if err := rows.Scan(&id, &code); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				api.LogError("Insert scan failed: %v", err)
				return
			}
			ids = append(ids, id)
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "frequency_id": id, "frequency_code": code})
		}

		if len(ids) > 0 {
			auditVals := make([]string, len(ids))
			auditArgs := make([]interface{}, 0, len(ids)*2)
			for i, id := range ids {
				auditVals[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQ := fmt.Sprintf("INSERT INTO investment.fd_audit_compounding_frequency (frequency_id, action_type, processing_status, requested_by, requested_at) VALUES %s", strings.Join(auditVals, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				api.LogError("Batch audit failed: %v", err)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk create commit failed: %v", err)
			return
		}

		allResults := append(inserted, errorsList...)
		api.RespondWithPayload(w, len(inserted) > 0, "", allResults)
		api.LogInfo("Compounding frequency bulk create: %d inserted, %d errors", len(inserted), len(errorsList))
	}
}

// --- Simple Upload Handler ---
func UploadCompoundingFrequencySimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse multipart form")
			api.LogError("ParseMultipartForm failed: %v", err)
			return
		}

		userID := r.FormValue("user_id")
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrUserIIsRequired)
			return
		}

		file, handler, err := r.FormFile("file")
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "File upload failed")
			api.LogError("File upload failed: %v", err)
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
			api.RespondWithError(w, http.StatusBadRequest, "Only CSV and XLSX files are supported")
			return
		}

		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		var rows [][]string
		if ext == ".csv" {
			rows, err = parseCSVFile(newBytesMultipartFile(fileBytes))
		} else {
			rows, err = parseXLSXFile(newBytesMultipartFile(fileBytes))
		}
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "File parsing failed: "+err.Error())
			api.LogError("File parsing failed: %v", err)
			return
		}

		if len(rows) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, "File must contain header and at least one data row")
			return
		}

		header := rows[0]
		data := rows[1:]
		// normalize header keys: lowercase, trim, remove non-alphanumeric chars
		normalize := func(s string) string {
			s = strings.ToLower(strings.TrimSpace(s))
			// remove any character that is not a-z, 0-9 or _
			out := make([]rune, 0, len(s))
			for _, r := range s {
				if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
					out = append(out, r)
				}
			}
			return string(out)
		}

		colMap := make(map[string]int)
		for i, col := range header {
			colMap[normalize(col)] = i
		}

		requiredCols := []string{"frequency_code", "frequency_name", "frequency_type"}
		// compounding_periods_per_year may be required depending on frequency_type per-row
		for _, rc := range requiredCols {
			if _, ok := colMap[rc]; !ok {
				api.RespondWithError(w, http.StatusBadRequest, "Required column '"+rc+"' not found")
				return
			}
		}

		// Reuse CreateCompoundingFrequency logic by building rows
		var inputs []map[string]interface{}
		var errorsList []map[string]interface{}

		seenCodesInFile := make(map[string]int)
		seenNamesInFile := make(map[string]int)

		// Fail-fast: validate each row and abort on first validation error
		for ri, row := range data {
			get := func(col string) string { return getColumnValue(row, colMap, col) }

			code := strings.TrimSpace(get("frequency_code"))
			name := strings.TrimSpace(get("frequency_name"))
			ftype := strings.TrimSpace(get("frequency_type"))
			periodsStr := strings.TrimSpace(get("compounding_periods_per_year"))
			daysStr := strings.TrimSpace(get("days_per_period"))
			desc := strings.TrimSpace(get("description"))

			normCode := strings.ToUpper(code)
			if prevRow, dup := seenCodesInFile[normCode]; dup && normCode != "" {
				errorsList = append(errorsList, map[string]interface{}{
					"frequency_code":       code,
					constants.ValueSuccess: false,
					constants.ValueError:   fmt.Sprintf("duplicate data in file: row %d conflicts with row %d (matching frequency_code: %s)", ri+2, prevRow, code),
				})
				continue
			}
			seenCodesInFile[normCode] = ri + 2

			normName := strings.ToUpper(name)
			if prevRow, dup := seenNamesInFile[normName]; dup && normName != "" {
				errorsList = append(errorsList, map[string]interface{}{
					"frequency_code":       code,
					constants.ValueSuccess: false,
					constants.ValueError:   fmt.Sprintf("duplicate data in file: row %d conflicts with row %d (matching frequency_name: %s)", ri+2, prevRow, name),
				})
				continue
			}
			seenNamesInFile[normName] = ri + 2

			// Build map for validation helper
			var periodsVal interface{} = nil
			if periodsStr != "" {
				if p, err := parseIntPtr(periodsStr); err == nil && p != nil {
					periodsVal = *p
				}
			}
			var daysVal interface{} = nil
			if daysStr != "" {
				if d, err := parseIntPtr(daysStr); err == nil && d != nil {
					daysVal = *d
				}
			}

			input := map[string]interface{}{
				"frequency_code":               code,
				"frequency_name":               name,
				"frequency_type":               ftype,
				"compounding_periods_per_year": periodsVal,
				"days_per_period":              daysVal,
				"description":                  desc,
			}

			if vErr := validateCompoundingFrequencyMap(input); vErr != "" {
				// Fail-fast response
				summary := fmt.Sprintf("Compounding frequency upload aborted: row %d failed validation: %s", ri+2, vErr)
				api.RespondWithPayload(w, false, summary, nil)
				return
			}

			// prepare normalized values for insertion
			isActive := func() *bool { b := true; return &b }()
			// if periodsVal is nil, set default 0
			var periodsInt int
			if periodsVal != nil {
				switch v := periodsVal.(type) {
				case int:
					periodsInt = v
				case float64:
					periodsInt = int(v)
				}
			}

			var daysPtr *int
			if daysVal != nil {
				switch v := daysVal.(type) {
				case int:
					daysPtr = &v
				case float64:
					dv := int(v)
					daysPtr = &dv
				}
			}

			inputs = append(inputs, map[string]interface{}{
				"frequency_code":               code,
				"frequency_name":               name,
				"frequency_type":               strings.ToUpper(ftype),
				"compounding_periods_per_year": periodsInt,
				"days_per_period":              daysPtr,
				"description":                  desc,
				"is_active":                    isActive,
			})
		}

		if len(inputs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errorsList)
			return
		}

		// Reuse bulk insert logic
		ctx := r.Context()

		s3Key, storedFileName := "", ""
		if s3storage.IsS3UploadEnabled() {
			folder := s3storage.GetStoragePrefix("master-compounding-frequency")
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

		// EFFICIENT: Batch check for existing codes/names
		codes := make([]string, len(inputs))
		names := make([]string, len(inputs))
		for i, input := range inputs {
			codes[i] = input["frequency_code"].(string)
			names[i] = input["frequency_name"].(string)
		}

		// Single query to check ALL potential duplicates at once
		duplicateQuery := `
			SELECT frequency_code, frequency_name 
			FROM investment.fd_compounding_frequency_master 
			WHERE (frequency_code = ANY($1::text[]) OR frequency_name = ANY($2::text[]))
			  AND is_active = true AND COALESCE(is_deleted, false) = false
		`
		duplicateRows, err := tx.Query(ctx, duplicateQuery, codes, names)
		if err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Duplicate check failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer duplicateRows.Close()

		// Build map of existing codes/names
		existingCodes := make(map[string]bool)
		existingNames := make(map[string]bool)
		for duplicateRows.Next() {
			var code, name string
			if err := duplicateRows.Scan(&code, &name); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Duplicate check scan failed")
				return
			}
			existingCodes[code] = true
			existingNames[name] = true
		}

		// Filter out duplicates and create error reports
		var finalValidInputs []map[string]interface{}
		for _, input := range inputs {
			code := input["frequency_code"].(string)
			name := input["frequency_name"].(string)
			if existingCodes[code] {
				errorsList = append(errorsList, map[string]interface{}{
					"frequency_code":       code,
					constants.ValueSuccess: false,
					constants.ValueError:   "Frequency code already exists and is active.",
				})
			} else if existingNames[name] {
				errorsList = append(errorsList, map[string]interface{}{
					"frequency_code":       code,
					constants.ValueSuccess: false,
					constants.ValueError:   "Frequency name already exists and is active.",
				})
			} else {
				finalValidInputs = append(finalValidInputs, input)
			}
		}

		if len(finalValidInputs) == 0 {
			api.RespondWithPayload(w, false, "All rows are duplicates", errorsList)
			return
		}

		valueStrings := make([]string, len(finalValidInputs))
		valueArgs := make([]interface{}, 0, len(finalValidInputs)*7)
		for i, v := range finalValidInputs {
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*7+1, i*7+2, i*7+3, i*7+4, i*7+5, i*7+6, i*7+7)
			valueArgs = append(valueArgs, v["frequency_code"], v["frequency_name"], v["frequency_type"], v["compounding_periods_per_year"], v["days_per_period"], v["description"], v["is_active"])
		}

		batchInsert := fmt.Sprintf(`
			INSERT INTO investment.fd_compounding_frequency_master (
				frequency_code, frequency_name, frequency_type, compounding_periods_per_year, days_per_period, description, is_active
			) VALUES %s RETURNING frequency_id, frequency_code
		`, strings.Join(valueStrings, ","))

		rowsRes, err := tx.Query(ctx, batchInsert, valueArgs...)
		if err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Batch insert failed: %v", err)
			return
		}
		defer rowsRes.Close()

		var inserted []map[string]interface{}
		var ids []string
		for rowsRes.Next() {
			var id, code string
			if err := rowsRes.Scan(&id, &code); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				api.LogError("Insert scan failed: %v", err)
				return
			}
			ids = append(ids, id)
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "frequency_id": id, "frequency_code": code})
		}

		if len(ids) > 0 {
			auditVals := make([]string, len(ids))
			auditArgs := make([]interface{}, 0, len(ids)*2)
			for i, id := range ids {
				auditVals[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQ := fmt.Sprintf("INSERT INTO investment.fd_audit_compounding_frequency (frequency_id, action_type, processing_status, requested_by, requested_at) VALUES %s", strings.Join(auditVals, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				api.LogError("Batch audit failed: %v", err)
				return
			}
		}

		if s3Key != "" && len(ids) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE investment.fd_compounding_frequency_master SET upload_s3_key = $1 WHERE frequency_id = ANY($2)`, s3Key, ids); err != nil {
				api.LogError("Failed to store upload_s3_key: %v", err)
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Upload commit failed: %v", err)
			return
		}
		committed = true
		bulkuploadaudit.Record(ctx, pgxPool, bulkuploadaudit.Entry{
			ModuleKey:        "master-compounding-frequency",
			OriginalFileName: handler.Filename,
			StoredFileName:   storedFileName,
			UploadS3Key:      s3Key,
			ContentType:      contentType,
			FileSize:         int64(len(fileBytes)),
			TotalRows:        len(inserted) + len(errorsList),
			InsertedCount:    len(inserted),
			ErrorCount:       len(errorsList),
			Status:           bulkuploadaudit.StatusFor(len(inserted), len(errorsList)),
			UploadedBy:       userEmail,
			UploadedAt:       time.Now().UTC(),
		})

		allResults := append(inserted, errorsList...)
		api.RespondWithPayload(w, len(inserted) > 0, "", allResults)
		api.LogInfo("Compounding frequency upload: %d inserted, %d errors", len(inserted), len(errorsList))
	}
}

// --- Get Single Compounding Frequency ---
func GetCompoundingFrequency(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("frequency_id")
		if id == "" {
			api.RespondWithError(w, http.StatusBadRequest, "frequency_id is required")
			return
		}

		ctx := r.Context()
		q := `
			SELECT frequency_id, frequency_code, frequency_name, frequency_type, compounding_periods_per_year, days_per_period, description, is_active, is_deleted
			FROM investment.fd_compounding_frequency_master
			WHERE frequency_id = $1
		`
		var fid, code, name, ftype, desc string
		var periods int
		var days *int
		var isActive, isDeleted bool
		if err := pgxPool.QueryRow(ctx, q, id).Scan(&fid, &code, &name, &ftype, &periods, &days, &desc, &isActive, &isDeleted); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"frequency_id":                 fid,
			"frequency_code":               code,
			"frequency_name":               name,
			"frequency_type":               ftype,
			"compounding_periods_per_year": periods,
			"days_per_period":              days,
			"description":                  desc,
			"is_active":                    isActive,
			"is_deleted":                   isDeleted,
		})
	}
}

// --- Compounding Frequency: With Audit ---
func GetCompoundingFrequenciesWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.frequency_id)
					a.frequency_id,
					a.processing_status,
					a.action_type,
					a.audit_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason,
					a.old_frequency_code,
					a.old_frequency_name,
					a.old_frequency_type,
					a.old_compounding_periods_per_year,
					a.old_days_per_period,
					a.old_description,
					a.old_is_active
				FROM investment.fd_audit_compounding_frequency a
				WHERE a.action_type IN ('CREATE','EDIT','DELETE')
				ORDER BY a.frequency_id, GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT 
					frequency_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') END) AS deleted_at
				FROM investment.fd_audit_compounding_frequency
				GROUP BY frequency_id
			)
			SELECT
				m.frequency_id,
				COALESCE(m.frequency_code,'') AS frequency_code,
				COALESCE(l.old_frequency_code,'') AS old_frequency_code,
				COALESCE(m.frequency_name,'') AS frequency_name,
				COALESCE(l.old_frequency_name,'') AS old_frequency_name,
				COALESCE(m.frequency_type,'') AS frequency_type,
				COALESCE(l.old_frequency_type,'') AS old_frequency_type,
				COALESCE(m.compounding_periods_per_year,0) AS compounding_periods_per_year,
				COALESCE(l.old_compounding_periods_per_year,0) AS old_compounding_periods_per_year,
				COALESCE(m.days_per_period,0) AS days_per_period,
				COALESCE(l.old_days_per_period,0) AS old_days_per_period,
				COALESCE(m.description,'') AS description,
				COALESCE(l.old_description,'') AS old_description,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(l.old_is_active,false) AS old_is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,

				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_type,'') AS action_type,
				COALESCE(l.audit_id::text,'') AS audit_id,
				COALESCE(l.requested_by,'') AS requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,

				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at

			FROM investment.fd_compounding_frequency_master m
			LEFT JOIN latest_audit l ON l.frequency_id = m.frequency_id
			LEFT JOIN history h ON h.frequency_id = m.frequency_id
			WHERE COALESCE(m.is_deleted,false)=false
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		fields := rows.FieldDescriptions()
		out := make([]map[string]interface{}, 0, 1000)

		for rows.Next() {
			vals, _ := rows.Values()
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
			api.LogError("Row iteration error in GetCompoundingFrequenciesWithAudit: %v", rows.Err())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"rows":                 out,
		})
		api.LogInfo("Retrieved %d compounding frequencies with audit data", len(out))
	}
}

// --- OPTIMIZED Bulk Delete Handler - Compounding Frequency ---
func DeleteCompoundingFrequency(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			FrequencyIDs []string `json:"frequency_ids"`
			Reason       string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if len(req.FrequencyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFrequencyIDsProvided)
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
			api.LogError("Bulk delete transaction begin failed: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		// Verify which frequency IDs exist and are not already deleted
		verifyQuery := `
			SELECT frequency_id 
			FROM investment.fd_compounding_frequency_master 
			WHERE frequency_id = ANY($1::text[]) AND COALESCE(is_deleted, false) = false`

		rows, err := tx.Query(ctx, verifyQuery, req.FrequencyIDs)
		if err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Verification failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Delete verification failed: %v", err)
			return
		}
		defer rows.Close()

		var validIDs []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Verification scan failed: "+err.Error())
				api.LogError("Delete verification scan failed: %v", err)
				return
			}
			validIDs = append(validIDs, id)
		}

		// Block delete request if any compounding frequency is referenced by live FD bookings
		var deleteBlockers []map[string]interface{}
		for _, id := range validIDs {
			blockers, _ := dependency.HasCoreBelow(ctx, pgxPool, "fd_compounding_frequency_master", id)
			if len(blockers) > 0 {
				deleteBlockers = append(deleteBlockers, map[string]interface{}{
					"frequency_id": id,
					"blocked_by":   dependency.BlockersSummary(blockers),
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

		if len(validIDs) > 0 {
			auditValues := make([]string, len(validIDs))
			auditArgs := make([]interface{}, 0, len(validIDs)*3)
			for i, id := range validIDs {
				auditValues[i] = fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", i*3+1, i*3+2, i*3+3)
				auditArgs = append(auditArgs, id, userEmail, req.Reason)
			}

			auditQuery := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_compounding_frequency
					(frequency_id, action_type, processing_status, requested_by, reason, requested_at)
				VALUES %s
			`, strings.Join(auditValues, ","))

			if _, err := tx.Exec(ctx, auditQuery, auditArgs...); err != nil {
				msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				api.LogError("Batch delete audit insert failed: %v", err)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk delete commit failed: %v", err)
			return
		}

		var results []map[string]interface{}
		validSet := make(map[string]bool)
		for _, id := range validIDs {
			validSet[id] = true
			results = append(results, map[string]interface{}{
				constants.ValueSuccess: true,
				"frequency_id":         id,
			})
		}
		for _, id := range req.FrequencyIDs {
			if !validSet[id] {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					"frequency_id":         id,
					constants.ValueError:   "Record not found or already deleted",
				})
			}
		}

		success := len(validIDs) > 0
		api.RespondWithPayload(w, success, "", results)
		api.LogInfo("Delete requests created for compounding frequency: %d requested, %d errors, %d total", len(validIDs), len(req.FrequencyIDs)-len(validIDs), len(req.FrequencyIDs))
	}
}

// --- Bulk Approve Handler - Compounding Frequency ---
func BulkApproveCompoundingFrequency(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			FrequencyIDs []string `json:"frequency_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.FrequencyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFrequencyIDsProvided)
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
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Begin transaction failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Bulk approve by frequency IDs (existing working logic)
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_compounding_frequency
			SET processing_status = 'APPROVED', checker_by = $1, checker_at = now(), checker_comment = $2
			WHERE frequency_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.FrequencyIDs)

		if err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Approval failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Dependency check: revert DELETE approval for frequencies referenced by live FD bookings.
		var blockedItems []map[string]interface{}
		{
			checkRows, _ := tx.Query(ctx, `SELECT DISTINCT frequency_id FROM investment.fd_audit_compounding_frequency WHERE frequency_id = ANY($1::text[]) AND action_type='DELETE' AND processing_status='APPROVED'`, req.FrequencyIDs)
			var deleteApprovedIDs []string
			if checkRows != nil {
				for checkRows.Next() {
					var id string
					if checkRows.Scan(&id) == nil {
						deleteApprovedIDs = append(deleteApprovedIDs, id)
					}
				}
				checkRows.Close()
			}
			for _, id := range deleteApprovedIDs {
				blockers, _ := dependency.HasCoreBelow(ctx, pgxPool, "fd_compounding_frequency_master", id)
				if len(blockers) > 0 {
					_, _ = tx.Exec(ctx, `UPDATE investment.fd_audit_compounding_frequency SET processing_status='PENDING_DELETE_APPROVAL', checker_by=NULL, checker_at=NULL, checker_comment=NULL WHERE frequency_id=$1 AND action_type='DELETE' AND processing_status='APPROVED'`, id)
					blockedItems = append(blockedItems, map[string]interface{}{"frequency_id": id, "blocked_by": dependency.BlockersSummary(blockers)})
				}
			}
		}

		// Handle DELETE approvals - set is_deleted=true (blocked ones were reverted above)
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_compounding_frequency_master
			SET is_deleted = true
			WHERE frequency_id IN (
				SELECT DISTINCT a.frequency_id
				FROM investment.fd_audit_compounding_frequency a
				WHERE a.frequency_id = ANY($1::text[])
				  AND a.action_type = 'DELETE'
				  AND a.processing_status = 'APPROVED'
			)
		`, req.FrequencyIDs)

		if err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Delete execution failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]interface{}{
			constants.ValueSuccess: true,
			"approved_count":       len(req.FrequencyIDs) - len(blockedItems),
			"checker":              userEmail,
			"blocked":              blockedItems,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Bulk approval completed: %d frequency IDs approved by %s", len(req.FrequencyIDs)-len(blockedItems), userEmail)
	}
}

// --- Bulk Reject Handler - Compounding Frequency ---
func BulkRejectCompoundingFrequency(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID       string   `json:"user_id"`
			FrequencyIDs []string `json:"frequency_ids"`
			Comment      string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.FrequencyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoFrequencyIDsProvided)
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
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Begin transaction failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_compounding_frequency
			SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_comment = $2
			WHERE frequency_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.FrequencyIDs)

		if err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Rejection failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]interface{}{
			constants.ValueSuccess: true,
			"rejected_count":       len(req.FrequencyIDs),
			"checker":              userEmail,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Bulk rejection completed: %d frequency IDs rejected by %s", len(req.FrequencyIDs), userEmail)
	}
}

// --- Single Update Handler ---
func UpdateCompoundingFrequency(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string                 `json:"user_id"`
			FrequencyID string                 `json:"frequency_id"`
			Fields      map[string]interface{} `json:"fields"`
			Reason      string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if req.FrequencyID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "frequency_id is required")
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
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrTxStartFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT frequency_code, frequency_name, frequency_type,
				   compounding_periods_per_year, days_per_period, description, is_active
			FROM investment.fd_compounding_frequency_master
			WHERE frequency_id=$1
			FOR UPDATE`
		var oldVals [7]interface{}
		if err := tx.QueryRow(ctx, sel, req.FrequencyID).Scan(&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5], &oldVals[6]); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		fieldPairs := map[string]int{
			"frequency_code":               0,
			"frequency_name":               1,
			"frequency_type":               2,
			"compounding_periods_per_year": 3,
			"days_per_period":              4,
			"description":                  5,
			"is_active":                    6,
		}

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

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}

		q := fmt.Sprintf("UPDATE investment.fd_compounding_frequency_master SET %s WHERE frequency_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.FrequencyID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrUpdateFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		auditCols := []string{"frequency_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
		auditVals := []interface{}{req.FrequencyID, "EDIT", constants.StatusPendingEditApproval, req.Reason, userEmail}
		auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
		paramPos := 6

		for k := range req.Fields {
			k = strings.ToLower(k)
			if idx, ok := fieldPairs[k]; ok {
				oldField := "old_" + k
				auditCols = append(auditCols, oldField)
				auditVals = append(auditVals, oldVals[idx])
				auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
				paramPos++
			}
		}

		auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_compounding_frequency (%s) VALUES (%s)", strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
		if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]interface{}{
			constants.ValueSuccess: true,
			"frequency_id":         req.FrequencyID,
			"requested":            userEmail,
			"reason":               req.Reason,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Compounding frequency updated successfully: ID=%s, Reason=%s", req.FrequencyID, req.Reason)
	}
}

// --- Bulk Update Handler ---
func UpdateCompoundingFrequencyBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				FrequencyID string                 `json:"frequency_id"`
				Fields      map[string]interface{} `json:"fields"`
				Reason      string                 `json:"reason"`
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

		var validUpdates []struct {
			FrequencyID string
			Fields      map[string]interface{}
			Reason      string
		}
		var errors []map[string]interface{}
		allIDs := make([]string, 0, len(req.Rows))

		for i, row := range req.Rows {
			if row.FrequencyID == "" {
				errors = append(errors, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: "Missing frequency_id"})
				continue
			}
			if len(row.Fields) == 0 {
				errors = append(errors, map[string]interface{}{"row_index": i, "frequency_id": row.FrequencyID, constants.ValueSuccess: false, constants.ValueError: "No fields to update"})
				continue
			}
			validUpdates = append(validUpdates, struct {
				FrequencyID string
				Fields      map[string]interface{}
				Reason      string
			}{row.FrequencyID, row.Fields, row.Reason})
			allIDs = append(allIDs, row.FrequencyID)
		}

		if len(validUpdates) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errors)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			api.LogError("Bulk update transaction begin failed: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		oldValuesQuery := `
			SELECT frequency_id, frequency_code, frequency_name, frequency_type, compounding_periods_per_year, days_per_period, description, is_active
			FROM investment.fd_compounding_frequency_master
			WHERE frequency_id = ANY($1::text[])
		`
		rowsOld, err := tx.Query(ctx, oldValuesQuery, allIDs)
		if err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, "Fetch old values failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Batch old values fetch failed: %v", err)
			return
		}
		defer rowsOld.Close()

		oldMap := make(map[string][7]interface{})
		for rowsOld.Next() {
			var id string
			var vals [7]interface{}
			if err := rowsOld.Scan(&id, &vals[0], &vals[1], &vals[2], &vals[3], &vals[4], &vals[5], &vals[6]); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Old values scan failed: "+err.Error())
				api.LogError("Old values scan failed: %v", err)
				return
			}
			oldMap[id] = vals
		}

		fieldPairs := map[string]int{
			"frequency_code":               0,
			"frequency_name":               1,
			"frequency_type":               2,
			"compounding_periods_per_year": 3,
			"days_per_period":              4,
			"description":                  5,
			"is_active":                    6,
		}

		fieldUpdates := make(map[string][]struct {
			ID    string
			Value interface{}
		})
		for _, up := range validUpdates {
			for f, v := range up.Fields {
				f = strings.ToLower(f)
				if _, ok := fieldPairs[f]; ok {
					fieldUpdates[f] = append(fieldUpdates[f], struct {
						ID    string
						Value interface{}
					}{up.FrequencyID, v})
				}
			}
		}

		if len(fieldUpdates) > 0 {
			var setClauses []string
			var updateArgs []interface{}
			argIndex := 1
			for field, updates := range fieldUpdates {
				var caseWhen []string
				for _, u := range updates {
					caseWhen = append(caseWhen, fmt.Sprintf("WHEN frequency_id = $%d THEN $%d", argIndex, argIndex+1))
					updateArgs = append(updateArgs, u.ID, u.Value)
					argIndex += 2
				}
				caseClause := fmt.Sprintf("%s = CASE %s ELSE %s END", field, strings.Join(caseWhen, " "), field)
				setClauses = append(setClauses, caseClause)
			}
			updateQuery := fmt.Sprintf(`
				UPDATE investment.fd_compounding_frequency_master
				SET %s
				WHERE frequency_id = ANY($%d::text[])
			`, strings.Join(setClauses, ", "), argIndex)
			updateArgs = append(updateArgs, allIDs)
			if _, err := tx.Exec(ctx, updateQuery, updateArgs...); err != nil {
				msg, status := getUserFriendlyCompoundingFrequencyError(err, "Batch update failed")
				api.RespondWithError(w, status, msg)
				api.LogError("Batch update failed: %v", err)
				return
			}
		}

		var successResults []map[string]interface{}
		for _, up := range validUpdates {
			oldVals, exists := oldMap[up.FrequencyID]
			if !exists {
				errors = append(errors, map[string]interface{}{"frequency_id": up.FrequencyID, constants.ValueSuccess: false, constants.ValueError: "Record not found"})
				continue
			}

			auditCols := []string{"frequency_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
			auditVals := []interface{}{up.FrequencyID, "EDIT", constants.StatusPendingEditApproval, up.Reason, userEmail}
			auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
			paramPos := 6

			for field := range up.Fields {
				field = strings.ToLower(field)
				if idx, ok := fieldPairs[field]; ok {
					oldField := "old_" + field
					auditCols = append(auditCols, oldField)
					auditVals = append(auditVals, oldVals[idx])
					auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
					paramPos++
				}
			}

			auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_compounding_frequency (%s) VALUES (%s)", strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
			if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
				errors = append(errors, map[string]interface{}{"frequency_id": up.FrequencyID, constants.ValueSuccess: false, constants.ValueError: "Audit failed: " + err.Error()})
				continue
			}

			successResults = append(successResults, map[string]interface{}{constants.ValueSuccess: true, "frequency_id": up.FrequencyID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyCompoundingFrequencyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk update commit failed: %v", err)
			return
		}

		allResults := append(successResults, errors...)
		success := len(successResults) > 0
		api.RespondWithPayload(w, success, "", allResults)
		api.LogInfo("ULTRA FAST bulk update frequency: %d updated, %d errors, %d total - single transaction", len(successResults), len(errors), len(req.Rows))
	}
}
