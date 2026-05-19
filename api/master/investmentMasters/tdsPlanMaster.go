package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/master/bulkuploadaudit"
	"CimplrCorpSaas/api/utils/s3storage"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// coerceDateValue converts various date input representations to either
// a non-empty string (passed to Postgres) or nil (so DB receives NULL).
func coerceDateValue(iv interface{}) interface{} {
	if iv == nil {
		return nil
	}
	switch v := iv.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return v
	case *string:
		if v == nil {
			return nil
		}
		if strings.TrimSpace(*v) == "" {
			return nil
		}
		return *v
	default:
		return nil
	}
}

// logDBError provides richer logging for database errors (commit failures etc.).
func logDBError(err error, context string) {
	if err == nil {
		return
	}
	api.LogError("%s: %v", context, err)
	// Always log the plain error string and a verbose representation
	api.LogError("Error string: %s", err.Error())
	api.LogError("Verbose error: %#v", err)

	// Walk the unwrap chain to capture wrapped errors
	for u := errors.Unwrap(err); u != nil; u = errors.Unwrap(u) {
		api.LogError("Unwrapped: %T -> %v", u, u)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		api.LogError("Postgres error detail: Code=%s Message=%s Detail=%s Where=%s Constraint=%s Table=%s Column=%s", pgErr.Code, pgErr.Message, pgErr.Detail, pgErr.Where, pgErr.ConstraintName, pgErr.TableName, pgErr.ColumnName)
	} else {
		api.LogError("Non-pg error type: %T", err)
	}
}

// validateTDSPlanFields validates TDS plan fields against database constraints
func validateTDSPlanFields(input map[string]interface{}) string {
	// Check threshold_type
	if thresholdType, ok := input["threshold_type"].(string); ok && thresholdType != "" {
		validThresholdTypes := map[string]bool{"PER_BANK": true, "PER_BRANCH": true, "AGGREGATE": true}
		if !validThresholdTypes[thresholdType] {
			return fmt.Sprintf("Invalid threshold_type '%s'. Must be PER_BANK, PER_BRANCH, or AGGREGATE.", thresholdType)
		}
	}

	// Check deduction_timing
	if deductionTiming, ok := input["deduction_timing"].(string); ok && deductionTiming != "" {
		validTimings := map[string]bool{"ACCRUAL": true, "RECEIPT": true, "MATURITY": true, "ACCRUAL_ANNUAL": true}
		if !validTimings[deductionTiming] {
			return fmt.Sprintf("Invalid deduction_timing '%s'. Must be ACCRUAL_ANNUAL, ACCRUAL, RECEIPT, or MATURITY.", deductionTiming)
		}
	}

	// Check tds_rate >= 0
	if rate, ok := input["tds_rate"].(float64); ok && rate < 0 {
		return "TDS rate must be greater than or equal to 0."
	}

	return ""
}

// getUserFriendlyTDSPlanError converts database errors to user-friendly messages for TDS plans
func getUserFriendlyTDSPlanError(err error, context string) (string, int) {
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

	// TDS-specific constraint violations - Business logic error (200 OK)
	if strings.Contains(errStr, "uniq_tds_code_active") {
		return "TDS plan code already exists. Please use a different code.", http.StatusOK
	}

	if strings.Contains(errStr, "uniq_tds_name_active") {
		return "TDS plan name already exists. Please use a different name.", http.StatusOK
	}

	if strings.Contains(errStr, "fd_tds_deduction_timing_chk") {
		return "Invalid deduction timing. Must be ACCRUAL_ANNUAL,ACCRUAL, RECEIPT, or MATURITY.", http.StatusOK
	}

	if strings.Contains(errStr, "fd_tds_rate_chk") {
		return "TDS rate must be greater than or equal to 0.", http.StatusOK
	}

	if strings.Contains(errStr, "fd_tds_threshold_type_chk") {
		return "Invalid threshold type. Must be PER_BANK, PER_BRANCH, or AGGREGATE.", http.StatusOK
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

// --- TDS Plan: Active Approved ---
func GetTdsPlansApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			SELECT
				m.tds_plan_id,
				m.tds_plan_code,
				m.tds_plan_name,
				m.tds_section,
				m.tds_rate,
				m.has_pan,
				m.threshold_amount,
				m.threshold_type,
				m.deduction_timing,
				TO_CHAR(m.applicable_from, 'YYYY-MM-DD') AS applicable_from,
				TO_CHAR(m.applicable_to, 'YYYY-MM-DD') AS applicable_to,
				m.description,
				m.is_active
			FROM investment.fd_tds_plan_master m
			INNER JOIN investment.fd_audit_tds_plan a ON a.tds_plan_id = m.tds_plan_id
			WHERE a.processing_status = 'APPROVED'
				AND m.is_active = true
				AND COALESCE(m.is_deleted, false) = false
			ORDER BY m.tds_plan_name
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)

		for rows.Next() {
			var id, code, name, section, thresholdType, deductionTiming, desc string
			var rate float64
			var hasPan bool
			var thresholdAmount *float64
			var applicableFrom string
			var applicableTo *string
			var isActive bool

			if err := rows.Scan(&id, &code, &name, &section, &rate, &hasPan, &thresholdAmount, &thresholdType, &deductionTiming, &applicableFrom, &applicableTo, &desc, &isActive); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				api.LogError("Database scan error in GetTdsPlansApprovedActive: %v", err)
				return
			}

			out = append(out, map[string]interface{}{
				"tds_plan_id":      id,
				"tds_plan_code":    code,
				"tds_plan_name":    name,
				"tds_section":      section,
				"tds_rate":         rate,
				"has_pan":          hasPan,
				"threshold_amount": thresholdAmount,
				"threshold_type":   thresholdType,
				"deduction_timing": deductionTiming,
				"applicable_from":  applicableFrom,
				"applicable_to":    applicableTo,
				"description":      desc,
				"is_active":        isActive,
			})
		}

		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("Retrieved %d active approved tds plans", len(out))
	}
}

// --- Create Single Handler ---
func CreateTDSPlanSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID          string   `json:"user_id"`
			TdsPlanCode     string   `json:"tds_plan_code"`
			TdsPlanName     string   `json:"tds_plan_name"`
			TdsSection      string   `json:"tds_section"`
			TdsRate         float64  `json:"tds_rate"`
			HasPan          bool     `json:"has_pan"`
			ThresholdAmount *float64 `json:"threshold_amount"`
			ThresholdType   string   `json:"threshold_type"`
			DeductionTiming string   `json:"deduction_timing"`
			ApplicableFrom  string   `json:"applicable_from"`
			ApplicableTo    *string  `json:"applicable_to"`
			Description     string   `json:"description"`
			IsActive        *bool    `json:"is_active"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if req.TdsPlanCode == "" || req.TdsPlanName == "" || req.TdsSection == "" {
			api.RespondWithError(w, http.StatusBadRequest, "tds_plan_code, tds_plan_name and tds_section are required")
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
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		insertQ := `
			INSERT INTO investment.fd_tds_plan_master (
				tds_plan_code, tds_plan_name, tds_section, tds_rate, has_pan, threshold_amount, threshold_type, deduction_timing, applicable_from, applicable_to, description, is_active
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING tds_plan_id
		`
		var id string
		af := coerceDateValue(req.ApplicableFrom)
		at := coerceDateValue(req.ApplicableTo)
		if err := tx.QueryRow(ctx, insertQ, req.TdsPlanCode, req.TdsPlanName, req.TdsSection, req.TdsRate, req.HasPan, req.ThresholdAmount, req.ThresholdType, req.DeductionTiming, af, at, req.Description, req.IsActive).Scan(&id); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		auditQ := `INSERT INTO investment.fd_audit_tds_plan (tds_plan_id, action_type, processing_status, requested_by, requested_at) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`
		if _, err := tx.Exec(ctx, auditQ, id, userEmail); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{constants.ValueSuccess: true, "tds_plan_id": id, "requested": userEmail})
		api.LogInfo("TDS plan created: ID=%s, Code=%s", id, req.TdsPlanCode)
	}
}

// --- Bulk Create Handler ---
func CreateTDSPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				TdsPlanCode     string   `json:"tds_plan_code"`
				TdsPlanName     string   `json:"tds_plan_name"`
				TdsSection      string   `json:"tds_section"`
				TdsRate         float64  `json:"tds_rate"`
				HasPan          bool     `json:"has_pan"`
				ThresholdAmount *float64 `json:"threshold_amount"`
				ThresholdType   string   `json:"threshold_type"`
				DeductionTiming string   `json:"deduction_timing"`
				ApplicableFrom  string   `json:"applicable_from"`
				ApplicableTo    *string  `json:"applicable_to"`
				Description     string   `json:"description"`
				IsActive        *bool    `json:"is_active"`
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
			if row.TdsPlanCode == "" || row.TdsPlanName == "" || row.TdsSection == "" {
				errorsList = append(errorsList, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: "Missing required fields"})
				continue
			}
			if row.IsActive == nil {
				dv := true
				row.IsActive = &dv
			}
			inputMap := map[string]interface{}{
				"tds_plan_code":    row.TdsPlanCode,
				"tds_plan_name":    row.TdsPlanName,
				"tds_section":      row.TdsSection,
				"tds_rate":         row.TdsRate,
				"has_pan":          row.HasPan,
				"threshold_amount": row.ThresholdAmount,
				"threshold_type":   row.ThresholdType,
				"deduction_timing": row.DeductionTiming,
				"applicable_from":  row.ApplicableFrom,
				"applicable_to":    row.ApplicableTo,
				"description":      row.Description,
				"is_active":        row.IsActive,
			}
			// Validate constraints before DB operations
			if validationErr := validateTDSPlanFields(inputMap); validationErr != "" {
				errorsList = append(errorsList, map[string]interface{}{"row_index": i, "tds_plan_code": row.TdsPlanCode, constants.ValueSuccess: false, constants.ValueError: validationErr})
				continue
			}
			validRows = append(validRows, inputMap)
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
			codes[i] = input["tds_plan_code"].(string)
			names[i] = input["tds_plan_name"].(string)
		}

		// Single query to check ALL potential duplicates at once
		duplicateQuery := `
			SELECT tds_plan_code, tds_plan_name 
			FROM investment.fd_tds_plan_master 
			WHERE (tds_plan_code = ANY($1::text[]) OR tds_plan_name = ANY($2::text[]))
			  AND is_active = true AND COALESCE(is_deleted, false) = false
		`
		duplicateRows, err := tx.Query(ctx, duplicateQuery, codes, names)
		if err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Duplicate check failed")
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
			code := input["tds_plan_code"].(string)
			name := input["tds_plan_name"].(string)
			if existingCodes[code] {
				errorsList = append(errorsList, map[string]interface{}{
					"row_index":            i,
					"tds_plan_code":        code,
					constants.ValueSuccess: false,
					constants.ValueError:   "TDS plan code already exists and is active.",
				})
			} else if existingNames[name] {
				errorsList = append(errorsList, map[string]interface{}{
					"row_index":            i,
					"tds_plan_code":        code,
					constants.ValueSuccess: false,
					constants.ValueError:   "TDS plan name already exists and is active.",
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
		valueArgs := make([]interface{}, 0, len(finalValidInputs)*12)
		for i, v := range finalValidInputs {
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*12+1, i*12+2, i*12+3, i*12+4, i*12+5, i*12+6, i*12+7, i*12+8, i*12+9, i*12+10, i*12+11, i*12+12)
			// ensure empty date strings are passed as NULL to Postgres
			af := coerceDateValue(v["applicable_from"])
			at := coerceDateValue(v["applicable_to"])
			valueArgs = append(valueArgs, v["tds_plan_code"], v["tds_plan_name"], v["tds_section"], v["tds_rate"], v["has_pan"], v["threshold_amount"], v["threshold_type"], v["deduction_timing"], af, at, v["description"], v["is_active"])
		}

		batchInsert := fmt.Sprintf(`
			INSERT INTO investment.fd_tds_plan_master (
				tds_plan_code, tds_plan_name, tds_section, tds_rate, has_pan, threshold_amount, threshold_type, deduction_timing, applicable_from, applicable_to, description, is_active
			) VALUES %s RETURNING tds_plan_id, tds_plan_code
		`, strings.Join(valueStrings, ","))

		rowsR, err := tx.Query(ctx, batchInsert, valueArgs...)
		if err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Batch insert failed: %v", err)
			return
		}
		defer rowsR.Close()

		var inserted []map[string]interface{}
		var ids []string
		for rowsR.Next() {
			var id, code string
			if err := rowsR.Scan(&id, &code); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				api.LogError("Insert scan failed: %v", err)
				return
			}
			ids = append(ids, id)
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "tds_plan_id": id, "tds_plan_code": code})
		}

		if len(ids) > 0 {
			auditVals := make([]string, len(ids))
			auditArgs := make([]interface{}, 0, len(ids)*2)
			for i, id := range ids {
				auditVals[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQ := fmt.Sprintf("INSERT INTO investment.fd_audit_tds_plan (tds_plan_id, action_type, processing_status, requested_by, requested_at) VALUES %s", strings.Join(auditVals, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyTDSPlanError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				api.LogError("Batch audit failed: %v", err)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			// Log rich DB error details for debugging
			logDBError(err, "Bulk create commit failed")
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		allResults := append(inserted, errorsList...)
		api.RespondWithPayload(w, len(inserted) > 0, "", allResults)
		api.LogInfo("TDS plan bulk create: %d inserted, %d errors", len(inserted), len(errorsList))
	}
}

// --- Simple Upload Handler ---
func UploadTDSPlanSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to read file: "+err.Error())
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

		// normalize header keys (remove non-alphanumeric, lowercase)
		normalize := func(s string) string {
			s = strings.ToLower(strings.TrimSpace(s))
			// remove any character that is not a-z or 0-9
			re := regexp.MustCompile(`[^a-z0-9]`)
			return re.ReplaceAllString(s, "")
		}

		colMap := make(map[string]int)
		for i, col := range header {
			colMap[normalize(col)] = i
		}

		// required columns (normalize keys used above)
		requiredCols := []string{"tds_plan_code", "tds_plan_name", "tds_section", "tds_rate"}
		for _, rc := range requiredCols {
			if _, ok := colMap[normalize(rc)]; !ok {
				api.RespondWithError(w, http.StatusBadRequest, "Required column '"+rc+"' not found")
				return
			}
		}

		var inputs []map[string]interface{}
		var errorsList []map[string]interface{}

		// helper to send a concise fail-fast response
		sendFail := func(row int, errMsg string) {
			summary := fmt.Sprintf("TDS upload aborted: row %d failed validation: %s", row, errMsg)
			api.RespondWithPayload(w, false, summary, nil)
		}
		for ri, row := range data {
			// helper to fetch by normalized name
			get := func(col string) string { return getColumnValue(row, colMap, normalize(col)) }

			code := get("tds_plan_code")
			name := get("tds_plan_name")
			section := get("tds_section")
			rateStr := get("tds_rate")
			if code == "" || name == "" || section == "" || rateStr == "" {
				sendFail(ri+2, "Missing required columns")
				return
			}
			rate := 0.0
			if parsed, err := parseFloatPtr(rateStr); err == nil {
				rate = *parsed
			}
			hasPan := false
			if v := get("has_pan"); v != "" {
				if p, err := parseBoolPtr(v); err == nil {
					hasPan = *p
				}
			}
			thr := (*float64)(nil)
			if v := get("threshold_amount"); v != "" {
				if p, err := parseFloatPtr(v); err == nil {
					thr = p
				}
			}
			inputMap := map[string]interface{}{
				"tds_plan_code":    code,
				"tds_plan_name":    name,
				"tds_section":      section,
				"tds_rate":         rate,
				"has_pan":          hasPan,
				"threshold_amount": thr,
				"threshold_type":   get("threshold_type"),
				"deduction_timing": get("deduction_timing"),
				"applicable_from":  get("applicable_from"),
				"applicable_to":    get("applicable_to"),
				"description":      get("description"),
				"is_active":        func() *bool { b := true; return &b }(),
				"source_row":       ri + 2,
			}
			// Row-level validations per schema
			// tds_plan_code: ^TDS-[A-Za-z0-9-]+$, max 50
			if code == "" {
				sendFail(ri+2, "tds_plan_code is required")
				return
			}
			if len(code) > 50 {
				sendFail(ri+2, "tds_plan_code exceeds max length 50")
				return
			}
			codeRe := regexp.MustCompile(`^TDS-[A-Za-z0-9-]+$`)
			if !codeRe.MatchString(code) {
				sendFail(ri+2, "tds_plan_code must match pattern ^TDS-[A-Za-z0-9-]+$")
				return
			}

			// tds_plan_name: required, maxLength 100
			if name == "" {
				sendFail(ri+2, "tds_plan_name is required")
				return
			}
			if len(name) > 100 {
				sendFail(ri+2, "tds_plan_name exceeds max length 100")
				return
			}

			// tds_section: required, /^[A-Za-z0-9]+$/
			if section == "" {
				sendFail(ri+2, "tds_section is required")
				return
			}
			secRe := regexp.MustCompile(`^[A-Za-z0-9]+$`)
			if !secRe.MatchString(section) {
				sendFail(ri+2, "tds_section must be alphanumeric")
				return
			}

			// tds_rate: required, 0 <= rate <= 100
			if rateStr == "" {
				sendFail(ri+2, "tds_rate is required")
				return
			}
			if rate < 0 || rate > 100 {
				sendFail(ri+2, "tds_rate must be between 0 and 100")
				return
			}

			// threshold_amount: if provided must be >=0 and REQUIRED per schema
			if get("threshold_amount") == "" {
				sendFail(ri+2, "threshold_amount is required")
				return
			}
			if thr != nil && *thr < 0 {
				sendFail(ri+2, "threshold_amount must be >= 0")
				return
			}

			// applicable_from: required and must be a valid date
			afStr := get("applicable_from")
			if afStr == "" {
				sendFail(ri+2, "applicable_from is required")
				return
			}
			var afTime time.Time
			if t, err := time.Parse(constants.DateFormat, afStr); err == nil {
				afTime = t
			} else if t, err := time.Parse(time.RFC3339, afStr); err == nil {
				afTime = t
			} else {
				sendFail(ri+2, "applicable_from must be a valid date (YYYY-MM-DD)")
				return
			}

			// applicable_to: optional but if present must be > applicable_from
			atStr := get("applicable_to")
			if atStr != "" {
				var atTime time.Time
				if t, err := time.Parse(constants.DateFormat, atStr); err == nil {
					atTime = t
				} else if t, err := time.Parse(time.RFC3339, atStr); err == nil {
					atTime = t
				} else {
					sendFail(ri+2, "applicable_to must be a valid date (YYYY-MM-DD)")
					return
				}
				if !atTime.After(afTime) {
					sendFail(ri+2, "applicable_to must be later than applicable_from")
					return
				}
			}

			// Deduction timing / threshold_type validation also covered in validateTDSPlanFields
			if validationErr := validateTDSPlanFields(inputMap); validationErr != "" {
				sendFail(ri+2, validationErr)
				return
			}
			inputs = append(inputs, inputMap)
		}

		// Bulk insert (similar to CreateTDSPlan)
		ctx := r.Context()

		s3Key, storedFileName := "", ""
		if s3storage.IsS3UploadEnabled() {
			folder := s3storage.GetStoragePrefix("master-tds-plan")
			storedFileName = s3storage.BuildUploadedFilename(handler.Filename, userEmail, time.Now().UTC())
			s3Key = s3storage.BuildNamedS3Key(folder, "", storedFileName)
			if err = s3storage.PutObjectToS3(ctx, s3Key, fileBytes, contentType); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Failed to store file: "+err.Error())
				return
			}
		}

		api.LogInfo("Starting TDS plan upload transaction with %d input rows", len(inputs))
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
			codes[i] = input["tds_plan_code"].(string)
			names[i] = input["tds_plan_name"].(string)
		}

		// Single query to check ALL potential duplicates at once
		duplicateQuery := `
			SELECT tds_plan_code, tds_plan_name 
			FROM investment.fd_tds_plan_master 
			WHERE (tds_plan_code = ANY($1::text[]) OR tds_plan_name = ANY($2::text[]))
			  AND is_active = true AND COALESCE(is_deleted, false) = false
		`
		api.LogInfo("Upload: Running duplicate check query for %d codes and %d names", len(codes), len(names))
		duplicateRows, err := tx.Query(ctx, duplicateQuery, codes, names)
		if err != nil {
			logDBError(err, "Upload duplicate check query failed")
			msg, status := getUserFriendlyTDSPlanError(err, "Duplicate check failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer duplicateRows.Close()
		api.LogInfo("Upload: Duplicate check query executed successfully")

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

		// Fail-fast on first duplicate (treat duplicates as validation errors)
		var finalValidInputs []map[string]interface{}
		for _, input := range inputs {
			code := input["tds_plan_code"].(string)
			name := input["tds_plan_name"].(string)
			if existingCodes[code] {
				// report the source row for the duplicate
				src := 0
				if v, ok := input["source_row"].(int); ok {
					src = v
				}
				sendFail(src, "TDS plan code already exists and is active.")
				return
			}
			if existingNames[name] {
				src := 0
				if v, ok := input["source_row"].(int); ok {
					src = v
				}
				sendFail(src, "TDS plan name already exists and is active.")
				return
			}
			finalValidInputs = append(finalValidInputs, input)
		}

		valueStrings := make([]string, len(finalValidInputs))
		valueArgs := make([]interface{}, 0, len(finalValidInputs)*12)
		for i, v := range finalValidInputs {
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)", i*12+1, i*12+2, i*12+3, i*12+4, i*12+5, i*12+6, i*12+7, i*12+8, i*12+9, i*12+10, i*12+11, i*12+12)
			// ensure empty date strings are passed as NULL to Postgres
			af := coerceDateValue(v["applicable_from"])
			at := coerceDateValue(v["applicable_to"])
			valueArgs = append(valueArgs, v["tds_plan_code"], v["tds_plan_name"], v["tds_section"], v["tds_rate"], v["has_pan"], v["threshold_amount"], v["threshold_type"], v["deduction_timing"], af, at, v["description"], v["is_active"])
		}

		batchInsert := fmt.Sprintf(`
			INSERT INTO investment.fd_tds_plan_master (
				tds_plan_code, tds_plan_name, tds_section, tds_rate, has_pan, threshold_amount, threshold_type, deduction_timing, applicable_from, applicable_to, description, is_active
			) VALUES %s RETURNING tds_plan_id, tds_plan_code
		`, strings.Join(valueStrings, ","))

		rowsR, err := tx.Query(ctx, batchInsert, valueArgs...)
		if err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Batch insert failed: %v", err)
			return
		}
		defer rowsR.Close()

		var inserted []map[string]interface{}
		var ids []string
		for rowsR.Next() {
			var id, code string
			if err := rowsR.Scan(&id, &code); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				api.LogError("Insert scan failed: %v", err)
				return
			}
			ids = append(ids, id)
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "tds_plan_id": id, "tds_plan_code": code})
		}

		if len(ids) > 0 {
			auditVals := make([]string, len(ids))
			auditArgs := make([]interface{}, 0, len(ids)*2)
			for i, id := range ids {
				auditVals[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQ := fmt.Sprintf("INSERT INTO investment.fd_audit_tds_plan (tds_plan_id, action_type, processing_status, requested_by, requested_at) VALUES %s", strings.Join(auditVals, ","))
			if _, err := tx.Exec(ctx, auditQ, auditArgs...); err != nil {
				msg, status := getUserFriendlyTDSPlanError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				api.LogError("Batch audit failed: %v", err)
				return
			}
		}

		if s3Key != "" && len(ids) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE investment.fd_tds_plan_master SET upload_s3_key = $1 WHERE tds_plan_id = ANY($2)`, s3Key, ids); err != nil {
				api.LogError("Failed to store upload_s3_key: %v", err)
			}
		}

		api.LogInfo("All queries executed successfully, attempting transaction commit")
		if err := tx.Commit(ctx); err != nil {
			// Log rich DB error details for debugging
			logDBError(err, "Upload bulk create commit failed")
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}
		committed = true
		bulkuploadaudit.Record(ctx, pgxPool, bulkuploadaudit.Entry{
			ModuleKey:        "master-tds-plan",
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
		api.LogInfo("Transaction committed successfully")

		if len(inserted) > 0 {
			if len(errorsList) > 0 {
				msg := fmt.Sprintf("%d TDS plans created successfully; %d duplicates skipped", len(inserted), len(errorsList))
				api.RespondWithPayload(w, true, msg, nil)
			} else {
				msg := fmt.Sprintf("%d TDS plans created successfully", len(inserted))
				api.RespondWithPayload(w, true, msg, nil)
			}
			api.LogInfo("TDS plan upload/bulk create: %d inserted, %d errors", len(inserted), len(errorsList))
			return
		}
		api.RespondWithPayload(w, false, "No TDS plans were created", errorsList)
		api.LogInfo("TDS plan upload/bulk create: %d inserted, %d errors", len(inserted), len(errorsList))
	}
}

// --- Get Single TDS Plan ---
func GetTDSPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("tds_plan_id")
		if id == "" {
			api.RespondWithError(w, http.StatusBadRequest, "tds_plan_id is required")
			return
		}
		ctx := r.Context()
		q := `SELECT tds_plan_id, tds_plan_code, tds_plan_name, tds_section, tds_rate, has_pan, threshold_amount, threshold_type, deduction_timing, applicable_from, applicable_to, description, is_active, is_deleted FROM investment.fd_tds_plan_master WHERE tds_plan_id=$1`
		var tid, code, name, section, thresholdType, deductionTiming, description string
		var rate float64
		var hasPan bool
		var thresholdAmount *float64
		var applicableFrom string
		var applicableTo *string
		var isActive, isDeleted bool
		if err := pgxPool.QueryRow(ctx, q, id).Scan(&tid, &code, &name, &section, &rate, &hasPan, &thresholdAmount, &thresholdType, &deductionTiming, &applicableFrom, &applicableTo, &description, &isActive, &isDeleted); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		api.RespondWithPayload(w, true, "", map[string]interface{}{"tds_plan_id": tid, "tds_plan_code": code, "tds_plan_name": name, "tds_section": section, "tds_rate": rate, "has_pan": hasPan, "threshold_amount": thresholdAmount, "threshold_type": thresholdType, "deduction_timing": deductionTiming, "applicable_from": applicableFrom, "applicable_to": applicableTo, "description": description, "is_active": isActive, "is_deleted": isDeleted})
	}
}

// --- TDS Plan: With Audit ---
func GetTdsPlansWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.tds_plan_id)
					a.tds_plan_id,
					a.processing_status,
					a.action_type,
					a.audit_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason,
					a.old_tds_plan_code,
					a.old_tds_plan_name,
					a.old_tds_rate,
					a.old_tds_section,
					a.old_has_pan,
					a.old_threshold_amount,
					a.old_threshold_type,
					a.old_deduction_timing,
					a.old_applicable_from,
					a.old_applicable_to,
					a.old_description,
					a.old_is_active,
					a.old_is_deleted
				FROM investment.fd_audit_tds_plan a
				ORDER BY a.tds_plan_id, GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT 
					tds_plan_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.fd_audit_tds_plan
				GROUP BY tds_plan_id
			)
			SELECT
				m.tds_plan_id,
				COALESCE(m.tds_plan_code,'') AS tds_plan_code,
				COALESCE(l.old_tds_plan_code,'') AS old_tds_plan_code,
				COALESCE(m.tds_plan_name,'') AS tds_plan_name,
				COALESCE(l.old_tds_plan_name,'') AS old_tds_plan_name,
				COALESCE(m.tds_section,'') AS tds_section,
				COALESCE(m.tds_rate,0) AS tds_rate,
				COALESCE(l.old_tds_rate,0) AS old_tds_rate,
				COALESCE(l.old_tds_section,'') AS old_tds_section,
				COALESCE(m.has_pan,false) AS has_pan,
				COALESCE(l.old_has_pan,false) AS old_has_pan,
				COALESCE(m.threshold_amount,0) AS threshold_amount,
				COALESCE(l.old_threshold_amount,0) AS old_threshold_amount,
				COALESCE(l.old_threshold_type,'') AS old_threshold_type,
				COALESCE(l.old_deduction_timing,'') AS old_deduction_timing,
				COALESCE(TO_CHAR(l.old_applicable_from,'YYYY-MM-DD'),'') AS old_applicable_from,
				COALESCE(TO_CHAR(l.old_applicable_to,'YYYY-MM-DD'),'') AS old_applicable_to,
				COALESCE(l.old_description,'') AS old_description,
				COALESCE(m.threshold_type,'') AS threshold_type,
				COALESCE(m.deduction_timing,'') AS deduction_timing,
				COALESCE(TO_CHAR(m.applicable_from,'YYYY-MM-DD'),'') AS applicable_from,
				COALESCE(TO_CHAR(m.applicable_to,'YYYY-MM-DD'),'') AS applicable_to,
				COALESCE(m.description,'') AS description,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(l.old_is_active,false) AS old_is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,

				COALESCE(l.processing_status,'') AS processing_status,
				COALESCE(l.action_type,'') AS action_type,
				COALESCE(l.audit_id::text,'') AS audit_id,
				COALESCE(l.requested_by,'') AS requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'') AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(l.checker_comment,'') AS checker_comment,
				COALESCE(l.reason,'') AS reason,

				COALESCE(h.created_by,'') AS created_by,
				COALESCE(h.created_at,'') AS created_at,
				COALESCE(h.edited_by,'') AS edited_by,
				COALESCE(h.edited_at,'') AS edited_at,
				COALESCE(h.deleted_by,'') AS deleted_by,
				COALESCE(h.deleted_at,'') AS deleted_at

			FROM investment.fd_tds_plan_master m
			LEFT JOIN latest_audit l ON l.tds_plan_id = m.tds_plan_id
			LEFT JOIN history h ON h.tds_plan_id = m.tds_plan_id
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
			api.LogError("Row iteration error in GetTdsPlansWithAudit: %v", rows.Err())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"rows":                 out,
		})
		api.LogInfo("Retrieved %d tds plans with audit data", len(out))
	}
}

// --- OPTIMIZED Bulk Delete Handler - TDS Plan ---
func DeleteTdsPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string   `json:"user_id"`
			TdsIDs []string `json:"tds_plan_ids"`
			Reason string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.TdsIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNotdsIDsProvided)
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

		// Verify which TDS plan IDs exist and are not already deleted
		verifyQuery := `
			SELECT tds_plan_id 
			FROM investment.fd_tds_plan_master 
			WHERE tds_plan_id = ANY($1::text[]) AND COALESCE(is_deleted, false) = false`

		rows, err := tx.Query(ctx, verifyQuery, req.TdsIDs)
		if err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Verification failed")
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

		if len(validIDs) > 0 {
			auditValues := make([]string, len(validIDs))
			auditArgs := make([]interface{}, 0, len(validIDs)*3)
			for i, id := range validIDs {
				auditValues[i] = fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", i*3+1, i*3+2, i*3+3)
				auditArgs = append(auditArgs, id, userEmail, req.Reason)
			}

			auditQuery := fmt.Sprintf(`
				INSERT INTO investment.fd_audit_tds_plan
					(tds_plan_id, action_type, processing_status, requested_by, reason, requested_at)
				VALUES %s
			`, strings.Join(auditValues, ","))

			if _, err := tx.Exec(ctx, auditQuery, auditArgs...); err != nil {
				msg, status := getUserFriendlyTDSPlanError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				api.LogError("Batch delete audit insert failed: %v", err)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrCommitFailedUser)
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
				"tds_plan_id":          id,
			})
		}
		for _, id := range req.TdsIDs {
			if !validSet[id] {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					"tds_plan_id":          id,
					constants.ValueError:   "Record not found or already deleted",
				})
			}
		}

		success := len(validIDs) > 0
		api.RespondWithPayload(w, success, "", results)
		api.LogInfo("Delete requests created for TDS plans: %d requested, %d errors, %d total", len(validIDs), len(req.TdsIDs)-len(validIDs), len(req.TdsIDs))
	}
}

// Compatibility wrappers (master.go expects these exact exported names)
func DeleteTDSPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return DeleteTdsPlan(pgxPool)
}

func BulkApproveTDSPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return BulkApproveTdsPlan(pgxPool)
}

func BulkRejectTDSPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return BulkRejectTdsPlan(pgxPool)
}

func GetTDSPlansApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return GetTdsPlansApprovedActive(pgxPool)
}

func GetTDSPlansWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return GetTdsPlansWithAudit(pgxPool)
}

func GetTDSPlanAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return GetTdsPlanAuditHistory(pgxPool)
}

// --- Bulk Approve Handler - TDS Plan ---
func BulkApproveTdsPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TdsIDs  []string `json:"tds_plan_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.TdsIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNotdsIDsProvided)
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
			msg, status := getUserFriendlyTDSPlanError(err, "Begin transaction failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Bulk approve by TDS IDs (existing working logic)
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_tds_plan
			SET processing_status = 'APPROVED', checker_by = $1, checker_at = now(), checker_comment = $2
			WHERE tds_plan_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.TdsIDs)

		if err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Approval failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Handle DELETE approvals - set is_deleted=true for DELETE actions
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_tds_plan_master 
			SET is_deleted = true 
			WHERE tds_plan_id IN (
				SELECT DISTINCT a.tds_plan_id 
				FROM investment.fd_audit_tds_plan a 
				WHERE a.tds_plan_id = ANY($1::text[]) 
				  AND a.action_type = 'DELETE' 
				  AND a.processing_status = 'APPROVED'
			)
		`, req.TdsIDs)

		if err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Delete execution failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]interface{}{
			constants.ValueSuccess: true,
			"approved_count":       len(req.TdsIDs),
			"checker":              userEmail,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Bulk approval completed: %d tds plan IDs approved by %s", len(req.TdsIDs), userEmail)
	}
}

// --- Bulk Reject Handler - TDS Plan ---
func BulkRejectTdsPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID  string   `json:"user_id"`
			TdsIDs  []string `json:"tds_plan_ids"`
			Comment string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.TdsIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNotdsIDsProvided)
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
			msg, status := getUserFriendlyTDSPlanError(err, "Begin transaction failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_tds_plan
			SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_comment = $2
			WHERE tds_plan_id = ANY($3::text[]) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.TdsIDs)

		if err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Rejection failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]interface{}{
			constants.ValueSuccess: true,
			"rejected_count":       len(req.TdsIDs),
			"checker":              userEmail,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Bulk rejection completed: %d tds plan IDs rejected by %s", len(req.TdsIDs), userEmail)
	}
}

// --- Single Update Handler ---
func UpdateTDSPlan(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string                 `json:"user_id"`
			TdsID  string                 `json:"tds_plan_id"`
			Fields map[string]interface{} `json:"fields"`
			Reason string                 `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		api.LogInfo("UpdateTDSPlan request: UserID=%s, TdsID=%s, Fields=%+v, Reason=%s", req.UserID, req.TdsID, req.Fields, req.Reason)

		if req.TdsID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "tds_plan_id is required")
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
			msg, status := getUserFriendlyTDSPlanError(err, "Transaction start failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		sel := `
			SELECT tds_plan_code, tds_plan_name, tds_section, tds_rate, has_pan, threshold_amount, threshold_type, deduction_timing, applicable_from, applicable_to, description, is_active
			FROM investment.fd_tds_plan_master
			WHERE tds_plan_id=$1
			FOR UPDATE`
		var oldVals [12]interface{}
		if err := tx.QueryRow(ctx, sel, req.TdsID).Scan(&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9], &oldVals[10], &oldVals[11]); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		fieldPairs := map[string]int{
			"tds_plan_code":    0,
			"tds_plan_name":    1,
			"tds_section":      2,
			"tds_rate":         3,
			"has_pan":          4,
			"threshold_amount": 5,
			"threshold_type":   6,
			"deduction_timing": 7,
			"applicable_from":  8,
			"applicable_to":    9,
			"description":      10,
			"is_active":        11,
		}

		var sets []string
		var args []interface{}
		pos := 1
		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if _, ok := fieldPairs[k]; ok {
				// Validate constraint fields before database update
				if errMsg := validateTDSPlanFields(map[string]interface{}{k: v}); errMsg != "" {
					api.RespondWithError(w, http.StatusBadRequest, errMsg)
					return
				}

				sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, k, pos))
				args = append(args, v)
				pos++
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}

		q := fmt.Sprintf("UPDATE investment.fd_tds_plan_master SET %s WHERE tds_plan_id=$%d", strings.Join(sets, ", "), pos)
		args = append(args, req.TdsID)

		api.LogInfo("Debug update query: %s", q)
		api.LogInfo("Debug update args: %+v", args)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			logDBError(err, "Update failed in UpdateTDSPlan")
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrUpdateFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		auditCols := []string{"tds_plan_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
		auditVals := []interface{}{req.TdsID, "EDIT", "PENDING_EDIT_APPROVAL", req.Reason, userEmail}
		auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}

		// Append old_* columns for every field actually being updated
		paramPos := len(auditVals) + 1 // next $n index
		for k := range req.Fields {
			k = strings.ToLower(k)
			if idx, ok := fieldPairs[k]; ok {
				oldCol := "old_" + k
				auditCols = append(auditCols, oldCol)
				// append the old value from oldVals array
				auditVals = append(auditVals, oldVals[idx])
				auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
				paramPos++
			}
		}

		auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_tds_plan (%s) VALUES (%s)", strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
		api.LogInfo("Debug audit insert query: %s", auditQuery)
		api.LogInfo("Debug audit insert values: %+v", auditVals)
		if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
			logDBError(err, "Audit insert failed in UpdateTDSPlan")
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]interface{}{
			constants.ValueSuccess: true,
			"tds_plan_id":          req.TdsID,
			"requested":            userEmail,
			"reason":               req.Reason,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("TDS plan updated successfully: ID=%s, Reason=%s", req.TdsID, req.Reason)
	}
}

// --- Bulk Update Handler ---
func UpdateTDSPlanBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				TdsID  string                 `json:"tds_plan_id"`
				Fields map[string]interface{} `json:"fields"`
				Reason string                 `json:"reason"`
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
			TdsID  string
			Fields map[string]interface{}
			Reason string
		}
		var errors []map[string]interface{}
		allIDs := make([]string, 0, len(req.Rows))

		for i, row := range req.Rows {
			if row.TdsID == "" {
				errors = append(errors, map[string]interface{}{"row_index": i, constants.ValueSuccess: false, constants.ValueError: "Missing tds_plan_id"})
				continue
			}
			if len(row.Fields) == 0 {
				errors = append(errors, map[string]interface{}{"row_index": i, "tds_plan_id": row.TdsID, constants.ValueSuccess: false, constants.ValueError: "No fields to update"})
				continue
			}
			validUpdates = append(validUpdates, struct {
				TdsID  string
				Fields map[string]interface{}
				Reason string
			}{row.TdsID, row.Fields, row.Reason})
			allIDs = append(allIDs, row.TdsID)
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
			SELECT tds_plan_id, tds_plan_code, tds_plan_name, tds_section, tds_rate, has_pan, threshold_amount, threshold_type, deduction_timing, applicable_from, applicable_to, description, is_active
			FROM investment.fd_tds_plan_master
			WHERE tds_plan_id = ANY($1::text[])
		`
		rowsOld, err := tx.Query(ctx, oldValuesQuery, allIDs)
		if err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, "Fetch old values failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Batch old values fetch failed: %v", err)
			return
		}
		defer rowsOld.Close()

		oldMap := make(map[string][]interface{})
		for rowsOld.Next() {
			var id string
			var code, name, section, thresholdType, deductionTiming, applicableFrom, applicableTo, desc interface{}
			var rate float64
			var hasPan bool
			var thresholdAmount float64
			var isActive bool
			if err := rowsOld.Scan(&id, &code, &name, &section, &rate, &hasPan, &thresholdAmount, &thresholdType, &deductionTiming, &applicableFrom, &applicableTo, &desc, &isActive); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Old values scan failed: "+err.Error())
				api.LogError("Old values scan failed: %v", err)
				return
			}
			oldMap[id] = []interface{}{code, name, section, rate, hasPan, thresholdAmount, thresholdType, deductionTiming, applicableFrom, applicableTo, desc, isActive}
		}

		fieldPairs := map[string]int{
			"tds_plan_code":    0,
			"tds_plan_name":    1,
			"tds_section":      2,
			"tds_rate":         3,
			"has_pan":          4,
			"threshold_amount": 5,
			"threshold_type":   6,
			"deduction_timing": 7,
			"applicable_from":  8,
			"applicable_to":    9,
			"description":      10,
			"is_active":        11,
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
					}{up.TdsID, v})
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
					caseWhen = append(caseWhen, fmt.Sprintf("WHEN tds_plan_id = $%d THEN $%d", argIndex, argIndex+1))
					updateArgs = append(updateArgs, u.ID, u.Value)
					argIndex += 2
				}
				caseClause := fmt.Sprintf("%s = CASE %s ELSE %s END", field, strings.Join(caseWhen, " "), field)
				setClauses = append(setClauses, caseClause)
			}
			updateQuery := fmt.Sprintf(`
				UPDATE investment.fd_tds_plan_master
				SET %s
				WHERE tds_plan_id = ANY($%d::text[])
			`, strings.Join(setClauses, ", "), argIndex)
			updateArgs = append(updateArgs, allIDs)
			if _, err := tx.Exec(ctx, updateQuery, updateArgs...); err != nil {
				msg, status := getUserFriendlyTDSPlanError(err, "Batch update failed")
				api.RespondWithError(w, status, msg)
				api.LogError("Batch update failed: %v", err)
				return
			}
		}

		var successResults []map[string]interface{}
		for _, up := range validUpdates {
			oldVals, exists := oldMap[up.TdsID]
			if !exists {
				errors = append(errors, map[string]interface{}{"tds_plan_id": up.TdsID, constants.ValueSuccess: false, constants.ValueError: "Record not found"})
				continue
			}

			auditCols := []string{"tds_plan_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
			auditVals := []interface{}{up.TdsID, "EDIT", "PENDING_EDIT_APPROVAL", up.Reason, userEmail}
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

			auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_tds_plan (%s) VALUES (%s)", strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))
			if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
				errors = append(errors, map[string]interface{}{"tds_plan_id": up.TdsID, constants.ValueSuccess: false, constants.ValueError: "Audit failed: " + err.Error()})
				continue
			}

			successResults = append(successResults, map[string]interface{}{constants.ValueSuccess: true, "tds_plan_id": up.TdsID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyTDSPlanError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk update commit failed: %v", err)
			return
		}

		allResults := append(successResults, errors...)
		success := len(successResults) > 0
		api.RespondWithPayload(w, success, "", allResults)
		api.LogInfo("ULTRA FAST bulk update tds plan: %d updated, %d errors, %d total - single transaction", len(successResults), len(errors), len(req.Rows))
	}
}
