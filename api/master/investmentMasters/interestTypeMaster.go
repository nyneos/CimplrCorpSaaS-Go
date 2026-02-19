package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xuri/excelize/v2"
)

// getUserFriendlyInterestTypeError converts database errors to user-friendly messages
func getUserFriendlyInterestTypeError(err error, context string) (string, int) {
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

	// Constraint violations - Business logic error (200 OK)
	if strings.Contains(errStr, "uniq_interest_type_code_active") {
		return "Interest type code already exists.", http.StatusOK
	}

	if strings.Contains(errStr, "uniq_interest_type_name_active") {
		return "Interest type name already exists.", http.StatusOK
	}

	if strings.Contains(errStr, "inv_interest_type_method_chk") {
		return "Invalid calculation method. Must be SIMPLE, COMPOUND, or STEPPED.", http.StatusOK
	}

	if strings.Contains(errStr, "inv_interest_type_tenor_chk") {
		return "Minimum tenor days must be less than or equal to maximum tenor days.", http.StatusOK
	}

	// Standard database constraint errors
	if strings.Contains(errStr, "unique constraint") || strings.Contains(errStr, "duplicate key") {
		return "Duplicate entry detected. Please check for existing records.", http.StatusOK
	}
	if strings.Contains(errStr, "foreign key constraint") || strings.Contains(errStr, "violates foreign key") {
		return "Referenced record not found or is invalid.", http.StatusOK
	}
	if strings.Contains(errStr, "check constraint") {
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

// --- Request Types ---
type InterestTypeInput struct {
	InterestTypeCode  string `json:"interest_type_code"`
	InterestTypeName  string `json:"interest_type_name"`
	CalculationMethod string `json:"calculation_method"`
	MinTenorDays      *int   `json:"min_tenor_days"`
	MaxTenorDays      *int   `json:"max_tenor_days"`
	IsDefault         *bool  `json:"is_default"`
	Description       string `json:"description"`
	IsActive          *bool  `json:"is_active"`
}

type CreateInterestTypeRequest struct {
	UserID string              `json:"user_id"`
	Rows   []InterestTypeInput `json:"rows"`
}

type CreateInterestTypeSingleRequest struct {
	UserID            string `json:"user_id"`
	InterestTypeCode  string `json:"interest_type_code"`
	InterestTypeName  string `json:"interest_type_name"`
	CalculationMethod string `json:"calculation_method"`
	MinTenorDays      *int   `json:"min_tenor_days"`
	MaxTenorDays      *int   `json:"max_tenor_days"`
	IsDefault         *bool  `json:"is_default"`
	Description       string `json:"description"`
	IsActive          *bool  `json:"is_active"`
}

type UpdateInterestTypeRequest struct {
	UserID     string                 `json:"user_id"`
	InterestID string                 `json:"interest_id"`
	Fields     map[string]interface{} `json:"fields"`
	Reason     string                 `json:"reason"`
}

// --- Helper Functions ---
func validateInterestTypeFields(input InterestTypeInput) error {
	if input.InterestTypeCode == "" {
		return errors.New("interest_type_code is required")
	}
	if input.InterestTypeName == "" {
		return errors.New("interest_type_name is required")
	}
	if input.CalculationMethod == "" {
		return errors.New("calculation_method is required")
	}

	validMethods := []string{"SIMPLE", "COMPOUND", "STEPPED"}
	if !slices.Contains(validMethods, strings.ToUpper(input.CalculationMethod)) {
		return errors.New("calculation_method must be SIMPLE, COMPOUND, or STEPPED")
	}

	if input.MinTenorDays != nil && input.MaxTenorDays != nil &&
		*input.MinTenorDays > *input.MaxTenorDays {
		return errors.New("min_tenor_days cannot be greater than max_tenor_days")
	}

	return nil
}

// --- Upload Handler ---
func UploadInterestTypeSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse multipart form")
			api.LogError("ParseMultipartForm failed: %v", err)
			return
		}

		userID := r.FormValue("user_id")
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id is required")
			return
		}

		file, handler, err := r.FormFile("file")
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "File upload failed")
			api.LogError("File upload failed: %v", err)
			return
		}
		defer file.Close()

		ext := strings.ToLower(filepath.Ext(handler.Filename))
		if ext != ".csv" && ext != ".xlsx" {
			api.RespondWithError(w, http.StatusBadRequest, "Only CSV and XLSX files are supported")
			return
		}

		userEmail := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == userID {
				userEmail = s.Email
				break
			}
		}
		if userEmail == "" {
			api.RespondWithError(w, http.StatusUnauthorized, constants.ErrInvalidSessionShort)
			return
		}

		var rows [][]string
		if ext == ".csv" {
			rows, err = parseCSVFile(file)
		} else {
			rows, err = parseXLSXFile(file)
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

		// Validate required columns
		requiredCols := []string{"interest_type_code", "interest_type_name", "calculation_method"}
		colMap := make(map[string]int)
		for i, col := range header {
			colMap[strings.ToLower(strings.TrimSpace(col))] = i
		}

		for _, reqCol := range requiredCols {
			if _, exists := colMap[reqCol]; !exists {
				errMsg := fmt.Sprintf("Required column '%s' not found", reqCol)
				api.RespondWithError(w, http.StatusBadRequest, errMsg)
				api.LogError("Column validation failed: %s", errMsg)
				return
			}
		}

		// Process each row
		ctx := r.Context()
		results := make([]map[string]interface{}, 0, len(data))

		for rowIdx, row := range data {
			if len(row) == 0 {
				continue
			}

			input := InterestTypeInput{
				InterestTypeCode:  getColumnValue(row, colMap, "interest_type_code"),
				InterestTypeName:  getColumnValue(row, colMap, "interest_type_name"),
				CalculationMethod: strings.ToUpper(getColumnValue(row, colMap, "calculation_method")),
			}

			// Parse optional fields
			if val := getColumnValue(row, colMap, "min_tenor_days"); val != "" {
				if parsed, err := parseIntPtr(val); err == nil {
					input.MinTenorDays = parsed
				}
			}
			if val := getColumnValue(row, colMap, "max_tenor_days"); val != "" {
				if parsed, err := parseIntPtr(val); err == nil {
					input.MaxTenorDays = parsed
				}
			}
			if val := getColumnValue(row, colMap, "is_default"); val != "" {
				if parsed, err := parseBoolPtr(val); err == nil {
					input.IsDefault = parsed
				}
			}
			if val := getColumnValue(row, colMap, "description"); val != "" {
				input.Description = val
			}
			if val := getColumnValue(row, colMap, "is_active"); val != "" {
				if parsed, err := parseBoolPtr(val); err == nil {
					input.IsActive = parsed
				}
			}

			// Set defaults
			if input.IsDefault == nil {
				defaultVal := false
				input.IsDefault = &defaultVal
			}
			if input.IsActive == nil {
				defaultVal := true
				input.IsActive = &defaultVal
			}

			// Validate
			if err := validateInterestTypeFields(input); err != nil {
				results = append(results, map[string]interface{}{
					"row":                  rowIdx + 2,
					constants.ValueSuccess: false,
					constants.ValueError:   err.Error(),
					"interest_type_code":   input.InterestTypeCode,
				})
				continue
			}

			// Insert into database
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					"row":                  rowIdx + 2,
					constants.ValueSuccess: false,
					constants.ValueError:   "Begin TX failed: " + err.Error(),
					"interest_type_code":   input.InterestTypeCode,
				})
				continue
			}

			func() {
				defer tx.Rollback(ctx)

				insertQuery := `
					INSERT INTO investment.fd_interest_type_master (
						interest_type_code, interest_type_name, calculation_method,
						min_tenor_days, max_tenor_days, is_default, description, is_active
					)
					VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
					RETURNING interest_id
				`

				var interestID string
				err = tx.QueryRow(ctx, insertQuery,
					input.InterestTypeCode,
					input.InterestTypeName,
					input.CalculationMethod,
					input.MinTenorDays,
					input.MaxTenorDays,
					input.IsDefault,
					input.Description,
					input.IsActive,
				).Scan(&interestID)

				if err != nil {
					msg, _ := getUserFriendlyInterestTypeError(err, "Insert failed")
					results = append(results, map[string]interface{}{
						"row":                  rowIdx + 2,
						constants.ValueSuccess: false,
						constants.ValueError:   msg,
						"interest_type_code":   input.InterestTypeCode,
					})
					return
				}

				auditQuery := `
					INSERT INTO investment.fd_audit_interest_type (
						interest_id, action_type, processing_status, requested_by, requested_at
					) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`
				if _, err := tx.Exec(ctx, auditQuery, interestID, userEmail); err != nil {
					results = append(results, map[string]interface{}{
						"row":                  rowIdx + 2,
						constants.ValueSuccess: false,
						constants.ValueError:   constants.ErrAuditInsertFailed + err.Error(),
						"interest_type_code":   input.InterestTypeCode,
					})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{
						"row":                  rowIdx + 2,
						constants.ValueSuccess: false,
						constants.ValueError:   constants.ErrCommitFailedCapitalized + err.Error(),
						"interest_type_code":   input.InterestTypeCode,
					})
					return
				}

				results = append(results, map[string]interface{}{
					"row":                  rowIdx + 2,
					constants.ValueSuccess: true,
					"interest_id":          interestID,
					"interest_type_code":   input.InterestTypeCode,
					"interest_type_name":   input.InterestTypeName,
					"requested":            userEmail,
				})
			}()
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// --- Single Create Handler ---
func CreateInterestTypeSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateInterestTypeSingleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if req.InterestTypeCode == "" || req.InterestTypeName == "" || req.CalculationMethod == "" {
			api.RespondWithError(w, http.StatusBadRequest, "interest_type_code, interest_type_name and calculation_method are required")
			return
		}

		// Set defaults
		if req.IsDefault == nil {
			defaultVal := false
			req.IsDefault = &defaultVal
		}
		if req.IsActive == nil {
			defaultVal := true
			req.IsActive = &defaultVal
		}

		input := InterestTypeInput{
			InterestTypeCode:  req.InterestTypeCode,
			InterestTypeName:  req.InterestTypeName,
			CalculationMethod: strings.ToUpper(req.CalculationMethod),
			MinTenorDays:      req.MinTenorDays,
			MaxTenorDays:      req.MaxTenorDays,
			IsDefault:         req.IsDefault,
			Description:       req.Description,
			IsActive:          req.IsActive,
		}

		if err := validateInterestTypeFields(input); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			api.LogError("Validation failed for interest type: %v", err)
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
			msg, status := getUserFriendlyInterestTypeError(err, "Transaction begin failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		insertQuery := `
			INSERT INTO investment.fd_interest_type_master (
				interest_type_code, interest_type_name, calculation_method,
				min_tenor_days, max_tenor_days, is_default, description, is_active
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			RETURNING interest_id
		`

		var interestID string
		err = tx.QueryRow(ctx, insertQuery,
			input.InterestTypeCode,
			input.InterestTypeName,
			input.CalculationMethod,
			input.MinTenorDays,
			input.MaxTenorDays,
			input.IsDefault,
			input.Description,
			input.IsActive,
		).Scan(&interestID)

		if err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		auditQuery := `
			INSERT INTO investment.fd_audit_interest_type (
				interest_id, action_type, processing_status, requested_by, requested_at
			) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`
		if _, err := tx.Exec(ctx, auditQuery, interestID, userEmail); err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]any{
			constants.ValueSuccess: true,
			"interest_id":          interestID,
			"interest_type_code":   req.InterestTypeCode,
			"interest_type_name":   req.InterestTypeName,
			"requested":            userEmail,
			"is_active":            req.IsActive,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Interest type created successfully: ID=%s, Code=%s", interestID, req.InterestTypeCode)
	}
}

// --- Bulk Create Handler ---
func CreateInterestType(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateInterestTypeRequest
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
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			// Set defaults
			if row.IsDefault == nil {
				defaultVal := false
				row.IsDefault = &defaultVal
			}
			if row.IsActive == nil {
				defaultVal := true
				row.IsActive = &defaultVal
			}

			if err := validateInterestTypeFields(row); err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   err.Error(),
					"interest_type_code":   row.InterestTypeCode,
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					constants.ValueError:   "Begin TX failed: " + err.Error(),
					"interest_type_code":   row.InterestTypeCode,
				})
				continue
			}

			func() {
				defer tx.Rollback(ctx)

				insertQuery := `
					INSERT INTO investment.fd_interest_type_master (
						interest_type_code, interest_type_name, calculation_method,
						min_tenor_days, max_tenor_days, is_default, description, is_active
					)
					VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
					RETURNING interest_id
				`

				var interestID string
				err = tx.QueryRow(ctx, insertQuery,
					row.InterestTypeCode,
					row.InterestTypeName,
					strings.ToUpper(row.CalculationMethod),
					row.MinTenorDays,
					row.MaxTenorDays,
					row.IsDefault,
					row.Description,
					row.IsActive,
				).Scan(&interestID)

				if err != nil {
					msg, _ := getUserFriendlyInterestTypeError(err, "Insert failed")
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false,
						constants.ValueError:   msg,
						"interest_type_code":   row.InterestTypeCode,
					})
					return
				}

				auditQuery := `
					INSERT INTO investment.fd_audit_interest_type (
						interest_id, action_type, processing_status, requested_by, requested_at
					) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`
				if _, err := tx.Exec(ctx, auditQuery, interestID, userEmail); err != nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false,
						constants.ValueError:   constants.ErrAuditInsertFailed + err.Error(),
						"interest_type_code":   row.InterestTypeCode,
					})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false,
						constants.ValueError:   constants.ErrCommitFailedCapitalized + err.Error(),
						"interest_type_code":   row.InterestTypeCode,
					})
					return
				}

				results = append(results, map[string]interface{}{
					constants.ValueSuccess: true,
					"interest_id":          interestID,
					"interest_type_code":   row.InterestTypeCode,
					"interest_type_name":   row.InterestTypeName,
					"requested":            userEmail,
				})
			}()
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// --- Single Update Handler ---
func UpdateInterestType(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdateInterestTypeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if req.InterestID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "interest_id is required")
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
			msg, status := getUserFriendlyInterestTypeError(err, "Transaction start failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Fetch existing row for "old" values
		sel := `
			SELECT interest_type_code, interest_type_name, calculation_method,
				   min_tenor_days, max_tenor_days, is_default, description, is_active
			FROM investment.fd_interest_type_master
			WHERE interest_id=$1
			FOR UPDATE`
		var oldVals [8]interface{}
		if err := tx.QueryRow(ctx, sel, req.InterestID).Scan(
			&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
			&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
		); err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Define field mapping
		fieldPairs := map[string]int{
			"interest_type_code": 0,
			"interest_type_name": 1,
			"calculation_method": 2,
			"min_tenor_days":     3,
			"max_tenor_days":     4,
			"is_default":         5,
			"description":        6,
			"is_active":          7,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if _, ok := fieldPairs[k]; ok {
				sets = append(sets, fmt.Sprintf("%s=$%d", k, pos))
				args = append(args, v)
				pos++
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}

		// Update query
		q := fmt.Sprintf("UPDATE investment.fd_interest_type_master SET %s WHERE interest_id=$%d",
			strings.Join(sets, ", "), pos)
		args = append(args, req.InterestID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, constants.ErrUpdateFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		// Insert audit record with old values
		auditCols := []string{"interest_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
		auditVals := []interface{}{req.InterestID, "EDIT", "PENDING_EDIT_APPROVAL", req.Reason, userEmail}
		auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
		paramPos := 6

		// Add old values to audit
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

		auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_interest_type (%s) VALUES (%s)",
			strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))

		if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]interface{}{
			constants.ValueSuccess: true,
			"interest_id":          req.InterestID,
			"requested":            userEmail,
			"reason":               req.Reason,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Interest type updated successfully: ID=%s, Reason=%s", req.InterestID, req.Reason)
	}
}

// --- Bulk Update Handler ---
func UpdateInterestTypeBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				InterestID string                 `json:"interest_id"`
				Fields     map[string]interface{} `json:"fields"`
				Reason     string                 `json:"reason"`
			} `json:"rows"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
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
		results := make([]map[string]interface{}, 0, len(req.Rows))

		for _, row := range req.Rows {
			if row.InterestID == "" {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: "Missing interest_id",
				})
				continue
			}

			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false, constants.ValueError: "Begin TX failed: " + err.Error(),
				})
				continue
			}

			func() {
				defer tx.Rollback(ctx)

				// Fetch existing row
				sel := `
					SELECT interest_type_code, interest_type_name, calculation_method,
						   min_tenor_days, max_tenor_days, is_default, description, is_active
					FROM investment.fd_interest_type_master
					WHERE interest_id=$1
					FOR UPDATE`
				var oldVals [8]interface{}
				if err := tx.QueryRow(ctx, sel, row.InterestID).Scan(
					&oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3],
					&oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7],
				); err != nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false, "interest_id": row.InterestID, constants.ValueError: "Fetch failed: " + err.Error(),
					})
					return
				}

				var sets []string
				var args []interface{}
				pos := 1

				fieldPairs := map[string]int{
					"interest_type_code": 0,
					"interest_type_name": 1,
					"calculation_method": 2,
					"min_tenor_days":     3,
					"max_tenor_days":     4,
					"is_default":         5,
					"description":        6,
					"is_active":          7,
				}

				for k, v := range row.Fields {
					k = strings.ToLower(k)
					if _, ok := fieldPairs[k]; ok {
						sets = append(sets, fmt.Sprintf("%s=$%d", k, pos))
						args = append(args, v)
						pos++
					}
				}

				if len(sets) == 0 {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false, "interest_id": row.InterestID, constants.ValueError: "No updatable fields found",
					})
					return
				}

				q := fmt.Sprintf("UPDATE investment.fd_interest_type_master SET %s WHERE interest_id=$%d",
					strings.Join(sets, ", "), pos)
				args = append(args, row.InterestID)

				if _, err := tx.Exec(ctx, q, args...); err != nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false, "interest_id": row.InterestID, constants.ValueError: constants.ErrUpdateFailed + err.Error(),
					})
					return
				}

				// Insert audit record with old values
				auditCols := []string{"interest_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
				auditVals := []interface{}{row.InterestID, "EDIT", "PENDING_EDIT_APPROVAL", row.Reason, userEmail}
				auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
				paramPos := 6

				// Add old values to audit
				for k := range row.Fields {
					k = strings.ToLower(k)
					if idx, ok := fieldPairs[k]; ok {
						oldField := "old_" + k
						auditCols = append(auditCols, oldField)
						auditVals = append(auditVals, oldVals[idx])
						auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
						paramPos++
					}
				}

				auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_interest_type (%s) VALUES (%s)",
					strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))

				if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false, "interest_id": row.InterestID, constants.ValueError: constants.ErrAuditInsertFailed + err.Error(),
					})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false, "interest_id": row.InterestID, constants.ValueError: constants.ErrCommitFailedCapitalized + err.Error(),
					})
					return
				}

				results = append(results, map[string]interface{}{
					constants.ValueSuccess: true,
					"interest_id":          row.InterestID,
				})
			}()
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// --- Delete Handler ---
func DeleteInterestType(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID      string   `json:"user_id"`
			InterestIDs []string `json:"interest_ids"`
			Reason      string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if len(req.InterestIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No interest IDs provided")
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
		results := make([]map[string]interface{}, 0, len(req.InterestIDs))

		for _, id := range req.InterestIDs {
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				results = append(results, map[string]interface{}{
					constants.ValueSuccess: false,
					"interest_id":          id,
					constants.ValueError:   "Begin TX failed: " + err.Error(),
				})
				continue
			}

			func() {
				defer tx.Rollback(ctx)

				// Mark as deleted
				if _, err := tx.Exec(ctx, "UPDATE investment.fd_interest_type_master SET is_deleted=true WHERE interest_id=$1", id); err != nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false,
						"interest_id":          id,
						constants.ValueError:   "Delete failed: " + err.Error(),
					})
					return
				}

				// Insert audit
				audit := `
					INSERT INTO investment.fd_audit_interest_type
						(interest_id, action_type, processing_status, reason, requested_by, requested_at)
					VALUES ($1,'DELETE','PENDING_DELETE_APPROVAL',$2,$3,now())`
				if _, err := tx.Exec(ctx, audit, id, req.Reason, userEmail); err != nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false,
						"interest_id":          id,
						constants.ValueError:   constants.ErrAuditInsertFailed + err.Error(),
					})
					return
				}

				if err := tx.Commit(ctx); err != nil {
					results = append(results, map[string]interface{}{
						constants.ValueSuccess: false,
						"interest_id":          id,
						constants.ValueError:   constants.ErrCommitFailedCapitalized + err.Error(),
					})
					return
				}

				results = append(results, map[string]interface{}{
					constants.ValueSuccess: true,
					"interest_id":          id,
				})
			}()
		}

		api.RespondWithPayload(w, api.IsBulkSuccess(results), "", results)
	}
}

// --- Bulk Approve Handler ---
func BulkApproveInterestType(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			AuditIDs []string `json:"audit_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if len(req.AuditIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No audit IDs provided")
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
			msg, status := getUserFriendlyInterestTypeError(err, "Begin transaction failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Bulk approve
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_interest_type 
			SET processing_status = 'APPROVED', checker_by = $1, checker_at = now(), checker_comment = $2
			WHERE audit_id = ANY($3) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.AuditIDs)

		if err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, "Approval failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]interface{}{
			constants.ValueSuccess: true,
			"approved_count":       len(req.AuditIDs),
			"checker":              userEmail,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Bulk approval completed: %d audit IDs approved by %s", len(req.AuditIDs), userEmail)
	}
}

// --- Bulk Reject Handler ---
func BulkRejectInterestType(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID   string   `json:"user_id"`
			AuditIDs []string `json:"audit_ids"`
			Comment  string   `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if len(req.AuditIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No audit IDs provided")
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
			msg, status := getUserFriendlyInterestTypeError(err, "Begin transaction failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Bulk reject
		_, err = tx.Exec(ctx, `
			UPDATE investment.fd_audit_interest_type 
			SET processing_status = 'REJECTED', checker_by = $1, checker_at = now(), checker_comment = $2
			WHERE audit_id = ANY($3) AND processing_status LIKE '%PENDING%'
		`, userEmail, req.Comment, req.AuditIDs)

		if err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, "Rejection failed")
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]interface{}{
			constants.ValueSuccess: true,
			"rejected_count":       len(req.AuditIDs),
			"checker":              userEmail,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Bulk rejection completed: %d audit IDs rejected by %s", len(req.AuditIDs), userEmail)
	}
}

// --- Get Active Approved Handler ---
func GetInterestTypesApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			SELECT 
				m.interest_id,
				m.interest_type_code,
				m.interest_type_name,
				m.calculation_method,
				m.min_tenor_days,
				m.max_tenor_days,
				m.is_default,
				m.description,
				m.is_active
			FROM investment.fd_interest_type_master m
			INNER JOIN investment.fd_audit_interest_type a ON a.interest_id = m.interest_id
			WHERE a.processing_status = 'APPROVED' 
				AND m.is_active = true 
				AND COALESCE(m.is_deleted, false) = false
			ORDER BY m.interest_type_name
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, "Query failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)

		for rows.Next() {
			var id, code, name, method, desc string
			var minTenor, maxTenor *int
			var isDefault, isActive bool

			if err := rows.Scan(&id, &code, &name, &method, &minTenor, &maxTenor, &isDefault, &desc, &isActive); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				api.LogError("Database scan error in GetInterestTypesApprovedActive: %v", err)
				return
			}

			out = append(out, map[string]interface{}{
				"interest_id":        id,
				"interest_type_code": code,
				"interest_type_name": name,
				"calculation_method": method,
				"min_tenor_days":     minTenor,
				"max_tenor_days":     maxTenor,
				"is_default":         isDefault,
				"description":        desc,
				"is_active":          isActive,
			})
		}
		api.RespondWithPayload(w, true, "", out)
		api.LogInfo("Retrieved %d active approved interest types", len(out))
	}
}

// --- Get All with Audit Handler ---
func GetInterestTypesWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.interest_id)
					a.interest_id,
					a.processing_status,
					a.action_type,
					a.audit_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason,
					a.old_interest_type_code,
					a.old_interest_type_name,
					a.old_calculation_method,
					a.old_min_tenor_days,
					a.old_max_tenor_days,
					a.old_is_default,
					a.old_description,
					a.old_is_active
				FROM investment.fd_audit_interest_type a
				ORDER BY a.interest_id, GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT 
					interest_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.fd_audit_interest_type
				GROUP BY interest_id
			)
			SELECT
				m.interest_id,
				COALESCE(m.interest_type_code,'') AS interest_type_code,
				COALESCE(l.old_interest_type_code,'') AS old_interest_type_code,
				COALESCE(m.interest_type_name,'') AS interest_type_name,
				COALESCE(l.old_interest_type_name,'') AS old_interest_type_name,
				COALESCE(m.calculation_method,'') AS calculation_method,
				COALESCE(l.old_calculation_method,'') AS old_calculation_method,
				COALESCE(m.min_tenor_days,0) AS min_tenor_days,
				COALESCE(l.old_min_tenor_days,0) AS old_min_tenor_days,
				COALESCE(m.max_tenor_days,0) AS max_tenor_days,
				COALESCE(l.old_max_tenor_days,0) AS old_max_tenor_days,
				COALESCE(m.is_default,false) AS is_default,
				COALESCE(l.old_is_default,false) AS old_is_default,
				COALESCE(m.description,'') AS description,
				COALESCE(l.old_description,'') AS old_description,
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

			FROM investment.fd_interest_type_master m
			LEFT JOIN latest_audit l ON l.interest_id = m.interest_id
			LEFT JOIN history h ON h.interest_id = m.interest_id
			WHERE COALESCE(m.is_deleted,false)=false
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, "Query failed")
			api.RespondWithError(w, status, msg)
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
			api.LogError("Row iteration error in GetInterestTypesWithAudit: %v", rows.Err())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"rows":                 out,
		})
		api.LogInfo("Retrieved %d interest types with audit data", len(out))
	}
}

// --- Get Audit Logs Handler ---
func GetInterestTypeAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		interestID := r.URL.Query().Get("interest_id")

		var q string
		var args []interface{}

		if interestID != "" {
			q = `
				SELECT 
					a.audit_id,
					a.interest_id,
					a.action_type,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment,

					-- Current/New values from master table
					COALESCE(m.interest_type_code,'') AS interest_type_code,
					COALESCE(m.interest_type_name,'') AS interest_type_name,
					COALESCE(m.calculation_method,'') AS calculation_method,
					COALESCE(m.min_tenor_days,0) AS min_tenor_days,
					COALESCE(m.max_tenor_days,0) AS max_tenor_days,
					COALESCE(m.is_default,false) AS is_default,
					COALESCE(m.description,'') AS description,
					COALESCE(m.is_active,false) AS is_active,
					COALESCE(m.is_deleted,false) AS is_deleted,

					-- Old values from audit table
					COALESCE(a.old_interest_type_code,'') AS old_interest_type_code,
					COALESCE(a.old_interest_type_name,'') AS old_interest_type_name,
					COALESCE(a.old_calculation_method,'') AS old_calculation_method,
					COALESCE(a.old_min_tenor_days,0) AS old_min_tenor_days,
					COALESCE(a.old_max_tenor_days,0) AS old_max_tenor_days,
					COALESCE(a.old_is_default,false) AS old_is_default,
					COALESCE(a.old_description,'') AS old_description,
					COALESCE(a.old_is_active,false) AS old_is_active

				FROM investment.fd_audit_interest_type a
				LEFT JOIN investment.fd_interest_type_master m ON m.interest_id = a.interest_id
				WHERE a.interest_id = $1
				ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
			`
			args = append(args, interestID)
		} else {
			q = `
				SELECT 
					a.audit_id,
					a.interest_id,
					a.action_type,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment,

					-- Current/New values from master table
					COALESCE(m.interest_type_code,'') AS interest_type_code,
					COALESCE(m.interest_type_name,'') AS interest_type_name,
					COALESCE(m.calculation_method,'') AS calculation_method,
					COALESCE(m.min_tenor_days,0) AS min_tenor_days,
					COALESCE(m.max_tenor_days,0) AS max_tenor_days,
					COALESCE(m.is_default,false) AS is_default,
					COALESCE(m.description,'') AS description,
					COALESCE(m.is_active,false) AS is_active,
					COALESCE(m.is_deleted,false) AS is_deleted,

					-- Old values from audit table
					COALESCE(a.old_interest_type_code,'') AS old_interest_type_code,
					COALESCE(a.old_interest_type_name,'') AS old_interest_type_name,
					COALESCE(a.old_calculation_method,'') AS old_calculation_method,
					COALESCE(a.old_min_tenor_days,0) AS old_min_tenor_days,
					COALESCE(a.old_max_tenor_days,0) AS old_max_tenor_days,
					COALESCE(a.old_is_default,false) AS old_is_default,
					COALESCE(a.old_description,'') AS old_description,
					COALESCE(a.old_is_active,false) AS old_is_active

				FROM investment.fd_audit_interest_type a
				LEFT JOIN investment.fd_interest_type_master m ON m.interest_id = a.interest_id
				ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
				LIMIT 1000
			`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyInterestTypeError(err, "Query failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)

		for rows.Next() {
			// Audit fields
			var auditID, intID, actionType, processStatus, reason, reqBy, reqAt, checkerBy, checkerAt, checkerComment string

			// Current/New values
			var currentCode, currentName, currentMethod, currentDesc string
			var currentMinTenor, currentMaxTenor int
			var currentIsDefault, currentIsActive, currentIsDeleted bool

			// Old values
			var oldCode, oldName, oldMethod, oldDesc string
			var oldMinTenor, oldMaxTenor int
			var oldIsDefault, oldIsActive bool

			if err := rows.Scan(
				// Audit fields
				&auditID, &intID, &actionType, &processStatus, &reason, &reqBy, &reqAt, &checkerBy, &checkerAt, &checkerComment,
				// Current values
				&currentCode, &currentName, &currentMethod, &currentMinTenor, &currentMaxTenor, &currentIsDefault, &currentDesc, &currentIsActive, &currentIsDeleted,
				// Old values
				&oldCode, &oldName, &oldMethod, &oldMinTenor, &oldMaxTenor, &oldIsDefault, &oldDesc, &oldIsActive,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				api.LogError("Database scan error in GetInterestTypeAuditHistory: %v", err)
				return
			}

			out = append(out, map[string]interface{}{
				// Audit information
				"audit_id":          auditID,
				"interest_id":       intID,
				"action_type":       actionType,
				"processing_status": processStatus,
				"reason":            reason,
				"requested_by":      reqBy,
				"requested_at":      reqAt,
				"checker_by":        checkerBy,
				"checker_at":        checkerAt,
				"checker_comment":   checkerComment,

				// Current/New values
				"interest_type_code": currentCode,
				"interest_type_name": currentName,
				"calculation_method": currentMethod,
				"min_tenor_days":     currentMinTenor,
				"max_tenor_days":     currentMaxTenor,
				"is_default":         currentIsDefault,
				"description":        currentDesc,
				"is_active":          currentIsActive,
				"is_deleted":         currentIsDeleted,

				// Old values (what was changed FROM)
				"old_interest_type_code": oldCode,
				"old_interest_type_name": oldName,
				"old_calculation_method": oldMethod,
				"old_min_tenor_days":     oldMinTenor,
				"old_max_tenor_days":     oldMaxTenor,
				"old_is_default":         oldIsDefault,
				"old_description":        oldDesc,
				"old_is_active":          oldIsActive,
			})
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row iteration error: "+rows.Err().Error())
			api.LogError("Row iteration error in GetInterestTypeAuditHistory: %v", rows.Err())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"audit_logs":           out,
		})
		api.LogInfo("Retrieved %d audit history records", len(out))
	}
}

// --- Helper functions for CSV/Excel parsing ---
func parseCSVFile(file multipart.File) ([][]string, error) {
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow variable field counts
	return reader.ReadAll()
}

func parseXLSXFile(file multipart.File) ([][]string, error) {
	buf := bytes.NewBuffer(nil)
	if _, err := io.Copy(buf, file); err != nil {
		return nil, err
	}

	f, err := excelize.OpenReader(buf)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sheetName := f.GetSheetName(0)
	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func getColumnValue(row []string, colMap map[string]int, colName string) string {
	if idx, exists := colMap[colName]; exists && idx < len(row) {
		return strings.TrimSpace(row[idx])
	}
	return ""
}

func parseIntPtr(s string) (*int, error) {
	if s == "" {
		return nil, nil
	}
	val := 0
	if _, err := fmt.Sscanf(s, "%d", &val); err != nil {
		return nil, err
	}
	return &val, nil
}

func parseBoolPtr(s string) (*bool, error) {
	if s == "" {
		return nil, nil
	}
	s = strings.ToLower(strings.TrimSpace(s))

	switch s {
	case "true", "1", "yes", "y", "on", "active":
		val := true
		return &val, nil
	case "false", "0", "no", "n", "off", "inactive":
		val := false
		return &val, nil
	default:
		return nil, errors.New("invalid boolean value")
	}
}
