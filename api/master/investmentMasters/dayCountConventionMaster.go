package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyDayCountError converts database errors to user-friendly messages
func getUserFriendlyDayCountError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}
	errStr := strings.ToLower(err.Error())

	if strings.Contains(errStr, constants.ErrTxBeginFailed) ||
		strings.Contains(errStr, constants.ErrAuditInsertFailed) {
		return err.Error(), http.StatusOK
	}

	if strings.Contains(errStr, "uniq_day_count_name_active") ||
		(strings.Contains(errStr, constants.ErrDuplicateKeyC) && strings.Contains(errStr, "day_count_name")) {
		return "Day count convention name already exists and is active.", http.StatusOK
	}

	if strings.Contains(errStr, "inv_day_count_convention_type_chk") ||
		(strings.Contains(errStr, constants.CheckConstraint) && strings.Contains(errStr, "convention_type")) {
		return "Invalid convention_type. Must be one of: ACT_365, ACT_360, ACT_ACT, 30_360.", http.StatusBadRequest
	}

	if strings.Contains(errStr, constants.ErrDuplicateKeyC) {
		return "Duplicate entry detected. This record already exists.", http.StatusOK
	}

	if strings.Contains(errStr, "foreign key constraint") || strings.Contains(errStr, "violates foreign key") {
		return "Referenced record not found or invalid.", http.StatusOK
	}

	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue", http.StatusServiceUnavailable
	}

	return "Internal server error: " + context, http.StatusInternalServerError
}

// --- Request Types ---
type DayCountConventionInput struct {
	DayCountName   string   `json:"day_count_name"`
	ConventionType string   `json:"convention_type"` // ACT_365|ACT_360|ACT_ACT|30_360
	Description    *string  `json:"description"`
	FormulaExample *string  `json:"formula_example"`
	IsActive       *bool    `json:"is_active"`
	UsedByBanks    []string `json:"used_by_banks"` // populates fd_day_count_bank_map
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

// syncDayCountBankMap deletes existing map rows for a day_count_code and inserts the new set.
// Must be called inside a transaction.
func syncDayCountBankMap(ctx interface{ Value(interface{}) interface{} }, tx interface {
	Exec(ctx interface{}, sql string, args ...interface{}) (interface{}, error)
}, dayCountCode string, bankCodes []string) error {
	return nil // placeholder - actual implementation inline below
}

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
		defer file.Close()

		ext := strings.ToLower(filepath.Ext(handler.Filename))
		if ext != ".csv" && ext != ".xlsx" {
			api.RespondWithError(w, http.StatusBadRequest, "Only .csv and .xlsx files are supported")
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

		var data [][]string
		if ext == ".csv" {
			data, err = parseCSVFile(file)
		} else {
			data, err = parseXLSXFile(file)
		}
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "Failed to parse file: "+err.Error())
			return
		}

		if len(data) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, "File must have a header row and at least one data row")
			return
		}

		// Build column index map from header row
		header := data[0]
		colMap := make(map[string]int)
		for i, col := range header {
			colMap[strings.ToLower(strings.TrimSpace(col))] = i
		}

		requiredCols := []string{"day_count_name", "convention_type"}
		for _, col := range requiredCols {
			if _, ok := colMap[col]; !ok {
				api.RespondWithError(w, http.StatusBadRequest, "Missing required column: "+col)
				return
			}
		}

		ctx := r.Context()

		// Pre-validate rows
		var validInputs []DayCountConventionInput
		var errors []map[string]interface{}

		for i, row := range data[1:] {
			if len(row) == 0 {
				continue
			}
			input := DayCountConventionInput{
				DayCountName:   getColumnValue(row, colMap, "day_count_name"),
				ConventionType: strings.ToUpper(getColumnValue(row, colMap, "convention_type")),
			}

			if desc := getColumnValue(row, colMap, "description"); desc != "" {
				input.Description = &desc
			}
			if fe := getColumnValue(row, colMap, "formula_example"); fe != "" {
				input.FormulaExample = &fe
			}
			isActiveStr := getColumnValue(row, colMap, "is_active")
			if isActiveStr != "" {
				bv, bErr := parseBoolPtr(isActiveStr)
				if bErr != nil {
					errors = append(errors, map[string]interface{}{
						"row_index":            i + 2,
						constants.ValueSuccess: false,
						constants.ValueError:   "Invalid is_active value: " + isActiveStr,
					})
					continue
				}
				input.IsActive = bv
			}

			if err := validateDayCountFields(input); err != nil {
				errors = append(errors, map[string]interface{}{
					"row_index":            i + 2,
					constants.ValueSuccess: false,
					constants.ValueError:   err.Error(),
				})
				continue
			}
			validInputs = append(validInputs, input)
		}

		if len(validInputs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errors)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			return
		}
		defer tx.Rollback(ctx)

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

		var finalValid []DayCountConventionInput
		for _, input := range validInputs {
			if existingNames[input.DayCountName] {
				errors = append(errors, map[string]interface{}{
					"day_count_name":       input.DayCountName,
					constants.ValueSuccess: false,
					constants.ValueError:   "Day count convention name already exists and is active.",
				})
			} else {
				finalValid = append(finalValid, input)
			}
		}

		if len(finalValid) == 0 {
			api.RespondWithPayload(w, false, "All rows failed validation or are duplicates", errors)
			return
		}

		// Batch insert
		valueStrings := make([]string, len(finalValid))
		valueArgs := make([]interface{}, 0, len(finalValid)*5)

		for i, input := range finalValid {
			isActive := true
			if input.IsActive != nil {
				isActive = *input.IsActive
			}
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)", i*5+1, i*5+2, i*5+3, i*5+4, i*5+5)
			valueArgs = append(valueArgs, input.DayCountName, input.ConventionType, input.Description, input.FormulaExample, isActive)
		}

		batchInsertQuery := fmt.Sprintf(`
			INSERT INTO investment.fd_day_count_convention_master
				(day_count_name, convention_type, description, formula_example, is_active)
			VALUES %s
			RETURNING day_count_code
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
			if err := insertRows.Scan(&code); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Insert scan failed: "+err.Error())
				return
			}
			insertedCodes = append(insertedCodes, code)
			insertedRecords = append(insertedRecords, map[string]interface{}{
				constants.ValueSuccess: true,
				"day_count_code":       code,
			})
		}
		insertRows.Close()

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

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			return
		}

		allResults := append(insertedRecords, errors...)
		success := len(insertedRecords) > 0
		api.RespondWithPayload(w, success, "", allResults)
		api.LogInfo("DayCount upload: %d inserted, %d errors from file %s", len(insertedRecords), len(errors), handler.Filename)
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
		err = tx.QueryRow(ctx, `
			INSERT INTO investment.fd_day_count_convention_master
				(day_count_name, convention_type, description, formula_example, is_active)
			VALUES ($1,$2,$3,$4,$5)
			RETURNING day_count_code
		`, req.DayCountName, req.ConventionType, req.Description, req.FormulaExample, req.IsActive).Scan(&dayCountCode)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Insert bank map entries if provided
		if len(req.UsedByBanks) > 0 {
			bmValues := make([]string, len(req.UsedByBanks))
			bmArgs := make([]interface{}, 0, len(req.UsedByBanks)*2)
			for i, bc := range req.UsedByBanks {
				bmValues[i] = fmt.Sprintf(constants.FormatTuple, i*2+1, i*2+2)
				bmArgs = append(bmArgs, dayCountCode, bc)
			}
			_, err = tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO investment.fd_day_count_bank_map (day_count_code, bank_code)
				VALUES %s ON CONFLICT (day_count_code, bank_code) DO NOTHING
			`, strings.Join(bmValues, ",")), bmArgs...)
			if err != nil {
				msg, status := getUserFriendlyDayCountError(err, "Bank map insert failed")
				api.RespondWithError(w, status, msg)
				return
			}
		}

		// Audit record
		if _, err := tx.Exec(ctx, `
			INSERT INTO investment.fd_audit_day_count_convention
				(day_count_code, action_type, processing_status, requested_by, requested_at)
			VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())
		`, dayCountCode, userEmail); err != nil {
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
			"day_count_code":       dayCountCode,
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

		for i, row := range req.Rows {
			row.ConventionType = strings.ToUpper(strings.TrimSpace(row.ConventionType))
			if row.IsActive == nil {
				t := true
				row.IsActive = &t
			}
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

		names := make([]string, len(validRows))
		for i, r := range validRows {
			names[i] = r.DayCountName
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
		dupRows.Close()

		var finalValid []DayCountConventionInput
		for i, input := range validRows {
			if existingNames[input.DayCountName] {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					"day_count_name":       input.DayCountName,
					constants.ValueSuccess: false,
					constants.ValueError:   "Day count name already exists and is active.",
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
		valueArgs := make([]interface{}, 0, len(finalValid)*5)
		for i, input := range finalValid {
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)", i*5+1, i*5+2, i*5+3, i*5+4, i*5+5)
			valueArgs = append(valueArgs, input.DayCountName, input.ConventionType, input.Description, input.FormulaExample, input.IsActive)
		}

		batchInsertQuery := fmt.Sprintf(`
			INSERT INTO investment.fd_day_count_convention_master
				(day_count_name, convention_type, description, formula_example, is_active)
			VALUES %s RETURNING day_count_code
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
		codeIdx := 0

		for insertRows.Next() {
			var code string
			if err := insertRows.Scan(&code); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				return
			}
			// Insert bank maps if any
			if codeIdx < len(finalValid) && len(finalValid[codeIdx].UsedByBanks) > 0 {
				bmValues := make([]string, len(finalValid[codeIdx].UsedByBanks))
				bmArgs := make([]interface{}, 0, len(finalValid[codeIdx].UsedByBanks)*2)
				for j, bc := range finalValid[codeIdx].UsedByBanks {
					bmValues[j] = fmt.Sprintf(constants.FormatTuple, j*2+1, j*2+2)
					bmArgs = append(bmArgs, code, bc)
				}
				_, bmerr := tx.Exec(ctx, fmt.Sprintf(`
					INSERT INTO investment.fd_day_count_bank_map (day_count_code, bank_code)
					VALUES %s ON CONFLICT (day_count_code, bank_code) DO NOTHING
				`, strings.Join(bmValues, ",")), bmArgs...)
				if bmerr != nil {
					api.LogError("Bank map insert failed for %s: %v", code, bmerr)
				}
			}
			insertedCodes = append(insertedCodes, code)
			insertedRecords = append(insertedRecords, map[string]interface{}{
				constants.ValueSuccess: true,
				"day_count_code":       code,
			})
			codeIdx++
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
			api.RespondWithError(w, http.StatusBadRequest, "day_count_code is required")
			return
		}
		if len(req.Fields) == 0 && req.UsedByBanks == nil {
			api.RespondWithError(w, http.StatusBadRequest, "No fields or used_by_banks provided for update")
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
			msg, status := getUserFriendlyDayCountError(err, "Transaction start failed")
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

		// Sync bank map if provided
		if req.UsedByBanks != nil {
			// Delete existing
			if _, err := tx.Exec(ctx, `DELETE FROM investment.fd_day_count_bank_map WHERE day_count_code=$1`, req.DayCountCode); err != nil {
				msg, status := getUserFriendlyDayCountError(err, "Bank map delete failed")
				api.RespondWithError(w, status, msg)
				return
			}
			// Insert new
			if len(*req.UsedByBanks) > 0 {
				bmValues := make([]string, len(*req.UsedByBanks))
				bmArgs := make([]interface{}, 0, len(*req.UsedByBanks)*2)
				for i, bc := range *req.UsedByBanks {
					bmValues[i] = fmt.Sprintf(constants.FormatTuple, i*2+1, i*2+2)
					bmArgs = append(bmArgs, req.DayCountCode, bc)
				}
				_, err = tx.Exec(ctx, fmt.Sprintf(`
					INSERT INTO investment.fd_day_count_bank_map (day_count_code, bank_code)
					VALUES %s ON CONFLICT (day_count_code, bank_code) DO NOTHING
				`, strings.Join(bmValues, ",")), bmArgs...)
				if err != nil {
					msg, status := getUserFriendlyDayCountError(err, "Bank map insert failed")
					api.RespondWithError(w, status, msg)
					return
				}
			}
		}

		// Build audit record
		auditCols := []string{"day_count_code", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
		auditVals := []interface{}{req.DayCountCode, "EDIT", "PENDING_EDIT_APPROVAL", req.Reason, userEmail}
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
				UsedByBanks  *[]string              `json:"used_by_banks"`
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
			UsedByBanks  *[]string
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
			if len(row.Fields) == 0 && row.UsedByBanks == nil {
				errResults = append(errResults, map[string]interface{}{
					"row_index":            i,
					"day_count_code":       row.DayCountCode,
					constants.ValueSuccess: false,
					constants.ValueError:   "No fields to update",
				})
				continue
			}
			validUpdates = append(validUpdates, validUpdate{row.DayCountCode, row.Fields, row.UsedByBanks, row.Reason})
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

			if update.UsedByBanks != nil {
				if _, err := tx.Exec(ctx, `DELETE FROM investment.fd_day_count_bank_map WHERE day_count_code=$1`, update.DayCountCode); err != nil {
					errResults = append(errResults, map[string]interface{}{
						"day_count_code":       update.DayCountCode,
						constants.ValueSuccess: false,
						constants.ValueError:   "Bank map clear failed: " + err.Error(),
					})
					continue
				}
				if len(*update.UsedByBanks) > 0 {
					bmValues := make([]string, len(*update.UsedByBanks))
					bmArgs := make([]interface{}, 0, len(*update.UsedByBanks)*2)
					for i, bc := range *update.UsedByBanks {
						bmValues[i] = fmt.Sprintf(constants.FormatTuple, i*2+1, i*2+2)
						bmArgs = append(bmArgs, update.DayCountCode, bc)
					}
					_, err = tx.Exec(ctx, fmt.Sprintf(`
						INSERT INTO investment.fd_day_count_bank_map (day_count_code, bank_code)
						VALUES %s ON CONFLICT (day_count_code, bank_code) DO NOTHING
					`, strings.Join(bmValues, ",")), bmArgs...)
					if err != nil {
						errResults = append(errResults, map[string]interface{}{
							"day_count_code":       update.DayCountCode,
							constants.ValueSuccess: false,
							constants.ValueError:   "Bank map insert failed: " + err.Error(),
						})
						continue
					}
				}
			}

			// Audit
			auditCols := []string{"day_count_code", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
			auditVals := []interface{}{update.DayCountCode, "EDIT", "PENDING_EDIT_APPROVAL", update.Reason, userEmail}
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

		// Handle DELETE approvals - set is_deleted=true (bank_map cascade handled by FK)
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
			"approved_count":       len(req.DayCountCodes),
			"checker":              userEmail,
		})
		api.LogInfo("DayCount bulk approve: %d approved by %s", len(req.DayCountCodes), userEmail)
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
			SELECT m.day_count_code, m.day_count_name, m.convention_type,
			       COALESCE(m.description,'') AS description,
			       COALESCE(m.formula_example,'') AS formula_example,
			       m.is_active,
			       COALESCE(array_agg(bm.bank_code ORDER BY bm.bank_code) FILTER (WHERE bm.bank_code IS NOT NULL), '{}') AS used_by_banks,
			       COALESCE(array_agg(COALESCE(mb.bank_name,'') ORDER BY bm.bank_code) FILTER (WHERE bm.bank_code IS NOT NULL), '{}') AS used_by_bank_names
			FROM investment.fd_day_count_convention_master m
			INNER JOIN investment.fd_audit_day_count_convention a ON a.day_count_code = m.day_count_code
			LEFT JOIN investment.fd_day_count_bank_map bm ON bm.day_count_code = m.day_count_code
			LEFT JOIN masterbank mb ON mb.bank_id::text = bm.bank_code
			WHERE a.processing_status='APPROVED' AND m.is_active=true AND COALESCE(m.is_deleted,false)=false
			GROUP BY m.day_count_code, m.day_count_name, m.convention_type, m.description, m.formula_example, m.is_active
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
			var code, name, convType, desc, formula string
			var isActive bool
			var usedByBanks []string
			var usedByBankNames []string

			if err := rows.Scan(&code, &name, &convType, &desc, &formula, &isActive, &usedByBanks, &usedByBankNames); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				return
			}
			if usedByBanks == nil {
				usedByBanks = []string{}
			}
			if usedByBankNames == nil {
				usedByBankNames = []string{}
			}
			out = append(out, map[string]interface{}{
				"day_count_code":     code,
				"day_count_name":     name,
				"convention_type":    convType,
				"description":        desc,
				"formula_example":    formula,
				"is_active":          isActive,
				"used_by_banks":      usedByBanks,
				"used_by_bank_names": usedByBankNames,
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
				ORDER BY a.day_count_code,
				         GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT
					day_count_code,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT'   THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT'   THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.fd_audit_day_count_convention
				GROUP BY day_count_code
			)
			SELECT
				m.day_count_code,
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

				COALESCE(l.processing_status,'')      AS processing_status,
				COALESCE(l.action_type,'')            AS action_type,
				COALESCE(l.audit_id::text,'')         AS audit_id,
				COALESCE(l.requested_by,'')           AS requested_by,
				TO_CHAR(l.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(l.checker_by,'')             AS checker_by,
				TO_CHAR(l.checker_at,'YYYY-MM-DD HH24:MI:SS')   AS checker_at,
				COALESCE(l.checker_comment,'')        AS checker_comment,
				COALESCE(l.reason,'')                 AS reason,

				COALESCE(h.created_by,'')             AS created_by,
				COALESCE(h.created_at,'')             AS created_at,
				COALESCE(h.edited_by,'')              AS edited_by,
				COALESCE(h.edited_at,'')              AS edited_at,
				COALESCE(h.deleted_by,'')             AS deleted_by,
				COALESCE(h.deleted_at,'')             AS deleted_at,

				COALESCE(array_agg(bm.bank_code ORDER BY bm.bank_code) FILTER (WHERE bm.bank_code IS NOT NULL), '{}') AS used_by_banks,
				COALESCE(array_agg(COALESCE(mb.bank_name,'') ORDER BY bm.bank_code) FILTER (WHERE bm.bank_code IS NOT NULL), '{}') AS used_by_bank_names

			FROM investment.fd_day_count_convention_master m
			LEFT JOIN latest_audit l ON l.day_count_code = m.day_count_code
			LEFT JOIN history h      ON h.day_count_code = m.day_count_code
			LEFT JOIN investment.fd_day_count_bank_map bm ON bm.day_count_code = m.day_count_code
			LEFT JOIN masterbank mb ON mb.bank_id::text = bm.bank_code
			WHERE COALESCE(m.is_deleted,false) = false
			GROUP BY
				m.day_count_code, m.day_count_name, l.old_day_count_name,
				m.convention_type, l.old_convention_type,
				m.description, l.old_description,
				m.formula_example, l.old_formula_example,
				m.is_active, l.old_is_active, m.is_deleted,
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

// GetDayCountConventionAuditHistory returns audit history for day count conventions
func GetDayCountConventionAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		dayCountCode := r.URL.Query().Get("day_count_code")

		var q string
		var args []interface{}

		baseSelect := `
			SELECT
				a.audit_id,
				a.day_count_code,
				a.action_type,
				a.processing_status,
				COALESCE(a.reason,'') AS reason,
				COALESCE(a.requested_by,'') AS requested_by,
				TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
				COALESCE(a.checker_by,'') AS checker_by,
				TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
				COALESCE(a.checker_comment,'') AS checker_comment,

				COALESCE(m.day_count_name,'') AS day_count_name,
				COALESCE(m.convention_type,'') AS convention_type,
				COALESCE(m.description,'') AS description,
				COALESCE(m.formula_example,'') AS formula_example,
				COALESCE(m.is_active,false) AS is_active,
				COALESCE(m.is_deleted,false) AS is_deleted,

				COALESCE(a.old_day_count_name,'') AS old_day_count_name,
				COALESCE(a.old_convention_type,'') AS old_convention_type,
				COALESCE(a.old_description,'') AS old_description,
				COALESCE(a.old_formula_example,'') AS old_formula_example,
				COALESCE(a.old_is_active,false) AS old_is_active

			FROM investment.fd_audit_day_count_convention a
			LEFT JOIN investment.fd_day_count_convention_master m ON m.day_count_code = a.day_count_code`

		if dayCountCode != "" {
			q = baseSelect + `
			WHERE a.day_count_code = $1
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC`
			args = append(args, dayCountCode)
		} else {
			q = baseSelect + `
			ORDER BY GREATEST(COALESCE(a.requested_at,'1970-01-01'::timestamp), COALESCE(a.checker_at,'1970-01-01'::timestamp)) DESC
			LIMIT 1000`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyDayCountError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)

		for rows.Next() {
			var auditID, code, actionType, processStatus, reason, reqBy, reqAt, checkerBy, checkerAt, checkerComment string
			var curName, curType, curDesc, curFormula string
			var curIsActive, curIsDeleted bool
			var oldName, oldType, oldDesc, oldFormula string
			var oldIsActive bool

			if err := rows.Scan(
				&auditID, &code, &actionType, &processStatus, &reason, &reqBy, &reqAt, &checkerBy, &checkerAt, &checkerComment,
				&curName, &curType, &curDesc, &curFormula, &curIsActive, &curIsDeleted,
				&oldName, &oldType, &oldDesc, &oldFormula, &oldIsActive,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				return
			}

			out = append(out, map[string]interface{}{
				"audit_id":          auditID,
				"day_count_code":    code,
				"action_type":       actionType,
				"processing_status": processStatus,
				"reason":            reason,
				"requested_by":      reqBy,
				"requested_at":      reqAt,
				"checker_by":        checkerBy,
				"checker_at":        checkerAt,
				"checker_comment":   checkerComment,

				"day_count_name":  curName,
				"convention_type": curType,
				"description":     curDesc,
				"formula_example": curFormula,
				"is_active":       curIsActive,
				"is_deleted":      curIsDeleted,

				"old_day_count_name":  oldName,
				"old_convention_type": oldType,
				"old_description":     oldDesc,
				"old_formula_example": oldFormula,
				"old_is_active":       oldIsActive,
			})
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row iteration error: "+rows.Err().Error())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"audit_logs":           out,
		})
		api.LogInfo("DayCount AuditHistory: returned %d records", len(out))
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
			api.RespondWithError(w, http.StatusBadRequest, "day_count_code is required")
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
			SELECT m.day_count_code, m.day_count_name, m.convention_type,
			       COALESCE(m.description,'') AS description,
			       COALESCE(m.formula_example,'') AS formula_example,
			       m.is_active, COALESCE(m.is_deleted,false) AS is_deleted,
			       COALESCE(array_agg(bm.bank_code ORDER BY bm.bank_code) FILTER (WHERE bm.bank_code IS NOT NULL), '{}') AS used_by_banks,
			       COALESCE(array_agg(COALESCE(mb.bank_name,'') ORDER BY bm.bank_code) FILTER (WHERE bm.bank_code IS NOT NULL), '{}') AS used_by_bank_names
			FROM investment.fd_day_count_convention_master m
			LEFT JOIN investment.fd_day_count_bank_map bm ON bm.day_count_code = m.day_count_code
			LEFT JOIN masterbank mb ON mb.bank_id::text = bm.bank_code
			WHERE m.day_count_code = $1
			GROUP BY m.day_count_code, m.day_count_name, m.convention_type, m.description, m.formula_example, m.is_active, m.is_deleted
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

		var code, name, convType, desc, formula string
		var isActive, isDeleted bool
		var usedByBanks []string
		var usedByBankNames []string

		if err := rows.Scan(&code, &name, &convType, &desc, &formula, &isActive, &isDeleted, &usedByBanks, &usedByBankNames); err != nil {
			msg, status := getUserFriendlyDayCountError(err, "Scan failed")
			api.RespondWithError(w, status, msg)
			return
		}
		if usedByBanks == nil {
			usedByBanks = []string{}
		}
		if usedByBankNames == nil {
			usedByBankNames = []string{}
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"day_count_code":     code,
			"day_count_name":     name,
			"convention_type":    convType,
			"description":        desc,
			"formula_example":    formula,
			"is_active":          isActive,
			"is_deleted":         isDeleted,
			"used_by_banks":      usedByBanks,
			"used_by_bank_names": usedByBankNames,
		})
		api.LogInfo("GetDayCountConvention: code=%s by %s", code, userEmail)
	}
}
