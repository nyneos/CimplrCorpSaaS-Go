package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	// "time"
	"path/filepath"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// getUserFriendlyPenaltyError converts database errors to user-friendly messages.
// It unwraps pgconn.PgError for precise constraint/type details.
func getUserFriendlyPenaltyError(err error, context string) (string, int) {
	if err == nil {
		return "", http.StatusOK
	}

	// Unwrap pgconn.PgError first — most specific handler
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		api.LogError("PgError [%s] constraint=%s table=%s detail=%s", pgErr.Code, pgErr.ConstraintName, pgErr.TableName, pgErr.Detail)
		switch pgErr.Code {
		case "23505": // unique_violation
			if strings.Contains(pgErr.ConstraintName, "penalty_code") {
				return "Penalty code already exists", http.StatusOK
			}
			return "Duplicate entry detected. This record already exists in the system.", http.StatusOK
		case "23503": // foreign_key_violation
			return "Referenced record not found or is invalid.", http.StatusOK
		case "23514": // check_violation
			return fmt.Sprintf("Invalid value: check constraint '%s' failed", pgErr.ConstraintName), http.StatusBadRequest
		case "22P02": // invalid_text_representation (enum cast failure)
			return "Invalid enum value provided for one of the fields", http.StatusBadRequest
		case "42804": // datatype_mismatch
			return fmt.Sprintf("Field type mismatch: %s", pgErr.Message), http.StatusBadRequest
		}
		// Fall through with the raw Postgres message for any other code
		return fmt.Sprintf("Database error [%s]: %s", pgErr.Code, pgErr.Message), http.StatusInternalServerError
	}

	errStr := strings.ToLower(err.Error())

	// Known audit/transaction errors
	if strings.Contains(errStr, constants.ErrTxBeginFailed) ||
		strings.Contains(errStr, constants.ErrCommitFailedCapitalized) ||
		strings.Contains(errStr, constants.ErrAuditInsertFailed) {
		return err.Error(), http.StatusOK
	}

	// Constraint violations (string-based fallback)
	if strings.Contains(errStr, "uniq_penalty_code_active") ||
		strings.Contains(errStr, constants.ErrDuplicateKeyC) && strings.Contains(errStr, "penalty_code") {
		return "Penalty code already exists", http.StatusOK
	}

	if strings.Contains(errStr, constants.ErrDuplicateKeyC) {
		return "Duplicate entry detected. This record already exists in the system.", http.StatusOK
	}

	if strings.Contains(errStr, "foreign key constraint") || strings.Contains(errStr, "violates foreign key") {
		return "Referenced record not found or is invalid.", http.StatusOK
	}

	if strings.Contains(errStr, "connection") || strings.Contains(errStr, "timeout") {
		return "Database connection issue", http.StatusServiceUnavailable
	}

	return "Internal server error: " + context, http.StatusInternalServerError
}

// --- Request Types ---
type PenaltyStructureInput struct {
	BankCode                    string   `json:"bank_code"`
	MinAmountRange              *float64 `json:"min_amount_range"`
	MaxAmountRange              *float64 `json:"max_amount_range"`
	MinTenorDays                int      `json:"min_tenor_days"`
	MaxTenorDays                int      `json:"max_tenor_days"`
	MinHeldDays                 *int     `json:"min_held_days"`
	MaxHeldDays                 *int     `json:"max_held_days"`
	PenaltyType                 string   `json:"penalty_type"`
	PenaltyValue                float64  `json:"penalty_value"`
	CalculationMethod           string   `json:"calculation_method"`
	NoInterestIfWithdrawnBefore *int     `json:"no_interest_if_withdrawn_before"`
	Description                 string   `json:"description"`
	EffectiveFrom               string   `json:"effective_from"` // date in YYYY-MM-DD
	EffectiveTo                 *string  `json:"effective_to"`   // nullable date
	IsActive                    *bool    `json:"is_active"`
}

type CreatePenaltyStructureSingleRequest struct {
	UserID string `json:"user_id"`
	PenaltyStructureInput
}

type CreatePenaltyStructureRequest struct {
	UserID string                  `json:"user_id"`
	Rows   []PenaltyStructureInput `json:"rows"`
}

type UpdatePenaltyStructureRequest struct {
	UserID    string                 `json:"user_id"`
	PenaltyID string                 `json:"penalty_id"`
	Fields    map[string]interface{} `json:"fields"`
	Reason    string                 `json:"reason"`
}

// validate basic required fields
func validatePenaltyFields(input PenaltyStructureInput) error {
	if strings.TrimSpace(input.BankCode) == "" {
		return errors.New("bank_code is required")
	}
	if input.MinTenorDays < 0 {
		return errors.New("min_tenor_days must be >= 0")
	}
	if input.MaxTenorDays < input.MinTenorDays {
		return errors.New("max_tenor_days must be >= min_tenor_days")
	}
	if input.MinHeldDays != nil && *input.MinHeldDays < 0 {
		return errors.New("min_held_days must be >= 0")
	}
	if input.MaxHeldDays != nil && input.MinHeldDays != nil && *input.MaxHeldDays < *input.MinHeldDays {
		return errors.New("max_held_days must be >= min_held_days")
	}
	if input.PenaltyType == "" {
		return errors.New("penalty_type is required")
	}
	if strings.ToUpper(input.PenaltyType) != "PERCENTAGE" && strings.ToUpper(input.PenaltyType) != "FLAT_AMOUNT" && strings.ToUpper(input.PenaltyType) != "RATE_REDUCTION" {
		return errors.New("penalty_type must be one of PERCENTAGE, FLAT_AMOUNT, RATE_REDUCTION")
	}
	if input.PenaltyValue <= 0 {
		return errors.New("penalty_value must be > 0")
	}
	if strings.TrimSpace(input.CalculationMethod) == "" {
		return errors.New("calculation_method is required")
	}
	if strings.TrimSpace(input.EffectiveFrom) == "" {
		return errors.New("effective_from is required")
	}
	return nil
}

// --- Single Create Handler ---
func CreatePenaltyStructureSingle(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreatePenaltyStructureSingleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		// Set defaults
		if req.IsActive == nil {
			defaultVal := true
			req.IsActive = &defaultVal
		}

		input := req.PenaltyStructureInput

		if err := validatePenaltyFields(input); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, err.Error())
			api.LogError("Validation failed for penalty structure: %v", err)
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
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		insertQuery := `
            INSERT INTO investment.fd_penalty_structure_master (
                bank_code, min_amount_range, max_amount_range, min_tenor_days,
                max_tenor_days, min_held_days, max_held_days, penalty_type,
                penalty_value, calculation_method, no_interest_if_withdrawn_before,
                description, effective_from, effective_to, is_active
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
            RETURNING penalty_id
        `

		var penaltyID string
		err = tx.QueryRow(ctx, insertQuery,
			input.BankCode,
			input.MinAmountRange,
			input.MaxAmountRange,
			input.MinTenorDays,
			input.MaxTenorDays,
			input.MinHeldDays,
			input.MaxHeldDays,
			strings.ToUpper(input.PenaltyType),
			input.PenaltyValue,
			input.CalculationMethod,
			input.NoInterestIfWithdrawnBefore,
			input.Description,
			input.EffectiveFrom,
			input.EffectiveTo,
			input.IsActive,
		).Scan(&penaltyID)

		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, "Insert failed")
			api.RespondWithError(w, status, msg)
			return
		}

		auditQuery := `
            INSERT INTO investment.fd_audit_penalty_structure (
                penalty_id, action_type, processing_status, requested_by, requested_at
            ) VALUES ($1,'CREATE','PENDING_APPROVAL',$2,now())`
		if _, err := tx.Exec(ctx, auditQuery, penaltyID, userEmail); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		response := map[string]any{
			constants.ValueSuccess: true,
			"penalty_id":           penaltyID,
			"bank_code":            input.BankCode,
			"effective_from":       input.EffectiveFrom,
			"requested":            userEmail,
			"is_active":            input.IsActive,
		}
		api.RespondWithPayload(w, true, "", response)
		api.LogInfo("Penalty structure created successfully: ID=%s, Bank=%s", penaltyID, input.BankCode)
	}
}

// --- Stubs for other handlers ---
func CreatePenaltyStructure(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreatePenaltyStructureRequest
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

		// Pre-validate ALL rows first
		var validRows []PenaltyStructureInput
		var errorsList []map[string]interface{}

		for i, row := range req.Rows {
			if row.IsActive == nil {
				defaultVal := true
				row.IsActive = &defaultVal
			}
			if err := validatePenaltyFields(row); err != nil {
				errorsList = append(errorsList, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   err.Error(),
					"bank_code":            row.BankCode,
				})
			} else {
				validRows = append(validRows, row)
			}
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

		// No explicit duplicate-code field in this schema; accept validated rows and rely on DB constraints
		finalValidInputs := validRows

		if len(finalValidInputs) == 0 {
			api.RespondWithPayload(w, false, "All rows are duplicates", errorsList)
			return
		}

		// Build batch insert (columns aligned to DDL)
		valueStrings := make([]string, len(finalValidInputs))
		valueArgs := make([]interface{}, 0, len(finalValidInputs)*15)

		for i, row := range finalValidInputs {
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				i*15+1, i*15+2, i*15+3, i*15+4, i*15+5, i*15+6, i*15+7, i*15+8, i*15+9, i*15+10, i*15+11, i*15+12, i*15+13, i*15+14, i*15+15)

			valueArgs = append(valueArgs,
				row.BankCode,
				row.MinAmountRange,
				row.MaxAmountRange,
				row.MinTenorDays,
				row.MaxTenorDays,
				row.MinHeldDays,
				row.MaxHeldDays,
				strings.ToUpper(row.PenaltyType),
				row.PenaltyValue,
				row.CalculationMethod,
				row.NoInterestIfWithdrawnBefore,
				row.Description,
				row.EffectiveFrom,
				row.EffectiveTo,
				row.IsActive,
			)
		}

		batchInsertQuery := fmt.Sprintf(`
            INSERT INTO investment.fd_penalty_structure_master (
                bank_code, min_amount_range, max_amount_range, min_tenor_days,
                max_tenor_days, min_held_days, max_held_days, penalty_type,
                penalty_value, calculation_method, no_interest_if_withdrawn_before,
                description, effective_from, effective_to, is_active
            ) VALUES %s
            RETURNING penalty_id, bank_code
        `, strings.Join(valueStrings, ","))

		rows, err := tx.Query(ctx, batchInsertQuery, valueArgs...)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Batch insert failed: %v", err)
			return
		}
		defer rows.Close()

		var insertedRecords []map[string]interface{}
		var penaltyIDs []string

		for rows.Next() {
			var id, code string
			if err := rows.Scan(&id, &code); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				api.LogError("Insert result scan failed: %v", err)
				return
			}
			penaltyIDs = append(penaltyIDs, id)
			insertedRecords = append(insertedRecords, map[string]interface{}{
				constants.ValueSuccess: true,
				"penalty_id":           id,
				"penalty_code":         code,
			})
		}

		// Batch audit insert
		if len(penaltyIDs) > 0 {
			auditValues := make([]string, len(penaltyIDs))
			auditArgs := make([]interface{}, 0, len(penaltyIDs)*2)
			for i, id := range penaltyIDs {
				auditValues[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}

			auditQuery := fmt.Sprintf(`
                INSERT INTO investment.fd_audit_penalty_structure 
                    (penalty_id, action_type, processing_status, requested_by, requested_at)
                VALUES %s
            `, strings.Join(auditValues, ","))

			if _, err := tx.Exec(ctx, auditQuery, auditArgs...); err != nil {
				msg, status := getUserFriendlyPenaltyError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				api.LogError("Batch audit insert failed: %v", err)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk create commit failed: %v", err)
			return
		}

		allResults := append(insertedRecords, errorsList...)
		success := len(insertedRecords) > 0
		api.RespondWithPayload(w, success, "", allResults)
		api.LogInfo("Bulk create: %d inserted, %d errors, %d total", len(insertedRecords), len(errorsList), len(req.Rows))
	}
}

func UpdatePenaltyStructure(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UpdatePenaltyStructureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if req.PenaltyID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "penalty_id is required")
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
			msg, status := getUserFriendlyPenaltyError(err, "Transaction start failed")
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Fetch existing row for old values
		// Cast date columns to text (YYYY-MM-DD) to avoid binary-date scan errors
		sel := `
		     SELECT bank_code, min_amount_range, max_amount_range, min_tenor_days,
			     max_tenor_days, min_held_days, max_held_days, penalty_type,
			     penalty_value, calculation_method, no_interest_if_withdrawn_before,
			     description, TO_CHAR(effective_from, 'YYYY-MM-DD') as effective_from,
			     TO_CHAR(effective_to, 'YYYY-MM-DD') as effective_to, is_active
		     FROM investment.fd_penalty_structure_master
		     WHERE penalty_id=$1
		     FOR UPDATE`

		var oldBankCode string
		var oldMinAmount *float64
		var oldMaxAmount *float64
		var oldMinTenor int
		var oldMaxTenor int
		var oldMinHeld *int
		var oldMaxHeld *int
		var oldPenaltyType string
		var oldPenaltyValue float64
		var oldCalcMethod string
		var oldNoInterest *int
		var oldDescription *string
		var oldEffectiveFrom string
		var oldEffectiveTo *string
		var oldIsActive *bool

		if err := tx.QueryRow(ctx, sel, req.PenaltyID).Scan(
			&oldBankCode, &oldMinAmount, &oldMaxAmount, &oldMinTenor,
			&oldMaxTenor, &oldMinHeld, &oldMaxHeld, &oldPenaltyType,
			&oldPenaltyValue, &oldCalcMethod, &oldNoInterest,
			&oldDescription, &oldEffectiveFrom, &oldEffectiveTo, &oldIsActive,
		); err != nil {
			// If no row found, return a 404 Not Found with a clear message.
			if errors.Is(err, pgx.ErrNoRows) {
				api.RespondWithError(w, http.StatusNotFound, "Penalty record not found")
				api.LogError("Penalty fetch: not found penalty_id=%s", req.PenaltyID)
				return
			}

			// Log the DB error with context so we can diagnose intermittent failures.
			api.LogError("Penalty fetch failed: penalty_id=%s err=%v", req.PenaltyID, err)
			msg, status := getUserFriendlyPenaltyError(err, "Fetch failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Build update sets
		fieldPairs := map[string]bool{
			"bank_code": true, "min_amount_range": true, "max_amount_range": true,
			"min_tenor_days": true, "max_tenor_days": true, "min_held_days": true,
			"max_held_days": true, "penalty_type": true, "penalty_value": true,
			"calculation_method": true, "no_interest_if_withdrawn_before": true,
			"description": true, "effective_from": true, "effective_to": true,
			"is_active": true,
		}

		// Enum columns: use $N::text so Postgres coerces the text value into the enum type.
		// This avoids "column is of type X but expression is of type text" when pgx sends
		// plain strings over the binary protocol.
		enumColumns := map[string]bool{
			"penalty_type":       true,
			"calculation_method": true,
		}

		var sets []string
		var args []interface{}
		pos := 1

		for k, v := range req.Fields {
			k = strings.ToLower(k)
			if _, ok := fieldPairs[k]; ok {
				if enumColumns[k] {
					// Cast via text to allow Postgres to coerce into the enum type
					sets = append(sets, fmt.Sprintf("%s=$%d::text", k, pos))
				} else {
					sets = append(sets, fmt.Sprintf(constants.FormatSQLColumnArgAlt, k, pos))
				}
				args = append(args, v)
				pos++
			}
		}

		if len(sets) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, "No valid updatable fields found")
			return
		}

		// Update query
		q := fmt.Sprintf("UPDATE investment.fd_penalty_structure_master SET %s WHERE penalty_id=$%d",
			strings.Join(sets, ", "), pos)
		args = append(args, req.PenaltyID)

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			// Log the failing UPDATE with query and sanitized args for diagnostics
			api.LogError("Penalty update failed: penalty_id=%s query=%s args=%v err=%v", req.PenaltyID, q, args, err)
			msg, status := getUserFriendlyPenaltyError(err, "Update failed")
			api.RespondWithError(w, status, msg)
			return
		}

		// Insert audit record with old values
		auditQuery := `
            INSERT INTO investment.fd_audit_penalty_structure (
                penalty_id, action_type, processing_status, reason, requested_by, requested_at,
                old_bank_code, old_min_amount_range, old_max_amount_range, old_min_tenor_days,
                old_max_tenor_days, old_min_held_days, old_max_held_days, old_penalty_type,
                old_penalty_value, old_calculation_method, old_no_interest_if_withdrawn_before,
                old_description, old_effective_from, old_effective_to, old_is_active
            ) VALUES ($1,'EDIT','PENDING_EDIT_APPROVAL',$2,$3,now(),$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        `

		if _, err := tx.Exec(ctx, auditQuery,
			req.PenaltyID, req.Reason, userEmail,
			oldBankCode, oldMinAmount, oldMaxAmount, oldMinTenor, oldMaxTenor,
			oldMinHeld, oldMaxHeld, oldPenaltyType, oldPenaltyValue, oldCalcMethod,
			oldNoInterest, oldDescription, oldEffectiveFrom, oldEffectiveTo, oldIsActive,
		); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrCommitFailedCapitalized)
			api.RespondWithError(w, status, msg)
			return
		}

		api.RespondWithPayload(w, true, "", map[string]any{constants.ValueSuccess: true, "penalty_id": req.PenaltyID})
		api.LogInfo("Penalty updated: ID=%s, requested=%s", req.PenaltyID, userEmail)
	}
}

func UpdatePenaltyStructureBulk(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID string `json:"user_id"`
			Rows   []struct {
				PenaltyID string                 `json:"penalty_id"`
				Fields    map[string]interface{} `json:"fields"`
				Reason    string                 `json:"reason"`
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

		// Pre-validate
		var validUpdates []struct {
			PenaltyID string
			Fields    map[string]interface{}
			Reason    string
		}
		var errorsList []map[string]interface{}
		allIDs := make([]string, 0, len(req.Rows))

		for i, row := range req.Rows {
			if row.PenaltyID == "" {
				errorsList = append(errorsList, map[string]interface{}{
					"row_index":            i,
					constants.ValueSuccess: false,
					constants.ValueError:   "Missing penalty_id",
				})
				continue
			}
			if len(row.Fields) == 0 {
				errorsList = append(errorsList, map[string]interface{}{
					"row_index":            i,
					"penalty_id":           row.PenaltyID,
					constants.ValueSuccess: false,
					constants.ValueError:   "No fields to update",
				})
				continue
			}

			validUpdates = append(validUpdates, struct {
				PenaltyID string
				Fields    map[string]interface{}
				Reason    string
			}{row.PenaltyID, row.Fields, row.Reason})
			allIDs = append(allIDs, row.PenaltyID)
		}

		if len(validUpdates) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errorsList)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			api.LogError("Bulk update transaction begin failed: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		// Batch select old values
		oldValuesQuery := `
            SELECT penalty_id, bank_code, min_amount_range, max_amount_range, min_tenor_days,
                   max_tenor_days, min_held_days, max_held_days, penalty_type,
                   penalty_value, calculation_method, no_interest_if_withdrawn_before,
                   description, effective_from, effective_to, is_active
            FROM investment.fd_penalty_structure_master
            WHERE penalty_id = ANY($1::text[])
        `

		rows, err := tx.Query(ctx, oldValuesQuery, allIDs)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, "Fetch old values failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Batch old values fetch failed: %v", err)
			return
		}
		defer rows.Close()

		oldValuesMap := make(map[string][16]interface{})
		for rows.Next() {
			var id string
			var oldVals [16]interface{}
			if err := rows.Scan(&id, &oldVals[0], &oldVals[1], &oldVals[2], &oldVals[3], &oldVals[4], &oldVals[5], &oldVals[6], &oldVals[7], &oldVals[8], &oldVals[9], &oldVals[10], &oldVals[11], &oldVals[12], &oldVals[13], &oldVals[14], &oldVals[15]); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Old values scan failed: "+err.Error())
				api.LogError("Old values scan failed: %v", err)
				return
			}
			oldValuesMap[id] = oldVals
		}

		// Allowed fields mapping
		fieldPairs := map[string]int{
			"bank_code":                       0,
			"min_amount_range":                1,
			"max_amount_range":                2,
			"min_tenor_days":                  3,
			"max_tenor_days":                  4,
			"min_held_days":                   5,
			"max_held_days":                   6,
			"penalty_type":                    7,
			"penalty_value":                   8,
			"calculation_method":              9,
			"no_interest_if_withdrawn_before": 10,
			"description":                     11,
			"effective_from":                  12,
			"effective_to":                    13,
			"is_active":                       14,
		}

		// Group updates per field
		fieldUpdates := make(map[string][]struct {
			ID    string
			Value interface{}
		})

		for _, update := range validUpdates {
			for field, value := range update.Fields {
				field = strings.ToLower(field)
				if _, ok := fieldPairs[field]; ok {
					fieldUpdates[field] = append(fieldUpdates[field], struct {
						ID    string
						Value interface{}
					}{update.PenaltyID, value})
				}
			}
		}

		// Build and execute batch update
		if len(fieldUpdates) > 0 {
			var setClauses []string
			var updateArgs []interface{}
			argIndex := 1

			for field, updatesF := range fieldUpdates {
				var caseWhen []string
				for _, u := range updatesF {
					caseWhen = append(caseWhen, fmt.Sprintf("WHEN penalty_id = $%d THEN $%d", argIndex, argIndex+1))
					updateArgs = append(updateArgs, u.ID, u.Value)
					argIndex += 2
				}

				caseClause := fmt.Sprintf("%s = CASE %s ELSE %s END", field, strings.Join(caseWhen, " "), field)
				setClauses = append(setClauses, caseClause)
			}

			updateQuery := fmt.Sprintf(`
                UPDATE investment.fd_penalty_structure_master
                SET %s
                WHERE penalty_id = ANY($%d::text[])
            `, strings.Join(setClauses, ", "), argIndex)
			updateArgs = append(updateArgs, allIDs)

			if _, err := tx.Exec(ctx, updateQuery, updateArgs...); err != nil {
				msg, status := getUserFriendlyPenaltyError(err, "Batch update failed")
				api.RespondWithError(w, status, msg)
				api.LogError("Batch update failed: %v", err)
				return
			}
		}

		// Insert audits per update using old values
		var successResults []map[string]interface{}
		for _, update := range validUpdates {
			oldVals, exists := oldValuesMap[update.PenaltyID]
			if !exists {
				errorsList = append(errorsList, map[string]interface{}{
					"penalty_id":           update.PenaltyID,
					constants.ValueSuccess: false,
					constants.ValueError:   "Record not found",
				})
				continue
			}

			// Build audit insert dynamically
			auditCols := []string{"penalty_id", "action_type", "processing_status", "reason", "requested_by", "requested_at"}
			auditVals := []interface{}{update.PenaltyID, "EDIT", "PENDING_EDIT_APPROVAL", update.Reason, userEmail}
			auditParams := []string{"$1", "$2", "$3", "$4", "$5", "now()"}
			paramPos := 6

			for field := range update.Fields {
				field = strings.ToLower(field)
				if idx, ok := fieldPairs[field]; ok {
					oldField := "old_" + field
					auditCols = append(auditCols, oldField)
					auditVals = append(auditVals, oldVals[idx])
					auditParams = append(auditParams, fmt.Sprintf("$%d", paramPos))
					paramPos++
				}
			}

			auditQuery := fmt.Sprintf("INSERT INTO investment.fd_audit_penalty_structure (%s) VALUES (%s)", strings.Join(auditCols, ", "), strings.Join(auditParams, ", "))

			if _, err := tx.Exec(ctx, auditQuery, auditVals...); err != nil {
				errorsList = append(errorsList, map[string]interface{}{
					"penalty_id":           update.PenaltyID,
					constants.ValueSuccess: false,
					constants.ValueError:   "Audit failed: " + err.Error(),
				})
				continue
			}

			successResults = append(successResults, map[string]interface{}{
				constants.ValueSuccess: true,
				"penalty_id":           update.PenaltyID,
			})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk update commit failed: %v", err)
			return
		}

		allResults := append(successResults, errorsList...)
		success := len(successResults) > 0
		api.RespondWithPayload(w, success, "", allResults)
		api.LogInfo("Bulk update completed: %d updated, %d errors, %d total", len(successResults), len(errorsList), len(req.Rows))
	}
}

func DeletePenaltyStructure(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID     string   `json:"user_id"`
			PenaltyIDs []string `json:"penalty_ids"`
			Reason     string   `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if len(req.PenaltyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoPenaltyIDsProvided)
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
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Verify which penalty_ids exist
		existingRows, err := tx.Query(ctx, `SELECT penalty_id FROM investment.fd_penalty_structure_master WHERE penalty_id = ANY($1::text[])`, req.PenaltyIDs)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, "Fetch existing penalty ids failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Delete: fetch existing penalty ids failed: %v", err)
			return
		}
		defer existingRows.Close()

		found := make(map[string]bool)
		for existingRows.Next() {
			var id string
			if err := existingRows.Scan(&id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Existing id scan failed: "+err.Error())
				api.LogError("Delete: existing id scan failed: %v", err)
				return
			}
			found[id] = true
		}

		var auditValues []string
		var auditArgs []interface{}
		var errorsList []map[string]interface{}
		var success []map[string]interface{}

		for i, pid := range req.PenaltyIDs {
			if !found[pid] {
				errorsList = append(errorsList, map[string]interface{}{"penalty_id": pid, constants.ValueSuccess: false, constants.ValueError: "Record not found"})
				continue
			}
			// placeholders: penalty_id, reason, requested_by
			auditValues = append(auditValues, fmt.Sprintf("($%d,'DELETE','PENDING_DELETE_APPROVAL',$%d,$%d,now())", i*3+1, i*3+2, i*3+3))
			auditArgs = append(auditArgs, pid, req.Reason, userEmail)
			success = append(success, map[string]interface{}{constants.ValueSuccess: true, "penalty_id": pid})
		}

		if len(auditValues) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errorsList)
			return
		}

		auditQuery := fmt.Sprintf(`
            INSERT INTO investment.fd_audit_penalty_structure
                (penalty_id, action_type, processing_status, reason, requested_by, requested_at)
            VALUES %s
        `, strings.Join(auditValues, ","))

		if _, err := tx.Exec(ctx, auditQuery, auditArgs...); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrAuditInsertFailed)
			api.RespondWithError(w, status, msg)
			api.LogError("Delete: audit insert failed: %v", err)
			return
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Delete: commit failed: %v", err)
			return
		}

		allResults := append(success, errorsList...)
		api.RespondWithPayload(w, len(success) > 0, "", allResults)
		api.LogInfo("Delete request created for %d penalty_ids, %d errors", len(success), len(errorsList))
	}
}

func BulkApprovePenaltyStructure(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string   `json:"user_id"`
			PenaltyIDs     []string `json:"penalty_ids"`
			CheckerComment string   `json:"checker_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}
		if len(req.PenaltyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoPenaltyIDsProvided)
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
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Lock pending audit rows for update by penalty_id
		sel := `
            SELECT audit_id, penalty_id, action_type, processing_status
            FROM investment.fd_audit_penalty_structure
            WHERE penalty_id = ANY($1::text[]) AND processing_status LIKE 'PENDING%'
            FOR UPDATE
        `

		rows, err := tx.Query(ctx, sel, req.PenaltyIDs)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, "Fetch audit rows failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk approve: fetch audits failed: %v", err)
			return
		}
		defer rows.Close()

		type auditRow struct {
			AuditID          string
			PenaltyID        string
			ActionType       string
			ProcessingStatus string
		}
		var audits []auditRow
		for rows.Next() {
			var a auditRow
			if err := rows.Scan(&a.AuditID, &a.PenaltyID, &a.ActionType, &a.ProcessingStatus); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Audit scan failed: "+err.Error())
				api.LogError("Bulk approve: audit scan failed: %v", err)
				return
			}
			audits = append(audits, a)
		}

		if len(audits) == 0 {
			api.RespondWithPayload(w, false, "No matching pending audit records found for provided penalty_ids", nil)
			return
		}

		var success []map[string]interface{}
		var errorsList []map[string]interface{}

		for _, a := range audits {
			// If this is a DELETE request, mark master row as deleted
			if strings.ToUpper(a.ActionType) == "DELETE" {
				if _, err := tx.Exec(ctx, `UPDATE investment.fd_penalty_structure_master SET is_deleted = true WHERE penalty_id = $1`, a.PenaltyID); err != nil {
					errorsList = append(errorsList, map[string]interface{}{"penalty_id": a.PenaltyID, "audit_id": a.AuditID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
			}

			// Mark audit as approved
			if _, err := tx.Exec(ctx, `UPDATE investment.fd_audit_penalty_structure SET processing_status='APPROVED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE audit_id=$3`, userEmail, req.CheckerComment, a.AuditID); err != nil {
				errorsList = append(errorsList, map[string]interface{}{"penalty_id": a.PenaltyID, "audit_id": a.AuditID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}

			success = append(success, map[string]interface{}{constants.ValueSuccess: true, "penalty_id": a.PenaltyID, "audit_id": a.AuditID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk approve commit failed: %v", err)
			return
		}

		allResults := append(success, errorsList...)
		api.RespondWithPayload(w, len(success) > 0, "", allResults)
		api.LogInfo("Bulk approve completed: %d approved, %d errors", len(success), len(errorsList))
	}
}

func BulkRejectPenaltyStructure(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID         string   `json:"user_id"`
			PenaltyIDs     []string `json:"penalty_ids"`
			CheckerComment string   `json:"checker_comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if len(req.PenaltyIDs) == 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrNoPenaltyIDsProvided)
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
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrTransactionFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer tx.Rollback(ctx)

		// Select pending audit rows and old values needed to revert EDITs by penalty_id
		sel := `
            SELECT audit_id, penalty_id, action_type,
                   old_bank_code, old_min_amount_range, old_max_amount_range,
                   old_min_tenor_days, old_max_tenor_days, old_min_held_days, old_max_held_days,
                   old_penalty_type, old_penalty_value, old_calculation_method,
                   old_no_interest_if_withdrawn_before, old_description, old_effective_from, old_effective_to, old_is_active
            FROM investment.fd_audit_penalty_structure
            WHERE penalty_id = ANY($1::text[]) AND processing_status LIKE 'PENDING%'
            FOR UPDATE
        `

		rows, err := tx.Query(ctx, sel, req.PenaltyIDs)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, "Fetch audit rows failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk reject: fetch audits failed: %v", err)
			return
		}
		defer rows.Close()

		type auditOld struct {
			AuditID          string
			PenaltyID        string
			ActionType       string
			OldBankCode      *string
			OldMinAmount     *float64
			OldMaxAmount     *float64
			OldMinTenor      *int
			OldMaxTenor      *int
			OldMinHeld       *int
			OldMaxHeld       *int
			OldPenaltyType   *string
			OldPenaltyValue  *float64
			OldCalcMethod    *string
			OldNoInterest    *int
			OldDescription   *string
			OldEffectiveFrom *string
			OldEffectiveTo   *string
			OldIsActive      *bool
		}

		var audits []auditOld
		for rows.Next() {
			var a auditOld
			if err := rows.Scan(&a.AuditID, &a.PenaltyID, &a.ActionType,
				&a.OldBankCode, &a.OldMinAmount, &a.OldMaxAmount,
				&a.OldMinTenor, &a.OldMaxTenor, &a.OldMinHeld, &a.OldMaxHeld,
				&a.OldPenaltyType, &a.OldPenaltyValue, &a.OldCalcMethod,
				&a.OldNoInterest, &a.OldDescription, &a.OldEffectiveFrom, &a.OldEffectiveTo, &a.OldIsActive); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Audit scan failed: "+err.Error())
				api.LogError("Bulk reject: audit scan failed: %v", err)
				return
			}
			audits = append(audits, a)
		}

		if len(audits) == 0 {
			api.RespondWithPayload(w, false, "No matching pending audit records found for provided penalty_ids", nil)
			return
		}

		var success []map[string]interface{}
		var errorsList []map[string]interface{}

		for _, a := range audits {
			act := strings.ToUpper(a.ActionType)
			switch act {
			case "EDIT":
				// Revert master to old values
				q := `UPDATE investment.fd_penalty_structure_master SET
                    bank_code=$1, min_amount_range=$2, max_amount_range=$3,
                    min_tenor_days=$4, max_tenor_days=$5, min_held_days=$6, max_held_days=$7,
                    penalty_type=$8, penalty_value=$9, calculation_method=$10,
                    no_interest_if_withdrawn_before=$11, description=$12,
                    effective_from=$13, effective_to=$14, is_active=$15
                    WHERE penalty_id=$16`
				if _, err := tx.Exec(ctx, q,
					a.OldBankCode, a.OldMinAmount, a.OldMaxAmount,
					a.OldMinTenor, a.OldMaxTenor, a.OldMinHeld, a.OldMaxHeld,
					a.OldPenaltyType, a.OldPenaltyValue, a.OldCalcMethod,
					a.OldNoInterest, a.OldDescription,
					a.OldEffectiveFrom, a.OldEffectiveTo, a.OldIsActive,
					a.PenaltyID); err != nil {
					errorsList = append(errorsList, map[string]interface{}{"penalty_id": a.PenaltyID, "audit_id": a.AuditID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
			case "CREATE":
				// Remove the created master row
				if _, err := tx.Exec(ctx, `DELETE FROM investment.fd_penalty_structure_master WHERE penalty_id=$1`, a.PenaltyID); err != nil {
					errorsList = append(errorsList, map[string]interface{}{"penalty_id": a.PenaltyID, "audit_id": a.AuditID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
					continue
				}
			case "DELETE":
				// Nothing to revert if delete was only pending; if delete was applied, we currently don't handle rollback here
				// Keep as no-op (audit will be marked REJECTED)
			default:
				// Unknown action: record error and continue
				errorsList = append(errorsList, map[string]interface{}{"audit_id": a.AuditID, constants.ValueSuccess: false, constants.ValueError: "unknown action_type"})
				continue
			}

			// Mark audit as rejected
			if _, err := tx.Exec(ctx, `UPDATE investment.fd_audit_penalty_structure SET processing_status='REJECTED', checker_by=$1, checker_at=now(), checker_comment=$2 WHERE audit_id=$3`, userEmail, req.CheckerComment, a.AuditID); err != nil {
				errorsList = append(errorsList, map[string]interface{}{"audit_id": a.AuditID, constants.ValueSuccess: false, constants.ValueError: err.Error()})
				continue
			}

			success = append(success, map[string]interface{}{constants.ValueSuccess: true, "audit_id": a.AuditID, "penalty_id": a.PenaltyID})
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Bulk reject commit failed: %v", err)
			return
		}

		allResults := append(success, errorsList...)
		api.RespondWithPayload(w, len(success) > 0, "", allResults)
		api.LogInfo("Bulk reject completed: %d rejected, %d errors", len(success), len(errorsList))
	}
}

func GetPenaltyStructuresApprovedActive(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		bankCode := strings.TrimSpace(q.Get("bank_code"))
		limitStr := q.Get("limit")
		offsetStr := q.Get("offset")

		limit := 100
		offset := 0
		if limitStr != "" {
			if v, err := strconv.Atoi(limitStr); err == nil && v > 0 {
				limit = v
			}
		}
		if offsetStr != "" {
			if v, err := strconv.Atoi(offsetStr); err == nil && v >= 0 {
				offset = v
			}
		}

		ctx := r.Context()
		baseQuery := `
            SELECT m.penalty_id, m.bank_code,
                   COALESCE(mb.bank_name,'') AS bank_name,
                   COALESCE(mb.bank_short_name,'') AS bank_short_name,
                   m.min_amount_range, m.max_amount_range, m.min_tenor_days,
                   m.max_tenor_days, m.min_held_days, m.max_held_days, m.penalty_type,
                   m.penalty_value, m.calculation_method, m.no_interest_if_withdrawn_before,
                   m.description, m.effective_from, m.effective_to, m.is_active
            FROM investment.fd_penalty_structure_master m
            LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code
            WHERE m.is_active = true AND (m.is_deleted IS NULL OR m.is_deleted = false)
              AND m.effective_from <= now()::date AND (m.effective_to IS NULL OR m.effective_to >= now()::date)
        `

		var args []interface{}
		if bankCode != "" {
			baseQuery += " AND bank_code = $1"
			args = append(args, bankCode)
		}

		// Add ordering + pagination
		baseQuery += fmt.Sprintf(" ORDER BY effective_from DESC LIMIT %d OFFSET %d", limit, offset)

		rows, err := pgxPool.Query(ctx, baseQuery, args...)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, "Fetch approved active failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Get approved active failed: %v", err)
			return
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			var id string
			var bank, bankName, bankShortName string
			var minAmt *float64
			var maxAmt *float64
			var minTenor int
			var maxTenor int
			var minHeld *int
			var maxHeld *int
			var pType string
			var pValue float64
			var calcMethod string
			var noInterest *int
			var desc *string
			var effFrom string
			var effTo *string
			var isActive *bool

			if err := rows.Scan(&id, &bank, &bankName, &bankShortName, &minAmt, &maxAmt, &minTenor, &maxTenor, &minHeld, &maxHeld, &pType, &pValue, &calcMethod, &noInterest, &desc, &effFrom, &effTo, &isActive); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Row scan failed: "+err.Error())
				api.LogError("Get approved active scan failed: %v", err)
				return
			}

			results = append(results, map[string]interface{}{
				"penalty_id":                      id,
				"bank_code":                       bank,
				"bank_name":                       bankName,
				"bank_short_name":                 bankShortName,
				"min_amount_range":                minAmt,
				"max_amount_range":                maxAmt,
				"min_tenor_days":                  minTenor,
				"max_tenor_days":                  maxTenor,
				"min_held_days":                   minHeld,
				"max_held_days":                   maxHeld,
				"penalty_type":                    pType,
				"penalty_value":                   pValue,
				"calculation_method":              calcMethod,
				"no_interest_if_withdrawn_before": noInterest,
				"description":                     desc,
				"effective_from":                  effFrom,
				"effective_to":                    effTo,
				"is_active":                       isActive,
			})
		}

		api.RespondWithPayload(w, true, "", results)
	}
}

func GetPenaltyStructuresWithAudit(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		q := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.penalty_id)
					a.penalty_id,
					a.processing_status,
					a.action_type,
					a.audit_id,
					a.requested_by,
					a.requested_at,
					a.checker_by,
					a.checker_at,
					a.checker_comment,
					a.reason,
					a.old_bank_code,
					a.old_min_amount_range,
					a.old_max_amount_range,
					a.old_min_tenor_days,
					a.old_max_tenor_days,
					a.old_min_held_days,
					a.old_max_held_days,
					a.old_penalty_type,
					a.old_penalty_value,
					a.old_calculation_method,
					a.old_no_interest_if_withdrawn_before,
					a.old_description,
					a.old_effective_from,
					a.old_effective_to,
					a.old_is_active
				FROM investment.fd_audit_penalty_structure a
				ORDER BY a.penalty_id, GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
			),
			history AS (
				SELECT
					penalty_id,
					MAX(CASE WHEN action_type='CREATE' THEN requested_by END) AS created_by,
					MAX(CASE WHEN action_type='CREATE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS created_at,
					MAX(CASE WHEN action_type='EDIT' THEN requested_by END) AS edited_by,
					MAX(CASE WHEN action_type='EDIT' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS edited_at,
					MAX(CASE WHEN action_type='DELETE' THEN requested_by END) AS deleted_by,
					MAX(CASE WHEN action_type='DELETE' THEN TO_CHAR(requested_at,'YYYY-MM-DD HH24:MI:SS') END) AS deleted_at
				FROM investment.fd_audit_penalty_structure
				GROUP BY penalty_id
			)
			SELECT
				m.penalty_id,
				COALESCE(m.bank_code,'') AS bank_code,
				COALESCE(l.old_bank_code,'') AS old_bank_code,
				COALESCE(mb.bank_name,'') AS bank_name,
				COALESCE(mb.bank_short_name,'') AS bank_short_name,
				m.min_amount_range,
				l.old_min_amount_range,
				m.max_amount_range,
				l.old_max_amount_range,
				COALESCE(m.min_tenor_days,0) AS min_tenor_days,
				COALESCE(l.old_min_tenor_days,0) AS old_min_tenor_days,
				COALESCE(m.max_tenor_days,0) AS max_tenor_days,
				COALESCE(l.old_max_tenor_days,0) AS old_max_tenor_days,
				m.min_held_days,
				l.old_min_held_days,
				m.max_held_days,
				l.old_max_held_days,
				COALESCE(m.penalty_type,'') AS penalty_type,
				COALESCE(l.old_penalty_type,'') AS old_penalty_type,
				COALESCE(m.penalty_value,0) AS penalty_value,
				COALESCE(l.old_penalty_value,0) AS old_penalty_value,
				COALESCE(m.calculation_method,'') AS calculation_method,
				COALESCE(l.old_calculation_method,'') AS old_calculation_method,
				m.no_interest_if_withdrawn_before,
				l.old_no_interest_if_withdrawn_before,
				COALESCE(m.description,'') AS description,
				COALESCE(l.old_description,'') AS old_description,
				TO_CHAR(m.effective_from,'YYYY-MM-DD') AS effective_from,
				TO_CHAR(l.old_effective_from,'YYYY-MM-DD') AS old_effective_from,
				TO_CHAR(m.effective_to,'YYYY-MM-DD') AS effective_to,
				TO_CHAR(l.old_effective_to,'YYYY-MM-DD') AS old_effective_to,
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

			FROM investment.fd_penalty_structure_master m
			LEFT JOIN latest_audit l ON l.penalty_id = m.penalty_id
			LEFT JOIN history h ON h.penalty_id = m.penalty_id
			LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code
			WHERE COALESCE(m.is_deleted,false) = false
			ORDER BY GREATEST(COALESCE(l.requested_at, '1970-01-01'::timestamp), COALESCE(l.checker_at, '1970-01-01'::timestamp)) DESC
		`

		rows, err := pgxPool.Query(ctx, q)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrQueryFailed)
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
			api.LogError("Row iteration error in GetPenaltyStructuresWithAudit: %v", rows.Err())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"rows":                 out,
		})
		api.LogInfo("Retrieved %d penalty structures with audit data", len(out))
	}
}

func GetPenaltyStructureAuditHistory(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		penaltyID := r.URL.Query().Get("penalty_id")

		var q string
		var args []interface{}

		if penaltyID != "" {
			q = `
				SELECT
					a.audit_id,
					a.penalty_id,
					a.action_type,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment,

					-- Current values from master
					COALESCE(m.bank_code,'') AS bank_code,
					m.min_amount_range,
					m.max_amount_range,
					COALESCE(m.min_tenor_days,0) AS min_tenor_days,
					COALESCE(m.max_tenor_days,0) AS max_tenor_days,
					m.min_held_days,
					m.max_held_days,
					COALESCE(m.penalty_type,'') AS penalty_type,
					COALESCE(m.penalty_value,0) AS penalty_value,
					COALESCE(m.calculation_method,'') AS calculation_method,
					m.no_interest_if_withdrawn_before,
					COALESCE(m.description,'') AS description,
					TO_CHAR(m.effective_from,'YYYY-MM-DD') AS effective_from,
					TO_CHAR(m.effective_to,'YYYY-MM-DD') AS effective_to,
					COALESCE(m.is_active,false) AS is_active,
					COALESCE(m.is_deleted,false) AS is_deleted,

					-- Old values from audit
					COALESCE(a.old_bank_code,'') AS old_bank_code,
					a.old_min_amount_range,
					a.old_max_amount_range,
					COALESCE(a.old_min_tenor_days,0) AS old_min_tenor_days,
					COALESCE(a.old_max_tenor_days,0) AS old_max_tenor_days,
					a.old_min_held_days,
					a.old_max_held_days,
					COALESCE(a.old_penalty_type,'') AS old_penalty_type,
					COALESCE(a.old_penalty_value,0) AS old_penalty_value,
					COALESCE(a.old_calculation_method,'') AS old_calculation_method,
					a.old_no_interest_if_withdrawn_before,
					COALESCE(a.old_description,'') AS old_description,
					TO_CHAR(a.old_effective_from,'YYYY-MM-DD') AS old_effective_from,
					TO_CHAR(a.old_effective_to,'YYYY-MM-DD') AS old_effective_to,
					COALESCE(a.old_is_active,false) AS old_is_active

				FROM investment.fd_audit_penalty_structure a
				LEFT JOIN investment.fd_penalty_structure_master m ON m.penalty_id = a.penalty_id
				WHERE a.penalty_id = $1
				ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
			`
			args = append(args, penaltyID)
		} else {
			q = `
				SELECT
					a.audit_id,
					a.penalty_id,
					a.action_type,
					a.processing_status,
					COALESCE(a.reason,'') AS reason,
					COALESCE(a.requested_by,'') AS requested_by,
					TO_CHAR(a.requested_at,'YYYY-MM-DD HH24:MI:SS') AS requested_at,
					COALESCE(a.checker_by,'') AS checker_by,
					TO_CHAR(a.checker_at,'YYYY-MM-DD HH24:MI:SS') AS checker_at,
					COALESCE(a.checker_comment,'') AS checker_comment,

					-- Current values from master
					COALESCE(m.bank_code,'') AS bank_code,
					COALESCE(mb.bank_name,'') AS bank_name,
					COALESCE(mb.bank_short_name,'') AS bank_short_name,
					m.min_amount_range,
					m.max_amount_range,
					COALESCE(m.min_tenor_days,0) AS min_tenor_days,
					COALESCE(m.max_tenor_days,0) AS max_tenor_days,
					m.min_held_days,
					m.max_held_days,
					COALESCE(m.penalty_type,'') AS penalty_type,
					COALESCE(m.penalty_value,0) AS penalty_value,
					COALESCE(m.calculation_method,'') AS calculation_method,
					m.no_interest_if_withdrawn_before,
					COALESCE(m.description,'') AS description,
					TO_CHAR(m.effective_from,'YYYY-MM-DD') AS effective_from,
					TO_CHAR(m.effective_to,'YYYY-MM-DD') AS effective_to,
					COALESCE(m.is_active,false) AS is_active,
					COALESCE(m.is_deleted,false) AS is_deleted,

					-- Old values from audit
					COALESCE(a.old_bank_code,'') AS old_bank_code,
					a.old_min_amount_range,
					a.old_max_amount_range,
					COALESCE(a.old_min_tenor_days,0) AS old_min_tenor_days,
					COALESCE(a.old_max_tenor_days,0) AS old_max_tenor_days,
					a.old_min_held_days,
					a.old_max_held_days,
					COALESCE(a.old_penalty_type,'') AS old_penalty_type,
					COALESCE(a.old_penalty_value,0) AS old_penalty_value,
					COALESCE(a.old_calculation_method,'') AS old_calculation_method,
					a.old_no_interest_if_withdrawn_before,
					COALESCE(a.old_description,'') AS old_description,
					TO_CHAR(a.old_effective_from,'YYYY-MM-DD') AS old_effective_from,
					TO_CHAR(a.old_effective_to,'YYYY-MM-DD') AS old_effective_to,
					COALESCE(a.old_is_active,false) AS old_is_active

				FROM investment.fd_audit_penalty_structure a
				LEFT JOIN investment.fd_penalty_structure_master m ON m.penalty_id = a.penalty_id
				LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code
				ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
				LIMIT 1000
			`
		}

		rows, err := pgxPool.Query(ctx, q, args...)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrQueryFailed)
			api.RespondWithError(w, status, msg)
			return
		}
		defer rows.Close()

		out := make([]map[string]interface{}, 0)

		for rows.Next() {
			// Audit fields
			var auditID, penID, actionType, processStatus, reason, reqBy, reqAt, checkerBy, checkerAt, checkerComment string

			// Current values
			var curBank, curBankName, curBankShortName, curPenaltyType, curCalcMethod, curDesc string
			var curMinAmt, curMaxAmt *float64
			var curPenaltyValue float64
			var curMinTenor, curMaxTenor int
			var curMinHeld, curMaxHeld, curNoInterest *int
			var curEffFrom, curEffTo string
			var curIsActive, curIsDeleted bool

			// Old values
			var oldBank, oldPenaltyType, oldCalcMethod, oldDesc string
			var oldMinAmt, oldMaxAmt *float64
			var oldPenaltyValue float64
			var oldMinTenor, oldMaxTenor int
			var oldMinHeld, oldMaxHeld, oldNoInterest *int
			var oldEffFrom, oldEffTo string
			var oldIsActive bool

			if err := rows.Scan(
				// Audit
				&auditID, &penID, &actionType, &processStatus, &reason, &reqBy, &reqAt, &checkerBy, &checkerAt, &checkerComment,
				// Current values (order must match SELECT)
				&curBank, &curBankName, &curBankShortName, &curMinAmt, &curMaxAmt, &curMinTenor, &curMaxTenor, &curMinHeld, &curMaxHeld,
				&curPenaltyType, &curPenaltyValue, &curCalcMethod, &curNoInterest, &curDesc, &curEffFrom, &curEffTo, &curIsActive, &curIsDeleted,
				// Old values
				&oldBank, &oldMinAmt, &oldMaxAmt, &oldMinTenor, &oldMaxTenor, &oldMinHeld, &oldMaxHeld,
				&oldPenaltyType, &oldPenaltyValue, &oldCalcMethod, &oldNoInterest, &oldDesc, &oldEffFrom, &oldEffTo, &oldIsActive,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan error: "+err.Error())
				api.LogError("Database scan error in GetPenaltyStructureAuditHistory: %v", err)
				return
			}

			out = append(out, map[string]interface{}{
				// Audit
				"audit_id":          auditID,
				"penalty_id":        penID,
				"action_type":       actionType,
				"processing_status": processStatus,
				"reason":            reason,
				"requested_by":      reqBy,
				"requested_at":      reqAt,
				"checker_by":        checkerBy,
				"checker_at":        checkerAt,
				"checker_comment":   checkerComment,

				// Current/New values
				"bank_code":                       curBank,
				"bank_name":                       curBankName,
				"bank_short_name":                 curBankShortName,
				"min_amount_range":                curMinAmt,
				"max_amount_range":                curMaxAmt,
				"min_tenor_days":                  curMinTenor,
				"max_tenor_days":                  curMaxTenor,
				"min_held_days":                   curMinHeld,
				"max_held_days":                   curMaxHeld,
				"penalty_type":                    curPenaltyType,
				"penalty_value":                   curPenaltyValue,
				"calculation_method":              curCalcMethod,
				"no_interest_if_withdrawn_before": curNoInterest,
				"description":                     curDesc,
				"effective_from":                  curEffFrom,
				"effective_to":                    curEffTo,
				"is_active":                       curIsActive,
				"is_deleted":                      curIsDeleted,

				// Old values
				"old_bank_code":                       oldBank,
				"old_min_amount_range":                oldMinAmt,
				"old_max_amount_range":                oldMaxAmt,
				"old_min_tenor_days":                  oldMinTenor,
				"old_max_tenor_days":                  oldMaxTenor,
				"old_min_held_days":                   oldMinHeld,
				"old_max_held_days":                   oldMaxHeld,
				"old_penalty_type":                    oldPenaltyType,
				"old_penalty_value":                   oldPenaltyValue,
				"old_calculation_method":              oldCalcMethod,
				"old_no_interest_if_withdrawn_before": oldNoInterest,
				"old_description":                     oldDesc,
				"old_effective_from":                  oldEffFrom,
				"old_effective_to":                    oldEffTo,
				"old_is_active":                       oldIsActive,
			})
		}

		if rows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Row iteration error: "+rows.Err().Error())
			api.LogError("Row iteration error in GetPenaltyStructureAuditHistory: %v", rows.Err())
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]any{
			constants.ValueSuccess: true,
			"audit_logs":           out,
		})
		api.LogInfo("Retrieved %d penalty audit history records", len(out))
	}
}

func UploadPenaltyStructureSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Expect multipart form with 'file' and form value 'user_id'
		userID := r.FormValue("user_id")
		if userID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "user_id is required")
			return
		}

		file, handler, err := r.FormFile("file")
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "file is required")
			return
		}
		defer file.Close()

		ext := strings.ToLower(filepath.Ext(handler.Filename))
		if ext != ".csv" && ext != ".xlsx" {
			api.RespondWithError(w, http.StatusBadRequest, "unsupported file type; only .csv and .xlsx allowed")
			return
		}

		// Resolve user session
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

		var rowsData [][]string
		if ext == ".csv" {
			rowsData, err = parseCSVFile(file)
		} else {
			rowsData, err = parseXLSXFile(file)
		}
		if err != nil {
			api.RespondWithError(w, http.StatusBadRequest, "failed to parse file: "+err.Error())
			return
		}

		if len(rowsData) < 2 {
			api.RespondWithError(w, http.StatusBadRequest, "file must contain a header and at least one data row")
			return
		}

		header := rowsData[0]
		data := rowsData[1:]

		requiredCols := []string{"bank_code", "min_tenor_days", "max_tenor_days", "penalty_type", "penalty_value", "calculation_method", "effective_from"}
		colMap := make(map[string]int)
		for i, c := range header {
			colMap[strings.ToLower(strings.TrimSpace(c))] = i
		}
		for _, rc := range requiredCols {
			if _, ok := colMap[rc]; !ok {
				api.RespondWithError(w, http.StatusBadRequest, "missing required column: "+rc)
				return
			}
		}

		ctx := r.Context()
		var validInputs []PenaltyStructureInput
		var errorsList []map[string]interface{}

		for rowIdx, row := range data {
			// Helper to get value safely
			get := func(col string) string { return getColumnValue(row, colMap, col) }

			bankCode := strings.TrimSpace(get("bank_code"))
			minTenorStr := strings.TrimSpace(get("min_tenor_days"))
			maxTenorStr := strings.TrimSpace(get("max_tenor_days"))
			penaltyType := strings.TrimSpace(get("penalty_type"))
			penaltyValueStr := strings.TrimSpace(get("penalty_value"))
			calcMethod := strings.TrimSpace(get("calculation_method"))
			effFrom := strings.TrimSpace(get("effective_from"))

			// Optional numeric fields
			minHeldStr := strings.TrimSpace(get("min_held_days"))
			maxHeldStr := strings.TrimSpace(get("max_held_days"))
			minAmtStr := strings.TrimSpace(get("min_amount_range"))
			maxAmtStr := strings.TrimSpace(get("max_amount_range"))
			noInterestStr := strings.TrimSpace(get("no_interest_if_withdrawn_before"))
			desc := strings.TrimSpace(get("description"))
			effToStr := strings.TrimSpace(get("effective_to"))
			isActiveStr := strings.TrimSpace(get("is_active"))

			// Parse required numeric fields
			minTenor, errMin := strconv.Atoi(minTenorStr)
			maxTenor, errMax := strconv.Atoi(maxTenorStr)
			if errMin != nil || errMax != nil {
				errorsList = append(errorsList, map[string]interface{}{"row_index": rowIdx, constants.ValueSuccess: false, constants.ValueError: "invalid tenor days"})
				continue
			}

			// penalty value
			pvPtr, err := parseFloatPtr(penaltyValueStr)
			if err != nil || pvPtr == nil {
				errorsList = append(errorsList, map[string]interface{}{"row_index": rowIdx, constants.ValueSuccess: false, constants.ValueError: "invalid penalty_value"})
				continue
			}

			// parse optional fields
			minHeldPtr, _ := parseIntPtr(minHeldStr)
			maxHeldPtr, _ := parseIntPtr(maxHeldStr)
			minAmtPtr, _ := parseFloatPtr(minAmtStr)
			maxAmtPtr, _ := parseFloatPtr(maxAmtStr)
			noInterestPtr, _ := parseIntPtr(noInterestStr)
			var effToPtr *string = nil
			if effToStr != "" {
				effToPtr = &effToStr
			}
			var isActivePtr *bool = nil
			if isActiveStr != "" {
				bptr, err := parseBoolPtr(isActiveStr)
				if err == nil {
					isActivePtr = bptr
				}
			}

			input := PenaltyStructureInput{
				BankCode:                    bankCode,
				MinAmountRange:              minAmtPtr,
				MaxAmountRange:              maxAmtPtr,
				MinTenorDays:                minTenor,
				MaxTenorDays:                maxTenor,
				MinHeldDays:                 minHeldPtr,
				MaxHeldDays:                 maxHeldPtr,
				PenaltyType:                 penaltyType,
				PenaltyValue:                *pvPtr,
				CalculationMethod:           calcMethod,
				NoInterestIfWithdrawnBefore: noInterestPtr,
				Description:                 desc,
				EffectiveFrom:               effFrom,
				EffectiveTo:                 effToPtr,
				IsActive:                    isActivePtr,
			}

			if input.IsActive == nil {
				def := true
				input.IsActive = &def
			}

			if err := validatePenaltyFields(input); err != nil {
				errorsList = append(errorsList, map[string]interface{}{"row_index": rowIdx, constants.ValueSuccess: false, constants.ValueError: err.Error(), "bank_code": input.BankCode})
				continue
			}

			validInputs = append(validInputs, input)
		}

		if len(validInputs) == 0 {
			api.RespondWithPayload(w, false, constants.ErrAllRowsFailedValidation, errorsList)
			return
		}

		// Transactional batch insert
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrTransactionFailed+err.Error())
			api.LogError("Upload: transaction begin failed: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		valueStrings := make([]string, len(validInputs))
		valueArgs := make([]interface{}, 0, len(validInputs)*15)
		for i, row := range validInputs {
			valueStrings[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				i*15+1, i*15+2, i*15+3, i*15+4, i*15+5, i*15+6, i*15+7, i*15+8, i*15+9, i*15+10, i*15+11, i*15+12, i*15+13, i*15+14, i*15+15)
			valueArgs = append(valueArgs,
				row.BankCode,
				row.MinAmountRange,
				row.MaxAmountRange,
				row.MinTenorDays,
				row.MaxTenorDays,
				row.MinHeldDays,
				row.MaxHeldDays,
				strings.ToUpper(row.PenaltyType),
				row.PenaltyValue,
				row.CalculationMethod,
				row.NoInterestIfWithdrawnBefore,
				row.Description,
				row.EffectiveFrom,
				row.EffectiveTo,
				row.IsActive,
			)
		}

		batchInsertQuery := fmt.Sprintf(`
            INSERT INTO investment.fd_penalty_structure_master (
                bank_code, min_amount_range, max_amount_range, min_tenor_days,
                max_tenor_days, min_held_days, max_held_days, penalty_type,
                penalty_value, calculation_method, no_interest_if_withdrawn_before,
                description, effective_from, effective_to, is_active
            ) VALUES %s
            RETURNING penalty_id
        `, strings.Join(valueStrings, ","))

		rows, err := tx.Query(ctx, batchInsertQuery, valueArgs...)
		if err != nil {
			msg, status := getUserFriendlyPenaltyError(err, "Batch insert failed")
			api.RespondWithError(w, status, msg)
			api.LogError("Upload: batch insert failed: %v", err)
			return
		}
		defer rows.Close()

		var inserted []map[string]interface{}
		var penaltyIDs []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "Scan failed: "+err.Error())
				api.LogError("Upload: scan failed: %v", err)
				return
			}
			penaltyIDs = append(penaltyIDs, id)
			inserted = append(inserted, map[string]interface{}{constants.ValueSuccess: true, "penalty_id": id})
		}

		// Batch audit insert
		if len(penaltyIDs) > 0 {
			auditVals := make([]string, len(penaltyIDs))
			auditArgs := make([]interface{}, 0, len(penaltyIDs)*2)
			for i, id := range penaltyIDs {
				auditVals[i] = fmt.Sprintf("($%d,'CREATE','PENDING_APPROVAL',$%d,now())", i*2+1, i*2+2)
				auditArgs = append(auditArgs, id, userEmail)
			}
			auditQuery := fmt.Sprintf(`INSERT INTO investment.fd_audit_penalty_structure (penalty_id, action_type, processing_status, requested_by, requested_at) VALUES %s`, strings.Join(auditVals, ","))
			if _, err := tx.Exec(ctx, auditQuery, auditArgs...); err != nil {
				msg, status := getUserFriendlyPenaltyError(err, constants.ErrBatchAuditFailed)
				api.RespondWithError(w, status, msg)
				api.LogError("Upload: audit insert failed: %v", err)
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			msg, status := getUserFriendlyPenaltyError(err, constants.ErrCommitFailedUser)
			api.RespondWithError(w, status, msg)
			api.LogError("Upload: commit failed: %v", err)
			return
		}

		allResults := append(inserted, errorsList...)
		api.RespondWithPayload(w, len(inserted) > 0, "", allResults)
		api.LogInfo("Upload completed: %d inserted, %d errors, file=%s", len(inserted), len(errorsList), handler.Filename)
	}
}

func GetPenaltyStructure(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			UserID    string `json:"user_id"`
			PenaltyID string `json:"penalty_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONRequired)
			return
		}

		if req.PenaltyID == "" {
			api.RespondWithError(w, http.StatusBadRequest, "penalty_id is required")
			return
		}

		// Verify session
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
		row := pgxPool.QueryRow(ctx, `
            SELECT m.penalty_id, m.bank_code,
                   COALESCE(mb.bank_name,'') AS bank_name,
                   COALESCE(mb.bank_short_name,'') AS bank_short_name,
                   m.min_amount_range, m.max_amount_range, m.min_tenor_days,
                   m.max_tenor_days, m.min_held_days, m.max_held_days, m.penalty_type,
                   m.penalty_value, m.calculation_method, m.no_interest_if_withdrawn_before,
                   m.description, m.effective_from, m.effective_to, m.is_active, m.is_deleted, m.created_at, m.updated_at
            FROM investment.fd_penalty_structure_master m
            LEFT JOIN masterbank mb ON mb.bank_id::text = m.bank_code
            WHERE m.penalty_id = $1
        `, req.PenaltyID)

		var (
			pid           string
			bank          string
			bankName      string
			bankShortName string
			minAmt        *float64
			maxAmt        *float64
			minTenor      int
			maxTenor      int
			minHeld       *int
			maxHeld       *int
			pType         string
			pValue        float64
			calcMethod    string
			noInterest    *int
			desc          *string
			effFrom       string
			effTo         *string
			isActive      *bool
			isDeleted     *bool
			createdAt     interface{}
			updatedAt     interface{}
		)

		err := row.Scan(&pid, &bank, &bankName, &bankShortName, &minAmt, &maxAmt, &minTenor, &maxTenor, &minHeld, &maxHeld, &pType, &pValue, &calcMethod, &noInterest, &desc, &effFrom, &effTo, &isActive, &isDeleted, &createdAt, &updatedAt)
		if err != nil {
			if err.Error() == "no rows in result set" {
				api.RespondWithError(w, http.StatusNotFound, "Penalty structure not found")
			} else {
				msg, status := getUserFriendlyPenaltyError(err, "Get failed")
				api.RespondWithError(w, status, msg)
				api.LogError("Get penalty structure failed: %v", err)
			}
			return
		}

		result := map[string]interface{}{
			"penalty_id":                      pid,
			"bank_code":                       bank,
			"bank_name":                       bankName,
			"bank_short_name":                 bankShortName,
			"min_amount_range":                minAmt,
			"max_amount_range":                maxAmt,
			"min_tenor_days":                  minTenor,
			"max_tenor_days":                  maxTenor,
			"min_held_days":                   minHeld,
			"max_held_days":                   maxHeld,
			"penalty_type":                    pType,
			"penalty_value":                   pValue,
			"calculation_method":              calcMethod,
			"no_interest_if_withdrawn_before": noInterest,
			"description":                     desc,
			"effective_from":                  effFrom,
			"effective_to":                    effTo,
			"is_active":                       isActive,
			"is_deleted":                      isDeleted,
			"created_at":                      createdAt,
			"updated_at":                      updatedAt,
		}

		api.RespondWithPayload(w, true, "", result)
		api.LogInfo("Get penalty structure completed for ID %s by %s", req.PenaltyID, userEmail)
	}
}
