package sweepconfig

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/notification/catalog"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func parseDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	// Prefer dd/mm/yyyy for bank statements before falling back to the broader parser set.
	if t, err := time.Parse("02/01/2006", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2/1/2006", s); err == nil {
		return t, nil
	}
	s = strings.TrimSpace(s)

	// Critical: dd/mm/yyyy formats MUST come before mm/dd/yyyy to prevent misparsing Indian bank statements
	layouts := []string{
		// dd/mm/yyyy variants (Indian/European format) - MUST BE FIRST
		"02/01/2006", "02/01/06", "2/1/2006", "2/1/06",
		"02/01/2006 03:04:05 PM", "02/01/06 03:04:05 PM", "2/1/2006 03:04:05 PM", "2/1/06 03:04:05 PM",
		"02/01/2006 3:04:05 PM", "02/01/06 3:04:05 PM", "2/1/2006 3:04:05 PM", "2/1/06 3:04:05 PM",
		"02/01/06 15:04", "02/01/06 3:04", "02/01/06 15:04:05", "02/01/06 3:04:05",
		"2/1/06 15:04", "2/1/06 3:04", "2/1/06 15:04:05", "2/1/06 3:04:05",
		// mm/dd/yyyy variants (American format) - AFTER dd/mm/yyyy
		"01/02/2006", "01/02/06", "1/2/2006", "1/2/06",
		"01/02/2006 03:04:05 PM", "01/02/2006 03:04 PM", "01/02/06 03:04:05 PM", "01/02/06 03:04 PM",
		"1/2/2006 03:04:05 PM", "1/2/2006 03:04 PM", "1/2/06 03:04:05 PM", "1/2/06 03:04 PM",
		"01/02/06 15:04", "01/02/06 3:04", "01/02/06 15:04:05", "01/02/06 3:04:05",
		"1/2/06 15:04", "1/2/06 3:04", "1/2/06 15:04:05", "1/2/06 3:04:05",
		// Named month formats
		constants.DateFormatSlash, constants.DateFormatDash, // for 29/Aug/2025 and 29-Aug-2025
		"2-Jan-2006", "1/Feb/2006",
		// ISO and other formats
		constants.DateFormat, "2006/01/02", "2006.01.02", "01.02.2006", "1.2.2006", "01-02-2006", "1-2-2006",
		"01-02-06", "1-2-06", "2006/1/2", "2006-1-2",
		// dd-Mon-yy and dd/Mon/yy variants
		"02-Jan-06", "02-Jan-2006", "02/Jan/06", "02/Jan/2006",
		"02-Jan-06 15:04", "02-Jan-2006 15:04", "02-Jan-06 3:04", "02-Jan-2006 3:04",
		"02-Jan-06 15:04:05", "02-Jan-2006 15:04:05", "02-Jan-06 3:04:05", "02-Jan-2006 3:04:05",
		"02/Jan/06 15:04", "02/Jan/2006 15:04", "02/Jan/06 3:04", "02/Jan/2006 3:04",
		"02/Jan/06 15:04:05", "02/Jan/2006 15:04:05", "02/Jan/06 3:04:05", "02/Jan/2006 3:04:05",
		"02-Jan-2006 03:04:05 PM", "02-Jan-06 03:04:05 PM", "02-Jan-2006 3:04:05 PM", "02-Jan-06 3:04:05 PM",
		"02/Jan/2006 03:04:05 PM", "02/Jan/06 03:04:05 PM", "02/Jan/2006 3:04:05 PM", "02/Jan/06 3:04:05 PM",
		// dd-Mon-yy variants (American style)
		"01-Feb-06", "01-Feb-2006", "01/Feb/06", "01/Feb/2006",
		"01-Feb-06 15:04", "01-Feb-2006 15:04", "01-Feb-06 3:04", "01-Feb-2006 3:04",
		"01-Feb-06 15:04:05", "01-Feb-2006 15:04:05", "01-Feb-06 3:04:05", "01-Feb-2006 3:04:05",
		"01/Feb/06 15:04", "01/Feb/2006 15:04", "01/Feb/06 3:04", "01/Feb/2006 3:04",
		"01/Feb/06 15:04:05", "01/Feb/2006 15:04:05", "01/Feb/06 3:04:05", "01/Feb/2006 3:04:05",
		// ISO-ish layouts to catch Excel exports that already render as 2026-01-15 or RFC3339 strings
		constants.DateFormat, constants.DateTimeFormat,
		"2006-01-02 15:04:05 -0700 MST", // Go time.String() format from DB scan
		time.RFC3339, "2006-01-02T15:04:05", "2006-01-02T15:04",
	}
	// Try all layouts
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	// Try to parse with 2-digit year fallback (e.g., 13-Dec-25 as 2025)
	if len(s) == 9 && s[2] == '-' && s[6] == '-' { // e.g., 13-Dec-25
		t, err := time.Parse("02-Jan-06", s)
		if err == nil {
			// If year < 100, add 2000
			y := t.Year()
			if y < 100 {
				t = t.AddDate(2000, 0, 0)
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse date: %s", s)
}

// CreateSweepInitiation creates a new initiation record with optional overrides, creates PENDING_APPROVAL audit entry
// If sweep_id doesn't exist, auto-creates sweep with APPROVED status (enabling unplanned sweeps)
func CreateSweepInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID                      string   `json:"user_id"`
			SweepID                     *string  `json:"sweep_id,omitempty"`       // If null, auto-create
			EntityName                  string   `json:"entity_name"`              // Required for auto-create
			SourceBankName              string   `json:"source_bank_name"`         // Required for auto-create
			SourceBankAccount           string   `json:"source_bank_account"`      // Required for auto-create
			TargetBankName              string   `json:"target_bank_name"`         // Required for auto-create
			TargetBankAccount           string   `json:"target_bank_account"`      // Required for auto-create
			SweepType                   string   `json:"sweep_type,omitempty"`     // Default: ZBA
			Frequency                   string   `json:"frequency,omitempty"`      // Default: SPECIFIC_DATE
			EffectiveDate               string   `json:"effective_date,omitempty"` // Default: today
			ExecutionTime               string   `json:"execution_time,omitempty"` // Default: 10:00
			BufferAmount                *float64 `json:"buffer_amount,omitempty"`
			SweepAmount                 *float64 `json:"sweep_amount,omitempty"`
			OverriddenAmount            *float64 `json:"overridden_amount,omitempty"`
			OverriddenExecutionTime     string   `json:"overridden_execution_time,omitempty"`
			OverriddenSourceBankAccount *string  `json:"overridden_source_bank_account,omitempty"`
			OverriddenTargetBankAccount *string  `json:"overridden_target_bank_account,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// resolve initiated_by
		initiatedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				initiatedBy = s.Name
				break
			}
		}
		if initiatedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		var sweepID string
		// autoCreated tracking variable (for future logging/metrics)

		// Case 1: sweep_id is null → auto-create sweep
		if req.SweepID == nil || *req.SweepID == "" {
			// Validate required fields
			if req.EntityName == "" || req.SourceBankName == "" || req.SourceBankAccount == "" ||
				req.TargetBankName == "" || req.TargetBankAccount == "" {
				api.RespondWithResult(w, false, "entity_name, source_bank_name, source_bank_account, target_bank_name, target_bank_account required for auto-create")
				return
			}

			// Validate entity scope
			if !api.IsEntityAllowed(ctx, req.EntityName) {
				api.RespondWithResult(w, false, "unauthorized entity: "+req.EntityName)
				return
			}

			// Set defaults
			sweepType := req.SweepType
			if sweepType == "" {
				sweepType = "ZBA"
			}
			frequency := req.Frequency
			if frequency == "" {
				frequency = "SPECIFIC_DATE"
			}
			effectiveDate := req.EffectiveDate
			if effectiveDate == "" {
				effectiveDate = time.Now().Format(constants.DateFormat)
			}
			executionTime := req.ExecutionTime
			if executionTime == "" {
				executionTime = "10:00"
			}

			// Begin transaction
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
				return
			}
			defer tx.Rollback(ctx)

			// Create sweep configuration
			sweepID = uuid.New().String()
			insSweep := `INSERT INTO cimplrcorpsaas.sweepconfiguration (
				sweep_id, entity_name, source_bank_name, source_bank_account,
				target_bank_name, target_bank_account, sweep_type, frequency,
				effective_date, execution_time, buffer_amount, sweep_amount,
				is_deleted, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, false, now())`

			_, err = tx.Exec(ctx, insSweep,
				sweepID, req.EntityName, req.SourceBankName, req.SourceBankAccount,
				req.TargetBankName, req.TargetBankAccount, sweepType, frequency,
				nullifyEmpty(effectiveDate), executionTime,
				nullifyFloat(req.BufferAmount), nullifyFloat(req.SweepAmount))

			if err != nil {
				api.RespondWithResult(w, false, "failed to auto-create sweep: "+err.Error())
				return
			}

			// Auto-approve sweep (requested_by = checker_by = user)
			insAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepconfiguration (
				sweep_id, actiontype, processing_status, reason,
				requested_by, requested_at, checker_by, checker_at
			) VALUES ($1, 'CREATE', 'APPROVED', $2, $3, now(), $4, now())`

			_, err = tx.Exec(ctx, insAudit,
				sweepID,
				"Auto-created from unplanned initiation",
				initiatedBy,
				initiatedBy)

			if err != nil {
				api.RespondWithResult(w, false, "failed to auto-approve sweep: "+err.Error())
				return
			}

			// Create initiation
			insInit := `INSERT INTO cimplrcorpsaas.sweep_initiation (
				sweep_id, initiated_by, initiation_time, 
				overridden_amount, overridden_execution_time,
				overridden_source_bank_account, overridden_target_bank_account
			) VALUES ($1,$2,now(),$3,$4,$5,$6) RETURNING initiation_id`

			// compute final resolved accounts for duplicate check (consider overrides)
			finalSource := req.SourceBankAccount
			if req.OverriddenSourceBankAccount != nil && strings.TrimSpace(*req.OverriddenSourceBankAccount) != "" {
				finalSource = *req.OverriddenSourceBankAccount
			}
			finalTarget := req.TargetBankAccount
			if req.OverriddenTargetBankAccount != nil && strings.TrimSpace(*req.OverriddenTargetBankAccount) != "" {
				finalTarget = *req.OverriddenTargetBankAccount
			}

			// Duplicate initiation check (prevent same logical sweep for same entity/accounts/time)
			ik := InitiationKey{
				Entity:        req.EntityName,
				SourceAccount: finalSource,
				TargetAccount: finalTarget,
				Frequency:     sweepType,
				EffectiveDate: effectiveDate,
				ExecutionTime: executionTime,
				BufferAmount:  req.BufferAmount,
				SweepAmount:   req.SweepAmount,
			}
			isDup, dErr := isDuplicateInitiation(ctx, pgxPool, ik)
			if dErr != nil {
				tx.Rollback(ctx)
				api.RespondWithResult(w, false, constants.ErrFailedToValidateDuplicateInitiation+dErr.Error())
				return
			}
			if isDup {
				tx.Rollback(ctx)
				api.RespondWithResult(w, false, constants.ErrDuplicateInitiationExists)
				return
			}

			var initiationID string
			err = tx.QueryRow(ctx, insInit,
				sweepID,
				initiatedBy,
				nullifyFloat(req.OverriddenAmount),
				nullifyEmpty(req.OverriddenExecutionTime),
				nullifyStringPtr(req.OverriddenSourceBankAccount),
				nullifyStringPtr(req.OverriddenTargetBankAccount),
			).Scan(&initiationID)

			if err != nil {
				api.RespondWithResult(w, false, "failed to create initiation: "+err.Error())
				return
			}

			// Create PENDING_APPROVAL audit entry for initiation
			auditIns := `INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
				initiation_id, sweep_id, actiontype, processing_status, requested_by, requested_at
			) VALUES ($1, $2, 'CREATE', 'PENDING_APPROVAL', $3, now())`

			_, err = tx.Exec(ctx, auditIns, initiationID, sweepID, initiatedBy)
			if err != nil {
				api.RespondWithResult(w, false, "failed to create audit entry: "+err.Error())
				return
			}

			// Commit transaction
			if err := tx.Commit(ctx); err != nil {
				api.RespondWithResult(w, false, constants.ErrTxCommitFailed+err.Error())
				return
			}

			// autoCreated = true (sweep was auto-created)

			api.RespondWithPayload(w, true, "Sweep auto-created and initiation created successfully, pending approval", map[string]interface{}{
				"initiation_id":      initiationID,
				"sweep_id":           sweepID,
				"processing_status":  constants.StatusPendingApproval,
				"actiontype":         "CREATE",
				"auto_created_sweep": true,
			})
			return
		}

		// Case 2: sweep_id provided → verify exists and is approved (original logic)
		sweepID = *req.SweepID

		// Verify sweep exists and is approved
		var entityName, sourceBank, sourceAccount, targetBank, targetAccount string
		err := pgxPool.QueryRow(ctx, `
			SELECT entity_name, source_bank_name, source_bank_account, target_bank_name, target_bank_account
			FROM cimplrcorpsaas.sweepconfiguration
			WHERE sweep_id = $1 AND is_deleted = false
		`, sweepID).Scan(&entityName, &sourceBank, &sourceAccount, &targetBank, &targetAccount)

		if err != nil {
			api.RespondWithResult(w, false, "Sweep configuration not found: "+err.Error())
			return
		}

		// Validate sweep scope against prevalidation context
		if strings.TrimSpace(entityName) != "" {
			if !api.IsEntityAllowed(ctx, entityName) {
				api.RespondWithResult(w, false, "unauthorized entity")
				return
			}
		}
		// if strings.TrimSpace(sourceBank) != "" {
		// 	if !api.IsBankAllowed(ctx, sourceBank) {
		// 		api.RespondWithResult(w, false, "unauthorized source bank")
		// 		return
		// 	}
		// }
		// if strings.TrimSpace(targetBank) != "" {
		// 	if !api.IsBankAllowed(ctx, targetBank) {
		// 		api.RespondWithResult(w, false, "unauthorized target bank")
		// 		return
		// 	}
		// }
		// if strings.TrimSpace(sourceAccount) != "" {
		// 	if !ctxHasApprovedBankAccountFor(ctx, sourceAccount, sourceBank, entityName) {
		// 		api.RespondWithResult(w, false, "unauthorized source bank account")
		// 		return
		// 	}
		// }
		// if strings.TrimSpace(targetAccount) != "" {
		// 	if !ctxHasApprovedBankAccountFor(ctx, targetAccount, targetBank, entityName) {
		// 		api.RespondWithResult(w, false, "unauthorized target bank account")
		// 		return
		// 	}
		// }

		// Validate overridden accounts if provided
		// if req.OverriddenSourceBankAccount != nil && strings.TrimSpace(*req.OverriddenSourceBankAccount) != "" {
		// 	if !ctxHasApprovedBankAccountFor(ctx, *req.OverriddenSourceBankAccount, sourceBank, entityName) {
		// 		api.RespondWithResult(w, false, "unauthorized overridden source bank account")
		// 		return
		// 	}
		// }
		// if req.OverriddenTargetBankAccount != nil && strings.TrimSpace(*req.OverriddenTargetBankAccount) != "" {
		// 	if !ctxHasApprovedBankAccountFor(ctx, *req.OverriddenTargetBankAccount, targetBank, entityName) {
		// 		api.RespondWithResult(w, false, "unauthorized overridden target bank account")
		// 		return
		// 	}
		// }

		// Check if sweep is approved
		var processingStatus string
		err = pgxPool.QueryRow(ctx, `
			SELECT processing_status
			FROM cimplrcorpsaas.auditactionsweepconfiguration
			WHERE sweep_id = $1
			ORDER BY requested_at DESC
			LIMIT 1
		`, req.SweepID).Scan(&processingStatus)

		if err != nil || processingStatus != constants.StatusApproved {
			api.RespondWithResult(w, false, "Sweep must be approved before creating initiation")
			return
		}

		// Fetch sweep configuration fields needed for duplicate detection
		var sweepType, frequency, cfgEffectiveDate, executionTime string
		var cfgBufferAmount, cfgSweepAmount sqlNullFloat
		err = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(sweep_type,''), COALESCE(frequency,''), COALESCE(effective_date::text,''), COALESCE(execution_time::text,''), buffer_amount, sweep_amount
			FROM cimplrcorpsaas.sweepconfiguration
			WHERE sweep_id = $1 AND is_deleted = false
		`, sweepID).Scan(&sweepType, &frequency, &cfgEffectiveDate, &executionTime, &cfgBufferAmount, &cfgSweepAmount)
		if err != nil {
			api.RespondWithResult(w, false, "failed to read sweep configuration: "+err.Error())
			return
		}

		var bufPtr, sweepPtr *float64
		if cfgBufferAmount.Valid {
			v := cfgBufferAmount.F
			bufPtr = &v
		}
		if cfgSweepAmount.Valid {
			v := cfgSweepAmount.F
			sweepPtr = &v
		}

		// Insert initiation record (removed status and initiation_type, added overridden accounts)
		ins := `INSERT INTO cimplrcorpsaas.sweep_initiation (
			sweep_id, initiated_by, initiation_time, 
			overridden_amount, overridden_execution_time,
			overridden_source_bank_account, overridden_target_bank_account
		) VALUES ($1,$2,now(),$3,$4,$5,$6) RETURNING initiation_id`

		// compute final resolved accounts for duplicate check (consider overrides)
		finalSource := sourceAccount
		if req.OverriddenSourceBankAccount != nil && strings.TrimSpace(*req.OverriddenSourceBankAccount) != "" {
			finalSource = *req.OverriddenSourceBankAccount
		}
		finalTarget := targetAccount
		if req.OverriddenTargetBankAccount != nil && strings.TrimSpace(*req.OverriddenTargetBankAccount) != "" {
			finalTarget = *req.OverriddenTargetBankAccount
		}

		// Duplicate initiation check (prevent same logical sweep for same entity/accounts/time)
		ik := InitiationKey{
			Entity:        entityName,
			SourceAccount: finalSource,
			TargetAccount: finalTarget,
			Frequency:     frequency,
			EffectiveDate: cfgEffectiveDate,
			ExecutionTime: executionTime,
			BufferAmount:  bufPtr,
			SweepAmount:   sweepPtr,
		}
		isDup, dErr := isDuplicateInitiation(ctx, pgxPool, ik)
		if dErr != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToValidateDuplicateInitiation+dErr.Error())
			return
		}
		if isDup {
			api.RespondWithResult(w, false, constants.ErrDuplicateInitiationExists)
			return
		}

		var initiationID string
		err = pgxPool.QueryRow(ctx, ins,
			req.SweepID,
			initiatedBy,
			nullifyFloat(req.OverriddenAmount),
			nullifyEmpty(req.OverriddenExecutionTime),
			nullifyStringPtr(req.OverriddenSourceBankAccount),
			nullifyStringPtr(req.OverriddenTargetBankAccount),
		).Scan(&initiationID)

		if err != nil {
			api.RespondWithResult(w, false, "failed to create sweep initiation: "+err.Error())
			return
		}

		// Create PENDING_APPROVAL audit entry
		auditIns := `INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
			initiation_id, sweep_id, actiontype, processing_status, requested_by, requested_at
		) VALUES ($1, $2, 'CREATE', 'PENDING_APPROVAL', $3, now())`

		_, err = pgxPool.Exec(ctx, auditIns, initiationID, req.SweepID, initiatedBy)
		if err != nil {
			api.RespondWithResult(w, false, "failed to create audit entry: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Sweep initiation created successfully, pending approval", map[string]interface{}{
			"initiation_id":     initiationID,
			"sweep_id":          req.SweepID,
			"processing_status": constants.StatusPendingApproval,
			"actiontype":        "CREATE",
		})
	}
}

// GetSweepInitiations returns initiation records for a specific sweep or all sweeps
func GetSweepInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  string `json:"user_id"`
			SweepID string `json:"sweep_id,omitempty"`
			Status  string `json:"status,omitempty"` // Filter by processing_status
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrMissingUserID)
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// Validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Build query with entity/bank scoping
		entityNames := api.GetEntityNamesFromCtx(ctx)
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}

		query := `
			SELECT 
				i.initiation_id,
				i.sweep_id,
				i.initiated_by,
				i.initiation_time,
				i.overridden_amount,
				i.overridden_execution_time,
				i.overridden_source_bank_account,
				i.overridden_target_bank_account,
				a.actiontype,
				a.processing_status,
				a.requested_by,
				a.checker_by,
				a.checker_comment,
				c.entity_name,
				c.source_bank_name,
				c.source_bank_account,
				c.target_bank_name,
				c.target_bank_account,
				c.sweep_type
			FROM cimplrcorpsaas.sweep_initiation i
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = i.sweep_id
			LEFT JOIN LATERAL (
				SELECT actiontype, processing_status, requested_by, checker_by, checker_comment
				FROM cimplrcorpsaas.auditactionsweepinitiation
				WHERE initiation_id = i.initiation_id
				  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON true
			WHERE COALESCE(c.is_deleted, false) = false
			  AND COALESCE(i.is_deleted, false) = false
		`

		args := []interface{}{}
		argPos := 1

		if len(normEntities) > 0 {
			query += fmt.Sprintf(constants.QuerryEntityNameLower, argPos)
			args = append(args, normEntities)
			argPos++
		}

		if req.SweepID != "" {
			query += fmt.Sprintf(constants.QuerryInitiationID, argPos)
			args = append(args, req.SweepID)
			argPos++
		}

		if req.Status != "" {
			query += fmt.Sprintf(constants.QuerryProcessingStatus, argPos)
			args = append(args, strings.ToUpper(req.Status))
			argPos++
		}

		query += constants.QuerryOrderByInitiationTime
		// Removed pagination - returns all matching initiations

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		initiations := make([]map[string]interface{}, 0)
		for rows.Next() {
			var initiationID, sweepID, initiatedBy string
			var entityName, sourceBank, sourceAccount, targetBank, targetAccount, sweepType string
			var actiontype, processingStatus, requestedBy *string
			var checkerBy, checkerComment *string
			var initiationTime time.Time
			var overriddenAmount *float64
			var overriddenExecutionTime, overriddenSourceAccount, overriddenTargetAccount *string

			err := rows.Scan(
				&initiationID, &sweepID, &initiatedBy, &initiationTime,
				&overriddenAmount, &overriddenExecutionTime,
				&overriddenSourceAccount, &overriddenTargetAccount,
				&actiontype, &processingStatus, &requestedBy, &checkerBy, &checkerComment,
				&entityName, &sourceBank, &sourceAccount, &targetBank, &targetAccount, &sweepType,
			)
			if err != nil {
				continue
			}

			// Apply bank-level filtering
			if sourceBank != "" && !api.IsBankAllowed(ctx, sourceBank) {
				continue
			}
			if targetBank != "" && !api.IsBankAllowed(ctx, targetBank) {
				continue
			}
			if sourceAccount != "" && !ctxHasApprovedBankAccount(ctx, sourceAccount) {
				continue
			}
			if targetAccount != "" && !ctxHasApprovedBankAccount(ctx, targetAccount) {
				continue
			}

			initiation := map[string]interface{}{
				"initiation_id":                  initiationID,
				"sweep_id":                       sweepID,
				"initiated_by":                   initiatedBy,
				"initiation_time":                initiationTime.Format(constants.DateTimeFormat),
				"overridden_amount":              overriddenAmount,
				"overridden_execution_time":      overriddenExecutionTime,
				"overridden_source_bank_account": overriddenSourceAccount,
				"overridden_target_bank_account": overriddenTargetAccount,
				"actiontype":                     actiontype,
				"processing_status":              processingStatus,
				"requested_by":                   requestedBy,
				"checker_by":                     checkerBy,
				"checker_comment":                checkerComment,
				"entity_name":                    entityName,
				"source_bank_name":               sourceBank,
				"source_bank_account":            sourceAccount,
				"target_bank_name":               targetBank,
				"target_bank_account":            targetAccount,
				"sweep_type":                     sweepType,
			}

			initiations = append(initiations, initiation)
		}

		if rows.Err() != nil {
			api.RespondWithResult(w, false, "DB rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"initiations": initiations,
			"total":       len(initiations),
		})
	}
}

// DEPRECATED: UpdateSweepInitiationStatus - Use bulk approve/reject instead
/*
func UpdateSweepInitiationStatus(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID       string `json:"user_id"`
			InitiationID string `json:"initiation_id"`
			Status       string `json:"status"` // INITIATED, IN_PROGRESS, COMPLETED, FAILED, CANCELLED
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || req.InitiationID == "" || req.Status == "" {
			api.RespondWithResult(w, false, "user_id, initiation_id, and status required")
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Validate status
		statusUpper := strings.ToUpper(strings.TrimSpace(req.Status))
		validStatuses := map[string]bool{
			"INITIATED":   true,
			"IN_PROGRESS": true,
			"COMPLETED":   true,
			"FAILED":      true,
			"CANCELLED":   true,
		}
		if !validStatuses[statusUpper] {
			api.RespondWithResult(w, false, "invalid status. Allowed values: INITIATED, IN_PROGRESS, COMPLETED, FAILED, CANCELLED")
			return
		}

		upd := `UPDATE cimplrcorpsaas.sweep_initiation SET status = $1 WHERE initiation_id = $2 RETURNING sweep_id`
		var sweepID string
		err := pgxPool.QueryRow(ctx, upd, statusUpper, req.InitiationID).Scan(&sweepID)
		if err != nil {
			api.RespondWithResult(w, false, "failed to update initiation status: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Initiation status updated successfully", map[string]interface{}{
			"initiation_id": req.InitiationID,
			"sweep_id":      sweepID,
			"status":        statusUpper,
		})
	}
}
*/

// DEPRECATED: CancelSweepInitiation - Use bulk delete instead
/*
func CancelSweepInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID       string `json:"user_id"`
			InitiationID string `json:"initiation_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || req.InitiationID == "" {
			api.RespondWithResult(w, false, "user_id and initiation_id required")
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Only cancel if status is INITIATED or IN_PROGRESS
		upd := `UPDATE cimplrcorpsaas.sweep_initiation
				SET status = 'CANCELLED'
				WHERE initiation_id = $1
				AND status IN ('INITIATED', 'IN_PROGRESS')
				RETURNING sweep_id, status`

		var sweepID, oldStatus string
		err := pgxPool.QueryRow(ctx, upd, req.InitiationID).Scan(&sweepID, &oldStatus)
		if err != nil {
			api.RespondWithResult(w, false, "failed to cancel initiation (may already be completed/failed/cancelled): "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Initiation cancelled successfully", map[string]interface{}{
			"initiation_id": req.InitiationID,
			"sweep_id":      sweepID,
			"old_status":    oldStatus,
			"new_status":    "CANCELLED",
		})
	}
}
*/

// GetApprovedActiveSweepInitiations returns only sweep initiations for APPROVED and ACTIVE sweep configurations
func GetApprovedActiveSweepInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  string `json:"user_id"`
			SweepID string `json:"sweep_id,omitempty"`
			Status  string `json:"status,omitempty"` // Filter by processing_status
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrMissingUserID)
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// Validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// Removed pagination - returns all approved active initiations

		// Build query with entity/bank scoping + APPROVED status filter
		entityNames := api.GetEntityNamesFromCtx(ctx)
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}

		query := `
			SELECT 
				i.initiation_id,
				i.sweep_id,
				i.initiated_by,
				i.initiation_time,
				i.overridden_amount,
				i.overridden_execution_time,
				i.overridden_source_bank_account,
				i.overridden_target_bank_account,
				a.actiontype,
				a.processing_status,
				c.entity_name,
				c.source_bank_name,
				c.source_bank_account,
				c.target_bank_name,
				c.target_bank_account,
				c.sweep_type
			FROM cimplrcorpsaas.sweep_initiation i
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = i.sweep_id
			JOIN cimplrcorpsaas.auditactionsweepconfiguration asc ON asc.sweep_id = c.sweep_id
			LEFT JOIN LATERAL (
				SELECT actiontype, processing_status
				FROM cimplrcorpsaas.auditactionsweepinitiation
				WHERE initiation_id = i.initiation_id
				  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON true
			WHERE c.is_deleted = false
				AND asc.processing_status = 'APPROVED'
		`

		args := []interface{}{}
		argPos := 1

		if len(normEntities) > 0 {
			query += fmt.Sprintf(constants.QuerryEntityNameLower, argPos)
			args = append(args, normEntities)
			argPos++
		}

		if req.SweepID != "" {
			query += fmt.Sprintf(constants.QuerryInitiationID, argPos)
			args = append(args, req.SweepID)
			argPos++
		}

		if req.Status != "" {
			query += fmt.Sprintf(constants.QuerryProcessingStatus, argPos)
			args = append(args, strings.ToUpper(req.Status))
			argPos++
		}

		query += constants.QuerryOrderByInitiationTime
		// Removed pagination - returns all matching initiations

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		initiations := make([]map[string]interface{}, 0)
		for rows.Next() {
			var initiationID, sweepID, initiatedBy string
			var entityName, sourceBank, sourceAccount, targetBank, targetAccount, sweepType string
			var actiontype, processingStatus *string
			var initiationTime time.Time
			var overriddenAmount *float64
			var overriddenExecutionTime, overriddenSourceAccount, overriddenTargetAccount *string

			err := rows.Scan(
				&initiationID, &sweepID, &initiatedBy, &initiationTime,
				&overriddenAmount, &overriddenExecutionTime,
				&overriddenSourceAccount, &overriddenTargetAccount,
				&actiontype, &processingStatus,
				&entityName, &sourceBank, &sourceAccount, &targetBank, &targetAccount, &sweepType,
			)
			if err != nil {
				api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
				return
			}

			initiation := map[string]interface{}{
				"initiation_id":                  initiationID,
				"sweep_id":                       sweepID,
				"initiated_by":                   initiatedBy,
				"initiation_time":                initiationTime,
				"overridden_amount":              overriddenAmount,
				"overridden_execution_time":      overriddenExecutionTime,
				"overridden_source_bank_account": overriddenSourceAccount,
				"overridden_target_bank_account": overriddenTargetAccount,
				"actiontype":                     actiontype,
				"processing_status":              processingStatus,
				"entity_name":                    entityName,
				"source_bank_name":               sourceBank,
				"source_bank_account":            sourceAccount,
				"target_bank_name":               targetBank,
				"target_bank_account":            targetAccount,
				"sweep_type":                     sweepType,
			}
			initiations = append(initiations, initiation)
		}

		if rows.Err() != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", initiations)
	}
}

// Helper functions
func nullifyStringPtr(s *string) interface{} {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}

// isDuplicateInitiation checks whether an initiation (or an already-created initiation)
// exists for the same logical parameters: entity, source account, target account,
// frequency, effective_date, execution_time, buffer_amount and sweep_amount.
// It considers initiation-level overrides (overridden_source/target) when present.
// InitiationKey groups logical parameters for duplicate initiation checks
type InitiationKey struct {
	Entity        string
	SourceAccount string
	TargetAccount string
	Frequency     string
	EffectiveDate string
	ExecutionTime string
	BufferAmount  *float64
	SweepAmount   *float64
}

func isDuplicateInitiation(ctx context.Context, pgxPool *pgxpool.Pool, key InitiationKey) (bool, error) {
	// Normalize empty strings to '' for comparison
	entityNorm := strings.TrimSpace(key.Entity)
	srcNorm := strings.TrimSpace(key.SourceAccount)
	tgtNorm := strings.TrimSpace(key.TargetAccount)
	freqNorm := strings.ToUpper(strings.TrimSpace(key.Frequency))
	effNorm := strings.TrimSpace(key.EffectiveDate)
	execNorm := strings.TrimSpace(key.ExecutionTime)

	// Query looks for any initiation whose resolved source/target (overrides or config)
	// match the provided values, and where the initiation audit is PENDING_APPROVAL or APPROVED.
	// We compare execution_time and effective_date as text to keep comparisons simple.
	q := `
		SELECT COUNT(1)
		FROM cimplrcorpsaas.sweep_initiation si
		JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = si.sweep_id
		JOIN cimplrcorpsaas.auditactionsweepinitiation asi ON asi.initiation_id = si.initiation_id
		WHERE COALESCE(NULLIF(COALESCE(si.overridden_source_bank_account, sc.source_bank_account), ''), '') = $1
		  AND COALESCE(NULLIF(COALESCE(si.overridden_target_bank_account, sc.target_bank_account), ''), '') = $2
		  AND COALESCE(NULLIF(COALESCE(sc.entity_name, ''), ''), '') = $3
		  AND COALESCE(UPPER(TRIM(sc.frequency)), '') = $4
		  AND COALESCE(sc.effective_date::text, '') = $5
		  AND COALESCE(sc.execution_time::text, '') = $6
		  AND COALESCE(sc.buffer_amount::double precision,0) = COALESCE($7::double precision,0)
		  AND COALESCE(sc.sweep_amount::double precision,0) = COALESCE($8::double precision,0)
		  AND COALESCE(si.is_deleted, false) = false
		  AND COALESCE(sc.is_deleted, false) = false
		  AND asi.actiontype IN ('CREATE', 'EDIT', 'DELETE')
		  AND COALESCE(asi.processing_status, '') IN ('PENDING_APPROVAL','APPROVED')
	`

	var count int
	// Prepare numeric nils as interface{}; leaving nil is acceptable
	if err := pgxPool.QueryRow(ctx, q, srcNorm, tgtNorm, entityNorm, freqNorm, effNorm, execNorm, key.BufferAmount, key.SweepAmount).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// BulkApproveSweepInitiations approves multiple sweep initiations
func BulkApproveSweepInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID         string   `json:"user_id"`
			InitiationIDs  []string `json:"initiation_ids"`
			CheckerComment string   `json:"checker_comment,omitempty"`
			Comment        string   `json:"comment,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.InitiationIDs) == 0 {
			api.RespondWithResult(w, false, constants.ErrUserIDAndInitiationIDsRequired)
			return
		}

		// Validate session
		checkerName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerName = s.Name
				break
			}
		}
		if checkerName == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		comment := req.CheckerComment
		if strings.TrimSpace(comment) == "" {
			comment = req.Comment
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		rows, err := tx.Query(ctx, `
			SELECT DISTINCT ON (initiation_id)
				action_id, initiation_id, actiontype, processing_status
			FROM cimplrcorpsaas.auditactionsweepinitiation
			WHERE initiation_id = ANY($1)
			  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY initiation_id, requested_at DESC, action_id DESC
		`, req.InitiationIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest initiation audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0, len(req.InitiationIDs))
		deleteIDs := make([]string, 0)
		approvedIDs := make([]string, 0, len(req.InitiationIDs))
		found := map[string]bool{}
		for rows.Next() {
			var actionID, id, actionType, status string
			if err := rows.Scan(&actionID, &id, &actionType, &status); err != nil {
				api.RespondWithResult(w, false, "failed to read latest initiation audits: "+err.Error())
				return
			}
			found[id] = true
			if status != constants.StatusPendingApproval && status != constants.StatusPendingEditApproval && status != constants.StatusPendingDeleteApproval {
				api.RespondWithResult(w, false, "cannot approve non-pending initiation: "+id)
				return
			}
			actionIDs = append(actionIDs, actionID)
			approvedIDs = append(approvedIDs, id)
			if actionType == constants.AuditActionDelete || status == constants.StatusPendingDeleteApproval {
				deleteIDs = append(deleteIDs, id)
			}
		}
		for _, id := range req.InitiationIDs {
			if !found[id] {
				api.RespondWithResult(w, false, constants.ErrMissingLatestAuditForInitiation+id)
				return
			}
		}
		upd := `UPDATE cimplrcorpsaas.auditactionsweepinitiation 
				SET processing_status = 'APPROVED', 
					checker_by = $1, 
					checker_at = now(), 
					checker_comment = $2
				WHERE action_id = ANY($3)`
		if _, err := tx.Exec(ctx, upd, checkerName, nullifyEmpty(comment), actionIDs); err != nil {
			api.RespondWithResult(w, false, "failed to approve initiations: "+err.Error())
			return
		}
		if len(deleteIDs) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE cimplrcorpsaas.sweep_initiation SET is_deleted = TRUE WHERE initiation_id = ANY($1)`, deleteIDs); err != nil {
				api.RespondWithResult(w, false, "failed to soft delete initiations: "+err.Error())
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit approve: "+err.Error())
			return
		}
		committed = true

		api.RespondWithPayload(w, true, "Initiations approved successfully", map[string]interface{}{
			"approved_initiation_ids": approvedIDs,
			"total_approved":          len(approvedIDs),
		})
		// Notify with FULL initiation data for rich templates
		capturedIDs := approvedIDs
		capturedUser := req.UserID
		capturedComment := comment
		notifyCtx := context.WithoutCancel(ctx)
		payload := BuildSweepInitiationNotifPayload(notifyCtx, pgxPool, capturedIDs, "APPROVE", capturedUser)
		payloadMap := payload.ToMap()
		payloadMap["CheckerComment"] = capturedComment
		go catalog.TriggerNotification(
			notifyCtx, pgxPool,
			"/cash/sweep-initiation/bulk-approve",
			fmt.Sprintf("SWEEPINIT_APPROVE/%s/%d", capturedUser, time.Now().UnixMilli()),
			payloadMap,
		)
	}
}

// BulkRejectSweepInitiations rejects multiple sweep initiations
func BulkRejectSweepInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID         string   `json:"user_id"`
			InitiationIDs  []string `json:"initiation_ids"`
			CheckerComment string   `json:"checker_comment,omitempty"`
			Comment        string   `json:"comment,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.InitiationIDs) == 0 {
			api.RespondWithResult(w, false, constants.ErrUserIDAndInitiationIDsRequired)
			return
		}

		// Validate session
		checkerName := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				checkerName = s.Name
				break
			}
		}
		if checkerName == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		comment := req.CheckerComment
		if strings.TrimSpace(comment) == "" {
			comment = req.Comment
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		rows, err := tx.Query(ctx, `
			SELECT DISTINCT ON (initiation_id)
				action_id, initiation_id, processing_status
			FROM cimplrcorpsaas.auditactionsweepinitiation
			WHERE initiation_id = ANY($1)
			  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY initiation_id, requested_at DESC, action_id DESC
		`, req.InitiationIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to fetch latest initiation audits: "+err.Error())
			return
		}
		defer rows.Close()

		actionIDs := make([]string, 0, len(req.InitiationIDs))
		rejectedIDs := make([]string, 0, len(req.InitiationIDs))
		found := map[string]bool{}
		for rows.Next() {
			var actionID, id, status string
			if err := rows.Scan(&actionID, &id, &status); err != nil {
				api.RespondWithResult(w, false, "failed to read latest initiation audits: "+err.Error())
				return
			}
			found[id] = true
			if status != constants.StatusPendingApproval && status != constants.StatusPendingEditApproval && status != constants.StatusPendingDeleteApproval {
				api.RespondWithResult(w, false, "cannot reject non-pending initiation: "+id)
				return
			}
			actionIDs = append(actionIDs, actionID)
			rejectedIDs = append(rejectedIDs, id)
		}
		for _, id := range req.InitiationIDs {
			if !found[id] {
				api.RespondWithResult(w, false, constants.ErrMissingLatestAuditForInitiation+id)
				return
			}
		}
		upd := `UPDATE cimplrcorpsaas.auditactionsweepinitiation
				SET processing_status = 'REJECTED',
					checker_by = $1,
					checker_at = now(),
					checker_comment = $2
				WHERE action_id = ANY($3)`
		if _, err := tx.Exec(ctx, upd, checkerName, nullifyEmpty(comment), actionIDs); err != nil {
			api.RespondWithResult(w, false, "failed to reject initiations: "+err.Error())
			return
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit reject: "+err.Error())
			return
		}
		committed = true

		api.RespondWithPayload(w, true, "Initiations rejected successfully", map[string]interface{}{
			"rejected_initiation_ids": rejectedIDs,
			"total_rejected":          len(rejectedIDs),
		})
		// Notify with FULL initiation data for rich templates
		capturedIDs := rejectedIDs
		capturedUser := req.UserID
		capturedComment := comment
		notifyCtx := context.WithoutCancel(ctx)
		payload := BuildSweepInitiationNotifPayload(notifyCtx, pgxPool, capturedIDs, "REJECT", capturedUser)
		payloadMap := payload.ToMap()
		payloadMap["CheckerComment"] = capturedComment
		go catalog.TriggerNotification(
			notifyCtx, pgxPool,
			"/cash/sweep-initiation/bulk-reject",
			fmt.Sprintf("SWEEPINIT_REJECT/%s/%d", capturedUser, time.Now().UnixMilli()),
			payloadMap,
		)
	}
}

// BulkDeleteSweepInitiations creates delete requests for sweep initiations.
func BulkDeleteSweepInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID        string   `json:"user_id"`
			InitiationIDs []string `json:"initiation_ids"`
			Reason        string   `json:"reason,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.InitiationIDs) == 0 {
			api.RespondWithResult(w, false, constants.ErrUserIDAndInitiationIDsRequired)
			return
		}

		// Validate session
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		for _, id := range req.InitiationIDs {
			var sweepID, latestActionType, latestStatus string
			latestErr := tx.QueryRow(ctx, `
				SELECT si.sweep_id, asi.actiontype, asi.processing_status
				FROM cimplrcorpsaas.sweep_initiation si
				JOIN cimplrcorpsaas.auditactionsweepinitiation asi ON asi.initiation_id = si.initiation_id
				WHERE si.initiation_id = $1
				  AND COALESCE(si.is_deleted, false) = false
				  AND asi.actiontype IN ('CREATE', 'EDIT', 'DELETE')
				ORDER BY asi.requested_at DESC, asi.action_id DESC
				LIMIT 1
			`, id).Scan(&sweepID, &latestActionType, &latestStatus)
			if latestErr != nil {
				api.RespondWithResult(w, false, constants.ErrMissingLatestAuditForInitiation+id)
				return
			}
			if latestActionType == constants.AuditActionDelete && latestStatus == constants.StatusPendingDeleteApproval {
				api.RespondWithResult(w, false, "delete request already pending for initiation: "+id)
				return
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO cimplrcorpsaas.auditactionsweepinitiation
					(initiation_id, sweep_id, actiontype, processing_status, reason, requested_by, requested_at)
				VALUES ($1, $2, 'DELETE', 'PENDING_DELETE_APPROVAL', $3, $4, now())
			`, id, sweepID, nullifyEmpty(req.Reason), requestedBy); err != nil {
				api.RespondWithResult(w, false, "failed to create delete request: "+err.Error())
				return
			}
		}
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "failed to commit delete request: "+err.Error())
			return
		}
		committed = true

		api.RespondWithPayload(w, true, "Deletion submitted for approval", map[string]interface{}{
			"deleted_initiation_ids": req.InitiationIDs,
			"total_deleted":          len(req.InitiationIDs),
		})
		// Notify with FULL initiation data for rich templates
		capturedIDs := req.InitiationIDs
		capturedUser := req.UserID
		notifyCtx := context.WithoutCancel(ctx)
		payload := BuildSweepInitiationNotifPayload(notifyCtx, pgxPool, capturedIDs, "DELETE", capturedUser)
		go catalog.TriggerNotification(
			notifyCtx, pgxPool,
			"/cash/sweep-initiation/bulk-delete",
			fmt.Sprintf("SWEEPINIT_DELETE/%s/%d", capturedUser, time.Now().UnixMilli()),
			payload.ToMap(),
		)
	}
}

// ===================================================================================
// BULK CREATE SWEEP INITIATION WITH AUTO-SWEEP CREATION
// Critical business logic: If sweep doesn't exist, auto-create it with approval
// ===================================================================================

type InitiationRequest struct {
	SweepID                     *string  `json:"sweep_id,omitempty"`                       // If null, auto-create sweep
	EntityName                  string   `json:"entity_name"`                              // Required for auto-create
	SourceBankName              string   `json:"source_bank_name"`                         // Required for auto-create
	SourceBankAccount           string   `json:"source_bank_account"`                      // Required for auto-create
	TargetBankName              string   `json:"target_bank_name"`                         // Required for auto-create
	TargetBankAccount           string   `json:"target_bank_account"`                      // Required for auto-create
	SweepType                   string   `json:"sweep_type,omitempty"`                     // ZBA, CONCENTRATION, TARGET_BALANCE (default: ZBA)
	Frequency                   string   `json:"frequency,omitempty"`                      // Default: SPECIFIC_DATE
	EffectiveDate               string   `json:"effective_date,omitempty"`                 // Default: today
	ExecutionTime               string   `json:"execution_time,omitempty"`                 // Default: 10:00
	BufferAmount                *float64 `json:"buffer_amount,omitempty"`                  // Optional
	SweepAmount                 *float64 `json:"sweep_amount,omitempty"`                   // Optional
	OverriddenAmount            *float64 `json:"overridden_amount,omitempty"`              // Initiation override
	OverriddenExecutionTime     string   `json:"overridden_execution_time,omitempty"`      // Initiation override
	OverriddenSourceBankAccount *string  `json:"overridden_source_bank_account,omitempty"` // Initiation override
	OverriddenTargetBankAccount *string  `json:"overridden_target_bank_account,omitempty"` // Initiation override
}

// BulkCreateSweepInitiation creates multiple initiations, auto-creating sweeps if needed
func BulkCreateSweepInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID      string              `json:"user_id"`
			Initiations []InitiationRequest `json:"initiations"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" || len(req.Initiations) == 0 {
			api.RespondWithResult(w, false, "user_id and initiations array required")
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Validate session
		initiatedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				initiatedBy = s.Name
				break
			}
		}
		if initiatedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Begin transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		createdInitiations := make([]map[string]interface{}, 0)
		autoCreatedSweeps := make([]string, 0)

		// dedupe map to avoid creating duplicate initiations within the same bulk request
		seen := make(map[string]bool)

		for _, init := range req.Initiations {
			var sweepID string
			// declare vars that may be populated from either the initiation payload (auto-create)
			// or from an existing sweep config (provided sweep_id)
			var sweepType, frequency, effectiveDate, executionTime string
			// config-scoped variables (declared at loop scope so they're available
			// when we later resolve final parameters outside the creation-if block)
			var entityName, configSourceAccount, configTargetAccount, configSweepType, configFrequency, configEffectiveDate, configExecutionTime sql.NullString
			var configBufferAmount, configSweepAmount sqlNullFloat
			// normalized config/string pointers for later comparison
			var configFreqStr, configEffDateStr, configExecTimeStr string
			var configBufPtr, configSweepPtr *float64

			// Case 1: sweep_id is null → auto-create sweep
			if init.SweepID == nil || *init.SweepID == "" {
				// Validate required fields for auto-create
				if init.EntityName == "" || init.SourceBankName == "" || init.SourceBankAccount == "" ||
					init.TargetBankName == "" || init.TargetBankAccount == "" {
					tx.Rollback(ctx)
					api.RespondWithResult(w, false, "entity_name, source_bank_name, source_bank_account, target_bank_name, target_bank_account required for auto-create")
					return
				}

				// Validate entity scope
				if !api.IsEntityAllowed(ctx, init.EntityName) {
					tx.Rollback(ctx)
					api.RespondWithResult(w, false, "unauthorized entity: "+init.EntityName)
					return
				}

				// Set defaults
				sweepType = init.SweepType
				if sweepType == "" {
					sweepType = "ZBA"
				}
				frequency = init.Frequency
				if frequency == "" {
					frequency = "SPECIFIC_DATE"
				}
				effectiveDate = init.EffectiveDate
				if effectiveDate == "" {
					effectiveDate = time.Now().Format(constants.DateFormat)
				}
				executionTime = init.ExecutionTime
				if executionTime == "" {
					executionTime = "10:00"
				}

				// Create sweep configuration
				sweepID = uuid.New().String()
				insSweep := `INSERT INTO cimplrcorpsaas.sweepconfiguration (
					sweep_id, entity_name, source_bank_name, source_bank_account,
					target_bank_name, target_bank_account, sweep_type, frequency,
					effective_date, execution_time, buffer_amount, sweep_amount,
					is_deleted, created_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, false, now())`

				_, err := tx.Exec(ctx, insSweep,
					sweepID, init.EntityName, init.SourceBankName, init.SourceBankAccount,
					init.TargetBankName, init.TargetBankAccount, sweepType, frequency,
					nullifyEmpty(effectiveDate), executionTime,
					nullifyFloat(init.BufferAmount), nullifyFloat(init.SweepAmount))

				if err != nil {
					tx.Rollback(ctx)
					api.RespondWithResult(w, false, "failed to auto-create sweep: "+err.Error())
					return
				}

				// Auto-approve sweep (requested_by = checker_by = user, approved immediately)
				insAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepconfiguration (
					sweep_id, actiontype, processing_status, reason,
					requested_by, requested_at, checker_by, checker_at
				) VALUES ($1, 'CREATE', 'APPROVED', $2, $3, now(), $4, now())`

				_, err = tx.Exec(ctx, insAudit,
					sweepID,
					"Auto-created from unplanned initiation",
					initiatedBy,
					initiatedBy)

				if err != nil {
					tx.Rollback(ctx)
					api.RespondWithResult(w, false, "failed to auto-approve sweep: "+err.Error())
					return
				}

				autoCreatedSweeps = append(autoCreatedSweeps, sweepID)
			} else {
				// Case 2: sweep_id provided → verify it exists and is approved
				sweepID = *init.SweepID

				err = tx.QueryRow(ctx, `
					SELECT entity_name, source_bank_account, target_bank_account, sweep_type, frequency, COALESCE(effective_date::text,''), COALESCE(execution_time::text,''), buffer_amount, sweep_amount
					FROM cimplrcorpsaas.sweepconfiguration
					WHERE sweep_id = $1 AND is_deleted = false
				`, sweepID).Scan(&entityName, &configSourceAccount, &configTargetAccount, &configSweepType, &configFrequency, &configEffectiveDate, &configExecutionTime, &configBufferAmount, &configSweepAmount)

				if err != nil {
					tx.Rollback(ctx)
					api.RespondWithResult(w, false, "sweep_id not found: "+sweepID)
					return
				}

				// normalize config values for later checks
				if configFrequency.Valid {
					configFreqStr = configFrequency.String
				}
				if configEffectiveDate.Valid {
					configEffDateStr = configEffectiveDate.String
				}
				if configExecutionTime.Valid {
					configExecTimeStr = configExecutionTime.String
				}
				if configBufferAmount.Valid {
					v := configBufferAmount.F
					configBufPtr = &v
				}
				if configSweepAmount.Valid {
					v := configSweepAmount.F
					configSweepPtr = &v
				}

				// Validate entity scope
				if !api.IsEntityAllowed(ctx, entityName.String) {
					tx.Rollback(ctx)
					api.RespondWithResult(w, false, "unauthorized entity for sweep: "+sweepID)
					return
				}

				// Check if approved
				var processingStatus string
				err = tx.QueryRow(ctx, `
					SELECT processing_status
					FROM cimplrcorpsaas.auditactionsweepconfiguration
					WHERE sweep_id = $1
					ORDER BY requested_at DESC
					LIMIT 1
				`, sweepID).Scan(&processingStatus)

				if err != nil || processingStatus != constants.StatusApproved {
					tx.Rollback(ctx)
					api.RespondWithResult(w, false, "sweep must be approved before creating initiation: "+sweepID)
					return
				}
			}

			// Resolve final parameters and check for duplicates before creating initiation
			// finalSource/finalTarget: respect initiation-level overrides if provided
			finalSource := ""
			finalTarget := ""
			var checkFreq, checkEffDate, checkExecTime string
			var checkBufPtr, checkSweepPtr *float64

			if init.SweepID == nil || *init.SweepID == "" {
				// auto-created sweep: use init-provided/defaulted values
				finalSource = init.SourceBankAccount
				if init.OverriddenSourceBankAccount != nil && strings.TrimSpace(*init.OverriddenSourceBankAccount) != "" {
					finalSource = *init.OverriddenSourceBankAccount
				}
				finalTarget = init.TargetBankAccount
				if init.OverriddenTargetBankAccount != nil && strings.TrimSpace(*init.OverriddenTargetBankAccount) != "" {
					finalTarget = *init.OverriddenTargetBankAccount
				}
				checkFreq = frequency
				checkEffDate = effectiveDate
				checkExecTime = executionTime
				checkBufPtr = init.BufferAmount
				checkSweepPtr = init.SweepAmount
			} else {
				// existing sweep: use config values fetched earlier and respect initiation overrides
				finalSource = configSourceAccount.String
				if init.OverriddenSourceBankAccount != nil && strings.TrimSpace(*init.OverriddenSourceBankAccount) != "" {
					finalSource = *init.OverriddenSourceBankAccount
				}
				finalTarget = configTargetAccount.String
				if init.OverriddenTargetBankAccount != nil && strings.TrimSpace(*init.OverriddenTargetBankAccount) != "" {
					finalTarget = *init.OverriddenTargetBankAccount
				}
				checkFreq = configFreqStr
				checkEffDate = configEffDateStr
				checkExecTime = configExecTimeStr
				checkBufPtr = configBufPtr
				checkSweepPtr = configSweepPtr
			}

			// create a dedupe key for the incoming batch
			dedupeKey := strings.Join([]string{strings.TrimSpace(init.EntityName), strings.TrimSpace(finalSource), strings.TrimSpace(finalTarget), strings.ToUpper(strings.TrimSpace(checkFreq)), strings.TrimSpace(checkEffDate), strings.TrimSpace(checkExecTime), fmt.Sprintf("%.6f", func() float64 {
				if checkBufPtr == nil {
					return 0
				}
				return *checkBufPtr
			}()), fmt.Sprintf("%.6f", func() float64 {
				if checkSweepPtr == nil {
					return 0
				}
				return *checkSweepPtr
			}())}, "|")
			if seen[dedupeKey] {
				tx.Rollback(ctx)
				api.RespondWithResult(w, false, fmt.Sprintf("duplicate initiation in batch for entity=%s source=%s target=%s time=%s", init.EntityName, finalSource, finalTarget, checkExecTime))
				return
			}

			// check existing initiations/configs in DB
			ik := InitiationKey{
				Entity:        init.EntityName,
				SourceAccount: finalSource,
				TargetAccount: finalTarget,
				Frequency:     checkFreq,
				EffectiveDate: checkEffDate,
				ExecutionTime: checkExecTime,
				BufferAmount:  checkBufPtr,
				SweepAmount:   checkSweepPtr,
			}
			isDup, dErr := isDuplicateInitiation(ctx, pgxPool, ik)
			if dErr != nil {
				tx.Rollback(ctx)
				api.RespondWithResult(w, false, constants.ErrFailedToValidateDuplicateInitiation+dErr.Error())
				return
			}
			if isDup {
				tx.Rollback(ctx)
				api.RespondWithResult(w, false, constants.ErrDuplicateInitiationExists)
				return
			}
			// mark seen in this batch
			seen[dedupeKey] = true

			// Create initiation record
			insInit := `INSERT INTO cimplrcorpsaas.sweep_initiation (
				sweep_id, initiated_by, initiation_time,
				overridden_amount, overridden_execution_time,
				overridden_source_bank_account, overridden_target_bank_account
			) VALUES ($1, $2, now(), $3, $4, $5, $6) RETURNING initiation_id`

			var initiationID string
			err := tx.QueryRow(ctx, insInit,
				sweepID,
				initiatedBy,
				nullifyFloat(init.OverriddenAmount),
				nullifyEmpty(init.OverriddenExecutionTime),
				nullifyStringPtr(init.OverriddenSourceBankAccount),
				nullifyStringPtr(init.OverriddenTargetBankAccount),
			).Scan(&initiationID)

			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithResult(w, false, "failed to create initiation: "+err.Error())
				return
			}

			// Create PENDING_APPROVAL audit entry for initiation
			insInitAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
				initiation_id, sweep_id, actiontype, processing_status, requested_by, requested_at
			) VALUES ($1, $2, 'CREATE', 'PENDING_APPROVAL', $3, now())`

			_, err = tx.Exec(ctx, insInitAudit, initiationID, sweepID, initiatedBy)
			if err != nil {
				tx.Rollback(ctx)
				api.RespondWithResult(w, false, "failed to create initiation audit: "+err.Error())
				return
			}

			createdInitiations = append(createdInitiations, map[string]interface{}{
				"initiation_id":      initiationID,
				"sweep_id":           sweepID,
				"processing_status":  constants.StatusPendingApproval,
				"auto_created_sweep": init.SweepID == nil || *init.SweepID == "",
			})
		}

		// Commit transaction
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, constants.ErrTxCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Bulk initiations created successfully", map[string]interface{}{
			"created_initiations": createdInitiations,
			"total_created":       len(createdInitiations),
			"auto_created_sweeps": autoCreatedSweeps,
			"total_auto_created":  len(autoCreatedSweeps),
		})
		// Notify: pass FULL initiation data for rich templates
		capturedUser := req.UserID
		// Extract initiation_ids from created initiations
		createdInitiationIDs := []string{}
		for _, init := range createdInitiations {
			if initID, ok := init["initiation_id"].(string); ok && initID != "" {
				createdInitiationIDs = append(createdInitiationIDs, initID)
			}
		}
		if len(createdInitiationIDs) > 0 {
			notifyCtx := context.WithoutCancel(ctx)
			payload := BuildSweepInitiationNotifPayload(notifyCtx, pgxPool, createdInitiationIDs, "CREATE", capturedUser)
			go catalog.TriggerNotification(
				notifyCtx, pgxPool,
				"/cash/sweep-initiation/bulk-create",
				fmt.Sprintf("SWEEPINIT_CREATE/%s/%d", capturedUser, time.Now().UnixMilli()),
				payload.ToMap(),
			)
		}
	}
}

// ===================================================================================
// GET SWEEP INITIATIONS WITH JOINED DATA
// Returns initiations with full sweep configuration details
// ===================================================================================

func GetSweepInitiationsWithJoinedData(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string   `json:"user_id"`
			SweepID  string   `json:"sweep_id,omitempty"`
			Entities []string `json:"entities,omitempty"`
			Status   string   `json:"status,omitempty"` // PENDING_APPROVAL, APPROVED, REJECTED
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Get entity filter
		entityNames := api.GetEntityNamesFromCtx(ctx)
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}

		query := `
			SELECT 
				i.initiation_id,
				i.sweep_id,
				i.initiated_by,
				i.initiation_time,
				i.overridden_amount,
				i.overridden_execution_time,
				i.overridden_source_bank_account,
				i.overridden_target_bank_account,
				a.actiontype,
				a.processing_status,
				a.requested_by,
				a.checker_by,
				a.checker_comment,
				c.entity_name,
				c.source_bank_name,
				c.source_bank_account,
				c.target_bank_name,
				c.target_bank_account,
				c.sweep_type,
				c.frequency,
				c.effective_date,
				c.execution_time,
				c.buffer_amount,
				c.sweep_amount,
				sca.processing_status AS sweep_config_status,
				sca.requested_by AS sweep_config_requested_by,
				sca.checker_by AS sweep_config_checker_by,
				sca.requested_at AS sweep_config_requested_at,
				sca.checker_at AS sweep_config_checker_at
			FROM cimplrcorpsaas.sweep_initiation i
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = i.sweep_id
			LEFT JOIN LATERAL (
				SELECT actiontype, processing_status, requested_by, checker_by, checker_comment
				FROM cimplrcorpsaas.auditactionsweepinitiation
				WHERE initiation_id = i.initiation_id
				  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			) a ON true
			LEFT JOIN LATERAL (
				SELECT processing_status, requested_by, checker_by, requested_at, checker_at
				FROM cimplrcorpsaas.auditactionsweepconfiguration
				WHERE sweep_id = c.sweep_id
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			) sca ON true
			WHERE COALESCE(c.is_deleted, false) = false
			  AND COALESCE(i.is_deleted, false) = false
		`

		args := []interface{}{}
		argPos := 1

		if len(normEntities) > 0 {
			query += fmt.Sprintf(constants.QuerryEntityNameLower, argPos)
			args = append(args, normEntities)
			argPos++
		}

		if req.SweepID != "" {
			query += fmt.Sprintf(constants.QuerryInitiationID, argPos)
			args = append(args, req.SweepID)
			argPos++
		}

		if req.Status != "" {
			query += fmt.Sprintf(constants.QuerryProcessingStatus, argPos)
			args = append(args, strings.ToUpper(req.Status))
			argPos++
		}

		query += constants.QuerryOrderByInitiationTime

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		initiations := make([]map[string]interface{}, 0)
		for rows.Next() {
			var initiationID, sweepID, initiatedBy string
			var initiationTime time.Time
			var overriddenAmount *float64
			var overriddenExecutionTime, overriddenSourceAccount, overriddenTargetAccount *string
			var actiontype, processingStatus, requestedBy *string
			var checkerBy, checkerComment *string
			var entityName, sourceBank, sourceAccount, targetBank, targetAccount string
			var sweepType, frequency string
			var effectiveDate sql.NullTime
			var executionTime *string
			var bufferAmount, sweepAmount *float64
			var sweepConfigStatus, sweepConfigRequestedBy, sweepConfigCheckerBy *string
			var sweepConfigRequestedAt, sweepConfigCheckerAt *time.Time

			err := rows.Scan(
				&initiationID, &sweepID, &initiatedBy, &initiationTime,
				&overriddenAmount, &overriddenExecutionTime,
				&overriddenSourceAccount, &overriddenTargetAccount,
				&actiontype, &processingStatus, &requestedBy, &checkerBy, &checkerComment,
				&entityName, &sourceBank, &sourceAccount, &targetBank, &targetAccount,
				&sweepType, &frequency, &effectiveDate, &executionTime,
				&bufferAmount, &sweepAmount,
				&sweepConfigStatus, &sweepConfigRequestedBy, &sweepConfigCheckerBy,
				&sweepConfigRequestedAt, &sweepConfigCheckerAt,
			)
			if err != nil {
				api.RespondWithResult(w, false, "scan error: "+err.Error())
				return
			}

			var effectiveDateStr *string
			if effectiveDate.Valid {
				formatted := effectiveDate.Time.Format(constants.DateFormat)
				effectiveDateStr = &formatted
			}

			initiation := map[string]interface{}{
				// Initiation fields
				"initiation_id":                  initiationID,
				"sweep_id":                       sweepID,
				"initiated_by":                   initiatedBy,
				"initiation_time":                initiationTime,
				"overridden_amount":              overriddenAmount,
				"overridden_execution_time":      overriddenExecutionTime,
				"overridden_source_bank_account": overriddenSourceAccount,
				"overridden_target_bank_account": overriddenTargetAccount,
				// Initiation audit fields
				"initiation_actiontype":        actiontype,
				"initiation_processing_status": processingStatus,
				"initiation_requested_by":      requestedBy,
				"initiation_checker_by":        checkerBy,
				"initiation_checker_comment":   checkerComment,
				// Sweep config fields (base values)
				"entity_name":         entityName,
				"source_bank_name":    sourceBank,
				"source_bank_account": sourceAccount,
				"target_bank_name":    targetBank,
				"target_bank_account": targetAccount,
				"sweep_type":          sweepType,
				"frequency":           frequency,
				"effective_date":      effectiveDateStr,
				"execution_time":      executionTime,
				"buffer_amount":       bufferAmount,
				"sweep_amount":        sweepAmount,
				// Sweep config audit fields
				"sweep_config_processing_status": sweepConfigStatus,
				"sweep_config_requested_by":      sweepConfigRequestedBy,
				"sweep_config_checker_by":        sweepConfigCheckerBy,
				"sweep_config_requested_at":      sweepConfigRequestedAt,
				"sweep_config_checker_at":        sweepConfigCheckerAt,
			}

			// Apply overrides: execution time, amount, and source/target accounts
			var resolvedExecutionTime *string
			if overriddenExecutionTime != nil && strings.TrimSpace(*overriddenExecutionTime) != "" {
				resolvedExecutionTime = overriddenExecutionTime
			} else {
				resolvedExecutionTime = executionTime
			}

			var resolvedBufferAmount, resolvedSweepAmount *float64
			if overriddenAmount != nil {
				resolvedBufferAmount = overriddenAmount
				resolvedSweepAmount = overriddenAmount
			} else {
				resolvedBufferAmount = bufferAmount
				resolvedSweepAmount = sweepAmount
			}

			resolvedSourceAccount := sourceAccount
			if overriddenSourceAccount != nil && strings.TrimSpace(*overriddenSourceAccount) != "" {
				resolvedSourceAccount = *overriddenSourceAccount
			}
			resolvedTargetAccount := targetAccount
			if overriddenTargetAccount != nil && strings.TrimSpace(*overriddenTargetAccount) != "" {
				resolvedTargetAccount = *overriddenTargetAccount
			}

			// Now update the map with resolved values (so callers see the effective values)
			initiation["execution_time"] = resolvedExecutionTime
			initiation["buffer_amount"] = resolvedBufferAmount
			initiation["sweep_amount"] = resolvedSweepAmount
			initiation["source_bank_account"] = resolvedSourceAccount
			initiation["target_bank_account"] = resolvedTargetAccount

			// Also provide explicit resolved_* fields for clarity
			initiation["resolved_execution_time"] = resolvedExecutionTime
			initiation["resolved_buffer_amount"] = resolvedBufferAmount
			initiation["resolved_sweep_amount"] = resolvedSweepAmount
			initiation["resolved_source_bank_account"] = resolvedSourceAccount
			initiation["resolved_target_bank_account"] = resolvedTargetAccount

			initiations = append(initiations, initiation)
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			constants.ValueSuccess: true,
			"data": map[string]interface{}{
				"initiations": initiations,
				"total":       len(initiations),
			},
		})
	}
}

// ===================================================================================
// ENHANCED GET APPROVED ACTIVE SWEEP CONFIGURATIONS
// Returns: 1) approved_sweeps, 2) potential_sweeps (account pairs with no sweep)
// ===================================================================================

func GetApprovedActiveSweepConfigurationsEnhanced(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID string `json:"user_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" {
			api.RespondWithResult(w, false, "Missing user_id in body")
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// validate session
		valid := false
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				valid = true
				break
			}
		}
		if !valid {
			api.RespondWithResult(w, false, constants.ErrInvalidSessionCapitalized)
			return
		}

		// Get entity filter
		entityNames := api.GetEntityNamesFromCtx(ctx)
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}

		// ====== PART 1: Get approved sweeps (excluding those with existing initiations) ======
		approvedQuery := `
		SELECT DISTINCT ON (sc.sweep_id)
			sc.sweep_id, 
			sc.entity_name, 
			sc.source_bank_name, 
			sc.source_bank_account, 
			sc.target_bank_name, 
			sc.target_bank_account, 
			sc.sweep_type, 
			sc.frequency, 
			sc.effective_date, 
			sc.execution_time, 
			sc.buffer_amount, 
			sc.sweep_amount,
			sc.created_at
		FROM cimplrcorpsaas.sweepconfiguration sc
		JOIN cimplrcorpsaas.auditactionsweepconfiguration a 
			ON a.sweep_id = sc.sweep_id
		LEFT JOIN cimplrcorpsaas.sweep_initiation si
			ON si.sweep_id = sc.sweep_id
		WHERE sc.is_deleted = false 
			AND a.processing_status = 'APPROVED'
			AND si.initiation_id IS NULL
	`

		var approvedRows pgx.Rows
		var err error

		if len(normEntities) > 0 {
			approvedQuery += ` AND lower(trim(sc.entity_name)) = ANY($1)`
			approvedQuery += ` ORDER BY sc.sweep_id, a.requested_at DESC`
			approvedRows, err = pgxPool.Query(ctx, approvedQuery, normEntities)
			if err != nil {
				api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
				return
			}
		} else {
			approvedQuery += ` ORDER BY sc.sweep_id, a.requested_at DESC`
			approvedRows, err = pgxPool.Query(ctx, approvedQuery)
			if err != nil {
				api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
				return
			}
		}
		defer approvedRows.Close()

		approvedSweeps := make([]map[string]interface{}, 0)
		for approvedRows.Next() {
			var sweepID, entityName, sourceBank, sourceAccount, targetBank, targetAccount string
			var sweepType, frequency string
			var effectiveDate sql.NullTime
			var executionTime sql.NullString
			var bufferAmount, sweepAmount *float64
			var createdAt time.Time

			if err := approvedRows.Scan(
				&sweepID, &entityName,
				&sourceBank, &sourceAccount,
				&targetBank, &targetAccount,
				&sweepType, &frequency,
				&effectiveDate, &executionTime,
				&bufferAmount, &sweepAmount,
				&createdAt,
			); err != nil {
				api.RespondWithResult(w, false, "scan error: "+err.Error())
				return
			}

			var effectiveDateStr *string
			if effectiveDate.Valid {
				formatted := effectiveDate.Time.Format(constants.DateFormat)
				effectiveDateStr = &formatted
			}

			var executionTimeStr *string
			if executionTime.Valid {
				s := executionTime.String
				executionTimeStr = &s
			}

			approvedSweeps = append(approvedSweeps, map[string]interface{}{
				"sweep_id":            sweepID,
				"entity_name":         entityName,
				"source_bank_name":    sourceBank,
				"source_bank_account": sourceAccount,
				"target_bank_name":    targetBank,
				"target_bank_account": targetAccount,
				"sweep_type":          sweepType,
				"frequency":           frequency,
				"effective_date":      effectiveDateStr,
				"execution_time":      executionTimeStr,
				"buffer_amount":       bufferAmount,
				"sweep_amount":        sweepAmount,
				"created_at":          createdAt,
			})
		}

		// ====== PART 2: Get potential sweeps (account pairs with no sweep) ======
		potentialQuery := `
			SELECT DISTINCT
				COALESCE(me1.entity_name, mec1.entity_name) AS entity_name,
				COALESCE(mb1.bank_name, '') AS source_bank_name,
				COALESCE(ba1.account_no, ba1.account_number) AS source_account,
				COALESCE(mb2.bank_name, '') AS target_bank_name,
				COALESCE(ba2.account_no, ba2.account_number) AS target_account,
				COALESCE(ba1.currency, '') AS currency_code,
				COALESCE(bbal1.current_balance, 0)::numeric AS source_balance,
				COALESCE(bbal2.current_balance, 0)::numeric AS target_balance
			FROM masterbankaccount ba1
			CROSS JOIN masterbankaccount ba2
			LEFT JOIN masterbank mb1 ON mb1.bank_id = ba1.bank_id
			LEFT JOIN masterbank mb2 ON mb2.bank_id = ba2.bank_id
			LEFT JOIN masterentity me1 ON me1.entity_id::text = ba1.entity_id
			LEFT JOIN masterentitycash mec1 ON mec1.entity_id::text = ba1.entity_id
			LEFT JOIN masterentity me2 ON me2.entity_id::text = ba2.entity_id
			LEFT JOIN masterentitycash mec2 ON mec2.entity_id::text = ba2.entity_id
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionbankaccount
				WHERE account_id = ba1.account_id
				ORDER BY requested_at DESC
				LIMIT 1
			) audit1 ON true
			LEFT JOIN LATERAL (
				SELECT processing_status
				FROM auditactionbankaccount
				WHERE account_id = ba2.account_id
				ORDER BY requested_at DESC
				LIMIT 1
			) audit2 ON true
			-- latest approved balance for source account
			LEFT JOIN LATERAL (
				SELECT COALESCE(bbm.closing_balance, 0) AS current_balance
				FROM public.bank_balances_manual bbm
				JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
				WHERE a.processing_status = 'APPROVED'
				  AND bbm.account_no = COALESCE(ba1.account_no, ba1.account_number)
				ORDER BY bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
				LIMIT 1
			) bbal1 ON true
			-- latest approved balance for target account
			LEFT JOIN LATERAL (
				SELECT COALESCE(bbm.closing_balance, 0) AS current_balance
				FROM public.bank_balances_manual bbm
				JOIN public.auditactionbankbalances a ON a.balance_id = bbm.balance_id
				WHERE a.processing_status = 'APPROVED'
				  AND bbm.account_no = COALESCE(ba2.account_no, ba2.account_number)
				ORDER BY bbm.as_of_date DESC, bbm.as_of_time DESC, a.requested_at DESC
				LIMIT 1
			) bbal2 ON true
			WHERE COALESCE(me1.entity_name, mec1.entity_name) = COALESCE(me2.entity_name, mec2.entity_name)
				AND COALESCE(ba1.is_deleted, false) = false
				AND COALESCE(ba2.is_deleted, false) = false
				AND COALESCE(audit1.processing_status, 'APPROVED') = 'APPROVED'
				AND COALESCE(audit2.processing_status, 'APPROVED') = 'APPROVED'
				AND NOT EXISTS (
					SELECT 1 FROM cimplrcorpsaas.sweepconfiguration sc
					WHERE sc.source_bank_account = COALESCE(ba1.account_no, ba1.account_number)
						AND sc.target_bank_account = COALESCE(ba2.account_no, ba2.account_number)
						AND sc.entity_name = COALESCE(me1.entity_name, mec1.entity_name)
						AND sc.is_deleted = false
				)
		`

		var potentialRows pgx.Rows

		if len(normEntities) > 0 {
			potentialQuery += ` AND lower(trim(COALESCE(me1.entity_name, mec1.entity_name))) = ANY($1)`
			potentialQuery += ` ORDER BY entity_name, source_bank_name, source_account`
			potentialRows, err = pgxPool.Query(ctx, potentialQuery, normEntities)
			if err != nil {
				api.RespondWithResult(w, false, "potential sweeps query error: "+err.Error())
				return
			}
		} else {
			potentialQuery += ` ORDER BY entity_name, source_bank_name, source_account`
			potentialRows, err = pgxPool.Query(ctx, potentialQuery)
			if err != nil {
				api.RespondWithResult(w, false, "potential sweeps query error: "+err.Error())
				return
			}
		}
		defer potentialRows.Close()

		// collect into a dedupe map keyed by source_account so we don't repeat the same source
		dedupe := make(map[string]map[string]interface{})
		for potentialRows.Next() {
			var entityName, sourceBank, sourceAccount, targetBank, targetAccount, currency string
			var sourceBalance, targetBalance *float64

			if err := potentialRows.Scan(
				&entityName, &sourceBank, &sourceAccount,
				&targetBank, &targetAccount, &currency,
				&sourceBalance, &targetBalance,
			); err != nil {
				api.RespondWithResult(w, false, "potential sweep scan error: "+err.Error())
				return
			}
			// filter: require source balance > 0 and not same account
			if sourceBalance == nil || *sourceBalance <= 0 {
				continue
			}
			if sourceAccount == targetAccount {
				continue
			}

			// normalize numeric balance
			var srcBal float64
			if sourceBalance != nil {
				srcBal = *sourceBalance
			}

			entry := map[string]interface{}{
				"entity_name":         entityName,
				"currency_code":       currency,
				"source_bank_name":    sourceBank,
				"source_bank_account": sourceAccount,
				"source_balance":      srcBal,
				"recommended_type":    "ZBA",
				"sweep_type":          "ZBA",
				"frequency":           "DAILY",
			}

			// dedupe: keep the entry with the highest source_balance for the same source account
			if existing, ok := dedupe[sourceAccount]; ok {
				if existingBal, ok2 := existing["source_balance"].(float64); ok2 {
					if srcBal > existingBal {
						dedupe[sourceAccount] = entry
					}
				}
			} else {
				dedupe[sourceAccount] = entry
			}
		}

		// convert dedupe map to slice and sort by source_balance desc
		potentialSweeps := make([]map[string]interface{}, 0, len(dedupe))
		for _, v := range dedupe {
			potentialSweeps = append(potentialSweeps, v)
		}
		sort.Slice(potentialSweeps, func(i, j int) bool {
			bi, _ := potentialSweeps[i]["source_balance"].(float64)
			bj, _ := potentialSweeps[j]["source_balance"].(float64)
			return bi > bj
		})

		api.RespondWithPayload(w, true, "Approved and potential sweeps retrieved successfully", map[string]interface{}{
			"approved_sweeps":  approvedSweeps,
			"potential_sweeps": potentialSweeps,
			"total_approved":   len(approvedSweeps),
			"total_potential":  len(potentialSweeps),
		})
	}
}

// ===================================================================================
// UPDATE SWEEP INITIATION (Updates both initiation and sweep config)
// Initiation audit → PENDING_APPROVAL, Config audit → keeps existing status
// ===================================================================================

func UpdateSweepInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID       string `json:"user_id"`
			InitiationID string `json:"initiation_id"`
			// Initiation fields (optional updates)
			OverriddenAmount            *float64 `json:"overridden_amount,omitempty"`
			OverriddenExecutionTime     *string  `json:"overridden_execution_time,omitempty"`
			OverriddenSourceBankAccount *string  `json:"overridden_source_bank_account,omitempty"`
			OverriddenTargetBankAccount *string  `json:"overridden_target_bank_account,omitempty"`
			// Sweep config fields (optional updates)
			SweepType     *string  `json:"sweep_type,omitempty"`
			Frequency     *string  `json:"frequency,omitempty"`
			EffectiveDate *string  `json:"effective_date,omitempty"`
			ExecutionTime *string  `json:"execution_time,omitempty"`
			BufferAmount  *float64 `json:"buffer_amount,omitempty"`
			SweepAmount   *float64 `json:"sweep_amount,omitempty"`
			Reason        string   `json:"reason,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" || req.InitiationID == "" {
			api.RespondWithResult(w, false, "user_id and initiation_id required")
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Validate session
		requestedBy := ""
		for _, s := range auth.GetActiveSessions() {
			if s.UserID == req.UserID {
				requestedBy = s.Name
				break
			}
		}
		if requestedBy == "" {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Begin transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrFailedToBeginTransaction+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Get sweep_id from initiation
		var sweepID string
		err = tx.QueryRow(ctx, `
			SELECT sweep_id FROM cimplrcorpsaas.sweep_initiation
			WHERE initiation_id = $1
		`, req.InitiationID).Scan(&sweepID)

		if err != nil {
			api.RespondWithResult(w, false, "initiation not found: "+err.Error())
			return
		}

		// Update sweep_initiation table if any initiation fields provided
		if req.OverriddenAmount != nil || req.OverriddenExecutionTime != nil ||
			req.OverriddenSourceBankAccount != nil || req.OverriddenTargetBankAccount != nil {

			updateInit := `UPDATE cimplrcorpsaas.sweep_initiation SET `
			args := []interface{}{}
			argPos := 1
			updates := []string{}

			if req.OverriddenAmount != nil {
				updates = append(updates, fmt.Sprintf("overridden_amount = $%d", argPos))
				args = append(args, req.OverriddenAmount)
				argPos++
			}
			if req.OverriddenExecutionTime != nil {
				updates = append(updates, fmt.Sprintf("overridden_execution_time = $%d", argPos))
				args = append(args, nullifyStringPtr(req.OverriddenExecutionTime))
				argPos++
			}
			if req.OverriddenSourceBankAccount != nil {
				updates = append(updates, fmt.Sprintf("overridden_source_bank_account = $%d", argPos))
				args = append(args, nullifyStringPtr(req.OverriddenSourceBankAccount))
				argPos++
			}
			if req.OverriddenTargetBankAccount != nil {
				updates = append(updates, fmt.Sprintf("overridden_target_bank_account = $%d", argPos))
				args = append(args, nullifyStringPtr(req.OverriddenTargetBankAccount))
				argPos++
			}

			if len(updates) > 0 {
				updateInit += strings.Join(updates, ", ")
				updateInit += fmt.Sprintf(" WHERE initiation_id = $%d", argPos)
				args = append(args, req.InitiationID)

				_, err = tx.Exec(ctx, updateInit, args...)
				if err != nil {
					api.RespondWithResult(w, false, "failed to update initiation: "+err.Error())
					return
				}

				// Create PENDING_EDIT_APPROVAL audit for initiation update
				insInitAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
				initiation_id, sweep_id, actiontype, processing_status, requested_by, requested_at
			) VALUES ($1, $2, 'EDIT', 'PENDING_EDIT_APPROVAL', $3, now())`

				_, err = tx.Exec(ctx, insInitAudit, req.InitiationID, sweepID, requestedBy)
				if err != nil {
					api.RespondWithResult(w, false, "failed to create initiation audit: "+err.Error())
					return
				}
			}
		}

		// Update sweepconfiguration table if any config fields provided
		if req.SweepType != nil || req.Frequency != nil || req.EffectiveDate != nil ||
			req.ExecutionTime != nil || req.BufferAmount != nil || req.SweepAmount != nil {

			updateConfig := `UPDATE cimplrcorpsaas.sweepconfiguration SET `
			args := []interface{}{}
			argPos := 1
			updates := []string{}

			if req.SweepType != nil {
				updates = append(updates, fmt.Sprintf("sweep_type = $%d", argPos))
				args = append(args, req.SweepType)
				argPos++
			}
			if req.Frequency != nil {
				updates = append(updates, fmt.Sprintf("frequency = $%d", argPos))
				args = append(args, req.Frequency)
				argPos++
			}
			if req.EffectiveDate != nil {
				updates = append(updates, fmt.Sprintf("effective_date = $%d", argPos))
				// Normalize incoming date string to YYYY-MM-DD to avoid passing Go's default time.String()
				if d, err := parseDate(*req.EffectiveDate); err == nil {
					args = append(args, d.Format(constants.DateFormat))
				} else {
					args = append(args, nullifyStringPtr(req.EffectiveDate))
				}
				argPos++
			}
			if req.ExecutionTime != nil {
				updates = append(updates, fmt.Sprintf("execution_time = $%d", argPos))
				args = append(args, req.ExecutionTime)
				argPos++
			}
			if req.BufferAmount != nil {
				updates = append(updates, fmt.Sprintf("buffer_amount = $%d::double precision", argPos))
				args = append(args, req.BufferAmount)
				argPos++
			}
			if req.SweepAmount != nil {
				updates = append(updates, fmt.Sprintf("sweep_amount = $%d::double precision", argPos))
				args = append(args, req.SweepAmount)
				argPos++
			}

			if len(updates) > 0 {
				updateConfig += strings.Join(updates, ", ")
				updateConfig += fmt.Sprintf(" WHERE sweep_id = $%d", argPos)
				args = append(args, sweepID)

				_, err = tx.Exec(ctx, updateConfig, args...)
				if err != nil {
					api.RespondWithResult(w, false, "failed to update sweep config: "+err.Error())
					return
				}

				// Create audit for config update (keeps existing status - doesn't change to PENDING)
				// Get current status
				var currentStatus string
				err = tx.QueryRow(ctx, `
					SELECT processing_status
					FROM cimplrcorpsaas.auditactionsweepconfiguration
					WHERE sweep_id = $1
					ORDER BY requested_at DESC
					LIMIT 1
				`, sweepID).Scan(&currentStatus)

				if err != nil {
					currentStatus = constants.StatusPendingApproval // Default if no audit found
				}

				reason := req.Reason
				if reason == "" {
					reason = "Sweep config updated via initiation update"
				}

				insConfigAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepconfiguration (
					sweep_id, actiontype, processing_status, reason, requested_by, requested_at
				) VALUES ($1, 'EDIT', $2, $3, $4, now())`

				_, err = tx.Exec(ctx, insConfigAudit, sweepID, currentStatus, reason, requestedBy)
				if err != nil {
					api.RespondWithResult(w, false, "failed to create config audit: "+err.Error())
					return
				}
			}
		}

		// Commit transaction
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, constants.ErrTxCommitFailed+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Initiation and sweep config updated successfully", map[string]interface{}{
			"initiation_id":       req.InitiationID,
			"sweep_id":            sweepID,
			"initiation_status":   constants.StatusPendingEditApproval,
			"sweep_config_status": "unchanged (keeps existing status)",
		})
		// Notify: pass FULL initiation data for rich templates
		capturedInitID := req.InitiationID
		capturedUser := req.UserID
		capturedReason := req.Reason
		notifyCtx := context.WithoutCancel(ctx)
		payload := BuildSweepInitiationNotifPayload(notifyCtx, pgxPool, []string{capturedInitID}, "UPDATE", capturedUser)
		payloadMap := payload.ToMap()
		payloadMap["Reason"] = capturedReason
		go catalog.TriggerNotification(
			notifyCtx, pgxPool,
			"/cash/sweep-initiation/update",
			fmt.Sprintf("SWEEPINIT_UPDATE/%s/%d", capturedInitID, time.Now().UnixMilli()),
			payloadMap,
		)
	}
}

// Helper function to check if account pair already has a sweep
func sweepExistsForAccounts(ctx context.Context, pgxPool *pgxpool.Pool, sourceAccount, targetAccount, entityName string) (bool, error) {
	var count int
	err := pgxPool.QueryRow(ctx, `
		SELECT COUNT(*) FROM cimplrcorpsaas.sweepconfiguration
		WHERE source_bank_account = $1
			AND target_bank_account = $2
			AND entity_name = $3
			AND is_deleted = false
	`, sourceAccount, targetAccount, entityName).Scan(&count)

	if err != nil {
		return false, err
	}
	return count > 0, nil
}
