package sweepconfig

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateSweepInitiation creates a new initiation record with optional overrides, creates PENDING_APPROVAL audit entry
// If sweep_id doesn't exist, auto-creates sweep with APPROVED status (enabling unplanned sweeps)
func CreateSweepInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID                      string   `json:"user_id"`
			SweepID                     *string  `json:"sweep_id,omitempty"`                   // If null, auto-create
			EntityName                  string   `json:"entity_name"`                          // Required for auto-create
			SourceBankName              string   `json:"source_bank_name"`                     // Required for auto-create
			SourceBankAccount           string   `json:"source_bank_account"`                  // Required for auto-create
			TargetBankName              string   `json:"target_bank_name"`                     // Required for auto-create
			TargetBankAccount           string   `json:"target_bank_account"`                  // Required for auto-create
			SweepType                   string   `json:"sweep_type,omitempty"`                 // Default: ZBA
			Frequency                   string   `json:"frequency,omitempty"`                  // Default: SPECIFIC_DATE
			EffectiveDate               string   `json:"effective_date,omitempty"`             // Default: today
			ExecutionTime               string   `json:"execution_time,omitempty"`             // Default: 10:00
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
			api.RespondWithResult(w, false, "user_id required")
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
				effectiveDate = time.Now().Format("2006-01-02")
			}
			executionTime := req.ExecutionTime
			if executionTime == "" {
				executionTime = "10:00"
			}

			// Begin transaction
			tx, err := pgxPool.Begin(ctx)
			if err != nil {
				api.RespondWithResult(w, false, "failed to begin transaction: "+err.Error())
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
				api.RespondWithResult(w, false, "failed to commit transaction: "+err.Error())
				return
			}

			// autoCreated = true (sweep was auto-created)

			api.RespondWithPayload(w, true, "Sweep auto-created and initiation created successfully, pending approval", map[string]interface{}{
				"initiation_id":       initiationID,
				"sweep_id":            sweepID,
				"processing_status":   "PENDING_APPROVAL",
				"actiontype":          "CREATE",
				"auto_created_sweep":  true,
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

		if err != nil || processingStatus != "APPROVED" {
			api.RespondWithResult(w, false, "Sweep must be approved before creating initiation")
			return
		}

		// Insert initiation record (removed status and initiation_type, added overridden accounts)
		ins := `INSERT INTO cimplrcorpsaas.sweep_initiation (
			sweep_id, initiated_by, initiation_time, 
			overridden_amount, overridden_execution_time,
			overridden_source_bank_account, overridden_target_bank_account
		) VALUES ($1,$2,now(),$3,$4,$5,$6) RETURNING initiation_id`

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
			"processing_status": "PENDING_APPROVAL",
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
			api.RespondWithResult(w, false, "Missing user_id")
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
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON true
			WHERE COALESCE(c.is_deleted, false) = false
		`

		args := []interface{}{}
		argPos := 1

		if len(normEntities) > 0 {
			query += fmt.Sprintf(" AND lower(trim(c.entity_name)) = ANY($%d)", argPos)
			args = append(args, normEntities)
			argPos++
		}

		if req.SweepID != "" {
			query += fmt.Sprintf(" AND i.sweep_id = $%d", argPos)
			args = append(args, req.SweepID)
			argPos++
		}

		if req.Status != "" {
			query += fmt.Sprintf(" AND a.processing_status = $%d", argPos)
			args = append(args, strings.ToUpper(req.Status))
			argPos++
		}

		query += " ORDER BY i.initiation_time DESC"
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
				"initiation_id":                    initiationID,
				"sweep_id":                         sweepID,
				"initiated_by":                     initiatedBy,
				"initiation_time":                  initiationTime.Format("2006-01-02 15:04:05"),
				"overridden_amount":                overriddenAmount,
				"overridden_execution_time":        overriddenExecutionTime,
				"overridden_source_bank_account":   overriddenSourceAccount,
				"overridden_target_bank_account":   overriddenTargetAccount,
				"actiontype":                       actiontype,
				"processing_status":                processingStatus,
				"requested_by":                     requestedBy,
				"checker_by":                       checkerBy,
				"checker_comment":                  checkerComment,
				"entity_name":         entityName,
				"source_bank_name":    sourceBank,
				"source_bank_account": sourceAccount,
				"target_bank_name":    targetBank,
				"target_bank_account": targetAccount,
				"sweep_type":          sweepType,
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
			api.RespondWithResult(w, false, "Missing user_id")
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
				ORDER BY requested_at DESC
				LIMIT 1
			) a ON true
			WHERE c.is_deleted = false
				AND asc.processing_status = 'APPROVED'
		`

		args := []interface{}{}
		argPos := 1

		if len(normEntities) > 0 {
			query += fmt.Sprintf(" AND lower(trim(c.entity_name)) = ANY($%d)", argPos)
			args = append(args, normEntities)
			argPos++
		}

		if req.SweepID != "" {
			query += fmt.Sprintf(" AND i.sweep_id = $%d", argPos)
			args = append(args, req.SweepID)
			argPos++
		}

		if req.Status != "" {
			query += fmt.Sprintf(" AND a.processing_status = $%d", argPos)
			args = append(args, strings.ToUpper(req.Status))
			argPos++
		}

		query += " ORDER BY i.initiation_time DESC"
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
				"initiation_id":                    initiationID,
				"sweep_id":                         sweepID,
				"initiated_by":                     initiatedBy,
				"initiation_time":                  initiationTime,
				"overridden_amount":                overriddenAmount,
				"overridden_execution_time":        overriddenExecutionTime,
				"overridden_source_bank_account":   overriddenSourceAccount,
				"overridden_target_bank_account":   overriddenTargetAccount,
				"actiontype":                       actiontype,
				"processing_status":                processingStatus,
				"entity_name":                      entityName,
				"source_bank_name":                 sourceBank,
				"source_bank_account":              sourceAccount,
				"target_bank_name":                 targetBank,
				"target_bank_account":              targetAccount,
				"sweep_type":                       sweepType,
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

// BulkApproveSweepInitiations approves multiple sweep initiations
func BulkApproveSweepInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID         string   `json:"user_id"`
			InitiationIDs  []string `json:"initiation_ids"`
			CheckerComment string   `json:"checker_comment,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.InitiationIDs) == 0 {
			api.RespondWithResult(w, false, "user_id and initiation_ids required")
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

		// Update audit entries to APPROVED
		upd := `UPDATE cimplrcorpsaas.auditactionsweepinitiation 
				SET processing_status = 'APPROVED', 
					checker_by = $1, 
					checker_at = now(), 
					checker_comment = $2
				WHERE initiation_id = ANY($3) 
				AND processing_status = 'PENDING_APPROVAL'
				RETURNING initiation_id`

		rows, err := pgxPool.Query(ctx, upd, checkerName, nullifyEmpty(req.CheckerComment), req.InitiationIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to approve initiations: "+err.Error())
			return
		}
		defer rows.Close()

		approvedIDs := make([]string, 0)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				approvedIDs = append(approvedIDs, id)
			}
		}

		api.RespondWithPayload(w, true, "Initiations approved successfully", map[string]interface{}{
			"approved_initiation_ids": approvedIDs,
			"total_approved":          len(approvedIDs),
		})
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
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.InitiationIDs) == 0 {
			api.RespondWithResult(w, false, "user_id and initiation_ids required")
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

		// Update audit entries to REJECTED
		upd := `UPDATE cimplrcorpsaas.auditactionsweepinitiation 
				SET processing_status = 'REJECTED', 
					checker_by = $1, 
					checker_at = now(), 
					checker_comment = $2
				WHERE initiation_id = ANY($3) 
				AND processing_status = 'PENDING_APPROVAL'
				RETURNING initiation_id`

		rows, err := pgxPool.Query(ctx, upd, checkerName, nullifyEmpty(req.CheckerComment), req.InitiationIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to reject initiations: "+err.Error())
			return
		}
		defer rows.Close()

		rejectedIDs := make([]string, 0)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				rejectedIDs = append(rejectedIDs, id)
			}
		}

		api.RespondWithPayload(w, true, "Initiations rejected successfully", map[string]interface{}{
			"rejected_initiation_ids": rejectedIDs,
			"total_rejected":          len(rejectedIDs),
		})
	}
}

// BulkDeleteSweepInitiations deletes sweep initiations (hard delete from both tables)
func BulkDeleteSweepInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID        string   `json:"user_id"`
			InitiationIDs []string `json:"initiation_ids"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || len(req.InitiationIDs) == 0 {
			api.RespondWithResult(w, false, "user_id and initiation_ids required")
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

		// Delete from audit table first (FK constraint)
		_, err := pgxPool.Exec(ctx, `DELETE FROM cimplrcorpsaas.auditactionsweepinitiation WHERE initiation_id = ANY($1)`, req.InitiationIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to delete audit entries: "+err.Error())
			return
		}

		// Delete from sweep_initiation
		_, err = pgxPool.Exec(ctx, `DELETE FROM cimplrcorpsaas.sweep_initiation WHERE initiation_id = ANY($1)`, req.InitiationIDs)
		if err != nil {
			api.RespondWithResult(w, false, "failed to delete initiations: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Initiations deleted successfully", map[string]interface{}{
			"deleted_initiation_ids": req.InitiationIDs,
			"total_deleted":          len(req.InitiationIDs),
		})
	}
}
