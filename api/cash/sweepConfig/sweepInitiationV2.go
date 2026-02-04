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

	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateSweepInitiation creates a new initiation record (MANUAL or SCHEDULED) with optional overrides
func CreateSweepInitiation(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID                  string   `json:"user_id"`
			SweepID                 string   `json:"sweep_id"`
			InitiationType          string   `json:"initiation_type"` // MANUAL or SCHEDULED
			OverriddenAmount        *float64 `json:"overridden_amount,omitempty"`
			OverriddenExecutionTime string   `json:"overridden_execution_time,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		if req.UserID == "" || req.SweepID == "" {
			api.RespondWithResult(w, false, "user_id and sweep_id required")
			return
		}
		// user_id must match middleware-authenticated user
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

		// Validate initiation_type
		initiationTypeUpper := strings.ToUpper(strings.TrimSpace(req.InitiationType))
		if initiationTypeUpper != "MANUAL" && initiationTypeUpper != "SCHEDULED" {
			api.RespondWithResult(w, false, "invalid initiation_type. Allowed values: MANUAL, SCHEDULED")
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

		// Verify sweep exists and is approved
		var entityName, sourceBank, sourceAccount, targetBank, targetAccount string
		var requiresInitiation bool
		err := pgxPool.QueryRow(ctx, `
			SELECT entity_name, source_bank_name, source_bank_account, target_bank_name, target_bank_account, requires_initiation
			FROM cimplrcorpsaas.sweepconfiguration
			WHERE sweep_id = $1 AND is_deleted = false
		`, req.SweepID).Scan(&entityName, &sourceBank, &sourceAccount, &targetBank, &targetAccount, &requiresInitiation)

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
		if strings.TrimSpace(sourceBank) != "" {
			if !api.IsBankAllowed(ctx, sourceBank) {
				api.RespondWithResult(w, false, "unauthorized source bank")
				return
			}
		}
		if strings.TrimSpace(targetBank) != "" {
			if !api.IsBankAllowed(ctx, targetBank) {
				api.RespondWithResult(w, false, "unauthorized target bank")
				return
			}
		}
		if strings.TrimSpace(sourceAccount) != "" {
			if !ctxHasApprovedBankAccountFor(ctx, sourceAccount, sourceBank, entityName) {
				api.RespondWithResult(w, false, "unauthorized source bank account")
				return
			}
		}
		if strings.TrimSpace(targetAccount) != "" {
			if !ctxHasApprovedBankAccountFor(ctx, targetAccount, targetBank, entityName) {
				api.RespondWithResult(w, false, "unauthorized target bank account")
				return
			}
		}

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

		// For MANUAL initiation type, requires_initiation check is not mandatory (user can manually trigger anytime)
		// For SCHEDULED initiation type, requires_initiation should be true
		if initiationTypeUpper == "SCHEDULED" && !requiresInitiation {
			api.RespondWithResult(w, false, "Cannot create scheduled initiation for sweep with requires_initiation=false")
			return
		}

		// Insert initiation record
		ins := `INSERT INTO cimplrcorpsaas.sweep_initiation (
			sweep_id, initiated_by, initiation_type, initiation_time, 
			overridden_amount, overridden_execution_time, status
		) VALUES ($1,$2,$3,now(),$4,$5,'INITIATED') RETURNING initiation_id`

		var initiationID string
		err = pgxPool.QueryRow(ctx, ins,
			req.SweepID,
			initiatedBy,
			initiationTypeUpper,
			nullifyFloat(req.OverriddenAmount),
			nullifyEmpty(req.OverriddenExecutionTime),
		).Scan(&initiationID)

		if err != nil {
			api.RespondWithResult(w, false, "failed to create sweep initiation: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Sweep initiation created successfully", map[string]interface{}{
			"initiation_id":   initiationID,
			"sweep_id":        req.SweepID,
			"initiation_type": initiationTypeUpper,
			"status":          "INITIATED",
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
			Status  string `json:"status,omitempty"` // Filter by status: INITIATED, IN_PROGRESS, COMPLETED, FAILED, CANCELLED
			Limit   int    `json:"limit,omitempty"`
			Offset  int    `json:"offset,omitempty"`
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

		// Set defaults
		if req.Limit == 0 {
			req.Limit = 50
		}
		if req.Limit > 500 {
			req.Limit = 500
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
				i.initiation_type,
				i.initiation_time,
				i.overridden_amount,
				i.overridden_execution_time,
				i.status,
				c.entity_name,
				c.source_bank_name,
				c.source_bank_account,
				c.target_bank_name,
				c.target_bank_account,
				c.sweep_type
			FROM cimplrcorpsaas.sweep_initiation i
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = i.sweep_id
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
			query += fmt.Sprintf(" AND i.status = $%d", argPos)
			args = append(args, strings.ToUpper(req.Status))
			argPos++
		}

		query += " ORDER BY i.initiation_time DESC"
		query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argPos, argPos+1)
		args = append(args, req.Limit, req.Offset)

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		initiations := make([]map[string]interface{}, 0)
		for rows.Next() {
			var initiationID, sweepID, initiatedBy, initiationType, status string
			var entityName, sourceBank, sourceAccount, targetBank, targetAccount, sweepType string
			var initiationTime time.Time
			var overriddenAmount *float64
			var overriddenExecutionTime *string

			err := rows.Scan(
				&initiationID, &sweepID, &initiatedBy, &initiationType, &initiationTime,
				&overriddenAmount, &overriddenExecutionTime, &status,
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
				"initiation_id":       initiationID,
				"sweep_id":            sweepID,
				"initiated_by":        initiatedBy,
				"initiation_type":     initiationType,
				"initiation_time":     initiationTime.Format("2006-01-02 15:04:05"),
				"status":              status,
				"entity_name":         entityName,
				"source_bank_name":    sourceBank,
				"source_bank_account": sourceAccount,
				"target_bank_name":    targetBank,
				"target_bank_account": targetAccount,
				"sweep_type":          sweepType,
			}

			if overriddenAmount != nil {
				initiation["overridden_amount"] = *overriddenAmount
			} else {
				initiation["overridden_amount"] = nil
			}

			if overriddenExecutionTime != nil {
				initiation["overridden_execution_time"] = *overriddenExecutionTime
			} else {
				initiation["overridden_execution_time"] = nil
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
			"limit":       req.Limit,
			"offset":      req.Offset,
		})
	}
}

// UpdateSweepInitiationStatus updates the status of an initiation record
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

// CancelSweepInitiation cancels a pending initiation (sets status to CANCELLED)
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

// GetApprovedActiveSweepInitiations returns only sweep initiations for APPROVED and ACTIVE sweep configurations
func GetApprovedActiveSweepInitiations(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  string `json:"user_id"`
			SweepID string `json:"sweep_id,omitempty"`
			Status  string `json:"status,omitempty"` // Filter by status: INITIATED, IN_PROGRESS, COMPLETED, FAILED, CANCELLED
			Limit   int    `json:"limit,omitempty"`
			Offset  int    `json:"offset,omitempty"`
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

		// Set defaults
		if req.Limit == 0 {
			req.Limit = 50
		}
		if req.Limit > 500 {
			req.Limit = 500
		}

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
				i.initiation_type,
				i.initiation_time,
				i.overridden_amount,
				i.overridden_execution_time,
				i.status,
				c.entity_name,
				c.source_bank_name,
				c.source_bank_account,
				c.target_bank_name,
				c.target_bank_account,
				c.sweep_type
			FROM cimplrcorpsaas.sweep_initiation i
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = i.sweep_id
			JOIN cimplrcorpsaas.auditactionsweepconfiguration a ON a.sweep_id = c.sweep_id
			WHERE c.is_deleted = false
				AND a.processing_status = 'APPROVED'
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
			query += fmt.Sprintf(" AND i.status = $%d", argPos)
			args = append(args, strings.ToUpper(req.Status))
			argPos++
		}

		query += " ORDER BY i.initiation_time DESC"
		query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argPos, argPos+1)
		args = append(args, req.Limit, req.Offset)

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		initiations := make([]map[string]interface{}, 0)
		for rows.Next() {
			var initiationID, sweepID, initiatedBy, initiationType, status string
			var entityName, sourceBank, sourceAccount, targetBank, targetAccount, sweepType string
			var initiationTime time.Time
			var overriddenAmount *float64
			var overriddenExecutionTime *string

			err := rows.Scan(
				&initiationID, &sweepID, &initiatedBy, &initiationType, &initiationTime,
				&overriddenAmount, &overriddenExecutionTime, &status,
				&entityName, &sourceBank, &sourceAccount, &targetBank, &targetAccount, &sweepType,
			)
			if err != nil {
				api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
				return
			}

			initiation := map[string]interface{}{
				"initiation_id":             initiationID,
				"sweep_id":                  sweepID,
				"initiated_by":              initiatedBy,
				"initiation_type":           initiationType,
				"initiation_time":           initiationTime,
				"overridden_amount":         overriddenAmount,
				"overridden_execution_time": overriddenExecutionTime,
				"status":                    status,
				"entity_name":               entityName,
				"source_bank_name":          sourceBank,
				"source_bank_account":       sourceAccount,
				"target_bank_name":          targetBank,
				"target_bank_account":       targetAccount,
				"sweep_type":                sweepType,
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
