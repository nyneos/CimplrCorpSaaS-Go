package sweepconfig

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GetSweepExecutionLogsV2 returns the execution history for V2 sweep configurations (includes initiation_id)
func GetSweepExecutionLogsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID       string `json:"user_id"`
			SweepID      string `json:"sweep_id,omitempty"`
			InitiationID string `json:"initiation_id,omitempty"`
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

		// Build query (scoped by allowed entities/banks when context is present)
		entityNames := api.GetEntityNamesFromCtx(ctx)
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}

		query := `
			SELECT 
				l.execution_id,
				l.initiation_id,
				l.sweep_id,
				l.execution_date,
				l.amount_swept,
				l.from_account,
				l.to_account,
				l.status,
				l.error_message,
				l.balance_before,
				l.balance_after
			FROM cimplrcorpsaas.sweep_execution_log l
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = l.sweep_id
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
			query += fmt.Sprintf(" AND l.sweep_id = $%d", argPos)
			args = append(args, req.SweepID)
			argPos++
		}

		if req.InitiationID != "" {
			query += fmt.Sprintf(" AND l.initiation_id = $%d", argPos)
			args = append(args, req.InitiationID)
			argPos++
		}

		query += " ORDER BY execution_date DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		logs := make([]map[string]interface{}, 0)
		for rows.Next() {
			var executionID, sweepID, fromAccount, toAccount, status string
			var initiationID sql.NullString
			var errorMessage *string
			var executionDate time.Time
			var amountSwept, balanceBefore, balanceAfter float64

			err := rows.Scan(
				&executionID, &initiationID, &sweepID, &executionDate, &amountSwept,
				&fromAccount, &toAccount, &status, &errorMessage,
				&balanceBefore, &balanceAfter,
			)
			if err != nil {
				continue
			}

			log := map[string]interface{}{
				"execution_id":   executionID,
				"sweep_id":       sweepID,
				"execution_date": executionDate.Format("2006-01-02 15:04:05"),
				"amount_swept":   amountSwept,
				"from_account":   fromAccount,
				"to_account":     toAccount,
				"status":         status,
				"balance_before": balanceBefore,
				"balance_after":  balanceAfter,
			}

			if initiationID.Valid {
				log["initiation_id"] = initiationID.String
			} else {
				log["initiation_id"] = nil
			}

			if errorMessage != nil {
				log["error_message"] = *errorMessage
			} else {
				log["error_message"] = ""
			}

			logs = append(logs, log)
		}

		if rows.Err() != nil {
			api.RespondWithResult(w, false, "DB rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"logs":  logs,
			"total": len(logs),
		})
	}
}

// GetAllSweepExecutionLogsV2 returns ALL execution logs across all banks (no entity/bank filtering)
func GetAllSweepExecutionLogsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID       string `json:"user_id"`
			SweepID      string `json:"sweep_id,omitempty"`
			InitiationID string `json:"initiation_id,omitempty"`
			Status       string `json:"status,omitempty"` // SUCCESS, FAILED
			FromDate     string `json:"from_date,omitempty"`
			ToDate       string `json:"to_date,omitempty"`
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

		// Build query - NO entity/bank filtering, returns ALL execution logs
		query := `
			SELECT 
				l.execution_id,
				l.initiation_id,
				l.sweep_id,
				l.execution_date,
				l.amount_swept,
				l.from_account,
				l.to_account,
				l.status,
				l.error_message,
				l.balance_before,
				l.balance_after,
				c.entity_name,
				c.source_bank_name,
				c.target_bank_name,
				c.sweep_type
			FROM cimplrcorpsaas.sweep_execution_log l
			LEFT JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = l.sweep_id
			WHERE 1=1
		`

		args := []interface{}{}
		argPos := 1

		if req.SweepID != "" {
			query += fmt.Sprintf(" AND l.sweep_id = $%d", argPos)
			args = append(args, req.SweepID)
			argPos++
		}

		if req.InitiationID != "" {
			query += fmt.Sprintf(" AND l.initiation_id = $%d", argPos)
			args = append(args, req.InitiationID)
			argPos++
		}

		if req.Status != "" {
			query += fmt.Sprintf(" AND l.status = $%d", argPos)
			args = append(args, strings.ToUpper(req.Status))
			argPos++
		}

		if req.FromDate != "" {
			query += fmt.Sprintf(" AND l.execution_date >= $%d", argPos)
			args = append(args, req.FromDate)
			argPos++
		}

		if req.ToDate != "" {
			query += fmt.Sprintf(" AND l.execution_date <= $%d", argPos)
			args = append(args, req.ToDate)
			argPos++
		}

		query += " ORDER BY l.execution_date DESC"

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		logs := make([]map[string]interface{}, 0)
		for rows.Next() {
			var executionID, sweepID, fromAccount, toAccount, status string
			var initiationID sql.NullString
			var errorMessage *string
			var executionDate time.Time
			var amountSwept, balanceBefore, balanceAfter float64
			var entityName, sourceBankName, targetBankName, sweepType sql.NullString

			err := rows.Scan(
				&executionID, &initiationID, &sweepID, &executionDate, &amountSwept,
				&fromAccount, &toAccount, &status, &errorMessage,
				&balanceBefore, &balanceAfter,
				&entityName, &sourceBankName, &targetBankName, &sweepType,
			)
			if err != nil {
				continue
			}

			log := map[string]interface{}{
				"execution_id":   executionID,
				"sweep_id":       sweepID,
				"execution_date": executionDate.Format("2006-01-02 15:04:05"),
				"amount_swept":   amountSwept,
				"from_account":   fromAccount,
				"to_account":     toAccount,
				"status":         status,
				"balance_before": balanceBefore,
				"balance_after":  balanceAfter,
			}

			if initiationID.Valid {
				log["initiation_id"] = initiationID.String
			} else {
				log["initiation_id"] = nil
			}

			if errorMessage != nil {
				log["error_message"] = *errorMessage
			} else {
				log["error_message"] = ""
			}

			if entityName.Valid {
				log["entity_name"] = entityName.String
			} else {
				log["entity_name"] = ""
			}

			if sourceBankName.Valid {
				log["source_bank_name"] = sourceBankName.String
			} else {
				log["source_bank_name"] = ""
			}

			if targetBankName.Valid {
				log["target_bank_name"] = targetBankName.String
			} else {
				log["target_bank_name"] = ""
			}

			if sweepType.Valid {
				log["sweep_type"] = sweepType.String
			} else {
				log["sweep_type"] = ""
			}

			logs = append(logs, log)
		}

		if rows.Err() != nil {
			api.RespondWithResult(w, false, "DB rows error: "+rows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", logs)
	}
}

// GetSweepStatisticsV2 returns statistics about V2 sweep executions
func GetSweepStatisticsV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID   string `json:"user_id"`
			SweepID  string `json:"sweep_id,omitempty"`
			FromDate string `json:"from_date,omitempty"`
			ToDate   string `json:"to_date,omitempty"`
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

		// Build statistics query (scoped by allowed entities/banks when context is present)
		entityNames := api.GetEntityNamesFromCtx(ctx)
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}

		query := `
			SELECT 
				COUNT(*) as total_executions,
				COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful,
				COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed,
				COUNT(CASE WHEN status = 'INSUFFICIENT_FUNDS' THEN 1 END) as insufficient_funds,
				COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN amount_swept ELSE 0 END), 0) as total_amount_swept,
				COALESCE(AVG(CASE WHEN status = 'SUCCESS' THEN amount_swept END), 0) as avg_sweep_amount,
				MAX(execution_date) as last_execution
			FROM cimplrcorpsaas.sweep_execution_log l
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = l.sweep_id
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
			query += fmt.Sprintf(" AND l.sweep_id = $%d", argPos)
			args = append(args, req.SweepID)
			argPos++
		}

		if req.FromDate != "" {
			query += fmt.Sprintf(" AND execution_date >= $%d", argPos)
			args = append(args, req.FromDate)
			argPos++
		}

		if req.ToDate != "" {
			query += fmt.Sprintf(" AND execution_date <= $%d", argPos)
			args = append(args, req.ToDate)
			argPos++
		}

		var totalExec, successful, failed, insufficientFunds int
		var totalAmount, avgAmount float64
		var lastExecution *time.Time

		err := pgxPool.QueryRow(ctx, query, args...).Scan(
			&totalExec, &successful, &failed, &insufficientFunds,
			&totalAmount, &avgAmount, &lastExecution,
		)

		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}

		stats := map[string]interface{}{
			"total_executions":     totalExec,
			"successful":           successful,
			"failed":               failed,
			"insufficient_funds":   insufficientFunds,
			"total_amount_swept":   totalAmount,
			"average_sweep_amount": avgAmount,
			"success_rate":         0.0,
		}

		if totalExec > 0 {
			stats["success_rate"] = float64(successful) / float64(totalExec) * 100
		}

		if lastExecution != nil {
			stats["last_execution"] = lastExecution.Format("2006-01-02 15:04:05")
		} else {
			stats["last_execution"] = nil
		}

		api.RespondWithPayload(w, true, "", stats)
	}
}

// DEPRECATED: ManualTriggerSweepV2Direct - ALL sweeps now require initiation workflow
// Use ManualTriggerSweepV2 instead, which creates initiation + auto-approves
/*
func ManualTriggerSweepV2Direct(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  string `json:"user_id"`
			SweepID string `json:"sweep_id"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || req.SweepID == "" {
			api.RespondWithResult(w, false, "Missing user_id or sweep_id")
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

		// Fetch sweep configuration
		var entityName, sourceBank, sourceAccount, targetBank, targetAccount, sweepType, frequency string
		var effectiveDate sql.NullString
		var bufferAmount, sweepAmount *float64
		var requiresInitiation bool
		var requestedAt time.Time

		err := pgxPool.QueryRow(ctx, `
			SELECT sc.entity_name, sc.source_bank_name, sc.source_bank_account,
			       sc.target_bank_name, sc.target_bank_account, sc.sweep_type,
			       sc.buffer_amount, sc.sweep_amount, sc.requires_initiation,
			       sc.frequency, sc.effective_date,
			       audit.requested_at
			FROM cimplrcorpsaas.sweepconfiguration sc
			INNER JOIN LATERAL (
				SELECT requested_at
				FROM cimplrcorpsaas.auditactionsweepconfiguration
				WHERE sweep_id = sc.sweep_id
				ORDER BY requested_at DESC
				LIMIT 1
			) audit ON true
			WHERE sc.sweep_id = $1 AND sc.is_deleted = false
		`, req.SweepID).Scan(&entityName, &sourceBank, &sourceAccount, &targetBank, &targetAccount,
			&sweepType, &bufferAmount, &sweepAmount, &requiresInitiation, &frequency, &effectiveDate, &requestedAt)

		if err != nil {
			api.RespondWithResult(w, false, "Sweep configuration not found: "+err.Error())
			return
		}

		// CHECK: This endpoint is ONLY for requires_initiation = FALSE
		// For manual trigger, we create an initiation and auto-approve it for audit trail
		if requiresInitiation {
			api.RespondWithResult(w, false, "This sweep requires initiation. Use /manual-trigger endpoint instead.")
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

		// Check if approved
		var processingStatus string
		err = pgxPool.QueryRow(ctx, `
			SELECT processing_status
			FROM cimplrcorpsaas.auditactionsweepconfiguration
			WHERE sweep_id = $1
			ORDER BY requested_at DESC
			LIMIT 1
		`, req.SweepID).Scan(&processingStatus)

		if err != nil || processingStatus != "APPROVED" {
			api.RespondWithResult(w, false, "Sweep must be approved before manual execution")
			return
		}

		// Create initiation record for audit trail
		var initiationID string
		insInitiation := `INSERT INTO cimplrcorpsaas.sweep_initiation (
			sweep_id, initiated_by, initiation_time
		) VALUES ($1, $2, now()) RETURNING initiation_id`

		err = pgxPool.QueryRow(ctx, insInitiation, req.SweepID, requestedBy).Scan(&initiationID)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to create initiation: "+err.Error())
			return
		}

		// Auto-approve the initiation (manual trigger bypasses approval workflow)
		insAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
			initiation_id, sweep_id, actiontype, processing_status,
			requested_by, requested_at, checker_by, checker_at
		) VALUES ($1, $2, 'CREATE', 'APPROVED', $3, now(), $4, now())`

		_, err = pgxPool.Exec(ctx, insAudit, initiationID, req.SweepID, requestedBy, requestedBy)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to auto-approve initiation: "+err.Error())
			return
		}

		// Get current balances
		var sourceBalance, targetBalance float64
		err = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(closing_balance, 0)
			FROM bank_balances_manual
			WHERE account_no = $1
			ORDER BY as_of_date DESC, as_of_time DESC
			LIMIT 1
		`, sourceAccount).Scan(&sourceBalance)

		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch source account balance: "+err.Error())
			return
		}

		err = pgxPool.QueryRow(ctx, `
			SELECT COALESCE(closing_balance, 0)
			FROM bank_balances_manual
			WHERE account_no = $1
			ORDER BY as_of_date DESC, as_of_time DESC
			LIMIT 1
		`, targetAccount).Scan(&targetBalance)

		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch target account balance: "+err.Error())
			return
		}

		// Build SweepDataV2 structure (same as scheduler uses)
		sweepData := struct {
			sweepID       string
			entityName    string
			sourceAccount string
			targetAccount string
			sweepType     string
			frequency     string
			effectiveDate sql.NullString
			bufferAmount  *float64
			sweepAmount   *float64
			requestedAt   time.Time
			sourceBalance float64
			targetBalance float64
		}{
			sweepID:       req.SweepID,
			entityName:    entityName,
			sourceAccount: sourceAccount,
			targetAccount: targetAccount,
			sweepType:     sweepType,
			frequency:     frequency,
			effectiveDate: effectiveDate,
			bufferAmount:  bufferAmount,
			sweepAmount:   sweepAmount,
			requestedAt:   requestedAt,
			sourceBalance: sourceBalance,
			targetBalance: targetBalance,
		}

		// Execute direct sweep (same logic as ExecuteSweepV2Direct from sweep_processorV2.go)
		result, err := executeDirectSweepV2(ctx, pgxPool, sweepData, requestedBy)
		if err != nil {
			api.RespondWithResult(w, false, "Sweep execution failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Sweep executed successfully (direct, no initiation)", result)
	}
}

// executeDirectSweepV2 executes a sweep without initiation (for requires_initiation=false sweeps)
func executeDirectSweepV2(ctx context.Context, pgxPool *pgxpool.Pool, sweep interface{}, requestedBy string) (map[string]interface{}, error) {
	// Type assertion to extract fields (using reflection-like approach)
	type SweepData struct {
		sweepID       string
		entityName    string
		sourceAccount string
		targetAccount string
		sweepType     string
		bufferAmount  *float64
		sweepAmount   *float64
		sourceBalance float64
		targetBalance float64
	}

	s, ok := sweep.(struct {
		sweepID       string
		entityName    string
		sourceAccount string
		targetAccount string
		sweepType     string
		frequency     string
		effectiveDate sql.NullString
		bufferAmount  *float64
		sweepAmount   *float64
		requestedAt   time.Time
		sourceBalance float64
		targetBalance float64
	})

	if !ok {
		return nil, fmt.Errorf("invalid sweep data structure")
	}

	currentBalance := s.sourceBalance

	// Get buffer amount
	buffer := 0.0
	if s.bufferAmount != nil {
		buffer = *s.bufferAmount
	}

	// BR-1.1: Buffer Violation Check - CRITICAL HARD RULE
	if currentBalance <= buffer {
		errMsg := fmt.Sprintf("BLOCKED: Critical Buffer Violation - Current Balance (%.2f) is at or below Buffer (%.2f)", currentBalance, buffer)
		return nil, fmt.Errorf(errMsg)
	}

	// Calculate sweep amount based on sweep type
	var sweepAmountFinal float64
	sweepTypeUpper := strings.ToUpper(strings.TrimSpace(s.sweepType))

	switch sweepTypeUpper {
	case "ZBA":
		// ZBA: Zero Balance Account - Sweep EVERYTHING to leave source at ZERO
		// Only execute if current balance exceeds buffer threshold
		if currentBalance <= buffer {
			return nil, fmt.Errorf("BLOCKED: ZBA sweep - Balance (%.2f) has not exceeded buffer threshold (%.2f)", currentBalance, buffer)
		}
		// Sweep entire balance to make source account zero
		sweepAmountFinal = currentBalance

	case "CONCENTRATION":
		// CONCENTRATION: Fixed amount sweep - only sweep if balance exceeds buffer
		if s.sweepAmount == nil || *s.sweepAmount <= 0 {
			return nil, fmt.Errorf("BLOCKED: CONCENTRATION sweep requires fixed sweep_amount to be configured")
		}

		// Only sweep if current balance exceeds buffer
		if currentBalance <= buffer {
			return nil, fmt.Errorf("BLOCKED: CONCENTRATION sweep - Balance (%.2f) has not exceeded buffer threshold (%.2f)", currentBalance, buffer)
		}

		sweepAmountFinal = *s.sweepAmount

		// Maintain buffer - ensure sweep doesn't violate buffer requirement
		if (currentBalance - sweepAmountFinal) < buffer {
			return nil, fmt.Errorf("BLOCKED: CONCENTRATION sweep - Sweeping %.2f would leave balance below buffer (%.2f)", sweepAmountFinal, buffer)
		}

	case "TARGET_BALANCE":
		// TARGET_BALANCE: Sweep excess to maintain target balance (buffer) in source
		if currentBalance <= buffer {
			return nil, fmt.Errorf("BLOCKED: TARGET_BALANCE sweep - Balance (%.2f) is at or below target (%.2f), no excess to sweep", currentBalance, buffer)
		}
		// Sweep only the excess above buffer, leaving buffer amount in source
		sweepAmountFinal = currentBalance - buffer

	default:
		return nil, fmt.Errorf("unknown sweep type: %s", s.sweepType)
	}

	// Final validation
	if sweepAmountFinal <= 0 {
		return nil, fmt.Errorf("BLOCKED: Calculated sweep amount (%.2f) is invalid", sweepAmountFinal)
	}

	// Calculate new balance after sweep
	newBalance := currentBalance - sweepAmountFinal

	// Begin transaction
	tx, err := pgxPool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("SYSTEM ERROR: Unable to start sweep transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Update source account
	_, err = tx.Exec(ctx, `
		UPDATE bank_balances_manual
		SET old_opening_balance = opening_balance,
			old_closing_balance = closing_balance,
			old_balance_amount = balance_amount,
			old_total_debits = total_debits,
			opening_balance = closing_balance,
			closing_balance = $1,
			balance_amount = $1,
			total_debits = total_debits + $2
		WHERE account_no = $3
	`, newBalance, sweepAmountFinal, s.sourceAccount)

	if err != nil {
		return nil, fmt.Errorf("SYSTEM ERROR: Failed to update source account balance: %v", err)
	}

	// Create audit for source
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		)
		SELECT balance_id, 'EDIT', 'PENDING_EDIT_APPROVAL', $1, $2, NOW()
		FROM bank_balances_manual
		WHERE account_no = $3
		LIMIT 1
	`, fmt.Sprintf("Manual sweep V2 execution (direct): %s", s.sweepID), requestedBy, s.sourceAccount)

	if err != nil {
		return nil, fmt.Errorf(constants.ErrAuditInsertFailedUser)
	}

	// Update target account
	targetNewBalance := s.targetBalance + sweepAmountFinal

	_, err = tx.Exec(ctx, `
		UPDATE bank_balances_manual
		SET old_opening_balance = opening_balance,
			old_closing_balance = closing_balance,
			old_balance_amount = balance_amount,
			old_total_credits = total_credits,
			opening_balance = closing_balance,
			closing_balance = $1,
			balance_amount = $1,
			total_credits = total_credits + $2
		WHERE account_no = $3
	`, targetNewBalance, sweepAmountFinal, s.targetAccount)

	if err != nil {
		return nil, fmt.Errorf("SYSTEM ERROR: Failed to update target account balance: %v", err)
	}

	// Create audit for target
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		)
		SELECT balance_id, 'EDIT', 'PENDING_EDIT_APPROVAL', $1, $2, NOW()
		FROM bank_balances_manual
		WHERE account_no = $3
		LIMIT 1
	`, fmt.Sprintf("Manual sweep V2 receipt (direct): %s", s.sweepID), requestedBy, s.targetAccount)

	if err != nil {
		return nil, fmt.Errorf(constants.ErrAuditInsertFailedUser)
	}

	// Log sweep execution (NO initiation_id for direct sweeps)
	_, err = tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.sweep_execution_log (
			sweep_id, amount_swept, from_account, to_account, status,
			balance_before, balance_after, error_message
		) VALUES ($1, $2, $3, $4, 'SUCCESS', $5, $6, NULL)
	`, s.sweepID, sweepAmountFinal, s.sourceAccount, s.targetAccount, currentBalance, newBalance)

	if err != nil {
		return nil, fmt.Errorf("SYSTEM ERROR: Failed to log sweep execution: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("SYSTEM ERROR: Failed to commit sweep transaction: %v", err)
	}

	return map[string]interface{}{
		"sweep_id":       s.sweepID,
		"amount_swept":   sweepAmountFinal,
		"from_account":   s.sourceAccount,
		"to_account":     s.targetAccount,
		"balance_before": currentBalance,
		"balance_after":  newBalance,
		"execution_type": "DIRECT_MANUAL",
	}, nil
}
*/

// ManualTriggerSweepV2 allows manual triggering of a V2 sweep by creating a MANUAL initiation record
// Then immediately executes the sweep based on that initiation
func ManualTriggerSweepV2(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID                  string   `json:"user_id"`
			SweepID                 string   `json:"sweep_id"`
			OverriddenAmount        *float64 `json:"overridden_amount,omitempty"`
			OverriddenExecutionTime string   `json:"overridden_execution_time,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" || req.SweepID == "" {
			api.RespondWithResult(w, false, "Missing user_id or sweep_id")
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

		// Fetch sweep configuration
		var entityName, sourceBank, sourceAccount, targetBank, targetAccount, sweepType string
		var bufferAmount, sweepAmount *float64

		err := pgxPool.QueryRow(ctx, `
		SELECT entity_name, source_bank_name, source_bank_account, target_bank_name, target_bank_account, 
			   sweep_type, buffer_amount, sweep_amount
		FROM cimplrcorpsaas.sweepconfiguration
		WHERE sweep_id = $1 AND is_deleted = false
	`, req.SweepID).Scan(&entityName, &sourceBank, &sourceAccount, &targetBank, &targetAccount,
			&sweepType, &bufferAmount, &sweepAmount)

		if err != nil {
			api.RespondWithResult(w, false, "Sweep configuration not found: "+err.Error())
			return
		}

		// NOTE: ALL sweeps now use initiation workflow (requires_initiation flag removed)		// Validate sweep scope against prevalidation context
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

		// Check if approved (ONLY requirement for manual trigger)
		var processingStatus string
		err = pgxPool.QueryRow(ctx, `
			SELECT processing_status
			FROM cimplrcorpsaas.auditactionsweepconfiguration
			WHERE sweep_id = $1
			ORDER BY requested_at DESC
			LIMIT 1
		`, req.SweepID).Scan(&processingStatus)

		if err != nil || processingStatus != "APPROVED" {
			api.RespondWithResult(w, false, "Sweep must be approved before manual execution")
			return
		}

		// Create MANUAL initiation record (no status/initiation_type - using audit table)
		ins := `INSERT INTO cimplrcorpsaas.sweep_initiation (
			sweep_id, initiated_by, initiation_time, 
			overridden_amount, overridden_execution_time
		) VALUES ($1, $2, now(), $3, $4) RETURNING initiation_id`

		var initiationID string
		err = pgxPool.QueryRow(ctx, ins,
			req.SweepID,
			requestedBy,
			nullifyFloat(req.OverriddenAmount),
			nullifyEmpty(req.OverriddenExecutionTime),
		).Scan(&initiationID)

		if err != nil {
			api.RespondWithResult(w, false, "failed to create manual initiation: "+err.Error())
			return
		}

		// Auto-approve the manual initiation (manual triggers bypass approval workflow)
		insAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
			initiation_id, sweep_id, actiontype, processing_status, 
			requested_by, requested_at, checker_by, checker_at
		) VALUES ($1, $2, 'CREATE', 'APPROVED', $3, now(), $4, now())`

		_, err = pgxPool.Exec(ctx, insAudit, initiationID, req.SweepID, requestedBy, requestedBy)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to auto-approve manual initiation: "+err.Error())
			return
		}

		// Execute the sweep with the initiation context
		result, err := executeSweepV2WithInitiation(ctx, pgxPool, req.SweepID, initiationID, sourceAccount, targetAccount,
			sweepType, bufferAmount, sweepAmount, req.OverriddenAmount, requestedBy)

		if err != nil {
			// Execution failure is already logged in sweep_execution_log by executeSweepV2WithInitiation
			api.RespondWithResult(w, false, "Sweep execution failed: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Sweep executed successfully", result)
	}
}

// executeSweepV2WithInitiation executes a V2 sweep with initiation context
func executeSweepV2WithInitiation(ctx context.Context, pgxPool *pgxpool.Pool, sweepID, initiationID,
	sourceAccount, targetAccount, sweepType string, bufferAmount, sweepAmount, overriddenAmount *float64,
	requestedBy string) (map[string]interface{}, error) {

	// Get current balance for source account
	var balanceID string
	var currentBalance float64

	err := pgxPool.QueryRow(ctx, `
		SELECT balance_id, COALESCE(closing_balance, 0)
		FROM bank_balances_manual
		WHERE account_no = $1
		ORDER BY as_of_date DESC, as_of_time DESC
		LIMIT 1
	`, sourceAccount).Scan(&balanceID, &currentBalance)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch source account balance: %w", err)
	}

	// Calculate sweep amount based on sweep type and overrides
	var finalSweepAmount float64
	buffer := 0.0
	if bufferAmount != nil {
		buffer = *bufferAmount
	}

	// If overridden_amount is provided, use it directly (ignores sweep logic)
	if overriddenAmount != nil && *overriddenAmount > 0 {
		finalSweepAmount = *overriddenAmount

		// For ZBA with override, allow sweeping to zero
		sweepTypeUpper := strings.ToUpper(strings.TrimSpace(sweepType))
		if sweepTypeUpper != "ZBA" {
			// For CONCENTRATION and TARGET_BALANCE, validate override doesn't violate buffer
			if (currentBalance - finalSweepAmount) < buffer {
				return nil, fmt.Errorf("BLOCKED: Override amount (%.2f) would leave balance below buffer (%.2f)", finalSweepAmount, buffer)
			}
		}
	} else {
		// Normal sweep logic based on sweep_type
		sweepTypeUpper := strings.ToUpper(strings.TrimSpace(sweepType))

		switch sweepTypeUpper {
		case "ZBA": // Zero Balance Account - sweep EVERYTHING to leave source at ZERO
			// Only execute if current balance exceeds buffer threshold
			if currentBalance <= buffer {
				return nil, fmt.Errorf("BLOCKED: ZBA sweep - Balance (%.2f) has not exceeded buffer threshold (%.2f)", currentBalance, buffer)
			}
			// Sweep entire balance to make source account zero
			finalSweepAmount = currentBalance

		case "CONCENTRATION": // Concentration - fixed amount sweep maintaining buffer
			if sweepAmount == nil || *sweepAmount <= 0 {
				return nil, fmt.Errorf("BLOCKED: CONCENTRATION requires fixed sweep_amount")
			}

			// Only sweep if current balance exceeds buffer
			if currentBalance <= buffer {
				return nil, fmt.Errorf("BLOCKED: CONCENTRATION - Balance (%.2f) has not exceeded buffer threshold (%.2f)", currentBalance, buffer)
			}

			finalSweepAmount = *sweepAmount

			// Maintain buffer - ensure sweep doesn't violate buffer requirement
			if (currentBalance - finalSweepAmount) < buffer {
				return nil, fmt.Errorf("BLOCKED: CONCENTRATION - Sweeping %.2f would leave balance below buffer (%.2f)", finalSweepAmount, buffer)
			}

		case "TARGET_BALANCE": // Target Balance - sweep excess above buffer
			if currentBalance <= buffer {
				return nil, fmt.Errorf("BLOCKED: TARGET_BALANCE - Balance (%.2f) is at or below target (%.2f)", currentBalance, buffer)
			}
			// Sweep only the excess above buffer, leaving buffer amount in source
			finalSweepAmount = currentBalance - buffer

		default:
			return nil, fmt.Errorf("unknown sweep type: %s", sweepType)
		}
	}

	if finalSweepAmount <= 0 {
		return nil, fmt.Errorf("no amount to sweep (calculated: %.2f)", finalSweepAmount)
	}

	// Begin transaction
	tx, err := pgxPool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Update source account
	newBalance := currentBalance - finalSweepAmount

	_, err = tx.Exec(ctx, `
		UPDATE bank_balances_manual
		SET old_opening_balance = opening_balance,
			old_closing_balance = closing_balance,
			old_balance_amount = balance_amount,
			old_total_debits = total_debits,
			opening_balance = closing_balance,
			closing_balance = $1,
			balance_amount = $1,
			total_debits = total_debits + $2
		WHERE balance_id = $3
	`, newBalance, finalSweepAmount, balanceID)

	if err != nil {
		return nil, fmt.Errorf("failed to update source account: %w", err)
	}

	// Create audit for source
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, NOW())
	`, balanceID, fmt.Sprintf("Sweep V2 execution (initiation: %s): %s", initiationID, sweepID), requestedBy)

	if err != nil {
		return nil, fmt.Errorf("failed to create audit for source: %w", err)
	}

	// Get target account
	var targetBalanceID string
	var targetBalance float64

	err = tx.QueryRow(ctx, `
		SELECT balance_id, COALESCE(closing_balance, 0)
		FROM bank_balances_manual
		WHERE account_no = $1
		ORDER BY as_of_date DESC, as_of_time DESC
		LIMIT 1
	`, targetAccount).Scan(&targetBalanceID, &targetBalance)

	if err != nil {
		return nil, fmt.Errorf("target account '%s' not found in bank_balances_manual: %w", targetAccount, err)
	}

	// Update target account
	targetNewBalance := targetBalance + finalSweepAmount

	_, err = tx.Exec(ctx, `
		UPDATE bank_balances_manual
		SET old_opening_balance = opening_balance,
			old_closing_balance = closing_balance,
			old_balance_amount = balance_amount,
			old_total_credits = total_credits,
			opening_balance = closing_balance,
			closing_balance = $1,
			balance_amount = $1,
			total_credits = total_credits + $2
		WHERE balance_id = $3
	`, targetNewBalance, finalSweepAmount, targetBalanceID)

	if err != nil {
		return nil, fmt.Errorf("failed to update target account: %w", err)
	}

	// Create audit for target
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, NOW())
	`, targetBalanceID, fmt.Sprintf("Sweep V2 receipt (initiation: %s): %s", initiationID, sweepID), requestedBy)

	if err != nil {
		return nil, fmt.Errorf("failed to create audit for target: %w", err)
	}

	// Log execution with initiation_id
	_, err = tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.sweep_execution_log (
			initiation_id, sweep_id, amount_swept, from_account, to_account, status,
			balance_before, balance_after, error_message
		) VALUES ($1, $2, $3, $4, $5, 'SUCCESS', $6, $7, NULL)
	`, initiationID, sweepID, finalSweepAmount, sourceAccount, targetAccount, currentBalance, newBalance)

	if err != nil {
		return nil, fmt.Errorf("failed to log execution: %w", err)
	}

	// Commit
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	return map[string]interface{}{
		"initiation_id":  initiationID,
		"sweep_id":       sweepID,
		"amount_swept":   finalSweepAmount,
		"from_account":   sourceAccount,
		"to_account":     targetAccount,
		"balance_before": currentBalance,
		"balance_after":  newBalance,
	}, nil
}

// BulkManualTriggerSweepV2WithAutoApproval - Super admin endpoint for bulk sweep trigger with auto-create and auto-approve
// If sweep_id is provided, uses existing sweep (must be approved)
// If sweep_id is empty, creates new sweep config and auto-approves it
// Always creates initiation and auto-approves it for immediate worker execution
func BulkManualTriggerSweepV2WithAutoApproval(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		type SweepTriggerRequest struct {
			// If SweepID is provided, use existing approved sweep
			SweepID string `json:"sweep_id,omitempty"`

			// If SweepID is empty, these fields are required to create new sweep
			EntityName        string   `json:"entity_name,omitempty"`
			SourceBankName    string   `json:"source_bank_name,omitempty"`
			SourceBankAccount string   `json:"source_bank_account,omitempty"`
			TargetBankName    string   `json:"target_bank_name,omitempty"`
			TargetBankAccount string   `json:"target_bank_account,omitempty"`
			SweepType         string   `json:"sweep_type,omitempty"`         // ZBA, CONCENTRATION, TARGET_BALANCE
			Frequency         string   `json:"frequency,omitempty"`          // DAILY, MONTHLY, SPECIFIC_DATE
			EffectiveDate     string   `json:"effective_date,omitempty"`
			ExecutionTime     string   `json:"execution_time,omitempty"`
			BufferAmount      *float64 `json:"buffer_amount,omitempty"`
			SweepAmount       *float64 `json:"sweep_amount,omitempty"`
			RequiresInitiation *bool   `json:"requires_initiation,omitempty"`

			// Optional overrides for initiation
			OverriddenAmount        *float64 `json:"overridden_amount,omitempty"`
			OverriddenExecutionTime string   `json:"overridden_execution_time,omitempty"`
			Reason                  string   `json:"reason,omitempty"`
		}

		var req struct {
			UserID  string                 `json:"user_id"`
			Sweeps  []SweepTriggerRequest  `json:"sweeps"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			api.RespondWithResult(w, false, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}

		if req.UserID == "" {
			api.RespondWithResult(w, false, constants.ErrUserIDRequired)
			return
		}

		if len(req.Sweeps) == 0 {
			api.RespondWithResult(w, false, "sweeps array cannot be empty")
			return
		}

		// Validate session
		if ctxUID := api.GetUserIDFromCtx(ctx); ctxUID != "" && ctxUID != req.UserID {
			api.RespondWithResult(w, false, constants.ErrInvalidSession)
			return
		}

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

		// Start transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "failed to begin transaction: "+err.Error())
			return
		}
		committed := false
		defer func() {
			if !committed {
				tx.Rollback(ctx)
			}
		}()

		type ResultItem struct {
			SweepID       string `json:"sweep_id"`
			InitiationID  string `json:"initiation_id"`
			Status        string `json:"status"`
			Error         string `json:"error,omitempty"`
			IsNewSweep    bool   `json:"is_new_sweep"`
		}

		var results []ResultItem

		for i, sweep := range req.Sweeps {
			result := ResultItem{Status: "success"}

			// Determine if we need to create new sweep or use existing
			sweepID := strings.TrimSpace(sweep.SweepID)

			if sweepID == "" {
				// CREATE NEW SWEEP CONFIG + AUTO-APPROVE

				// Validate required fields for new sweep
				if strings.TrimSpace(sweep.EntityName) == "" {
					tx.Rollback(ctx)
					result.Status = "failed"
					result.Error = fmt.Sprintf("sweep[%d]: entity_name required when sweep_id is empty", i)
					results = append(results, result)
					continue
				}

				// Validate entity authorization
				if !api.IsEntityAllowed(ctx, sweep.EntityName) {
					tx.Rollback(ctx)
					result.Status = "failed"
					result.Error = fmt.Sprintf("sweep[%d]: unauthorized entity %s", i, sweep.EntityName)
					results = append(results, result)
					continue
				}

				// Validate sweep_type
				sweepTypeUpper := strings.ToUpper(strings.TrimSpace(sweep.SweepType))
				if sweepTypeUpper != "ZBA" && sweepTypeUpper != "CONCENTRATION" && sweepTypeUpper != "TARGET_BALANCE" {
					tx.Rollback(ctx)
					result.Status = "failed"
					result.Error = fmt.Sprintf("sweep[%d]: invalid sweep_type (must be ZBA, CONCENTRATION, or TARGET_BALANCE)", i)
					results = append(results, result)
					continue
				}

				// Validate frequency
				frequencyUpper := strings.ToUpper(strings.TrimSpace(sweep.Frequency))
				if frequencyUpper != "DAILY" && frequencyUpper != "MONTHLY" && frequencyUpper != "SPECIFIC_DATE" {
					tx.Rollback(ctx)
					result.Status = "failed"
					result.Error = fmt.Sprintf("sweep[%d]: invalid frequency (must be DAILY, MONTHLY, or SPECIFIC_DATE)", i)
					results = append(results, result)
					continue
				}

				// Insert sweep configuration
				insConfig := `INSERT INTO cimplrcorpsaas.sweepconfiguration (
					entity_name, 
					source_bank_name, source_bank_account, 
					target_bank_name, target_bank_account, 
					sweep_type, frequency, 
					effective_date, execution_time, 
					buffer_amount, sweep_amount, 
					requires_initiation, 
					created_at, updated_at
				) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now(),now()) RETURNING sweep_id`

			err := tx.QueryRow(ctx, insConfig,
				nullifyEmpty(sweep.EntityName),
				nullifyEmpty(sweep.SourceBankName),
				nullifyEmpty(sweep.SourceBankAccount),
				nullifyEmpty(sweep.TargetBankName),
				nullifyEmpty(sweep.TargetBankAccount),
				sweepTypeUpper,
				frequencyUpper,
				nullifyEmpty(sweep.EffectiveDate),
				nullifyEmpty(sweep.ExecutionTime),
				nullifyFloat(sweep.BufferAmount),
				nullifyFloat(sweep.SweepAmount),
				true, // Default to true (all sweeps use initiation workflow)
			).Scan(&sweepID)				if err != nil {
					tx.Rollback(ctx)
					result.Status = "failed"
					result.Error = fmt.Sprintf("sweep[%d]: failed to create config: %s", i, err.Error())
					results = append(results, result)
					continue
				}

				// AUTO-APPROVE sweep config (bypass approval workflow)
				insConfigAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepconfiguration (
					sweep_id, actiontype, processing_status, reason, 
					requested_by, requested_at, checker_by, checker_at
				) VALUES ($1, 'CREATE', 'APPROVED', $2, $3, now(), $4, now())`

				_, err = tx.Exec(ctx, insConfigAudit,
					sweepID,
					nullifyEmpty(fmt.Sprintf("Super admin bulk trigger: %s", sweep.Reason)),
					requestedBy,
					requestedBy, // Auto-approved by same user
				)

				if err != nil {
					tx.Rollback(ctx)
					result.Status = "failed"
					result.Error = fmt.Sprintf("sweep[%d]: failed to auto-approve config: %s", i, err.Error())
					results = append(results, result)
					continue
				}

				result.IsNewSweep = true

			} else {
				// USE EXISTING SWEEP - Validate it exists and is approved

				var entityName, sourceBank, sourceAccount, targetBank, targetAccount string
				err := tx.QueryRow(ctx, `
					SELECT entity_name, source_bank_name, source_bank_account, 
						   target_bank_name, target_bank_account
					FROM cimplrcorpsaas.sweepconfiguration
					WHERE sweep_id = $1 AND is_deleted = false
				`, sweepID).Scan(&entityName, &sourceBank, &sourceAccount, &targetBank, &targetAccount)

				if err != nil {
					tx.Rollback(ctx)
					result.Status = "failed"
					result.Error = fmt.Sprintf("sweep[%d]: sweep configuration not found", i)
					results = append(results, result)
					continue
				}

				// Validate entity authorization
				if strings.TrimSpace(entityName) != "" {
					if !api.IsEntityAllowed(ctx, entityName) {
						tx.Rollback(ctx)
						result.Status = "failed"
						result.Error = fmt.Sprintf("sweep[%d]: unauthorized entity", i)
						results = append(results, result)
						continue
					}
				}

				// Check if sweep is approved
				var processingStatus string
				err = tx.QueryRow(ctx, `
					SELECT processing_status
					FROM cimplrcorpsaas.auditactionsweepconfiguration
					WHERE sweep_id = $1
					ORDER BY requested_at DESC
					LIMIT 1
				`, sweepID).Scan(&processingStatus)

				if err != nil || processingStatus != "APPROVED" {
					tx.Rollback(ctx)
					result.Status = "failed"
					result.Error = fmt.Sprintf("sweep[%d]: sweep must be approved before triggering", i)
					results = append(results, result)
					continue
				}

				result.IsNewSweep = false
			}

			result.SweepID = sweepID

			// CREATE INITIATION + AUTO-APPROVE
			insInitiation := `INSERT INTO cimplrcorpsaas.sweep_initiation (
				sweep_id, initiated_by, initiation_time, 
				overridden_amount, overridden_execution_time
			) VALUES ($1, $2, now(), $3, $4) RETURNING initiation_id`

			var initiationID string
			err = tx.QueryRow(ctx, insInitiation,
				sweepID,
				requestedBy,
				nullifyFloat(sweep.OverriddenAmount),
				nullifyEmpty(sweep.OverriddenExecutionTime),
			).Scan(&initiationID)

			if err != nil {
				tx.Rollback(ctx)
				result.Status = "failed"
				result.Error = fmt.Sprintf("sweep[%d]: failed to create initiation: %s", i, err.Error())
				results = append(results, result)
				continue
			}

			// AUTO-APPROVE initiation (bypass approval workflow)
			insInitiationAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
				initiation_id, sweep_id, actiontype, processing_status, 
				requested_by, requested_at, checker_by, checker_at
			) VALUES ($1, $2, 'CREATE', 'APPROVED', $3, now(), $4, now())`

			_, err = tx.Exec(ctx, insInitiationAudit,
				initiationID,
				sweepID,
				requestedBy,
				requestedBy, // Auto-approved by same user
			)

			if err != nil {
				tx.Rollback(ctx)
				result.Status = "failed"
				result.Error = fmt.Sprintf("sweep[%d]: failed to auto-approve initiation: %s", i, err.Error())
				results = append(results, result)
				continue
			}

			// Commit this sweep's transaction
			if err := tx.Commit(ctx); err != nil {
				result.Status = "failed"
				result.Error = fmt.Sprintf("sweep[%d]: failed to commit: %s", i, err.Error())
				results = append(results, result)
				continue
			}

			result.InitiationID = initiationID
			results = append(results, result)
		}

		// Count successes and failures
		successCount := 0
		failureCount := 0
		for _, r := range results {
			if r.Status == "success" {
				successCount++
			} else {
				failureCount++
			}
		}

		api.RespondWithPayload(w, true,
			fmt.Sprintf("Processed %d sweeps: %d successful, %d failed", len(results), successCount, failureCount),
			map[string]interface{}{
				"results":       results,
				"total":         len(results),
				"successful":    successCount,
				"failed":        failureCount,
			})
	}
}
