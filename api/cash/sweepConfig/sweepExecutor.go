package sweepconfig

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/auth"
	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GetSweepExecutionLogs returns the execution history for sweep configurations
func GetSweepExecutionLogs(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req struct {
			UserID  string `json:"user_id"`
			SweepID string `json:"sweep_id,omitempty"`
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

		// Build query (scoped by allowed entities/banks when context is present)
		entityNames := api.GetEntityNamesFromCtx(ctx)
		bankNames := api.GetBankNamesFromCtx(ctx)
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}
		normBanks := make([]string, 0, len(bankNames))
		for _, n := range bankNames {
			if s := strings.TrimSpace(n); s != "" {
				normBanks = append(normBanks, strings.ToLower(s))
			}
		}

		query := `
			SELECT 
				l.execution_id,
				l.sweep_id,
				l.execution_date,
				l.amount_swept,
				l.from_account,
				l.to_account,
				l.status,
				l.error_message,
				l.balance_before,
				l.balance_after
			FROM sweep_execution_log l
			JOIN mastersweepconfiguration c ON c.sweep_id = l.sweep_id
			WHERE COALESCE(c.is_deleted, false) = false
		`

		args := []interface{}{}
		argPos := 1

		if len(normEntities) > 0 {
			query += fmt.Sprintf(constants.QuerryEntityNameLower, argPos)
			args = append(args, normEntities)
			argPos++
		}
		if len(normBanks) > 0 {
			query += fmt.Sprintf(constants.QuerryBankNameLower, argPos)
			args = append(args, normBanks)
			argPos++
		}
		if req.SweepID != "" {
			query += fmt.Sprintf(constants.QuerrySweepID, argPos)
			args = append(args, req.SweepID)
			argPos++
		}

		query += " ORDER BY execution_date DESC"
		query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argPos, argPos+1)
		args = append(args, req.Limit, req.Offset)

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithResult(w, false, constants.ErrDBPrefix+err.Error())
			return
		}
		defer rows.Close()

		logs := make([]map[string]interface{}, 0)
		for rows.Next() {
			var executionID, sweepID, fromAccount, toAccount, status string
			var errorMessage *string
			var executionDate time.Time
			var amountSwept, balanceBefore, balanceAfter float64

			err := rows.Scan(
				&executionID, &sweepID, &executionDate, &amountSwept,
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

		// Get total count (same scoping)
		var totalCount int
		countQuery := `
			SELECT COUNT(*)
			FROM sweep_execution_log l
			JOIN mastersweepconfiguration c ON c.sweep_id = l.sweep_id
			WHERE COALESCE(c.is_deleted, false) = false
		`
		countArgs := []interface{}{}
		countPos := 1
		if len(normEntities) > 0 {
			countQuery += fmt.Sprintf(constants.QuerryEntityNameLower, countPos)
			countArgs = append(countArgs, normEntities)
			countPos++
		}
		if len(normBanks) > 0 {
			countQuery += fmt.Sprintf(constants.QuerryBankNameLower, countPos)
			countArgs = append(countArgs, normBanks)
			countPos++
		}

		if req.SweepID != "" {
			countQuery += fmt.Sprintf(constants.QuerrySweepID, countPos)
			countArgs = append(countArgs, req.SweepID)
			countPos++
		}

		err = pgxPool.QueryRow(ctx, countQuery, countArgs...).Scan(&totalCount)
		if err != nil {
			totalCount = 0
		}

		api.RespondWithPayload(w, true, "", map[string]interface{}{
			"logs":   logs,
			"total":  totalCount,
			"limit":  req.Limit,
			"offset": req.Offset,
		})
	}
}

// GetSweepStatistics returns statistics about sweep executions
func GetSweepStatistics(pgxPool *pgxpool.Pool) http.HandlerFunc {
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
		bankNames := api.GetBankNamesFromCtx(ctx)
		normEntities := make([]string, 0, len(entityNames))
		for _, n := range entityNames {
			if s := strings.TrimSpace(n); s != "" {
				normEntities = append(normEntities, strings.ToLower(s))
			}
		}
		normBanks := make([]string, 0, len(bankNames))
		for _, n := range bankNames {
			if s := strings.TrimSpace(n); s != "" {
				normBanks = append(normBanks, strings.ToLower(s))
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
			FROM sweep_execution_log l
			JOIN mastersweepconfiguration c ON c.sweep_id = l.sweep_id
			WHERE COALESCE(c.is_deleted, false) = false
		`

		args := []interface{}{}
		argPos := 1

		if len(normEntities) > 0 {
			query += fmt.Sprintf(constants.QuerryEntityNameLower, argPos)
			args = append(args, normEntities)
			argPos++
		}
		if len(normBanks) > 0 {
			query += fmt.Sprintf(constants.QuerryBankNameLower, argPos)
			args = append(args, normBanks)
			argPos++
		}

		if req.SweepID != "" {
			query += fmt.Sprintf(constants.QuerrySweepID, argPos)
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

// ManualTriggerSweep allows manual triggering of a specific sweep
// IMPORTANT: Manual trigger executes IMMEDIATELY regardless of:
//   - cutoff_time (ignores scheduled time)
//   - frequency (ignores Daily/Weekly/Monthly schedule)
//   - last_execution (can run multiple times)
//
// Only requirement: Sweep must be APPROVED
//
// Example: If sweep is configured for Wednesday 5pm but you run manual trigger
// on Monday 6pm, it will execute immediately at Monday 6pm.
func ManualTriggerSweep(pgxPool *pgxpool.Pool) http.HandlerFunc {
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

		// Fetch sweep configuration (no cutoff_time or frequency check needed for manual)
		var entityName, bankName, bankAccount, sweepType, parentAccount string
		var bufferAmount *float64
		var activeStatus string

		err := pgxPool.QueryRow(ctx, `
			SELECT entity_name, bank_name, bank_account, sweep_type, parent_account, buffer_amount, active_status
			FROM mastersweepconfiguration
			WHERE sweep_id = $1 AND is_deleted = false
		`, req.SweepID).Scan(&entityName, &bankName, &bankAccount, &sweepType, &parentAccount, &bufferAmount, &activeStatus)

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
		if strings.TrimSpace(bankName) != "" {
			if !api.IsBankAllowed(ctx, bankName) {
				api.RespondWithResult(w, false, "unauthorized bank")
				return
			}
		}
		if strings.TrimSpace(bankAccount) != "" {
			if !ctxHasApprovedBankAccountFor(ctx, bankAccount, bankName, entityName) {
				api.RespondWithResult(w, false, "unauthorized bank account")
				return
			}
		}
		if strings.TrimSpace(parentAccount) != "" {
			// parent must be an approved account and belong to the same entity (when entity is known)
			if !ctxHasApprovedBankAccountFor(ctx, parentAccount, "", entityName) {
				api.RespondWithResult(w, false, "unauthorized parent account")
				return
			}
		}

		// Check if approved (ONLY requirement for manual trigger)
		var processingStatus string
		err = pgxPool.QueryRow(ctx, `
			SELECT processing_status
			FROM auditactionsweepconfiguration
			WHERE sweep_id = $1
			ORDER BY requested_at DESC
			LIMIT 1
		`, req.SweepID).Scan(&processingStatus)

		if err != nil || processingStatus != "APPROVED" {
			api.RespondWithResult(w, false, "Sweep must be approved before manual execution")
			return
		}

		// Manual execution logic (bypasses cutoff_time and frequency checks)

		// Get current balance
		var balanceID string
		var currentBalance float64

		err = pgxPool.QueryRow(ctx, `
			SELECT balance_id, COALESCE(closing_balance, 0)
			FROM bank_balances_manual
			WHERE account_no = $1
			ORDER BY as_of_date DESC, as_of_time DESC
			LIMIT 1
		`, bankAccount).Scan(&balanceID, &currentBalance)

		if err != nil {
			api.RespondWithResult(w, false, "Failed to fetch current balance: "+err.Error())
			return
		}

		// Calculate sweep amount
		var sweepAmount float64
		buffer := 0.0
		if bufferAmount != nil {
			buffer = *bufferAmount
		}

		// Normalize sweep type (case-insensitive, handle hyphens and spaces)
		sweepTypeNormalized := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(sweepType), "-", " "))

		switch sweepTypeNormalized {
		case "concentration":
			if currentBalance > buffer {
				sweepAmount = currentBalance - buffer
			} else {
				api.RespondWithResult(w, false, fmt.Sprintf("Current balance (%.2f) is below buffer (%.2f). No sweep needed.", currentBalance, buffer))
				return
			}

		case "zero balance":
			if currentBalance >= buffer {
				sweepAmount = currentBalance
			} else {
				api.RespondWithResult(w, false, fmt.Sprintf("Current balance (%.2f) has not reached buffer (%.2f). No sweep needed.", currentBalance, buffer))
				return
			}

		case "target balance":
			if currentBalance > buffer {
				sweepAmount = currentBalance - buffer
			} else {
				api.RespondWithResult(w, false, fmt.Sprintf("Current balance (%.2f) is at or below target (%.2f). No sweep needed.", currentBalance, buffer))
				return
			}

		case "standalone":
			if currentBalance > buffer {
				sweepAmount = currentBalance - buffer
			} else {
				api.RespondWithResult(w, false, "No amount to sweep - balance at or below buffer")
				return
			}

		default:
			api.RespondWithResult(w, false, "Unknown sweep type: "+sweepType)
			return
		}

		if sweepAmount <= 0 {
			api.RespondWithResult(w, false, "No amount to sweep")
			return
		}

		// Begin transaction
		tx, err := pgxPool.Begin(ctx)
		if err != nil {
			api.RespondWithResult(w, false, "Failed to begin transaction: "+err.Error())
			return
		}
		defer tx.Rollback(ctx)

		// Update source account
		newBalance := currentBalance - sweepAmount

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
		`, newBalance, sweepAmount, balanceID)

		if err != nil {
			api.RespondWithResult(w, false, "Failed to update source account: "+err.Error())
			return
		}

		// Create audit for source
		_, err = tx.Exec(ctx, `
			INSERT INTO auditactionbankbalances (
				balance_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, NOW())
		`, balanceID, fmt.Sprintf("Manual sweep execution: %s", req.SweepID), requestedBy)

		if err != nil {
			api.RespondWithResult(w, false, "Failed to create audit for source: "+err.Error())
			return
		}

		// Get parent account
		var parentBalanceID string
		var parentBalance float64

		err = tx.QueryRow(ctx, `
			SELECT balance_id, COALESCE(closing_balance, 0)
			FROM bank_balances_manual
			WHERE account_no = $1
			ORDER BY as_of_date DESC, as_of_time DESC
			LIMIT 1
		`, parentAccount).Scan(&parentBalanceID, &parentBalance)

		if err != nil {
			api.RespondWithResult(w, false, fmt.Sprintf("Parent account '%s' not found in bank_balances_manual. Please ensure the parent account exists before executing sweep.", parentAccount))
			return
		}

		// Update parent account
		parentNewBalance := parentBalance + sweepAmount

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
		`, parentNewBalance, sweepAmount, parentBalanceID)

		if err != nil {
			api.RespondWithResult(w, false, "Failed to update parent account: "+err.Error())
			return
		}

		// Create audit for parent
		_, err = tx.Exec(ctx, `
			INSERT INTO auditactionbankbalances (
				balance_id, actiontype, processing_status, reason, requested_by, requested_at
			) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, $3, NOW())
		`, parentBalanceID, fmt.Sprintf("Manual sweep receipt: %s", req.SweepID), requestedBy)

		if err != nil {
			api.RespondWithResult(w, false, "Failed to create audit for parent: "+err.Error())
			return
		}

		// Log execution
		_, err = tx.Exec(ctx, `
			INSERT INTO sweep_execution_log (
				sweep_id, amount_swept, from_account, to_account, status,
				balance_before, balance_after
			) VALUES ($1, $2, $3, $4, 'SUCCESS', $5, $6)
		`, req.SweepID, sweepAmount, bankAccount, parentAccount, currentBalance, newBalance)

		if err != nil {
			api.RespondWithResult(w, false, "Failed to log execution: "+err.Error())
			return
		}

		// Commit
		if err := tx.Commit(ctx); err != nil {
			api.RespondWithResult(w, false, "Failed to commit: "+err.Error())
			return
		}

		api.RespondWithPayload(w, true, "Sweep executed successfully", map[string]interface{}{
			"sweep_id":       req.SweepID,
			"amount_swept":   sweepAmount,
			"from_account":   bankAccount,
			"to_account":     parentAccount,
			"balance_before": currentBalance,
			"balance_after":  newBalance,
		})
	}
}
