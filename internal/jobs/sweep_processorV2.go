package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

// SweepConfigV2 holds configuration for V2 sweep processing
type SweepConfigV2 struct {
	Schedule  string
	BatchSize int
	TimeZone  string
}

// SweepDataV2 holds pre-loaded sweep data for V2 sweeps
type SweepDataV2 struct {
	sweepID            string
	entityName         string
	sourceAccount      string
	targetAccount      string
	sweepType          string
	frequency          string
	effectiveDate      sql.NullString
	executionTime      time.Time
	bufferAmount       *float64
	sweepAmount        *float64
	requiresInitiation bool
	requestedAt        time.Time
	sourceBalance      float64
	sourceClosing      float64
	targetBalance      float64
	targetClosing      float64
}

// NewDefaultSweepConfigV2 creates a new SweepConfigV2 with default values
func NewDefaultSweepConfigV2() *SweepConfigV2 {
	return &SweepConfigV2{
		Schedule:  config.DefaultSweepSchedule,
		BatchSize: config.SweepBatchSize,
		TimeZone:  config.DefaultTimeZone,
	}
}

// RunSweepSchedulerV2 starts the cron job for automated V2 sweep processing
func RunSweepSchedulerV2(cfg *SweepConfigV2, db *pgxpool.Pool) error {
	if cfg.Schedule == "" {
		cfg.Schedule = config.DefaultSweepSchedule
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = config.SweepBatchSize
	}
	if cfg.TimeZone == "" {
		cfg.TimeZone = config.DefaultTimeZone
	}

	loc, err := time.LoadLocation(cfg.TimeZone)
	if err != nil {
		loc = time.UTC
	}

	c := cron.New(cron.WithLocation(loc))

	_, err = c.AddFunc(cfg.Schedule, func() {
		err := ProcessApprovedSweepsV2(db, cfg.BatchSize)
		if err != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Sweep V2 processor failed: %v", err))
		}
	})

	if err != nil {
		return fmt.Errorf("unable to schedule sweep V2 processor: %v", err)
	}

	c.Start()
	logger.GlobalLogger.LogAudit("Sweep V2 scheduler started")

	return nil
}

// ProcessApprovedSweepsV2 fetches approved V2 sweeps and processes them based on:
// - effective_date, execution_time, frequency
// - requires_initiation flag (creates initiation records if true)
// - Executes sweeps that have pending initiations
func ProcessApprovedSweepsV2(db *pgxpool.Pool, batchSize int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	now := time.Now()
	currentHour := now.Hour()
	currentMinute := now.Minute()
	today := now.Format(constants.DateFormat)

	log.Printf("[SWEEP V2] ═══════════════════════════════════════════════════════")
	log.Printf("[SWEEP V2] Starting scheduled sweep processing at %s", now.Format("2006-01-02 15:04:05"))
	log.Printf("[SWEEP V2] Current time: %02d:%02d | Batch size: %d", currentHour, currentMinute, batchSize)
	log.Printf("[SWEEP V2] ═══════════════════════════════════════════════════════")

	// Step 1: Find approved sweeps that are due for execution
	query := `
		SELECT 
			sc.sweep_id,
			sc.entity_name,
			sc.source_bank_account,
			sc.target_bank_account,
			sc.sweep_type,
			sc.frequency,
			sc.effective_date,
			sc.execution_time,
			sc.buffer_amount,
			sc.sweep_amount,
			sc.requires_initiation,
			audit.requested_at,
			COALESCE(source_bal.closing_balance, 0) as source_balance,
			COALESCE(target_bal.closing_balance, 0) as target_balance
		FROM cimplrcorpsaas.sweepconfiguration sc
		INNER JOIN LATERAL (
			SELECT requested_at, processing_status
			FROM cimplrcorpsaas.auditactionsweepconfiguration
			WHERE sweep_id = sc.sweep_id
			ORDER BY requested_at DESC
			LIMIT 1
		) audit ON true
		LEFT JOIN bank_balances_manual source_bal ON source_bal.account_no = sc.source_bank_account
		LEFT JOIN bank_balances_manual target_bal ON target_bal.account_no = sc.target_bank_account
		WHERE sc.is_deleted = false
			AND audit.processing_status = 'APPROVED'
			AND (sc.effective_date IS NULL OR sc.effective_date <= $1::date)
		AND EXTRACT(HOUR FROM sc.execution_time) = $2
		AND EXTRACT(MINUTE FROM sc.execution_time) BETWEEN $3 AND $4
	`

	log.Printf("[SWEEP V2]  Querying for sweeps at %02d:%02d (today=%s)", currentHour, currentMinute, today)
	rows, err := db.Query(ctx, query, today, currentHour, currentMinute, currentMinute+1)
	if err != nil {
		return fmt.Errorf("unable to fetch approved V2 sweeps: %v", err)
	}
	defer rows.Close()

	// Collect sweeps for processing
	var sweeps []SweepDataV2
	for rows.Next() {
		var s SweepDataV2
		var execTimeStr string
		err := rows.Scan(
			&s.sweepID, &s.entityName, &s.sourceAccount, &s.targetAccount, &s.sweepType,
			&s.frequency, &s.effectiveDate, &execTimeStr, &s.bufferAmount, &s.sweepAmount,
			&s.requiresInitiation, &s.requestedAt, &s.sourceBalance, &s.targetBalance,
		)
		if err != nil {
			log.Printf("[SWEEP V2] Error scanning sweep row: %v", err)
			continue
		}

		// Parse execution_time
		s.executionTime, _ = time.Parse("15:04:05", execTimeStr)

		sweeps = append(sweeps, s)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if len(sweeps) == 0 {
		log.Printf("[SWEEP V2]  No approved sweeps found for execution at %s", now.Format("15:04:05"))
		log.Printf("[SWEEP V2] Query parameters: hour=%d, minute=%d-%d", currentHour, currentMinute, currentMinute+1)
		return nil
	}

	log.Printf("[SWEEP V2]  Found %d sweeps ready for processing", len(sweeps))
	for i, s := range sweeps {
		log.Printf("[SWEEP V2]   [%d] ID=%s Type=%s Freq=%s Source=%s Target=%s RequiresInitiation=%v",
			i+1, s.sweepID, s.sweepType, s.frequency, s.sourceAccount, s.targetAccount, s.requiresInitiation)
	}

	// Step 2: Process each sweep based on requires_initiation flag
	const maxConcurrent = 10
	semaphore := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	failCount := 0
	initiationCreatedCount := 0

	for _, s := range sweeps {
		wg.Add(1)
		go func(sweep SweepDataV2) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			// Frequency validation (DAILY, MONTHLY, SPECIFIC_DATE)
			shouldExecute, err := shouldExecuteSweepV2ByFrequency(sweep.frequency, sweep.effectiveDate, sweep.requestedAt, now)
			if err != nil {
				log.Printf("[SWEEP V2] %s - Error checking frequency: %v", sweep.sweepID, err)
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			if !shouldExecute {
				log.Printf("[SWEEP V2] %s - Skipped: Frequency check failed (%s)", sweep.sweepID, sweep.frequency)
				return
			}

			// ALL sweeps now require initiation -> approval -> execution workflow
			// Create initiation with auto-approval for scheduled sweeps
			err = createScheduledInitiation(ctx, db, sweep.sweepID)
			if err != nil {
				log.Printf("[SWEEP V2] %s - Failed to create scheduled initiation: %v", sweep.sweepID, err)
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}
			mu.Lock()
			initiationCreatedCount++
			mu.Unlock()
			log.Printf("[SWEEP V2] %s - Scheduled initiation created and auto-approved", sweep.sweepID)
			// Sweep will be executed by ProcessPendingInitiations when initiation status becomes INITIATED
		}(s)
	}

	wg.Wait()

	// Step 3: Process pending initiations (status='INITIATED')
	initiationsProcessed, initiationsFailed := ProcessPendingInitiations(ctx, db, batchSize)

	log.Printf("[SWEEP V2] ═══════════════════════════════════════════════════════")
	log.Printf("[SWEEP V2]  PROCESSING SUMMARY:")
	log.Printf("[SWEEP V2]    Direct Executions: %d", successCount)
	log.Printf("[SWEEP V2]    Failed: %d", failCount)
	log.Printf("[SWEEP V2]    Initiations Created: %d", initiationCreatedCount)
	log.Printf("[SWEEP V2]    Initiations Processed: %d", initiationsProcessed)
	log.Printf("[SWEEP V2]    Initiations Failed: %d", initiationsFailed)
	log.Printf("[SWEEP V2] ═══════════════════════════════════════════════════════")

	return nil
}

// shouldExecuteSweepV2ByFrequency determines if a V2 sweep should be executed based on frequency
func shouldExecuteSweepV2ByFrequency(frequency string, effectiveDate sql.NullString, requestedAt, now time.Time) (bool, error) {
	frequencyUpper := strings.ToUpper(strings.TrimSpace(frequency))

	switch frequencyUpper {
	case "DAILY":
		// Execute every day at the specified execution_time
		return true, nil

	case "WEEKLY":
		// Execute every 7th day from effective_date
		referenceDate := requestedAt
		if effectiveDate.Valid && effectiveDate.String != "" {
			parsed, err := time.Parse(constants.DateFormat, effectiveDate.String)
			if err == nil {
				referenceDate = parsed
			}
		}

		// Calculate days since reference date
		daysSince := int(now.Sub(referenceDate).Hours() / 24)
		
		// Execute if today is exactly a multiple of 7 days from reference
		if daysSince >= 0 && daysSince%7 == 0 {
			return true, nil
		}
		return false, nil

	case "MONTHLY":
		// Execute on the same day of month as effective_date (or requested_at if no effective_date)
		referenceDate := requestedAt
		if effectiveDate.Valid && effectiveDate.String != "" {
			parsed, err := time.Parse(constants.DateFormat, effectiveDate.String)
			if err == nil {
				referenceDate = parsed
			}
		}

		targetDay := getTargetDayForMonthV2(referenceDate.Day(), now.Year(), int(now.Month()))
		if now.Day() != targetDay {
			return false, nil
		}
		return true, nil

	case "SPECIFIC_DATE":
		// Execute only on the effective_date
		if !effectiveDate.Valid || effectiveDate.String == "" {
			return false, fmt.Errorf("effective_date required for SPECIFIC_DATE frequency")
		}

		effectiveDateParsed, err := time.Parse(constants.DateFormat, effectiveDate.String)
		if err != nil {
			return false, fmt.Errorf("invalid effective_date format: %w", err)
		}

		// Check if today matches effective_date
		if now.Format(constants.DateFormat) == effectiveDateParsed.Format(constants.DateFormat) {
			return true, nil
		}
		return false, nil

	default:
		return false, fmt.Errorf("unknown frequency: %s", frequency)
	}
}

// getTargetDayForMonthV2 handles month-end variations (e.g., 31st in February becomes 28/29)
func getTargetDayForMonthV2(targetDay, year, month int) int {
	firstOfNextMonth := time.Date(year, time.Month(month+1), 1, 0, 0, 0, 0, time.UTC)
	lastDayOfMonth := firstOfNextMonth.AddDate(0, 0, -1).Day()

	if targetDay > lastDayOfMonth {
		return lastDayOfMonth
	}
	return targetDay
}

// createScheduledInitiation creates a SCHEDULED initiation record for a sweep with auto-approval
func createScheduledInitiation(ctx context.Context, db *pgxpool.Pool, sweepID string) error {
	// Check if there's already a pending or approved initiation for today
	var existingCount int
	err := db.QueryRow(ctx, `
		SELECT COUNT(*) 
		FROM cimplrcorpsaas.sweep_initiation si
		JOIN cimplrcorpsaas.auditactionsweepinitiation asi ON asi.initiation_id = si.initiation_id
		WHERE si.sweep_id = $1 
		AND DATE(si.initiation_time) = CURRENT_DATE
		AND asi.processing_status IN ('PENDING_APPROVAL', 'APPROVED')
	`, sweepID).Scan(&existingCount)

	if err == nil && existingCount > 0 {
		return nil // Already has a pending/approved initiation for today, skip
	}

	// Create initiation
	ins := `INSERT INTO cimplrcorpsaas.sweep_initiation (
		sweep_id, initiated_by, initiation_time
	) VALUES ($1, 'sweep_system', now()) RETURNING initiation_id`

	var initiationID string
	err = db.QueryRow(ctx, ins, sweepID).Scan(&initiationID)
	if err != nil {
		return err
	}

	// Auto-approve the scheduled initiation
	insAudit := `INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
		initiation_id, sweep_id, actiontype, processing_status, 
		requested_by, requested_at, checker_by, checker_at
	) VALUES ($1, $2, 'CREATE', 'APPROVED', 'sweep_system', now(), 'sweep_system', now())`

	_, err = db.Exec(ctx, insAudit, initiationID, sweepID)
	return err
}

// ProcessPendingInitiations processes all APPROVED initiations
func ProcessPendingInitiations(ctx context.Context, db *pgxpool.Pool, batchSize int) (int, int) {
	query := `
		SELECT 
			i.initiation_id,
			i.sweep_id,
			i.overridden_amount,
			i.overridden_execution_time,
			i.overridden_source_bank_account,
			i.overridden_target_bank_account,
			sc.source_bank_account,
			sc.target_bank_account,
			sc.sweep_type,
			sc.buffer_amount,
			sc.sweep_amount
		FROM cimplrcorpsaas.sweep_initiation i
		JOIN cimplrcorpsaas.sweepconfiguration sc ON sc.sweep_id = i.sweep_id
		JOIN cimplrcorpsaas.auditactionsweepinitiation asi ON asi.initiation_id = i.initiation_id
		WHERE asi.processing_status = 'APPROVED'
		AND sc.is_deleted = false
		ORDER BY i.initiation_time ASC
	`

	log.Printf("[SWEEP V2]  Checking for approved initiations (processing_status=APPROVED)...")
	rows, err := db.Query(ctx, query)
	if err != nil {
		log.Printf("[SWEEP V2] Failed to fetch approved initiations: %v", err)
		return 0, 0
	}
	defer rows.Close()

	successCount := 0
	failCount := 0
	initiationCount := 0

	for rows.Next() {
		initiationCount++
		var initiationID, sweepID, configSourceAccount, configTargetAccount, sweepType string
		var overriddenAmount, bufferAmount, sweepAmount *float64
		var overriddenExecutionTime, overriddenSourceAccount, overriddenTargetAccount *string

		err := rows.Scan(&initiationID, &sweepID, &overriddenAmount, &overriddenExecutionTime,
			&overriddenSourceAccount, &overriddenTargetAccount,
			&configSourceAccount, &configTargetAccount, &sweepType, &bufferAmount, &sweepAmount)
		if err != nil {
			log.Printf("[SWEEP V2] Failed to scan initiation row: %v", err)
			continue
		}

		log.Printf("[SWEEP V2] 📝 Processing initiation [%d]: %s (sweep: %s)", initiationCount, initiationID, sweepID)

		// Determine final source and target accounts
		finalSourceAccount := configSourceAccount
		if overriddenSourceAccount != nil && *overriddenSourceAccount != "" {
			finalSourceAccount = *overriddenSourceAccount
		}

		finalTargetAccount := configTargetAccount
		if overriddenTargetAccount != nil && *overriddenTargetAccount != "" {
			finalTargetAccount = *overriddenTargetAccount
		}

		// Execute the sweep
		err = executeSweepWithInitiation(ctx, db, sweepID, initiationID, finalSourceAccount, finalTargetAccount,
			sweepType, bufferAmount, sweepAmount, overriddenAmount)

		if err != nil {
			log.Printf("[SWEEP V2] %s (initiation: %s) - Execution failed: %v", sweepID, initiationID, err)
			failCount++
		} else {
			log.Printf("[SWEEP V2] %s (initiation: %s) - Execution successful", sweepID, initiationID)
			successCount++
		}
	}

	if initiationCount > 0 {
		log.Printf("[SWEEP V2] Processed %d initiations: %d succeeded, %d failed", initiationCount, successCount, failCount)
	}

	return successCount, failCount
}

// ExecuteSweepV2Direct executes a V2 sweep directly (without initiation record)
func ExecuteSweepV2Direct(ctx context.Context, db *pgxpool.Pool, sweep SweepDataV2) error {
	currentBalance := sweep.sourceBalance

	// Get buffer amount
	buffer := 0.0
	if sweep.bufferAmount != nil {
		buffer = *sweep.bufferAmount
	}

	// BR-1.1: Buffer Violation Check - CRITICAL HARD RULE
	if currentBalance <= buffer {
		errMsg := fmt.Sprintf("BLOCKED: Critical Buffer Violation - Current Balance (%.2f) is at or below Buffer (%.2f)", currentBalance, buffer)
		logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	// Calculate sweep amount based on sweep type
	var sweepAmountFinal float64
	sweepTypeUpper := strings.ToUpper(strings.TrimSpace(sweep.sweepType))

	switch sweepTypeUpper {
	case "ZBA":
		// ZBA: Zero Balance Account - Sweep EVERYTHING to leave source at ZERO
		// Only execute if current balance exceeds buffer threshold
		if currentBalance <= buffer {
			errMsg := fmt.Sprintf("BLOCKED: ZBA sweep - Balance (%.2f) has not exceeded buffer threshold (%.2f)", currentBalance, buffer)
			logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
			return fmt.Errorf(errMsg)
		}
		// Sweep entire balance to make source account zero
		sweepAmountFinal = currentBalance

	case "CONCENTRATION":
		// CONCENTRATION: Fixed amount sweep - only sweep if balance exceeds buffer
		if sweep.sweepAmount == nil || *sweep.sweepAmount <= 0 {
			errMsg := fmt.Sprintf("BLOCKED: CONCENTRATION sweep requires fixed sweep_amount to be configured")
			logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
			return fmt.Errorf(errMsg)
		}

		// Only sweep if current balance exceeds buffer
		if currentBalance <= buffer {
			errMsg := fmt.Sprintf("BLOCKED: CONCENTRATION sweep - Balance (%.2f) has not exceeded buffer threshold (%.2f)", currentBalance, buffer)
			logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
			return fmt.Errorf(errMsg)
		}

		sweepAmountFinal = *sweep.sweepAmount

		// Maintain buffer - ensure sweep doesn't violate buffer requirement
		if (currentBalance - sweepAmountFinal) < buffer {
			errMsg := fmt.Sprintf("BLOCKED: CONCENTRATION sweep - Sweeping %.2f would leave balance below buffer (%.2f)", sweepAmountFinal, buffer)
			logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
			return fmt.Errorf(errMsg)
		}

	case "TARGET_BALANCE":
		// TARGET_BALANCE: Sweep excess to maintain target balance (buffer) in source
		if currentBalance <= buffer {
			errMsg := fmt.Sprintf("BLOCKED: TARGET_BALANCE sweep - Balance (%.2f) is at or below target (%.2f), no excess to sweep", currentBalance, buffer)
			logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
			return fmt.Errorf(errMsg)
		}
		// Sweep only the excess above buffer, leaving buffer amount in source
		sweepAmountFinal = currentBalance - buffer

	default:
		return fmt.Errorf("unknown sweep type: %s", sweep.sweepType)
	}

	// Additional validation: Ensure final sweep amount is positive
	if sweepAmountFinal <= 0 {
		errMsg := fmt.Sprintf("BLOCKED: Calculated sweep amount (%.2f) is invalid", sweepAmountFinal)
		logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	// Calculate new balance after sweep
	newBalance := currentBalance - sweepAmountFinal

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		errMsg := fmt.Sprintf("SYSTEM ERROR: Unable to start sweep transaction: %v", err)
		logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
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
	`, newBalance, sweepAmountFinal, sweep.sourceAccount)

	if err != nil {
		errMsg := fmt.Sprintf("SYSTEM ERROR: Failed to update source account balance: %v", err)
		logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	// Create audit for source
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		)
		SELECT balance_id, 'EDIT', 'PENDING_EDIT_APPROVAL', $1, 'sweep_system_v2', NOW()
		FROM bank_balances_manual
		WHERE account_no = $2
		LIMIT 1
	`, fmt.Sprintf("Auto sweep V2 execution: %s", sweep.sweepID), sweep.sourceAccount)

	if err != nil {
		return fmt.Errorf(constants.ErrAuditInsertFailedUser)
	}

	// Update target account
	targetNewBalance := sweep.targetBalance + sweepAmountFinal

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
	`, targetNewBalance, sweepAmountFinal, sweep.targetAccount)

	if err != nil {
		errMsg := fmt.Sprintf("SYSTEM ERROR: Failed to update target account balance: %v", err)
		logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	// Create audit for target
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		)
		SELECT balance_id, 'EDIT', 'PENDING_EDIT_APPROVAL', $1, 'sweep_system_v2', NOW()
		FROM bank_balances_manual
		WHERE account_no = $2
		LIMIT 1
	`, fmt.Sprintf("Auto sweep V2 receipt: %s", sweep.sweepID), sweep.targetAccount)

	if err != nil {
		return fmt.Errorf(constants.ErrAuditInsertFailedUser)
	}

	// Log sweep execution (no initiation_id for direct sweeps)
	_, err = tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.sweep_execution_log (
			sweep_id, amount_swept, from_account, to_account, status, 
			balance_before, balance_after, error_message
		) VALUES ($1, $2, $3, $4, 'SUCCESS', $5, $6, NULL)
	`, sweep.sweepID, sweepAmountFinal, sweep.sourceAccount, sweep.targetAccount, currentBalance, newBalance)

	if err != nil {
		errMsg := fmt.Sprintf("SYSTEM ERROR: Failed to log sweep execution: %v", err)
		logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	if err := tx.Commit(ctx); err != nil {
		errMsg := fmt.Sprintf("SYSTEM ERROR: Failed to commit sweep transaction: %v", err)
		logSweepFailure(ctx, db, sweep.sweepID, "", sweep.sourceAccount, sweep.targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	log.Printf("[SWEEP V2] %s  SUCCESS | Type: %s | Amount: %.2f | From: %s (%.2f → %.2f) | To: %s (%.2f → %.2f) | Buffer: %.2f",
		sweep.sweepID, sweep.sweepType, sweepAmountFinal, sweep.sourceAccount, currentBalance, newBalance,
		sweep.targetAccount, sweep.targetBalance, targetNewBalance, buffer)

	return nil
}

// logSweepFailure logs failed sweep attempts with detailed validation information
func logSweepFailure(ctx context.Context, db *pgxpool.Pool, sweepID, initiationID, fromAccount, toAccount, reason string, balanceBefore float64) {
	query := `
		INSERT INTO cimplrcorpsaas.sweep_execution_log (
			sweep_id, initiation_id, from_account, to_account, status, 
			balance_before, balance_after, amount_swept, error_message
		) VALUES ($1, NULLIF($2, ''), $3, $4, 'FAILED', $5, $5, 0, $6)
	`
	_, err := db.Exec(ctx, query, sweepID, initiationID, fromAccount, toAccount, balanceBefore, reason)
	if err != nil {
		log.Printf("[SWEEP V2] Failed to log sweep failure: %v", err)
	}
	log.Printf("[SWEEP V2] %s  FAILED | Reason: %s", sweepID, reason)
}

// executeSweepWithInitiation executes a sweep with initiation context (similar to executeSweepV2WithInitiation in sweepExecutorV2.go)
func executeSweepWithInitiation(ctx context.Context, db *pgxpool.Pool, sweepID, initiationID,
	sourceAccount, targetAccount, sweepType string, bufferAmount, sweepAmount, overriddenAmount *float64) error {

	// Get current balance for source account
	var balanceID string
	var currentBalance float64

	err := db.QueryRow(ctx, `
		SELECT balance_id, COALESCE(closing_balance, 0)
		FROM bank_balances_manual
		WHERE account_no = $1
		ORDER BY as_of_date DESC, as_of_time DESC
		LIMIT 1
	`, sourceAccount).Scan(&balanceID, &currentBalance)

	if err != nil {
		return fmt.Errorf("failed to fetch source account balance: %w", err)
	}

	// Get buffer amount
	buffer := 0.0
	if bufferAmount != nil {
		buffer = *bufferAmount
	}

	// BR-1.1: Buffer Violation Check
	if currentBalance <= buffer {
		errMsg := fmt.Sprintf("BLOCKED: Buffer Violation - Balance (%.2f) <= Buffer (%.2f)", currentBalance, buffer)
		logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	// Calculate sweep amount
	var finalSweepAmount float64

	// If overridden_amount is provided, validate and use it
	if overriddenAmount != nil && *overriddenAmount > 0 {
		finalSweepAmount = *overriddenAmount

		// For ZBA with override, allow sweeping to zero
		sweepTypeUpper := strings.ToUpper(strings.TrimSpace(sweepType))
		if sweepTypeUpper != "ZBA" {
			// For CONCENTRATION and TARGET_BALANCE, validate override doesn't violate buffer
			if (currentBalance - finalSweepAmount) < buffer {
				errMsg := fmt.Sprintf("BLOCKED: Override amount (%.2f) would leave balance below buffer (%.2f)", finalSweepAmount, buffer)
				logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
				return fmt.Errorf(errMsg)
			}
		}
	} else {
		// Use configured sweep logic
		sweepTypeUpper := strings.ToUpper(strings.TrimSpace(sweepType))

		switch sweepTypeUpper {
		case "ZBA":
			// ZBA: Zero Balance Account - Sweep EVERYTHING to leave source at ZERO
			// Only execute if current balance exceeds buffer threshold
			if currentBalance <= buffer {
				errMsg := fmt.Sprintf("BLOCKED: ZBA sweep - Balance (%.2f) has not exceeded buffer threshold (%.2f)", currentBalance, buffer)
				logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
				return fmt.Errorf(errMsg)
			}
			// Sweep entire balance to make source account zero
			finalSweepAmount = currentBalance

		case "CONCENTRATION":
			// CONCENTRATION: Fixed amount sweep - only sweep if balance exceeds buffer
			if sweepAmount == nil || *sweepAmount <= 0 {
				errMsg := "BLOCKED: CONCENTRATION requires fixed sweep_amount"
				logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
				return fmt.Errorf(errMsg)
			}

			// Only sweep if current balance exceeds buffer
			if currentBalance <= buffer {
				errMsg := fmt.Sprintf("BLOCKED: CONCENTRATION sweep - Balance (%.2f) has not exceeded buffer threshold (%.2f)", currentBalance, buffer)
				logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
				return fmt.Errorf(errMsg)
			}

			finalSweepAmount = *sweepAmount

			// Maintain buffer - ensure sweep doesn't violate buffer requirement
			if (currentBalance - finalSweepAmount) < buffer {
				errMsg := fmt.Sprintf("BLOCKED: CONCENTRATION sweep - Sweeping %.2f would leave balance below buffer (%.2f)", finalSweepAmount, buffer)
				logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
				return fmt.Errorf(errMsg)
			}

		case "TARGET_BALANCE":
			// TARGET_BALANCE: Sweep excess to maintain target balance (buffer) in source
			if currentBalance <= buffer {
				errMsg := fmt.Sprintf("BLOCKED: TARGET_BALANCE sweep - Balance (%.2f) is at or below target (%.2f), no excess to sweep", currentBalance, buffer)
				logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
				return fmt.Errorf(errMsg)
			}
			// Sweep only the excess above buffer, leaving buffer amount in source
			finalSweepAmount = currentBalance - buffer

		default:
			return fmt.Errorf("unknown sweep type: %s", sweepType)
		}
	}

	if finalSweepAmount <= 0 {
		errMsg := fmt.Sprintf("BLOCKED: No funds available to sweep (Amount: %.2f)", finalSweepAmount)
		logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	// Calculate new balance after sweep
	newBalance := currentBalance - finalSweepAmount

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		errMsg := fmt.Sprintf("SYSTEM ERROR: Failed to begin transaction: %v", err)
		logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
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
		WHERE balance_id = $3
	`, newBalance, finalSweepAmount, balanceID)

	if err != nil {
		return fmt.Errorf("failed to update source account: %w", err)
	}

	// Create audit for source
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, 'sweep_system_v2', NOW())
	`, balanceID, fmt.Sprintf("Sweep V2 execution (initiation: %s): %s", initiationID, sweepID))

	if err != nil {
		return fmt.Errorf("failed to create audit for source: %w", err)
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
		return fmt.Errorf("target account not found: %w", err)
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
		return fmt.Errorf("failed to update target account: %w", err)
	}

	// Create audit for target
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, 'sweep_system_v2', NOW())
	`, targetBalanceID, fmt.Sprintf("Sweep V2 receipt (initiation: %s): %s", initiationID, sweepID))

	if err != nil {
		return fmt.Errorf("failed to create audit for target: %w", err)
	}

	// Log execution with initiation_id
	_, err = tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.sweep_execution_log (
			initiation_id, sweep_id, amount_swept, from_account, to_account, status,
			balance_before, balance_after, error_message
		) VALUES ($1, $2, $3, $4, $5, 'SUCCESS', $6, $7, NULL)
	`, initiationID, sweepID, finalSweepAmount, sourceAccount, targetAccount, currentBalance, newBalance)

	if err != nil {
		errMsg := fmt.Sprintf("SYSTEM ERROR: Failed to log execution: %v", err)
		logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	// Commit
	if err := tx.Commit(ctx); err != nil {
		errMsg := fmt.Sprintf("SYSTEM ERROR: Failed to commit: %v", err)
		logSweepFailure(ctx, db, sweepID, initiationID, sourceAccount, targetAccount, errMsg, currentBalance)
		return fmt.Errorf(errMsg)
	}

	log.Printf("[SWEEP V2] %s SUCCESS (Initiation: %s) | Type: %s | Amount: %.2f | Buffer: %.2f",
		sweepID, initiationID, sweepType, finalSweepAmount, buffer)

	return nil
}
