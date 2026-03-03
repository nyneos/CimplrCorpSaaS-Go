package jobs

import (
	"context"
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

// SweepConfig holds configuration for sweep processing
type SweepConfig struct {
	Schedule  string
	BatchSize int
	TimeZone  string
}

// SweepData holds pre-loaded sweep and balance data for optimized processing
type SweepData struct {
	sweepID          string
	entityName       string
	bankName         string
	bankAccount      string
	sweepType        string
	parentAccount    string
	bufferAmount     *float64
	frequency        string
	cutoffTime       time.Time
	requestedAt      time.Time
	childBalance     float64
	childClosing     float64
	childOpening     float64
	childOldBal      float64
	childOldClosing  float64
	childOldOpening  float64
	childCredits     float64
	childDebits      float64
	childOldCredits  float64
	childOldDebits   float64
	parentBalance    float64
	parentClosing    float64
	parentOpening    float64
	parentOldBal     float64
	parentOldClosing float64
	parentOldOpening float64
	parentCredits    float64
	parentDebits     float64
	parentOldCredits float64
	parentOldDebits  float64
}

// Parameter structs to avoid long parameter lists
type ExecuteSweepParams struct {
	SweepID       string
	EntityName    string
	BankAccount   string
	SweepType     string
	ParentAccount string
	BufferAmount  *float64
}

type ReverseSweepParams struct {
	SweepID        string
	ChildAccount   string
	ParentAccount  string
	AmountNeeded   float64
	CurrentBalance float64
	ChildBalanceID string
}

type SweepLogEntry struct {
	SweepID       string
	FromAccount   string
	ToAccount     string
	AmountSwept   float64
	Status        string
	ErrorMessage  string
	BalanceBefore float64
	BalanceAfter  float64
}

// NewDefaultSweepConfig creates a new SweepConfig with default values
func NewDefaultSweepConfig() *SweepConfig {
	return &SweepConfig{
		Schedule:  config.DefaultSweepSchedule,
		BatchSize: config.SweepBatchSize,
		TimeZone:  config.DefaultTimeZone,
	}
}

// RunSweepScheduler starts the cron job for automated sweep processing
func RunSweepScheduler(cfg *SweepConfig, db *pgxpool.Pool) error {
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
		err := ProcessApprovedSweeps(db, cfg.BatchSize)
		if err != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Sweep processor failed: %v", err))
		}
	})

	if err != nil {
		return fmt.Errorf("unable to schedule sweep processor: %v", err)
	}

	c.Start()
	logger.GlobalLogger.LogAudit("Sweep scheduler started")

	return nil
}

// ProcessApprovedSweeps fetches approved sweeps and executes them based on cutoff time and frequency
func ProcessApprovedSweeps(db *pgxpool.Pool, batchSize int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	now := time.Now()
	currentHour := now.Hour()
	currentMinute := now.Minute()
	today := now.Format(constants.DateFormat)

	// OPTIMIZATION 1-3: Time-window filtering, execution log filtering, and balance JOIN in single optimized query
	query := `
		SELECT 
			sc.sweep_id,
			sc.entity_name,
			sc.bank_name,
			sc.bank_account,
			sc.sweep_type,
			sc.parent_account,
			sc.buffer_amount,
			sc.frequency,
			sc.cutoff_time,
			audit.requested_at,
			COALESCE(child_bal.balance_amount, 0) as child_balance,
			COALESCE(child_bal.closing_balance, 0) as child_closing_balance,
			COALESCE(child_bal.opening_balance, 0) as child_opening_balance,
			COALESCE(child_bal.old_balance_amount, 0) as child_old_balance_amount,
			COALESCE(child_bal.old_closing_balance, 0) as child_old_closing_balance,
			COALESCE(child_bal.old_opening_balance, 0) as child_old_opening_balance,
			COALESCE(child_bal.total_credits, 0) as child_total_credits,
			COALESCE(child_bal.total_debits, 0) as child_total_debits,
			COALESCE(child_bal.old_total_credits, 0) as child_old_total_credits,
			COALESCE(child_bal.old_total_debits, 0) as child_old_total_debits,
			COALESCE(parent_bal.balance_amount, 0) as parent_balance,
			COALESCE(parent_bal.closing_balance, 0) as parent_closing_balance,
			COALESCE(parent_bal.opening_balance, 0) as parent_opening_balance,
			COALESCE(parent_bal.old_balance_amount, 0) as parent_old_balance_amount,
			COALESCE(parent_bal.old_closing_balance, 0) as parent_old_closing_balance,
			COALESCE(parent_bal.old_opening_balance, 0) as parent_old_opening_balance,
			COALESCE(parent_bal.total_credits, 0) as parent_total_credits,
			COALESCE(parent_bal.total_debits, 0) as parent_total_debits,
			COALESCE(parent_bal.old_total_credits, 0) as parent_old_total_credits,
			COALESCE(parent_bal.old_total_debits, 0) as parent_old_total_debits
		FROM mastersweepconfiguration sc
		INNER JOIN LATERAL (
			SELECT requested_at, processing_status
			FROM auditactionsweepconfiguration
			WHERE sweep_id = sc.sweep_id
			ORDER BY requested_at DESC
			LIMIT 1
		) audit ON true
		LEFT JOIN bank_balances_manual child_bal ON child_bal.account_no = sc.bank_account
		LEFT JOIN bank_balances_manual parent_bal ON parent_bal.account_no = sc.parent_account
		LEFT JOIN LATERAL (
			SELECT execution_date, status
			FROM sweep_execution_log
			WHERE sweep_id = sc.sweep_id AND status = 'SUCCESS'
			ORDER BY execution_date DESC
			LIMIT 1
		) last_exec ON true
		WHERE LOWER(sc.auto_sweep) = 'yes'
			AND sc.active_status = 'Active'
			AND sc.is_deleted = false
			AND audit.processing_status = 'APPROVED'
			AND EXTRACT(HOUR FROM sc.cutoff_time) = $1
			AND EXTRACT(MINUTE FROM sc.cutoff_time) BETWEEN $2 AND $3
			AND (
				last_exec.execution_date IS NULL 
				OR DATE(last_exec.execution_date) < $4::date
			)
		LIMIT $5
	`

	rows, err := db.Query(ctx, query, currentHour, currentMinute, currentMinute+1, today, batchSize)
	if err != nil {
		return fmt.Errorf("unable to fetch approved sweeps: %v", err)
	}
	defer rows.Close()

	// Collect sweeps for parallel processing
	var sweeps []SweepData
	for rows.Next() {
		var s SweepData
		err := rows.Scan(
			&s.sweepID, &s.entityName, &s.bankName, &s.bankAccount, &s.sweepType,
			&s.parentAccount, &s.bufferAmount, &s.frequency, &s.cutoffTime, &s.requestedAt,
			&s.childBalance, &s.childClosing, &s.childOpening, &s.childOldBal,
			&s.childOldClosing, &s.childOldOpening, &s.childCredits, &s.childDebits,
			&s.childOldCredits, &s.childOldDebits,
			&s.parentBalance, &s.parentClosing, &s.parentOpening, &s.parentOldBal,
			&s.parentOldClosing, &s.parentOldOpening, &s.parentCredits, &s.parentDebits,
			&s.parentOldCredits, &s.parentOldDebits,
		)
		if err != nil {
			log.Printf("Error scanning sweep row: %v", err)
			continue
		}
		sweeps = append(sweeps, s)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if len(sweeps) == 0 {
		log.Printf("No approved sweeps found for execution at %s", now.Format("15:04:05"))
		return nil
	}

	log.Printf("Found %d sweeps ready for execution", len(sweeps))

	// OPTIMIZATION 4: Parallel processing with semaphore to limit concurrency
	const maxConcurrent = 10
	semaphore := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	failCount := 0

	for _, s := range sweeps {
		wg.Add(1)
		go func(sweep SweepData) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			// Frequency validation (still needed for weekly/monthly)
			shouldExecute, err := shouldExecuteSweepByFrequency(sweep.frequency, sweep.requestedAt, now)
			if err != nil {
				log.Printf("Error checking sweep %s frequency: %v", sweep.sweepID, err)
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			if !shouldExecute {
				log.Printf("Sweep %s skipped: Frequency check failed", sweep.sweepID)
				return
			}

			log.Printf("Executing sweep %s...", sweep.sweepID)
			err = ExecuteSweepOptimized(ctx, db, sweep)
			if err != nil {
				log.Printf("Sweep %s failed: %v", sweep.sweepID, err)
				mu.Lock()
				failCount++
				mu.Unlock()
			} else {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(s)
	}

	wg.Wait()
	log.Printf("Sweep execution completed: %d success, %d failed out of %d total", successCount, failCount, len(sweeps))

	return nil
}

// shouldExecuteSweepByFrequency determines if a sweep should be executed based on frequency (for weekly/monthly)
// Daily checks are now handled in SQL query for performance
func shouldExecuteSweepByFrequency(frequency string, requestedAt, now time.Time) (bool, error) {
	// Normalize frequency to lowercase for case-insensitive comparison
	frequencyLower := strings.ToLower(strings.TrimSpace(frequency))

	switch frequencyLower {
	case "daily":
		// Daily checks already handled in SQL query
		return true, nil

	case "weekly":
		// Execute on the same weekday as creation
		creationWeekday := requestedAt.Weekday()
		if now.Weekday() != creationWeekday {
			return false, nil
		}
		return true, nil

	case "monthly":
		// Execute on the same date as creation (with month-end handling)
		creationDay := requestedAt.Day()
		targetDay := getTargetDayForMonth(creationDay, now.Year(), int(now.Month()))

		if now.Day() != targetDay {
			return false, nil
		}
		return true, nil

	default:
		log.Printf("Unknown frequency '%s' - skipping", frequency)
		return false, nil
	}
}

// getWeekStart returns the start of the week (Monday) for a given date
func getWeekStart(t time.Time) time.Time {
	weekday := int(t.Weekday())
	if weekday == 0 {
		weekday = 7 // Sunday
	}
	daysToMonday := weekday - 1
	return t.AddDate(0, 0, -daysToMonday).Truncate(24 * time.Hour)
}

// getTargetDayForMonth handles month-end variations (e.g., 31st in February becomes 28/29)
func getTargetDayForMonth(targetDay, year, month int) int {
	// Get the last day of the month
	firstOfNextMonth := time.Date(year, time.Month(month+1), 1, 0, 0, 0, 0, time.UTC)
	lastDayOfMonth := firstOfNextMonth.AddDate(0, 0, -1).Day()

	// If target day exceeds the last day of the month, use the last day
	if targetDay > lastDayOfMonth {
		return lastDayOfMonth
	}
	return targetDay
}

// ExecuteSweep performs the actual sweep transaction
func ExecuteSweep(ctx context.Context, db *pgxpool.Pool, p ExecuteSweepParams) error {
	// Get current balance from bank_balances_manual
	var balanceID string
	var currentBalance float64

	err := db.QueryRow(ctx, `
		SELECT balance_id, COALESCE(closing_balance, 0)
		FROM bank_balances_manual
		WHERE account_no = $1
		ORDER BY as_of_date DESC, as_of_time DESC
		LIMIT 1
	`, p.BankAccount).Scan(&balanceID, &currentBalance)

	if err != nil {
		return fmt.Errorf("account %s not found or has no balance records", p.BankAccount)
	}

	// Calculate sweep amount based on sweep type
	var sweepAmount float64
	buffer := 0.0
	if p.BufferAmount != nil {
		buffer = *p.BufferAmount
	}

	// Normalize sweep type (case-insensitive, handle hyphens, underscores, and spaces)
	// Accepts: "Target Balance", "target-balance", "target_balance", "TARGET BALANCE", etc.
	sweepTypeNormalized := strings.ToLower(
		strings.ReplaceAll(
			strings.ReplaceAll(strings.TrimSpace(p.SweepType), "_", " "),
			"-", " ",
		),
	)
	// Remove extra spaces
	sweepTypeNormalized = strings.Join(strings.Fields(sweepTypeNormalized), " ")

	switch sweepTypeNormalized {
	case "concentration":
		// Transfer all funds above buffer to parent account
		if currentBalance > buffer {
			sweepAmount = currentBalance - buffer
		} else {
			// If below buffer, transfer from parent to reach buffer (reverse sweep)
			sweepAmount = buffer - currentBalance
			// In this case, we need to swap source and destination
			return executeReverseSweep(ctx, db, ReverseSweepParams{
				SweepID:        p.SweepID,
				ChildAccount:   p.BankAccount,
				ParentAccount:  p.ParentAccount,
				AmountNeeded:   sweepAmount,
				CurrentBalance: currentBalance,
				ChildBalanceID: balanceID,
			})
		}

	case "zero balance", "zerobalance":
		// Once buffer is reached, transfer all funds to parent (set to zero)
		if currentBalance >= buffer {
			sweepAmount = currentBalance
		} else {
			// Not enough to sweep
			return nil
		}

	case "target balance", "targetbalance", "standalone", "stand alone":
		// Maintain buffer amount / standalone behavior: sweep excess above buffer
		if currentBalance > buffer {
			sweepAmount = currentBalance - buffer
			// Contextual logging: prefer 'Standalone' message when configured as such
			if strings.Contains(strings.ToLower(p.SweepType), "stand") {
				log.Printf("[SWEEP %s] Standalone sweep: %.2f - %.2f = %.2f",
					p.SweepID, currentBalance, buffer, sweepAmount)
			} else {
				log.Printf("[SWEEP %s] Target balance sweep: %.2f - %.2f = %.2f",
					p.SweepID, currentBalance, buffer, sweepAmount)
			}
		} else {
			// Balance is at or below target/buffer
			return nil
		}

	default:
		return fmt.Errorf("invalid sweep type '%s'. Allowed types: Concentration, Zero Balance, Target Balance, Standalone", p.SweepType)
	}

	if sweepAmount <= 0 {
		return nil // Nothing to sweep
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to start sweep transaction")
	}
	defer tx.Rollback(ctx)

	// Calculate new balance
	newBalance := currentBalance - sweepAmount

	// Update source account (debit)
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
		log.Printf("Error updating source account %s: %v", p.BankAccount, err)
		return fmt.Errorf("unable to update source account balance")
	}

	// Create audit action for source account balance change
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, 'sweep_system', NOW())
	`, balanceID, fmt.Sprintf("Auto sweep execution: %s", p.SweepID))

	if err != nil {
		log.Printf("Error creating audit record for source account %s: %v", p.BankAccount, err)
		return fmt.Errorf("unable to create audit record for source account")
	}

	// Get parent account balance
	var parentBalanceID string
	var parentCurrentBalance float64

	err = tx.QueryRow(ctx, `
		SELECT balance_id, COALESCE(closing_balance, 0)
		FROM bank_balances_manual
		WHERE account_no = $1
		ORDER BY as_of_date DESC, as_of_time DESC
		LIMIT 1
	`, p.ParentAccount).Scan(&parentBalanceID, &parentCurrentBalance)

	if err != nil {
		log.Printf("Error fetching parent account %s: %v", p.ParentAccount, err)
		return fmt.Errorf("parent account %s not found or has no balance records", p.ParentAccount)
	}

	// Update parent account (credit)
	parentNewBalance := parentCurrentBalance + sweepAmount

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
		log.Printf("Error updating parent account %s: %v", p.ParentAccount, err)
		return fmt.Errorf(constants.ErrUnableToUpdateParentAccountBalance)
	}

	// Create audit action for parent account balance change
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, 'sweep_system', NOW())
	`, parentBalanceID, fmt.Sprintf("Auto sweep receipt: %s", p.SweepID))

	if err != nil {
		log.Printf("Error creating audit record for parent account %s: %v", p.ParentAccount, err)
		return fmt.Errorf("unable to create audit record for parent account")
	}

	// Log sweep execution
	_, err = tx.Exec(ctx, `
		INSERT INTO sweep_execution_log (
			sweep_id, amount_swept, from_account, to_account, status, 
			balance_before, balance_after
		) VALUES ($1, $2, $3, $4, 'SUCCESS', $5, $6)
	`, p.SweepID, sweepAmount, p.BankAccount, p.ParentAccount, currentBalance, newBalance)

	if err != nil {
		log.Printf("Error logging sweep execution for %s: %v", p.SweepID, err)
		return fmt.Errorf(constants.ErrUnableToLogSweepExecution)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Error committing sweep transaction for %s: %v", p.SweepID, err)
		return fmt.Errorf("unable to complete sweep transaction")
	}

	log.Printf("Sweep executed: %s | Amount: %.2f | From: %s | To: %s",
		p.SweepID, sweepAmount, p.BankAccount, p.ParentAccount)

	return nil
}

// executeReverseSweep handles concentration when balance is below buffer (transfer from parent to child)
func executeReverseSweep(ctx context.Context, db *pgxpool.Pool, p ReverseSweepParams) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to start reverse sweep transaction")
	}
	defer tx.Rollback(ctx)

	// Get parent account balance
	var parentBalanceID string
	var parentCurrentBalance float64

	err = tx.QueryRow(ctx, `
		SELECT balance_id, COALESCE(closing_balance, 0)
		FROM bank_balances_manual
		WHERE account_no = $1
		ORDER BY as_of_date DESC, as_of_time DESC
		LIMIT 1
	`, p.ParentAccount).Scan(&parentBalanceID, &parentCurrentBalance)

	if err != nil {
		log.Printf("Error fetching parent account %s for reverse sweep: %v", p.ParentAccount, err)
		return fmt.Errorf("parent account %s not found", p.ParentAccount)
	}

	// Check if parent has sufficient funds
	if parentCurrentBalance < p.AmountNeeded {
		return logSweepExecution(ctx, db, SweepLogEntry{
			SweepID:       p.SweepID,
			FromAccount:   p.ParentAccount,
			ToAccount:     p.ChildAccount,
			AmountSwept:   0,
			Status:        "INSUFFICIENT_FUNDS",
			ErrorMessage:  fmt.Sprintf("Insufficient funds in parent account. Required: %.2f, Available: %.2f", p.AmountNeeded, parentCurrentBalance),
			BalanceBefore: parentCurrentBalance,
			BalanceAfter:  parentCurrentBalance,
		})
	}

	// Debit parent account
	parentNewBalance := parentCurrentBalance - p.AmountNeeded

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
	`, parentNewBalance, p.AmountNeeded, parentBalanceID)

	if err != nil {
		log.Printf("Error updating parent account %s in reverse sweep: %v", p.ParentAccount, err)
		return fmt.Errorf(constants.ErrUnableToUpdateParentAccountBalance)
	}

	// Create audit action for parent
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, 'sweep_system', NOW())
	`, parentBalanceID, fmt.Sprintf("Reverse sweep (concentration funding): %s", p.SweepID))
	if err != nil {
		log.Printf("Error creating audit record for parent account %s in reverse sweep: %v", p.ParentAccount, err)
		return fmt.Errorf("unable to create audit record for parent account")
	}

	// Credit child account
	childNewBalance := p.CurrentBalance + p.AmountNeeded

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
	`, childNewBalance, p.AmountNeeded, p.ChildBalanceID)

	if err != nil {
		log.Printf("Error updating child account %s in reverse sweep: %v", p.ChildAccount, err)
		return fmt.Errorf("unable to update child account balance")
	}

	// Create audit action for child
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', $2, 'sweep_system', NOW())
	`, p.ChildBalanceID, fmt.Sprintf("Reverse sweep receipt (concentration funding): %s", p.SweepID))
	if err != nil {
		log.Printf("Error creating audit record for child account %s in reverse sweep: %v", p.ChildAccount, err)
		return fmt.Errorf("unable to create audit record for child account")
	}

	// Log sweep execution
	_, err = tx.Exec(ctx, `
		INSERT INTO sweep_execution_log (
			sweep_id, amount_swept, from_account, to_account, status, 
			balance_before, balance_after
		) VALUES ($1, $2, $3, $4, 'SUCCESS', $5, $6)
	`, p.SweepID, p.AmountNeeded, p.ParentAccount, p.ChildAccount, parentCurrentBalance, parentNewBalance)
	if err != nil {
		log.Printf("Error logging reverse sweep execution for %s: %v", p.SweepID, err)
		return fmt.Errorf(constants.ErrUnableToLogSweepExecution)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Error committing reverse sweep transaction for %s: %v", p.SweepID, err)
		return fmt.Errorf("unable to complete reverse sweep transaction")
	}

	log.Printf("Reverse sweep executed: %s | Amount: %.2f | From: %s | To: %s",
		p.SweepID, p.AmountNeeded, p.ParentAccount, p.ChildAccount)

	return nil
}

// ExecuteSweepOptimized performs the sweep using pre-loaded balance data (eliminates SELECT queries)
func ExecuteSweepOptimized(ctx context.Context, db *pgxpool.Pool, sweep SweepData) error {
	currentBalance := sweep.childClosing
	log.Printf("[SWEEP %s] Starting execution - Type: %s, Child Balance: %.2f, Buffer: %.2f",
		sweep.sweepID, sweep.sweepType, currentBalance,
		func() float64 {
			if sweep.bufferAmount != nil {
				return *sweep.bufferAmount
			} else {
				return 0.0
			}
		}())

	// Calculate sweep amount based on sweep type
	var sweepAmount float64
	buffer := 0.0
	if sweep.bufferAmount != nil {
		buffer = *sweep.bufferAmount
	}

	// Normalize sweep type (case-insensitive)
	sweepTypeNormalized := strings.ToLower(
		strings.ReplaceAll(
			strings.ReplaceAll(strings.TrimSpace(sweep.sweepType), "_", " "),
			"-", " ",
		),
	)
	sweepTypeNormalized = strings.Join(strings.Fields(sweepTypeNormalized), " ")

	switch sweepTypeNormalized {
	case "concentration":
		if currentBalance > buffer {
			sweepAmount = currentBalance - buffer
			log.Printf("[SWEEP %s] Concentration sweep: %.2f - %.2f = %.2f",
				sweep.sweepID, currentBalance, buffer, sweepAmount)
		} else {
			// Reverse sweep
			sweepAmount = buffer - currentBalance
			log.Printf("[SWEEP %s] Reverse concentration sweep needed: %.2f - %.2f = %.2f",
				sweep.sweepID, buffer, currentBalance, sweepAmount)
			return executeReverseSweepOptimized(ctx, db, sweep, sweepAmount)
		}

	case "zero balance", "zerobalance":
		if currentBalance >= buffer {
			sweepAmount = currentBalance
			log.Printf("[SWEEP %s] Zero balance sweep: %.2f", sweep.sweepID, sweepAmount)
		} else {
			log.Printf("[SWEEP %s] Skipped - balance %.2f below buffer %.2f",
				sweep.sweepID, currentBalance, buffer)
			return nil
		}

	case "target balance", "targetbalance", "standalone", "stand alone":
		if currentBalance > buffer {
			sweepAmount = currentBalance - buffer
			// Use a combined message depending on configured type
			if strings.Contains(strings.ToLower(sweep.sweepType), "stand") {
				log.Printf("[SWEEP %s] Standalone sweep: %.2f - %.2f = %.2f",
					sweep.sweepID, currentBalance, buffer, sweepAmount)
			} else {
				log.Printf("[SWEEP %s] Target balance sweep: %.2f - %.2f = %.2f",
					sweep.sweepID, currentBalance, buffer, sweepAmount)
			}
		} else {
			log.Printf("[SWEEP %s] Skipped - balance %.2f at or below buffer/target %.2f",
				sweep.sweepID, currentBalance, buffer)
			return nil
		}

	default:
		return fmt.Errorf("invalid sweep type '%s'", sweep.sweepType)
	}

	if sweepAmount <= 0 {
		return nil
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to start sweep transaction")
	}
	defer tx.Rollback(ctx)

	newBalance := currentBalance - sweepAmount

	// Update source account (debit) - use bank_account to find balance_id
	_, err = tx.Exec(ctx, `
		UPDATE bank_balances_manual
		SET old_opening_balance = opening_balance,
			old_closing_balance = closing_balance,
			old_balance_amount = balance_amount,
			old_total_debits = total_debits,
			old_total_credits = total_credits,
			opening_balance = closing_balance,
			closing_balance = $1,
			balance_amount = $1,
			total_debits = total_debits + $2
		WHERE account_no = $3
	`, newBalance, sweepAmount, sweep.bankAccount)

	if err != nil {
		log.Printf("Error updating source account %s: %v", sweep.bankAccount, err)
		return fmt.Errorf("unable to update source account balance")
	}

	// Create audit action for source account
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		)
		SELECT balance_id, 'EDIT', 'PENDING_EDIT_APPROVAL', $1, 'sweep_system', NOW()
		FROM bank_balances_manual
		WHERE account_no = $2
		LIMIT 1
	`, fmt.Sprintf("Auto sweep execution: %s", sweep.sweepID), sweep.bankAccount)

	if err != nil {
		log.Printf("Error creating audit record for source account %s: %v", sweep.bankAccount, err)
		return fmt.Errorf(constants.ErrAuditInsertFailedUser)
	}

	// Update parent account (credit)
	parentNewBalance := sweep.parentClosing + sweepAmount

	_, err = tx.Exec(ctx, `
		UPDATE bank_balances_manual
		SET old_opening_balance = opening_balance,
			old_closing_balance = closing_balance,
			old_balance_amount = balance_amount,
			old_total_credits = total_credits,
			old_total_debits = total_debits,
			opening_balance = closing_balance,
			closing_balance = $1,
			balance_amount = $1,
			total_credits = total_credits + $2
		WHERE account_no = $3
	`, parentNewBalance, sweepAmount, sweep.parentAccount)

	if err != nil {
		log.Printf("Error updating parent account %s: %v", sweep.parentAccount, err)
		return fmt.Errorf(constants.ErrUnableToUpdateParentAccountBalance)
	}

	// Create audit action for parent account
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		)
		SELECT balance_id, 'EDIT', 'PENDING_EDIT_APPROVAL', $1, 'sweep_system', NOW()
		FROM bank_balances_manual
		WHERE account_no = $2
		LIMIT 1
	`, fmt.Sprintf("Auto sweep receipt: %s", sweep.sweepID), sweep.parentAccount)

	if err != nil {
		log.Printf("Error creating audit record for parent account %s: %v", sweep.parentAccount, err)
		return fmt.Errorf(constants.ErrAuditInsertFailedUser)
	}

	// Log sweep execution
	_, err = tx.Exec(ctx, `
		INSERT INTO sweep_execution_log (
			sweep_id, amount_swept, from_account, to_account, status, 
			balance_before, balance_after
		) VALUES ($1, $2, $3, $4, 'SUCCESS', $5, $6)
	`, sweep.sweepID, sweepAmount, sweep.bankAccount, sweep.parentAccount, currentBalance, newBalance)

	if err != nil {
		log.Printf("Error logging sweep execution for %s: %v", sweep.sweepID, err)
		return fmt.Errorf(constants.ErrUnableToLogSweepExecution)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("[SWEEP %s] Error committing transaction: %v", sweep.sweepID, err)
		return fmt.Errorf("unable to complete sweep transaction")
	}

	log.Printf("[SWEEP %s] ✓ SUCCESS | Amount: %.2f | From: %s (new bal: %.2f) | To: %s (new bal: %.2f)",
		sweep.sweepID, sweepAmount, sweep.bankAccount, newBalance, sweep.parentAccount, sweep.parentClosing+sweepAmount)

	return nil
}

// executeReverseSweepOptimized handles concentration reverse sweep with pre-loaded data
func executeReverseSweepOptimized(ctx context.Context, db *pgxpool.Pool, sweep SweepData, amountNeeded float64) error {
	parentCurrentBalance := sweep.parentClosing
	currentBalance := sweep.childClosing

	log.Printf("[SWEEP %s] Reverse sweep - Need: %.2f, Parent has: %.2f, Child has: %.2f",
		sweep.sweepID, amountNeeded, parentCurrentBalance, currentBalance)

	// Check if parent has sufficient funds
	if parentCurrentBalance < amountNeeded {
		log.Printf("[SWEEP %s] ✗ INSUFFICIENT_FUNDS in parent account", sweep.sweepID)
		err := logSweepExecution(ctx, db, SweepLogEntry{
			SweepID:       sweep.sweepID,
			FromAccount:   sweep.parentAccount,
			ToAccount:     sweep.bankAccount,
			AmountSwept:   0,
			Status:        "INSUFFICIENT_FUNDS",
			ErrorMessage:  fmt.Sprintf("Insufficient funds in parent account. Required: %.2f, Available: %.2f", amountNeeded, parentCurrentBalance),
			BalanceBefore: parentCurrentBalance,
			BalanceAfter:  parentCurrentBalance,
		})
		if err != nil {
			log.Printf("Error logging INSUFFICIENT_FUNDS for sweep %s: %v", sweep.sweepID, err)
		}
		return fmt.Errorf("insufficient funds in parent account")
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("unable to start reverse sweep transaction")
	}
	defer tx.Rollback(ctx)

	// Debit parent account
	parentNewBalance := parentCurrentBalance - amountNeeded

	_, err = tx.Exec(ctx, `
		UPDATE bank_balances_manual
		SET old_opening_balance = opening_balance,
			old_closing_balance = closing_balance,
			old_balance_amount = balance_amount,
			old_total_debits = total_debits,
			old_total_credits = total_credits,
			opening_balance = closing_balance,
			closing_balance = $1,
			balance_amount = $1,
			total_debits = total_debits + $2
		WHERE account_no = $3
	`, parentNewBalance, amountNeeded, sweep.parentAccount)

	if err != nil {
		log.Printf("Error updating parent account %s in reverse sweep: %v", sweep.parentAccount, err)
		return fmt.Errorf(constants.ErrUnableToUpdateParentAccountBalance)
	}

	// Create audit action for parent
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		)
		SELECT balance_id, 'EDIT', 'PENDING_EDIT_APPROVAL', $1, 'sweep_system', NOW()
		FROM bank_balances_manual
		WHERE account_no = $2
		LIMIT 1
	`, fmt.Sprintf("Reverse sweep (concentration funding): %s", sweep.sweepID), sweep.parentAccount)

	if err != nil {
		log.Printf("Error creating audit record for parent account %s in reverse sweep: %v", sweep.parentAccount, err)
		return fmt.Errorf(constants.ErrAuditInsertFailedUser)
	}

	// Credit child account
	childNewBalance := currentBalance + amountNeeded

	_, err = tx.Exec(ctx, `
		UPDATE bank_balances_manual
		SET old_opening_balance = opening_balance,
			old_closing_balance = closing_balance,
			old_balance_amount = balance_amount,
			old_total_credits = total_credits,
			old_total_debits = total_debits,
			opening_balance = closing_balance,
			closing_balance = $1,
			balance_amount = $1,
			total_credits = total_credits + $2
		WHERE account_no = $3
	`, childNewBalance, amountNeeded, sweep.bankAccount)

	if err != nil {
		log.Printf("Error updating child account %s in reverse sweep: %v", sweep.bankAccount, err)
		return fmt.Errorf("unable to update child account balance")
	}

	// Create audit action for child
	_, err = tx.Exec(ctx, `
		INSERT INTO auditactionbankbalances (
			balance_id, actiontype, processing_status, reason, requested_by, requested_at
		)
		SELECT balance_id, 'EDIT', 'PENDING_EDIT_APPROVAL', $1, 'sweep_system', NOW()
		FROM bank_balances_manual
		WHERE account_no = $2
		LIMIT 1
	`, fmt.Sprintf("Reverse sweep receipt (concentration funding): %s", sweep.sweepID), sweep.bankAccount)

	if err != nil {
		log.Printf("Error creating audit record for child account %s in reverse sweep: %v", sweep.bankAccount, err)
		return fmt.Errorf(constants.ErrAuditInsertFailedUser)
	}

	// Log sweep execution
	_, err = tx.Exec(ctx, `
		INSERT INTO sweep_execution_log (
			sweep_id, amount_swept, from_account, to_account, status, 
			balance_before, balance_after
		) VALUES ($1, $2, $3, $4, 'SUCCESS', $5, $6)
	`, sweep.sweepID, amountNeeded, sweep.parentAccount, sweep.bankAccount, parentCurrentBalance, parentNewBalance)

	if err != nil {
		log.Printf("Error logging reverse sweep execution for %s: %v", sweep.sweepID, err)
		return fmt.Errorf(constants.ErrUnableToLogSweepExecution)
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("[SWEEP %s] Error committing reverse sweep transaction: %v", sweep.sweepID, err)
		return fmt.Errorf("unable to complete reverse sweep transaction")
	}

	log.Printf("[SWEEP %s] ✓ REVERSE SUCCESS | Amount: %.2f | From: %s (new bal: %.2f) | To: %s (new bal: %.2f)",
		sweep.sweepID, amountNeeded, sweep.parentAccount, parentCurrentBalance-amountNeeded,
		sweep.bankAccount, currentBalance+amountNeeded)

	return nil
}

// logSweepExecution logs a sweep execution result
func logSweepExecution(ctx context.Context, db *pgxpool.Pool, entry SweepLogEntry) error {
	_, err := db.Exec(ctx, `
		INSERT INTO sweep_execution_log (
			sweep_id, amount_swept, from_account, to_account, status, 
			error_message, balance_before, balance_after
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, entry.SweepID, entry.AmountSwept, entry.FromAccount, entry.ToAccount, entry.Status, entry.ErrorMessage, entry.BalanceBefore, entry.BalanceAfter)

	return err
}
