package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
	"github.com/shopspring/decimal"
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

	c := cron.New(cron.WithLocation(loc), cron.WithChain(cron.Recover(cron.DefaultLogger), cron.SkipIfStillRunning(cron.DefaultLogger)))

	_, err = c.AddFunc(cfg.Schedule, func() {
		err := ProcessApprovedSweepsV2(db, cfg.BatchSize, loc)
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
func ProcessApprovedSweepsV2(db *pgxpool.Pool, batchSize int, loc *time.Location) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if loc == nil {
		loc = time.UTC
	}

	lockConn, err := db.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire sweep lock connection: %w", err)
	}
	lockTx, err := lockConn.Begin(ctx)
	if err != nil {
		lockConn.Release()
		return fmt.Errorf("begin sweep lock transaction: %w", err)
	}
	defer func() {
		_ = lockTx.Rollback(ctx)
		lockConn.Release()
	}()
	var lockAcquired bool
	if err := lockTx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('sweep-v2-processor'))`).Scan(&lockAcquired); err != nil {
		return fmt.Errorf("sweep advisory lock: %w", err)
	}
	if !lockAcquired {
		logger.LogInfo("[SWEEP V2] another instance is already processing sweeps — skipping")
		return nil
	}

	now := time.Now().In(loc)
	currentHour := now.Hour()
	currentMinute := now.Minute()
	today := now.Format(constants.DateFormat)

	logger.LogInfo(constants.LogSweepWorker)
	logger.LogInfo("[SWEEP V2] Starting scheduled sweep processing at %s", now.Format(constants.DateTimeFormat))
	logger.LogInfo("[SWEEP V2] Current time: %02d:%02d | Batch size: %d", currentHour, currentMinute, batchSize)
	logger.LogInfo(constants.LogSweepWorker)

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
			audit.requested_at
		FROM cimplrcorpsaas.sweepconfiguration sc
		INNER JOIN LATERAL (
			SELECT requested_at, processing_status
			FROM cimplrcorpsaas.auditactionsweepconfiguration
			WHERE sweep_id = sc.sweep_id
			ORDER BY requested_at DESC
			LIMIT 1
		) audit ON true
		WHERE sc.is_deleted = false
			AND audit.processing_status = 'APPROVED'
			AND (sc.effective_date IS NULL OR sc.effective_date <= $1::date)
		AND EXTRACT(HOUR FROM sc.execution_time) = $2
		AND EXTRACT(MINUTE FROM sc.execution_time) BETWEEN $3 AND $4
	`

	logger.LogInfo("[SWEEP V2]  Querying for sweeps at %02d:%02d (today=%s)", currentHour, currentMinute, today)
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
			&s.requiresInitiation, &s.requestedAt,
		)
		if err != nil {
			logger.LogError("[SWEEP V2] Error scanning sweep row: %v", err)
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
		logger.LogInfo("[SWEEP V2]  No approved sweeps found for execution at %s", now.Format("15:04:05"))
		logger.LogInfo("[SWEEP V2] Query parameters: hour=%d, minute=%d-%d", currentHour, currentMinute, currentMinute+1)
		return nil
	}

	logger.LogInfo("[SWEEP V2]  Found %d sweeps ready for processing", len(sweeps))
	for i, s := range sweeps {
		logger.LogInfo("[SWEEP V2]   [%d] ID=%s Type=%s Freq=%s Source=%s Target=%s RequiresInitiation=%v",
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
				logger.LogError("[SWEEP V2] %s - Error checking frequency: %v", sweep.sweepID, err)
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			if !shouldExecute {
				logger.LogError("[SWEEP V2] %s - Skipped: Frequency check failed (%s)", sweep.sweepID, sweep.frequency)
				return
			}

			// Create initiation with auto-approval for scheduled sweeps
			if ok, msg := enforceScheduledSweepFire(ctx, db, sweep.sweepID, "", "createScheduledInitiation"); !ok {
				logger.LogError("[SWEEP V2] %s - Policy blocked scheduled initiation: %s", sweep.sweepID, msg)
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}
			err = createScheduledInitiation(ctx, db, sweep.sweepID)
			if err != nil {
				logger.LogError("[SWEEP V2] %s - Failed to create scheduled initiation: %v", sweep.sweepID, err)
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}
			mu.Lock()
			initiationCreatedCount++
			mu.Unlock()
			logger.LogInfo("[SWEEP V2] %s - Scheduled initiation created and auto-approved", sweep.sweepID)
			// Sweep will be executed by ProcessPendingInitiations when initiation status becomes INITIATED
		}(s)
	}

	wg.Wait()

	// Step 3: Process pending initiations (status='INITIATED')
	initiationsProcessed, initiationsFailed := ProcessPendingInitiations(ctx, db, batchSize)

	logger.LogInfo(constants.LogSweepWorker)
	logger.LogInfo("[SWEEP V2]  PROCESSING SUMMARY:")
	logger.LogInfo("[SWEEP V2]    Direct Executions: %d", successCount)
	logger.LogError("[SWEEP V2]    Failed: %d", failCount)
	logger.LogInfo("[SWEEP V2]    Initiations Created: %d", initiationCreatedCount)
	logger.LogInfo("[SWEEP V2]    Initiations Processed: %d", initiationsProcessed)
	logger.LogError("[SWEEP V2]    Initiations Failed: %d", initiationsFailed)
	logger.LogInfo(constants.LogSweepWorker)

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
		referenceDate := requestedAt.In(now.Location())
		if effectiveDate.Valid && effectiveDate.String != "" {
			parsed, err := time.ParseInLocation(constants.DateFormat, effectiveDate.String, now.Location())
			if err == nil {
				referenceDate = parsed
			}
		}
		refDay := time.Date(referenceDate.Year(), referenceDate.Month(), referenceDate.Day(), 0, 0, 0, 0, time.UTC)
		nowDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		daysSince := int(nowDay.Sub(refDay).Hours() / 24)

		// Execute if today is exactly a multiple of 7 days from reference
		if daysSince >= 0 && daysSince%7 == 0 {
			return true, nil
		}
		return false, nil

	case "MONTHLY":
		// Execute on the same day of month as effective_date (or requested_at if no effective_date)
		referenceDate := requestedAt.In(now.Location())
		if effectiveDate.Valid && effectiveDate.String != "" {
			parsed, err := time.ParseInLocation(constants.DateFormat, effectiveDate.String, now.Location())
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

func createScheduledInitiation(ctx context.Context, db *pgxpool.Pool, sweepID string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('sweep-initiation:' || $1))`, sweepID); err != nil {
		return fmt.Errorf("lock sweep %s: %w", sweepID, err)
	}

	var existingCount int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM cimplrcorpsaas.sweep_initiation si
		JOIN cimplrcorpsaas.auditactionsweepinitiation asi ON asi.initiation_id = si.initiation_id
		WHERE si.sweep_id = $1
		AND DATE(si.initiation_time) = CURRENT_DATE
		AND asi.processing_status IN ('PENDING_APPROVAL', 'APPROVED')
	`, sweepID).Scan(&existingCount); err != nil {
		return fmt.Errorf("check existing initiation: %w", err)
	}
	if existingCount > 0 {
		return nil // Already has a pending/approved initiation for today, skip
	}

	var initiationID string
	if err := tx.QueryRow(ctx, `INSERT INTO cimplrcorpsaas.sweep_initiation (
		sweep_id, initiated_by, initiation_time
	) VALUES ($1, 'sweep_system', now()) RETURNING initiation_id`, sweepID).Scan(&initiationID); err != nil {
		return err
	}

	// Auto-approve the scheduled initiation
	if _, err := tx.Exec(ctx, `INSERT INTO cimplrcorpsaas.auditactionsweepinitiation (
		initiation_id, sweep_id, actiontype, processing_status,
		requested_by, requested_at, requested_ip, checker_by, checker_at, checker_ip
	) VALUES ($1, $2, 'CREATE', 'APPROVED', 'sweep_system', now(), NULL::text, 'sweep_system', now(), NULL::text)`, initiationID, sweepID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ProcessPendingInitiations processes approved initiations that are eligible for execution.
// Each initiation may produce multiple execution logs across scheduled runs (new initiation per day).
// Within one initiation, failed attempts are retried up to SWEEP_MAX_RETRIES (env, default 2).
func ProcessPendingInitiations(ctx context.Context, db *pgxpool.Pool, batchSize int) (int, int) {
	if batchSize <= 0 {
		batchSize = config.SweepBatchSize
	}
	maxAttempts := config.SweepMaxAttemptsPerInitiation()

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
		AND COALESCE(i.is_deleted, false) = false
		AND sc.is_deleted = false
		AND NOT EXISTS (
			SELECT 1
			FROM cimplrcorpsaas.sweep_execution_log el
			WHERE el.initiation_id = i.initiation_id
			  AND el.status = 'SUCCESS'
		)
		AND (
			SELECT COUNT(*)
			FROM cimplrcorpsaas.sweep_execution_log el
			WHERE el.initiation_id = i.initiation_id
		) < $2
		ORDER BY i.initiation_time ASC
		LIMIT $1
	`

	logger.LogInfo("[SWEEP V2]  Checking approved initiations (max_attempts=%d incl. retries=%d)...",
		maxAttempts, config.SweepMaxRetries())
	rows, err := db.Query(ctx, query, batchSize, maxAttempts)
	if err != nil {
		logger.LogError("[SWEEP V2] Failed to fetch approved initiations: %v", err)
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
			logger.LogError("[SWEEP V2] Failed to scan initiation row: %v", err)
			continue
		}

		logger.LogInfo("[SWEEP V2] 📝 Processing initiation [%d]: %s (sweep: %s)", initiationCount, initiationID, sweepID)

		if ok, msg := enforceScheduledSweepFire(ctx, db, sweepID, initiationID, "ProcessPendingInitiations"); !ok {
			logger.LogError("[SWEEP V2] %s (initiation: %s) - Policy blocked execution: %s", sweepID, initiationID, msg)
			failCount++
			continue
		}

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
		params := SweepExecParams{
			SweepID:          sweepID,
			InitiationID:     initiationID,
			FromAccount:      finalSourceAccount,
			ToAccount:        finalTargetAccount,
			SweepType:        sweepType,
			BufferAmount:     bufferAmount,
			SweepAmount:      sweepAmount,
			OverriddenAmount: overriddenAmount,
		}
		err = executeSweepWithInitiation(ctx, db, params)

		if errors.Is(err, ErrSweepInitiationBusy) {
			logger.LogInfo("[SWEEP V2] %s (initiation: %s) - skipped: %v", sweepID, initiationID, err)
			continue
		}
		if err != nil {
			logger.LogInfo("[SWEEP V2] %s (initiation: %s) - Execution failed: %v", sweepID, initiationID, err)
			failCount++
		} else {
			logger.LogInfo("[SWEEP V2] %s (initiation: %s) - Execution successful", sweepID, initiationID)
			successCount++
		}
	}

	if initiationCount > 0 {
		logger.LogError("[SWEEP V2] Processed %d initiations: %d succeeded, %d failed", initiationCount, successCount, failCount)
	}

	return successCount, failCount
}

// logSweepFailure logs a failed sweep attempt. Multiple rows per initiation_id are allowed
// (retries within the same initiation, and separate initiations on later scheduled runs).
func logSweepFailure(ctx context.Context, db *pgxpool.Pool, info SweepFailureInfo) {
	query := `
		INSERT INTO cimplrcorpsaas.sweep_execution_log (
			sweep_id, initiation_id, from_account, to_account, status, 
			balance_before, balance_after, amount_swept, error_message
		) VALUES ($1, NULLIF($2, '')::uuid, $3, $4, 'FAILED', $5, $5, 0, $6)
	`
	_, err := db.Exec(ctx, query,
		info.Params.SweepID,
		info.Params.InitiationID,
		info.Params.FromAccount,
		info.Params.ToAccount,
		info.BalanceBefore,
		info.Reason,
	)
	if err != nil {
		logger.LogError("[SWEEP V2] Failed to log sweep failure: %v", err)
	}
	logger.LogError("[SWEEP V2] %s  FAILED | Reason: %s", info.Params.SweepID, info.Reason)
}

type sweepBalanceRow struct {
	balanceID string
	closing   decimal.Decimal
}

func lockSweepBalances(ctx context.Context, tx pgx.Tx, sourceAccount, targetAccount string) (sweepBalanceRow, sweepBalanceRow, error) {
	var src, tgt sweepBalanceRow
	latestID := func(account string) (string, error) {
		var id string
		err := tx.QueryRow(ctx, `
			SELECT balance_id
			FROM bank_balances_manual
			WHERE account_no = $1
			ORDER BY as_of_date DESC, as_of_time DESC
			LIMIT 1
		`, account).Scan(&id)
		return id, err
	}
	srcID, err := latestID(sourceAccount)
	if err != nil {
		return src, tgt, fmt.Errorf("source account balance not found (%s): %w", sourceAccount, err)
	}
	tgtID, err := latestID(targetAccount)
	if err != nil {
		return src, tgt, fmt.Errorf("target account balance not found (%s): %w", targetAccount, err)
	}
	rows, err := tx.Query(ctx, `
		SELECT balance_id::text, COALESCE(closing_balance, 0)::text
		FROM bank_balances_manual
		WHERE balance_id::text = ANY($1)
		ORDER BY balance_id
		FOR UPDATE
	`, []string{srcID, tgtID})
	if err != nil {
		return src, tgt, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, closingText string
		if err := rows.Scan(&id, &closingText); err != nil {
			return src, tgt, err
		}
		closing, err := decimal.NewFromString(closingText)
		if err != nil {
			return src, tgt, fmt.Errorf("invalid closing balance %q on %s: %w", closingText, id, err)
		}
		if id == srcID {
			src = sweepBalanceRow{balanceID: id, closing: closing}
		}
		if id == tgtID {
			tgt = sweepBalanceRow{balanceID: id, closing: closing}
		}
	}
	if err := rows.Err(); err != nil {
		return src, tgt, err
	}
	if src.balanceID == "" || tgt.balanceID == "" {
		return src, tgt, fmt.Errorf("balance rows changed while locking (%s -> %s)", sourceAccount, targetAccount)
	}
	return src, tgt, nil
}

func decimalFromPtr(v *float64) (decimal.Decimal, bool) {
	if v == nil {
		return decimal.Zero, false
	}
	return decimal.NewFromFloat(*v), true
}

func computeSweepAmount(sweepType string, balance, buffer decimal.Decimal, sweepAmount, overriddenAmount *float64) (decimal.Decimal, error) {
	typ := strings.ToUpper(strings.TrimSpace(sweepType))
	if balance.LessThanOrEqual(buffer) {
		return decimal.Zero, fmt.Errorf("BLOCKED: Buffer Violation - Balance (%s) <= Buffer (%s)", balance.StringFixed(2), buffer.StringFixed(2))
	}
	var amount decimal.Decimal
	if override, ok := decimalFromPtr(overriddenAmount); ok && override.IsPositive() {
		amount = override
	} else {
		switch typ {
		case "ZBA":
			amount = balance
		case "CONCENTRATION":
			fixed, ok := decimalFromPtr(sweepAmount)
			if !ok || !fixed.IsPositive() {
				return decimal.Zero, errors.New("BLOCKED: CONCENTRATION requires fixed sweep_amount")
			}
			amount = fixed
		case "TARGET_BALANCE":
			amount = balance.Sub(buffer)
		default:
			return decimal.Zero, fmt.Errorf("BLOCKED: unknown sweep type: %s", sweepType)
		}
	}
	if !amount.IsPositive() {
		return decimal.Zero, fmt.Errorf("BLOCKED: No funds available to sweep (Amount: %s)", amount.StringFixed(2))
	}
	if amount.GreaterThan(balance) {
		return decimal.Zero, fmt.Errorf("BLOCKED: Sweep amount (%s) exceeds available balance (%s)", amount.StringFixed(2), balance.StringFixed(2))
	}
	if typ != "ZBA" && balance.Sub(amount).LessThan(buffer) {
		return decimal.Zero, fmt.Errorf("BLOCKED: Sweeping %s would leave balance below buffer (%s)", amount.StringFixed(2), buffer.StringFixed(2))
	}
	return amount, nil
}

var ErrSweepInitiationBusy = errors.New("initiation is being executed by another worker or already succeeded")

type SweepAuditOptions struct {
	AuditStatus string
	RequestedBy string
	RequestedIP string
	Notify      bool
}

type SweepResult struct {
	Amount        decimal.Decimal
	BalanceBefore decimal.Decimal
	BalanceAfter  decimal.Decimal
}

func executeSweepWithInitiation(ctx context.Context, db *pgxpool.Pool, params SweepExecParams) error {
	_, err := ExecuteSweepInitiation(ctx, db, params, SweepAuditOptions{
		AuditStatus: "PENDING_EDIT_APPROVAL",
		RequestedBy: "sweep_system_v2",
		Notify:      true,
	})
	return err
}

func ExecuteSweepInitiation(ctx context.Context, db *pgxpool.Pool, params SweepExecParams, opts SweepAuditOptions) (SweepResult, error) {
	sweepID := params.SweepID
	initiationID := params.InitiationID
	sourceAccount := params.FromAccount
	targetAccount := params.ToAccount
	fail := func(balanceBefore decimal.Decimal, reason string) error {
		before, _ := balanceBefore.Float64()
		logSweepFailure(ctx, db, SweepFailureInfo{Params: SweepExecParams{
			SweepID:      sweepID,
			InitiationID: initiationID,
			FromAccount:  sourceAccount,
			ToAccount:    targetAccount,
		}, BalanceBefore: before, Reason: reason})
		return errors.New(reason)
	}

	var result SweepResult
	tx, err := db.Begin(ctx)
	if err != nil {
		return result, fail(decimal.Zero, fmt.Sprintf("SYSTEM ERROR: Failed to begin transaction: %v", err))
	}
	defer tx.Rollback(ctx)

	var claimed string
	err = tx.QueryRow(ctx, `
		SELECT i.initiation_id::text
		FROM cimplrcorpsaas.sweep_initiation i
		WHERE i.initiation_id = $1
		  AND NOT EXISTS (
			SELECT 1 FROM cimplrcorpsaas.sweep_execution_log el
			WHERE el.initiation_id = i.initiation_id AND el.status = 'SUCCESS'
		  )
		FOR UPDATE SKIP LOCKED
	`, initiationID).Scan(&claimed)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrSweepInitiationBusy
	}
	if err != nil {
		return result, fail(decimal.Zero, fmt.Sprintf("SYSTEM ERROR: failed to claim initiation: %v", err))
	}

	src, tgt, err := lockSweepBalances(ctx, tx, sourceAccount, targetAccount)
	if err != nil {
		return result, fail(decimal.Zero, "SYSTEM ERROR: "+err.Error())
	}
	buffer, _ := decimalFromPtr(params.BufferAmount)
	amount, err := computeSweepAmount(params.SweepType, src.closing, buffer, params.SweepAmount, params.OverriddenAmount)
	if err != nil {
		return result, fail(src.closing, err.Error())
	}
	newBalance := src.closing.Sub(amount)
	targetNewBalance := tgt.closing.Add(amount)

	updateBalance := func(balanceID string, newClosing decimal.Decimal) error {
		tag, err := tx.Exec(ctx, `
			UPDATE bank_balances_manual
			SET closing_balance = $1::numeric
			WHERE balance_id::text = $2
		`, newClosing.String(), balanceID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("expected 1 balance row updated for %s, got %d", balanceID, tag.RowsAffected())
		}
		return nil
	}
	insertAudit := func(balanceID, reason string) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO auditactionbankbalances (
				balance_id, actiontype, processing_status, reason, requested_by, requested_at, requested_ip
			) VALUES ($1, 'EDIT', $2, $3, $4, NOW(), NULLIF($5, ''))
		`, balanceID, opts.AuditStatus, reason, opts.RequestedBy, opts.RequestedIP)
		return err
	}

	if err := updateBalance(src.balanceID, newBalance); err != nil {
		return result, fail(src.closing, fmt.Sprintf("SYSTEM ERROR: failed to update source account: %v", err))
	}
	if err := insertAudit(src.balanceID, fmt.Sprintf("Sweep V2 execution (initiation: %s): %s", initiationID, sweepID)); err != nil {
		return result, fail(src.closing, fmt.Sprintf("SYSTEM ERROR: failed to create audit for source account: %v", err))
	}
	if err := updateBalance(tgt.balanceID, targetNewBalance); err != nil {
		return result, fail(src.closing, fmt.Sprintf("SYSTEM ERROR: failed to update target account: %v", err))
	}
	if err := insertAudit(tgt.balanceID, fmt.Sprintf("Sweep V2 receipt (initiation: %s): %s", initiationID, sweepID)); err != nil {
		return result, fail(src.closing, fmt.Sprintf("SYSTEM ERROR: failed to create audit for target account: %v", err))
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.sweep_execution_log (
			initiation_id, sweep_id, amount_swept, from_account, to_account, status,
			balance_before, balance_after, error_message
		) VALUES ($1, $2, $3::numeric, $4, $5, 'SUCCESS', $6::numeric, $7::numeric, NULL)
	`, initiationID, sweepID, amount.String(), sourceAccount, targetAccount, src.closing.String(), newBalance.String()); err != nil {
		return result, fail(src.closing, fmt.Sprintf("SYSTEM ERROR: Failed to log execution: %v", err))
	}
	if err := tx.Commit(ctx); err != nil {
		return result, fail(src.closing, fmt.Sprintf("SYSTEM ERROR: Failed to commit: %v", err))
	}

	result = SweepResult{Amount: amount, BalanceBefore: src.closing, BalanceAfter: newBalance}
	logger.LogInfo("[SWEEP V2] %s SUCCESS (Initiation: %s) | Type: %s | Amount: %s | Buffer: %s",
		sweepID, initiationID, params.SweepType, amount.StringFixed(2), buffer.StringFixed(2))

	if !opts.Notify {
		return result, nil
	}
	amountF, _ := amount.Float64()
	beforeF, _ := src.closing.Float64()
	afterF, _ := newBalance.Float64()
	bufferF, _ := buffer.Float64()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.LogError("[SWEEP V2] notification panic for %s: %v", sweepID, r)
			}
		}()
		notifyCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		catalog.TriggerNotification(
			notifyCtx, db,
			"/cash/sweep/executed",
			fmt.Sprintf("SWEEP_EXEC/%s/%s", sweepID, initiationID),
			map[string]interface{}{
				"SweepID":       sweepID,
				"InitiationID":  initiationID,
				"FromAccount":   sourceAccount,
				"ToAccount":     targetAccount,
				"SweepType":     params.SweepType,
				"AmountSwept":   amountF,
				"BalanceBefore": beforeF,
				"BalanceAfter":  afterF,
				"BufferAmount":  bufferF,
				"ExecutedAt":    time.Now().Format(time.RFC3339),
				"Status":        "SUCCESS",
			},
		)
	}()
	return result, nil
}
