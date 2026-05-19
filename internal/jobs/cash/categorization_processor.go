package jobs

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/logger"
	cat "CimplrCorpSaas/internal/services/categorizer"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

// auditLog logs to GlobalLogger if initialized, falling back to the standard log package.
func auditLog(msg string) {
	if logger.GlobalLogger != nil {
		logger.GlobalLogger.LogAudit(msg)
	} else {
		log.Printf("[AUDIT] %s", msg)
	}
}

// CategorizationConfig holds configuration for auto-categorization processing.
type CategorizationConfig struct {
	Schedule  string // Cron schedule (default: "0 18 * * *" for 6 PM daily)
	BatchSize int    // Number of transactions per batch
	TimeZone  string // Timezone for scheduling
}

// NewDefaultCategorizationConfig creates a CategorizationConfig from env vars.
func NewDefaultCategorizationConfig() *CategorizationConfig {
	schedule := os.Getenv("CATEGORIZATION_SCHEDULE")
	if schedule == "" {
		schedule = "0 18 * * *" // 6 PM daily
	}
	batchSize := 1000
	if bs := os.Getenv("CATEGORIZATION_BATCH_SIZE"); bs != "" {
		if parsed, err := parseInt(bs); err == nil && parsed > 0 {
			batchSize = parsed
		}
	}
	return &CategorizationConfig{
		Schedule:  schedule,
		BatchSize: batchSize,
		TimeZone:  config.DefaultTimeZone,
	}
}

// RunCategorizationScheduler starts the cron job for automated transaction categorization.
func RunCategorizationScheduler(cfg *CategorizationConfig, db *pgxpool.Pool) error {
	if cfg.Schedule == "" {
		cfg.Schedule = "0 18 * * *"
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}
	if cfg.TimeZone == "" {
		cfg.TimeZone = config.DefaultTimeZone
	}

	loc, err := time.LoadLocation(cfg.TimeZone)
	if err != nil {
		loc = time.UTC
		auditLog(fmt.Sprintf("Invalid timezone %s, falling back to UTC: %v", cfg.TimeZone, err))
	}

	c := cron.New(cron.WithLocation(loc))
	_, err = c.AddFunc(cfg.Schedule, func() {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Starting auto-categorization job at %s", time.Now().In(loc).Format(time.RFC3339)))
		err := ProcessUncategorizedTransactions(db, cfg.BatchSize)
		if err != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Auto-categorization job failed: %v", err))
			logger.LogError("ERROR: Auto-categorization job failed: %v", err)
		} else {
			auditLog("Smart-categorization job completed successfully")
		}
	})
	if err != nil {
		return fmt.Errorf("unable to schedule auto-categorization processor: %v", err)
	}

	c.Start()
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Auto-categorization scheduler started with schedule: %s (timezone: %s)", cfg.Schedule, cfg.TimeZone))
	logger.LogAudit("Auto-categorization scheduler started: %s (%s)", cfg.Schedule, cfg.TimeZone)

	return nil
}

// ProcessUncategorizedTransactions performs a full smart recategorization of
// bank statement transactions using the 10-step waterfall in internal/services/categorizer.
//
// Safety guarantees:
//   - A PostgreSQL advisory lock (hashtext('smart-cat-batch')) prevents concurrent runs.
//   - Transactions already classified by a human (CORRECTION / CONFIRMATION) are skipped.
//
// Pipeline:
//  1. Acquire advisory lock — abort if already held by another caller.
//  2. Load all active rules (once, in memory).
//  3. Load counterparty + GL caches (once), including per-account GL account IDs.
//  4. Stream only engine-classifiable transactions in batches (value_date DESC).
//  5. For each batch: process narration → filter rules → run waterfall → collect results.
//  6. Persist via PersistBatch (pgx.Batch pipeline, 4 ops per item).
//  7. Track affected bank statements and insert audit approval rows afterwards.
//
// Pass an optional bankStatementID to restrict processing to a single statement.
// If empty (or not supplied), all eligible transactions are processed.
func ProcessUncategorizedTransactions(db *pgxpool.Pool, batchSize int, bankStatementID ...string) error {
	filterBSID := ""
	if len(bankStatementID) > 0 {
		filterBSID = bankStatementID[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
	defer cancel()

	if batchSize <= 0 {
		batchSize = 1000
	}

	// Worker count: SMART_CAT_WORKERS env var, default min(8, 2*NumCPU).
	workerCount := runtime.NumCPU() * 2
	if workerCount > 8 {
		workerCount = 8
	}
	if wc := os.Getenv("SMART_CAT_WORKERS"); wc != "" {
		if n, err := parseInt(wc); err == nil && n > 0 {
			workerCount = n
		}
	}

	// ── 0. Advisory lock — prevent concurrent runs ────────────────────────────
	// Targeted runs (specific bankStatementID) use a per-statement lock key so
	// they don't block each other or the global cron batch.
	lockKey := "smart-cat-batch"
	if filterBSID != "" {
		lockKey = "smart-cat-bs-" + filterBSID
	}
	var lockAcquired bool
	if err := db.QueryRow(ctx,
		`SELECT pg_try_advisory_lock(hashtext($1))`, lockKey,
	).Scan(&lockAcquired); err != nil {
		return fmt.Errorf("advisory lock check: %w", err)
	}
	if !lockAcquired {
		auditLog("Smart-categorization: another instance is already running — skipping")
		return nil
	}
	// ── 2. Count total eligible transactions ──────────────────────────────────
	var totalCount int
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*) FROM cimplrcorpsaas.bank_statement_transactions
		WHERE bank_statement_id IS NOT NULL
		  AND (classification_step IS NULL OR classification_step NOT IN ('CORRECTION', 'CONFIRMATION'))
		  AND ($1 = '' OR bank_statement_id::text = $1)
	`, filterBSID).Scan(&totalCount); err != nil {
		return fmt.Errorf("count transactions: %w", err)
	}
	if totalCount == 0 {
		auditLog("No transactions found for smart-categorization")
		return nil
	}
	logger.LogAudit("Total transactions to consider for recategorization: %d", totalCount)
	log.Printf("[AUDIT] Smart-categorization: %d transactions to process (workers=%d batch=%d)", totalCount, workerCount, batchSize)

	// Load all rules once
	logger.LogAudit("Loading all active categorization rules for recategorization...")
	allRules, err := cat.LoadAllSmartRules(ctx, db)
	if err != nil {
		return fmt.Errorf("load rules: %w", err)
	}
	caches, err := cat.LoadSmartCaches(ctx, db)
	if err != nil {
		return fmt.Errorf("load caches: %w", err)
	}

	// ── 3. Keyset-cursor batch loop ───────────────────────────────────────────
	// Keyset on transaction_id is stable — rows that get classified mid-run don't
	// shift the cursor position the way OFFSET does.
	var lastID int64 = 0
	totalProcessed := 0
	totalUpdated := 0
	progressAt := time.Now()
	affectedBS := make(map[string]struct{})

	for {
		rows, err := db.Query(ctx, `
			SELECT
				t.transaction_id,
				bs.account_number,
				bs.entity_id,
				COALESCE(t.description, ''),
				t.withdrawal_amount,
				t.deposit_amount,
				t.value_date,
				COALESCE(m.currency, ''),
				COALESCE(t.bank_statement_id::text, '')
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements bs
			    ON bs.bank_statement_id = t.bank_statement_id
			LEFT JOIN public.masterbankaccount m
			    ON m.account_number = bs.account_number
			WHERE t.bank_statement_id IS NOT NULL
			  AND t.transaction_id > $1
			  AND (
			        t.classification_step IS NULL
			        OR t.classification_step NOT IN ('CORRECTION', 'CONFIRMATION')
			  )
			  AND ($3 = '' OR t.bank_statement_id::text = $3)
			ORDER BY t.transaction_id
			LIMIT $2
		`, lastID, batchSize, filterBSID)
		if err != nil {
			return fmt.Errorf("query batch after id=%d: %w", lastID, err)
		}

		type rowData struct {
			input cat.TxnInput
			bsID  string
		}

		var batch []rowData
		for rows.Next() {
			var rd rowData
			var withdrawal, deposit *float64
			var valueDate *time.Time
			if err := rows.Scan(
				&rd.input.TransactionID,
				&rd.input.AccountNumber,
				&rd.input.EntityID,
				&rd.input.Description,
				&withdrawal,
				&deposit,
				&valueDate,
				&rd.input.Currency,
				&rd.bsID,
			); err != nil {
				continue
			}
			rd.input.Withdrawal = withdrawal
			rd.input.Deposit = deposit
			rd.input.ValueDate = valueDate
			batch = append(batch, rd)
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		// Advance keyset cursor.
		lastID = batch[len(batch)-1].input.TransactionID

		// ── 4. Classify transactions in parallel ──────────────────────────────
		// Each goroutine is independent: allRules and caches are read-only;
		// SmartCategorize only reads from DB (Steps 4 & 5). PersistBatch handles writes.
		persistItems := make([]cat.BatchClassification, len(batch))
		sem := make(chan struct{}, workerCount)
		var wg sync.WaitGroup

		for i, rd := range batch {
			sem <- struct{}{}
			wg.Add(1)
			go func(idx int, rd rowData) {
				defer func() { <-sem; wg.Done() }()
				filtered := cat.FilterRulesForTxn(allRules, rd.input.AccountNumber, rd.input.EntityID, rd.input.Currency)
				rd.input.GLAccountID = caches.GLAccountIDForAccount(rd.input.AccountNumber)
				narration := cat.ProcessNarration(rd.input.Description)
				result := cat.SmartCategorize(ctx, db, narration, filtered, caches, rd.input)
				persistItems[idx] = cat.BatchClassification{
					TxnID:     rd.input.TransactionID,
					Narration: narration,
					Result:    result,
				}
			}(i, rd)
		}
		wg.Wait()

		// Collect affected bank-statement IDs (serial, safe).
		for _, rd := range batch {
			if rd.bsID != "" {
				affectedBS[rd.bsID] = struct{}{}
			}
		}

		// ── 5. Persist batch ──────────────────────────────────────────────────
		cat.PersistBatch(ctx, db, persistItems)
		totalUpdated += len(persistItems)
		totalProcessed += len(batch)

		// small progress log
		if time.Since(progressAt) > 30*time.Second {
			logger.LogAudit("Recategorization progress: processed %d/%d, updated %d", totalProcessed, totalCount, totalUpdated)
			progressAt = time.Now()
		}
	}

	// ── 6. Insert audit approval rows for affected bank statements ────────────
	if len(affectedBS) > 0 {
		pgxBatch := &pgx.Batch{}
		for bsID := range affectedBS {
			pgxBatch.Queue(`
				INSERT INTO cimplrcorpsaas.auditactionbankstatement
				    (bankstatementid, actiontype, processing_status, requested_by, requested_at)
				VALUES ($1, 'RECAT', 'PENDING_EDIT_APPROVAL', 'system', now())
				ON CONFLICT DO NOTHING
			`, bsID)
		}
		br := db.SendBatch(ctx, pgxBatch)
		for range affectedBS {
			br.Exec() //nolint:errcheck
		}
		br.Close()
	}

	auditLog(fmt.Sprintf("Smart-categorization completed: processed=%d updated=%d", totalProcessed, totalUpdated))
	return nil
}

// parseInt is a helper to parse int from string.
func parseInt(s string) (int, error) {
	var result int
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}
