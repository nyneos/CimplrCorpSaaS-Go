package jobs

import (
	"context"
	"fmt"
	"log"
	"os"
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
		auditLog(fmt.Sprintf("Starting smart-categorization job at %s", time.Now().In(loc).Format(time.RFC3339)))
		if err := ProcessUncategorizedTransactions(db, cfg.BatchSize); err != nil {
			auditLog(fmt.Sprintf("Smart-categorization job failed: %v", err))
			log.Printf("ERROR: Smart-categorization job failed: %v", err)
		} else {
			auditLog("Smart-categorization job completed successfully")
		}
	})
	if err != nil {
		return fmt.Errorf("unable to schedule auto-categorization processor: %v", err)
	}

	c.Start()
	auditLog(fmt.Sprintf("Auto-categorization scheduler started: %s (%s)", cfg.Schedule, cfg.TimeZone))
	log.Printf("[AUDIT] Auto-categorization scheduler started: %s (%s)", cfg.Schedule, cfg.TimeZone)
	return nil
}

// ProcessUncategorizedTransactions performs a full smart recategorization of all
// bank statement transactions using the 6-step waterfall in internal/services/categorizer.
//
// Pipeline:
//  1. Load all active rules (once, in memory).
//  2. Load counterparty + GL caches (once).
//  3. Stream transactions in batches of batchSize ordered by value_date DESC.
//  4. For each batch: process narration → filter rules → run waterfall → collect results.
//  5. Persist via PersistBatch (pgx.Batch pipeline, 4 ops per item).
//  6. Track affected bank statements and insert audit approval rows.
func ProcessUncategorizedTransactions(db *pgxpool.Pool, batchSize int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
	defer cancel()

	// startTime := time.Now()
	// logger.GlobalLogger.LogAudit("Recategorization: Starting to count transactions")

	// pgDB := db.Config().ConnConfig.Database
	// pgUser := db.Config().ConnConfig.User
	// pgPass := db.Config().ConnConfig.Password
	// pgHost := db.Config().ConnConfig.Host
	// pgPort := db.Config().ConnConfig.Port
	// sslMode := os.Getenv("DB_SSLMODE")
	// if sslMode == "" {
	// 	sslMode = "disable"
	// }

	// connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", pgUser, pgPass, pgHost, pgPort, pgDB, sslMode)
	// sqlDB, err := sql.Open("postgres", connStr)
	// if err != nil {
	// 	return fmt.Errorf("failed to open sql.DB connection: %w", err)
	if batchSize <= 0 {
		batchSize = 1000
	}

	auditLog("Smart-categorization: loading rules and caches…")

	// ── 1. Pre-load rules and caches ──────────────────────────────────────────
	allRules, err := cat.LoadAllSmartRules(ctx, db)
	if err != nil {
		return fmt.Errorf("load rules: %w", err)
	}
	caches, err := cat.LoadSmartCaches(ctx, db)
	if err != nil {
		return fmt.Errorf("load caches: %w", err)
	}

	// ── 2. Count total transactions ───────────────────────────────────────────
	var totalCount int
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*) FROM cimplrcorpsaas.bank_statement_transactions
		WHERE bank_statement_id IS NOT NULL
	`).Scan(&totalCount); err != nil {
		return fmt.Errorf("count transactions: %w", err)
	}
	if totalCount == 0 {
		auditLog("No transactions found for smart-categorization")
		return nil
	}
	log.Printf("[AUDIT] Smart-categorization: %d transactions to process", totalCount)

	// ── 3. Batch loop ─────────────────────────────────────────────────────────
	offset := 0
	totalProcessed := 0
	totalUpdated := 0
	progressAt := time.Now()

	// Track affected bank-statement IDs to insert audit approval rows afterwards.
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
			ORDER BY t.value_date DESC
			LIMIT $1 OFFSET $2
		`, batchSize, offset)
		if err != nil {
			return fmt.Errorf("query batch at offset %d: %w", offset, err)
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

		// ── 4. Classify each transaction ──────────────────────────────────────
		persistItems := make([]cat.BatchClassification, 0, len(batch))
		for _, rd := range batch {
			filtered := cat.FilterRulesForTxn(allRules, rd.input.AccountNumber, rd.input.EntityID, rd.input.Currency)
			narration := cat.ProcessNarration(rd.input.Description)
			result := cat.SmartCategorize(ctx, db, narration, filtered, caches, rd.input)
			persistItems = append(persistItems, cat.BatchClassification{
				TxnID:     rd.input.TransactionID,
				Narration: narration,
				Result:    result,
			})
			if rd.bsID != "" {
				affectedBS[rd.bsID] = struct{}{}
			}
		}

		// ── 5. Persist batch ──────────────────────────────────────────────────
		cat.PersistBatch(ctx, db, persistItems)
		totalUpdated += len(persistItems)
		totalProcessed += len(batch)
		offset += len(batch)

		if time.Since(progressAt) > 30*time.Second {
			log.Printf("[AUDIT] Smart-categorization progress: processed=%d/%d updated=%d", totalProcessed, totalCount, totalUpdated)
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
