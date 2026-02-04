package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"CimplrCorpSaas/internal/config"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
)

// CategorizationConfig holds configuration for auto-categorization processing
type CategorizationConfig struct {
	Schedule  string // Cron schedule (default: "0 18 * * *" for 6 PM daily)
	BatchSize int    // Number of transactions to process per batch
	TimeZone  string // Timezone for scheduling
}

// categoryRuleComponentForJob represents a single rule component for categorization
// This mirrors the struct used in bankstatUplV2.go but adds EffectiveDate field for cron job date filtering
type categoryRuleComponentForJob struct {
	RuleID         int64
	Priority       int
	CategoryID     string
	CategoryName   string
	CategoryType   string
	ComponentType  string
	MatchType      sql.NullString
	MatchValue     sql.NullString
	AmountOperator sql.NullString
	AmountValue    sql.NullFloat64
	TxnFlow        sql.NullString
	CurrencyCode   sql.NullString
	EffectiveDate  sql.NullTime // Rule effective date - only for cron job date filtering
}

// categorizationUpdate represents a transaction that needs its category updated
type categorizationUpdate struct {
	txnID      int64
	categoryID string
	bsID       string
}

// NewDefaultCategorizationConfig creates a new CategorizationConfig with default values
func NewDefaultCategorizationConfig() *CategorizationConfig {
	schedule := os.Getenv("CATEGORIZATION_SCHEDULE")
	if schedule == "" {
		schedule = "0 18 * * *" // Default: 6 PM daily
	}

	batchSize := 500
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

// RunCategorizationScheduler starts the cron job for automated transaction categorization
func RunCategorizationScheduler(cfg *CategorizationConfig, db *pgxpool.Pool) error {
	if cfg.Schedule == "" {
		cfg.Schedule = "0 18 * * *" // 6 PM daily
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 500
	}
	if cfg.TimeZone == "" {
		cfg.TimeZone = config.DefaultTimeZone
	}

	loc, err := time.LoadLocation(cfg.TimeZone)
	if err != nil {
		loc = time.UTC
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Invalid timezone %s, falling back to UTC: %v", cfg.TimeZone, err))
	}

	c := cron.New(cron.WithLocation(loc))

	_, err = c.AddFunc(cfg.Schedule, func() {
		logger.GlobalLogger.LogAudit(fmt.Sprintf("Starting auto-categorization job at %s", time.Now().In(loc).Format(time.RFC3339)))
		err := ProcessUncategorizedTransactions(db, cfg.BatchSize)
		if err != nil {
			logger.GlobalLogger.LogAudit(fmt.Sprintf("Auto-categorization job failed: %v", err))
			log.Printf("ERROR: Auto-categorization job failed: %v", err)
		} else {
			logger.GlobalLogger.LogAudit("Auto-categorization job completed successfully")
		}
	})

	if err != nil {
		return fmt.Errorf("unable to schedule auto-categorization processor: %v", err)
	}

	c.Start()
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Auto-categorization scheduler started with schedule: %s (timezone: %s)", cfg.Schedule, cfg.TimeZone))
	log.Printf("[AUDIT] Auto-categorization scheduler started: %s (%s)", cfg.Schedule, cfg.TimeZone)

	return nil
}

// ProcessUncategorizedTransactions is the main categorization job that processes uncategorized bank statement transactions
// based on rules with effective_date logic:
// - If rule has effective_date AND transaction_date >= effective_date: apply rule
// - If rule has effective_date AND transaction_date < effective_date: skip rule
// - If rule has NO effective_date (NULL): apply rule regardless of transaction date
// batchSize controls how many transactions are updated in a single bulk UPDATE (not how many are processed)
func ProcessUncategorizedTransactions(db *pgxpool.Pool, batchSize int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	startTime := time.Now()
	logger.GlobalLogger.LogAudit("Auto-categorization: Starting to count uncategorized transactions")

	// Step 1: Get database connection
	pgDB := db.Config().ConnConfig.Database
	pgUser := db.Config().ConnConfig.User
	pgPass := db.Config().ConnConfig.Password
	pgHost := db.Config().ConnConfig.Host
	pgPort := db.Config().ConnConfig.Port

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", pgUser, pgPass, pgHost, pgPort, pgDB)
	sqlDB, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open sql.DB connection: %w", err)
	}
	defer sqlDB.Close()

	// Count total uncategorized transactions
	var totalCount int
	countQuery := `SELECT COUNT(*) FROM cimplrcorpsaas.bank_statement_transactions WHERE category_id IS NULL AND bank_statement_id IS NOT NULL`
	err = sqlDB.QueryRowContext(ctx, countQuery).Scan(&totalCount)
	if err != nil {
		return fmt.Errorf("failed to count uncategorized transactions: %w", err)
	}

	if totalCount == 0 {
		logger.GlobalLogger.LogAudit("No uncategorized transactions found")
		return nil
	}

	log.Printf("[AUDIT] Total uncategorized transactions: %d", totalCount)
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Found %d total uncategorized transactions to process", totalCount))

	log.Printf("[AUDIT] Total uncategorized transactions: %d", totalCount)
	logger.GlobalLogger.LogAudit(fmt.Sprintf("Found %d total uncategorized transactions to process", totalCount))

	// Step 2: Load ALL active rules ONCE (avoid N+1 query problem)
	log.Println("[AUDIT] Loading all active categorization rules...")
	allRules, err := loadAllCategoryRules(ctx, sqlDB)
	if err != nil {
		return fmt.Errorf("failed to load category rules: %w", err)
	}
	log.Printf("[AUDIT] Loaded %d rule components across all scopes", len(allRules))

	// Step 3: Process ALL transactions in batches
	type txnRow struct {
		id            int64
		bsID          string
		accountNumber string
		entityID      string
		description   string
		withdrawalAmt sql.NullFloat64
		depositAmt    sql.NullFloat64
		valueDate     sql.NullTime
		currency      sql.NullString
	}

	offset := 0
	totalProcessed := 0
	totalCategorized := 0
	bsSet := make(map[string]struct{})
	lastLogTime := time.Now()

	// Set batch size for fetching/updating (default 5000 if not specified)
	if batchSize <= 0 {
		batchSize = 5000
	}

	log.Printf("[AUDIT] Starting batch processing (batch size: %d)...", batchSize)

	for {
		// Fetch next batch of uncategorized transactions
		query := `
			SELECT 
				t.transaction_id,
				t.bank_statement_id,
				bs.account_number,
				bs.entity_id,
				COALESCE(t.description, '') AS description,
				t.withdrawal_amount,
				t.deposit_amount,
				t.value_date,
				m.currency
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
			LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
			WHERE t.category_id IS NULL
				AND t.bank_statement_id IS NOT NULL
			ORDER BY t.value_date DESC
			LIMIT $1 OFFSET $2
		`

		rows, err := sqlDB.QueryContext(ctx, query, batchSize, offset)
		if err != nil {
			return fmt.Errorf("failed to query uncategorized transactions at offset %d: %w", offset, err)
		}

		var txns []txnRow
		for rows.Next() {
			var tr txnRow
			if err := rows.Scan(&tr.id, &tr.bsID, &tr.accountNumber, &tr.entityID, &tr.description, &tr.withdrawalAmt, &tr.depositAmt, &tr.valueDate, &tr.currency); err != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf("Failed to scan transaction row: %v", err))
				continue
			}
			txns = append(txns, tr)
		}
		rows.Close()

		if len(txns) == 0 {
			break // No more transactions to process
		}

		log.Printf("[AUDIT] Processing batch: transactions %d-%d of %d", offset+1, offset+len(txns), totalCount)

		// Match categories for this batch (in-memory processing)
		categorized := make([]categorizationUpdate, 0, len(txns))
		batchCategorized := 0

		for idx, tr := range txns {
			totalProcessed++

			// Filter applicable rules for this transaction
			applicableRules := filterRulesForTransaction(allRules, tr.accountNumber, tr.entityID, tr.currency)

			// Match category using effective_date logic
			matched := matchCategoryForTransactionWithEffectiveDate(applicableRules, tr.description, tr.withdrawalAmt, tr.depositAmt, tr.valueDate)

			if matched.Valid && strings.TrimSpace(matched.String) != "" {
				categorized = append(categorized, categorizationUpdate{
					txnID:      tr.id,
					categoryID: matched.String,
					bsID:       tr.bsID,
				})
				batchCategorized++
				totalCategorized++
				bsSet[tr.bsID] = struct{}{}
			}
			// Note: Transactions that don't match any rule remain uncategorized (no counter needed)

			// Progress logging every 1000 transactions or every 10 seconds
			if (totalProcessed)%1000 == 0 || time.Since(lastLogTime) > 10*time.Second {
				elapsed := time.Since(startTime)
				rate := float64(totalProcessed) / elapsed.Seconds()
				remaining := totalCount - totalProcessed
				eta := time.Duration(float64(remaining)/rate) * time.Second
				progress := fmt.Sprintf("⏳ Progress: %d/%d processed (%d matched, %.1f txns/sec, ETA: %v)",
					totalProcessed, totalCount, totalCategorized, rate, eta.Round(time.Second))
				log.Println(progress)
				logger.GlobalLogger.LogAudit(progress)
				lastLogTime = time.Now()
			}

			_ = idx // unused
		}

		// Bulk update this batch
		if len(categorized) > 0 {
			log.Printf("💾 Bulk updating %d categorized transactions in this batch...", len(categorized))
			err = bulkUpdateCategories(ctx, sqlDB, categorized)
			if err != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf("Bulk update failed for batch at offset %d, falling back to individual updates: %v", offset, err))
				// Fallback to individual updates if bulk fails
				for _, cat := range categorized {
					updateQuery := `UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id = $1 WHERE transaction_id = $2`
					_, err := sqlDB.ExecContext(ctx, updateQuery, cat.categoryID, cat.txnID)
					if err != nil {
						logger.GlobalLogger.LogAudit(fmt.Sprintf("Failed to update transaction %d: %v", cat.txnID, err))
					}
				}
			} else {
				log.Printf("✅ Batch update successful (%d categorized)", batchCategorized)
			}
		}

		offset += len(txns)

		// Safety check: if we processed fewer than batchSize, we're done
		if len(txns) < batchSize {
			break
		}
	}

	log.Printf("[AUDIT] All batches processed!")

	// Step 4: Insert pending edit approval for affected bank statements
	if len(bsSet) > 0 {
		log.Printf("[AUDIT]Creating audit entries for %d affected bank statements...", len(bsSet))
		for bsID := range bsSet {
			auditQuery := `INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at) VALUES ($1, 'EDIT', 'PENDING_EDIT_APPROVAL', 'AUTO_CATEGORIZATION_JOB', $2)`
			_, err = sqlDB.ExecContext(ctx, auditQuery, bsID, time.Now())
			if err != nil {
				logger.GlobalLogger.LogAudit(fmt.Sprintf("Failed to create audit entry for bank statement %s: %v", bsID, err))
			}
		}
	}

	duration := time.Since(startTime)
	uncategorized := totalCount - totalCategorized
	summary := fmt.Sprintf("🎉 Auto-categorization completed: %d/%d transactions categorized (%.1f%%), %d remain uncategorized, %d bank statements affected (Duration: %v, Avg: %.1f txns/sec)",
		totalCategorized, totalCount, float64(totalCategorized)/float64(totalCount)*100, uncategorized, len(bsSet), duration, float64(totalCount)/duration.Seconds())
	logger.GlobalLogger.LogAudit(summary)
	log.Println(summary)

	return nil
}

// loadCategoryRuleComponentsForJob fetches all active rule components for a given account/entity/currency scope
// INCLUDING the effective_date for each rule
func loadCategoryRuleComponentsForJob(ctx context.Context, db *sql.DB, accountNumber, entityID string, accountCurrency *string) ([]categoryRuleComponentForJob, error) {
	query := `
		SELECT 
			r.rule_id, 
			r.priority, 
			r.category_id, 
			c.category_name, 
			c.category_type, 
			comp.component_type, 
			comp.match_type, 
			comp.match_value, 
			comp.amount_operator, 
			comp.amount_value, 
			comp.txn_flow, 
			comp.currency_code,
			r.effective_date
		FROM cimplrcorpsaas.category_rules r
		JOIN public.mastercashflowcategory c ON r.category_id = c.category_id
		JOIN cimplrcorpsaas.category_rule_components comp ON r.rule_id = comp.rule_id AND comp.is_active = true
		JOIN cimplrcorpsaas.rule_scope s ON r.scope_id = s.scope_id
		WHERE r.is_active = true
			AND (
				(s.scope_type = 'ACCOUNT' AND s.account_number = $1)
				OR (s.scope_type = 'ENTITY' AND s.entity_id = $2)
				OR (s.scope_type = 'BANK' AND s.bank_code IS NOT NULL)
				OR (s.scope_type = 'CURRENCY' AND s.currency = $3)
				OR (s.scope_type = 'GLOBAL')
			)
		ORDER BY r.priority ASC, r.rule_id ASC, comp.component_id ASC
	`

	var acctCurrencyParam interface{}
	if accountCurrency != nil {
		acctCurrencyParam = *accountCurrency
	} else {
		acctCurrencyParam = nil
	}

	rows, err := db.QueryContext(ctx, query, accountNumber, entityID, acctCurrencyParam)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rules []categoryRuleComponentForJob
	for rows.Next() {
		var rc categoryRuleComponentForJob
		if err := rows.Scan(&rc.RuleID, &rc.Priority, &rc.CategoryID, &rc.CategoryName, &rc.CategoryType, &rc.ComponentType, &rc.MatchType, &rc.MatchValue, &rc.AmountOperator, &rc.AmountValue, &rc.TxnFlow, &rc.CurrencyCode, &rc.EffectiveDate); err != nil {
			return nil, err
		}
		rules = append(rules, rc)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return rules, nil
}

// matchCategoryForTransactionWithEffectiveDate applies the rule components to a single transaction
// and returns the matched category_id.
//
// This function EXACTLY mirrors matchCategoryForTransaction() from bankstatUplV2.go, but ADDS
// effective_date checking:
// - If rule.effective_date IS NULL: apply the rule (always eligible)
// - If rule.effective_date IS NOT NULL AND transaction.value_date >= rule.effective_date: apply the rule
// - If rule.effective_date IS NOT NULL AND transaction.value_date < rule.effective_date: skip this rule component
//
// Matching logic (from bankstatUplV2.go):
// - NARRATION_LOGIC: Match description using CONTAINS/EQUALS/STARTS_WITH/ENDS_WITH
// - AMOUNT_LOGIC: Match amount using >/>=/</<=/= operators
// - TRANSACTION_LOGIC: Match Outflow/Inflow flow
// - First matching component wins (OR logic, not AND)
// - If no rule matches, returns sql.NullString{Valid: false} and transaction remains uncategorized
func matchCategoryForTransactionWithEffectiveDate(rules []categoryRuleComponentForJob, description string, withdrawal, deposit sql.NullFloat64, txnValueDate sql.NullTime) sql.NullString {
	matchedCategoryID := sql.NullString{Valid: false}
	descLower := strings.ToLower(description)

	for _, rule := range rules {
		// CRON JOB ADDITION: Check effective_date before applying rule
		// If effective_date is set, transaction value_date must be >= effective_date
		if rule.EffectiveDate.Valid && txnValueDate.Valid {
			if txnValueDate.Time.Before(rule.EffectiveDate.Time) {
				// Transaction date is before rule's effective date - skip this rule component
				continue
			}
		}
		// If effective_date is NULL, rule applies to all transactions (no date filtering)

		// NARRATION_LOGIC (exact copy from bankstatUplV2.go)
		if rule.ComponentType == "NARRATION_LOGIC" && rule.MatchType.Valid && rule.MatchValue.Valid {
			val := strings.ToLower(rule.MatchValue.String)
			switch rule.MatchType.String {
			case "CONTAINS", "ILIKE":
				if strings.Contains(descLower, val) {
					matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
				}
			case "EQUALS":
				if descLower == val {
					matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
				}
			case "STARTS_WITH":
				if strings.HasPrefix(descLower, val) {
					matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
				}
			case "ENDS_WITH":
				if strings.HasSuffix(descLower, val) {
					matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
				}
			case "REGEX":
				// Regex not implemented in original logic
			}
		}

		// AMOUNT_LOGIC (exact copy from bankstatUplV2.go - applies to both withdrawal and deposit)
		if !matchedCategoryID.Valid && rule.ComponentType == "AMOUNT_LOGIC" && rule.AmountOperator.Valid && rule.AmountValue.Valid {
			amounts := []float64{}
			if withdrawal.Valid {
				amounts = append(amounts, withdrawal.Float64)
			}
			if deposit.Valid {
				amounts = append(amounts, deposit.Float64)
			}
			for _, amt := range amounts {
				switch rule.AmountOperator.String {
				case ">":
					if amt > rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
					}
				case ">=":
					if amt >= rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
					}
				case "=":
					if amt == rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
					}
				case "<=":
					if amt <= rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
					}
				case "<":
					if amt < rule.AmountValue.Float64 {
						matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
					}
				}
			}
		}

		// TRANSACTION_LOGIC (Outflow/Inflow)
		if !matchedCategoryID.Valid && rule.ComponentType == "TRANSACTION_LOGIC" && rule.TxnFlow.Valid {
			if rule.TxnFlow.String == "Outflow" && withdrawal.Valid && withdrawal.Float64 > 0 {
				matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
			}
			if rule.TxnFlow.String == "Inflow" && deposit.Valid && deposit.Float64 > 0 {
				matchedCategoryID = sql.NullString{String: rule.CategoryID, Valid: true}
			}
		}

		// CURRENCY_CONDITION and other component types are ignored (same as bankstatUplV2.go)

		// If we found a match, validate category_type matches transaction type
		if matchedCategoryID.Valid {
			// Validate category_type matches transaction type
			if rule.CategoryType == "Outflow" && (!withdrawal.Valid || withdrawal.Float64 <= 0) {
				matchedCategoryID = sql.NullString{Valid: false} // Reset - Outflow category can't match deposit
				continue
			}
			if rule.CategoryType == "Inflow" && (!deposit.Valid || deposit.Float64 <= 0) {
				matchedCategoryID = sql.NullString{Valid: false} // Reset - Inflow category can't match withdrawal
				continue
			}
			// Both type is allowed for both withdrawals and deposits
			break
		}
	}

	return matchedCategoryID
}

// parseInt is a helper to parse int from string
func parseInt(s string) (int, error) {
	var result int
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}

// loadAllCategoryRules loads ALL active category rules across all scopes in a single query
// This avoids N+1 query problem by loading everything upfront
func loadAllCategoryRules(ctx context.Context, db *sql.DB) ([]categoryRuleComponentForJob, error) {
	query := `
		SELECT 
			r.rule_id, 
			r.priority, 
			r.category_id, 
			c.category_name, 
			c.category_type, 
			comp.component_type, 
			comp.match_type, 
			comp.match_value, 
			comp.amount_operator, 
			comp.amount_value, 
			comp.txn_flow, 
			comp.currency_code,
			r.effective_date,
			s.scope_type,
			s.account_number,
			s.entity_id,
			s.bank_code,
			s.currency
		FROM cimplrcorpsaas.category_rules r
		JOIN public.mastercashflowcategory c ON r.category_id = c.category_id
		JOIN cimplrcorpsaas.category_rule_components comp ON r.rule_id = comp.rule_id AND comp.is_active = true
		JOIN cimplrcorpsaas.rule_scope s ON r.scope_id = s.scope_id
		WHERE r.is_active = true
		ORDER BY r.priority ASC, r.rule_id ASC, comp.component_id ASC
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type enrichedRule struct {
		categoryRuleComponentForJob
		ScopeType     string
		ScopeAccount  sql.NullString
		ScopeEntity   sql.NullString
		ScopeBank     sql.NullString
		ScopeCurrency sql.NullString
	}

	var enrichedRules []enrichedRule
	for rows.Next() {
		var er enrichedRule
		if err := rows.Scan(
			&er.RuleID, &er.Priority, &er.CategoryID, &er.CategoryName, &er.CategoryType,
			&er.ComponentType, &er.MatchType, &er.MatchValue, &er.AmountOperator,
			&er.AmountValue, &er.TxnFlow, &er.CurrencyCode, &er.EffectiveDate,
			&er.ScopeType, &er.ScopeAccount, &er.ScopeEntity, &er.ScopeBank, &er.ScopeCurrency,
		); err != nil {
			return nil, err
		}
		enrichedRules = append(enrichedRules, er)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Convert to regular rules (scope filtering happens in filterRulesForTransaction)
	var rules []categoryRuleComponentForJob
	for _, er := range enrichedRules {
		// Store scope info in CategoryType field temporarily (hack for in-memory filtering)
		// Format: "TYPE|SCOPE|ACCOUNT|ENTITY|BANK|CURRENCY"
		scopeData := fmt.Sprintf("%s|%s|%s|%s|%s|%s",
			er.CategoryType,
			er.ScopeType,
			nullStringValue(er.ScopeAccount),
			nullStringValue(er.ScopeEntity),
			nullStringValue(er.ScopeBank),
			nullStringValue(er.ScopeCurrency),
		)
		er.CategoryType = scopeData
		rules = append(rules, er.categoryRuleComponentForJob)
	}

	return rules, nil
}

// filterRulesForTransaction filters the global rule list to only those applicable to this transaction
func filterRulesForTransaction(allRules []categoryRuleComponentForJob, accountNumber, entityID string, currency sql.NullString) []categoryRuleComponentForJob {
	var applicable []categoryRuleComponentForJob

	currencyStr := ""
	if currency.Valid {
		currencyStr = strings.TrimSpace(currency.String)
	}

	for _, rule := range allRules {
		// Parse scope data from CategoryType field (format: TYPE|SCOPE|ACCOUNT|ENTITY|BANK|CURRENCY)
		parts := strings.Split(rule.CategoryType, "|")
		if len(parts) != 6 {
			continue // Invalid format, skip
		}

		actualCategoryType := parts[0]
		scopeType := parts[1]
		scopeAccount := parts[2]
		scopeEntity := parts[3]
		// scopeBank := parts[4] // Not used for filtering yet
		scopeCurrency := parts[5]

		// Check if rule applies to this transaction
		applies := false
		switch scopeType {
		case "GLOBAL":
			applies = true
		case "ACCOUNT":
			applies = (scopeAccount == accountNumber)
		case "ENTITY":
			applies = (scopeEntity == entityID)
		case "CURRENCY":
			applies = (scopeCurrency == currencyStr)
		case "BANK":
			// For bank-level rules, we'd need bank_code from transaction
			// For now, include all BANK rules (conservative approach)
			applies = true
		}

		if applies {
			// Restore original CategoryType
			rule.CategoryType = actualCategoryType
			applicable = append(applicable, rule)
		}
	}

	return applicable
}

// bulkUpdateCategories performs a single bulk UPDATE using PostgreSQL arrays
func bulkUpdateCategories(ctx context.Context, db *sql.DB, updates []categorizationUpdate) error {
	if len(updates) == 0 {
		return nil
	}

	// Build arrays of transaction IDs and category IDs
	txnIDs := make([]int64, len(updates))
	categoryIDs := make([]string, len(updates))

	for i, u := range updates {
		txnIDs[i] = u.txnID
		categoryIDs[i] = u.categoryID
	}

	// Use PostgreSQL's UPDATE FROM with unnest()
	query := `
		UPDATE cimplrcorpsaas.bank_statement_transactions AS t
		SET category_id = u.category_id
		FROM (
			SELECT unnest($1::bigint[]) AS txn_id, unnest($2::text[]) AS category_id
		) AS u
		WHERE t.transaction_id = u.txn_id
	`

	_, err := db.ExecContext(ctx, query, pq.Array(txnIDs), pq.Array(categoryIDs))
	return err
}

// nullStringValue safely extracts string from sql.NullString
func nullStringValue(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}
