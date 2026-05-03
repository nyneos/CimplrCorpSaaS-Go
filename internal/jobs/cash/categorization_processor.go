package jobs

import (
	"context"
	"database/sql"
	"fmt"
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
			logger.LogError("ERROR: Auto-categorization job failed: %v", err)
		} else {
			logger.GlobalLogger.LogAudit("Auto-categorization job completed successfully")
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

// ProcessUncategorizedTransactions is the main categorization job that processes uncategorized bank statement transactions
// based on rules with effective_date logic:
// - If rule has effective_date AND transaction_date >= effective_date: apply rule
// - If rule has effective_date AND transaction_date < effective_date: skip rule
// - If rule has NO effective_date (NULL): apply rule regardless of transaction date
// batchSize controls how many transactions are updated in a single bulk UPDATE (not how many are processed)
// ProcessUncategorizedTransactions used to process only uncategorized transactions.
// It now performs a full recategorization: scans all transactions (categorized and uncategorized),
// applies effective_date-aware matching and updates categories where the new match differs
// from the current value. This replaces the previous separate Recategorize job so cron
// and manual triggers continue to call this function.
func ProcessUncategorizedTransactions(db *pgxpool.Pool, batchSize int) error {
	// Adopt the recategorization behavior and longer timeout
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
	defer cancel()

	startTime := time.Now()
	logger.GlobalLogger.LogAudit("Recategorization: Starting to count transactions")

	pgDB := db.Config().ConnConfig.Database
	pgUser := db.Config().ConnConfig.User
	pgPass := db.Config().ConnConfig.Password
	pgHost := db.Config().ConnConfig.Host
	pgPort := db.Config().ConnConfig.Port
	sslMode := os.Getenv("DB_SSLMODE")
	if sslMode == "" {
		sslMode = "disable"
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", pgUser, pgPass, pgHost, pgPort, pgDB, sslMode)
	sqlDB, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open sql.DB connection: %w", err)
	}
	defer sqlDB.Close()

	var totalCount int
	countQuery := `SELECT COUNT(*) FROM cimplrcorpsaas.bank_statement_transactions WHERE bank_statement_id IS NOT NULL`
	if err := sqlDB.QueryRowContext(ctx, countQuery).Scan(&totalCount); err != nil {
		return fmt.Errorf("failed to count transactions: %w", err)
	}
	if totalCount == 0 {
		logger.GlobalLogger.LogAudit("No transactions found for recategorization")
		return nil
	}
	logger.LogAudit("Total transactions to consider for recategorization: %d", totalCount)

	// Load all rules once
	logger.LogAudit("Loading all active categorization rules for recategorization...")
	allRules, err := loadAllCategoryRules(ctx, sqlDB)
	if err != nil {
		return fmt.Errorf("failed to load category rules: %w", err)
	}

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
		currCategory  sql.NullString
	}

	offset := 0
	totalProcessed := 0
	totalUpdated := 0
	bsSet := make(map[string]struct{})

	if batchSize <= 0 {
		batchSize = 5000
	}

	for {
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
				m.currency,
				t.category_id
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements bs ON t.bank_statement_id = bs.bank_statement_id
			LEFT JOIN public.masterbankaccount m ON bs.account_number = m.account_number
			WHERE t.bank_statement_id IS NOT NULL
			ORDER BY t.value_date DESC
			LIMIT $1 OFFSET $2
		`

		rows, err := sqlDB.QueryContext(ctx, query, batchSize, offset)
		if err != nil {
			return fmt.Errorf("failed to query transactions at offset %d: %w", offset, err)
		}

		var txns []txnRow
		for rows.Next() {
			var tr txnRow
			if err := rows.Scan(&tr.id, &tr.bsID, &tr.accountNumber, &tr.entityID, &tr.description, &tr.withdrawalAmt, &tr.depositAmt, &tr.valueDate, &tr.currency, &tr.currCategory); err == nil {
				txns = append(txns, tr)
			}
		}
		rows.Close()
		if len(txns) == 0 {
			break
		}

		// For each transaction, filter applicable rules and match
		updates := make([]categorizationUpdate, 0)
		nullifyIDs := make([]int64, 0)

		for _, tr := range txns {
			applicable := filterRulesForTransaction(allRules, tr.accountNumber, tr.entityID, tr.currency)
			matched := matchCategoryForTransactionWithEffectiveDate(applicable, tr.description, tr.withdrawalAmt, tr.depositAmt, tr.valueDate)

			// If matched differs from current category (including NULL vs non-NULL), schedule update
			if matched.Valid {
				if !tr.currCategory.Valid || tr.currCategory.String != matched.String {
					updates = append(updates, categorizationUpdate{txnID: tr.id, categoryID: matched.String})
					bsSet[tr.bsID] = struct{}{}
				}
			} else {
				// matched invalid (no rule) — if current category exists, nullify it
				if tr.currCategory.Valid {
					nullifyIDs = append(nullifyIDs, tr.id)
					bsSet[tr.bsID] = struct{}{}
				}
			}
		}

		// Bulk update non-null categories
		if len(updates) > 0 {
			if err := bulkUpdateCategories(ctx, sqlDB, updates); err != nil {
				return fmt.Errorf("failed to bulk update categories: %w", err)
			}
			totalUpdated += len(updates)
		}
		// Nullify categories where needed
		if len(nullifyIDs) > 0 {
			_, err := sqlDB.ExecContext(ctx, `UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id = NULL WHERE transaction_id = ANY($1)`, pq.Array(nullifyIDs))
			if err != nil {
				return fmt.Errorf("failed to nullify categories: %w", err)
			}
			totalUpdated += len(nullifyIDs)
		}

		totalProcessed += len(txns)
		offset += len(txns)

		// small progress log
		if time.Since(startTime) > 30*time.Second {
			logger.LogAudit("Recategorization progress: processed %d/%d, updated %d", totalProcessed, totalCount, totalUpdated)
			startTime = time.Now()
		}
	}

	// insert pending edit approval for affected bank statements similar to existing job
	if len(bsSet) > 0 {
		sqlDb, err := sql.Open("postgres", connStr)
		if err == nil {
			// use a simple insert per bsID (transactional safety left to caller)
			for bsID := range bsSet {
				_, _ = sqlDb.ExecContext(ctx, `INSERT INTO cimplrcorpsaas.auditactionbankstatement (bankstatementid, actiontype, processing_status, requested_by, requested_at) VALUES ($1, 'RECAT', 'PENDING_EDIT_APPROVAL', $2, $3)`, bsID, "system", time.Now())
			}
			sqlDb.Close()
		}
	}

	logger.GlobalLogger.LogAudit(fmt.Sprintf("Recategorization completed: processed=%d updated=%d", totalProcessed, totalUpdated))
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
