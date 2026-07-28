package categorizer

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ═══════════════════════════════════════════════════════════════
// RULE LOADING (Step 1 data)
// ═══════════════════════════════════════════════════════════════

// LoadAllSmartRules fetches ALL active rules with their scope.
// Call once at the start of a batch run; filter per-transaction with FilterRulesForTxn.
func LoadAllSmartRules(ctx context.Context, pool *pgxpool.Pool) ([]smartRuleComp, error) {
	const q = `
		SELECT
			r.rule_id, r.priority, r.category_id, c.category_name, c.category_type,
			comp.component_type, comp.match_type, comp.match_value,
			comp.amount_operator, comp.amount_value, comp.txn_flow, comp.currency_code,
			r.effective_date,
			s.scope_type, s.account_number, s.entity_id, s.bank_code, s.currency
		FROM cimplrcorpsaas.category_rules r
		JOIN public.mastercashflowcategory c    ON c.category_id = r.category_id
		JOIN cimplrcorpsaas.category_rule_components comp
		                                        ON comp.rule_id = r.rule_id AND comp.is_active = true
		JOIN cimplrcorpsaas.rule_scope s        ON s.scope_id  = r.scope_id
		WHERE r.is_active = true
		  AND COALESCE(r.rule_status, 'ACTIVE') = 'ACTIVE'
		ORDER BY r.priority ASC, r.rule_id ASC, comp.component_id ASC
	`
	rows, err := pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("load smart rules: %w", err)
	}
	defer rows.Close()

	var rules []smartRuleComp
	for rows.Next() {
		var rc smartRuleComp
		if err := rows.Scan(
			&rc.RuleID, &rc.Priority, &rc.CategoryID, &rc.CategoryName, &rc.CategoryType,
			&rc.ComponentType, &rc.MatchType, &rc.MatchValue,
			&rc.AmountOperator, &rc.AmountValue, &rc.TxnFlow, &rc.CurrencyCode,
			&rc.EffectiveDate,
			&rc.ScopeType, &rc.ScopeAccount, &rc.ScopeEntity, &rc.ScopeBank, &rc.ScopeCurrency,
		); err != nil {
			return nil, fmt.Errorf("scan smart rule: %w", err)
		}
		rules = append(rules, rc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	log.Printf("[SMART-CAT] Loaded %d rule components", len(rules))
	return rules, nil
}

// FilterRulesForTxn narrows the global rule slice to those applicable
// to this transaction's account / entity / currency.
func FilterRulesForTxn(all []smartRuleComp, accountNumber, entityID, currency string) []smartRuleComp {
	out := make([]smartRuleComp, 0, len(all)/2)
	for _, r := range all {
		switch r.ScopeType {
		case "GLOBAL":
			out = append(out, r)
		case "ACCOUNT":
			if r.ScopeAccount != nil && *r.ScopeAccount == accountNumber {
				out = append(out, r)
			}
		case "ENTITY":
			if r.ScopeEntity != nil && *r.ScopeEntity == entityID {
				out = append(out, r)
			}
		case "CURRENCY":
			if r.ScopeCurrency != nil && *r.ScopeCurrency == currency {
				out = append(out, r)
			}
		case "BANK":
			// Include conservatively until bank_code is on each transaction
			out = append(out, r)
		}
	}
	return out
}

// ═══════════════════════════════════════════════════════════════
// CACHE LOADING (Steps 2 & 3 data)
// ═══════════════════════════════════════════════════════════════

// LoadSmartCaches preloads counterparty defaults and GL mapping into memory.
// Call once per batch run.
func LoadSmartCaches(ctx context.Context, pool *pgxpool.Pool) (*SmartCaches, error) {
	caches := &SmartCaches{
		CounterpartyByName: make(map[string]cpDefault),
		GLMapping:          make(map[string]glDefault),
		AccountDefaults:    make(map[string]accountDefault),
		AccountGLID:        make(map[string]string),
	}

	// Step 2 — counterparties with default_category_id
	cpRows, err := pool.Query(ctx, `
		SELECT LOWER(cp.counterparty_name), cp.counterparty_name,
		       cp.default_category_id, COALESCE(mc.category_name, '')
		FROM mastercounterparty cp
		LEFT JOIN public.mastercashflowcategory mc ON mc.category_id = cp.default_category_id
		WHERE cp.default_category_id IS NOT NULL
		  AND (cp.is_deleted = false OR cp.is_deleted IS NULL)
	`)
	if err != nil {
		return nil, fmt.Errorf("load counterparty cache: %w", err)
	}
	defer cpRows.Close()
	for cpRows.Next() {
		var key, name, catID, catName string
		if err := cpRows.Scan(&key, &name, &catID, &catName); err != nil {
			return nil, err
		}
		caches.CounterpartyByName[key] = cpDefault{CPName: name, CategoryID: catID, CatName: catName}
	}
	if err := cpRows.Err(); err != nil {
		return nil, err
	}

	// Step 3 — GL mapping
	glRows, err := pool.Query(ctx, `
		SELECT gm.gl_account_id, gm.category_id, COALESCE(mc.category_name, '')
		FROM cimplrcorpsaas.gl_category_mapping gm
		LEFT JOIN public.mastercashflowcategory mc ON mc.category_id = gm.category_id
	`)
	if err != nil {
		return nil, fmt.Errorf("load GL cache: %w", err)
	}
	defer glRows.Close()
	for glRows.Next() {
		var glID, catID, catName string
		if err := glRows.Scan(&glID, &catID, &catName); err != nil {
			return nil, err
		}
		caches.GLMapping[glID] = glDefault{CategoryID: catID, CatName: catName}
	}
	if err := glRows.Err(); err != nil {
		return nil, err
	}

	// Step 6 — account-level category defaults from masterbankaccount
	// cat_inflow, cat_outflow, cat_charges, cat_int_inc, cat_int_exp
	accRows, err := pool.Query(ctx, `
		SELECT
			account_number,
			COALESCE(cat_inflow, ''),
			COALESCE(cat_outflow, ''),
			COALESCE(cat_charges, ''),
			COALESCE(cat_int_inc, ''),
			COALESCE(cat_int_exp, '')
		FROM public.masterbankaccount
		WHERE is_deleted = false
		  AND (cat_inflow IS NOT NULL OR cat_outflow IS NOT NULL
		       OR cat_charges IS NOT NULL OR cat_int_inc IS NOT NULL
		       OR cat_int_exp IS NOT NULL)
	`)
	if err != nil {
		return nil, fmt.Errorf("load account defaults: %w", err)
	}
	defer accRows.Close()
	for accRows.Next() {
		var acctNum, catIn, catOut, catChg, catII, catIE string
		if err := accRows.Scan(&acctNum, &catIn, &catOut, &catChg, &catII, &catIE); err != nil {
			return nil, err
		}
		caches.AccountDefaults[acctNum] = accountDefault{
			CatInflow:  catIn,
			CatOutflow: catOut,
			CatCharges: catChg,
			CatIntInc:  catII,
			CatIntExp:  catIE,
		}
	}
	if err := accRows.Err(); err != nil {
		return nil, err
	}

	// Step 3 ERP enrichment — GL account IDs for cash/bank accounts.
	// masterglaccount is a standalone master (PK: gl_account_id like GLA-XXXXXXX).
	// We read it via the erp_ref column which stores the bank account number when
	// the GL account represents a bank/cash account (is_cash_bank = TRUE).
	// This is non-fatal: if masterglaccount is not yet linked the engine still runs.
	glIDRows, glErr := pool.Query(ctx, `
		SELECT COALESCE(erp_ref, ''), gl_account_id
		FROM public.masterglaccount
		WHERE is_cash_bank = TRUE
		  AND (is_deleted = FALSE OR is_deleted IS NULL)
		  AND erp_ref IS NOT NULL
		  AND erp_ref <> ''
	`)
	if glErr != nil {
		// Non-fatal: log and continue without GL enrichment.
		log.Printf("[SMART-CAT] GL enrichment unavailable (masterglaccount query): %v", glErr)
	} else {
		defer glIDRows.Close()
		for glIDRows.Next() {
			var acctNum, glID string
			if err := glIDRows.Scan(&acctNum, &glID); err == nil {
				caches.AccountGLID[acctNum] = glID
			}
		}
	}

	log.Printf("[SMART-CAT] Caches: %d counterparties, %d GL mappings, %d account defaults, %d account GL IDs",
		len(caches.CounterpartyByName), len(caches.GLMapping), len(caches.AccountDefaults), len(caches.AccountGLID))
	return caches, nil
}

// ═══════════════════════════════════════════════════════════════
// SMART CATEGORIZE — main waterfall (pure, no DB writes)
// ═══════════════════════════════════════════════════════════════

// SmartCategorize runs the full classification waterfall for one transaction.
// It does NOT write to the database. Call PersistClassification or PersistBatch afterwards.
//
// Waterfall order (BRD Section 5.2):
//  1. Rule matching       → confidence 1.00
//  2. Counterparty lookup → confidence 0.95 (in-memory)
//  3. GL code lookup      → confidence 0.95 (in-memory)
//  4. Correction memory   → confidence 0.85–0.95 (DB, pg_trgm)
//  5. Similarity match    → confidence derived from trigram score (DB, pg_trgm)
//  6. Unallocated         → confidence 0.00
func SmartCategorize(
	ctx context.Context,
	pool *pgxpool.Pool,
	narration NarrationResult,
	rules []smartRuleComp,
	caches *SmartCaches,
	txn TxnInput,
) ClassificationResult {
	// Step 1: Rule matching
	if r, ok := matchRules(rules, narration, txn); ok {
		return r
	}

	// Step 2: Counterparty lookup (in-memory, O(n) over CP names)
	if r, ok := lookupCPMemory(narration.Clean, caches); ok {
		return r
	}

	// Step 3: GL code lookup (in-memory map)
	if r, ok := lookupGLMemory(txn.GLAccountID, caches); ok {
		return r
	}

	// Step 4: Correction memory (DB, GIN-indexed)
	if r, ok := lookupCorrection(ctx, pool, narration.Clean, narration.Stemmed); ok {
		return r
	}

	// bestGuess tracks the highest-confidence result that fell below MinConfidenceForActuals.
	// It is returned as the final result when all deterministic steps fail, so the review
	// queue entry gets a non-NULL suggested_cat even for near-miss matches.
	var bestGuess *ClassificationResult

	// Step 5: Trigram similarity against confirmed transactions (DB, GIN-indexed)
	if r, ok := lookupSimilarity(ctx, pool, narration.Stemmed, txn.EntityID, txn); ok {
		if r.Confidence >= MinConfidenceForActuals {
			return r
		}
		// Below threshold — save as best guess for review queue
		cp := r
		bestGuess = &cp
	}

	// Step 6: Account-level default category (masterbankaccount.cat_inflow / cat_outflow etc.)
	// Confidence 0.75 — above threshold so the category IS set, but lower than specific matches.
	if r, ok := lookupAccountDefault(narration, txn, caches); ok {
		return r
	}

	// Step 9: AI / Intelligent Inference — gated by bindref.BrOn().
	// if r, ok := InferWithAI(ctx, pool, narration, txn); ok {
	// 	if r.Confidence >= MinConfidenceForActuals {
	// 		return r
	// 	}
	// 	if bestGuess == nil || r.Confidence > bestGuess.Confidence {
	// 		cp := r
	// 		bestGuess = &cp
	// 	}
	// }

	// Step 10: Return best low-confidence guess so the review queue has a suggested_cat.
	// This avoids NULL suggested_cat for near-miss transactions.
	if bestGuess != nil {
		return *bestGuess
	}

	// Nothing at all — truly unallocated
	return ClassificationResult{
		Step:       StepUnallocated,
		Confidence: 0,
		SourceRef:  "no_match",
	}
}

// ═══════════════════════════════════════════════════════════════
// PERSISTENCE
// ═══════════════════════════════════════════════════════════════

// PersistClassification writes one transaction's narration columns, category update,
// immutable audit log entry, and review-queue entry (if below threshold).
// Safe to call concurrently.
func PersistClassification(ctx context.Context, pool *pgxpool.Pool, txnID int64, narration NarrationResult, result ClassificationResult) {
	// Always update narration + classification metadata
	if _, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.bank_statement_transactions
		SET narration_clean     = $1,
		    narration_stemmed   = $2,
		    narration_ref       = $3,
		    payment_channel     = $4,
		    classification_step = $5,
		    confidence_score    = $6
		WHERE transaction_id = $7
	`, narration.Clean, narration.Stemmed, narration.Ref, string(narration.Channel),
		result.Step, result.Confidence, txnID); err != nil {
		log.Printf("[SMART-CAT] narration update txn=%d: %v", txnID, err)
	}

	// Set or clear category based on confidence
	if result.CategoryID != "" && result.Confidence >= MinConfidenceForActuals {
		if _, err := pool.Exec(ctx,
			`UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id=$1 WHERE transaction_id=$2`,
			result.CategoryID, txnID,
		); err != nil {
			log.Printf("[SMART-CAT] category update txn=%d: %v", txnID, err)
		}
	} else {
		if _, err := pool.Exec(ctx,
			`UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id=NULL WHERE transaction_id=$1`,
			txnID,
		); err != nil {
			log.Printf("[SMART-CAT] category clear txn=%d: %v", txnID, err)
		}
	}

	// Immutable audit log (never UPDATE/DELETE this table)
	if _, err := pool.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.classification_audit_log
		    (transaction_id, category_id, confidence, classification_step, rule_id, source_ref, classified_by)
		VALUES ($1, $2, $3, $4, $5, $6, 'system')
	`, txnID, nullStr(result.CategoryID), result.Confidence,
		result.Step, result.RuleID, result.SourceRef); err != nil {
		log.Printf("[SMART-CAT] audit log txn=%d: %v", txnID, err)
	}

	// Route to review queue when below threshold
	needsReview := result.Confidence < MinConfidenceForActuals || result.Step == StepUnallocated
	if needsReview {
		if _, err := pool.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.categorization_review_queue
			    (transaction_id, suggested_cat, confidence, step, status, created_at)
			VALUES ($1, $2, $3, $4, 'PENDING', now())
			ON CONFLICT (transaction_id) DO UPDATE
			    SET suggested_cat = EXCLUDED.suggested_cat,
			        confidence    = EXCLUDED.confidence,
			        step          = EXCLUDED.step,
			        status        = 'PENDING',
			        created_at    = now()
		`, txnID, nullStr(result.CategoryID), result.Confidence, result.Step); err != nil {
			log.Printf("[SMART-CAT] review queue txn=%d: %v", txnID, err)
		}
	} else {
		// Remove from queue if it was previously pending and is now confident
		_, _ = pool.Exec(ctx,
			`DELETE FROM cimplrcorpsaas.categorization_review_queue WHERE transaction_id=$1 AND status='PENDING'`,
			txnID)
	}
}

// PersistBatch sends all writes for a batch of classified transactions in a single
// PostgreSQL round-trip using pgx.Batch (pipeline). Dramatically faster than N individual
// calls in a batch job.
func PersistBatch(ctx context.Context, pool *pgxpool.Pool, items []BatchClassification) {
	PersistBatchWithOptions(ctx, pool, items, nil)
}

// PersistBatchWithOptions skips category_id writes for txn IDs listed in skipCategoryTxnIDs
// so async RECAT can run policy before mutating categories.
func PersistBatchWithOptions(ctx context.Context, pool *pgxpool.Pool, items []BatchClassification, skipCategoryTxnIDs map[int64]struct{}) {
	if len(items) == 0 {
		return
	}

	batch := &pgx.Batch{}
	for _, item := range items {
		// 1. Narration columns + step metadata
		batch.Queue(`
			UPDATE cimplrcorpsaas.bank_statement_transactions
			SET narration_clean=$1, narration_stemmed=$2, narration_ref=$3,
			    payment_channel=$4, classification_step=$5, confidence_score=$6
			WHERE transaction_id=$7`,
			item.Narration.Clean, item.Narration.Stemmed, item.Narration.Ref,
			string(item.Narration.Channel), item.Result.Step, item.Result.Confidence, item.TxnID,
		)

		// 2. Category update / clear (deferred when RECAT policy will gate the write)
		if _, skip := skipCategoryTxnIDs[item.TxnID]; skip {
			// narration + audit log only; category applied after EnforceSmartCatJob
		} else if item.Result.CategoryID != "" && item.Result.Confidence >= MinConfidenceForActuals {
			batch.Queue(
				`UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id=$1 WHERE transaction_id=$2`,
				item.Result.CategoryID, item.TxnID,
			)
		} else {
			batch.Queue(
				`UPDATE cimplrcorpsaas.bank_statement_transactions SET category_id=NULL WHERE transaction_id=$1`,
				item.TxnID,
			)
		}

		// 3. Immutable audit log
		batch.Queue(`
			INSERT INTO cimplrcorpsaas.classification_audit_log
			    (transaction_id, category_id, confidence, classification_step, rule_id, source_ref, classified_by)
			VALUES ($1,$2,$3,$4,$5,$6,'system')`,
			item.TxnID, nullStr(item.Result.CategoryID), item.Result.Confidence,
			item.Result.Step, item.Result.RuleID, item.Result.SourceRef,
		)

		// 4. Review queue
		if item.Result.Confidence < MinConfidenceForActuals || item.Result.Step == StepUnallocated {
			batch.Queue(`
				INSERT INTO cimplrcorpsaas.categorization_review_queue
				    (transaction_id, suggested_cat, confidence, step, status, created_at)
				VALUES ($1,$2,$3,$4,'PENDING',now())
				ON CONFLICT (transaction_id) DO UPDATE
				    SET suggested_cat=$2, confidence=$3, step=$4, status='PENDING', created_at=now()`,
				item.TxnID, nullStr(item.Result.CategoryID), item.Result.Confidence, item.Result.Step,
			)
		} else {
			batch.Queue(
				`DELETE FROM cimplrcorpsaas.categorization_review_queue WHERE transaction_id=$1 AND status='PENDING'`,
				item.TxnID,
			)
		}
	}

	br := pool.SendBatch(ctx, batch)
	n := batch.Len()
	for i := 0; i < n; i++ {
		if _, err := br.Exec(); err != nil {
			log.Printf("[SMART-CAT] batch persist item %d: %v", i, err)
		}
	}
	if err := br.Close(); err != nil {
		log.Printf("[SMART-CAT] batch close: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════
// STEP IMPLEMENTATIONS
// ═══════════════════════════════════════════════════════════════

// ── Step 1: Rule matching ─────────────────────────────────────

func matchRules(rules []smartRuleComp, narration NarrationResult, txn TxnInput) (ClassificationResult, bool) {
	descLower := strings.ToLower(narration.Clean)

	var refDate time.Time
	if txn.ValueDate != nil {
		refDate = *txn.ValueDate
	} else {
		refDate = time.Now()
	}

	i := 0
	for i < len(rules) {
		r := rules[i]
		rid := r.RuleID
		prio := r.Priority
		catID := r.CategoryID
		catName := r.CategoryName
		catType := r.CategoryType

		// Skip rule if not yet effective
		if r.EffectiveDate != nil && refDate.Before(*r.EffectiveDate) {
			for i < len(rules) && rules[i].RuleID == rid {
				i++
			}
			continue
		}

		// Evaluate all components of this rule (AND logic)
		ruleOK := true
		for i < len(rules) && rules[i].RuleID == rid {
			if !matchComponent(rules[i], descLower, narration.Channel, txn) {
				ruleOK = false
			}
			i++
		}

		if !ruleOK {
			continue
		}

		// Validate category direction vs transaction type
		if catType == "Outflow" && (txn.Withdrawal == nil || *txn.Withdrawal <= 0) {
			continue
		}
		if catType == "Inflow" && (txn.Deposit == nil || *txn.Deposit <= 0) {
			continue
		}

		ruleIDCopy := rid // avoid loop-variable capture
		return ClassificationResult{
			CategoryID:   catID,
			CategoryName: catName,
			Confidence:   1.0,
			Step:         StepRule,
			RuleID:       &ruleIDCopy,
			SourceRef:    fmt.Sprintf("rule_id=%d priority=%d", ruleIDCopy, prio),
		}, true
	}
	return ClassificationResult{}, false
}

func matchComponent(comp smartRuleComp, descLower string, channel IndianBankChannel, txn TxnInput) bool {
	switch comp.ComponentType {
	case "NARRATION_LOGIC":
		if comp.MatchType == nil || comp.MatchValue == nil {
			return false
		}
		val := strings.ToLower(*comp.MatchValue)
		switch *comp.MatchType {
		case "CONTAINS", "ILIKE":
			return strings.Contains(descLower, val)
		case "EQUALS":
			return descLower == val
		case "STARTS_WITH":
			return strings.HasPrefix(descLower, val)
		case "ENDS_WITH":
			return strings.HasSuffix(descLower, val)
		}
		return false

	case "AMOUNT_LOGIC":
		if comp.AmountOperator == nil || comp.AmountValue == nil {
			return false
		}
		var amounts []float64
		if txn.Withdrawal != nil {
			amounts = append(amounts, *txn.Withdrawal)
		}
		if txn.Deposit != nil {
			amounts = append(amounts, *txn.Deposit)
		}
		for _, amt := range amounts {
			switch *comp.AmountOperator {
			case ">":
				if amt > *comp.AmountValue {
					return true
				}
			case ">=":
				if amt >= *comp.AmountValue {
					return true
				}
			case "=":
				if amt == *comp.AmountValue {
					return true
				}
			case "<=":
				if amt <= *comp.AmountValue {
					return true
				}
			case "<":
				if amt < *comp.AmountValue {
					return true
				}
			}
		}
		return false

	case "TRANSACTION_LOGIC":
		if comp.TxnFlow == nil {
			return true // no directional constraint
		}
		switch strings.ToUpper(*comp.TxnFlow) {
		case "OUTFLOW", "DEBIT":
			return txn.Withdrawal != nil && *txn.Withdrawal > 0
		case "INFLOW", "CREDIT":
			return txn.Deposit != nil && *txn.Deposit > 0
		}
		return true

	case "CHANNEL_LOGIC":
		if comp.MatchValue == nil {
			return true
		}
		return strings.EqualFold(*comp.MatchValue, string(channel))
	}
	// Unknown component types do not disqualify a rule
	return true
}

// ── Step 2: Counterparty lookup (in-memory) ───────────────────
// Keys are iterated longest-first so a more-specific name (e.g. "HDFC BANK")
// always wins over a shorter subset (e.g. "HDFC"), making matches deterministic.

func lookupCPMemory(narrationClean string, caches *SmartCaches) (ClassificationResult, bool) {
	if caches == nil || len(caches.CounterpartyByName) == 0 {
		return ClassificationResult{}, false
	}
	lower := strings.ToLower(narrationClean)

	// Collect keys sorted by descending length for longest-match-first semantics.
	keys := make([]string, 0, len(caches.CounterpartyByName))
	for k := range caches.CounterpartyByName {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })

	for _, cpKey := range keys {
		if strings.Contains(lower, cpKey) {
			cp := caches.CounterpartyByName[cpKey]
			return ClassificationResult{
				CategoryID:   cp.CategoryID,
				CategoryName: cp.CatName,
				Confidence:   0.95,
				Step:         StepCounterparty,
				SourceRef:    fmt.Sprintf("counterparty=%s", cp.CPName),
			}, true
		}
	}
	return ClassificationResult{}, false
}

// ── Step 3: GL code lookup (in-memory) ───────────────────────

func lookupGLMemory(glAccountID string, caches *SmartCaches) (ClassificationResult, bool) {
	if caches == nil || glAccountID == "" {
		return ClassificationResult{}, false
	}
	if gd, ok := caches.GLMapping[glAccountID]; ok {
		return ClassificationResult{
			CategoryID:   gd.CategoryID,
			CategoryName: gd.CatName,
			Confidence:   0.95,
			Step:         StepGL,
			SourceRef:    fmt.Sprintf("gl_account=%s", glAccountID),
		}, true
	}
	return ClassificationResult{}, false
}

// ── Step 6: Account-level default category ────────────────────
// Uses masterbankaccount cat_inflow / cat_outflow / cat_charges / cat_int_inc / cat_int_exp.
// Priority order within the account defaults:
//  1. cat_charges — if narration suggests bank charges/commission/fee
//  2. cat_int_inc — if deposit and narration suggests interest
//  3. cat_int_exp — if withdrawal and narration suggests interest
//  4. cat_inflow  — any deposit with no more-specific match
//  5. cat_outflow — any withdrawal with no more-specific match

func lookupAccountDefault(narration NarrationResult, txn TxnInput, caches *SmartCaches) (ClassificationResult, bool) {
	if caches == nil || len(caches.AccountDefaults) == 0 {
		return ClassificationResult{}, false
	}
	ad, ok := caches.AccountDefaults[txn.AccountNumber]
	if !ok {
		return ClassificationResult{}, false
	}

	desc := strings.ToLower(narration.Clean)
	isDebit := txn.Withdrawal != nil && *txn.Withdrawal > 0
	isCredit := txn.Deposit != nil && *txn.Deposit > 0

	isCharge := strings.ContainsAny(desc, "") || // initialise
		strings.Contains(desc, "charge") ||
		strings.Contains(desc, "commission") ||
		strings.Contains(desc, "fee") ||
		strings.Contains(desc, "chg") ||
		strings.Contains(desc, "levy")

	isInterest := strings.Contains(desc, "interest") ||
		strings.Contains(desc, "int cr") ||
		strings.Contains(desc, "int dr") ||
		strings.Contains(desc, "int inc") ||
		strings.Contains(desc, "int exp")

	pick := func(catID, label string) (ClassificationResult, bool) {
		if catID == "" {
			return ClassificationResult{}, false
		}
		return ClassificationResult{
			CategoryID: catID,
			Confidence: 0.75,
			Step:       StepAccountDefault,
			SourceRef:  fmt.Sprintf("account_default=%s acct=%s", label, txn.AccountNumber),
		}, true
	}

	// 1. Bank charges/fees
	if isCharge && isDebit && ad.CatCharges != "" {
		return pick(ad.CatCharges, "cat_charges")
	}
	// 2. Interest income (credit)
	if isInterest && isCredit && ad.CatIntInc != "" {
		return pick(ad.CatIntInc, "cat_int_inc")
	}
	// 3. Interest expense (debit)
	if isInterest && isDebit && ad.CatIntExp != "" {
		return pick(ad.CatIntExp, "cat_int_exp")
	}
	// 4. General inflow
	if isCredit && ad.CatInflow != "" {
		return pick(ad.CatInflow, "cat_inflow")
	}
	// 5. General outflow
	if isDebit && ad.CatOutflow != "" {
		return pick(ad.CatOutflow, "cat_outflow")
	}

	return ClassificationResult{}, false
}

// ── Step 4: Correction memory (DB, pg_trgm) ──────────────────
// Exact match → confidence 0.95; fuzzy ≥0.72 → confidence scaled 0.85–0.95.

func lookupCorrection(ctx context.Context, pool *pgxpool.Pool, narrationClean, stemmed string) (ClassificationResult, bool) {
	var catID, catName string
	var sim float64

	err := pool.QueryRow(ctx, `
		SELECT cc.category_id,
		       COALESCE(mc.category_name, ''),
		       GREATEST(
		           CASE WHEN cc.narration_clean = $1 THEN 1.0::float8 ELSE 0::float8 END,
		           COALESCE(similarity(cc.narration_stemmed, $2), 0::float8)
		       ) AS sim
		FROM cimplrcorpsaas.categorization_corrections cc
		LEFT JOIN public.mastercashflowcategory mc ON mc.category_id = cc.category_id
		WHERE cc.is_active = TRUE
		  AND (
		        cc.narration_clean = $1
		        OR (cc.narration_stemmed IS NOT NULL AND similarity(cc.narration_stemmed, $2) > 0.72)
		  )
		ORDER BY sim DESC
		LIMIT 1
	`, narrationClean, stemmed).Scan(&catID, &catName, &sim)

	if err != nil {
		if err != pgx.ErrNoRows {
			log.Printf("[SMART-CAT] correction lookup: %v", err)
		}
		return ClassificationResult{}, false
	}

	conf := 0.85 + sim*0.10
	if conf > 0.98 {
		conf = 0.98
	}
	return ClassificationResult{
		CategoryID:   catID,
		CategoryName: catName,
		Confidence:   conf,
		Step:         StepCorrection,
		SourceRef:    fmt.Sprintf("correction_sim=%.2f", sim),
	}, true
}

// ── Step 5: Trigram similarity against confirmed transactions ─
// Only matches transactions flowing in the SAME direction (debit↔debit,
// credit↔credit) to prevent cross-direction misclassification.

func lookupSimilarity(ctx context.Context, pool *pgxpool.Pool, stemmed, entityID string, txn TxnInput) (ClassificationResult, bool) {
	if stemmed == "" {
		return ClassificationResult{}, false
	}
	var catID, catName string
	var sim float64

	// Direction filter: match only same-direction transactions so that
	// "NEFT FROM VENDOR" (credit) never misclassifies a debit payment.
	isDebit := txn.Withdrawal != nil && *txn.Withdrawal > 0

	err := pool.QueryRow(ctx, `
		SELECT t.category_id,
		       COALESCE(mc.category_name, ''),
		       similarity(t.narration_stemmed, $1)::FLOAT8 AS sim
		FROM cimplrcorpsaas.bank_statement_transactions t
		JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
		LEFT JOIN public.mastercashflowcategory mc ON mc.category_id = t.category_id
		WHERE bs.entity_id = $2
		  AND t.category_id IS NOT NULL
		  AND t.confidence_score >= 0.90
		  AND t.narration_stemmed IS NOT NULL
		  AND similarity(t.narration_stemmed, $1) > 0.70
		  AND (
		        ($3 = TRUE  AND t.withdrawal_amount > 0)
		     OR ($3 = FALSE AND t.deposit_amount    > 0)
		  )
		ORDER BY sim DESC
		LIMIT 1
	`, stemmed, entityID, isDebit).Scan(&catID, &catName, &sim)

	if err != nil {
		if err != pgx.ErrNoRows {
			log.Printf("[SMART-CAT] similarity lookup: %v", err)
		}
		return ClassificationResult{}, false
	}

	return ClassificationResult{
		CategoryID:   catID,
		CategoryName: catName,
		Confidence:   sim * 0.90, // similarity 0.70–1.0 → confidence 0.63–0.90
		Step:         StepSimilarity,
		SourceRef:    fmt.Sprintf("trgm_sim=%.2f", sim),
	}, true
}

// ─────────────────────────────────────────────────────────────
// Internal helper
// ─────────────────────────────────────────────────────────────

// nullStr returns nil for empty strings, *string otherwise.
// pgx v5 sends NULL for nil pointers, which is what we want for optional fields.
func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
