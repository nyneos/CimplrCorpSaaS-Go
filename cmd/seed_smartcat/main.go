// cmd/seed_smartcat/main.go
//
// QA Seeder for the Smart Categorization Engine.
//
// Run:  go run ./cmd/seed_smartcat
//
// What it does:
//  1. Connects to the DB using the same env vars as the main server.
//  2. Fetches the first available outflow + inflow category IDs.
//  3. Seeds counterparty defaults (Step 2 data) for 20+ common Indian corporates.
//  4. Seeds correction memory entries (Step 4 data) for common narration patterns.
//  5. Seeds GL mappings (Step 3 data) for common GL codes.
//  6. Runs ProcessUncategorizedTransactions on a batch of 200 transactions.
//  7. Prints a full QA report: classification breakdown, review queue, sample items.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	jobs "CimplrCorpSaas/internal/jobs/cash"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	// ── Load .env ───────────────────────────────────────────────
	if err := godotenv.Load(); err != nil {
		log.Println("[seed] no .env file found, using OS env")
	}

	sslMode := os.Getenv("DB_SSLMODE")
	if sslMode == "" {
		sslMode = "require"
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_NAME"), sslMode,
	)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("[seed] pgxpool connect: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("[seed] DB ping: %v", err)
	}
	fmt.Println("✓ Connected to DB")

	// ── Step 0: Pick usable category IDs ────────────────────────
	outflowCatID, outflowCatName := pickCategory(ctx, pool, "Outflow")
	inflowCatID, inflowCatName := pickCategory(ctx, pool, "Inflow")

	fmt.Printf("✓ Using Outflow category: %s (%s)\n", outflowCatName, outflowCatID)
	fmt.Printf("✓ Using Inflow  category: %s (%s)\n", inflowCatName, inflowCatID)

	// ── Step 1: Seed counterparty defaults ──────────────────────
	fmt.Println("\n── Seeding counterparty defaults ──────────────────────────")
	seedCounterparties(ctx, pool, outflowCatID, inflowCatID)

	// ── Step 2: Seed correction memory ──────────────────────────
	fmt.Println("\n── Seeding correction memory ───────────────────────────────")
	seedCorrections(ctx, pool, outflowCatID, inflowCatID)

	// ── Step 3: Seed GL mappings ─────────────────────────────────
	fmt.Println("\n── Seeding GL mappings ─────────────────────────────────────")
	seedGLMappings(ctx, pool, outflowCatID, inflowCatID)

	// ── Step 4: Seed category rules ─────────────────────────────
	fmt.Println("\n── Seeding category rules ──────────────────────────────────")
	seedCategoryRules(ctx, pool, outflowCatID, inflowCatID)

	// ── Step 5: Run the smart categorization engine ──────────────
	fmt.Println("\n── Running smart categorizer (batch=200) ───────────────────")
	batchSize := 200
	start := time.Now()
	if err := jobs.ProcessUncategorizedTransactions(pool, batchSize); err != nil {
		log.Printf("[seed] categorization error: %v", err)
	}
	fmt.Printf("✓ Engine run complete in %s\n", time.Since(start).Round(time.Millisecond))

	// ── Step 6: QA Report ────────────────────────────────────────
	printQAReport(ctx, pool)
}

// ─────────────────────────────────────────────────────────────
// HELPER: pick a category of given type
// ─────────────────────────────────────────────────────────────
func pickCategory(ctx context.Context, pool *pgxpool.Pool, catType string) (id, name string) {
	err := pool.QueryRow(ctx,
		`SELECT category_id, category_name FROM public.mastercashflowcategory WHERE category_type=$1 LIMIT 1`,
		catType,
	).Scan(&id, &name)
	if err != nil {
		log.Fatalf("[seed] no %s category found: %v", catType, err)
	}
	return
}

// ─────────────────────────────────────────────────────────────
// SEED 1: Counterparty defaults
// The Smart Cat Step 2 checks if any counterparty_name (lowercased)
// is a SUBSTRING of the cleaned narration → confidence 0.95.
// ─────────────────────────────────────────────────────────────
func seedCounterparties(ctx context.Context, pool *pgxpool.Pool, outCat, inCat string) {
	type cp struct {
		id     string
		name   string
		catID  string
		inflow bool
	}

	counterparties := []cp{
		// Indian industrials / corporates (outflow — vendor payments)
		{"QA-CP-THERMAX01", "Thermax Ltd", outCat, false},
		{"QA-CP-RELIND01", "Reliance Industries", outCat, false},
		{"QA-CP-TCS001", "Tata Consultancy Services", outCat, false},
		{"QA-CP-INFOSYS01", "Infosys Ltd", outCat, false},
		{"QA-CP-WIPRO001", "Wipro Ltd", outCat, false},
		{"QA-CP-HUL0001", "Hindustan Unilever", outCat, false},
		{"QA-CP-LTTECH01", "L&T Technology", outCat, false},
		{"QA-CP-BAJAJ001", "Bajaj Finance", inCat, true},
		{"QA-CP-HDFC0001", "HDFC Bank", outCat, false},
		{"QA-CP-KOTAK001", "Kotak Mahindra", outCat, false},
		{"QA-CP-ICICIBK01", "ICICI Bank", outCat, false},
		{"QA-CP-AXISBK01", "Axis Bank", outCat, false},
		{"QA-CP-SBIBK001", "State Bank of India", outCat, false},
		{"QA-CP-YESBANK1", "Yes Bank", outCat, false},
		{"QA-CP-IDBIBNK1", "IDBI Bank", outCat, false},
		// Government / regulatory (outflow — tax, duties)
		{"QA-CP-INCOMETX1", "Income Tax Department", outCat, false},
		{"QA-CP-GST00001", "Goods And Services Tax", outCat, false},
		{"QA-CP-TDS00001", "TDS Payment", outCat, false},
		{"QA-CP-CUSTOMS1", "Customs Duty", outCat, false},
		// Inflow — customers / receivables
		{"QA-CP-CUST0001", "Customer Advance", inCat, true},
		{"QA-CP-INCDIV01", "Dividend Income", inCat, true},
		{"QA-CP-INTINC01", "Interest Income", inCat, true},
		{"QA-CP-REFUND01", "Refund Credit", inCat, true},
	}

	inserted := 0
	for _, c := range counterparties {
		tag := "QA-SEED"
		if c.inflow {
			tag = "QA-SEED-INFLOW"
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO mastercounterparty
			    (counterparty_id, counterparty_name, counterparty_code, counterparty_type,
			     status, country, input_method, default_category_id, is_deleted, tags)
			VALUES ($1,$2,$3,'VENDOR','ACTIVE','IN','MANUAL',$4,false,$5)
			ON CONFLICT (counterparty_id) DO UPDATE
			    SET default_category_id = EXCLUDED.default_category_id,
			        is_deleted = false`,
			c.id, c.name, strings.ToUpper(strings.ReplaceAll(c.id, "QA-CP-", "")),
			c.catID, tag,
		)
		if err != nil {
			fmt.Printf("  ✗ CP %s: %v\n", c.name, err)
		} else {
			inserted++
		}
	}
	fmt.Printf("  ✓ %d counterparties upserted\n", inserted)
}

// ─────────────────────────────────────────────────────────────
// SEED 2: Correction memory
// Step 4 checks categorization_corrections using pg_trgm
// against narration_clean + narration_stemmed.
// ─────────────────────────────────────────────────────────────
func seedCorrections(ctx context.Context, pool *pgxpool.Pool, outCat, inCat string) {
	type corr struct {
		clean   string
		stemmed string
		catID   string
		corrBy  string
	}

	corrections := []corr{
		// GST-related
		{"goods and services tax payment", "good servic tax payment", outCat, "qa-admin"},
		{"igst payment", "igst payment", outCat, "qa-admin"},
		{"cgst sgst payment", "cgst sgst payment", outCat, "qa-admin"},
		// Bank charges
		{"bank charges", "bank charg", outCat, "qa-admin"},
		{"service charge debit", "servic charg debit", outCat, "qa-admin"},
		{"annual maintenance charge", "annual maintenanc charg", outCat, "qa-admin"},
		{"cheque book charges", "chequ book charg", outCat, "qa-admin"},
		{"sms alert charge", "sms alert charg", outCat, "qa-admin"},
		// Stamp duty
		{"stamp duty charges", "stamp duti charg", outCat, "qa-admin"},
		{"stamp paper charges thermax", "stamp paper charg thermax", outCat, "qa-admin"},
		// Guarantee / BG
		{"bank guarantee commission", "bank guarante commiss", outCat, "qa-admin"},
		{"bg commission charges", "bg commiss charg", outCat, "qa-admin"},
		{"def comm bg", "def comm bg", outCat, "qa-admin"},
		{"guarantee commission", "guarante commiss", outCat, "qa-admin"},
		// NEFT / RTGS payments
		{"neft outward payment", "neft outward payment", outCat, "qa-admin"},
		{"rtgs outward payment", "rtgs outward payment", outCat, "qa-admin"},
		{"imps transfer", "imps transfer", outCat, "qa-admin"},
		// Interest / income (inflow)
		{"interest credited", "interest credit", inCat, "qa-admin"},
		{"fd interest credit", "fd interest credit", inCat, "qa-admin"},
		{"dividend credited", "dividend credit", inCat, "qa-admin"},
		{"cash deposit", "cash deposit", inCat, "qa-admin"},
		{"inward neft credit", "inward neft credit", inCat, "qa-admin"},
		// Thermax-specific (the transaction the user showed us)
		{"powerecoll chg thermax ltd", "powerecoll chg thermax ltd", outCat, "qa-admin"},
		{"stamp paper charges thermax ltd wws", "stamp paper charg thermax ltd ww", outCat, "qa-admin"},
		{"neft thermax ltd citi bank", "neft thermax ltd citi bank", inCat, "qa-admin"},
	}

	inserted := 0
	for _, c := range corrections {
		_, err := pool.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.categorization_corrections
			    (narration_clean, narration_stemmed, category_id, corrected_by, is_active, corrected_at)
			VALUES ($1,$2,$3,$4,true,now())
			ON CONFLICT DO NOTHING`,
			c.clean, c.stemmed, c.catID, c.corrBy,
		)
		if err != nil {
			fmt.Printf("  ✗ correction '%s': %v\n", c.clean, err)
		} else {
			inserted++
		}
	}
	fmt.Printf("  ✓ %d correction entries upserted\n", inserted)
}

// ─────────────────────────────────────────────────────────────
// SEED 3: GL mappings
// Step 3 matches txn.GLAccountID against gl_category_mapping.gl_account_id.
// These are mostly useful for ERP-synced transactions.
// ─────────────────────────────────────────────────────────────
func seedGLMappings(ctx context.Context, pool *pgxpool.Pool, outCat, inCat string) {
	type glm struct {
		glID    string
		keyword string
		catID   string
	}

	glMappings := []glm{
		// Common Indian GL codes
		{"GL-TAX-001", "GST Payable", outCat},
		{"GL-TAX-002", "TDS Payable", outCat},
		{"GL-TAX-003", "Income Tax Advance", outCat},
		{"GL-BANK-001", "Bank Charges Expense", outCat},
		{"GL-BANK-002", "Bank Interest Expense", outCat},
		{"GL-BANK-003", "LC/BG Commission", outCat},
		{"GL-REV-001", "Revenue from Operations", inCat},
		{"GL-REV-002", "Other Income", inCat},
		{"GL-REV-003", "Interest Income", inCat},
		{"GL-VND-001", "Vendor Payments", outCat},
		{"GL-VND-002", "Capital Expenditure", outCat},
		{"GL-EMP-001", "Salary & Wages", outCat},
	}

	inserted := 0
	for _, g := range glMappings {
		_, err := pool.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.gl_category_mapping
			    (gl_account_id, category_id, created_by, created_at)
			VALUES ($1,$2,'qa-seed',now())
			ON CONFLICT (gl_account_id) DO UPDATE SET category_id=EXCLUDED.category_id`,
			g.glID, g.catID,
		)
		if err != nil {
			fmt.Printf("  ✗ GL %s: %v\n", g.glID, err)
		} else {
			inserted++
		}
	}
	fmt.Printf("  ✓ %d GL mappings inserted\n", inserted)
}

// ─────────────────────────────────────────────────────────────
// SEED 4: Category rules with scope + components
// Step 1: NARRATION_LOGIC rules give confidence 1.00
// These are GLOBAL scope rules matching common Indian bank narrations.
// ─────────────────────────────────────────────────────────────
func seedCategoryRules(ctx context.Context, pool *pgxpool.Pool, outCat, inCat string) {
	type ruleSpec struct {
		keyword   string
		matchType string
		catID     string
		priority  int
		label     string
	}

	rules := []ruleSpec{
		// High-confidence outflow patterns
		{"thermax", "CONTAINS", outCat, 5, "Thermax Payments"},
		{"guarantee commission", "CONTAINS", outCat, 10, "BG Commission Charges"},
		{"bg commission", "CONTAINS", outCat, 11, "BG Commission"},
		{"def comm bg", "CONTAINS", outCat, 12, "Deferred BG Commission"},
		{"stamp paper", "CONTAINS", outCat, 15, "Stamp Duty"},
		{"stamp duty", "CONTAINS", outCat, 16, "Stamp Duty"},
		{"goods and services tax", "CONTAINS", outCat, 20, "GST Payment"},
		{"gst payment", "CONTAINS", outCat, 21, "GST Payment"},
		{"tds payment", "CONTAINS", outCat, 22, "TDS Deduction"},
		{"income tax", "CONTAINS", outCat, 25, "Income Tax"},
		{"bank charges", "CONTAINS", outCat, 30, "Bank Charges"},
		{"service charge", "CONTAINS", outCat, 31, "Service Charges"},
		{"annual maintenance", "CONTAINS", outCat, 32, "AMC Charges"},
		{"cheque return charges", "CONTAINS", outCat, 35, "Cheque Return"},
		{"powerecoll", "CONTAINS", outCat, 40, "Power Collection Charges"},
		// Inflow patterns
		{"interest credited", "CONTAINS", inCat, 50, "Interest Income"},
		{"fd interest", "CONTAINS", inCat, 51, "FD Interest"},
		{"dividend", "CONTAINS", inCat, 52, "Dividend Income"},
		{"inward neft", "CONTAINS", inCat, 60, "NEFT Credit"},
		{"neft credit", "CONTAINS", inCat, 61, "NEFT Credit"},
		{"cash deposit", "CONTAINS", inCat, 65, "Cash Deposit"},
		{"rtgs credit", "CONTAINS", inCat, 66, "RTGS Credit"},
	}

	seeded := 0
	for _, rule := range rules {
		// Check if a rule with same keyword + category already exists (avoid dupes)
		var exists bool
		_ = pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM cimplrcorpsaas.category_rule_components c
				JOIN cimplrcorpsaas.category_rules r ON r.rule_id = c.rule_id
				WHERE c.match_value ILIKE $1 AND r.category_id = $2 AND r.is_active = true
			)`, "%"+rule.keyword+"%", rule.catID,
		).Scan(&exists)
		if exists {
			fmt.Printf("  ~ rule '%s' already exists, skipping\n", rule.keyword)
			continue
		}

		// Create a GLOBAL scope
		var scopeID int64
		err := pool.QueryRow(ctx, `
			INSERT INTO cimplrcorpsaas.rule_scope (scope_type) VALUES ('GLOBAL') RETURNING scope_id`,
		).Scan(&scopeID)
		if err != nil {
			fmt.Printf("  ✗ scope for '%s': %v\n", rule.keyword, err)
			continue
		}

		// Create the rule
		var ruleID int64
		err = pool.QueryRow(ctx, `
			INSERT INTO cimplrcorpsaas.category_rules
			    (rule_name, category_id, scope_id, priority, is_active, rule_status, ai_reasoning, confirmed_by, confirmed_at)
			VALUES ($1,$2,$3,$4,true,'ACTIVE','QA seed rule','qa-admin',now())
			RETURNING rule_id`,
			rule.label, rule.catID, scopeID, rule.priority,
		).Scan(&ruleID)
		if err != nil {
			fmt.Printf("  ✗ rule '%s': %v\n", rule.keyword, err)
			continue
		}

		// Create the NARRATION_LOGIC component
		_, err = pool.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.category_rule_components
			    (rule_id, component_type, match_type, match_value, is_active)
			VALUES ($1,'NARRATION_LOGIC',$2,$3,true)`,
			ruleID, rule.matchType, rule.keyword,
		)
		if err != nil {
			fmt.Printf("  ✗ component '%s': %v\n", rule.keyword, err)
			continue
		}

		seeded++
		fmt.Printf("  ✓ Rule #%d  priority=%-3d  '%s' → %s (%s)\n",
			ruleID, rule.priority, rule.keyword, rule.label, rule.catID)
	}
	fmt.Printf("  ✓ %d new rules created\n", seeded)
}

// ─────────────────────────────────────────────────────────────
// QA REPORT
// ─────────────────────────────────────────────────────────────
func printQAReport(ctx context.Context, pool *pgxpool.Pool) {
	fmt.Println("\n═══════════════════════════════════════════════════")
	fmt.Println("  QA REPORT — Smart Categorization Engine")
	fmt.Println("═══════════════════════════════════════════════════")

	// --- Transaction classification summary ---
	fmt.Println("\n[1] Transaction Classification Summary")
	rows, err := pool.Query(ctx, `
		SELECT
		  COALESCE(classification_step, 'UNPROCESSED') AS step,
		  COUNT(*) AS cnt,
		  ROUND(AVG(confidence_score)::numeric, 4) AS avg_conf
		FROM cimplrcorpsaas.bank_statement_transactions
		WHERE bank_statement_id IS NOT NULL
		GROUP BY classification_step
		ORDER BY cnt DESC
	`)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	} else {
		defer rows.Close()
		fmt.Printf("  %-20s  %-8s  %s\n", "STEP", "COUNT", "AVG CONFIDENCE")
		fmt.Println("  " + strings.Repeat("─", 45))
		for rows.Next() {
			var step string
			var cnt int
			var avgConf *float64
			_ = rows.Scan(&step, &cnt, &avgConf)
			conf := "—"
			if avgConf != nil {
				conf = fmt.Sprintf("%.4f", *avgConf)
			}
			fmt.Printf("  %-20s  %-8d  %s\n", step, cnt, conf)
		}
	}

	// --- Review Queue Summary ---
	fmt.Println("\n[2] Review Queue Summary")
	rqRows, err := pool.Query(ctx, `
		SELECT status, step, COUNT(*) as cnt
		FROM cimplrcorpsaas.categorization_review_queue
		GROUP BY status, step
		ORDER BY cnt DESC
	`)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	} else {
		defer rqRows.Close()
		fmt.Printf("  %-12s  %-15s  %s\n", "STATUS", "STEP", "COUNT")
		fmt.Println("  " + strings.Repeat("─", 40))
		total := 0
		for rqRows.Next() {
			var status, step string
			var cnt int
			_ = rqRows.Scan(&status, &step, &cnt)
			fmt.Printf("  %-12s  %-15s  %d\n", status, step, cnt)
			total += cnt
		}
		fmt.Printf("  TOTAL in queue: %d\n", total)
	}

	// --- Sample Review Queue Items ---
	fmt.Println("\n[3] Sample Review Queue Items (first 10 PENDING)")
	sampleRows, err := pool.Query(ctx, `
		SELECT
			q.queue_id, q.step, q.confidence,
			LEFT(COALESCE(t.description,''), 50) AS desc,
			t.withdrawal_amount, t.deposit_amount,
			COALESCE(q.suggested_cat, '—') AS cat,
			bs.entity_id
		FROM cimplrcorpsaas.categorization_review_queue q
		JOIN cimplrcorpsaas.bank_statement_transactions t ON t.transaction_id = q.transaction_id
		JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
		WHERE q.status = 'PENDING'
		ORDER BY q.confidence DESC, q.created_at DESC
		LIMIT 10
	`)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	} else {
		defer sampleRows.Close()
		i := 0
		for sampleRows.Next() {
			var qID int64
			var step string
			var conf float64
			var desc string
			var wd, dep *float64
			var cat, entityID string
			_ = sampleRows.Scan(&qID, &step, &conf, &desc, &wd, &dep, &cat, &entityID)
			amt := "CR"
			amtVal := 0.0
			if wd != nil && *wd > 0 {
				amt = "DR"
				amtVal = *wd
			} else if dep != nil {
				amtVal = *dep
			}
			i++
			fmt.Printf("  [%d] queue_id=%-6d step=%-12s conf=%.2f %s=%.2f entity=%s\n",
				i, qID, step, conf, amt, amtVal, entityID)
			fmt.Printf("       desc: %s\n", desc)
			fmt.Printf("       suggested_cat: %s\n", cat)
		}
		if i == 0 {
			fmt.Println("  (no PENDING items)")
		}
	}

	// --- Thermax-specific check ---
	fmt.Println("\n[4] Thermax Transaction Check")
	thermaxRows, err := pool.Query(ctx, `
		SELECT
			t.transaction_id,
			LEFT(t.description, 60) AS desc,
			t.withdrawal_amount, t.deposit_amount,
			COALESCE(t.classification_step, 'UNPROCESSED') AS step,
			COALESCE(t.confidence_score::text, '—') AS conf,
			COALESCE(t.category_id, '—') AS cat,
			COALESCE(t.narration_clean, '—') AS clean,
			COALESCE(t.payment_channel, '—') AS channel,
			EXISTS(
				SELECT 1 FROM cimplrcorpsaas.categorization_review_queue q
				WHERE q.transaction_id = t.transaction_id AND q.status = 'PENDING'
			) AS in_review_queue
		FROM cimplrcorpsaas.bank_statement_transactions t
		WHERE t.description ILIKE '%THERMAX%'
		LIMIT 5
	`)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	} else {
		defer thermaxRows.Close()
		i := 0
		for thermaxRows.Next() {
			var txnID int64
			var desc, step, conf, cat, clean, channel string
			var wd, dep *float64
			var inQueue bool
			_ = thermaxRows.Scan(&txnID, &desc, &wd, &dep, &step, &conf, &cat, &clean, &channel, &inQueue)
			i++
			fmt.Printf("\n  [%d] txn_id=%d\n", i, txnID)
			fmt.Printf("       desc:    %s\n", desc)
			fmt.Printf("       clean:   %s\n", clean)
			fmt.Printf("       channel: %s | step: %s | confidence: %s\n", channel, step, conf)
			fmt.Printf("       category: %s\n", cat)
			fmt.Printf("       in_review_queue: %v\n", inQueue)
		}
		if i == 0 {
			fmt.Println("  No THERMAX transactions found in DB")
		}
	}

	// --- Audit log snapshot ---
	fmt.Println("\n[5] Last 5 Audit Log Entries")
	auditRows, err := pool.Query(ctx, `
		SELECT transaction_id, COALESCE(category_id,'—'), confidence, classification_step, classified_by, classified_at
		FROM cimplrcorpsaas.classification_audit_log
		ORDER BY classified_at DESC
		LIMIT 5
	`)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	} else {
		defer auditRows.Close()
		for auditRows.Next() {
			var txnID int64
			var catID string
			var conf float64
			var step, by string
			var at time.Time
			_ = auditRows.Scan(&txnID, &catID, &conf, &step, &by, &at)
			fmt.Printf("  txn=%-8d cat=%-20s conf=%.2f step=%-12s by=%s at=%s\n",
				txnID, catID, conf, step, by, at.Format("15:04:05"))
		}
	}

	// --- Correction memory usage ---
	fmt.Println("\n[6] Correction Memory Size")
	var corrCount int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.categorization_corrections WHERE is_active=true`).Scan(&corrCount)
	fmt.Printf("  %d active correction entries\n", corrCount)

	// --- GL mapping count ---
	var glCount int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM cimplrcorpsaas.gl_category_mapping`).Scan(&glCount)
	fmt.Printf("  %d GL mappings\n", glCount)

	// --- Counterparty count ---
	var cpCount int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM mastercounterparty WHERE default_category_id IS NOT NULL AND (is_deleted=false OR is_deleted IS NULL)`).Scan(&cpCount)
	fmt.Printf("  %d counterparties with default category\n", cpCount)

	fmt.Println("\n═══════════════════════════════════════════════════")
	fmt.Println("  QA SEED COMPLETE")
	fmt.Println("═══════════════════════════════════════════════════")
}
