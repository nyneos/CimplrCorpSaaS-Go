package bankstatement

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SmartCatStatusHandler returns a live snapshot of the categorization engine.
// POST /cash/smart-cat/status
//
// Response shape (all fields always present):
//
//	{
//	  "success": true,
//	  "totals": {
//	    "transactions":   10000,
//	    "categorized":    9200,
//	    "uncategorized":  800,
//	    "categorized_pct": 92.0
//	  },
//	  "by_step": {
//	    "RULE":         7500,
//	    "COUNTERPARTY": 300,
//	    "GL":           100,
//	    "CORRECTION":   150,
//	    "SIMILARITY":   650,
//	    "UNALLOCATED":  800,
//	    "MANUAL":       200
//	  },
//	  "confidence": {
//	    "high_pct":   80.0,   // >= 0.90
//	    "medium_pct": 12.0,   // 0.70–0.89
//	    "low_pct":    8.0     // < 0.70
//	  },
//	  "review_queue": {
//	    "PENDING":   350,
//	    "CONFIRMED": 150,
//	    "CORRECTED": 100,
//	    "DISMISSED": 50
//	  },
//	  "correction_memory_size": 250,
//	  "last_run":    "2026-04-21T18:00:00Z",
//	  "last_run_ago_mins": 120
//	}
func SmartCatStatusHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			UserID   string `json:"user_id"`
			EntityID string `json:"entity_id,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			http.Error(w, constants.ErrMissingUserID, http.StatusBadRequest)
			return
		}

		ctx := r.Context()

		entityFilter := ""
		entityArgs := []interface{}{}
		if req.EntityID != "" {
			entityFilter = ` AND bs.entity_id = $1`
			entityArgs = append(entityArgs, req.EntityID)
		}

		txnScopeJoin := `
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
			WHERE COALESCE(bs.is_deleted, false) = false` + entityFilter

		// ── 1. Transaction totals ─────────────────────────────────────
		var totalTxns, categorized, uncategorized int

		_ = pool.QueryRow(ctx, `
			SELECT
				COUNT(*),
				COUNT(t.category_id),
				COUNT(*) FILTER (WHERE t.category_id IS NULL)
			`+txnScopeJoin,
			entityArgs...,
		).Scan(&totalTxns, &categorized, &uncategorized)

		categorizedPct := 0.0
		if totalTxns > 0 {
			categorizedPct = float64(categorized) / float64(totalTxns) * 100
		}

		// ── 2. By-step breakdown (from latest audit log entry per txn) ─
		byStep := map[string]int{
			"RULE": 0, "COUNTERPARTY": 0, "GL": 0,
			"CORRECTION": 0, "SIMILARITY": 0, "UNALLOCATED": 0,
			"ACCOUNT_DEFAULT": 0, "AI_INFERENCE": 0,
		}
		stepRows, err := pool.Query(ctx, `
			WITH latest AS (
				SELECT DISTINCT ON (cal.transaction_id)
					cal.transaction_id, cal.classification_step
				FROM cimplrcorpsaas.classification_audit_log cal
				JOIN cimplrcorpsaas.bank_statement_transactions t ON t.transaction_id = cal.transaction_id
				JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
				WHERE COALESCE(bs.is_deleted, false) = false`+entityFilter+`
				ORDER BY cal.transaction_id, cal.classified_at DESC
			)
			SELECT classification_step, COUNT(*)
			FROM latest
			WHERE classification_step IS NOT NULL AND classification_step <> ''
			GROUP BY classification_step
		`, entityArgs...)
		if err == nil {
			for stepRows.Next() {
				var step string
				var cnt int64
				if scanErr := stepRows.Scan(&step, &cnt); scanErr == nil {
					byStep[step] = int(cnt)
				} else {
					log.Printf("[SMART-CAT-STATUS] by_step scan error: %v", scanErr)
				}
			}
			if rowsErr := stepRows.Err(); rowsErr != nil {
				log.Printf("[SMART-CAT-STATUS] by_step rows error: %v", rowsErr)
			}
			stepRows.Close()
		} else {
			log.Printf("[SMART-CAT-STATUS] by_step query error: %v", err)
		}

		// ── 3. Confidence distribution ────────────────────────────────
		var highConf, medConf, lowConf int
		_ = pool.QueryRow(ctx, `
			SELECT
				COUNT(*) FILTER (WHERE t.confidence_score >= 0.90),
				COUNT(*) FILTER (WHERE t.confidence_score >= 0.70 AND t.confidence_score < 0.90),
				COUNT(*) FILTER (WHERE t.confidence_score < 0.70 AND t.confidence_score IS NOT NULL)
			`+txnScopeJoin+`
			  AND t.classification_step IS NOT NULL
		`, entityArgs...).Scan(&highConf, &medConf, &lowConf)

		classifiedTotal := highConf + medConf + lowConf
		highPct, medPct, lowPct := 0.0, 0.0, 0.0
		if classifiedTotal > 0 {
			highPct = float64(highConf) / float64(classifiedTotal) * 100
			medPct = float64(medConf) / float64(classifiedTotal) * 100
			lowPct = float64(lowConf) / float64(classifiedTotal) * 100
		}

		// ── 4. Review queue counts ────────────────────────────────────
		reviewQueue := map[string]int{
			"PENDING": 0, "CONFIRMED": 0, "CORRECTED": 0, "DISMISSED": 0,
		}
		rqRows, err := pool.Query(ctx, `
			SELECT q.status, COUNT(*)
			FROM cimplrcorpsaas.categorization_review_queue q
			JOIN cimplrcorpsaas.bank_statement_transactions t
			    ON t.transaction_id = q.transaction_id
			JOIN cimplrcorpsaas.bank_statements bs
			    ON bs.bank_statement_id = t.bank_statement_id
			WHERE COALESCE(bs.is_deleted, false) = false`+entityFilter+`
			GROUP BY q.status
		`, entityArgs...)
		if err == nil {
			for rqRows.Next() {
				var status string
				var cnt int64
				if scanErr := rqRows.Scan(&status, &cnt); scanErr == nil {
					reviewQueue[status] = int(cnt)
				} else {
					log.Printf("[SMART-CAT-STATUS] review_queue scan error: %v", scanErr)
				}
			}
			if rowsErr := rqRows.Err(); rowsErr != nil {
				log.Printf("[SMART-CAT-STATUS] review_queue rows error: %v", rowsErr)
			}
			rqRows.Close()
		} else {
			log.Printf("[SMART-CAT-STATUS] review_queue query error: %v", err)
		}

		// ── 5. Correction memory size ─────────────────────────────────
		var correctionCount int
		_ = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM cimplrcorpsaas.categorization_corrections WHERE is_active = TRUE`,
		).Scan(&correctionCount)

		// ── 6. Last run timestamp ─────────────────────────────────────
		var lastRunStr string
		var lastRunAgo float64
		var lastRun *time.Time

		var lastRunTS time.Time
		err = pool.QueryRow(ctx,
			`SELECT MAX(classified_at) FROM cimplrcorpsaas.classification_audit_log WHERE classified_by = 'system'`,
		).Scan(&lastRunTS)
		if err == nil && !lastRunTS.IsZero() {
			lastRun = &lastRunTS
			lastRunStr = lastRunTS.UTC().Format(time.RFC3339)
			lastRunAgo = time.Since(lastRunTS).Minutes()
		}
		_ = lastRun

		// ── 7. Top 10 categories × classification step heatmap ───────
		// Uses the category assigned at each waterfall step (audit log), not the
		// post-review transaction category, so RULE/COUNTERPARTY/GL assignments show correctly.
		stepKeys := []string{
			"RULE", "COUNTERPARTY", "GL", "CORRECTION",
			"SIMILARITY", "ACCOUNT_DEFAULT", "AI_INFERENCE", "UNALLOCATED",
		}
		type catStepKey struct {
			category string
			step     string
		}
		catStepCounts := map[catStepKey]int{}
		catTotals := map[string]int{}

		hmRows, hmErr := pool.Query(ctx, `
			WITH latest AS (
				SELECT DISTINCT ON (cal.transaction_id)
					cal.transaction_id,
					cal.classification_step,
					cal.category_id
				FROM cimplrcorpsaas.classification_audit_log cal
				JOIN cimplrcorpsaas.bank_statement_transactions t ON t.transaction_id = cal.transaction_id
				JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
				WHERE COALESCE(bs.is_deleted, false) = false`+entityFilter+`
				ORDER BY cal.transaction_id, cal.classified_at DESC
			),
			classified AS (
				SELECT
					l.transaction_id,
					l.classification_step,
					COALESCE(
						NULLIF(TRIM(l.category_id), ''),
						NULLIF(TRIM(q.suggested_cat), ''),
						NULLIF(TRIM(t.category_id::text), '')
					) AS effective_category_id
				FROM latest l
				JOIN cimplrcorpsaas.bank_statement_transactions t ON t.transaction_id = l.transaction_id
				LEFT JOIN cimplrcorpsaas.categorization_review_queue q ON q.transaction_id = l.transaction_id
				WHERE l.classification_step IS NOT NULL AND l.classification_step <> ''
			),
			cat_step AS (
				SELECT
					COALESCE(NULLIF(TRIM(mc.category_name), ''), 'Unallocated') AS category_name,
					c.classification_step,
					COUNT(*) AS cnt
				FROM classified c
				LEFT JOIN public.mastercashflowcategory mc
				    ON mc.category_id::text = c.effective_category_id
				GROUP BY category_name, c.classification_step
			),
			top_cats AS (
				SELECT category_name
				FROM cat_step
				GROUP BY category_name
				ORDER BY SUM(cnt) DESC
				LIMIT 10
			)
			SELECT cs.category_name, cs.classification_step, cs.cnt
			FROM cat_step cs
			JOIN top_cats tc ON tc.category_name = cs.category_name
			ORDER BY (
				SELECT SUM(cs2.cnt) FROM cat_step cs2 WHERE cs2.category_name = cs.category_name
			) DESC, cs.category_name, cs.classification_step
		`, entityArgs...)
		if hmErr == nil {
			for hmRows.Next() {
				var category, step string
				var cnt int64
				if scanErr := hmRows.Scan(&category, &step, &cnt); scanErr == nil {
					catStepCounts[catStepKey{category, step}] = int(cnt)
					catTotals[category] += int(cnt)
				}
			}
			if rowsErr := hmRows.Err(); rowsErr != nil {
				log.Printf("[SMART-CAT-STATUS] top_categories_heatmap rows error: %v", rowsErr)
			}
			hmRows.Close()
		} else {
			log.Printf("[SMART-CAT-STATUS] top_categories_heatmap query error: %v", hmErr)
		}

		type catTotal struct {
			name  string
			total int
		}
		rankedCats := make([]catTotal, 0, len(catTotals))
		for name, total := range catTotals {
			rankedCats = append(rankedCats, catTotal{name, total})
		}
		for i := 0; i < len(rankedCats); i++ {
			for j := i + 1; j < len(rankedCats); j++ {
				if rankedCats[j].total > rankedCats[i].total {
					rankedCats[i], rankedCats[j] = rankedCats[j], rankedCats[i]
				}
			}
		}

		topCategoriesHeatmap := make([]map[string]interface{}, 0, len(rankedCats))
		for _, cat := range rankedCats {
			row := map[string]interface{}{"category": cat.name}
			for _, step := range stepKeys {
				row[step] = catStepCounts[catStepKey{cat.name, step}]
			}
			topCategoriesHeatmap = append(topCategoriesHeatmap, row)
		}

		// ── Build response ────────────────────────────────────────────
		resp := map[string]interface{}{
			"totals": map[string]interface{}{
				"transactions":    totalTxns,
				"categorized":     categorized,
				"uncategorized":   uncategorized,
				"categorized_pct": round2(categorizedPct),
			},
			"by_step": byStep,
			"confidence": map[string]interface{}{
				"high_pct":   round2(highPct),
				"medium_pct": round2(medPct),
				"low_pct":    round2(lowPct),
			},
			"review_queue":           reviewQueue,
			"correction_memory_size": correctionCount,
			"heatmap_steps":          stepKeys,
			"top_categories_heatmap": topCategoriesHeatmap,
			"last_run":               lastRunStr,
			"last_run_ago_mins":      round2(lastRunAgo),
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", resp)
	})
}

func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
