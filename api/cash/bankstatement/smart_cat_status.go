package bankstatement

import (
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

		// ── 1. Transaction totals ─────────────────────────────────────
		var totalTxns, categorized, uncategorized int
		entityFilter := ""
		entityArgs := []interface{}{}
		if req.EntityID != "" {
			entityFilter = ` AND bs.entity_id = $1`
			entityArgs = append(entityArgs, req.EntityID)
		}

		_ = pool.QueryRow(ctx, `
			SELECT
				COUNT(*),
				COUNT(category_id),
				COUNT(*) FILTER (WHERE category_id IS NULL)
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
			WHERE COALESCE(bs.is_deleted, false) = false`+entityFilter,
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
				SELECT DISTINCT ON (transaction_id)
					transaction_id, classification_step
				FROM cimplrcorpsaas.classification_audit_log
				WHERE transaction_id IN (
					SELECT t.transaction_id
					FROM cimplrcorpsaas.bank_statement_transactions t
					JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
					WHERE COALESCE(bs.is_deleted, false) = false
				)
				ORDER BY transaction_id, classified_at DESC
			)
			SELECT classification_step, COUNT(*)
			FROM latest
			GROUP BY classification_step
		`)
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
			FROM cimplrcorpsaas.bank_statement_transactions t
			JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
			WHERE t.classification_step IS NOT NULL
			  AND COALESCE(bs.is_deleted, false) = false
		`).Scan(&highConf, &medConf, &lowConf)

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
			WHERE COALESCE(bs.is_deleted, false) = false
			GROUP BY q.status
		`)
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

		// ── Build response ────────────────────────────────────────────
		resp := map[string]interface{}{
			"success": true,
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
			"last_run":               lastRunStr,
			"last_run_ago_mins":      round2(lastRunAgo),
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(resp)
	})
}

func round2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
