package bankstatement

// AiBulkSuggestHandler — POST /cash/smart-cat/ai-bulk-suggest
//
// Queries every PENDING UNALLOCATED row in the review queue, deduplicates by
// narration_clean, sends them to the AI in one batched call, and returns the
// suggestions grouped by narration pattern.
//
// Request  : { "user_id": "...", "entity_id": "..." }
// Response :
//
//	{
//	  "success": true,
//	  "suggestions": [
//	    {
//	      "narration_clean":   "BG AMENDMENT CHARGES",
//	      "category_id":       "CAT-001",
//	      "category_name":     "Bank Charges",
//	      "confidence":        0.93,
//	      "reasoning":         "BG amendment fees are bank service charges",
//	      "transaction_ids":   [447737, 447847, ...],
//	      "transaction_count": 15
//	    },
//	    ...
//	  ],
//	  "total_patterns":     22,
//	  "total_transactions": 135,
//	  "ai_configured":      true
//	}

import (
	"encoding/json"
	"net/http"
	"os"

	"CimplrCorpSaas/api/constants"
	cat "CimplrCorpSaas/internal/services/categorizer"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AiBulkSuggestHandler returns AI-generated category suggestions for all
// PENDING UNALLOCATED transactions in the review queue.
func AiBulkSuggestHandler(pool *pgxpool.Pool) http.Handler {
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

		aiConfigured := os.Getenv("SMART_CAT_AI_URL") != "" && os.Getenv("SMART_CAT_AI_KEY") != ""

		ctx := r.Context()

		// ── 1. Query all PENDING UNALLOCATED transactions ────────────────────
		var query string
		var args []interface{}
		if req.EntityID != "" {
			query = `
				SELECT q.transaction_id,
				       COALESCE(t.narration_clean, t.description, q.transaction_id::text),
				       COALESCE(t.payment_channel, 'UNKNOWN')
				FROM cimplrcorpsaas.categorization_review_queue q
				JOIN cimplrcorpsaas.bank_statement_transactions t ON t.transaction_id = q.transaction_id
				JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
				WHERE q.status = 'PENDING'
				  AND q.step = 'UNALLOCATED'
				  AND COALESCE(bs.is_deleted, false) = false
				  AND bs.entity_id = $1
				ORDER BY t.narration_clean`
			args = []interface{}{req.EntityID}
		} else {
			query = `
				SELECT q.transaction_id,
				       COALESCE(t.narration_clean, t.description, q.transaction_id::text),
				       COALESCE(t.payment_channel, 'UNKNOWN')
				FROM cimplrcorpsaas.categorization_review_queue q
				JOIN cimplrcorpsaas.bank_statement_transactions t ON t.transaction_id = q.transaction_id
				JOIN cimplrcorpsaas.bank_statements bs ON bs.bank_statement_id = t.bank_statement_id
				WHERE q.status = 'PENDING'
				  AND q.step = 'UNALLOCATED'
				  AND COALESCE(bs.is_deleted, false) = false
				ORDER BY t.narration_clean`
		}

		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			http.Error(w, "failed to query review queue", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// ── 2. Group by narration_clean ───────────────────────────────────────
		type narGroup struct {
			channel string
			txnIDs  []int64
		}
		grouped := make(map[string]*narGroup)
		var order []string // preserve insertion order for stable output

		for rows.Next() {
			var txnID int64
			var narration, channel string
			if err := rows.Scan(&txnID, &narration, &channel); err != nil {
				continue
			}
			if _, exists := grouped[narration]; !exists {
				grouped[narration] = &narGroup{channel: channel}
				order = append(order, narration)
			}
			grouped[narration].txnIDs = append(grouped[narration].txnIDs, txnID)
		}
		rows.Close()

		totalTxns := 0
		for _, g := range grouped {
			totalTxns += len(g.txnIDs)
		}

		// If AI is not configured, return empty suggestions with a flag
		if !aiConfigured || len(order) == 0 {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":            true,
				"suggestions":        []interface{}{},
				"total_patterns":     len(order),
				"total_transactions": totalTxns,
				"ai_configured":      aiConfigured,
			})
			return
		}

		// ── 3. Build input slice ──────────────────────────────────────────────
		inputs := make([]cat.BulkNarrationInput, 0, len(order))
		for _, nar := range order {
			g := grouped[nar]
			inputs = append(inputs, cat.BulkNarrationInput{
				NarrationClean: nar,
				PaymentChannel: g.channel,
				TransactionIDs: g.txnIDs,
			})
		}

		// ── 4. Call AI ────────────────────────────────────────────────────────
		suggestions, err := cat.BulkClassifyNarrations(ctx, pool, inputs)
		if err != nil {
			w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success":       false,
				"error":         err.Error(),
				"ai_configured": aiConfigured,
			})
			return
		}

		// ── 5. Respond ────────────────────────────────────────────────────────
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":            true,
			"suggestions":        suggestions,
			"total_patterns":     len(suggestions),
			"total_transactions": totalTxns,
			"ai_configured":      aiConfigured,
		})
	})
}
