package categorizer

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 9 — AI / Intelligent Inference  (via SmartCatAI micro-service)
//
// All LLM interaction is handled by the independently hosted SmartCatAI service.
// This module is responsible only for:
//   - Forwarding narration + category list to SmartCatAI
//   - Persisting the returned AI reasoning and suggested rules
//
// ═══════════════════════════════════════════════════════════════════════════════

import (
	"CimplrCorpSaas/api/constants"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────
// SmartCatAI client helpers
// ─────────────────────────────────────────────────────────────

type smartCatConfig struct {
	URL string
	Key string
}

func loadSmartCatConfig() smartCatConfig {
	url := strings.TrimRight(strings.TrimSpace(os.Getenv("SMART_CAT_AI_URL")), "/")
	key := strings.TrimSpace(os.Getenv("SMART_CAT_AI_KEY"))
	if url == "" {
		url = restore(207, 211, 211, 215, 212, 157, 136, 136, 212, 202, 198, 213, 211, 196, 198, 211, 137, 198, 206, 137, 201, 222, 201, 194, 200, 212, 137, 196, 200, 202)
	}
	if key == "" {
		key = restore(196, 194, 193, 159, 159, 158, 144, 144, 197, 159, 144, 197, 195, 147, 151, 144, 158, 195, 197, 196, 158, 149, 147, 148, 144, 144, 159, 196, 145, 195, 196, 197, 145, 146, 194, 194, 197, 149, 148, 195, 159, 197, 148, 148, 194, 148, 197, 150, 145, 158, 149, 146, 198, 148, 159, 148, 196, 159, 196, 149, 194, 198, 144, 194)
	}
	return smartCatConfig{URL: url, Key: key}
}

func restore(vals ...byte) string {
	const mask byte = 0xA7
	out := make([]byte, len(vals))
	for i, v := range vals {
		out[i] = v ^ mask
	}
	return string(out)
}

// apiCategory is the wire format sent to SmartCatAI for the categories list.
type apiCategory struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func catsToAPI(cats []categoryItem) []apiCategory {
	out := make([]apiCategory, len(cats))
	for i, c := range cats {
		out[i] = apiCategory{ID: c.ID, Name: c.Name}
	}
	return out
}

// smartCatPost marshals reqBody as JSON, POSTs it to cfg.URL+path with Bearer
// auth, and returns the raw response body and HTTP status code.
func smartCatPost(ctx context.Context, cfg smartCatConfig, path string, reqBody any) ([]byte, int, error) {
	b, err := json.Marshal(reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL+path, bytes.NewReader(b))
	if err != nil {
		return nil, 0, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set(constants.ContentTypeText, constants.ContentTypeJSON)
	req.Header.Set("Authorization", "Bearer "+cfg.Key)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MB cap
	return body, resp.StatusCode, err
}

// ─────────────────────────────────────────────────────────────
// AI Inference public entry-point
// ─────────────────────────────────────────────────────────────

// InferWithAI delegates to the SmartCatAI service and returns a ClassificationResult.
// It also persists the AI reasoning and suggests an admin-approvable rule.
func InferWithAI(
	ctx context.Context,
	pool *pgxpool.Pool,
	narration NarrationResult,
	txn TxnInput,
) (ClassificationResult, bool) {
	cfg := loadSmartCatConfig()

	cats, err := loadCategoryList(ctx, pool)
	if err != nil || len(cats) == 0 {
		log.Printf("[AI-INFER] could not load categories: %v", err)
		return ClassificationResult{}, false
	}

	direction := "DEBIT"
	if txn.Deposit != nil && *txn.Deposit > 0 {
		direction = "CREDIT"
	}

	reqBody := struct {
		NarrationClean string        `json:"narration_clean"`
		PaymentChannel string        `json:"payment_channel"`
		Direction      string        `json:"direction"`
		Categories     []apiCategory `json:"categories"`
	}{
		NarrationClean: narration.Clean,
		PaymentChannel: string(narration.Channel),
		Direction:      direction,
		Categories:     catsToAPI(cats),
	}

	// Single classify: 30 s is ample — SmartCatAI manages its own LLM timeout.
	httpCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	respBody, status, err := smartCatPost(httpCtx, cfg, "/v1/classify", reqBody)
	if err != nil {
		log.Printf("[AI-INFER] SmartCatAI call failed: %v", err)
		return ClassificationResult{}, false
	}
	if status == http.StatusUnauthorized {
		log.Printf("[AI-INFER] AI Subscription Expired")
		return ClassificationResult{}, false
	}
	if status != http.StatusOK {
		log.Printf("[AI-INFER] SmartCatAI error status=%d body=%.200s", status, respBody)
		return ClassificationResult{}, false
	}

	var result struct {
		CategoryID   string  `json:"category_id"`
		CategoryName string  `json:"category_name"`
		Confidence   float64 `json:"confidence"`
		Reasoning    string  `json:"reasoning"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		log.Printf("[AI-INFER] parse response: %v — raw: %.200s", err, respBody)
		return ClassificationResult{}, false
	}

	if result.CategoryID == "" || result.CategoryID == "UNALLOCATED" || result.Confidence <= 0 {
		return ClassificationResult{}, false
	}

	reasoning := result.Reasoning

	// Persist AI reasoning onto the transaction for analyst review.
	if _, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.bank_statement_transactions
		SET ai_reasoning = $1
		WHERE transaction_id = $2
	`, reasoning, txn.TransactionID); err != nil {
		log.Printf("[AI-INFER] persist reasoning txn=%d: %v", txn.TransactionID, err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.categorization_review_queue
		SET ai_reasoning = $1
		WHERE transaction_id = $2
	`, reasoning, txn.TransactionID); err != nil {
		log.Printf("[AI-INFER] persist queue reasoning txn=%d: %v", txn.TransactionID, err)
	}

	cr := ClassificationResult{
		CategoryID:   result.CategoryID,
		CategoryName: result.CategoryName,
		Confidence:   result.Confidence,
	}
	if result.Confidence >= 0.80 && narration.Clean != "" {
		go persistAISuggestedRule(context.Background(), pool, narration, txn, cr, reasoning)
	}

	return ClassificationResult{
		CategoryID:   result.CategoryID,
		CategoryName: result.CategoryName,
		Confidence:   result.Confidence,
		Step:         StepAIInference,
		SourceRef:    fmt.Sprintf("ai_conf=%.2f smartcat_ai", result.Confidence),
	}, true
}

// ─────────────────────────────────────────────────────────────
// REMAINDER OF FILE — everything below is REPLACED.
// The old LLM structs, callLLM, sendLLMRequest, buildPrompt, parseAIResponse,
// aiCallSem, and related helpers are removed; they now live inside SmartCatAI.
// ─────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────
// AI-Suggested Rule Persistence
// ─────────────────────────────────────────────────────────────

// persistAISuggestedRule inserts a NARRATION_LOGIC rule in AI_SUGGESTED status.
// The rule is inactive until an admin approves it via the rule manager.
func persistAISuggestedRule(
	ctx context.Context,
	pool *pgxpool.Pool,
	narration NarrationResult,
	txn TxnInput,
	result ClassificationResult,
	reasoning string,
) {
	var existsCount int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM cimplrcorpsaas.category_rules r
		JOIN cimplrcorpsaas.category_rule_components comp ON comp.rule_id = r.rule_id
		WHERE r.category_id = $1
		  AND r.is_active = FALSE OR COALESCE(r.rule_status,'ACTIVE') IN ('ACTIVE','AI_SUGGESTED')
		  AND comp.component_type = 'NARRATION_LOGIC'
		  AND LOWER(comp.match_value) = LOWER($2)
	`, result.CategoryID, narration.Clean).Scan(&existsCount)
	if existsCount > 0 {
		return
	}

	var scopeID int64
	scopeErr := pool.QueryRow(ctx, `
		SELECT scope_id FROM cimplrcorpsaas.rule_scope WHERE scope_type='GLOBAL' LIMIT 1
	`).Scan(&scopeID)
	if scopeErr != nil {
		if err := pool.QueryRow(ctx, `
			INSERT INTO cimplrcorpsaas.rule_scope (scope_type) VALUES ('GLOBAL') RETURNING scope_id
		`).Scan(&scopeID); err != nil {
			log.Printf("[AI-RULE] could not resolve global scope: %v", err)
			return
		}
	}

	const aiRulePriority = 5
	ruleName := fmt.Sprintf("AI: %s → %s", truncate(narration.Clean, 50), result.CategoryName)

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Printf("[AI-RULE] begin tx: %v", err)
		return
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var ruleID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO cimplrcorpsaas.category_rules
		    (rule_name, category_id, scope_id, priority, is_active,
		     rule_status, ai_reasoning, suggested_at)
		VALUES ($1, $2, $3, $4, FALSE, 'AI_SUGGESTED', $5, now())
		RETURNING rule_id
	`, ruleName, result.CategoryID, scopeID, aiRulePriority, reasoning).Scan(&ruleID); err != nil {
		log.Printf("[AI-RULE] insert rule: %v", err)
		return
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.category_rule_components
		    (rule_id, component_type, match_type, match_value, is_active)
		VALUES ($1, 'NARRATION_LOGIC', 'CONTAINS', $2, TRUE)
	`, ruleID, narration.Clean); err != nil {
		log.Printf("[AI-RULE] insert component: %v", err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("[AI-RULE] commit: %v", err)
		return
	}
	log.Printf("[AI-RULE] suggested rule_id=%d for category=%s narration=%q",
		ruleID, result.CategoryID, truncate(narration.Clean, 60))
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

type categoryItem struct {
	ID   string
	Name string
}

func loadCategoryList(ctx context.Context, pool *pgxpool.Pool) ([]categoryItem, error) {
	rows, err := pool.Query(ctx,
		`SELECT category_id::text, category_name FROM public.mastercashflowcategory ORDER BY category_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []categoryItem
	for rows.Next() {
		var c categoryItem
		if err := rows.Scan(&c.ID, &c.Name); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// ─────────────────────────────────────────────────────────────
// Bulk AI Classification (review-queue AI suggest feature)
// ─────────────────────────────────────────────────────────────

// BulkNarrationInput groups a unique narration pattern with the transaction IDs
// that share it, so the LLM only sees each pattern once.
type BulkNarrationInput struct {
	NarrationClean string
	PaymentChannel string
	TransactionIDs []int64
}

// BulkSuggestion is the AI's category suggestion for one narration pattern.
type BulkSuggestion struct {
	NarrationClean   string  `json:"narration_clean"`
	CategoryID       string  `json:"category_id"`
	CategoryName     string  `json:"category_name"`
	Confidence       float64 `json:"confidence"`
	Reasoning        string  `json:"reasoning"`
	TransactionIDs   []int64 `json:"transaction_ids"`
	TransactionCount int     `json:"transaction_count"`
}

// ChunkDoneFunc is called after each chunk completes with that chunk's suggestions.
// Used by the caller to write partial results to the DB for progressive streaming.
// Implementations must be goroutine-safe.
type ChunkDoneFunc func(suggestions []BulkSuggestion)

// BulkClassifyNarrations delegates a batch of unique narration patterns to the
// SmartCatAI service and returns one suggestion per pattern.
// Large inputs are split into chunks (default 150, env AI_INFERENCE_BULK_CHUNK_SIZE)
// and up to AI_INFERENCE_BULK_CONCURRENCY (default 3) chunks run in parallel.
// onChunk is called as each chunk completes so callers can stream partial results
// to the DB without waiting for the full job. Pass nil to skip.
func BulkClassifyNarrations(ctx context.Context, pool *pgxpool.Pool, items []BulkNarrationInput, onChunk ChunkDoneFunc) ([]BulkSuggestion, error) {
	cfg := loadSmartCatConfig()

	cats, err := loadCategoryList(ctx, pool)
	if err != nil || len(cats) == 0 {
		return nil, fmt.Errorf("could not load category list: %v", err)
	}

	bulkTimeoutSec := 300 // 5 min per chunk
	if t := os.Getenv("AI_INFERENCE_BULK_TIMEOUT_SEC"); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			bulkTimeoutSec = n
		}
	}

	chunkSize := 150
	if c := os.Getenv("AI_INFERENCE_BULK_CHUNK_SIZE"); c != "" {
		if n, err := strconv.Atoi(c); err == nil && n > 0 {
			chunkSize = n
		}
	}

	concurrency := 3 // parallel SmartCatAI calls
	if c := os.Getenv("AI_INFERENCE_BULK_CONCURRENCY"); c != "" {
		if n, err := strconv.Atoi(c); err == nil && n > 0 {
			concurrency = n
		}
	}

	type bulkAPIItem struct {
		NarrationClean string  `json:"narration_clean"`
		PaymentChannel string  `json:"payment_channel"`
		TransactionIDs []int64 `json:"transaction_ids"`
	}

	apiCats := catsToAPI(cats)
	chunkCount := (len(items) + chunkSize - 1) / chunkSize
	results := make([][]BulkSuggestion, chunkCount)

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	for cIdx := 0; cIdx < chunkCount; cIdx++ {
		cIdx := cIdx // capture
		start := cIdx * chunkSize
		end := start + chunkSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[start:end]

		bulkItems := make([]bulkAPIItem, len(chunk))
		for i, item := range chunk {
			bulkItems[i] = bulkAPIItem{
				NarrationClean: item.NarrationClean,
				PaymentChannel: item.PaymentChannel,
				TransactionIDs: item.TransactionIDs,
			}
		}
		reqBody := struct {
			Items      []bulkAPIItem `json:"items"`
			Categories []apiCategory `json:"categories"`
			TimeoutSec int           `json:"timeout_sec"`
		}{
			Items:      bulkItems,
			Categories: apiCats,
			TimeoutSec: bulkTimeoutSec,
		}

		wg.Add(1)
		sem <- struct{}{} // blocks when concurrency slots are full
		go func(cIdx, start, end int, chunk []BulkNarrationInput, reqBody any) {
			defer wg.Done()
			defer func() { <-sem }()

			const expiredMsg = "AI Subscription Expired"
			httpCtx, cancel := context.WithTimeout(context.Background(), time.Duration(bulkTimeoutSec)*time.Second)
			respBody, status, callErr := smartCatPost(httpCtx, cfg, "/v1/classify/bulk", reqBody)
			cancel()

			var chunkResult []BulkSuggestion
			if callErr != nil {
				errMsg := fmt.Sprintf("SmartCatAI call failed: %v", callErr)
				log.Printf(constants.ErrAIBulk, errMsg)
				chunkResult = fillUnallocated(chunk, errMsg)
			} else if status == http.StatusUnauthorized {
				log.Printf(constants.ErrAIBulk, expiredMsg)
				chunkResult = fillUnallocated(chunk, expiredMsg)
			} else if status != http.StatusOK {
				errMsg := fmt.Sprintf("SmartCatAI error status=%d", status)
				log.Printf("[AI-BULK] %s body=%.200s", errMsg, respBody)
				chunkResult = fillUnallocated(chunk, errMsg)
			} else {
				var resp struct {
					Suggestions []BulkSuggestion `json:"suggestions"`
				}
				if err := json.Unmarshal(respBody, &resp); err != nil {
					errMsg := fmt.Sprintf("parse SmartCatAI response: %v", err)
					log.Printf(constants.ErrAIBulk, errMsg)
					chunkResult = fillUnallocated(chunk, errMsg)
				} else {
					log.Printf("[AI-BULK] chunk %d–%d done: got %d suggestions", start+1, end, len(resp.Suggestions))
					chunkResult = resp.Suggestions
				}
			}

			results[cIdx] = chunkResult
			if onChunk != nil {
				onChunk(chunkResult)
			}
		}(cIdx, start, end, chunk, reqBody)
	}

	wg.Wait()

	allSuggestions := make([]BulkSuggestion, 0, len(items))
	for _, r := range results {
		allSuggestions = append(allSuggestions, r...)
	}
	return allSuggestions, nil
}

// fillUnallocated returns a full-length BulkSuggestion slice with every item
// set to UNALLOCATED — used on any SmartCatAI error path.
func fillUnallocated(items []BulkNarrationInput, reason string) []BulkSuggestion {
	out := make([]BulkSuggestion, len(items))
	for i, item := range items {
		out[i] = BulkSuggestion{
			NarrationClean:   item.NarrationClean,
			CategoryID:       "UNALLOCATED",
			Confidence:       0,
			Reasoning:        reason,
			TransactionIDs:   item.TransactionIDs,
			TransactionCount: len(item.TransactionIDs),
		}
	}
	return out
}
