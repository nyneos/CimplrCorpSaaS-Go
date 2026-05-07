package categorizer

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 9 — AI / Intelligent Inference
//
// When all deterministic and statistical steps (RULE, COUNTERPARTY, GL,
// CORRECTION, SIMILARITY, ACCOUNT_DEFAULT) have failed or returned a
// confidence below MinConfidenceForActuals, the engine calls a configured
// LLM endpoint to infer the best category.
//
// Behaviour:
//  - The LLM receives: narration_clean, payment_channel, amount direction,
//    and the full list of available category names.
//  - The LLM returns: category_id, category_name, confidence (0.0–1.0),
//    reasoning text (stored in ai_reasoning on the transaction row).
//  - Confidence is returned directly by the model; no fixed default.
//  - Results below MinConfidenceForActuals still enter the review queue.
//
// AI-generated rules:
//  - After a successful AI classification the engine records an
//    AI_SUGGESTED rule into category_rules / category_rule_components
//    with is_active=FALSE and rule_status='AI_SUGGESTED'.
//  - An admin must approve the rule via POST /cash/smart-cat/rule/confirm
//    before it becomes active in the waterfall.
//  - Duplicate-rule detection prevents conflicting suggestions.
//
// Configuration (environment variables):
//  AI_INFERENCE_URL    — OpenAI-compatible chat completions endpoint
//                        e.g. https://api.openai.com/v1/chat/completions
//  AI_INFERENCE_KEY    — Bearer token / API key
//  AI_INFERENCE_MODEL  — Model name (default: gpt-4o-mini)
//  AI_INFERENCE_TIMEOUT_SEC — HTTP timeout in seconds (default: 15)
// ═══════════════════════════════════════════════════════════════════════════════

import (
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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StepAIInference is declared in types.go; see that file for the full constant block.
// (package-level constant removed to avoid redeclaration)
// AI Inference public entry-point
// ─────────────────────────────────────────────────────────────

// InferWithAI calls the configured LLM endpoint and returns a ClassificationResult.
// It also persists an AI_SUGGESTED rule when the confidence is sufficient.
// Returns (result, false) when the LLM is not configured or returns an unusable answer.
func InferWithAI(
	ctx context.Context,
	pool *pgxpool.Pool,
	narration NarrationResult,
	txn TxnInput,
) (ClassificationResult, bool) {
	cfg := loadAIConfig()
	if cfg.URL == "" || cfg.APIKey == "" {
		return ClassificationResult{}, false
	}

	// Load category list for the prompt
	cats, err := loadCategoryList(ctx, pool)
	if err != nil || len(cats) == 0 {
		log.Printf("[AI-INFER] could not load categories: %v", err)
		return ClassificationResult{}, false
	}

	prompt := buildPrompt(narration, txn, cats)
	raw, err := callLLM(ctx, cfg, prompt, 256)
	if err != nil {
		log.Printf("[AI-INFER] LLM call failed: %v", err)
		return ClassificationResult{}, false
	}

	result, reasoning, ok := parseAIResponse(raw, cats)
	if !ok {
		log.Printf("[AI-INFER] could not parse LLM response: %s", raw)
		return ClassificationResult{}, false
	}

	// Persist the AI reasoning onto the transaction for analyst review
	if _, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.bank_statement_transactions
		SET ai_reasoning = $1
		WHERE transaction_id = $2
	`, reasoning, txn.TransactionID); err != nil {
		log.Printf("[AI-INFER] persist reasoning txn=%d: %v", txn.TransactionID, err)
	}

	// Also store reasoning in the review queue entry (populated later by PersistClassification)
	if _, err := pool.Exec(ctx, `
		UPDATE cimplrcorpsaas.categorization_review_queue
		SET ai_reasoning = $1
		WHERE transaction_id = $2
	`, reasoning, txn.TransactionID); err != nil {
		log.Printf("[AI-INFER] persist queue reasoning txn=%d: %v", txn.TransactionID, err)
	}

	// If confidence is high enough, suggest a reusable rule (awaiting admin approval)
	if result.Confidence >= 0.80 && narration.Clean != "" {
		go persistAISuggestedRule(context.Background(), pool, narration, txn, result, reasoning)
	}

	return result, true
}

// ─────────────────────────────────────────────────────────────
// AI-Suggested Rule Persistence
// ─────────────────────────────────────────────────────────────

// persistAISuggestedRule inserts a NARRATION_LOGIC rule in AI_SUGGESTED status.
// The rule is inactive until an admin approves it via the rule manager.
// Duplicate detection: does not insert if an identical narration+category rule
// already exists in ACTIVE or AI_SUGGESTED status.
func persistAISuggestedRule(
	ctx context.Context,
	pool *pgxpool.Pool,
	narration NarrationResult,
	txn TxnInput,
	result ClassificationResult,
	reasoning string,
) {
	// Deduplication: skip if an active or pending rule already covers this narration
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

	// Create a GLOBAL scope entry if none exists
	var scopeID int64
	scopeErr := pool.QueryRow(ctx, `
		SELECT scope_id FROM cimplrcorpsaas.rule_scope WHERE scope_type='GLOBAL' LIMIT 1
	`).Scan(&scopeID)
	if scopeErr != nil {
		// Insert a global scope
		if err := pool.QueryRow(ctx, `
			INSERT INTO cimplrcorpsaas.rule_scope (scope_type) VALUES ('GLOBAL') RETURNING scope_id
		`).Scan(&scopeID); err != nil {
			log.Printf("[AI-RULE] could not resolve global scope: %v", err)
			return
		}
	}

	// Determine next available priority (lowest existing + 10)
	var maxPrio int
	_ = pool.QueryRow(ctx, `SELECT COALESCE(MAX(priority),0) FROM cimplrcorpsaas.category_rules`).Scan(&maxPrio)

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
	`, ruleName, result.CategoryID, scopeID, maxPrio+10, reasoning).Scan(&ruleID); err != nil {
		log.Printf("[AI-RULE] insert rule: %v", err)
		return
	}

	matchType := "CONTAINS"
	matchValue := narration.Clean

	if _, err := tx.Exec(ctx, `
		INSERT INTO cimplrcorpsaas.category_rule_components
		    (rule_id, component_type, match_type, match_value, is_active)
		VALUES ($1, 'NARRATION_LOGIC', $2, $3, TRUE)
	`, ruleID, matchType, matchValue); err != nil {
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
// LLM communication
// ─────────────────────────────────────────────────────────────

type aiConfig struct {
	URL        string
	APIKey     string
	Model      string
	TimeoutSec int
}

func loadAIConfig() aiConfig {
	url := os.Getenv("AI_INFERENCE_URL")
	key := os.Getenv("AI_INFERENCE_KEY")
	model := os.Getenv("AI_INFERENCE_MODEL")
	if model == "" {
		model = "gpt-4o-mini"
	}
	timeout := 15
	if t := os.Getenv("AI_INFERENCE_TIMEOUT_SEC"); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			timeout = n
		}
	}
	return aiConfig{URL: url, APIKey: key, Model: model, TimeoutSec: timeout}
}

type aiChatRequest struct {
	Model       string        `json:"model"`
	Temperature float64       `json:"temperature"`
	Messages    []aiMessage   `json:"messages"`
	MaxTokens   int           `json:"max_tokens"`
}

type aiMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type aiChatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func buildPrompt(narration NarrationResult, txn TxnInput, cats []categoryItem) string {
	direction := "DEBIT (outflow)"
	if txn.Deposit != nil && *txn.Deposit > 0 {
		direction = "CREDIT (inflow)"
	}
	var catLines strings.Builder
	for _, c := range cats {
		catLines.WriteString(fmt.Sprintf("  - %s (%s)\n", c.Name, c.ID))
	}
	return fmt.Sprintf(
		`You are a treasury analyst classifying a bank transaction.

Transaction details:
  Narration  : %s
  Channel    : %s
  Direction  : %s

Available categories:
%s

Respond ONLY with a valid JSON object in this exact shape:
{
  "category_id":   "<id from the list above>",
  "category_name": "<name from the list above>",
  "confidence":    <float 0.0–1.0>,
  "reasoning":     "<one sentence explaining your choice>"
}

If none of the categories fits, set confidence to 0 and category_id to "UNALLOCATED".`,
		narration.Clean,
		string(narration.Channel),
		direction,
		catLines.String(),
	)
}

func callLLM(ctx context.Context, cfg aiConfig, prompt string, maxTokens int) (string, error) {
	if maxTokens <= 0 {
		maxTokens = 256
	}
	payload := aiChatRequest{
		Model:       cfg.Model,
		Temperature: 0.0,
		MaxTokens:   maxTokens,
		Messages: []aiMessage{
			{Role: "system", Content: "You are a strict JSON-only responder. Output only valid JSON."},
			{Role: "user", Content: prompt},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	httpCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TimeoutSec)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("LLM status %d: %s", resp.StatusCode, b)
	}

	var chatResp aiChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if len(chatResp.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}
	return chatResp.Choices[0].Message.Content, nil
}

// ─────────────────────────────────────────────────────────────
// Response parsing
// ─────────────────────────────────────────────────────────────

type categoryItem struct {
	ID   string
	Name string
}

type aiAnswerShape struct {
	CategoryID   string  `json:"category_id"`
	CategoryName string  `json:"category_name"`
	Confidence   float64 `json:"confidence"`
	Reasoning    string  `json:"reasoning"`
}

func parseAIResponse(raw string, cats []categoryItem) (ClassificationResult, string, bool) {
	// Strip markdown code fences if present
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var ans aiAnswerShape
	if err := json.Unmarshal([]byte(raw), &ans); err != nil {
		return ClassificationResult{}, "", false
	}

	if ans.CategoryID == "" || ans.CategoryID == "UNALLOCATED" || ans.Confidence <= 0 {
		return ClassificationResult{}, ans.Reasoning, false
	}

	// Validate category_id exists in the allowed list to prevent hallucinations
	valid := false
	for _, c := range cats {
		if c.ID == ans.CategoryID {
			valid = true
			if ans.CategoryName == "" {
				ans.CategoryName = c.Name
			}
			break
		}
	}
	if !valid {
		return ClassificationResult{}, ans.Reasoning, false
	}

	if ans.Confidence > 1.0 {
		ans.Confidence = 1.0
	}

	return ClassificationResult{
		CategoryID:   ans.CategoryID,
		CategoryName: ans.CategoryName,
		Confidence:   ans.Confidence,
		Step:         StepAIInference,
		SourceRef:    fmt.Sprintf("ai_conf=%.2f model=%s", ans.Confidence, os.Getenv("AI_INFERENCE_MODEL")),
	}, ans.Reasoning, true
}

// ─────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────

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
// Bulk AI Classification (for review-queue AI suggest feature)
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

// BulkClassifyNarrations sends a batch of unique narration patterns to the
// configured LLM and returns one suggestion per pattern. It returns an error
// (not a partial result) when AI is not configured.
func BulkClassifyNarrations(ctx context.Context, pool *pgxpool.Pool, items []BulkNarrationInput) ([]BulkSuggestion, error) {
	cfg := loadAIConfig()
	if cfg.URL == "" || cfg.APIKey == "" {
		return nil, fmt.Errorf("AI inference not configured: set AI_INFERENCE_URL and AI_INFERENCE_KEY environment variables")
	}

	cats, err := loadCategoryList(ctx, pool)
	if err != nil || len(cats) == 0 {
		return nil, fmt.Errorf("could not load category list: %v", err)
	}

	// Bulk calls need a much longer timeout — the model generates up to 4096 tokens
	// across many narrations. Default to 120 s; override with AI_INFERENCE_BULK_TIMEOUT_SEC.
	bulkTimeoutSec := 120
	if t := os.Getenv("AI_INFERENCE_BULK_TIMEOUT_SEC"); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			bulkTimeoutSec = n
		}
	}
	bulkCfg := cfg
	bulkCfg.TimeoutSec = bulkTimeoutSec

	const batchSize = 50
	var all []BulkSuggestion
	for start := 0; start < len(items); start += batchSize {
		end := start + batchSize
		if end > len(items) {
			end = len(items)
		}
		batch := items[start:end]
		prompt := buildBatchPrompt(batch, cats, start)
		raw, err := callLLM(ctx, bulkCfg, prompt, 4096)
		if err != nil {
			errMsg := fmt.Sprintf("AI call failed: %v", err)
			log.Printf("[AI-BULK] LLM call failed for batch starting at %d: %v", start, err)
			// fill with UNALLOCATED so callers always get a full slice
			for _, item := range batch {
				all = append(all, BulkSuggestion{
					NarrationClean:   item.NarrationClean,
					CategoryID:       "UNALLOCATED",
					Confidence:       0,
					Reasoning:        errMsg,
					TransactionIDs:   item.TransactionIDs,
					TransactionCount: len(item.TransactionIDs),
				})
			}
			continue
		}
		suggestions, err := parseBatchResponse(raw, batch, cats, start)
		if err != nil {
			errMsg := fmt.Sprintf("AI response could not be parsed: %v — raw: %.200s", err, raw)
			log.Printf("[AI-BULK] parse error for batch starting at %d: %v — raw: %.200s", start, err, raw)
			for _, item := range batch {
				all = append(all, BulkSuggestion{
					NarrationClean:   item.NarrationClean,
					CategoryID:       "UNALLOCATED",
					Confidence:       0,
					Reasoning:        errMsg,
					TransactionIDs:   item.TransactionIDs,
					TransactionCount: len(item.TransactionIDs),
				})
			}
			continue
		}
		all = append(all, suggestions...)
	}
	return all, nil
}

func buildBatchPrompt(batch []BulkNarrationInput, cats []categoryItem, offset int) string {
	var sb strings.Builder
	sb.WriteString("You are a treasury analyst classifying bank transactions.\n\n")
	sb.WriteString("Classify each narration below into the best matching category.\n\n")
	sb.WriteString("Narrations:\n")
	for i, item := range batch {
		sb.WriteString(fmt.Sprintf("%d: %q | channel: %s\n", offset+i, item.NarrationClean, item.PaymentChannel))
	}
	sb.WriteString("\nAvailable categories (id: name):\n")
	for _, c := range cats {
		sb.WriteString(fmt.Sprintf("  %s: %s\n", c.ID, c.Name))
	}
	sb.WriteString(`
Return ONLY a valid JSON array with one object per narration (same order, 0-indexed from the offset):
[
  {"index": <int>, "category_id": "<id from list>", "category_name": "<name from list>", "confidence": <0.0-1.0>, "reasoning": "<one sentence>"},
  ...
]
If no category fits, set confidence to 0 and category_id to "UNALLOCATED".
Do NOT include any text outside the JSON array.`)
	return sb.String()
}

type batchAIItem struct {
	Index        int     `json:"index"`
	CategoryID   string  `json:"category_id"`
	CategoryName string  `json:"category_name"`
	Confidence   float64 `json:"confidence"`
	Reasoning    string  `json:"reasoning"`
}

func parseBatchResponse(raw string, batch []BulkNarrationInput, cats []categoryItem, offset int) ([]BulkSuggestion, error) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)
	// Extract the JSON array
	start := strings.Index(raw, "[")
	end := strings.LastIndex(raw, "]")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("no JSON array found in LLM response")
	}
	raw = raw[start : end+1]

	var items []batchAIItem
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	byIndex := make(map[int]batchAIItem, len(items))
	for _, it := range items {
		byIndex[it.Index] = it
	}

	suggestions := make([]BulkSuggestion, 0, len(batch))
	for i, item := range batch {
		idx := offset + i
		ai, ok := byIndex[idx]
		if !ok {
			ai = batchAIItem{Index: idx, CategoryID: "UNALLOCATED", Confidence: 0, Reasoning: "no AI response for this index"}
		}
		catName := ai.CategoryName
		valid := ai.CategoryID == "UNALLOCATED" || ai.CategoryID == ""
		if ai.CategoryID == "" {
			ai.CategoryID = "UNALLOCATED"
		}
		for _, c := range cats {
			if c.ID == ai.CategoryID {
				valid = true
				if catName == "" {
					catName = c.Name
				}
				break
			}
		}
		if !valid {
			ai.CategoryID = "UNALLOCATED"
			ai.Confidence = 0
			catName = "Unallocated"
		}
		if ai.Confidence > 1.0 {
			ai.Confidence = 1.0
		}
		suggestions = append(suggestions, BulkSuggestion{
			NarrationClean:   item.NarrationClean,
			CategoryID:       ai.CategoryID,
			CategoryName:     catName,
			Confidence:       ai.Confidence,
			Reasoning:        ai.Reasoning,
			TransactionIDs:   item.TransactionIDs,
			TransactionCount: len(item.TransactionIDs),
		})
	}
	return suggestions, nil
}
