package bankstatement

// AiBulkSuggestHandler — POST /cash/smart-cat/ai-bulk-suggest
//
// Accepts immediately (202) and classifies PENDING UNALLOCATED review-queue rows
// in the background — avoids ALB/gateway 30s timeouts.
//
// POST   /cash/smart-cat/ai-bulk-suggest        → { job_id, status: "processing" }
// POST   /cash/smart-cat/ai-bulk-suggest/status → { job_id } → full job + suggestions

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	apipreval "CimplrCorpSaas/api/middlewares"
	notif "CimplrCorpSaas/api/notification/catalog"
	"CimplrCorpSaas/internal/logger"
	cat "CimplrCorpSaas/internal/services/categorizer"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const aiSuggestJobRoute = "/cash/smart-cat/ai-bulk-suggest/ready"

type aiSuggestJobRequest struct {
	UserID   string `json:"user_id"`
	EntityID string `json:"entity_id,omitempty"`
}

type aiSuggestStatusRequest struct {
	JobID string `json:"job_id"`
}

// AiBulkSuggestHandler starts an async AI bulk-suggest job.
func AiBulkSuggestHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req aiSuggestJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		sessionUID := apipreval.GetUserIDFromContext(r.Context())
		if sessionUID == "" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if req.UserID == "" {
			req.UserID = sessionUID
		}

		ctx := r.Context()
		inputs, totalTxns, err := loadBulkSuggestInputs(ctx, pool, req.EntityID)
		if err != nil {
			http.Error(w, "failed to query review queue", http.StatusInternalServerError)
			return
		}

		jobID := uuid.New().String()
		_, err = pool.Exec(ctx, `
			INSERT INTO cimplrcorpsaas.smartcat_ai_suggest_job
				(job_id, user_id, entity_id, status, total_patterns, total_transactions, suggestions)
			VALUES ($1, $2, NULLIF($3, ''), 'processing', $4, $5, '[]'::jsonb)
		`, jobID, sessionUID, req.EntityID, len(inputs), totalTxns)
		if err != nil {
			http.Error(w, "failed to create AI suggest job", http.StatusInternalServerError)
			return
		}

		go runAiBulkSuggestJob(pool, jobID, sessionUID, inputs, totalTxns)

		api.RespondEnvelopeSuccessCompat(w, "AI analysis started. You will be notified when suggestions are ready.", map[string]interface{}{
			"status":              "processing",
			"job_id":              jobID,
			"total_patterns":      len(inputs),
			"total_transactions":  totalTxns,
			"ai_configured":       true,
		})
	})
}

// AiBulkSuggestStatusHandler returns job status and suggestions when ready.
func AiBulkSuggestStatusHandler(pool *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		var req aiSuggestStatusRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.JobID == "" {
			http.Error(w, "job_id is required", http.StatusBadRequest)
			return
		}

		sessionUID := apipreval.GetUserIDFromContext(r.Context())
		if sessionUID == "" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		job, err := loadAiSuggestJob(r.Context(), pool, req.JobID, sessionUID)
		if err != nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		api.RespondEnvelopeSuccessCompat(w, "Success", job)
	})
}

func loadBulkSuggestInputs(ctx context.Context, pool *pgxpool.Pool, entityID string) ([]cat.BulkNarrationInput, int, error) {
	var query string
	var args []interface{}
	if entityID != "" {
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
		args = []interface{}{entityID}
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
		return nil, 0, err
	}
	defer rows.Close()

	type narGroup struct {
		channel string
		txnIDs  []int64
	}
	grouped := make(map[string]*narGroup)
	var order []string

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

	inputs := make([]cat.BulkNarrationInput, 0, len(order))
	for _, nar := range order {
		g := grouped[nar]
		inputs = append(inputs, cat.BulkNarrationInput{
			NarrationClean: nar,
			PaymentChannel: g.channel,
			TransactionIDs: g.txnIDs,
		})
	}
	return inputs, totalTxns, nil
}

func runAiBulkSuggestJob(pool *pgxpool.Pool, jobID, userID string, inputs []cat.BulkNarrationInput, totalTxns int) {
	bgCtx := context.Background()
	status := "ready"
	var errMsg string
	var suggestions []cat.BulkSuggestion

	if len(inputs) == 0 {
		suggestions = []cat.BulkSuggestion{}
	} else {
		// Append each completed chunk to the DB immediately so the status API
		// can stream partial results to the frontend while processing continues.
		onChunk := func(chunk []cat.BulkSuggestion) {
			if len(chunk) == 0 {
				return
			}
			chunkJSON, err := json.Marshal(chunk)
			if err != nil {
				return
			}
			_, _ = pool.Exec(bgCtx, `
				UPDATE cimplrcorpsaas.smartcat_ai_suggest_job
				   SET suggestions = suggestions || $2::jsonb
				 WHERE job_id = $1
			`, jobID, string(chunkJSON))
		}

		var err error
		suggestions, err = cat.BulkClassifyNarrations(bgCtx, pool, inputs, onChunk)
		if err != nil {
			status = "failed"
			errMsg = err.Error()
			logger.LogError("[AI-BULK-JOB] job %s failed: %v", jobID, err)
		}
	}

	// Final write: replace suggestions with the fully ordered slice and mark done.
	suggJSON, _ := json.Marshal(suggestions)
	_, dbErr := pool.Exec(bgCtx, `
		UPDATE cimplrcorpsaas.smartcat_ai_suggest_job
		   SET status = $2,
		       suggestions = $3::jsonb,
		       error_message = NULLIF($4, ''),
		       completed_at = now()
		 WHERE job_id = $1
	`, jobID, status, string(suggJSON), errMsg)
	if dbErr != nil {
		logger.LogError("[AI-BULK-JOB] job %s store failed: %v", jobID, dbErr)
		return
	}

	notifyAiSuggestReady(bgCtx, pool, aiSuggestReadyParams{
		jobID:        jobID,
		userID:       userID,
		status:       status,
		patternCount: len(suggestions),
		totalTxns:    totalTxns,
		errMsg:       errMsg,
	})
	logger.LogInfo("[AI-BULK-JOB] job %s done status=%s patterns=%d", jobID, status, len(suggestions))
}

func loadAiSuggestJob(ctx context.Context, pool *pgxpool.Pool, jobID, userID string) (map[string]interface{}, error) {
	var (
		status, entityID, errMsg string
		totalPatterns, totalTxns int
		processedPatterns        int
		suggestionsJSON          []byte
		completedAt              *time.Time
	)
	err := pool.QueryRow(ctx, `
		SELECT status, COALESCE(entity_id, ''), total_patterns, total_transactions,
		       suggestions, COALESCE(error_message, ''), completed_at,
		       COALESCE(jsonb_array_length(suggestions), 0) AS processed_patterns
		FROM cimplrcorpsaas.smartcat_ai_suggest_job
		WHERE job_id = $1 AND user_id = $2
	`, jobID, userID).Scan(&status, &entityID, &totalPatterns, &totalTxns, &suggestionsJSON, &errMsg, &completedAt, &processedPatterns)
	if err != nil {
		return nil, err
	}

	var suggestions []cat.BulkSuggestion
	if len(suggestionsJSON) > 0 {
		_ = json.Unmarshal(suggestionsJSON, &suggestions)
	}
	if suggestions == nil {
		suggestions = []cat.BulkSuggestion{}
	}

	out := map[string]interface{}{
		"success":             status == "ready",
		"status":              status,
		"job_id":              jobID,
		"suggestions":         suggestions,
		"total_patterns":      totalPatterns,
		"total_transactions":  totalTxns,
		"processed_patterns":  processedPatterns,
		"ai_configured":       true,
	}
	if errMsg != "" {
		out["error"] = errMsg
	}
	if completedAt != nil {
		out["completed_at"] = completedAt.Format(time.RFC3339)
	}
	return out, nil
}

type aiSuggestReadyParams struct {
	jobID        string
	userID       string
	status       string
	patternCount int
	totalTxns    int
	errMsg       string
}

func notifyAiSuggestReady(ctx context.Context, pool *pgxpool.Pool, p aiSuggestReadyParams) {
	if pool == nil || p.userID == "" {
		return
	}
	payload := map[string]interface{}{
		"job_id":             p.jobID,
		"user_id":            p.userID,
		"status":             p.status,
		"total_patterns":     p.patternCount,
		"total_transactions": p.totalTxns,
	}
	if p.errMsg != "" {
		payload["error"] = p.errMsg
	}
	notif.TriggerNotification(
		ctx, pool,
		aiSuggestJobRoute,
		fmt.Sprintf("SMARTCAT-AI/%s/%d", p.jobID, time.Now().UnixMilli()),
		payload,
	)
}
