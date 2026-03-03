package jobs

// outbox_worker.go — Notification Outbox Background Worker
//
// WHAT IT DOES
// ────────────
// 1. Polls notification_svc.outbox every OUTBOX_WORKER_POLL_SECS seconds.
// 2. Claims up to OUTBOX_WORKER_BATCH_SIZE PENDING EMAIL rows using
//    FOR UPDATE SKIP LOCKED — safe for multiple parallel instances.
// 3. Marks each claimed row PROCESSING then POSTs the batch as JSON to
//    SEND_ENDPOINT_URL (your email provider gateway or internal relay).
// 4. On success → marks outbox SENT + writes send_history.
//    On failure → retries with exponential back-off (config from
//    notification_svc.notification_config); after retry_max attempts
//    moves the row to DEAD.
//
// REQUIRED ENV VARS
// ─────────────────
// SEND_ENDPOINT_URL       — e.g. https://mail-relay.internal/api/v1/send/bulk
//
// OPTIONAL ENV VARS
// ─────────────────
// OUTBOX_WORKER_ENABLED        true|false   (default: true)
// OUTBOX_WORKER_POLL_SECS      integer      (default: 10)
// OUTBOX_WORKER_BATCH_SIZE     integer      (default: 50)
// OUTBOX_WORKER_TIMEOUT_SECS   integer      (default: 15)
// SEND_ENDPOINT_API_KEY        Bearer token for SEND_ENDPOINT_URL (optional)
//
// WIRING
// ──────
// Called from CronService.Start() in scheduler.go:
//   go StartOutboxWorker(ctx, pool)

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

// StartOutboxWorker runs the polling loop. Call in a goroutine:
//
//	go jobs.StartOutboxWorker(rootCtx, pgxPool)
func StartOutboxWorker(ctx context.Context, pool *pgxpool.Pool) {
	if !owGetenvBool("OUTBOX_WORKER_ENABLED", true) {
		log.Println("[outbox-worker] disabled via OUTBOX_WORKER_ENABLED=false")
		return
	}

	endpointURL := strings.TrimSpace(os.Getenv("SEND_ENDPOINT_URL"))
	if endpointURL == "" {
		log.Println("[outbox-worker] SEND_ENDPOINT_URL is not set — worker will not start")
		return
	}

	pollInterval := time.Duration(owGetenvInt("OUTBOX_WORKER_POLL_SECS", 10)) * time.Second
	batchSize := owGetenvInt("OUTBOX_WORKER_BATCH_SIZE", 50)

	log.Printf("[outbox-worker] started (poll=%s batch=%d endpoint=%s)", pollInterval, batchSize, endpointURL)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[outbox-worker] stopped (context cancelled)")
			return
		case <-ticker.C:
			owProcessBatch(ctx, pool, endpointURL, batchSize)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal types
// ─────────────────────────────────────────────────────────────────────────────

// owOutboxRow holds only the columns needed by the worker.
type owOutboxRow struct {
	OutboxID        string
	Channel         string
	RecipientEmail  string
	RecipientName   string
	RenderedSubject string
	RenderedBody    string
	SenderEmail     string
	SenderName      string
	RetryCount      int
	PriorityLevel   int
	EventID         string
	AuditID         string
	CorrelationID   string
	RecipientUserID string
}

// owSendPayload is one item in the JSON array POSTed to SEND_ENDPOINT_URL.
type owSendPayload struct {
	OutboxID        string `json:"outbox_id"`
	To              string `json:"to"`
	From            string `json:"from,omitempty"`
	Subject         string `json:"subject"`
	HTMLBody        string `json:"html_body"`
	RecipientName   string `json:"recipient_name,omitempty"`
	SenderName      string `json:"sender_name,omitempty"`
	EventID         string `json:"event_id,omitempty"`
	CorrelationID   string `json:"correlation_id,omitempty"`
	AuditID         string `json:"audit_id,omitempty"`
	RecipientUserID string `json:"recipient_user_id,omitempty"`
	RetryCount      int    `json:"retry_count"`
}

// owBulkResponse is what SEND_ENDPOINT_URL must return.
// Shape: { "results": [ { "outbox_id": "...", "success": true, "message_id": "..." }, ... ] }
type owBulkResponse struct {
	Results []owItemResult `json:"results"`
}

type owItemResult struct {
	OutboxID  string `json:"outbox_id"`
	Success   bool   `json:"success"`
	MessageID string `json:"message_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Core processing loop
// ─────────────────────────────────────────────────────────────────────────────

func owProcessBatch(ctx context.Context, pool *pgxpool.Pool, endpointURL string, batchSize int) {
	// Step 1 — SELECT pending rows (FOR UPDATE SKIP LOCKED prevents double-processing)
	const selectQ = `
		SELECT o.outbox_id, o.channel,
		       o.recipient_email, o.recipient_name,
		       o.rendered_subject, o.rendered_body,
		       o.sender_email, o.sender_name,
		       o.retry_count, o.priority_level,
		       o.event_id, o.audit_id::text, o.correlation_id, o.recipient_user_id
		FROM   notification_svc.outbox o
		WHERE  o.processing_status = 'PENDING'
		  AND  o.channel = 'EMAIL'
		  AND  o.scheduled_at <= now()
		ORDER  BY o.priority_level ASC, o.scheduled_at ASC
		LIMIT  $1
		FOR UPDATE SKIP LOCKED`

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Printf("[outbox-worker] begin tx: %v", err)
		return
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, selectQ, batchSize)
	if err != nil {
		log.Printf("[outbox-worker] fetch batch: %v", err)
		return
	}

	var batch []owOutboxRow
	for rows.Next() {
		var r owOutboxRow
		var recipientName, senderEmail, senderName, auditID, correlationID, recipientUserID *string
		if err := rows.Scan(
			&r.OutboxID, &r.Channel,
			&r.RecipientEmail, &recipientName,
			&r.RenderedSubject, &r.RenderedBody,
			&senderEmail, &senderName,
			&r.RetryCount, &r.PriorityLevel,
			&r.EventID, &auditID, &correlationID, &recipientUserID,
		); err != nil {
			log.Printf("[outbox-worker] scan row: %v", err)
			continue
		}
		owDeref(&r.RecipientName, recipientName)
		owDeref(&r.SenderEmail, senderEmail)
		owDeref(&r.SenderName, senderName)
		owDeref(&r.AuditID, auditID)
		owDeref(&r.CorrelationID, correlationID)
		owDeref(&r.RecipientUserID, recipientUserID)
		batch = append(batch, r)
	}
	rows.Close()
	if rows.Err() != nil {
		log.Printf("[outbox-worker] rows iteration error: %v", rows.Err())
	}

	// Commit the SELECT (releases the FOR UPDATE lock after we read the IDs)
	if err := tx.Commit(ctx); err != nil {
		log.Printf("[outbox-worker] commit read tx: %v", err)
		return
	}

	if len(batch) == 0 {
		return // nothing to do this tick
	}
	log.Printf("[outbox-worker] picked up %d rows", len(batch))

	// Step 2 — Claim each row by atomically flipping it to PROCESSING.
	// Individual UPDATEs (not batch) so a race on any single row doesn't
	// block the others.
	var toSend []owOutboxRow
	for _, row := range batch {
		n, err := owExecRowsAffected(ctx, pool,
			`UPDATE notification_svc.outbox
			    SET processing_status = 'PROCESSING', processed_at = now()
			  WHERE outbox_id = $1 AND processing_status = 'PENDING'`,
			row.OutboxID)
		if err != nil || n == 0 {
			log.Printf("[outbox-worker] outbox_id=%s already claimed or error: %v", row.OutboxID, err)
			continue
		}
		toSend = append(toSend, row)
	}

	if len(toSend) == 0 {
		return
	}

	// Step 3 — Build the bulk payload and POST to SEND_ENDPOINT_URL
	payloads := make([]owSendPayload, 0, len(toSend))
	for _, row := range toSend {
		payloads = append(payloads, owSendPayload{
			OutboxID:        row.OutboxID,
			To:              row.RecipientEmail,
			From:            row.SenderEmail,
			Subject:         row.RenderedSubject,
			HTMLBody:        row.RenderedBody,
			RecipientName:   row.RecipientName,
			SenderName:      row.SenderName,
			EventID:         row.EventID,
			CorrelationID:   row.CorrelationID,
			AuditID:         row.AuditID,
			RecipientUserID: row.RecipientUserID,
			RetryCount:      row.RetryCount,
		})
	}

	resultMap := owCallEndpoint(ctx, endpointURL, payloads)

	// Step 4 — Update DB based on provider response
	for _, row := range toSend {
		res, found := resultMap[row.OutboxID]
		if !found {
			owHandleFailure(ctx, pool, row, "send endpoint returned no result for this outbox_id")
			continue
		}
		if res.Success {
			_ = owExec(ctx, pool,
				`UPDATE notification_svc.outbox
				    SET processing_status = 'SENT', sent_at = now(), last_error = NULL
				  WHERE outbox_id = $1`,
				row.OutboxID)
			owInsertHistory(ctx, pool, row, "SENT", res.MessageID, res.MessageID, row.RetryCount+1)
			log.Printf("[outbox-worker] SENT outbox_id=%s to=%s", row.OutboxID, row.RecipientEmail)
		} else {
			owHandleFailure(ctx, pool, row, res.Error)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP call to send endpoint
// ─────────────────────────────────────────────────────────────────────────────

func owCallEndpoint(ctx context.Context, endpointURL string, payloads []owSendPayload) map[string]owItemResult {
	resultMap := make(map[string]owItemResult, len(payloads))

	body, err := json.Marshal(payloads)
	if err != nil {
		log.Printf("[outbox-worker] marshal payload: %v", err)
		return resultMap
	}

	timeout := time.Duration(owGetenvInt("OUTBOX_WORKER_TIMEOUT_SECS", 15)) * time.Second
	client := &http.Client{Timeout: timeout}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("[outbox-worker] build HTTP request: %v", err)
		return resultMap
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey := strings.TrimSpace(os.Getenv("SEND_ENDPOINT_API_KEY")); apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[outbox-worker] POST %s failed: %v", endpointURL, err)
		return resultMap
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Printf("[outbox-worker] endpoint returned HTTP %d: %s",
			resp.StatusCode, owTruncate(string(respBody), 300))
		return resultMap
	}

	var bulk owBulkResponse
	if err := json.Unmarshal(respBody, &bulk); err != nil {
		log.Printf("[outbox-worker] decode response: %v", err)
		return resultMap
	}

	for _, item := range bulk.Results {
		resultMap[item.OutboxID] = item
	}
	return resultMap
}

// ─────────────────────────────────────────────────────────────────────────────
// Retry / dead-letter logic
// ─────────────────────────────────────────────────────────────────────────────

func owHandleFailure(ctx context.Context, pool *pgxpool.Pool, row owOutboxRow, errMsg string) {
	retryMax, retryBackoffSecs := owFetchRetryConfig(ctx, pool, row.EventID)

	if row.RetryCount < retryMax {
		// Exponential back-off: backoffSecs * 2^retryCount
		backoff := float64(retryBackoffSecs) * math.Pow(2, float64(row.RetryCount))
		nextScheduled := time.Now().Add(time.Duration(backoff) * time.Second)
		_ = owExec(ctx, pool,
			`UPDATE notification_svc.outbox
			    SET processing_status = 'PENDING',
			        retry_count       = retry_count + 1,
			        last_error        = $2,
			        scheduled_at      = $3
			  WHERE outbox_id = $1`,
			row.OutboxID, errMsg, nextScheduled)
		log.Printf("[outbox-worker] RETRY outbox_id=%s attempt=%d next_at=%s err=%s",
			row.OutboxID, row.RetryCount+1, nextScheduled.Format(time.RFC3339), owTruncate(errMsg, 120))
	} else {
		_ = owExec(ctx, pool,
			`UPDATE notification_svc.outbox
			    SET processing_status = 'DEAD',
			        retry_count       = retry_count + 1,
			        last_error        = $2
			  WHERE outbox_id = $1`,
			row.OutboxID, errMsg)
		log.Printf("[outbox-worker] DEAD outbox_id=%s after %d attempts err=%s",
			row.OutboxID, row.RetryCount+1, owTruncate(errMsg, 120))
	}
	owInsertHistory(ctx, pool, row, "FAILED", errMsg, "", row.RetryCount+1)
}

// owFetchRetryConfig reads retry_max and retry_backoff_secs from
// notification_svc.notification_config for this event+EMAIL channel.
// Falls back to (3, 60) if no row found.
func owFetchRetryConfig(ctx context.Context, pool *pgxpool.Pool, eventID string) (retryMax, retryBackoffSecs int) {
	retryMax, retryBackoffSecs = 3, 60
	_ = pool.QueryRow(ctx,
		`SELECT retry_max, retry_backoff_secs
		   FROM notification_svc.notification_config
		  WHERE event_id = $1 AND channel = 'EMAIL'`,
		eventID,
	).Scan(&retryMax, &retryBackoffSecs)
	return
}

// owInsertHistory writes or updates a send_history row for this outbox_id.
//
// Strategy: try UPDATE first (row was pre-seeded as QUEUED by dispatcher).
// If no row exists yet (rowsAffected == 0), INSERT it.
// This avoids relying on a UNIQUE constraint that may not exist on all envs.
func owInsertHistory(
	ctx context.Context, pool *pgxpool.Pool,
	row owOutboxRow,
	status, providerResponse, providerMessageID string,
	attemptNumber int,
) {
	// Try UPDATE first — the QUEUED seed row is normally already there.
	n, err := owExecRowsAffected(ctx, pool,
		`UPDATE notification_svc.send_history
		    SET processing_status   = $2,
		        provider_response   = $3,
		        provider_message_id = $4,
		        attempt_number      = $5,
		        attempted_at        = now()
		  WHERE outbox_id = $1`,
		row.OutboxID, status, providerResponse, providerMessageID, attemptNumber,
	)
	if err != nil {
		log.Printf("[outbox-worker] send_history update outbox_id=%s: %v", row.OutboxID, err)
		return
	}
	if n > 0 {
		return // updated the existing QUEUED row — done
	}

	// No pre-seeded row found — INSERT fresh (idempotent via DO NOTHING).
	_, err = pool.Exec(ctx, `
		INSERT INTO notification_svc.send_history
		  (outbox_id, correlation_id, event_id, audit_id, channel,
		   recipient_user_id, recipient_email,
		   processing_status, provider_response, provider_message_id,
		   attempt_number, attempted_at,
		   sender_email, sender_name,
		   rendered_subject, rendered_body)
		VALUES ($1,$2,$3,$4::uuid,$5,$6,$7,$8,$9,$10,$11,now(),$12,$13,$14,$15)
		ON CONFLICT DO NOTHING`,
		row.OutboxID, row.CorrelationID, row.EventID, row.AuditID, row.Channel,
		row.RecipientUserID, row.RecipientEmail,
		status, providerResponse, providerMessageID,
		attemptNumber,
		row.SenderEmail, row.SenderName,
		row.RenderedSubject, row.RenderedBody,
	)
	if err != nil {
		log.Printf("[outbox-worker] send_history insert outbox_id=%s: %v", row.OutboxID, err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// DB helpers
// ─────────────────────────────────────────────────────────────────────────────

func owExec(ctx context.Context, pool *pgxpool.Pool, sql string, args ...interface{}) error {
	_, err := pool.Exec(ctx, sql, args...)
	return err
}

func owExecRowsAffected(ctx context.Context, pool *pgxpool.Pool, sql string, args ...interface{}) (int64, error) {
	ct, err := pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

func owDeref(dst *string, src *string) {
	if src != nil {
		*dst = *src
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Env helpers  (prefixed ow_ to avoid collision with other jobs helpers)
// ─────────────────────────────────────────────────────────────────────────────

func owGetenvBool(k string, dflt bool) bool {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return dflt
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return dflt
	}
	return b
}

func owGetenvInt(k string, dflt int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return dflt
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return dflt
	}
	return n
}

func owTruncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + fmt.Sprintf("…[+%d bytes]", len(s)-max)
}
