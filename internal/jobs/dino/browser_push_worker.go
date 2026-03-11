package jobs

// browser_push_worker.go — VAPID Web Push Delivery Worker
//
// TWO RESPONSIBILITIES
// ────────────────────
//
// A) EVENT PUSH (every BROWSER_PUSH_POLL_SECS = 15s)
//    Polls notification_svc.outbox WHERE channel='PUSH' AND status='PENDING'
//    For each row → lookup user's active push subscriptions → send VAPID push.
//    On 410/404 from push service → permanently deactivate that subscription.
//    Success/failure follows same retry+backoff pattern as email outbox worker.
//
// B) DIGEST PUSH (every BROWSER_PUSH_DIGEST_MINS = 30 min)
//    For each user with active push subscriptions AND unread in_app_notification rows:
//      - Check push_digest_tracker: if last_digest_at > NOW() - 90min → skip (throttle)
//      - Otherwise: fetch top N unread subjects → send one OS digest notification
//      - Upsert push_digest_tracker.last_digest_at = NOW()
//    Ensures users who miss event-time pushes still get periodic reminders.
//
// PUSH PAYLOAD (rendered in OS notification tray via sw.js)
// ─────────────────────────────────────────────────────────
//   { title, body, icon, badge, tag, url, correlation_id, event_id }
//
// ENV VARS
// ────────
// BROWSER_PUSH_ENABLED          true|false  (default: true)
// BROWSER_PUSH_POLL_SECS        integer     (default: 15)
// BROWSER_PUSH_BATCH_SIZE       integer     (default: 50)
// BROWSER_PUSH_DIGEST_MINS      integer     (default: 30)
// BROWSER_PUSH_DIGEST_THROTTLE  integer     (default: 90)
// BROWSER_PUSH_DIGEST_TOP_N     integer     (default: 5)
// VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY, VAPID_SUBJECT  — required

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	webpush "github.com/SherClockHolmes/webpush-go"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Payload types (serialised and sent to sw.js)
// ─────────────────────────────────────────────────────────────────────────────

type bpwEventPayload struct {
	Title         string `json:"title"`
	Body          string `json:"body"`
	Icon          string `json:"icon,omitempty"`
	Badge         string `json:"badge,omitempty"`
	Tag           string `json:"tag,omitempty"`
	URL           string `json:"url,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	EventID       string `json:"event_id,omitempty"`
}

type bpwDigestPayload struct {
	Title       string `json:"title"`
	Body        string `json:"body"`
	UnreadCount int    `json:"unread_count"`
	Tag         string `json:"tag"`
	URL         string `json:"url,omitempty"`
}

// vapidConfig holds the three VAPID credentials loaded once at startup.
type vapidConfig struct {
	public, private, subject string
}

// ─────────────────────────────────────────────────────────────────────────────
// StartBrowserPushWorker — public entry point
// ─────────────────────────────────────────────────────────────────────────────

// StartBrowserPushWorker runs both the event-push and digest-push delivery loops.
// Wire in scheduler.go: go dinojobs.StartBrowserPushWorker(ctx, pool)
func StartBrowserPushWorker(ctx context.Context, pool *pgxpool.Pool) {
	if !bpwGetenvBool("BROWSER_PUSH_ENABLED", true) {
		log.Println("[browser-push] disabled via BROWSER_PUSH_ENABLED=false")
		return
	}

	vapidPublic := strings.TrimSpace(os.Getenv("VAPID_PUBLIC_KEY"))
	vapidPrivate := strings.TrimSpace(os.Getenv("VAPID_PRIVATE_KEY"))
	vapidSubject := strings.TrimSpace(os.Getenv("VAPID_SUBJECT"))
	if vapidPublic == "" || vapidPrivate == "" || vapidSubject == "" {
		log.Println("[browser-push] VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY / VAPID_SUBJECT not set — skipping")
		return
	}

	cfg := &vapidConfig{public: vapidPublic, private: vapidPrivate, subject: vapidSubject}

	pollInterval := time.Duration(bpwGetenvInt("BROWSER_PUSH_POLL_SECS", 15)) * time.Second
	digestInterval := time.Duration(bpwGetenvInt("BROWSER_PUSH_DIGEST_MINS", 30)) * time.Minute
	batchSize := bpwGetenvInt("BROWSER_PUSH_BATCH_SIZE", 50)

	log.Printf("[browser-push] started (event_poll=%s digest_interval=%s batch=%d)",
		pollInterval, digestInterval, batchSize)

	eventTicker := time.NewTicker(pollInterval)
	digestTicker := time.NewTicker(digestInterval)
	defer eventTicker.Stop()
	defer digestTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[browser-push] stopped")
			return
		case <-eventTicker.C:
			bpwProcessOutbox(ctx, pool, cfg, batchSize)
		case <-digestTicker.C:
			bpwSendDigest(ctx, pool, cfg)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// A) Event push — outbox → VAPID
// ─────────────────────────────────────────────────────────────────────────────

type bpwOutboxRow struct {
	OutboxID        string
	RecipientUserID string
	RenderedSubject string
	RenderedBody    string
	EventID         string
	AuditID         string
	CorrelationID   string
	RetryCount      int
	PriorityLevel   int
}

type bpwSubscription struct {
	ID       string
	Endpoint string
	P256DH   string
	AuthKey  string
}

func bpwProcessOutbox(ctx context.Context, pool *pgxpool.Pool, cfg *vapidConfig, batchSize int) {
	// Fetch PENDING PUSH rows using FOR UPDATE SKIP LOCKED to prevent double-delivery
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Printf("[browser-push] begin tx: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT outbox_id, COALESCE(recipient_user_id,''),
		       COALESCE(rendered_subject,''), COALESCE(rendered_body,''),
		       event_id, audit_id::text, correlation_id,
		       COALESCE(retry_count,0), COALESCE(priority_level,3)
		FROM notification_svc.outbox
		WHERE processing_status = 'PENDING'
		  AND channel           = 'PUSH'
		  AND scheduled_at     <= NOW()
		ORDER BY priority_level ASC, scheduled_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, batchSize)
	if err != nil {
		log.Printf("[browser-push] fetch outbox: %v", err)
		return
	}

	var batch []bpwOutboxRow
	for rows.Next() {
		var r bpwOutboxRow
		if err := rows.Scan(
			&r.OutboxID, &r.RecipientUserID,
			&r.RenderedSubject, &r.RenderedBody,
			&r.EventID, &r.AuditID, &r.CorrelationID,
			&r.RetryCount, &r.PriorityLevel,
		); err != nil {
			log.Printf("[browser-push] scan: %v", err)
			continue
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		log.Printf("[browser-push] commit: %v", err)
		return
	}

	if len(batch) == 0 {
		return
	}
	log.Printf("[browser-push] processing %d PUSH outbox rows", len(batch))

	for _, row := range batch {
		// Claim as PROCESSING (optimistic lock — skip if already taken)
		n, err := bpwExecRowsAffected(ctx, pool,
			`UPDATE notification_svc.outbox
			    SET processing_status = 'PROCESSING', processed_at = NOW()
			  WHERE outbox_id = $1 AND processing_status = 'PENDING'`,
			row.OutboxID)
		if err != nil || n == 0 {
			continue
		}

		if row.RecipientUserID == "" {
			// No user to deliver to — mark sent and move on
			bpwMarkSent(ctx, pool, row)
			continue
		}

		subs, err := bpwFetchSubscriptions(ctx, pool, row.RecipientUserID)
		if err != nil {
			log.Printf("[browser-push] fetch subs userID=%s: %v", row.RecipientUserID, err)
			bpwMarkFailed(ctx, pool, row, "db error fetching subscriptions")
			continue
		}
		if len(subs) == 0 {
			// User has no registered browser push subscription — mark SENT (not an error)
			bpwMarkSent(ctx, pool, row)
			log.Printf("[browser-push] no subscriptions for userID=%s — SENT (skipped)", row.RecipientUserID)
			continue
		}

		payload := bpwEventPayload{
			Title:         row.RenderedSubject,
			Body:          bpwTruncate(bpwStripHTML(row.RenderedBody), 200),
			Tag:           row.EventID,
			URL:           "/notifications",
			CorrelationID: row.CorrelationID,
			EventID:       row.EventID,
		}
		payloadBytes, _ := json.Marshal(payload)

		deliveredToAtLeastOne := false
		for _, sub := range subs {
			if err := bpwSendVAPID(cfg, sub, payloadBytes); err != nil {
				log.Printf("[browser-push] VAPID error outbox=%s sub=%s: %v", row.OutboxID, sub.ID, err)
				if bpwIsExpiredSub(err) {
					bpwDeactivateSubscription(ctx, pool, sub.ID)
				}
				continue
			}
			deliveredToAtLeastOne = true
			log.Printf("[browser-push] delivered outbox=%s sub=%s userID=%s",
				row.OutboxID, sub.ID, row.RecipientUserID)
		}

		if deliveredToAtLeastOne {
			bpwMarkSent(ctx, pool, row)
		} else {
			bpwMarkFailed(ctx, pool, row, "all VAPID deliveries failed")
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// B) Digest push — top N unread, max once per 90 min per user
// ─────────────────────────────────────────────────────────────────────────────

func bpwSendDigest(ctx context.Context, pool *pgxpool.Pool, cfg *vapidConfig) {
	throttleMinutes := bpwGetenvInt("BROWSER_PUSH_DIGEST_THROTTLE", 90)
	topN := bpwGetenvInt("BROWSER_PUSH_DIGEST_TOP_N", 5)

	// Find users who:
	//   1. Have at least one active push subscription
	//   2. Have unread in_app_notifications
	//   3. Have NOT received a digest within the throttle window
	candidateRows, err := pool.Query(ctx, `
		SELECT DISTINCT ps.user_id
		FROM notification_svc.push_subscription ps
		JOIN notification_svc.in_app_notification ian
		     ON ian.recipient_user_id = ps.user_id
		LEFT JOIN notification_svc.push_digest_tracker pdt
		     ON pdt.user_id = ps.user_id
		WHERE ps.is_active  = TRUE
		  AND ian.is_read    = FALSE
		  AND ian.is_deleted = FALSE
		  AND (
		      pdt.last_digest_at IS NULL
		      OR pdt.last_digest_at < NOW() - ($1 || ' minutes')::INTERVAL
		  )
	`, throttleMinutes)
	if err != nil {
		log.Printf("[browser-push-digest] candidate query: %v", err)
		return
	}

	var userIDs []string
	for candidateRows.Next() {
		var uid string
		if err := candidateRows.Scan(&uid); err == nil {
			userIDs = append(userIDs, uid)
		}
	}
	candidateRows.Close()

	if len(userIDs) == 0 {
		return
	}
	log.Printf("[browser-push-digest] sending digest to %d user(s)", len(userIDs))

	for _, userID := range userIDs {
		bpwSendDigestToUser(ctx, pool, cfg, userID, topN)
	}
}

func bpwSendDigestToUser(ctx context.Context, pool *pgxpool.Pool, cfg *vapidConfig, userID string, topN int) {
	type digestItem struct {
		Subject string
	}

	itemRows, err := pool.Query(ctx, `
		SELECT COALESCE(subject,'')
		FROM notification_svc.in_app_notification
		WHERE recipient_user_id = $1
		  AND is_read    = FALSE
		  AND is_deleted = FALSE
		ORDER BY priority_level ASC, created_at DESC
		LIMIT $2
	`, userID, topN)
	if err != nil {
		log.Printf("[browser-push-digest] fetch items userID=%s: %v", userID, err)
		return
	}

	var items []digestItem
	for itemRows.Next() {
		var it digestItem
		if err := itemRows.Scan(&it.Subject); err == nil {
			items = append(items, it)
		}
	}
	itemRows.Close()

	if len(items) == 0 {
		return
	}

	var totalUnread int
	pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM notification_svc.in_app_notification
		WHERE recipient_user_id = $1 AND is_read = FALSE AND is_deleted = FALSE
	`, userID).Scan(&totalUnread)

	// Build bullet list body
	var lines []string
	for _, it := range items {
		if it.Subject != "" {
			lines = append(lines, "• "+it.Subject)
		}
	}
	bodyText := strings.Join(lines, "\n")
	if totalUnread > topN {
		bodyText += fmt.Sprintf("\n+%d more unread", totalUnread-topN)
	}

	digest := bpwDigestPayload{
		Title:       fmt.Sprintf("%d unread notification(s)", totalUnread),
		Body:        bodyText,
		UnreadCount: totalUnread,
		Tag:         "digest",
		URL:         "/notifications",
	}
	payloadBytes, _ := json.Marshal(digest)

	subs, _ := bpwFetchSubscriptions(ctx, pool, userID)
	deliveredToAtLeastOne := false
	for _, sub := range subs {
		if err := bpwSendVAPID(cfg, sub, payloadBytes); err != nil {
			if bpwIsExpiredSub(err) {
				bpwDeactivateSubscription(ctx, pool, sub.ID)
			}
			continue
		}
		deliveredToAtLeastOne = true
	}

	if !deliveredToAtLeastOne {
		log.Printf("[browser-push-digest] all subs failed for userID=%s", userID)
		return
	}

	// Upsert digest tracker
	pool.Exec(ctx, `
		INSERT INTO notification_svc.push_digest_tracker (user_id, last_digest_at)
		VALUES ($1, NOW())
		ON CONFLICT (user_id) DO UPDATE SET last_digest_at = NOW()
	`, userID)

	log.Printf("[browser-push-digest] sent to userID=%s unread=%d top=%d",
		userID, totalUnread, len(items))
}

// ─────────────────────────────────────────────────────────────────────────────
// VAPID send (webpush-go library)
// ─────────────────────────────────────────────────────────────────────────────

func bpwSendVAPID(cfg *vapidConfig, sub bpwSubscription, payload []byte) error {
	resp, err := webpush.SendNotification(payload, &webpush.Subscription{
		Endpoint: sub.Endpoint,
		Keys: webpush.Keys{
			P256dh: sub.P256DH,
			Auth:   sub.AuthKey,
		},
	}, &webpush.Options{
		VAPIDPublicKey:  cfg.public,
		VAPIDPrivateKey: cfg.private,
		Subscriber:      cfg.subject,
		TTL:             3600,
		Urgency:         webpush.UrgencyNormal,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("push service HTTP %d", resp.StatusCode)
	}
	return nil
}

// bpwIsExpiredSub returns true when the push service signals the subscription is gone.
func bpwIsExpiredSub(err error) bool {
	s := err.Error()
	return strings.Contains(s, "410") || strings.Contains(s, "404")
}

// ─────────────────────────────────────────────────────────────────────────────
// DB helpers
// ─────────────────────────────────────────────────────────────────────────────

func bpwFetchSubscriptions(ctx context.Context, pool *pgxpool.Pool, userID string) ([]bpwSubscription, error) {
	rows, err := pool.Query(ctx, `
		SELECT id::text, endpoint, p256dh, auth_key
		FROM notification_svc.push_subscription
		WHERE user_id = $1 AND is_active = TRUE
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []bpwSubscription
	for rows.Next() {
		var s bpwSubscription
		if err := rows.Scan(&s.ID, &s.Endpoint, &s.P256DH, &s.AuthKey); err == nil {
			out = append(out, s)
		}
	}
	return out, rows.Err()
}

func bpwMarkSent(ctx context.Context, pool *pgxpool.Pool, row bpwOutboxRow) {
	pool.Exec(ctx, `
		UPDATE notification_svc.outbox
		   SET processing_status = 'SENT', sent_at = NOW()
		 WHERE outbox_id = $1
	`, row.OutboxID)
}

func bpwMarkFailed(ctx context.Context, pool *pgxpool.Pool, row bpwOutboxRow, errMsg string) {
	retryMax, retryBackoffSecs := bpwFetchRetryConfig(ctx, pool, row.EventID)
	if row.RetryCount < retryMax {
		backoffSecs := float64(retryBackoffSecs) * math.Pow(2, float64(row.RetryCount))
		nextAt := time.Now().Add(time.Duration(backoffSecs) * time.Second)
		pool.Exec(ctx, `
			UPDATE notification_svc.outbox
			   SET processing_status = 'PENDING',
			       retry_count       = retry_count + 1,
			       last_error        = $2,
			       scheduled_at      = $3
			 WHERE outbox_id = $1
		`, row.OutboxID, errMsg, nextAt)
		log.Printf("[browser-push] RETRY outbox=%s attempt=%d next=%s",
			row.OutboxID, row.RetryCount+1, nextAt.Format(time.RFC3339))
	} else {
		pool.Exec(ctx, `
			UPDATE notification_svc.outbox
			   SET processing_status = 'DEAD',
			       retry_count       = retry_count + 1,
			       last_error        = $2
			 WHERE outbox_id = $1
		`, row.OutboxID, errMsg)
		log.Printf("[browser-push] DEAD outbox=%s after %d attempts",
			row.OutboxID, row.RetryCount+1)
	}
}

func bpwFetchRetryConfig(ctx context.Context, pool *pgxpool.Pool, eventID string) (int, int) {
	retryMax, retryBackoffSecs := 3, 60
	pool.QueryRow(ctx, `
		SELECT COALESCE(retry_max,3), COALESCE(retry_backoff_secs,60)
		FROM notification_svc.notification_config
		WHERE event_id = $1 AND channel = 'PUSH'
	`, eventID).Scan(&retryMax, &retryBackoffSecs)
	return retryMax, retryBackoffSecs
}

func bpwDeactivateSubscription(ctx context.Context, pool *pgxpool.Pool, subID string) {
	pool.Exec(ctx, `
		UPDATE notification_svc.push_subscription
		   SET is_active = FALSE, updated_at = NOW()
		 WHERE id = $1
	`, subID)
	log.Printf("[browser-push] deactivated expired subscription id=%s", subID)
}

func bpwExecRowsAffected(ctx context.Context, pool *pgxpool.Pool, sql string, args ...interface{}) (int64, error) {
	ct, err := pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return ct.RowsAffected(), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// String helpers
// ─────────────────────────────────────────────────────────────────────────────

// bpwStripHTML removes HTML tags so the OS notification shows plain text.
func bpwStripHTML(s string) string {
	var out strings.Builder
	inTag := false
	for _, ch := range s {
		switch {
		case ch == '<':
			inTag = true
		case ch == '>':
			inTag = false
		case !inTag:
			out.WriteRune(ch)
		}
	}
	result := strings.TrimSpace(out.String())
	for strings.Contains(result, "  ") {
		result = strings.ReplaceAll(result, "  ", " ")
	}
	return result
}

func bpwTruncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

// ─────────────────────────────────────────────────────────────────────────────
// Env helpers (bpw_ prefix avoids collision with iaw_ helpers in inbox_worker.go)
// ─────────────────────────────────────────────────────────────────────────────

func bpwGetenvBool(k string, dflt bool) bool {
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

func bpwGetenvInt(k string, dflt int) int {
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
