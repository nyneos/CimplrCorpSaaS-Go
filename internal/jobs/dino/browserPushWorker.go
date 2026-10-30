package jobs

// browser_push_worker.go — VAPID Web Push Delivery Worker
//
// TWO RESPONSIBILITIES
// ────────────────────
//
// A) EVENT PUSH (every BROWSER_PUSH_POLL_SECS = 15s)
//    Polls notification_svc.outbox WHERE channel='PUSH' AND processing_status='PENDING'.
//    The claim is conditional and runs inside the SKIP LOCKED transaction.
//    Rows stuck in PROCESSING longer than the 15s send timeout plus 60s are reaped.
//    An ambiguous send timeout stays PROCESSING until that reaper and is not resent.
//    For each row → lookup user's active push subscriptions → send VAPID push.
//    On 410/404 from push service → permanently deactivate that subscription.
//    Success/failure follows the same retry, backoff, and lease rules as the email outbox worker.
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
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	webpush "github.com/SherClockHolmes/webpush-go"
	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
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
		logger.LogInfo("[browser-push] disabled via BROWSER_PUSH_ENABLED=false")
		return
	}

	vapidPublic := strings.TrimSpace(os.Getenv("VAPID_PUBLIC_KEY"))
	vapidPrivate := strings.TrimSpace(os.Getenv("VAPID_PRIVATE_KEY"))
	vapidSubject := strings.TrimSpace(os.Getenv("VAPID_SUBJECT"))
	if vapidPublic == "" || vapidPrivate == "" || vapidSubject == "" {
		logger.LogInfo("[browser-push] VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY / VAPID_SUBJECT not set — skipping")
		return
	}

	cfg := &vapidConfig{public: vapidPublic, private: vapidPrivate, subject: vapidSubject}

	pollInterval := time.Duration(bpwGetenvInt("BROWSER_PUSH_POLL_SECS", 15)) * time.Second
	digestInterval := time.Duration(bpwGetenvInt("BROWSER_PUSH_DIGEST_MINS", 30)) * time.Minute
	batchSize := bpwGetenvInt("BROWSER_PUSH_BATCH_SIZE", 50)

	logger.LogInfo("[browser-push] started (event_poll=%s digest_interval=%s batch=%d)",
		pollInterval, digestInterval, batchSize)

	eventTicker := time.NewTicker(pollInterval)
	digestTicker := time.NewTicker(digestInterval)
	defer eventTicker.Stop()
	defer digestTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.LogInfo("[browser-push] stopped")
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
	LeaseToken      string
}

const (
	// Written immediately before the VAPID call. A stale row still carrying this
	// may already have been accepted, so the reaper must not send it again.
	bpwLastErrDispatching = "dispatching:"
	// Written after a push is accepted, before the SENT status update.
	// The reaper finishes SENT from this marker and does not send again.
	bpwLastErrSendAccepted = "send accepted"
	bpwAmbiguousSendPrefix = "ambiguous send:"
	// Same 15s budget as the HTTP client in bpwSendVAPID.
	bpwSendTimeout = 15 * time.Second
	// Added to the send timeout before a PROCESSING row is treated as stale.
	bpwStaleMargin = 60 * time.Second
)

type bpwSubscription struct {
	ID       string
	Endpoint string
	P256DH   string
	AuthKey  string
}

func bpwProcessingStaleAfter() time.Duration {
	return bpwSendTimeout + bpwStaleMargin
}

func bpwIsAmbiguousSendErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := err.Error()
	return msg == "push service HTTP 408" || msg == "push service HTTP 504"
}

func bpwAmbiguousReason(err error) string {
	if err == nil {
		return bpwAmbiguousSendPrefix + " outcome unknown"
	}
	msg := err.Error()
	if errors.Is(err, context.Canceled) || strings.Contains(msg, "context cancelled") || strings.Contains(msg, "context canceled") {
		return bpwAmbiguousSendPrefix + " context cancelled; outcome unknown"
	}
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) || strings.Contains(strings.ToLower(msg), "timeout") {
		return bpwAmbiguousSendPrefix + " timeout; outcome unknown"
	}
	return bpwAmbiguousSendPrefix + " incomplete response; outcome unknown"
}

func bpwProcessOutbox(ctx context.Context, pool *pgxpool.Pool, cfg *vapidConfig, batchSize int) {
	bpwReapStaleProcessing(ctx, pool, batchSize)

	// Fetch PENDING PUSH rows using FOR UPDATE SKIP LOCKED to prevent double-delivery.
	tx, err := pool.Begin(ctx)
	if err != nil {
		logger.LogError("[browser-push] begin tx: %v", err)
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
		logger.LogError("[browser-push] fetch outbox: %v", err)
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
			logger.LogError("[browser-push] scan: %v", err)
			continue
		}
		batch = append(batch, r)
	}
	if rows.Err() != nil {
		logger.LogError("[browser-push] rows iteration error: %v", rows.Err())
	}
	rows.Close()

	// Claim while the SKIP LOCKED row locks are still held. The status predicate
	// stays conditional so a second worker cannot send the same row.
	var toSend []bpwOutboxRow
	for _, row := range batch {
		row.LeaseToken = "claimed:" + row.OutboxID + ":" + strconv.FormatInt(time.Now().UnixNano(), 10)
		ct, err := tx.Exec(ctx,
			`UPDATE notification_svc.outbox
			    SET processing_status = 'PROCESSING', processed_at = NOW(), last_error = $2
			  WHERE outbox_id = $1 AND processing_status = 'PENDING'`,
			row.OutboxID, row.LeaseToken)
		if err != nil {
			logger.LogError("[browser-push] outbox=%s claim: %v", row.OutboxID, err)
			return
		}
		if ct.RowsAffected() == 0 {
			logger.LogError("[browser-push] outbox=%s already claimed", row.OutboxID)
			continue
		}
		toSend = append(toSend, row)
	}

	if err := tx.Commit(ctx); err != nil {
		logger.LogError("[browser-push] commit claim tx: %v", err)
		return
	}

	if len(toSend) == 0 {
		return
	}
	logger.LogInfo("[browser-push] processing %d PUSH outbox rows", len(toSend))

	for _, row := range toSend {
		bpwDeliverClaimed(ctx, pool, cfg, row)
	}
}

func bpwDeliverClaimed(ctx context.Context, pool *pgxpool.Pool, cfg *vapidConfig, row bpwOutboxRow) {
	if row.RecipientUserID == "" {
		bpwMarkSent(ctx, pool, row)
		return
	}

	subs, err := bpwFetchSubscriptions(ctx, pool, row.RecipientUserID)
	if err != nil {
		logger.LogError("[browser-push] fetch subs userID=%s: %v", row.RecipientUserID, err)
		bpwMarkFailed(ctx, pool, row, "db error fetching subscriptions")
		return
	}
	if len(subs) == 0 {
		logger.LogInfo("[browser-push] no subscriptions for userID=%s", row.RecipientUserID)
		bpwMarkSent(ctx, pool, row)
		return
	}

	if !bpwMarkDispatching(ctx, pool, &row) {
		return
	}

	title := bpwPushTitle(row.RenderedSubject)
	body := bpwPushBody(title, row.RenderedBody)
	payload := bpwEventPayload{
		Title:         title,
		Body:          bpwTruncate(bpwStripHTML(body), 200),
		Tag:           row.EventID,
		URL:           "/notifications",
		CorrelationID: row.CorrelationID,
		EventID:       row.EventID,
	}
	payloadBytes, _ := json.Marshal(payload)

	deliveredToAtLeastOne := false
	var ambiguousErr error
	for _, sub := range subs {
		err := bpwSendVAPID(cfg, sub, payloadBytes)
		if err != nil {
			logger.LogError("[browser-push] VAPID error outbox=%s sub=%s: %v", row.OutboxID, sub.ID, err)
			if bpwIsAmbiguousSendErr(err) {
				if ambiguousErr == nil {
					ambiguousErr = err
				}
				continue
			}
			if bpwIsExpiredSub(err) {
				bpwDeactivateSubscription(ctx, pool, sub.ID)
			}
			continue
		}
		deliveredToAtLeastOne = true
		logger.LogInfo("[browser-push] delivered outbox=%s sub=%s userID=%s",
			row.OutboxID, sub.ID, row.RecipientUserID)
	}

	if deliveredToAtLeastOne {
		bpwMarkSent(ctx, pool, row)
		return
	}
	if ambiguousErr != nil {
		bpwRecordAmbiguous(ctx, pool, row, bpwAmbiguousReason(ambiguousErr))
		return
	}
	bpwMarkFailed(ctx, pool, row, "all VAPID deliveries failed")
}

func bpwMarkDispatching(ctx context.Context, pool *pgxpool.Pool, row *bpwOutboxRow) bool {
	if !strings.HasPrefix(row.LeaseToken, "claimed:") {
		logger.LogError("[browser-push] outbox=%s missing claim lease; not dispatching", row.OutboxID)
		return false
	}
	marker := bpwLastErrDispatching + strings.TrimPrefix(row.LeaseToken, "claimed:")
	n, err := bpwExecRowsAffected(ctx, pool,
		`UPDATE notification_svc.outbox
		    SET last_error = $2, processed_at = NOW()
		  WHERE outbox_id = $1
		    AND processing_status = 'PROCESSING'
		    AND last_error = $3`,
		row.OutboxID, marker, row.LeaseToken)
	if err != nil || n == 0 {
		logger.LogError("[browser-push] outbox=%s dispatch lease not taken: %v", row.OutboxID, err)
		return false
	}
	row.LeaseToken = marker
	return true
}

func bpwRecordAmbiguous(ctx context.Context, pool *pgxpool.Pool, row bpwOutboxRow, reason string) {
	if !strings.HasPrefix(reason, bpwAmbiguousSendPrefix) {
		reason = bpwAmbiguousSendPrefix + " " + reason
	}
	if row.LeaseToken == "" {
		logger.LogError("[browser-push] ambiguous outcome not recorded outbox=%s: missing lease", row.OutboxID)
		return
	}
	n, err := bpwExecRowsAffected(ctx, pool,
		`UPDATE notification_svc.outbox
		    SET last_error = $2
		  WHERE outbox_id = $1
		    AND processing_status = 'PROCESSING'
		    AND last_error = $3`,
		row.OutboxID, bpwLimit(reason, 400), row.LeaseToken)
	if err != nil || n == 0 {
		logger.LogError("[browser-push] ambiguous outcome not recorded outbox=%s: %v", row.OutboxID, err)
		return
	}
	logger.LogError("[browser-push] ambiguous send outbox=%s left PROCESSING (no immediate retry): %s",
		row.OutboxID, bpwLimit(reason, 200))
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
		logger.LogError("[browser-push-digest] candidate query: %v", err)
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
	logger.LogInfo("[browser-push-digest] sending digest to %d user(s)", len(userIDs))

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
		logger.LogError("[browser-push-digest] fetch items userID=%s: %v", userID, err)
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
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM notification_svc.in_app_notification
		WHERE recipient_user_id = $1 AND is_read = FALSE AND is_deleted = FALSE
	`, userID).Scan(&totalUnread); err != nil {
		logger.LogError("[browser-push-digest] count unread query row failed: %v", err)
	}

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
		logger.LogError("[browser-push-digest] all subs failed for userID=%s", userID)
		return
	}

	// Upsert digest tracker
	if _, err := pool.Exec(ctx, `
		INSERT INTO notification_svc.push_digest_tracker (user_id, last_digest_at)
		VALUES ($1, NOW())
		ON CONFLICT (user_id) DO UPDATE SET last_digest_at = NOW()
	`, userID); err != nil {
		logger.LogError("[browser-push-digest] upsert digest tracker exec failed: %v", err)
	}

	logger.LogInfo("[browser-push-digest] sent to userID=%s unread=%d top=%d",
		userID, totalUnread, len(items))
}

// ─────────────────────────────────────────────────────────────────────────────
// VAPID send (webpush-go library)
// ─────────────────────────────────────────────────────────────────────────────

func bpwSendVAPID(cfg *vapidConfig, sub bpwSubscription, payload []byte) error {
	endpoint, err := url.Parse(strings.TrimSpace(sub.Endpoint))
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" {
		return fmt.Errorf("push endpoint must be https")
	}
	resp, err := webpush.SendNotification(payload, &webpush.Subscription{
		Endpoint: sub.Endpoint,
		Keys: webpush.Keys{
			P256dh: sub.P256DH,
			Auth:   sub.AuthKey,
		},
	}, &webpush.Options{
		HTTPClient:      pushHTTPClient,
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
	if row.LeaseToken == "" {
		logger.LogError("[browser-push] SENT update skipped outbox=%s: missing lease", row.OutboxID)
		return
	}
	if _, err := bpwExecRowsAffected(ctx, pool,
		`UPDATE notification_svc.outbox
		    SET last_error = $2
		  WHERE outbox_id = $1
		    AND processing_status = 'PROCESSING'
		    AND last_error = $3`,
		row.OutboxID, bpwLastErrSendAccepted, row.LeaseToken); err != nil {
		logger.LogError("[browser-push] record send accepted failed outbox=%s: %v", row.OutboxID, err)
	}

	n, err := bpwExecRowsAffected(ctx, pool,
		`UPDATE notification_svc.outbox
		    SET processing_status = 'SENT', sent_at = NOW(), last_error = NULL
		  WHERE outbox_id = $1
		    AND (
		          (processing_status = 'PROCESSING' AND last_error IN ($2, $3))
		       OR (processing_status = 'DEAD' AND last_error LIKE 'stale PROCESSING after dispatch%')
		    )`,
		row.OutboxID, bpwLastErrSendAccepted, row.LeaseToken)
	if err != nil {
		logger.LogError("[browser-push] SENT update failed outbox=%s: %v", row.OutboxID, err)
		return
	}
	if n == 0 {
		var status string
		qerr := pool.QueryRow(ctx,
			`SELECT processing_status FROM notification_svc.outbox WHERE outbox_id = $1`,
			row.OutboxID).Scan(&status)
		if qerr != nil || status != "SENT" {
			logger.LogError("[browser-push] SENT update matched no row outbox=%s status=%q err=%v; not treating as done",
				row.OutboxID, status, qerr)
			return
		}
	}
	logger.LogInfo("[browser-push] SENT outbox=%s", row.OutboxID)
}

func bpwMarkFailed(ctx context.Context, pool *pgxpool.Pool, row bpwOutboxRow, errMsg string) {
	if strings.HasPrefix(errMsg, bpwAmbiguousSendPrefix) || strings.HasPrefix(errMsg, "dispatching") {
		bpwRecordAmbiguous(ctx, pool, row, errMsg)
		return
	}
	if row.LeaseToken == "" {
		logger.LogError("[browser-push] outbox=%s failure not applied; missing lease", row.OutboxID)
		return
	}

	retryMax, retryBackoffSecs := bpwFetchRetryConfig(ctx, pool, row.EventID)
	if row.RetryCount < retryMax {
		backoffSecs := float64(retryBackoffSecs) * math.Pow(2, float64(row.RetryCount))
		nextAt := time.Now().Add(time.Duration(backoffSecs) * time.Second)
		n, err := bpwExecRowsAffected(ctx, pool, `
			UPDATE notification_svc.outbox
			   SET processing_status = 'PENDING',
			       retry_count       = retry_count + 1,
			       last_error        = $2,
			       scheduled_at      = $3
			 WHERE outbox_id = $1
			   AND processing_status = 'PROCESSING'
			   AND last_error = $4
		`, row.OutboxID, errMsg, nextAt, row.LeaseToken)
		if err != nil {
			logger.LogError("[browser-push] retry update failed outbox=%s: %v", row.OutboxID, err)
			return
		}
		if n == 0 {
			logger.LogError("[browser-push] retry update matched no row outbox=%s; not treating as retried", row.OutboxID)
			return
		}
		logger.LogInfo("[browser-push] RETRY outbox=%s attempt=%d next=%s",
			row.OutboxID, row.RetryCount+1, nextAt.Format(time.RFC3339))
	} else {
		n, err := bpwExecRowsAffected(ctx, pool, `
			UPDATE notification_svc.outbox
			   SET processing_status = 'DEAD',
			       retry_count       = retry_count + 1,
			       last_error        = $2
			 WHERE outbox_id = $1
			   AND processing_status = 'PROCESSING'
			   AND last_error = $3
		`, row.OutboxID, errMsg, row.LeaseToken)
		if err != nil {
			logger.LogError("[browser-push] DEAD update failed outbox=%s: %v", row.OutboxID, err)
			return
		}
		if n == 0 {
			logger.LogError("[browser-push] DEAD update matched no row outbox=%s; not treating as failed", row.OutboxID)
			return
		}
		logger.LogInfo("[browser-push] DEAD outbox=%s after %d attempts",
			row.OutboxID, row.RetryCount+1)
	}
}

func bpwReapStaleProcessing(ctx context.Context, pool *pgxpool.Pool, limit int) {
	if limit < 1 {
		limit = 1
	}
	staleSecs := int(bpwProcessingStaleAfter() / time.Second)
	rows, err := pool.Query(ctx, `
		SELECT o.outbox_id,
		       COALESCE(o.recipient_user_id, ''),
		       o.event_id,
		       COALESCE(o.retry_count, 0),
		       COALESCE(o.last_error, ''),
		       EXISTS (
		           SELECT 1 FROM notification_svc.send_history h
		            WHERE h.outbox_id = o.outbox_id AND h.processing_status = 'SENT'
		       )
		FROM   notification_svc.outbox o
		WHERE  o.processing_status = 'PROCESSING'
		  AND  o.channel = 'PUSH'
		  AND  (o.processed_at IS NULL OR o.processed_at < now() - make_interval(secs => $1))
		ORDER  BY o.processed_at ASC NULLS FIRST
		LIMIT  $2`, staleSecs, limit)
	if err != nil {
		logger.LogError("[browser-push] reap stale PROCESSING: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var row bpwOutboxRow
		var lastErr string
		var sendAccepted bool
		if err := rows.Scan(
			&row.OutboxID, &row.RecipientUserID, &row.EventID, &row.RetryCount,
			&lastErr, &sendAccepted,
		); err != nil {
			logger.LogError("[browser-push] reap scan: %v", err)
			continue
		}
		bpwReconcileStale(ctx, pool, row, strings.TrimSpace(lastErr), sendAccepted)
	}
	if rows.Err() != nil {
		logger.LogError("[browser-push] reap rows: %v", rows.Err())
	}
}

func bpwReconcileStale(ctx context.Context, pool *pgxpool.Pool, row bpwOutboxRow, lastErr string, sendAccepted bool) {
	if sendAccepted || lastErr == bpwLastErrSendAccepted {
		bpwReconcileAccepted(ctx, pool, row)
		return
	}
	if lastErr == "" || strings.HasPrefix(lastErr, "claimed:") {
		bpwRequeueBeforeDispatch(ctx, pool, row, lastErr)
		return
	}
	if strings.HasPrefix(lastErr, "dispatching") || strings.HasPrefix(lastErr, bpwAmbiguousSendPrefix) {
		msg := lastErr
		if strings.HasPrefix(lastErr, "dispatching") {
			msg = "stale PROCESSING after dispatch; send outcome unknown, not resent"
		}
		bpwFailStaleProcessing(ctx, pool, row, lastErr, msg)
		return
	}
	row.LeaseToken = lastErr
	bpwMarkFailed(ctx, pool, row, lastErr)
}

func bpwReconcileAccepted(ctx context.Context, pool *pgxpool.Pool, row bpwOutboxRow) {
	n, err := bpwExecRowsAffected(ctx, pool,
		`UPDATE notification_svc.outbox
		    SET processing_status = 'SENT', sent_at = COALESCE(sent_at, now()), last_error = NULL
		  WHERE outbox_id = $1 AND processing_status = 'PROCESSING'`,
		row.OutboxID)
	if err != nil {
		logger.LogError("[browser-push] reconcile SENT failed outbox=%s: %v", row.OutboxID, err)
		return
	}
	if n == 0 {
		return
	}
	logger.LogInfo("[browser-push] reconciled SENT outbox=%s (no resend)", row.OutboxID)
}

func bpwRequeueBeforeDispatch(ctx context.Context, pool *pgxpool.Pool, row bpwOutboxRow, observed string) {
	msg := "reaped stale PROCESSING before dispatch"
	n, err := bpwExecRowsAffected(ctx, pool,
		`UPDATE notification_svc.outbox
		    SET processing_status = 'PENDING',
		        last_error = $2,
		        scheduled_at = now()
		  WHERE outbox_id = $1
		    AND processing_status = 'PROCESSING'
		    AND COALESCE(last_error, '') = $3`,
		row.OutboxID, msg, observed)
	if err != nil {
		logger.LogError("[browser-push] requeue stale outbox=%s: %v", row.OutboxID, err)
		return
	}
	if n == 0 {
		return
	}
	logger.LogInfo("[browser-push] requeued stale outbox=%s (never dispatched)", row.OutboxID)
}

func bpwFailStaleProcessing(ctx context.Context, pool *pgxpool.Pool, row bpwOutboxRow, observed, msg string) {
	n, err := bpwExecRowsAffected(ctx, pool,
		`UPDATE notification_svc.outbox
		    SET processing_status = 'DEAD',
		        last_error = $2
		  WHERE outbox_id = $1
		    AND processing_status = 'PROCESSING'
		    AND last_error = $3`,
		row.OutboxID, bpwLimit(msg, 400), observed)
	if err != nil {
		logger.LogError("[browser-push] stale PROCESSING fail update outbox=%s: %v", row.OutboxID, err)
		return
	}
	if n == 0 {
		return
	}
	logger.LogError("[browser-push] DEAD stale outbox=%s err=%s", row.OutboxID, bpwLimit(msg, 200))
}

func bpwFetchRetryConfig(ctx context.Context, pool *pgxpool.Pool, eventID string) (int, int) {
	retryMax, retryBackoffSecs := 3, 60
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(retry_max,3), COALESCE(retry_backoff_secs,60)
		FROM notification_svc.notification_config
		WHERE event_id = $1 AND channel = 'PUSH'
	`, eventID).Scan(&retryMax, &retryBackoffSecs); err != nil {
		logger.LogError("[browser-push] fetch retry config query row failed for event=%s: %v", eventID, err)
	}
	return retryMax, retryBackoffSecs
}

func bpwDeactivateSubscription(ctx context.Context, pool *pgxpool.Pool, subID string) {
	if _, err := pool.Exec(ctx, `
		UPDATE notification_svc.push_subscription
		   SET is_active = FALSE, updated_at = NOW()
		 WHERE id = $1
	`, subID); err != nil {
		logger.LogError("[browser-push] deactivate subscription exec failed for id=%s: %v", subID, err)
	}
	logger.LogInfo("[browser-push] deactivated expired subscription id=%s", subID)
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

func bpwLimit(s string, max int) string {
	if max < 0 || len(s) <= max {
		return s
	}
	return s[:max]
}

func bpwPushTitle(subject string) string {
	subject = strings.TrimSpace(subject)
	if subject != "" {
		return subject
	}
	return "Notification"
}

func bpwPushBody(subject, body string) string {
	body = strings.TrimSpace(body)
	if body != "" {
		return body
	}
	subject = strings.TrimSpace(subject)
	if subject != "" {
		return subject
	}
	return "You have a new notification. Please review it in the system."
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
