package jobs

// inbox_worker.go — SSE delivery worker for in-app (PUSH channel) notifications.
//
// Responsibilities:
//  1. Every IN_APP_WORKER_POLL_SECS seconds, fetch all users currently connected
//     to the SSE server.
//  2. For each connected user, query in_app_notification rows that are either
//     PENDING or DELIVERED-but-resend_after<=NOW() and is_read=false, is_deleted=false.
//  3. Push each notification over SSE, then mark sse_status='DELIVERED'.
//  4. Also export PushUnreadCountToUser — used by sse.go on initial connect.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"CimplrCorpSaas/internal/dashboard"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ─────────────────────────────────────────────────────────────────────────────
// Env helpers (prefixed iaw_ to avoid collisions with outbox_worker iaw_ helpers)
// ─────────────────────────────────────────────────────────────────────────────

func iawGetenvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func iawGetenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// ─────────────────────────────────────────────────────────────────────────────
// SSE message types
// ─────────────────────────────────────────────────────────────────────────────

type inAppSSENotification struct {
	Type          string `json:"type"`
	ID            string `json:"id"`
	OutboxID      string `json:"outbox_id"`
	CorrelationID string `json:"correlation_id"`
	EventID       string `json:"event_id"`
	Subject       string `json:"subject"`
	Body          string `json:"body"`
	PriorityLevel int    `json:"priority_level"`
	CreatedAt     string `json:"created_at"`
}

type inAppSSECount struct {
	Type   string `json:"type"`
	Unread int    `json:"unread"`
}

// ─────────────────────────────────────────────────────────────────────────────
// StartInboxWorker
// ─────────────────────────────────────────────────────────────────────────────

// StartInboxWorker polls in_app_notification for connected SSE users and pushes
// pending notifications. Controlled by env vars:
//
//	IN_APP_WORKER_ENABLED   — default true
//	IN_APP_WORKER_POLL_SECS — default 15
//	IN_APP_RESEND_MINUTES   — default 90  (re-push DELIVERED rows after N minutes)
//	IN_APP_WORKER_BATCH     — default 50  (max rows fetched per user per tick)
func StartInboxWorker(ctx context.Context, pool *pgxpool.Pool) {
	if !iawGetenvBool("IN_APP_WORKER_ENABLED", true) {
		fmt.Println("[IAW] In-app inbox worker disabled via env")
		return
	}

	pollSecs := iawGetenvInt("IN_APP_WORKER_POLL_SECS", 15)
	ticker := time.NewTicker(time.Duration(pollSecs) * time.Second)
	defer ticker.Stop()

	fmt.Printf("[IAW] In-app inbox worker started (poll=%ds)\n", pollSecs)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("[IAW] In-app inbox worker stopped")
			return
		case <-ticker.C:
			batchSize := iawGetenvInt("IN_APP_WORKER_BATCH", 50)
			if err := iawProcessBatch(ctx, pool, batchSize); err != nil {
				fmt.Printf("[IAW] iawProcessBatch error: %v\n", err)
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// iawProcessBatch — core delivery loop
// ─────────────────────────────────────────────────────────────────────────────

type iawInboxRow struct {
	id            string
	outboxID      string
	correlationID string
	eventID       string
	subject       string
	body          string
	priorityLevel int
	createdAt     time.Time
}

func iawProcessBatch(ctx context.Context, pool *pgxpool.Pool, batchSize int) error {
	onlineUsers := dashboard.GetClients()
	if len(onlineUsers) == 0 {
		return nil
	}

	resendMinutes := iawGetenvInt("IN_APP_RESEND_MINUTES", 90)

	for _, userID := range onlineUsers {
		rows, err := iawFetchPending(ctx, pool, userID, batchSize, resendMinutes)
		if err != nil {
			fmt.Printf("[IAW] fetch error for user %s: %v\n", userID, err)
			continue
		}
		if len(rows) == 0 {
			continue
		}

		deliveredIDs := make([]string, 0, len(rows))

		for _, row := range rows {
			msg := inAppSSENotification{
				Type:          "notification",
				ID:            row.id,
				OutboxID:      row.outboxID,
				CorrelationID: row.correlationID,
				EventID:       row.eventID,
				Subject:       row.subject,
				Body:          row.body,
				PriorityLevel: row.priorityLevel,
				CreatedAt:     row.createdAt.Format(time.RFC3339),
			}
			b, err := json.Marshal(msg)
			if err != nil {
				fmt.Printf("[IAW] marshal error for row %s: %v\n", row.id, err)
				continue
			}
			dashboard.SendToUser(userID, b)
			deliveredIDs = append(deliveredIDs, row.id)
		}

		if len(deliveredIDs) > 0 {
			resendAfter := time.Now().Add(time.Duration(resendMinutes) * time.Minute)
			if err := iawMarkDelivered(ctx, pool, deliveredIDs, resendAfter); err != nil {
				fmt.Printf("[IAW] markDelivered error for user %s: %v\n", userID, err)
			}
		}

		// Push updated unread count after delivery
		if err := PushUnreadCountToUser(ctx, pool, userID); err != nil {
			fmt.Printf("[IAW] unread count push error for user %s: %v\n", userID, err)
		}
	}
	return nil
}

// iawFetchPending returns rows that are:
//   - PENDING (never delivered), OR
//   - DELIVERED but resend_after has passed (user may have been offline)
//
// Excludes is_deleted=true and is_read=true rows.
func iawFetchPending(
	ctx context.Context,
	pool *pgxpool.Pool,
	userID string,
	limit int,
	resendMinutes int,
) ([]iawInboxRow, error) {
	q := `
		SELECT id, outbox_id, correlation_id, event_id,
		       COALESCE(subject,''), COALESCE(body,''),
		       COALESCE(priority_level, 3), created_at
		FROM notification_svc.in_app_notification
		WHERE recipient_user_id = $1
		  AND is_deleted = false
		  AND is_read    = false
		  AND (
		      sse_status = 'PENDING'
		      OR (sse_status = 'DELIVERED' AND resend_after <= NOW())
		  )
		ORDER BY priority_level ASC, created_at ASC
		LIMIT $2
	`
	dbRows, err := pool.Query(ctx, q, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("iawFetchPending: %w", err)
	}
	defer dbRows.Close()

	var out []iawInboxRow
	for dbRows.Next() {
		var r iawInboxRow
		if err := dbRows.Scan(
			&r.id, &r.outboxID, &r.correlationID, &r.eventID,
			&r.subject, &r.body, &r.priorityLevel, &r.createdAt,
		); err != nil {
			fmt.Printf("[IAW] scan error: %v\n", err)
			continue
		}
		out = append(out, r)
	}
	if dbRows.Err() != nil {
		return nil, fmt.Errorf("iawFetchPending rows: %w", dbRows.Err())
	}
	return out, nil
}

// iawMarkDelivered sets sse_status='DELIVERED' and resend_after for delivered rows.
func iawMarkDelivered(ctx context.Context, pool *pgxpool.Pool, ids []string, resendAfter time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	// Build $1,$2,... placeholders
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids)+1)
	args[0] = resendAfter
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id
	}
	q := fmt.Sprintf(`
		UPDATE notification_svc.in_app_notification
		SET sse_status = 'DELIVERED',
		    sse_delivered_at = NOW(),
		    resend_after = $1
		WHERE id IN (%s)
	`, joinStrings(placeholders, ","))
	_, err := pool.Exec(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("iawMarkDelivered: %w", err)
	}
	return nil
}

// joinStrings joins a slice of strings with sep (avoids importing strings in this file).
func joinStrings(ss []string, sep string) string {
	if len(ss) == 0 {
		return ""
	}
	out := ss[0]
	for _, s := range ss[1:] {
		out += sep + s
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// PushUnreadCountToUser — public, called from sse.go on connect
// ─────────────────────────────────────────────────────────────────────────────

// PushUnreadCountToUser queries the unread count for userID and pushes a
// notification_count SSE message. Non-fatal if the user is not connected.
func PushUnreadCountToUser(ctx context.Context, pool *pgxpool.Pool, userID string) error {
	var count int
	q := `
		SELECT COUNT(*)
		FROM notification_svc.in_app_notification
		WHERE recipient_user_id = $1
		  AND is_read    = false
		  AND is_deleted = false
	`
	if err := pool.QueryRow(ctx, q, userID).Scan(&count); err != nil {
		return fmt.Errorf("PushUnreadCountToUser: %w", err)
	}

	msg := inAppSSECount{
		Type:   "notification_count",
		Unread: count,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("PushUnreadCountToUser marshal: %w", err)
	}
	dashboard.SendToUser(userID, b)
	return nil
}
