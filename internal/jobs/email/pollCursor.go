package emailjobs

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func formatPollSince(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func parsePollSince(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	return time.Parse(time.RFC3339, s)
}

// loadKnownGraphMessageIDs returns provider message IDs already ingested for an inbox.
// When since is set, only IDs from messages on/after (since - lookback) are returned so
// poll payloads stay bounded as mailboxes grow.
func loadKnownGraphMessageIDs(ctx context.Context, pool *pgxpool.Pool, inboxID string, since *time.Time) ([]string, error) {
	const lookback = 7 * 24 * time.Hour

	var (
		rows pgx.Rows
		err  error
	)
	if since != nil && !since.IsZero() {
		cutoff := since.UTC().Add(-lookback)
		rows, err = pool.Query(ctx, `
			SELECT graph_message_id
			FROM email_svc.message
			WHERE inbox_id = $1::uuid
			  AND graph_message_id IS NOT NULL
			  AND graph_message_id <> ''
			  AND COALESCE(received_at, created_at) >= $2
		`, inboxID, cutoff)
	} else {
		rows, err = pool.Query(ctx, `
			SELECT graph_message_id
			FROM email_svc.message
			WHERE inbox_id = $1::uuid
			  AND graph_message_id IS NOT NULL
			  AND graph_message_id <> ''
		`, inboxID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// loadKnownIMAPMessageKeys returns imap_message_key values already ingested for an inbox.
func loadKnownIMAPMessageKeys(ctx context.Context, pool *pgxpool.Pool, inboxID string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT imap_message_key
		FROM email_svc.message
		WHERE inbox_id = $1::uuid
		  AND imap_message_key IS NOT NULL
		  AND imap_message_key <> ''
	`, inboxID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

type pollCursorStore func(context.Context, *pgxpool.Pool, string, time.Time) error

// applyPollPageCursor persists new_since from the email service when it moves forward.
func applyPollPageCursor(
	ctx context.Context,
	pool *pgxpool.Pool,
	inboxID string,
	sinceStr string,
	newSinceStr string,
	cursor **time.Time,
	store pollCursorStore,
) error {
	newSinceStr = strings.TrimSpace(newSinceStr)
	if newSinceStr == "" {
		return nil
	}
	nextSince, err := parsePollSince(newSinceStr)
	if err != nil {
		return err
	}
	prev := time.Time{}
	if strings.TrimSpace(sinceStr) != "" {
		prev, _ = parsePollSince(sinceStr)
	}
	if sinceStr != "" && !nextSince.After(prev) {
		return nil
	}
	if err := store(ctx, pool, inboxID, nextSince); err != nil {
		return err
	}
	t := nextSince
	*cursor = &t
	return nil
}
