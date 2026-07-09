package emailjobs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	mailDirectionReceived = "RECEIVED"
	mailDirectionSent     = "SENT"
)

const graphPullPageSize = 100

type graphCred struct {
	TenantLabel  string
	TenantID     string
	ClientID     string
	ClientSecret string
}

func (g graphCred) toPayload() mailruntime.GraphConnection {
	return mailruntime.GraphConnection{
		TenantLabel:  g.TenantLabel,
		TenantID:     g.TenantID,
		ClientID:     g.ClientID,
		ClientSecret: g.ClientSecret,
	}
}

type graphInboxRow struct {
	InboxID        string
	MailboxAddress string
	FiltersJSON    []byte
	Module         string
	EntityID       string
	GraphTenantKey string
	Graph          graphCred
	LastSync       *time.Time
	SentLastSync   *time.Time
}

type graphFolderPoll struct {
	direction  string
	sentFolder bool
	lastSync   **time.Time
	setSyncFn  func(ctx context.Context, pool *pgxpool.Pool, inboxID string, t time.Time) error
}

func graphPollerEnabled() bool {
	return getenvBool("GRAPH_POLLER_ENABLED", true)
}

// StartGraphPoller polls Outlook mailboxes via Microsoft Graph (inbox + sent items).
func StartGraphPoller(ctx context.Context, pool *pgxpool.Pool) {
	if !graphPollerEnabled() {
		logger.LogInfo("[graph-poller] disabled via GRAPH_POLLER_ENABLED=false")
		return
	}
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		logger.LogInfo("[graph-poller] mail runtime key not set — idle")
		return
	}

	interval := time.Duration(getenvInt("GRAPH_POLL_INTERVAL_SECS", getenvInt("EMAIL_POLL_INTERVAL_SECS", 60))) * time.Second
	logger.LogInfo("[graph-poller] started (poll=%s inbox+sent)", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		if err := graphPollOnce(ctx, pool, rt); err != nil {
			logger.LogError("[graph-poller] tick error: %v", err)
		}
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			logger.LogInfo("[graph-poller] stopped")
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// TriggerGraphPoll runs one Graph poll cycle immediately.
func TriggerGraphPoll(ctx context.Context, pool *pgxpool.Pool) error {
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return fmt.Errorf("mail processing not configured")
	}
	return graphPollOnce(ctx, pool, rt)
}

func graphPollOnce(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime) error {
	inboxes, err := loadGraphInboxes(ctx, pool)
	if err != nil {
		return err
	}
	if len(inboxes) == 0 {
		logger.LogInfo("[graph-poller] no OUTLOOK_GRAPH mailboxes approved")
		return nil
	}

	var ingested int
	for _, inbox := range inboxes {
		folders := []graphFolderPoll{
			{
				direction:  mailDirectionReceived,
				sentFolder: false,
				lastSync:   &inbox.LastSync,
				setSyncFn:  setGraphLastSync,
			},
			{
				direction:  mailDirectionSent,
				sentFolder: true,
				lastSync:   &inbox.SentLastSync,
				setSyncFn:  setGraphSentLastSync,
			},
		}
		for _, folder := range folders {
			n, err := pollGraphMailboxFolder(ctx, pool, rt, inbox, folder)
			if err != nil {
				logger.LogError("[graph-poller] mailbox=%s direction=%s err=%v", inbox.MailboxAddress, folder.direction, err)
				continue
			}
			ingested += n
		}
	}
	if ingested > 0 {
		logger.LogInfo("[graph-poller] ingested %d message(s)", ingested)
	}
	return nil
}

func loadGraphInboxes(ctx context.Context, pool *pgxpool.Pool) ([]graphInboxRow, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT inbox_id::text, mailbox_address, filters_json,
		       COALESCE(module,''), COALESCE(entity_id,''),
		       %s, %s, %s,
		       %s, %s,
		       graph_last_sync_at, graph_sent_last_sync_at
		FROM email_svc.inbox_config
		WHERE is_active = true
		  AND processing_status = 'APPROVED'
		  AND is_deleted = false
		  AND COALESCE(source_type,'OUTLOOK_GRAPH') = 'OUTLOOK_GRAPH'
	`, SQLGraphTenantKey, SQLGraphTenantLabel, SQLGraphTenantID,
		SQLGraphClientID, SQLGraphSecret))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []graphInboxRow
	for rows.Next() {
		var r graphInboxRow
		if err := rows.Scan(&r.InboxID, &r.MailboxAddress, &r.FiltersJSON, &r.Module, &r.EntityID,
			&r.GraphTenantKey, &r.Graph.TenantLabel, &r.Graph.TenantID, &r.Graph.ClientID, &r.Graph.ClientSecret,
			&r.LastSync, &r.SentLastSync); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func pollGraphMailboxFolder(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime, inbox graphInboxRow, folder graphFolderPoll) (int, error) {
	conn, err := ResolveGraphConnection(ctx, pool, GraphMailboxCreds{
		TenantKey:    inbox.GraphTenantKey,
		TenantLabel:  inbox.Graph.TenantLabel,
		TenantID:     inbox.Graph.TenantID,
		ClientID:     inbox.Graph.ClientID,
		ClientSecret: inbox.Graph.ClientSecret,
	})
	if err != nil {
		return 0, fmt.Errorf("resolve graph creds: %w", err)
	}

	cursor := folder.lastSync
	sinceStr := ""
	if *cursor != nil {
		sinceStr = (*cursor).UTC().Format(time.RFC3339)
	}

	var totalIngested int
	for {
		resp, err := rt.PullGraphMessages(ctx, inbox.InboxID, inbox.MailboxAddress, folder.sentFolder, sinceStr, graphPullPageSize, conn)
		if err != nil {
			return totalIngested, err
		}

		if resp.Initialized {
			t, err := time.Parse(time.RFC3339, resp.NewSince)
			if err != nil {
				t = time.Now().UTC()
			}
			if err := folder.setSyncFn(ctx, pool, inbox.InboxID, t); err != nil {
				return totalIngested, err
			}
			*cursor = &t
			logger.LogInfo("[graph-poller] mailbox=%s direction=%s initialized cursor (new mail only from %s)",
				inbox.MailboxAddress, folder.direction, t.Format(time.RFC3339))
			return 0, nil
		}

		if resp.Fetched == 0 {
			break
		}

		n, err := ingestGraphPollPage(ctx, pool, inbox, folder, resp)
		if err != nil {
			return totalIngested, err
		}
		totalIngested += n

		if strings.TrimSpace(resp.NewSince) != "" {
			nextSince, err := time.Parse(time.RFC3339, resp.NewSince)
			if err == nil && (sinceStr == "" || nextSince.After(mustParseRFC3339(sinceStr))) {
				sinceStr = resp.NewSince
				if err := folder.setSyncFn(ctx, pool, inbox.InboxID, nextSince); err != nil {
					return totalIngested, err
				}
				t := nextSince
				*cursor = &t
			}
		}

		if resp.Fetched < graphPullPageSize {
			break
		}
	}
	return totalIngested, nil
}

func mustParseRFC3339(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func ingestGraphPollPage(ctx context.Context, pool *pgxpool.Pool, inbox graphInboxRow, folder graphFolderPoll, resp *mailruntime.GraphPullResult) (int, error) {
	inboxRow := inboxRow{
		InboxID:        inbox.InboxID,
		MailboxAddress: inbox.MailboxAddress,
		FiltersJSON:    inbox.FiltersJSON,
		Module:         inbox.Module,
		EntityID:       inbox.EntityID,
	}

	var f filters
	_ = json.Unmarshal(inbox.FiltersJSON, &f)

	var ingested int
	for _, gm := range resp.Messages {
		if gm.GraphMessageID == "" {
			continue
		}
		exists, err := graphMessageExists(ctx, pool, gm.GraphMessageID)
		if err != nil {
			return ingested, err
		}
		if exists {
			continue
		}

		if filtersActive(f) {
			matchIn := matchInputFromParsed(gm.Parsed)
			var ok bool
			if folder.sentFolder {
				ok = matchSentFilters(f, matchIn)
			} else {
				ok = matchFilters(f, matchIn)
			}
			if !ok {
				logger.LogInfo("[graph-poller] skip graph_id=%s direction=%s (inbox filter)", gm.GraphMessageID, folder.direction)
				continue
			}
		}

		if err := ingestGraphMessage(ctx, pool, inboxRow, gm.Parsed, gm.GraphMessageID, folder.direction); err != nil {
			logger.LogError("[graph-poller] ingest graph_id=%s err=%v", gm.GraphMessageID, err)
			continue
		}
		ingested++
		logger.LogInfo("[graph-poller] ingested graph_id=%s direction=%s subject=%q", gm.GraphMessageID, folder.direction, gm.Parsed.Envelope.Subject)
	}
	return ingested, nil
}

func graphMessageExists(ctx context.Context, pool *pgxpool.Pool, graphID string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.message WHERE graph_message_id = $1)
	`, graphID).Scan(&exists)
	return exists, err
}

func setGraphLastSync(ctx context.Context, pool *pgxpool.Pool, inboxID string, t time.Time) error {
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET graph_last_sync_at = $2, updated_at = now()
		WHERE inbox_id = $1::uuid
	`, inboxID, t.UTC())
	return err
}

func setGraphSentLastSync(ctx context.Context, pool *pgxpool.Pool, inboxID string, t time.Time) error {
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET graph_sent_last_sync_at = $2, updated_at = now()
		WHERE inbox_id = $1::uuid
	`, inboxID, t.UTC())
	return err
}

func ingestGraphMessage(ctx context.Context, pool *pgxpool.Pool, inbox inboxRow, msg mailruntime.ParsedEmail, graphMessageID, mailDirection string) error {
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.message WHERE s3_raw_key = $1 OR graph_message_id = $2)
	`, msg.S3RawKey, graphMessageID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}

	var f filters
	_ = json.Unmarshal(inbox.FiltersJSON, &f)
	matchInput := matchInputFromParsed(msg)
	var filterMatched bool
	if filtersActive(f) {
		if strings.EqualFold(mailDirection, mailDirectionSent) {
			filterMatched = matchSentFilters(f, matchInput)
			if !filterMatched {
				return nil
			}
		} else {
			filterMatched = matchFilters(f, matchInput)
			if !filterMatched {
				return nil
			}
		}
	}

	preview := msg.Body.TextPlain
	if preview == "" {
		preview = msg.Body.TextHTML
	}
	if len(preview) > 300 {
		preview = preview[:300]
	}

	toAddrs := msg.Envelope.To
	if len(toAddrs) == 0 {
		if strings.EqualFold(mailDirection, mailDirectionSent) {
			toAddrs = []string{}
		} else {
			toAddrs = []string{inbox.MailboxAddress}
		}
	}

	if mailDirection == "" {
		mailDirection = mailDirectionReceived
	}

	var messageID string
	err := pool.QueryRow(ctx, `
		INSERT INTO email_svc.message (
			inbox_id, s3_raw_key, s3_parsed_key, message_id_header, graph_message_id,
			envelope_from, envelope_to, subject, received_at,
			has_attachments, filter_matched, processing_status,
			module, entity_id, body_text_preview, mail_direction, updated_at
		) VALUES (
			$1::uuid, $2, $3, $4, NULLIF($5,''),
			$6, $7, $8, NULLIF($9,'')::timestamptz,
			$10, $11, 'INGESTED',
			NULLIF($12,''), NULLIF($13,''), $14, $15, now()
		)
		RETURNING message_id::text
	`,
		inbox.InboxID, msg.S3RawKey, msg.S3ParsedKey, msg.Envelope.MessageIDHeader, graphMessageID,
		msg.Envelope.From, toAddrs, msg.Envelope.Subject, msg.Envelope.Date,
		len(msg.Attachments) > 0, filterMatched,
		inbox.Module, inbox.EntityID, preview, mailDirection,
	).Scan(&messageID)
	if err != nil {
		return err
	}

	for _, att := range msg.Attachments {
		var attachmentID string
		err := pool.QueryRow(ctx, `
			INSERT INTO email_svc.message_attachment (
				message_id, filename, content_type, file_size, s3_key, file_hash
			) VALUES ($1::uuid, $2, $3, $4, $5, $6)
			RETURNING attachment_id::text
		`, messageID, att.Filename, att.ContentType, att.SizeBytes, att.S3Key, att.SHA256).Scan(&attachmentID)
		if err != nil {
			return err
		}
		logAttachmentIngest(ctx, pool, messageID, attachmentID, "OUTLOOK_GRAPH", att.Filename, att.ContentType, att.S3Key, att.SizeBytes)
	}

	detail, _ := json.Marshal(map[string]string{
		"source":           "OUTLOOK_GRAPH",
		"graph_message_id": graphMessageID,
		"s3_raw_key":       msg.S3RawKey,
		"mailbox":          inbox.MailboxAddress,
		"mail_direction":   mailDirection,
	})
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.processing_log (message_id, step, status, detail)
		VALUES ($1::uuid, 'GRAPH_INGEST', 'OK', $2::jsonb)
	`, messageID, string(detail))

	return nil
}
