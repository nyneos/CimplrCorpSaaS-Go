package emailjobs

import (
	"context"
	"fmt"
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
	if err := RequireEmailService(ctx, pool, rt); err != nil {
		return err
	}
	inboxes, err := loadGraphInboxes(ctx, pool)
	if err != nil {
		return err
	}
	if len(inboxes) == 0 {
		logger.LogInfo("[graph-poller] no OUTLOOK_GRAPH mailboxes approved")
		return nil
	}

	type graphPollTarget struct {
		inbox  graphInboxRow
		folder graphFolderPoll
	}

	receivedTargets := make([]graphPollTarget, 0, len(inboxes))
	sentTargets := make([]graphPollTarget, 0, len(inboxes))
	for _, inbox := range inboxes {
		receivedTargets = append(receivedTargets, graphPollTarget{
			inbox: inbox,
			folder: graphFolderPoll{
				direction:  mailDirectionReceived,
				sentFolder: false,
				lastSync:   &inbox.LastSync,
				setSyncFn:  setGraphLastSync,
			},
		})
		sentTargets = append(sentTargets, graphPollTarget{
			inbox: inbox,
			folder: graphFolderPoll{
				direction:  mailDirectionSent,
				sentFolder: true,
				lastSync:   &inbox.SentLastSync,
				setSyncFn:  setGraphSentLastSync,
			},
		})
	}

	var ingested int
	mailboxDelay := time.Duration(getenvInt("GRAPH_MAILBOX_POLL_DELAY_MS", 1500)) * time.Millisecond
	pollTargets := func(targets []graphPollTarget) {
		for i, target := range targets {
			if i > 0 && mailboxDelay > 0 {
				time.Sleep(mailboxDelay)
			}
			n, err := pollGraphMailboxFolder(ctx, pool, rt, target.inbox, target.folder)
			if err != nil {
				logger.LogError("[graph-poller] mailbox=%s direction=%s err=%v", target.inbox.MailboxAddress, target.folder.direction, err)
				_, _ = pool.Exec(ctx, `
					UPDATE email_svc.inbox_config
					SET ses_last_error = $2, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, target.inbox.InboxID, "graph: "+err.Error())
				continue
			}
			ingested += n
			clearMailboxPollError(ctx, pool, target.inbox.InboxID)
		}
	}
	pollTargets(receivedTargets)
	pollTargets(sentTargets)
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
		sinceStr = formatPollSince(**cursor)
	}

	skipIDs, err := loadKnownGraphMessageIDs(ctx, pool, inbox.InboxID, *cursor)
	if err != nil {
		return 0, fmt.Errorf("load known graph message ids: %w", err)
	}

	var totalIngested int
	for {
		resp, err := rt.PullGraphMessages(ctx, inbox.InboxID, inbox.MailboxAddress, folder.sentFolder, sinceStr, graphPullPageSize, conn, skipIDs)
		if err != nil {
			return totalIngested, err
		}

		if resp.Initialized {
			t, err := parsePollSince(resp.NewSince)
			if err != nil {
				t = time.Now().UTC()
			}
			if err := folder.setSyncFn(ctx, pool, inbox.InboxID, t); err != nil {
				return totalIngested, err
			}
			*cursor = &t
			logger.LogInfo("[graph-poller] mailbox=%s direction=%s initialized cursor (new mail only from %s)",
				inbox.MailboxAddress, folder.direction, formatPollSince(t))
			return 0, nil
		}

		if resp.Fetched == 0 {
			break
		}

		n, err := ingestGraphPollPage(ctx, pool, inbox, folder, resp, "OUTLOOK_GRAPH")
		if err != nil {
			return totalIngested, err
		}
		totalIngested += n

		if err := applyPollPageCursor(ctx, pool, inbox.InboxID, sinceStr, resp.NewSince, cursor, folder.setSyncFn); err != nil {
			return totalIngested, err
		}
		if *cursor != nil {
			sinceStr = formatPollSince(**cursor)
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

func ingestGraphPollPage(ctx context.Context, pool *pgxpool.Pool, inbox graphInboxRow, folder graphFolderPoll, resp *mailruntime.GraphPullResult, sourceType string) (int, error) {
	inboxRow := inboxRow{
		InboxID:        inbox.InboxID,
		MailboxAddress: inbox.MailboxAddress,
		FiltersJSON:    inbox.FiltersJSON,
		Module:         inbox.Module,
		EntityID:       inbox.EntityID,
	}

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

		matchIn := matchInputFromParsed(gm.Parsed)
		ok, active := directionFilterMatch(inbox.FiltersJSON, folder.direction, matchIn)
		if !active || !ok {
			logger.LogInfo("[graph-poller] skip graph_id=%s direction=%s (inbox filter)", gm.GraphMessageID, folder.direction)
			continue
		}

		if err := ingestGraphMessage(ctx, pool, inboxRow, gm.Parsed, gm.GraphMessageID, folder.direction, sourceType); err != nil {
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

func ingestGraphMessage(ctx context.Context, pool *pgxpool.Pool, inbox inboxRow, msg mailruntime.ParsedMessage, graphMessageID, mailDirection, sourceType string) error {
	step := "GRAPH_INGEST"
	if sourceType == "GOOGLE_WORKSPACE" {
		step = "GMAIL_DWD_INGEST"
	} else if sourceType == "OAUTH" {
		step = "OAUTH_INGEST"
	}
	return ingestPollMessage(ctx, pool, inbox, msg, pollIngestIdentity{
		GraphMessageID: graphMessageID,
		SourceType:     sourceType,
		LogStep:        step,
	}, mailDirection)
}
