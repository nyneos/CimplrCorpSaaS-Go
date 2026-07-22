package emailjobs

import (
	"context"
	"fmt"
	"time"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const googleWorkspacePullPageSize = 100

type googleWorkspaceCred struct {
	TenantKey           string
	TenantLabel         string
	ServiceAccountEmail string
	ClientID            string
	PrivateKey          string
}

type googleWorkspaceInboxRow struct {
	InboxID        string
	MailboxAddress string
	FiltersJSON    []byte
	Module         string
	EntityID       string
	GoogleTenantKey string
	Google          googleWorkspaceCred
	LastSync        *time.Time
	SentLastSync    *time.Time
}

func googleWorkspacePollerEnabled() bool {
	return getenvBool("GOOGLE_WORKSPACE_POLLER_ENABLED", true)
}

// StartGoogleWorkspacePoller polls Google Workspace mailboxes via Gmail API (domain-wide delegation).
func StartGoogleWorkspacePoller(ctx context.Context, pool *pgxpool.Pool) {
	if !googleWorkspacePollerEnabled() {
		logger.LogInfo("[google-workspace-poller] disabled via GOOGLE_WORKSPACE_POLLER_ENABLED=false")
		return
	}
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		logger.LogInfo("[google-workspace-poller] mail runtime key not set — idle")
		return
	}

	interval := time.Duration(getenvInt("GOOGLE_WORKSPACE_POLL_INTERVAL_SECS", getenvInt("EMAIL_POLL_INTERVAL_SECS", 60))) * time.Second
	logger.LogInfo("[google-workspace-poller] started (poll=%s inbox+sent)", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		if err := googleWorkspacePollOnce(ctx, pool, rt); err != nil {
			logger.LogError("[google-workspace-poller] tick error: %v", err)
		}
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			logger.LogInfo("[google-workspace-poller] stopped")
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// TriggerGoogleWorkspacePoll runs one Google Workspace poll cycle immediately.
func TriggerGoogleWorkspacePoll(ctx context.Context, pool *pgxpool.Pool) error {
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return fmt.Errorf("mail processing not configured")
	}
	return googleWorkspacePollOnce(ctx, pool, rt)
}

func googleWorkspacePollOnce(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime) error {
	if err := RequireEmailService(ctx, pool, rt); err != nil {
		return err
	}
	inboxes, err := loadGoogleWorkspaceInboxes(ctx, pool)
	if err != nil {
		return err
	}
	if len(inboxes) == 0 {
		logger.LogInfo("[google-workspace-poller] no GOOGLE_WORKSPACE mailboxes approved")
		return nil
	}

	type gwPollTarget struct {
		inbox  googleWorkspaceInboxRow
		folder graphFolderPoll
	}

	receivedTargets := make([]gwPollTarget, 0, len(inboxes))
	sentTargets := make([]gwPollTarget, 0, len(inboxes))
	for _, inbox := range inboxes {
		receivedTargets = append(receivedTargets, gwPollTarget{
			inbox: inbox,
			folder: graphFolderPoll{
				direction:  mailDirectionReceived,
				sentFolder: false,
				lastSync:   &inbox.LastSync,
				setSyncFn:  setGraphLastSync,
			},
		})
		sentTargets = append(sentTargets, gwPollTarget{
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
	pollTargets := func(targets []gwPollTarget) {
		for _, target := range targets {
			n, err := pollGoogleWorkspaceMailboxFolder(ctx, pool, rt, target.inbox, target.folder)
			if err != nil {
				logger.LogError("[google-workspace-poller] mailbox=%s direction=%s err=%v", target.inbox.MailboxAddress, target.folder.direction, err)
				_, _ = pool.Exec(ctx, `
					UPDATE email_svc.inbox_config
					SET ses_last_error = $2, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, target.inbox.InboxID, "google: "+err.Error())
				continue
			}
			ingested += n
			clearMailboxPollError(ctx, pool, target.inbox.InboxID)
		}
	}
	pollTargets(receivedTargets)
	pollTargets(sentTargets)
	if ingested > 0 {
		logger.LogInfo("[google-workspace-poller] ingested %d message(s)", ingested)
	}
	return nil
}

func loadGoogleWorkspaceInboxes(ctx context.Context, pool *pgxpool.Pool) ([]googleWorkspaceInboxRow, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT inbox_id::text, mailbox_address, filters_json,
		       COALESCE(module,''), COALESCE(entity_id,''),
		       %s, %s, %s, %s, %s,
		       graph_last_sync_at, graph_sent_last_sync_at
		FROM email_svc.inbox_config
		WHERE is_active = true
		  AND processing_status = 'APPROVED'
		  AND is_deleted = false
		  AND COALESCE(source_type,'') = 'GOOGLE_WORKSPACE'
	`, SQLGoogleWorkspaceTenantKey, SQLGoogleWorkspaceTenantLabel,
		SQLGoogleWorkspaceServiceAccountEmail, SQLGoogleWorkspaceClientID, SQLGoogleWorkspacePrivateKey))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []googleWorkspaceInboxRow
	for rows.Next() {
		var r googleWorkspaceInboxRow
		if err := rows.Scan(&r.InboxID, &r.MailboxAddress, &r.FiltersJSON, &r.Module, &r.EntityID,
			&r.GoogleTenantKey, &r.Google.TenantLabel, &r.Google.ServiceAccountEmail, &r.Google.ClientID, &r.Google.PrivateKey,
			&r.LastSync, &r.SentLastSync); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func pollGoogleWorkspaceMailboxFolder(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime, inbox googleWorkspaceInboxRow, folder graphFolderPoll) (int, error) {
	conn, err := ResolveGoogleWorkspaceConnection(ctx, pool, GoogleWorkspaceMailboxCreds{
		TenantKey:           inbox.GoogleTenantKey,
		TenantLabel:         inbox.Google.TenantLabel,
		ServiceAccountEmail: inbox.Google.ServiceAccountEmail,
		ClientID:            inbox.Google.ClientID,
		PrivateKey:          inbox.Google.PrivateKey,
	})
	if err != nil {
		return 0, fmt.Errorf("resolve google workspace creds: %w", err)
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
		resp, err := rt.PullGmailDWDMessages(ctx, inbox.InboxID, inbox.MailboxAddress, folder.sentFolder, sinceStr, googleWorkspacePullPageSize, conn, skipIDs)
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
			logger.LogInfo("[google-workspace-poller] mailbox=%s direction=%s initialized cursor (new mail only from %s)",
				inbox.MailboxAddress, folder.direction, formatPollSince(t))
			return 0, nil
		}

		if resp.Fetched == 0 {
			break
		}

		graphInbox := graphInboxRow{
			InboxID:        inbox.InboxID,
			MailboxAddress: inbox.MailboxAddress,
			FiltersJSON:    inbox.FiltersJSON,
			Module:         inbox.Module,
			EntityID:       inbox.EntityID,
		}
		n, err := ingestGraphPollPage(ctx, pool, graphInbox, folder, resp, "GOOGLE_WORKSPACE")
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

		if resp.Fetched < googleWorkspacePullPageSize {
			break
		}
	}
	return totalIngested, nil
}
