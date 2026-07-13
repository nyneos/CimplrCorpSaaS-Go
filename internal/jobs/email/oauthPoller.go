package emailjobs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const oauthPullPageSize = 100

type oauthInboxRow struct {
	InboxID        string
	MailboxAddress string
	FiltersJSON    []byte
	Module         string
	EntityID       string
	Provider       string
	MailTransport  string
	RefreshToken   string
	AccessToken    string
	TokenExpires   *time.Time
	IMAP           imapCred
	InboxLastUID   uint32
	SentLastUID    uint32
	LastSync       *time.Time
	SentLastSync   *time.Time
}

func oauthPollerEnabled() bool {
	return getenvBool("OAUTH_POLLER_ENABLED", true)
}

// StartOAuthPoller polls mailboxes connected via delegated OAuth (Microsoft personal, Gmail).
func StartOAuthPoller(ctx context.Context, pool *pgxpool.Pool) {
	if !oauthPollerEnabled() {
		logger.LogInfo("[oauth-poller] disabled via OAUTH_POLLER_ENABLED=false")
		return
	}
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		logger.LogInfo("[oauth-poller] mail runtime key not set — idle")
		return
	}

	interval := time.Duration(getenvInt("OAUTH_POLL_INTERVAL_SECS", getenvInt("EMAIL_POLL_INTERVAL_SECS", 60))) * time.Second
	logger.LogInfo("[oauth-poller] started (poll=%s inbox+sent)", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		if err := oauthPollOnce(ctx, pool, rt); err != nil {
			logger.LogError("[oauth-poller] tick error: %v", err)
		}
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			logger.LogInfo("[oauth-poller] stopped")
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// TriggerOAuthPoll runs one OAuth poll cycle immediately.
func TriggerOAuthPoll(ctx context.Context, pool *pgxpool.Pool) error {
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return fmt.Errorf("mail processing not configured")
	}
	return oauthPollOnce(ctx, pool, rt)
}

func oauthPollOnce(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime) error {
	if err := RequireEmailService(ctx, pool, rt); err != nil {
		return err
	}
	inboxes, err := loadOAuthInboxes(ctx, pool)
	if err != nil {
		return err
	}
	if len(inboxes) == 0 {
		return nil
	}

	var ingested int
	for _, inbox := range inboxes {
		accessToken, err := ensureOAuthAccessToken(ctx, pool, rt, inbox)
		if err != nil {
			logger.LogError("[oauth-poller] mailbox=%s token refresh: %v", inbox.MailboxAddress, err)
			continue
		}
		inbox.AccessToken = accessToken

		if strings.EqualFold(inbox.MailTransport, "imap") {
			n, err := pollOAuthIMAPMailbox(ctx, pool, rt, inbox)
			if err != nil {
				logger.LogError("[oauth-poller] mailbox=%s imap err=%v", inbox.MailboxAddress, err)
				continue
			}
			ingested += n
			continue
		}

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
			n, err := pollOAuthMailboxFolder(ctx, pool, rt, inbox, folder)
			if err != nil {
				logger.LogError("[oauth-poller] mailbox=%s direction=%s err=%v", inbox.MailboxAddress, folder.direction, err)
				continue
			}
			ingested += n
		}
	}
	if ingested > 0 {
		logger.LogInfo("[oauth-poller] ingested %d message(s)", ingested)
	}
	return nil
}

func loadOAuthInboxes(ctx context.Context, pool *pgxpool.Pool) ([]oauthInboxRow, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT inbox_id::text, mailbox_address, filters_json,
		       COALESCE(module,''), COALESCE(entity_id,''),
		       %s, %s, %s,
		       %s, %s,
		       %s, %s,
		       %s, %s,
		       %s,
		       COALESCE(imap_inbox_last_uid, 0)::bigint,
		       COALESCE(imap_sent_last_uid, 0)::bigint,
		       graph_last_sync_at, graph_sent_last_sync_at,
		       oauth_token_expires_at
		FROM email_svc.inbox_config
		WHERE is_active = true
		  AND processing_status = 'APPROVED'
		  AND is_deleted = false
		  AND COALESCE(source_type,'') = 'OAUTH'
		  AND %s <> ''
		  AND %s <> ''
	`, SQLOAuthProvider, SQLOAuthMailTransport, SQLOAuthRefreshToken,
		SQLIMAPProvider, SQLIMAPHost,
		SQLIMAPPort, SQLIMAPUsername,
		SQLIMAPInboxFolder, SQLIMAPSentFolder,
		SQLIMAPUseTLS,
		SQLOAuthProvider, SQLOAuthRefreshToken))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []oauthInboxRow
	for rows.Next() {
		var r oauthInboxRow
		var inboxUID, sentUID int64
		if err := rows.Scan(&r.InboxID, &r.MailboxAddress, &r.FiltersJSON, &r.Module, &r.EntityID,
			&r.Provider, &r.MailTransport, &r.RefreshToken,
			&r.IMAP.Provider, &r.IMAP.Host, &r.IMAP.Port, &r.IMAP.Username,
			&r.IMAP.InboxFolder, &r.IMAP.SentFolder, &r.IMAP.UseTLS,
			&inboxUID, &sentUID,
			&r.LastSync, &r.SentLastSync, &r.TokenExpires); err != nil {
			return nil, err
		}
		if strings.TrimSpace(r.MailTransport) == "" {
			r.MailTransport = "api"
		}
		r.InboxLastUID = uint32(inboxUID)
		r.SentLastUID = uint32(sentUID)
		out = append(out, r)
	}
	return out, rows.Err()
}

func pollOAuthIMAPMailbox(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime, inbox oauthInboxRow) (int, error) {
	inbox.IMAP.AuthMode = "oauth"
	inbox.IMAP.AccessToken = inbox.AccessToken
	payload := inbox.IMAP.toPayload()
	folders := []imapFolderPoll{
		{
			direction: mailDirectionReceived,
			folder:    strings.TrimSpace(payload.InboxFolder),
			lastUID:   &inbox.InboxLastUID,
			setUIDFn:  setIMAPInboxLastUID,
		},
	}
	if strings.TrimSpace(payload.SentFolder) != "" {
		folders = append(folders, imapFolderPoll{
			direction: mailDirectionSent,
			folder:    strings.TrimSpace(payload.SentFolder),
			lastUID:   &inbox.SentLastUID,
			setUIDFn:  setIMAPSentLastUID,
		})
	}

	imapInbox := imapInboxRow{
		InboxID:        inbox.InboxID,
		MailboxAddress: inbox.MailboxAddress,
		FiltersJSON:    inbox.FiltersJSON,
		Module:         inbox.Module,
		EntityID:       inbox.EntityID,
		IMAP:           inbox.IMAP,
		InboxLastUID:   inbox.InboxLastUID,
		SentLastUID:    inbox.SentLastUID,
	}

	var ingested int
	for _, folder := range folders {
		n, err := pollIMAPFolder(ctx, pool, rt, imapInbox, folder)
		if err != nil {
			return ingested, err
		}
		ingested += n
	}
	return ingested, nil
}

func ensureOAuthAccessToken(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime, inbox oauthInboxRow) (string, error) {
	var accessToken string
	var expiresAt *time.Time
	err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s, oauth_token_expires_at
		FROM email_svc.inbox_config WHERE inbox_id = $1::uuid
	`, SQLOAuthAccessToken), inbox.InboxID).Scan(&accessToken, &expiresAt)
	if err != nil {
		return "", err
	}
	if accessToken != "" && expiresAt != nil && time.Now().Before(expiresAt.Add(-2*time.Minute)) {
		return accessToken, nil
	}

	refreshed, err := rt.OAuthRefresh(ctx, inbox.Provider, inbox.MailTransport, inbox.RefreshToken)
	if err != nil {
		return "", err
	}
	newRefresh := refreshed.RefreshToken
	if newRefresh == "" {
		newRefresh = inbox.RefreshToken
	}
	exp := time.Now().UTC().Add(time.Duration(refreshed.ExpiresIn) * time.Second)
	_, err = pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET oauth_access_token = $2,
		    oauth_refresh_token = $3,
		    oauth_token_expires_at = $4,
		    oauth_scopes = COALESCE(NULLIF($5,''), oauth_scopes),
		    updated_at = now()
		WHERE inbox_id = $1::uuid
	`, inbox.InboxID, refreshed.AccessToken, newRefresh, exp, refreshed.Scope)
	if err != nil {
		return "", err
	}
	return refreshed.AccessToken, nil
}

func pollOAuthMailboxFolder(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime, inbox oauthInboxRow, folder graphFolderPoll) (int, error) {
	cursor := folder.lastSync
	sinceStr := ""
	if *cursor != nil {
		sinceStr = (*cursor).UTC().Format(time.RFC3339)
	}

	conn := mailruntime.OAuthConnection{
		Provider:    inbox.Provider,
		AccessToken: inbox.AccessToken,
	}

	var totalIngested int
	for {
		resp, err := rt.PullOAuthMessages(ctx, inbox.InboxID, inbox.MailboxAddress, inbox.Provider, folder.sentFolder, sinceStr, oauthPullPageSize, conn)
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
			logger.LogInfo("[oauth-poller] mailbox=%s direction=%s initialized cursor (new mail only from %s)",
				inbox.MailboxAddress, folder.direction, t.Format(time.RFC3339))
			return 0, nil
		}

		if resp.Fetched == 0 {
			break
		}

		n, err := ingestOAuthPollPage(ctx, pool, inbox, folder, resp)
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

		if resp.Fetched < oauthPullPageSize {
			break
		}
	}
	return totalIngested, nil
}

func ingestOAuthPollPage(ctx context.Context, pool *pgxpool.Pool, inbox oauthInboxRow, folder graphFolderPoll, resp *mailruntime.OAuthPullResult) (int, error) {
	inboxRow := inboxRow{
		InboxID:        inbox.InboxID,
		MailboxAddress: inbox.MailboxAddress,
		FiltersJSON:    inbox.FiltersJSON,
		Module:         inbox.Module,
		EntityID:       inbox.EntityID,
	}

	var ingested int
	for _, om := range resp.Messages {
		if om.ProviderMessageID == "" {
			continue
		}
		exists, err := graphMessageExists(ctx, pool, om.ProviderMessageID)
		if err != nil {
			return ingested, err
		}
		if exists {
			continue
		}

		matchIn := matchInputFromParsed(om.Parsed)
		ok, active := directionFilterMatch(inbox.FiltersJSON, folder.direction, matchIn)
		if !active || !ok {
			continue
		}

		if err := ingestGraphMessage(ctx, pool, inboxRow, om.Parsed, om.ProviderMessageID, folder.direction, "OAUTH"); err != nil {
			logger.LogError("[oauth-poller] ingest id=%s err=%v", om.ProviderMessageID, err)
			continue
		}
		ingested++
	}
	return ingested, nil
}
