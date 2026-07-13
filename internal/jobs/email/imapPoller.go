package emailjobs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/imapmail"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const imapPullPageSize = 100

type imapCred struct {
	Provider    string
	Host        string
	Port        int
	Username    string
	Password    string
	AuthMode    string
	AccessToken string
	InboxFolder string
	SentFolder  string
	UseTLS      bool
}

func (c imapCred) toPayload() mailruntime.IMAPConnection {
	port := c.Port
	if port <= 0 {
		port = 993
	}
	return mailruntime.IMAPConnection{
		Provider:    c.Provider,
		Host:        c.Host,
		Port:        port,
		Username:    c.Username,
		Password:    c.Password,
		AuthMode:    c.AuthMode,
		AccessToken: c.AccessToken,
		UseTLS:      c.UseTLS,
		InboxFolder: c.InboxFolder,
		SentFolder:  c.SentFolder,
	}
}

type imapInboxRow struct {
	InboxID        string
	MailboxAddress string
	FiltersJSON    []byte
	Module         string
	EntityID       string
	IMAP           imapCred
	InboxLastUID   uint32
	SentLastUID    uint32
}

type imapFolderPoll struct {
	direction string
	folder    string
	lastUID   *uint32
	setUIDFn  func(ctx context.Context, pool *pgxpool.Pool, inboxID string, uid uint32) error
}

func imapPollerEnabled() bool {
	return getenvBool("IMAP_POLLER_ENABLED", true)
}

// StartIMAPPoller polls approved IMAP mailboxes (Gmail, Yahoo, generic IMAP).
func StartIMAPPoller(ctx context.Context, pool *pgxpool.Pool) {
	if !imapPollerEnabled() {
		logger.LogInfo("[imap-poller] disabled via IMAP_POLLER_ENABLED=false")
		return
	}
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		logger.LogInfo("[imap-poller] mail runtime key not set — idle")
		return
	}

	interval := time.Duration(getenvInt("IMAP_POLL_INTERVAL_SECS", getenvInt("EMAIL_POLL_INTERVAL_SECS", 60))) * time.Second
	logger.LogInfo("[imap-poller] started (poll=%s inbox+sent)", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		if err := imapPollOnce(ctx, pool, rt); err != nil {
			logger.LogError("[imap-poller] tick error: %v", err)
		}
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			logger.LogInfo("[imap-poller] stopped")
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// TriggerIMAPPoll runs one IMAP poll cycle immediately.
func TriggerIMAPPoll(ctx context.Context, pool *pgxpool.Pool) error {
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return fmt.Errorf("mail processing not configured")
	}
	return imapPollOnce(ctx, pool, rt)
}

func imapPollOnce(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime) error {
	if err := RequireEmailService(ctx, pool, rt); err != nil {
		return err
	}
	inboxes, err := loadIMAPInboxes(ctx, pool)
	if err != nil {
		return err
	}
	if len(inboxes) == 0 {
		logger.LogInfo("[imap-poller] no IMAP mailboxes approved")
		return nil
	}

	type imapPollTarget struct {
		inbox  imapInboxRow
		folder imapFolderPoll
	}

	receivedTargets := make([]imapPollTarget, 0, len(inboxes))
	sentTargets := make([]imapPollTarget, 0, len(inboxes))
	for _, inbox := range inboxes {
		payload := inbox.IMAP.toPayload()
		receivedTargets = append(receivedTargets, imapPollTarget{
			inbox: inbox,
			folder: imapFolderPoll{
				direction: mailDirectionReceived,
				folder:    strings.TrimSpace(payload.InboxFolder),
				lastUID:   &inbox.InboxLastUID,
				setUIDFn:  setIMAPInboxLastUID,
			},
		})
		if strings.TrimSpace(payload.SentFolder) != "" {
			sentTargets = append(sentTargets, imapPollTarget{
				inbox: inbox,
				folder: imapFolderPoll{
					direction: mailDirectionSent,
					folder:    strings.TrimSpace(payload.SentFolder),
					lastUID:   &inbox.SentLastUID,
					setUIDFn:  setIMAPSentLastUID,
				},
			})
		}
	}

	var ingested int
	pollTargets := func(targets []imapPollTarget) {
		for _, target := range targets {
			n, err := pollIMAPFolder(ctx, pool, rt, target.inbox, target.folder)
			if err != nil {
				logger.LogError("[imap-poller] mailbox=%s folder=%s err=%v", target.inbox.MailboxAddress, target.folder.folder, err)
				_, _ = pool.Exec(ctx, `
					UPDATE email_svc.inbox_config
					SET ses_last_error = $2, updated_at = now()
					WHERE inbox_id = $1::uuid
				`, target.inbox.InboxID, "imap: "+err.Error())
				continue
			}
			ingested += n
			_, _ = pool.Exec(ctx, `
				UPDATE email_svc.inbox_config
				SET ses_last_error = NULL, updated_at = now()
				WHERE inbox_id = $1::uuid
			`, target.inbox.InboxID)
		}
	}
	// Poll all inbox (received) folders first so cross-mailbox tests show RECEIVED before SENT.
	pollTargets(receivedTargets)
	pollTargets(sentTargets)
	if ingested > 0 {
		logger.LogInfo("[imap-poller] ingested %d message(s)", ingested)
	}
	return nil
}

func loadIMAPInboxes(ctx context.Context, pool *pgxpool.Pool) ([]imapInboxRow, error) {
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT inbox_id::text, mailbox_address, filters_json,
		       COALESCE(module,''), COALESCE(entity_id,''),
		       %s, %s,
		       %s, %s,
		       %s, %s,
		       %s, %s,
		       COALESCE(imap_inbox_last_uid, 0)::bigint,
		       COALESCE(imap_sent_last_uid, 0)::bigint
		FROM email_svc.inbox_config
		WHERE is_active = true
		  AND processing_status = 'APPROVED'
		  AND is_deleted = false
		  AND COALESCE(source_type,'') = 'IMAP'
	`, SQLIMAPProvider, SQLIMAPHost,
		SQLIMAPPort, SQLIMAPUsername,
		SQLIMAPPassword, SQLIMAPInboxFolder,
		SQLIMAPSentFolder, SQLIMAPUseTLS))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []imapInboxRow
	for rows.Next() {
		var r imapInboxRow
		var inboxUID, sentUID int64
		if err := rows.Scan(&r.InboxID, &r.MailboxAddress, &r.FiltersJSON, &r.Module, &r.EntityID,
			&r.IMAP.Provider, &r.IMAP.Host, &r.IMAP.Port, &r.IMAP.Username, &r.IMAP.Password,
			&r.IMAP.InboxFolder, &r.IMAP.SentFolder, &r.IMAP.UseTLS,
			&inboxUID, &sentUID); err != nil {
			return nil, err
		}
		r.InboxLastUID = uint32(inboxUID)
		r.SentLastUID = uint32(sentUID)
		resolveIMAPRowDefaults(&r)
		persistResolvedIMAPFolders(ctx, pool, &r)
		out = append(out, r)
	}
	return out, rows.Err()
}

func resolveIMAPRowDefaults(r *imapInboxRow) {
	cfg := imapmail.Config{
		Provider:    r.IMAP.Provider,
		Host:        r.IMAP.Host,
		Port:        r.IMAP.Port,
		Username:    r.IMAP.Username,
		InboxFolder: r.IMAP.InboxFolder,
		SentFolder:  r.IMAP.SentFolder,
		UseTLS:      r.IMAP.UseTLS,
	}
	if strings.TrimSpace(cfg.Username) == "" {
		cfg.Username = r.MailboxAddress
	}
	if err := cfg.Resolve(r.MailboxAddress); err != nil {
		return
	}
	if strings.TrimSpace(r.IMAP.Host) == "" {
		r.IMAP.Host = cfg.Host
	}
	if r.IMAP.Port <= 0 {
		r.IMAP.Port = cfg.Port
	}
	if strings.TrimSpace(r.IMAP.InboxFolder) == "" {
		r.IMAP.InboxFolder = cfg.InboxFolder
	}
	if strings.TrimSpace(r.IMAP.SentFolder) == "" {
		r.IMAP.SentFolder = cfg.SentFolder
	}
}

func persistResolvedIMAPFolders(ctx context.Context, pool *pgxpool.Pool, r *imapInboxRow) {
	inboxFolder := strings.TrimSpace(r.IMAP.InboxFolder)
	sentFolder := strings.TrimSpace(r.IMAP.SentFolder)
	if inboxFolder == "" && sentFolder == "" {
		return
	}
	_, _ = pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET imap_inbox_folder = CASE WHEN imap_inbox_folder = '' AND $2 <> '' THEN $2 ELSE imap_inbox_folder END,
		    imap_sent_folder = CASE WHEN imap_sent_folder = '' AND $3 <> '' THEN $3 ELSE imap_sent_folder END,
		    updated_at = now()
		WHERE inbox_id = $1::uuid
	`, r.InboxID, inboxFolder, sentFolder)
}

func pollIMAPFolder(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime,
	inbox imapInboxRow, folder imapFolderPoll) (int, error) {

	var totalIngested int
	lastUID := *folder.lastUID

	for {
		resp, err := rt.PullIMAPMessages(ctx, inbox.InboxID, inbox.MailboxAddress, folder.folder, folder.direction, lastUID, imapPullPageSize, inbox.IMAP.toPayload())
		if err != nil {
			return totalIngested, err
		}

		if resp.Initialized {
			if err := folder.setUIDFn(ctx, pool, inbox.InboxID, resp.NewLastUID); err != nil {
				return totalIngested, err
			}
			lastUID = resp.NewLastUID
			*folder.lastUID = lastUID
			logger.LogInfo("[imap-poller] mailbox=%s folder=%s initialized uid cursor=%d (continuing poll)",
				inbox.MailboxAddress, folder.folder, resp.NewLastUID)
			continue
		}

		if len(resp.Messages) == 0 {
			break
		}

		logger.LogInfo("[imap-poller] mailbox=%s folder=%s found %d message(s) after uid=%d",
			inbox.MailboxAddress, folder.folder, len(resp.Messages), lastUID)

		inboxRow := inboxRow{
			InboxID:        inbox.InboxID,
			MailboxAddress: inbox.MailboxAddress,
			FiltersJSON:    inbox.FiltersJSON,
			Module:         inbox.Module,
			EntityID:       inbox.EntityID,
		}

		for _, im := range resp.Messages {
			exists, err := imapMessageExists(ctx, pool, im.IMAPMessageKey)
			if err != nil {
				return totalIngested, err
			}
			if exists {
				continue
			}

			matchIn := matchInputFromParsed(im.Parsed)
			ok, active := directionFilterMatch(inbox.FiltersJSON, folder.direction, matchIn)
			if !active || !ok {
				logger.LogInfo("[imap-poller] skip uid=%d folder=%s (inbox filter)", im.UID, folder.folder)
				continue
			}

			if err := ingestIMAPMessage(ctx, pool, inboxRow, im.Parsed, im.IMAPMessageKey, folder.direction); err != nil {
				logger.LogError("[imap-poller] ingest uid=%d err=%v", im.UID, err)
				continue
			}
			totalIngested++
		}

		if resp.NewLastUID > lastUID {
			if err := folder.setUIDFn(ctx, pool, inbox.InboxID, resp.NewLastUID); err != nil {
				return totalIngested, err
			}
			lastUID = resp.NewLastUID
			*folder.lastUID = lastUID
		}

		if len(resp.Messages) < imapPullPageSize {
			break
		}
	}
	return totalIngested, nil
}

func imapMessageExists(ctx context.Context, pool *pgxpool.Pool, imapKey string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.message WHERE imap_message_key = $1)
	`, imapKey).Scan(&exists)
	return exists, err
}

func setIMAPInboxLastUID(ctx context.Context, pool *pgxpool.Pool, inboxID string, uid uint32) error {
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET imap_inbox_last_uid = $2, graph_last_sync_at = now(), updated_at = now()
		WHERE inbox_id = $1::uuid
	`, inboxID, int64(uid))
	return err
}

func setIMAPSentLastUID(ctx context.Context, pool *pgxpool.Pool, inboxID string, uid uint32) error {
	_, err := pool.Exec(ctx, `
		UPDATE email_svc.inbox_config
		SET imap_sent_last_uid = $2, graph_sent_last_sync_at = now(), updated_at = now()
		WHERE inbox_id = $1::uuid
	`, inboxID, int64(uid))
	return err
}

func ingestIMAPMessage(ctx context.Context, pool *pgxpool.Pool, inbox inboxRow, msg mailruntime.ParsedEmail, imapKey, mailDirection string) error {
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.message WHERE s3_raw_key = $1 OR imap_message_key = $2)
	`, msg.S3RawKey, imapKey).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}

	matchInput := matchInputFromParsed(msg)
	filterMatched, active := directionFilterMatch(inbox.FiltersJSON, mailDirection, matchInput)
	if !active || !filterMatched {
		return nil
	}

	preview := BodyPreviewForStorage(msg.Body.TextPlain, msg.Body.TextHTML)

	toAddrs := msg.Envelope.To
	if len(toAddrs) == 0 && !strings.EqualFold(mailDirection, mailDirectionSent) {
		toAddrs = []string{inbox.MailboxAddress}
	}
	if mailDirection == "" {
		mailDirection = mailDirectionReceived
	}

	var messageID string
	err := pool.QueryRow(ctx, `
		INSERT INTO email_svc.message (
			inbox_id, s3_raw_key, s3_parsed_key, message_id_header, imap_message_key,
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
		inbox.InboxID, msg.S3RawKey, msg.S3ParsedKey, msg.Envelope.MessageIDHeader, imapKey,
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
		logAttachmentIngest(ctx, pool, messageID, attachmentID, "IMAP", att.Filename, att.ContentType, att.S3Key, att.SizeBytes)
	}

	detail, _ := json.Marshal(map[string]string{
		"source":           "IMAP",
		"imap_message_key": imapKey,
		"s3_raw_key":       msg.S3RawKey,
		"mailbox":          inbox.MailboxAddress,
		"mail_direction":   mailDirection,
	})
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.processing_log (message_id, step, status, detail)
		VALUES ($1::uuid, 'IMAP_INGEST', 'OK', $2::jsonb)
	`, messageID, string(detail))

	return nil
}
