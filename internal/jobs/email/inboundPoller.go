package emailjobs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

type inboxRow struct {
	InboxID        string
	MailboxAddress string
	FiltersJSON    []byte
	Module         string
	EntityID       string
}

type filters struct {
	Senders         []string `json:"senders"`
	Recipients      []string `json:"recipients"`
	Domains         []string `json:"domains"`
	Subjects        []string `json:"subjects"`
	ExcludeSenders  []string `json:"exclude_senders"`
	HasAttachments  *bool    `json:"has_attachments"`
	AttachmentTypes []string `json:"attachment_types"`
}

func getenvInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 {
		return def
	}
	return n
}

func getenvBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return def
	}
	return v == "1" || v == "true" || v == "yes"
}

// StartInboundPoller polls S3 for new raw .eml files and ingests parsed messages.
func StartInboundPoller(ctx context.Context, pool *pgxpool.Pool) {
	if !getenvBool("EMAIL_POLLER_ENABLED", true) {
		logger.LogInfo("[email-poller] disabled via EMAIL_POLLER_ENABLED=false")
		return
	}

	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		logger.LogInfo("[email-poller] mail runtime key not set — poller idle")
		return
	}

	interval := time.Duration(getenvInt("EMAIL_POLL_INTERVAL_SECS", 60)) * time.Second
	logger.LogInfo("[email-poller] started (poll=%s)", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	runOnce := func() {
		if err := pollOnce(ctx, pool, rt); err != nil {
			logger.LogError("[email-poller] tick error: %v", err)
		}
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			logger.LogInfo("[email-poller] stopped")
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// TriggerInboundPoll runs one S3 poll cycle immediately (manual sync from UI).
func TriggerInboundPoll(ctx context.Context, pool *pgxpool.Pool) error {
	rt := mailruntime.NewRuntime()
	if !rt.Ready() {
		return fmt.Errorf("mail processing not configured")
	}
	return pollOnce(ctx, pool, rt)
}

func pollOnce(ctx context.Context, pool *pgxpool.Pool, rt *mailruntime.Runtime) error {
	if err := RequireEmailService(ctx, pool, rt); err != nil {
		return err
	}
	lastKey, err := loadCursor(ctx, pool)
	if err != nil {
		return err
	}

	inboxes, err := loadActiveInboxes(ctx, pool)
	if err != nil {
		return err
	}

	for {
		keys, err := rt.ListPendingKeys(ctx, lastKey, 0)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			logger.LogInfo("[email-poller] no new S3 keys after cursor=%q", lastKey)
			break
		}
		logger.LogInfo("[email-poller] found %d new S3 key(s) after cursor=%q", len(keys), lastKey)

		newestKey := lastKey
		filtered := make([]string, 0, len(keys))
		for _, k := range keys {
			if k > newestKey {
				newestKey = k
			}
			// Graph and IMAP pollers upload under dedicated prefixes — skip here to avoid duplicate ingest.
			if strings.Contains(k, "/graph/") || strings.Contains(k, "/imap/") {
				continue
			}
			filtered = append(filtered, k)
		}

		if len(filtered) > 0 {
			parsed, err := rt.DecodeMessages(ctx, filtered)
			if err != nil {
				return err
			}
			for _, msg := range parsed.Results {
				if msg.S3RawKey > newestKey {
					newestKey = msg.S3RawKey
				}
				if err := ingestMessage(ctx, pool, inboxes, msg); err != nil {
					logger.LogError("[email-poller] ingest %s: %v", msg.S3RawKey, err)
				}
			}
		}

		lastKey = newestKey
		if err := touchCursor(ctx, pool, lastKey); err != nil {
			return err
		}
	}

	return touchCursor(ctx, pool, lastKey)
}

func loadCursor(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	var lastKey *string
	err := pool.QueryRow(ctx, `
		SELECT last_s3_key
		FROM email_svc.poll_cursor
		WHERE scope = 'global'
	`).Scan(&lastKey)
	if err != nil {
		_, insErr := pool.Exec(ctx, `
			INSERT INTO email_svc.poll_cursor (scope, last_polled_at)
			VALUES ('global', now())
			ON CONFLICT (scope) DO NOTHING
		`)
		if insErr != nil {
			return "", insErr
		}
		return "", nil
	}
	if lastKey == nil {
		return "", nil
	}
	return *lastKey, nil
}

func touchCursor(ctx context.Context, pool *pgxpool.Pool, lastKey string) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO email_svc.poll_cursor (scope, last_s3_key, last_polled_at)
		VALUES ('global', NULLIF($1,''), now())
		ON CONFLICT (scope) DO UPDATE
		SET last_s3_key = EXCLUDED.last_s3_key,
		    last_polled_at = now()
	`, lastKey)
	return err
}

func loadActiveInboxes(ctx context.Context, pool *pgxpool.Pool) ([]inboxRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT inbox_id::text, mailbox_address, filters_json, COALESCE(module,''), COALESCE(entity_id,'')
		FROM email_svc.inbox_config
		WHERE is_active = true
		  AND processing_status = 'APPROVED'
		  AND is_deleted = false
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []inboxRow
	for rows.Next() {
		var r inboxRow
		if err := rows.Scan(&r.InboxID, &r.MailboxAddress, &r.FiltersJSON, &r.Module, &r.EntityID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func ingestMessage(ctx context.Context, pool *pgxpool.Pool, inboxes []inboxRow, msg mailruntime.ParsedEmail) error {
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.message WHERE s3_raw_key = $1)
	`, msg.S3RawKey).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}

	matchInput := matchInputFromParsed(msg)
	matchedInboxID := ""
	matchedModule := ""
	matchedEntity := ""
	filterMatched := false

	for _, inbox := range inboxes {
		mf := parseMailboxFilters(inbox.FiltersJSON)
		if !matchInboundRules(mf.Inbound, matchInput) || !mailboxMatches(inbox.MailboxAddress, msg.Envelope.To) {
			continue
		}
		matchedInboxID = inbox.InboxID
		matchedModule = inbox.Module
		matchedEntity = inbox.EntityID
		filterMatched = filterRulesActive(mf.Inbound)
		break
	}

	if matchedInboxID == "" {
		logger.LogInfo("[email-poller] skip key=%s (no approved inbox matched filters)", msg.S3RawKey)
		return nil
	}

	preview := BodyPreviewForStorage(msg.Body.TextPlain, msg.Body.TextHTML)

	var messageID string
	err := pool.QueryRow(ctx, `
		INSERT INTO email_svc.message (
			inbox_id, s3_raw_key, s3_parsed_key, message_id_header,
			envelope_from, envelope_to, subject, received_at,
			has_attachments, filter_matched, processing_status,
			module, entity_id, body_text_preview, mail_direction, updated_at
		) VALUES (
			NULLIF($1::text,'')::uuid, $2, $3, $4,
			$5, $6, $7, NULLIF($8,'')::timestamptz,
			$9, $10, 'INGESTED',
			NULLIF($11,''), NULLIF($12,''), $13, 'RECEIVED', now()
		)
		RETURNING message_id::text
	`,
		matchedInboxID, msg.S3RawKey, msg.S3ParsedKey, msg.Envelope.MessageIDHeader,
		msg.Envelope.From, msg.Envelope.To, msg.Envelope.Subject, msg.Envelope.Date,
		len(msg.Attachments) > 0, filterMatched,
		matchedModule, matchedEntity, preview,
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
		logAttachmentIngest(ctx, pool, attachmentIngestInfo{
			MessageID: messageID, AttachmentID: attachmentID, Source: "SES",
			Filename: att.Filename, ContentType: att.ContentType, S3Key: att.S3Key, FileSize: att.SizeBytes,
		})
	}

	ingestDetail, _ := json.Marshal(map[string]string{
		"s3_raw_key": msg.S3RawKey,
		"source":     "SES",
	})
	_, _ = pool.Exec(ctx, `
		INSERT INTO email_svc.processing_log (message_id, step, status, detail)
		VALUES ($1::uuid, 'INGEST', 'OK', $2::jsonb)
	`, messageID, string(ingestDetail))
	return nil
}

type matchInput struct {
	From            string
	To              []string
	Subject         string
	HasAttachments  bool
	AttachmentNames []string
}

func matchInputFromParsed(msg mailruntime.ParsedEmail) matchInput {
	names := make([]string, 0, len(msg.Attachments))
	for _, a := range msg.Attachments {
		names = append(names, a.Filename)
	}
	return matchInput{
		From:            msg.Envelope.From,
		To:              msg.Envelope.To,
		Subject:         msg.Envelope.Subject,
		HasAttachments:  len(msg.Attachments) > 0,
		AttachmentNames: names,
	}
}

func mailboxMatches(mailbox string, to []string) bool {
	mailbox = strings.ToLower(strings.TrimSpace(mailbox))
	if mailbox == "" {
		return true
	}
	for _, addr := range to {
		if strings.ToLower(strings.TrimSpace(addr)) == mailbox {
			return true
		}
	}
	return false
}

func filtersActive(f filters) bool {
	return filterRulesActive(filterRules{
		Senders: f.Senders, Recipients: f.Recipients, Domains: f.Domains,
		Subjects: f.Subjects, ExcludeSenders: f.ExcludeSenders,
		HasAttachments: f.HasAttachments, AttachmentTypes: f.AttachmentTypes,
	})
}

// matchFilters applies inbound (received) rules — From field only for sender/domain patterns.
func matchFilters(f filters, in matchInput) bool {
	return matchInboundRules(filterRules{
		Senders: f.Senders, Recipients: f.Recipients, Domains: f.Domains,
		Subjects: f.Subjects, ExcludeSenders: f.ExcludeSenders,
		HasAttachments: f.HasAttachments, AttachmentTypes: f.AttachmentTypes,
	}, in)
}

// matchSentFilters applies outbound (sent) rules — To field for recipient/domain patterns.
func matchSentFilters(f filters, in matchInput) bool {
	return matchOutboundRules(filterRules{
		Senders: f.Senders, Recipients: f.Recipients, Domains: f.Domains,
		Subjects: f.Subjects, ExcludeSenders: f.ExcludeSenders,
		HasAttachments: f.HasAttachments, AttachmentTypes: f.AttachmentTypes,
	}, in)
}

func anyGlob(patterns []string, value string) bool {
	for _, p := range patterns {
		if globMatch(strings.ToLower(strings.TrimSpace(p)), strings.ToLower(value)) {
			return true
		}
	}
	return false
}

func globMatch(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	ok, _ := path.Match(pattern, value)
	return ok
}

func extractDomain(email string) string {
	at := strings.LastIndex(email, "@")
	if at < 0 {
		return email
	}
	return strings.ToLower(email[at+1:])
}

func attachmentTypeMatch(types []string, names []string) bool {
	for _, name := range names {
		ext := strings.TrimPrefix(strings.ToLower(path.Ext(name)), ".")
		for _, t := range types {
			t = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(t)), ".")
			if t == ext {
				return true
			}
		}
	}
	return false
}
