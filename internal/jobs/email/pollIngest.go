package emailjobs

import (
	"context"
	"encoding/json"
	"strings"

	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pollIngestIdentity carries provider-specific dedupe keys for one polled message.
type pollIngestIdentity struct {
	GraphMessageID string
	IMAPMessageKey string
	SourceType     string
	LogStep        string
}

func (id pollIngestIdentity) dedupeArgs(s3RawKey string) (string, string, string) {
	return s3RawKey, strings.TrimSpace(id.GraphMessageID), strings.TrimSpace(id.IMAPMessageKey)
}

// ingestPollMessage persists one parsed mail row, attachments, and processing_log audit.
// Graph / OAuth / Gmail DWD / IMAP pollers share this path (same S3 bucket + audit shape).
func ingestPollMessage(ctx context.Context, pool *pgxpool.Pool, inbox inboxRow, msg mailruntime.ParsedMessage, id pollIngestIdentity, mailDirection string) error {
	s3Raw, graphID, imapKey := id.dedupeArgs(msg.S3RawKey)
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM email_svc.message
			WHERE s3_raw_key = $1
			   OR ($2 <> '' AND graph_message_id = $2)
			   OR ($3 <> '' AND imap_message_key = $3)
		)
	`, s3Raw, graphID, imapKey).Scan(&exists); err != nil {
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
	sourceType := id.SourceType
	if sourceType == "" {
		sourceType = "OUTLOOK_GRAPH"
	}
	logStep := id.LogStep
	if logStep == "" {
		logStep = "MAIL_INGEST"
	}

	var messageID string
	err := pool.QueryRow(ctx, `
		INSERT INTO email_svc.message (
			inbox_id, s3_raw_key, s3_parsed_key, message_id_header, graph_message_id, imap_message_key,
			envelope_from, envelope_to, subject, received_at,
			has_attachments, filter_matched, processing_status,
			module, entity_id, body_text_preview, mail_direction, updated_at
		) VALUES (
			$1::uuid, $2, $3, $4,
			NULLIF($5,''), NULLIF($6,''),
			$7, $8, $9, NULLIF($10,'')::timestamptz,
			$11, $12, 'INGESTED',
			NULLIF($13,''), NULLIF($14,''), $15, $16, now()
		)
		RETURNING message_id::text
	`,
		inbox.InboxID, msg.S3RawKey, msg.S3ParsedKey, msg.Envelope.MessageIDHeader, graphID, imapKey,
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
		logAttachmentIngest(ctx, pool, attachmentIngestInfo{
			MessageID: messageID, AttachmentID: attachmentID, Source: sourceType,
			Filename: att.Filename, ContentType: att.ContentType, S3Key: att.S3Key, FileSize: att.SizeBytes,
		})
		go ProcessAttachmentRules(context.Background(), pool, inbox.InboxID, messageID, attachmentID, att.Filename, att.S3Key)
	}

	detail, _ := json.Marshal(map[string]string{
		"source":           sourceType,
		"graph_message_id": graphID,
		"imap_message_key": imapKey,
		"s3_raw_key":       msg.S3RawKey,
		"s3_parsed_key":    msg.S3ParsedKey,
		"mailbox":          inbox.MailboxAddress,
		"mail_direction":   mailDirection,
	})
	logMessageIngest(ctx, pool, messageID, logStep, detail)

	return nil
}
