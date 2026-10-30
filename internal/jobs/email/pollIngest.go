package emailjobs

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"CimplrCorpSaas/internal/logger"
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
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var exists bool
	if err := tx.QueryRow(ctx, `
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
	err = tx.QueryRow(ctx, `
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

	type queuedRule struct {
		attachmentID string
		filename     string
		contentType  string
		s3Key        string
		fileSize     int64
	}
	queued := make([]queuedRule, 0, len(msg.Attachments))
	for _, att := range msg.Attachments {
		var attachmentID string
		err := tx.QueryRow(ctx, `
			INSERT INTO email_svc.message_attachment (
				message_id, filename, content_type, file_size, s3_key, file_hash
			) VALUES ($1::uuid, $2, $3, $4, $5, $6)
			RETURNING attachment_id::text
		`, messageID, att.Filename, att.ContentType, att.SizeBytes, att.S3Key, att.SHA256).Scan(&attachmentID)
		if err != nil {
			return err
		}
		queued = append(queued, queuedRule{
			attachmentID: attachmentID,
			filename:     att.Filename,
			contentType:  att.ContentType,
			s3Key:        att.S3Key,
			fileSize:     att.SizeBytes,
		})
	}

	detail, err := json.Marshal(map[string]string{
		"source":           sourceType,
		"graph_message_id": graphID,
		"imap_message_key": imapKey,
		"s3_raw_key":       msg.S3RawKey,
		"s3_parsed_key":    msg.S3ParsedKey,
		"mailbox":          inbox.MailboxAddress,
		"mail_direction":   mailDirection,
	})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO email_svc.processing_log (message_id, step, status, detail)
		VALUES ($1::uuid, $2, 'OK', $3::jsonb)
	`, messageID, logStep, string(detail)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	for _, att := range queued {
		logAttachmentIngest(ctx, pool, attachmentIngestInfo{
			MessageID: messageID, AttachmentID: att.attachmentID, Source: sourceType,
			Filename: att.filename, ContentType: att.contentType, S3Key: att.s3Key, FileSize: att.fileSize,
		})
		inboxID, attachmentID, filename, s3Key := inbox.InboxID, att.attachmentID, att.filename, att.s3Key
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					logger.LogError("[email-poll] attachment rules panic: %v", rec)
				}
			}()
			runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
			defer cancel()
			ProcessAttachmentRules(runCtx, pool, inboxID, messageID, attachmentID, filename, s3Key)
		}()
	}
	return nil
}
