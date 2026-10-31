package emailupload

import (
	"CimplrCorpSaas/api/errtext"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	emailcommon "CimplrCorpSaas/api/email/common"
	apipreval "CimplrCorpSaas/api/middlewares"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	emailjobs "CimplrCorpSaas/internal/jobs/email"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxEmlUploadBytes = 25 << 20 // 25 MB

func respondUploadInternal(w http.ResponseWriter, err error) {
	logger.LogError("[email-upload] %v", err)
	emailcommon.RespondInternal(w, "upload failed")
}

func scheduleAttachmentRules(ctx context.Context, pool *pgxpool.Pool, inboxID, messageID, attachmentID, filename, s3Key string) {
	if inboxID == "" || attachmentID == "" || s3Key == "" {
		return
	}
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				logger.LogError("[email-upload] attachment rules panic: %v", rec)
			}
		}()
		runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		emailjobs.ProcessAttachmentRules(runCtx, pool, inboxID, messageID, attachmentID, filename, s3Key)
	}()
}

func HandleUploadEml(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxEmlUploadBytes+(1<<20))
		if err := r.ParseMultipartForm(maxEmlUploadBytes); err != nil {
			emailcommon.RespondBadRequest(w, "invalid multipart form")
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			emailcommon.RespondBadRequest(w, "file is required")
			return
		}
		defer file.Close()

		ext := strings.ToLower(filepath.Ext(header.Filename))
		if ext != ".eml" && ext != ".txt" {
			emailcommon.RespondBadRequest(w, "only .eml files are accepted")
			return
		}

		body, err := io.ReadAll(io.LimitReader(file, maxEmlUploadBytes+1))
		if err != nil {
			respondUploadInternal(w, err)
			return
		}
		if len(body) > maxEmlUploadBytes {
			emailcommon.RespondBadRequest(w, "file too large (max 25MB)")
			return
		}

		now := time.Now().UTC()
		safeName := filepath.Base(strings.TrimSpace(header.Filename))
		if safeName == "" || safeName == "." {
			safeName = "upload.eml"
		}
		s3Key := fmt.Sprintf("email/inbound/raw/manual/%04d/%02d/%02d/%d-%s",
			now.Year(), now.Month(), now.Day(), now.Unix(), safeName)

		if err := s3storage.PutObjectToS3(r.Context(), s3Key, body, "message/rfc822"); err != nil {
			respondUploadInternal(w, err)
			return
		}
		stored := true
		defer func() {
			if !stored {
				return
			}
			delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if derr := s3storage.DeleteFromS3(delCtx, s3Key); derr != nil {
				logger.LogError("[email-upload] cleanup %s: %v", s3Key, derr)
			}
		}()

		client := mailruntime.NewRuntime()
		if !client.Ready() {
			emailcommon.RespondUnavailable(w, "mail processing unavailable")
			return
		}

		parsed, err := client.DecodeMessages(r.Context(), []string{s3Key})
		if err != nil {
			respondUploadInternal(w, err)
			return
		}
		if len(parsed.Results) == 0 {
			if len(parsed.Errors) > 0 {
				logger.LogError("[email-upload] parser: %s", parsed.Errors[0])
			}
			emailcommon.RespondInternal(w, "parser returned no result")
			return
		}

		msg := parsed.Results[0]
		userID := apipreval.GetUserIDFromContext(r.Context())
		userEmail := api.GetUserEmailFromCtx(r.Context())
		if userEmail == "" {
			userEmail = userID
		}
		_, _, _, entityIDs := emailcommon.RequestIdentity(r, "", "")
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)

		inbox, mailDirection, err := emailcommon.ResolveManualUploadInbox(r.Context(), pool, msg, emailcommon.ManualUploadIdentity{
			UserID: userID, UserEmail: userEmail, EntityIDs: entityIDs, Admin: admin, SkipInboxFilters: true,
		})
		if err != nil {
			emailcommon.RespondBadRequest(w, errtext.Detail(w, err))
			return
		}

		messageID, err := ingestParsedMessage(r.Context(), pool, msg, ingestMessageInfo{
			Status: "MANUAL_UPLOAD", InboxID: inbox.InboxID, Module: inbox.Module, EntityID: inbox.EntityID, MailDirection: mailDirection,
		})
		if err != nil {
			respondUploadInternal(w, err)
			return
		}

		uploadedBy := userID
		if userEmail != "" {
			uploadedBy = userEmail
		}
		emailcommon.LogProcessing(r.Context(), pool, messageID, "UPLOAD_EML", "OK", map[string]interface{}{
			"s3_raw_key":  s3Key,
			"filename":    safeName,
			"uploaded_by": uploadedBy,
			"source":      "manual",
		})

		logger.LogInfo("[email-upload] ingested manual .eml message_id=%s key=%s", messageID, s3Key)
		stored = false
		emailcommon.RespondPayload(w, "upload/eml", map[string]interface{}{
			"message_id": messageID,
			"s3_raw_key": s3Key,
			"subject":    msg.Envelope.Subject,
			"from":       msg.Envelope.From,
		})
	}
}

func HandleAttachmentUpload(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxEmlUploadBytes+(1<<20))
		if err := r.ParseMultipartForm(maxEmlUploadBytes); err != nil {
			emailcommon.RespondBadRequest(w, "invalid multipart form")
			return
		}

		messageID := strings.TrimSpace(r.FormValue("message_id"))
		if messageID == "" {
			emailcommon.RespondBadRequest(w, "message_id is required")
			return
		}
		if strings.TrimSpace(apipreval.GetUserIDFromContext(r.Context())) == "" {
			emailcommon.RespondUnauthorized(w, "user context missing")
			return
		}
		if !emailcommon.RequireMessageAccess(w, r, pool, emailcommon.MessageByIDQuery, messageID) {
			return
		}
		if !api.RequireRecordsInScope(r.Context(), w, pool, `
			SELECT COALESCE(NULLIF(m.entity_id, ''), NULLIF(i.entity_id, ''))
			FROM email_svc.message m
			LEFT JOIN email_svc.inbox_config i ON i.inbox_id = m.inbox_id
			WHERE m.message_id = $1::uuid
		`, messageID) {
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			emailcommon.RespondBadRequest(w, "file is required")
			return
		}
		defer file.Close()

		body, err := io.ReadAll(io.LimitReader(file, maxEmlUploadBytes+1))
		if err != nil {
			respondUploadInternal(w, err)
			return
		}
		if len(body) > maxEmlUploadBytes {
			emailcommon.RespondBadRequest(w, "file too large (max 25MB)")
			return
		}

		filename := filepath.Base(strings.TrimSpace(header.Filename))
		if filename == "" {
			filename = "attachment"
		}
		contentType := header.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		hash := sha256.Sum256(body)
		hashHex := hex.EncodeToString(hash[:])
		now := time.Now().UTC()
		s3Key := fmt.Sprintf("email/inbound/attachments/manual/%04d/%02d/%02d/%s/%s-%s",
			now.Year(), now.Month(), now.Day(), messageID, hashHex, filename)
		var keyTaken bool
		if err := pool.QueryRow(r.Context(), `
			SELECT EXISTS(SELECT 1 FROM email_svc.message_attachment WHERE s3_key = $1)
		`, s3Key).Scan(&keyTaken); err != nil {
			respondUploadInternal(w, err)
			return
		}
		if keyTaken {
			s3Key = fmt.Sprintf("email/inbound/attachments/manual/%04d/%02d/%02d/%s/%s-%d-%s",
				now.Year(), now.Month(), now.Day(), messageID, hashHex, now.UnixNano(), filename)
		}

		if err := s3storage.PutObjectToS3(r.Context(), s3Key, body, contentType); err != nil {
			respondUploadInternal(w, err)
			return
		}
		stored := true
		defer func() {
			if !stored {
				return
			}
			delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if derr := s3storage.DeleteFromS3(delCtx, s3Key); derr != nil {
				logger.LogError("[email-upload] cleanup %s: %v", s3Key, derr)
			}
		}()

		var attachmentID string
		err = pool.QueryRow(r.Context(), `
			INSERT INTO email_svc.message_attachment (
				message_id, filename, content_type, file_size, s3_key, file_hash
			) VALUES ($1::uuid, $2, $3, $4, $5, $6)
			RETURNING attachment_id::text
		`, messageID, filename, contentType, len(body), s3Key, hashHex).Scan(&attachmentID)
		if err != nil {
			respondUploadInternal(w, err)
			return
		}
		stored = false

		var inboxID string
		_ = pool.QueryRow(r.Context(), `
			SELECT COALESCE(inbox_id::text, '') FROM email_svc.message WHERE message_id = $1::uuid
		`, messageID).Scan(&inboxID)

		_, _ = pool.Exec(r.Context(), `
			UPDATE email_svc.message SET has_attachments = true, updated_at = now()
			WHERE message_id = $1::uuid
		`, messageID)

		uploadedBy := apipreval.GetUserIDFromContext(r.Context())
		emailcommon.LogProcessing(r.Context(), pool, messageID, "UPLOAD_ATTACHMENT", "OK", map[string]interface{}{
			"attachment_id": attachmentID,
			"filename":      filename,
			"s3_key":        s3Key,
			"uploaded_by":   uploadedBy,
			"source":        "manual_dms",
		})

		if inboxID != "" {
			scheduleAttachmentRules(r.Context(), pool, inboxID, messageID, attachmentID, filename, s3Key)
		}

		emailcommon.RespondPayload(w, "attachments/upload", map[string]interface{}{
			"attachment_id": attachmentID,
			"s3_key":        s3Key,
		})
	}
}

type ingestMessageInfo struct {
	Status        string
	InboxID       string
	Module        string
	EntityID      string
	MailDirection string
}

func ingestParsedMessage(ctx context.Context, pool *pgxpool.Pool, msg mailruntime.ParsedEmail, info ingestMessageInfo) (string, error) {
	status, inboxID, module, entityID, mailDirection :=
		info.Status, info.InboxID, info.Module, info.EntityID, info.MailDirection
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.message WHERE s3_raw_key = $1)
	`, msg.S3RawKey).Scan(&exists); err != nil {
		return "", err
	}
	if exists {
		var messageID string
		err := tx.QueryRow(ctx, `
			SELECT message_id::text FROM email_svc.message WHERE s3_raw_key = $1
		`, msg.S3RawKey).Scan(&messageID)
		return messageID, err
	}

	preview := emailjobs.BodyPreviewForStorage(msg.Body.TextPlain, msg.Body.TextHTML)

	if status == "" {
		status = "INGESTED"
	}
	if mailDirection == "" {
		mailDirection = "RECEIVED"
	}

	var messageID string
	err = tx.QueryRow(ctx, `
		INSERT INTO email_svc.message (
			inbox_id, s3_raw_key, s3_parsed_key, message_id_header,
			envelope_from, envelope_to, subject, received_at,
			has_attachments, filter_matched, processing_status,
			body_text_preview, mail_direction, module, entity_id, updated_at
		) VALUES (
			NULLIF($1,'')::uuid, $2, $3, $4, $5, $6, $7, NULLIF($8,'')::timestamptz,
			$9, true, $10, $11, $12, NULLIF($13,''), NULLIF($14,''), now()
		)
		RETURNING message_id::text
	`,
		inboxID, msg.S3RawKey, msg.S3ParsedKey, msg.Envelope.MessageIDHeader,
		msg.Envelope.From, msg.Envelope.To, msg.Envelope.Subject, msg.Envelope.Date,
		len(msg.Attachments) > 0, status, preview, mailDirection, module, entityID,
	).Scan(&messageID)
	if err != nil {
		return "", err
	}

	type queuedRule struct {
		attachmentID, filename, s3Key string
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
			return "", err
		}
		queued = append(queued, queuedRule{attachmentID: attachmentID, filename: att.Filename, s3Key: att.S3Key})
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}

	for _, att := range queued {
		scheduleAttachmentRules(ctx, pool, inboxID, messageID, att.attachmentID, att.filename, att.s3Key)
	}
	emailcommon.LogProcessing(ctx, pool, messageID, "INGEST", "OK", map[string]interface{}{
		"s3_raw_key": msg.S3RawKey,
		"source":     status,
	})
	return messageID, nil
}
