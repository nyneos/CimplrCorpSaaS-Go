package emailupload

import (
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
	apipreval "CimplrCorpSaas/api/middlewares"
	emailcommon "CimplrCorpSaas/api/email/common"
	s3storage "CimplrCorpSaas/api/utils/s3storage"
	emailjobs "CimplrCorpSaas/internal/jobs/email"
	"CimplrCorpSaas/internal/logger"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxEmlUploadBytes = 25 << 20 // 25 MB

func HandleUploadEml(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
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
			emailcommon.RespondInternal(w, err.Error())
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
			emailcommon.RespondInternal(w, "s3 upload failed: "+err.Error())
			return
		}

		client := mailruntime.NewRuntime()
		if !client.Ready() {
			emailcommon.RespondUnavailable(w, "mail processing unavailable")
			return
		}

		parsed, err := client.DecodeMessages(r.Context(), []string{s3Key})
		if err != nil {
			emailcommon.RespondInternal(w, "parse failed: "+err.Error())
			return
		}
		if len(parsed.Results) == 0 {
			msg := "parser returned no result"
			if len(parsed.Errors) > 0 {
				msg = parsed.Errors[0]
			}
			emailcommon.RespondInternal(w, msg)
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

		inbox, mailDirection, err := emailcommon.ResolveManualUploadInbox(r.Context(), pool, msg, userID, userEmail, entityIDs, admin, true)
		if err != nil {
			emailcommon.RespondBadRequest(w, err.Error())
			return
		}

		messageID, err := ingestParsedMessage(r.Context(), pool, msg, "MANUAL_UPLOAD", inbox.InboxID, inbox.Module, inbox.EntityID, mailDirection)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}

		uploadedBy := userID
		if userEmail != "" {
			uploadedBy = userEmail
		}
		emailcommon.LogProcessing(r.Context(), pool, messageID, "UPLOAD_EML", "OK", map[string]interface{}{
			"s3_raw_key":    s3Key,
			"filename":      safeName,
			"uploaded_by":   uploadedBy,
			"source":        "manual",
		})

		logger.LogInfo("[email-upload] ingested manual .eml message_id=%s key=%s", messageID, s3Key)
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
		if err := r.ParseMultipartForm(maxEmlUploadBytes); err != nil {
			emailcommon.RespondBadRequest(w, "invalid multipart form")
			return
		}

		messageID := strings.TrimSpace(r.FormValue("message_id"))
		if messageID == "" {
			emailcommon.RespondBadRequest(w, "message_id is required")
			return
		}

		var exists bool
		if err := pool.QueryRow(r.Context(), `
			SELECT EXISTS(SELECT 1 FROM email_svc.message WHERE message_id = $1::uuid)
		`, messageID).Scan(&exists); err != nil || !exists {
			emailcommon.RespondNotFound(w, "message not found")
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
			emailcommon.RespondInternal(w, err.Error())
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
		s3Key := fmt.Sprintf("email/inbound/attachments/manual/%04d/%02d/%02d/%s/%s",
			now.Year(), now.Month(), now.Day(), messageID, filename)

		if err := s3storage.PutObjectToS3(r.Context(), s3Key, body, contentType); err != nil {
			emailcommon.RespondInternal(w, "s3 upload failed: "+err.Error())
			return
		}

		var attachmentID string
		err = pool.QueryRow(r.Context(), `
			INSERT INTO email_svc.message_attachment (
				message_id, filename, content_type, file_size, s3_key, file_hash
			) VALUES ($1::uuid, $2, $3, $4, $5, $6)
			RETURNING attachment_id::text
		`, messageID, filename, contentType, len(body), s3Key, hashHex).Scan(&attachmentID)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}

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

		emailcommon.RespondPayload(w, "attachments/upload", map[string]interface{}{
			"attachment_id": attachmentID,
			"s3_key":        s3Key,
		})
	}
}

func ingestParsedMessage(ctx context.Context, pool *pgxpool.Pool, msg mailruntime.ParsedEmail, status, inboxID, module, entityID, mailDirection string) (string, error) {
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.message WHERE s3_raw_key = $1)
	`, msg.S3RawKey).Scan(&exists); err != nil {
		return "", err
	}
	if exists {
		var messageID string
		err := pool.QueryRow(ctx, `
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
	err := pool.QueryRow(ctx, `
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

	for _, att := range msg.Attachments {
		_, err := pool.Exec(ctx, `
			INSERT INTO email_svc.message_attachment (
				message_id, filename, content_type, file_size, s3_key, file_hash
			) VALUES ($1::uuid, $2, $3, $4, $5, $6)
		`, messageID, att.Filename, att.ContentType, att.SizeBytes, att.S3Key, att.SHA256)
		if err != nil {
			return "", err
		}
	}

	emailcommon.LogProcessing(ctx, pool, messageID, "INGEST", "OK", map[string]interface{}{
		"s3_raw_key": msg.S3RawKey,
		"source":     status,
	})
	return messageID, nil
}
