package emailmessages

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	emailcommon "CimplrCorpSaas/api/email/common"
	"CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

func HandleMessageList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}

		var req struct {
			Module   string `json:"module"`
			EntityID string `json:"entity_id"`
			Status   string `json:"status"`
			DateFrom string `json:"date_from"`
			DateTo   string `json:"date_to"`
			Limit    int    `json:"limit"`
			Offset   int    `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "invalid body")
			return
		}

		module := strings.TrimSpace(req.Module)
		entityID := strings.TrimSpace(req.EntityID)
		status := strings.TrimSpace(req.Status)
		dateFrom := strings.TrimSpace(req.DateFrom)
		dateTo := strings.TrimSpace(req.DateTo)
		userID, userEmail, _, entityIDs := emailcommon.RequestIdentity(r, "", "")
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)
		offset := req.Offset
		if offset < 0 {
			offset = 0
		}

		query := `
			SELECT m.message_id::text, COALESCE(m.inbox_id::text,''), m.s3_raw_key, COALESCE(m.s3_parsed_key,''),
			       COALESCE(m.envelope_from,''), COALESCE(m.subject,''), m.received_at,
			       m.has_attachments, m.filter_matched, m.processing_status,
			       COALESCE(m.module,''), COALESCE(m.entity_id,''), COALESCE(m.body_text_preview,''),
			       m.created_at,
			       COALESCE(upl.uploaded_by, ''),
			       COALESCE(att.attachment_count, 0),
			       COALESCE(att.parsed_count, 0),
			       COALESCE(att.additional_count, 0),
			       COALESCE(m.mail_direction, 'RECEIVED'),
			       COALESCE(m.envelope_to[1], '')
			FROM email_svc.message m
			LEFT JOIN LATERAL (
				SELECT COALESCE(pl.detail->>'uploaded_by', '') AS uploaded_by
				FROM email_svc.processing_log pl
				WHERE pl.message_id = m.message_id
				  AND pl.step IN ('UPLOAD_EML', 'UPLOAD_ATTACHMENT', 'INGEST')
				  AND pl.detail ? 'uploaded_by'
				ORDER BY pl.created_at DESC
				LIMIT 1
			) upl ON true
			LEFT JOIN LATERAL (
				SELECT COUNT(*)::int AS attachment_count,
				       COUNT(*) FILTER (WHERE ma.s3_key NOT LIKE '%/attachments/manual/%')::int AS parsed_count,
				       COUNT(*) FILTER (WHERE ma.s3_key LIKE '%/attachments/manual/%')::int AS additional_count
				FROM email_svc.message_attachment ma
				WHERE ma.message_id = m.message_id
			) att ON true
			WHERE ($1 = '' OR m.module = $1)
			  AND ($2 = '' OR m.entity_id = $2)
			  AND ($3 = '' OR m.processing_status = $3)
			  AND ($4 = '' OR COALESCE(m.received_at, m.created_at) >= $4::date)
			  AND ($5 = '' OR COALESCE(m.received_at, m.created_at) < ($5::date + interval '1 day'))
			` + emailcommon.ListMessageScopedToUserSQL + `
			ORDER BY CASE WHEN m.processing_status = 'MANUAL_UPLOAD' THEN m.created_at ELSE COALESCE(m.received_at, m.created_at) END DESC`

		args := []interface{}{module, entityID, status, dateFrom, dateTo, admin, userID, entityIDs, userEmail}
		if req.Limit > 0 {
			query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
			args = append(args, req.Limit, offset)
		}

		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		defer rows.Close()

		type row struct {
			MessageID             string     `json:"message_id"`
			InboxID               string     `json:"inbox_id"`
			S3RawKey              string     `json:"s3_raw_key"`
			S3ParsedKey           string     `json:"s3_parsed_key"`
			EnvelopeFrom          string     `json:"envelope_from"`
			Subject               string     `json:"subject"`
			ReceivedAt            *time.Time `json:"received_at"`
			HasAttachments        bool       `json:"has_attachments"`
			FilterMatched         bool       `json:"filter_matched"`
			ProcessingStatus      string     `json:"processing_status"`
			Module                string     `json:"module"`
			EntityID              string     `json:"entity_id"`
			BodyTextPreview       string     `json:"body_text_preview"`
			CreatedAt             time.Time  `json:"created_at"`
			UploadedBy            string     `json:"uploaded_by"`
			AttachmentCount       int        `json:"attachment_count"`
			ParsedAttachmentCount int        `json:"parsed_attachment_count"`
			AdditionalFileCount   int        `json:"additional_file_count"`
			MailDirection         string     `json:"mail_direction"`
			EnvelopeTo            string     `json:"envelope_to"`
		}

		items := make([]row, 0)
		for rows.Next() {
			var item row
			if err := rows.Scan(&item.MessageID, &item.InboxID, &item.S3RawKey, &item.S3ParsedKey,
				&item.EnvelopeFrom, &item.Subject, &item.ReceivedAt, &item.HasAttachments,
				&item.FilterMatched, &item.ProcessingStatus, &item.Module, &item.EntityID,
				&item.BodyTextPreview, &item.CreatedAt, &item.UploadedBy,
				&item.AttachmentCount, &item.ParsedAttachmentCount, &item.AdditionalFileCount,
				&item.MailDirection, &item.EnvelopeTo); err != nil {
				emailcommon.RespondInternal(w, err.Error())
				return
			}
			items = append(items, item)
		}
		emailcommon.RespondList(w, "messages/list", items, len(items))
	}
}

func attachmentSource(s3Key string) string {
	if strings.Contains(s3Key, "/attachments/manual/") {
		return "MANUAL"
	}
	return "PARSED"
}

func HandleMessageGet(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			MessageID string `json:"message_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "invalid body")
			return
		}
		messageID := strings.TrimSpace(req.MessageID)
		if messageID == "" {
			emailcommon.RespondBadRequest(w, "message_id is required")
			return
		}
		userID, userEmail, _, entityIDs := emailcommon.RequestIdentity(r, "", "")
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)

		var (
			s3ParsedKey, fromAddr, subject, module, entityID, preview, status, mailDirection string
			receivedAt                                                                       *time.Time
			extractedMeta                                                                    []byte
		)
		err := pool.QueryRow(r.Context(), `
			SELECT COALESCE(s3_parsed_key,''), COALESCE(envelope_from,''), COALESCE(subject,''),
			       received_at, COALESCE(module,''), COALESCE(entity_id,''),
			       COALESCE(body_text_preview,''), processing_status,
			       COALESCE(extracted_metadata::text, '{}'),
			       COALESCE(mail_direction, 'RECEIVED')
			FROM email_svc.message
			WHERE message_id = $1::uuid
			`+emailcommon.SingleMessageScopedToUserSQL+`
		`, messageID, admin, userID, entityIDs, userEmail).Scan(&s3ParsedKey, &fromAddr, &subject, &receivedAt, &module, &entityID, &preview, &status, &extractedMeta, &mailDirection)
		if err != nil {
			emailcommon.RespondNotFound(w, "message not found")
			return
		}

		body := map[string]interface{}{
			"text_plain": preview,
			"text_html":  "",
			"preferred":  "text",
		}
		envelope := map[string]interface{}{
			"from": fromAddr, "subject": subject,
		}

		if s3ParsedKey != "" {
			raw, err := s3storage.GetObjectBytes(r.Context(), s3ParsedKey)
			if err == nil {
				var parsed map[string]interface{}
				if json.Unmarshal(raw, &parsed) == nil {
					if b, ok := parsed["body"].(map[string]interface{}); ok {
						body = b
					}
					if e, ok := parsed["envelope"].(map[string]interface{}); ok {
						envelope = e
					}
				}
			}
		}

		attRows, err := pool.Query(r.Context(), `
			SELECT attachment_id::text, filename, content_type, file_size, s3_key
			FROM email_svc.message_attachment
			WHERE message_id = $1::uuid
		`, messageID)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		defer attRows.Close()

		type att struct {
			AttachmentID string `json:"attachment_id"`
			Filename     string `json:"filename"`
			ContentType  string `json:"content_type"`
			FileSize     int64  `json:"file_size"`
			S3Key        string `json:"s3_key"`
			Source       string `json:"source"`
			DownloadURL  string `json:"download_url,omitempty"`
		}
		var attachments []att
		for attRows.Next() {
			var a att
			if err := attRows.Scan(&a.AttachmentID, &a.Filename, &a.ContentType, &a.FileSize, &a.S3Key); err != nil {
				emailcommon.RespondInternal(w, err.Error())
				return
			}
			a.Source = attachmentSource(a.S3Key)
			if url, err := s3storage.GetDownloadPresignedURL(r.Context(), a.S3Key, 15*time.Minute); err == nil {
				a.DownloadURL = url
			}
			attachments = append(attachments, a)
		}

		var meta map[string]interface{}
		_ = json.Unmarshal(extractedMeta, &meta)

		emailcommon.RespondPayload(w, "messages/get", map[string]interface{}{
			"row": map[string]interface{}{
				"message_id":         messageID,
				"envelope":           envelope,
				"body":               body,
				"attachments":        attachments,
				"received_at":        api.FormatAuditTimestampPtrIST(receivedAt),
				"module":             module,
				"entity_id":          entityID,
				"processing_status":  status,
				"mail_direction":     mailDirection,
				"extracted_metadata": meta,
			},
		})
	}
}

func HandleMessageExtract(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			MessageID string `json:"message_id"`
			Module    string `json:"module"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "invalid body")
			return
		}
		if strings.TrimSpace(req.MessageID) == "" {
			emailcommon.RespondBadRequest(w, "message_id is required")
			return
		}

		var s3ParsedKey, module string
		err := pool.QueryRow(r.Context(), `
			SELECT COALESCE(s3_parsed_key,''), COALESCE(NULLIF($2,''), module, '')
			FROM email_svc.message
			WHERE message_id = $1::uuid
		`, req.MessageID, req.Module).Scan(&s3ParsedKey, &module)
		if err != nil || s3ParsedKey == "" {
			emailcommon.RespondNotFound(w, "parsed message not found")
			return
		}

		rt := mailruntime.NewRuntime()
		if !rt.Ready() {
			emailcommon.RespondUnavailable(w, "mail processing unavailable")
			return
		}

		out, err := rt.ExtractStructured(r.Context(), s3ParsedKey, module)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}

		metaBytes, _ := json.Marshal(out.ExtractedMetadata)
		_, _ = pool.Exec(r.Context(), `
			UPDATE email_svc.message
			SET extracted_metadata = $2::jsonb, updated_at = now()
			WHERE message_id = $1::uuid
		`, req.MessageID, string(metaBytes))

		emailcommon.RespondPayload(w, "messages/extract", map[string]interface{}{"row": out})
	}
}

func HandleMessageLink(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			MessageID string `json:"message_id"`
			Module    string `json:"module"`
			EntityID  string `json:"entity_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "invalid body")
			return
		}
		if strings.TrimSpace(req.MessageID) == "" || strings.TrimSpace(req.EntityID) == "" {
			emailcommon.RespondBadRequest(w, "message_id and entity_id are required")
			return
		}

		_, err := pool.Exec(r.Context(), `
			UPDATE email_svc.message
			SET module = COALESCE(NULLIF($2,''), module),
			    entity_id = $3,
			    updated_at = now()
			WHERE message_id = $1::uuid
		`, req.MessageID, req.Module, req.EntityID)
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		emailcommon.RespondPayload(w, "inbox/update", map[string]interface{}{})
	}
}

func HandleAttachmentDownload(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}
		var req struct {
			AttachmentID string `json:"attachment_id"`
			Preview      bool   `json:"preview"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, "invalid body")
			return
		}
		attachmentID := strings.TrimSpace(req.AttachmentID)
		if attachmentID == "" {
			emailcommon.RespondBadRequest(w, "attachment_id is required")
			return
		}
		userID, userEmail, _, entityIDs := emailcommon.RequestIdentity(r, "", "")
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)

		var s3Key string
		err := pool.QueryRow(r.Context(), `
			SELECT ma.s3_key
			FROM email_svc.message_attachment ma
			JOIN email_svc.message m ON m.message_id = ma.message_id
			WHERE ma.attachment_id = $1::uuid
			`+emailcommon.JoinedMessageScopedToUserSQL+`
		`, attachmentID, admin, userID, entityIDs, userEmail).Scan(&s3Key)
		if err != nil {
			emailcommon.RespondNotFound(w, "attachment not found")
			return
		}

		var signedURL string
		if req.Preview {
			signedURL, err = s3storage.GetInlinePresignedURL(r.Context(), s3Key, 15*time.Minute)
		} else {
			signedURL, err = s3storage.GetDownloadPresignedURL(r.Context(), s3Key, 15*time.Minute)
		}
		if err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}
		emailcommon.RespondPayload(w, "attachments/download", map[string]interface{}{
			"download_url": signedURL,
		})
	}
}
