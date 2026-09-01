package emailmessages

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	emailcommon "CimplrCorpSaas/api/email/common"
	"CimplrCorpSaas/api/utils/s3storage"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

func bodyNeedsFullParse(body map[string]interface{}, preview string) bool {
	plain, _ := body["text_plain"].(string)
	html, _ := body["text_html"].(string)
	plain = strings.TrimSpace(plain)
	html = strings.TrimSpace(html)
	if plain == "" && html == "" {
		return true
	}
	if preview != "" && plain == preview {
		return true
	}
	if preview != "" && len(plain) > 0 && len(plain) <= 320 && len(preview) >= 280 && strings.HasPrefix(plain, strings.TrimSpace(preview)) {
		return true
	}
	return false
}

func HandleMessageList(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			emailcommon.RespondMethodNotAllowed(w)
			return
		}

		var req struct {
			Module            string `json:"module"`
			EntityID          string `json:"entity_id"`
			BusinessEntityID  string `json:"business_entity_id"`
			RateRequestID     string `json:"rate_request_id"`
			Status            string `json:"status"`
			MailDirection     string `json:"mail_direction"`
			DateFrom          string `json:"date_from"`
			DateTo            string `json:"date_to"`
			InboxID           string `json:"inbox_id"`
			Limit             int    `json:"limit"`
			Offset            int    `json:"offset"`
			FilterMatchedOnly *bool  `json:"filter_matched_only"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}

		module := strings.TrimSpace(req.Module)
		entityID := strings.TrimSpace(req.EntityID)
		businessEntityID := strings.TrimSpace(req.BusinessEntityID)
		rateRequestID := strings.TrimSpace(req.RateRequestID)
		status := strings.TrimSpace(req.Status)
		mailDirection := strings.ToUpper(strings.TrimSpace(req.MailDirection))
		dateFrom := strings.TrimSpace(req.DateFrom)
		dateTo := strings.TrimSpace(req.DateTo)
		inboxID := strings.TrimSpace(req.InboxID)
		userID, userEmail, _, entityIDs := emailcommon.RequestIdentity(r, "", "")
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)
		offset := req.Offset
		if offset < 0 {
			offset = 0
		}
		filterMatchedOnly := true
		if req.FilterMatchedOnly != nil {
			filterMatchedOnly = *req.FilterMatchedOnly
		}

		baseWhere := `
			WHERE ($1 = '' OR m.module = $1)
			  AND ($2 = '' OR m.entity_id = $2)
			  AND ($3 = '' OR m.processing_status = $3)
			  AND ($4 = '' OR (COALESCE(m.received_at, m.created_at) AT TIME ZONE 'Asia/Kolkata') >= $4::date)
			  AND ($5 = '' OR (COALESCE(m.received_at, m.created_at) AT TIME ZONE 'Asia/Kolkata') < ($5::date + interval '1 day'))
			  AND ($6 = '' OR m.inbox_id::text = $6)
			  AND (NOT $7 OR m.filter_matched = true OR m.processing_status = 'MANUAL_UPLOAD')
			  AND ($8 = '' OR UPPER(COALESCE(m.mail_direction, 'RECEIVED')) = $8)`

		args := []interface{}{module, entityID, status, dateFrom, dateTo, inboxID, filterMatchedOnly, mailDirection}
		scopeSQL := ""
		if !admin && len(entityIDs) > 0 {
			scopeSQL += emailcommon.MessagePrevalidationEntityScopeSQL(len(args) + 1)
			args = append(args, entityIDs)
		}
		if businessEntityID != "" {
			treeIDs, treeErr := emailcommon.ResolveEntityTreeIDs(r.Context(), pool, businessEntityID)
			if treeErr != nil || len(treeIDs) == 0 {
				treeIDs = []string{businessEntityID}
			}
			scopeSQL += emailcommon.MessageBusinessEntityFilterSQL(len(args) + 1)
			args = append(args, treeIDs)
		}
		if rateRequestID != "" {
			scopeSQL += emailcommon.MessageUnlinkedOrRateRequestSQL(len(args) + 1)
			args = append(args, rateRequestID)
		}
		adminN := len(args) + 1
		userIDN := len(args) + 2
		userEmailN := len(args) + 3
		args = append(args, admin, userID, userEmail)

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
			       COALESCE(array_to_string(m.envelope_to, ', '), ''),
			       COALESCE(
			         me_ent.entity_name,
			         n_ent.entity_name,
			         ''
			       ),
			       COALESCE(
			         n_ent.entity_id,
			         NULLIF(m.entity_id, ''),
			         ic_ent.entity_id,
			         ''
			       )
			FROM email_svc.message m
			LEFT JOIN investment.fd_rate_negotiation n_ent
			  ON n_ent.rate_request_id::text = m.entity_id
			 AND COALESCE(n_ent.is_deleted, false) = false
			LEFT JOIN email_svc.inbox_config ic_ent ON ic_ent.inbox_id = m.inbox_id
			LEFT JOIN masterentitycash me_ent
			  ON me_ent.entity_id = COALESCE(n_ent.entity_id, NULLIF(m.entity_id, ''), ic_ent.entity_id)
			 AND (me_ent.is_deleted IS NOT TRUE)
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
			` + baseWhere + scopeSQL + emailcommon.ActiveInboxMessageSQL + `
			` + emailcommon.ListMessageScopedToUserSQLAt(adminN, userIDN, userEmailN) + `
			ORDER BY CASE WHEN m.processing_status = 'MANUAL_UPLOAD' THEN m.created_at ELSE COALESCE(m.received_at, m.created_at) END DESC`

		limit := req.Limit
		if limit <= 0 {
			limit = 50
		}
		if limit > 200 {
			limit = 200
		}

		countQuery := `
			SELECT COUNT(*)
			FROM email_svc.message m
			LEFT JOIN investment.fd_rate_negotiation n_ent
			  ON n_ent.rate_request_id::text = m.entity_id
			 AND COALESCE(n_ent.is_deleted, false) = false
			LEFT JOIN email_svc.inbox_config ic_ent ON ic_ent.inbox_id = m.inbox_id
			LEFT JOIN masterentitycash me_ent
			  ON me_ent.entity_id = COALESCE(n_ent.entity_id, NULLIF(m.entity_id, ''), ic_ent.entity_id)
			 AND (me_ent.is_deleted IS NOT TRUE)
			` + baseWhere + scopeSQL + emailcommon.ActiveInboxMessageSQL + `
			` + emailcommon.ListMessageScopedToUserSQLAt(adminN, userIDN, userEmailN)

		countArgs := args
		var totalCount int
		if err := pool.QueryRow(r.Context(), countQuery, countArgs...).Scan(&totalCount); err != nil {
			emailcommon.RespondInternal(w, err.Error())
			return
		}

		query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
		args = append(args, limit, offset)

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
			EntityName            string     `json:"entity_name"`
			BusinessEntityID      string     `json:"business_entity_id"`
		}

		items := make([]row, 0)
		for rows.Next() {
			var item row
			if err := rows.Scan(&item.MessageID, &item.InboxID, &item.S3RawKey, &item.S3ParsedKey,
				&item.EnvelopeFrom, &item.Subject, &item.ReceivedAt, &item.HasAttachments,
				&item.FilterMatched, &item.ProcessingStatus, &item.Module, &item.EntityID,
				&item.BodyTextPreview, &item.CreatedAt, &item.UploadedBy,
				&item.AttachmentCount, &item.ParsedAttachmentCount, &item.AdditionalFileCount,
				&item.MailDirection, &item.EnvelopeTo, &item.EntityName, &item.BusinessEntityID); err != nil {
				emailcommon.RespondInternal(w, err.Error())
				return
			}
			items = append(items, item)
		}
		emailcommon.RespondListPaged(w, "messages/list", items, len(items), totalCount)
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
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		messageID := strings.TrimSpace(req.MessageID)
		if messageID == "" {
			emailcommon.RespondBadRequest(w, "message_id is required")
			return
		}
		userID, userEmail, _, _ := emailcommon.RequestIdentity(r, "", "")
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)

		var (
			s3RawKey, s3ParsedKey, fromAddr, subject, module, entityID, preview, status, mailDirection string
			envelopeTo                                                                                 []string
			receivedAt                                                                                 *time.Time
			extractedMeta                                                                              []byte
		)
		err := pool.QueryRow(r.Context(), `
			SELECT COALESCE(s3_raw_key,''), COALESCE(s3_parsed_key,''), COALESCE(envelope_from,''), COALESCE(subject,''),
			       received_at, COALESCE(module,''), COALESCE(entity_id,''),
			       COALESCE(body_text_preview,''), processing_status,
			       COALESCE(extracted_metadata::text, '{}'),
			       COALESCE(mail_direction, 'RECEIVED'),
			       COALESCE(envelope_to, ARRAY[]::text[])
			FROM email_svc.message
			WHERE message_id = $1::uuid
			`+emailcommon.SingleMessageScopedToUserSQL+`
		`, messageID, admin, userID, userEmail).Scan(&s3RawKey, &s3ParsedKey, &fromAddr, &subject, &receivedAt, &module, &entityID, &preview, &status, &extractedMeta, &mailDirection, &envelopeTo)
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
		if len(envelopeTo) > 0 {
			envelope["to"] = envelopeTo
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
		if bodyNeedsFullParse(body, preview) && strings.TrimSpace(s3RawKey) != "" {
			rt := mailruntime.NewRuntime()
			if rt.Ready() {
				if parsed, err := rt.DecodeMessage(r.Context(), s3RawKey); err == nil && parsed != nil {
					if plain := strings.TrimSpace(parsed.Body.TextPlain); plain != "" {
						body["text_plain"] = plain
					}
					if html := strings.TrimSpace(parsed.Body.TextHTML); html != "" {
						body["text_html"] = html
						if plain := strings.TrimSpace(parsed.Body.TextPlain); plain == "" {
							body["preferred"] = "html"
						}
					}
					if parsed.Envelope.From != "" {
						envelope["from"] = parsed.Envelope.From
					}
					if len(parsed.Envelope.To) > 0 {
						envelope["to"] = parsed.Envelope.To
					}
					if parsed.Envelope.Subject != "" {
						envelope["subject"] = parsed.Envelope.Subject
					}
				}
			}
		}
		if toVal, ok := envelope["to"]; !ok || toVal == nil {
			if len(envelopeTo) > 0 {
				envelope["to"] = envelopeTo
			}
		} else if arr, ok := toVal.([]string); ok && len(arr) == 0 && len(envelopeTo) > 0 {
			envelope["to"] = envelopeTo
		} else if arr, ok := toVal.([]interface{}); ok && len(arr) == 0 && len(envelopeTo) > 0 {
			envelope["to"] = envelopeTo
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
	const (
		minExtractBodyRunes = 20
		maxExtractBodyRunes = 512000
	)

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
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		if strings.TrimSpace(req.MessageID) == "" {
			emailcommon.RespondBadRequest(w, "message_id is required")
			return
		}

		var s3RawKey, s3ParsedKey, module string
		err := pool.QueryRow(r.Context(), `
			SELECT COALESCE(s3_raw_key,''), COALESCE(s3_parsed_key,''), COALESCE(NULLIF($2,''), module, '')
			FROM email_svc.message
			WHERE message_id = $1::uuid
		`, req.MessageID, req.Module).Scan(&s3RawKey, &s3ParsedKey, &module)
		if err != nil {
			emailcommon.RespondNotFound(w, "message not found")
			return
		}

		rt := mailruntime.NewRuntime()
		if !rt.Ready() {
			emailcommon.RespondUnavailable(w, "mail processing unavailable")
			return
		}

		if s3ParsedKey == "" && strings.TrimSpace(s3RawKey) != "" {
			decoded, decErr := rt.DecodeMessage(r.Context(), s3RawKey)
			if decErr == nil && decoded != nil && strings.TrimSpace(decoded.S3ParsedKey) != "" {
				s3ParsedKey = decoded.S3ParsedKey
				_, _ = pool.Exec(r.Context(), `
					UPDATE email_svc.message SET s3_parsed_key = $2, updated_at = now() WHERE message_id = $1::uuid
				`, req.MessageID, s3ParsedKey)
			}
		}
		if s3ParsedKey == "" {
			emailcommon.RespondNotFound(w, "parsed message not found — re-ingest or open preview to parse from raw")
			return
		}

		bodyLen, bodyErr := parsedBodyRuneLen(r.Context(), s3ParsedKey)
		if bodyErr != nil {
			emailcommon.RespondBadRequest(w, bodyErr.Error())
			return
		}
		if bodyLen < minExtractBodyRunes {
			emailcommon.RespondBadRequest(w, fmt.Sprintf("email body too short for extraction (%d chars, minimum %d)", bodyLen, minExtractBodyRunes))
			return
		}
		if bodyLen > maxExtractBodyRunes {
			emailcommon.RespondBadRequest(w, fmt.Sprintf("email body too large for extraction (%d chars, maximum %d)", bodyLen, maxExtractBodyRunes))
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

func parsedBodyRuneLen(ctx context.Context, s3ParsedKey string) (int, error) {
	raw, err := s3storage.GetObjectBytes(ctx, s3ParsedKey)
	if err != nil {
		return 0, fmt.Errorf("could not read parsed message from storage")
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return 0, fmt.Errorf("invalid parsed message json")
	}
	body, _ := parsed["body"].(map[string]interface{})
	plain, _ := body["text_plain"].(string)
	html, _ := body["text_html"].(string)
	text := strings.TrimSpace(plain)
	if text == "" {
		text = strings.TrimSpace(html)
	}
	if text == "" {
		return 0, nil
	}
	return len([]rune(text)), nil
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
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
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
			emailcommon.RespondBadRequest(w, constants.ErrInvalidBody)
			return
		}
		attachmentID := strings.TrimSpace(req.AttachmentID)
		if attachmentID == "" {
			emailcommon.RespondBadRequest(w, "attachment_id is required")
			return
		}
		userID, userEmail, _, _ := emailcommon.RequestIdentity(r, "", "")
		admin := emailcommon.IsEmailAdmin(r.Context(), userID)

		var s3Key string
		err := pool.QueryRow(r.Context(), `
			SELECT ma.s3_key
			FROM email_svc.message_attachment ma
			JOIN email_svc.message m ON m.message_id = ma.message_id
			WHERE ma.attachment_id = $1::uuid
			`+emailcommon.JoinedMessageScopedToUserSQL+`
		`, attachmentID, admin, userID, userEmail).Scan(&s3Key)
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
