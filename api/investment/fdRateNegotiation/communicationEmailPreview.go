package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type communicationSentEmailAttachment struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
}

type communicationSentEmailRow struct {
	OutboxID         string                             `json:"outbox_id"`
	CommunicationID  string                             `json:"communication_id"`
	RecipientEmail   string                             `json:"recipient_email"`
	CCEmails         string                             `json:"cc_emails"`
	RenderedSubject  string                             `json:"rendered_subject"`
	RenderedBody     string                             `json:"rendered_body"`
	ProcessingStatus string                             `json:"processing_status"`
	SentAt           string                             `json:"sent_at"`
	LastError        string                             `json:"last_error"`
	IsDraft          bool                               `json:"is_draft"`
	Attachments      []communicationSentEmailAttachment `json:"attachments"`
}

func loadCommunicationFileNames(
	ctx context.Context,
	pgxPool *pgxpool.Pool,
	communicationID string,
) []communicationSentEmailAttachment {
	out := make([]communicationSentEmailAttachment, 0)
	rows, err := pgxPool.Query(ctx, `
		SELECT COALESCE(stored_file_name,''), COALESCE(content_type,'')
		FROM investment.fd_rate_negotiation_files
		WHERE communication_id = $1::uuid
		  AND COALESCE(is_deleted,false) = false
		  AND COALESCE(upload_s3_key,'') <> ''
		ORDER BY uploaded_at, file_id`, communicationID)
	if err != nil {
		api.LogError("[FDRateNeg] draft email attachments %s: %v", communicationID, err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var filename, contentType string
		if err := rows.Scan(&filename, &contentType); err != nil {
			continue
		}
		if filename == "" {
			continue
		}
		out = append(out, communicationSentEmailAttachment{Filename: filename, ContentType: contentType})
	}
	return out
}

func loadDraftSentEmail(
	ctx context.Context,
	pgxPool *pgxpool.Pool,
	communicationID string,
) (communicationSentEmailRow, bool) {
	var row communicationSentEmailRow
	var content, templateName, status, sentAt, requestRef string
	err := pgxPool.QueryRow(ctx, `
		SELECT
			COALESCE(c.email_content,''),
			COALESCE(c.email_template_name,''),
			COALESCE(c.communication_status,''),
			COALESCE(TO_CHAR(c.sent_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
			COALESCE(n.rate_request_ref,'')
		FROM investment.fd_rate_communication c
		LEFT JOIN investment.fd_rate_negotiation n ON n.rate_request_id = c.rate_request_id
		WHERE c.communication_id = $1::uuid
		  AND COALESCE(c.is_deleted,false) = false`, communicationID).
		Scan(&content, &templateName, &status, &sentAt, &requestRef)
	if err != nil || strings.TrimSpace(content) == "" {
		return row, false
	}

	recs, err := loadRecipientsMap(ctx, pgxPool, []string{communicationID})
	if err != nil {
		api.LogError("[FDRateNeg] draft email recipients %s: %v", communicationID, err)
	}
	to, cc := splitRecipientEmails(recs[communicationID])

	subject := templateName
	if requestRef != "" {
		subject = "FD rate request " + requestRef
		if strings.Contains(strings.ToLower(templateName), "urgent") {
			subject = "URGENT FD rate request " + requestRef
		}
	}

	row = communicationSentEmailRow{
		OutboxID:         "draft-" + communicationID,
		CommunicationID:  communicationID,
		RecipientEmail:   strings.Join(to, ", "),
		CCEmails:         strings.Join(cc, ", "),
		RenderedSubject:  subject,
		RenderedBody:     content,
		ProcessingStatus: status,
		SentAt:           sentAt,
		IsDraft:          true,
		Attachments:      loadCommunicationFileNames(ctx, pgxPool, communicationID),
	}
	return row, true
}

func loadSentEmailAttachments(
	ctx context.Context,
	pgxPool *pgxpool.Pool,
	outboxIDs []string,
) map[string][]communicationSentEmailAttachment {
	out := make(map[string][]communicationSentEmailAttachment, len(outboxIDs))
	if len(outboxIDs) == 0 {
		return out
	}
	rows, err := pgxPool.Query(ctx, `
		SELECT outbox_id::text, COALESCE(filename,''), COALESCE(content_type,'')
		FROM notification_svc.outbox_attachment
		WHERE outbox_id::text = ANY($1::text[])
		ORDER BY sort_order, filename`, outboxIDs)
	if err != nil {
		api.LogError("[FDRateNeg] sent email attachments: %v", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var outboxID, filename, contentType string
		if err := rows.Scan(&outboxID, &filename, &contentType); err != nil {
			continue
		}
		if filename == "" {
			continue
		}
		out[outboxID] = append(out[outboxID], communicationSentEmailAttachment{
			Filename:    filename,
			ContentType: contentType,
		})
	}
	return out
}

// GetCommunicationSentEmail returns the rendered subject/body notification_svc
// actually dispatched for a bank communication SYSTEM_EMAIL send. Bank
// Communication emails go through the notification_svc.outbox pipeline
// (fireBankCommunicationEmail in notify.go), keyed by correlation_id =
// communication_id — they are not written to email_svc.message, so this reads
// the outbox directly rather than going through the inbound email preview path.
func GetCommunicationSentEmail(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CommunicationID string `json:"communication_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "Invalid JSON", "")
			return
		}
		communicationID := strings.TrimSpace(req.CommunicationID)
		if communicationID == "" {
			api.RespondEnvelopeError(w, http.StatusBadRequest, "communication_id is required", "")
			return
		}

		ctx := r.Context()
		rows, err := pgxPool.Query(ctx, `
			SELECT
				outbox_id, correlation_id,
				COALESCE(recipient_email,''), COALESCE(cc_emails,''),
				COALESCE(rendered_subject,''), rendered_body,
				processing_status,
				COALESCE(TO_CHAR(sent_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
				COALESCE(last_error,'')
			FROM notification_svc.outbox
			WHERE correlation_id = $1 AND channel = 'EMAIL'
			ORDER BY created_at DESC`,
			communicationID,
		)
		if err != nil {
			api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to fetch sent email", "")
			return
		}
		defer rows.Close()

		results := make([]communicationSentEmailRow, 0)
		outboxIDs := make([]string, 0)
		for rows.Next() {
			var item communicationSentEmailRow
			if err := rows.Scan(
				&item.OutboxID, &item.CommunicationID,
				&item.RecipientEmail, &item.CCEmails,
				&item.RenderedSubject, &item.RenderedBody,
				&item.ProcessingStatus, &item.SentAt, &item.LastError,
			); err != nil {
				continue
			}
			item.Attachments = make([]communicationSentEmailAttachment, 0)
			results = append(results, item)
			outboxIDs = append(outboxIDs, item.OutboxID)
		}

		attachments := loadSentEmailAttachments(ctx, pgxPool, outboxIDs)
		for i := range results {
			if files := attachments[results[i].OutboxID]; len(files) > 0 {
				results[i].Attachments = files
			}
		}

		if len(results) == 0 {
			if draft, ok := loadDraftSentEmail(ctx, pgxPool, communicationID); ok {
				results = append(results, draft)
			}
		}

		api.RespondEnvelopeSuccess(w, "", results)
	}
}
