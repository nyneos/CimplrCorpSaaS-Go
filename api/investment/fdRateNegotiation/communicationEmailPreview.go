package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type communicationSentEmailRow struct {
	OutboxID         string `json:"outbox_id"`
	CommunicationID  string `json:"communication_id"`
	RecipientEmail   string `json:"recipient_email"`
	CCEmails         string `json:"cc_emails"`
	RenderedSubject  string `json:"rendered_subject"`
	RenderedBody     string `json:"rendered_body"`
	ProcessingStatus string `json:"processing_status"`
	SentAt           string `json:"sent_at"`
	LastError        string `json:"last_error"`
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
			results = append(results, item)
		}

		api.RespondEnvelopeSuccess(w, "", results)
	}
}
