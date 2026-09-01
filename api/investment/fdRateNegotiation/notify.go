package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	sourceRouteCreate    = "/investment/fd/rate-negotiation/create"
	sourceRouteUpdate    = "/investment/fd/rate-negotiation/update"
	sourceRouteApprove   = "/investment/fd/rate-negotiation/approve"
	sourceRouteReject    = "/investment/fd/rate-negotiation/reject"
	sourceRouteDelete    = "/investment/fd/rate-negotiation/delete"
	sourceRouteBankEmail = "/investment/fd/rate-negotiation/communication/create"
)

func looksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	// 8-4-4-4-12
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
				return false
			}
		}
	}
	return true
}

func sourceRouteForAction(action string) string {
	switch strings.ToUpper(strings.TrimSpace(action)) {
	case "CREATE":
		return sourceRouteCreate
	case "EDIT", "UPDATE":
		return sourceRouteUpdate
	case "APPROVE":
		return sourceRouteApprove
	case "REJECT":
		return sourceRouteReject
	case "DELETE":
		return sourceRouteDelete
	default:
		return sourceRouteCreate
	}
}

type rateNotifRow struct {
	RateRequestID  string
	RateRequestRef string
	RequestStatus  string
	ProposedAmount float64
	Currency       string
	TenureType     string
	TenureValue    int
	StartDate      string
	InterestType   string
	BankNames      string
}

func loadRateNotifRow(ctx context.Context, pool *pgxpool.Pool, rateRequestID string) (rateNotifRow, error) {
	var row rateNotifRow
	err := pool.QueryRow(ctx, `
		SELECT
			rate_request_id::text,
			COALESCE(rate_request_ref,''),
			COALESCE(request_status,''),
			COALESCE(proposed_fd_amount,0),
			COALESCE(currency_code,''),
			COALESCE(tenure_type,''),
			COALESCE(tenure_value,0),
			COALESCE(TO_CHAR(expected_start_date,'YYYY-MM-DD'),''),
			COALESCE(interest_type,''),
			COALESCE(array_to_string(target_bank_names, ', '),'')
		FROM investment.fd_rate_negotiation
		WHERE rate_request_id = $1::uuid`, rateRequestID).Scan(
		&row.RateRequestID, &row.RateRequestRef, &row.RequestStatus,
		&row.ProposedAmount, &row.Currency, &row.TenureType, &row.TenureValue,
		&row.StartDate, &row.InterestType, &row.BankNames,
	)
	return row, err
}

func (row rateNotifRow) tenureLabel() string {
	if row.TenureValue <= 0 {
		return row.TenureType
	}
	return fmt.Sprintf("%d %s", row.TenureValue, strings.ToLower(row.TenureType))
}

func (row rateNotifRow) toPayload(action, actorEmail, emailContent string, emailTo []string) map[string]interface{} {
	to := emailTo
	if to == nil {
		to = []string{}
	}
	return map[string]interface{}{
		"Action":          action,
		"ActorEmail":      actorEmail,
		"actor_email":     actorEmail,
		"UserID":          actorEmail,
		"RateRequestID":   row.RateRequestID,
		"RateRequestRef":  row.RateRequestRef,
		"RequestStatus":   row.RequestStatus,
		"ProposedAmount":  row.ProposedAmount,
		"Currency":        row.Currency,
		"Tenure":          row.tenureLabel(),
		"TenureType":      row.TenureType,
		"TenureValue":     row.TenureValue,
		"StartDate":       row.StartDate,
		"InterestType":    row.InterestType,
		"BankNames":       row.BankNames,
		"EmailContent":    emailContent,
		"EmailTo":         to,
		"RecipientEmails": to,
		"ActionAt":        time.Now().Format(time.RFC3339),
	}
}

func fireRateNegotiationNotification(pool *pgxpool.Pool, rateRequestID, actorEmail, action string) {
	if pool == nil || strings.TrimSpace(rateRequestID) == "" {
		return
	}
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDRateNeg] notification panic %s %s: %v", action, rateRequestID, rec)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		row, err := loadRateNotifRow(ctx, pool, rateRequestID)
		if err != nil {
			api.LogError("[FDRateNeg] notification load %s: %v", rateRequestID, err)
			return
		}
		notifcatalog.TriggerNotification(
			ctx, pool, sourceRouteForAction(action), rateRequestID,
			row.toPayload(action, actorEmail, "", nil),
		)
	}()
}

const (
	notifTplBankStandard = "[FD Rate] Bank Rate Request — Standard"
	notifTplBankUrgent   = "[FD Rate] Bank Rate Request — Urgent"
)

// resolveBankEmailNotificationTemplates maps the DMS template picked in Bank
// Communication to exactly one notification_svc template. Without this filter,
// every approved EMAIL template for the source route fires (Standard + Urgent).
func resolveBankEmailNotificationTemplates(
	ctx context.Context,
	pool *pgxpool.Pool,
	communicationID, templateID string,
) []string {
	id := strings.TrimSpace(templateID)
	if id != "" && !looksLikeUUID(id) {
		return []string{id}
	}

	templateName := ""
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE(NULLIF(TRIM(email_template_name), ''), '')
		FROM investment.fd_rate_communication
		WHERE communication_id = $1::uuid`, communicationID).Scan(&templateName)

	if templateName == "" && looksLikeUUID(id) {
		_ = pool.QueryRow(ctx, `
			SELECT COALESCE(name, '')
			FROM dms_svc.template
			WHERE template_id = $1::uuid AND COALESCE(is_deleted, false) = false
			LIMIT 1`, id).Scan(&templateName)
	}

	notifName := notifTplBankStandard
	if strings.Contains(strings.ToLower(templateName), "urgent") {
		notifName = notifTplBankUrgent
	}

	var notifID string
	err := pool.QueryRow(ctx, `
		SELECT t.template_id::text
		FROM notification_svc.template t
		JOIN notification_svc.event e ON e.event_id = t.event_id
		WHERE e.source_route = $1
		  AND COALESCE(e.is_deleted, false) = false
		  AND COALESCE(t.is_deleted, false) = false
		  AND t.template_name = $2
		  AND t.channel = 'EMAIL'
		LIMIT 1`, sourceRouteBankEmail, notifName).Scan(&notifID)
	if err != nil || notifID == "" {
		_ = pool.QueryRow(ctx, `
			SELECT t.template_id::text
			FROM notification_svc.template t
			JOIN notification_svc.event e ON e.event_id = t.event_id
			WHERE e.source_route = $1
			  AND COALESCE(e.is_deleted, false) = false
			  AND COALESCE(t.is_deleted, false) = false
			  AND t.template_name = $2
			  AND t.channel = 'EMAIL'
			LIMIT 1`, sourceRouteBankEmail, notifTplBankStandard).Scan(&notifID)
	}
	if notifID != "" {
		return []string{notifID}
	}
	return nil
}

func fireBankCommunicationEmail(
	pool *pgxpool.Pool,
	communicationID, rateRequestID, actorEmail, templateID, emailContent string,
) {
	if pool == nil || strings.TrimSpace(communicationID) == "" || strings.TrimSpace(rateRequestID) == "" {
		return
	}
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FDRateNeg] bank email panic comm=%s: %v", communicationID, rec)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		row, err := loadRateNotifRow(ctx, pool, rateRequestID)
		if err != nil {
			api.LogError("[FDRateNeg] bank email load request %s: %v", rateRequestID, err)
			return
		}

		var bankID, bankName, emailTemplateName string
		if err = pool.QueryRow(ctx, `
			SELECT COALESCE(bank_id,''), COALESCE(bank_name,''),
			       COALESCE(NULLIF(TRIM(email_template_name), ''), '')
			FROM investment.fd_rate_communication
			WHERE communication_id = $1::uuid`, communicationID).Scan(&bankID, &bankName, &emailTemplateName); err != nil {
			api.LogError("[FDRateNeg] bank email load communication bank %s: %v", communicationID, err)
			// continue — email can still send; BankName may be empty
		}

		recs, err := loadRecipientsMap(ctx, pool, []string{communicationID})
		if err != nil {
			api.LogError("[FDRateNeg] bank email load recipients %s: %v", communicationID, err)
			return
		}
		to, cc := splitRecipientEmails(recs[communicationID])
		// One outbox row per TO recipient; CC riders are stamped on those rows
		// after dispatch so the bank gets a single mail carrying a Cc header
		// instead of one separate mail per CC address.
		if len(to) == 0 {
			to = cc
			cc = nil
		}

		payload := row.toPayload("SEND", actorEmail, emailContent, to)
		payload["CommunicationID"] = communicationID
		payload["BankID"] = bankID
		payload["BankName"] = bankName
		// Prefer the attributed bank in body tokens when set; keep BankNames as full target list.
		if bankName != "" {
			payload["Bank"] = bankName
		}

		content := strings.TrimSpace(emailContent)
		if content != "" {
			// The DMS cover email is what the bank must receive — it replaces the
			// notification wrapper body outright so no template markup reaches the
			// recipient. Plain-text content is wrapped so it is valid HTML too.
			if !strings.Contains(content, "<") {
				content = "<p>" + strings.ReplaceAll(html.EscapeString(content), "\n", "<br/>") + "</p>"
			}
			payload["EmailBodyHTML"] = content
			subject := fmt.Sprintf("FD rate request %s", row.RateRequestRef)
			if strings.Contains(strings.ToLower(emailTemplateName), "urgent") {
				subject = fmt.Sprintf("URGENT FD rate request %s", row.RateRequestRef)
			}
			payload["EmailSubject"] = subject
		}

		allowed := resolveBankEmailNotificationTemplates(ctx, pool, communicationID, templateID)
		notifcatalog.TriggerNotificationForTemplatesWithAttachments(
			ctx, pool, sourceRouteBankEmail, communicationID, payload, allowed,
			loadCommunicationAttachments(ctx, pool, communicationID),
		)

		if len(cc) > 0 {
			if _, err = pool.Exec(ctx, `
				UPDATE notification_svc.outbox
				SET cc_emails = $2
				WHERE correlation_id = $1
				  AND channel = 'EMAIL'
				  AND processing_status IN ('PENDING','QUEUED','PROCESSING')`,
				communicationID, strings.Join(cc, ", ")); err != nil {
				api.LogError("[FDRateNeg] bank email apply cc %s: %v", communicationID, err)
			}
		}
	}()
}

// loadCommunicationAttachments returns the files linked to this communication so
// the bank mail carries whatever the maker attached when composing it.
func loadCommunicationAttachments(
	ctx context.Context,
	pool *pgxpool.Pool,
	communicationID string,
) []notifcatalog.AttachmentRef {
	rows, err := pool.Query(ctx, `
		SELECT COALESCE(upload_s3_key,''), COALESCE(stored_file_name,''), COALESCE(content_type,'')
		FROM investment.fd_rate_negotiation_files
		WHERE communication_id = $1::uuid
		  AND COALESCE(is_deleted,false) = false
		  AND COALESCE(upload_s3_key,'') <> ''
		ORDER BY uploaded_at, file_id`, communicationID)
	if err != nil {
		api.LogError("[FDRateNeg] bank email load attachments %s: %v", communicationID, err)
		return nil
	}
	defer rows.Close()

	out := make([]notifcatalog.AttachmentRef, 0)
	for rows.Next() {
		var s3Key, fileName, contentType string
		if err := rows.Scan(&s3Key, &fileName, &contentType); err != nil {
			continue
		}
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		out = append(out, notifcatalog.AttachmentRef{
			S3Key:          s3Key,
			Filename:       fileName,
			ContentType:    contentType,
			StorageBackend: "MAIN_S3",
		})
	}
	return out
}
