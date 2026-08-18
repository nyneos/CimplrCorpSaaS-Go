package fdRateNegotiation

import (
	"CimplrCorpSaas/api"
	notifcatalog "CimplrCorpSaas/api/notification/catalog"
	"context"
	"fmt"
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

		recs, err := loadRecipientsMap(ctx, pool, []string{communicationID})
		if err != nil {
			api.LogError("[FDRateNeg] bank email load recipients %s: %v", communicationID, err)
			return
		}
		to, cc := splitRecipientEmails(recs[communicationID])
		all := append([]string{}, to...)
		all = append(all, cc...)

		payload := row.toPayload("SEND", actorEmail, emailContent, all)
		payload["CommunicationID"] = communicationID

		// Bank Communication picks a DMS template. Delivery still goes through
		// the notification EMAIL pipeline for this source_route. A DMS UUID
		// must not be used as a notification_svc.template_id filter — that
		// would match nothing and silently skip the send.
		var allowed []string
		id := strings.TrimSpace(templateID)
		if id != "" && !looksLikeUUID(id) {
			allowed = []string{id}
		}
		notifcatalog.TriggerNotificationForTemplates(
			ctx, pool, sourceRouteBankEmail, communicationID, payload, allowed,
		)
	}()
}
