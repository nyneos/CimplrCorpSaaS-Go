package emailmailbox

import (
	"context"
	"fmt"
	"os"
	"strings"

	emailjobs "CimplrCorpSaas/internal/jobs/email"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// validateMailboxReady ensures required integrations are configured before approval.
func ValidateMailboxReady(ctx context.Context, pool *pgxpool.Pool, inboxID string) error {
	var sourceType, mailbox string
	var graphFields MailboxGraphFields
	var imapFields MailboxIMAPFields
	var graphTenantKey string
	err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT COALESCE(source_type, 'OUTLOOK_GRAPH'), mailbox_address,
		       %s, %s, %s, %s, %s,
		       %s, %s, %s,
		       %s, %s,
		       %s, %s,
		       %s
		FROM email_svc.inbox_config
		WHERE inbox_id = $1::uuid AND is_deleted = false
	`, emailjobs.SQLGraphTenantKey, emailjobs.SQLGraphTenantLabel, emailjobs.SQLGraphTenantID,
		emailjobs.SQLGraphClientID, emailjobs.SQLGraphSecret,
		emailjobs.SQLIMAPProvider, emailjobs.SQLIMAPHost, emailjobs.SQLIMAPPort,
		emailjobs.SQLIMAPUsername, emailjobs.SQLIMAPPassword,
		emailjobs.SQLIMAPInboxFolder, emailjobs.SQLIMAPSentFolder,
		emailjobs.SQLIMAPUseTLS), inboxID).Scan(
		&sourceType, &mailbox,
		&graphTenantKey, &graphFields.TenantLabel, &graphFields.TenantID, &graphFields.ClientID, &graphFields.ClientSecret,
		&imapFields.Provider, &imapFields.Host, &imapFields.Port,
		&imapFields.Username, &imapFields.Password,
		&imapFields.InboxFolder, &imapFields.SentFolder, &imapFields.UseTLS,
	)
	if err != nil {
		return fmt.Errorf("mailbox not found")
	}

	emailClient := mailruntime.NewRuntime()
	if !emailClient.Ready() {
		return fmt.Errorf("mail processing not configured")
	}

	switch strings.ToUpper(strings.TrimSpace(sourceType)) {
	case "SES":
		if strings.TrimSpace(os.Getenv("EMAIL_SES_IAM_ROLE_ARN")) == "" {
			return fmt.Errorf("SES mailbox %s requires EMAIL_SES_IAM_ROLE_ARN", mailbox)
		}
		if strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_BUCKET")) == "" {
			return fmt.Errorf("SES mailbox %s requires EMAIL_INBOUND_S3_BUCKET", mailbox)
		}
	case "IMAP":
		if err := emailClient.VerifyIMAP(ctx, mailbox, imapFields.ToRuntimeIMAP()); err != nil {
			return fmt.Errorf("IMAP mailbox %s connection failed: %w", mailbox, err)
		}
	default:
		graphFields.TenantKey = graphTenantKey
		payload, err := ResolveGraphForMailbox(ctx, pool, graphFields)
		if err != nil {
			return fmt.Errorf("Outlook mailbox %s: %w", mailbox, err)
		}
		if err := emailClient.VerifyGraph(ctx, payload); err != nil {
			return fmt.Errorf("Outlook mailbox %s graph connection failed: %w", mailbox, err)
		}
	}
	return nil
}
