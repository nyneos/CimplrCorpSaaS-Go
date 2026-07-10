package emailmailbox

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	emailjobs "CimplrCorpSaas/internal/jobs/email"
	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pendingEditCreds mirrors the credential fields stored in inbox_config.pending_edit_json
// so approval can validate the credentials that will actually be applied.
type pendingEditCreds struct {
	SourceType string `json:"source_type"`
	MailboxIMAPFields
	MailboxGraphFields
}

// validateMailboxReady ensures required integrations are configured before approval.
func ValidateMailboxReady(ctx context.Context, pool *pgxpool.Pool, inboxID string) error {
	var sourceType, mailbox string
	var graphFields MailboxGraphFields
	var imapFields MailboxIMAPFields
	var graphTenantKey string
	var pendingEditJSON string
	err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT COALESCE(source_type, 'OUTLOOK_GRAPH'), mailbox_address,
		       %s, %s, %s, %s, %s,
		       %s, %s, %s,
		       %s, %s,
		       %s, %s,
		       %s,
		       COALESCE(pending_edit_json::text, '')
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
		&pendingEditJSON,
	)
	if err != nil {
		return fmt.Errorf("mailbox not found")
	}

	// When approving a pending edit, the new credentials live in pending_edit_json
	// (not yet applied to the columns). Validate the effective, merged credentials.
	if pendingEditJSON != "" && pendingEditJSON != "null" {
		var edit pendingEditCreds
		if json.Unmarshal([]byte(pendingEditJSON), &edit) == nil {
			if strings.TrimSpace(edit.SourceType) != "" {
				sourceType = edit.SourceType
			}
			if IMAPPatchPresent(edit.MailboxIMAPFields) {
				if merged, mErr := MergeIMAPFields(imapFields, edit.MailboxIMAPFields, mailbox); mErr == nil {
					imapFields = merged
				}
			}
			if GraphPatchPresent(edit.MailboxGraphFields) {
				if merged, mErr := MergeGraphFields(graphFields, edit.MailboxGraphFields); mErr == nil {
					graphFields = merged
				}
			}
		}
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
