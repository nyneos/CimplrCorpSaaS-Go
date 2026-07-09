package emailmailbox

import (
	"context"
	"fmt"
	"strings"

	emailjobs "CimplrCorpSaas/internal/jobs/email"
	"CimplrCorpSaas/internal/services/mailruntime"
	"CimplrCorpSaas/internal/services/graphmail"
	"CimplrCorpSaas/internal/services/imapmail"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MailboxGraphFields are typed Graph credential columns (not JSONB).
type MailboxGraphFields struct {
	TenantKey      string `json:"graph_tenant_key"`
	TenantLabel    string `json:"graph_tenant_label"`
	TenantID       string `json:"graph_tenant_id"`
	ClientID       string `json:"graph_client_id"`
	ClientSecret   string `json:"graph_client_secret,omitempty"`
	UseCustomGraph bool   `json:"use_custom_graph,omitempty"`
}

// MailboxIMAPFields are typed IMAP credential columns (not JSONB).
type MailboxIMAPFields struct {
	Provider    string `json:"imap_provider"`
	Host        string `json:"imap_host"`
	Port        int    `json:"imap_port"`
	Username    string `json:"imap_username"`
	Password    string `json:"imap_password,omitempty"`
	UseTLS      bool   `json:"imap_use_tls"`
	InboxFolder string `json:"imap_inbox_folder"`
	SentFolder  string `json:"imap_sent_folder"`
}

func (f MailboxGraphFields) ToGraphMailboxCreds() emailjobs.GraphMailboxCreds {
	return emailjobs.GraphMailboxCreds{
		TenantKey:    strings.TrimSpace(f.TenantKey),
		TenantLabel:  strings.TrimSpace(f.TenantLabel),
		TenantID:     strings.TrimSpace(f.TenantID),
		ClientID:     strings.TrimSpace(f.ClientID),
		ClientSecret: strings.TrimSpace(f.ClientSecret),
	}
}

func ResolveGraphForMailbox(ctx context.Context, pool *pgxpool.Pool, fields MailboxGraphFields) (mailruntime.GraphConnection, error) {
	return emailjobs.ResolveGraphConnection(ctx, pool, fields.ToGraphMailboxCreds())
}

func (f MailboxGraphFields) ToRuntimeGraph() mailruntime.GraphConnection {
	return mailruntime.GraphConnection{
		TenantLabel:  strings.TrimSpace(f.TenantLabel),
		TenantID:     strings.TrimSpace(f.TenantID),
		ClientID:     strings.TrimSpace(f.ClientID),
		ClientSecret: strings.TrimSpace(f.ClientSecret),
	}
}

func (f MailboxGraphFields) toGraphConfig() graphmail.Config {
	return graphmail.Config{
		Label:        strings.TrimSpace(f.TenantLabel),
		TenantID:     strings.TrimSpace(f.TenantID),
		ClientID:     strings.TrimSpace(f.ClientID),
		ClientSecret: strings.TrimSpace(f.ClientSecret),
	}
}

func (f MailboxGraphFields) redacted() MailboxGraphFields {
	out := f
	if out.ClientSecret != "" {
		out.ClientSecret = "********"
	}
	return out
}

func graphFieldsFromConfig(cfg graphmail.Config) MailboxGraphFields {
	return MailboxGraphFields{
		TenantLabel: cfg.Label,
		TenantID:    cfg.TenantID,
		ClientID:    cfg.ClientID,
	}
}

func (f MailboxIMAPFields) ToRuntimeIMAP() mailruntime.IMAPConnection {
	port := f.Port
	if port <= 0 {
		port = 993
	}
	return mailruntime.IMAPConnection{
		Provider:    strings.TrimSpace(f.Provider),
		Host:        strings.TrimSpace(f.Host),
		Port:        port,
		Username:    strings.TrimSpace(f.Username),
		Password:    strings.TrimSpace(f.Password),
		UseTLS:      f.UseTLS,
		InboxFolder: strings.TrimSpace(f.InboxFolder),
		SentFolder:  strings.TrimSpace(f.SentFolder),
	}
}

func (f MailboxIMAPFields) toIMAPConfig(mailbox string) imapmail.Config {
	port := f.Port
	if port <= 0 {
		port = 993
	}
	return imapmail.Config{
		Provider:    strings.TrimSpace(f.Provider),
		Host:        strings.TrimSpace(f.Host),
		Port:        port,
		Username:    strings.TrimSpace(f.Username),
		Password:    strings.TrimSpace(f.Password),
		UseTLS:      f.UseTLS,
		InboxFolder: strings.TrimSpace(f.InboxFolder),
		SentFolder:  strings.TrimSpace(f.SentFolder),
	}
}

func (f MailboxIMAPFields) redacted() MailboxIMAPFields {
	out := f
	if out.Password != "" {
		out.Password = "********"
	}
	return out
}

func MergeGraphFields(existing MailboxGraphFields, patch MailboxGraphFields) (MailboxGraphFields, error) {
	out := existing
	if strings.TrimSpace(patch.TenantKey) != "" {
		out.TenantKey = strings.TrimSpace(patch.TenantKey)
	}
	if strings.TrimSpace(patch.TenantLabel) != "" {
		out.TenantLabel = strings.TrimSpace(patch.TenantLabel)
	}
	if strings.TrimSpace(patch.TenantID) != "" {
		out.TenantID = strings.TrimSpace(patch.TenantID)
	}
	if strings.TrimSpace(patch.ClientID) != "" {
		out.ClientID = strings.TrimSpace(patch.ClientID)
	}
	if strings.TrimSpace(patch.ClientSecret) != "" && patch.ClientSecret != "********" {
		out.ClientSecret = strings.TrimSpace(patch.ClientSecret)
	}
	cfg := out.toGraphConfig()
	if GraphFieldsConfigured(out) {
		if err := cfg.Validate(); err != nil {
			return out, err
		}
	}
	return out, nil
}

func MergeIMAPFields(existing MailboxIMAPFields, patch MailboxIMAPFields, mailbox string) (MailboxIMAPFields, error) {
	out := existing
	if strings.TrimSpace(patch.Provider) != "" {
		out.Provider = strings.TrimSpace(patch.Provider)
	}
	if strings.TrimSpace(patch.Host) != "" {
		out.Host = strings.TrimSpace(patch.Host)
	}
	if patch.Port > 0 {
		out.Port = patch.Port
	}
	if strings.TrimSpace(patch.Username) != "" {
		out.Username = strings.TrimSpace(patch.Username)
	}
	if strings.TrimSpace(patch.Password) != "" && patch.Password != "********" {
		out.Password = strings.TrimSpace(patch.Password)
	}
	if strings.TrimSpace(patch.InboxFolder) != "" {
		out.InboxFolder = strings.TrimSpace(patch.InboxFolder)
	}
	if strings.TrimSpace(patch.SentFolder) != "" {
		out.SentFolder = strings.TrimSpace(patch.SentFolder)
	}
	out.UseTLS = patch.UseTLS || out.UseTLS
	cfg := out.toIMAPConfig(mailbox)
	if err := cfg.Resolve(mailbox); err != nil {
		return out, err
	}
	return out, nil
}

func graphClientForFields(ctx context.Context, pool *pgxpool.Pool, f MailboxGraphFields) (*graphmail.Client, error) {
	conn, err := ResolveGraphForMailbox(ctx, pool, f)
	if err != nil {
		return nil, err
	}
	return graphmail.NewClientWithConfig(graphmail.Config{
		Label:        conn.TenantLabel,
		TenantID:     conn.TenantID,
		ClientID:     conn.ClientID,
		ClientSecret: conn.ClientSecret,
	}), nil
}

func GraphFieldsConfigured(f MailboxGraphFields) bool {
	return strings.TrimSpace(f.TenantID) != "" &&
		strings.TrimSpace(f.ClientID) != "" &&
		strings.TrimSpace(f.ClientSecret) != ""
}

func IMAPPatchPresent(p MailboxIMAPFields) bool {
	return strings.TrimSpace(p.Provider) != "" ||
		strings.TrimSpace(p.Host) != "" ||
		strings.TrimSpace(p.Username) != "" ||
		(strings.TrimSpace(p.Password) != "" && p.Password != "********") ||
		p.Port > 0 ||
		strings.TrimSpace(p.InboxFolder) != "" ||
		strings.TrimSpace(p.SentFolder) != ""
}

func GraphPatchPresent(p MailboxGraphFields) bool {
	return strings.TrimSpace(p.TenantKey) != "" ||
		strings.TrimSpace(p.TenantLabel) != "" ||
		strings.TrimSpace(p.TenantID) != "" ||
		strings.TrimSpace(p.ClientID) != "" ||
		(strings.TrimSpace(p.ClientSecret) != "" && p.ClientSecret != "********")
}

func LoadMailboxIMAPFields(ctx context.Context, pool *pgxpool.Pool, inboxID string) (mailbox string, fields MailboxIMAPFields, err error) {
	err = pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT mailbox_address,
		       %s, %s, %s,
		       %s, %s,
		       %s, %s,
		       %s
		FROM email_svc.inbox_config
		WHERE inbox_id = $1::uuid
	`, emailjobs.SQLIMAPProvider, emailjobs.SQLIMAPHost, emailjobs.SQLIMAPPort,
		emailjobs.SQLIMAPUsername, emailjobs.SQLIMAPPassword,
		emailjobs.SQLIMAPInboxFolder, emailjobs.SQLIMAPSentFolder,
		emailjobs.SQLIMAPUseTLS), inboxID).Scan(
		&mailbox,
		&fields.Provider, &fields.Host, &fields.Port,
		&fields.Username, &fields.Password,
		&fields.InboxFolder, &fields.SentFolder, &fields.UseTLS,
	)
	return mailbox, fields, err
}

func LoadMailboxGraphFields(ctx context.Context, pool *pgxpool.Pool, inboxID string) (MailboxGraphFields, error) {
	var f MailboxGraphFields
	err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT %s, %s, %s, %s, %s
		FROM email_svc.inbox_config
		WHERE inbox_id = $1::uuid
	`, emailjobs.SQLGraphTenantKey, emailjobs.SQLGraphTenantLabel, emailjobs.SQLGraphTenantID,
		emailjobs.SQLGraphClientID, emailjobs.SQLGraphSecret), inboxID).Scan(
		&f.TenantKey, &f.TenantLabel, &f.TenantID, &f.ClientID, &f.ClientSecret)
	return f, err
}
