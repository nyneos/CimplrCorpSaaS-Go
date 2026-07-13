package emailjobs

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GoogleWorkspaceTenantRow is one row from email_svc.google_workspace_tenant_config.
type GoogleWorkspaceTenantRow struct {
	TenantKey           string
	TenantLabel         string
	ServiceAccountEmail string
	ClientID            string
	PrivateKey          string
	DefaultDomain       string
}

// GoogleWorkspaceMailboxCreds are per-mailbox Google DWD columns from inbox_config.
type GoogleWorkspaceMailboxCreds struct {
	TenantKey           string
	TenantLabel         string
	ServiceAccountEmail string
	ClientID            string
	PrivateKey          string
}

func LoadGoogleWorkspaceTenant(ctx context.Context, pool *pgxpool.Pool, tenantKey string) (GoogleWorkspaceTenantRow, error) {
	key := strings.TrimSpace(tenantKey)
	if key == "" {
		key = "default"
	}
	var row GoogleWorkspaceTenantRow
	err := pool.QueryRow(ctx, `
		SELECT tenant_key, COALESCE(tenant_label,''), COALESCE(service_account_email,''),
		       COALESCE(client_id,''), COALESCE(private_key,''), COALESCE(default_domain,'')
		FROM email_svc.google_workspace_tenant_config
		WHERE tenant_key = $1 AND is_active = true
	`, key).Scan(&row.TenantKey, &row.TenantLabel, &row.ServiceAccountEmail, &row.ClientID, &row.PrivateKey, &row.DefaultDomain)
	if err != nil {
		if key != "default" {
			return LoadGoogleWorkspaceTenant(ctx, pool, "default")
		}
		return row, fmt.Errorf("google workspace tenant %q not found", key)
	}
	return row, nil
}

func LoadDefaultGoogleWorkspaceTenant(ctx context.Context, pool *pgxpool.Pool) (GoogleWorkspaceTenantRow, error) {
	var row GoogleWorkspaceTenantRow
	err := pool.QueryRow(ctx, `
		SELECT tenant_key, COALESCE(tenant_label,''), COALESCE(service_account_email,''),
		       COALESCE(client_id,''), COALESCE(private_key,''), COALESCE(default_domain,'')
		FROM email_svc.google_workspace_tenant_config
		WHERE is_default = true AND is_active = true
		ORDER BY updated_at DESC
		LIMIT 1
	`).Scan(&row.TenantKey, &row.TenantLabel, &row.ServiceAccountEmail, &row.ClientID, &row.PrivateKey, &row.DefaultDomain)
	if err != nil {
		return row, fmt.Errorf("no default google workspace tenant configured")
	}
	return row, nil
}

func mergeGoogleWorkspaceTenant(base GoogleWorkspaceTenantRow, mailbox GoogleWorkspaceMailboxCreds) mailruntime.GmailDWDConnection {
	out := mailruntime.GmailDWDConnection{
		TenantLabel:         base.TenantLabel,
		ServiceAccountEmail: base.ServiceAccountEmail,
		ClientID:            base.ClientID,
		PrivateKey:          base.PrivateKey,
	}
	if s := strings.TrimSpace(mailbox.TenantLabel); s != "" {
		out.TenantLabel = s
	}
	if s := strings.TrimSpace(mailbox.ServiceAccountEmail); s != "" {
		out.ServiceAccountEmail = s
	}
	if s := strings.TrimSpace(mailbox.ClientID); s != "" {
		out.ClientID = s
	}
	if s := strings.TrimSpace(mailbox.PrivateKey); s != "" && mailbox.PrivateKey != "********" {
		out.PrivateKey = s
	}
	return out
}

func googleWorkspaceConnectionComplete(c mailruntime.GmailDWDConnection) bool {
	return strings.TrimSpace(c.ServiceAccountEmail) != "" && strings.TrimSpace(c.PrivateKey) != ""
}

func googleWorkspaceMailboxHasInlineCreds(m GoogleWorkspaceMailboxCreds) bool {
	return strings.TrimSpace(m.ServiceAccountEmail) != "" &&
		strings.TrimSpace(m.PrivateKey) != "" &&
		m.PrivateKey != "********"
}

// ResolveGoogleWorkspaceConnection loads tenant row from DB, merges mailbox overrides.
func ResolveGoogleWorkspaceConnection(ctx context.Context, pool *pgxpool.Pool, mailbox GoogleWorkspaceMailboxCreds) (mailruntime.GmailDWDConnection, error) {
	tenantKey := strings.TrimSpace(mailbox.TenantKey)
	var base GoogleWorkspaceTenantRow
	var err error
	if tenantKey != "" {
		base, err = LoadGoogleWorkspaceTenant(ctx, pool, tenantKey)
	} else if googleWorkspaceMailboxHasInlineCreds(mailbox) {
		return mergeGoogleWorkspaceTenant(GoogleWorkspaceTenantRow{}, mailbox), nil
	} else {
		base, err = LoadDefaultGoogleWorkspaceTenant(ctx, pool)
	}
	if err != nil {
		return mailruntime.GmailDWDConnection{}, err
	}
	conn := mergeGoogleWorkspaceTenant(base, mailbox)
	if !googleWorkspaceConnectionComplete(conn) {
		return conn, fmt.Errorf("google workspace credentials incomplete for tenant_key=%q", tenantKey)
	}
	return conn, nil
}
