package emailjobs

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/internal/services/mailruntime"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GraphMailboxCreds are per-mailbox Graph columns from inbox_config.
type GraphMailboxCreds struct {
	TenantKey    string
	TenantLabel  string
	TenantID     string
	ClientID     string
	ClientSecret string
}

// GraphTenantRow is one row from email_svc.graph_tenant_config.
type GraphTenantRow struct {
	TenantKey    string
	TenantLabel  string
	TenantID     string
	ClientID     string
	ClientSecret string
}

func LoadGraphTenant(ctx context.Context, pool *pgxpool.Pool, tenantKey string) (GraphTenantRow, error) {
	key := strings.TrimSpace(tenantKey)
	if key == "" {
		key = "default"
	}
	var row GraphTenantRow
	err := pool.QueryRow(ctx, `
		SELECT tenant_key, COALESCE(tenant_label,''), COALESCE(tenant_id,''),
		       COALESCE(client_id,''), COALESCE(client_secret,'')
		FROM email_svc.graph_tenant_config
		WHERE tenant_key = $1 AND is_active = true
	`, key).Scan(&row.TenantKey, &row.TenantLabel, &row.TenantID, &row.ClientID, &row.ClientSecret)
	if err != nil {
		if key != "default" {
			return LoadGraphTenant(ctx, pool, "default")
		}
		return row, fmt.Errorf("graph tenant %q not found in email_svc.graph_tenant_config", key)
	}
	return row, nil
}

func LoadDefaultGraphTenant(ctx context.Context, pool *pgxpool.Pool) (GraphTenantRow, error) {
	var row GraphTenantRow
	err := pool.QueryRow(ctx, `
		SELECT tenant_key, COALESCE(tenant_label,''), COALESCE(tenant_id,''),
		       COALESCE(client_id,''), COALESCE(client_secret,'')
		FROM email_svc.graph_tenant_config
		WHERE is_default = true AND is_active = true
		ORDER BY updated_at DESC
		LIMIT 1
	`).Scan(&row.TenantKey, &row.TenantLabel, &row.TenantID, &row.ClientID, &row.ClientSecret)
	if err != nil {
		return row, fmt.Errorf("no default graph tenant in email_svc.graph_tenant_config")
	}
	return row, nil
}

func mergeGraphTenant(base GraphTenantRow, mailbox GraphMailboxCreds) mailruntime.GraphConnection {
	out := mailruntime.GraphConnection{
		TenantLabel:  base.TenantLabel,
		TenantID:     base.TenantID,
		ClientID:     base.ClientID,
		ClientSecret: base.ClientSecret,
	}
	if s := strings.TrimSpace(mailbox.TenantLabel); s != "" {
		out.TenantLabel = s
	}
	if s := strings.TrimSpace(mailbox.TenantID); s != "" {
		out.TenantID = s
	}
	if s := strings.TrimSpace(mailbox.ClientID); s != "" {
		out.ClientID = s
	}
	if s := strings.TrimSpace(mailbox.ClientSecret); s != "" && mailbox.ClientSecret != "********" {
		out.ClientSecret = s
	}
	return out
}

func graphConnectionComplete(c mailruntime.GraphConnection) bool {
	return strings.TrimSpace(c.TenantID) != "" &&
		strings.TrimSpace(c.ClientID) != "" &&
		strings.TrimSpace(c.ClientSecret) != ""
}

// ResolveGraphConnection loads tenant row from DB, merges mailbox overrides, returns payload for mail runtime.
func ResolveGraphConnection(ctx context.Context, pool *pgxpool.Pool, mailbox GraphMailboxCreds) (mailruntime.GraphConnection, error) {
	tenantKey := strings.TrimSpace(mailbox.TenantKey)
	var base GraphTenantRow
	var err error
	if tenantKey != "" {
		base, err = LoadGraphTenant(ctx, pool, tenantKey)
	} else if graphMailboxHasInlineCreds(mailbox) {
		return mergeGraphTenant(GraphTenantRow{}, mailbox), nil
	} else {
		base, err = LoadDefaultGraphTenant(ctx, pool)
	}
	if err != nil {
		return mailruntime.GraphConnection{}, err
	}
	conn := mergeGraphTenant(base, mailbox)
	if !graphConnectionComplete(conn) {
		return conn, fmt.Errorf("graph credentials incomplete for tenant_key=%q", tenantKey)
	}
	return conn, nil
}

func graphMailboxHasInlineCreds(m GraphMailboxCreds) bool {
	return strings.TrimSpace(m.TenantID) != "" &&
		strings.TrimSpace(m.ClientID) != "" &&
		strings.TrimSpace(m.ClientSecret) != "" &&
		m.ClientSecret != "********"
}
