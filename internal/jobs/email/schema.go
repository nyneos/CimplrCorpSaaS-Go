package emailjobs

import (
	"context"
	"os"
	"strings"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsureInboxCredentialSchema adds typed credential columns (idempotent).
func EnsureInboxCredentialSchema(ctx context.Context, pool *pgxpool.Pool) {
	const ddl = `
ALTER TABLE email_svc.inbox_config
    ADD COLUMN IF NOT EXISTS graph_tenant_label TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS graph_tenant_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS graph_client_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS graph_client_secret TEXT NOT NULL DEFAULT '';

ALTER TABLE email_svc.inbox_config
    ADD COLUMN IF NOT EXISTS imap_provider TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS imap_host TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS imap_port INT NOT NULL DEFAULT 993,
    ADD COLUMN IF NOT EXISTS imap_username TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS imap_password TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS imap_inbox_folder TEXT NOT NULL DEFAULT 'INBOX',
    ADD COLUMN IF NOT EXISTS imap_sent_folder TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS imap_use_tls BOOLEAN NOT NULL DEFAULT true;
`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		logger.LogError("[email-schema] credential columns: %v", err)
		return
	}

	backfillFromLegacyJSONB(ctx, pool)
	ensureGraphTenantSchema(ctx, pool)
	ensureGoogleWorkspaceTenantSchema(ctx, pool)
	ensureOAuthSchema(ctx, pool)
	ensureInboxSoftDeleteIndex(ctx, pool)
	backfillIMAPSentFolders(ctx, pool)
}

func ensureInboxSoftDeleteIndex(ctx context.Context, pool *pgxpool.Pool) {
	const ddl = `
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'inbox_config_mailbox_address_key'
      AND conrelid = 'email_svc.inbox_config'::regclass
  ) THEN
    ALTER TABLE email_svc.inbox_config DROP CONSTRAINT inbox_config_mailbox_address_key;
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_inbox_mailbox'
      AND conrelid = 'email_svc.inbox_config'::regclass
  ) THEN
    ALTER TABLE email_svc.inbox_config DROP CONSTRAINT uq_inbox_mailbox;
  END IF;
EXCEPTION WHEN undefined_table THEN
  NULL;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS inbox_config_mailbox_address_active_idx
  ON email_svc.inbox_config (LOWER(mailbox_address))
  WHERE COALESCE(is_deleted, false) = false;
`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		logger.LogError("[email-schema] inbox soft-delete index: %v", err)
	}
}

func backfillIMAPSentFolders(ctx context.Context, pool *pgxpool.Pool) {
	const ddl = `
UPDATE email_svc.inbox_config
SET imap_inbox_folder = CASE
      WHEN imap_inbox_folder = '' OR imap_inbox_folder IS NULL THEN 'INBOX'
      ELSE imap_inbox_folder END,
    imap_sent_folder = CASE
      WHEN imap_sent_folder <> '' AND imap_sent_folder IS NOT NULL THEN imap_sent_folder
      WHEN imap_provider IN ('gmail_personal', 'google_workspace') THEN '[Gmail]/Sent Mail'
      WHEN imap_provider = 'yahoo' THEN 'Sent'
      WHEN imap_provider = 'outlook_imap' THEN 'Sent Items'
      WHEN imap_provider = 'zoho' THEN 'Sent'
      WHEN imap_provider = 'icloud' THEN 'Sent Messages'
      WHEN imap_provider = 'aol' THEN 'Sent'
      WHEN imap_provider = 'generic' THEN 'Sent'
      ELSE imap_sent_folder END,
    updated_at = now()
WHERE COALESCE(source_type, '') = 'IMAP'
  AND processing_status = 'APPROVED'
  AND is_deleted = false
  AND (COALESCE(imap_sent_folder, '') = '' OR COALESCE(imap_inbox_folder, '') = '');
`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		logger.LogError("[email-schema] imap sent folder backfill: %v", err)
	}
}

func ensureOAuthSchema(ctx context.Context, pool *pgxpool.Pool) {
	const ddl = `
ALTER TABLE email_svc.inbox_config
    ADD COLUMN IF NOT EXISTS oauth_provider TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS oauth_mail_transport TEXT NOT NULL DEFAULT 'api',
    ADD COLUMN IF NOT EXISTS oauth_refresh_token TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS oauth_access_token TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS oauth_token_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS oauth_connected_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS oauth_scopes TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS email_svc.oauth_connect_state (
    state          TEXT PRIMARY KEY,
    provider       TEXT NOT NULL,
    transport      TEXT NOT NULL DEFAULT 'api',
    user_id        TEXT NOT NULL DEFAULT '',
    mailbox_hint   TEXT NOT NULL DEFAULT '',
    redirect_uri   TEXT NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS oauth_connect_state_expires_idx
    ON email_svc.oauth_connect_state (expires_at);
ALTER TABLE email_svc.oauth_connect_state
    ADD COLUMN IF NOT EXISTS transport TEXT NOT NULL DEFAULT 'api';
`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		logger.LogError("[email-schema] oauth columns: %v", err)
	}
}

func ensureGraphTenantSchema(ctx context.Context, pool *pgxpool.Pool) {
	const ddl = `
CREATE TABLE IF NOT EXISTS email_svc.graph_tenant_config (
    tenant_key     TEXT PRIMARY KEY,
    tenant_label   TEXT NOT NULL DEFAULT '',
    tenant_id      TEXT NOT NULL DEFAULT '',
    client_id      TEXT NOT NULL DEFAULT '',
    client_secret  TEXT NOT NULL DEFAULT '',
    is_default     BOOLEAN NOT NULL DEFAULT false,
    is_active      BOOLEAN NOT NULL DEFAULT true,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS graph_tenant_config_one_default_idx
    ON email_svc.graph_tenant_config (is_default) WHERE is_default = true;
ALTER TABLE email_svc.inbox_config
    ADD COLUMN IF NOT EXISTS graph_tenant_key TEXT NOT NULL DEFAULT '';
`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		logger.LogError("[email-schema] graph_tenant_config: %v", err)
		return
	}
	seedDefaultGraphTenantIfEmpty(ctx, pool)
}

func seedDefaultGraphTenantIfEmpty(ctx context.Context, pool *pgxpool.Pool) {
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.graph_tenant_config WHERE is_default = true AND is_active = true)
	`).Scan(&exists); err != nil || exists {
		return
	}
	tenantID := strings.TrimSpace(os.Getenv("AZURE_TENANT_ID"))
	clientID := strings.TrimSpace(os.Getenv("AZURE_CLIENT_ID"))
	clientSecret := strings.TrimSpace(os.Getenv("AZURE_CLIENT_SECRET"))
	if tenantID == "" || clientID == "" || clientSecret == "" {
		return
	}
	label := strings.TrimSpace(os.Getenv("AZURE_GRAPH_TENANT_LABEL"))
	if label == "" {
		label = "default"
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO email_svc.graph_tenant_config (tenant_key, tenant_label, tenant_id, client_id, client_secret, is_default)
		VALUES ('default', $1, $2, $3, $4, true)
		ON CONFLICT (tenant_key) DO UPDATE SET
		    tenant_label = EXCLUDED.tenant_label,
		    tenant_id = EXCLUDED.tenant_id,
		    client_id = EXCLUDED.client_id,
		    client_secret = EXCLUDED.client_secret,
		    is_default = true,
		    updated_at = now()
	`, label, tenantID, clientID, clientSecret)
	if err != nil {
		logger.LogError("[email-schema] seed default graph tenant: %v", err)
		return
	}
	logger.LogInfo("[email-schema] seeded default graph tenant from AZURE_* env")
}

func ensureGoogleWorkspaceTenantSchema(ctx context.Context, pool *pgxpool.Pool) {
	const ddl = `
CREATE TABLE IF NOT EXISTS email_svc.google_workspace_tenant_config (
    tenant_key              TEXT PRIMARY KEY,
    tenant_label            TEXT NOT NULL DEFAULT '',
    service_account_email   TEXT NOT NULL DEFAULT '',
    client_id               TEXT NOT NULL DEFAULT '',
    private_key             TEXT NOT NULL DEFAULT '',
    default_domain          TEXT NOT NULL DEFAULT '',
    is_default              BOOLEAN NOT NULL DEFAULT false,
    is_active               BOOLEAN NOT NULL DEFAULT true,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS google_workspace_tenant_one_default_idx
    ON email_svc.google_workspace_tenant_config (is_default) WHERE is_default = true;
ALTER TABLE email_svc.inbox_config
    ADD COLUMN IF NOT EXISTS google_workspace_tenant_key TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS google_workspace_tenant_label TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS google_workspace_service_account_email TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS google_workspace_client_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS google_workspace_private_key TEXT NOT NULL DEFAULT '';
`
	if _, err := pool.Exec(ctx, ddl); err != nil {
		logger.LogError("[email-schema] google_workspace_tenant_config: %v", err)
		return
	}
	seedDefaultGoogleWorkspaceTenantIfEmpty(ctx, pool)
}

func seedDefaultGoogleWorkspaceTenantIfEmpty(ctx context.Context, pool *pgxpool.Pool) {
	var exists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM email_svc.google_workspace_tenant_config WHERE is_default = true AND is_active = true)
	`).Scan(&exists); err != nil || exists {
		return
	}
	saEmail := strings.TrimSpace(os.Getenv("GOOGLE_DWD_SERVICE_ACCOUNT_EMAIL"))
	privateKey := strings.TrimSpace(os.Getenv("GOOGLE_DWD_PRIVATE_KEY"))
	clientID := strings.TrimSpace(os.Getenv("GOOGLE_DWD_CLIENT_ID"))
	if saEmail == "" || privateKey == "" {
		return
	}
	label := strings.TrimSpace(os.Getenv("GOOGLE_WORKSPACE_TENANT_LABEL"))
	if label == "" {
		label = "default"
	}
	defaultDomain := ""
	if parts := AllowedMailboxDomainsFromEnv(); len(parts) > 0 {
		defaultDomain = parts[0]
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO email_svc.google_workspace_tenant_config
		    (tenant_key, tenant_label, service_account_email, client_id, private_key, default_domain, is_default)
		VALUES ('default', $1, $2, $3, $4, $5, true)
		ON CONFLICT (tenant_key) DO UPDATE SET
		    tenant_label = EXCLUDED.tenant_label,
		    service_account_email = EXCLUDED.service_account_email,
		    client_id = EXCLUDED.client_id,
		    private_key = EXCLUDED.private_key,
		    default_domain = EXCLUDED.default_domain,
		    is_default = true,
		    updated_at = now()
	`, label, saEmail, clientID, privateKey, defaultDomain)
	if err != nil {
		logger.LogError("[email-schema] seed default google workspace tenant: %v", err)
		return
	}
	logger.LogInfo("[email-schema] seeded default google workspace tenant from GOOGLE_DWD_* env")
}

func AllowedMailboxDomainsFromEnv() []string {
	raw := strings.TrimSpace(os.Getenv("EMAIL_ALLOWED_MAILBOX_DOMAINS"))
	if raw == "" {
		raw = "nyneos.com"
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.ToLower(strings.TrimSpace(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func backfillFromLegacyJSONB(ctx context.Context, pool *pgxpool.Pool) {
	var hasIMAPJSON, hasGraphJSON bool
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'email_svc' AND table_name = 'inbox_config' AND column_name = 'imap_config_json'
		),
		EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'email_svc' AND table_name = 'inbox_config' AND column_name = 'graph_config_json'
		)
	`).Scan(&hasIMAPJSON, &hasGraphJSON)

	if hasGraphJSON {
		if _, err := pool.Exec(ctx, `
			UPDATE email_svc.inbox_config
			SET graph_tenant_label = COALESCE(NULLIF(graph_tenant_label, ''), graph_config_json->>'label', ''),
			    graph_tenant_id = COALESCE(NULLIF(graph_tenant_id, ''), graph_config_json->>'tenant_id', ''),
			    graph_client_id = COALESCE(NULLIF(graph_client_id, ''), graph_config_json->>'client_id', ''),
			    graph_client_secret = COALESCE(NULLIF(graph_client_secret, ''), graph_config_json->>'client_secret', graph_config_json->>'password', '')
			WHERE graph_config_json IS NOT NULL
			  AND graph_config_json <> '{}'::jsonb
			  AND (graph_tenant_label = '' OR graph_tenant_id = '' OR graph_client_id = '' OR graph_client_secret = '')
		`); err != nil {
			logger.LogError("[email-schema] graph jsonb backfill: %v", err)
		}
	}

	if hasIMAPJSON {
		if _, err := pool.Exec(ctx, `
			UPDATE email_svc.inbox_config
			SET imap_provider = COALESCE(NULLIF(imap_provider, ''), imap_config_json->>'provider', ''),
			    imap_host = COALESCE(NULLIF(imap_host, ''), imap_config_json->>'host', ''),
			    imap_port = COALESCE(NULLIF(imap_port, 0), NULLIF(imap_config_json->>'port', '')::int, 993),
			    imap_username = COALESCE(NULLIF(imap_username, ''), imap_config_json->>'username', ''),
			    imap_password = COALESCE(NULLIF(imap_password, ''), imap_config_json->>'password', ''),
			    imap_inbox_folder = COALESCE(NULLIF(imap_inbox_folder, ''), NULLIF(imap_config_json->>'inbox_folder', ''), 'INBOX'),
			    imap_sent_folder = COALESCE(NULLIF(imap_sent_folder, ''), imap_config_json->>'sent_folder', ''),
			    imap_use_tls = COALESCE(
			        CASE WHEN imap_use_tls IS FALSE THEN NULL ELSE imap_use_tls END,
			        (imap_config_json->>'use_tls')::boolean,
			        true
			    )
			WHERE imap_config_json IS NOT NULL
			  AND imap_config_json <> '{}'::jsonb
			  AND (imap_provider = '' OR imap_username = '' OR imap_password = '')
		`); err != nil {
			logger.LogError("[email-schema] imap jsonb backfill: %v", err)
		}
	}
}
