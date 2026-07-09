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
