package emailjobs

// SQL expressions for typed credential columns on email_svc.inbox_config.
const (
	SQLCoalesceGraphTenantLabel = `COALESCE(i.graph_tenant_label,'')`
	SQLCoalesceGraphTenantID    = `COALESCE(i.graph_tenant_id,'')`
	SQLCoalesceGraphClientID    = `COALESCE(i.graph_client_id,'')`
	SQLCoalesceGraphSecret      = `COALESCE(i.graph_client_secret,'')`
	SQLCoalesceGraphTenantKey   = `COALESCE(i.graph_tenant_key,'')`

	SQLCoalesceIMAPProvider    = `COALESCE(i.imap_provider,'')`
	SQLCoalesceIMAPHost        = `COALESCE(i.imap_host,'')`
	SQLCoalesceIMAPPort        = `COALESCE(i.imap_port,993)`
	SQLCoalesceIMAPUsername    = `COALESCE(i.imap_username,'')`
	SQLCoalesceIMAPPassword    = `COALESCE(i.imap_password,'')`
	SQLCoalesceIMAPInboxFolder = `COALESCE(i.imap_inbox_folder,'INBOX')`
	SQLCoalesceIMAPSentFolder  = `COALESCE(i.imap_sent_folder,'')`
	SQLCoalesceIMAPUseTLS      = `COALESCE(i.imap_use_tls,true)`

	SQLCoalesceGoogleWorkspaceTenantKey           = `COALESCE(i.google_workspace_tenant_key,'')`
	SQLCoalesceGoogleWorkspaceTenantLabel         = `COALESCE(i.google_workspace_tenant_label,'')`
	SQLCoalesceGoogleWorkspaceServiceAccountEmail = `COALESCE(i.google_workspace_service_account_email,'')`
	SQLCoalesceGoogleWorkspaceClientID            = `COALESCE(i.google_workspace_client_id,'')`
	SQLCoalesceGoogleWorkspacePrivateKey          = `COALESCE(i.google_workspace_private_key,'')`
)

// Unqualified variants (no table alias) for single-table queries.
const (
	SQLGraphTenantLabel = `COALESCE(graph_tenant_label,'')`
	SQLGraphTenantID    = `COALESCE(graph_tenant_id,'')`
	SQLGraphClientID    = `COALESCE(graph_client_id,'')`
	SQLGraphSecret      = `COALESCE(graph_client_secret,'')`
	SQLGraphTenantKey   = `COALESCE(graph_tenant_key,'')`

	SQLIMAPProvider    = `COALESCE(imap_provider,'')`
	SQLIMAPHost        = `COALESCE(imap_host,'')`
	SQLIMAPPort        = `COALESCE(imap_port,993)`
	SQLIMAPUsername    = `COALESCE(imap_username,'')`
	SQLIMAPPassword    = `COALESCE(imap_password,'')`
	SQLIMAPInboxFolder = `COALESCE(imap_inbox_folder,'INBOX')`
	SQLIMAPSentFolder  = `COALESCE(imap_sent_folder,'')`
	SQLIMAPUseTLS      = `COALESCE(imap_use_tls,true)`

	SQLOAuthProvider      = `COALESCE(oauth_provider,'')`
	SQLOAuthMailTransport = `COALESCE(oauth_mail_transport,'api')`
	SQLOAuthRefreshToken  = `COALESCE(oauth_refresh_token,'')`
	SQLOAuthAccessToken   = `COALESCE(oauth_access_token,'')`
	SQLOAuthScopes        = `COALESCE(oauth_scopes,'')`

	SQLGoogleWorkspaceTenantKey           = `COALESCE(google_workspace_tenant_key,'')`
	SQLGoogleWorkspaceTenantLabel         = `COALESCE(google_workspace_tenant_label,'')`
	SQLGoogleWorkspaceServiceAccountEmail = `COALESCE(google_workspace_service_account_email,'')`
	SQLGoogleWorkspaceClientID            = `COALESCE(google_workspace_client_id,'')`
	SQLGoogleWorkspacePrivateKey          = `COALESCE(google_workspace_private_key,'')`
)
