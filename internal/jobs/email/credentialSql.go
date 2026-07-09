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
)
