// Package mailruntime is the in-process mail engine used by api/email and
// internal/jobs/email. It used to relay every call over HTTP to a standalone
// CIMPLR-Email-Service process; that service's code now lives locally under
// internal/mailengine (plus internal/services/{imapmail,graphmail}), and
// Runtime's methods call straight into it. The exported type/method surface
// below is unchanged on purpose so no caller in api/email or
// internal/jobs/email had to change.
package mailruntime

import (
	"encoding/json"
)

// Runtime is the mail engine handle. It carries no network config anymore —
// kept as a struct (rather than package-level functions) so call sites did
// not need to change when the HTTP relay was removed.
type Runtime struct{}

func NewRuntime() *Runtime {
	return &Runtime{}
}

// Ready reports whether the mail engine can be used. There is no longer a
// separate service/token to misconfigure, so this is always true; HealthCheck
// is the meaningful readiness signal (it verifies S3/AWS config resolves).
func (r *Runtime) Ready() bool {
	return true
}

type ParsedEmail = ParsedMessage

type ParsedMessage struct {
	MessageID   string `json:"message_id"`
	S3RawKey    string `json:"s3_raw_key"`
	S3ParsedKey string `json:"s3_parsed_key"`
	Envelope    struct {
		From            string   `json:"from"`
		To              []string `json:"to"`
		Cc              []string `json:"cc"`
		Subject         string   `json:"subject"`
		Date            string   `json:"date"`
		MessageIDHeader string   `json:"message_id_header"`
	} `json:"envelope"`
	Body struct {
		TextPlain string `json:"text_plain"`
		TextHTML  string `json:"text_html"`
		Preferred string `json:"preferred"`
	} `json:"body"`
	Attachments []struct {
		Filename    string `json:"filename"`
		ContentType string `json:"content_type"`
		SizeBytes   int64  `json:"size_bytes"`
		S3Key       string `json:"s3_key"`
		SHA256      string `json:"sha256"`
	} `json:"attachments"`
	Status string `json:"status"`
}

type BatchDecodeResult struct {
	Results []ParsedMessage `json:"results"`
	Errors  []string        `json:"errors"`
}

type PendingKeysResult struct {
	Prefix string   `json:"prefix"`
	Keys   []string `json:"keys"`
}

type InboundRuleSyncResult struct {
	RuleSetName string   `json:"rule_set_name"`
	Synced      int      `json:"synced"`
	Removed     int      `json:"removed"`
	Errors      []string `json:"errors,omitempty"`
}

type InboundRuleSpec struct {
	RuleName  string `json:"rule_name"`
	Recipient string `json:"recipient"`
}

type StructuredExtractResult struct {
	Intent            string                 `json:"intent"`
	ExtractedMetadata map[string]interface{} `json:"extracted_metadata"`
	Confidence        float64                `json:"confidence"`
}

type IMAPConnection struct {
	Provider    string `json:"provider"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	AuthMode    string `json:"auth_mode,omitempty"`
	AccessToken string `json:"access_token,omitempty"`
	InboxFolder string `json:"inbox_folder"`
	SentFolder  string `json:"sent_folder"`
	UseTLS      bool   `json:"use_tls"`
}

type GraphConnection struct {
	TenantLabel  string `json:"tenant_label"`
	TenantID     string `json:"tenant_id"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

type GmailDWDConnection struct {
	TenantLabel         string `json:"tenant_label"`
	ServiceAccountEmail string `json:"service_account_email"`
	ClientID            string `json:"client_id"`
	PrivateKey          string `json:"private_key"`
}

type IMAPPulledMessage struct {
	UID            uint32        `json:"uid"`
	IMAPMessageKey string        `json:"imap_message_key"`
	Parsed         ParsedMessage `json:"parsed"`
}

type IMAPPullResult struct {
	Initialized bool                `json:"initialized"`
	NewLastUID  uint32              `json:"new_last_uid"`
	Messages    []IMAPPulledMessage `json:"messages"`
}

type GraphPulledMessage struct {
	GraphMessageID string        `json:"graph_message_id"`
	CursorTime     string        `json:"cursor_time"`
	Parsed         ParsedMessage `json:"parsed"`
}

type GraphPullResult struct {
	Initialized bool                 `json:"initialized"`
	NewSince    string               `json:"new_since"`
	Fetched     int                  `json:"fetched"`
	Messages    []GraphPulledMessage `json:"messages"`
}

type OAuthConnection struct {
	Provider     string `json:"provider"`
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

type OAuthExchangeResult struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
	Email        string `json:"email"`
}

type OAuthRefreshResult struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
}

type OAuthPulledMessage struct {
	ProviderMessageID string        `json:"provider_message_id"`
	CursorTime        string        `json:"cursor_time"`
	Parsed            ParsedMessage `json:"parsed"`
}

type OAuthPullResult struct {
	Initialized bool                 `json:"initialized"`
	NewSince    string               `json:"new_since"`
	Fetched     int                  `json:"fetched"`
	Messages    []OAuthPulledMessage `json:"messages"`
}

type IMAPPullRequest struct {
	InboxID             string
	Mailbox             string
	Folder              string
	Direction           string
	LastUID             uint32
	PageSize            int
	Conn                IMAPConnection
	SkipIMAPMessageKeys []string
	FiltersJSON         json.RawMessage
}

// GraphPullRequest groups the parameters for PullGraphMessages.
type GraphPullRequest struct {
	InboxID        string
	Mailbox        string
	SentFolder     bool
	Since          string
	PageSize       int
	Conn           GraphConnection
	SkipMessageIDs []string
	FiltersJSON    []byte
}

// GmailDWDPullRequest groups the parameters for PullGmailDWDMessages.
type GmailDWDPullRequest struct {
	InboxID        string
	Mailbox        string
	SentFolder     bool
	Since          string
	PageSize       int
	Conn           GmailDWDConnection
	SkipMessageIDs []string
	FiltersJSON    []byte
}

type OAuthPullRequest struct {
	InboxID        string
	Mailbox        string
	Provider       string
	SentFolder     bool
	Since          string
	PageSize       int
	Conn           OAuthConnection
	SkipMessageIDs []string
	FiltersJSON    json.RawMessage
}

// StoragePutRequest mirrors internal/mailengine/storage.Put's request shape.
type StoragePutRequest struct {
	ContentBase64    string `json:"content_base64"`
	ContentType      string `json:"content_type,omitempty"`
	FileExt          string `json:"file_ext,omitempty"`
	DestinationType  string `json:"destination_type"`
	OutputNamePrefix string `json:"output_name_prefix,omitempty"`
	AppendDatetime   bool   `json:"append_datetime"`
	S3Prefix         string `json:"s3_prefix,omitempty"`
	LocalFolder      string `json:"local_folder,omitempty"`
	SftpHost         string `json:"sftp_host,omitempty"`
	SftpPort         int    `json:"sftp_port,omitempty"`
	SftpUser         string `json:"sftp_user,omitempty"`
	SftpPassword     string `json:"sftp_password,omitempty"`
	SftpFolder       string `json:"sftp_folder,omitempty"`
	APIURL           string `json:"api_url,omitempty"`
	APIAuthToken     string `json:"api_auth_token,omitempty"`
}

// StoragePutResult is the data payload of a storage put.
type StoragePutResult struct {
	DestinationType string `json:"destination_type"`
	OutputFilename  string `json:"output_filename"`
	OutputLocation  string `json:"output_location"`
	S3Key           string `json:"s3_key,omitempty"`
}

// ReadAPIInboxRequest fetches a file saved by the demo test-receive endpoints.
type ReadAPIInboxRequest struct {
	Filename string `json:"filename"`
	Folder   string `json:"folder"` // api-inbox | api-inbox-2
}

// ReadAPIInboxResult is the data payload of a read-api-inbox call.
type ReadAPIInboxResult struct {
	Filename      string `json:"filename"`
	Folder        string `json:"folder"`
	Path          string `json:"path"`
	ContentBase64 string `json:"content_base64"`
	ByteSize      int    `json:"byte_size"`
}
