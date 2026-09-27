package model

import "encoding/json"

// OAuthAuthorizeURLRequest builds a browser consent URL for a mail provider.
type OAuthAuthorizeURLRequest struct {
	ServiceKey  string `json:"service_key"`
	Provider    string `json:"provider"`  // microsoft | google
	Transport   string `json:"transport"` // api | imap
	RedirectURI string `json:"redirect_uri"`
	State       string `json:"state"`
}

// OAuthExchangeRequest trades an authorization code for tokens.
type OAuthExchangeRequest struct {
	ServiceKey  string `json:"service_key"`
	Provider    string `json:"provider"`
	Transport   string `json:"transport"`
	Code        string `json:"code"`
	RedirectURI string `json:"redirect_uri"`
}

// OAuthExchangeResponse returns tokens plus the resolved mailbox identity.
type OAuthExchangeResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
	Email        string `json:"email"`
}

// OAuthRefreshRequest obtains a fresh access token.
type OAuthRefreshRequest struct {
	ServiceKey   string `json:"service_key"`
	Provider     string `json:"provider"`
	Transport    string `json:"transport"`
	RefreshToken string `json:"refresh_token"`
}

// OAuthTestRequest verifies a delegated access token.
type OAuthTestRequest struct {
	ServiceKey  string `json:"service_key"`
	Provider    string `json:"provider"`
	AccessToken string `json:"access_token"`
}

// OAuthPollPageRequest fetches one page of mailbox messages with a delegated token.
type OAuthPollPageRequest struct {
	ServiceKey     string          `json:"service_key"`
	Provider       string          `json:"provider"`
	Transport      string          `json:"transport"` // api | imap
	MailboxAddress string          `json:"mailbox_address"`
	InboxID        string          `json:"inbox_id"`
	SentFolder     bool            `json:"sent_folder"`
	Since          string          `json:"since"` // RFC3339 / RFC3339Nano
	Batch          int             `json:"batch"`
	SkipMessageIDs []string        `json:"skip_message_ids,omitempty"`
	FiltersJSON    json.RawMessage `json:"filters_json,omitempty"`
	AccessToken    string          `json:"access_token"`
	IMAP           IMAPConfig      `json:"imap"`
}

// OAuthPollPageResponse mirrors GraphPollPageResponse for OAuth mailboxes.
type OAuthPollPageResponse struct {
	Initialized bool                 `json:"initialized"`
	NewSince    string               `json:"new_since"`
	Fetched     int                  `json:"fetched"`
	Messages    []OAuthPolledMessage `json:"messages"`
}

// OAuthPolledMessage is one fetched + parsed message.
type OAuthPolledMessage struct {
	ProviderMessageID string      `json:"provider_message_id"`
	CursorTime        string      `json:"cursor_time"`
	Parsed            ParsedEmail `json:"parsed"`
}
