package graphmail

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const graphBase = "https://graph.microsoft.com/v1.0"

// Message is a lightweight Graph mail message reference.
type Message struct {
	ID                string    `json:"id"`
	Subject           string    `json:"subject"`
	ReceivedDateTime  time.Time `json:"receivedDateTime"`
	SentDateTime      time.Time `json:"sentDateTime"`
	InternetMessageID string    `json:"internetMessageId"`
	HasAttachments    bool      `json:"hasAttachments"`
	From              struct {
		EmailAddress struct {
			Address string `json:"address"`
		} `json:"emailAddress"`
	} `json:"from"`
	ToRecipients []struct {
		EmailAddress struct {
			Address string `json:"address"`
		} `json:"emailAddress"`
	} `json:"toRecipients"`
}

// ToAddresses returns lowercased To recipient addresses from Graph metadata.
func (m Message) ToAddresses() []string {
	out := make([]string, 0, len(m.ToRecipients))
	for _, r := range m.ToRecipients {
		if a := strings.TrimSpace(r.EmailAddress.Address); a != "" {
			out = append(out, strings.ToLower(a))
		}
	}
	return out
}

// CursorTime returns the timestamp used for sync cursor advancement.
func (m Message) CursorTime(sentFolder bool) time.Time {
	if sentFolder && !m.SentDateTime.IsZero() {
		return m.SentDateTime
	}
	return m.ReceivedDateTime
}

type listResponse struct {
	Value []Message `json:"value"`
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
	Error       string `json:"error"`
	ErrorDesc   string `json:"error_description"`
}

// Client calls Microsoft Graph with app-only (client credentials) auth.
type Client struct {
	tenantID     string
	clientID     string
	clientSecret string
	http         *http.Client

	mu        sync.Mutex
	token     string
	tokenExp  time.Time
}

// NewClient builds a Graph client from GRAPH_* or AZURE_* env vars.
func NewClient() *Client {
	return NewClientWithConfig(Config{})
}

// DefaultConfigFromEnv reads shared server credentials from environment.
func DefaultConfigFromEnv() Config {
	tenant := strings.TrimSpace(os.Getenv("GRAPH_TENANT_ID"))
	if tenant == "" {
		tenant = strings.TrimSpace(os.Getenv("AZURE_TENANT_ID"))
	}
	clientID := strings.TrimSpace(os.Getenv("GRAPH_CLIENT_ID"))
	if clientID == "" {
		clientID = strings.TrimSpace(os.Getenv("AZURE_CLIENT_ID"))
	}
	secret := strings.TrimSpace(os.Getenv("GRAPH_CLIENT_SECRET"))
	if secret == "" {
		secret = strings.TrimSpace(os.Getenv("AZURE_CLIENT_SECRET"))
	}
	return Config{TenantID: tenant, ClientID: clientID, ClientSecret: secret}
}

// NewClientWithConfig builds a client using mailbox config merged with env defaults.
func NewClientWithConfig(cfg Config) *Client {
	resolved := (&cfg).Resolve(DefaultConfigFromEnv())
	return &Client{
		tenantID:     resolved.TenantID,
		clientID:     resolved.ClientID,
		clientSecret: resolved.ClientSecret,
		http:         &http.Client{Timeout: 45 * time.Second},
	}
}

// TestConnection obtains an access token to verify credentials.
func (c *Client) TestConnection(ctx context.Context) error {
	if !c.Enabled() {
		return fmt.Errorf("graph credentials not configured")
	}
	_, err := c.accessToken(ctx)
	return err
}

func (c *Client) Enabled() bool {
	return c.tenantID != "" && c.clientID != "" && c.clientSecret != ""
}

func (c *Client) tokenURL() string {
	return fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", c.tenantID)
}

func (c *Client) accessToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Now().Before(c.tokenExp.Add(-60*time.Second)) {
		return c.token, nil
	}

	form := url.Values{}
	form.Set("client_id", c.clientID)
	form.Set("client_secret", c.clientSecret)
	form.Set("scope", "https://graph.microsoft.com/.default")
	form.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.tokenURL(), strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", err
	}
	if tr.AccessToken == "" {
		if tr.Error != "" {
			return "", fmt.Errorf("graph token: %s — %s", tr.Error, tr.ErrorDesc)
		}
		return "", fmt.Errorf("graph token: empty access_token status=%d body=%s", resp.StatusCode, string(body))
	}

	c.token = tr.AccessToken
	if tr.ExpiresIn <= 0 {
		tr.ExpiresIn = 3600
	}
	c.tokenExp = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	return c.token, nil
}

func (c *Client) invalidateToken() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = ""
	c.tokenExp = time.Time{}
}

func (c *Client) doGET(ctx context.Context, path string, accept string) ([]byte, int, error) {
	body, status, err := c.doGETOnce(ctx, path, accept)
	if err != nil {
		return nil, 0, err
	}
	if status == http.StatusUnauthorized {
		c.invalidateToken()
		return c.doGETOnce(ctx, path, accept)
	}
	return body, status, nil
}

func (c *Client) doGETOnce(ctx context.Context, path string, accept string) ([]byte, int, error) {
	token, err := c.accessToken(ctx)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, graphBase+path, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, resp.StatusCode, nil
}

// ListInboxMessagesSince returns inbox messages received after since (UTC).
func (c *Client) ListInboxMessagesSince(ctx context.Context, mailbox string, since time.Time, top int) ([]Message, error) {
	return c.listFolderMessagesSince(ctx, mailbox, "inbox", "receivedDateTime", since, top)
}

// ListSentMessagesSince returns sent-items messages sent after since (UTC).
func (c *Client) ListSentMessagesSince(ctx context.Context, mailbox string, since time.Time, top int) ([]Message, error) {
	return c.listFolderMessagesSince(ctx, mailbox, "sentitems", "sentDateTime", since, top)
}

func (c *Client) listFolderMessagesSince(ctx context.Context, mailbox, folder, dateField string, since time.Time, top int) ([]Message, error) {
	if top <= 0 {
		top = 25
	}
	if top > 50 {
		top = 50
	}
	path := folderMessagesPath(mailbox, folder, dateField, since, top)

	body, status, err := c.doGET(ctx, path, "application/json")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("graph list messages status=%d path=%s body=%s", status, path, truncate(string(body), 300))
	}

	var lr listResponse
	if err := json.Unmarshal(body, &lr); err != nil {
		return nil, err
	}
	return lr.Value, nil
}

// folderMessagesPath builds a Graph list URL with correctly encoded OData query params.
func folderMessagesPath(mailbox, folder, dateField string, since time.Time, top int) string {
	sinceISO := since.UTC().Format("2006-01-02T15:04:05Z")
	q := url.Values{}
	q.Set("$filter", fmt.Sprintf("%s gt %s", dateField, sinceISO))
	q.Set("$orderby", dateField+" asc")
	q.Set("$top", strconv.Itoa(top))
	q.Set("$select", "id,subject,receivedDateTime,sentDateTime,internetMessageId,from,toRecipients,hasAttachments")
	user := url.PathEscape(strings.TrimSpace(mailbox))
	return fmt.Sprintf("/users/%s/mailFolders/%s/messages?%s", user, folder, q.Encode())
}

// GetMessageMIME downloads the raw RFC822 MIME content for a message.
func (c *Client) GetMessageMIME(ctx context.Context, mailbox, messageID string) ([]byte, error) {
	mailbox = url.PathEscape(strings.TrimSpace(mailbox))
	messageID = url.PathEscape(strings.TrimSpace(messageID))
	path := fmt.Sprintf("/users/%s/messages/%s/$value", mailbox, messageID)

	body, status, err := c.doGET(ctx, path, "message/rfc822")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("graph get mime status=%d body=%s", status, truncate(string(body), 200))
	}
	return body, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
