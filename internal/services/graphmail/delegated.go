package graphmail

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// DelegatedClient calls Microsoft Graph /me endpoints with a caller-supplied
// delegated access token (OAuth authorization-code flow). Works for personal
// Microsoft accounts and work accounts alike; token refresh is the caller's job.
type DelegatedClient struct {
	token string
	http  *http.Client
}

func NewDelegatedClient(accessToken string) *DelegatedClient {
	return &DelegatedClient{
		token: accessToken,
		http:  &http.Client{Timeout: 45 * time.Second},
	}
}

func (c *DelegatedClient) doGET(ctx context.Context, path, accept string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, graphBase+path, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
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

// TestConnection verifies the token can read the signed-in user's profile.
func (c *DelegatedClient) TestConnection(ctx context.Context) error {
	body, status, err := c.doGET(ctx, "/me", "application/json")
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("graph delegated /me status=%d body=%s", status, truncate(string(body), 200))
	}
	return nil
}

// ListInboxMessagesSince returns the signed-in user's inbox messages after since.
func (c *DelegatedClient) ListInboxMessagesSince(ctx context.Context, since time.Time, top int) ([]Message, error) {
	return c.listFolderSince(ctx, "inbox", "receivedDateTime", since, top)
}

// ListSentMessagesSince returns the signed-in user's sent messages after since.
func (c *DelegatedClient) ListSentMessagesSince(ctx context.Context, since time.Time, top int) ([]Message, error) {
	return c.listFolderSince(ctx, "sentitems", "sentDateTime", since, top)
}

func (c *DelegatedClient) listFolderSince(ctx context.Context, folder, dateField string, since time.Time, top int) ([]Message, error) {
	if top <= 0 {
		top = 25
	}
	if top > 50 {
		top = 50
	}
	q := url.Values{}
	q.Set("$filter", fmt.Sprintf("%s gt %s", dateField, since.UTC().Format("2006-01-02T15:04:05.000Z")))
	q.Set("$orderby", dateField+" asc")
	q.Set("$top", strconv.Itoa(top))
	q.Set("$select", "id,subject,receivedDateTime,sentDateTime,internetMessageId,from,toRecipients,hasAttachments")
	path := fmt.Sprintf("/me/mailFolders/%s/messages?%s", folder, q.Encode())

	body, status, err := c.doGET(ctx, path, "application/json")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("graph delegated list status=%d path=%s body=%s", status, path, truncate(string(body), 300))
	}
	var lr listResponse
	if err := json.Unmarshal(body, &lr); err != nil {
		return nil, err
	}
	return lr.Value, nil
}

// GetMessageMIME downloads raw RFC822 content for the signed-in user's message.
func (c *DelegatedClient) GetMessageMIME(ctx context.Context, messageID string) ([]byte, error) {
	path := fmt.Sprintf("/me/messages/%s/$value", url.PathEscape(messageID))
	body, status, err := c.doGET(ctx, path, "message/rfc822")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("graph delegated mime status=%d body=%s", status, truncate(string(body), 200))
	}
	return body, nil
}
