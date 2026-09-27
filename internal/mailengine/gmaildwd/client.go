package gmaildwd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client calls Gmail API as an impersonated Workspace user (domain-wide delegation).
type Client struct {
	userEmail string
	token     string
	http      *http.Client
}

func NewClient(userEmail, accessToken string) *Client {
	return &Client{
		userEmail: strings.TrimSpace(strings.ToLower(userEmail)),
		token:     strings.TrimSpace(accessToken),
		http:      &http.Client{Timeout: 45 * time.Second},
	}
}

type RawMessage struct {
	ID           string
	InternalDate time.Time
	Raw          []byte
}

// MessageMeta is lightweight header metadata for pre-filter (no body).
type MessageMeta struct {
	ID             string
	InternalDate   time.Time
	From           string
	To             []string
	Subject        string
	HasAttachments bool
}

func (c *Client) baseURL() string {
	return "https://gmail.googleapis.com/gmail/v1/users/" + url.PathEscape(c.userEmail)
}

func (c *Client) doGET(ctx context.Context, path string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL()+path, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, resp.StatusCode, nil
}

func (c *Client) TestConnection(ctx context.Context) error {
	body, status, err := c.doGET(ctx, "/profile")
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("gmail profile status=%d body=%s", status, truncate(string(body), 200))
	}
	return nil
}

func (c *Client) ListMessageIDsSince(ctx context.Context, sent bool, since time.Time, max int) ([]string, error) {
	if max <= 0 {
		max = 25
	}
	if max > 100 {
		max = 100
	}
	label := "INBOX"
	if sent {
		label = "SENT"
	}
	q := url.Values{}
	q.Set("labelIds", label)
	q.Set("maxResults", strconv.Itoa(max))
	q.Set("q", fmt.Sprintf("after:%d", since.UTC().Unix()))
	body, status, err := c.doGET(ctx, "/messages?"+q.Encode())
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("gmail list status=%d body=%s", status, truncate(string(body), 300))
	}
	var lr struct {
		Messages []struct {
			ID string `json:"id"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &lr); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(lr.Messages))
	for _, m := range lr.Messages {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	return ids, nil
}

// GetMessageMeta fetches headers only for inbox filter pre-check.
func (c *Client) GetMessageMeta(ctx context.Context, id string) (MessageMeta, error) {
	q := url.Values{}
	q.Set("format", "metadata")
	q.Add("metadataHeaders", "From")
	q.Add("metadataHeaders", "To")
	q.Add("metadataHeaders", "Subject")
	body, status, err := c.doGET(ctx, "/messages/"+url.PathEscape(id)+"?"+q.Encode())
	if err != nil {
		return MessageMeta{}, err
	}
	if status != http.StatusOK {
		return MessageMeta{}, fmt.Errorf("gmail get meta status=%d body=%s", status, truncate(string(body), 200))
	}
	var mr struct {
		ID           string `json:"id"`
		InternalDate string `json:"internalDate"`
		Payload      struct {
			Headers []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"headers"`
			Parts []struct {
				Filename string `json:"filename"`
			} `json:"parts"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(body, &mr); err != nil {
		return MessageMeta{}, err
	}
	out := MessageMeta{ID: mr.ID}
	if ms, err := strconv.ParseInt(mr.InternalDate, 10, 64); err == nil && ms > 0 {
		out.InternalDate = time.UnixMilli(ms).UTC()
	}
	for _, h := range mr.Payload.Headers {
		switch strings.ToLower(h.Name) {
		case "from":
			out.From = firstEmail(h.Value)
		case "to":
			out.To = splitEmails(h.Value)
		case "subject":
			out.Subject = strings.TrimSpace(h.Value)
		}
	}
	for _, p := range mr.Payload.Parts {
		if strings.TrimSpace(p.Filename) != "" {
			out.HasAttachments = true
			break
		}
	}
	return out, nil
}

func (c *Client) GetRawMessage(ctx context.Context, id string) (RawMessage, error) {
	body, status, err := c.doGET(ctx, "/messages/"+url.PathEscape(id)+"?format=raw")
	if err != nil {
		return RawMessage{}, err
	}
	if status != http.StatusOK {
		return RawMessage{}, fmt.Errorf("gmail get raw status=%d body=%s", status, truncate(string(body), 200))
	}
	var mr struct {
		ID           string `json:"id"`
		Raw          string `json:"raw"`
		InternalDate string `json:"internalDate"`
	}
	if err := json.Unmarshal(body, &mr); err != nil {
		return RawMessage{}, err
	}
	raw, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(strings.TrimRight(mr.Raw, "="))
	if err != nil {
		return RawMessage{}, fmt.Errorf("gmail raw decode: %w", err)
	}
	out := RawMessage{ID: mr.ID, Raw: raw}
	if ms, err := strconv.ParseInt(mr.InternalDate, 10, 64); err == nil && ms > 0 {
		out.InternalDate = time.UnixMilli(ms).UTC()
	}
	return out, nil
}

func firstEmail(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	if i := strings.Index(v, "<"); i >= 0 {
		if j := strings.Index(v[i:], ">"); j > 0 {
			return strings.TrimSpace(v[i+1 : i+j])
		}
	}
	return strings.TrimSpace(v)
}

func splitEmails(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if e := firstEmail(p); e != "" {
			out = append(out, strings.ToLower(e))
		}
	}
	return out
}
