package mailruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Runtime struct {
	wireRoot string
	token    string
	http     *http.Client
}

type serviceEnvelope struct {
	Success    *bool           `json:"success"`
	StatusCode int             `json:"statusCode"`
	Message    string          `json:"message"`
	Data       json.RawMessage `json:"data"`
	Error      struct {
		Code    string `json:"code"`
		Details string `json:"details"`
	} `json:"error"`
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

func NewRuntime() *Runtime {
	wireRoot := strings.TrimRight(materializeRelayWire(), "/")
	token := strings.TrimSpace(os.Getenv("EMAIL_SERVICE_KEY"))
	timeout := defaultTimeout()
	if v := strings.TrimSpace(os.Getenv("MAIL_RUNTIME_TIMEOUT_SECS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			timeout = time.Duration(n) * time.Second
		}
	}
	return &Runtime{
		wireRoot: wireRoot,
		token:    token,
		http: &http.Client{
			Timeout: timeout,
		},
	}
}

func defaultTimeout() time.Duration {
	return 5 * time.Minute
}

func pullTimeout() time.Duration {
	if v := strings.TrimSpace(os.Getenv("MAIL_RUNTIME_PULL_TIMEOUT_SECS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 10 * time.Minute
}

func (r *Runtime) Ready() bool {
	return r.token != ""
}

func (r *Runtime) invoke(ctx context.Context, route string, payload map[string]interface{}, out interface{}, timeout time.Duration) error {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	payload["service_key"] = r.token

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.wireRoot+route, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	req.Header.Set("Content-Type", "application/json")

	client := r.http
	if timeout > 0 && client.Timeout != timeout {
		clone := *client
		clone.Timeout = timeout
		client = &clone
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var envelope serviceEnvelope
	if err := json.Unmarshal(respBody, &envelope); err == nil && envelope.Success != nil {
		if !*envelope.Success {
			details := strings.TrimSpace(envelope.Error.Details)
			if details == "" {
				details = strings.TrimSpace(envelope.Message)
			}
			if details == "" {
				details = fmt.Sprintf("mail operation failed (status %d)", resp.StatusCode)
			}
			return fmt.Errorf("%s", details)
		}
		if out != nil && len(envelope.Data) > 0 && string(envelope.Data) != "null" {
			if err := json.Unmarshal(envelope.Data, out); err != nil {
				return err
			}
		}
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mail operation failed (status %d)", resp.StatusCode)
	}
	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) ListPendingKeys(ctx context.Context, after string, limit int) ([]string, error) {
	payload := map[string]interface{}{"after": after}
	if limit > 0 {
		payload["limit"] = limit
	}
	var out PendingKeysResult
	if err := r.invoke(ctx, "/v1/list-new", payload, &out, 0); err != nil {
		return nil, err
	}
	return out.Keys, nil
}

func (r *Runtime) DecodeMessages(ctx context.Context, keys []string) (*BatchDecodeResult, error) {
	var out BatchDecodeResult
	err := r.invoke(ctx, "/v1/parse/batch", map[string]interface{}{
		"s3_raw_keys": keys,
	}, &out, 0)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) DecodeMessage(ctx context.Context, rawKey string) (*ParsedMessage, error) {
	var out ParsedMessage
	err := r.invoke(ctx, "/v1/parse", map[string]interface{}{
		"s3_raw_key": rawKey,
	}, &out, 0)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) ExtractStructured(ctx context.Context, s3ParsedKey, module string) (*StructuredExtractResult, error) {
	var out StructuredExtractResult
	err := r.invoke(ctx, "/v1/extract", map[string]interface{}{
		"s3_parsed_key": s3ParsedKey,
		"module":        module,
	}, &out, 0)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) ApplyInboundRules(ctx context.Context, ruleSetName, bucket, prefix string, rules []InboundRuleSpec) (*InboundRuleSyncResult, error) {
	var out InboundRuleSyncResult
	payload := map[string]interface{}{"rules": rules}
	if ruleSetName != "" {
		payload["rule_set_name"] = ruleSetName
	}
	if bucket != "" {
		payload["s3_bucket"] = bucket
	}
	if prefix != "" {
		payload["s3_prefix"] = prefix
	}
	err := r.invoke(ctx, "/v1/ses/rules/sync", payload, &out, 0)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) RemoveInboundRule(ctx context.Context, ruleSetName, ruleName string) error {
	payload := map[string]interface{}{"rule_name": ruleName}
	if ruleSetName != "" {
		payload["rule_set_name"] = ruleSetName
	}
	return r.invoke(ctx, "/v1/ses/rules/delete", payload, nil, 0)
}

func (r *Runtime) VerifyIMAP(ctx context.Context, mailbox string, cfg IMAPConnection) error {
	var out map[string]interface{}
	return r.invoke(ctx, "/v1/imap/test", map[string]interface{}{
		"mailbox_address": mailbox,
		"imap":            cfg,
	}, &out, 0)
}

func (r *Runtime) VerifyGraph(ctx context.Context, cfg GraphConnection) error {
	var out map[string]interface{}
	return r.invoke(ctx, "/v1/graph/test", map[string]interface{}{
		"graph": cfg,
	}, &out, 0)
}

func (r *Runtime) PullIMAPMessages(ctx context.Context, inboxID, mailbox, folder, direction string, lastUID uint32, pageSize int, cfg IMAPConnection) (*IMAPPullResult, error) {
	var out IMAPPullResult
	err := r.invoke(ctx, "/v1/imap/poll-folder", map[string]interface{}{
		"inbox_id":        inboxID,
		"mailbox_address": mailbox,
		"folder":          folder,
		"direction":       direction,
		"last_uid":        lastUID,
		"batch":           pageSize,
		"imap":            cfg,
	}, &out, pullTimeout())
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (r *Runtime) PullGraphMessages(ctx context.Context, inboxID, mailbox string, sentFolder bool, since string, pageSize int, cfg GraphConnection) (*GraphPullResult, error) {
	var out GraphPullResult
	err := r.invoke(ctx, "/v1/graph/poll-page", map[string]interface{}{
		"inbox_id":        inboxID,
		"mailbox_address": mailbox,
		"sent_folder":     sentFolder,
		"since":           since,
		"batch":           pageSize,
		"graph":           cfg,
	}, &out, pullTimeout())
	if err != nil {
		return nil, err
	}
	return &out, nil
}
