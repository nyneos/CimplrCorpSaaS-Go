package policysvc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Client is the outbound policy check relay. Safe for concurrent use.
type Client struct {
	wireRoot string
	token    string
	http     *http.Client
}

func NewFromEnv() *Client {
	return &Client{
		wireRoot: strings.TrimRight(materializeRelayWire(), "/"),
		token:    strings.TrimSpace(os.Getenv("POLICY_SERVICE_KEY")),
		http:     &http.Client{Timeout: 30 * time.Second},
	}
}

type EvaluateRequest struct {
	ServiceKey string                   `json:"service_key,omitempty"`
	EventCode  string                   `json:"event_code"`
	ModuleCode string                   `json:"module_code,omitempty"`
	FormID     string                   `json:"form_id,omitempty"`
	EntityCode string                   `json:"entity_code,omitempty"`
	ActorUser  string                   `json:"actor_user_id,omitempty"`
	Variables  map[string]string        `json:"variables"`
	Policies   []map[string]interface{} `json:"policies"`
}

type PolicyResult struct {
	PolicyID string `json:"policy_id"`
	Code     string `json:"code"`
	Result   string `json:"result"`
	Action   string `json:"action,omitempty"`
	Message  string `json:"message,omitempty"`
}

type EvaluateResponse struct {
	Success          bool           `json:"success"`
	AggregatedAction string         `json:"aggregated_action,omitempty"`
	Results          []PolicyResult `json:"results"`
	Error            string         `json:"error,omitempty"`
}

func (c *Client) Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	return c.post(ctx, "/v1/evaluate", req)
}

func (c *Client) Test(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	return c.post(ctx, "/v1/test", req)
}

func (c *Client) post(ctx context.Context, route string, req EvaluateRequest) (*EvaluateResponse, error) {
	req.ServiceKey = c.token
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.wireRoot+route, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("policy relay unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("policy relay status %d: %s", resp.StatusCode, string(raw))
	}
	var out EvaluateResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// EvaluateMany runs multiple evaluate calls in parallel (no queue / no DB).
// Each request is independent; results align with input order.
func (c *Client) EvaluateMany(ctx context.Context, reqs []EvaluateRequest) ([]*EvaluateResponse, []error) {
	out := make([]*EvaluateResponse, len(reqs))
	errs := make([]error, len(reqs))
	var wg sync.WaitGroup
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := c.Evaluate(ctx, reqs[i])
			out[i] = res
			errs[i] = err
		}(i)
	}
	wg.Wait()
	return out, errs
}

func (c *Client) Health(ctx context.Context) error {
	body, _ := json.Marshal(map[string]string{"service_key": c.token})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.wireRoot+"/v1/health", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("policy relay probe status %d", resp.StatusCode)
	}
	return nil
}
