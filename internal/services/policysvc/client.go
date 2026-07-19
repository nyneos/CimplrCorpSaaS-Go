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

// Client talks to CIMPLR-Policy-Service (no DB). Safe for concurrent use.
type Client struct {
	baseURL string
	key     string
	http    *http.Client
}

func NewFromEnv() *Client {
	base := strings.TrimRight(strings.TrimSpace(os.Getenv("POLICY_SERVICE_URL")), "/")
	if base == "" {
		base = "http://localhost:8184"
	}
	return &Client{
		baseURL: base,
		key:     strings.TrimSpace(os.Getenv("POLICY_SERVICE_KEY")),
		http:    &http.Client{Timeout: 30 * time.Second},
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

// Evaluate posts one check request to the standalone policy service.
func (c *Client) Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	return c.post(ctx, "/v1/evaluate", req)
}

// Test posts one workbench test-harness request (no execution_log side effects
// on the caller's side) — the standalone service handles /v1/test identically
// to /v1/evaluate today.
func (c *Client) Test(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	return c.post(ctx, "/v1/test", req)
}

func (c *Client) post(ctx context.Context, path string, req EvaluateRequest) (*EvaluateResponse, error) {
	req.ServiceKey = c.key
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.key != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.key)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("policy service unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("policy service status %d: %s", resp.StatusCode, string(raw))
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

// Health checks the standalone service.
func (c *Client) Health(ctx context.Context) error {
	body, _ := json.Marshal(map[string]string{"service_key": c.key})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/health", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.key != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.key)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("policy service health status %d", resp.StatusCode)
	}
	return nil
}
