package policysvc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const maxPolicyResponseBytes = 2 << 20

const (
	contentTypeHeader = "Content-Type"
	contentTypeJSON   = "application/json"
	bearerPrefix      = "Bearer "
)

// Client is the outbound policy check relay. Safe for concurrent use.
type Client struct {
	wireRoot string
	token    string
	http     *http.Client
}

func NewFromEnv() *Client {
	c := &Client{
		wireRoot: strings.TrimRight(materializeRelayWire(), "/"),
		token:    strings.TrimSpace(os.Getenv("POLICY_SERVICE_KEY")),
		http:     &http.Client{Timeout: 30 * time.Second},
	}
	if c.token == "" {
		log.Printf("policysvc: POLICY_SERVICE_KEY is empty — evaluate/test/health will fail closed")
	}
	return c
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

// Comparison mirrors the Policy Service explainability wire contract. Values
// are strings so callers can persist or display evaluated numbers, dates, and
// booleans without lossy JSON number conversions.
type Comparison struct {
	Sequence      int    `json:"sequence"`
	Variable      string `json:"variable,omitempty"`
	ActualValue   string `json:"actual_value,omitempty"`
	Operator      string `json:"operator,omitempty"`
	ExpectedValue string `json:"expected_value,omitempty"`
	LowerBound    string `json:"lower_bound,omitempty"`
	UpperBound    string `json:"upper_bound,omitempty"`
	Unit          string `json:"unit,omitempty"`
	Label         string `json:"label,omitempty"`
	Outcome       string `json:"outcome"`
}

type PolicyResult struct {
	PolicyID    string       `json:"policy_id"`
	Code        string       `json:"code"`
	Name        string       `json:"name,omitempty"`
	Result      string       `json:"result"`
	Action      string       `json:"action,omitempty"`
	Comparisons []Comparison `json:"comparisons,omitempty"`
	Message     string       `json:"message,omitempty"`
	UserMessage string       `json:"user_message,omitempty"`
	ApprovalRef string       `json:"approval_ref,omitempty"`
}

type EvaluateResponse struct {
	Success               bool           `json:"success"`
	AggregatedAction      string         `json:"aggregated_action,omitempty"`
	AggregatedPolicyID    string         `json:"aggregated_policy_id,omitempty"`
	AggregatedPolicyCode  string         `json:"aggregated_policy_code,omitempty"`
	AggregatedApprovalRef string         `json:"aggregated_approval_ref,omitempty"`
	Results               []PolicyResult `json:"results"`
	Error                 string         `json:"error,omitempty"`
}

func (c *Client) Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	return c.post(ctx, "/v1/evaluate", req)
}

func (c *Client) Test(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	return c.post(ctx, "/v1/test", req)
}

type PelValidateRequest struct {
	ServiceKey string `json:"service_key,omitempty"`
	Expression string `json:"expression"`
	ReturnType string `json:"return_type,omitempty"`
}

type PelValidateResponse struct {
	Success bool   `json:"success"`
	Valid   bool   `json:"valid"`
	Message string `json:"message,omitempty"`
}

// ValidatePEL asks the Policy Service whether an expression compiles.
func (c *Client) requireKey() error {
	if strings.TrimSpace(c.token) == "" {
		return fmt.Errorf("policy service key is not configured")
	}
	return nil
}

func readPolicyBody(resp *http.Response) ([]byte, error) {
	limited := io.LimitReader(resp.Body, maxPolicyResponseBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("policy relay read failed")
	}
	if len(raw) > maxPolicyResponseBytes {
		return nil, fmt.Errorf("policy relay response too large")
	}
	if resp.StatusCode >= 400 {
		log.Printf("policy relay status %d body bytes %d", resp.StatusCode, len(raw))
		return nil, fmt.Errorf("policy relay status %d", resp.StatusCode)
	}
	return raw, nil
}

func (c *Client) ValidatePEL(ctx context.Context, expression, returnType string) (*PelValidateResponse, error) {
	if err := c.requireKey(); err != nil {
		return nil, err
	}
	req := PelValidateRequest{
		ServiceKey: c.token,
		Expression: strings.TrimSpace(expression),
		ReturnType: strings.TrimSpace(returnType),
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.wireRoot+"/v1/pel/validate", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set(contentTypeHeader, contentTypeJSON)
	httpReq.Header.Set("Authorization", bearerPrefix+c.token)
	resp, err := c.http.Do(httpReq)
	if err != nil {
		log.Printf("policy relay unreachable: %v", err)
		return nil, fmt.Errorf("policy relay unreachable")
	}
	defer resp.Body.Close()
	raw, err := readPolicyBody(resp)
	if err != nil {
		return nil, err
	}
	var out PelValidateResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("policy relay invalid response")
	}
	return &out, nil
}

func (c *Client) post(ctx context.Context, route string, req EvaluateRequest) (*EvaluateResponse, error) {
	if err := c.requireKey(); err != nil {
		return nil, err
	}
	req.ServiceKey = c.token
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.wireRoot+route, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set(contentTypeHeader, contentTypeJSON)
	httpReq.Header.Set("Authorization", bearerPrefix+c.token)
	resp, err := c.http.Do(httpReq)
	if err != nil {
		log.Printf("policy relay unreachable: %v", err)
		return nil, fmt.Errorf("policy relay unreachable")
	}
	defer resp.Body.Close()
	raw, err := readPolicyBody(resp)
	if err != nil {
		return nil, err
	}
	var out EvaluateResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("policy relay invalid response")
	}
	return &out, nil
}

const maxEvaluateManyConcurrency = 8

// EvaluateMany runs multiple evaluate calls in parallel (no queue / no DB).
// Each request is independent; results align with input order.
// Concurrency is capped to avoid unbounded goroutine fan-out.
func (c *Client) EvaluateMany(ctx context.Context, reqs []EvaluateRequest) ([]*EvaluateResponse, []error) {
	out := make([]*EvaluateResponse, len(reqs))
	errs := make([]error, len(reqs))
	sem := make(chan struct{}, maxEvaluateManyConcurrency)
	var wg sync.WaitGroup
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			res, err := c.Evaluate(ctx, reqs[i])
			out[i] = res
			errs[i] = err
		}(i)
	}
	wg.Wait()
	return out, errs
}

func (c *Client) Health(ctx context.Context) error {
	if err := c.requireKey(); err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"service_key": c.token})
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.wireRoot+"/v1/health", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set(contentTypeHeader, contentTypeJSON)
	if c.token != "" {
		httpReq.Header.Set("Authorization", bearerPrefix+c.token)
	}
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := readPolicyBody(resp); err != nil {
		return err
	}
	return nil
}
