package docsvc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Client talks to CIMPLR-Document-Service for hard-stop, quota counts, and format rendering.
// Job queue / client payloads stay in Main (dms_svc).
type Client struct {
	base  string
	token string
	http  *http.Client
}

func NewFromEnv() *Client {
	base := strings.TrimSpace(os.Getenv("DOCUMENT_SERVICE_URL"))
	if base == "" {
		base = materializeRelayWire()
	}
	return &Client{
		base:  strings.TrimRight(base, "/"),
		token: strings.TrimSpace(os.Getenv("DOCUMENT_SERVICE_KEY")),
		http:  &http.Client{Timeout: 30 * time.Second},
	}
}

func renderHTTPClient() *http.Client {
	return &http.Client{Timeout: 3 * time.Minute}
}

func healthHTTPClient() *http.Client {
	return &http.Client{Timeout: 4 * time.Second}
}

// Health pings /v1/health — used by the DMS status light so the UI can tell
// whether Document-Service is reachable without waiting on a full quota check.
func (c *Client) Health(ctx context.Context) error {
	var wrap struct {
		Success bool   `json:"success"`
		Status  string `json:"status"`
	}
	if err := c.postWithClient(ctx, healthHTTPClient(), "/v1/health", map[string]any{"service_key": c.token}, &wrap); err != nil {
		return err
	}
	if !wrap.Success {
		return fmt.Errorf("document-service unhealthy")
	}
	return nil
}

func QueueEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("DMS_GENERATION_QUEUE_ENABLED")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

type QuotaCheckData struct {
	Allowed           bool   `json:"allowed"`
	GenerationEnabled bool   `json:"generation_enabled"`
	ErrorCode         string `json:"error_code"`
	Message           string `json:"message"`
	MonthCount        int64  `json:"month_count"`
	MonthLimit        int64  `json:"month_limit"`
	HourCount         int64  `json:"hour_count"`
	HourLimit         int64  `json:"hour_limit"`
}

func (c *Client) QuotaCheck(ctx context.Context) (QuotaCheckData, error) {
	var wrap struct {
		Success bool           `json:"success"`
		Data    QuotaCheckData `json:"data"`
		Error   string         `json:"error"`
	}
	err := c.post(ctx, "/v1/quota/check", mergeCaller(map[string]any{
		"service_key": c.token,
	}, DefaultCallerContext("")), &wrap)
	if err != nil {
		return QuotaCheckData{}, err
	}
	if wrap.Data.ErrorCode == "" && wrap.Error != "" {
		wrap.Data.ErrorCode = "DOC_SERVICE_ERROR"
		wrap.Data.Message = wrap.Error
	}
	return wrap.Data, nil
}

func (c *Client) QuotaIncrement(ctx context.Context, generationCount, byteCount int64, actor string) error {
	var wrap struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	err := c.post(ctx, "/v1/quota/increment", mergeCaller(map[string]any{
		"service_key":      c.token,
		"generation_count": generationCount,
		"byte_count":       byteCount,
		"actor":            actor,
	}, DefaultCallerContext(actor)), &wrap)
	if err != nil {
		return err
	}
	if !wrap.Success && wrap.Error != "" {
		return fmt.Errorf("%s", wrap.Error)
	}
	return nil
}

func (c *Client) Pause(ctx context.Context, by, reason string) error {
	var wrap struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	return c.post(ctx, "/v1/control/pause", map[string]any{
		"service_key": c.token, "by": by, "reason": reason,
	}, &wrap)
}

func (c *Client) Resume(ctx context.Context, by string) error {
	var wrap struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	return c.post(ctx, "/v1/control/resume", map[string]any{
		"service_key": c.token, "by": by,
	}, &wrap)
}

// PageDesignJSON mirrors template content_json.pageDesign for render payloads.
type PageDesignJSON struct {
	HeaderText        string  `json:"headerText"`
	FooterText        string  `json:"footerText"`
	WatermarkText     string  `json:"watermarkText"`
	WatermarkDataURL  string  `json:"watermarkDataUrl"`
	WatermarkAngle    float64 `json:"watermarkAngle"`
	LetterheadDataURL string  `json:"letterheadDataUrl"`
	LogoDataURL       string  `json:"logoDataUrl"`
	FooterLogoDataURL string  `json:"footerLogoDataUrl"`
	BackgroundColor   string  `json:"backgroundColor"`
	LogoAlign         string  `json:"logoAlign"`
	HeaderAlign       string  `json:"headerAlign"`
	PageSize          string  `json:"pageSize"`
	Orientation       string  `json:"orientation"`
	MarginTop         float64 `json:"marginTop"`
	MarginRight       float64 `json:"marginRight"`
	MarginBottom      float64 `json:"marginBottom"`
	MarginLeft        float64 `json:"marginLeft"`
}

type RenderFormatRequest struct {
	OutputFormat string            `json:"output_format"`
	MergedHTML   string            `json:"merged_html"`
	MergeValues  map[string]string `json:"merge_values"`
	SheetTokens  []string          `json:"sheet_tokens"`
	SheetRows    [][]string        `json:"sheet_rows"`
	Kind         string            `json:"kind"`
	PageDesign   PageDesignJSON    `json:"page_design"`
}

type RenderFormatData struct {
	BytesBase64 string `json:"bytes_base64"`
	Ext         string `json:"ext"`
	ContentType string `json:"content_type"`
	Format      string `json:"format"`
}

func (c *Client) RenderFormat(ctx context.Context, req RenderFormatRequest) (RenderFormatData, error) {
	var wrap struct {
		Success bool             `json:"success"`
		Data    RenderFormatData `json:"data"`
		Error   string           `json:"error"`
		Message string           `json:"message"`
	}
	body := mergeCaller(map[string]any{
		"service_key":   c.token,
		"output_format": req.OutputFormat,
		"merged_html":   req.MergedHTML,
		"merge_values":  req.MergeValues,
		"sheet_tokens":  req.SheetTokens,
		"sheet_rows":    req.SheetRows,
		"kind":          req.Kind,
		"page_design":   req.PageDesign,
	}, DefaultCallerContext(""))
	if err := c.postWithClient(ctx, renderHTTPClient(), "/v1/render/format", body, &wrap); err != nil {
		return RenderFormatData{}, err
	}
	if !wrap.Success {
		msg := strings.TrimSpace(wrap.Message)
		if msg == "" {
			msg = strings.TrimSpace(wrap.Error)
		}
		if msg == "" {
			msg = "document-service render failed"
		}
		return RenderFormatData{}, fmt.Errorf("%s", msg)
	}
	if wrap.Data.BytesBase64 == "" {
		return RenderFormatData{}, fmt.Errorf("document-service render returned empty payload")
	}
	return wrap.Data, nil
}

// mainS3Fields resolves Main's own S3 identity to pass per-request to
// Document-Service for storage calls — same env vars and same MAIN_S3
// resolution as internal/jobs/dino/outboxWorker.go's owS3CredsForBackend,
// same "fishy" field names Document-Service expects (see its api/caller.go
// StorageCreds). Document-Service is one shared instance; Main is
// tenant-based (each deployment has its own bucket), so credentials travel
// with the request rather than living in Document-Service's own env.
func mainS3Fields() map[string]any {
	region := strings.TrimSpace(os.Getenv("BANK_STMT_S3_REGION"))
	if region == "" {
		region = strings.TrimSpace(os.Getenv("AWS_REGION"))
	}
	bucket := strings.TrimSpace(os.Getenv("BANK_STMT_S3_BUCKET"))
	if bucket == "" {
		bucket = "cimplr"
	}
	return map[string]any{
		"x7k": strings.TrimSpace(os.Getenv("AWS_ACCESS_KEY_ID")),
		"m2p": strings.TrimSpace(os.Getenv("AWS_SECRET_ACCESS_KEY")),
		"z1q": region,
		"b9r": bucket,
	}
}

func mergeS3Fields(body map[string]any) map[string]any {
	if body == nil {
		body = map[string]any{}
	}
	for k, v := range mainS3Fields() {
		body[k] = v
	}
	return body
}

// RenderAndStoreData is /v1/render/store's response — same as RenderFormatData
// plus the S3 key Document-Service stored the result under in its own bucket.
type RenderAndStoreData struct {
	BytesBase64 string `json:"bytes_base64"`
	Key         string `json:"key"`
	Ext         string `json:"ext"`
	ContentType string `json:"content_type"`
	Format      string `json:"format"`
	Size        int64  `json:"size"`
}

// RenderAndStore renders like RenderFormat, but Document-Service also uploads
// the result to its own S3 bucket and returns the key — Main records the key
// on dms_svc.generated_document (storage_backend='DOCSVC_S3') instead of
// calling its own S3 credentials. Bytes are still returned for the LOCAL
// destination case and checksum verification.
func (c *Client) RenderAndStore(ctx context.Context, req RenderFormatRequest) (RenderAndStoreData, error) {
	var wrap struct {
		Success bool               `json:"success"`
		Data    RenderAndStoreData `json:"data"`
		Error   string             `json:"error"`
		Message string             `json:"message"`
	}
	body := mergeS3Fields(mergeCaller(map[string]any{
		"service_key":   c.token,
		"output_format": req.OutputFormat,
		"merged_html":   req.MergedHTML,
		"merge_values":  req.MergeValues,
		"sheet_tokens":  req.SheetTokens,
		"sheet_rows":    req.SheetRows,
		"kind":          req.Kind,
		"page_design":   req.PageDesign,
	}, DefaultCallerContext("")))
	if err := c.postWithClient(ctx, renderHTTPClient(), "/v1/render/store", body, &wrap); err != nil {
		return RenderAndStoreData{}, err
	}
	if !wrap.Success {
		msg := strings.TrimSpace(wrap.Message)
		if msg == "" {
			msg = strings.TrimSpace(wrap.Error)
		}
		if msg == "" {
			msg = "document-service render+store failed"
		}
		return RenderAndStoreData{}, fmt.Errorf("%s", msg)
	}
	if wrap.Data.Key == "" {
		return RenderAndStoreData{}, fmt.Errorf("document-service render+store returned no key")
	}
	return wrap.Data, nil
}

// DownloadDocument fetches raw bytes for a key Document-Service stored (i.e.
// dms_svc.generated_document.storage_backend='DOCSVC_S3'). Used where Main
// needs actual bytes (email attachment MIME embedding), not just a link.
func (c *Client) DownloadDocument(ctx context.Context, key string) ([]byte, error) {
	var wrap struct {
		Success bool `json:"success"`
		Data    struct {
			BytesBase64 string `json:"bytes_base64"`
		} `json:"data"`
		Error string `json:"error"`
	}
	body := mergeS3Fields(mergeCaller(map[string]any{
		"service_key": c.token,
		"key":         key,
	}, DefaultCallerContext("")))
	if err := c.postWithClient(ctx, renderHTTPClient(), "/v1/documents/download", body, &wrap); err != nil {
		return nil, err
	}
	if !wrap.Success {
		msg := strings.TrimSpace(wrap.Error)
		if msg == "" {
			msg = "document-service download failed"
		}
		return nil, fmt.Errorf("%s", msg)
	}
	raw, err := base64.StdEncoding.DecodeString(wrap.Data.BytesBase64)
	if err != nil {
		return nil, fmt.Errorf("document-service download decode: %w", err)
	}
	return raw, nil
}

// UploadDocument stores bytes Main already has (no render step) — e.g. a ZIP
// package built from several already-generated documents.
func (c *Client) UploadDocument(ctx context.Context, prefix string, data []byte, contentType, ext string) (string, error) {
	var wrap struct {
		Success bool `json:"success"`
		Data    struct {
			Key string `json:"key"`
		} `json:"data"`
		Error string `json:"error"`
	}
	body := mergeS3Fields(mergeCaller(map[string]any{
		"service_key":  c.token,
		"bytes_base64": base64.StdEncoding.EncodeToString(data),
		"content_type": contentType,
		"prefix":       prefix,
		"ext":          ext,
	}, DefaultCallerContext("")))
	if err := c.postWithClient(ctx, renderHTTPClient(), "/v1/documents/upload", body, &wrap); err != nil {
		return "", err
	}
	if !wrap.Success || wrap.Data.Key == "" {
		msg := strings.TrimSpace(wrap.Error)
		if msg == "" {
			msg = "document-service upload failed"
		}
		return "", fmt.Errorf("%s", msg)
	}
	return wrap.Data.Key, nil
}

// DownloadDocumentURL returns a short-lived presigned link for a key
// Document-Service stored — for browser-facing downloads (Sent Box) that
// shouldn't proxy the whole file through Main as a JSON payload.
func (c *Client) DownloadDocumentURL(ctx context.Context, key, contentType string, inline bool) (string, error) {
	var wrap struct {
		Success bool `json:"success"`
		Data    struct {
			URL string `json:"url"`
		} `json:"data"`
		Error string `json:"error"`
	}
	body := mergeS3Fields(mergeCaller(map[string]any{
		"service_key":  c.token,
		"key":          key,
		"content_type": contentType,
		"inline":       inline,
	}, DefaultCallerContext("")))
	if err := c.post(ctx, "/v1/documents/download-url", body, &wrap); err != nil {
		return "", err
	}
	if !wrap.Success || wrap.Data.URL == "" {
		msg := strings.TrimSpace(wrap.Error)
		if msg == "" {
			msg = "document-service presign failed"
		}
		return "", fmt.Errorf("%s", msg)
	}
	return wrap.Data.URL, nil
}

func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	return c.postWithClient(ctx, c.http, path, body, out)
}

func (c *Client) postWithClient(ctx context.Context, httpClient *http.Client, path string, body any, out any) error {
	if strings.TrimSpace(c.token) == "" {
		return fmt.Errorf("DOCUMENT_SERVICE_KEY not configured")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	client := httpClient
	if client == nil {
		client = c.http
	}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("document-service unreachable: %w", err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("DOCUMENT_SERVICE_UNAUTHORIZED: key mismatch or missing")
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("docsvc %s: status=%d body=%s", path, res.StatusCode, truncate(string(b), 300))
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
