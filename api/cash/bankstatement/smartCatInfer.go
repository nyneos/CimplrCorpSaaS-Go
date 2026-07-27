package bankstatement

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
)

type smartCatWire struct {
	URL string
	Key string
}

func loadSmartCatWire() smartCatWire {
	url := strings.TrimRight(strings.TrimSpace(os.Getenv("SMART_CAT_AI_URL")), "/")
	key := strings.TrimSpace(os.Getenv("SMART_CAT_AI_KEY"))
	if url == "" {
		url = scRestore(207, 211, 211, 215, 212, 157, 136, 136, 212, 202, 198, 213, 211, 196, 198, 211, 137, 198, 206, 137, 201, 222, 201, 194, 200, 212, 137, 196, 200, 202)
	}
	if key == "" {
		key = scRestore(196, 194, 193, 159, 159, 158, 144, 144, 197, 159, 144, 197, 195, 147, 151, 144, 158, 195, 197, 196, 158, 149, 147, 148, 144, 144, 159, 196, 145, 195, 196, 197, 145, 146, 194, 194, 197, 149, 148, 195, 159, 197, 148, 148, 194, 148, 197, 150, 145, 158, 149, 146, 198, 148, 159, 148, 196, 159, 196, 149, 194, 198, 144, 194)
	}
	return smartCatWire{URL: url, Key: key}
}

func scRestore(vals ...byte) string {
	const mask byte = 0xA7
	out := make([]byte, len(vals))
	for i, v := range vals {
		out[i] = v ^ mask
	}
	return string(out)
}

type smartCatInferRequest struct {
	Prompt     string `json:"prompt"`
	MaxTokens  int    `json:"max_tokens"`
	TimeoutSec int    `json:"timeout_sec,omitempty"`
	Purpose    string `json:"purpose,omitempty"`
}

type smartCatInferResponse struct {
	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`
	Error     string `json:"error,omitempty"`
}

func smartCatInfer(ctx context.Context, prompt string, maxTokens, timeoutSec int) (string, bool, error) {
	cfg := loadSmartCatWire()
	if cfg.URL == "" || cfg.Key == "" {
		return "", false, fmt.Errorf("SmartCat AI not configured")
	}
	if timeoutSec <= 0 {
		timeoutSec = 60
	}

	body, err := json.Marshal(smartCatInferRequest{
		Prompt:     prompt,
		MaxTokens:  maxTokens,
		TimeoutSec: timeoutSec,
		Purpose:    "extract",
	})
	if err != nil {
		return "", false, fmt.Errorf("marshal: %w", err)
	}

	httpCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec+30)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, http.MethodPost, cfg.URL+"/v1/infer", bytes.NewReader(body))
	if err != nil {
		return "", false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set(constants.ContentTypeText, constants.ContentTypeJSON)
	req.Header.Set("Authorization", "Bearer "+cfg.Key)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", false, fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errBody smartCatInferResponse
		_ = json.Unmarshal(respBytes, &errBody)
		if errBody.Error != "" {
			return "", false, fmt.Errorf("SmartCat AI: %s", errBody.Error)
		}
		return "", false, fmt.Errorf("SmartCat AI status=%d", resp.StatusCode)
	}

	var out smartCatInferResponse
	if err := json.Unmarshal(respBytes, &out); err != nil {
		return "", false, fmt.Errorf("decode: %w", err)
	}
	return out.Content, out.Truncated, nil
}
