package pdfco

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"CimplrCorpSaas/internal/logger")

const (
	baseURL    = "https://api.pdf.co"
	uploadPath = "/v1/file/upload"
	csvPath    = "/v1/pdf/convert/to/csv"
	xlsxPath   = "/v1/pdf/convert/to/xlsx"

	// outputDir is relative to the binary's working directory at runtime.
	// At dev time this resolves to internal/pdfco/output/ from the repo root.
	outputDir = "internal/pdfco/output"
)

type uploadResponse struct {
	URL    string `json:"url"`
	Error  bool   `json:"error"`
	Status int    `json:"status"`
}

type convertResponse struct {
	URL              string `json:"url"`
	Error            bool   `json:"error"`
	Status           int    `json:"status"`
	Message          string `json:"message"`
	Credits          int    `json:"credits"`
	RemainingCredits int    `json:"remainingCredits"`
}

// ConvertMeta carries pdf.co billing metadata back to the caller.
type ConvertMeta struct {
	CreditsUsed      int
	RemainingCredits int
}

// ConvertPDFToCSV uploads pdfBytes to pdf.co, converts to CSV, saves a local copy,
// and returns the CSV bytes plus billing metadata. password may be empty.
func ConvertPDFToCSV(ctx context.Context, pdfBytes []byte, originalFilename, password string) ([]byte, ConvertMeta, error) {
	logger.LogInfo("[PDFCO] ── starting conversion ──────────────────────")
	logger.LogInfo("[PDFCO] input file : %s (%d bytes)", originalFilename, len(pdfBytes))

	apiKey := os.Getenv("PDFCO_API_KEY")
	if apiKey == "" {
		return nil, ConvertMeta{}, fmt.Errorf("PDFCO_API_KEY environment variable not set")
	}
	logger.LogInfo("[PDFCO] api key   : set (length=%d)", len(apiKey))

	logger.LogInfo("[PDFCO] step 1/3  : uploading PDF to pdf.co temp storage …")
	fileURL, err := uploadFile(ctx, apiKey, pdfBytes, originalFilename)
	if err != nil {
		logger.LogError("[PDFCO] step 1/3 FAILED: %v", err)
		return nil, ConvertMeta{}, fmt.Errorf("upload: %w", err)
	}
	logger.LogInfo("[PDFCO] step 1/3  : upload OK → temp url=%s", fileURL)

	logger.LogInfo("[PDFCO] step 2/3  : requesting PDF→CSV conversion …")
	csvURL, meta, err := convertToCSV(ctx, apiKey, fileURL, password)
	if err != nil {
		logger.LogError("[PDFCO] step 2/3 FAILED: %v", err)
		return nil, ConvertMeta{}, fmt.Errorf("convert: %w", err)
	}
	logger.LogInfo("[PDFCO] step 2/3  : conversion OK → csv url=%s (credits used=%d remaining=%d)", csvURL, meta.CreditsUsed, meta.RemainingCredits)

	logger.LogInfo("[PDFCO] step 3/3  : downloading CSV from pdf.co …")
	csvBytes, err := downloadFile(ctx, csvURL)
	if err != nil {
		logger.LogError("[PDFCO] step 3/3 FAILED: %v", err)
		return nil, meta, fmt.Errorf("download: %w", err)
	}
	logger.LogInfo("[PDFCO] step 3/3  : download OK → csv size=%d bytes", len(csvBytes))

	savedPath, saveErr := saveCSVLocally(csvBytes, originalFilename)
	if saveErr != nil {
		logger.LogError("[PDFCO] warning   : could not save local CSV copy: %v", saveErr)
	} else {
		logger.LogInfo("[PDFCO] saved copy : %s", savedPath)
	}

	logger.LogInfo("[PDFCO] ── conversion complete ────────────────────────")
	return csvBytes, meta, nil
}

// ConvertPDFToXLSX uploads pdfBytes to pdf.co, converts to XLSX, saves a local copy,
// and returns the XLSX bytes plus billing metadata. password may be empty.
func ConvertPDFToXLSX(ctx context.Context, pdfBytes []byte, originalFilename, password string) ([]byte, ConvertMeta, error) {
	logger.LogInfo("[PDFCO] ── starting PDF→XLSX conversion ──────────────────")
	logger.LogInfo("[PDFCO] input file : %s (%d bytes)", originalFilename, len(pdfBytes))

	apiKey := os.Getenv("PDFCO_API_KEY")
	if apiKey == "" {
		return nil, ConvertMeta{}, fmt.Errorf("PDFCO_API_KEY environment variable not set")
	}
	logger.LogInfo("[PDFCO] api key   : set (length=%d)", len(apiKey))

	logger.LogInfo("[PDFCO] step 1/3  : uploading PDF to pdf.co temp storage …")
	fileURL, err := uploadFile(ctx, apiKey, pdfBytes, originalFilename)
	if err != nil {
		logger.LogError("[PDFCO] step 1/3 FAILED: %v", err)
		return nil, ConvertMeta{}, fmt.Errorf("upload: %w", err)
	}
	logger.LogInfo("[PDFCO] step 1/3  : upload OK → temp url=%s", fileURL)

	logger.LogInfo("[PDFCO] step 2/3  : requesting PDF→XLSX conversion …")
	xlsxURL, meta, err := convertToXLSX(ctx, apiKey, fileURL, password)
	if err != nil {
		logger.LogError("[PDFCO] step 2/3 FAILED: %v", err)
		return nil, ConvertMeta{}, fmt.Errorf("convert: %w", err)
	}
	logger.LogInfo("[PDFCO] step 2/3  : conversion OK → xlsx url=%s (credits used=%d remaining=%d)", xlsxURL, meta.CreditsUsed, meta.RemainingCredits)

	logger.LogInfo("[PDFCO] step 3/3  : downloading XLSX from pdf.co …")
	xlsxBytes, err := downloadFile(ctx, xlsxURL)
	if err != nil {
		logger.LogError("[PDFCO] step 3/3 FAILED: %v", err)
		return nil, meta, fmt.Errorf("download: %w", err)
	}
	logger.LogInfo("[PDFCO] step 3/3  : download OK → xlsx size=%d bytes", len(xlsxBytes))

	savedPath, saveErr := saveFileLocally(xlsxBytes, originalFilename, ".xlsx")
	if saveErr != nil {
		logger.LogError("[PDFCO] warning   : could not save local XLSX copy: %v", saveErr)
	} else {
		logger.LogInfo("[PDFCO] saved copy : %s", savedPath)
	}

	logger.LogInfo("[PDFCO] ── XLSX conversion complete ──────────────────────")
	return xlsxBytes, meta, nil
}

// ── internal helpers ──────────────────────────────────────────────────────────

func uploadFile(ctx context.Context, apiKey string, data []byte, filename string) (string, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err := fw.Write(data); err != nil {
		return "", fmt.Errorf("write file bytes: %w", err)
	}
	if err := mw.Close(); err != nil {
		return "", fmt.Errorf("close multipart writer: %w", err)
	}

	logger.LogInfo("[PDFCO] upload    : POST %s%s multipart size=%d bytes", baseURL, uploadPath, buf.Len())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+uploadPath, &buf)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	logger.LogInfo("[PDFCO] upload    : HTTP %d", resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	logger.LogInfo("[PDFCO] upload    : raw response=%s", string(body))

	var result uploadResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if result.Error {
		return "", fmt.Errorf("pdf.co returned error status=%d", result.Status)
	}
	if result.URL == "" {
		return "", fmt.Errorf("pdf.co upload returned empty URL")
	}
	return result.URL, nil
}

func convertToXLSX(ctx context.Context, apiKey, fileURL, password string) (string, ConvertMeta, error) {
	payload := map[string]interface{}{
		"url":   fileURL,
		"name":  "result.xlsx",
		"async": false,
	}
	if password != "" {
		payload["password"] = password
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", ConvertMeta{}, fmt.Errorf("marshal payload: %w", err)
	}
	logger.LogInfo("[PDFCO] convert   : POST %s%s payload=%s", baseURL, xlsxPath, string(payloadBytes))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+xlsxPath, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", ConvertMeta{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", ConvertMeta{}, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	logger.LogInfo("[PDFCO] convert   : HTTP %d", resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	logger.LogInfo("[PDFCO] convert   : raw response=%s", string(body))

	var result convertResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", ConvertMeta{}, fmt.Errorf("decode response: %w", err)
	}
	if result.Error {
		return "", ConvertMeta{}, fmt.Errorf("pdf.co convert failed (status %d): %s", result.Status, result.Message)
	}
	if result.URL == "" {
		return "", ConvertMeta{}, fmt.Errorf("pdf.co convert returned empty URL")
	}
	return result.URL, ConvertMeta{CreditsUsed: result.Credits, RemainingCredits: result.RemainingCredits}, nil
}

func convertToCSV(ctx context.Context, apiKey, fileURL, password string) (string, ConvertMeta, error) {
	payload := map[string]interface{}{
		"url":   fileURL,
		"name":  "result.csv",
		"async": false,
	}
	if password != "" {
		payload["password"] = password
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", ConvertMeta{}, fmt.Errorf("marshal payload: %w", err)
	}
	logger.LogInfo("[PDFCO] convert   : POST %s%s payload=%s", baseURL, csvPath, string(payloadBytes))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+csvPath, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", ConvertMeta{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", ConvertMeta{}, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	logger.LogInfo("[PDFCO] convert   : HTTP %d", resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	logger.LogInfo("[PDFCO] convert   : raw response=%s", string(body))

	var result convertResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", ConvertMeta{}, fmt.Errorf("decode response: %w", err)
	}
	if result.Error {
		return "", ConvertMeta{}, fmt.Errorf("pdf.co convert failed (status %d): %s", result.Status, result.Message)
	}
	if result.URL == "" {
		return "", ConvertMeta{}, fmt.Errorf("pdf.co convert returned empty URL")
	}
	return result.URL, ConvertMeta{CreditsUsed: result.Credits, RemainingCredits: result.RemainingCredits}, nil
}

// GetRemainingCredits fetches the pdf.co account credit balance.
func GetRemainingCredits(ctx context.Context) (int, error) {
	apiKey := os.Getenv("PDFCO_API_KEY")
	if apiKey == "" {
		return 0, fmt.Errorf("PDFCO_API_KEY environment variable not set")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/account/credit/balance", nil)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("x-api-key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	logger.LogInfo("[PDFCO] credits   : HTTP %d raw=%s", resp.StatusCode, string(body))
	var result struct {
		RemainingCredits int    `json:"remainingCredits"`
		Credits          int    `json:"credits"`
		Error            bool   `json:"error"`
		Message          string `json:"message"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("decode response: %w", err)
	}
	if result.Error {
		return 0, fmt.Errorf("pdf.co error: %s", result.Message)
	}
	if result.RemainingCredits > 0 {
		return result.RemainingCredits, nil
	}
	return result.Credits, nil
}

func downloadFile(ctx context.Context, fileURL string) ([]byte, error) {
	logger.LogInfo("[PDFCO] download  : GET %s", fileURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	logger.LogInfo("[PDFCO] download  : HTTP %d", resp.StatusCode)
	return io.ReadAll(resp.Body)
}

// saveCSVLocally writes csvBytes to internal/pdfco/output/<timestamp>_<originalName>.csv.
func saveCSVLocally(csvBytes []byte, originalFilename string) (string, error) {
	return saveFileLocally(csvBytes, originalFilename, ".csv")
}

// saveFileLocally writes data to internal/pdfco/output/<timestamp>_<originalName><ext>.
// ext must include the leading dot, e.g. ".csv" or ".xlsx".
func saveFileLocally(data []byte, originalFilename string, ext string) (string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", fmt.Errorf("create output dir: %w", err)
	}

	base := filepath.Base(originalFilename)
	origExt := filepath.Ext(base)
	nameNoExt := base[:len(base)-len(origExt)]
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s%s", timestamp, nameNoExt, ext)
	destPath := filepath.Join(outputDir, filename)

	logger.LogInfo("[PDFCO] save      : writing %d bytes → %s", len(data), destPath)
	if err := os.WriteFile(destPath, data, 0o644); err != nil {
		return "", fmt.Errorf("write file: %w", err)
	}
	return destPath, nil
}
