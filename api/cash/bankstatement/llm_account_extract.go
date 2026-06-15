package bankstatement

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

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/logger"
)

// LLMExtractTestHandler is a debug-only endpoint that runs the LLM account and
// column extraction against an uploaded file and returns the raw results — no
// DB reads or writes, no auth checks on the data itself.
//
// POST /cash/bank-statement/llm-extract
// Body: multipart/form-data with a "file" field (xlsx / xls / csv / numbers)
//
// Response shape:
//
//	{
//	  "success": true,
//	  "total_rows": 120,
//	  "header_rows": [["Row 0 cell0", ...], ...],   // first 40 rows shown for inspection
//	  "account_extraction": {
//	    "account_number": "1234567890",
//	    "account_name":   "ACME Corp Pvt Ltd",
//	    "error":          ""
//	  },
//	  "column_layout": {
//	    "header_row_index": 5,
//	    "columns":          { "date": 0, "description": 3, ... },
//	    "internal_col_idx": { "Date": 0, "transaction remarks": 3, ... },
//	    "header_row_cells": ["Date", "Value Date", "Tran. Id", ...],
//	    "error":            ""
//	  }
//	}
func LLMExtractTestHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)

		respond := func(v any) { json.NewEncoder(w).Encode(v) }
		fail := func(msg string) {
			respond(map[string]any{"success": false, "message": msg})
		}

		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			fail("POST required")
			return
		}

		if err := r.ParseMultipartForm(32 << 20); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fail("failed to parse multipart form: " + err.Error())
			return
		}

		file, _, err := r.FormFile("file")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fail(`missing "file" field in form`)
			return
		}
		defer file.Close()

		fileBytes, err := io.ReadAll(file)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fail("failed to read uploaded file: " + err.Error())
			return
		}

		rows, err := parseFileToRows(fileBytes)
		if err != nil {
			w.WriteHeader(http.StatusUnprocessableEntity)
			fail("failed to parse file (xlsx/xls/csv supported): " + err.Error())
			return
		}

		ctx := r.Context()

		// --- account extraction ---
		acctResult := map[string]any{"account_number": "", "account_name": "", "error": ""}
		acctInfo, acctErr := extractAccountInfoWithLLM(ctx, rows)
		if acctErr != nil {
			acctResult["error"] = acctErr.Error()
		} else {
			acctResult["account_number"] = acctInfo.AccountNumber
			acctResult["account_name"] = acctInfo.AccountName
		}

		// --- column layout ---
		colResult := map[string]any{
			"header_row_index": -1,
			"columns":          map[string]int{},
			"internal_col_idx": map[string]int{},
			"header_row_cells": []string{},
			"error":            "",
		}
		layout, colErr := extractColumnLayoutWithLLM(ctx, rows)
		if colErr != nil {
			colResult["error"] = colErr.Error()
		} else {
			colResult["header_row_index"] = layout.HeaderRowIndex
			colResult["columns"] = layout.Columns
			colResult["internal_col_idx"] = layout.toColIdx()
			if layout.HeaderRowIndex >= 0 && layout.HeaderRowIndex < len(rows) {
				colResult["header_row_cells"] = rows[layout.HeaderRowIndex]
			}
		}

		// first 40 rows for visual inspection in the response
		previewLimit := 40
		if len(rows) < previewLimit {
			previewLimit = len(rows)
		}

		respond(map[string]any{
			"success":            true,
			"total_rows":         len(rows),
			"header_rows":        rows[:previewLimit],
			"account_extraction": acctResult,
			"column_layout":      colResult,
		})
	})
}

type llmAccountInfo struct {
	AccountNumber string `json:"account_number"`
	AccountName   string `json:"account_name"`
}

// extractAccountInfoWithLLM sends the first 20 header rows to the configured
// inference endpoint and asks the model to extract the bank account number and name.
//
// Returns empty strings (not an error) when the model cannot find the values —
// the caller should treat that as "no result" and fall through to manual strategies.
// A non-nil error means the call itself failed (missing config, network, auth, bad JSON).
//
// Reads the same env vars used by the rest of the AI inference pipeline:
//
//	AI_INFERENCE_URL         — full chat-completions endpoint URL (required)
//	AI_INFERENCE_KEY         — Bearer token / API key (required)
//	AI_INFERENCE_MODEL       — model name; defaults to gpt-4o-mini
//	AI_INFERENCE_TIMEOUT_SEC — per-call timeout in seconds; defaults to 15
func extractAccountInfoWithLLM(ctx context.Context, rows [][]string) (llmAccountInfo, error) {
	inferURL := os.Getenv("AI_INFERENCE_URL")
	inferKey := os.Getenv("AI_INFERENCE_KEY")
	if inferURL == "" || inferKey == "" {
		return llmAccountInfo{}, fmt.Errorf("AI_INFERENCE_URL or AI_INFERENCE_KEY not set")
	}

	model := os.Getenv("AI_INFERENCE_MODEL")
	if model == "" {
		model = "gpt-4o-mini"
	}

	timeoutSec := 15
	if t := os.Getenv("AI_INFERENCE_TIMEOUT_SEC"); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			timeoutSec = n
		}
	}

	// Serialize the first 20 rows into a readable pipe-separated block.
	var sb strings.Builder
	for i, row := range rows {
		if i >= 20 {
			break
		}
		sb.WriteString(fmt.Sprintf("Row %d: %s\n", i, strings.Join(row, " | ")))
	}

	prompt := `You are parsing the header section of a bank statement spreadsheet.
Given the rows below, extract ONLY these two fields:
- account_number: the bank account number (digits only; strip all spaces and dashes)
- account_name: the account holder name or account nickname

Return ONLY valid JSON: {"account_number": "...", "account_name": "..."}
Use empty string "" for any field you cannot find with confidence. Do not guess.

Header rows:
` + sb.String()

	reqBody := map[string]interface{}{
		"model": model,
		"messages": []map[string]interface{}{
			{"role": "user", "content": prompt},
		},
		"response_format": map[string]string{"type": "json_object"},
		"max_tokens":      150,
		"temperature":     0,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return llmAccountInfo{}, fmt.Errorf("llm-acct: marshal: %w", err)
	}

	httpCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, http.MethodPost, inferURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return llmAccountInfo{}, fmt.Errorf("llm-acct: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+inferKey)
	req.Header.Set(constants.ContentTypeText, constants.ContentTypeJSON)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return llmAccountInfo{}, fmt.Errorf("llm-acct: http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return llmAccountInfo{}, fmt.Errorf("llm-acct: status=%d body=%.200s", resp.StatusCode, body)
	}

	var apiResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return llmAccountInfo{}, fmt.Errorf("llm-acct: decode: %w", err)
	}
	if len(apiResp.Choices) == 0 || strings.TrimSpace(apiResp.Choices[0].Message.Content) == "" {
		return llmAccountInfo{}, fmt.Errorf("llm-acct: empty response")
	}

	var info llmAccountInfo
	if err := json.Unmarshal([]byte(apiResp.Choices[0].Message.Content), &info); err != nil {
		return llmAccountInfo{}, fmt.Errorf("llm-acct: parse json: %w — raw: %.200s",
			err, apiResp.Choices[0].Message.Content)
	}

	// Normalize to match the shape the rest of the pipeline expects.
	info.AccountNumber = strings.ReplaceAll(
		strings.ReplaceAll(strings.TrimSpace(info.AccountNumber), "-", ""), " ", "")
	info.AccountName = strings.TrimSpace(info.AccountName)

	logger.LogInfo("[LLM-ACCT] model=%s extracted account_number=%q account_name=%q",
		model, info.AccountNumber, info.AccountName)
	return info, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Column layout detection
// ─────────────────────────────────────────────────────────────────────────────

type llmColumnLayout struct {
	HeaderRowIndex int            `json:"header_row_index"`
	Columns        map[string]int `json:"columns"`
}

func (l llmColumnLayout) col(name string) int {
	if l.Columns == nil {
		return -1
	}
	if v, ok := l.Columns[name]; ok {
		return v
	}
	return -1
}

// toColIdx maps the LLM semantic field names to the internal colIdx keys used
// by the transaction row parser.  Keys that the model returned as -1 are omitted
// so the downstream alias-expansion code can still fill them in.
func (l llmColumnLayout) toColIdx() map[string]int {
	out := map[string]int{}
	set := func(v int, keys ...string) {
		if v < 0 {
			return
		}
		for _, k := range keys {
			out[k] = v
		}
	}
	set(l.col("date"), "Date", transactionDateHeader)
	set(l.col("value_date"), valueDateHeader)
	set(l.col("tran_id"), tranIDHeader)
	set(l.col("description"), "Description", transactionRemarksHeader)
	set(l.col("withdrawal"), withdrawalAmtHeader, "Withdrawal")
	set(l.col("deposit"), depositAmtHeader, "Deposit")
	set(l.col("balance"), balanceHeader, "Balance")
	set(l.col("crdr"), "CrDr")
	set(l.col("timestamp"), "TimeStamp")
	set(l.col("posted_date"), "PostedDate")
	return out
}

// extractColumnLayoutWithLLM sends up to 40 rows to the inference endpoint and
// asks it to identify the header row index and column positions for each
// standard bank-statement field.
//
// Uses the same AI_INFERENCE_* env vars as extractAccountInfoWithLLM.
func extractColumnLayoutWithLLM(ctx context.Context, rows [][]string) (llmColumnLayout, error) {
	inferURL := os.Getenv("AI_INFERENCE_URL")
	inferKey := os.Getenv("AI_INFERENCE_KEY")
	if inferURL == "" || inferKey == "" {
		return llmColumnLayout{}, fmt.Errorf("AI_INFERENCE_URL or AI_INFERENCE_KEY not set")
	}

	model := os.Getenv("AI_INFERENCE_MODEL")
	if model == "" {
		model = "gpt-4o-mini"
	}

	timeoutSec := 30
	if t := os.Getenv("AI_INFERENCE_TIMEOUT_SEC"); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			timeoutSec = n
		}
	}

	// Serialize up to 100 rows showing both row index and per-cell column index.
	// The explicit [colIdx] prefix lets the model return an integer directly.
	var sb strings.Builder
	limit := len(rows)
	if limit > 100 {
		limit = 100
	}
	for i := 0; i < limit; i++ {
		sb.WriteString(fmt.Sprintf("Row %d:", i))
		for j, cell := range rows[i] {
			sb.WriteString(fmt.Sprintf(" [%d]%s", j, cell))
		}
		sb.WriteString("\n")
	}

	prompt := `You are parsing a bank statement spreadsheet converted from a PDF.
Given the rows below (format: "Row N: [colIndex]cellValue ..."), identify the header row and the correct column index for each field.

CRITICAL — PDF-to-CSV conversion often shifts data so it does not align with header labels. Follow these rules strictly:

RULE 1 — Balance column:
  The balance column contains the running account total that changes by the transaction amount each row.
  Look at consecutive data rows: balance[N] = balance[N-1] + deposit[N] - withdrawal[N].
  If the column whose header says "Balance" is EMPTY in data rows, it is NOT the balance column.
  Find the column that actually contains the running totals in data rows and use that index for "balance".

RULE 2 — Withdrawal / Deposit columns:
  Withdrawal and deposit columns contain individual transaction amounts (relatively small, varying).
  If a column labeled "Deposits" contains large values that change like a running total across ALL rows
  (including rows that are obviously ATM withdrawals or fee charges), that column is the BALANCE column, not deposits.
  A genuine deposit column will be EMPTY (or zero) in rows that are clearly debits.

RULE 3 — Value Date:
  A value_date column must contain dates. If that column has numeric amounts in data rows, it is not the value date.

RULE 4 — Single combined amount column:
  If there is only ONE column for transaction amounts (both credits and debits share the same column),
  set both "withdrawal" and "deposit" to that same column index.

Fields to identify:
   - date        : primary transaction date column
   - value_date  : column with value/posting dates (must have dates in data rows); fall back to date index if none
   - tran_id     : transaction ID / reference / cheque number column
   - description : narration / remarks / description / particulars column
   - withdrawal  : debit / withdrawal / money-out amount column
   - deposit     : credit / deposit / money-in amount column
   - balance     : running balance column (MUST have running totals in data rows — not empty)
   - crdr        : Cr/Dr or Credit/Debit indicator column (if present, else -1)
   - posted_date : cleared / posted date if separate from value date (-1 otherwise)
   - timestamp   : time-of-day stamp column if present (-1 otherwise)

Return ONLY valid JSON matching this exact schema (all fields required, use -1 when absent):
{
  "header_row_index": <int>,
  "columns": {
    "date": <int>, "value_date": <int>, "tran_id": <int>,
    "description": <int>, "withdrawal": <int>, "deposit": <int>,
    "balance": <int>, "crdr": <int>, "posted_date": <int>, "timestamp": <int>
  }
}

Rows:
` + sb.String()

	reqBody := map[string]interface{}{
		"model": model,
		"messages": []map[string]interface{}{
			{"role": "user", "content": prompt},
		},
		"response_format": map[string]string{"type": "json_object"},
		"max_tokens":      400,
		"temperature":     0,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return llmColumnLayout{}, fmt.Errorf("llm-cols: marshal: %w", err)
	}

	httpCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(httpCtx, http.MethodPost, inferURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return llmColumnLayout{}, fmt.Errorf("llm-cols: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+inferKey)
	req.Header.Set(constants.ContentTypeText, constants.ContentTypeJSON)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return llmColumnLayout{}, fmt.Errorf("llm-cols: http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return llmColumnLayout{}, fmt.Errorf("llm-cols: status=%d body=%.200s", resp.StatusCode, body)
	}

	var apiResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return llmColumnLayout{}, fmt.Errorf("llm-cols: decode: %w", err)
	}
	if len(apiResp.Choices) == 0 || strings.TrimSpace(apiResp.Choices[0].Message.Content) == "" {
		return llmColumnLayout{}, fmt.Errorf("llm-cols: empty response")
	}

	var layout llmColumnLayout
	if err := json.Unmarshal([]byte(apiResp.Choices[0].Message.Content), &layout); err != nil {
		return llmColumnLayout{}, fmt.Errorf("llm-cols: parse json: %w — raw: %.200s",
			err, apiResp.Choices[0].Message.Content)
	}
	if layout.HeaderRowIndex < 0 || layout.HeaderRowIndex >= len(rows) {
		return llmColumnLayout{}, fmt.Errorf("llm-cols: header_row_index=%d out of range (rows=%d)",
			layout.HeaderRowIndex, len(rows))
	}

	logger.LogInfo("[LLM-COLS] model=%s header_row=%d cols=%v", model, layout.HeaderRowIndex, layout.Columns)
	layout = fixMisalignedBalanceColumn(layout, rows)
	return layout, nil
}

// colFillRate returns the fraction of the first sampleSize data rows (starting at
// dataStart) that have a non-empty numeric value in the given column.
func colFillRate(rows [][]string, dataStart, col, sampleSize int) float64 {
	nonEmpty, sampled := 0, 0
	for i := dataStart; i < len(rows) && sampled < sampleSize; i++ {
		if col >= len(rows[i]) {
			sampled++
			continue
		}
		v := strings.TrimSpace(rows[i][col])
		if v == "" {
			sampled++
			continue
		}
		sampled++
		if _, err := strconv.ParseFloat(strings.ReplaceAll(strings.ReplaceAll(v, ",", ""), " ", ""), 64); err == nil {
			nonEmpty++
		}
	}
	if sampled == 0 {
		return 0
	}
	return float64(nonEmpty) / float64(sampled)
}

// fixMisalignedBalanceColumn corrects a common finparse PDF-to-CSV artifact where the
// balance column label ends up one position to the right of the actual balance data.
//
// Strategy:
//  1. If the LLM-identified balance column is mostly empty, look for another column that
//     is non-empty in 90%+ of data rows (characteristic of a running-total column).
//  2. Once the real balance column is found, verify the LLM's amount columns (withdrawal /
//     deposit) also have data. If they're empty, scan for the nearest numeric column to
//     the balance column and use that as the single-amount column.
func fixMisalignedBalanceColumn(layout llmColumnLayout, rows [][]string) llmColumnLayout {
	dataStart := layout.HeaderRowIndex + 1
	if dataStart >= len(rows) {
		return layout
	}

	balIdx := layout.col("balance")
	wIdx := layout.col("withdrawal")
	dIdx := layout.col("deposit")

	// ── Step 1: find the real balance column ─────────────────────────────────
	realBalIdx := balIdx
	if balIdx < 0 || colFillRate(rows, dataStart, balIdx, 10) <= 0.5 {
		// LLM balance column is absent or mostly empty — scan withdrawal / deposit columns.
		for _, candidateIdx := range []int{wIdx, dIdx} {
			if candidateIdx < 0 {
				continue
			}
			if colFillRate(rows, dataStart, candidateIdx, 10) >= 0.9 {
				realBalIdx = candidateIdx
				break
			}
		}
	}

	if realBalIdx == balIdx && wIdx == layout.col("withdrawal") && dIdx == layout.col("deposit") {
		return layout // Nothing to fix.
	}

	newCols := make(map[string]int, len(layout.Columns))
	for k, v := range layout.Columns {
		newCols[k] = v
	}
	newCols["balance"] = realBalIdx

	// ── Step 2: find the real amount column ──────────────────────────────────
	// The LLM's surviving withdrawal / deposit column might itself be empty.
	// Determine what non-balance numeric column actually has transaction amounts.
	amountIdx := -1

	// First try: whichever of {wIdx, dIdx} is NOT the new balance and has data.
	for _, c := range []int{wIdx, dIdx} {
		if c < 0 || c == realBalIdx {
			continue
		}
		if colFillRate(rows, dataStart, c, 10) >= 0.4 {
			amountIdx = c
			break
		}
	}

	// Second try: if neither wIdx nor dIdx works, scan all columns for a numeric
	// column adjacent to realBalIdx (the finparse shift is usually ±1..3 columns).
	if amountIdx < 0 && realBalIdx >= 0 {
		maxCols := 0
		for _, row := range rows {
			if len(row) > maxCols {
				maxCols = len(row)
			}
		}
		for dist := 1; dist <= 5 && amountIdx < 0; dist++ {
			for _, candidate := range []int{realBalIdx - dist, realBalIdx + dist} {
				if candidate < 0 || candidate >= maxCols || candidate == realBalIdx {
					continue
				}
				if colFillRate(rows, dataStart, candidate, 10) >= 0.4 {
					amountIdx = candidate
					break
				}
			}
		}
	}

	if amountIdx >= 0 {
		newCols["withdrawal"] = amountIdx
		newCols["deposit"] = amountIdx
	} else {
		newCols["withdrawal"] = -1
		newCols["deposit"] = -1
	}

	logger.LogInfo("[LLM-COLS] balance-fix: balance col=%d amount col=%d (single-amount mode)", realBalIdx, amountIdx)
	layout.Columns = newCols
	return layout
}
