package bankstatement

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/bindref"
	"CimplrCorpSaas/internal/logger"
)

// flexFloat tolerates amounts that the model returns either as JSON numbers or as quoted
// strings with currency formatting ("1,234.50", "-", ""). Anything unparseable becomes 0.
type flexFloat float64

func (f *flexFloat) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(strings.Trim(string(b), `"`))
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "₹", "")
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "null") || s == "-" {
		*f = 0
		return nil
	}
	if strings.HasSuffix(s, "Cr") || strings.HasSuffix(s, "cr") {
		s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(s, "Cr"), "cr"))
	}
	if strings.HasSuffix(s, "Dr") || strings.HasSuffix(s, "dr") {
		s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(s, "Dr"), "dr"))
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		*f = 0
		return nil
	}
	*f = flexFloat(v)
	return nil
}

// llmExtractedTxn is one transaction as returned by the LLM extractor.
type llmExtractedTxn struct {
	Date        string    `json:"date"`
	ValueDate   string    `json:"value_date"`
	Ref         string    `json:"ref"`
	Description string    `json:"description"`
	Withdrawal  flexFloat `json:"withdrawal"`
	Deposit     flexFloat `json:"deposit"`
	Balance     flexFloat `json:"balance"`
}

type llmTxnResponse struct {
	OpeningBalance *flexFloat        `json:"opening_balance"`
	Transactions   []llmExtractedTxn `json:"transactions"`
}

// llmTxnChunkSize bounds how many CSV rows are sent per LLM call. We ALWAYS chunk (rather than
// sending the whole statement) because a single large call is where transactions silently go
// missing: the JSON output overruns the model's token budget and is truncated, or the model
// "compresses" long repetitive runs. 150 rows keeps each call's structured output comfortably
// within the budget. The header rows are repeated and the running balance is threaded into each
// chunk so column meaning and the balance chain survive the boundaries; transactions are
// concatenated across chunks.
const llmTxnChunkSize = 150

// llmMinAdaptiveChunk is the floor for truncation-driven splitting: when a chunk still reports a
// truncated (finish_reason="length") response, it is halved and retried down to this size. Below it
// we accept whatever came back (the count backstop in buildTxnMapsFromLLM then routes a genuine
// shortfall to the deterministic parser).
const llmMinAdaptiveChunk = 25

// llmTxnMaxTokens caps the model's output per call. Set high enough that a full llmTxnChunkSize
// chunk fits, so finish_reason="length" reliably signals a real overflow rather than a stingy cap.
const llmTxnMaxTokens = 16384

// llmHeaderRowSpan is how many leading rows (column headers, opening-balance line, page banner) we
// repeat at the top of every chunk so the model keeps column context.
const llmHeaderRowSpan = 12

// extractTransactionsWithLLM is the LLM-first extractor: it sends the converter CSV rows to
// the inference endpoint and asks for the transactions as structured JSON. Because the model
// reasons semantically (a running total is a balance, a debit lowers it), it is robust to the
// converter scrambling columns or splitting one transaction across several rows — the exact
// failures that defeat position-based parsing. The caller MUST validate the result against the
// running-balance identity before trusting it (the model can mis-transcribe a digit).
// llmTxnDefaultModel is the model used for bank-statement transaction extraction when neither the
// BANK_STATEMENT_LLM_MODEL env var nor the bound config overrides it. gpt-4.1 is materially more
// accurate and faster than gpt-4o-mini on the long, split-row statements this extractor sees.
const llmTxnDefaultModel = "gpt-4.1"

// resolveLLMTxnModel picks the extraction model: the BANK_STATEMENT_LLM_MODEL env var when set,
// otherwise the gpt-4.1 default. The legacy bound model (bindref.BrG3) is intentionally NOT used
// here — it is pinned to gpt-4o-mini, which is too weak for these statements.
func resolveLLMTxnModel() string {
	if m := strings.TrimSpace(os.Getenv("BANK_STATEMENT_LLM_MODEL")); m != "" {
		return m
	}
	return llmTxnDefaultModel
}

func extractTransactionsWithLLM(ctx context.Context, rows [][]string) (*llmTxnResponse, error) {
	inferURL := bindref.BrG1()
	inferKey := bindref.BrG2()
	if inferURL == "" || inferKey == "" {
		return nil, fmt.Errorf("AI inference not configured")
	}
	model := resolveLLMTxnModel()

	// Always chunk. Sending the whole statement in one call is exactly where transactions go missing
	// — the structured-JSON output overruns the token budget and is truncated, or the model collapses
	// long repetitive runs. Each chunk repeats the header rows so column meaning survives, and the
	// previous chunk's closing balance is threaded forward so the running-balance chain (and the
	// debit/balance disambiguation that depends on it) does not reset across boundaries.
	out := &llmTxnResponse{}
	headerRows := rows[:min(llmHeaderRowSpan, len(rows))]
	var prevBalance *float64
	for start := 0; start < len(rows); start += llmTxnChunkSize {
		end := min(start+llmTxnChunkSize, len(rows))
		isFirst := start == 0
		var hdr [][]string
		if !isFirst {
			hdr = headerRows
		}
		resp, err := callLLMTxnChunkAdaptive(ctx, inferURL, inferKey, model, rows[start:end], hdr, isFirst, prevBalance)
		if err != nil {
			return nil, fmt.Errorf("llm-txn: chunk [%d:%d]: %w", start, end, err)
		}
		if isFirst && resp.OpeningBalance != nil {
			out.OpeningBalance = resp.OpeningBalance
		}
		out.Transactions = append(out.Transactions, resp.Transactions...)
		if n := len(resp.Transactions); n > 0 {
			b := float64(resp.Transactions[n-1].Balance)
			prevBalance = &b
		}
	}
	logger.LogInfo("[LLM-TXN] model=%s extracted %d transactions from %d rows (%d-row chunks)",
		model, len(out.Transactions), len(rows), llmTxnChunkSize)
	return out, nil
}

// callLLMTxnChunkAdaptive calls the model for one chunk and, if the response was truncated
// (finish_reason="length") — meaning some transactions were cut off — halves the chunk and retries
// each half, threading the running balance and repeating the header context, until the pieces fit or
// the floor (llmMinAdaptiveChunk) is reached. This is what guarantees every row is returned on large
// multi-page statements instead of silently losing the rows that overran the output budget.
func callLLMTxnChunkAdaptive(ctx context.Context, inferURL, inferKey, model string, chunk, headerRows [][]string, isFirst bool, prevBalance *float64) (*llmTxnResponse, error) {
	resp, truncated, err := callLLMTxnChunk(ctx, inferURL, inferKey, model, chunk, headerRows, isFirst, prevBalance)
	if err != nil {
		return nil, err
	}
	if !truncated || len(chunk) <= llmMinAdaptiveChunk {
		return resp, nil
	}

	logger.LogInfo("[LLM-TXN] chunk of %d rows truncated (finish_reason=length) — splitting and retrying", len(chunk))
	mid := len(chunk) / 2
	left, lerr := callLLMTxnChunkAdaptive(ctx, inferURL, inferKey, model, chunk[:mid], headerRows, isFirst, prevBalance)
	if lerr != nil {
		return nil, lerr
	}
	// Thread the left half's closing balance into the right half so the chain continues; on the right
	// half the header rows are always supplied as context (it is never the statement's first chunk).
	nextPrev := prevBalance
	if n := len(left.Transactions); n > 0 {
		b := float64(left.Transactions[n-1].Balance)
		nextPrev = &b
	}
	rightHeader := headerRows
	if rightHeader == nil {
		rightHeader = chunk[:min(llmHeaderRowSpan, len(chunk))]
	}
	right, rerr := callLLMTxnChunkAdaptive(ctx, inferURL, inferKey, model, chunk[mid:], rightHeader, false, nextPrev)
	if rerr != nil {
		return nil, rerr
	}
	merged := &llmTxnResponse{OpeningBalance: left.OpeningBalance}
	merged.Transactions = append(merged.Transactions, left.Transactions...)
	merged.Transactions = append(merged.Transactions, right.Transactions...)
	return merged, nil
}

// callLLMTxnChunk issues a single extraction request. It returns truncated=true when the model
// stopped because it hit the output-token cap (finish_reason="length") — the signal that some
// transactions were cut off — so the caller can split the chunk and retry instead of silently
// accepting a short result.
func callLLMTxnChunk(ctx context.Context, inferURL, inferKey, model string, chunk, headerRows [][]string, isFirst bool, prevBalance *float64) (*llmTxnResponse, bool, error) {
	var sb strings.Builder
	if len(headerRows) > 0 {
		sb.WriteString("Header/context rows (for column meaning only — do NOT extract transactions from these):\n")
		for _, row := range headerRows {
			sb.WriteString("Header:")
			for _, cell := range row {
				sb.WriteString(" | ")
				sb.WriteString(strings.TrimSpace(cell))
			}
			sb.WriteString("\n")
		}
		sb.WriteString("Transaction rows:\n")
	}
	for i, row := range chunk {
		sb.WriteString(fmt.Sprintf("Row %d:", i))
		for _, cell := range row {
			sb.WriteString(" | ")
			sb.WriteString(strings.TrimSpace(cell))
		}
		sb.WriteString("\n")
	}

	prompt := `You are extracting ledger transactions from a bank statement that was converted from PDF to CSV.
The conversion is unreliable: columns may be misaligned, a single transaction may be split across several rows
(date on one row, amount/balance on another, description wrapped over multiple rows), and there are header,
summary and footer rows mixed in.

Use MEANING, not column position, to extract each transaction:
- The BALANCE is the running account total. It changes every transaction and satisfies:
      balance[N] = balance[N-1] - withdrawal[N] + deposit[N]
- A WITHDRAWAL (debit / "amount subtracted" / money out) decreases the balance.
- A DEPOSIT (credit / "amount added" / money in) increases the balance.
- Each transaction has exactly ONE of withdrawal or deposit (the other is 0).
- CRITICAL: the running BALANCE and the transaction AMOUNT are different numbers. The balance is
  usually the LARGEST number on the row and stays close to the previous row's balance; the
  withdrawal/deposit is the smaller delta. NEVER copy the balance into the withdrawal or deposit
  field. Verify each row with balance[N] = balance[N-1] - withdrawal[N] + deposit[N]; if it does
  not hold, you have swapped the balance and the amount — fix it before returning.
- Merge wrapped/continuation rows into the single transaction they belong to (combine description text,
  and pull the amount/balance from whichever row carries them).
- COPY the balance VERBATIM from the row's Balance column — never compute, round, or invent it. Every
  balance you output MUST be a number that literally appears in the rows below. If you cannot find a
  balance for a transaction, leave its balance as the previous balance adjusted by the amount; do NOT
  fabricate a new number just to make the math close.
- When the rows have clear, consistently aligned Debit/Withdrawal, Credit/Deposit and Balance columns,
  TRUST those columns: read the debit and credit straight from them. Only fall back to inferring
  direction from the balance movement when the columns are visibly misaligned or empty.
- SKIP non-transactions: column headers, opening/closing-balance summary lines, page headers/footers,
  "brought forward" labels, totals, and marketing text.

COMPLETENESS IS MANDATORY:
- Return EVERY transaction row in the input. Do NOT skip, omit, summarize, deduplicate, abbreviate,
  or collapse rows — even when many consecutive rows look almost identical (repeated charges, GST/CGST/
  SGST lines, reversals). Each is a distinct transaction and must appear separately.
- Output exactly one JSON object per transaction, after merging ONLY the wrapped continuation lines of
  the SAME transaction. Never merge two different transactions into one. When unsure, include the row.
- Process rows strictly in order; the last transaction you output must correspond to the last
  transaction row in the input. Do not stop early.

For each transaction output:
  date        : transaction date as DD-MM-YYYY (or the statement's own format if unclear)
  value_date  : value/posting date if present, else same as date
  ref         : cheque/reference/UTR/transaction id if present, else ""
  description : full narration (continuation lines joined)
  withdrawal  : debit amount as a plain number (no commas), 0 if none
  deposit     : credit amount as a plain number (no commas), 0 if none
  balance     : running balance after this transaction as a plain number (no commas)

Also report opening_balance: the balance BEFORE the first transaction (from an "Opening Balance" /
"Balance Brought Forward" row if present, else null).

Return ONLY valid JSON in exactly this shape:
{
  "opening_balance": <number or null>,
  "transactions": [
    {"date":"","value_date":"","ref":"","description":"","withdrawal":0,"deposit":0,"balance":0}
  ]
}

Rows:
`
	if !isFirst {
		prompt += "(This is a continuation chunk; opening_balance may be null.)\n"
	}
	if prevBalance != nil {
		prompt += fmt.Sprintf("(The balance AFTER the last transaction of the previous chunk was %.2f; "+
			"the first transaction below must continue the running balance from there.)\n", *prevBalance)
	}
	prompt += sb.String()

	reqBody := map[string]interface{}{
		"model": model,
		"messages": []map[string]interface{}{
			{"role": "user", "content": prompt},
		},
		"response_format": map[string]string{"type": "json_object"},
		"temperature":     0,
		"max_tokens":      llmTxnMaxTokens,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, false, fmt.Errorf("marshal: %w", err)
	}

	var apiResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}

	// Retry transient failures — chiefly 429 (the org's gpt-4.1 TPM cap is easily hit when chunks
	// fire back-to-back). Honour the API's Retry-After hint, falling back to exponential backoff.
	const maxAttempts = 5
	for attempt := 1; ; attempt++ {
		httpCtx, cancel := context.WithTimeout(ctx, 180*time.Second)
		req, err := http.NewRequestWithContext(httpCtx, http.MethodPost, inferURL, bytes.NewReader(bodyBytes))
		if err != nil {
			cancel()
			return nil, false, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+inferKey)
		req.Header.Set(constants.ContentTypeText, constants.ContentTypeJSON)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			return nil, false, fmt.Errorf("http: %w", err)
		}
		if resp.StatusCode == http.StatusOK {
			decErr := json.NewDecoder(resp.Body).Decode(&apiResp)
			resp.Body.Close()
			cancel()
			if decErr != nil {
				return nil, false, fmt.Errorf("decode: %w", decErr)
			}
			break
		}

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		wait := parseRetryAfter(resp.Header.Get("Retry-After"), string(body))
		retryable := resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusInternalServerError ||
			resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusGatewayTimeout
		statusCode := resp.StatusCode
		resp.Body.Close()
		cancel()
		if !retryable || attempt >= maxAttempts {
			return nil, false, fmt.Errorf("status=%d body=%.200s", statusCode, body)
		}
		if wait <= 0 {
			wait = time.Duration(1<<uint(attempt-1)) * 4 * time.Second // 4s, 8s, 16s, 32s
		}
		wait += 500 * time.Millisecond // small cushion past the window boundary
		logger.LogInfo("[LLM-TXN] status=%d — retrying in %s (attempt %d/%d)", statusCode, wait, attempt, maxAttempts)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}

	// finish_reason="length" means the model ran out of output budget mid-response — some
	// transactions were cut off. Signal it so the adaptive caller splits the chunk and retries.
	truncated := len(apiResp.Choices) > 0 && apiResp.Choices[0].FinishReason == "length"

	content := ""
	if len(apiResp.Choices) > 0 {
		content = strings.TrimSpace(apiResp.Choices[0].Message.Content)
	}
	if content == "" {
		if truncated {
			return &llmTxnResponse{}, true, nil
		}
		return nil, false, fmt.Errorf("empty response")
	}

	var parsed llmTxnResponse
	if err := json.Unmarshal([]byte(content), &parsed); err != nil {
		// A truncated response is often invalid JSON (cut mid-array); treat it as a split signal
		// rather than a hard failure so the caller can retry the chunk in smaller pieces.
		if truncated {
			return &llmTxnResponse{}, true, nil
		}
		return nil, false, fmt.Errorf("parse json: %w — raw: %.200s", err, content)
	}
	return &parsed, truncated, nil
}

// retryAfterBodyRe extracts the "Please try again in 7.5s" / "try again in 200ms" hint OpenAI
// embeds in 429 bodies when no Retry-After header is present.
var retryAfterBodyRe = regexp.MustCompile(`try again in ([0-9.]+)(ms|s)`)

// parseRetryAfter returns how long to wait before retrying, preferring the Retry-After header
// (integer seconds) and falling back to the hint inside the response body. Zero means "no hint".
func parseRetryAfter(header, body string) time.Duration {
	if h := strings.TrimSpace(header); h != "" {
		if secs, err := strconv.ParseFloat(h, 64); err == nil && secs > 0 {
			return time.Duration(secs * float64(time.Second))
		}
	}
	if m := retryAfterBodyRe.FindStringSubmatch(body); m != nil {
		if v, err := strconv.ParseFloat(m[1], 64); err == nil && v > 0 {
			if m[2] == "ms" {
				return time.Duration(v * float64(time.Millisecond))
			}
			return time.Duration(v * float64(time.Second))
		}
	}
	return 0
}

// llmTxnContext carries the master-data fields needed to shape extracted transactions exactly
// like the deterministic parser's output.
type llmTxnContext struct {
	accountNumber string
	entityID      string
	bankName      string
	currency      string
	rules         []categoryRuleComponent
	batchID       string
	// sourceRows is the raw converter CSV. The extracted running balances are cross-checked
	// against the numbers literally present here: a balance the model "computed" but that never
	// appears in the statement is a mis-extraction, even when the chain is internally consistent.
	sourceRows [][]string
}

// collectSourceNumbers returns every numeric value present in the source CSV, keyed to the cent so
// floating-point noise cannot cause a false miss. Used to tell a real balance from one the model
// invented.
func collectSourceNumbers(rows [][]string) map[int64]struct{} {
	set := make(map[int64]struct{})
	for _, row := range rows {
		for _, cell := range row {
			// Reuse the shared strict amount parser so balances carrying a currency/direction suffix
			// ("22.91 CR", "1,47,103.00 Cr.") are recognised. The previous inline cleaning used a
			// case-sensitive TrimSuffix("Cr") that silently dropped every "... CR" balance, which made
			// the cross-check flag all valid balances as ghosts and discard a correct LLM extraction.
			if v, ok := parseStrictAmount(cell); ok {
				set[int64(math.Round(v*100))] = struct{}{}
			}
		}
	}
	return set
}

// inSourceSet reports whether v (a currency amount) appears in the source-number set.
func inSourceSet(set map[int64]struct{}, v float64) bool {
	_, ok := set[int64(math.Round(v*100))]
	return ok
}

// repairGhostBalances replaces balances the model invented (ones that never appear in the source
// CSV) with the real balance from the source. The true balance is the source number b for which the
// transaction amount into it (|b - prev|) and out of it (|next - b|) are BOTH amounts that also
// appear in the source — so a single split-row slip is corrected from the surrounding anchors
// rather than poisoning the ledger. Returns how many were fixed and how many could not be.
func repairGhostBalances(opening float64, views []amtRow, srcNums map[int64]struct{}) (fixed, unfixed int) {
	cands := make([]float64, 0, len(srcNums))
	for c := range srcNums {
		cands = append(cands, float64(c)/100.0)
	}
	for i := range views {
		if inSourceSet(srcNums, views[i].bal) {
			continue
		}
		prev := opening
		if i > 0 {
			prev = views[i-1].bal
		}
		hasNext := i+1 < len(views)
		nextKnown := hasNext && inSourceSet(srcNums, views[i+1].bal)

		best := math.NaN()
		bestLeg := math.MaxFloat64
		for _, b := range cands {
			legIn := math.Abs(b - prev)
			if legIn < 0.005 || !inSourceSet(srcNums, legIn) {
				continue
			}
			if nextKnown {
				if legOut := math.Abs(views[i+1].bal - b); !inSourceSet(srcNums, legOut) {
					continue
				}
			}
			// Prefer the candidate whose incoming amount is smallest; combined with the outgoing-leg
			// constraint this resolves to the genuine balance in the overwhelming majority of cases.
			if legIn < bestLeg {
				bestLeg = legIn
				best = b
			}
		}
		if !math.IsNaN(best) {
			views[i].bal = best
			fixed++
		} else {
			unfixed++
		}
	}
	return fixed, unfixed
}

// buildTxnMapsFromLLM converts LLM-extracted transactions into the standard preview txn maps and
// validates them against the running-balance identity. It returns ok=false when the extraction
// is too inconsistent to trust, so the caller can fall back to the deterministic parser. The
// minReconcileRate fraction of rows must satisfy balance[N] = prev - withdrawal + deposit.
func buildTxnMapsFromLLM(resp *llmTxnResponse, ctx llmTxnContext) ([]map[string]interface{}, bool) {
	if resp == nil || len(resp.Transactions) == 0 {
		return nil, false
	}

	// Backstop against the model silently dropping transactions on large / multi-page statements.
	// Because each amount is reconstructed from the balance DELTA below, a dropped row is invisible
	// to the running-balance reconciliation: the gap is absorbed into the next row's delta, the chain
	// still closes, and the opening/closing totals still match — but transactions are lost and the
	// boundary amounts are merged. The independent signal is COUNT: a cleanly-aligned statement
	// exposes one complete (date+balance) row per transaction, so the extractor must return roughly
	// that many. A large shortfall means rows were dropped despite the adaptive chunking — fall back
	// to the positional parser, which reads every row. (Messy split-row statements have few complete
	// rows, so this never fires for the case the LLM path exists to handle.)
	if expected := countCompleteTransactionRows(ctx.sourceRows); expected >= 10 && len(resp.Transactions)*10 < expected*9 {
		logger.LogInfo("[LLM-TXN] extracted %d transactions but source has %d complete transaction rows — dropped rows; falling back to deterministic parser",
			len(resp.Transactions), expected)
		return nil, false
	}

	// Determine opening balance: explicit from the model, else derived from the first row
	// (balance + withdrawal - deposit).
	var opening float64
	openingKnown := false
	if resp.OpeningBalance != nil {
		opening = float64(*resp.OpeningBalance)
		openingKnown = true
	} else {
		f := resp.Transactions[0]
		opening = float64(f.Balance) + float64(f.Withdrawal) - float64(f.Deposit)
		openingKnown = true
	}

	// Validate the chain and repair debit↔balance swaps.
	views := make([]amtRow, len(resp.Transactions))
	for i, t := range resp.Transactions {
		views[i] = amtRow{w: float64(t.Withdrawal), d: float64(t.Deposit), bal: float64(t.Balance)}
	}
	reconcileLLMChain(opening, views)

	reconciled := 0
	running := opening
	for i := range views {
		if absf(running-views[i].w+views[i].d-views[i].bal) <= 0.01 {
			reconciled++
		}
		running = views[i].bal
	}
	rate := float64(reconciled) / float64(len(views))
	logger.LogInfo("[LLM-TXN] validation: %d/%d rows reconcile (%.0f%%) opening=%.2f",
		reconciled, len(views), rate*100, opening)
	if rate < 0.85 {
		logger.LogInfo("[LLM-TXN] reconcile rate below threshold — falling back to deterministic parser")
		return nil, false
	}

	// Repair invented balances BEFORE deriving amounts. A self-consistent chain can still be
	// FACTUALLY wrong: when the converter splits a row the model may invent an intermediate balance
	// that keeps the math closing (e.g. it read +6/-130 as -6/-118, landing on the same later
	// balance). Such a balance never appears in the source CSV, whereas the TRUE balance does — so
	// we snap each invented balance to the real source number that makes both its incoming and
	// outgoing transaction amounts match numbers that also appear in the source.
	var srcNums map[int64]struct{}
	if len(ctx.sourceRows) > 0 {
		srcNums = collectSourceNumbers(ctx.sourceRows)
		fixed, unfixed := repairGhostBalances(opening, views, srcNums)
		if fixed > 0 || unfixed > 0 {
			logger.LogInfo("[LLM-TXN] balance cross-check: repaired %d invented balances, %d unrepairable (of %d)",
				fixed, unfixed, len(views))
		}
		// Only bail out when a large share of balances are invented AND cannot be recovered from the
		// source — that signals a wholesale mis-extraction, not a few split-row slips.
		if float64(unfixed)/float64(len(views)) > 0.10 {
			logger.LogInfo("[LLM-TXN] too many unrecoverable balances — falling back to deterministic parser")
			return nil, false
		}
	}

	// Re-anchor the FINAL balance against the statement's own declared closing (summary / "Page
	// Total" / "Closing Balance" row). The last transaction has no following row, so neither the
	// chain reconciliation nor the ghost-balance repair can catch a wrong closing balance — and when
	// the wrong value happens to equal a balance from earlier in the statement it slips through as
	// "present in source". A wrong closing then zeroes the final transaction's amount once amounts
	// are derived from balance deltas below (e.g. a sweep-to-nil debit silently vanishes).
	//
	// Guard against a misread declared closing: only re-anchor when the resulting final-row amount
	// (|declared − previous balance|) is itself an amount that literally appears in the source, or is
	// zero. That keeps a correct extraction untouched while fixing the genuine miss.
	if n := len(views); n > 0 && len(ctx.sourceRows) > 0 {
		if declaredClosing, ok := scanDeclaredClosingBalance(ctx.sourceRows); ok && absf(views[n-1].bal-declaredClosing) > 0.01 {
			prev := opening
			if n > 1 {
				prev = views[n-2].bal
			}
			impliedAmt := absf(declaredClosing - prev)
			if impliedAmt < 0.01 || inSourceSet(srcNums, impliedAmt) {
				logger.LogInfo("[LLM-TXN] closing cross-check: extracted last balance=%.2f != declared closing=%.2f (implied amount=%.2f) — re-anchoring final row",
					views[n-1].bal, declaredClosing, impliedAmt)
				views[n-1].bal = declaredClosing
			} else {
				logger.LogInfo("[LLM-TXN] closing cross-check: declared closing=%.2f disagrees with extracted last balance=%.2f but implied amount=%.2f is absent from source — leaving final row unchanged",
					declaredClosing, views[n-1].bal, impliedAmt)
			}
		}
	}

	// Now derive every transaction's amount from the (repaired) balance DELTA rather than the
	// model's own withdrawal/deposit cells. The running balance is the most reliably extracted
	// column, whereas the amount is frequently placed in the wrong column or mis-transcribed.
	// Deriving the amount from balance[N] - balance[N-1] makes each row self-consistent with the
	// ledger, so a wrong amount in the MIDDLE of the statement no longer breaks the running balance.
	repaired := 0
	prev := opening
	for i := range views {
		delta := views[i].bal - prev
		var w, d float64
		switch {
		case delta <= -0.01:
			w = -delta
		case delta >= 0.01:
			d = delta
		}
		if absf(views[i].w-w) > 0.01 || absf(views[i].d-d) > 0.01 {
			repaired++
		}
		views[i].w, views[i].d = w, d
		prev = views[i].bal
	}
	if repaired > 0 {
		logger.LogInfo("[LLM-TXN] reconstructed %d/%d amounts from balance deltas", repaired, len(views))
	}

	out := make([]map[string]interface{}, 0, len(resp.Transactions))
	seq := 0
	for i, t := range resp.Transactions {
		td := parseLLMDate(t.Date)
		if td.IsZero() {
			td = parseLLMDate(t.ValueDate)
		}
		if td.IsZero() {
			// A transaction with no parseable date is almost certainly a mis-extracted
			// summary line; skip it rather than poison the ledger.
			continue
		}
		vd := parseLLMDate(t.ValueDate)
		if vd.IsZero() {
			vd = td
		}

		tranID := strings.TrimSpace(t.Ref)
		if tranID == "" {
			tranID = buildSyntheticTranID(ctx.batchID, seq)
		}
		seq++

		w := views[i].w
		d := views[i].d
		bal := views[i].bal

		txn := map[string]interface{}{
			"account_number":     ctx.accountNumber,
			"entity_id":          ctx.entityID,
			"entity_name":        ctx.entityID,
			"bank_name":          ctx.bankName,
			"currency":           ctx.currency,
			"misclassified_flag": false,
			"tran_id":            tranID,
			"transaction_date":   td.Format(time.RFC3339),
			"value_date":         vd.Format(time.RFC3339),
			"description":        sanitizeForPostgres(normalizeCell(t.Description)),
			"withdrawal_amount":  w,
			"deposit_amount":     d,
			"balance":            bal,
		}

		catID := matchCategoryForTransaction(ctx.rules, t.Description,
			sql.NullFloat64{Float64: w, Valid: w > 0},
			sql.NullFloat64{Float64: d, Valid: d > 0},
			sql.NullTime{Time: vd, Valid: !vd.IsZero()})
		if catID.Valid && catID.String != "" {
			txn["category_id"] = catID.String
			for _, r := range ctx.rules {
				if r.CategoryID == catID.String {
					txn["category_name"] = r.CategoryName
					break
				}
			}
		} else {
			txn["category_name"] = "Uncategorized"
		}

		out = append(out, txn)
	}

	if len(out) == 0 {
		return nil, false
	}
	if openingKnown {
		out[0]["_parser_opening_balance"] = opening
	}
	return out, true
}

func parseLLMDate(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	if dt, err := parseDate(s); err == nil {
		return dt
	}
	return time.Time{}
}

func absf(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// amtRow is a minimal (withdrawal, deposit, balance) view used for chain validation/repair.
type amtRow struct {
	w   float64
	d   float64
	bal float64
}

// reconcileLLMChain repairs debit↔balance swaps in the extracted transactions. Such a swap is
// mathematically ambiguous on a single row — when amount + balance == prev (true for every
// ordinary withdrawal) both the correct and the swapped reading satisfy
// balance = prev - debit + credit — so it can only be resolved by which orientation keeps the
// running-balance chain intact across the following rows.
func reconcileLLMChain(opening float64, rows []amtRow) {
	const eps = 0.01
	running := opening
	for i := range rows {
		t := &rows[i]
		asIs := absf(running-t.w+t.d-t.bal) <= eps
		swapOK := t.d == 0 && t.w > 0 &&
			absf(running-t.bal-t.w) <= eps &&
			absf(t.bal-t.w) > eps

		switch {
		case asIs && swapOK:
			keep := llmChainContinues(t.bal, rows, i+1)
			swap := llmChainContinues(t.w, rows, i+1)
			doSwap := false
			switch {
			case swap && !keep:
				doSwap = true
			case keep && !swap:
				doSwap = false
			default:
				// Inconclusive (last row, or both orientations chain): the real balance is the
				// candidate nearer the running balance — a transaction nudges the balance, it
				// never replaces it with a small amount.
				doSwap = absf(t.w-running) < absf(t.bal-running)
			}
			if doSwap {
				t.w, t.bal = t.bal, t.w
			}
			running = t.bal
		case asIs:
			running = t.bal
		case swapOK:
			t.w, t.bal = t.bal, t.w
			running = t.bal
		default:
			// Cannot reconcile in either orientation; trust the stated balance to resync.
			running = t.bal
		}
	}
}

func llmChainContinues(startBal float64, rows []amtRow, idx int) bool {
	if idx >= len(rows) {
		return true
	}
	const eps = 0.01
	t := rows[idx]
	if absf(startBal-t.w+t.d-t.bal) <= eps {
		return true
	}
	if t.d == 0 && absf(startBal-t.bal-t.w) <= eps {
		return true
	}
	return false
}

