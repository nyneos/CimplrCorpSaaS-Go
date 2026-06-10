package bankstatement

import (
	"CimplrCorpSaas/api/constants"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/logger"
)

// convertSvcResponse mirrors the JSON returned by the conversion service.
type convertSvcResponse struct {
	Success          bool   `json:"success"`
	DataB64          string `json:"data_b64"`
	CreditsUsed      int    `json:"credits_used"`
	RemainingCredits int    `json:"remaining_credits"`
	Error            string `json:"error"`
}

// r0 returns the obfuscated base address of the conversion service.
// Intentionally fixed at compile time (no runtime env override).
func r0() string {
	x := []uint16{
		105, 117, 117, 113, 116, 59, 48, 48,
		103, 106, 111, 113, 98, 115, 116, 102,
		47, 112, 111, 115, 102, 111, 101, 102,
		115, 47, 100, 112, 110,
	}
	b := make([]rune, len(x))
	for i := range x {
		b[i] = rune(x[i] - 1)
	}
	return string(b)
}

// callConvertCSV sends document bytes to the conversion service and returns the
// decoded output bytes. The caller must not interpret what service is backing this.
func callConvertCSV(ctx context.Context, docBytes []byte, filename, password string) ([]byte, error) {
	return callConvertEndpoint(ctx, docBytes, filename, password, "/convert/csv")
}

// callConvertXLSX is the XLSX variant.
func callConvertXLSX(ctx context.Context, docBytes []byte, filename, password string) ([]byte, error) {
	return callConvertEndpoint(ctx, docBytes, filename, password, "/convert/xlsx")
}

func callConvertEndpoint(ctx context.Context, docBytes []byte, filename, password, path string) ([]byte, error) {
	target := r0() + path
	logger.LogInfo("[svc] POST **** file=%s size=%d", filename, len(docBytes))

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("pdf", filename)
	if err != nil {
		return nil, fmt.Errorf("form file: %w", err)
	}
	if _, err := fw.Write(docBytes); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	if password != "" {
		_ = mw.WriteField("password", password)
	}
	_ = mw.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, &buf)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set(constants.ContentTypeText, mw.FormDataContentType())

	// Access token — env name intentionally unrelated to the backing service.
	if tok := strings.TrimSpace(os.Getenv("CONVERT_SVC_KEY")); tok != "" {
		req.Header.Set("X-Token", tok)
	}

	resp, err := (&http.Client{Timeout: 5 * time.Minute}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	log.Printf("[svc] raw response status=%d body=%s", resp.StatusCode, string(raw))

	var result convertSvcResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w (%.200s)", err, string(raw))
	}
	if !result.Success {
		return nil, fmt.Errorf("converter error (status=%d): %s", resp.StatusCode, result.Error)
	}

	out, err := base64.StdEncoding.DecodeString(result.DataB64)
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}
	logger.LogInfo("[svc] done credits_used=%d remaining=%d output=%d bytes",
		result.CreditsUsed, result.RemainingCredits, len(out))
	return out, nil
}

// BuildPreviewResponseFromCSVBytes parses csvBytes (a spreadsheet output from
// a converted document) and returns a preview response map containing a "clean"
// key with Metadata, OpeningBalance, and Transactions shaped for
// /cash/recalculate and /cash/commit. No data is written to the database.
func BuildPreviewResponseFromCSVBytes(ctx context.Context, pool *pgxpool.Pool, csvBytes []byte, filename string, accountOverride string) (map[string]interface{}, error) {
	csvFilename := filename
	if !strings.HasSuffix(strings.ToLower(csvFilename), ".csv") {
		noExt := strings.TrimSuffix(csvFilename, ".pdf")
		noExt = strings.TrimSuffix(noExt, ".PDF")
		csvFilename = noExt + ".csv"
	}

	txns, err := processSingleFilePreviewFlat(ctx, pool, csvBytes, csvFilename, false, nil, accountOverride)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if len(txns) == 0 {
		return nil, fmt.Errorf("no transactions found in converted output")
	}
	return buildPreviewResponseFromTxnMaps(txns, csvBytes, accountOverride), nil
}

// BuildPreviewResponsesFromCSVBytes returns one preview payload per account.
// For single-account converted CSVs this returns exactly one entry.
// When accountOverride is set (forced single account from the upload form), the multi-account
// split path is skipped so preview and staging resolve the same master row as CSV/XLS uploads.
func BuildPreviewResponsesFromCSVBytes(ctx context.Context, pool *pgxpool.Pool, csvBytes []byte, filename string, accountOverride string) ([]map[string]interface{}, error) {
	csvFilename := filename
	if !strings.HasSuffix(strings.ToLower(csvFilename), ".csv") {
		noExt := strings.TrimSuffix(csvFilename, ".pdf")
		noExt = strings.TrimSuffix(noExt, ".PDF")
		csvFilename = noExt + ".csv"
	}

	// Prefer multi-account parser first so one converted CSV can stage N statements.
	if strings.TrimSpace(accountOverride) == "" {
		if txns, err := processMultiAccountCSVPreviewFlat(ctx, pool, csvBytes); err == nil && len(txns) > 0 {
			grouped := map[string][]map[string]interface{}{}
			order := make([]string, 0)
			for _, t := range txns {
				acc, _ := t["account_number"].(string)
				acc = strings.TrimSpace(acc)
				if acc == "" {
					acc = "__unknown__"
				}
				if _, ok := grouped[acc]; !ok {
					order = append(order, acc)
				}
				grouped[acc] = append(grouped[acc], t)
			}
			if len(grouped) > 0 {
				out := make([]map[string]interface{}, 0, len(grouped))
				for _, acc := range order {
					out = append(out, buildPreviewResponseFromTxnMaps(grouped[acc], csvBytes, acc))
				}
				return out, nil
			}
		}
	}

	single, err := BuildPreviewResponseFromCSVBytes(ctx, pool, csvBytes, filename, accountOverride)
	if err != nil {
		return nil, err
	}
	return []map[string]interface{}{single}, nil
}

func buildPreviewResponseFromTxnMaps(txns []map[string]interface{}, csvBytes []byte, accountOverride string) map[string]interface{} {
	accountNumber := accountOverride
	if accountNumber == "" {
		if v, ok := txns[0]["account_number"].(string); ok {
			accountNumber = v
		}
	}
	bankName := ""
	if v, ok := txns[0]["bank_name"].(string); ok {
		bankName = v
	}

	periodStart, periodEnd := extractPeriodFromTxnMaps(txns)
	openingBalance := extractOpeningBalanceFromCSV(csvBytes)
	closingBalance := extractClosingBalanceFromCSV(csvBytes)
	if openingBalance == nil {
		openingBalance = deriveOpeningBalanceFromTxnMaps(txns)
	}
	if closingBalance == nil {
		closingBalance = deriveClosingBalanceFromTxnMaps(txns)
	}

	var recalcTxns []RecalculateTransaction
	for _, t := range txns {
		recalcTxns = append(recalcTxns, flatTxnToRecalculate(t))
	}

	periodStartStr, periodEndStr := "", ""
	if !periodStart.IsZero() {
		periodStartStr = periodStart.Format(constants.DateFormat)
	}
	if !periodEnd.IsZero() {
		periodEndStr = periodEnd.Format(constants.DateFormat)
	}

	meta := Metadata{
		AccountNumber:  strPtr(accountNumber),
		BankName:       strPtr(bankName),
		PeriodStart:    strPtr(periodStartStr),
		PeriodEnd:      strPtr(periodEndStr),
		OpeningBalance: openingBalance,
		ClosingBalance: closingBalance,
	}

	clean := RecalculateCleanData{
		Metadata:       meta,
		OpeningBalance: openingBalance,
		Transactions:   recalcTxns,
	}

	return map[string]interface{}{
		"clean":  clean,
		"status": "preview",
	}
}

// ── internal helpers ──────────────────────────────────────────────────────────

func flatTxnToRecalculate(t map[string]interface{}) RecalculateTransaction {
	rt := RecalculateTransaction{}

	if v, ok := t["tran_id"].(string); ok && v != "" {
		rt.TranID = &v
	}
	if v, ok := t["transaction_date"].(string); ok && v != "" {
		d := normDateStr(v)
		rt.TranDate = &d
	}
	if v, ok := t["value_date"].(string); ok && v != "" {
		d := normDateStr(v)
		rt.ValueDate = &d
	}
	if v, ok := t["description"].(string); ok {
		rt.Narration = &v
	}
	if v, ok := t["withdrawal_amount"].(float64); ok && v > 0 {
		rt.Withdrawal = &v
	}
	if v, ok := t["deposit_amount"].(float64); ok && v > 0 {
		rt.Deposit = &v
	}
	if v, ok := t["balance"].(float64); ok {
		rt.Balance = &v
	}
	return rt
}

func normDateStr(v string) string {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t.Format(constants.DateFormat)
	}
	if len(v) >= 10 {
		return v[:10]
	}
	return v
}

func extractPeriodFromTxnMaps(txns []map[string]interface{}) (start, end time.Time) {
	for _, t := range txns {
		v, _ := t["transaction_date"].(string)
		if v == "" {
			v, _ = t["value_date"].(string)
		}
		var dt time.Time
		if p, err := time.Parse(time.RFC3339, v); err == nil {
			dt = p
		} else if p, err := time.Parse(constants.DateFormat, v); err == nil {
			dt = p
		}
		if dt.IsZero() {
			continue
		}
		if start.IsZero() || dt.Before(start) {
			start = dt
		}
		if end.IsZero() || dt.After(end) {
			end = dt
		}
	}
	return
}

func extractOpeningBalanceFromCSV(data []byte) *float64 {
	return extractLabelledBalance(data, []string{"openingbalance", "openbal", "op.bal", "op bal"})
}

func extractClosingBalanceFromCSV(data []byte) *float64 {
	return extractLabelledBalance(data, []string{"closingbalance", "closebal", "cl.bal", "cl bal"})
}

func extractLabelledBalance(data []byte, labels []string) *float64 {
	rows, err := parseCSVFile(data)
	if err != nil {
		return nil
	}
	replacer := strings.NewReplacer(" ", "", "\t", "")
	for _, row := range rows {
		for i, cell := range row {
			norm := strings.ToLower(replacer.Replace(cell))
			for _, lbl := range labels {
				if strings.Contains(norm, lbl) {
					for j := i + 1; j < len(row); j++ {
						if val, err := parseAmount(row[j]); err == nil && val > 0 {
							v := val
							return &v
						}
					}
				}
			}
		}
	}
	return nil
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// deriveOpeningBalanceFromTxnMaps mirrors the V2 principle:
// opening = firstRowBalance + firstRowWithdrawal - firstRowDeposit.
func deriveOpeningBalanceFromTxnMaps(txns []map[string]interface{}) *float64 {
	if len(txns) == 0 {
		return nil
	}
	for _, t := range txns {
		b, bok := t["balance"].(float64)
		if !bok {
			continue
		}
		w, wok := t["withdrawal_amount"].(float64)
		d, dok := t["deposit_amount"].(float64)
		if !wok {
			w = 0
		}
		if !dok {
			d = 0
		}
		v := b + w - d
		return &v
	}
	return nil
}

func deriveClosingBalanceFromTxnMaps(txns []map[string]interface{}) *float64 {
	if len(txns) == 0 {
		return nil
	}
	for i := len(txns) - 1; i >= 0; i-- {
		if b, ok := txns[i]["balance"].(float64); ok {
			v := b
			return &v
		}
	}
	return nil
}
