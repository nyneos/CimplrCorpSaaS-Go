package payablerecievable

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// payableReceivablePolicyFields.go — canonical PAYABLE_RECEIVABLE policy field
// builders.
//
// PAYABLE_RECEIVABLE genuinely backs onto TWO real tables under one
// sub_module_code (confirmed via psql: domain_catalog.sub_module has exactly
// one PAYABLE_RECEIVABLE row, module CASH):
//   - public.tr_payables    (PK payable_id,    audit table auditactionpayable)
//   - public.tr_receivables (PK receivable_id, audit table auditactionreceivable)
//
// Design decision — two separate row types, not one shared struct: same
// rationale as fdClosurePolicyFields.go (different real tables, different
// PKs, different audit tables = different lifecycle identity, even though
// the column shapes are near-mirrors here). Unlike FD_CLOSURE though, the
// two tables here are almost column-for-column identical (entity_name,
// counterparty_name, invoice_number, invoice_date, due_date, currency_code
// on both; only the amount column is named differently — tr_payables.amount
// vs tr_receivables.invoice_amount). domain_catalog already reflects this:
// there is ONE shared field_code vocabulary (e.g. "amount", not
// "payable_amount"/"receivable_amount"), reused by both builders below so a
// policy author writes one rule that fires regardless of which side of the
// ledger produced the record — same reuse approach as fdClosurePolicyFields.go's
// per-builder comments.
//
// Before this file: BulkRequestDeleteTransactions / BulkRejectTransactions /
// BulkApproveTransactions each passed only {"transaction_id": id};
// BulkCreateTransactions passed {"transaction_type","entity_name","index"} (3
// of 8 real business fields); UpdateTransaction passed the raw unvalidated
// edit blob ({"transaction_id","fields": req.Fields}) instead of the
// post-edit row. A policy checking e.g. "amount" or "currency_code" would
// fire on Create/Update-ish paths only and silently never fire on
// Approve/Reject/Delete.
//
// Excluded as plumbing (not policy-comparable business data): is_deleted,
// deleted_by, deleted_at (state already implied by the WHERE clause /
// terminal action itself), upload_link/upload_s3_key (attachment plumbing,
// same rationale fdClosurePolicyFields.go used to exclude upload_s3_key),
// and the old_* columns (audit diff snapshot, not current business state).
//
// field_code keys below match what's already seeded in domain_catalog for
// PAYABLE_RECEIVABLE (see cmd/seedDomainCatalog/payableReceivableCanonical.go
// for the cdm_path backfill) — reusing existing vocabulary rather than
// inventing new spellings. "processing_status" was already seeded with no
// cdm_path and no code producing it; it's derived from the *_audit tables
// (latest processing_status per id), not a tr_payables/tr_receivables
// column — included here (best-effort, never blocks the load on failure)
// since the catalog already anticipated it and it is genuinely useful for a
// policy author wanting to distinguish e.g. re-approval of an
// already-approved row. Flagged as a judgment call for review.

// ─────────────────────────── 1. tr_payables ────────────────────────────────

// payableRow is the canonical field set for the PAYABLE side of
// PAYABLE_RECEIVABLE (public.tr_payables).
type payableRow struct {
	PayableID        string
	EntityName       string
	CounterpartyName string
	InvoiceNumber    string
	InvoiceDate      string
	DueDate          string
	Amount           float64
	CurrencyCode     string
	ProcessingStatus string
}

// buildPayablePolicyFields maps a payableRow onto the exact field_code keys
// seeded in domain_catalog for PAYABLE_RECEIVABLE.
func buildPayablePolicyFields(row payableRow) map[string]interface{} {
	return map[string]interface{}{
		"transaction_id":    row.PayableID,
		"transaction_type":  "PAYABLE",
		"entity_name":       row.EntityName,
		"counterparty_name": row.CounterpartyName,
		"invoice_number":    row.InvoiceNumber,
		"invoice_date":      row.InvoiceDate,
		"due_date":          row.DueDate,
		"amount":            row.Amount,
		"currency_code":     row.CurrencyCode,
		"processing_status": row.ProcessingStatus,
	}
}

// loadPayableRow fetches the full canonical row by payable_id — used by
// BulkRequestDeleteTransactions/BulkRejectTransactions/BulkApproveTransactions
// (id-only call sites) and by UpdateTransaction (loaded before edits are
// applied, see applyPayableEdits below).
func loadPayableRow(ctx context.Context, pool *pgxpool.Pool, payableID string) (payableRow, error) {
	var row payableRow
	row.PayableID = payableID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_name,''), COALESCE(counterparty_name,''), COALESCE(invoice_number,''),
		       COALESCE(TO_CHAR(invoice_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(due_date,'YYYY-MM-DD'),''),
		       COALESCE(amount,0), COALESCE(currency_code,'')
		FROM tr_payables
		WHERE payable_id = $1 AND COALESCE(is_deleted,false) = false`, payableID,
	).Scan(&row.EntityName, &row.CounterpartyName, &row.InvoiceNumber,
		&row.InvoiceDate, &row.DueDate, &row.Amount, &row.CurrencyCode)
	if err != nil {
		return row, err
	}
	// Best-effort: latest processing_status from the audit trail. Never
	// blocks the load — a policy check on other fields should still proceed
	// even if this lookup fails.
	_ = pool.QueryRow(ctx, `
		SELECT processing_status FROM auditactionpayable
		WHERE payable_id = $1
		ORDER BY requested_at DESC, action_id DESC LIMIT 1`, payableID,
	).Scan(&row.ProcessingStatus)
	return row, nil
}

// applyPayableEdits overlays a partial edit map (UpdateTransaction's
// req.Fields for a TR-PAY- id — arbitrary subset, whatever the user actually
// changed) onto an already-loaded canonical row, so the policy check sees
// the full row as-it-will-be-after-the-edit, not just the touched keys.
// Keys match exactly what UpdateTransaction's extractStr/extractDate/
// extractFloat closures already read for the payable branch (amount, not
// invoice_amount — see applyReceivableEdits for the receivable-side key).
func applyPayableEdits(row payableRow, edits map[string]interface{}) payableRow {
	if s, ok := stringField(edits, "entity_name"); ok {
		row.EntityName = s
	}
	if s, ok := stringField(edits, "counterparty_name"); ok {
		row.CounterpartyName = s
	}
	if s, ok := stringField(edits, "invoice_number"); ok {
		row.InvoiceNumber = s
	}
	if s, ok := stringField(edits, "currency_code"); ok {
		row.CurrencyCode = s
	}
	if s, ok := dateField(edits, "invoice_date"); ok {
		row.InvoiceDate = s
	}
	if s, ok := dateField(edits, "due_date"); ok {
		row.DueDate = s
	}
	if f, ok := floatField(edits, "amount"); ok {
		row.Amount = f
	}
	return row
}

// ────────────────────────── 2. tr_receivables ───────────────────────────────

// receivableRow is the canonical field set for the RECEIVABLE side of
// PAYABLE_RECEIVABLE (public.tr_receivables).
type receivableRow struct {
	ReceivableID     string
	EntityName       string
	CounterpartyName string
	InvoiceNumber    string
	InvoiceDate      string
	DueDate          string
	InvoiceAmount    float64
	CurrencyCode     string
	ProcessingStatus string
}

// buildReceivablePolicyFields maps a receivableRow onto the same field_code
// vocabulary buildPayablePolicyFields uses — InvoiceAmount -> "amount" (see
// file header comment on shared field_code reuse).
func buildReceivablePolicyFields(row receivableRow) map[string]interface{} {
	return map[string]interface{}{
		"transaction_id":    row.ReceivableID,
		"transaction_type":  "RECEIVABLE",
		"entity_name":       row.EntityName,
		"counterparty_name": row.CounterpartyName,
		"invoice_number":    row.InvoiceNumber,
		"invoice_date":      row.InvoiceDate,
		"due_date":          row.DueDate,
		"amount":            row.InvoiceAmount,
		"currency_code":     row.CurrencyCode,
		"processing_status": row.ProcessingStatus,
	}
}

// loadReceivableRow fetches the full canonical row by receivable_id — same
// rationale as loadPayableRow.
func loadReceivableRow(ctx context.Context, pool *pgxpool.Pool, receivableID string) (receivableRow, error) {
	var row receivableRow
	row.ReceivableID = receivableID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_name,''), COALESCE(counterparty_name,''), COALESCE(invoice_number,''),
		       COALESCE(TO_CHAR(invoice_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(due_date,'YYYY-MM-DD'),''),
		       COALESCE(invoice_amount,0), COALESCE(currency_code,'')
		FROM tr_receivables
		WHERE receivable_id = $1 AND COALESCE(is_deleted,false) = false`, receivableID,
	).Scan(&row.EntityName, &row.CounterpartyName, &row.InvoiceNumber,
		&row.InvoiceDate, &row.DueDate, &row.InvoiceAmount, &row.CurrencyCode)
	if err != nil {
		return row, err
	}
	_ = pool.QueryRow(ctx, `
		SELECT processing_status FROM auditactionreceivable
		WHERE receivable_id = $1
		ORDER BY requested_at DESC, action_id DESC LIMIT 1`, receivableID,
	).Scan(&row.ProcessingStatus)
	return row, nil
}

// applyReceivableEdits — same rationale as applyPayableEdits. Reads
// "invoice_amount" (not "amount") to match UpdateTransaction's receivable
// branch (extractFloat("invoice_amount")).
func applyReceivableEdits(row receivableRow, edits map[string]interface{}) receivableRow {
	if s, ok := stringField(edits, "entity_name"); ok {
		row.EntityName = s
	}
	if s, ok := stringField(edits, "counterparty_name"); ok {
		row.CounterpartyName = s
	}
	if s, ok := stringField(edits, "invoice_number"); ok {
		row.InvoiceNumber = s
	}
	if s, ok := stringField(edits, "currency_code"); ok {
		row.CurrencyCode = s
	}
	if s, ok := dateField(edits, "invoice_date"); ok {
		row.InvoiceDate = s
	}
	if s, ok := dateField(edits, "due_date"); ok {
		row.DueDate = s
	}
	if f, ok := floatField(edits, "invoice_amount"); ok {
		row.InvoiceAmount = f
	}
	return row
}

// ───────────────────────── 3. shared helpers ────────────────────────────────

// loadTransactionPolicyFields dispatches on the TR-PAY-/TR-REC- id prefix
// (constants.ErrPrefixPayable/ErrPrefixReceivable — the same prefixes every
// bulk handler in payrecAll.go already keys off of) and returns the full
// canonical Fields map for whichever side the id belongs to. Falls back to a
// thin id-only map if the row can't be loaded (e.g. unknown prefix, or the
// row vanished between the caller's own lookup and here) so the enforcement
// call site still gets *a* policy decision instead of erroring out — same
// fallback shape fdClosurePolicyFields.go's cimplrClosureUploadPolicyFields
// uses.
func loadTransactionPolicyFields(ctx context.Context, pool *pgxpool.Pool, transactionID string) map[string]interface{} {
	id := strings.TrimSpace(transactionID)
	switch {
	case strings.HasPrefix(id, constants.ErrPrefixPayable):
		if row, err := loadPayableRow(ctx, pool, id); err == nil {
			return buildPayablePolicyFields(row)
		}
	case strings.HasPrefix(id, constants.ErrPrefixReceivable):
		if row, err := loadReceivableRow(ctx, pool, id); err == nil {
			return buildReceivablePolicyFields(row)
		}
	}
	return map[string]interface{}{"transaction_id": id}
}

// buildPayableReceivablePolicyFieldsFromItem builds the canonical Fields map
// for BulkCreateTransactions — the one Create-shaped call site, which has no
// DB row yet (the audit/insert happens after enforcement). Reads the same
// keys BulkCreateTransactions itself reads a few lines below its own
// enforcement loop (entity_name, counterparty_name, invoice_number,
// invoice_date, due_date, amount, currency_code, transaction_type) straight
// from the parsed request item, so the policy check sees what will actually
// be inserted rather than re-deriving it differently. invoice_date/due_date
// are run through normalizeDate (the same helper BulkCreateTransactions
// itself calls a few lines later) so the policy check sees the same
// YYYY-MM-DD shape the row will actually have post-insert.
func buildPayableReceivablePolicyFieldsFromItem(idx int, itm map[string]interface{}) map[string]interface{} {
	str := func(key string) string {
		if v, ok := itm[key]; ok && v != nil {
			return strings.TrimSpace(toStringAny(v))
		}
		return ""
	}
	return map[string]interface{}{
		"transaction_type":  strings.ToUpper(str("transaction_type")),
		"entity_name":       str("entity_name"),
		"counterparty_name": str("counterparty_name"),
		"invoice_number":    str("invoice_number"),
		"invoice_date":      normalizeDate(str("invoice_date")),
		"due_date":          normalizeDate(str("due_date")),
		"amount":            toFloat64Any(itm["amount"]),
		"currency_code":     str("currency_code"),
		"index":             idx,
	}
}

// ─────────────────────── small scalar coercion helpers ──────────────────────
// Mirrors the inline extractStr/extractDate/extractFloat closures already in
// UpdateTransaction/BulkCreateTransactions — factored out here so both the
// edit-merge and the create-from-item builder share one implementation
// instead of three ad hoc copies.

func stringField(m map[string]interface{}, key string) (string, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", false
	}
	return toStringAny(v), true
}

func dateField(m map[string]interface{}, key string) (string, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", false
	}
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return "", false
	}
	return normalizeDate(s), true
}

func floatField(m map[string]interface{}, key string) (float64, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case string:
		if strings.TrimSpace(n) == "" {
			return 0, false
		}
		var out float64
		if _, err := fmt.Sscan(n, &out); err != nil {
			return 0, false
		}
		return out, true
	}
	return 0, false
}

func toStringAny(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func toFloat64Any(v interface{}) float64 {
	f, _ := floatField(map[string]interface{}{"v": v}, "v")
	return f
}
