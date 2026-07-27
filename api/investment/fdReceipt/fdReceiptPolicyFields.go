package fdReceipt

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fdReceiptRow is the canonical field set for FD_RECEIPT policy checks — every
// real scalar column on investment.fd_interest_receipt (is_deleted excluded,
// pure soft-delete plumbing). One struct, reused by Create/Update/Delete/
// BulkApprove/BulkReject/PostReceiptJournals — previously Create passed 5 of
// 24 seeded fields, Update/Delete passed only 4, BulkApprove passed 5,
// BulkReject passed only 4, PostReceiptJournals passed only 4. See
// database/2026-07-27.sql for the full audit.
type fdReceiptRow struct {
	ReceiptID, FDID, FDRefNo                                                  string
	EntityID, EntityName                                                      string
	BankID, BankName                                                          string
	ReceiptDate, PeriodStart, PeriodEnd                                       string
	GrossInterestReceived, TDSAmountDeducted, OtherCharges, NetAmountReceived float64
	Currency, BankReferenceNo, Narration                                      string
	Attachment, UploadS3Key                                                   string
	IngestionMode, IngestionBatchID                                           string
	ReceiptStatus, ReconcileRunID                                             string
	ReconcileStatus, JournalEntryID                                           string
	IsActive                                                                  bool
}

// buildFDReceiptPolicyFields maps the canonical row onto the exact field_code
// keys seeded in domain_catalog for FD_RECEIPT (see
// cmd/seedDomainCatalog/fdReceiptCanonical.go).
func buildFDReceiptPolicyFields(row fdReceiptRow) map[string]interface{} {
	return map[string]interface{}{
		"receipt_id":              row.ReceiptID,
		"fd_id":                   row.FDID,
		"fd_ref_no":               row.FDRefNo,
		"entity_id":               row.EntityID,
		"entity_code":             row.EntityID,
		"entity_name":             row.EntityName,
		"bank_id":                 row.BankID,
		"bank_name":               row.BankName,
		"receipt_date":            row.ReceiptDate,
		"period_start":            row.PeriodStart,
		"period_end":              row.PeriodEnd,
		"gross_interest_received": row.GrossInterestReceived,
		"tds_amount_deducted":     row.TDSAmountDeducted,
		"other_charges":           row.OtherCharges,
		"net_amount_received":     row.NetAmountReceived,
		"currency":                row.Currency,
		"bank_reference_no":       row.BankReferenceNo,
		"narration":               row.Narration,
		"attachment":              row.Attachment,
		"upload_s3_key":           row.UploadS3Key,
		"ingestion_mode":          row.IngestionMode,
		"ingestion_batch_id":      row.IngestionBatchID,
		"receipt_status":          row.ReceiptStatus,
		"reconcile_run_id":        row.ReconcileRunID,
		"reconcile_status":        row.ReconcileStatus,
		"journal_entry_id":        row.JournalEntryID,
		"is_active":               row.IsActive,
	}
}

// loadFDReceiptRow fetches the full canonical row by receipt_id — used by
// Delete/BulkApprove/BulkReject/PostReceiptJournals, which only ever receive
// a receipt_id in the request, never the business data itself.
func loadFDReceiptRow(ctx context.Context, pool *pgxpool.Pool, receiptID string) (fdReceiptRow, error) {
	var row fdReceiptRow
	row.ReceiptID = receiptID
	err := pool.QueryRow(ctx, `
		SELECT fd_id, COALESCE(fd_ref_no,''), entity_id, COALESCE(entity_name,''),
		       bank_id, COALESCE(bank_name,''),
		       TO_CHAR(receipt_date,'YYYY-MM-DD'), TO_CHAR(period_start,'YYYY-MM-DD'), TO_CHAR(period_end,'YYYY-MM-DD'),
		       COALESCE(gross_interest_received,0), COALESCE(tds_amount_deducted,0),
		       COALESCE(other_charges,0), COALESCE(net_amount_received,0),
		       COALESCE(currency,''), COALESCE(bank_reference_no,''), COALESCE(narration,''),
		       COALESCE(attachment,''), COALESCE(upload_s3_key,''),
		       COALESCE(ingestion_mode,''), COALESCE(ingestion_batch_id,''),
		       COALESCE(receipt_status,''), COALESCE(reconcile_run_id,''),
		       COALESCE(reconcile_status,''), COALESCE(journal_entry_id,''),
		       COALESCE(is_active,true)
		FROM investment.fd_interest_receipt
		WHERE receipt_id = $1 AND is_deleted = false`, receiptID,
	).Scan(
		&row.FDID, &row.FDRefNo, &row.EntityID, &row.EntityName,
		&row.BankID, &row.BankName,
		&row.ReceiptDate, &row.PeriodStart, &row.PeriodEnd,
		&row.GrossInterestReceived, &row.TDSAmountDeducted,
		&row.OtherCharges, &row.NetAmountReceived,
		&row.Currency, &row.BankReferenceNo, &row.Narration,
		&row.Attachment, &row.UploadS3Key,
		&row.IngestionMode, &row.IngestionBatchID,
		&row.ReceiptStatus, &row.ReconcileRunID,
		&row.ReconcileStatus, &row.JournalEntryID,
		&row.IsActive,
	)
	if err != nil {
		return row, fmt.Errorf("load fd interest receipt for policy: %w", err)
	}
	return row, nil
}

// applyFDReceiptEdits overlays a partial edit map (UpdateReceipt's
// req.Fields — arbitrary subset restricted to the same `allowed` set the
// handler itself enforces) onto an already-loaded canonical row, so the
// policy check sees the full row as-it-will-be-after-the-edit, not just the
// touched keys.
func applyFDReceiptEdits(row fdReceiptRow, edits map[string]interface{}) fdReceiptRow {
	str := func(v interface{}) (string, bool) {
		s, ok := v.(string)
		return s, ok
	}
	num := func(v interface{}) (float64, bool) {
		switch n := v.(type) {
		case float64:
			return n, true
		case int:
			return float64(n), true
		}
		return 0, false
	}
	boolean := func(v interface{}) (bool, bool) {
		b, ok := v.(bool)
		return b, ok
	}
	for k, v := range edits {
		switch k {
		case "receipt_date":
			if s, ok := str(v); ok {
				row.ReceiptDate = s
			}
		case "period_start":
			if s, ok := str(v); ok {
				row.PeriodStart = s
			}
		case "period_end":
			if s, ok := str(v); ok {
				row.PeriodEnd = s
			}
		case "gross_interest_received":
			if n, ok := num(v); ok {
				row.GrossInterestReceived = n
			}
		case "tds_amount_deducted":
			if n, ok := num(v); ok {
				row.TDSAmountDeducted = n
			}
		case "other_charges":
			if n, ok := num(v); ok {
				row.OtherCharges = n
			}
		case "bank_reference_no":
			if s, ok := str(v); ok {
				row.BankReferenceNo = s
			}
		case "narration":
			if s, ok := str(v); ok {
				row.Narration = s
			}
		case "attachment":
			if s, ok := str(v); ok {
				row.Attachment = s
			}
		case "is_active":
			if b, ok := boolean(v); ok {
				row.IsActive = b
			}
		}
	}
	if row.GrossInterestReceived != 0 || row.TDSAmountDeducted != 0 || row.OtherCharges != 0 {
		row.NetAmountReceived = row.GrossInterestReceived - row.TDSAmountDeducted - row.OtherCharges
	}
	return row
}
