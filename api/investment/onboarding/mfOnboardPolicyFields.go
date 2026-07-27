package investment

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MF_ONBOARD spans two real tables with two independent shapes of "the row
// the policy check should see" — there is no single canonical row like
// BANK_BALANCE/FD_MASTER:
//
//   - investment.onboard_batch — a batch container acted on by
//     BulkApproveBatch (approve/reject) and DeleteOnboardBatch (delete).
//   - investment.onboard_transaction — a single ledger line acted on by
//     DeleteOnboardTransaction, which previously passed only transaction_id
//     and nothing else to the policy check (BANK_BALANCE-style thin field
//     set on an ID-only action).
//
// The individual AMC/Scheme/DP/Demat/Folio master rows created inside
// UploadInvestmentBulkk are NOT covered here — that handler is a pre-parse
// batch gate (no per-record row exists yet when Enforce fires, same
// precedent as EXPOSURE_UPLOAD/UploadBankBalances) and is left thin on
// purpose.
//
// Both row types share field_code vocabulary where the same business
// concept exists under a different physical column (e.g. "approval_status"
// is investment.onboard_batch.approval_status on the batch row and
// investment.onboard_transaction.approval_status on the transaction row),
// same pattern as FD_CLOSURE's three-table builder.

// mfOnboardBatchRow is the canonical field set for investment.onboard_batch.
type mfOnboardBatchRow struct {
	BatchID        string
	UserID         string
	UserEmail      string
	Source         string
	TotalRecords   int
	Status         string
	Remarks        string
	ApprovalStatus string
}

// buildMFOnboardBatchPolicyFields maps a batch row onto the domain_catalog
// field_code keys seeded for MF_ONBOARD, plus the two request-only values
// every batch action carries (the requested action verb and an optional
// checker comment) — neither is a column on onboard_batch, but both were
// already being passed ad hoc by BulkApproveBatch before this file existed.
func buildMFOnboardBatchPolicyFields(row mfOnboardBatchRow, action, comment string) map[string]interface{} {
	return map[string]interface{}{
		"batch_id":        row.BatchID,
		"user_id":         row.UserID,
		"user_email":      row.UserEmail,
		"source":          row.Source,
		"total_records":   row.TotalRecords,
		"batch_status":    row.Status,
		"approval_status": row.ApprovalStatus,
		"remarks":         row.Remarks,
		"action":          action,
		"comment":         comment,
	}
}

// loadMFOnboardBatchRow fetches the full onboard_batch row by batch_id —
// used by BulkApproveBatch and DeleteOnboardBatch, which only ever receive a
// batch_id, never the batch's own business fields.
func loadMFOnboardBatchRow(ctx context.Context, pool *pgxpool.Pool, batchID string) (mfOnboardBatchRow, error) {
	var row mfOnboardBatchRow
	row.BatchID = batchID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(user_id,''), COALESCE(user_email,''), COALESCE(source,''),
		       COALESCE(total_records,0), COALESCE(status,''), COALESCE(remarks,''),
		       COALESCE(approval_status,'')
		FROM investment.onboard_batch
		WHERE batch_id::text = $1`, batchID,
	).Scan(&row.UserID, &row.UserEmail, &row.Source, &row.TotalRecords, &row.Status, &row.Remarks, &row.ApprovalStatus)
	if err != nil {
		return mfOnboardBatchRow{}, err
	}
	return row, nil
}

// mfOnboardTransactionRow is the canonical field set for
// investment.onboard_transaction.
type mfOnboardTransactionRow struct {
	TransactionID      string
	BatchID            string
	TransactionDate    string
	TransactionType    string
	SchemeInternalCode string
	FolioNumber        string
	Amount             *float64
	Units              *float64
	Nav                *float64
	SchemeID           string
	FolioID            string
	DematAccNumber     string
	DematID            string
	EntityName         string
	BlockedUnits       *float64
	ApprovalStatus     string
}

// buildMFOnboardTransactionPolicyFields maps a transaction row onto the
// domain_catalog field_code keys seeded for MF_ONBOARD.
func buildMFOnboardTransactionPolicyFields(row mfOnboardTransactionRow) map[string]interface{} {
	return map[string]interface{}{
		"transaction_id":       row.TransactionID,
		"batch_id":             row.BatchID,
		"transaction_date":     row.TransactionDate,
		"transaction_type":     row.TransactionType,
		"scheme_internal_code": row.SchemeInternalCode,
		"folio_number":         row.FolioNumber,
		"amount":               row.Amount,
		"units":                row.Units,
		"nav":                  row.Nav,
		"scheme_id":            row.SchemeID,
		"folio_id":             row.FolioID,
		"demat_acc_number":     row.DematAccNumber,
		"demat_id":             row.DematID,
		"entity_name":          row.EntityName,
		"blocked_units":        row.BlockedUnits,
		"approval_status":      row.ApprovalStatus,
	}
}

// loadMFOnboardTransactionRow fetches the full onboard_transaction row by id
// (the "transaction_id" the handler receives) — used by
// DeleteOnboardTransaction, which previously passed nothing but
// transaction_id itself to the policy check. Note: the real
// onboard_transaction primary key column is `id` (bigint), not
// `transaction_id` — matched with ::text = $1 so both string and numeric
// input work.
func loadMFOnboardTransactionRow(ctx context.Context, pool *pgxpool.Pool, transactionID string) (mfOnboardTransactionRow, error) {
	var row mfOnboardTransactionRow
	row.TransactionID = transactionID
	err := pool.QueryRow(ctx, `
		SELECT batch_id::text, COALESCE(TO_CHAR(transaction_date,'YYYY-MM-DD'),''),
		       COALESCE(transaction_type,''), COALESCE(scheme_internal_code,''), COALESCE(folio_number,''),
		       amount, units, nav, COALESCE(scheme_id,''), COALESCE(folio_id,''),
		       COALESCE(demat_acc_number,''), COALESCE(demat_id,''), COALESCE(entity_name,''),
		       blocked_units, COALESCE(approval_status,'')
		FROM investment.onboard_transaction
		WHERE id::text = $1`, transactionID,
	).Scan(&row.BatchID, &row.TransactionDate, &row.TransactionType, &row.SchemeInternalCode, &row.FolioNumber,
		&row.Amount, &row.Units, &row.Nav, &row.SchemeID, &row.FolioID,
		&row.DematAccNumber, &row.DematID, &row.EntityName, &row.BlockedUnits, &row.ApprovalStatus)
	if err != nil {
		return mfOnboardTransactionRow{}, err
	}
	return row, nil
}
