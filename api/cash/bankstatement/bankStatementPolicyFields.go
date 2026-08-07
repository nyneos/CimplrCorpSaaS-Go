package bankstatement

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// lookupEntityIDForAccount resolves the owning entity for an account number so
// the PRE_UPLOAD gate can supply entity_id / EntityCode. Upload runs before the
// file is parsed, so without this an entity-keyed rule sees no value at all and
// falls through to null_handling — breaching on every file regardless of which
// entity it belongs to. Empty result keeps the previous behaviour (no entity
// known) rather than failing the upload.
func lookupEntityIDForAccount(ctx context.Context, pool *pgxpool.Pool, accountNumber string) string {
	accountNumber = strings.TrimSpace(accountNumber)
	if accountNumber == "" || pool == nil {
		return ""
	}
	var entityID string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_id, '')
		FROM public.masterbankaccount
		WHERE account_number = $1 AND is_deleted = false
		LIMIT 1`, accountNumber).Scan(&entityID); err != nil {
		return ""
	}
	return strings.TrimSpace(entityID)
}

// bankStatementRow is the canonical business-field shape for BANK_STATEMENT —
// one field per real scalar column on cimplrcorpsaas.bank_statements (the
// master record), plus total_transactions/uncategorized_count (computed
// aggregates over cimplrcorpsaas.bank_statement_transactions that every
// existing call site already derives the same way). is_deleted/deleted_by/
// deleted_at are pure soft-delete plumbing and excluded, same as every other
// sub-module fixed so far.
//
// Before this file: ApproveBankStatementHandler's two branches shared a
// small ad hoc bankStatementPolicyFields(ctx, pool, bsid, entityID,
// actionType) helper (8 fields: bank_statement_id, entity_id, action_type,
// opening_balance, closing_balance, account_number, total_transactions,
// uncategorized_count) that WAS reasonably consistent between the two
// Approve branches, but RejectBankStatementHandler only ever passed 2 fields
// (bank_statement_id, entity_id) and CommitHandler passed a 4-field ad hoc
// map keyed as "transaction_count" — a key that was never in
// domain_catalog.field for this sub-module at all (the seeded field_code is
// "total_transactions"), so any policy keyed on it could never fire on
// Commit. See database/2026-07-27.sql for the full audit.
//
// action_type is the pending cimplrcorpsaas.auditactionbankstatement.action-
// type (CREATE/EDIT/RECAT/DELETE) driving a given Approve/Reject call. It
// lives on the audit trail, not on the bank_statements master row, so it is
// passed into buildBankStatementPolicyFields alongside the row rather than
// stored on bankStatementRow itself — mirrors how the pre-existing ad hoc
// helper already treated it.
type bankStatementRow struct {
	BankStatementID      string
	EntityID             string
	AccountNumber        string
	StatementPeriodStart string
	StatementPeriodEnd   string
	StatementRequestDate string
	FileHash             string
	UploadedAt           string
	OpeningBalance       *float64
	ClosingBalance       *float64
	UploadLink           string
	UploadS3Key          string
	CurrentStatus        string
	TotalTransactions    int
	UncategorizedCount   int
}

// buildBankStatementPolicyFields maps the canonical row (+ the caller-
// supplied audit actionType) onto the exact field_code keys seeded in
// domain_catalog for BANK_STATEMENT (see
// cmd/seedDomainCatalog/bankStatementCanonical.go). processing_status is
// sourced from bank_statements.current_status, which
// cimplrcorpsaas.fn_sync_bank_statement_status() keeps in sync with the
// latest auditactionbankstatement row — so it reflects the same concept the
// pre-existing field_code name implies without needing a second query.
func buildBankStatementPolicyFields(row bankStatementRow, actionType string) map[string]interface{} {
	return map[string]interface{}{
		"bank_statement_id":      row.BankStatementID,
		"entity_id":              row.EntityID,
		"account_number":         row.AccountNumber,
		"statement_period_start": row.StatementPeriodStart,
		"statement_period_end":   row.StatementPeriodEnd,
		"statement_request_date": row.StatementRequestDate,
		"file_hash":              row.FileHash,
		"uploaded_at":            row.UploadedAt,
		"opening_balance":        row.OpeningBalance,
		"closing_balance":        row.ClosingBalance,
		"upload_link":            row.UploadLink,
		"upload_s3_key":          row.UploadS3Key,
		"processing_status":      row.CurrentStatus,
		"total_transactions":     row.TotalTransactions,
		"uncategorized_count":    row.UncategorizedCount,
		"action_type":            actionType,
	}
}

// loadBankStatementRow fetches the full canonical row by bank_statement_id —
// used by Approve/Reject, which only ever receive an id in the request, never
// the business data itself. total_transactions/uncategorized_count are
// computed the same way the pre-existing ad hoc helper computed them.
func loadBankStatementRow(ctx context.Context, pool *pgxpool.Pool, bankStatementID string) (bankStatementRow, error) {
	var row bankStatementRow
	row.BankStatementID = bankStatementID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(s.entity_id,''), COALESCE(s.account_number,''),
		       COALESCE(TO_CHAR(s.statement_period_start,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(s.statement_period_end,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(s.statement_request_date,'YYYY-MM-DD'),''),
		       COALESCE(s.file_hash,''),
		       COALESCE(TO_CHAR(s.uploaded_at,'YYYY-MM-DD"T"HH24:MI:SS'),''),
		       s.opening_balance, s.closing_balance,
		       COALESCE(s.upload_link,''), COALESCE(s.upload_s3_key,''),
		       COALESCE(s.current_status,''),
		       (SELECT COUNT(*)::int FROM cimplrcorpsaas.bank_statement_transactions t
		         WHERE t.bank_statement_id = s.bank_statement_id),
		       (SELECT COUNT(*)::int FROM cimplrcorpsaas.bank_statement_transactions t
		         WHERE t.bank_statement_id = s.bank_statement_id AND t.category_id IS NULL)
		FROM cimplrcorpsaas.bank_statements s
		WHERE s.bank_statement_id = $1`, bankStatementID,
	).Scan(&row.EntityID, &row.AccountNumber, &row.StatementPeriodStart, &row.StatementPeriodEnd,
		&row.StatementRequestDate, &row.FileHash, &row.UploadedAt, &row.OpeningBalance, &row.ClosingBalance,
		&row.UploadLink, &row.UploadS3Key, &row.CurrentStatus, &row.TotalTransactions, &row.UncategorizedCount)
	if err != nil {
		return bankStatementRow{}, err
	}
	return row, nil
}
