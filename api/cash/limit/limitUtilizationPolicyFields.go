package limit

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// limitUtilizationRow is the canonical business-field shape for
// LIMIT_UTILIZATION — one field per real scalar column on
// cimplrcorpsaas.bank_limit_utilization (old_* audit mirror columns,
// is_deleted, deleted_at/deleted_by skipped as pure audit plumbing), plus
// EntityName/BankName resolved via a join to the parent cimplrcorpsaas.
// bank_limit row (mirrors the fdCashflowRow precedent of denormalizing a
// parent identity field onto a child sub-module row — every handler here
// already validates cash scope against the parent's entity_name/bank_name
// via validateBankLimitRecordScope, so a policy checking entity/bank scope
// needs to see the same values). Before this file: CreateUtilization passed
// 3 fields (limit_id/currency_code/utilized_amount), BulkCreateUtilization
// passed the same 3 plus index, UpdateUtilization passed a raw unvalidated
// {utilization_id, fields} edit blob, and Delete/BulkApprove/BulkReject each
// passed only utilization_id — see database/2026-07-27.sql for the full
// audit. UploadUtilization is the deliberate pre-parse-batch exception (per
// the tracker's EXPOSURE_UPLOAD/UploadBankBalances precedent) and is left as
// its existing thin filename/row_count field set.
type limitUtilizationRow struct {
	UtilizationID   string
	LimitID         string
	EntityName      string
	BankName        string
	UtilizationDate string
	CurrencyCode    string
	UtilizedAmount  float64
	Remarks         string
	ReferenceDoc    string
	EntryMode       string
	Status          string
	UploadS3Key     string
}

// buildLimitUtilizationPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for LIMIT_UTILIZATION (see
// cmd/seedDomainCatalog/limitUtilizationCanonical.go).
func buildLimitUtilizationPolicyFields(row limitUtilizationRow) map[string]interface{} {
	return map[string]interface{}{
		"utilization_id":   row.UtilizationID,
		"limit_id":         row.LimitID,
		"entity_name":      row.EntityName,
		"bank_name":        row.BankName,
		"utilization_date": row.UtilizationDate,
		"currency_code":    row.CurrencyCode,
		"utilized_amount":  row.UtilizedAmount,
		"remarks":          row.Remarks,
		"reference_doc":    row.ReferenceDoc,
		"entry_mode":       row.EntryMode,
		"status":           row.Status,
		"upload_s3_key":    row.UploadS3Key,
	}
}

// loadLimitUtilizationRow fetches the full canonical row by utilization_id,
// joined to the parent bank_limit for entity_name/bank_name — used by
// Delete/BulkApprove/BulkReject, which only ever receive an ID in the
// request, never the business data itself.
func loadLimitUtilizationRow(ctx context.Context, pool *pgxpool.Pool, utilizationID string) (limitUtilizationRow, error) {
	var row limitUtilizationRow
	row.UtilizationID = utilizationID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(u.limit_id,''), COALESCE(l.entity_name,''), COALESCE(l.bank_name,''),
		       COALESCE(TO_CHAR(u.utilization_date,'YYYY-MM-DD'),''), COALESCE(u.currency_code,''),
		       COALESCE(u.utilized_amount,0), COALESCE(u.remarks,''), COALESCE(u.reference_doc,''),
		       COALESCE(u.entry_mode,''), COALESCE(u.status,''), COALESCE(u.upload_s3_key,'')
		FROM cimplrcorpsaas.bank_limit_utilization u
		LEFT JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
		WHERE u.utilization_id = $1 AND COALESCE(u.is_deleted,false) = false`, utilizationID,
	).Scan(
		&row.LimitID, &row.EntityName, &row.BankName,
		&row.UtilizationDate, &row.CurrencyCode,
		&row.UtilizedAmount, &row.Remarks, &row.ReferenceDoc,
		&row.EntryMode, &row.Status, &row.UploadS3Key,
	)
	if err != nil {
		return row, fmt.Errorf("load limit utilization for policy: %w", err)
	}
	return row, nil
}

// loadLimitEntityBank resolves entity_name/bank_name for a given limit_id —
// used by CreateUtilization/BulkCreateUtilization to build the canonical
// policy row before the utilization row exists (no utilization_id yet to
// load by). Mirrors the same lookup validateBankLimitRecordScope already
// performs for cash-scope validation, kept separate since that function
// returns only a validation message, not the resolved values.
func loadLimitEntityBank(ctx context.Context, pool *pgxpool.Pool, limitID string) (entityName, bankName string, err error) {
	err = pool.QueryRow(ctx, `
		SELECT COALESCE(entity_name,''), COALESCE(bank_name,'')
		FROM cimplrcorpsaas.bank_limit WHERE limit_id = $1`, limitID,
	).Scan(&entityName, &bankName)
	if err != nil {
		return "", "", fmt.Errorf("load limit entity/bank for policy: %w", err)
	}
	return entityName, bankName, nil
}

// applyLimitUtilizationEdits overlays a partial edit map (UpdateUtilization's
// req.Fields — arbitrary subset, whatever the user actually changed) onto an
// already-loaded canonical row, so the policy check sees the full row
// as-it-will-be-after-the-edit, not just the touched keys. Mirrors the exact
// field set UpdateUtilization's own dynamic SET-clause builder accepts (see
// utilization.go) — no derived-column side effects in that UPDATE beyond the
// old_<col> mirror, which is audit plumbing, not a policy-visible field. If
// limit_id changes, entity_name/bank_name are NOT re-resolved here (the
// handler itself doesn't re-validate scope against the new limit's
// entity/bank beyond currency — see effectiveLimitID/effectiveCurrency in
// UpdateUtilization); this mirrors existing handler behavior, not an
// oversight — flagged for the reviewer.
func applyLimitUtilizationEdits(row limitUtilizationRow, edits map[string]interface{}) limitUtilizationRow {
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
	for k, v := range edits {
		switch k {
		case "limit_id":
			if s, ok := str(v); ok {
				row.LimitID = s
			}
		case "utilization_date":
			if s, ok := str(v); ok {
				row.UtilizationDate = s
			}
		case "currency_code":
			if s, ok := str(v); ok {
				row.CurrencyCode = s
			}
		case "utilized_amount":
			if n, ok := num(v); ok {
				row.UtilizedAmount = n
			}
		case "remarks":
			if s, ok := str(v); ok {
				row.Remarks = s
			}
		case "reference_doc":
			if s, ok := str(v); ok {
				row.ReferenceDoc = s
			}
		}
	}
	return row
}
