package limit

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// bankLimitRow is the canonical business-field shape for BANK_LIMIT — one
// field per real scalar column on cimplrcorpsaas.bank_limit (old_* audit
// mirror columns, is_deleted, deleted_at/deleted_by skipped as pure audit
// plumbing). Every policy-check call site in this package builds its
// Fields{} map from a value of this type instead of hand-picking its own ad
// hoc subset. Before this file: CreateBankLimit passed 3 fields
// (entity_name/bank_name/currency_code), BulkCreateBankLimit passed the same
// 3 plus index, UpdateBankLimit passed a raw unvalidated {limit_id, fields}
// edit blob, and Delete/BulkApprove/BulkReject each passed only limit_id —
// see database/2026-07-27.sql for the full audit.
type bankLimitRow struct {
	LimitID            string
	EntityName         string
	BankName           string
	CoreLimitType      string
	LimitType          string
	LimitSubType       string
	SanctionDate       string
	EffectiveDate      string
	CurrencyCode       string
	SanctionedAmount   float64
	FungibilityType    string
	FungibilityPct     *float64
	SecurityType       string
	Remarks            string
	InitialUtilization *float64
}

// buildBankLimitPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for BANK_LIMIT (see
// cmd/seedDomainCatalog/bankLimitCanonical.go).
func buildBankLimitPolicyFields(row bankLimitRow) map[string]interface{} {
	return map[string]interface{}{
		"limit_id":            row.LimitID,
		"entity_name":         row.EntityName,
		"bank_name":           row.BankName,
		"core_limit_type":     row.CoreLimitType,
		"limit_type":          row.LimitType,
		"limit_sub_type":      row.LimitSubType,
		"sanction_date":       row.SanctionDate,
		"effective_date":      row.EffectiveDate,
		"currency_code":       row.CurrencyCode,
		"sanctioned_amount":   row.SanctionedAmount,
		"fungibility_type":    row.FungibilityType,
		"fungibility_pct":     row.FungibilityPct,
		"security_type":       row.SecurityType,
		"remarks":             row.Remarks,
		"initial_utilization": row.InitialUtilization,
	}
}

// loadBankLimitRow fetches the full canonical row by limit_id — used by
// Delete/BulkApprove/BulkReject, which only ever receive an ID in the
// request, never the business data itself.
func loadBankLimitRow(ctx context.Context, pool *pgxpool.Pool, limitID string) (bankLimitRow, error) {
	var row bankLimitRow
	row.LimitID = limitID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_name,''), COALESCE(bank_name,''), COALESCE(core_limit_type,''),
		       COALESCE(limit_type,''), COALESCE(limit_sub_type,''),
		       COALESCE(TO_CHAR(sanction_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(effective_date,'YYYY-MM-DD'),''),
		       COALESCE(currency_code,''), COALESCE(sanctioned_amount,0),
		       COALESCE(fungibility_type,''), fungibility_pct, COALESCE(security_type,''),
		       COALESCE(remarks,''), initial_utilization
		FROM cimplrcorpsaas.bank_limit
		WHERE limit_id = $1 AND COALESCE(is_deleted,false) = false`, limitID,
	).Scan(
		&row.EntityName, &row.BankName, &row.CoreLimitType,
		&row.LimitType, &row.LimitSubType,
		&row.SanctionDate, &row.EffectiveDate,
		&row.CurrencyCode, &row.SanctionedAmount,
		&row.FungibilityType, &row.FungibilityPct, &row.SecurityType,
		&row.Remarks, &row.InitialUtilization,
	)
	if err != nil {
		return row, fmt.Errorf("load bank limit for policy: %w", err)
	}
	return row, nil
}

// applyBankLimitEdits overlays a partial edit map (UpdateBankLimit's
// req.Fields — arbitrary subset, whatever the user actually changed) onto an
// already-loaded canonical row, so the policy check sees the full row
// as-it-will-be-after-the-edit, not just the touched keys. Mirrors the exact
// field set/casing UpdateBankLimit's own dynamic SET-clause builder accepts
// (see limit.go) — no derived-column side effects in that UPDATE beyond the
// old_<col> mirror, which is audit plumbing, not a policy-visible field.
func applyBankLimitEdits(row bankLimitRow, edits map[string]interface{}) bankLimitRow {
	str := func(v interface{}) (string, bool) {
		s, ok := v.(string)
		return s, ok
	}
	num := func(v interface{}) (*float64, bool) {
		switch n := v.(type) {
		case float64:
			return &n, true
		case int:
			f := float64(n)
			return &f, true
		}
		return nil, false
	}
	for k, v := range edits {
		switch k {
		case "entity_name":
			if s, ok := str(v); ok {
				row.EntityName = s
			}
		case "bank_name":
			if s, ok := str(v); ok {
				row.BankName = s
			}
		case "core_limit_type":
			if s, ok := str(v); ok {
				row.CoreLimitType = s
			}
		case "limit_type":
			if s, ok := str(v); ok {
				row.LimitType = s
			}
		case "limit_sub_type":
			if s, ok := str(v); ok {
				row.LimitSubType = s
			}
		case "sanction_date":
			if s, ok := str(v); ok {
				row.SanctionDate = s
			}
		case "effective_date":
			if s, ok := str(v); ok {
				row.EffectiveDate = s
			}
		case "currency_code":
			if s, ok := str(v); ok {
				row.CurrencyCode = s
			}
		case "sanctioned_amount":
			if n, ok := num(v); ok {
				row.SanctionedAmount = *n
			}
		case "fungibility_type":
			if s, ok := str(v); ok {
				row.FungibilityType = s
			}
		case "fungibility_pct":
			if n, ok := num(v); ok {
				row.FungibilityPct = n
			}
		case "security_type":
			if s, ok := str(v); ok {
				row.SecurityType = s
			}
		case "remarks":
			if s, ok := str(v); ok {
				row.Remarks = s
			}
		case "initial_utilization":
			if n, ok := num(v); ok {
				row.InitialUtilization = n
			}
		}
	}
	return row
}
