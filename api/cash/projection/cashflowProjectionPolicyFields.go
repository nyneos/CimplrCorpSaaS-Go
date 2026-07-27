package projection

import (
	"context"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// cashflowProjectionRow is the canonical business-field shape for
// CASHFLOW_PROJECTION policy checks. The sub-module genuinely spans two
// tables under one policy scope, and every real call site (Create, Update,
// BulkApprove, BulkReject, Delete) already operates at PROPOSAL granularity
// — bulk actions loop one runtime.EnforceInline call per proposal_id, never
// per item:
//
//   - cimplrcorpsaas.cashflow_proposal — the header (1 row per proposal):
//     ProposalID, ProposalName, BaseCurrencyCode, EffectiveDate.
//   - cimplrcorpsaas.cashflow_proposal_item — child line items (N per
//     proposal): Description, CashflowType, CategoryID, CurrencyCode,
//     ExpectedAmount, IsRecurring, RecurrenceFrequency, MaturityDate,
//     BankName, BankAccountNumber, EntityName.
//
// Judgment call (flag for review): a proposal can have many items with
// different values, but the policy check fires once per proposal, not once
// per item. Rather than leaving the item-level fields out entirely (which
// would silently drop 6 already-seeded field_codes — bank_name,
// bank_account_number, cashflow_type, category_id, expected_amount,
// maturity_date — back to uncataloged), this struct takes the FIRST
// non-deleted item (ORDER BY created_at) as representative, matching the
// existing code's own pattern: CreateCashFlowProposalV2/
// UpdateCashFlowProposalV2 already derive EntityCode from Items[0] for the
// EnforceInput.EntityCode field. ItemCount is a true aggregate (COUNT), not
// a first-item value. A policy author checking an item-level field against a
// multi-item proposal only sees item #1 — reviewer should confirm this is an
// acceptable approximation, or that CASHFLOW_PROJECTION policies are not
// expected to discriminate on per-item fields for multi-item proposals.
//
// Before this file: Create passed 3 fields (proposal_name,
// base_currency_code, item_count), Update passed 3 (proposal_id,
// proposal_name, base_currency_code), and BulkApprove/BulkReject/Delete each
// passed only proposal_id — a BANK_BALANCE-style consistency gap, not just a
// thin field set.
type cashflowProjectionRow struct {
	ProposalID          string
	ProposalName        string
	BaseCurrencyCode    string
	EffectiveDate       string
	ItemCount           int
	Description         string
	CashflowType        string
	CategoryID          string
	CurrencyCode        string
	ExpectedAmount      *float64
	IsRecurring         bool
	RecurrenceFrequency string
	MaturityDate        string
	BankName            string
	BankAccountNumber   string
	EntityName          string
}

// buildCashflowProjectionPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for CASHFLOW_PROJECTION (see
// cmd/seedDomainCatalog/cashflowProjectionCanonical.go).
func buildCashflowProjectionPolicyFields(row cashflowProjectionRow) map[string]interface{} {
	return map[string]interface{}{
		"proposal_id":          row.ProposalID,
		"proposal_name":        row.ProposalName,
		"base_currency_code":   row.BaseCurrencyCode,
		"effective_date":       row.EffectiveDate,
		"item_count":           row.ItemCount,
		"description":          row.Description,
		"cashflow_type":        row.CashflowType,
		"category_id":          row.CategoryID,
		"currency_code":        row.CurrencyCode,
		"expected_amount":      row.ExpectedAmount,
		"is_recurring":         row.IsRecurring,
		"recurrence_frequency": row.RecurrenceFrequency,
		"maturity_date":        row.MaturityDate,
		"bank_name":            row.BankName,
		"bank_account_number":  row.BankAccountNumber,
		"entity_name":          row.EntityName,
	}
}

// loadCashflowProjectionRow fetches the full canonical row by proposal_id —
// used by BulkApprove/BulkReject/Delete, which only ever receive an ID in
// the request, never the business data itself. The representative item
// (first non-deleted, ordered by created_at) and the live item_count are
// both computed server-side via LATERAL joins.
func loadCashflowProjectionRow(ctx context.Context, pool *pgxpool.Pool, proposalID string) (cashflowProjectionRow, error) {
	var row cashflowProjectionRow
	var effectiveDate time.Time
	err := pool.QueryRow(ctx, `
		SELECT p.proposal_id, p.proposal_name, p.base_currency_code, p.effective_date,
		       COALESCE(ic.item_count, 0),
		       COALESCE(i.description, ''), COALESCE(i.cashflow_type, ''), COALESCE(i.category_id, ''),
		       COALESCE(i.currency_code, ''), i.expected_amount, COALESCE(i.is_recurring, false),
		       COALESCE(i.recurrence_frequency, ''), COALESCE(i.maturity_date::text, ''),
		       COALESCE(i.bank_name, ''), COALESCE(i.bank_account_number, ''), COALESCE(i.entity_name, '')
		FROM cimplrcorpsaas.cashflow_proposal p
		LEFT JOIN LATERAL (
			SELECT description, cashflow_type, category_id, currency_code, expected_amount,
			       is_recurring, recurrence_frequency, maturity_date, bank_name, bank_account_number, entity_name
			FROM cimplrcorpsaas.cashflow_proposal_item
			WHERE proposal_id = p.proposal_id
			  AND COALESCE(is_deleted, false) = false
			ORDER BY created_at
			LIMIT 1
		) i ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(*) AS item_count
			FROM cimplrcorpsaas.cashflow_proposal_item
			WHERE proposal_id = p.proposal_id
			  AND COALESCE(is_deleted, false) = false
		) ic ON TRUE
		WHERE p.proposal_id = $1`, proposalID,
	).Scan(&row.ProposalID, &row.ProposalName, &row.BaseCurrencyCode, &effectiveDate,
		&row.ItemCount, &row.Description, &row.CashflowType, &row.CategoryID,
		&row.CurrencyCode, &row.ExpectedAmount, &row.IsRecurring,
		&row.RecurrenceFrequency, &row.MaturityDate, &row.BankName, &row.BankAccountNumber, &row.EntityName)
	if err != nil {
		return cashflowProjectionRow{}, err
	}
	row.EffectiveDate = effectiveDate.Format(constants.DateFormat)
	return row, nil
}

// cashflowProjectionItemInput is the minimal per-item shape needed to build
// policy fields directly from an in-flight Create/Update request, before any
// DB round trip (the new/edited proposal_id doesn't exist as a committed row
// yet at Create time, and Update's payload is a full items replace, not a
// partial-field edit — so there is no applyCashflowProjectionEdits here,
// unlike bankBalancesRow/UpdateBankBalance's partial-edit-map case). Mirrors
// the fields already present on the request-scoped anonymous item structs in
// CreateCashFlowProposalV2 / UpdateCashFlowProposalV2.
type cashflowProjectionItemInput struct {
	Description         string
	CashflowType        string
	CategoryID          string
	CurrencyCode        string
	ExpectedAmount      float64
	IsRecurring         bool
	RecurrenceFrequency string
	MaturityDate        string
	BankName            string
	BankAccountNumber   string
	EntityName          string
}

// buildCashflowProjectionRowFromRequest builds the canonical row straight
// from an in-flight Create/Update request payload (header fields + items),
// taking the first item as representative — same rationale as
// loadCashflowProjectionRow's LATERAL join, kept consistent between the
// pre-commit (Create/Update) and post-commit (load-by-id) paths.
func buildCashflowProjectionRowFromRequest(proposalID, proposalName, baseCurrencyCode, effectiveDate string, items []cashflowProjectionItemInput) cashflowProjectionRow {
	row := cashflowProjectionRow{
		ProposalID:       proposalID,
		ProposalName:     proposalName,
		BaseCurrencyCode: baseCurrencyCode,
		EffectiveDate:    effectiveDate,
		ItemCount:        len(items),
	}
	if len(items) > 0 {
		first := items[0]
		row.Description = first.Description
		row.CashflowType = first.CashflowType
		row.CategoryID = first.CategoryID
		row.CurrencyCode = first.CurrencyCode
		amt := first.ExpectedAmount
		row.ExpectedAmount = &amt
		row.IsRecurring = first.IsRecurring
		row.RecurrenceFrequency = first.RecurrenceFrequency
		row.MaturityDate = first.MaturityDate
		row.BankName = first.BankName
		row.BankAccountNumber = first.BankAccountNumber
		row.EntityName = first.EntityName
	}
	return row
}
