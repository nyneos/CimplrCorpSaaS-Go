package fdAccrual

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fdAccrualRow is the canonical field set for the FD_ACCRUAL sub-module.
// Unlike every other sub-module fixed this session, FD_ACCRUAL's own live
// Enforce call sites (run.go) genuinely span TWO real tables under one
// sub_module_code: the run header (investment.fd_accrual_run — Create,
// Submit, BulkApprove/Reject, Recompute, BulkGenerate) and the per-FD
// override ledger (investment.fd_accrual_ledger — Propose/Approve/
// RejectOverride). Before this file: CreateAccrualRun passed 8 ad hoc fields,
// SubmitForApproval passed 6 different ones, the three override handlers each
// passed a different 3-6 field ad hoc subset of ledger columns, and only the
// run/Bulk*/Recompute path funnelled through accrualRunPolicyFields (10
// fields, itself thinner than the real column set). One struct — a superset
// covering both tables — gives every action in this sub-module the same
// field universe: run-triggered actions leave the ledger-only fields at zero
// value, ledger-triggered actions join back to fd_accrual_run so run-level
// context (run_mode, run_status, financial_period, ...) is still present.
// jsonb columns (fd_inclusion_list, config_snapshot, fds_excluded_reasons,
// cashflow_row_ids) excluded per Database Modeling Rules. Pure derived/
// running-total engine bookkeeping (fds_excluded/calculated/failed/
// overridden, total_principal_in_scope, total_accrued_closing_balance,
// simple_interest_total, compound_interest_total, total_tds_applicable,
// error_count, execution_progress_pct, and every req_*/old_*/config_*
// comparison column on the ledger) excluded — same rationale as
// fdCashflowRow's exclusion of cumulative_*_fy columns: intermediate
// engine artifacts, not stable business data. See database/2026-07-27.sql.
type fdAccrualRow struct {
	// ── Run-level (investment.fd_accrual_run) ──────────────────────────────
	RunID                string
	EntityID             string
	EntityName           string
	RunType              string
	RunMode              string
	RunStatus            string
	CurrencyFilter       string
	BankIDFilter         string
	BankNameFilter       string
	FDStatusFilter       string
	FDInclusionMethod    string
	AccrualPeriodStart   string
	AccrualPeriodEnd     string
	FinancialPeriod      string
	AccrualGranularity   string
	PeriodLocked         bool
	DayCountConvention   string
	AccrualBasis         string
	RoundingRule         string
	PrecisionDecimals    int
	HolidayHandling      string
	EngineVersion        string
	FDsInScope           int
	TotalInterestAccrued float64
	TotalTDSDeducted     float64
	StartedAt            string
	CompletedAt          string
	ReRunAllowed         bool
	SubmittedAt          string
	SubmittedBy          string
	PostingStatus        string
	PostingBatchID       string
	PostedBy             string
	IsActive             bool
	CreatedBy            string

	// ── Ledger-level (investment.fd_accrual_ledger) — populated only when
	// the action targets a specific override/ledger row (ProposeOverride,
	// ApproveOverride, RejectOverride); zero-value for run-only actions. ──
	LedgerID                 string
	FDID                     string
	FDRefNo                  string
	BankID                   string
	BankName                 string
	InterestTypeCode         string
	PrincipalAmount          float64
	InterestRate             float64
	DayCountCode             string
	AccrualDays              int
	OpeningPrincipal         float64
	DailyAccrualRate         float64
	Divisor                  int
	PeriodInterestAccrued    float64
	OpeningAccruedBalance    float64
	InterestReceivedInPeriod float64
	ClosingAccruedBalance    float64
	TDSApplicableAmount      float64
	TDSDeductedInPeriod      float64
	NetInterestInPeriod      float64
	FormulaUsed              string
	LedgerRowStatus          string
	IsOverridden             bool
	OverrideAmount           float64
	OverrideAdjustment       float64
	OverrideReasonCode       string
	OverrideReasonText       string
	OverrideStatus           string
	OverrideProposedBy       string
	OverrideApprovedBy       string
	JournalEntryID           string
}

// buildFDAccrualPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for FD_ACCRUAL (see
// cmd/seedDomainCatalog/fdAccrualCanonical.go).
func buildFDAccrualPolicyFields(row fdAccrualRow) map[string]interface{} {
	return map[string]interface{}{
		// run-level
		"run_id":                 row.RunID,
		"entity_id":              row.EntityID,
		"entity_code":            row.EntityID,
		"entity_name":            row.EntityName,
		"run_type":               row.RunType,
		"run_mode":               row.RunMode,
		"run_status":             row.RunStatus,
		"currency_filter":        row.CurrencyFilter,
		"bank_id_filter":         row.BankIDFilter,
		"bank_name_filter":       row.BankNameFilter,
		"fd_status_filter":       row.FDStatusFilter,
		"fd_inclusion_method":    row.FDInclusionMethod,
		"accrual_period_start":   row.AccrualPeriodStart,
		"accrual_period_end":     row.AccrualPeriodEnd,
		"financial_period":       row.FinancialPeriod,
		"accrual_granularity":    row.AccrualGranularity,
		"period_locked":          row.PeriodLocked,
		"day_count_convention":   row.DayCountConvention,
		"accrual_basis":          row.AccrualBasis,
		"rounding_rule":          row.RoundingRule,
		"precision_decimals":     row.PrecisionDecimals,
		"holiday_handling":       row.HolidayHandling,
		"engine_version":         row.EngineVersion,
		"fds_in_scope":           row.FDsInScope,
		"total_interest_accrued": row.TotalInterestAccrued,
		"total_tds_deducted":     row.TotalTDSDeducted,
		"started_at":             row.StartedAt,
		"completed_at":           row.CompletedAt,
		"re_run_allowed":         row.ReRunAllowed,
		"submitted_at":           row.SubmittedAt,
		"submitted_by":           row.SubmittedBy,
		"posting_status":         row.PostingStatus,
		"posting_batch_id":       row.PostingBatchID,
		"posted_by":              row.PostedBy,
		"is_active":              row.IsActive,
		"created_by":             row.CreatedBy,

		// ledger-level
		"ledger_id":                   row.LedgerID,
		"fd_id":                       row.FDID,
		"fd_ref_no":                   row.FDRefNo,
		"bank_id":                     row.BankID,
		"bank_name":                   row.BankName,
		"interest_type_code":          row.InterestTypeCode,
		"principal_amount":            row.PrincipalAmount,
		"interest_rate":               row.InterestRate,
		"day_count_code":              row.DayCountCode,
		"accrual_days":                row.AccrualDays,
		"opening_principal":           row.OpeningPrincipal,
		"daily_accrual_rate":          row.DailyAccrualRate,
		"divisor":                     row.Divisor,
		"period_interest_accrued":     row.PeriodInterestAccrued,
		"opening_accrued_balance":     row.OpeningAccruedBalance,
		"interest_received_in_period": row.InterestReceivedInPeriod,
		"closing_accrued_balance":     row.ClosingAccruedBalance,
		"tds_applicable_amount":       row.TDSApplicableAmount,
		"tds_deducted_in_period":      row.TDSDeductedInPeriod,
		"net_interest_in_period":      row.NetInterestInPeriod,
		"formula_used":                row.FormulaUsed,
		"ledger_row_status":           row.LedgerRowStatus,
		"is_overridden":               row.IsOverridden,
		"override_amount":             row.OverrideAmount,
		"override_adjustment":         row.OverrideAdjustment,
		"override_reason_code":        row.OverrideReasonCode,
		"override_reason_text":        row.OverrideReasonText,
		"override_status":             row.OverrideStatus,
		"override_proposed_by":        row.OverrideProposedBy,
		"override_approved_by":        row.OverrideApprovedBy,
		"journal_entry_id":            row.JournalEntryID,
	}
}

// loadFDAccrualRunRow fetches the full fd_accrual_run row by run_id — used by
// every run-level action (RunAccrual, SubmitForApproval, BulkApprove/Reject,
// RecomputeAccrualRun, BulkGenerateMonthlyAccruals' execute step) instead of
// each hand-picking its own ad hoc SELECT.
func loadFDAccrualRunRow(ctx context.Context, pool *pgxpool.Pool, runID string) (fdAccrualRow, error) {
	var row fdAccrualRow
	row.RunID = runID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(entity_id,''), COALESCE(entity_name,''),
		       COALESCE(run_type,''), COALESCE(run_mode,''), COALESCE(run_status,''),
		       COALESCE(currency_filter,''), COALESCE(bank_id_filter,''), COALESCE(bank_name_filter,''),
		       COALESCE(fd_status_filter,''), COALESCE(fd_inclusion_method,''),
		       COALESCE(TO_CHAR(accrual_period_start,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(accrual_period_end,'YYYY-MM-DD'),''),
		       COALESCE(financial_period,''), COALESCE(accrual_granularity,''), COALESCE(period_locked,false),
		       COALESCE(day_count_convention,''), COALESCE(accrual_basis,''), COALESCE(rounding_rule,''),
		       COALESCE(precision_decimals,0), COALESCE(holiday_handling,''), COALESCE(engine_version,''),
		       COALESCE(fds_in_scope,0), COALESCE(total_interest_accrued,0), COALESCE(total_tds_deducted,0),
		       COALESCE(TO_CHAR(started_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
		       COALESCE(TO_CHAR(completed_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
		       COALESCE(re_run_allowed,false),
		       COALESCE(TO_CHAR(submitted_at,'YYYY-MM-DD"T"HH24:MI:SS"Z"'),''),
		       COALESCE(submitted_by,''), COALESCE(posting_status,''), COALESCE(posting_batch_id,''),
		       COALESCE(posted_by,''), COALESCE(is_active,false), COALESCE(created_by,'')
		FROM investment.fd_accrual_run
		WHERE run_id=$1 AND COALESCE(is_deleted,false)=false`, runID,
	).Scan(
		&row.EntityID, &row.EntityName,
		&row.RunType, &row.RunMode, &row.RunStatus,
		&row.CurrencyFilter, &row.BankIDFilter, &row.BankNameFilter,
		&row.FDStatusFilter, &row.FDInclusionMethod,
		&row.AccrualPeriodStart, &row.AccrualPeriodEnd,
		&row.FinancialPeriod, &row.AccrualGranularity, &row.PeriodLocked,
		&row.DayCountConvention, &row.AccrualBasis, &row.RoundingRule,
		&row.PrecisionDecimals, &row.HolidayHandling, &row.EngineVersion,
		&row.FDsInScope, &row.TotalInterestAccrued, &row.TotalTDSDeducted,
		&row.StartedAt, &row.CompletedAt, &row.ReRunAllowed,
		&row.SubmittedAt, &row.SubmittedBy, &row.PostingStatus, &row.PostingBatchID,
		&row.PostedBy, &row.IsActive, &row.CreatedBy,
	)
	if err != nil {
		return row, fmt.Errorf("load accrual run for policy: %w", err)
	}
	return row, nil
}

// loadFDAccrualLedgerRow fetches the full fd_accrual_ledger override row by
// ledger_id, left-joined back to its parent fd_accrual_run so run-level
// context (run_mode, run_status, financial_period, ...) is present even
// though the action being enforced (Propose/Approve/RejectOverride) targets
// the ledger row specifically. Used after each handler resolves ledger_id
// from its own LedgerID / (RunID,FDID) request shape.
func loadFDAccrualLedgerRow(ctx context.Context, pool *pgxpool.Pool, ledgerID string) (fdAccrualRow, error) {
	var row fdAccrualRow
	err := pool.QueryRow(ctx, `
		SELECT
			l.run_id, COALESCE(r.entity_id,''), COALESCE(r.entity_name,''),
			COALESCE(r.run_type,''), COALESCE(r.run_mode,''), COALESCE(r.run_status,''),
			COALESCE(r.fd_status_filter,''), COALESCE(r.financial_period,''), COALESCE(r.accrual_granularity,''),
			COALESCE(r.day_count_convention,''), COALESCE(r.rounding_rule,''), COALESCE(r.precision_decimals,0),
			COALESCE(r.total_interest_accrued,0), COALESCE(r.total_tds_deducted,0),
			COALESCE(l.fd_id,''), COALESCE(l.fd_ref_no,''), COALESCE(l.bank_id,''), COALESCE(l.bank_name,''),
			COALESCE(l.interest_type_code,''), COALESCE(l.principal_amount,0), COALESCE(l.interest_rate,0),
			COALESCE(l.day_count_code,''), COALESCE(l.accrual_days,0),
			COALESCE(l.opening_principal,0), COALESCE(l.daily_accrual_rate,0), COALESCE(l.divisor,0),
			COALESCE(l.period_interest_accrued,0), COALESCE(l.opening_accrued_balance,0),
			COALESCE(l.interest_received_in_period,0), COALESCE(l.closing_accrued_balance,0),
			COALESCE(l.tds_applicable_amount,0), COALESCE(l.tds_deducted_in_period,0), COALESCE(l.net_interest_in_period,0),
			COALESCE(l.formula_used,''), COALESCE(l.ledger_row_status,''), COALESCE(l.is_overridden,false),
			COALESCE(l.override_amount,0), COALESCE(l.override_adjustment,0),
			COALESCE(l.override_reason_code,''), COALESCE(l.override_reason_text,''), COALESCE(l.override_status,''),
			COALESCE(l.override_proposed_by,''), COALESCE(l.override_approved_by,''), COALESCE(l.journal_entry_id,'')
		FROM investment.fd_accrual_ledger l
		LEFT JOIN investment.fd_accrual_run r ON r.run_id = l.run_id
		WHERE l.ledger_id=$1 AND COALESCE(l.is_deleted,false)=false`, ledgerID,
	).Scan(
		&row.RunID, &row.EntityID, &row.EntityName,
		&row.RunType, &row.RunMode, &row.RunStatus,
		&row.FDStatusFilter, &row.FinancialPeriod, &row.AccrualGranularity,
		&row.DayCountConvention, &row.RoundingRule, &row.PrecisionDecimals,
		&row.TotalInterestAccrued, &row.TotalTDSDeducted,
		&row.FDID, &row.FDRefNo, &row.BankID, &row.BankName,
		&row.InterestTypeCode, &row.PrincipalAmount, &row.InterestRate,
		&row.DayCountCode, &row.AccrualDays,
		&row.OpeningPrincipal, &row.DailyAccrualRate, &row.Divisor,
		&row.PeriodInterestAccrued, &row.OpeningAccruedBalance,
		&row.InterestReceivedInPeriod, &row.ClosingAccruedBalance,
		&row.TDSApplicableAmount, &row.TDSDeductedInPeriod, &row.NetInterestInPeriod,
		&row.FormulaUsed, &row.LedgerRowStatus, &row.IsOverridden,
		&row.OverrideAmount, &row.OverrideAdjustment,
		&row.OverrideReasonCode, &row.OverrideReasonText, &row.OverrideStatus,
		&row.OverrideProposedBy, &row.OverrideApprovedBy, &row.JournalEntryID,
	)
	if err != nil {
		return row, fmt.Errorf("load accrual ledger for policy: %w", err)
	}
	row.LedgerID = ledgerID
	return row, nil
}

// applyFDAccrualOverrideProposal overlays a ProposeOverride request onto an
// already-loaded ledger row so the policy check sees the row as it will read
// immediately after the proposal is written, not just the pre-edit state —
// same rationale as applyBankBalanceEdits/applyFDAccrualScheduleEdits.
func applyFDAccrualOverrideProposal(row fdAccrualRow, overrideAmount float64, reasonCode, reasonText string) fdAccrualRow {
	row.IsOverridden = true
	row.OverrideAmount = overrideAmount
	row.OverrideAdjustment = overrideAmount - row.PeriodInterestAccrued
	row.OverrideReasonCode = reasonCode
	row.OverrideReasonText = reasonText
	row.OverrideStatus = "PROPOSED"
	row.LedgerRowStatus = "OVERRIDDEN"
	return row
}
