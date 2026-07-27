package fdMaster

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fdCashflowRow is the canonical field set for FD_CASHFLOW policy checks —
// the real scalar columns on investment.fd_cashflow_schedule most relevant to
// a policy author (the same set EditCashflowLineItem already snapshots into
// its fd_audit_cashflow_schedule old_* columns), plus entity_id resolved via
// fd_master. Pure derived/running-total bookkeeping columns (cumulative_*_fy,
// cumulative_interest_total, due_not_accrued, accr_rev_k, tds_rev_l) are
// intermediate accrual-engine artifacts, not stable per-line business data —
// excluded, same rationale as excluding jsonb elsewhere.
//
// One struct, reused by all 8 live call sites (Edit, Approve/Reject/Delete,
// ApproveDelete, and their Bulk* variants) — previously EditCashflowLineItem
// alone snapshotted the full row (for its own audit trail) but the Enforce
// call right next to it only passed 4 fields; every other call site passed
// the same thin 4.
type fdCashflowRow struct {
	CashflowID, FDID, EntityID string
	SequenceNumber             int
	EventType                  string
	EventDate                  string
	PeriodStartDate            string
	PeriodEndDate              string
	PeriodDays                 int
	OpeningPrincipal           float64
	InterestAccrued            float64
	CapitalizedAmount          float64
	ClosingPrincipal           float64
	TDSAmount                  float64
	NetCashFlow                float64
	DayCountCode               string
	Divisor                    int
	FormulaUsed                string
	AccrualRatePerDay          float64
	DrAccountCode              string
	DrAccountName              string
	CrAccountCode              string
	CrAccountName              string
	SystemCalculated           bool
	IsEdited                   bool
	BankConfirmed              bool
	BankConfirmedDate          string
	BankReference              string
	VoucherGenerated           bool
	VoucherNumber              string
	PostingStatus              string
	Remarks                    string
	ReceiptID                  string
	ReceiptCleared             bool
	InterestRate               float64
	TDSRate                    float64
	HolidaysInPeriod           int
	ValueDate                  string
	CashflowType               string
	AccrualFrequency           string
	NetAmount                  float64
	FinancialYear              string
}

func buildFDCashflowPolicyFields(row fdCashflowRow) map[string]interface{} {
	return map[string]interface{}{
		"cashflow_id":          row.CashflowID,
		"fd_id":                row.FDID,
		"entity_id":            row.EntityID,
		"entity_code":          row.EntityID,
		"sequence_number":      row.SequenceNumber,
		"event_type":           row.EventType,
		"event_date":           row.EventDate,
		"period_start_date":    row.PeriodStartDate,
		"period_end_date":      row.PeriodEndDate,
		"period_days":          row.PeriodDays,
		"opening_principal":    row.OpeningPrincipal,
		"interest_accrued":     row.InterestAccrued,
		"capitalized_amount":   row.CapitalizedAmount,
		"closing_principal":    row.ClosingPrincipal,
		"tds_amount":           row.TDSAmount,
		"net_cash_flow":        row.NetCashFlow,
		"day_count_code":       row.DayCountCode,
		"divisor":              row.Divisor,
		"formula_used":         row.FormulaUsed,
		"accrual_rate_per_day": row.AccrualRatePerDay,
		"dr_account_code":      row.DrAccountCode,
		"dr_account_name":      row.DrAccountName,
		"cr_account_code":      row.CrAccountCode,
		"cr_account_name":      row.CrAccountName,
		"system_calculated":    row.SystemCalculated,
		"is_edited":            row.IsEdited,
		"bank_confirmed":       row.BankConfirmed,
		"bank_confirmed_date":  row.BankConfirmedDate,
		"bank_reference":       row.BankReference,
		"voucher_generated":    row.VoucherGenerated,
		"voucher_number":       row.VoucherNumber,
		"posting_status":       row.PostingStatus,
		"remarks":              row.Remarks,
		"receipt_id":           row.ReceiptID,
		"receipt_cleared":      row.ReceiptCleared,
		"interest_rate":        row.InterestRate,
		"tds_rate":             row.TDSRate,
		"holidays_in_period":   row.HolidaysInPeriod,
		"value_date":           row.ValueDate,
		"cashflow_type":        row.CashflowType,
		"accrual_frequency":    row.AccrualFrequency,
		"net_amount":           row.NetAmount,
		"financial_year":       row.FinancialYear,
	}
}

// loadFDCashflowRow fetches the full cashflow line by cashflow_id, joined to
// fd_master for entity_id. Used by every call site that only receives an ID
// (which is all of them — even Edit re-derives its own snapshot separately
// for the audit trail; this is the policy-facing read).
func loadFDCashflowRow(ctx context.Context, pool *pgxpool.Pool, cashflowID string) (fdCashflowRow, error) {
	var row fdCashflowRow
	row.CashflowID = cashflowID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(c.fd_id,''), COALESCE(m.entity_id,''),
		       COALESCE(c.sequence_number,0), COALESCE(c.event_type,''),
		       COALESCE(TO_CHAR(c.event_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(c.period_start_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(c.period_end_date,'YYYY-MM-DD'),''),
		       COALESCE(c.period_days,0),
		       COALESCE(c.opening_principal,0), COALESCE(c.interest_accrued,0), COALESCE(c.capitalized_amount,0),
		       COALESCE(c.closing_principal,0), COALESCE(c.tds_amount,0), COALESCE(c.net_cash_flow,0),
		       COALESCE(c.day_count_code,''), COALESCE(c.divisor,0), COALESCE(c.formula_used,''), COALESCE(c.accrual_rate_per_day,0),
		       COALESCE(c.dr_account_code,''), COALESCE(c.dr_account_name,''), COALESCE(c.cr_account_code,''), COALESCE(c.cr_account_name,''),
		       COALESCE(c.system_calculated,false), COALESCE(c.is_edited,false),
		       COALESCE(c.bank_confirmed,false), COALESCE(TO_CHAR(c.bank_confirmed_date,'YYYY-MM-DD'),''), COALESCE(c.bank_reference,''),
		       COALESCE(c.voucher_generated,false), COALESCE(c.voucher_number,''), COALESCE(c.posting_status,''),
		       COALESCE(c.remarks,''), COALESCE(c.receipt_id,''), COALESCE(c.receipt_cleared,false),
		       COALESCE(c.interest_rate,0), COALESCE(c.tds_rate,0), COALESCE(c.holidays_in_period,0),
		       COALESCE(TO_CHAR(c.value_date,'YYYY-MM-DD'),''), COALESCE(c.cashflow_type,''), COALESCE(c.accrual_frequency,''),
		       COALESCE(c.net_amount,0), COALESCE(c.financial_year,'')
		FROM investment.fd_cashflow_schedule c
		LEFT JOIN investment.fd_master m ON m.fd_id = c.fd_id
		WHERE c.cashflow_id = $1 AND COALESCE(c.is_deleted,false) = false`, cashflowID,
	).Scan(
		&row.FDID, &row.EntityID,
		&row.SequenceNumber, &row.EventType,
		&row.EventDate, &row.PeriodStartDate, &row.PeriodEndDate, &row.PeriodDays,
		&row.OpeningPrincipal, &row.InterestAccrued, &row.CapitalizedAmount,
		&row.ClosingPrincipal, &row.TDSAmount, &row.NetCashFlow,
		&row.DayCountCode, &row.Divisor, &row.FormulaUsed, &row.AccrualRatePerDay,
		&row.DrAccountCode, &row.DrAccountName, &row.CrAccountCode, &row.CrAccountName,
		&row.SystemCalculated, &row.IsEdited,
		&row.BankConfirmed, &row.BankConfirmedDate, &row.BankReference,
		&row.VoucherGenerated, &row.VoucherNumber, &row.PostingStatus,
		&row.Remarks, &row.ReceiptID, &row.ReceiptCleared,
		&row.InterestRate, &row.TDSRate, &row.HolidaysInPeriod,
		&row.ValueDate, &row.CashflowType, &row.AccrualFrequency,
		&row.NetAmount, &row.FinancialYear,
	)
	if err != nil {
		return row, fmt.Errorf("load fd cashflow for policy: %w", err)
	}
	return row, nil
}
