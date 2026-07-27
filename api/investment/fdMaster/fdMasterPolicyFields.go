package fdMaster

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fdMasterRow is the canonical field set for FD_MASTER policy checks — every
// real scalar column on investment.fd_master (jsonb cashflow_terms_snapshot
// excluded, not policy-comparable). One struct, reused by Create/Approve/Reject
// so all three see the same field universe — previously ActivateFD passed 8
// fields while BulkApprove/BulkReject each passed only 4.
type fdMasterRow struct {
	FDID, BookingID, ConfirmationID       string
	EntityID, EntityName                  string
	BankID, BankName                      string
	BankFDRefNo, BankReferenceNumber      string
	PrincipalAmount                       float64
	InterestTypeCode                      string
	InterestRate                          float64
	TenureDays, TenureMonths, TenureYears int
	TenureType                            string
	StartDate, MaturityDate               string
	FrequencyID                           string
	InterestPayoutFrequency               string
	PayoutFrequencyID                     string
	TDSPlanID, BankConfigID               string
	DayCountCode                          string
	SourceAccountID, ProductCode          string
	AutoRenewal                           bool
	ConfirmationCaptured                  bool
	VarianceResolved                      bool
	PolicyComplianceOK                    bool
	AccountingMappingOK                   bool
	FDStatus                              string
	MaturityInstructions                  string
	CashflowGenerated                     bool
	AccrualFrequencyCode                  string
	ResetType                             string
	PenaltyID                             string
	FirstPayoutDate                       string
	FirstCapitalizationDate               string
	ReceiptDate                           string
	PrematureClosureTerms                 string
	SettlementAccountID                   string
	AccountingPosted                      bool
}

func buildFDMasterPolicyFields(row fdMasterRow) map[string]interface{} {
	return map[string]interface{}{
		"fd_id":                     row.FDID,
		"booking_id":                row.BookingID,
		"confirmation_id":           row.ConfirmationID,
		"entity_id":                 row.EntityID,
		"entity_code":               row.EntityID,
		"entity_name":               row.EntityName,
		"bank_id":                   row.BankID,
		"bank_name":                 row.BankName,
		"bank_fd_ref_no":            row.BankFDRefNo,
		"bank_reference_number":     row.BankReferenceNumber,
		"principal_amount":          row.PrincipalAmount,
		"interest_type_code":        row.InterestTypeCode,
		"interest_rate":             row.InterestRate,
		"tenure_days":               row.TenureDays,
		"tenure_months":             row.TenureMonths,
		"tenure_years":              row.TenureYears,
		"tenure_type":               row.TenureType,
		"start_date":                row.StartDate,
		"maturity_date":             row.MaturityDate,
		"frequency_id":              row.FrequencyID,
		"interest_payout_frequency": row.InterestPayoutFrequency,
		"payout_frequency_id":       row.PayoutFrequencyID,
		"tds_plan_id":               row.TDSPlanID,
		"bank_config_id":            row.BankConfigID,
		"day_count_code":            row.DayCountCode,
		"source_account_id":         row.SourceAccountID,
		"product_code":              row.ProductCode,
		"auto_renewal":              row.AutoRenewal,
		"confirmation_captured":     row.ConfirmationCaptured,
		"variance_resolved":         row.VarianceResolved,
		"policy_compliance_ok":      row.PolicyComplianceOK,
		"accounting_mapping_ok":     row.AccountingMappingOK,
		"fd_status":                 row.FDStatus,
		"maturity_instructions":     row.MaturityInstructions,
		"cashflow_generated":        row.CashflowGenerated,
		"accrual_frequency_code":    row.AccrualFrequencyCode,
		"reset_type":                row.ResetType,
		"penalty_id":                row.PenaltyID,
		"first_payout_date":         row.FirstPayoutDate,
		"first_capitalization_date": row.FirstCapitalizationDate,
		"receipt_date":              row.ReceiptDate,
		"premature_closure_terms":   row.PrematureClosureTerms,
		"settlement_account_id":     row.SettlementAccountID,
		"accounting_posted":         row.AccountingPosted,
	}
}

// loadFDMasterRow fetches the full fd_master row by fd_id, for
// Approve/Reject (which only ever receive an fd_id or confirmation_id already
// resolved to fd_id by the caller).
func loadFDMasterRow(ctx context.Context, pool *pgxpool.Pool, fdID string) (fdMasterRow, error) {
	var row fdMasterRow
	row.FDID = fdID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(booking_id,''), COALESCE(confirmation_id,''),
		       COALESCE(entity_id,''), COALESCE(entity_name,''),
		       COALESCE(bank_id,''), COALESCE(bank_name,''),
		       COALESCE(bank_fd_ref_no,''), COALESCE(bank_reference_number,''),
		       COALESCE(principal_amount,0), COALESCE(interest_type_code,''), COALESCE(interest_rate,0),
		       COALESCE(tenure_days,0), COALESCE(tenure_months,0), COALESCE(tenure_years,0), COALESCE(tenure_type,''),
		       COALESCE(TO_CHAR(start_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(maturity_date,'YYYY-MM-DD'),''),
		       COALESCE(frequency_id,''), COALESCE(interest_payout_frequency,''), COALESCE(payout_frequency_id,''),
		       COALESCE(tds_plan_id,''), COALESCE(bank_config_id,''), COALESCE(day_count_code,''),
		       COALESCE(source_account_id,''), COALESCE(product_code,''),
		       COALESCE(auto_renewal,false), COALESCE(confirmation_captured,false), COALESCE(variance_resolved,false),
		       COALESCE(policy_compliance_ok,false), COALESCE(accounting_mapping_ok,false),
		       COALESCE(fd_status,''), COALESCE(maturity_instructions,''), COALESCE(cashflow_generated,false),
		       COALESCE(accrual_frequency_code,''), COALESCE(reset_type,''), COALESCE(penalty_id,''),
		       COALESCE(TO_CHAR(first_payout_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(first_capitalization_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(receipt_date,'YYYY-MM-DD'),''), COALESCE(premature_closure_terms,''),
		       COALESCE(settlement_account_id,''), COALESCE(accounting_posted,false)
		FROM investment.fd_master
		WHERE fd_id = $1 AND COALESCE(is_deleted,false) = false`, fdID,
	).Scan(
		&row.BookingID, &row.ConfirmationID,
		&row.EntityID, &row.EntityName,
		&row.BankID, &row.BankName,
		&row.BankFDRefNo, &row.BankReferenceNumber,
		&row.PrincipalAmount, &row.InterestTypeCode, &row.InterestRate,
		&row.TenureDays, &row.TenureMonths, &row.TenureYears, &row.TenureType,
		&row.StartDate, &row.MaturityDate,
		&row.FrequencyID, &row.InterestPayoutFrequency, &row.PayoutFrequencyID,
		&row.TDSPlanID, &row.BankConfigID, &row.DayCountCode,
		&row.SourceAccountID, &row.ProductCode,
		&row.AutoRenewal, &row.ConfirmationCaptured, &row.VarianceResolved,
		&row.PolicyComplianceOK, &row.AccountingMappingOK,
		&row.FDStatus, &row.MaturityInstructions, &row.CashflowGenerated,
		&row.AccrualFrequencyCode, &row.ResetType, &row.PenaltyID,
		&row.FirstPayoutDate, &row.FirstCapitalizationDate,
		&row.ReceiptDate, &row.PrematureClosureTerms,
		&row.SettlementAccountID, &row.AccountingPosted,
	)
	if err != nil {
		return row, fmt.Errorf("load fd master for policy: %w", err)
	}
	return row, nil
}
