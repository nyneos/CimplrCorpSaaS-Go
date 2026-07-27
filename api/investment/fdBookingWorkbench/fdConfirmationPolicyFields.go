package fdBooking

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fdConfirmationRow is the canonical field set for FD_CONFIRMATION policy checks —
// every real scalar column on investment.fd_confirmation (jsonb columns
// payout_dates/compounding_dates/variance_details excluded, not policy-comparable)
// plus the booking_id/entity_id it's scoped to. One struct, reused by every action
// so Create/Edit/VarianceResolve/VarianceException/Approve/Reject/Delete all see
// the same field universe — previously ranged from 4 fields (Delete) to 14 (Create).
type fdConfirmationRow struct {
	ConfirmationID, BookingID, EntityID string

	ConfirmationMode          string
	ConfirmationReceivedDate  string
	BankFDRefNo               string
	BankReferenceNumber       string
	ActualPrincipal           float64
	ConfirmedRate             float64
	ActualStartDate           string
	ActualMaturityDate        string
	ConfirmedInterestTypeCode string
	ConfirmedFrequencyID      string
	TenorType                 string
	TenorDays                 int
	TenorMonths               int
	TenorYears                int
	PayoutFrequencyID         string
	AccrualFrequencyCode      string
	ResetType                 string
	PenaltyID                 string
	FirstPayoutDate           string
	FirstCapitalizationDate   string
	PrematureClosureTerms     string
	ValueType                 string
	VarianceFlag              bool
	VarianceThresholdBreached bool
	VarianceAction            string
	VarianceRemarks           string
	ConfirmationStatus        string
}

// buildFDConfirmationPolicyFields maps a row onto domain_catalog.field.field_code
// keys for FD_CONFIRMATION. Field-code vocabulary (confirmed_* prefix) matches
// what was already seeded before this change — extended, not renamed.
func buildFDConfirmationPolicyFields(row fdConfirmationRow) map[string]interface{} {
	return map[string]interface{}{
		"confirmation_id":             row.ConfirmationID,
		"booking_id":                  row.BookingID,
		"entity_id":                   row.EntityID,
		"entity_code":                 row.EntityID,
		"confirmation_mode":           row.ConfirmationMode,
		"confirmation_received_date":  row.ConfirmationReceivedDate,
		"bank_fd_reference":           row.BankFDRefNo,
		"bank_reference_number":       row.BankReferenceNumber,
		"confirmed_principal_amount":  row.ActualPrincipal,
		"actual_principal":            row.ActualPrincipal,
		"confirmed_rate":              row.ConfirmedRate,
		"confirmed_interest_rate":     row.ConfirmedRate,
		"confirmed_value_date":        row.ActualStartDate,
		"confirmed_maturity_date":     row.ActualMaturityDate,
		"confirmed_interest_type":     row.ConfirmedInterestTypeCode,
		"confirmed_frequency_id":      row.ConfirmedFrequencyID,
		"tenor_type":                  row.TenorType,
		"confirmed_tenor_days":        row.TenorDays,
		"confirmed_tenor_months":      row.TenorMonths,
		"confirmed_tenor_years":       row.TenorYears,
		"payout_frequency_id":         row.PayoutFrequencyID,
		"accrual_frequency_code":      row.AccrualFrequencyCode,
		"reset_type":                  row.ResetType,
		"penalty_id":                  row.PenaltyID,
		"first_payout_date":           row.FirstPayoutDate,
		"first_capitalization_date":   row.FirstCapitalizationDate,
		"premature_closure_terms":     row.PrematureClosureTerms,
		"value_type":                  row.ValueType,
		"variance_flag":               row.VarianceFlag,
		"variance_threshold_breached": row.VarianceThresholdBreached,
		"variance_action":             row.VarianceAction,
		"variance_remarks":            row.VarianceRemarks,
		"confirmation_status":         row.ConfirmationStatus,
	}
}

// loadFDConfirmationRow fetches the full confirmation row by ID, for
// Approve/Reject/Delete/VarianceException (which only ever receive an ID).
// entity_id is resolved from the linked booking, matching existing call sites.
func loadFDConfirmationRow(ctx context.Context, pool *pgxpool.Pool, confirmationID string) (fdConfirmationRow, error) {
	var row fdConfirmationRow
	row.ConfirmationID = confirmationID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(b.entity_id,''), COALESCE(c.booking_id,''),
		       COALESCE(c.confirmation_mode,''),
		       COALESCE(TO_CHAR(c.confirmation_received_date,'YYYY-MM-DD'),''),
		       COALESCE(c.bank_fd_ref_no,''), COALESCE(c.bank_reference_number,''),
		       COALESCE(c.actual_principal,0), COALESCE(c.confirmed_rate,0),
		       COALESCE(TO_CHAR(c.actual_start_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(c.actual_maturity_date,'YYYY-MM-DD'),''),
		       COALESCE(c.confirmed_interest_type_code,''), COALESCE(c.confirmed_frequency_id,''),
		       COALESCE(c.tenor_type,''), COALESCE(c.tenor_days,0), COALESCE(c.tenor_months,0), COALESCE(c.tenor_years,0),
		       COALESCE(c.payout_frequency_id,''), COALESCE(c.accrual_frequency_code,''), COALESCE(c.reset_type,''),
		       COALESCE(c.penalty_id,''),
		       COALESCE(TO_CHAR(c.first_payout_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(c.first_capitalization_date,'YYYY-MM-DD'),''),
		       COALESCE(c.premature_closure_terms,''), COALESCE(c.value_type,''),
		       COALESCE(c.variance_flag,false), COALESCE(c.variance_threshold_breached,false),
		       COALESCE(c.variance_action,''), COALESCE(c.variance_remarks,''),
		       COALESCE(c.confirmation_status,'')
		FROM investment.fd_confirmation c
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
		WHERE c.confirmation_id = $1 AND COALESCE(c.is_deleted,false) = false`, confirmationID,
	).Scan(
		&row.EntityID, &row.BookingID,
		&row.ConfirmationMode, &row.ConfirmationReceivedDate,
		&row.BankFDRefNo, &row.BankReferenceNumber,
		&row.ActualPrincipal, &row.ConfirmedRate,
		&row.ActualStartDate, &row.ActualMaturityDate,
		&row.ConfirmedInterestTypeCode, &row.ConfirmedFrequencyID,
		&row.TenorType, &row.TenorDays, &row.TenorMonths, &row.TenorYears,
		&row.PayoutFrequencyID, &row.AccrualFrequencyCode, &row.ResetType,
		&row.PenaltyID,
		&row.FirstPayoutDate, &row.FirstCapitalizationDate,
		&row.PrematureClosureTerms, &row.ValueType,
		&row.VarianceFlag, &row.VarianceThresholdBreached,
		&row.VarianceAction, &row.VarianceRemarks,
		&row.ConfirmationStatus,
	)
	if err != nil {
		return row, fmt.Errorf("load fd confirmation for policy: %w", err)
	}
	return row, nil
}
