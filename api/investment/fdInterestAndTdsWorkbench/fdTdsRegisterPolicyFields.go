package fdInterestAndTdsWorkbench

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fdTDSRegisterRow is the canonical field set for FD_TDS_REGISTER policy
// checks — every real scalar column on investment.fd_tds_receipt (is_active /
// is_deleted excluded as soft-delete plumbing, same rationale as other
// sub-modules in this family). One struct, reused by every real call site in
// tdsRegister.go — previously Create passed 6 of 21+ real fields while
// Approve/Reject/BulkApprove/BulkReject/BulkDelete each passed only 3
// (tds_id, entity_id, entity_code) and Update passed 4 — see
// database/2026-07-27.sql for the full audit.
type fdTDSRegisterRow struct {
	TDSID             string
	FDID              string
	FDRefNo           string
	EntityID          string
	BankID            string
	ReceiptID         string
	IngestionSource   string
	TDSPlanID         string
	PeriodStart       string
	PeriodEnd         string
	DeductionDate     string
	GrossInterest     float64
	TDSRateApplied    float64
	TDSRateExpected   float64
	TDSExpected       float64
	TDSDeductedActual float64
	TDSVariance       float64
	BankTDSReference  string
	HasPAN            bool
	TDSSection        string
	PANNumber         string
	TDSStatus         string
	ReconcileRunID    string
	ReconcileStatus   string
	ExceptionRaised   bool
	ExceptionReason   string
	JournalEntryID    string
}

// buildFDTDSRegisterPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for FD_TDS_REGISTER (see
// cmd/seedDomainCatalog/fdTdsRegisterCanonical.go). reconcile_action is a
// request-level field (not a row column) — callers that have it (Reconcile)
// add it on top of this map; it is not part of the canonical row.
func buildFDTDSRegisterPolicyFields(row fdTDSRegisterRow) map[string]interface{} {
	return map[string]interface{}{
		"tds_id":              row.TDSID,
		"fd_id":               row.FDID,
		"fd_ref_no":           row.FDRefNo,
		"entity_id":           row.EntityID,
		"entity_code":         row.EntityID,
		"bank_id":             row.BankID,
		"receipt_id":          row.ReceiptID,
		"ingestion_source":    row.IngestionSource,
		"tds_plan_id":         row.TDSPlanID,
		"period_start":        row.PeriodStart,
		"period_end":          row.PeriodEnd,
		"deduction_date":      row.DeductionDate,
		"gross_interest":      row.GrossInterest,
		"tds_rate_applied":    row.TDSRateApplied,
		"tds_rate_expected":   row.TDSRateExpected,
		"tds_expected":        row.TDSExpected,
		"tds_deducted_actual": row.TDSDeductedActual,
		"tds_variance":        row.TDSVariance,
		"bank_tds_reference":  row.BankTDSReference,
		"has_pan":             row.HasPAN,
		"tds_section":         row.TDSSection,
		"pan_number":          row.PANNumber,
		"tds_status":          row.TDSStatus,
		"reconcile_run_id":    row.ReconcileRunID,
		"reconcile_status":    row.ReconcileStatus,
		"exception_raised":    row.ExceptionRaised,
		"exception_reason":    row.ExceptionReason,
		"journal_entry_id":    row.JournalEntryID,
	}
}

// loadFDTDSRegisterRow fetches the full canonical row by tds_id — used by
// Approve/Reject/BulkApprove/BulkReject/BulkDelete and as the base row for
// Update, which only ever receive a tds_id in the request, never the full
// business record.
func loadFDTDSRegisterRow(ctx context.Context, pool *pgxpool.Pool, tdsID string) (fdTDSRegisterRow, error) {
	var row fdTDSRegisterRow
	row.TDSID = tdsID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(fd_id,''), COALESCE(fd_ref_no,''), COALESCE(entity_id,''), COALESCE(bank_id,''),
		       COALESCE(receipt_id,''), COALESCE(ingestion_source,''), COALESCE(tds_plan_id,''),
		       COALESCE(TO_CHAR(period_start,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(period_end,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(deduction_date,'YYYY-MM-DD'),''),
		       COALESCE(gross_interest,0), COALESCE(tds_rate_applied,0), COALESCE(tds_rate_expected,0),
		       COALESCE(tds_expected,0), COALESCE(tds_deducted_actual,0), COALESCE(tds_variance,0),
		       COALESCE(bank_tds_reference,''), COALESCE(has_pan,false), COALESCE(tds_section,''),
		       COALESCE(pan_number,''), COALESCE(tds_status,''), COALESCE(reconcile_run_id,''),
		       COALESCE(reconcile_status,''), COALESCE(exception_raised,false), COALESCE(exception_reason,''),
		       COALESCE(journal_entry_id,'')
		FROM investment.fd_tds_receipt
		WHERE tds_id = $1 AND COALESCE(is_deleted,false) = false`, tdsID,
	).Scan(
		&row.FDID, &row.FDRefNo, &row.EntityID, &row.BankID,
		&row.ReceiptID, &row.IngestionSource, &row.TDSPlanID,
		&row.PeriodStart, &row.PeriodEnd, &row.DeductionDate,
		&row.GrossInterest, &row.TDSRateApplied, &row.TDSRateExpected,
		&row.TDSExpected, &row.TDSDeductedActual, &row.TDSVariance,
		&row.BankTDSReference, &row.HasPAN, &row.TDSSection,
		&row.PANNumber, &row.TDSStatus, &row.ReconcileRunID,
		&row.ReconcileStatus, &row.ExceptionRaised, &row.ExceptionReason,
		&row.JournalEntryID,
	)
	if err != nil {
		return row, fmt.Errorf("load fd tds register row for policy: %w", err)
	}
	return row, nil
}

// applyFDTDSRegisterEdits overlays UpdateTDSRegister's partial edit map
// (period_start, period_end, deduction_date, gross_interest, tds_expected,
// tds_deducted_actual, tds_variance, tds_rate_applied, tds_section, has_pan —
// the only fields that handler's request struct actually carries) onto an
// already-loaded canonical row, so the policy check sees the full row
// as-it-will-be-after-the-edit, not just the touched keys.
func applyFDTDSRegisterEdits(row fdTDSRegisterRow, edits map[string]interface{}) fdTDSRegisterRow {
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
	bl := func(v interface{}) (bool, bool) {
		b, ok := v.(bool)
		return b, ok
	}
	for k, v := range edits {
		switch k {
		case "period_start":
			if s, ok := str(v); ok {
				row.PeriodStart = s
			}
		case "period_end":
			if s, ok := str(v); ok {
				row.PeriodEnd = s
			}
		case "deduction_date":
			if s, ok := str(v); ok {
				row.DeductionDate = s
			}
		case "gross_interest":
			if n, ok := num(v); ok {
				row.GrossInterest = n
			}
		case "tds_expected":
			if n, ok := num(v); ok {
				row.TDSExpected = n
			}
		case "tds_deducted_actual":
			if n, ok := num(v); ok {
				row.TDSDeductedActual = n
			}
		case "tds_variance":
			if n, ok := num(v); ok {
				row.TDSVariance = n
				row.ExceptionRaised = n != 0
			}
		case "tds_rate_applied":
			if n, ok := num(v); ok {
				row.TDSRateApplied = n
				row.TDSRateExpected = n
			}
		case "tds_section":
			if s, ok := str(v); ok {
				row.TDSSection = s
			}
		case "has_pan":
			if b, ok := bl(v); ok {
				row.HasPAN = b
			}
		}
	}
	return row
}
