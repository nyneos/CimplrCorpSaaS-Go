package fdMaturityAndRollover

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fdClosurePolicyFields.go — canonical FD_CLOSURE policy field builders.
//
// FD_CLOSURE turned out to genuinely back onto THREE tables, not the two the
// tracker guessed at, spread across two independent, still-both-live
// implementations that share the same policy sub-module code:
//
//  1. investment.fd_closure_request (+ 1:1 detail tables fd_closure_maturity_payout /
//     fd_closure_premature / fd_closure_rollover, keyed by closure_request_id) —
//     the OLDER single-stage workflow (closure.go: InitiateClosure, UpdateClosure,
//     BulkApproveClosureRequest, BulkRejectClosureRequest, DeleteClosureRequest).
//  2. cimplr.fd_closure_initiate — the NEWER two-stage workflow's initiate leg
//     (closureV2.go: CimplrInitiateCreate/Edit/Delete/Approve/Reject).
//  3. cimplr.fd_closure_confirm (+ 1:1 detail tables fd_closure_premature_confirm /
//     fd_closure_rollover_confirm, keyed by closure_confirm_id) — the NEWER
//     workflow's confirm leg (closureV2.go: CimplrConfirmCreate/Edit/Delete/
//     Approve/Reject, plus CimplrPrematureCreate which is a one-step flow
//     that lands directly on this table).
//
// Both workflows are wired to the same runtime.Enforce SubModule: "FD_CLOSURE"
// (see policyCheck.go's fdSubClosure const) and were both hand-rolling thin,
// per-handler Fields{} maps (2-5 keys — typically just id/fd_id/entity_id/
// entity_code, occasionally closure_type or a single amount) despite each
// handler already having the FULL row in hand (either freshly computed at
// Create, or loaded via loadCimplrInitiateOld/loadCimplrConfirmOld — both of
// which already do a `SELECT *` for audit-snapshot purposes right next to the
// enforce call that ignores 90% of it).
//
// Design decision — three separate row types, not one shared struct: the old
// request table and the two new cimplr tables have different primary keys,
// different column sets, and different lifecycle semantics (single-stage vs
// initiate/confirm maker-checker), so forcing them into one struct would mean
// mostly-empty zero-value noise on every call. Where the same business
// concept exists on more than one table under a different column name (e.g.
// old fd_closure_request.effective_closure_date vs new
// fd_closure_initiate/confirm.requested_closure_date), the SAME field_code is
// reused across builders so a policy author writes one rule that fires
// regardless of which workflow produced the record — see the per-builder
// comments below for the exact reuse map.
//
// Scope boundary (documented, not silently dropped): none of the three
// builders reach into the 1:1 detail child tables (fd_closure_maturity_payout/
// premature/rollover on the old side; fd_closure_premature_confirm/
// rollover_confirm on the new side). Those are populated by whichever
// closure_type applies and would mostly be zero-value noise for the other
// two types; the header tables alone already take every live call site from
// 2-5 fields to 29-37. A follow-up can extend the confirm builder with a
// joined premature/rollover-specific variant if a policy author needs those
// child fields specifically.
//
// field_code keys below match what's already seeded in domain_catalog for
// FD_CLOSURE (see cmd/seedDomainCatalog/fdClosureCanonical.go for the full
// backfill) — reusing existing vocabulary rather than inventing per-table
// spellings.

// ─────────────────────────── 1. OLD workflow: fd_closure_request ───────────

// fdClosureRequestRow is the canonical field set for the older single-stage
// FD_CLOSURE workflow (investment.fd_closure_request). One field per real
// scalar column on that table; pure audit/actor plumbing (submitted_by,
// approved_by, rejected_by, their *_email twins, approval_instance_id,
// approved_at/rejected_at, accounting_posted_at, is_deleted/deleted_by/
// deleted_at, created_*/updated_*, last_validated_at, upload_s3_key) is
// excluded — not policy-comparable business data.
type fdClosureRequestRow struct {
	ClosureRequestID      string
	FDID                  string
	BookingID             string
	ConfirmationID        string
	EntityID              string
	EntityName            string
	ClosureType           string
	ClosureStatus         string
	InitiationDate        string
	EffectiveClosureDate  string
	MaturityDate          string
	PrincipalAmount       float64
	AccruedInterest       float64
	TDSDeducted           float64
	PenaltyAmount         float64
	NetPayoutAmount       float64
	SettlementAccountID   string
	SettlementBankName    string
	RolloverFDID          string
	RolloverAmount        float64
	RolloverTenorDays     int
	RolloverInterestRate  float64
	MaturityInstructions  string
	ClosureReason         string
	ClosureNotes          string
	AccountingPosted      bool
	HasVariance           bool
	HasUnresolvedVariance bool
	LastVarianceRunID     string
	VarianceRemark        string
}

// buildFDClosureRequestPolicyFields maps the canonical old-workflow row onto
// domain_catalog field_codes. Several columns reuse a field_code that was
// seeded against the newer cimplr tables because they represent the same
// business concept under a different column name:
//   - effective_closure_date -> "requested_closure_date"
//   - accrued_interest       -> "accrued_interest_till_date"
//   - rollover_tenor_days    -> "tentative_new_tenor_days"
//   - closure_notes          -> "remarks"
//   - last_variance_run_id   -> "variance_run_id"
func buildFDClosureRequestPolicyFields(row fdClosureRequestRow) map[string]interface{} {
	return map[string]interface{}{
		"closure_request_id":         row.ClosureRequestID,
		"fd_id":                      row.FDID,
		"booking_id":                 row.BookingID,
		"confirmation_id":            row.ConfirmationID,
		"entity_id":                  row.EntityID,
		"entity_code":                row.EntityID,
		"entity_name":                row.EntityName,
		"closure_type":               row.ClosureType,
		"closure_status":             row.ClosureStatus,
		"initiation_date":            row.InitiationDate,
		"requested_closure_date":     row.EffectiveClosureDate,
		"maturity_date":              row.MaturityDate,
		"principal_amount":           row.PrincipalAmount,
		"accrued_interest_till_date": row.AccruedInterest,
		"tds_deducted":               row.TDSDeducted,
		"penalty_amount":             row.PenaltyAmount,
		"net_payout_amount":          row.NetPayoutAmount,
		"settlement_account_id":      row.SettlementAccountID,
		"settlement_bank_name":       row.SettlementBankName,
		"rollover_fd_id":             row.RolloverFDID,
		"rollover_amount":            row.RolloverAmount,
		"tentative_new_tenor_days":   row.RolloverTenorDays,
		"rollover_interest_rate":     row.RolloverInterestRate,
		"maturity_instructions":      row.MaturityInstructions,
		"closure_reason":             row.ClosureReason,
		"remarks":                    row.ClosureNotes,
		"accounting_posted":          row.AccountingPosted,
		"has_variance":               row.HasVariance,
		"has_unresolved_variance":    row.HasUnresolvedVariance,
		"variance_run_id":            row.LastVarianceRunID,
		"variance_remark":            row.VarianceRemark,
	}
}

// loadFDClosureRequestRow fetches the full canonical row by closure_request_id
// — used by BulkApproveClosureRequest/BulkRejectClosureRequest/
// DeleteClosureRequest, which only ever receive an id, and by UpdateClosure
// (whose own oldRow/req values are folded in separately at the call site
// since Update already loads a similar projection for its SET-clause logic).
func loadFDClosureRequestRow(ctx context.Context, pool *pgxpool.Pool, closureRequestID string) (fdClosureRequestRow, error) {
	var row fdClosureRequestRow
	row.ClosureRequestID = closureRequestID
	err := pool.QueryRow(ctx, `
		SELECT fd_id, COALESCE(booking_id,''), COALESCE(confirmation_id,''),
		       COALESCE(entity_id,''), COALESCE(entity_name,''),
		       closure_type, closure_status,
		       COALESCE(TO_CHAR(initiation_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(effective_closure_date,'YYYY-MM-DD'),''),
		       COALESCE(TO_CHAR(maturity_date,'YYYY-MM-DD'),''),
		       COALESCE(principal_amount,0), COALESCE(accrued_interest,0), COALESCE(tds_deducted,0),
		       COALESCE(penalty_amount,0), COALESCE(net_payout_amount,0),
		       COALESCE(settlement_account_id,''), COALESCE(settlement_bank_name,''),
		       COALESCE(rollover_fd_id,''), COALESCE(rollover_amount,0), COALESCE(rollover_tenor_days,0),
		       COALESCE(rollover_interest_rate,0), COALESCE(maturity_instructions,''),
		       COALESCE(closure_reason,''), COALESCE(closure_notes,''),
		       COALESCE(accounting_posted,false), COALESCE(has_variance,false), COALESCE(has_unresolved_variance,false),
		       COALESCE(last_variance_run_id,''), COALESCE(variance_remark,'')
		FROM investment.fd_closure_request
		WHERE closure_request_id = $1 AND COALESCE(is_deleted,false) = false`, closureRequestID,
	).Scan(
		&row.FDID, &row.BookingID, &row.ConfirmationID,
		&row.EntityID, &row.EntityName,
		&row.ClosureType, &row.ClosureStatus,
		&row.InitiationDate, &row.EffectiveClosureDate,
		&row.MaturityDate,
		&row.PrincipalAmount, &row.AccruedInterest, &row.TDSDeducted,
		&row.PenaltyAmount, &row.NetPayoutAmount,
		&row.SettlementAccountID, &row.SettlementBankName,
		&row.RolloverFDID, &row.RolloverAmount, &row.RolloverTenorDays,
		&row.RolloverInterestRate, &row.MaturityInstructions,
		&row.ClosureReason, &row.ClosureNotes,
		&row.AccountingPosted, &row.HasVariance, &row.HasUnresolvedVariance,
		&row.LastVarianceRunID, &row.VarianceRemark,
	)
	if err != nil {
		return row, fmt.Errorf("load fd closure request for policy: %w", err)
	}
	return row, nil
}

// ─────────────────────── 2. NEW workflow, initiate leg ─────────────────────

// fdClosureInitiateRow is the canonical field set for the newer two-stage
// workflow's initiate leg (cimplr.fd_closure_initiate). One field per real
// scalar column; approval_instance_id, is_active, is_deleted excluded as
// plumbing.
type fdClosureInitiateRow struct {
	ClosureInitiateID       string
	FDID                    string
	BookingID               string
	ConfirmationID          string
	EntityID                string
	EntityName              string
	BankID                  string
	BankName                string
	FDRefNo                 string
	BankFDRefNo             string
	ClosureType             string
	ClosureStatus           string
	ActionAtMaturity        string
	MaturityDate            string
	RequestedClosureDate    string
	PrincipalAmount         float64
	InterestTypeCode        string
	InterestRate            float64
	ExpectedMaturityValue   float64
	AccruedInterestTillDate float64
	TDSExpected             float64
	NetExpectedAmount       float64
	AutoRenewalFlag         bool
	MaturityStatus          string
	ActionRequired          bool
	RolloverType            string
	RolloverBankType        string
	TentativeNewTenorDays   int
	Remarks                 string
	HasVariance             bool
	HasUnresolvedVariance   bool
	VarianceRunID           string
	RolloverNewBankID       string
	RolloverNewBankName     string
}

func buildFDClosureInitiatePolicyFields(row fdClosureInitiateRow) map[string]interface{} {
	return map[string]interface{}{
		"closure_initiate_id":        row.ClosureInitiateID,
		"fd_id":                      row.FDID,
		"booking_id":                 row.BookingID,
		"confirmation_id":            row.ConfirmationID,
		"entity_id":                  row.EntityID,
		"entity_code":                row.EntityID,
		"entity_name":                row.EntityName,
		"bank_id":                    row.BankID,
		"bank_name":                  row.BankName,
		"fd_ref_no":                  row.FDRefNo,
		"bank_fd_ref_no":             row.BankFDRefNo,
		"closure_type":               row.ClosureType,
		"closure_status":             row.ClosureStatus,
		"action_at_maturity":         row.ActionAtMaturity,
		"maturity_date":              row.MaturityDate,
		"requested_closure_date":     row.RequestedClosureDate,
		"principal_amount":           row.PrincipalAmount,
		"interest_type_code":         row.InterestTypeCode,
		"interest_rate":              row.InterestRate,
		"expected_maturity_value":    row.ExpectedMaturityValue,
		"accrued_interest_till_date": row.AccruedInterestTillDate,
		"tds_expected":               row.TDSExpected,
		"net_expected_amount":        row.NetExpectedAmount,
		"auto_renewal_flag":          row.AutoRenewalFlag,
		"maturity_status":            row.MaturityStatus,
		"action_required":            row.ActionRequired,
		"rollover_type":              row.RolloverType,
		"rollover_bank_type":         row.RolloverBankType,
		"tentative_new_tenor_days":   row.TentativeNewTenorDays,
		"remarks":                    row.Remarks,
		"has_variance":               row.HasVariance,
		"has_unresolved_variance":    row.HasUnresolvedVariance,
		"variance_run_id":            row.VarianceRunID,
		"rollover_new_bank_id":       row.RolloverNewBankID,
		"rollover_new_bank_name":     row.RolloverNewBankName,
	}
}

// loadFDClosureInitiateRow fetches the full initiate row by
// closure_initiate_id — used by CimplrInitiateDelete/Approve/Reject (id-only
// call sites) and CimplrInitiateEdit (which already loads an oldRow map via
// loadCimplrInitiateOld for its own audit snapshot; this is the separate
// policy-facing typed read, same rationale as fdCashflowPolicyFields.go's
// loadFDCashflowRow next to EditCashflowLineItem's own snapshot).
func loadFDClosureInitiateRow(ctx context.Context, pool *pgxpool.Pool, closureInitiateID string) (fdClosureInitiateRow, error) {
	var row fdClosureInitiateRow
	row.ClosureInitiateID = closureInitiateID
	err := pool.QueryRow(ctx, `
		SELECT fd_id, COALESCE(booking_id,''), COALESCE(confirmation_id,''),
		       COALESCE(entity_id,''), COALESCE(entity_name,''),
		       COALESCE(bank_id,''), COALESCE(bank_name,''), COALESCE(fd_ref_no,''), COALESCE(bank_fd_ref_no,''),
		       closure_type, closure_status, COALESCE(action_at_maturity,''),
		       COALESCE(TO_CHAR(maturity_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(requested_closure_date,'YYYY-MM-DD'),''),
		       COALESCE(principal_amount,0), COALESCE(interest_type_code,''), COALESCE(interest_rate,0),
		       COALESCE(expected_maturity_value,0), COALESCE(accrued_interest_till_date,0),
		       COALESCE(tds_expected,0), COALESCE(net_expected_amount,0),
		       COALESCE(auto_renewal_flag,false), COALESCE(maturity_status,''), COALESCE(action_required,false),
		       COALESCE(rollover_type,''), COALESCE(rollover_bank_type,''), COALESCE(tentative_new_tenor_days,0),
		       COALESCE(remarks,''), COALESCE(has_variance,false), COALESCE(has_unresolved_variance,false),
		       COALESCE(variance_run_id,''), COALESCE(rollover_new_bank_id,''), COALESCE(rollover_new_bank_name,'')
		FROM cimplr.fd_closure_initiate
		WHERE closure_initiate_id = $1 AND COALESCE(is_deleted,false) = false`, closureInitiateID,
	).Scan(
		&row.FDID, &row.BookingID, &row.ConfirmationID,
		&row.EntityID, &row.EntityName,
		&row.BankID, &row.BankName, &row.FDRefNo, &row.BankFDRefNo,
		&row.ClosureType, &row.ClosureStatus, &row.ActionAtMaturity,
		&row.MaturityDate, &row.RequestedClosureDate,
		&row.PrincipalAmount, &row.InterestTypeCode, &row.InterestRate,
		&row.ExpectedMaturityValue, &row.AccruedInterestTillDate,
		&row.TDSExpected, &row.NetExpectedAmount,
		&row.AutoRenewalFlag, &row.MaturityStatus, &row.ActionRequired,
		&row.RolloverType, &row.RolloverBankType, &row.TentativeNewTenorDays,
		&row.Remarks, &row.HasVariance, &row.HasUnresolvedVariance,
		&row.VarianceRunID, &row.RolloverNewBankID, &row.RolloverNewBankName,
	)
	if err != nil {
		return row, fmt.Errorf("load fd closure initiate for policy: %w", err)
	}
	return row, nil
}

// ──────────────────────── 3. NEW workflow, confirm leg ─────────────────────

// fdClosureConfirmRow is the canonical field set for the newer two-stage
// workflow's confirm leg (cimplr.fd_closure_confirm) — also the table
// CimplrPrematureCreate's one-step PREMATURE flow lands on. One field per
// real scalar column; approval_instance_id, is_active, is_deleted, upload_s3_key
// excluded as plumbing.
type fdClosureConfirmRow struct {
	ClosureConfirmID      string
	ClosureInitiateID     string
	FDID                  string
	BookingID             string
	ConfirmationID        string
	EntityID              string
	EntityName            string
	BankID                string
	BankName              string
	FDRefNo               string
	BankFDRefNo           string
	ClosureType           string
	ClosureStatus         string
	PostingStatus         string
	ConfirmationMode      string
	BankReferenceNo       string
	ActualPayoutDate      string
	RequestedClosureDate  string
	PrematureReason       string
	PrincipalExpected     float64
	InterestExpected      float64
	TDSExpected           float64
	NetExpected           float64
	PrincipalReceived     float64
	InterestReceived      float64
	TDSDeducted           float64
	NetAmountReceived     float64
	VarianceType          string
	ResolutionAction      string
	Remarks               string
	HasVariance           bool
	HasUnresolvedVariance bool
	VarianceRunID         string
	AccountingPosted      bool
	JournalEntryID        string
	NewBookingID          string
}

func buildFDClosureConfirmPolicyFields(row fdClosureConfirmRow) map[string]interface{} {
	return map[string]interface{}{
		"closure_confirm_id":      row.ClosureConfirmID,
		"closure_initiate_id":     row.ClosureInitiateID,
		"fd_id":                   row.FDID,
		"booking_id":              row.BookingID,
		"confirmation_id":         row.ConfirmationID,
		"entity_id":               row.EntityID,
		"entity_code":             row.EntityID,
		"entity_name":             row.EntityName,
		"bank_id":                 row.BankID,
		"bank_name":               row.BankName,
		"fd_ref_no":               row.FDRefNo,
		"bank_fd_ref_no":          row.BankFDRefNo,
		"closure_type":            row.ClosureType,
		"closure_status":          row.ClosureStatus,
		"posting_status":          row.PostingStatus,
		"confirmation_mode":       row.ConfirmationMode,
		"bank_reference_no":       row.BankReferenceNo,
		"actual_payout_date":      row.ActualPayoutDate,
		"requested_closure_date":  row.RequestedClosureDate,
		"premature_reason":        row.PrematureReason,
		"principal_expected":      row.PrincipalExpected,
		"interest_expected":       row.InterestExpected,
		"tds_expected":            row.TDSExpected,
		"net_expected":            row.NetExpected,
		"principal_received":      row.PrincipalReceived,
		"interest_received":       row.InterestReceived,
		"tds_deducted":            row.TDSDeducted,
		"net_amount_received":     row.NetAmountReceived,
		"variance_type":           row.VarianceType,
		"resolution_action":       row.ResolutionAction,
		"remarks":                 row.Remarks,
		"has_variance":            row.HasVariance,
		"has_unresolved_variance": row.HasUnresolvedVariance,
		"variance_run_id":         row.VarianceRunID,
		"accounting_posted":       row.AccountingPosted,
		"journal_entry_id":        row.JournalEntryID,
		"new_booking_id":          row.NewBookingID,
	}
}

// loadFDClosureConfirmRow fetches the full confirm row by closure_confirm_id
// — used by CimplrConfirmDelete/Approve/Reject and CimplrConfirmEdit (see
// loadFDClosureInitiateRow doc comment for the "separate typed read next to
// the existing map-based audit load" rationale).
func loadFDClosureConfirmRow(ctx context.Context, pool *pgxpool.Pool, closureConfirmID string) (fdClosureConfirmRow, error) {
	var row fdClosureConfirmRow
	row.ClosureConfirmID = closureConfirmID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(closure_initiate_id,''), fd_id, COALESCE(booking_id,''), COALESCE(confirmation_id,''),
		       COALESCE(entity_id,''), COALESCE(entity_name,''),
		       COALESCE(bank_id,''), COALESCE(bank_name,''), COALESCE(fd_ref_no,''), COALESCE(bank_fd_ref_no,''),
		       closure_type, closure_status, posting_status,
		       COALESCE(confirmation_mode,''), COALESCE(bank_reference_no,''),
		       COALESCE(TO_CHAR(actual_payout_date,'YYYY-MM-DD'),''), COALESCE(TO_CHAR(requested_closure_date,'YYYY-MM-DD'),''),
		       COALESCE(premature_reason,''),
		       COALESCE(principal_expected,0), COALESCE(interest_expected,0), COALESCE(tds_expected,0), COALESCE(net_expected,0),
		       COALESCE(principal_received,0), COALESCE(interest_received,0), COALESCE(tds_deducted,0), COALESCE(net_amount_received,0),
		       COALESCE(variance_type,''), COALESCE(resolution_action,''), COALESCE(remarks,''),
		       COALESCE(has_variance,false), COALESCE(has_unresolved_variance,false), COALESCE(variance_run_id,''),
		       COALESCE(accounting_posted,false), COALESCE(journal_entry_id,''), COALESCE(new_booking_id,'')
		FROM cimplr.fd_closure_confirm
		WHERE closure_confirm_id = $1 AND COALESCE(is_deleted,false) = false`, closureConfirmID,
	).Scan(
		&row.ClosureInitiateID, &row.FDID, &row.BookingID, &row.ConfirmationID,
		&row.EntityID, &row.EntityName,
		&row.BankID, &row.BankName, &row.FDRefNo, &row.BankFDRefNo,
		&row.ClosureType, &row.ClosureStatus, &row.PostingStatus,
		&row.ConfirmationMode, &row.BankReferenceNo,
		&row.ActualPayoutDate, &row.RequestedClosureDate,
		&row.PrematureReason,
		&row.PrincipalExpected, &row.InterestExpected, &row.TDSExpected, &row.NetExpected,
		&row.PrincipalReceived, &row.InterestReceived, &row.TDSDeducted, &row.NetAmountReceived,
		&row.VarianceType, &row.ResolutionAction, &row.Remarks,
		&row.HasVariance, &row.HasUnresolvedVariance, &row.VarianceRunID,
		&row.AccountingPosted, &row.JournalEntryID, &row.NewBookingID,
	)
	if err != nil {
		return row, fmt.Errorf("load fd closure confirm for policy: %w", err)
	}
	return row, nil
}

// ─────────────────────────── 4. Shared: upload leg ─────────────────────────

// cimplrClosureUploadPolicyFields backs CimplrClosureUpload/
// CimplrClosureUploadMultipart (TriggerPreUpload — attaching a supporting
// document to whichever leg the id belongs to). Loads the full initiate or
// confirm row (whichever id is set — confirm takes priority when both are,
// same precedence cimplrClosureUploadEntityID already uses) and adds
// file_type on top since that's upload-request metadata, not a table column.
// Falls back to a thin id/file_type map if the row can't be loaded (e.g. the
// parent was deleted between the scope check and here) so the upload flow
// still gets *a* policy decision instead of erroring out.
func cimplrClosureUploadPolicyFields(ctx context.Context, pool *pgxpool.Pool, closureInitiateID, closureConfirmID, fileType string) map[string]interface{} {
	var fields map[string]interface{}
	if strings.TrimSpace(closureConfirmID) != "" {
		if row, err := loadFDClosureConfirmRow(ctx, pool, closureConfirmID); err == nil {
			fields = buildFDClosureConfirmPolicyFields(row)
		}
	}
	if fields == nil && strings.TrimSpace(closureInitiateID) != "" {
		if row, err := loadFDClosureInitiateRow(ctx, pool, closureInitiateID); err == nil {
			fields = buildFDClosureInitiatePolicyFields(row)
		}
	}
	if fields == nil {
		fields = map[string]interface{}{
			"closure_initiate_id": closureInitiateID,
			"closure_confirm_id":  closureConfirmID,
		}
	}
	fields["file_type"] = fileType
	return fields
}
