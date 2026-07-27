package fdReceipt

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fdExceptionRow is the canonical field set for FD_EXCEPTION policy checks —
// every real scalar column on investment.fd_receipt_exception (raised_by/
// raised_at excluded, pure audit plumbing; is_deleted excluded, soft-delete
// plumbing), plus entity_id resolved via fd_master (same rationale as
// fdCashflowRow in fdMaster/fdCashflowPolicyFields.go — not a native column
// but the scope dimension every other FD sub-module's Fields map keys on).
// One struct, reused by EditVariance/ResolveVariance/ApproveVariance/
// RejectVariance — previously every one of these passed the same thin 4
// fields (exception_id, entity_id, entity_code, fd_id, receipt_id) despite
// FD_EXCEPTION having zero rows in domain_catalog.field before this change,
// meaning none of those fields were ever policy-evaluable regardless. See
// database/2026-07-27.sql for the full audit.
type fdExceptionRow struct {
	ExceptionID, ReconcileRunID, ResultID string
	FDID, FDRefNo, ResultType             string
	ReceiptID, TDSID                      string
	ExceptionType, Severity               string
	ExpectedAmount, ReceivedAmount        float64
	VarianceAmount                        float64
	ProposedResolution, ReasonCode        string
	ResolutionRemarks, Attachment         string
	ExceptionStatus, CaseType             string
	VarianceOutcome                       string
	IsActive                              bool
	EntityID                              string
}

// buildFDExceptionPolicyFields maps the canonical row onto the exact
// field_code keys seeded in domain_catalog for FD_EXCEPTION (see
// cmd/seedDomainCatalog/fdReceiptCanonical.go).
func buildFDExceptionPolicyFields(row fdExceptionRow) map[string]interface{} {
	return map[string]interface{}{
		"exception_id":        row.ExceptionID,
		"reconcile_run_id":    row.ReconcileRunID,
		"result_id":           row.ResultID,
		"fd_id":               row.FDID,
		"fd_ref_no":           row.FDRefNo,
		"result_type":         row.ResultType,
		"receipt_id":          row.ReceiptID,
		"tds_id":              row.TDSID,
		"exception_type":      row.ExceptionType,
		"severity":            row.Severity,
		"expected_amount":     row.ExpectedAmount,
		"received_amount":     row.ReceivedAmount,
		"variance_amount":     row.VarianceAmount,
		"proposed_resolution": row.ProposedResolution,
		"reason_code":         row.ReasonCode,
		"resolution_remarks":  row.ResolutionRemarks,
		"attachment":          row.Attachment,
		"exception_status":    row.ExceptionStatus,
		"case_type":           row.CaseType,
		"variance_outcome":    row.VarianceOutcome,
		"is_active":           row.IsActive,
		"entity_id":           row.EntityID,
		"entity_code":         row.EntityID,
	}
}

// loadFDExceptionRow fetches the full canonical row by exception_id, joined
// to fd_master for entity_id. Used when no varianceCaseHeader is already in
// hand; handlers that already called loadVarianceCase should prefer
// fdExceptionRowFromHeader instead, to avoid a duplicate query.
func loadFDExceptionRow(ctx context.Context, pool *pgxpool.Pool, exceptionID string) (fdExceptionRow, error) {
	var row fdExceptionRow
	row.ExceptionID = exceptionID
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(e.reconcile_run_id,''), COALESCE(e.result_id,''),
		       COALESCE(e.fd_id,''), COALESCE(e.fd_ref_no,''), COALESCE(e.result_type,''),
		       COALESCE(e.receipt_id,''), COALESCE(e.tds_id,''),
		       COALESCE(e.exception_type,''), COALESCE(e.severity,''),
		       COALESCE(e.expected_amount,0), COALESCE(e.received_amount,0), COALESCE(e.variance_amount,0),
		       COALESCE(e.proposed_resolution,''), COALESCE(e.reason_code,''),
		       COALESCE(e.resolution_remarks,''), COALESCE(e.attachment,''),
		       COALESCE(e.exception_status,''), COALESCE(e.case_type,''), COALESCE(e.variance_outcome,''),
		       COALESCE(e.is_active,true), COALESCE(m.entity_id,'')
		FROM investment.fd_receipt_exception e
		LEFT JOIN investment.fd_master m ON m.fd_id = e.fd_id AND COALESCE(m.is_deleted,false) = false
		WHERE e.exception_id = $1 AND COALESCE(e.is_deleted,false) = false`, exceptionID,
	).Scan(
		&row.ReconcileRunID, &row.ResultID,
		&row.FDID, &row.FDRefNo, &row.ResultType,
		&row.ReceiptID, &row.TDSID,
		&row.ExceptionType, &row.Severity,
		&row.ExpectedAmount, &row.ReceivedAmount, &row.VarianceAmount,
		&row.ProposedResolution, &row.ReasonCode,
		&row.ResolutionRemarks, &row.Attachment,
		&row.ExceptionStatus, &row.CaseType, &row.VarianceOutcome,
		&row.IsActive, &row.EntityID,
	)
	if err != nil {
		return row, fmt.Errorf("load fd receipt exception for policy: %w", err)
	}
	return row, nil
}

// fdExceptionRowFromHeader builds the canonical row from an
// already-loaded varianceCaseHeader (see loadVarianceCase in
// exceptionAudit.go) plus its resolved entity_id, avoiding a second query in
// handlers that already fetched the header for their own workflow-status
// checks. is_active/reconcile_run_id/result_id/result_type aren't carried on
// varianceCaseHeader (it's a slimmer projection) — those come back zero-value
// here; call loadFDExceptionRow instead when those fields matter to a policy.
func fdExceptionRowFromHeader(hdr *varianceCaseHeader, entityID string) fdExceptionRow {
	if hdr == nil {
		return fdExceptionRow{EntityID: entityID}
	}
	return fdExceptionRow{
		ExceptionID:        hdr.ExceptionID,
		ReconcileRunID:     hdr.ReconcileRunID,
		ResultID:           hdr.ResultID,
		FDID:               hdr.FDID,
		FDRefNo:            hdr.FdRefNo,
		ResultType:         hdr.ResultType,
		ReceiptID:          hdr.ReceiptID,
		TDSID:              hdr.TDSID,
		ExceptionType:      hdr.ExceptionType,
		Severity:           hdr.Severity,
		ExpectedAmount:     hdr.ExpectedAmount,
		ReceivedAmount:     hdr.ReceivedAmount,
		VarianceAmount:     hdr.VarianceAmount,
		ProposedResolution: hdr.ProposedResolution,
		ReasonCode:         hdr.ReasonCode,
		ResolutionRemarks:  hdr.ResolutionRemarks,
		Attachment:         hdr.Attachment,
		ExceptionStatus:    hdr.WorkflowStatus,
		CaseType:           hdr.CaseType,
		VarianceOutcome:    hdr.VarianceOutcome,
		IsActive:           true,
		EntityID:           entityID,
	}
}
