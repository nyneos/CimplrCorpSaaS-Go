package fdReceipt

import (
	"CimplrCorpSaas/api/constants"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type varianceListFilters struct {
	ReconcileRunID  string
	FdID            string
	ExceptionStatus string
}

// loadVarianceListRows returns exceptions with latest audit row (ordered by latest audit time).
func loadVarianceListRows(ctx context.Context, pool *pgxpool.Pool, f varianceListFilters) ([]map[string]interface{}, error) {
	sql := `
		SELECT
			e.exception_id,
			e.reconcile_run_id,
			e.result_id,
			e.fd_id,
			COALESCE(e.fd_ref_no,''),
			COALESCE(e.result_type,'INTEREST'),
			COALESCE(e.receipt_id,''),
			COALESCE(e.tds_id,''),
			COALESCE(e.exception_type,''),
			COALESCE(e.severity,''),
			COALESCE(e.expected_amount,0),
			COALESCE(e.received_amount,0),
			COALESCE(e.variance_amount,0),
			COALESCE(e.case_type,'VARIANCE'),
			COALESCE(e.exception_status,'OPEN'),
			e.variance_outcome,
			COALESCE(e.proposed_resolution,''),
			COALESCE(e.reason_code,''),
			COALESCE(e.resolution_remarks,''),
			COALESCE(e.attachment,''),
			COALESCE(e.raised_by,''),
			COALESCE(e.raised_at::text,''),
			COALESCE(e.is_active,true),
			COALESCE(e.is_deleted,false),
			COALESCE(la.audit_id::text,''),
			COALESCE(la.action_type,''),
			COALESCE(la.processing_status,''),
			COALESCE(la.reason,''),
			COALESCE(la.requested_by,''),
			COALESCE(la.requested_at::text,''),
			COALESCE(la.checker_by,''),
			COALESCE(la.checker_at::text,''),
			COALESCE(la.checker_comment,''),
			COALESCE(la.old_exception_status,''),
			COALESCE(la.old_proposed_resolution,''),
			COALESCE(la.old_reason_code,''),
			COALESCE(la.old_resolution_remarks,''),
			COALESCE(la.old_attachment,''),
			COALESCE(la.old_case_type,''),
			COALESCE(la.old_variance_outcome,''),
			COALESCE(ca.requested_at::text, e.raised_at::text,''),
			COALESCE(ca.requested_by, e.raised_by,''),
			COALESCE(ea.requested_at::text,''),
			COALESCE(ea.requested_by,'')
		FROM investment.fd_receipt_exception e
		LEFT JOIN LATERAL (
			SELECT a.*
			FROM investment.fd_receipt_exception_audit a
			WHERE a.exception_id = e.exception_id
			ORDER BY a.requested_at DESC, a.audit_id DESC
			LIMIT 1
		) la ON true
		LEFT JOIN LATERAL (
			SELECT a.requested_at, a.requested_by
			FROM investment.fd_receipt_exception_audit a
			WHERE a.exception_id = e.exception_id AND a.action_type = 'CREATE'
			ORDER BY a.requested_at ASC, a.audit_id ASC
			LIMIT 1
		) ca ON true
		LEFT JOIN LATERAL (
			SELECT a.requested_at, a.requested_by
			FROM investment.fd_receipt_exception_audit a
			WHERE a.exception_id = e.exception_id AND a.action_type = 'EDIT'
			ORDER BY a.requested_at DESC, a.audit_id DESC
			LIMIT 1
		) ea ON true
		WHERE COALESCE(e.is_deleted,false) = false`

	args := []interface{}{}
	argIdx := 1
	if f.ReconcileRunID != "" {
		sql += fmt.Sprintf(" AND e.reconcile_run_id=$%d", argIdx)
		args = append(args, f.ReconcileRunID)
		argIdx++
	}
	if f.FdID != "" {
		sql += fmt.Sprintf(" AND e.fd_id=$%d", argIdx)
		args = append(args, f.FdID)
		argIdx++
	}
	if f.ExceptionStatus != "" {
		sql += fmt.Sprintf(" AND e.exception_status=$%d", argIdx)
		args = append(args, f.ExceptionStatus)
		argIdx++
	}
	sql += " ORDER BY COALESCE(la.requested_at, e.raised_at) DESC NULLS LAST"

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var (
			exceptionID, reconcileRunID, resultID, fdID, fdRefNo, resultType     string
			receiptID, tdsID, exceptionType, severity                            string
			caseType, exceptionStatus                                            string
			proposedResolution, reasonCode, resolutionRemarks, attachment        string
			raisedBy, raisedAt, auditID, actionType, auditProcessingStatus       string
			reason, requestedBy, requestedAt, checkerBy, checkerAt               string
			checkerComment                                                       string
			oldExceptionStatus, oldProposedResolution, oldReasonCode             string
			oldResolutionRemarks, oldAttachment, oldCaseType, oldVarianceOutcome string
			createdAt, createdBy, editedAt, editedBy                             string
			varianceOutcome                                                      *string
			expectedAmount, receivedAmount, varianceAmount                       float64
			isActive, isDeleted                                                  bool
		)
		if err := rows.Scan(
			&exceptionID, &reconcileRunID, &resultID, &fdID, &fdRefNo, &resultType,
			&receiptID, &tdsID, &exceptionType, &severity,
			&expectedAmount, &receivedAmount, &varianceAmount,
			&caseType, &exceptionStatus, &varianceOutcome,
			&proposedResolution, &reasonCode, &resolutionRemarks, &attachment,
			&raisedBy, &raisedAt, &isActive, &isDeleted,
			&auditID, &actionType, &auditProcessingStatus,
			&reason, &requestedBy, &requestedAt,
			&checkerBy, &checkerAt, &checkerComment,
			&oldExceptionStatus, &oldProposedResolution, &oldReasonCode,
			&oldResolutionRemarks, &oldAttachment, &oldCaseType, &oldVarianceOutcome,
			&createdAt, &createdBy, &editedAt, &editedBy,
		); err != nil {
			return nil, err
		}

		processingStatus := auditProcessingStatus
		if processingStatus == "" {
			processingStatus = mapHeaderProcessingStatus(exceptionStatus)
		}
		varianceOutcomeStr := ""
		if varianceOutcome != nil {
			varianceOutcomeStr = *varianceOutcome
		}
		checkerApproved := auditProcessingStatus == constants.StatusApproved && exceptionStatus == "IN_REVIEW"
		awaitingClose := checkerApproved && exceptionStatus == "IN_REVIEW"
		isLocked := exceptionStatus == "CLOSE"

		row := map[string]interface{}{
			"exception_id":            exceptionID,
			"reconcile_run_id":        reconcileRunID,
			"result_id":               resultID,
			"fd_id":                   fdID,
			"fd_ref_no":               fdRefNo,
			"result_type":             resultType,
			"receipt_id":              receiptID,
			"tds_id":                  tdsID,
			"exception_type":          exceptionType,
			"severity":                severity,
			"expected_amount":         expectedAmount,
			"received_amount":         receivedAmount,
			"variance_amount":         varianceAmount,
			"case_type":               caseType,
			"exception_status":        exceptionStatus,
			"workflow_status":         exceptionStatus,
			"variance_outcome":        varianceOutcomeStr,
			"proposed_resolution":     proposedResolution,
			"reason_code":             reasonCode,
			"resolution_remarks":      resolutionRemarks,
			"attachment":              attachment,
			"raised_by":               raisedBy,
			"raised_at":               raisedAt,
			"is_active":               isActive,
			"is_deleted":              isDeleted,
			"processing_status":       processingStatus,
			"checker_approved":        checkerApproved,
			"awaiting_close":          awaitingClose,
			"is_locked":               isLocked,
			"allowed_actions":         varianceAllowedActions(exceptionStatus, checkerApproved),
			"created_at":              createdAt,
			"created_by":              createdBy,
			"edited_at":               editedAt,
			"edited_by":               editedBy,
			"audit_id":                auditID,
			"action_type":             actionType,
			"requested_at":            requestedAt,
			"requested_by":            requestedBy,
			"checker_by":              checkerBy,
			"checker_at":              checkerAt,
			"checker_comment":         checkerComment,
			"reason":                  reason,
			"old_exception_status":    oldExceptionStatus,
			"old_proposed_resolution": oldProposedResolution,
			"old_reason_code":         oldReasonCode,
			"old_resolution_remarks":  oldResolutionRemarks,
			"old_attachment":          oldAttachment,
			"old_case_type":           oldCaseType,
			"old_variance_outcome":    oldVarianceOutcome,
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
