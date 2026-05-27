package fdReceipt

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// varianceResolutionFields are editable on resolve / edit (not receipt amounts).
type varianceResolutionFields struct {
	ProposedResolution string `json:"proposed_resolution"`
	ReasonCode         string `json:"reason_code"`
	ResolutionRemarks  string `json:"resolution_remarks"`
	Attachment         string `json:"attachment"`
}

// varianceCaseHeader is the slim variance/exception record.
type varianceCaseHeader struct {
	ExceptionID        string  `json:"exception_id"`
	ReconcileRunID     string  `json:"reconcile_run_id"`
	ResultID           string  `json:"result_id"`
	FDID               string  `json:"fd_id"`
	FdRefNo            string  `json:"fd_ref_no"`
	ResultType         string  `json:"result_type"`
	ReceiptID          string  `json:"receipt_id"`
	TDSID              string  `json:"tds_id"`
	ExceptionType      string  `json:"exception_type"`
	Severity           string  `json:"severity"`
	ExpectedAmount     float64 `json:"expected_amount"`
	ReceivedAmount     float64 `json:"received_amount"`
	VarianceAmount     float64 `json:"variance_amount"`
	CaseType           string  `json:"case_type"`
	WorkflowStatus     string  `json:"workflow_status"`
	ExceptionStatus    string  `json:"exception_status"`
	ProcessingStatus   string  `json:"processing_status"`
	VarianceOutcome    string  `json:"variance_outcome"`
	CanPostJournals    bool    `json:"can_post_journals"`
	ProposedResolution string  `json:"proposed_resolution"`
	ReasonCode         string  `json:"reason_code"`
	ResolutionRemarks  string  `json:"resolution_remarks"`
	Attachment         string  `json:"attachment"`
	RaisedBy           string  `json:"raised_by"`
	RaisedAt           string  `json:"raised_at"`
}

// varianceAuditRow mirrors fd_interest_receipt_audit shape (typed columns, no jsonb).
type varianceAuditRow struct {
	AuditID               string `json:"audit_id"`
	ExceptionID           string `json:"exception_id"`
	ActionType            string `json:"action_type"`
	ProcessingStatus      string `json:"processing_status"`
	Reason                string `json:"reason"`
	RequestedBy           string `json:"requested_by"`
	RequestedAt           string `json:"requested_at"`
	CheckerBy             string `json:"checker_by"`
	CheckerAt             string `json:"checker_at"`
	CheckerComment        string `json:"checker_comment"`
	OldExceptionStatus    string `json:"old_exception_status"`
	OldProposedResolution string `json:"old_proposed_resolution"`
	OldReasonCode         string `json:"old_reason_code"`
	OldResolutionRemarks  string `json:"old_resolution_remarks"`
	OldAttachment         string `json:"old_attachment"`
	OldCaseType           string `json:"old_case_type"`
	OldVarianceOutcome    string `json:"old_variance_outcome"`
}

type varianceAuditInsert struct {
	ActionType       string
	ProcessingStatus string
	Reason           string
	RequestedBy      string
	CheckerBy        string
	CheckerComment   string
	Old              varianceAuditOld
}

type varianceAuditOld struct {
	ExceptionStatus    *string
	ProposedResolution *string
	ReasonCode         *string
	ResolutionRemarks  *string
	Attachment         *string
	CaseType           *string
	VarianceOutcome    *string
}

// Header workflow: OPEN | IN_REVIEW | CLOSE only.
// After approve: exception leaves pending queue — only close (lock) or reject (reopen).
func varianceAllowedActions(workflowStatus string, checkerApproved bool) []string {
	switch workflowStatus {
	case "OPEN":
		return []string{"resolve", "reject"}
	case "IN_REVIEW":
		if checkerApproved {
			return []string{"close", "reject"}
		}
		return []string{"resolve", "edit", "approve", "reject"}
	case "CLOSE":
		return []string{}
	default:
		return []string{}
	}
}

func mapHeaderProcessingStatus(exceptionStatus string) string {
	switch exceptionStatus {
	case "OPEN", "IN_REVIEW":
		return "PENDING_APPROVAL"
	case "CLOSE":
		return "APPROVED"
	default:
		return "PENDING_APPROVAL"
	}
}

func effectiveProcessingStatus(latest *varianceAuditRow, exceptionStatus string) string {
	if latest != nil && latest.ProcessingStatus != "" {
		return latest.ProcessingStatus
	}
	return mapHeaderProcessingStatus(exceptionStatus)
}

func loadLatestVarianceAudit(ctx context.Context, pool *pgxpool.Pool, exceptionID string) (*varianceAuditRow, error) {
	var r varianceAuditRow
	err := pool.QueryRow(ctx, `
		SELECT
			audit_id::text,
			exception_id,
			action_type,
			processing_status,
			COALESCE(reason,''),
			COALESCE(NULLIF(req_user.employee_name, ''), requested_by),
			requested_at::text,
			COALESCE(NULLIF(check_user.employee_name, ''), checker_by, ''),
			COALESCE(checker_at::text,''),
			COALESCE(checker_comment,''),
			COALESCE(old_exception_status,''),
			COALESCE(old_proposed_resolution,''),
			COALESCE(old_reason_code,''),
			COALESCE(old_resolution_remarks,''),
			COALESCE(old_attachment,''),
			COALESCE(old_case_type,''),
			COALESCE(old_variance_outcome,'')
		FROM investment.fd_receipt_exception_audit
		LEFT JOIN users req_user
			ON LOWER(req_user.email) = LOWER(requested_by)
			OR req_user.id::text = requested_by
		LEFT JOIN users check_user
			ON LOWER(check_user.email) = LOWER(checker_by)
			OR check_user.id::text = checker_by
		WHERE exception_id=$1
		ORDER BY requested_at DESC, audit_id DESC
		LIMIT 1`,
		exceptionID,
	).Scan(
		&r.AuditID, &r.ExceptionID, &r.ActionType, &r.ProcessingStatus,
		&r.Reason, &r.RequestedBy, &r.RequestedAt,
		&r.CheckerBy, &r.CheckerAt, &r.CheckerComment,
		&r.OldExceptionStatus, &r.OldProposedResolution, &r.OldReasonCode,
		&r.OldResolutionRemarks, &r.OldAttachment, &r.OldCaseType, &r.OldVarianceOutcome,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &r, nil
}

func hasVarianceCheckerApproval(ctx context.Context, pool *pgxpool.Pool, exceptionID string) bool {
	latest, err := loadLatestVarianceAudit(ctx, pool, exceptionID)
	if err != nil || latest == nil {
		return false
	}
	return latest.ProcessingStatus == "APPROVED"
}

func loadVarianceCase(ctx context.Context, pool *pgxpool.Pool, exceptionID string) (*varianceCaseHeader, error) {
	var h varianceCaseHeader
	var varianceOutcome *string
	err := pool.QueryRow(ctx, `
		SELECT
			exception_id,
			COALESCE(reconcile_run_id,''),
			COALESCE(result_id,''),
			COALESCE(fd_id,''),
			COALESCE(fd_ref_no,''),
			COALESCE(result_type,'INTEREST'),
			COALESCE(receipt_id,''),
			COALESCE(tds_id,''),
			COALESCE(exception_type,''),
			COALESCE(severity,''),
			COALESCE(expected_amount,0),
			COALESCE(received_amount,0),
			COALESCE(variance_amount,0),
			COALESCE(case_type,'VARIANCE'),
			COALESCE(exception_status,'OPEN'),
			variance_outcome,
			COALESCE(proposed_resolution,''),
			COALESCE(reason_code,''),
			COALESCE(resolution_remarks,''),
			COALESCE(attachment,''),
			COALESCE(raised_by,''),
			COALESCE(raised_at::text,'')
		FROM investment.fd_receipt_exception
		WHERE exception_id=$1 AND COALESCE(is_deleted,false)=false`,
		exceptionID,
	).Scan(
		&h.ExceptionID, &h.ReconcileRunID, &h.ResultID,
		&h.FDID, &h.FdRefNo, &h.ResultType, &h.ReceiptID, &h.TDSID,
		&h.ExceptionType, &h.Severity,
		&h.ExpectedAmount, &h.ReceivedAmount, &h.VarianceAmount,
		&h.CaseType, &h.WorkflowStatus, &varianceOutcome,
		&h.ProposedResolution, &h.ReasonCode, &h.ResolutionRemarks, &h.Attachment,
		&h.RaisedBy, &h.RaisedAt,
	)
	if err != nil {
		return nil, err
	}
	h.ExceptionStatus = h.WorkflowStatus
	latest, _ := loadLatestVarianceAudit(ctx, pool, exceptionID)
	h.ProcessingStatus = effectiveProcessingStatus(latest, h.WorkflowStatus)
	if varianceOutcome != nil {
		h.VarianceOutcome = *varianceOutcome
	}
	receiptID, _ := resolveExceptionReceiptLinks(ctx, pool, h.ReceiptID, h.TDSID)
	if receiptID != "" {
		h.CanPostJournals = checkReceiptPostingEligibility(ctx, pool, receiptID) == ""
	}
	return &h, nil
}

func loadVarianceAuditTrail(ctx context.Context, pool *pgxpool.Pool, exceptionID string) ([]varianceAuditRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			audit_id::text,
			exception_id,
			action_type,
			processing_status,
			COALESCE(reason,''),
			COALESCE(NULLIF(req_user.employee_name, ''), requested_by),
			requested_at::text,
			COALESCE(NULLIF(check_user.employee_name, ''), checker_by, ''),
			COALESCE(checker_at::text,''),
			COALESCE(checker_comment,''),
			COALESCE(old_exception_status,''),
			COALESCE(old_proposed_resolution,''),
			COALESCE(old_reason_code,''),
			COALESCE(old_resolution_remarks,''),
			COALESCE(old_attachment,''),
			COALESCE(old_case_type,''),
			COALESCE(old_variance_outcome,'')
		FROM investment.fd_receipt_exception_audit
		LEFT JOIN users req_user
			ON LOWER(req_user.email) = LOWER(requested_by)
			OR req_user.id::text = requested_by
		LEFT JOIN users check_user
			ON LOWER(check_user.email) = LOWER(checker_by)
			OR check_user.id::text = checker_by
		WHERE exception_id=$1
		ORDER BY requested_at DESC, audit_id DESC`,
		exceptionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]varianceAuditRow, 0)
	for rows.Next() {
		var r varianceAuditRow
		if err := rows.Scan(
			&r.AuditID, &r.ExceptionID, &r.ActionType, &r.ProcessingStatus,
			&r.Reason, &r.RequestedBy, &r.RequestedAt,
			&r.CheckerBy, &r.CheckerAt, &r.CheckerComment,
			&r.OldExceptionStatus, &r.OldProposedResolution, &r.OldReasonCode,
			&r.OldResolutionRemarks, &r.OldAttachment, &r.OldCaseType, &r.OldVarianceOutcome,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func insertVarianceAudit(ctx context.Context, tx pgx.Tx, exceptionID string, in varianceAuditInsert) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO investment.fd_receipt_exception_audit (
			exception_id, action_type, processing_status, reason,
			requested_by, checker_by, checker_at, checker_comment,
			old_exception_status, old_proposed_resolution, old_reason_code,
			old_resolution_remarks, old_attachment, old_case_type, old_variance_outcome
		) VALUES (
			$1,$2,$3,NULLIF($4,''),$5,
			NULLIF($6,''), CASE WHEN $6 <> '' THEN now() ELSE NULL END, NULLIF($7,''),
			$8,$9,$10,$11,$12,$13,$14
		)`,
		exceptionID, in.ActionType, in.ProcessingStatus, in.Reason, in.RequestedBy,
		in.CheckerBy, in.CheckerComment,
		in.Old.ExceptionStatus, in.Old.ProposedResolution, in.Old.ReasonCode,
		in.Old.ResolutionRemarks, in.Old.Attachment, in.Old.CaseType, in.Old.VarianceOutcome,
	)
	return err
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func snapshotResolution(h *varianceCaseHeader) varianceResolutionFields {
	return varianceResolutionFields{
		ProposedResolution: h.ProposedResolution,
		ReasonCode:         h.ReasonCode,
		ResolutionRemarks:  h.ResolutionRemarks,
		Attachment:         h.Attachment,
	}
}

func auditOldFromHeader(h *varianceCaseHeader) varianceAuditOld {
	return varianceAuditOld{
		ExceptionStatus:    strPtr(h.WorkflowStatus),
		ProposedResolution: strPtr(h.ProposedResolution),
		ReasonCode:         strPtr(h.ReasonCode),
		ResolutionRemarks:  strPtr(h.ResolutionRemarks),
		Attachment:         strPtr(h.Attachment),
		CaseType:           strPtr(h.CaseType),
		VarianceOutcome:    strPtr(h.VarianceOutcome),
	}
}

type varianceAuditParams struct {
	ExceptionType string
	Expected      float64
	Received      float64
	Variance      float64
}

func insertVarianceRaisedAudit(ctx context.Context, pool *pgxpool.Pool, exceptionID, actor string, _ varianceAuditParams) {
	_, _ = pool.Exec(ctx, `
		INSERT INTO investment.fd_receipt_exception_audit (
			exception_id, action_type, processing_status, requested_by
		) VALUES ($1,'CREATE','PENDING_APPROVAL',$2)`,
		exceptionID, actor,
	)
}

func blocksReceiptPosting(ctx context.Context, pool *pgxpool.Pool, receiptID string) bool {
	if receiptID == "" {
		return true
	}
	var n int
	_ = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM investment.fd_receipt_exception e
		WHERE COALESCE(e.is_deleted,false)=false
		  AND (
		    e.receipt_id = $1
		    OR e.tds_id IN (
		      SELECT t.tds_id FROM investment.fd_tds_receipt t
		      WHERE t.receipt_id = $1 AND COALESCE(t.is_deleted,false)=false
		    )
		  )
		  AND (
		    e.exception_status IN ('OPEN','IN_REVIEW')
		    OR (
		      e.exception_status = 'CLOSE'
		      AND (
		        COALESCE(e.case_type,'VARIANCE') <> 'EXCEPTION'
		        OR COALESCE(e.variance_outcome,'') <> 'ACCEPTED'
		      )
		    )
		  )`,
		receiptID,
	).Scan(&n)
	return n > 0
}
