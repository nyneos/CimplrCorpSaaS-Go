package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func queryFDClosingEvidencePack(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "c")
	df, dfArgs := dateRangeFilter(ctx, "p", "generated_at", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(p.pack_id::text, '') AS pack_id,
			COALESCE(p.cycle_id::text, '') AS cycle_id,
			COALESCE(c.close_type, '') AS close_type,
			COALESCE(c.entity_id, '') AS entity_id,
			COALESCE(c.entity_name, '') AS entity_name,
			COALESCE(c.bank_id, '') AS bank_id,
			COALESCE(c.bank_name, '') AS bank_name,
			COALESCE(c.currency_code, '') AS currency_code,
			COALESCE(c.financial_period, '') AS financial_period,
			c.period_start,
			c.period_end,
			COALESCE(c.status, '') AS cycle_status,
			COALESCE(p.format, '') AS format,
			COALESCE(p.include_accrual_ledger, false) AS include_accrual_ledger,
			COALESCE(p.include_reconciliation_report, false) AS include_reconciliation_report,
			COALESCE(p.include_exceptions_register, false) AS include_exceptions_register,
			COALESCE(p.include_posting_summary, false) AS include_posting_summary,
			COALESCE(p.include_approval_logs, false) AS include_approval_logs,
			COALESCE(p.include_period_lock_certificate, false) AS include_period_lock_certificate,
			COALESCE(p.include_audit_trail, false) AS include_audit_trail,
			COALESCE(p.include_supporting_documents, false) AS include_supporting_documents,
			COALESCE(p.file_size, 0) AS file_size,
			COALESCE(p.report_count, 0) AS report_count,
			COALESCE(p.page_count, 0) AS page_count,
			COALESCE(p.document_count, 0) AS document_count,
			COALESCE(p.checksum, '') AS checksum,
			COALESCE(p.download_count, 0) AS download_count,
			COALESCE(p.generated_by, '') AS generated_by,
			p.generated_at
		FROM investment.fd_closing_evidence_pack p
		JOIN investment.fd_closing_cycle c ON c.cycle_id = p.cycle_id
		WHERE COALESCE(p.is_deleted, false) = false %s
		  %s
		ORDER BY p.generated_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, df)

	return runSourceQuery(ctx, pool, q, args)
}

func fdClosingScopeBankFilter(ctx context.Context, argOffset int) (string, []any) {
	bf, bfArgs := bankIDFilter(ctx, "fm", argOffset)
	if bf == "" {
		return "", nil
	}
	return fmt.Sprintf(`AND EXISTS (
			SELECT 1 FROM investment.fd_closing_cycle_fd_scope bs
			JOIN investment.fd_master fm ON fm.fd_id = bs.fd_id
			WHERE bs.cycle_id = c.cycle_id AND bs.is_deleted = false AND bs.selection_status = 'APPROVED' %s
		)`, bf), bfArgs
}

func queryFDClosingCycle(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "c")
	bf, bfArgs := fdClosingScopeBankFilter(ctx, len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "c", "period_end", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(c.cycle_id::text, '') AS cycle_id,
			COALESCE(c.entity_id, '') AS entity_id,
			COALESCE(c.entity_name, '') AS entity_name,
			COALESCE(c.bank_id, '') AS bank_id,
			COALESCE(c.bank_name, '') AS bank_name,
			COALESCE(c.close_type, '') AS close_type,
			COALESCE(c.financial_period, '') AS financial_period,
			COALESCE(c.status, 'DRAFT') AS cycle_status,
			CASE
				WHEN COALESCE(agg.total_count, 0) = 0 THEN 'NOT_READY'
				WHEN agg.completed_count = agg.total_count THEN 'READY_TO_CLOSE'
				WHEN agg.critical_incomplete = 0 THEN 'CONDITIONALLY_READY'
				ELSE 'NOT_READY'
			END AS eligibility,
			COALESCE(c.source, '') AS source,
			CASE WHEN COALESCE(c.status, '') = 'LOCKED' THEN COALESCE((
				SELECT COALESCE(e.lock_type, 'HARD_LOCK')
				FROM investment.fd_closing_cycle_event_log e
				WHERE e.cycle_id = c.cycle_id
				  AND e.event_type IN ('LOCK', 'RELOCK')
				ORDER BY e.performed_at DESC
				LIMIT 1), '') ELSE '' END AS lock_type,
			COALESCE(c.initiated_by, '') AS initiated_by,
			COALESCE(la.processing_status, '') AS processing_status,
			c.period_start,
			c.period_end,
			c.initiated_at,
			COALESCE(c.include_matured, false) AS include_matured,
			COALESCE((
				SELECT COUNT(*)::int
				FROM investment.fd_closing_cycle_fd_scope s
				WHERE s.cycle_id = c.cycle_id
				  AND s.is_deleted = false
				  AND s.selection_status = 'APPROVED'
			), 0) AS fd_count,
			COALESCE(agg.readiness_score, 0) AS readiness_score,
			COALESCE(agg.blocker_count, 0)::int AS blocker_count,
			COALESCE(agg.total_count, 0)::int AS checklist_total,
			COALESCE(agg.completed_count, 0)::int AS checklist_completed,
			COALESCE(agg.critical_total, 0)::int AS critical_total,
			COALESCE(agg.critical_completed, 0)::int AS critical_completed,
			COALESCE((
				SELECT COUNT(*)::int
				FROM investment.fd_receipt_exception ex
				JOIN investment.fd_closing_cycle_fd_scope s
				  ON s.fd_id = ex.fd_id
				 AND s.cycle_id = c.cycle_id
				 AND s.is_deleted = false
				 AND s.selection_status = 'APPROVED'
				LEFT JOIN LATERAL (
					SELECT xa.processing_status
					FROM investment.fd_receipt_exception_audit xa
					WHERE xa.exception_id = ex.exception_id
					ORDER BY xa.requested_at DESC, xa.audit_id DESC
					LIMIT 1
				) xla ON true
				WHERE COALESCE(ex.is_deleted, false) = false
				  AND UPPER(COALESCE(ex.exception_status, 'OPEN')) IN ('OPEN', 'IN_REVIEW')
				  AND NOT (UPPER(COALESCE(ex.exception_status, '')) = 'IN_REVIEW' AND COALESCE(xla.processing_status, '') = 'APPROVED')
			), 0) AS open_exceptions
		FROM investment.fd_closing_cycle c
		LEFT JOIN LATERAL (
			SELECT
				COUNT(*) AS total_count,
				COUNT(*) FILTER (WHERE i.status = 'COMPLETED') AS completed_count,
				COUNT(*) FILTER (WHERE i.status = 'BLOCKED') AS blocker_count,
				CASE WHEN COUNT(*) = 0 THEN 0
				     ELSE ROUND(COUNT(*) FILTER (WHERE i.status = 'COMPLETED') * 100.0 / COUNT(*), 2)
				END AS readiness_score,
				COUNT(*) FILTER (WHERE i.is_critical = true AND i.status <> 'COMPLETED') AS critical_incomplete,
				COUNT(*) FILTER (WHERE i.is_critical = true) AS critical_total,
				COUNT(*) FILTER (WHERE i.is_critical = true AND i.status = 'COMPLETED') AS critical_completed
			FROM investment.fd_closing_checklist_item i
			JOIN investment.fd_closing_cycle_fd_scope s
			  ON s.scope_id = i.scope_id AND s.is_deleted = false
			WHERE i.cycle_id = c.cycle_id AND i.is_deleted = false
		) agg ON true
		LEFT JOIN LATERAL (
			SELECT a.processing_status
			FROM investment.fd_closing_cycle_audit a
			WHERE a.cycle_id = c.cycle_id
			ORDER BY GREATEST(a.requested_at, a.checker_at) DESC NULLS LAST
			LIMIT 1
		) la ON true
		WHERE COALESCE(c.is_deleted, false) = false %s
		  %s
		  %s
		ORDER BY c.period_end DESC NULLS LAST, c.cycle_id DESC
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, bf, df)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFDClosingChecklist(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, stepCodes []string) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "c")
	bf, bfArgs := bankIDFilter(ctx, "fm", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "c", "period_end", len(args)+1)
	args = append(args, dfArgs...)
	if len(stepCodes) > 0 {
		df += fmt.Sprintf(" AND i.step_code = ANY($%d)", len(args)+1)
		args = append(args, stepCodes)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(i.item_id::text, '') AS item_id,
			COALESCE(i.cycle_id::text, '') AS cycle_id,
			COALESCE(i.fd_id::text, '') AS fd_id,
			COALESCE(fm.bank_fd_ref_no, '') AS bank_fd_ref_no,
			COALESCE(c.entity_id, '') AS entity_id,
			COALESCE(c.entity_name, '') AS entity_name,
			COALESCE(fm.bank_name, '') AS bank_name,
			COALESCE(c.financial_period, '') AS financial_period,
			COALESCE(i.step_code, '') AS step_code,
			COALESCE(i.step_name, '') AS step_name,
			COALESCE(i.owner_role, '') AS owner_role,
			COALESCE(i.status, '') AS step_status,
			COALESCE(i.evidence_type, '') AS evidence_type,
			COALESCE(i.evidence_ref, '') AS evidence_ref,
			COALESCE(i.blocked_comment, '') AS blocked_comment,
			COALESCE(i.last_updated_by, '') AS last_updated_by,
			COALESCE(la.processing_status, '') AS processing_status,
			i.last_updated_at,
			COALESCE(i.is_critical, false) AS is_critical,
			COALESCE(i.sequence, 0) AS sequence,
			COALESCE(i.exception_count, 0) AS exception_count,
			CASE WHEN i.status = 'COMPLETED' THEN 1 ELSE 0 END AS is_completed,
			CASE WHEN i.status = 'BLOCKED' THEN 1 ELSE 0 END AS is_blocked
		FROM investment.fd_closing_checklist_item i
		JOIN investment.fd_closing_cycle c ON c.cycle_id = i.cycle_id
		JOIN investment.fd_closing_cycle_fd_scope s
		  ON s.scope_id = i.scope_id AND s.is_deleted = false
		LEFT JOIN investment.fd_master fm
		  ON fm.fd_id = i.fd_id AND COALESCE(fm.is_deleted, false) = false
		LEFT JOIN LATERAL (
			SELECT a.processing_status
			FROM investment.fd_closing_checklist_item_audit a
			WHERE a.item_id = i.item_id
			ORDER BY GREATEST(a.requested_at, a.checker_at) DESC NULLS LAST
			LIMIT 1
		) la ON true
		WHERE COALESCE(i.is_deleted, false) = false
		  AND COALESCE(c.is_deleted, false) = false %s
		  %s
		  %s
		ORDER BY c.period_end DESC NULLS LAST, i.cycle_id, i.fd_id, i.sequence
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, bf, df)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFDClosingLockRequest(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "c")
	bf, bfArgs := fdClosingScopeBankFilter(ctx, len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "lr", "requested_at", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(lr.request_id::text, '') AS request_id,
			COALESCE(lr.cycle_id::text, '') AS cycle_id,
			COALESCE(c.entity_id, '') AS entity_id,
			COALESCE(c.entity_name, '') AS entity_name,
			COALESCE(c.financial_period, '') AS financial_period,
			COALESCE(lr.lock_type, '') AS lock_type,
			COALESCE(lr.remarks, '') AS remarks,
			COALESCE(lr.approver_name, '') AS approver_name,
			COALESCE(lr.approver_role, '') AS approver_role,
			COALESCE(lr.processing_status, '') AS processing_status,
			COALESCE(lr.requested_by, '') AS requested_by,
			COALESCE(lr.checker_by, '') AS checker_by,
			COALESCE(lr.checker_comment, '') AS checker_comment,
			COALESCE(lr.applied_by, '') AS applied_by,
			CASE WHEN lr.applied_at IS NOT NULL THEN 'APPLIED' ELSE 'NOT_APPLIED' END AS applied_state,
			lr.lock_effective_date,
			lr.requested_at,
			lr.checker_at,
			lr.applied_at,
			1 AS request_count,
			COALESCE(lr.applied_at::date - lr.requested_at::date, 0) AS days_to_lock
		FROM investment.fd_closing_lock_request lr
		JOIN investment.fd_closing_cycle c ON c.cycle_id = lr.cycle_id
		WHERE COALESCE(lr.is_deleted, false) = false
		  AND COALESCE(c.is_deleted, false) = false %s
		  %s
		  %s
		ORDER BY lr.requested_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, bf, df)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFDClosingReopenRequest(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "c")
	bf, bfArgs := fdClosingScopeBankFilter(ctx, len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "rr", "requested_at", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(rr.request_id::text, '') AS request_id,
			COALESCE(rr.cycle_id::text, '') AS cycle_id,
			COALESCE(c.entity_id, '') AS entity_id,
			COALESCE(c.entity_name, '') AS entity_name,
			COALESCE(c.financial_period, '') AS financial_period,
			COALESCE(rr.reason, '') AS reason,
			COALESCE(rr.impact_summary, '') AS impact_summary,
			COALESCE(rr.approver_name, '') AS approver_name,
			COALESCE(rr.approver_role, '') AS approver_role,
			COALESCE(rr.processing_status, '') AS processing_status,
			COALESCE(rr.validation_status, '') AS validation_status,
			COALESCE(rr.validation_errors, '') AS validation_errors,
			COALESCE(rr.requested_by, '') AS requested_by,
			COALESCE(rr.checker_by, '') AS checker_by,
			COALESCE(rr.checker_comment, '') AS checker_comment,
			COALESCE(rr.reopened_by, '') AS reopened_by,
			COALESCE(rr.relocked_by, '') AS relocked_by,
			rr.requested_at,
			rr.checker_at,
			rr.reopened_at,
			rr.relocked_at,
			COALESCE(rr.accrual_valid, false) AS accrual_valid,
			COALESCE(rr.reconciliation_valid, false) AS reconciliation_valid,
			COALESCE(rr.accounting_valid, false) AS accounting_valid,
			1 AS request_count,
			COALESCE(rr.relocked_at::date - rr.reopened_at::date, 0) AS days_reopened
		FROM investment.fd_closing_reopen_request rr
		JOIN investment.fd_closing_cycle c ON c.cycle_id = rr.cycle_id
		WHERE COALESCE(rr.is_deleted, false) = false
		  AND COALESCE(c.is_deleted, false) = false %s
		  %s
		  %s
		ORDER BY rr.requested_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, bf, df)

	return runSourceQuery(ctx, pool, q, args)
}
