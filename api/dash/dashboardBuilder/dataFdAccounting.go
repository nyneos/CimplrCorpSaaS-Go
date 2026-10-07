package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const fdJournalPredicateSQL = `AND (
		je.fd_id IS NOT NULL OR je.receipt_id IS NOT NULL OR je.accrual_run_id IS NOT NULL
		OR je.closure_request_id IS NOT NULL OR je.entry_type LIKE 'FD\_%'
		OR je.entry_type IN ('CLOSURE','REVERSAL')
	)`

const fdJournalPeriodSQL = `CASE
			WHEN je.accounting_period ~ '^\d{4}-\d{2}$' THEN UPPER(TO_CHAR(TO_DATE(je.accounting_period, 'YYYY-MM'), 'MON YYYY'))
			ELSE UPPER(TRIM(COALESCE(je.accounting_period, '')))
		END`

const fdJournalCurrencySQL = `COALESCE(NULLIF(rc.currency, ''),
			NULLIF(to_jsonb(cf)->>'currency', ''), NULLIF(to_jsonb(cf)->>'currency_code', ''),
			NULLIF(to_jsonb(b)->>'currency', ''), NULLIF(to_jsonb(b)->>'currency_code', ''), 'INR')`

const fdJournalJoinsSQL = `LEFT JOIN investment.fd_master fm ON fm.fd_id = je.fd_id
		LEFT JOIN investment.fd_interest_receipt rc ON rc.receipt_id = je.receipt_id
		LEFT JOIN investment.fd_confirmation cf ON cf.confirmation_id = fm.confirmation_id
		LEFT JOIN investment.fd_booking_request b ON b.booking_id = cf.booking_id`

func fdGlMappingBankFilter(ctx context.Context, argOffset int) (string, []any) {
	ids, _ := ctx.Value(ctxKeyReqBankIDs).([]string)
	if len(ids) == 0 {
		return "", nil
	}
	return fmt.Sprintf("AND (m.bank_id IS NULL OR m.bank_id = ANY($%d))", argOffset), []any{ids}
}

func queryFDJournalEntry(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "je")
	bf, bfArgs := bankIDFilter(ctx, "fm", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "je", "entry_date", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(je.entry_id::text, '') AS entry_id,
			COALESCE(je.entry_type, '') AS entry_type,
			CASE
				WHEN NULLIF(je.reversal_of_entry_id, '') IS NOT NULL THEN 'REVERSAL'
				WHEN NULLIF(je.closure_request_id::text, '') IS NOT NULL THEN 'CLOSURE'
				WHEN NULLIF(je.accrual_run_id, '') IS NOT NULL THEN 'ACCRUAL_RUN'
				WHEN NULLIF(je.receipt_id, '') IS NOT NULL THEN 'RECEIPT'
				WHEN NULLIF(je.fd_id, '') IS NOT NULL THEN 'FD'
				ELSE 'ACTIVITY'
			END AS source_type,
			COALESCE(je.fd_id, '') AS fd_id,
			COALESCE(fm.bank_fd_ref_no, '') AS fd_ref_no,
			COALESCE(je.accrual_run_id, '') AS accrual_run_id,
			COALESCE(je.receipt_id, '') AS receipt_id,
			COALESCE(je.closure_request_id::text, '') AS closure_request_id,
			COALESCE(je.reversal_of_entry_id, '') AS reversal_of_entry_id,
			COALESCE((SELECT r.entry_id::text FROM investment.accounting_journal_entry r
				WHERE r.reversal_of_entry_id = je.entry_id AND COALESCE(r.is_deleted, false) = false
				  AND COALESCE(r.status, '') <> 'REJECTED' ORDER BY r.created_at DESC LIMIT 1), '') AS reversed_by_entry_id,
			COALESCE(je.reversal_type, '') AS reversal_type,
			COALESCE(je.entity_id, '') AS entity_id,
			COALESCE(je.entity_name, '') AS entity_name,
			COALESCE(fm.bank_id, '') AS bank_id,
			COALESCE(fm.bank_name, '') AS bank_name,
			%s AS currency_code,
			%s AS accounting_period,
			COALESCE(cyc.status, 'OPEN') AS period_status,
			COALESCE(je.status, '') AS ledger_status,
			COALESCE(l.processing_status, '') AS processing_status,
			COALESCE(je.gl_mapping_version, '') AS gl_mapping_version,
			COALESCE(je.reason_code, '') AS reason_code,
			COALESCE(je.posting_reference, '') AS posting_reference,
			COALESCE(je.created_by, '') AS created_by,
			COALESCE(l.requested_by, '') AS requested_by,
			COALESCE(je.posted_by, '') AS posted_by,
			COALESCE(l.checker_by, '') AS checker_by,
			je.entry_date,
			je.posted_at,
			je.created_at,
			l.checker_at,
			COALESCE(je.is_reversal, false) AS is_reversal,
			COALESCE(je.total_debit, 0) AS total_debit,
			COALESCE(je.total_credit, 0) AS total_credit,
			(SELECT COUNT(*)::int FROM investment.accounting_journal_entry_line jl WHERE jl.entry_id = je.entry_id) AS line_count,
			ABS(COALESCE(je.total_debit, 0) - COALESCE(je.total_credit, 0)) AS imbalance,
			CASE WHEN ABS(COALESCE(je.total_debit, 0) - COALESCE(je.total_credit, 0)) > 0.005 THEN 1 ELSE 0 END AS is_unbalanced,
			CASE WHEN je.status = 'POSTED' THEN 1 ELSE 0 END AS is_posted,
			CASE WHEN je.status = 'PENDING_APPROVAL' THEN 1 ELSE 0 END AS is_pending_approval,
			CASE WHEN je.status = 'APPROVED' THEN 1 ELSE 0 END AS is_ready_to_post,
			CASE WHEN je.status = 'FAILED' THEN 1 ELSE 0 END AS is_failed,
			1 AS journal_count,
			COALESCE(je.posted_at::date - je.created_at::date, 0) AS days_to_post
		FROM investment.accounting_journal_entry je
		%s
		LEFT JOIN LATERAL (
			SELECT cc.status FROM investment.fd_closing_cycle cc
			WHERE (cc.entity_id = je.entity_id OR cc.entity_name = je.entity_name)
			  AND cc.status IN ('LOCKED', 'CLOSED')
			  AND je.entry_date BETWEEN cc.period_start AND cc.period_end
			  AND COALESCE(cc.is_deleted, false) = false
			ORDER BY cc.period_end DESC LIMIT 1
		) cyc ON true
		LEFT JOIN LATERAL (
			SELECT a.processing_status, a.requested_by, a.checker_by, a.checker_at
			FROM investment.auditaction_fd_accounting_journal a
			WHERE a.entry_id = je.entry_id
			  AND UPPER(COALESCE(a.actiontype, '')) NOT IN ('UPLOAD_FILE', 'DOWNLOAD')
			ORDER BY a.requested_at DESC LIMIT 1
		) l ON true
		WHERE COALESCE(je.is_deleted, false) = false
		  %s %s
		  %s
		  %s
		ORDER BY je.entry_date DESC NULLS LAST, je.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, fdJournalCurrencySQL, fdJournalPeriodSQL, fdJournalJoinsSQL, fdJournalPredicateSQL, ef, bf, df)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFDJournalLine(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "je")
	bf, bfArgs := bankIDFilter(ctx, "fm", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "je", "entry_date", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(jl.line_id::text, '') AS line_id,
			COALESCE(jl.entry_id::text, '') AS entry_id,
			COALESCE(jl.line_number::text, '') AS line_number,
			COALESCE(jl.account_number, '') AS account_number,
			COALESCE(jl.account_name, '') AS account_name,
			COALESCE(jl.account_type, '') AS account_type,
			COALESCE(jl.cost_center, '') AS cost_center,
			COALESCE(jl.profit_center, '') AS profit_center,
			COALESCE(jl.project_code, '') AS project_code,
			COALESCE(jl.tax_code, '') AS tax_code,
			COALESCE(je.entry_type, '') AS entry_type,
			COALESCE(je.status, '') AS ledger_status,
			COALESCE(je.fd_id, '') AS fd_id,
			COALESCE(fm.bank_fd_ref_no, '') AS fd_ref_no,
			COALESCE(je.entity_id, '') AS entity_id,
			COALESCE(je.entity_name, '') AS entity_name,
			COALESCE(fm.bank_name, '') AS bank_name,
			%s AS currency_code,
			%s AS accounting_period,
			je.entry_date,
			COALESCE(jl.debit_amount, 0) AS debit_amount,
			COALESCE(jl.credit_amount, 0) AS credit_amount,
			COALESCE(jl.debit_amount, 0) - COALESCE(jl.credit_amount, 0) AS net_amount
		FROM investment.accounting_journal_entry_line jl
		JOIN investment.accounting_journal_entry je ON je.entry_id = jl.entry_id
		%s
		WHERE COALESCE(je.is_deleted, false) = false
		  %s %s
		  %s
		  %s
		ORDER BY je.entry_date DESC NULLS LAST, jl.entry_id, jl.line_number
		LIMIT NULLIF($1, 0) OFFSET $2
	`, fdJournalCurrencySQL, fdJournalPeriodSQL, fdJournalJoinsSQL, fdJournalPredicateSQL, ef, bf, df)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFDGlMapping(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "m")
	bf, bfArgs := fdGlMappingBankFilter(ctx, len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "m", "created_at", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(m.mapping_id, '') AS mapping_id,
			COALESCE(m.mapping_id, '') || ' v' || COALESCE(m.mapping_version, 0)::text AS mapping_key,
			COALESCE(m.entity_id, '') AS entity_id,
			COALESCE(m.entity_name, '') AS entity_name,
			COALESCE(m.bank_id, '') AS bank_id,
			COALESCE(NULLIF(m.bank_name, ''), 'All banks') AS bank_name,
			COALESCE(m.event_type, '') AS event_type,
			COALESCE(m.status, '') AS mapping_status,
			COALESCE(la.processing_status, '') AS processing_status,
			COALESCE(m.rounding_method, '') AS rounding_method,
			COALESCE(m.created_by, '') AS created_by,
			COALESCE(m.activated_by, '') AS activated_by,
			COALESCE(m.retired_by, '') AS retired_by,
			COALESCE(la.checker_by, '') AS checker_by,
			m.created_at,
			m.activated_at,
			m.retired_at,
			la.checker_at,
			COALESCE(m.mapping_version, 0) AS mapping_version,
			COALESCE(m.rounding_decimals, 0) AS rounding_decimals,
			COALESCE(lc.line_count, 0)::int AS line_count,
			COALESCE(lc.debit_lines, 0)::int AS debit_lines,
			COALESCE(lc.credit_lines, 0)::int AS credit_lines,
			COALESCE((
				SELECT COUNT(*)::int FROM investment.accounting_journal_entry je
				WHERE je.gl_mapping_version = m.mapping_id || ' v' || m.mapping_version::text
				  AND COALESCE(je.is_deleted, false) = false
			), 0) AS journals_using
		FROM investment.fd_gl_mapping m
		LEFT JOIN LATERAL (
			SELECT
				COUNT(*) AS line_count,
				COUNT(*) FILTER (WHERE UPPER(COALESCE(ml.leg, '')) = 'DEBIT') AS debit_lines,
				COUNT(*) FILTER (WHERE UPPER(COALESCE(ml.leg, '')) = 'CREDIT') AS credit_lines
			FROM investment.fd_gl_mapping_line ml
			WHERE ml.mapping_id = m.mapping_id
		) lc ON true
		LEFT JOIN LATERAL (
			SELECT a.processing_status, a.checker_by, a.checker_at
			FROM investment.auditaction_fd_gl_mapping a
			WHERE a.mapping_id = m.mapping_id
			  AND UPPER(COALESCE(a.actiontype, '')) <> 'UPLOAD_FILE'
			ORDER BY a.requested_at DESC NULLS LAST LIMIT 1
		) la ON true
		WHERE COALESCE(m.is_deleted, false) = false %s
		  %s
		  %s
		ORDER BY m.created_at DESC NULLS LAST, m.mapping_id
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, bf, df)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFDGlMappingLine(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "m")
	bf, bfArgs := fdGlMappingBankFilter(ctx, len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "m", "created_at", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(l.line_id::text, '') AS line_id,
			COALESCE(m.mapping_id, '') AS mapping_id,
			COALESCE(m.mapping_id, '') || ' v' || COALESCE(m.mapping_version, 0)::text AS mapping_key,
			COALESCE(m.entity_id, '') AS entity_id,
			COALESCE(m.entity_name, '') AS entity_name,
			COALESCE(NULLIF(m.bank_name, ''), 'All banks') AS bank_name,
			COALESCE(m.event_type, '') AS event_type,
			COALESCE(m.status, '') AS mapping_status,
			COALESCE(l.leg, '') AS leg,
			COALESCE(l.gl_account_code, '') AS gl_account_code,
			COALESCE(l.gl_account_name, '') AS gl_account_name,
			COALESCE(l.account_type, '') AS account_type,
			COALESCE(l.amount_basis, '') AS amount_basis,
			COALESCE(l.cost_center, '') AS cost_center,
			COALESCE(l.profit_center, '') AS profit_center,
			COALESCE(l.project_code, '') AS project_code,
			COALESCE(l.tax_code, '') AS tax_code,
			m.created_at,
			COALESCE(l.line_number, 0) AS line_number,
			COALESCE(m.mapping_version, 0) AS mapping_version,
			COALESCE((
				SELECT COUNT(*)::int FROM investment.fd_gl_mapping_line_allocation al
				WHERE al.mapping_id = l.mapping_id AND al.line_number = l.line_number
				  AND COALESCE(al.is_deleted, false) = false
			), 0) AS allocation_count,
			1 AS line_count
		FROM investment.fd_gl_mapping_line l
		JOIN investment.fd_gl_mapping m ON m.mapping_id = l.mapping_id
		WHERE COALESCE(m.is_deleted, false) = false %s
		  %s
		  %s
		ORDER BY m.created_at DESC NULLS LAST, l.mapping_id, l.line_number
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, bf, df)

	return runSourceQuery(ctx, pool, q, args)
}
