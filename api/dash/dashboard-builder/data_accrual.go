package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// accrualRunVisibleStatuses mirrors the default filter in GetAccrualRuns —
// ops-only intermediate states (DRAFT, COMPUTED, VALIDATED, …) are excluded
// from dashboard-builder views.
const accrualRunStatusSQL = `AND run_status = ANY(ARRAY['PENDING_APPROVAL','APPROVED','REJECTED','POSTED','POSTED_TO_GL','LOCKED'])`

func queryFDAccrualRunAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "r")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(r.run_id::text, '') AS run_id,
			COALESCE(r.entity_id, '') AS entity_id,
			COALESCE(r.entity_name, '') AS entity_name,
			COALESCE(r.run_type, '') AS run_type,
			COALESCE(r.run_mode, '') AS run_mode,
			COALESCE(r.run_status, '') AS run_status,
			r.run_date,
			r.accrual_period_start,
			r.accrual_period_end,
			COALESCE(r.financial_period, '') AS financial_period,
			COALESCE(r.day_count_convention, '') AS day_count_convention,
			COALESCE(r.rounding_rule, '') AS rounding_rule,
			COALESCE(r.precision_decimals, 0) AS precision_decimals,
			COALESCE(r.fd_status_filter, '') AS fd_status_filter,
			COALESCE(r.fd_inclusion_method, '') AS fd_inclusion_method,
			COALESCE(r.accrual_granularity, '') AS accrual_granularity,
			COALESCE(r.engine_version, '') AS engine_version,
			COALESCE(r.created_by, '') AS created_by,
			r.created_at,
			COALESCE(r.total_interest_accrued, 0) AS total_accrued,
			COALESCE(r.total_tds_applicable, 0) AS total_tds_applicable,
			COALESCE(r.error_count, 0) AS error_count,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.fd_accrual_run r
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.fd_accrual_run_audit 
			WHERE run_id = r.run_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(r.is_deleted, false) = false %s
		  %s
		ORDER BY r.run_date DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, accrualRunStatusSQL)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualLedger(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "l")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(l.ledger_id::text, '') AS ledger_id,
			COALESCE(l.run_id::text, '') AS run_id,
			COALESCE(l.fd_id::text, '') AS fd_id,
			COALESCE(l.fd_ref_no, '') AS fd_ref_no,
			COALESCE(l.bank_name, '') AS bank_name,
			COALESCE(l.entity_name, '') AS entity_name,
			l.fd_start_date,
			l.fd_maturity_date,
			COALESCE(l.principal_amount, 0) AS principal_amount,
			COALESCE(l.interest_rate, 0) AS interest_rate,
			COALESCE(l.accrual_days, 0) AS accrual_days,
			COALESCE(l.divisor, 365) AS divisor,
			COALESCE(l.daily_accrual_rate, 0) AS daily_accrual_rate,
			COALESCE(l.opening_accrued_balance, 0) AS opening_accrued_balance,
			COALESCE(l.period_interest_accrued, 0) AS period_interest_accrued,
			COALESCE(l.tds_applicable_amount, 0) AS tds_applicable_amount,
			COALESCE(l.tds_deducted_in_period, 0) AS tds_deducted_in_period,
			COALESCE(l.net_interest_in_period, 0) AS net_interest_in_period,
			COALESCE(l.closing_accrued_balance, 0) AS closing_accrued_balance,
			COALESCE(l.formula_used, '') AS formula_used,
			COALESCE(l.ledger_row_status, '') AS ledger_row_status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.fd_accrual_ledger l
		JOIN investment.fd_accrual_run ar ON ar.run_id = l.run_id
			AND ar.run_status = ANY(ARRAY['PENDING_APPROVAL','APPROVED','REJECTED','POSTED','POSTED_TO_GL','LOCKED'])
		LEFT JOIN LATERAL (
			SELECT processing_status
			FROM investment.fd_accrual_ledger_audit
			WHERE ledger_id = l.ledger_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(l.is_deleted, false) = false %s
		ORDER BY l.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualExecutionLog(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "r", "entity_name")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(l.log_id::text, '') AS log_id,
			COALESCE(l.run_id::text, '') AS run_id,
			COALESCE(l.fd_id::text, '') AS fd_id,
			COALESCE(l.log_level, '') AS log_level,
			COALESCE(l.event_type, '') AS event_type,
			COALESCE(l.message, '') AS message,
			COALESCE(l.detail::text, '') AS detail,
			l.logged_at
		FROM investment.fd_accrual_run_execution_log l
		JOIN investment.fd_accrual_run r ON l.run_id = r.run_id
			AND r.run_status = ANY(ARRAY['PENDING_APPROVAL','APPROVED','REJECTED','POSTED','POSTED_TO_GL','LOCKED'])
		WHERE 1=1 %s
		ORDER BY l.logged_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualRunAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "r", "entity_name")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(a.audit_id::text, '') AS audit_id,
			COALESCE(a.run_id::text, '') AS run_id,
			COALESCE(a.action_type, '') AS action_type,
			COALESCE(a.processing_status, '') AS processing_status,
			a.requested_at
		FROM investment.fd_accrual_run_audit a
		JOIN investment.fd_accrual_run r ON r.run_id::text = a.run_id::text
			AND r.run_status = ANY(ARRAY['PENDING_APPROVAL','APPROVED','REJECTED','POSTED','POSTED_TO_GL','LOCKED'])
		WHERE 1=1 %s
		ORDER BY a.requested_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualLedgerAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "r", "entity_name")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(a.audit_id::text, '') AS audit_id,
			COALESCE(a.ledger_id::text, '') AS ledger_id,
			COALESCE(a.action_type, '') AS action_type,
			COALESCE(a.processing_status, '') AS processing_status,
			a.requested_at
		FROM investment.fd_accrual_ledger_audit a
		JOIN investment.fd_accrual_run r ON r.run_id::text = a.run_id::text
			AND r.run_status = ANY(ARRAY['PENDING_APPROVAL','APPROVED','REJECTED','POSTED','POSTED_TO_GL','LOCKED'])
		WHERE 1=1 %s
		ORDER BY a.requested_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}


func queryFDAccrualScheduleAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "s")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(s.config_id::text, '') AS config_id,
			COALESCE(s.entity_id, '') AS entity_id,
			COALESCE(s.entity_name, '') AS entity_name,
			COALESCE(s.schedule_frequency, '') AS schedule_frequency,
			COALESCE(s.run_day_of_month, 0) AS run_day_of_month,
			COALESCE(s.run_time::text, '') AS run_time,
			COALESCE(s.default_bank_id_filter, '') AS default_bank_id_filter,
			COALESCE(s.default_fd_status_filter, '') AS default_fd_status_filter,
			COALESCE(s.default_run_mode, '') AS default_run_mode,
			COALESCE(s.auto_submit_for_approval, false) AS auto_submit_for_approval,
			COALESCE(s.is_active, false) AS is_active,
			s.last_run_at,
			COALESCE(s.last_run_id, '') AS last_run_id,
			COALESCE(s.last_run_status, '') AS last_run_status,
			s.next_run_at,
			s.created_at,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.fd_accrual_schedule_config s
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.fd_accrual_schedule_config_audit 
			WHERE config_id = s.config_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(s.is_deleted, false) = false %s
		ORDER BY s.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualScheduleExecutionLog(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "sc")

	parentFilter := ""
	if parentID != "" {
		parentFilter = fmt.Sprintf(" AND sc.config_id = $%d", len(args)+1)
		args = append(args, parentID)
	}

	// Mirrors GetScheduleExecutionLog / loadSchedulerRunsForConfig: anchor on
	// fd_accrual_schedule_config (active only) and left-join scheduler runs.
	q := fmt.Sprintf(`
		SELECT
			COALESCE(r.run_id::text, '')          AS run_id,
			COALESCE(sc.entity_id, '')            AS entity_id,
			COALESCE(NULLIF(r.entity_name, ''), sc.entity_name, '') AS entity_name,
			COALESCE(r.run_mode, sc.default_run_mode, '') AS run_mode,
			COALESCE(r.run_status, sc.last_run_status, '') AS run_status,
			r.accrual_period_start,
			r.accrual_period_end,
			COALESCE(r.financial_period, '')      AS financial_period,
			COALESCE(NULLIF(r.accrual_granularity, ''), sc.accrual_granularity, '') AS accrual_granularity,
			COALESCE(r.fds_in_scope, 0)           AS fds_in_scope,
			COALESCE(r.fds_calculated, 0)         AS fds_calculated,
			COALESCE(r.fds_failed, 0)             AS fds_failed,
			COALESCE(r.total_interest_accrued, 0) AS total_interest_accrued,
			COALESCE(sc.config_id::text, '')      AS schedule_config_id,
			COALESCE(sc.schedule_frequency, '')   AS schedule_frequency,
			COALESCE(sc.is_active, false)         AS schedule_is_active,
			COALESCE(r.created_at, sc.created_at)  AS created_at
		FROM investment.fd_accrual_schedule_config sc
		LEFT JOIN investment.fd_accrual_run r
			ON r.created_by = 'SCHEDULER'
			AND COALESCE(r.is_deleted, false) = false
			AND (
				r.run_id::text IN (
					SELECT DISTINCT el.run_id
					FROM investment.fd_accrual_run_execution_log el
					WHERE COALESCE(el.detail->>'schedule_config_id', '') = sc.config_id::text
				)
				OR (
					COALESCE(sc.last_run_id, '') <> ''
					AND sc.last_run_id = r.run_id::text
				)
			)
		WHERE COALESCE(sc.is_deleted, false) = false
		  AND COALESCE(sc.is_active, false) = true %s %s
		ORDER BY COALESCE(r.created_at, sc.created_at) DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, parentFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
