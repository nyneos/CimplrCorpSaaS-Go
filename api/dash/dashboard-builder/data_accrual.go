package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func queryFDAccrualRunAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "r", 2)
	args := append([]any{limit}, efArgs...)

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
		ORDER BY r.run_date DESC NULLS LAST
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// queryFDAccrualLedger lists ledger rows. When parentID (run_id) is non-empty it
// returns only rows belonging to that accrual run — used for drill-down from fdAccrualRunAll.
func queryFDAccrualLedger(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "l", 2)
	args := append([]any{limit}, efArgs...)

	runFilter := ""
	if parentID != "" {
		runFilter = fmt.Sprintf(" AND l.run_id = $%d", len(args)+1)
		args = append(args, parentID)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(l.ledger_id::text, '') AS ledger_id,
			COALESCE(l.run_id::text, '') AS run_id,
			COALESCE(l.fd_id::text, '') AS fd_id,
			COALESCE(l.entity_id, '') AS entity_id,
			COALESCE(l.entity_name, '') AS entity_name,
			COALESCE(l.bank_name, '') AS bank_name,
			l.accrual_period_start AS accrual_date,
			l.accrual_period_end,
			COALESCE(l.period_interest_accrued, 0) AS accrual_amount,
			COALESCE(l.ledger_row_status, '') AS status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.fd_accrual_ledger l
		LEFT JOIN LATERAL (
			SELECT processing_status
			FROM investment.fd_accrual_ledger_audit
			WHERE ledger_id = l.ledger_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(l.is_deleted, false) = false %s %s
		ORDER BY l.accrual_period_start DESC NULLS LAST
		LIMIT $1
	`, ef, runFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualDetail(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "l", 2)
	args := append([]any{limit}, efArgs...)

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
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.fd_accrual_ledger_audit 
			WHERE ledger_id = l.ledger_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(l.is_deleted, false) = false %s
		ORDER BY l.created_at DESC NULLS LAST
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualFindings(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "r", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(f.finding_id::text, '') AS finding_id,
			COALESCE(f.run_id::text, '') AS run_id,
			COALESCE(f.fd_id::text, '') AS fd_id,
			COALESCE(f.fd_ref_no, '') AS fd_ref_no,
			COALESCE(f.bank_name, '') AS bank_name,
			COALESCE(f.issue_type, '') AS issue_type,
			COALESCE(f.severity, '') AS severity,
			COALESCE(f.issue_description, '') AS issue_description,
			COALESCE(f.suggested_action, '') AS suggested_action,
			COALESCE(f.is_resolved, false) AS is_resolved,
			COALESCE(f.resolved_by, '') AS resolved_by,
			f.resolved_at,
			f.created_at
		FROM investment.fd_accrual_validation_finding f
		LEFT JOIN investment.fd_accrual_run r ON f.run_id = r.run_id
		WHERE 1=1 %s
		ORDER BY f.created_at DESC NULLS LAST
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualExecutionLog(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "r", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

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
		LEFT JOIN investment.fd_accrual_run r ON l.run_id = r.run_id
		WHERE 1=1 %s
		ORDER BY l.logged_at DESC NULLS LAST
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualRunAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "r", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(a.audit_id::text, '') AS audit_id,
			COALESCE(a.run_id::text, '') AS run_id,
			COALESCE(a.action_type, '') AS action_type,
			COALESCE(a.processing_status, '') AS processing_status,
			a.requested_at
		FROM investment.fd_accrual_run_audit a
		LEFT JOIN investment.fd_accrual_run r ON r.run_id::text = a.run_id::text
		WHERE 1=1 %s
		ORDER BY a.requested_at DESC NULLS LAST
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualLedgerAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "r", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(a.audit_id::text, '') AS audit_id,
			COALESCE(a.ledger_id::text, '') AS ledger_id,
			COALESCE(a.action_type, '') AS action_type,
			COALESCE(a.processing_status, '') AS processing_status,
			a.requested_at
		FROM investment.fd_accrual_ledger_audit a
		LEFT JOIN investment.fd_accrual_run r ON r.run_id::text = a.run_id::text
		WHERE 1=1 %s
		ORDER BY a.requested_at DESC NULLS LAST
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualExceptions(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "r", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(e.exception_id::text, '') AS exception_id,
			COALESCE(e.fd_id::text, '') AS fd_id,
			COALESCE(e.exception_type, '') AS exception_type,
			e.created_at
		FROM investment.fd_accrual_exception e
		LEFT JOIN investment.fd_accrual_run r ON r.run_id::text = e.run_id::text
		WHERE COALESCE(e.is_deleted, false) = false %s
		ORDER BY e.created_at DESC NULLS LAST
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualScheduleAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "s", 2)
	args := append([]any{limit}, efArgs...)

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
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFDAccrualScheduleExecutionLog(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "r", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

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
			COALESCE(r.total_interest_accrued, 0) AS total_accrued,
			r.created_at
		FROM investment.fd_accrual_run r
		WHERE COALESCE(r.is_deleted, false) = false AND r.run_type = 'SCHEDULED' %s
		ORDER BY r.run_date DESC NULLS LAST
		LIMIT $1
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
