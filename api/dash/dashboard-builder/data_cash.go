package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Bank Statements ────────────────────────────────────────────────────────
func queryCashBankStatements(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "b", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (bankstatementid)
				bankstatementid,
				processing_status,
				requested_at,
				checker_at
			FROM cimplrcorpsaas.auditactionbankstatement
			ORDER BY bankstatementid,
			         GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp),
			                  COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(b.bank_statement_id::text, '')     AS statement_id,
			COALESCE(b.entity_id, '')                   AS entity_id,
			COALESCE(e.entity_name, '')                 AS entity_name,
			COALESCE(b.account_number, '')              AS account_number,
			COALESCE(mb.bank_name, '')                  AS bank_name,
			b.statement_period_start,
			b.statement_period_end,
			b.uploaded_at,
			COALESCE(b.opening_balance, 0)              AS opening_balance,
			COALESCE(b.closing_balance, 0)              AS closing_balance,
			COALESCE(a.processing_status, '')           AS processing_status
		FROM cimplrcorpsaas.bank_statements b
		LEFT JOIN public.masterentitycash e ON e.entity_id = b.entity_id
		LEFT JOIN public.masterbankaccount mba ON mba.account_number = b.account_number AND COALESCE(mba.is_deleted, false) = false
		LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
		LEFT JOIN latest_audit a ON a.bankstatementid = b.bank_statement_id
		WHERE COALESCE(b.is_deleted, false) = false %s
		ORDER BY b.uploaded_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// parentID = bank_statement_id to drill into a specific statement's transactions.
func queryCashBankStatementTransactions(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "s", 2)
	args := append([]any{limit}, efArgs...)

	stmtFilter := ""
	if parentID != "" {
		stmtFilter = fmt.Sprintf(" AND t.bank_statement_id = $%d", len(args)+1)
		args = append(args, parentID)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(t.transaction_id::text, '')    AS transaction_id,
			COALESCE(t.bank_statement_id::text, '') AS statement_id,
			COALESCE(s.entity_id, '')               AS entity_id,
			COALESCE(e.entity_name, '')             AS entity_name,
			COALESCE(t.tran_id, '')                 AS tran_id,
			t.value_date,
			t.transaction_date,
			COALESCE(t.description, '')             AS description,
			COALESCE(t.withdrawal_amount, 0)        AS withdrawal_amount,
			COALESCE(t.deposit_amount, 0)           AS deposit_amount,
			COALESCE(t.balance, 0)                  AS balance,
			COALESCE(t.category_id::text, '')       AS category_id,
			COALESCE(t.misclassified_flag, false)   AS misclassified_flag,
			COALESCE(t.narration_clean, '')         AS narration_clean,
			COALESCE(t.narration_ref, '')           AS narration_ref,
			COALESCE(t.payment_channel, '')         AS payment_channel,
			COALESCE(t.confidence_score, 0)         AS confidence_score,
			COALESCE(t.classification_step, '')     AS classification_step
		FROM cimplrcorpsaas.bank_statement_transactions t
		JOIN cimplrcorpsaas.bank_statements s ON t.bank_statement_id = s.bank_statement_id
		LEFT JOIN public.masterentitycash e ON s.entity_id = e.entity_id
		WHERE 1=1 %s %s
		ORDER BY t.value_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef, stmtFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Payable / Receivable ───────────────────────────────────────────────────
func queryCashPayableReceivable(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	_ = entityIDs // entity filtering via entity_name handled in query if needed
	args := []any{limit}

	q := `
		WITH pr AS (
			SELECT
				payable_id::text             AS transaction_id,
				COALESCE(entity_name, '')    AS entity_name,
				'PAYABLE'                    AS type,
				COALESCE(amount, 0)          AS amount,
				due_date,
				COALESCE(counterparty_name, '') AS counterparty_name
			FROM public.tr_payables
			WHERE COALESCE(is_deleted, false) = false
			UNION ALL
			SELECT
				receivable_id::text               AS transaction_id,
				COALESCE(entity_name, '')         AS entity_name,
				'RECEIVABLE'                      AS type,
				COALESCE(invoice_amount, 0)       AS amount,
				due_date,
				COALESCE(counterparty_name, '')   AS counterparty_name
			FROM public.tr_receivables
			WHERE COALESCE(is_deleted, false) = false
		)
		SELECT
			COALESCE(pr.transaction_id,   '') AS transaction_id,
			COALESCE(pr.entity_name,      '') AS entity_name,
			COALESCE(pr.type,             '') AS type,
			COALESCE(pr.amount,            0) AS amount,
			pr.due_date,
			COALESCE(pr.counterparty_name,'') AS counterparty_name
		FROM pr
		ORDER BY pr.due_date ASC NULLS LAST
		LIMIT NULLIF($1, 0)
	`

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Fund Planning ──────────────────────────────────────────────────────────
func queryCashFundPlanSummary(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	// fund_plan_groups has entity_name (not entity_id), and group_id is integer.
	// Avoid CTE join on group_id to prevent implicit integer cast from empty strings.
	q := `
		SELECT
			COALESCE(s.group_id::text, '') AS plan_id,
			COALESCE(s.entity_name,    '') AS entity_name,
			COALESCE(s.entity_name,    '') AS entity_id,
			COALESCE(s.currency,       '') AS currency,
			COALESCE(s.direction,      '') AS direction,
			COALESCE(s.total_amount,    0) AS total_amount,
			COALESCE(s.horizon,         0) AS horizon,
			COALESCE((
				SELECT processing_status FROM public.auditaction_fund_plan_groups
				WHERE group_id::text = s.group_id::text
				ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp),
				                  COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
				LIMIT 1
			), '') AS processing_status
		FROM public.fund_plan_groups s
		ORDER BY s.group_id DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`
	args := []any{limit}

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// parentID = group_id to drill into a specific fund plan's line items.
func queryCashFundPlanDetails(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "g", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	groupFilter := ""
	if parentID != "" {
		groupFilter = fmt.Sprintf(" AND d.group_id = $%d", len(args)+1)
		args = append(args, parentID)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(d.line_id::text, '')    AS detail_id,
			COALESCE(d.group_id::text, '')   AS plan_id,
			COALESCE(g.entity_name, '')      AS entity_name,
			COALESCE(d.category, '')         AS category,
			COALESCE(d.amount, 0)            AS amount,
			COALESCE(d.currency, '')         AS currency,
			COALESCE(d.allocated_amount, 0)  AS allocated_amount
		FROM public.fund_plan_lines d
		LEFT JOIN public.fund_plan_groups g ON d.group_id = g.group_id
		WHERE 1=1 %s %s
		ORDER BY d.line_id DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef, groupFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Sweep Configuration & Execution ────────────────────────────────────────
func queryCashSweepConfig(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "c", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (sweep_id)
				sweep_id,
				processing_status,
				requested_at,
				checker_at
			FROM cimplrcorpsaas.auditactionsweepconfiguration
			ORDER BY sweep_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(c.sweep_id::text, '') AS config_id,
			COALESCE(c.entity_name, '') AS entity_id,
			COALESCE(c.source_bank_name, '') AS bank_name,
			COALESCE(c.sweep_type, '') AS sweep_type,
			COALESCE(c.frequency, '') AS frequency,
			c.updated_at,
			COALESCE(a.processing_status, '') AS processing_status
		FROM cimplrcorpsaas.sweepconfiguration c
		LEFT JOIN latest_audit a ON a.sweep_id = c.sweep_id::text
		WHERE COALESCE(c.is_deleted, false) = false %s
		ORDER BY c.updated_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashSweepInitiation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "c", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (initiation_id::text)
				initiation_id::text     AS initiation_id,
				processing_status,
				requested_at,
				checker_at
			FROM cimplrcorpsaas.auditactionsweepinitiation
			WHERE actiontype IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY initiation_id::text, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(i.initiation_id::text, '') AS initiation_id,
			COALESCE(i.sweep_id::text,      '') AS config_id,
			COALESCE(c.entity_name,         '') AS entity_name,
			COALESCE(c.source_bank_name,    '') AS source_bank_name,
			COALESCE(c.target_bank_name,    '') AS target_bank_name,
			COALESCE(c.sweep_type,          '') AS sweep_type,
			COALESCE(i.initiated_by,        '') AS initiated_by,
			i.initiation_time,
			COALESCE(i.overridden_amount,    0) AS overridden_amount,
			COALESCE(a.processing_status,   '') AS processing_status
		FROM cimplrcorpsaas.sweep_initiation i
		JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id::text = i.sweep_id::text
		LEFT JOIN latest_audit a ON a.initiation_id = i.initiation_id::text
		WHERE COALESCE(c.is_deleted, false) = false
		  AND COALESCE(i.is_deleted, false) = false %s
		ORDER BY i.initiation_time DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashSweepExecutionLogs(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "c", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(l.execution_id::text, '') AS log_id,
			COALESCE(l.sweep_id::text, '') AS config_id,
			COALESCE(l.initiation_id::text, '') AS initiation_id,
			COALESCE(c.entity_name, '') AS entity_name,
			COALESCE(l.status, '') AS status,
			l.execution_date,
			COALESCE(l.amount_swept, 0) AS amount_swept,
			COALESCE(l.from_account, '') AS from_account,
			COALESCE(l.to_account, '') AS to_account,
			COALESCE(l.balance_before, 0) AS balance_before,
			COALESCE(l.balance_after, 0) AS balance_after,
			COALESCE(l.error_message, '') AS error_message
		FROM cimplrcorpsaas.sweep_execution_log l
		LEFT JOIN cimplrcorpsaas.sweepconfiguration c ON l.sweep_id = c.sweep_id::text
		WHERE 1=1 %s
		ORDER BY l.execution_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashSweepAllExecutionLogs(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	return queryCashSweepExecutionLogs(ctx, pool, entityIDs, limit)
}

func queryCashSweepStatistics(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "c", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT 
			c.entity_name AS entity_id,
			COUNT(*) as total_executions,
			COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful,
			COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed,
			COUNT(CASE WHEN status = 'INSUFFICIENT_FUNDS' THEN 1 END) as insufficient_funds,
			COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN amount_swept ELSE 0 END), 0) as total_amount_swept,
			MAX(execution_date) as last_execution
		FROM cimplrcorpsaas.sweep_execution_log l
		JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = l.sweep_id
		WHERE COALESCE(c.is_deleted, false) = false %s
		GROUP BY c.entity_name
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Projections ────────────────────────────────────────────────────────────
func queryCashProjectionList(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	q := `
		WITH latest_audit AS (
			SELECT DISTINCT ON (proposal_id)
				proposal_id,
				processing_status,
				requested_at,
				checker_at
			FROM cimplrcorpsaas.audit_action_cashflow_proposal
			ORDER BY proposal_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(p.proposal_id::text, '') AS proposal_id,
			COALESCE(p.proposal_name, '') AS proposal_name,
			COALESCE(p.base_currency_code, '') AS base_currency_code,
			p.effective_date,
			COALESCE(p.upload_s3_key, '') AS upload_s3_key,
			COALESCE(a.processing_status, 'N/A') AS processing_status,
			COUNT(DISTINCT i.item_id) AS item_count
		FROM cimplrcorpsaas.cashflow_proposal p
		LEFT JOIN cimplrcorpsaas.cashflow_proposal_item i
			ON p.proposal_id = i.proposal_id
		   AND COALESCE(i.is_deleted, false) = false
		LEFT JOIN latest_audit a ON a.proposal_id = p.proposal_id::text
		WHERE COALESCE(p.is_deleted, false) = false
		GROUP BY p.proposal_id, p.proposal_name, p.base_currency_code, p.effective_date, p.upload_s3_key, a.processing_status, a.requested_at, a.checker_at
		ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`

	r, err := pool.Query(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// parentID = proposal_id to drill into a specific proposal's line items.
func queryCashProjectionDetail(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "i", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	proposalFilter := ""
	if parentID != "" {
		proposalFilter = fmt.Sprintf(" AND i.proposal_id = $%d", len(args)+1)
		args = append(args, parentID)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(i.item_id::text, '')         AS item_id,
			COALESCE(i.proposal_id::text, '')     AS proposal_id,
			COALESCE(i.entity_name, '')           AS entity_name,
			COALESCE(i.description, '')           AS description,
			COALESCE(i.cashflow_type, '')         AS cashflow_type,
			COALESCE(i.category_id::text, '')     AS category_id,
			COALESCE(i.currency_code, '')         AS currency_code,
			COALESCE(i.department_id::text, '')   AS department_id,
			COALESCE(i.counterparty_name, '')     AS counterparty_name,
			COALESCE(i.expected_amount, 0)        AS expected_amount,
			COALESCE(i.is_recurring, false)       AS is_recurring,
			COALESCE(i.recurrence_frequency, '')  AS recurrence_frequency,
			i.start_date,
			i.end_date,
			i.maturity_date,
			COALESCE(i.bank_name, '')             AS bank_name,
			COALESCE(i.bank_account_number, '')   AS bank_account_number
		FROM cimplrcorpsaas.cashflow_proposal_item i
		WHERE COALESCE(i.is_deleted, false) = false %s %s
		ORDER BY i.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef, proposalFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Balances, Limits & Availability ────────────────────────────────────────
func queryCashBankBalances(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	bf, bfArgs := bankNameFilter(ctx, "b", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "b", "as_of_date", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (balance_id)
				balance_id,
				processing_status,
				requested_at,
				checker_at
			FROM public.auditactionbankbalances
			ORDER BY balance_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(b.balance_id::text, '') AS balance_id,
			COALESCE(b.bank_name, '') AS bank_name,
			COALESCE(b.account_no, '') AS account_no,
			b.as_of_date,
			COALESCE(b.balance_amount, 0) AS balance_amount,
			COALESCE(b.closing_balance, 0) AS closing_balance,
			COALESCE(a.processing_status, '') AS processing_status
		FROM public.bank_balances_manual b
		LEFT JOIN latest_audit a ON a.balance_id = b.balance_id::text
		WHERE COALESCE(b.is_deleted, false) = false %s %s
		ORDER BY b.as_of_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashFundAvailability(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	// Return a dummy/empty set because this is a complex aggregate API (from GetFundAvailability) that aggregates bank statement actuals and cashflow projections across dates.
	// It doesn't have a single table we can just SELECT from. We will provide a minimal struct.
	return []map[string]any{}, nil
}

func queryCashBankLimits(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "l", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (limit_id)
				limit_id,
				processing_status,
				requested_at,
				checker_at
			FROM cimplrcorpsaas.auditactionbanklimit
			ORDER BY limit_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT 
			COALESCE(l.limit_id::text, '') AS limit_id, 
			COALESCE(l.entity_name, '') AS entity_name, 
			COALESCE(l.bank_name, '') AS bank_name, 
			COALESCE(l.core_limit_type, '') AS core_limit_type, 
			COALESCE(l.limit_type, '') AS limit_type, 
			COALESCE(l.limit_sub_type, '') AS limit_sub_type,
			l.sanction_date, 
			l.effective_date, 
			COALESCE(l.currency_code, '') AS currency_code, 
			COALESCE(l.sanctioned_amount, 0) AS sanctioned_amount,
			COALESCE(l.fungibility_type, '') AS fungibility_type, 
			COALESCE(l.fungibility_pct, 0) AS fungibility_pct, 
			COALESCE(l.security_type, '') AS security_type, 
			COALESCE(l.remarks, '') AS remarks, 
			COALESCE(l.initial_utilization, 0) AS initial_utilization,
			COALESCE(a.processing_status, '') AS processing_status
		FROM cimplrcorpsaas.bank_limit l
		LEFT JOIN latest_audit a ON a.limit_id = l.limit_id::text
		WHERE COALESCE(l.is_deleted, false) = false %s
		ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashUtilizations(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "l", "entity_name", 2) // via JOIN with bank_limit
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (utilization_id)
				utilization_id,
				processing_status,
				requested_at,
				checker_at
			FROM cimplrcorpsaas.auditactionbanklimitutilization
			ORDER BY utilization_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT 
			COALESCE(u.utilization_id::text, '') AS utilization_id,
			COALESCE(u.limit_id::text, '') AS limit_id,
			COALESCE(l.entity_name, '') AS entity_name,
			COALESCE(l.bank_name, '') AS bank_name,
			u.utilization_date,
			COALESCE(u.utilized_amount, 0) AS utilized_amount,
			COALESCE(u.remarks, '') AS remarks,
			COALESCE(a.processing_status, '') AS processing_status
		FROM cimplrcorpsaas.bank_limit_utilization u
		LEFT JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
		LEFT JOIN latest_audit a ON a.utilization_id = u.utilization_id::text
		WHERE COALESCE(u.is_deleted, false) = false %s
		ORDER BY GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
