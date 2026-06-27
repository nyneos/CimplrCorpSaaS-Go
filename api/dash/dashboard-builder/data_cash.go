package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Bank Statements ────────────────────────────────────────────────────────
// Mirrors /cash/bank-statements/v2/get (GetAllBankStatementsHandler).
func queryCashBankStatements(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	ef, efArgs := entityFilter(entityIDs, "s", len(args)+1)
	args = append(args, efArgs...)
	bf, bfArgs := bankIDFilter(ctx, "mba", len(args)+1)
	args = append(args, bfArgs...)
	df, dfArgs := dateRangeFilter(ctx, "s", "uploaded_at", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		WITH scoped_statements AS (
			SELECT
				s.bank_statement_id,
				s.entity_id,
				e.entity_name,
				s.account_number,
				s.statement_period_start,
				s.statement_period_end,
				s.opening_balance,
				s.closing_balance,
				s.uploaded_at,
				COALESCE(mb.bank_name, '') AS bank_name
			FROM cimplrcorpsaas.bank_statements s
			JOIN public.masterentitycash e ON s.entity_id = e.entity_id
			LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND COALESCE(mba.is_deleted, false) = false
			LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
			WHERE COALESCE(s.is_deleted, false) = false %s %s %s
		),
		prioritized_audit AS (
			SELECT a.*,
				ROW_NUMBER() OVER(PARTITION BY a.bankstatementid ORDER BY
					CASE WHEN a.actiontype = 'DELETE' AND a.processing_status = 'PENDING_DELETE_APPROVAL' THEN 1
					WHEN a.processing_status IN ('PENDING_APPROVAL', 'PENDING_EDIT_APPROVAL') AND a.actiontype IN ('CREATE', 'EDIT', 'RECAT') THEN 2
					WHEN a.actiontype IN ('CREATE', 'EDIT', 'RECAT', 'DELETE') THEN 3
					ELSE 4 END,
					a.requested_at DESC,
					a.action_id DESC
				) AS rn
			FROM cimplrcorpsaas.auditactionbankstatement a
			JOIN scoped_statements ss ON a.bankstatementid = ss.bank_statement_id
			WHERE COALESCE(a.actiontype, '') NOT IN ('UPLOAD_FILE', 'DOWNLOAD')
		),
		latest_audit AS (
			SELECT * FROM prioritized_audit WHERE rn = 1
		)
		SELECT
			COALESCE(ss.bank_statement_id::text, '') AS statement_id,
			COALESCE(ss.entity_id, '')               AS entity_id,
			COALESCE(ss.entity_name, '')             AS entity_name,
			COALESCE(ss.account_number, '')          AS account_number,
			COALESCE(ss.bank_name, '')               AS bank_name,
			ss.statement_period_start,
			ss.statement_period_end,
			ss.uploaded_at,
			COALESCE(ss.opening_balance, 0)          AS opening_balance,
			COALESCE(ss.closing_balance, 0)          AS closing_balance,
			COALESCE(la.processing_status, '')       AS processing_status
		FROM scoped_statements ss
		LEFT JOIN latest_audit la ON la.bankstatementid = ss.bank_statement_id
		ORDER BY GREATEST(COALESCE(la.requested_at, ss.uploaded_at), COALESCE(la.checker_at, ss.uploaded_at)) DESC
		LIMIT NULLIF($1, 0)
	`, ef, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// parentID = bank_statement_id to drill into a specific statement's transactions.
func queryCashBankStatementTransactions(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	args := []any{limit}
	ef, efArgs := entityFilter(entityIDs, "s", len(args)+1)
	args = append(args, efArgs...)

	stmtFilter := ""
	if parentID != "" {
		stmtFilter = fmt.Sprintf(" AND t.bank_statement_id::text = $%d", len(args)+1)
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
		WHERE COALESCE(s.is_deleted, false) = false %s %s
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
// Mirrors /cash/transactions/all (GetAllPayableReceivable) with entity scoping.
func queryCashPayableReceivable(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	_ = entityIDs
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "pr", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	q := fmt.Sprintf(`
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
			COALESCE(pr.entity_name,      '') AS entity_id,
			COALESCE(pr.entity_name,      '') AS entity_name,
			COALESCE(pr.type,             '') AS type,
			COALESCE(pr.amount,            0) AS amount,
			pr.due_date,
			COALESCE(pr.counterparty_name,'') AS counterparty_name,
			COALESCE(pr.counterparty_name,'') AS counterparty_id
		FROM pr
		WHERE 1=1 %s
		ORDER BY pr.due_date ASC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Fund Planning ──────────────────────────────────────────────────────────
// Mirrors /cash/fund-planning/summary (GetFundPlanSummary).
func queryCashFundPlanSummary(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	_ = entityIDs
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "fpg", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(fpg.plan_id::text, '') AS plan_id,
			COALESCE(fpg.entity_name, '')   AS entity_name,
			COALESCE(fpg.entity_name, '')   AS entity_id,
			COALESCE(fpg.horizon, 0)        AS horizon,
			COUNT(*)                        AS total_groups,
			COALESCE(SUM(fpg.total_amount), 0) AS total_amount,
			COALESCE(STRING_AGG(DISTINCT fpg.direction, ', '), '') AS direction,
			COALESCE(STRING_AGG(DISTINCT fpg.currency, ', '), '')   AS currency,
			COALESCE((
				SELECT processing_status FROM public.auditaction_fund_plan_groups
				WHERE group_id IN (
					SELECT group_id FROM public.fund_plan_groups fpg2 WHERE fpg2.plan_id = fpg.plan_id LIMIT 1
				)
				ORDER BY GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp),
				                  COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
				LIMIT 1
			), '') AS processing_status
		FROM public.fund_plan_groups fpg
		WHERE 1=1 %s
		GROUP BY fpg.plan_id, fpg.entity_name, fpg.horizon
		ORDER BY fpg.plan_id DESC
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// parentID = plan_id to drill into groups for a specific fund plan.
func queryCashFundPlanDetails(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	_ = entityIDs
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "fpg", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	planFilter := ""
	if parentID != "" {
		planFilter = fmt.Sprintf(" AND fpg.plan_id::text = $%d", len(args)+1)
		args = append(args, parentID)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(fpg.group_id::text, '')    AS detail_id,
			COALESCE(fpg.plan_id::text, '')     AS plan_id,
			COALESCE(fpg.entity_name, '')       AS entity_name,
			COALESCE(fpg.primary_key, '')       AS category,
			COALESCE(fpg.primary_value, '')     AS primary_value,
			COALESCE(fpg.direction, '')         AS direction,
			COALESCE(fpg.total_amount, 0)       AS amount,
			COALESCE(fpg.currency, '')          AS currency,
			COALESCE(fpg.total_amount, 0)       AS allocated_amount,
			COALESCE((
				SELECT processing_status FROM public.auditaction_fund_plan_groups
				WHERE group_id = fpg.group_id
				ORDER BY requested_at DESC, action_id DESC
				LIMIT 1
			), '') AS processing_status
		FROM public.fund_plan_groups fpg
		WHERE 1=1 %s %s
		ORDER BY fpg.group_id DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef, planFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Sweep Configuration & Execution ────────────────────────────────────────
// Mirrors /cash/sweep-config-v2/all (GetSweepConfigurationsV2).
func queryCashSweepConfig(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	_ = entityIDs
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "c", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(c.sweep_id::text, '') AS config_id,
			COALESCE(c.entity_name, '')   AS entity_id,
			COALESCE(c.entity_name, '')   AS entity_name,
			COALESCE(c.source_bank_name, '') AS bank_name,
			COALESCE(c.source_bank_name, '') AS source_bank_name,
			COALESCE(c.target_bank_name, '') AS target_bank_name,
			COALESCE(c.sweep_type, '')    AS sweep_type,
			COALESCE(c.frequency, '')     AS frequency,
			c.updated_at,
			COALESCE(a.processing_status, '') AS processing_status,
			CASE
				WHEN COALESCE(a.processing_status, '') = 'APPROVED' THEN 'ACTIVE'
				ELSE 'INACTIVE'
			END AS active_status
		FROM cimplrcorpsaas.sweepconfiguration c
		LEFT JOIN LATERAL (
			SELECT processing_status
			FROM cimplrcorpsaas.auditactionsweepconfiguration
			WHERE sweep_id = c.sweep_id::text
			  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY requested_at DESC, action_id DESC
			LIMIT 1
		) a ON TRUE
		WHERE COALESCE(c.is_deleted, false) = false %s
		ORDER BY GREATEST(COALESCE(c.created_at, '1970-01-01'::timestamp), COALESCE(c.updated_at, '1970-01-01'::timestamp)) DESC, c.sweep_id
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// Mirrors /cash/sweep-initiation/all (GetSweepInitiations).
func queryCashSweepInitiation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	_ = entityIDs
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "c", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(i.initiation_id::text, '') AS initiation_id,
			COALESCE(i.sweep_id::text,      '') AS config_id,
			COALESCE(c.entity_name,         '') AS entity_id,
			COALESCE(c.entity_name,         '') AS entity_name,
			COALESCE(c.source_bank_name,    '') AS source_bank_name,
			COALESCE(c.target_bank_name,    '') AS target_bank_name,
			COALESCE(c.sweep_type,          '') AS sweep_type,
			COALESCE(i.initiated_by,        '') AS initiated_by,
			i.initiation_time,
			COALESCE(i.overridden_amount,    0) AS overridden_amount,
			COALESCE(a.processing_status,   '') AS processing_status
		FROM cimplrcorpsaas.sweep_initiation i
		JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = i.sweep_id
		LEFT JOIN LATERAL (
			SELECT processing_status
			FROM cimplrcorpsaas.auditactionsweepinitiation
			WHERE initiation_id = i.initiation_id
			  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY requested_at DESC
			LIMIT 1
		) a ON TRUE
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

// Mirrors /cash/sweep-execution-v2/logs and /all-logs.
func queryCashSweepExecutionLogs(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	return queryCashSweepExecutionLogsFiltered(ctx, pool, entityIDs, limit, "")
}

func queryCashSweepAllExecutionLogs(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	return queryCashSweepExecutionLogsFiltered(ctx, pool, entityIDs, limit, "")
}

func queryCashSweepExecutionLogsFiltered(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	_ = entityIDs
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "c", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	parentFilter := ""
	if parentID != "" {
		parentFilter = fmt.Sprintf(" AND (l.sweep_id::text = $%d OR l.initiation_id::text = $%d)", len(args)+1, len(args)+1)
		args = append(args, parentID)
	}

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
		LEFT JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id::text = l.sweep_id::text
		WHERE 1=1 %s %s
		ORDER BY l.execution_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef, parentFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// Mirrors /cash/sweep-execution-v2/statistics (GetSweepStatisticsV2) per entity.
func queryCashSweepStatistics(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	_ = entityIDs
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "c", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(c.entity_name, '') AS stat_id,
			COALESCE(c.entity_name, '') AS entity_id,
			COALESCE(c.entity_name, '') AS entity_name,
			MAX(l.execution_date) AS updated_at,
			COUNT(*) AS total_sweeps,
			COUNT(CASE WHEN l.status = 'SUCCESS' THEN 1 END) AS success_sweeps,
			COUNT(CASE WHEN l.status = 'FAILED' THEN 1 END) AS failed_sweeps,
			COUNT(CASE WHEN l.status = 'INSUFFICIENT_FUNDS' THEN 1 END) AS insufficient_funds,
			COALESCE(SUM(CASE WHEN l.status = 'SUCCESS' THEN l.amount_swept ELSE 0 END), 0) AS total_amount_swept
		FROM cimplrcorpsaas.sweep_execution_log l
		JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id::text = l.sweep_id::text
		WHERE COALESCE(c.is_deleted, false) = false %s
		GROUP BY c.entity_name
		ORDER BY MAX(l.execution_date) DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Projections ────────────────────────────────────────────────────────────
// Mirrors /cash/projection/v2/list (ListProposalsV2).
func queryCashProjectionList(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	_ = entityIDs
	q := `
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
		LEFT JOIN LATERAL (
			SELECT processing_status
			FROM cimplrcorpsaas.audit_action_cashflow_proposal a2
			WHERE a2.proposal_id = p.proposal_id
			  AND a2.action_type IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY requested_at DESC, action_id DESC
			LIMIT 1
		) a ON TRUE
		WHERE COALESCE(p.is_deleted, false) = false
		GROUP BY p.proposal_id, p.proposal_name, p.base_currency_code, p.effective_date, p.upload_s3_key, a.processing_status
		ORDER BY COALESCE((
			SELECT GREATEST(COALESCE(requested_at, '1970-01-01'::timestamp), COALESCE(checker_at, '1970-01-01'::timestamp))
			FROM cimplrcorpsaas.audit_action_cashflow_proposal
			WHERE proposal_id = p.proposal_id AND action_type IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY requested_at DESC, action_id DESC
			LIMIT 1
		), '1970-01-01'::timestamp) DESC
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
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "i", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	proposalFilter := ""
	if parentID != "" {
		proposalFilter = fmt.Sprintf(" AND i.proposal_id::text = $%d", len(args)+1)
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
// Mirrors /cash/bank-balances/all (GetBankBalances).
func queryCashBankBalances(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	args := []any{limit}
	pos := len(args) + 1

	entityClause := ""
	if len(entityIDs) > 0 {
		entityClause = fmt.Sprintf("AND mba.entity_id = ANY($%d)", pos)
		args = append(args, entityIDs)
		pos++
	}

	bf, bfArgs := bankNameFilter(ctx, "b", pos)
	args = append(args, bfArgs...)
	if bf != "" {
		pos += len(bfArgs)
	}

	df, dfArgs := dateRangeFilter(ctx, "b", "as_of_date", pos)
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
			COALESCE(ec.entity_name, me.entity_name, '') AS entity_name,
			b.as_of_date,
			COALESCE(b.balance_amount, 0) AS balance_amount,
			COALESCE(b.closing_balance, 0) AS closing_balance,
			COALESCE(a.processing_status, '') AS processing_status
		FROM public.bank_balances_manual b
		JOIN (
			SELECT account_number, MIN(entity_id) AS entity_id
			FROM public.masterbankaccount
			GROUP BY account_number
		) mba ON b.account_no = mba.account_number
		LEFT JOIN public.masterentitycash ec ON mba.entity_id = ec.entity_id
		LEFT JOIN public.masterentity me ON me.entity_id::text = mba.entity_id
		LEFT JOIN latest_audit a ON a.balance_id = b.balance_id::text
		WHERE COALESCE(b.is_deleted, false) = false %s %s %s
		ORDER BY b.as_of_date DESC NULLS LAST, b.as_of_time DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, entityClause, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashFundAvailability(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	// Complex aggregate API — no flat table equivalent for dashboard builder yet.
	return []map[string]any{}, nil
}

// Mirrors /cash/limit/all (GetAllBankLimits).
func queryCashBankLimits(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	_ = entityIDs
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "l", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(l.limit_id::text, '') AS limit_id,
			COALESCE(l.entity_name, '') AS entity_id,
			COALESCE(l.entity_name, '') AS entity_name,
			COALESCE(l.bank_name, '') AS bank_name,
			COALESCE(l.bank_name, '') AS bank_id,
			COALESCE(l.core_limit_type, '') AS core_limit_type,
			COALESCE(l.limit_type, '') AS limit_type,
			COALESCE(l.limit_sub_type, '') AS limit_sub_type,
			l.sanction_date,
			l.effective_date,
			COALESCE(l.currency_code, '') AS currency_code,
			COALESCE(l.sanctioned_amount, 0) AS sanctioned_amount,
			COALESCE(l.sanctioned_amount, 0) AS limit_amount,
			COALESCE(l.fungibility_type, '') AS fungibility_type,
			COALESCE(l.fungibility_pct, 0) AS fungibility_pct,
			COALESCE(l.security_type, '') AS security_type,
			COALESCE(l.remarks, '') AS remarks,
			COALESCE(l.initial_utilization, 0) AS initial_utilization,
			COALESCE(a.processing_status, '') AS processing_status,
			GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) AS updated_at
		FROM cimplrcorpsaas.bank_limit l
		LEFT JOIN LATERAL (
			SELECT processing_status, requested_at, checker_at
			FROM cimplrcorpsaas.auditactionbanklimit
			WHERE limit_id = l.limit_id
			ORDER BY requested_at DESC
			LIMIT 1
		) a ON TRUE
		WHERE COALESCE(l.is_deleted, false) = false %s
		ORDER BY updated_at DESC
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// Mirrors /cash/limit/utilization/all (GetAllUtilizations).
func queryCashUtilizations(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	_ = entityIDs
	args := []any{limit}
	ef, efArgs := entityNameLowerFilter(ctx, "l", "entity_name", len(args)+1)
	args = append(args, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(u.utilization_id::text, '') AS utilization_id,
			COALESCE(u.limit_id::text, '') AS limit_id,
			COALESCE(l.entity_name, '') AS entity_id,
			COALESCE(l.entity_name, '') AS entity_name,
			COALESCE(l.bank_name, '') AS bank_name,
			u.utilization_date,
			COALESCE(u.utilized_amount, 0) AS utilized_amount,
			COALESCE(u.remarks, '') AS remarks,
			COALESCE(a.processing_status, '') AS processing_status,
			GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) AS updated_at
		FROM cimplrcorpsaas.bank_limit_utilization u
		LEFT JOIN cimplrcorpsaas.bank_limit l ON l.limit_id = u.limit_id
		LEFT JOIN LATERAL (
			SELECT processing_status, requested_at, checker_at
			FROM cimplrcorpsaas.auditactionbanklimitutilization
			WHERE utilization_id = u.utilization_id
			ORDER BY requested_at DESC
			LIMIT 1
		) a ON TRUE
		WHERE COALESCE(u.is_deleted, false) = false %s
		ORDER BY updated_at DESC
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
