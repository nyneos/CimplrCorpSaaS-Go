package dashboardbuilder

import (
	"context"
	"fmt"
	"strings"
	"time"

	fundavailibilty "CimplrCorpSaas/api/cash/fundavailibilty"
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/validation"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Bank Statements ────────────────────────────────────────────────────────
func queryCashBankStatements(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, scopePairs []bankAccountScopePair) ([]map[string]any, error) {
	args := []any{limit}
	argIdx := 2
	extraFilters := ""

	ef, efArgs := entityFilter(entityIDs, "b", argIdx)
	if ef != "" {
		extraFilters += " " + ef
		args = append(args, efArgs...)
		argIdx += len(efArgs)
	}

	sf, sfArgs, _ := bankStatementScopeFilter("b", scopePairs, argIdx)
	extraFilters += sf
	args = append(args, sfArgs...)

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
	`, extraFilters)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// parentID = bank_statement_id to drill into a specific statement's transactions.
func queryCashBankStatementTransactions(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string, scopePairs []bankAccountScopePair) ([]map[string]any, error) {
	args := []any{limit}
	argIdx := 2
	extraFilters := ""

	ef, efArgs := entityFilter(entityIDs, "s", argIdx)
	if ef != "" {
		extraFilters += " " + ef
		args = append(args, efArgs...)
		argIdx += len(efArgs)
	}

	sf, sfArgs, nextIdx := bankStatementScopeFilter("s", scopePairs, argIdx)
	extraFilters += sf
	args = append(args, sfArgs...)
	argIdx = nextIdx

	stmtFilter := ""
	if parentID != "" {
		stmtFilter = fmt.Sprintf(" AND t.bank_statement_id = $%d", argIdx)
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
		LEFT JOIN public.masterbankaccount mba ON mba.account_number = s.account_number AND COALESCE(mba.is_deleted, false) = false
		WHERE COALESCE(s.is_deleted, false) = false %s %s
		ORDER BY t.value_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, extraFilters, stmtFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Payable / Receivable ───────────────────────────────────────────────────
func queryCashPayable(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "p", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(p.payable_id::text, '')      AS payable_id,
			COALESCE(p.entity_name, '')            AS entity_name,
			COALESCE(p.counterparty_name, '')      AS counterparty_name,
			COALESCE(p.invoice_number, '')         AS invoice_number,
			p.invoice_date,
			p.due_date,
			COALESCE(p.amount, 0)                  AS amount,
			COALESCE(p.currency_code, '')          AS currency_code
		FROM public.tr_payables p
		WHERE COALESCE(p.is_deleted, false) = false %s
		ORDER BY p.due_date ASC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	rows, err := scanRows(r)
	if err != nil {
		return nil, err
	}
	return filterPayRecScopeRows(ctx, rows), nil
}

func queryCashReceivable(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "r", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(r.receivable_id::text, '')    AS receivable_id,
			COALESCE(r.entity_name, '')            AS entity_name,
			COALESCE(r.counterparty_name, '')      AS counterparty_name,
			COALESCE(r.invoice_number, '')         AS invoice_number,
			r.invoice_date,
			r.due_date,
			COALESCE(r.invoice_amount, 0)          AS invoice_amount,
			COALESCE(r.currency_code, '')          AS currency_code
		FROM public.tr_receivables r
		WHERE COALESCE(r.is_deleted, false) = false %s
		ORDER BY r.due_date ASC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	rows, err := scanRows(r)
	if err != nil {
		return nil, err
	}
	return filterPayRecScopeRows(ctx, rows), nil
}

func filterPayRecScopeRows(ctx context.Context, rows []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		if validation.ValidateCashMasterReferences(ctx, map[string]interface{}{
			"entity_name":       dashboardStr(row["entity_name"]),
			"counterparty_name": dashboardStr(row["counterparty_name"]),
			"currency_code":     dashboardStr(row["currency_code"]),
		}) != "" {
			continue
		}
		out = append(out, row)
	}
	return out
}

// queryCashPayableReceivable is the combined legacy source (payables + receivables).
func queryCashPayableReceivable(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	payables, err := queryCashPayable(ctx, pool, entityIDs, 0)
	if err != nil {
		return nil, err
	}
	receivables, err := queryCashReceivable(ctx, pool, entityIDs, 0)
	if err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0, len(payables)+len(receivables))
	for _, row := range payables {
		out = append(out, map[string]any{
			"transaction_id":    row["payable_id"],
			"entity_id":         row["entity_name"],
			"entity_name":       row["entity_name"],
			"type":              "PAYABLE",
			"amount":            row["amount"],
			"due_date":          row["due_date"],
			"counterparty_name": row["counterparty_name"],
			"counterparty_id":   row["counterparty_name"],
			"currency_code":     row["currency_code"],
		})
	}
	for _, row := range receivables {
		out = append(out, map[string]any{
			"transaction_id":    row["receivable_id"],
			"entity_id":         row["entity_name"],
			"entity_name":       row["entity_name"],
			"type":              "RECEIVABLE",
			"amount":            row["invoice_amount"],
			"due_date":          row["due_date"],
			"counterparty_name": row["counterparty_name"],
			"counterparty_id":   row["counterparty_name"],
			"currency_code":     row["currency_code"],
		})
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
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
	rows, err := scanRows(r)
	if err != nil {
		return nil, err
	}
	return filterProjectionProposalRows(ctx, pool, rows)
}

func filterProjectionProposalRows(ctx context.Context, pool *pgxpool.Pool, rows []map[string]any) ([]map[string]any, error) {
	if len(rows) == 0 {
		return rows, nil
	}

	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if id := dashboardStr(row["proposal_id"]); id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return []map[string]any{}, nil
	}

	r, err := pool.Query(ctx, `
		SELECT
			COALESCE(i.proposal_id::text, '')     AS proposal_id,
			COALESCE(i.entity_name, '')           AS entity_name,
			COALESCE(i.category_id::text, '')     AS category_id,
			COALESCE(i.currency_code, '')         AS currency_code,
			COALESCE(i.bank_name, '')             AS bank_name,
			COALESCE(i.bank_account_number, '')   AS bank_account_number
		FROM cimplrcorpsaas.cashflow_proposal_item i
		WHERE COALESCE(i.is_deleted, false) = false
		  AND i.proposal_id::text = ANY($1)
	`, ids)
	if err != nil {
		return nil, err
	}
	itemRows, err := scanRows(r)
	if err != nil {
		return nil, err
	}

	scopedCounts := make(map[string]int, len(ids))
	for _, item := range itemRows {
		if validation.ValidateCashMasterReferences(ctx, map[string]interface{}{
			"entity_name":         dashboardStr(item["entity_name"]),
			"category_id":         dashboardStr(item["category_id"]),
			"currency_code":       dashboardStr(item["currency_code"]),
			"bank_name":           dashboardStr(item["bank_name"]),
			"bank_account_number": dashboardStr(item["bank_account_number"]),
		}) != "" {
			continue
		}
		proposalID := dashboardStr(item["proposal_id"])
		scopedCounts[proposalID]++
	}

	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		proposalID := dashboardStr(row["proposal_id"])
		if validation.ValidateCashMasterReferences(ctx, map[string]interface{}{
			"currency_code": dashboardStr(row["base_currency_code"]),
		}) != "" {
			continue
		}
		count := scopedCounts[proposalID]
		if count == 0 {
			continue
		}
		row["item_count"] = count
		out = append(out, row)
	}
	return out, nil
}

// proposalIDs filters line items to the selected cashflow proposals.
func queryCashProjectionDetail(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, proposalIDs []string) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "i", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	proposalFilter := ""
	proposalIDs = normalizeProposalIDs(proposalIDs)
	if len(proposalIDs) > 0 {
		proposalFilter = fmt.Sprintf(" AND i.proposal_id::text = ANY($%d)", len(args)+1)
		args = append(args, proposalIDs)
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
			COALESCE(i.expected_amount, 0)        AS expected_amount,
			COALESCE(i.is_recurring, false)       AS is_recurring,
			COALESCE(i.recurrence_frequency, '')  AS recurrence_frequency,
			i.maturity_date,
			COALESCE(i.bank_name, '')             AS bank_name,
			COALESCE(i.bank_account_number, '')   AS bank_account_number
		FROM cimplrcorpsaas.cashflow_proposal_item i
		JOIN cimplrcorpsaas.cashflow_proposal p ON p.proposal_id = i.proposal_id
		WHERE i.is_deleted IS NOT TRUE
		  AND p.is_deleted IS NOT TRUE %s %s
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
	`, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashFundAvailability(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	asOfDate := time.Now()
	if asOf, ok := ctx.Value(ctxKeyReqAsOfDate).(string); ok {
		if parsed, err := time.Parse(constants.DateFormat, strings.TrimSpace(asOf)); err == nil {
			asOfDate = parsed
		}
	}

	viewType := "daily"
	entityNames := api.GetEntityNamesFromCtx(ctx)
	bankNames := api.GetBankNamesFromCtx(ctx)
	if names, ok := ctx.Value(ctxKeyReqBankNamesNorm).([]string); ok && len(names) > 0 {
		bankNames = names
	}

	rows, err := fundavailibilty.CombinedFundAvailabilityRows(
		ctx, pool, asOfDate, viewType, entityIDs, entityNames, bankNames,
	)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
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
