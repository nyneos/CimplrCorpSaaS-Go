package dashboardbuilder

import (
	"context"
	"fmt"
	"strings"
	"time"

	"CimplrCorpSaas/api"
	fundavailibilty "CimplrCorpSaas/api/cash/fundavailibilty"
	cashlimit "CimplrCorpSaas/api/cash/limit"
	payablerecievable "CimplrCorpSaas/api/cash/payablerecievable"
	cashprojection "CimplrCorpSaas/api/cash/projection"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/validation"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Bank Statements ────────────────────────────────────────────────────────
func queryCashBankStatements(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, scopePairs []bankAccountScopePair) ([]map[string]any, error) {
	args := []any{limit, offset}
	argIdx := 3
	extraFilters := ""

	ef, efArgs := entityFilter(entityIDs, "s", argIdx)
	if ef != "" {
		extraFilters += " " + ef
		args = append(args, efArgs...)
		argIdx += len(efArgs)
	}

	sf, sfArgs, _ := bankStatementScopeFilter("s", scopePairs, argIdx)
	extraFilters += sf
	args = append(args, sfArgs...)

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
				COALESCE(mb.bank_name, '') AS bank_name,
				mba.account_nickname AS account_nickname
			FROM cimplrcorpsaas.bank_statements s
			JOIN public.masterentitycash e ON s.entity_id = e.entity_id
			LEFT JOIN public.masterbankaccount mba
				ON mba.account_number = s.account_number AND COALESCE(mba.is_deleted, false) = false
			LEFT JOIN public.masterbank mb ON mb.bank_id = mba.bank_id
			WHERE COALESCE(s.is_deleted, false) = false %s
		),
		prioritized_audit AS (
			SELECT a.*,
				ROW_NUMBER() OVER(PARTITION BY a.bankstatementid ORDER BY
					CASE WHEN a.actiontype = '%s' AND a.processing_status = '%s' THEN 1
					WHEN a.processing_status IN ('PENDING_APPROVAL', '%s') AND a.actiontype IN ('CREATE', 'EDIT', 'RECAT') THEN 2
					WHEN a.actiontype IN ('CREATE', 'EDIT', 'RECAT', '%s') THEN 3
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
			COALESCE(ss.bank_statement_id::text, '') AS bank_statement_id,
			COALESCE(ss.entity_id, '')               AS entity_id,
			COALESCE(ss.entity_name, '')             AS entity_name,
			COALESCE(ss.account_number, '')          AS account_number,
			COALESCE(ss.bank_name, '')               AS bank_name,
			COALESCE(ss.account_nickname, '')        AS account_nickname,
			ss.statement_period_start,
			ss.statement_period_end,
			ss.uploaded_at,
			COALESCE(ss.opening_balance, 0)          AS opening_balance,
			COALESCE(ss.closing_balance, 0)          AS closing_balance,
			CASE
				WHEN la.actiontype = 'RECAT' AND la.processing_status = '%s' THEN 'APPROVED'
				ELSE COALESCE(la.processing_status, '')
			END AS processing_status
		FROM scoped_statements ss
		LEFT JOIN latest_audit la ON la.bankstatementid = ss.bank_statement_id
		ORDER BY GREATEST(COALESCE(la.requested_at, ss.uploaded_at), COALESCE(la.checker_at, ss.uploaded_at)) DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, extraFilters,
		constants.AuditActionDelete,
		constants.StatusPendingDeleteApproval,
		constants.StatusPendingEditApproval,
		constants.AuditActionDelete,
		constants.StatusPendingEditApproval,
	)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// parentID = bank_statement_id to drill into a specific statement's transactions.
func queryCashBankStatementTransactions(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string, scopePairs []bankAccountScopePair) ([]map[string]any, error) {
	args := []any{limit, offset}
	argIdx := 3
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
			COALESCE(mcc.category_name, '')         AS category_name,
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
		LEFT JOIN public.mastercashflowcategory mcc ON mcc.category_id = t.category_id
		WHERE COALESCE(s.is_deleted, false) = false %s %s
		ORDER BY t.value_date DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, extraFilters, stmtFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Payable / Receivable ───────────────────────────────────────────────────
func queryCashPayable(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "p", "entity_name")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(p.payable_id::text, '')         AS payable_id,
			COALESCE(e.entity_id::text, '')           AS entity_id,
			COALESCE(p.entity_name, '')               AS entity_name,
			COALESCE(c.counterparty_id::text, '')     AS counterparty_id,
			COALESCE(p.counterparty_name, '')         AS counterparty_name,
			COALESCE(p.invoice_number, '')            AS invoice_number,
			p.invoice_date,
			p.due_date,
			COALESCE(p.amount, 0)                     AS amount,
			COALESCE(p.currency_code, '')             AS currency_code
		FROM public.tr_payables p
		LEFT JOIN public.masterentitycash e
			ON LOWER(TRIM(e.entity_name)) = LOWER(TRIM(p.entity_name))
			AND COALESCE(e.is_deleted, false) = false
		LEFT JOIN public.mastercounterparty c
			ON LOWER(TRIM(c.counterparty_name)) = LOWER(TRIM(p.counterparty_name))
			AND COALESCE(c.is_deleted, false) = false
		WHERE COALESCE(p.is_deleted, false) = false %s
		ORDER BY p.due_date ASC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	rows, err := scanRows(r)
	if err != nil {
		return nil, err
	}
	rows = enrichPayRecDatasetRows(ctx, pool, rows)
	return filterPayRecScopeRows(ctx, rows), nil
}

func queryCashReceivable(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "r", "entity_name")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(r.receivable_id::text, '')       AS receivable_id,
			COALESCE(e.entity_id::text, '')           AS entity_id,
			COALESCE(r.entity_name, '')               AS entity_name,
			COALESCE(c.counterparty_id::text, '')     AS counterparty_id,
			COALESCE(r.counterparty_name, '')         AS counterparty_name,
			COALESCE(r.invoice_number, '')            AS invoice_number,
			r.invoice_date,
			r.due_date,
			COALESCE(r.invoice_amount, 0)             AS invoice_amount,
			COALESCE(r.currency_code, '')             AS currency_code
		FROM public.tr_receivables r
		LEFT JOIN public.masterentitycash e
			ON LOWER(TRIM(e.entity_name)) = LOWER(TRIM(r.entity_name))
			AND COALESCE(e.is_deleted, false) = false
		LEFT JOIN public.mastercounterparty c
			ON LOWER(TRIM(c.counterparty_name)) = LOWER(TRIM(r.counterparty_name))
			AND COALESCE(c.is_deleted, false) = false
		WHERE COALESCE(r.is_deleted, false) = false %s
		ORDER BY r.due_date ASC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	rows, err := scanRows(r)
	if err != nil {
		return nil, err
	}
	rows = enrichPayRecDatasetRows(ctx, pool, rows)
	return filterPayRecScopeRows(ctx, rows), nil
}

func enrichPayRecDatasetRows(ctx context.Context, pool *pgxpool.Pool, rows []map[string]any) []map[string]any {
	if len(rows) == 0 {
		return rows
	}
	idMaps := payablerecievable.LoadPayRecMasterIDMaps(ctx, pool)
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		entityName := dashboardStr(row["entity_name"])
		counterpartyName := dashboardStr(row["counterparty_name"])
		entityID, counterpartyID := payablerecievable.EnrichPayRecRowIDs(
			idMaps,
			entityName,
			counterpartyName,
			dashboardStr(row["entity_id"]),
			dashboardStr(row["counterparty_id"]),
		)
		next := make(map[string]any, len(row)+2)
		for k, v := range row {
			next[k] = v
		}
		next["entity_id"] = entityID
		next["counterparty_id"] = counterpartyID
		out[i] = next
	}
	return out
}

func filterPayRecScopeRows(ctx context.Context, rows []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		if validation.ValidateCashMasterReferences(ctx, map[string]interface{}{
			"entity_name":       dashboardStr(row["entity_name"]),
			"counterparty_name": dashboardStr(row["counterparty_name"]),
		}) != "" {
			continue
		}
		out = append(out, row)
	}
	return out
}

// queryCashPayableReceivable is the combined legacy source (payables + receivables).
func queryCashPayableReceivable(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	payables, err := queryCashPayable(ctx, pool, entityIDs, 0, 0)
	if err != nil {
		return nil, err
	}
	receivables, err := queryCashReceivable(ctx, pool, entityIDs, 0, 0)
	if err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0, len(payables)+len(receivables))
	for _, row := range payables {
		out = append(out, map[string]any{
			"transaction_id":    row["payable_id"],
			"entity_id":         row["entity_id"],
			"entity_name":       row["entity_name"],
			"type":              "PAYABLE",
			"amount":            row["amount"],
			"due_date":          row["due_date"],
			"counterparty_id":   row["counterparty_id"],
			"counterparty_name": row["counterparty_name"],
			"currency_code":     row["currency_code"],
		})
	}
	for _, row := range receivables {
		out = append(out, map[string]any{
			"transaction_id":    row["receivable_id"],
			"entity_id":         row["entity_id"],
			"entity_name":       row["entity_name"],
			"type":              "RECEIVABLE",
			"amount":            row["invoice_amount"],
			"due_date":          row["due_date"],
			"counterparty_id":   row["counterparty_id"],
			"counterparty_name": row["counterparty_name"],
			"currency_code":     row["currency_code"],
		})
	}
	if offset > 0 {
		if offset >= len(out) {
			out = nil
		} else {
			out = out[offset:]
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ── Fund Planning ──────────────────────────────────────────────────────────
// Group-grain rows so charts can use Currency / Direction / Horizon as X or stack fields.
// Plan-level aggregation collapses those dimensions (STRING_AGG / no direction), so stacking
// by them would be empty or incorrect.
func queryCashFundPlanSummary(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "fpg", "entity_name")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(fpg.plan_id, '') AS plan_id,
			COALESCE(fpg.group_id::text, '') AS group_id,
			COALESCE(fpg.entity_name, '') AS entity_name,
			CASE
				WHEN lower(COALESCE(fpg.direction, '')) = 'inflow' THEN 'Inflow'
				WHEN lower(COALESCE(fpg.direction, '')) = 'outflow' THEN 'Outflow'
				ELSE COALESCE(fpg.direction, '')
			END AS direction,
			COALESCE(fpg.currency, '') AS currency,
			COALESCE(fpg.horizon::text, '') AS horizon,
			COALESCE(fpg.primary_key, '') AS primary_types,
			COALESCE(fpg.primary_value, '') AS primary_values,
			COALESCE(fpg.total_amount, 0) AS total_amount,
			COALESCE(aa.actiontype, '') AS action_type,
			COALESCE(aa.processing_status, '') AS processing_status,
			COALESCE(aa.requested_by, '') AS requested_by,
			aa.requested_at,
			COALESCE(aa.requested_ip, '') AS requested_ip,
			COALESCE(aa.checker_by, '') AS checker_by,
			aa.checker_at,
			COALESCE(aa.checker_ip, '') AS checker_ip,
			COALESCE(aa.checker_comment, '') AS checker_comment,
			COALESCE(aa.reason, '') AS reason
		FROM public.fund_plan_groups fpg
		LEFT JOIN LATERAL (
			SELECT actiontype, processing_status, requested_by, requested_at, requested_ip,
				   checker_by, checker_at, checker_ip, checker_comment, reason
			FROM public.auditaction_fund_plan_groups aafpg
			WHERE aafpg.group_id = fpg.group_id
			ORDER BY requested_at DESC, action_id DESC
			LIMIT 1
		) aa ON TRUE
		WHERE 1=1 %s
		ORDER BY fpg.plan_id DESC, fpg.group_id DESC
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// parentID = plan_id from /cash/fund-planning/summary (e.g. "plan-1783403924162").
func queryCashFundPlanDetails(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "g", "entity_name")

	planFilter := ""
	if parentID != "" {
		planFilter = fmt.Sprintf(" AND g.plan_id = $%d", len(args)+1)
		args = append(args, parentID)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(d.line_id::text, '')    AS detail_id,
			COALESCE(g.plan_id, '')          AS plan_id,
			COALESCE(d.group_id::text, '')   AS group_id,
			COALESCE(g.entity_name, '')      AS entity_name,
			COALESCE(d.category, '')         AS category,
			COALESCE(d.amount, 0)            AS amount,
			COALESCE(d.currency, '')         AS currency,
			COALESCE(d.allocated_amount, 0)  AS allocated_amount
		FROM public.fund_plan_lines d
		LEFT JOIN public.fund_plan_groups g ON d.group_id = g.group_id
		WHERE 1=1 %s %s
		ORDER BY d.line_id DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, planFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Sweep Configuration & Execution ────────────────────────────────────────
func queryCashSweepConfig(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "c", "entity_name")

	// Mirrors /cash/sweep-config-v2/all:
	// - entity_name / frequency / source bank from sweepconfiguration (V2 has no active_status)
	// - processing_status from latest CREATE/EDIT/DELETE audit (covers PENDING_DELETE_APPROVAL)
	q := fmt.Sprintf(`
		SELECT
			COALESCE(c.sweep_id::text, '') AS config_id,
			COALESCE(c.entity_name, '') AS entity_name,
			COALESCE(c.source_bank_name, '') AS bank_name,
			COALESCE(c.sweep_type, '') AS sweep_type,
			COALESCE(c.frequency, '') AS frequency,
			c.updated_at,
			COALESCE(a.processing_status, '') AS processing_status
		FROM cimplrcorpsaas.sweepconfiguration c
		LEFT JOIN LATERAL (
			SELECT processing_status, requested_at, action_id
			FROM cimplrcorpsaas.auditactionsweepconfiguration
			WHERE sweep_id = c.sweep_id::text
			  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY requested_at DESC, action_id DESC
			LIMIT 1
		) a ON true
		WHERE COALESCE(c.is_deleted, false) = false %s
		ORDER BY c.updated_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashSweepInitiation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "c", "entity_name")

	// Mirrors /cash/sweep-initiation/with-details:
	// - latest CREATE/EDIT/DELETE audit by requested_at (so PENDING_* statuses surface)
	// - buffer/sweep amounts resolve to overridden_amount when present (same as UI)
	q := fmt.Sprintf(`
		SELECT
			COALESCE(i.initiation_id::text, '') AS initiation_id,
			COALESCE(i.sweep_id::text,      '') AS config_id,
			COALESCE(c.entity_name,         '') AS entity_name,
			COALESCE(c.source_bank_name,    '') AS source_bank_name,
			COALESCE(c.source_bank_account, '') AS source_bank_account,
			COALESCE(c.target_bank_name,    '') AS target_bank_name,
			COALESCE(c.target_bank_account, '') AS target_bank_account,
			COALESCE(c.sweep_type,          '') AS sweep_type,
			COALESCE(NULLIF(TRIM(i.initiated_by), ''), NULLIF(TRIM(a.requested_by), ''), '') AS initiated_by,
			i.initiation_time,
			COALESCE(a.processing_status,   '') AS processing_status,
			COALESCE(
				CASE WHEN i.overridden_amount IS NOT NULL THEN i.overridden_amount ELSE c.buffer_amount END,
				0
			) AS buffer_amount,
			COALESCE(
				CASE WHEN i.overridden_amount IS NOT NULL THEN i.overridden_amount ELSE c.sweep_amount END,
				0
			) AS sweep_amount,
			COALESCE(i.overridden_amount, 0) AS overridden_amount
		FROM cimplrcorpsaas.sweep_initiation i
		JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = i.sweep_id
		LEFT JOIN LATERAL (
			SELECT processing_status, requested_by, requested_at, action_id
			FROM cimplrcorpsaas.auditactionsweepinitiation
			WHERE initiation_id = i.initiation_id
			  AND actiontype IN ('CREATE', 'EDIT', 'DELETE')
			ORDER BY requested_at DESC, action_id DESC
			LIMIT 1
		) a ON true
		WHERE COALESCE(c.is_deleted, false) = false
		  AND COALESCE(i.is_deleted, false) = false %s
		ORDER BY i.initiation_time DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashSweepStatistics(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "c", "entity_name")

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
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Projections ────────────────────────────────────────────────────────────
func queryCashProjectionList(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	rows, err := cashprojection.QueryProposalListV2(ctx, pool, limit)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, len(rows))
	copy(out, rows)
	return out, nil
}

// proposalIDs filters line items to the selected cashflow proposals.
func queryCashProjectionDetail(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, proposalIDs []string) ([]map[string]any, error) {
	cols, err := projectionItemColumns(ctx, pool, []string{
		"department_id",
		"counterparty_name",
		"start_date",
		"end_date",
		"maturity_date",
		"bank_name",
		"bank_account_number",
		"currency_code",
		"recurrence_frequency",
	})
	if err != nil {
		return nil, err
	}

	textExpr := func(column string) string {
		if cols[column] {
			return fmt.Sprintf("COALESCE(i.%s, '')", column)
		}
		return "''"
	}
	dateExpr := func(column string) string {
		if cols[column] {
			return "i." + column
		}
		return "NULL::date"
	}
	currencyExpr := "NULLIF(TRIM(p.base_currency_code), '')"
	if cols["currency_code"] {
		currencyExpr = "COALESCE(NULLIF(TRIM(i.currency_code), ''), NULLIF(TRIM(p.base_currency_code), ''))"
	}

	args := []any{limit, offset}
	ef, efArgs := entityNameFilter(ctx, "i", "entity_name", len(args)+1)
	args = append(args, efArgs...)

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
			%s                                   AS department_id,
			%s                                   AS currency_code,
			%s                                   AS counterparty_name,
			COALESCE(i.expected_amount, 0)        AS expected_amount,
			COALESCE(i.is_recurring, false)       AS is_recurring,
			%s                                   AS recurrence_frequency,
			%s                                   AS start_date,
			%s                                   AS end_date,
			%s                                   AS maturity_date,
			%s                                   AS bank_name,
			%s                                   AS bank_account_number
		FROM cimplrcorpsaas.cashflow_proposal_item i
		JOIN cimplrcorpsaas.cashflow_proposal p ON p.proposal_id = i.proposal_id
		WHERE i.is_deleted IS NOT TRUE
		  AND p.is_deleted IS NOT TRUE %s %s
		ORDER BY i.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`,
		textExpr("department_id"),
		currencyExpr,
		textExpr("counterparty_name"),
		textExpr("recurrence_frequency"),
		dateExpr("start_date"),
		dateExpr("end_date"),
		dateExpr("maturity_date"),
		textExpr("bank_name"),
		textExpr("bank_account_number"),
		ef,
		proposalFilter,
	)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func projectionItemColumns(ctx context.Context, pool *pgxpool.Pool, names []string) (map[string]bool, error) {
	out := make(map[string]bool, len(names))
	rows, err := pool.Query(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'cimplrcorpsaas'
		  AND table_name = 'cashflow_proposal_item'
		  AND column_name = ANY($1::text[])
	`, names)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return out, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

// ── Balances, Limits & Availability ────────────────────────────────────────
func queryCashBankBalances(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args := []any{limit, offset}
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
		LIMIT NULLIF($1, 0) OFFSET $2
	`, bf, df)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryCashFundAvailability(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	asOfDate := time.Now()
	if asOf, ok := ctx.Value(ctxKeyReqAsOfDate).(string); ok {
		if parsed, err := time.Parse(constants.DateFormat, strings.TrimSpace(asOf)); err == nil {
			asOfDate = parsed
		}
	}

	viewType := ""
	if vt, ok := ctx.Value(ctxKeyReqViewType).(string); ok {
		viewType = strings.ToLower(strings.TrimSpace(vt))
	}
	if viewType == "" {
		return []map[string]any{}, nil
	}
	if viewType != "daily" && viewType != "weekly" && viewType != "monthly" && viewType != "quarterly" && viewType != "yearly" {
		return []map[string]any{}, nil
	}
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

func queryCashBankLimits(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "l", "entity_name")

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
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// Dashboard builder exposes only these utilization columns (see CI_DSAHBOARD dataSourceFields cashUtilizations).
var cashUtilizationDashboardFields = []string{
	"utilization_id",
	"currency_code",
	"entry_mode",
	"limit_action_type",
	"limit_available",
	"limit_bank_name",
	"limit_core_limit_type",
	"limit_currency_code",
	"limit_effective_date",
	"limit_entity_name",
	"limit_fungibility_pct",
	"limit_fungibility_type",
	"limit_initial_utilization",
	"limit_limit_sub_type",
	"limit_limit_type",
	"limit_processing_status",
	"limit_remarks",
	"limit_requested_at",
	"limit_requested_by",
	"limit_sanction_date",
	"limit_sanctioned_amount",
	"limit_security_type",
	"limit_utilization_pct",
	"processing_status",
	"reference_doc",
	"remarks",
	"utilization_date",
	"utilized_amount",
}

func projectUtilizationRow(row map[string]interface{}) map[string]any {
	out := make(map[string]any, len(cashUtilizationDashboardFields))
	for _, key := range cashUtilizationDashboardFields {
		if v, ok := row[key]; ok {
			out[key] = v
		}
	}
	return out
}

func queryCashUtilizations(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, rowLimit int, offset int) ([]map[string]any, error) {
	// Reuse the cash module query for scope validation and KPI columns; project to dashboard schema.
	fetchLimit := 0
	if rowLimit > 0 {
		fetchLimit = rowLimit + offset
	}
	rows, err := cashlimit.QueryAllUtilizations(ctx, pool, fetchLimit)
	if err != nil {
		return nil, err
	}

	if names, _ := ctx.Value("reqEntityNames").([]string); len(names) > 0 {
		rows = filterUtilizationRowsByEntityNames(rows, names)
		if fetchLimit > 0 && len(rows) > fetchLimit {
			rows = rows[:fetchLimit]
		}
	}

	if offset > 0 {
		if offset >= len(rows) {
			rows = nil
		} else {
			rows = rows[offset:]
		}
	}
	if rowLimit > 0 && len(rows) > rowLimit {
		rows = rows[:rowLimit]
	}

	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		out[i] = projectUtilizationRow(row)
	}
	return out, nil
}

func filterUtilizationRowsByEntityNames(rows []map[string]interface{}, names []string) []map[string]interface{} {
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		n := strings.ToLower(strings.TrimSpace(name))
		if n != "" {
			allowed[n] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return rows
	}

	filtered := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		entityName, _ := row["limit_entity_name"].(string)
		if _, ok := allowed[strings.ToLower(strings.TrimSpace(entityName))]; ok {
			filtered = append(filtered, row)
		}
	}
	return filtered
}
