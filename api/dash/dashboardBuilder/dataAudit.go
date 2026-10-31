package dashboardbuilder

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/schemacache"

	"github.com/jackc/pgx/v5/pgxpool"
)

type auditTableConfig struct {
	Table, ParentCol, ParentID  string
	PKCol, ActionCol, ExtraCols string
}

// auditSource identifies the table/column pair an audit helper reads from.
type auditSource struct {
	Table     string
	ParentCol string
	ExtraCols string
}

// auditQuery — for FD investment audit tables that use (audit_id, action_type).
func auditQuery(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit, offset int, parentID string, src auditSource) ([]map[string]any, error) {
	return auditQueryInner(ctx, pool, entityIDs, limit, offset, auditTableConfig{
		Table: src.Table, ParentCol: src.ParentCol, ParentID: parentID,
		PKCol: "audit_id", ActionCol: "action_type", ExtraCols: src.ExtraCols,
	})
}

// stdAuditQuery — for standard cimplr audit tables that use (action_id, actiontype).
func stdAuditQuery(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit, offset int, parentID string, src auditSource) ([]map[string]any, error) {
	return auditQueryInner(ctx, pool, entityIDs, limit, offset, auditTableConfig{
		Table: src.Table, ParentCol: src.ParentCol, ParentID: parentID,
		PKCol: "action_id", ActionCol: "actiontype",
	})
}

func auditQueryInner(
	ctx context.Context,
	pool *pgxpool.Pool,
	entityIDs []string,
	limit int,
	offset int,
	cfg auditTableConfig,
) ([]map[string]any, error) {
	args := []any{limit, offset}

	parentFilter := ""
	if cfg.ParentID != "" {
		parentFilter = fmt.Sprintf("AND %s::text = $%d", cfg.ParentCol, len(args)+1)
		args = append(args, cfg.ParentID)
	}

	extra := ""
	if cfg.ExtraCols != "" {
		extra = ", " + cfg.ExtraCols
	}

	scopeFilter, scopeArgs, allowed := auditScopeFilter(ctx, pool, cfg, entityIDs, len(args)+1)
	if !allowed {
		return []map[string]any{}, nil
	}
	parentFilter += scopeFilter
	args = append(args, scopeArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(%s::text,          '') AS audit_id,
			COALESCE(%s::text,          '') AS parent_id,
			COALESCE(%s,                '') AS action_type,
			COALESCE(processing_status, '') AS processing_status,
			COALESCE(reason,            '') AS reason,
			COALESCE(requested_by,      '') AS requested_by,
			requested_at,
			COALESCE(checker_by,        '') AS checker_by,
			checker_at,
			COALESCE(checker_comment,   '') AS checker_comment
			%s
		FROM %s
		WHERE 1=1 %s
		ORDER BY requested_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, cfg.PKCol, cfg.ParentCol, cfg.ActionCol, extra, cfg.Table, parentFilter)

	return runSourceQuery(ctx, pool, q, args)
}

// ── FD Booking Audit ──────────────────────────────────────────────────────────
func queryFDBookingAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.fd_audit_booking_request", ParentCol: "booking_id"})
}

// ── FD Confirmation Audit ─────────────────────────────────────────────────────
func queryFDConfirmationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.fd_audit_confirmation", ParentCol: "confirmation_id"})
}

// ── FD Activation (Master) Audit ──────────────────────────────────────────────
func queryFDActivationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.fd_audit_master", ParentCol: "fd_id"})
}

// ── FD Cashflow Audit ─────────────────────────────────────────────────────────
func queryFDCashflowAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.fd_audit_cashflow_schedule", ParentCol: "cashflow_id"})
}

// ── FD Closure Initiate Audit ─────────────────────────────────────────────────
func queryFDClosureInitiateAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "cimplr.fd_closure_initiate_audit", ParentCol: "closure_initiate_id"})
}

// ── FD Closure Confirm Audit ──────────────────────────────────────────────────
func queryFDClosureConfirmAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "cimplr.fd_closure_confirm_audit", ParentCol: "closure_confirm_id"})
}

// ── FD TDS Receipt Audit ──────────────────────────────────────────────────────
func queryFDTdsAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.fd_tds_receipt_audit", ParentCol: "tds_id"})
}

// ── FD Interest Receipt Audit ─────────────────────────────────────────────────
func queryFDReceiptAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.fd_interest_receipt_audit", ParentCol: "receipt_id"})
}

// ── FD Receipt Exception Audit ────────────────────────────────────────────────
func queryFDExceptionAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.fd_receipt_exception_audit", ParentCol: "exception_id"})
}

// ── Bank Statement Audit ──────────────────────────────────────────────────────
func queryCashBankStatementAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "cimplrcorpsaas.auditactionbankstatement", ParentCol: "bankstatementid"})
}

// ── Sweep Config Audit ────────────────────────────────────────────────────────
func queryCashSweepConfigAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "cimplrcorpsaas.auditactionsweepconfiguration", ParentCol: "sweep_id"})
}

// ── Sweep Initiation Audit ────────────────────────────────────────────────────
func queryCashSweepInitiationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "cimplrcorpsaas.auditactionsweepinitiation", ParentCol: "initiation_id"})
}

// ── Cashflow Projection Audit ─────────────────────────────────────────────────
func queryCashProjectionAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, proposalIDs []string) ([]map[string]any, error) {
	args := []any{limit, offset}
	parentFilter := ""
	proposalIDs = normalizeProposalIDs(proposalIDs)
	if len(proposalIDs) > 0 {
		parentFilter = fmt.Sprintf("AND proposal_id::text = ANY($%d)", len(args)+1)
		args = append(args, proposalIDs)
	}
	if !api.IsAdminOverride(ctx) {
		scopeNames := api.FilterEntitiesInScope(ctx, append(append([]string{}, api.GetEntityNamesFromCtx(ctx)...), api.GetEntityIDsFromCtx(ctx)...))
		if len(entityIDs) > 0 {
			scopeNames = api.FilterEntitiesInScope(ctx, entityIDs)
		}
		if len(scopeNames) == 0 {
			return []map[string]any{}, nil
		}
		parentFilter += fmt.Sprintf(` AND EXISTS (
			SELECT 1 FROM cimplrcorpsaas.cashflow_proposal_item i
			WHERE i.proposal_id = cimplrcorpsaas.audit_action_cashflow_proposal.proposal_id
			  AND COALESCE(i.is_deleted, false) = false
			  AND i.entity_name = ANY($%d::text[])
		)`, len(args)+1)
		args = append(args, scopeNames)
	}
	q := fmt.Sprintf(`
		SELECT
			COALESCE(action_id::text,   '') AS audit_id,
			COALESCE(proposal_id::text, '') AS parent_id,
			COALESCE(action_type,       '') AS action_type,
			COALESCE(processing_status, '') AS processing_status,
			COALESCE(reason,            '') AS reason,
			COALESCE(requested_by,      '') AS requested_by,
			requested_at,
			COALESCE(checker_by,        '') AS checker_by,
			checker_at,
			COALESCE(checker_comment,   '') AS checker_comment
		FROM cimplrcorpsaas.audit_action_cashflow_proposal
		WHERE 1=1 %s
		ORDER BY requested_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, parentFilter)
	return runSourceQuery(ctx, pool, q, args)
}

// ── Fund Plan Audit ───────────────────────────────────────────────────────────
// parentID is plan_id from /cash/fund-planning/summary (e.g. "plan-1783403924162").
// Audit rows are keyed by group_id, so we join fund_plan_groups to resolve plan_id.
func queryCashFundPlanAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	args := []any{limit, offset}

	planFilter := ""
	if parentID != "" {
		planFilter = fmt.Sprintf("AND g.plan_id = $%d", len(args)+1)
		args = append(args, parentID)
	}
	if !api.IsAdminOverride(ctx) {
		scopeNames := api.FilterEntitiesInScope(ctx, append(append([]string{}, api.GetEntityNamesFromCtx(ctx)...), api.GetEntityIDsFromCtx(ctx)...))
		if len(entityIDs) > 0 {
			scopeNames = api.FilterEntitiesInScope(ctx, entityIDs)
		}
		if len(scopeNames) == 0 {
			return []map[string]any{}, nil
		}
		planFilter += fmt.Sprintf(" AND g.entity_name = ANY($%d::text[])", len(args)+1)
		args = append(args, scopeNames)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(a.action_id::text, '') AS audit_id,
			COALESCE(g.plan_id, '')         AS parent_id,
			COALESCE(a.group_id::text, '')  AS group_id,
			COALESCE(a.actiontype, '')      AS action_type,
			COALESCE(a.processing_status, '') AS processing_status,
			COALESCE(a.reason, '')          AS reason,
			COALESCE(a.requested_by, '')    AS requested_by,
			a.requested_at,
			COALESCE(a.checker_by, '')      AS checker_by,
			a.checker_at,
			COALESCE(a.checker_comment, '') AS checker_comment
		FROM public.auditaction_fund_plan_groups a
		JOIN public.fund_plan_groups g ON g.group_id = a.group_id
		WHERE 1=1 %s
		ORDER BY a.requested_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, planFilter)

	return runSourceQuery(ctx, pool, q, args)
}

// ── Investment Proposal Audit ─────────────────────────────────────────────────
func queryInvestmentProposalAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.auditactionproposal", ParentCol: "proposal_id"})
}

// ── Investment Initiation Audit ───────────────────────────────────────────────
func queryInvestmentInitiationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.auditactioninitiation", ParentCol: "initiation_id"})
}

// ── Investment Confirmation Audit ─────────────────────────────────────────────
func queryInvestmentConfirmationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.auditactioninvestmentconfirmation", ParentCol: "confirmation_id"})
}

// ── Redemption Audit ──────────────────────────────────────────────────────────
func queryInvestmentRedemptionAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.auditactionredemption", ParentCol: "redemption_id"})
}

// ── Redemption Confirm Audit ──────────────────────────────────────────────────
func queryInvestmentRedemptionConfirmAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "investment.auditactionredemptionconfirmation", ParentCol: "redemption_confirm_id"})
}

// ── FX Forward Booking Audit ──────────────────────────────────────────────────
func queryFXForwardBookingAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "public.auditactionforwardbooking", ParentCol: "system_transaction_id"})
}

// ── FX Exposure Audit ─────────────────────────────────────────────────────────
func queryFXExposureAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit, offset, parentID,
		auditSource{Table: "public.auditactionexposure", ParentCol: "exposure_header_id"})
}

// ── Onboard Batch Info (summary with record counts per entity type) ────────────
func queryInvestmentOnboardBatchInfo(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	args := []any{limit, offset}
	parentFilter := ""
	if parentID != "" {
		parentFilter = fmt.Sprintf("WHERE ob.batch_id::text = $%d", len(args)+1)
		args = append(args, parentID)
	} else {
		parentFilter = "WHERE 1=1"
	}
	if !api.IsAdminOverride(ctx) {
		if len(entityIDs) > 0 && len(api.FilterEntitiesInScope(ctx, entityIDs)) == 0 {
			return []map[string]any{}, nil
		}
		scopeNames := api.FilterEntitiesInScope(ctx, append(append([]string{}, api.GetEntityNamesFromCtx(ctx)...), api.GetEntityIDsFromCtx(ctx)...))
		if len(scopeNames) == 0 {
			return []map[string]any{}, nil
		}
		parentFilter += fmt.Sprintf(` AND EXISTS (SELECT 1 FROM investment.onboard_transaction ot WHERE ot.batch_id::text = ob.batch_id::text AND ot.entity_name = ANY($%d::text[]))`, len(args)+1)
		args = append(args, scopeNames)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(ob.batch_id::text,    '') AS batch_id,
			COALESCE(ob.user_id::text,     '') AS user_id,
			COALESCE(ob.user_email,        '') AS user_email,
			COALESCE(ob.source,            '') AS source,
			COALESCE(ob.total_records,      0) AS total_records,
			COALESCE(ob.status,            '') AS status,
			COALESCE(ob.approval_status,   '') AS approval_status,
			COALESCE(ob.remarks,           '') AS remarks,
			ob.created_at,
			ob.completed_at,
			(SELECT COUNT(DISTINCT pom.amc_id)    FROM investment.portfolio_onboarding_map pom WHERE pom.batch_id::text = ob.batch_id::text AND COALESCE(pom.amc_id, '') <> '') AS amc_count,
			(SELECT COUNT(DISTINCT pom.scheme_id) FROM investment.portfolio_onboarding_map pom WHERE pom.batch_id::text = ob.batch_id::text AND COALESCE(pom.scheme_id, '') <> '') AS scheme_count,
			(SELECT COUNT(DISTINCT m.dp_id)      FROM investment.portfolio_onboarding_map pom JOIN investment.masterdepositoryparticipant m ON m.dp_name = pom.entity_name WHERE pom.batch_id::text = ob.batch_id::text AND pom.folio_id IS NULL AND pom.demat_id IS NULL AND pom.amc_id IS NULL AND pom.scheme_id IS NULL AND COALESCE(m.dp_id, '') <> '') AS dp_count,
			(SELECT COUNT(DISTINCT pom.demat_id)  FROM investment.portfolio_onboarding_map pom WHERE pom.batch_id::text = ob.batch_id::text AND COALESCE(pom.demat_id, '') <> '') AS demat_count,
			(SELECT COUNT(DISTINCT pom.folio_id)  FROM investment.portfolio_onboarding_map pom WHERE pom.batch_id::text = ob.batch_id::text AND COALESCE(pom.folio_id, '') <> '') AS folio_count
		FROM investment.onboard_batch ob
		%s
		ORDER BY ob.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, parentFilter)

	return runSourceQuery(ctx, pool, q, args)
}

type auditParent struct {
	table string
	col   string
}

var auditParents = map[string]auditParent{
	"cimplr.fd_closure_confirm_audit":              {"cimplr.fd_closure_confirm", "closure_confirm_id"},
	"cimplr.fd_closure_initiate_audit":             {"cimplr.fd_closure_initiate", "closure_initiate_id"},
	"cimplrcorpsaas.auditactionbankstatement":      {"cimplrcorpsaas.bank_statements", "bank_statement_id"},
	"investment.auditactioninitiation":             {"investment.investment_initiation", "initiation_id"},
	"investment.auditactioninvestmentconfirmation": {"investment.investment_confirmation", "confirmation_id"},
	"investment.auditactionproposal":               {"investment.investment_proposal", "proposal_id"},
	"investment.auditactionredemption":             {"investment.redemption_initiation", "redemption_id"},
	"investment.auditactionredemptionconfirmation": {"investment.redemption_confirmation", "redemption_confirm_id"},
	"investment.fd_audit_booking_request":          {"investment.fd_booking_request", "booking_id"},
	"investment.fd_audit_master":                   {"investment.fd_master", "fd_id"},
	"investment.fd_interest_receipt_audit":         {"investment.fd_interest_receipt", "receipt_id"},
	"investment.fd_tds_receipt_audit":              {"investment.fd_tds_receipt", "tds_id"},
	"public.auditactionexposure":                   {"public.exposure_headers", "exposure_header_id"},
	"public.auditactionforwardbooking":             {"public.forward_bookings", "system_transaction_id"},
	"cimplrcorpsaas.auditactionsweepconfiguration": {"cimplrcorpsaas.sweepconfiguration", "sweep_id"},
}

func auditScopeFilter(ctx context.Context, pool *pgxpool.Pool, cfg auditTableConfig, entityIDs []string, argN int) (string, []any, bool) {
	if api.IsAdminOverride(ctx) {
		return "", nil, true
	}
	ids := entityIDs
	if len(ids) == 0 {
		ids = api.GetEntityIDsFromCtx(ctx)
	}
	ids = api.FilterEntitiesInScope(ctx, ids)
	names := api.FilterEntitiesInScope(ctx, api.GetEntityNamesFromCtx(ctx))
	if len(ids) == 0 && len(names) == 0 {
		return "", nil, false
	}
	if clause, ok := indirectAuditScopeSQL(cfg.Table, argN); ok {
		return clause, []any{ids, names}, true
	}
	p, ok := auditParents[cfg.Table]
	if !ok {
		return "", nil, false
	}
	cols, err := parentColumns(ctx, pool, p.table)
	if err != nil || !cols[p.col] {
		return "", nil, false
	}
	entityCol := ""
	for _, c := range []string{"entity_id", "entity_name", "entity", "entity_level_0"} {
		if cols[c] {
			entityCol = c
			break
		}
	}
	if entityCol == "" {
		return "", nil, false
	}
	filter := fmt.Sprintf(` AND %s::text IN (SELECT %s::text FROM %s WHERE %s::text = ANY($%d::text[]) OR %s::text = ANY($%d::text[]))`,
		cfg.ParentCol, p.col, p.table, entityCol, argN, entityCol, argN+1)
	return filter, []any{ids, names}, true
}

func indirectAuditScopeSQL(table string, argN int) (string, bool) {
	id := argN
	name := argN + 1
	switch table {
	case "investment.fd_audit_confirmation":
		return fmt.Sprintf(` AND confirmation_id::text IN (
			SELECT c.confirmation_id::text
			FROM investment.fd_confirmation c
			JOIN investment.fd_booking_request b ON b.booking_id = c.booking_id
			WHERE b.entity_id::text = ANY($%d::text[])
			   OR COALESCE(b.entity_name, '') = ANY($%d::text[])
			   OR b.entity_id::text = ANY($%d::text[])
			   OR COALESCE(b.entity_name, '') = ANY($%d::text[])
		)`, id, id, name, name), true
	case "investment.fd_audit_cashflow_schedule":
		return fmt.Sprintf(` AND fd_id::text IN (
			SELECT fm.fd_id::text
			FROM investment.fd_master fm
			WHERE fm.entity_id::text = ANY($%d::text[])
			   OR COALESCE(fm.entity_name, '') = ANY($%d::text[])
			   OR fm.entity_id::text = ANY($%d::text[])
			   OR COALESCE(fm.entity_name, '') = ANY($%d::text[])
		)`, id, id, name, name), true
	case "investment.fd_receipt_exception_audit":
		return fmt.Sprintf(` AND exception_id::text IN (
			SELECT e.exception_id::text
			FROM investment.fd_receipt_exception e
			JOIN investment.fd_receipt_reconcile_run rr ON rr.reconcile_run_id = e.reconcile_run_id
			WHERE rr.entity_id::text = ANY($%d::text[])
			   OR rr.entity_id::text = ANY($%d::text[])
		)`, id, name), true
	case "cimplrcorpsaas.auditactionsweepinitiation":
		return fmt.Sprintf(` AND initiation_id::text IN (
			SELECT i.initiation_id::text
			FROM cimplrcorpsaas.sweep_initiation i
			JOIN cimplrcorpsaas.sweepconfiguration c ON c.sweep_id = i.sweep_id
			WHERE COALESCE(c.entity_name, '') = ANY($%d::text[])
			   OR COALESCE(c.entity_name, '') = ANY($%d::text[])
		)`, id, name), true
	default:
		return "", false
	}
}

func parentColumns(ctx context.Context, pool *pgxpool.Pool, table string) (map[string]bool, error) {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("unqualified table %s", table)
	}
	return schemacache.Columns("dashaudit:"+table, func() (map[string]bool, error) {
		rows, err := pool.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`, parts[0], parts[1])
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := map[string]bool{}
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				return nil, err
			}
			out[c] = true
		}
		return out, rows.Err()
	})
}
