package dashboardbuilder

import (
	"context"
	"fmt"

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
func auditQuery(ctx context.Context, pool *pgxpool.Pool, limit, offset int, parentID string, src auditSource) ([]map[string]any, error) {
	return auditQueryInner(ctx, pool, limit, offset, auditTableConfig{
		Table: src.Table, ParentCol: src.ParentCol, ParentID: parentID,
		PKCol: "audit_id", ActionCol: "action_type", ExtraCols: src.ExtraCols,
	})
}

// stdAuditQuery — for standard cimplr audit tables that use (action_id, actiontype).
func stdAuditQuery(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit, offset int, parentID string, src auditSource) ([]map[string]any, error) {
	_ = entityIDs
	return auditQueryInner(ctx, pool, limit, offset, auditTableConfig{
		Table: src.Table, ParentCol: src.ParentCol, ParentID: parentID,
		PKCol: "action_id", ActionCol: "actiontype",
	})
}

func auditQueryInner(
	ctx context.Context,
	pool *pgxpool.Pool,
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
	return auditQuery(ctx, pool, limit, offset, parentID,
		auditSource{Table: "investment.fd_audit_booking_request", ParentCol: "booking_id"})
}

// ── FD Confirmation Audit ─────────────────────────────────────────────────────
func queryFDConfirmationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit, offset, parentID,
		auditSource{Table: "investment.fd_audit_confirmation", ParentCol: "confirmation_id"})
}

// ── FD Activation (Master) Audit ──────────────────────────────────────────────
func queryFDActivationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit, offset, parentID,
		auditSource{Table: "investment.fd_audit_master", ParentCol: "fd_id"})
}

// ── FD Cashflow Audit ─────────────────────────────────────────────────────────
func queryFDCashflowAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit, offset, parentID,
		auditSource{Table: "investment.fd_audit_cashflow_schedule", ParentCol: "cashflow_id"})
}

// ── FD Closure Initiate Audit ─────────────────────────────────────────────────
func queryFDClosureInitiateAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit, offset, parentID,
		auditSource{Table: "cimplr.fd_closure_initiate_audit", ParentCol: "closure_initiate_id"})
}

// ── FD Closure Confirm Audit ──────────────────────────────────────────────────
func queryFDClosureConfirmAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit, offset, parentID,
		auditSource{Table: "cimplr.fd_closure_confirm_audit", ParentCol: "closure_confirm_id"})
}

// ── FD TDS Receipt Audit ──────────────────────────────────────────────────────
func queryFDTdsAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit, offset, parentID,
		auditSource{Table: "investment.fd_tds_receipt_audit", ParentCol: "tds_id"})
}

// ── FD Interest Receipt Audit ─────────────────────────────────────────────────
func queryFDReceiptAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit, offset, parentID,
		auditSource{Table: "investment.fd_interest_receipt_audit", ParentCol: "receipt_id"})
}

// ── FD Receipt Exception Audit ────────────────────────────────────────────────
func queryFDExceptionAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit, offset, parentID,
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
	_ = entityIDs
	args := []any{limit, offset}

	planFilter := ""
	if parentID != "" {
		planFilter = fmt.Sprintf("AND g.plan_id = $%d", len(args)+1)
		args = append(args, parentID)
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
