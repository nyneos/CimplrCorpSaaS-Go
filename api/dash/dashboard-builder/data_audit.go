package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type auditTableConfig struct {
	Table, ParentCol, ParentID string
	PKCol, ActionCol, ExtraCols string
}

// auditQuery — for FD investment audit tables that use (audit_id, action_type).
func auditQuery(
	ctx context.Context,
	pool *pgxpool.Pool,
	limit int,
	table, parentCol, parentID string,
	extraCols string,
) ([]map[string]any, error) {
	return auditQueryInner(ctx, pool, limit, auditTableConfig{
		Table: table, ParentCol: parentCol, ParentID: parentID,
		PKCol: "audit_id", ActionCol: "action_type", ExtraCols: extraCols,
	})
}

// stdAuditQuery — for standard cimplr audit tables that use (action_id, actiontype).
func stdAuditQuery(
	ctx context.Context,
	pool *pgxpool.Pool,
	entityIDs []string,
	limit int,
	table, parentCol, parentID string,
) ([]map[string]any, error) {
	_ = entityIDs
	return auditQueryInner(ctx, pool, limit, auditTableConfig{
		Table: table, ParentCol: parentCol, ParentID: parentID,
		PKCol: "action_id", ActionCol: "actiontype",
	})
}

func auditQueryInner(
	ctx context.Context,
	pool *pgxpool.Pool,
	limit int,
	cfg auditTableConfig,
) ([]map[string]any, error) {
	args := []any{limit}

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
		LIMIT $1
	`, cfg.PKCol, cfg.ParentCol, cfg.ActionCol, extra, cfg.Table, parentFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── FD Booking Audit ──────────────────────────────────────────────────────────
func queryFDBookingAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit,
		"investment.fd_audit_booking_request", "booking_id", parentID, "")
}

// ── FD Confirmation Audit ─────────────────────────────────────────────────────
func queryFDConfirmationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit,
		"investment.fd_audit_confirmation", "confirmation_id", parentID, "")
}

// ── FD Activation (Master) Audit ──────────────────────────────────────────────
func queryFDActivationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit,
		"investment.fd_audit_master", "fd_id", parentID, "")
}

// ── FD Cashflow Audit ─────────────────────────────────────────────────────────
func queryFDCashflowAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit,
		"investment.fd_audit_cashflow_schedule", "cashflow_id", parentID, "")
}

// ── FD Closure Initiate Audit ─────────────────────────────────────────────────
func queryFDClosureInitiateAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit,
		"cimplr.fd_closure_initiate_audit", "closure_initiate_id", parentID, "")
}

// ── FD Closure Confirm Audit ──────────────────────────────────────────────────
func queryFDClosureConfirmAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit,
		"cimplr.fd_closure_confirm_audit", "closure_confirm_id", parentID, "")
}

// ── FD TDS Receipt Audit ──────────────────────────────────────────────────────
func queryFDTdsAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit,
		"investment.fd_tds_receipt_audit", "tds_id", parentID, "")
}

// ── FD Interest Receipt Audit ─────────────────────────────────────────────────
func queryFDReceiptAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit,
		"investment.fd_interest_receipt_audit", "receipt_id", parentID, "")
}

// ── FD Receipt Exception Audit ────────────────────────────────────────────────
func queryFDExceptionAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return auditQuery(ctx, pool, limit,
		"investment.fd_receipt_exception_audit", "exception_id", parentID, "")
}

// ── Bank Statement Audit ──────────────────────────────────────────────────────
func queryCashBankStatementAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"cimplrcorpsaas.auditactionbankstatement", "bankstatementid", parentID)
}

// ── Sweep Config Audit ────────────────────────────────────────────────────────
func queryCashSweepConfigAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"cimplrcorpsaas.auditactionsweepconfiguration", "sweep_id", parentID)
}

// ── Sweep Initiation Audit ────────────────────────────────────────────────────
func queryCashSweepInitiationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"cimplrcorpsaas.auditactionsweepinitiation", "initiation_id", parentID)
}

// ── Cashflow Projection Audit ─────────────────────────────────────────────────
func queryCashProjectionAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	args := []any{limit}
	parentFilter := ""
	if parentID != "" {
		parentFilter = fmt.Sprintf("AND proposal_id::text = $%d", len(args)+1)
		args = append(args, parentID)
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
		LIMIT $1
	`, parentFilter)
	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Fund Plan Audit ───────────────────────────────────────────────────────────
func queryCashFundPlanAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"public.auditaction_fund_plan_groups", "group_id", parentID)
}

// ── Investment Proposal Audit ─────────────────────────────────────────────────
func queryInvestmentProposalAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"investment.auditactionproposal", "proposal_id", parentID)
}

// ── Investment Initiation Audit ───────────────────────────────────────────────
func queryInvestmentInitiationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"investment.auditactioninitiation", "initiation_id", parentID)
}

// ── Investment Confirmation Audit ─────────────────────────────────────────────
func queryInvestmentConfirmationAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"investment.auditactioninvestmentconfirmation", "confirmation_id", parentID)
}

// ── Redemption Audit ──────────────────────────────────────────────────────────
func queryInvestmentRedemptionAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"investment.auditactionredemption", "redemption_id", parentID)
}

// ── Redemption Confirm Audit ──────────────────────────────────────────────────
func queryInvestmentRedemptionConfirmAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"investment.auditactionredemptionconfirmation", "redemption_confirm_id", parentID)
}

// ── FX Forward Booking Audit ──────────────────────────────────────────────────
func queryFXForwardBookingAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"public.auditactionforwardbooking", "system_transaction_id", parentID)
}

// ── FX Exposure Audit ─────────────────────────────────────────────────────────
func queryFXExposureAudit(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	return stdAuditQuery(ctx, pool, entityIDs, limit,
		"public.auditactionexposure", "exposure_header_id", parentID)
}

// ── Onboard Batch Info (summary with record counts per entity type) ────────────
func queryInvestmentOnboardBatchInfo(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, parentID string) ([]map[string]any, error) {
	args := []any{limit}
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
			(SELECT COUNT(*) FROM investment.masteramc                WHERE batch_id::text = ob.batch_id::text) AS amc_count,
			(SELECT COUNT(*) FROM investment.masterscheme              WHERE batch_id::text = ob.batch_id::text) AS scheme_count,
			(SELECT COUNT(*) FROM investment.masterdepositoryparticipant WHERE batch_id::text = ob.batch_id::text) AS dp_count,
			(SELECT COUNT(*) FROM investment.masterdemataccount        WHERE batch_id::text = ob.batch_id::text) AS demat_count,
			(SELECT COUNT(*) FROM investment.masterfolio               WHERE batch_id::text = ob.batch_id::text) AS folio_count
		FROM investment.onboard_batch ob
		%s
		ORDER BY ob.created_at DESC NULLS LAST
		LIMIT $1
	`, parentFilter)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
