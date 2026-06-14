package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func queryInvestmentOnboardBatch(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	// onboard_batch does not have entity_id
	q := `
		SELECT
			COALESCE(b.batch_id::text, '') AS batch_id,
			COALESCE(b.user_id, '') AS user_id,
			COALESCE(b.user_email, '') AS user_email,
			COALESCE(b.source, '') AS source,
			COALESCE(b.total_records, 0) AS total_records,
			COALESCE(b.status, '') AS status,
			COALESCE(b.approval_status, '') AS approval_status,
			COALESCE(b.remarks, '') AS remarks,
			b.created_at,
			b.completed_at
		FROM investment.onboard_batch b
		ORDER BY b.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`

	r, err := pool.Query(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryInvestmentProposalMeta(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "p", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(p.proposal_id::text, '') AS proposal_id,
			COALESCE(p.entity_name, '') AS entity_name,
			COALESCE(p.proposal_name, '') AS proposal_name,
			COALESCE(p.total_amount, 0) AS total_amount,
			COALESCE(p.source, '') AS source,
			p.updated_at,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.investment_proposal p
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.auditactionproposal 
			WHERE proposal_id = p.proposal_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(p.is_deleted, false) = false %s
		ORDER BY p.updated_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryInvestmentInitiationAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "i", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(i.initiation_id::text, '') AS initiation_id,
			COALESCE(i.proposal_id::text, '') AS proposal_id,
			COALESCE(i.entity_name, '') AS entity_name,
			COALESCE(i.scheme_id::text, '') AS scheme_id,
			COALESCE(i.folio_id::text, '') AS folio_id,
			COALESCE(i.demat_id::text, '') AS demat_id,
			COALESCE(i.amount, 0) AS amount,
			COALESCE(i.source, '') AS source,
			i.transaction_date,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.investment_initiation i
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.auditactioninitiation 
			WHERE initiation_id = i.initiation_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(i.is_deleted, false) = false %s
		ORDER BY i.transaction_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryInvestmentConfirmationAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "i", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(c.confirmation_id::text, '') AS confirmation_id,
			COALESCE(c.initiation_id::text, '') AS initiation_id,
			COALESCE(i.entity_name, '') AS entity_name,
			COALESCE(c.allotted_units, 0) AS allotted_units,
			COALESCE(c.net_amount, 0) AS confirmed_amount,
			c.nav_date,
			COALESCE(c.status, '') AS status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.investment_confirmation c
		LEFT JOIN investment.investment_initiation i ON c.initiation_id = i.initiation_id
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.auditactioninvestmentconfirmation 
			WHERE confirmation_id = c.confirmation_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(c.is_deleted, false) = false %s
		ORDER BY c.nav_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryInvestmentPortfolioGet(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "p", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(p.id::text, '') AS portfolio_id,
			COALESCE(p.batch_id::text, '') AS batch_id,
			COALESCE(p.entity_name, '') AS entity_name,
			COALESCE(p.folio_number, '') AS folio_number,
			COALESCE(p.demat_acc_number, '') AS demat_acc_number,
			COALESCE(p.folio_id::text, '') AS folio_id,
			COALESCE(p.demat_id::text, '') AS demat_id,
			COALESCE(p.scheme_id::text, '') AS scheme_id,
			COALESCE(p.scheme_name, '') AS scheme_name,
			COALESCE(p.isin, '') AS isin,
			COALESCE(p.total_units, 0) AS total_units,
			COALESCE(p.avg_nav, 0) AS avg_nav,
			COALESCE(p.current_nav, 0) AS current_nav,
			COALESCE(p.current_value, 0) AS current_value,
			COALESCE(p.total_invested_amount, 0) AS total_invested_amount,
			COALESCE(p.gain_loss, 0) AS gain_loss,
			COALESCE(p.gain_losss_percent, 0) AS gain_loss_percent,
			p.created_at
		FROM investment.portfolio_snapshot p
		WHERE 1=1 %s
		ORDER BY p.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryInvestmentRedemptionInitiateAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "r", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(r.redemption_id::text, '') AS redemption_id,
			COALESCE(r.entity_name, '') AS entity_name,
			COALESCE(r.scheme_id::text, '') AS scheme_id,
			COALESCE(r.folio_id::text, '') AS folio_id,
			COALESCE(r.demat_id::text, '') AS demat_id,
			COALESCE(r.requested_by, '') AS requested_by,
			r.requested_date,
			r.transaction_date,
			COALESCE(r.by_amount, 0) AS by_amount,
			COALESCE(r.by_units, 0) AS by_units,
			COALESCE(r.method, '') AS method,
			COALESCE(r.estimated_proceeds, 0) AS estimated_proceeds,
			COALESCE(r.gain_loss, 0) AS gain_loss,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.redemption_initiation r
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.auditactionredemption 
			WHERE redemption_id = r.redemption_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(r.is_deleted, false) = false %s
		ORDER BY r.transaction_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryInvestmentRedemptionConfirmAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "i", "entity_name", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(c.redemption_confirm_id::text, '') AS confirmation_id,
			COALESCE(c.redemption_id::text, '') AS redemption_id,
			COALESCE(i.entity_name, '') AS entity_name,
			c.confirmed_at,
			COALESCE(c.actual_nav, 0) AS actual_nav,
			COALESCE(c.actual_units, 0) AS actual_units,
			COALESCE(c.gross_proceeds, 0) AS gross_proceeds,
			COALESCE(c.net_credited, 0) AS confirmed_amount,
			COALESCE(c.status, '') AS status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.redemption_confirmation c
		LEFT JOIN investment.redemption_initiation i ON c.redemption_id = i.redemption_id
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.auditactionredemptionconfirmation 
			WHERE redemption_confirm_id = c.redemption_confirm_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(c.is_deleted, false) = false %s
		ORDER BY c.confirmed_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
