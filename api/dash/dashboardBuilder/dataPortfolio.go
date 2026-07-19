package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func queryInvestmentOnboardBatch(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
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
		LIMIT NULLIF($1, 0) OFFSET $2
	`

	return runSourceQuery(ctx, pool, q, []any{limit, offset})
}

func queryInvestmentProposalMeta(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "p", "entity_name")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(p.proposal_id::text, '') AS proposal_id,
			COALESCE(p.batch_id::text, '') AS batch_id,
			COALESCE(p.entity_name, '') AS entity_name,
			COALESCE(p.proposal_name, '') AS proposal_name,
			COALESCE(p.total_amount, 0) AS total_amount,
			COALESCE(p.horizon_days, 0) AS horizon_days,
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
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

func queryInvestmentInitiationAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "i", "entity_name")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(i.initiation_id::text, '') AS initiation_id,
			COALESCE(i.proposal_id::text, '') AS proposal_id,
			COALESCE(i.entity_name, '') AS entity_name,
			COALESCE(s.scheme_id::text, i.scheme_id::text, '') AS scheme_id,
			COALESCE(s.scheme_name, i.scheme_id, '') AS scheme_name,
			COALESCE(s.amc_name, '') AS amc_name,
			COALESCE(f.folio_id::text, '') AS folio_id,
			COALESCE(f.folio_number, '') AS folio_number,
			COALESCE(d.demat_id::text, '') AS demat_id,
			COALESCE(d.demat_account_number, d.default_settlement_account, '') AS demat_number,
			COALESCE(i.amount, 0) AS amount,
			COALESCE(i.source, '') AS source,
			i.transaction_date,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.investment_initiation i
		LEFT JOIN investment.masterscheme s ON (
			COALESCE(s.is_deleted, false) = false AND NULLIF(TRIM(i.scheme_id), '') IS NOT NULL AND (
				s.scheme_id::text = TRIM(i.scheme_id)
				OR s.internal_scheme_code = TRIM(i.scheme_id)
				OR s.amfi_scheme_code = TRIM(i.scheme_id)
			)
		)
		LEFT JOIN investment.masterfolio f ON (f.folio_id::text = i.folio_id OR f.folio_number = i.folio_id)
		LEFT JOIN investment.masterdemataccount d ON (
			d.demat_id::text = i.demat_id
			OR d.default_settlement_account = i.demat_id
			OR d.demat_account_number = i.demat_id
		)
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.auditactioninitiation 
			WHERE initiation_id = i.initiation_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(i.is_deleted, false) = false %s
		ORDER BY i.transaction_date DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

func queryInvestmentConfirmationAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "i", "entity_name")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(c.confirmation_id::text, '') AS confirmation_id,
			COALESCE(c.initiation_id::text, '') AS initiation_id,

			-- initiation / scheme / folio / demat joins
			COALESCE(i.entity_name, '') AS initiation_entity_name,
			COALESCE(s.amc_name, '') AS initiation_amc_name,
			COALESCE(s.scheme_id::text, i.scheme_id::text, '') AS initiation_scheme_id,
			COALESCE(s.scheme_name, i.scheme_id, '') AS initiation_scheme_name,
			COALESCE(f.folio_id::text, '') AS initiation_folio_id,
			COALESCE(f.folio_number, '') AS initiation_folio_number,
			COALESCE(d.demat_id::text, '') AS initiation_demat_id,
			COALESCE(d.demat_account_number, '') AS initiation_demat_number,
			COALESCE(i.amount, 0) AS initiation_amount,
			i.transaction_date AS initiation_transaction_date,

			-- confirmation business fields
			COALESCE(c.status, '') AS status,
			COALESCE(a.processing_status, '') AS processing_status,
			COALESCE(c.confirmed_by, '') AS confirmed_by,
			COALESCE(c.resolution_comment, '') AS resolution_comment,
			COALESCE(c.resolution_variance, '') AS resolution_variance,
			COALESCE(c.is_deleted, false) AS is_deleted,
			c.nav_date,
			c.confirmed_at,
			c.updated_at,

			-- numeric fields
			(COALESCE(c.net_amount, 0) + COALESCE(c.stamp_duty, 0)) AS gross_amount,
			COALESCE(c.net_amount, 0) AS net_amount,
			COALESCE(c.allotted_units, 0) AS allotted_units,
			COALESCE(c.actual_allotted_units, 0) AS actual_allotted_units,
			COALESCE(c.nav, 0) AS nav,
			COALESCE(c.actual_nav, 0) AS actual_nav,
			COALESCE(c.stamp_duty, 0) AS stamp_duty,
			COALESCE(c.variance_nav, 0) AS variance_nav,
			COALESCE(c.variance_units, 0) AS variance_units
		FROM investment.investment_confirmation c
		LEFT JOIN investment.investment_initiation i ON c.initiation_id = i.initiation_id
		LEFT JOIN investment.masterscheme s ON (
			COALESCE(s.is_deleted, false) = false AND NULLIF(TRIM(i.scheme_id), '') IS NOT NULL AND (
				s.scheme_id::text = TRIM(i.scheme_id)
				OR s.internal_scheme_code = TRIM(i.scheme_id)
				OR s.amfi_scheme_code = TRIM(i.scheme_id)
			)
		)
		LEFT JOIN investment.masterfolio f ON (f.folio_id::text = i.folio_id OR f.folio_number = i.folio_id)
		LEFT JOIN investment.masterdemataccount d ON (
			d.demat_id::text = i.demat_id
			OR d.default_settlement_account = i.demat_id
			OR d.demat_account_number = i.demat_id
		)
		LEFT JOIN LATERAL (
			SELECT processing_status
			FROM investment.auditactioninvestmentconfirmation
			WHERE confirmation_id = c.confirmation_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(c.is_deleted, false) = false %s
		ORDER BY c.nav_date DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

func queryInvestmentRedemptionInitiateAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "r", "entity_name")

	q := fmt.Sprintf(`
		WITH resolved_folio AS (
			SELECT DISTINCT ON (ri.redemption_id)
				ri.redemption_id,
				f.folio_number,
				f.folio_id::text AS folio_id_text
			FROM investment.redemption_initiation ri
			LEFT JOIN investment.masterfolio f ON (
				(f.folio_id::text = ri.folio_id) OR 
				(ri.folio_id IS NOT NULL AND f.folio_number = ri.folio_id)
			)
			ORDER BY ri.redemption_id, f.folio_id
		),
		resolved_demat AS (
			SELECT DISTINCT ON (ri.redemption_id)
				ri.redemption_id,
				d.demat_account_number,
				d.demat_id::text AS demat_id_text
			FROM investment.redemption_initiation ri
			LEFT JOIN investment.masterdemataccount d ON (
				(d.demat_id::text = ri.demat_id) OR 
				(ri.demat_id IS NOT NULL AND d.default_settlement_account = ri.demat_id) OR 
				(ri.demat_id IS NOT NULL AND d.demat_account_number = ri.demat_id)
			)
			ORDER BY ri.redemption_id, d.demat_id
		)
		SELECT
			COALESCE(r.redemption_id::text, '') AS redemption_id,
			COALESCE(r.entity_name, '') AS entity_name,
			COALESCE(s.scheme_id::text, r.scheme_id::text, '') AS scheme_id,
			COALESCE(s.scheme_id::text, r.scheme_id::text, '') AS resolved_scheme_id,
			COALESCE(s.scheme_name, r.scheme_id, '') AS scheme_name,
			COALESCE(s.internal_scheme_code, '') AS scheme_code,
			COALESCE(s.amc_name, '') AS amc_name,
			COALESCE(s.isin, '') AS isin,
			COALESCE(r.folio_id::text, '') AS folio_id,
			COALESCE(rf.folio_id_text, '') AS folio_id_text,
			COALESCE(rf.folio_number, '') AS folio_number,
			COALESCE(r.demat_id::text, '') AS demat_id,
			COALESCE(rd.demat_id_text, '') AS demat_id_text,
			COALESCE(rd.demat_account_number, '') AS demat_number,
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
		LEFT JOIN investment.masterscheme s ON (
			s.scheme_id::text = r.scheme_id
			OR s.internal_scheme_code = r.scheme_id
		)
		LEFT JOIN resolved_folio rf ON rf.redemption_id = r.redemption_id
		LEFT JOIN resolved_demat rd ON rd.redemption_id = r.redemption_id
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.auditactionredemption 
			WHERE redemption_id = r.redemption_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(r.is_deleted, false) = false %s
		ORDER BY r.transaction_date DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

func queryInvestmentRedemptionConfirmAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "i", "entity_name")

	q := fmt.Sprintf(`
		WITH resolved_folio AS (
			SELECT DISTINCT ON (i.redemption_id)
				i.redemption_id,
				f.folio_number,
				f.folio_id::text AS folio_id_text
			FROM investment.redemption_initiation i
			LEFT JOIN investment.masterfolio f ON (
				(f.folio_id::text = i.folio_id) OR 
				(i.folio_id IS NOT NULL AND f.folio_number = i.folio_id)
			)
			ORDER BY i.redemption_id, f.folio_id
		),
		resolved_demat AS (
			SELECT DISTINCT ON (i.redemption_id)
				i.redemption_id,
				d.demat_account_number,
				d.demat_id::text AS demat_id_text
			FROM investment.redemption_initiation i
			LEFT JOIN investment.masterdemataccount d ON (
				(d.demat_id::text = i.demat_id) OR 
				(i.demat_id IS NOT NULL AND d.default_settlement_account = i.demat_id) OR 
				(i.demat_id IS NOT NULL AND d.demat_account_number = i.demat_id)
			)
			ORDER BY i.redemption_id, d.demat_id
		)
		SELECT
			COALESCE(c.redemption_confirm_id::text, '') AS redemption_confirm_id,
			COALESCE(c.redemption_confirm_id::text, '') AS confirmation_id,
			COALESCE(c.redemption_id::text, '') AS redemption_id,
			COALESCE(i.entity_name, '') AS initiation_entity_name,
			COALESCE(i.entity_name, '') AS entity_name,
			COALESCE(s.amc_name, '') AS initiation_amc_name,
			COALESCE(s.scheme_id::text, i.scheme_id::text, '') AS initiation_scheme_id,
			COALESCE(s.scheme_name, i.scheme_id, '') AS initiation_scheme_name,
			COALESCE(s.internal_scheme_code, '') AS initiation_scheme_code,
			COALESCE(s.isin, '') AS initiation_isin,
			COALESCE(rf.folio_id_text, '') AS initiation_folio_id,
			COALESCE(rf.folio_number, '') AS initiation_folio_number,
			COALESCE(rd.demat_id_text, '') AS initiation_demat_id,
			COALESCE(rd.demat_account_number, '') AS initiation_demat_number,
			COALESCE(c.status, '') AS status,
			COALESCE(a.processing_status, '') AS processing_status,
			COALESCE(c.confirmed_by, '') AS confirmed_by,
			COALESCE(c.resolution_variance, '') AS resolution_variance,
			TO_CHAR(i.requested_date, 'YYYY-MM-DD') AS initiation_requested_date,
			c.confirmed_at,

			COALESCE(i.by_amount, 0) AS initiation_by_amount,
			COALESCE(i.by_units, 0) AS initiation_by_units,
			COALESCE(c.actual_nav, 0) AS actual_nav,
			COALESCE(c.actual_units, 0) AS actual_units,
			COALESCE(c.gross_proceeds, 0) AS gross_proceeds,
			COALESCE(c.net_credited, 0) AS net_credited,
			COALESCE(c.net_credited, 0) AS confirmed_amount,
			COALESCE(c.exit_load, 0) AS exit_load,
			COALESCE(c.final_realised_capital_gain_loss, 0) AS final_realised_capital_gain_loss,
			COALESCE(c.stt_charges, 0) AS stt_charges,
			COALESCE(c.tds, 0) AS tds,
			COALESCE(c.variance_proceeds, 0) AS variance_proceeds
		FROM investment.redemption_confirmation c
		LEFT JOIN investment.redemption_initiation i ON c.redemption_id = i.redemption_id
		LEFT JOIN investment.masterscheme s ON (
			s.scheme_id::text = i.scheme_id
			OR s.internal_scheme_code = i.scheme_id
		)
		LEFT JOIN resolved_folio rf ON rf.redemption_id = i.redemption_id
		LEFT JOIN resolved_demat rd ON rd.redemption_id = i.redemption_id
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.auditactionredemptionconfirmation 
			WHERE redemption_confirm_id = c.redemption_confirm_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(c.is_deleted, false) = false %s
		ORDER BY c.confirmed_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

// ─── Accounting Workbench ─────────────────────────────────────────────────────
//
// These two sources are exposed by the frontend (DATA_SOURCE_SCHEMAS) but had no
// query registered here, so any widget using them failed with
// "unknown data source: <key>".

// queryInvestmentAccountingActivityAll — Financial Closing Workbench activities.
// investment.accounting_activity carries no entity column, so the entity filter band
// does not apply to it.
func queryInvestmentAccountingActivityAll(ctx context.Context, pool *pgxpool.Pool, limit int, offset int) ([]map[string]any, error) {
	args := limitOffsetArgs(limit, offset)

	q := `
		SELECT
			COALESCE(a.activity_id::text, '')     AS activity_id,
			COALESCE(a.activity_type, '')         AS activity_type,
			COALESCE(a.activity_subtype, '')      AS activity_subtype,
			COALESCE(a.accounting_period, '')     AS accounting_period,
			COALESCE(a.data_source, '')           AS data_source,
			COALESCE(a.status, '')                AS status,
			COALESCE(a.is_deleted, false)         AS is_deleted,
			a.effective_date,
			a.updated_at
		FROM investment.accounting_activity a
		WHERE COALESCE(a.is_deleted, false) = false
		ORDER BY a.updated_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`

	return runSourceQuery(ctx, pool, q, args)
}

// queryInvestmentJournalEntryAll — journal entry headers (line items excluded, so the
// row grain stays one row per entry).
func queryInvestmentJournalEntryAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "je")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(je.entry_id::text, '')       AS entry_id,
			COALESCE(je.activity_id::text, '')    AS activity_id,
			COALESCE(je.entity_id::text, '')      AS entity_id,
			COALESCE(je.entity_name, '')          AS entity_name,
			COALESCE(je.entry_type, '')           AS entry_type,
			COALESCE(je.description, '')          AS description,
			COALESCE(je.accounting_period, '')    AS accounting_period,
			COALESCE(je.status, '')               AS status,
			COALESCE(je.created_by, '')           AS created_by,
			COALESCE(je.folio_id::text, '')       AS folio_id,
			COALESCE(je.demat_id::text, '')       AS demat_id,
			je.entry_date,
			je.created_at,
			COALESCE(je.total_debit, 0)           AS total_debit,
			COALESCE(je.total_credit, 0)          AS total_credit
		FROM investment.accounting_journal_entry je
		WHERE COALESCE(je.is_deleted, false) = false %s
		ORDER BY je.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}
