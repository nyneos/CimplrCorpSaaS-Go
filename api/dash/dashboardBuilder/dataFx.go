package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Exposure Headers & Line Items ──────────────────────────────────────────
// Mirrors /fx/exposures/headers-line-items (GetExposureHeadersLineItems →
// queryHeadersLineItems / headersListSelectSQL): one row per approved header
// (no line-item JOIN), entity scope, pending approval_instance status overlay.
//
// Dashboard-only enrichments (still one row per header, safe to SUM):
//   hedged_amount / unhedged_amount from exposure_hedge_links
//   line_item_amount / line_item_count rolled up from exposure_line_items
func queryFXExposureHeadersLineItems(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	_ = entityIDs
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "h", "entity")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(h.exposure_header_id::text, '') AS exposure_header_id,
			COALESCE(h.document_id, '') AS document_id,
			COALESCE(h.exposure_type, '') AS exposure_type,
			COALESCE(h.entity, '') AS entity,
			COALESCE(h.company_code, '') AS company_code,
			COALESCE(h.counterparty_code, '') AS counterparty_code,
			COALESCE(h.counterparty_name, '') AS counterparty_name,
			COALESCE(h.exposure_category, '') AS exposure_category,
			COALESCE(h.currency, '') AS currency,
			h.document_date,
			COALESCE(i.status, h.approval_status, '') AS approval_status,
			COALESCE(h.total_original_amount, 0) AS total_original_amount,
			COALESCE(h.total_open_amount, 0) AS total_open_amount,
			COALESCE(h.amount_in_local_currency, 0) AS amount_in_local_currency,
			COALESCE(h.gl_account, '') AS gl_account,
			COALESCE(h.upload_s3_key, '') AS upload_s3_key,
			h.created_at,
			COALESCE(h.batch_id::text, '') AS batch_id,
			h.value_date,
			h.posting_date,
			COALESCE(h.status, '') AS status,
			COALESCE(h.entity1, '') AS entity1,
			COALESCE(h.entity2, '') AS entity2,
			COALESCE(h.entity3, '') AS entity3,
			COALESCE(h.counterparty_type, '') AS counterparty_type,
			COALESCE(h.reference, '') AS reference,
			COALESCE(h.text, '') AS text,
			COALESCE(h.exposure_creation_status, '') AS exposure_creation_status,
			COALESCE(h.exposure_creation_status, '') AS exposure_status,
			COALESCE(h.requested_by, '') AS requested_by,
			COALESCE(h.approved_by, '') AS approved_by,
			COALESCE(h.approved_at::text, '') AS approved_at,
			COALESCE(h.approval_comment, '') AS approval_comment,
			COALESCE(h.rejected_by, '') AS rejected_by,
			COALESCE(h.rejected_at::text, '') AS rejected_at,
			COALESCE(h.rejection_comment, '') AS rejection_comment,
			COALESCE(h.delete_comment, '') AS delete_comment,
			COALESCE((
				SELECT SUM(ehl.hedged_amount)
				FROM public.exposure_hedge_links ehl
				WHERE ehl.exposure_header_id = h.exposure_header_id
				  AND COALESCE(ehl.is_active, true) = true
			), 0) AS hedged_amount,
			GREATEST(
				COALESCE(h.total_open_amount, 0) - COALESCE((
					SELECT SUM(ehl.hedged_amount)
					FROM public.exposure_hedge_links ehl
					WHERE ehl.exposure_header_id = h.exposure_header_id
					  AND COALESCE(ehl.is_active, true) = true
				), 0),
				0
			) AS unhedged_amount,
			COALESCE((
				SELECT SUM(l.line_item_amount)
				FROM public.exposure_line_items l
				WHERE l.exposure_header_id = h.exposure_header_id
			), 0) AS line_item_amount,
			COALESCE((
				SELECT COUNT(*)::int
				FROM public.exposure_line_items l
				WHERE l.exposure_header_id = h.exposure_header_id
			), 0) AS line_item_count
		FROM public.exposure_headers h
		LEFT JOIN LATERAL (
			-- Same as FX headersListSelectSQL: instance status (PENDING/...),
			-- not eye ACTIVE (that made Status look like "Active" while pending).
			SELECT inst.status
			FROM uam.approval_instance inst
			WHERE inst.record_id = h.exposure_header_id::text
			  AND inst.module_code = 'FX'
			  AND inst.is_deleted = false
			  AND inst.status = 'PENDING'
			ORDER BY inst.submitted_at DESC NULLS LAST, inst.instance_id DESC
			LIMIT 1
		) i ON true
		WHERE h.exposure_creation_status = 'Approved'
		  AND h.is_deleted IS NOT TRUE %s
		ORDER BY h.created_at DESC NULLS LAST, h.exposure_header_id
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFXExposureBucketing(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "h", "entity")

	q := fmt.Sprintf(`
		SELECT
			COALESCE(h.exposure_header_id::text, '') AS exposure_header_id,
			COALESCE(h.document_id, '') AS document_id,
			COALESCE(h.exposure_type, '') AS exposure_type,
			COALESCE(h.entity, '') AS entity,
			COALESCE(h.counterparty_name, '') AS counterparty_name,
			COALESCE(h.counterparty_code, '') AS counterparty_code,
			COALESCE(h.company_code, '') AS company_code,
			COALESCE(h.gl_account, '') AS gl_account,
			COALESCE(l.line_item_id::text, '') AS line_item_id,
			COALESCE(h.currency, '') AS currency,
			h.document_date,
			COALESCE(b.status_bucketing, '') AS status_bucketing,
			COALESCE(b.status_bucketing, '') AS status,
			COALESCE(b.reference_no, '') AS reference_no,
			COALESCE(b.comments, '') AS comments,
			COALESCE(b.updated_by, '') AS updated_by,
			b.date AS "date",
			COALESCE(b.time::text, '') AS "time",
			b.updated_at,
			COALESCE(h.total_original_amount, 0) AS total_original_amount,
			COALESCE(h.total_open_amount, 0) AS total_open_amount,
			COALESCE(h.amount_in_local_currency, 0) AS amount_in_local_currency,
			COALESCE(b.advance, 0) AS advance,
			COALESCE(b.month_1, 0) AS month_1,
			COALESCE(b.month_2, 0) AS month_2,
			COALESCE(b.month_3, 0) AS month_3,
			COALESCE(b.month_4, 0) AS month_4,
			COALESCE(b.month_4_6, 0) AS month_4_6,
			COALESCE(b.month_6plus, 0) AS month_6plus,
			COALESCE(b.old_month1, 0) AS old_month1,
			COALESCE(b.old_month2, 0) AS old_month2,
			COALESCE(b.old_month3, 0) AS old_month3,
			COALESCE(b.old_month4, 0) AS old_month4,
			COALESCE(b.old_month4to6, 0) AS old_month4to6,
			COALESCE(b.old_month6plus, 0) AS old_month6plus
		FROM public.exposure_headers h
		JOIN public.exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
		LEFT JOIN public.exposure_bucketing b ON h.exposure_header_id = b.exposure_header_id
		WHERE (h.approval_status = 'approved' OR h.approval_status = 'Approved')
		  AND COALESCE(h.is_deleted, false) = false %s
		ORDER BY h.document_id, l.line_number NULLS FIRST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

// ── Hedging Proposal Documents (named proposals) ───────────────────────────
// Header rows from public.hedging_proposal_document (All Hedging Proposals list).
func queryFXHedgingProposalDocuments(ctx context.Context, pool *pgxpool.Pool, limit int, offset int) ([]map[string]any, error) {
	args := limitOffsetArgs(limit, offset)
	ef, efArgs := entityNameFilter(ctx, "l", "business_unit", len(args)+1)
	args = append(args, efArgs...)

	scope := ""
	if ef != "" {
		scope = fmt.Sprintf(`
		AND EXISTS (
			SELECT 1
			FROM public.hedging_proposal_document_line l
			WHERE l.proposal_id = d.proposal_id %s
		)`, ef)
	}

	q := fmt.Sprintf(`
		SELECT
			COALESCE(d.proposal_id::text, '') AS proposal_id,
			COALESCE(d.proposal_name, '') AS proposal_name,
			COALESCE(d.processing_status, '') AS processing_status,
			COALESCE(d.created_by, '') AS created_by,
			d.created_at,
			COALESCE(d.updated_by, '') AS updated_by,
			d.updated_at,
			COALESCE(d.comments, '') AS comments,
			COALESCE((
				SELECT COUNT(*)::int
				FROM public.hedging_proposal_document_line l
				WHERE l.proposal_id = d.proposal_id %s
			), 0) AS line_count
		FROM public.hedging_proposal_document d
		WHERE COALESCE(d.is_deleted, false) = false %s
		ORDER BY d.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, scope)

	return runSourceQuery(ctx, pool, q, args)
}

// Line items from public.hedging_proposal_document_line.
// proposalIDs filters to selected documents; empty = no rows (require scope like bank statements).
func queryFXHedgingProposalDocumentLines(ctx context.Context, pool *pgxpool.Pool, limit int, offset int, proposalIDs []string) ([]map[string]any, error) {
	proposalIDs = normalizeProposalIDs(proposalIDs)
	if len(proposalIDs) == 0 {
		return []map[string]any{}, nil
	}

	args := []any{limit, offset, proposalIDs}
	ef, efArgs := entityNameFilter(ctx, "l", "business_unit", len(args)+1)
	args = append(args, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(l.line_id::text, '') AS line_id,
			COALESCE(l.proposal_id::text, '') AS proposal_id,
			COALESCE(d.proposal_name, '') AS proposal_name,
			COALESCE(d.processing_status, '') AS processing_status,
			COALESCE(l.business_unit, '') AS business_unit,
			COALESCE(l.currency, '') AS currency,
			COALESCE(l.exposure_type, '') AS exposure_type,
			COALESCE(array_to_string(l.contributing_header_ids, ','), '') AS contributing_header_ids,
			COALESCE(l.hedge_month1, 0) AS hedge_month1,
			COALESCE(l.hedge_month2, 0) AS hedge_month2,
			COALESCE(l.hedge_month3, 0) AS hedge_month3,
			COALESCE(l.hedge_month4, 0) AS hedge_month4,
			COALESCE(l.hedge_month4to6, 0) AS hedge_month4to6,
			COALESCE(l.hedge_month6plus, 0) AS hedge_month6plus,
			COALESCE(l.old_hedge_month1, 0) AS old_hedge_month1,
			COALESCE(l.old_hedge_month2, 0) AS old_hedge_month2,
			COALESCE(l.old_hedge_month3, 0) AS old_hedge_month3,
			COALESCE(l.old_hedge_month4, 0) AS old_hedge_month4,
			COALESCE(l.old_hedge_month4to6, 0) AS old_hedge_month4to6,
			COALESCE(l.old_hedge_month6plus, 0) AS old_hedge_month6plus,
			COALESCE(l.line_status, '') AS status,
			COALESCE(l.comments, '') AS comments,
			l.created_at,
			l.updated_at
		FROM public.hedging_proposal_document_line l
		JOIN public.hedging_proposal_document d ON d.proposal_id = l.proposal_id
		WHERE COALESCE(d.is_deleted, false) = false
		  AND l.proposal_id::text = ANY($3) %s
		ORDER BY d.proposal_name, l.business_unit, l.currency, l.exposure_type, l.line_id
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)
	return runSourceQuery(ctx, pool, q, args)
}

// ── Hedge Links Details ────────────────────────────────────────────────────
func queryFXHedgeLinksDetails(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "h", "entity")

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (exposure_header_id, booking_id)
				exposure_header_id,
				booking_id,
				processing_status,
				requested_at,
				checker_at
			FROM public.auditactionhedgelink
			ORDER BY exposure_header_id, booking_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(ehl.link_id::text, '') AS link_id,
			COALESCE(ehl.exposure_header_id::text, '') AS exposure_header_id,
			COALESCE(ehl.booking_id::text, '') AS booking_id,
			COALESCE(h.entity, '') AS entity_id,
			COALESCE(ehl.hedged_amount, 0) AS hedged_amount,
			ehl.link_date,
			COALESCE(ehl.is_active, false) AS is_active,
			COALESCE(a.processing_status, '') AS processing_status
		FROM public.exposure_hedge_links ehl
		JOIN public.exposure_headers h ON ehl.exposure_header_id = h.exposure_header_id
		LEFT JOIN latest_audit a ON a.exposure_header_id = ehl.exposure_header_id::text AND a.booking_id = ehl.booking_id::text
		WHERE COALESCE(h.is_deleted, false) = false %s
		ORDER BY ehl.link_date DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

// ── MTM Management ─────────────────────────────────────────────────────────
func queryFXMtmManagement(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "fm", "entity")

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (mtm_id)
				mtm_id,
				processing_status,
				requested_at,
				checker_at
			FROM public.auditactionforwardmtm
			ORDER BY mtm_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(fm.mtm_id::text, '') AS mtm_id,
			COALESCE(fm.booking_id::text, '') AS booking_id,
			COALESCE(fm.entity, '') AS entity_id,
			COALESCE(fm.entity, '') AS entity,
			COALESCE(fm.internal_reference_id, '') AS internal_reference_id,
			-- Days REMAINING as of today, not the stored column: forward_mtm.days_to_maturity
			-- is frozen at upload time as (maturity_date - deal_date), i.e. the contract
			-- tenor, so a long-matured contract kept reporting its original tenor forever.
			-- Matured contracts clamp to 0. Matches how every other dashboard derives this
			-- (dash/cfo/fwdDashCfo.go, dash/investmentDashboards/fdTreasuryDashboard.go).
			COALESCE(GREATEST(fm.maturity_date::date - CURRENT_DATE, 0), 0)::int AS days_to_maturity,
			COALESCE(fm.days_to_maturity, 0) AS contract_tenor_days,
			fm.calculated_at,
			COALESCE(fm.upload_s3_key, '') AS upload_s3_key,
			fm.deal_date,
			fm.maturity_date,
			COALESCE(fm.currency_pair, '') AS currency_pair,
			COALESCE(fm.buy_sell, '') AS buy_sell,
			COALESCE(fm.notional_amount, 0) AS notional_amount,
			COALESCE(fm.contract_rate, 0) AS contract_rate,
			COALESCE(fm.mtm_rate, 0) AS mtm_rate,
			COALESCE(fm.mtm_value, 0) AS mtm_value,
			COALESCE(fm.status, '') AS status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM public.forward_mtm fm
		LEFT JOIN latest_audit a ON a.mtm_id = fm.mtm_id::text
		WHERE COALESCE(fm.is_deleted, false) = false %s
		ORDER BY fm.calculated_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

// ── Forward Bookings ───────────────────────────────────────────────────────
func queryFXForwardBookings(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	// For forward_bookings, entity_level_0 holds the primary entity/business unit
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "fb", "entity_level_0")

	q := fmt.Sprintf(`
		SELECT
			fb.actual_value_base_currency,
			fb.add_date,
			fb.additional_bank_details::text AS additional_bank_details,
			fb.bank_confirmation_date,
			fb.bank_margin,
			fb.bank_transaction_id,
			fb.base_currency,
			fb.booking_amount,
			fb.counterparty,
			fb.counterparty_dealer,
			fb.currency_pair,
			fb.delivery_date,
			fb.delivery_period,
			fb.entity_level_0,
			fb.entity_level_1,
			fb.entity_level_2,
			fb.entity_level_3,
			fb.forward_points,
			fb.internal_dealer,
			fb.internal_reference_id,
			fb.intervening_rate_quote_to_local,
			fb.local_currency,
			fb.maturity_date,
			fb.mode_of_delivery,
			fb.narration,
			fb.order_type,
			fb.processing_status,
			fb.quote_currency,
			fb.remarks,
			fb.settlement_date,
			fb.spot_rate,
			fb.status,
			fb.swift_unique_id,
			fb.system_transaction_id::text AS system_transaction_id,
			fb.system_transaction_id::text AS booking_id,
			fb.total_rate,
			fb.transaction_timestamp,
			fb.transaction_type,
			fb.value_local_currency,
			fb.value_quote_currency,
			fb.value_type
		FROM public.forward_bookings fb
		WHERE COALESCE(fb.is_deleted, false) = false %s
		ORDER BY fb.transaction_timestamp DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFXEntityRelevantForwardBookings(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	// This uses the exact same extraction query logic.
	return queryFXForwardBookings(ctx, pool, entityIDs, limit, offset)
}

func queryFXCancellation(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "fb", "entity_level_0")

	q := fmt.Sprintf(`
		SELECT
			'cancellation' AS request_type,
			COALESCE(fc.cancellation_id::text, '') AS cancellation_id,
			COALESCE(fc.booking_id::text, '') AS booking_id,
			COALESCE(fb.entity_level_0, '') AS business_unit,
			COALESCE(fc.amount_cancelled, 0) AS amount,
			COALESCE(fc.amount_cancelled, 0) AS amount_cancelled,
			fc.cancellation_date AS request_date,
			fc.cancellation_date,
			COALESCE(fc.status, '') AS status,
			COALESCE(fc.cancellation_rate, 0) AS cancellation_rate,
			COALESCE(fc.realized_gain_loss, 0) AS realized_gain_loss,
			COALESCE(fc.cancellation_reason, '') AS cancellation_reason,
			0::numeric AS rollover_cost,
			-- forward_cancellations carries no currency column; the pair lives on the booking.
			COALESCE(fb.currency_pair, '') AS fx_pair
		FROM public.forward_cancellations fc
		LEFT JOIN public.forward_bookings fb ON fc.booking_id = fb.system_transaction_id
		WHERE COALESCE(fc.is_deleted, false) = false %s
		ORDER BY fc.cancellation_date DESC NULLS LAST, fc.booking_id
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFXRollover(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "fb", "entity_level_0")

	q := fmt.Sprintf(`
		SELECT
			'rollover' AS request_type,
			COALESCE(fr.rollover_id::text, '') AS rollover_id,
			COALESCE(fr.booking_id::text, '') AS booking_id,
			COALESCE(fb.entity_level_0, '') AS business_unit,
			COALESCE(fr.amount_rolled_over, 0) AS amount,
			COALESCE(fr.amount_rolled_over, 0) AS amount_rolled_over,
			fr.rollover_date AS request_date,
			fr.rollover_date,
			fr.original_maturity_date,
			fr.new_maturity_date,
			COALESCE(fr.order_type, '') AS order_type,
			COALESCE(fr.new_forward_amount, 0) AS new_forward_amount,
			COALESCE(fr.new_forward_spot_rate, 0) AS new_forward_spot_rate,
			COALESCE(fr.new_forward_premium_discount, 0) AS new_forward_premium_discount,
			COALESCE(fr.new_forward_margin_rate, 0) AS new_forward_margin_rate,
			COALESCE(fr.new_forward_net_rate, 0) AS new_forward_net_rate,
			COALESCE(fr.status, '') AS status,
			0::numeric AS cancellation_rate,
			0::numeric AS realized_gain_loss,
			'' AS cancellation_reason,
			COALESCE(fr.rollover_cost, 0) AS rollover_cost,
			COALESCE(NULLIF(fr.fx_pair, ''), fb.currency_pair, '') AS fx_pair
		FROM public.forward_rollovers fr
		LEFT JOIN public.forward_bookings fb ON fr.booking_id = fb.system_transaction_id
		WHERE COALESCE(fr.is_deleted, false) = false %s
		ORDER BY fr.rollover_date DESC NULLS LAST, fr.booking_id
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

func queryFXCancellationRollover(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "fb", "entity_level_0")

	q := fmt.Sprintf(`
		SELECT * FROM (
			SELECT
				'cancellation' AS request_type,
				COALESCE(fc.booking_id::text, '') AS booking_id,
				COALESCE(fb.entity_level_0, '') AS business_unit,
				COALESCE(fb.entity_level_0, '') AS entity_level_0,
				COALESCE(fc.amount_cancelled, 0) AS amount,
				fc.cancellation_date AS request_date,
				COALESCE(fc.status, '') AS status,
				COALESCE(fc.cancellation_rate, 0) AS cancellation_rate,
				COALESCE(fc.realized_gain_loss, 0) AS realized_gain_loss,
				COALESCE(fc.cancellation_reason, '') AS cancellation_reason,
				0::numeric AS rollover_cost,
				-- forward_cancellations carries no currency column; the pair lives on the booking.
				COALESCE(fb.currency_pair, '') AS fx_pair
			FROM public.forward_cancellations fc
			LEFT JOIN public.forward_bookings fb ON fc.booking_id = fb.system_transaction_id
			WHERE COALESCE(fc.is_deleted, false) = false
			UNION ALL
			SELECT
				'rollover' AS request_type,
				COALESCE(fr.booking_id::text, '') AS booking_id,
				COALESCE(fb.entity_level_0, '') AS business_unit,
				COALESCE(fb.entity_level_0, '') AS entity_level_0,
				COALESCE(fr.amount_rolled_over, 0) AS amount,
				fr.rollover_date AS request_date,
				COALESCE(fr.status, '') AS status,
				0::numeric AS cancellation_rate,
				0::numeric AS realized_gain_loss,
				'' AS cancellation_reason,
				COALESCE(fr.rollover_cost, 0) AS rollover_cost,
				COALESCE(NULLIF(fr.fx_pair, ''), fb.currency_pair, '') AS fx_pair
			FROM public.forward_rollovers fr
			LEFT JOIN public.forward_bookings fb ON fr.booking_id = fb.system_transaction_id
			WHERE COALESCE(fr.is_deleted, false) = false
		) fb
		WHERE 1=1 %s
		ORDER BY request_date DESC NULLS LAST, booking_id
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}

// ── Settlement ─────────────────────────────────────────────────────────────
// Header rows from public.exposure_settlement_document (All Settlements list).
func queryFXSettlement(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityNameFilter(limitOffsetArgs(limit, offset), ctx, "esd", "entity")
	df, dfArgs := dateRangeFilter(ctx, "esd", "created_at", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(esd.settlement_id::text, '') AS settlement_id,
			COALESCE(esd.settlement_method, '') AS settlement_method,
			COALESCE(esd.entity, '') AS entity,
			COALESCE(esd.currency, '') AS currency,
			esd.settlement_date,
			COALESCE(esd.processing_status, '') AS processing_status,
			COALESCE(esd.new_exposure_header_id::text, '') AS new_exposure_header_id,
			COALESCE(esd.comments, '') AS comments,
			COALESCE(esd.created_by, '') AS created_by,
			esd.created_at,
			COALESCE(esd.updated_by, '') AS updated_by,
			esd.updated_at,
			COALESCE(esd.total_open_amount, 0) AS total_open_amount,
			COALESCE(esd.total_settled_amount, 0) AS total_settled_amount,
			COALESCE(esd.total_gain_loss, 0) AS total_gain_loss,
			COALESCE(exl.ids, '') AS exposure_header_id,
			COALESCE(exl.line_count, 0) AS line_count
		FROM public.exposure_settlement_document esd
		LEFT JOIN LATERAL (
			SELECT STRING_AGG(DISTINCT esl.exposure_header_id, ', ' ORDER BY esl.exposure_header_id) AS ids,
			       COUNT(*)::int AS line_count
			FROM public.exposure_settlement_line esl
			WHERE esl.settlement_id = esd.settlement_id
		) exl ON true
		WHERE COALESCE(esd.is_deleted, false) = false %s
		  %s
		ORDER BY esd.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, df)

	return runSourceQuery(ctx, pool, q, args)
}

// Line items from public.exposure_settlement_line.
// settlementIDs filters to selected documents; empty = no rows (require scope,
// same rule as hedging proposal lines).
func queryFXSettlementLineItems(ctx context.Context, pool *pgxpool.Pool, limit int, offset int, settlementIDs []string) ([]map[string]any, error) {
	settlementIDs = normalizeProposalIDs(settlementIDs)
	if len(settlementIDs) == 0 {
		return []map[string]any{}, nil
	}

	args := []any{limit, offset, settlementIDs}
	ef, efArgs := entityNameFilter(ctx, "esd", "entity", len(args)+1)
	args = append(args, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(esl.line_id::text, '') AS line_id,
			COALESCE(esl.settlement_id::text, '') AS settlement_id,
			COALESCE(esd.settlement_method, '') AS settlement_method,
			COALESCE(esd.entity, '') AS entity,
			COALESCE(esd.currency, '') AS currency,
			COALESCE(esl.exposure_header_id, '') AS exposure_header_id,
			COALESCE(esl.booking_id::text, '') AS booking_id,
			COALESCE(esl.forward_ref, '') AS forward_ref,
			COALESCE(esl.leg_type, '') AS leg_type,
			COALESCE(esl.bank_name, '') AS bank_name,
			COALESCE(esl.line_status, '') AS line_status,
			COALESCE(esl.comments, '') AS comments,
			esl.maturity_date,
			esl.created_at,
			COALESCE(esl.settlement_amount, 0) AS settlement_amount,
			COALESCE(esl.partial_amount, 0) AS partial_amount,
			COALESCE(esl.spot_rate, 0) AS spot_rate,
			COALESCE(esl.fwd_rate, 0) AS fwd_rate,
			COALESCE(esl.booked_rate, 0) AS booked_rate,
			COALESCE(esl.cancellation_rate, 0) AS cancellation_rate,
			COALESCE(esl.margin, 0) AS margin,
			COALESCE(esl.gain_loss, 0) AS gain_loss
		FROM public.exposure_settlement_line esl
		JOIN public.exposure_settlement_document esd ON esd.settlement_id = esl.settlement_id
		WHERE COALESCE(esd.is_deleted, false) = false
		  AND esl.settlement_id::text = ANY($3) %s
		ORDER BY esl.settlement_id, esl.line_id
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef)

	return runSourceQuery(ctx, pool, q, args)
}
