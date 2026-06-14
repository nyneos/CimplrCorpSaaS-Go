package dashboardbuilder

import (
	"context"
	"fmt"


	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Exposure Headers & Line Items ──────────────────────────────────────────
func queryFXExposureHeadersLineItems(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "h", "entity", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (exposure_header_id)
				exposure_header_id,
				processing_status,
				requested_at,
				checker_at
			FROM public.auditactionexposure
			ORDER BY exposure_header_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(h.exposure_header_id::text, '') AS exposure_header_id,
			COALESCE(l.line_item_id::text, '') AS line_item_id,
			COALESCE(h.entity, '') AS entity_id,
			COALESCE(h.currency, '') AS currency,
			COALESCE(h.exposure_type, '') AS exposure_type,
			COALESCE(h.total_open_amount, 0) AS total_open_amount,
			COALESCE(h.total_original_amount, 0) AS total_original_amount,
			h.value_date,
			COALESCE(h.status, '') AS status,
			COALESCE(l.product_id, '') AS product_id,
			COALESCE(l.quantity, 0) AS quantity,
			COALESCE(l.line_item_amount, 0) AS line_item_amount,
			COALESCE(a.processing_status, '') AS processing_status
		FROM public.exposure_headers h
		LEFT JOIN public.exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
		LEFT JOIN latest_audit a ON a.exposure_header_id = h.exposure_header_id::text
		WHERE COALESCE(h.is_deleted, false) = false %s
		ORDER BY h.value_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Exposure Bucketing ─────────────────────────────────────────────────────
func queryFXExposureBucketing(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "h", "entity", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (exposure_header_id)
				exposure_header_id,
				processing_status,
				requested_at,
				checker_at
			FROM public.auditactionexposurebucketing
			ORDER BY exposure_header_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(b.exposure_header_id::text, '') AS exposure_header_id,
			COALESCE(h.entity, '') AS entity_id,
			COALESCE(b.month_1, 0) AS month_1,
			COALESCE(b.month_2, 0) AS month_2,
			COALESCE(b.month_3, 0) AS month_3,
			COALESCE(b.month_4, 0) AS month_4,
			COALESCE(b.month_4_6, 0) AS month_4_6,
			COALESCE(b.month_6plus, 0) AS month_6plus,
			COALESCE(b.status_bucketing, '') AS status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM public.exposure_bucketing b
		JOIN public.exposure_headers h ON b.exposure_header_id = h.exposure_header_id
		LEFT JOIN latest_audit a ON a.exposure_header_id = b.exposure_header_id::text
		WHERE COALESCE(h.is_deleted, false) = false %s
		ORDER BY b.exposure_header_id
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Hedging Proposals ──────────────────────────────────────────────────────
func queryFXHedgingProposals(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "h", "entity", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (exposure_header_id)
				exposure_header_id,
				processing_status,
				requested_at,
				checker_at
			FROM public.auditactionhedgeproposal
			ORDER BY exposure_header_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(hp.reference_no, '') AS reference_no,
			COALESCE(hp.exposure_header_id::text, '') AS exposure_header_id,
			COALESCE(h.entity, '') AS entity_id,
			COALESCE(hp.hedge_month1, 0) AS hedge_month1,
			COALESCE(hp.hedge_month2, 0) AS hedge_month2,
			COALESCE(hp.hedge_month3, 0) AS hedge_month3,
			COALESCE(hp.hedge_month4, 0) AS hedge_month4,
			COALESCE(hp.hedge_month4to6, 0) AS hedge_month4to6,
			COALESCE(hp.hedge_month6plus, 0) AS hedge_month6plus,
			COALESCE(hp.status_hedging, '') AS status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM public.hedging_proposal hp
		JOIN public.exposure_headers h ON hp.exposure_header_id = h.exposure_header_id
		LEFT JOIN latest_audit a ON a.exposure_header_id = hp.exposure_header_id::text
		WHERE COALESCE(h.is_deleted, false) = false %s
		ORDER BY hp.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Hedge Links Details ────────────────────────────────────────────────────
func queryFXHedgeLinksDetails(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "h", "entity", 2)
	args := append([]any{limit}, efArgs...)

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
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Forward MTM ────────────────────────────────────────────────────────────
func queryFXForwardMTM(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityNameFilter(ctx, "fm", "entity", 2)
	args := append([]any{limit}, efArgs...)

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
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Forward Bookings ───────────────────────────────────────────────────────
func queryFXForwardBookings(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	// For forward_bookings, entity_level_0 holds the primary entity/business unit
	ef, efArgs := entityNameFilter(ctx, "fb", "entity_level_0", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		WITH latest_audit AS (
			SELECT DISTINCT ON (system_transaction_id)
				system_transaction_id,
				processing_status,
				requested_at,
				checker_at
			FROM public.auditactionforwardbooking
			ORDER BY system_transaction_id, GREATEST(COALESCE(requested_at,'1970-01-01'::timestamp), COALESCE(checker_at,'1970-01-01'::timestamp)) DESC
		)
		SELECT
			COALESCE(fb.system_transaction_id::text, '') AS booking_id,
			COALESCE(fb.internal_reference_id, '') AS internal_reference_id,
			COALESCE(fb.entity_level_0, '') AS entity_id,
			COALESCE(fb.currency_pair, '') AS currency_pair,
			COALESCE(fb.order_type, '') AS order_type,
			COALESCE(fb.booking_amount, 0) AS booking_amount,
			COALESCE(fb.total_rate, 0) AS total_rate,
			fb.maturity_date,
			COALESCE(fb.status, '') AS status,
			COALESCE(fb.processing_status, '') AS parent_processing_status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM public.forward_bookings fb
		LEFT JOIN latest_audit a ON a.system_transaction_id = fb.system_transaction_id::text
		WHERE COALESCE(fb.is_deleted, false) = false %s
		ORDER BY fb.transaction_timestamp DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

func queryFXEntityRelevantForwardBookings(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	// This uses the exact same extraction query logic.
	return queryFXForwardBookings(ctx, pool, entityIDs, limit)
}
