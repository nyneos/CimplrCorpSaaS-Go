package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Maturity Summary ──────────────────────────────────────────────────────────
func queryFDMaturitySummary(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "m", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(m.fd_id::text, '') AS fd_id,
			COALESCE(m.entity_id, '') AS entity_id,
			COALESCE(m.entity_name, '') AS entity_name,
			COALESCE(m.bank_id, '') AS bank_id,
			COALESCE(m.bank_name, '') AS bank_name,
			COALESCE(m.bank_fd_ref_no, '') AS bank_fd_ref_no,
			COALESCE(NULLIF(m.tenure_type, ''),
				CASE
					WHEN COALESCE(m.tenure_years,  0) > 0 THEN 'YEARS'
					WHEN COALESCE(m.tenure_months, 0) > 0 THEN 'MONTHS'
					WHEN COALESCE(m.tenure_days,   0) > 0 THEN 'DAYS'
					ELSE ''
				END) AS tenure_type,
			COALESCE(m.principal_amount, 0) AS principal_amount,
			COALESCE(m.interest_rate, 0) AS interest_rate,
			COALESCE(m.interest_type_code, '') AS interest_type_code,
			m.start_date,
			m.maturity_date,
			COALESCE(m.fd_status, '') AS fd_status,
			COALESCE(m.auto_renewal, false) AS auto_renewal,
			COALESCE(m.tenure_days, 0) AS tenure_days,
			COALESCE(m.tenure_months, 0) AS tenure_months,
			COALESCE(m.tenure_years, 0) AS tenure_years
		FROM investment.fd_master m
		WHERE COALESCE(m.is_deleted, false) = false %s
		ORDER BY m.maturity_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── TDS Register ─────────────────────────────────────────────────────────────
func queryFDTDSRegister(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "t", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(t.tds_id::text, '') AS tds_id,
			COALESCE(t.fd_id::text, '') AS fd_id,
			COALESCE(t.entity_id, '') AS entity_id,
			COALESCE(m.entity_name, '') AS entity_name,
			COALESCE(m.bank_name, '') AS bank_name,
			t.deduction_date,
			COALESCE(t.gross_interest, 0) AS gross_interest,
			COALESCE(t.tds_deducted_actual, 0) AS tds_deducted_actual,
			COALESCE(t.tds_status, '') AS status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.fd_tds_receipt t
		LEFT JOIN investment.fd_master m ON m.fd_id::text = t.fd_id::text
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.fd_tds_receipt_audit 
			WHERE tds_id = t.tds_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(t.is_deleted, false) = false
		  AND t.ingestion_source = 'TDS_WORKBENCH' %s
		ORDER BY t.deduction_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Receipt All ──────────────────────────────────────────────────────────────
func queryFDReceiptAll(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	ef, efArgs := entityFilter(entityIDs, "r", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(r.receipt_id::text, '') AS receipt_id,
			COALESCE(r.fd_id::text, '') AS fd_id,
			COALESCE(r.entity_id, '') AS entity_id,
			COALESCE(r.entity_name, '') AS entity_name,
			COALESCE(r.bank_name, '') AS bank_name,
			r.receipt_date,
			r.period_start,
			r.period_end,
			COALESCE(r.gross_interest_received, 0) AS gross_interest_received,
			COALESCE(r.tds_amount_deducted, 0) AS tds_amount_deducted,
			COALESCE(r.other_charges, 0) AS other_charges,
			COALESCE(r.net_amount_received, 0) AS net_amount_received,
			COALESCE(r.bank_reference_no, '') AS bank_reference_no,
			COALESCE(r.narration, '') AS narration,
			COALESCE(r.is_active, true) AS is_active,
			COALESCE(r.receipt_status, '') AS receipt_status,
			COALESCE(a.processing_status, '') AS processing_status
		FROM investment.fd_interest_receipt r
		LEFT JOIN LATERAL (
			SELECT processing_status 
			FROM investment.fd_interest_receipt_audit 
			WHERE receipt_id = r.receipt_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(r.is_deleted, false) = false %s
		ORDER BY r.receipt_date DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Reconcile Results ────────────────────────────────────────────────────────
func queryFDReconcileResults(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	// Reconcile results for FD receipts
	// Uses investment.fd_receipt_reconcile_result
	ef, efArgs := entityFilter(entityIDs, "rc", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(rc.result_id::text,         '') AS reconcile_id,
			COALESCE(rc.reconcile_run_id::text,  '') AS run_id,
			COALESCE(rc.receipt_id::text,        '') AS receipt_id,
			COALESCE(rc.fd_id::text,             '') AS fd_id,
			COALESCE(rc.fd_ref_no,               '') AS fd_ref_no,
			COALESCE(rc.entity_id,               '') AS entity_id,
			COALESCE(m.entity_name,              '') AS entity_name,
			COALESCE(m.bank_name,                '') AS bank_name,
			COALESCE(rc.result_type,             '') AS result_type,
			COALESCE(rc.match_status,            '') AS status,
			COALESCE(rc.match_type,              '') AS match_type,
			COALESCE(rc.expected_amount,          0) AS expected_amount,
			COALESCE(rc.received_amount,          0) AS received_amount,
			COALESCE(rc.amount_variance,          0) AS amount_variance,
			COALESCE(rc.amount_variance_pct,      0) AS amount_variance_pct,
			rc.created_at
		FROM investment.fd_receipt_reconcile_result rc
		LEFT JOIN investment.fd_master m ON m.fd_id::text = rc.fd_id::text
		WHERE COALESCE(rc.is_deleted, false) = false %s
		ORDER BY rc.created_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}

// ── Exceptions ───────────────────────────────────────────────────────────────
func queryFDExceptions(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int) ([]map[string]any, error) {
	// fd_receipt_exception has no entity_id; filter via fd_master join
	ef, efArgs := entityFilter(entityIDs, "m", 2)
	args := append([]any{limit}, efArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(ex.exception_id::text, '') AS exception_id,
			COALESCE(ex.fd_id::text,         '') AS fd_id,
			COALESCE(ex.fd_ref_no,           '') AS fd_ref_no,
			COALESCE(m.entity_id,            '') AS entity_id,
			COALESCE(m.entity_name,          '') AS entity_name,
			COALESCE(m.bank_name,            '') AS bank_name,
			COALESCE(ex.exception_type,      '') AS exception_type,
			COALESCE(ex.exception_status,    '') AS status,
			COALESCE(ex.severity,            '') AS severity,
			COALESCE(ex.reason_code,         '') AS reason_code,
			COALESCE(ex.variance_outcome,    '') AS variance_outcome,
			COALESCE(ex.expected_amount,      0) AS expected_amount,
			COALESCE(ex.received_amount,      0) AS received_amount,
			COALESCE(ex.variance_amount,      0) AS variance_amount,
			ex.raised_at AS created_at,
			COALESCE(a.processing_status,    '') AS processing_status
		FROM investment.fd_receipt_exception ex
		LEFT JOIN investment.fd_master m ON m.fd_id::text = ex.fd_id::text
		LEFT JOIN LATERAL (
			SELECT processing_status
			FROM investment.fd_receipt_exception_audit
			WHERE exception_id = ex.exception_id::text
			ORDER BY GREATEST(requested_at, checker_at) DESC NULLS LAST
			LIMIT 1
		) a ON true
		WHERE COALESCE(ex.is_deleted, false) = false %s
		ORDER BY ex.raised_at DESC NULLS LAST
		LIMIT NULLIF($1, 0)
	`, ef)

	r, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanRows(r)
}
