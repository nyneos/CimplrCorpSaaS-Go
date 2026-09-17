package dashboardbuilder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func queryFDClosingEvidencePack(ctx context.Context, pool *pgxpool.Pool, entityIDs []string, limit int, offset int) ([]map[string]any, error) {
	args, ef := withEntityFilter(limitOffsetArgs(limit, offset), entityIDs, "c")
	df, dfArgs := dateRangeFilter(ctx, "p", "generated_at", len(args)+1)
	args = append(args, dfArgs...)

	q := fmt.Sprintf(`
		SELECT
			COALESCE(p.pack_id::text, '') AS pack_id,
			COALESCE(p.cycle_id::text, '') AS cycle_id,
			COALESCE(c.close_type, '') AS close_type,
			COALESCE(c.entity_id, '') AS entity_id,
			COALESCE(c.entity_name, '') AS entity_name,
			COALESCE(c.bank_id, '') AS bank_id,
			COALESCE(c.bank_name, '') AS bank_name,
			COALESCE(c.currency_code, '') AS currency_code,
			COALESCE(c.financial_period, '') AS financial_period,
			c.period_start,
			c.period_end,
			COALESCE(c.status, '') AS cycle_status,
			COALESCE(p.format, '') AS format,
			COALESCE(p.include_accrual_ledger, false) AS include_accrual_ledger,
			COALESCE(p.include_reconciliation_report, false) AS include_reconciliation_report,
			COALESCE(p.include_exceptions_register, false) AS include_exceptions_register,
			COALESCE(p.include_posting_summary, false) AS include_posting_summary,
			COALESCE(p.include_approval_logs, false) AS include_approval_logs,
			COALESCE(p.include_period_lock_certificate, false) AS include_period_lock_certificate,
			COALESCE(p.include_audit_trail, false) AS include_audit_trail,
			COALESCE(p.include_supporting_documents, false) AS include_supporting_documents,
			COALESCE(p.s3_key, '') AS s3_key,
			COALESCE(p.file_size, 0) AS file_size,
			COALESCE(p.report_count, 0) AS report_count,
			COALESCE(p.page_count, 0) AS page_count,
			COALESCE(p.document_count, 0) AS document_count,
			COALESCE(p.checksum, '') AS checksum,
			COALESCE(p.download_count, 0) AS download_count,
			COALESCE(p.generated_by, '') AS generated_by,
			p.generated_at
		FROM investment.fd_closing_evidence_pack p
		JOIN investment.fd_closing_cycle c ON c.cycle_id = p.cycle_id
		WHERE COALESCE(p.is_deleted, false) = false %s
		  %s
		ORDER BY p.generated_at DESC NULLS LAST
		LIMIT NULLIF($1, 0) OFFSET $2
	`, ef, df)

	return runSourceQuery(ctx, pool, q, args)
}
