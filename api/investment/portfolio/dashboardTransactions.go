package portfolio

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// QueryAuditTransactions returns all YTD rows including superseded onboard edits (audit trail).
// Use for transaction drill-downs; holdings/AUM math still uses deduped scheme_resolved.
func QueryAuditTransactions(ctx context.Context, pool *pgxpool.Pool, entityFilter string, allowedEntities []string, fromDate time.Time, toDate time.Time) ([]PortfolioTxRow, error) {
	return querySchemeResolvedTransactions(ctx, pool, entityFilter, allowedEntities, fromDate, toDate, "scheme_resolved_raw")
}

// QueryDashboardTransactions returns YTD rows from the same scheme_resolved engine as live holdings.
// Use this for dashboard drill-downs where the full transactions UNION may fail or diverge.
func QueryDashboardTransactions(ctx context.Context, pool *pgxpool.Pool, entityFilter string, allowedEntities []string, fromDate time.Time, toDate time.Time) ([]PortfolioTxRow, error) {
	return querySchemeResolvedTransactions(ctx, pool, entityFilter, allowedEntities, fromDate, toDate, "scheme_resolved")
}

func querySchemeResolvedTransactions(ctx context.Context, pool *pgxpool.Pool, entityFilter string, allowedEntities []string, fromDate time.Time, toDate time.Time, sourceTable string) ([]PortfolioTxRow, error) {
	txTypeExpr := strings.Replace(SQLNormalizeTransactionType, "transaction_type", "sr.transaction_type", -1)
	query := fmt.Sprintf(`
		WITH %s
		SELECT
			CASE
				WHEN LOWER(TRIM(sr.transaction_type)) IN ('sell','redemption','redeem','switch_out') THEN 'Redemption'
				ELSE 'Onboard'
			END AS tx_type,
			TRIM(sr.entity_name) AS entity_name,
			COALESCE(sr.scheme_name, '') AS scheme_name,
			COALESCE(sr.amfi_scheme_code, '') AS amfi_scheme_code,
			COALESCE(ABS(sr.amount), 0)::float8 AS amount,
			'APPROVED'::text AS status,
			''::text AS processing_status,
			COALESCE(s.amc_name, '') AS amc_name,
			COALESCE(s.internal_scheme_code, sr.scheme_id, '') AS scheme_code,
			COALESCE(sr.isin, '') AS isin,
			COALESCE(sr.folio_number, '') AS folio_number,
			COALESCE(sr.demat_acc_number, '') AS demat_number,
			COALESCE(TO_CHAR(sr.transaction_date, 'YYYY-MM-DD'), '') AS transaction_date,
			%s AS transaction_type,
			COALESCE(sr.units, 0)::float8 AS units,
			COALESCE(sr.nav, 0)::float8 AS nav,
			''::text AS nav_date,
			COALESCE(ABS(sr.amount), 0)::float8 AS net_amount,
			0::float8 AS stamp_duty,
			0::float8 AS exit_load,
			0::float8 AS tds,
			''::text AS confirmed_at,
			'Holdings Engine'::text AS source,
			''::text AS initiation_id,
			''::text AS confirmation_id,
			''::text AS batch_id,
			COALESCE(sr.scheme_id, '') AS scheme_id,
			COALESCE(sr.folio_id::text, '') AS folio_id,
			COALESCE(sr.demat_id::text, '') AS demat_id
		FROM `+sourceTable+` sr
		LEFT JOIN investment.masterscheme s ON (`+joinOnboardSchemeID("s", "sr.scheme_id")+`)
		WHERE sr.transaction_date >= $2::date
		  AND sr.transaction_date <= $3::date
		ORDER BY sr.transaction_date DESC, sr.entity_name`,
		portfolioSchemeResolvedCTE, txTypeExpr)

	rows, err := pool.Query(ctx, query, holdingsEntityScopeArg(entityFilter, allowedEntities), fromDate.Format("2006-01-02"), toDate.Format("2006-01-02"))
	if err != nil {
		return nil, fmt.Errorf("dashboard transactions: %w", err)
	}
	defer rows.Close()

	var out []PortfolioTxRow
	for rows.Next() {
		var row PortfolioTxRow
		if err := rows.Scan(
			&row.TxType, &row.EntityName, &row.SchemeName, &row.AmfiSchemeCode,
			&row.Amount, &row.Status, &row.ProcessingStatus,
			&row.AmcName, &row.SchemeCode, &row.ISIN, &row.FolioNumber, &row.DematNumber,
			&row.TransactionDate, &row.TransactionType, &row.Units, &row.Nav, &row.NavDate,
			&row.NetAmount, &row.StampDuty, &row.ExitLoad, &row.TDS,
			&row.ConfirmedAt, &row.Source,
			&row.InitiationID, &row.ConfirmationID, &row.BatchID,
			&row.SchemeID, &row.FolioID, &row.DematID,
		); err != nil {
			return nil, fmt.Errorf("dashboard transactions scan: %w", err)
		}
		row.EntityName = strings.TrimSpace(row.EntityName)
		row.TransactionType = NormalizeTransactionType(row.TransactionType)
		out = append(out, row)
	}
	if out == nil {
		out = []PortfolioTxRow{}
	}
	return out, rows.Err()
}

func joinOnboardSchemeID(msAlias, schemeIDCol string) string {
	return `
COALESCE(` + msAlias + `.is_deleted, false) = false
AND NULLIF(TRIM(` + schemeIDCol + `), '') IS NOT NULL
AND ` + msAlias + `.scheme_id::text = TRIM(` + schemeIDCol + `)`
}
