package portfolio

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api/investment/schemejoin"

	"github.com/jackc/pgx/v5/pgxpool"
)

const masterschemeLookupJoinPS = `
		LEFT JOIN investment.masterscheme s ON (` + schemejoin.JoinOnSchemeID + `)`

const masterschemeLookupJoinTS = `
		LEFT JOIN investment.masterscheme s ON (
COALESCE(s.is_deleted, false) = false
AND NULLIF(TRIM(ts.scheme_id), '') IS NOT NULL
AND s.scheme_id::text = TRIM(ts.scheme_id))`

const holdingsJoinTransactionSummary = `
		JOIN transaction_summary ts ON (
			ts.entity_name = ps.entity_name
			AND ts.folio_number = ps.folio_number
			AND COALESCE(ts.demat_acc_number, '') = COALESCE(ps.demat_acc_number, '')
			AND ts.scheme_id = ps.scheme_id
		)`

const holdingsCostSelectCols = `
			ts.avg_nav,
			COALESCE(ln.nav_value, 0) as current_nav,
			COALESCE(ts.total_units, 0) * COALESCE(ln.nav_value, 0) as current_value,
			(COALESCE(ts.total_units, 0) * COALESCE(ln.nav_value, 0)) - COALESCE(ts.total_invested_amount, 0) as gain_loss,
			CASE WHEN COALESCE(ts.total_invested_amount, 0) > 0
				 THEN (((COALESCE(ts.total_units, 0) * COALESCE(ln.nav_value, 0)) - COALESCE(ts.total_invested_amount, 0)) / COALESCE(ts.total_invested_amount, 0)) * 100
				 ELSE 0 END as gain_loss_percent,
			COALESCE(ts.realized_gain_loss, 0) as realized_gain_loss,
			((COALESCE(ts.total_units, 0) * COALESCE(ln.nav_value, 0)) - COALESCE(ts.total_invested_amount, 0)) + COALESCE(ts.realized_gain_loss, 0) as total_gain_loss,
			COALESCE(ts.total_invested_amount, 0) as total_invested_amount`

type holdingsQueryArgs struct {
	entityScope interface{}
	amcLike     string
	schemeLike  string
}

func buildHoldingsQueryArgs(f HoldingsFilter, dumpAll bool, entityNames []string) holdingsQueryArgs {
	args := holdingsQueryArgs{}
	if f.EntityName != "" {
		args.entityScope = []string{f.EntityName}
	} else if !dumpAll && len(entityNames) > 0 {
		args.entityScope = entityNames
	}
	args.amcLike = strings.ToLower(f.AmcName)
	args.schemeLike = strings.ToLower(f.SchemeName)
	return args
}

func queryHoldingsFromSnapshot(ctx context.Context, pgxPool *pgxpool.Pool, f HoldingsFilter, dumpAll bool, entityNames []string) ([]PortfolioHoldingsRow, error) {
	qArgs := buildHoldingsQueryArgs(f, dumpAll, entityNames)

	sqlArgs := []interface{}{qArgs.entityScope}
	whereClauses := []string{"1=1"}
	addArg := func(val interface{}) string {
		sqlArgs = append(sqlArgs, val)
		return fmt.Sprintf("$%d", len(sqlArgs))
	}

	if f.EntityName != "" {
		whereClauses = append(whereClauses, "ps.entity_name = "+addArg(f.EntityName))
	} else if !dumpAll && len(entityNames) > 0 {
		whereClauses = append(whereClauses, "ps.entity_name = ANY("+addArg(entityNames)+"::text[])")
	}
	if f.AmcName != "" {
		whereClauses = append(whereClauses, "LOWER(COALESCE(s.amc_name,'')) LIKE "+addArg("%"+qArgs.amcLike+"%"))
	}
	if f.SchemeName != "" {
		whereClauses = append(whereClauses, "LOWER(COALESCE(ps.scheme_name,'')) LIKE "+addArg("%"+qArgs.schemeLike+"%"))
	}

	query := fmt.Sprintf(`
		WITH %s
		SELECT
			COALESCE(ps.entity_name, '') as entity_name,
			COALESCE(ps.folio_number, '') as folio_number,
			COALESCE(ps.demat_acc_number, '') as demat_account_number,
			COALESCE(ps.scheme_id, '') as scheme_id,
			COALESCE(ps.scheme_name, '') as scheme_name,
			COALESCE(ps.isin, '') as isin,
			COALESCE(s.amc_name, '') as amc_name,
			COALESCE(ts.total_units, 0) as total_units,
			`+holdingsCostSelectCols+`,
			COALESCE(TO_CHAR(ps.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'') as updated_at
		FROM (
			SELECT DISTINCT ON (entity_name, folio_number, COALESCE(demat_acc_number, ''), scheme_id)
				ps.*
			FROM investment.portfolio_snapshot ps
			ORDER BY entity_name, folio_number, COALESCE(demat_acc_number, ''), scheme_id, created_at DESC, id DESC
		) ps
		`+holdingsJoinTransactionSummary+`
		`+masterschemeLookupJoinPS+`
		`+schemejoin.NavLateralJoin("ts", "ts.amfi_scheme_code")+`
		WHERE %s
		  AND COALESCE(ts.total_units, 0) > 0
		ORDER BY ps.entity_name, ps.scheme_name
	`, portfolioSchemeResolvedCTE, strings.Join(whereClauses, " AND "))

	return scanHoldingsRows(ctx, pgxPool, query, sqlArgs...)
}

func queryHoldingsFromTransactions(ctx context.Context, pgxPool *pgxpool.Pool, f HoldingsFilter, dumpAll bool, entityNames []string) ([]PortfolioHoldingsRow, error) {
	qArgs := buildHoldingsQueryArgs(f, dumpAll, entityNames)

	sqlArgs := []interface{}{qArgs.entityScope}
	addArg := func(val interface{}) string {
		sqlArgs = append(sqlArgs, val)
		return fmt.Sprintf("$%d", len(sqlArgs))
	}

	whereClauses := []string{"ts.total_units > 0"}
	if f.EntityName != "" {
		whereClauses = append(whereClauses, "ts.entity_name = "+addArg(f.EntityName))
	} else if !dumpAll && len(entityNames) > 0 {
		whereClauses = append(whereClauses, "ts.entity_name = ANY("+addArg(entityNames)+"::text[])")
	}
	if f.AmcName != "" {
		whereClauses = append(whereClauses, "LOWER(COALESCE(s.amc_name,'')) LIKE "+addArg("%"+qArgs.amcLike+"%"))
	}
	if f.SchemeName != "" {
		whereClauses = append(whereClauses, "LOWER(COALESCE(ts.scheme_name,'')) LIKE "+addArg("%"+qArgs.schemeLike+"%"))
	}

	query := fmt.Sprintf(`
		WITH %s
		SELECT
			COALESCE(ts.entity_name, '') AS entity_name,
			COALESCE(ts.folio_number, '') AS folio_number,
			COALESCE(ts.demat_acc_number, '') AS demat_account_number,
			COALESCE(ts.scheme_id, '') AS scheme_id,
			COALESCE(ts.scheme_name, '') AS scheme_name,
			COALESCE(ts.isin, '') AS isin,
			COALESCE(s.amc_name, '') AS amc_name,
			ts.total_units,
			`+holdingsCostSelectCols+`,
			TO_CHAR(NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata', 'YYYY-MM-DD HH24:MI:SS') AS updated_at
		FROM transaction_summary ts
		`+masterschemeLookupJoinTS+`
		`+schemejoin.NavLateralJoin("ts", "ts.amfi_scheme_code")+`
		WHERE %s
		ORDER BY ts.entity_name, ts.scheme_name
	`, portfolioSchemeResolvedCTE, strings.Join(whereClauses, " AND "))

	return scanHoldingsRows(ctx, pgxPool, query, sqlArgs...)
}

func scanHoldingsRows(ctx context.Context, pgxPool *pgxpool.Pool, query string, args ...interface{}) ([]PortfolioHoldingsRow, error) {
	rows, err := pgxPool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []PortfolioHoldingsRow
	for rows.Next() {
		var row PortfolioHoldingsRow
		if err := rows.Scan(
			&row.EntityName, &row.FolioNumber, &row.DematAccountNumber,
			&row.SchemeID, &row.SchemeName, &row.ISIN, &row.AmcName,
			&row.TotalUnits, &row.AvgNav, &row.CurrentNav, &row.CurrentValue,
			&row.GainLoss, &row.GainLossPercent, &row.RealizedGainLoss, &row.TotalGainLoss,
			&row.TotalInvestedAmount, &row.UpdatedAt,
		); err != nil {
			return nil, err
		}
		row.Transactions = []map[string]interface{}{}
		results = append(results, row)
	}
	if results == nil {
		results = []PortfolioHoldingsRow{}
	}
	return results, rows.Err()
}

// QueryEntityHoldings returns holdings for one entity using live cost basis and AMFI NAV.
func QueryEntityHoldings(ctx context.Context, pgxPool *pgxpool.Pool, entityName string) ([]PortfolioHoldingsRow, error) {
	entityName = strings.TrimSpace(entityName)
	if entityName == "" {
		return []PortfolioHoldingsRow{}, nil
	}
	f := HoldingsFilter{EntityName: entityName}
	results, err := queryHoldingsFromSnapshot(ctx, pgxPool, f, false, nil)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return queryHoldingsFromTransactions(ctx, pgxPool, f, false, nil)
	}
	return results, nil
}

// MatchHoldingRow finds a holding row by scheme and folio/demat identifiers.
func MatchHoldingRow(rows []PortfolioHoldingsRow, schemeID, folioNumber, dematAcc string) *PortfolioHoldingsRow {
	schemeID = strings.TrimSpace(schemeID)
	folioNumber = strings.TrimSpace(folioNumber)
	dematAcc = strings.TrimSpace(dematAcc)
	for i := range rows {
		r := &rows[i]
		if schemeID != "" && strings.TrimSpace(r.SchemeID) != schemeID {
			continue
		}
		if folioNumber != "" && strings.TrimSpace(r.FolioNumber) != folioNumber {
			continue
		}
		if dematAcc != "" && strings.TrimSpace(r.DematAccountNumber) != dematAcc {
			continue
		}
		return r
	}
	return nil
}
