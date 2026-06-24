package portfolio

import (
	"context"
	"fmt"

	"CimplrCorpSaas/api/investment/schemejoin"

	"github.com/jackc/pgx/v5/pgxpool"
)

// HoldingsKPI holds live portfolio totals from the same engine as holdings API.
type HoldingsKPI struct {
	TotalAUM      float64
	UnrealizedPnL float64
	TotalPnL      float64
}

// HoldingsPnLRow is one heatmap cell grouped by entity / AMC / scheme.
type HoldingsPnLRow struct {
	Entity        string
	AMC           string
	Scheme        string
	UnrealizedPnL float64
}

func holdingsEntityScopeArg(entityFilter string, allowedEntities []string) interface{} {
	if entityFilter != "" {
		return []string{entityFilter}
	}
	if len(allowedEntities) > 0 {
		return allowedEntities
	}
	return nil
}

// QueryHoldingsKPI returns live AUM and P&L totals aligned with portfolio holdings.
func QueryHoldingsKPI(ctx context.Context, pool *pgxpool.Pool, entityFilter string, allowedEntities []string) (HoldingsKPI, error) {
	query := fmt.Sprintf(`
		WITH %s,
		valued AS (
			SELECT
				COALESCE(ts.total_units, 0) * COALESCE(ln.nav_value, 0) AS current_value,
				(COALESCE(ts.total_units, 0) * COALESCE(ln.nav_value, 0)) - COALESCE(ts.total_invested_amount, 0) AS gain_loss,
				((COALESCE(ts.total_units, 0) * COALESCE(ln.nav_value, 0)) - COALESCE(ts.total_invested_amount, 0)) + COALESCE(ts.realized_gain_loss, 0) AS total_gain_loss
			FROM transaction_summary ts
			`+schemejoin.NavLateralJoin("ts", "ts.amfi_scheme_code")+`
			WHERE COALESCE(ts.total_units, 0) > 0
		)
		SELECT
			COALESCE(SUM(current_value), 0)::float8,
			COALESCE(SUM(gain_loss), 0)::float8,
			COALESCE(SUM(total_gain_loss), 0)::float8
		FROM valued`,
		portfolioSchemeResolvedCTE)

	var kpi HoldingsKPI
	err := pool.QueryRow(ctx, query, holdingsEntityScopeArg(entityFilter, allowedEntities)).
		Scan(&kpi.TotalAUM, &kpi.UnrealizedPnL, &kpi.TotalPnL)
	return kpi, err
}

// QueryHoldingsPnLHeatmap returns unrealized P&L grouped for dashboard heatmaps.
func QueryHoldingsPnLHeatmap(ctx context.Context, pool *pgxpool.Pool, entityFilter string, allowedEntities []string, byScheme bool) ([]HoldingsPnLRow, error) {
	schemeCol := `'All Schemes'::text`
	groupCols := "COALESCE(v.entity_name, 'Unknown'), COALESCE(v.amc_name, 'Unknown')"
	if byScheme {
		schemeCol = `COALESCE(v.scheme_name, 'Unknown')`
		groupCols = "COALESCE(v.entity_name, 'Unknown'), COALESCE(v.amc_name, 'Unknown'), COALESCE(v.scheme_name, 'Unknown')"
	}

	query := fmt.Sprintf(`
		WITH %s,
		valued AS (
			SELECT
				ts.entity_name,
				COALESCE(s.amc_name, 'Unknown') AS amc_name,
				COALESCE(ts.scheme_name, 'Unknown') AS scheme_name,
				(COALESCE(ts.total_units, 0) * COALESCE(ln.nav_value, 0)) - COALESCE(ts.total_invested_amount, 0) AS gain_loss
			FROM transaction_summary ts
			LEFT JOIN investment.masterscheme s ON (`+schemejoin.JoinOnSchemeIDAlias("s", "ts.scheme_id")+`)
			`+schemejoin.NavLateralJoin("ts", "ts.amfi_scheme_code")+`
			WHERE COALESCE(ts.total_units, 0) > 0
		)
		SELECT
			COALESCE(v.entity_name, 'Unknown') AS entity,
			COALESCE(v.amc_name, 'Unknown') AS amc,
			%s,
			SUM(COALESCE(v.gain_loss, 0))::float8 AS pnl
		FROM valued v
		GROUP BY %s
		HAVING SUM(COALESCE(v.gain_loss, 0)) != 0
		ORDER BY entity, amc, ABS(SUM(COALESCE(v.gain_loss, 0))) DESC`,
		portfolioSchemeResolvedCTE, schemeCol, groupCols)

	rows, err := pool.Query(ctx, query, holdingsEntityScopeArg(entityFilter, allowedEntities))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []HoldingsPnLRow
	for rows.Next() {
		var row HoldingsPnLRow
		if err := rows.Scan(&row.Entity, &row.AMC, &row.Scheme, &row.UnrealizedPnL); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
