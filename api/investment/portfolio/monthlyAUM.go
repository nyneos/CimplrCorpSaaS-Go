package portfolio

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type monthlySchemeUnits struct {
	MonthIdx       int
	AmfiSchemeCode string
	Units          float64
}

// ComputeMonthlyPortfolioAUMForChart returns month-end AUM and net cash flows per FY month.
// AUM uses all holdings (including pre-FY) valued with MFAPI/staging NAV as of each month-end.
// Cash flows are net buys minus sells during each calendar month (for time-weighted indexing).
func ComputeMonthlyPortfolioAUMForChart(
	ctx context.Context,
	pool *pgxpool.Pool,
	entityFilter string,
	allowedEntities []string,
	monthDates []string,
) (aumByMonth map[int]float64, cashFlowByMonth map[int]float64, err error) {
	if len(monthDates) == 0 {
		return map[int]float64{}, map[int]float64{}, nil
	}

	schemeUnits, err := queryMonthlySchemeUnits(ctx, pool, entityFilter, allowedEntities, monthDates)
	if err != nil {
		return nil, nil, err
	}
	cashFlowByMonth, err = queryMonthlyCashFlows(ctx, pool, entityFilter, allowedEntities, monthDates)
	if err != nil {
		return nil, nil, err
	}

	schemeCodes := uniqueSchemeCodes(schemeUnits)
	firstDate, _ := time.Parse("2006-01-02", monthDates[0])
	lastDate, _ := time.Parse("2006-01-02", monthDates[len(monthDates)-1])
	// Load NAV history from 1 year before first month for month-end lookback.
	navFrom := firstDate.AddDate(-1, 0, 0)

	navHistory := make(map[string]map[string]float64, len(schemeCodes))
	for _, code := range schemeCodes {
		hist, fetchErr := FetchMFAPINAVHistory(ctx, code, navFrom, lastDate)
		if fetchErr != nil || len(hist) == 0 {
			hist = queryStagingNAVHistory(ctx, pool, code, navFrom, lastDate)
		}
		navHistory[code] = hist
	}

	aumByMonth = make(map[int]float64, len(monthDates))
	for _, su := range schemeUnits {
		monthEnd, parseErr := time.Parse("2006-01-02", monthDates[su.MonthIdx-1])
		if parseErr != nil {
			continue
		}
		nav := NAVAsOf(navHistory[su.AmfiSchemeCode], monthEnd)
		if nav <= 0 {
			nav = queryLatestTxnNAV(ctx, pool, entityFilter, allowedEntities, su.AmfiSchemeCode, monthEnd)
		}
		aumByMonth[su.MonthIdx] += su.Units * nav
	}
	return aumByMonth, cashFlowByMonth, nil
}

func queryMonthlySchemeUnits(ctx context.Context, pool *pgxpool.Pool, entityFilter string, allowedEntities []string, monthDates []string) ([]monthlySchemeUnits, error) {
	query := fmt.Sprintf(`
		WITH %s,
		month_dates AS (
			SELECT ordinality AS month_idx, month_end::date AS month_end
			FROM UNNEST($2::date[]) WITH ORDINALITY AS t(month_end, ordinality)
		),
		unit_flows AS (
			SELECT
				md.month_idx,
				md.month_end,
				COALESCE(NULLIF(TRIM(sr.amfi_scheme_code), ''), '') AS amfi_scheme_code,
				SUM(
					CASE
						WHEN sr.transaction_date <= md.month_end THEN
							CASE
								WHEN LOWER(TRIM(sr.transaction_type)) IN ('buy','purchase','subscription','sip','switch_in','bonus') THEN COALESCE(sr.units, 0)
								WHEN LOWER(TRIM(sr.transaction_type)) IN ('sell','redemption','redeem','switch_out') THEN -COALESCE(sr.units, 0)
								ELSE 0
							END
						ELSE 0
					END
				)::float8 AS units
			FROM month_dates md
			CROSS JOIN scheme_resolved sr
			WHERE ($1::text[] IS NULL OR TRIM(sr.entity_name) = ANY(SELECT TRIM(x) FROM unnest($1::text[]) AS x))
			GROUP BY md.month_idx, md.month_end, amfi_scheme_code
			HAVING SUM(
				CASE
					WHEN sr.transaction_date <= md.month_end THEN
						CASE
							WHEN LOWER(TRIM(sr.transaction_type)) IN ('buy','purchase','subscription','sip','switch_in','bonus') THEN COALESCE(sr.units, 0)
							WHEN LOWER(TRIM(sr.transaction_type)) IN ('sell','redemption','redeem','switch_out') THEN -COALESCE(sr.units, 0)
							ELSE 0
						END
					ELSE 0
				END
			) > 0.0001
		)
		SELECT month_idx, amfi_scheme_code, units FROM unit_flows ORDER BY month_idx, amfi_scheme_code`,
		portfolioSchemeResolvedCTE)

	rows, err := pool.Query(ctx, query, holdingsEntityScopeArg(entityFilter, allowedEntities), monthDates)
	if err != nil {
		return nil, fmt.Errorf("monthly scheme units: %w", err)
	}
	defer rows.Close()

	var out []monthlySchemeUnits
	for rows.Next() {
		var su monthlySchemeUnits
		if err := rows.Scan(&su.MonthIdx, &su.AmfiSchemeCode, &su.Units); err != nil {
			return nil, err
		}
		out = append(out, su)
	}
	return out, rows.Err()
}

func queryMonthlyCashFlows(ctx context.Context, pool *pgxpool.Pool, entityFilter string, allowedEntities []string, monthDates []string) (map[int]float64, error) {
	query := fmt.Sprintf(`
		WITH %s,
		month_dates AS (
			SELECT ordinality AS month_idx, month_end::date AS month_end,
			       date_trunc('month', month_end)::date AS month_start
			FROM UNNEST($2::date[]) WITH ORDINALITY AS t(month_end, ordinality)
		),
		flows AS (
			SELECT
				md.month_idx,
				COALESCE(SUM(
					CASE
						WHEN sr.transaction_date >= md.month_start AND sr.transaction_date <= md.month_end THEN
							CASE
								WHEN LOWER(TRIM(sr.transaction_type)) IN ('buy','purchase','subscription','sip','switch_in','bonus') THEN COALESCE(sr.amount, 0)
								WHEN LOWER(TRIM(sr.transaction_type)) IN ('sell','redemption','redeem','switch_out') THEN -COALESCE(sr.amount, 0)
								ELSE 0
							END
						ELSE 0
					END
				), 0)::float8 AS net_flow
			FROM month_dates md
			CROSS JOIN scheme_resolved sr
			WHERE ($1::text[] IS NULL OR TRIM(sr.entity_name) = ANY(SELECT TRIM(x) FROM unnest($1::text[]) AS x))
			GROUP BY md.month_idx
		)
		SELECT month_idx, net_flow FROM flows ORDER BY month_idx`,
		portfolioSchemeResolvedCTE)

	rows, err := pool.Query(ctx, query, holdingsEntityScopeArg(entityFilter, allowedEntities), monthDates)
	if err != nil {
		return nil, fmt.Errorf("monthly cash flows: %w", err)
	}
	defer rows.Close()

	out := make(map[int]float64, len(monthDates))
	for rows.Next() {
		var idx int
		var flow float64
		if err := rows.Scan(&idx, &flow); err != nil {
			return nil, err
		}
		out[idx] = flow
	}
	return out, rows.Err()
}

func queryStagingNAVHistory(ctx context.Context, pool *pgxpool.Pool, schemeCode string, from, to time.Time) map[string]float64 {
	rows, err := pool.Query(ctx, `
		SELECT nav_date::text, nav_value::float8
		FROM investment.amfi_nav_staging
		WHERE scheme_code::text = $1
		  AND nav_date >= $2::date AND nav_date <= $3::date
		ORDER BY nav_date`,
		schemeCode, from.Format("2006-01-02"), to.Format("2006-01-02"))
	if err != nil {
		return map[string]float64{}
	}
	defer rows.Close()

	out := make(map[string]float64)
	for rows.Next() {
		var dateStr string
		var nav float64
		if err := rows.Scan(&dateStr, &nav); err != nil {
			continue
		}
		if len(dateStr) >= 10 {
			out[dateStr[:10]] = nav
		}
	}
	return out
}

func queryLatestTxnNAV(ctx context.Context, pool *pgxpool.Pool, entityFilter string, allowedEntities []string, schemeCode string, asOf time.Time) float64 {
	query := fmt.Sprintf(`
		WITH %s
		SELECT COALESCE(sr.nav, 0)::float8
		FROM scheme_resolved sr
		WHERE COALESCE(NULLIF(TRIM(sr.amfi_scheme_code), ''), '') = $2
		  AND sr.transaction_date <= $3::date
		  AND COALESCE(sr.nav, 0) > 0
		  AND ($1::text[] IS NULL OR TRIM(sr.entity_name) = ANY(SELECT TRIM(x) FROM unnest($1::text[]) AS x))
		ORDER BY sr.transaction_date DESC
		LIMIT 1`, portfolioSchemeResolvedCTE)

	var nav float64
	_ = pool.QueryRow(ctx, query, holdingsEntityScopeArg(entityFilter, allowedEntities), schemeCode, asOf.Format("2006-01-02")).Scan(&nav)
	return nav
}

func uniqueSchemeCodes(units []monthlySchemeUnits) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, u := range units {
		if u.AmfiSchemeCode == "" {
			continue
		}
		if _, ok := seen[u.AmfiSchemeCode]; ok {
			continue
		}
		seen[u.AmfiSchemeCode] = struct{}{}
		out = append(out, u.AmfiSchemeCode)
	}
	return out
}

// QueryMonthlyPortfolioAUM is kept for callers that only need raw month-end AUM maps.
func QueryMonthlyPortfolioAUM(ctx context.Context, pool *pgxpool.Pool, entityFilter string, allowedEntities []string, monthDates []string, _ time.Time) (map[int]float64, error) {
	aum, _, err := ComputeMonthlyPortfolioAUMForChart(ctx, pool, entityFilter, allowedEntities, monthDates)
	return aum, err
}
