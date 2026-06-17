package portfolio

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HoldingsFilter struct {
	EntityName string `json:"entity_name"`
	AmcName    string `json:"amc_name"`
	SchemeName string `json:"scheme_name"`
}

type PortfolioHoldingsRow struct {
	EntityName          string  `json:"entity_name"`
	FolioNumber         string  `json:"folio_number"`
	DematAccountNumber  string  `json:"demat_account_number"`
	SchemeName          string  `json:"scheme_name"`
	ISIN                string  `json:"isin"`
	AmcName             string  `json:"amc_name"`
	TotalUnits          float64 `json:"total_units"`
	AvgNav              float64 `json:"avg_nav"`
	CurrentNav          float64 `json:"current_nav"`
	CurrentValue        float64 `json:"current_value"`
	GainLoss            float64 `json:"gain_loss"`
	GainLossPercent     float64 `json:"gain_loss_percent"`
	TotalInvestedAmount float64 `json:"total_invested_amount"`
	UpdatedAt           string  `json:"updated_at"`
}

func GetPortfolioHoldings(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		scope := ctxutil.FromContext(ctx)

		var f HoldingsFilter
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil && r.ContentLength > 0 {
			api.RespondWithError(w, http.StatusBadRequest, constants.ErrInvalidJSONPrefix+err.Error())
			return
		}
		f.EntityName = strings.TrimSpace(f.EntityName)
		f.AmcName = strings.TrimSpace(f.AmcName)
		f.SchemeName = strings.TrimSpace(f.SchemeName)

		if f.EntityName != "" && !scope.HasEntityNameAccess(f.EntityName) {
			api.RespondWithError(w, http.StatusForbidden, "entity_name is not within your authorized access scope")
			return
		}

		var args []interface{}
		whereClauses := []string{"1=1"}
		addArg := func(val interface{}) string {
			args = append(args, val)
			return fmt.Sprintf("$%d", len(args))
		}

		if f.EntityName != "" {
			whereClauses = append(whereClauses, "ps.entity_name = "+addArg(f.EntityName))
		} else if len(scope.EntityNames) > 0 {
			whereClauses = append(whereClauses, "ps.entity_name = ANY("+addArg(scope.EntityNames)+"::text[])")
		}

		if f.AmcName != "" {
			whereClauses = append(whereClauses, "LOWER(COALESCE(s.amc_name,'')) LIKE "+addArg("%"+strings.ToLower(f.AmcName)+"%"))
		}
		if f.SchemeName != "" {
			whereClauses = append(whereClauses, "LOWER(COALESCE(ps.scheme_name,'')) LIKE "+addArg("%"+strings.ToLower(f.SchemeName)+"%"))
		}

		whereStr := strings.Join(whereClauses, " AND ")

		// Query aggregates the snapshot across batches if there are multiple, though typically snapshot is unique per scheme/folio.
		query := fmt.Sprintf(`
			SELECT
				COALESCE(ps.entity_name, '') as entity_name,
				COALESCE(ps.folio_number, '') as folio_number,
				COALESCE(ps.demat_acc_number, '') as demat_account_number,
				COALESCE(ps.scheme_name, '') as scheme_name,
				COALESCE(ps.isin, '') as isin,
				COALESCE(s.amc_name, '') as amc_name,
				SUM(COALESCE(ps.total_units, 0)) as total_units,
				CASE WHEN SUM(COALESCE(ps.total_units, 0)) > 0 
					 THEN SUM(COALESCE(ps.total_invested_amount, 0)) / SUM(COALESCE(ps.total_units, 0)) 
					 ELSE 0 END as avg_nav,
				MAX(COALESCE(ps.current_nav, 0)) as current_nav,
				SUM(COALESCE(ps.total_units, 0)) * MAX(COALESCE(ps.current_nav, 0)) as current_value,
				(SUM(COALESCE(ps.total_units, 0)) * MAX(COALESCE(ps.current_nav, 0))) - SUM(COALESCE(ps.total_invested_amount, 0)) as gain_loss,
				CASE WHEN SUM(COALESCE(ps.total_invested_amount, 0)) > 0 
					 THEN (((SUM(COALESCE(ps.total_units, 0)) * MAX(COALESCE(ps.current_nav, 0))) - SUM(COALESCE(ps.total_invested_amount, 0))) / SUM(COALESCE(ps.total_invested_amount, 0))) * 100 
					 ELSE 0 END as gain_loss_percent,
				SUM(COALESCE(ps.total_invested_amount, 0)) as total_invested_amount,
				MAX(COALESCE(TO_CHAR(ps.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kolkata','YYYY-MM-DD HH24:MI:SS'),'')) as updated_at
			FROM investment.portfolio_snapshot ps
			LEFT JOIN investment.masterscheme s ON (s.scheme_id::text = ps.scheme_id OR s.isin = ps.isin OR s.scheme_name = ps.scheme_name)
			WHERE %s
			GROUP BY ps.entity_name, ps.folio_number, ps.demat_acc_number, ps.scheme_name, ps.isin, s.amc_name
			HAVING SUM(COALESCE(ps.total_units, 0)) > 0
			ORDER BY ps.entity_name, ps.scheme_name
		`, whereStr)

		rows, err := pgxPool.Query(ctx, query, args...)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "query failed: "+err.Error())
			return
		}
		defer rows.Close()

		var results []PortfolioHoldingsRow
		for rows.Next() {
			var row PortfolioHoldingsRow
			if err := rows.Scan(
				&row.EntityName, &row.FolioNumber, &row.DematAccountNumber,
				&row.SchemeName, &row.ISIN, &row.AmcName,
				&row.TotalUnits, &row.AvgNav, &row.CurrentNav, &row.CurrentValue,
				&row.GainLoss, &row.GainLossPercent, &row.TotalInvestedAmount, &row.UpdatedAt,
			); err != nil {
				api.RespondWithError(w, http.StatusInternalServerError, "scan failed: "+err.Error())
				return
			}
			results = append(results, row)
		}
		if results == nil {
			results = []PortfolioHoldingsRow{}
		}

		api.RespondWithPayload(w, true, "", results)
	}
}
