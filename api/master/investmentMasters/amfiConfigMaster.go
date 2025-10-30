package allMaster

import (
	"CimplrCorpSaas/api"
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// 1️⃣ Get data from AMFI Scheme Master Staging
func GetAMFISchemeMasterSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		query := `
			SELECT DISTINCT ON (amc_name, scheme_name)
			amc_name,
			scheme_name,
			isin_div_payout_growth
			FROM investment.amfi_scheme_master_staging
			WHERE isin_div_payout_growth IS NOT NULL 
 			AND isin_div_payout_growth <> ''
			ORDER BY amc_name, scheme_name, isin_div_payout_growth;
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var amcName, schemeName, isin string
			_ = rows.Scan(&amcName, &schemeName, &isin)
			out = append(out, map[string]interface{}{
				"amc_name":    amcName,
				"scheme_name": schemeName,
				"isin":        isin,
			})
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

// 2️⃣ Get data from AMFI NAV Staging
func GetAMFINavStagingSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		query := `
			SELECT 
				amc_name,
				scheme_name,
				isin_div_payout_growth
			FROM investment.amfi_nav_staging
			ORDER BY amc_name, scheme_name, nav_date DESC
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var amcName, schemeName, isin string
			_ = rows.Scan(&amcName, &schemeName, &isin)
			out = append(out, map[string]interface{}{
				"amc_name":    amcName,
				"scheme_name": schemeName,
				"isin":        isin,
			})
		}

		api.RespondWithPayload(w, true, "", out)
	}
}
