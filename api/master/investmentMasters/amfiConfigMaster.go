package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func GetAMFISchemeMasterSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		query := `
			SELECT DISTINCT ON (amc_name)
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
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
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

func GetAMFINavStagingSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		query := `
			SELECT DISTINCT
				amc_name,
				scheme_name,
				isin_div_payout_growth
			FROM investment.amfi_nav_staging
			ORDER BY amc_name, scheme_name, nav_date DESC
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
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

func GetApprovedAMCsAndSchemes(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		amcQuery := `
			WITH latest_audit AS (
				SELECT DISTINCT ON (a.amc_id)
					a.amc_id, a.processing_status, a.requested_at
				FROM investment.auditactionamc a
				ORDER BY a.amc_id, a.requested_at DESC
			)
			SELECT m.amc_id, m.amc_name
			FROM investment.masteramc m
			JOIN latest_audit l ON l.amc_id = m.amc_id
			WHERE COALESCE(m.is_deleted,false)=false
			  AND UPPER(l.processing_status)='APPROVED'
			  AND UPPER(m.status)='ACTIVE'
			ORDER BY m.amc_name;
		`

		rows, err := pgxPool.Query(ctx, amcQuery)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch AMCs: "+err.Error())
			return
		}
		defer rows.Close()

		type AMC struct {
			ID   string `json:"amc_id"`
			Name string `json:"amc_name"`
		}

		amcs := []AMC{}
		for rows.Next() {
			var a AMC
			_ = rows.Scan(&a.ID, &a.Name)
			amcs = append(amcs, a)
		}
		if len(amcs) == 0 {
			api.RespondWithPayload(w, true, "No approved active AMCs found", []map[string]any{})
			return
		}

		amcNames := make([]string, len(amcs))
		for i, a := range amcs {
			amcNames[i] = a.Name
		}

		schemeQuery := `
			SELECT 
				amc_name,
				scheme_code,
				scheme_name,
				COALESCE(isin_div_reinvestment, '') AS isin
			FROM investment.amfi_scheme_master_staging
			WHERE amc_name = ANY($1)
			ORDER BY amc_name, scheme_name;
		`

		schemeRows, err := pgxPool.Query(ctx, schemeQuery, amcNames)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch schemes: "+err.Error())
			return
		}
		defer schemeRows.Close()

		type Scheme struct {
			SchemeCode int64  `json:"scheme_code"`
			SchemeName string `json:"scheme_name"`
			ISIN       string `json:"isin"`
		}

		result := []map[string]interface{}{}

		for schemeRows.Next() {
			var amcName, schemeName, isin string
			var schemeCode int64
			if err := schemeRows.Scan(&amcName, &schemeCode, &schemeName, &isin); err != nil {
				continue
			}
			result = append(result, map[string]interface{}{
				"amc_name":    amcName,
				"scheme_code": schemeCode,
				"scheme_name": schemeName,
				"isin":        isin,
			})
		}

		if schemeRows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Scheme scan error: "+schemeRows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", result)
	}
}
