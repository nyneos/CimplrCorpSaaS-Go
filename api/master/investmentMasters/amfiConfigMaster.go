package allMaster

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func GetAMFISchemeMasterSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// scheme_nav_name is unique per plan type; scheme_name is not (same base name for all plans).
		// We join to masterscheme only on the unique amfi_scheme_code to avoid false matches.
		query := `
			SELECT DISTINCT ON (sm.scheme_code)
				sm.scheme_code,
				sm.amc_name,
				COALESCE(sm.scheme_nav_name, sm.scheme_name) AS scheme_name,
				COALESCE(sm.isin_div_reinvestment, '') AS isin
			FROM investment.amfi_scheme_master_staging sm
			LEFT JOIN investment.masterscheme ms
				ON ms.amfi_scheme_code = sm.scheme_code::text
				AND COALESCE(ms.is_deleted, false) = false
			WHERE ms.scheme_id IS NULL
			ORDER BY sm.scheme_code;
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var schemeCode int64
			var amcName, schemeName, isin string
			_ = rows.Scan(&schemeCode, &amcName, &schemeName, &isin)
			out = append(out, map[string]interface{}{
				"scheme_code": schemeCode,
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
		ctx := r.Context()

		// scheme_name in amfi_nav_staging is the scheme_nav_name equivalent from the AMFI file
		// (it already distinguishes plan types). Join only on amfi_scheme_code to avoid false matches.
		// DISTINCT ON (scheme_code) ordered by nav_date DESC gives the latest NAV per scheme.
		query := `
			SELECT DISTINCT ON (nv.scheme_code)
				nv.scheme_code,
				nv.amc_name,
				nv.scheme_name,
				COALESCE(nv.isin_div_reinvestment, '') AS isin,
				nv.nav_value,
				TO_CHAR(nv.nav_date, 'YYYY-MM-DD') AS nav_date
			FROM investment.amfi_nav_staging nv
			LEFT JOIN investment.masterscheme ms
				ON ms.amfi_scheme_code = nv.scheme_code::text
				AND COALESCE(ms.is_deleted, false) = false
			WHERE ms.scheme_id IS NULL
			  AND nv.nav_date IS NOT NULL
			ORDER BY nv.scheme_code, nv.nav_date DESC
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, constants.ErrQueryFailed+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var schemeCode int64
			var amcName, schemeName, isin string
			var navValue *float64
			var navDate *string
			_ = rows.Scan(&schemeCode, &amcName, &schemeName, &isin, &navValue, &navDate)
			out = append(out, map[string]interface{}{
				"scheme_code": schemeCode,
				"amc_name":    amcName,
				"scheme_name": schemeName,
				"isin":        isin,
				"nav_value":   navValue,
				"nav_date":    navDate,
			})
		}

		api.RespondWithPayload(w, true, "", out)
	}
}

func GetDistinctAMCNamesFromAMFI(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		query := `
			WITH amfi_amcs AS (
    SELECT DISTINCT amc_name
    FROM investment.amfi_scheme_master_staging
    WHERE amc_name IS NOT NULL AND amc_name <> ''

    UNION

    SELECT DISTINCT amc_name
    FROM investment.amfi_nav_staging
    WHERE amc_name IS NOT NULL AND amc_name <> ''
)
SELECT a.amc_name
FROM amfi_amcs a
WHERE NOT EXISTS (
    SELECT 1
    FROM investment.masteramc m
    WHERE m.amc_name = a.amc_name
      AND COALESCE(m.is_deleted, false) = false
)
ORDER BY a.amc_name;

		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Failed to fetch AMC names: "+err.Error())
			return
		}
		defer rows.Close()

		out := []map[string]interface{}{}
		for rows.Next() {
			var amcName string
			if err := rows.Scan(&amcName); err != nil {
				continue
			}
			out = append(out, map[string]interface{}{
				"amc_name": amcName,
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
				ORDER BY a.amc_id, GREATEST(COALESCE(a.requested_at, '1970-01-01'::timestamp), COALESCE(a.checker_at, '1970-01-01'::timestamp)) DESC
			)
			SELECT m.amc_id, m.amc_name
			FROM investment.masteramc m
			JOIN latest_audit l ON l.amc_id = m.amc_id
			WHERE COALESCE(m.is_deleted, false) = false
			  AND UPPER(l.processing_status) = 'APPROVED'
			  AND UPPER(m.status) = 'ACTIVE'
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

		// scheme_nav_name is unique per plan type; join NAV on scheme_code and pick latest nav_date.
		schemeQuery := `
			WITH latest_nav AS (
				SELECT DISTINCT ON (scheme_code)
					scheme_code,
					nav_value,
					nav_date
				FROM investment.amfi_nav_staging
				WHERE nav_date IS NOT NULL
				ORDER BY scheme_code, nav_date DESC
			)
			SELECT
				sm.amc_name,
				sm.scheme_code,
				COALESCE(sm.scheme_nav_name, sm.scheme_name) AS scheme_name,
				COALESCE(sm.isin_div_reinvestment, '') AS isin,
				ln.nav_value,
				TO_CHAR(ln.nav_date, 'YYYY-MM-DD') AS nav_date
			FROM investment.amfi_scheme_master_staging sm
			LEFT JOIN latest_nav ln ON ln.scheme_code = sm.scheme_code
			WHERE sm.amc_name = ANY($1)
				AND NOT EXISTS (
					SELECT 1 FROM investment.masterscheme m2
					WHERE m2.amfi_scheme_code = sm.scheme_code::text
					  AND COALESCE(m2.is_deleted, false) = false
				)
			ORDER BY sm.scheme_nav_name;
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
			var navValue *float64
			var navDate *string
			if err := schemeRows.Scan(&amcName, &schemeCode, &schemeName, &isin, &navValue, &navDate); err != nil {
				continue
			}
			result = append(result, map[string]interface{}{
				"amc_name":    amcName,
				"scheme_code": schemeCode,
				"scheme_name": schemeName,
				"isin":        isin,
				"nav_value":   navValue,
				"nav_date":    navDate,
			})
		}

		if schemeRows.Err() != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Scheme scan error: "+schemeRows.Err().Error())
			return
		}

		api.RespondWithPayload(w, true, "", result)
	}
}
