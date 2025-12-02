package allMaster

import (
	"CimplrCorpSaas/api"
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

func GetAMFISchemeMasterSimple(pgxPool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		query := `
			SELECT DISTINCT ON (sm.scheme_code)
			sm.scheme_code,
			sm.amc_name,
			sm.scheme_name,
			COALESCE(sm.isin_div_reinvestment, '') AS isin
			FROM investment.amfi_scheme_master_staging sm
			LEFT JOIN investment.masterscheme ms
				ON (ms.amfi_scheme_code = sm.scheme_code::text 
					OR (ms.scheme_name = sm.scheme_name AND ms.amc_name = sm.amc_name))
			WHERE ms.scheme_id IS NULL OR COALESCE(ms.is_deleted, false) = true
			ORDER BY sm.scheme_code, sm.scheme_name;
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
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
		ctx := context.Background()

		query := `
			SELECT DISTINCT ON (nv.scheme_code)
				nv.scheme_code,
				nv.amc_name,
				nv.scheme_name,
				COALESCE(nv.isin_div_reinvestment, '') AS isin,
				nv.nav_value,
				nv.nav_date
			FROM investment.amfi_nav_staging nv
			LEFT JOIN investment.masterscheme ms
				ON (ms.amfi_scheme_code = nv.scheme_code::text 
					OR (ms.scheme_name = nv.scheme_name AND ms.amc_name = nv.amc_name))
			WHERE ms.scheme_id IS NULL OR COALESCE(ms.is_deleted, false) = true
			ORDER BY nv.scheme_code, nv.nav_date DESC
		`

		rows, err := pgxPool.Query(ctx, query)
		if err != nil {
			api.RespondWithError(w, http.StatusInternalServerError, "Query failed: "+err.Error())
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

		// Get distinct AMC names from both AMFI staging tables
		// Exclude AMCs that exist in masteramc and are deleted
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
			LEFT JOIN investment.masteramc m ON m.amc_name = a.amc_name
			WHERE m.amc_id IS NULL OR COALESCE(m.is_deleted, false) = true
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
				ORDER BY a.amc_id, a.requested_at DESC
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

		schemeQuery := `
			SELECT DISTINCT ON (sm.scheme_code)
				sm.amc_name,
				sm.scheme_code,
				sm.scheme_name,
				COALESCE(sm.isin_div_reinvestment, '') AS isin,
				nv.nav_value,
				nv.nav_date
			FROM investment.amfi_scheme_master_staging sm
			LEFT JOIN investment.amfi_nav_staging nv 
				ON nv.scheme_code = sm.scheme_code
			LEFT JOIN investment.masterscheme ms
				ON (ms.amfi_scheme_code = sm.scheme_code::text 
					OR (ms.scheme_name = sm.scheme_name AND ms.amc_name = sm.amc_name))
			WHERE sm.amc_name = ANY($1)
				AND (ms.scheme_id IS NULL OR COALESCE(ms.is_deleted, false) = true)
			ORDER BY sm.scheme_code, nv.nav_date DESC NULLS LAST;
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
