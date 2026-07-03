package amfisync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"CimplrCorpSaas/api/constants"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SchemeData represents the scheme information returned from the database
type SchemeData struct {
	ID            int64    `json:"id"`
	SchemeCode    int64    `json:"schemeCode"`
	SchemeName    string   `json:"schemeName"`    // official name from amfi_scheme_master_staging
	SchemeNavName string   `json:"schemeNavName"` // scheme_nav_name from master (matches nav table scheme_name)
	AMCName       string   `json:"amcName"`
	Category      string   `json:"category"`
	ISIN          string   `json:"isin"`
	LatestNAV     *float64 `json:"latestNav"`
	LatestNAVDate string   `json:"latestNavDate"`
	PrevNAV       *float64 `json:"prevNav"`
	PrevNAVDate   string   `json:"prevNavDate"`
}

// SchemeDataRequest represents the request body for filtering schemes
type SchemeDataRequest struct {
	AMCNames    []string `json:"amcNames,omitempty"`
	Categories  []string `json:"categories,omitempty"`
	SchemeCodes []int64  `json:"schemeCodes,omitempty"`
	SearchText  string   `json:"searchText,omitempty"`
	Limit       int      `json:"limit,omitempty"`
	Offset      int      `json:"offset,omitempty"`
}

// SchemeDataResponse represents the API response
type SchemeDataResponse struct {
	Success bool         `json:"success"`
	Data    []SchemeData `json:"data"`
	Total   int          `json:"total"`
	Message string       `json:"message,omitempty"`
}

// GetSchemeDataHandler retrieves scheme data from amfi_scheme_master_staging joined with
// amfi_nav_staging. Returns the two most recent NAV dates available for each scheme.
func GetSchemeDataHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, constants.ErrMethodNotAllowed, http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set(constants.ContentTypeText, constants.ContentTypeJSON)

		// Parse request body (allow empty body)
		var req SchemeDataRequest
		if r.Body != nil && r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				json.NewEncoder(w).Encode(SchemeDataResponse{
					Success: false,
					Data:    []SchemeData{},
					Message: fmt.Sprintf("Invalid request body: %v", err),
				})
				return
			}
		}

		if req.Limit == 0 {
			req.Limit = 100000
		}
		if req.Limit > 100000 {
			req.Limit = 100000
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// ── build WHERE clause for master staging filters ──────────────────────
		where := " WHERE 1=1"
		filterArgs := []interface{}{}
		argIdx := 1

		if len(req.AMCNames) > 0 {
			where += fmt.Sprintf(" AND sm.amc_name = ANY($%d)", argIdx)
			filterArgs = append(filterArgs, req.AMCNames)
			argIdx++
		}
		if len(req.Categories) > 0 {
			where += fmt.Sprintf(" AND sm.scheme_category = ANY($%d)", argIdx)
			filterArgs = append(filterArgs, req.Categories)
			argIdx++
		}
		if len(req.SchemeCodes) > 0 {
			where += fmt.Sprintf(" AND sm.scheme_code = ANY($%d)", argIdx)
			filterArgs = append(filterArgs, req.SchemeCodes)
			argIdx++
		}
		if req.SearchText != "" {
			where += fmt.Sprintf(
				" AND (sm.scheme_name ILIKE $%d OR sm.amc_name ILIKE $%d OR sm.scheme_nav_name ILIKE $%d)",
				argIdx, argIdx, argIdx,
			)
			filterArgs = append(filterArgs, "%"+req.SearchText+"%")
			argIdx++
		}

		// ── count: hit master staging only — no NAV join needed ────────────────
		// This avoids running the expensive window-function CTE twice.
		countSQL := "SELECT COUNT(*) FROM investment.amfi_scheme_master_staging sm" + where
		var total int
		if err := pool.QueryRow(ctx, countSQL, filterArgs...).Scan(&total); err != nil {
			json.NewEncoder(w).Encode(SchemeDataResponse{
				Success: false,
				Data:    []SchemeData{},
				Message: fmt.Sprintf("Failed to get count: %v", err),
			})
			return
		}

		// ── data query: single-pass nav aggregation (rn≤2) ────────────────────
		// The inner sub-query is pruned to at most 2 rows per scheme_code, then
		// pivoted with conditional MAX so the join is cheap.
		dataArgs := append(filterArgs, req.Limit, req.Offset)
		limitArg := argIdx
		offsetArg := argIdx + 1

		dataSQL := fmt.Sprintf(`
WITH nav_agg AS (
    SELECT
        scheme_code,
        MAX(CASE WHEN rn = 1 THEN nav_value  END) AS latest_nav,
        MAX(CASE WHEN rn = 1 THEN nav_date_s END) AS latest_nav_date,
        MAX(CASE WHEN rn = 2 THEN nav_value  END) AS prev_nav,
        MAX(CASE WHEN rn = 2 THEN nav_date_s END) AS prev_nav_date
    FROM (
        SELECT
            scheme_code,
            nav_value,
            TO_CHAR(nav_date, 'YYYY-MM-DD') AS nav_date_s,
            ROW_NUMBER() OVER (PARTITION BY scheme_code ORDER BY nav_date DESC) AS rn
        FROM investment.amfi_nav_staging
    ) ranked
    WHERE rn <= 2
    GROUP BY scheme_code
)
SELECT
    sm.id,
    sm.scheme_code,
    sm.scheme_name,
    COALESCE(sm.scheme_nav_name, '')                                   AS scheme_nav_name,
    sm.amc_name,
    COALESCE(sm.scheme_category, '')                                   AS category,
    COALESCE(NULLIF(sm.isin_div_payout_growth,''), NULLIF(sm.isin_div_reinvestment,''), '') AS isin,
    na.latest_nav,
    COALESCE(na.latest_nav_date, '')                                   AS latest_nav_date,
    na.prev_nav,
    COALESCE(na.prev_nav_date, '')                                     AS prev_nav_date
FROM investment.amfi_scheme_master_staging sm
LEFT JOIN nav_agg na ON na.scheme_code = sm.scheme_code
%s
ORDER BY sm.scheme_name
LIMIT $%d OFFSET $%d`, where, limitArg, offsetArg)

		rows, err := pool.Query(ctx, dataSQL, dataArgs...)
		if err != nil {
			json.NewEncoder(w).Encode(SchemeDataResponse{
				Success: false,
				Data:    []SchemeData{},
				Message: fmt.Sprintf("Database query failed: %v", err),
			})
			return
		}
		defer rows.Close()

		schemes := []SchemeData{}
		for rows.Next() {
			var s SchemeData
			if err := rows.Scan(
				&s.ID, &s.SchemeCode, &s.SchemeName, &s.SchemeNavName,
				&s.AMCName, &s.Category, &s.ISIN,
				&s.LatestNAV, &s.LatestNAVDate,
				&s.PrevNAV, &s.PrevNAVDate,
			); err != nil {
				continue
			}
			schemes = append(schemes, s)
		}

		json.NewEncoder(w).Encode(SchemeDataResponse{
			Success: true,
			Data:    schemes,
			Total:   total,
			Message: "Success",
		})
	}
}
