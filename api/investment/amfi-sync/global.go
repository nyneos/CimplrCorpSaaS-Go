package amfisync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SchemeData represents the scheme information returned from the database
type SchemeData struct {
	ID         int64  `json:"id"`
	SchemeCode int64  `json:"schemeCode"`
	SchemeName string `json:"schemeName"`
	AMCName    string `json:"amcName"`
	Category   string `json:"category"`
	ISIN       string `json:"isin"`
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

// GetSchemeDataHandler retrieves scheme data from amfi_scheme_master_staging
func GetSchemeDataHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		// Parse request body (allow empty body)
		var req SchemeDataRequest
		if r.Body != nil && r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				response := SchemeDataResponse{
					Success: false,
					Data:    []SchemeData{},
					Message: fmt.Sprintf("Invalid request body: %v", err),
				}
				json.NewEncoder(w).Encode(response)
				return
			}
		}

		// Set default pagination
		if req.Limit == 0 {
			req.Limit = 100000
		}
		if req.Limit > 100000 {
			req.Limit = 100000 // Max limit
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Build dynamic query
		query := `
			SELECT id, scheme_code, scheme_name, amc_name, 
				   COALESCE(scheme_category, '') as category,
				   COALESCE(
					   NULLIF(isin_div_payout_growth, ''),
					   NULLIF(isin_div_reinvestment, ''),
					   ''
				   ) as isin
			FROM investment.amfi_scheme_master_staging
			WHERE 1=1`

		args := []interface{}{}
		argCount := 1

		// Add filters
		if len(req.AMCNames) > 0 {
			query += fmt.Sprintf(" AND amc_name = ANY($%d)", argCount)
			args = append(args, req.AMCNames)
			argCount++
		}

		if len(req.Categories) > 0 {
			query += fmt.Sprintf(" AND scheme_category = ANY($%d)", argCount)
			args = append(args, req.Categories)
			argCount++
		}

		if len(req.SchemeCodes) > 0 {
			query += fmt.Sprintf(" AND scheme_code = ANY($%d)", argCount)
			args = append(args, req.SchemeCodes)
			argCount++
		}

		if req.SearchText != "" {
			query += fmt.Sprintf(" AND (scheme_name ILIKE $%d OR amc_name ILIKE $%d)", argCount, argCount)
			args = append(args, "%"+req.SearchText+"%")
			argCount++
		}

		// Get total count
		countQuery := "SELECT COUNT(*) FROM (" + query + ") as count_query"
		var total int
		err := pool.QueryRow(ctx, countQuery, args...).Scan(&total)
		if err != nil {
			response := SchemeDataResponse{
				Success: false,
				Data:    []SchemeData{},
				Message: fmt.Sprintf("Failed to get count: %v", err),
			}
			json.NewEncoder(w).Encode(response)
			return
		}

		// Add pagination
		query += fmt.Sprintf(" ORDER BY scheme_name LIMIT $%d OFFSET $%d", argCount, argCount+1)
		args = append(args, req.Limit, req.Offset)

		// Execute query
		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			response := SchemeDataResponse{
				Success: false,
				Data:    []SchemeData{},
				Message: fmt.Sprintf("Database query failed: %v", err),
			}
			json.NewEncoder(w).Encode(response)
			return
		}
		defer rows.Close()

		// Parse results
		schemes := []SchemeData{}
		for rows.Next() {
			var scheme SchemeData
			err := rows.Scan(&scheme.ID, &scheme.SchemeCode, &scheme.SchemeName,
				&scheme.AMCName, &scheme.Category, &scheme.ISIN)
			if err != nil {
				continue
			}
			schemes = append(schemes, scheme)
		}

		// Return response
		response := SchemeDataResponse{
			Success: true,
			Data:    schemes,
			Total:   total,
			Message: "Success",
		}
		json.NewEncoder(w).Encode(response)
	}
}
