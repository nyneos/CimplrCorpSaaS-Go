package utils

import (
	"database/sql"
	"fmt"
	"math"
	"net/http"
	"strconv"
)

type PaginationParams struct {
	Page         int `json:"page"`
	Limit        int `json:"limit"`
	Offset       int `json:"offset"`
	TotalRecords int `json:"total_records"`
	TotalPages   int `json:"total_pages"`
}

func ExtractPagination(r *http.Request) (PaginationParams, error) {
	params := PaginationParams{
		Page:  1,
		Limit: 10,
	}

	if p := r.URL.Query().Get("page"); p != "" {
		val, err := strconv.Atoi(p)
		if err != nil || val <= 0 {
			return PaginationParams{}, fmt.Errorf("invalid page parameter: %s", p)
		}
		params.Page = val
	}
	if l := r.URL.Query().Get("limit"); l != "" {
		val, err := strconv.Atoi(l)
		if err != nil || val <= 0 {
			return PaginationParams{}, fmt.Errorf("invalid limit parameter: %s", l)
		}
		params.Limit = val
	}
	params.Offset = (params.Page - 1) * params.Limit
	return params, nil
}

func (p *PaginationParams) SetPaginationStats(totalRecords int) {
	p.TotalRecords = totalRecords
	if totalRecords > 0 {
		p.TotalPages = int(math.Ceil(float64(totalRecords) / float64(p.Limit)))
	} else {
		p.TotalPages = 0
	}
}

func CountTotal(db *sql.DB, query string, args ...interface{}) (int, error) {
	var total int
	err := db.QueryRow(query, args...).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("count query failed: %w", err)
	}
	return total, nil
}

// change
