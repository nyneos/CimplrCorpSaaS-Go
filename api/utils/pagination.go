package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PaginationParams struct {
	Page         int `json:"page"`
	Limit        int `json:"limit"`
	Offset       int `json:"offset"`
	TotalRecords int `json:"total_records"`
	TotalPages   int `json:"total_pages"`
}

// ListQuery is the WAVE-E pilot request shape for master list/all endpoints:
// limit (max 500), offset, search, sort (+ optional search_column / processing_status).
type ListQuery struct {
	Limit              int    `json:"limit"`
	Offset             int    `json:"offset"`
	Search             string `json:"search"`
	SearchColumn       string `json:"search_column"`
	Sort               string `json:"sort"`
	SortDir            string `json:"sort_dir"`
	ProcessingStatus   string `json:"processing_status"`
}

const paginationMaxLimit = 500

// DefaultListLimit is used when the caller omits limit (dropdown / legacy callers
// that expect a large page). Explicit All* screens send their own page size.
const DefaultListLimit = paginationMaxLimit

func ExtractPagination(r *http.Request) (PaginationParams, error) {
	params := PaginationParams{
		Page:  1,
		Limit: paginationMaxLimit,
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
		if val > paginationMaxLimit {
			val = paginationMaxLimit
		}
		params.Limit = val
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		val, err := strconv.Atoi(o)
		if err != nil || val < 0 {
			return PaginationParams{}, fmt.Errorf("invalid offset parameter: %s", o)
		}
		params.Offset = val
		if params.Limit > 0 {
			params.Page = (val / params.Limit) + 1
		}
		return params, nil
	}
	if params.Limit > 0 && params.Page > 1 && params.Page-1 > math.MaxInt/params.Limit {
		return PaginationParams{}, fmt.Errorf("invalid offset parameter")
	}
	params.Offset = (params.Page - 1) * params.Limit
	if params.Offset < 0 {
		return PaginationParams{}, fmt.Errorf("invalid offset parameter")
	}
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

func CountTotal(ctx context.Context, pool *pgxpool.Pool, query string, args ...interface{}) (int, error) {
	var total int
	err := pool.QueryRow(ctx, query, args...).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("count query failed: %w", err)
	}
	return total, nil
}

// NormalizeListQuery clamps limit (1..500), offset (>=0), and trims search/sort fields.
func NormalizeListQuery(q *ListQuery) {
	if q == nil {
		return
	}
	if q.Limit <= 0 {
		q.Limit = DefaultListLimit
	}
	if q.Limit > paginationMaxLimit {
		q.Limit = paginationMaxLimit
	}
	if q.Offset < 0 {
		q.Offset = 0
	}
	q.Search = strings.TrimSpace(q.Search)
	q.SearchColumn = strings.TrimSpace(q.SearchColumn)
	if strings.EqualFold(q.SearchColumn, "All") {
		q.SearchColumn = ""
	}
	q.Sort = strings.TrimSpace(q.Sort)
	q.SortDir = strings.ToLower(strings.TrimSpace(q.SortDir))
	if q.SortDir != "asc" && q.SortDir != "desc" {
		q.SortDir = "desc"
	}
	q.ProcessingStatus = strings.TrimSpace(q.ProcessingStatus)
	if strings.EqualFold(q.ProcessingStatus, "All") {
		q.ProcessingStatus = ""
	}
}

// SearchPattern returns an ILIKE pattern, or empty when q is blank.
func SearchPattern(q string) string {
	q = strings.TrimSpace(q)
	if q == "" {
		return ""
	}
	// Escape LIKE metacharacters so user input is literal.
	q = strings.ReplaceAll(q, `\`, `\\`)
	q = strings.ReplaceAll(q, `%`, `\%`)
	q = strings.ReplaceAll(q, `_`, `\_`)
	return "%" + q + "%"
}

// OrderBySQL builds a safe ORDER BY from an allow-list of sort keys → SQL expressions.
// Falls back to defaultOrder (must already include "ORDER BY ...") when sort is empty/unknown.
func OrderBySQL(sort, sortDir string, allowed map[string]string, defaultOrder string) string {
	expr, ok := allowed[sort]
	if !ok || expr == "" {
		return defaultOrder
	}
	dir := "DESC"
	if strings.EqualFold(sortDir, "asc") {
		dir = "ASC"
	}
	return "ORDER BY " + expr + " " + dir
}

// ListQueryFromMap copies known list keys from a decoded JSON object.
func ListQueryFromMap(m map[string]interface{}) ListQuery {
	var q ListQuery
	if m == nil {
		return q
	}
	q.Limit = intFromAny(m["limit"])
	q.Offset = intFromAny(m["offset"])
	q.Search = stringFromAny(m["search"])
	q.SearchColumn = stringFromAny(m["search_column"])
	q.Sort = stringFromAny(m["sort"])
	q.SortDir = stringFromAny(m["sort_dir"])
	q.ProcessingStatus = stringFromAny(m["processing_status"])
	return q
}

// DecodeListQueryBody reads an optional JSON body into dest (any struct) and a ListQuery.
// Empty body is allowed. Extra list fields may live on the same object as domain fields.
func DecodeListQueryBody(r *http.Request, dest interface{}) (ListQuery, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return ListQuery{}, err
	}
	_ = r.Body.Close()
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" || trimmed == "null" {
		q := ListQuery{}
		NormalizeListQuery(&q)
		return q, nil
	}
	if dest != nil {
		if err := json.Unmarshal(body, dest); err != nil {
			return ListQuery{}, err
		}
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return ListQuery{}, err
	}
	q := ListQueryFromMap(raw)
	NormalizeListQuery(&q)
	return q, nil
}

// ListQueryFromRequest merges query-string list params (for GET-style masters) over defaults.
func ListQueryFromRequest(r *http.Request) ListQuery {
	q := ListQuery{}
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			q.Limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			q.Offset = n
		}
	}
	q.Search = r.URL.Query().Get("search")
	q.SearchColumn = r.URL.Query().Get("search_column")
	q.Sort = r.URL.Query().Get("sort")
	q.SortDir = r.URL.Query().Get("sort_dir")
	q.ProcessingStatus = r.URL.Query().Get("processing_status")
	NormalizeListQuery(&q)
	return q
}

// MergeListQuery prefers body values when set; otherwise keeps query values.
func MergeListQuery(body, query ListQuery) ListQuery {
	out := query
	if body.Limit > 0 && body.Limit != DefaultListLimit {
		out.Limit = body.Limit
	} else if body.Limit > 0 {
		out.Limit = body.Limit
	}
	// Always take explicit body fields when the body was decoded (Normalize already ran).
	if body.Offset > 0 || body.Search != "" || body.Sort != "" || body.SearchColumn != "" || body.ProcessingStatus != "" || body.Limit > 0 {
		if body.Limit > 0 {
			out.Limit = body.Limit
		}
		out.Offset = body.Offset
		if body.Search != "" {
			out.Search = body.Search
		}
		if body.SearchColumn != "" {
			out.SearchColumn = body.SearchColumn
		}
		if body.Sort != "" {
			out.Sort = body.Sort
			out.SortDir = body.SortDir
		}
		if body.ProcessingStatus != "" {
			out.ProcessingStatus = body.ProcessingStatus
		}
	}
	NormalizeListQuery(&out)
	return out
}

func intFromAny(v interface{}) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	case json.Number:
		n, _ := t.Int64()
		return int(n)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(t))
		return n
	default:
		return 0
	}
}

func stringFromAny(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	default:
		return ""
	}
}
