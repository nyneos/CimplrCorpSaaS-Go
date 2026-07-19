package common

import "strings"

// PageRequest is the standard POST body for paginated policy-engine list endpoints.
type PageRequest struct {
	Page     int    `json:"page"`
	PageSize int    `json:"page_size"`
	Search   string `json:"search"`
}

// NormalizePage clamps page/page_size and returns SQL LIMIT/OFFSET values.
func NormalizePage(req PageRequest) (page, pageSize, offset int) {
	page = req.Page
	if page < 1 {
		page = 1
	}
	pageSize = req.PageSize
	if pageSize < 1 {
		pageSize = 15
	}
	if pageSize > 200 {
		pageSize = 200
	}
	offset = (page - 1) * pageSize
	return page, pageSize, offset
}

func SearchPattern(q string) string {
	q = strings.TrimSpace(q)
	if q == "" {
		return ""
	}
	return "%" + q + "%"
}
