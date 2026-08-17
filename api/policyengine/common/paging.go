package common

import "strings"

// PageRequest is the standard POST body for paginated policy-engine list endpoints.
type PageRequest struct {
	Page     int    `json:"page"`
	PageSize int    `json:"page_size"`
	Search   string `json:"search"`
	Outcome  string `json:"outcome"`
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

// NormalizeOutcomeFilter maps UI labels onto execution_run.outcome values.
// "Compliant" in the UI is stored as PASS.
func NormalizeOutcomeFilter(q string) string {
	switch strings.ToUpper(strings.TrimSpace(q)) {
	case "", "ALL":
		return ""
	case "COMPLIANT":
		return "PASS"
	default:
		return strings.ToUpper(strings.TrimSpace(q))
	}
}
