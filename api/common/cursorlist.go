package common

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
)

const (
	DefaultLimit = 25
	MaxLimit     = 100
)

// ListRequest is the shared JSON body for server-side cursor lists.
type ListRequest struct {
	UserID       string `json:"user_id"`
	Limit        int    `json:"limit"`
	Cursor       string `json:"cursor"`
	Search       string `json:"search"`
	SearchColumn string `json:"search_column"`
	SortBy       string `json:"sort_by"`
	SortDir      string `json:"sort_dir"` // asc | desc
	Status       string `json:"status"`
}

// CursorPayload is opaque to clients; encoded as base64 JSON.
type CursorPayload struct {
	V  string `json:"v"`  // sort value as string
	ID string `json:"id"` // stable tie-breaker id
}

// ListPage is the standard data envelope for cursor pages.
type ListPage struct {
	Rows       interface{} `json:"rows"`
	NextCursor string      `json:"next_cursor,omitempty"`
	HasMore    bool        `json:"has_more"`
	Limit      int         `json:"limit"`
	TotalCount *int64      `json:"total_count,omitempty"`
}

func NormalizeLimit(n int) int {
	if n <= 0 {
		return DefaultLimit
	}
	if n > MaxLimit {
		return MaxLimit
	}
	return n
}

func NormalizeSortDir(dir string) string {
	if strings.EqualFold(strings.TrimSpace(dir), "asc") {
		return "ASC"
	}
	return "DESC"
}

func EncodeCursor(v, id string) string {
	b, err := json.Marshal(CursorPayload{V: v, ID: id})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func DecodeCursor(raw string) (CursorPayload, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return CursorPayload{}, false
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return CursorPayload{}, false
	}
	var c CursorPayload
	if err := json.Unmarshal(b, &c); err != nil || strings.TrimSpace(c.ID) == "" {
		return CursorPayload{}, false
	}
	return c, true
}

// KeysetSQL returns "(sort_col, id_col) < ($n, $n+1)" or ">" depending on dir.
// For DESC pages we use "<" (walk downward); for ASC we use ">".
func KeysetSQL(sortCol, idCol, dir string, argStart int) (clause string, nextArg int) {
	op := "<"
	if strings.EqualFold(dir, "ASC") {
		op = ">"
	}
	clause = "(" + sortCol + ", " + idCol + ") " + op + " ($" + strconv.Itoa(argStart) + ", $" + strconv.Itoa(argStart+1) + ")"
	return clause, argStart + 2
}

// WhitelistSort picks sortCol SQL expression from allowed map; falls back to def.
func WhitelistSort(sortBy string, allowed map[string]string, def string) string {
	if expr, ok := allowed[strings.TrimSpace(sortBy)]; ok {
		return expr
	}
	if expr, ok := allowed[def]; ok {
		return expr
	}
	return def
}
