package dashboardbuilder

// Per-widget filter / sort / row-limit push-down.
//
// The dashboard *view* page ships each widget's configured filters (WidgetConfig
// .filters), sort, and row limit down to /dash/builder/data so the database
// returns only the rows the widget renders — instead of the frontend fetching
// the whole source (up to the page cap) and filtering in the browser.
//
// Every source query template is uniform: it ends in
//
//	LIMIT NULLIF($1, 0) OFFSET $2
//
// with $1/$2 being the only placeholders reserved for limit/offset. runSourceQuery
// wraps that template as a CTE and re-applies filter/sort/limit on the CTE's output
// columns (which match the frontend field keys 1:1), so no per-source SQL had to be
// rewritten by hand.
//
// Safety contract: pushed-down predicates must never drop a row the client would
// keep. Translation is therefore all-or-nothing — if any provided filter rule cannot
// be faithfully translated, nothing is pushed and the original template runs
// unchanged (the client keeps doing the full filter/sort/limit). The frontend only
// sends push-down for join-free, unscoped widgets whose rules are all translatable.

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WidgetFilterRule mirrors the frontend FilterRule. Type is supplied by the
// frontend (from dataSourceFields) so comparisons can be cast correctly without
// maintaining a per-source column registry on the backend.
type WidgetFilterRule struct {
	Field       string `json:"field"`
	Type        string `json:"type"` // text | id | numeric | date | boolean
	Op          string `json:"op"`
	Value       string `json:"value"`
	Value2      string `json:"value2"`
	Conjunction string `json:"conjunction"` // AND | OR (applied before this rule)
}

const (
	ctxKeyReqWidgetFilters = "reqWidgetFilters"
	ctxKeyReqSortField     = "reqSortField"
	ctxKeyReqSortDir       = "reqSortDir"
	ctxKeyReqSortType      = "reqSortType"
	ctxKeyReqRowLimit      = "reqRowLimit"
	ctxKeyReqLimit         = "reqLimit"
	ctxKeyReqOffset        = "reqOffset"
	ctxKeyReqEnforceWindow = "reqEnforceWindow"
)


var windowDateCandidates = []string{
	"as_of_date",
	"value_date",
	"transaction_date",
	"statement_date",
	"run_date",
	"booking_date",
	"deal_date",
	"invoice_date",
	"payment_date",
	"actual_payout_date",
	"requested_closure_date",
	"actual_start_date",
	"start_date",
	"projection_date",
	"effective_date",
	"due_date",
	"entry_date",
	"created_at",
}

var (
	// Trailing "LIMIT NULLIF($1, 0) OFFSET $2" every source template applies.
	baseLimitOffsetRe = regexp.MustCompile(`(?is)\s+LIMIT\s+NULLIF\(\$1,\s*0\)\s+OFFSET\s+\$2\s*$`)
	// Positional placeholder, for renumbering after limit/offset args are dropped.
	placeholderRe = regexp.MustCompile(`\$(\d+)`)
	// Whitelisted output-column identifier. Join-namespaced fields carry a "." and
	// are never sent for push-down; anything not matching is rejected (all-or-nothing).
	safeColumnRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
)

// runSourceQuery executes a source template, wrapping it to push widget filters,
// sort, and row-limit into SQL when the request carries any and they all translate.
// baseQ/baseArgs are the untouched template and args (args[0]=limit, args[1]=offset).
func runSourceQuery(ctx context.Context, pool *pgxpool.Pool, baseQ string, baseArgs []any) ([]map[string]any, error) {
	filters, _ := ctx.Value(ctxKeyReqWidgetFilters).([]WidgetFilterRule)
	sortField, _ := ctx.Value(ctxKeyReqSortField).(string)
	sortDir, _ := ctx.Value(ctxKeyReqSortDir).(string)
	sortType, _ := ctx.Value(ctxKeyReqSortType).(string)
	rowLimit, _ := ctx.Value(ctxKeyReqRowLimit).(int)
	reqLimit, _ := ctx.Value(ctxKeyReqLimit).(int)
	reqOffset, _ := ctx.Value(ctxKeyReqOffset).(int)

	hasFilters := len(filters) > 0
	hasSort := strings.TrimSpace(sortField) != ""
	hasRowLimit := rowLimit > 0
	loc := baseLimitOffsetRe.FindStringIndex(baseQ)

	// A row of base-CTE args survives (limit/offset excluded); filter placeholders
	// continue after them. Translate once, up front: an untranslatable rule disables
	// the whole push-down (never partially — a partial push can over-filter OR groups).
	baseArgCount := len(baseArgs) - 2
	filterClause, filterArgs, filtersOK := buildWidgetFilterSQL(filters, baseArgCount)
	windowClause, windowArgs := buildWindowSQL(ctx, baseArgCount+len(filterArgs))
	hasWindow := windowClause != ""

	pushdown := loc != nil && baseArgCount >= 0 &&
		(hasFilters || hasSort || hasRowLimit || hasWindow) &&
		(!hasFilters || filtersOK)

	if !pushdown {
		r, err := pool.Query(ctx, baseQ, baseArgs...)
		if err != nil {
			return nil, err
		}
		return scanRows(r)
	}

	// Strip the inner LIMIT/OFFSET and its two leading args, then shift every
	// remaining placeholder down by two so the base CTE is self-contained.
	inner := placeholderRe.ReplaceAllStringFunc(baseQ[:loc[0]], func(m string) string {
		n, _ := strconv.Atoi(m[1:])
		return "$" + strconv.Itoa(n-2)
	})
	args := append([]any{}, baseArgs[2:]...)

	// Effective window: a widget row-limit only ever narrows the page limit.
	limit := reqLimit
	if rowLimit > 0 && (limit <= 0 || rowLimit < limit) {
		limit = rowLimit
	}

	var b strings.Builder
	b.WriteString("WITH base AS (")
	b.WriteString(inner)
	b.WriteString(") SELECT * FROM base WHERE 1=1")

	if filterClause != "" {
		b.WriteString(filterClause)
		args = append(args, filterArgs...)
	}

	if windowClause != "" {
		b.WriteString(windowClause)
		args = append(args, windowArgs...)
	}

	if order := buildSortSQL(sortField, sortDir, sortType); order != "" {
		b.WriteString(order)
	}

	if limit > 0 {
		args = append(args, limit)
		b.WriteString(fmt.Sprintf(" LIMIT $%d", len(args)))
	}
	args = append(args, reqOffset)
	b.WriteString(fmt.Sprintf(" OFFSET $%d", len(args)))

	r, err := pool.Query(ctx, b.String(), args...)
	if err != nil {
		// Push-down is best-effort: a filter/sort on a column the base template
		// does not actually output (schema drift, or a field added only by
		// post-query enrichment) would error here. Fall back to the untouched
		// template so the client still gets a superset to filter — never worse
		// than the pre-push-down behaviour.
		logger.LogError("dashboard-builder push-down fell back to base query: %v", err)
		fr, ferr := pool.Query(ctx, baseQ, baseArgs...)
		if ferr != nil {
			return nil, ferr
		}
		return scanRows(fr)
	}
	return scanRows(r)
}

// buildWindowSQL returns an " AND (...)" clause pinning the CTE output to the
// request's as_of_date/as_on_date band, or "" when the request did not opt in.
// The row's date is read out of to_jsonb(base) so no per-source column registry
// is needed: a key the source does not output resolves to NULL rather than
// erroring, and only ISO-prefixed values are cast.
func buildWindowSQL(ctx context.Context, startArgs int) (string, []any) {
	if enforce, _ := ctx.Value(ctxKeyReqEnforceWindow).(bool); !enforce {
		return "", nil
	}
	asOf, _ := ctx.Value(ctxKeyReqAsOfDate).(string)
	asOn, _ := ctx.Value(ctxKeyReqAsOnDate).(string)
	asOf = strings.TrimSpace(asOf)
	asOn = strings.TrimSpace(asOn)
	if asOf == "" && asOn == "" {
		return "", nil
	}
	parts := make([]string, 0, len(windowDateCandidates))
	for _, c := range windowDateCandidates {
		parts = append(parts, "NULLIF(to_jsonb(base) ->> '"+c+"', '')")
	}
	src := "COALESCE(" + strings.Join(parts, ", ") + ")"
	d := "(CASE WHEN " + src + ` ~ '^\d{4}-\d{2}-\d{2}' THEN LEFT(` + src + ", 10)::date END)"
	idx := startArgs + 1
	switch {
	case asOf != "" && asOn != "":
		return fmt.Sprintf(" AND (%s IS NULL OR %s BETWEEN $%d::date AND $%d::date)", d, d, idx, idx+1), []any{asOf, asOn}
	case asOf != "":
		return fmt.Sprintf(" AND (%s IS NULL OR %s >= $%d::date)", d, d, idx), []any{asOf}
	default:
		return fmt.Sprintf(" AND (%s IS NULL OR %s <= $%d::date)", d, d, idx), []any{asOn}
	}
}

// buildSortSQL returns an " ORDER BY ..." clause for a validated sort field, or "".
func buildSortSQL(field, dir, fieldType string) string {
	field = strings.TrimSpace(field)
	if field == "" || !safeColumnRe.MatchString(field) {
		return ""
	}
	direction := "ASC"
	if strings.EqualFold(strings.TrimSpace(dir), "desc") {
		direction = "DESC"
	}
	col := `base."` + field + `"`
	if fieldType == "numeric" {
		col = "NULLIF(" + col + "::text,'')::numeric"
	}
	return fmt.Sprintf(" ORDER BY %s %s NULLS LAST", col, direction)
}

// buildWidgetFilterSQL translates FilterRule[] into a parameterized predicate on the
// CTE output columns, starting placeholders at $(startArgs+1). Rules fold left-to-right
// with no operator precedence, matching the frontend engine (applyWidgetFiltersOnly):
// "a OR b AND c" evaluates as "((a OR b) AND c)".
//
// Returns ok=false if any non-empty rule cannot be faithfully translated; callers must
// then push nothing. An empty-valued rule is a no-op (the client treats it as "match
// all"), so it is skipped without failing translation.
func buildWidgetFilterSQL(filters []WidgetFilterRule, startArgs int) (clause string, args []any, ok bool) {
	expr := ""
	idx := startArgs + 1
	for _, f := range filters {
		field := strings.TrimSpace(f.Field)
		if field == "" {
			continue
		}
		if !safeColumnRe.MatchString(field) {
			return "", nil, false
		}
		if filterRuleIsEmpty(f) {
			continue // no-op rule: matches everything, contributes nothing
		}
		pred, pArgs, translated := buildFilterPredicate(field, f, idx)
		if !translated {
			return "", nil, false
		}
		conj := "AND"
		if expr != "" && strings.EqualFold(strings.TrimSpace(f.Conjunction), "OR") {
			conj = "OR"
		}
		if expr == "" {
			expr = pred
		} else {
			expr = "(" + expr + " " + conj + " " + pred + ")"
		}
		args = append(args, pArgs...)
		idx += len(pArgs)
	}
	if expr == "" {
		return "", nil, true
	}
	return " AND (" + expr + ")", args, true
}

func filterRuleIsEmpty(f WidgetFilterRule) bool {
	if f.Op == "between" {
		return strings.TrimSpace(f.Value) == "" || strings.TrimSpace(f.Value2) == ""
	}
	return strings.TrimSpace(f.Value) == ""
}

// buildFilterPredicate emits one SQL predicate for a rule. translated=false means the
// op/type combination is not supported and the whole push-down must be abandoned.
func buildFilterPredicate(field string, f WidgetFilterRule, idx int) (sql string, args []any, translated bool) {
	col := `base."` + field + `"`
	switch f.Type {
	case "numeric":
		return numericPredicate(col, f, idx)
	case "date":
		return datePredicate(col, f, idx)
	case "boolean":
		return booleanPredicate(col, f, idx)
	case "id":
		return idPredicate(col, f, idx)
	default: // text
		return textPredicate(col, f, idx)
	}
}

func numericPredicate(col string, f WidgetFilterRule, idx int) (string, []any, bool) {
	n := "NULLIF(" + col + "::text,'')::numeric"
	num := func(s string) (float64, bool) {
		v, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(s), ",", ""), 64)
		return v, err == nil
	}
	v1, ok1 := num(f.Value)
	switch f.Op {
	case "=", "!=", ">", "<", ">=", "<=":
		if !ok1 {
			return "", nil, false
		}
		return fmt.Sprintf("%s %s $%d::numeric", n, sqlCompareOp(f.Op), idx), []any{v1}, true
	case "between":
		v2, ok2 := num(f.Value2)
		if !ok1 || !ok2 {
			return "", nil, false
		}
		return fmt.Sprintf("%s BETWEEN $%d::numeric AND $%d::numeric", n, idx, idx+1), []any{v1, v2}, true
	}
	return "", nil, false
}

func datePredicate(col string, f WidgetFilterRule, idx int) (string, []any, bool) {
	d := col + "::date"
	iso := func(s string) (string, bool) {
		v := strings.TrimSpace(s)
		// Only full YYYY-MM-DD dates translate cleanly; partial dates (year/month
		// granularity handled by the client) fall back to client-side filtering.
		if regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`).MatchString(v) {
			return v, true
		}
		return "", false
	}
	v1, ok1 := iso(f.Value)
	switch f.Op {
	case "=", "!=", ">", "<", ">=", "<=":
		if !ok1 {
			return "", nil, false
		}
		return fmt.Sprintf("%s %s $%d::date", d, sqlCompareOp(f.Op), idx), []any{v1}, true
	case "between":
		v2, ok2 := iso(f.Value2)
		if !ok1 || !ok2 {
			return "", nil, false
		}
		return fmt.Sprintf("%s BETWEEN $%d::date AND $%d::date", d, idx, idx+1), []any{v1, v2}, true
	}
	return "", nil, false
}

func booleanPredicate(col string, f WidgetFilterRule, idx int) (string, []any, bool) {
	b := strings.EqualFold(strings.TrimSpace(f.Value), "true")
	switch f.Op {
	case "=":
		return fmt.Sprintf("%s::boolean = $%d::boolean", col, idx), []any{b}, true
	case "!=":
		return fmt.Sprintf("%s::boolean <> $%d::boolean", col, idx), []any{b}, true
	}
	return "", nil, false
}

// idPredicate mirrors the client's id handling: equality/membership are trimmed but
// not lower-cased; contains is case-insensitive.
func idPredicate(col string, f WidgetFilterRule, idx int) (string, []any, bool) {
	t := "TRIM(" + col + "::text)"
	lower := "LOWER(" + t + ")"
	val := strings.TrimSpace(f.Value)
	switch f.Op {
	case "=":
		return fmt.Sprintf("%s = $%d", t, idx), []any{val}, true
	case "!=":
		return fmt.Sprintf("%s <> $%d", t, idx), []any{val}, true
	case "contains":
		return fmt.Sprintf("POSITION($%d IN %s) > 0", idx, lower), []any{strings.ToLower(val)}, true
	case "not_contains":
		return fmt.Sprintf("POSITION($%d IN %s) = 0", idx, lower), []any{strings.ToLower(val)}, true
	case "in":
		return fmt.Sprintf("%s = ANY($%d::text[])", t, idx), []any{splitCSV(val, false)}, true
	case "not_in":
		return fmt.Sprintf("%s <> ALL($%d::text[])", t, idx), []any{splitCSV(val, false)}, true
	}
	return "", nil, false
}

// textPredicate mirrors the client's text handling: normalized to lower(trim(...)).
func textPredicate(col string, f WidgetFilterRule, idx int) (string, []any, bool) {
	t := "LOWER(TRIM(" + col + "::text))"
	val := strings.ToLower(strings.TrimSpace(f.Value))
	switch f.Op {
	case "=":
		return fmt.Sprintf("%s = $%d", t, idx), []any{val}, true
	case "!=":
		return fmt.Sprintf("%s <> $%d", t, idx), []any{val}, true
	case "contains":
		return fmt.Sprintf("POSITION($%d IN %s) > 0", idx, t), []any{val}, true
	case "not_contains":
		return fmt.Sprintf("POSITION($%d IN %s) = 0", idx, t), []any{val}, true
	case "in":
		return fmt.Sprintf("%s = ANY($%d::text[])", t, idx), []any{splitCSV(val, true)}, true
	case "not_in":
		return fmt.Sprintf("%s <> ALL($%d::text[])", t, idx), []any{splitCSV(val, true)}, true
	}
	return "", nil, false
}

func sqlCompareOp(op string) string {
	if op == "!=" {
		return "<>"
	}
	return op
}

// splitCSV splits a comma list into trimmed, non-empty values (optionally lower-cased).
func splitCSV(s string, lower bool) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v == "" {
			continue
		}
		if lower {
			v = strings.ToLower(v)
		}
		out = append(out, v)
	}
	return out
}
