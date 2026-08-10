package dmsjobs

import (
	"context"
	"fmt"
	htmlpkg "html"
	"regexp"
	"strconv"
	"strings"

	dashboardbuilder "CimplrCorpSaas/api/dash/dashboardBuilder"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Generic data table placeholder:
//
//	<div data-dms-table
//	     data-source="pool"                         <!-- pool = rule's filtered rows; or a dash source key -->
//	     data-columns="booking_id,principal_amount,bank_name"
//	     data-labels="Booking ID,Amount,Bank Name"  <!-- optional; defaults to column keys -->
//	     data-parent-field="booking_id"             <!-- optional: fetch child source by parent id from merge row -->
//	     data-limit="500"></div>
var dataTableDivRe = regexp.MustCompile(`(?is)<div\b([^>]*)\bdata-dms-table(?:=["']([^"']*)["'])?([^>]*)>.*?</div>`)

func attrFromBlob(attrs, name string) string {
	re := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(name) + `=["']([^"']*)["']`)
	if m := re.FindStringSubmatch(attrs); len(m) >= 2 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// expandDataTablePlaceholders replaces every data-dms-table div with a real
// HTML table. Prefer genCtx.PoolRows when source is empty/"pool"/matches the
// rule source; otherwise fetch from the named dashboard source (optionally
// scoped by parent field on the current merge row).
func expandDataTablePlaceholders(
	ctx context.Context,
	pool *pgxpool.Pool,
	html string,
	headerRow map[string]any,
	genCtx attachmentGenCtx,
	format string,
) (string, error) {
	if !strings.Contains(strings.ToLower(html), "data-dms-table") {
		return html, nil
	}
	var expandErr error
	out := dataTableDivRe.ReplaceAllStringFunc(html, func(match string) string {
		if expandErr != nil {
			return match
		}
		sub := dataTableDivRe.FindStringSubmatch(match)
		attrs := ""
		if len(sub) >= 4 {
			attrs = strings.TrimSpace(sub[1] + " " + sub[2] + " " + sub[3])
		}
		source := strings.TrimSpace(attrFromBlob(attrs, "data-source"))
		if source == "" {
			source = strings.TrimSpace(sub[2])
		}
		if source == "" {
			source = "pool"
		}
		columns := splitCSV(attrFromBlob(attrs, "data-columns"))
		labels := splitCSV(attrFromBlob(attrs, "data-labels"))
		parentField := strings.TrimSpace(attrFromBlob(attrs, "data-parent-field"))
		limit := defaultTxnLimit(format)
		if n, err := strconv.Atoi(attrFromBlob(attrs, "data-limit")); err == nil && n > 0 {
			limit = n
		}

		var rows []map[string]any
		usePool := strings.EqualFold(source, "pool") ||
			(genCtx.SourceKey != "" && strings.EqualFold(source, genCtx.SourceKey))
		if usePool && parentField == "" {
			rows = genCtx.PoolRows
			if limit > 0 && len(rows) > limit {
				rows = rows[:limit]
			}
		} else {
			parentID := ""
			if parentField != "" && headerRow != nil {
				parentID = strings.TrimSpace(formatFieldValue(headerRow[parentField]))
			} else if headerRow != nil {
				// Legacy bank-statement convenience when source looks like transactions.
				parentID = strings.TrimSpace(formatFieldValue(headerRow["bank_statement_id"]))
				if parentID == "" {
					parentID = strings.TrimSpace(formatFieldValue(headerRow["statement_id"]))
				}
			}
			fetchSource := source
			if strings.EqualFold(fetchSource, "pool") {
				fetchSource = genCtx.SourceKey
			}
			if fetchSource == "" {
				expandErr = fmt.Errorf("data-dms-table has no resolvable data-source")
				return match
			}
			req := genCtx.poolDataRequest(fetchSource)
			req.ParentID = parentID
			if limit > 0 {
				// Template data-limit caps rows within the rule's configured window.
				if limit < req.Limit {
					req.Limit = limit
				}
			}
			fetched, _, err := dashboardbuilder.FetchSourceData(ctx, pool, req)
			if err != nil {
				expandErr = err
				return match
			}
			rows = fetched
		}

		if len(columns) == 0 && len(rows) > 0 {
			columns = inferColumns(rows[0])
		}
		if len(labels) == 0 {
			labels = make([]string, len(columns))
			for i, c := range columns {
				labels[i] = humanizeKey(c)
			}
		}
		for len(labels) < len(columns) {
			labels = append(labels, humanizeKey(columns[len(labels)]))
		}
		return buildGenericHTMLTable(labels, columns, rows, limit)
	})
	return out, expandErr
}

func inferColumns(row map[string]any) []string {
	preferred := []string{
		"booking_id", "id", "entity_name", "bank_name", "principal_amount",
		"amount", "interest_rate", "booking_status", "value_date",
		"account_number", "description", "transaction_date",
	}
	out := make([]string, 0, 8)
	seen := map[string]struct{}{}
	for _, k := range preferred {
		if _, ok := row[k]; ok {
			out = append(out, k)
			seen[k] = struct{}{}
		}
	}
	if len(out) >= 3 {
		return out
	}
	for k := range row {
		if _, ok := seen[k]; ok {
			continue
		}
		out = append(out, k)
		if len(out) >= 8 {
			break
		}
	}
	return out
}

func humanizeKey(key string) string {
	parts := strings.Split(key, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

func buildGenericHTMLTable(headers, columns []string, rows []map[string]any, limit int) string {
	var b strings.Builder
	if len(rows) == 0 {
		b.WriteString(`<p><em>No rows matched for this table.</em></p>`)
		return b.String()
	}
	if limit > 0 && len(rows) >= limit {
		b.WriteString(fmt.Sprintf(`<p><em>Showing first %d rows.</em></p>`, limit))
	}
	b.WriteString(`<table class="dms-table"><thead><tr>`)
	for _, h := range headers {
		b.WriteString("<th><p>")
		b.WriteString(htmlpkg.EscapeString(h))
		b.WriteString("</p></th>")
	}
	b.WriteString(`</tr></thead><tbody>`)
	for _, r := range rows {
		b.WriteString("<tr>")
		for _, col := range columns {
			b.WriteString("<td><p>")
			b.WriteString(htmlpkg.EscapeString(formatFieldValue(r[col])))
			b.WriteString("</p></td>")
		}
		b.WriteString("</tr>")
	}
	b.WriteString(`</tbody></table>`)
	return b.String()
}
