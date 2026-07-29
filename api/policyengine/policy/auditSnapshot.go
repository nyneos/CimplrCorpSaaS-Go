package policy

import (
	"encoding/json"
	"sort"
	"strings"
)

func joinSortedCSV(items []string) string {
	if len(items) == 0 {
		return ""
	}
	cp := append([]string(nil), items...)
	sort.Strings(cp)
	return strings.Join(cp, ", ")
}

func serializeSlabRows(rows []slabRowConfig) string {
	if len(rows) == 0 {
		return ""
	}
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		to := "∞"
		if r.To != nil {
			to = formatAuditNum(*r.To)
		}
		parts = append(parts, strings.Join([]string{
			formatAuditNum(r.From),
			to,
			strings.TrimSpace(r.Mode),
			strings.TrimSpace(r.Action),
			strings.TrimSpace(r.ApprovalRef),
			strings.TrimSpace(r.Label),
		}, "|"))
	}
	return strings.Join(parts, "; ")
}

func serializeCompBuckets(buckets []compositionBucketConfig) string {
	if len(buckets) == 0 {
		return ""
	}
	parts := make([]string, 0, len(buckets))
	for _, b := range buckets {
		min := "-"
		if b.Min != nil {
			min = formatAuditNum(*b.Min)
		}
		max := "-"
		if b.Max != nil {
			max = formatAuditNum(*b.Max)
		}
		parts = append(parts, strings.Join([]string{
			strings.TrimSpace(b.Label),
			strings.TrimSpace(b.Variable),
			min,
			max,
		}, "|"))
	}
	return strings.Join(parts, "; ")
}

func formatAuditNum(v float64) string {
	b, _ := json.Marshal(v)
	return strings.Trim(string(b), `"`)
}
