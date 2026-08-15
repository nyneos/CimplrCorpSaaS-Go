package dmsjobs

import (
	"strings"

	dashboardbuilder "CimplrCorpSaas/api/dash/dashboardBuilder"
)

const (
	defaultDataRowFrom = 1
	defaultDataRowTo   = 500
	maxDataRowTo       = 2000
)

// attachmentGenCtx is the rule-scoped pool shared by merge fields, data tables,
// charts, and KPIs. Every expander must respect Filters + window + row range —
// no silent unfiltered refetch of a named dash source.
type attachmentGenCtx struct {
	EntityIDs                []string
	BankAccountScope         []dashboardbuilder.BankAccountScopePair
	AllowUnscopedBankAccount bool
	PoolRows                 []map[string]any
	SourceKey                string
	Filters                  []dashboardbuilder.WidgetFilterRule
	AsOfDate                 string
	AsOnDate                 string
	DataRowFrom              int // 1-based inclusive
	DataRowTo                int // 1-based inclusive
	RuleVersionID            string

	FieldAliases map[string]string
}
func (c attachmentGenCtx) rowValue(row map[string]any, key string) any {
	return resolveRowValue(row, c.FieldAliases, key)
}

func normalizeFieldKey(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(s)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func resolveRowValue(row map[string]any, fieldAliases map[string]string, key string) any {
	if row == nil || strings.TrimSpace(key) == "" {
		return nil
	}
	if v, ok := row[key]; ok {
		return v
	}
	candidates := []string{key}
	if alias, ok := fieldAliases[key]; ok && alias != "" && alias != key {
		if v, ok := row[alias]; ok {
			return v
		}
		candidates = append(candidates, alias)
	}
	for code, alias := range fieldAliases {
		if alias == key && code != key {
			if v, ok := row[code]; ok {
				return v
			}
			candidates = append(candidates, code)
		}
	}
	for _, cand := range candidates {
		nc := normalizeFieldKey(cand)
		if nc == "" {
			continue
		}
		for k, v := range row {
			if normalizeFieldKey(k) == nc {
				return v
			}
		}
	}
	return nil
}

// normalizeDataRowRange clamps from/to to a valid 1-based inclusive window.
func normalizeDataRowRange(from, to int) (int, int) {
	if from < 1 {
		from = defaultDataRowFrom
	}
	if to < from {
		to = from
	}
	if to > maxDataRowTo {
		to = maxDataRowTo
	}
	if from > maxDataRowTo {
		from = maxDataRowTo
		to = maxDataRowTo
	}
	return from, to
}

// fetchLimitOffset maps 1-based from/to onto dash FetchSourceData Limit/Offset.
func (c attachmentGenCtx) fetchLimitOffset() (limit, offset int) {
	from, to := normalizeDataRowRange(c.DataRowFrom, c.DataRowTo)
	return to - from + 1, from - 1
}

// poolDataRequest builds a FetchSourceData request that carries the full rule
// scope (entity, filters, window, bank scope, row range).
func (c attachmentGenCtx) poolDataRequest(source string) dashboardbuilder.DataRequest {
	if source == "" {
		source = c.SourceKey
	}
	limit, offset := c.fetchLimitOffset()
	return dashboardbuilder.DataRequest{
		Source:                   source,
		EntityIDs:                c.EntityIDs,
		Filters:                  c.Filters,
		Limit:                    limit,
		Offset:                   offset,
		AsOfDate:                 c.AsOfDate,
		AsOnDate:                 c.AsOnDate,
		BankAccountScope:         c.BankAccountScope,
		AllowUnscopedBankAccount: c.AllowUnscopedBankAccount,
		EnforceDateWindow:        true,
	}
}
