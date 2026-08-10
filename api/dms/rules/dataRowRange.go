package rules

const (
	defaultDataRowFrom = 1
	defaultDataRowTo   = 500
	maxDataRowTo       = 2000
)

// normalizeDataRowRangePtr applies defaults (1–500) when both are nil, otherwise
// requires a valid 1-based inclusive window with to ≤ 2000.
func normalizeDataRowRangePtr(fromPtr, toPtr *int) (from, to int, ok bool) {
	from, to = defaultDataRowFrom, defaultDataRowTo
	if fromPtr == nil && toPtr == nil {
		return from, to, true
	}
	if fromPtr != nil {
		from = *fromPtr
	}
	if toPtr != nil {
		to = *toPtr
	}
	if from < 1 || to < from || to > maxDataRowTo {
		return 0, 0, false
	}
	return from, to, true
}
