package common

import "time"

// FormatAuditTime renders an optional audit timestamp as RFC3339 UTC.
// A nil timestamp means the action never happened (e.g. a record that was
// never edited), and must stay empty so the UI does not fall back to the
// created/requested timestamp.
func FormatAuditTime(ts *time.Time) string {
	if ts == nil {
		return ""
	}
	return ts.UTC().Format(time.RFC3339)
}
