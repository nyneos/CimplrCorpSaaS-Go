package pollcursor

import (
	"strings"
	"time"
)

// FormatStored serializes a poll watermark with sub-second precision so the next
// provider query does not re-list messages in the same UTC second.
func FormatStored(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// ParseStored accepts RFC3339Nano or RFC3339 timestamps from DB / poll requests.
func ParseStored(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	return time.Parse(time.RFC3339, s)
}

// FormatGraphOData formats a UTC timestamp for Microsoft Graph OData $filter (ms).
func FormatGraphOData(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// ResolveNewSince picks the next watermark after listing (and optionally skipping) messages.
func ResolveNewSince(since, maxCursor time.Time, listed, skippedKnown int) time.Time {
	since = since.UTC()
	maxCursor = maxCursor.UTC()

	if maxCursor.After(since) {
		return maxCursor
	}
	// Entire page already ingested — advance at least one second (Gmail after: uses unix seconds).
	if listed > 0 && skippedKnown == listed {
		return since.Add(time.Second)
	}
	// Same-second cluster or RFC3339 truncation: bump so the next poll excludes re-listed IDs.
	if listed > 0 {
		if !maxCursor.IsZero() {
			return maxCursor.Add(time.Millisecond)
		}
		return since.Add(time.Second)
	}
	if skippedKnown > 0 && !since.IsZero() {
		return since.Add(time.Second)
	}
	return since
}
