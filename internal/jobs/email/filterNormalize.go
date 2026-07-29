package emailjobs

import (
	"encoding/json"
)

// NormalizeMailboxFiltersJSON migrates legacy flat filters into the
// inbound/outbound shape. Empty outbound stays empty — never copy inbound
// rules into outbound (those are configured independently).
func NormalizeMailboxFiltersJSON(raw json.RawMessage) json.RawMessage {
	mf := parseMailboxFilters(raw)
	b, err := json.Marshal(mf)
	if err != nil {
		return raw
	}
	return b
}
