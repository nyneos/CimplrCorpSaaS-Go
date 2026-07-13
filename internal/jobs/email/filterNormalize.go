package emailjobs

import (
	"encoding/json"
)

// NormalizeMailboxFiltersJSON migrates legacy flat filters and mirrors inbound → outbound when outbound is empty.
func NormalizeMailboxFiltersJSON(raw json.RawMessage) json.RawMessage {
	mf := parseMailboxFilters(raw)
	outboundEmpty := !filterRulesActive(mf.Outbound)
	if outboundEmpty && filterRulesActive(mf.Inbound) {
		mf.Outbound = filterRules{
			Recipients:      append([]string(nil), mf.Inbound.Senders...),
			Domains:         append([]string(nil), mf.Inbound.Domains...),
			Subjects:        append([]string(nil), mf.Inbound.Subjects...),
			ExcludeSenders:  append([]string(nil), mf.Inbound.ExcludeSenders...),
			HasAttachments:  mf.Inbound.HasAttachments,
			AttachmentTypes: append([]string(nil), mf.Inbound.AttachmentTypes...),
		}
	}
	b, err := json.Marshal(mf)
	if err != nil {
		return raw
	}
	return b
}
