package emailjobs

import (
	"encoding/json"
	"strings"
)

// filterRules is one direction's filter set (received = match From, sent = match To).
type filterRules struct {
	Senders         []string `json:"senders"`
	Recipients      []string `json:"recipients"`
	Domains         []string `json:"domains"`
	Subjects        []string `json:"subjects"`
	ExcludeSenders  []string `json:"exclude_senders"`
	HasAttachments  *bool    `json:"has_attachments"`
	AttachmentTypes []string `json:"attachment_types"`
}

// mailboxFilters stores separate inbound (received) and outbound (sent) rules.
type mailboxFilters struct {
	Inbound  filterRules `json:"inbound"`
	Outbound filterRules `json:"outbound"`
}

type legacyFilters struct {
	Senders         []string `json:"senders"`
	Recipients      []string `json:"recipients"`
	Domains         []string `json:"domains"`
	Subjects        []string `json:"subjects"`
	ExcludeSenders  []string `json:"exclude_senders"`
	HasAttachments  *bool    `json:"has_attachments"`
	AttachmentTypes []string `json:"attachment_types"`
}

func parseMailboxFilters(raw []byte) mailboxFilters {
	var mf mailboxFilters
	var legacy legacyFilters
	_ = json.Unmarshal(raw, &mf)
	_ = json.Unmarshal(raw, &legacy)

	if !filterRulesActive(mf.Inbound) {
		mf.Inbound = filterRules{
			Senders:         append([]string(nil), legacy.Senders...),
			Domains:         append([]string(nil), legacy.Domains...),
			Subjects:        append([]string(nil), legacy.Subjects...),
			ExcludeSenders:  append([]string(nil), legacy.ExcludeSenders...),
			HasAttachments:  legacy.HasAttachments,
			AttachmentTypes: append([]string(nil), legacy.AttachmentTypes...),
		}
	}
	if !filterRulesActive(mf.Outbound) {
		mf.Outbound = filterRules{
			Recipients:      append([]string(nil), legacy.Recipients...),
			Domains:         append([]string(nil), legacy.Domains...),
			Subjects:        append([]string(nil), legacy.Subjects...),
			ExcludeSenders:  append([]string(nil), legacy.ExcludeSenders...),
			HasAttachments:  legacy.HasAttachments,
			AttachmentTypes: append([]string(nil), legacy.AttachmentTypes...),
		}
	}
	return mf
}

func filterRulesActive(f filterRules) bool {
	return len(f.Senders) > 0 || len(f.Recipients) > 0 || len(f.Domains) > 0 ||
		len(f.Subjects) > 0 || len(f.ExcludeSenders) > 0 ||
		f.HasAttachments != nil || len(f.AttachmentTypes) > 0
}

func inboundFiltersActive(mf mailboxFilters) bool {
	return filterRulesActive(mf.Inbound)
}

func outboundFiltersActive(mf mailboxFilters) bool {
	return filterRulesActive(mf.Outbound)
}

// filtersActive reports whether any inbound or outbound rule is configured (legacy helper).
func filtersActiveLegacy(raw []byte) bool {
	mf := parseMailboxFilters(raw)
	return inboundFiltersActive(mf) || outboundFiltersActive(mf)
}

func matchInboundRules(f filterRules, in matchInput) bool {
	from := strings.ToLower(strings.TrimSpace(in.From))
	for _, pat := range f.ExcludeSenders {
		if globMatch(strings.ToLower(pat), from) {
			return false
		}
	}
	if !filterRulesActive(f) {
		return true
	}
	return anyInboundCategoryMatches(f, in)
}

func matchOutboundRules(f filterRules, in matchInput) bool {
	for _, pat := range f.ExcludeSenders {
		p := strings.ToLower(strings.TrimSpace(pat))
		for _, to := range in.To {
			if globMatch(p, strings.ToLower(strings.TrimSpace(to))) {
				return false
			}
		}
	}
	if !filterRulesActive(f) {
		return true
	}
	return anyOutboundCategoryMatches(f, in)
}

func anyInboundCategoryMatches(f filterRules, in matchInput) bool {
	from := strings.ToLower(strings.TrimSpace(in.From))
	subject := strings.TrimSpace(in.Subject)

	var matches []bool
	if len(f.Senders) > 0 {
		matches = append(matches, anyGlob(f.Senders, from))
	}
	if len(f.Domains) > 0 {
		matches = append(matches, anyGlob(f.Domains, extractDomain(from)))
	}
	if len(f.Subjects) > 0 {
		matches = append(matches, anyGlob(f.Subjects, subject))
	}
	if f.HasAttachments != nil {
		matches = append(matches, *f.HasAttachments == in.HasAttachments)
	}
	if len(f.AttachmentTypes) > 0 && in.HasAttachments {
		matches = append(matches, attachmentTypeMatch(f.AttachmentTypes, in.AttachmentNames))
	}
	for _, m := range matches {
		if m {
			return true
		}
	}
	return false
}

func anyOutboundCategoryMatches(f filterRules, in matchInput) bool {
	subject := strings.TrimSpace(in.Subject)

	var matches []bool
	if len(f.Recipients) > 0 {
		ok := false
		for _, to := range in.To {
			if anyGlob(f.Recipients, strings.ToLower(strings.TrimSpace(to))) {
				ok = true
				break
			}
		}
		matches = append(matches, ok)
	}
	if len(f.Domains) > 0 {
		ok := false
		for _, to := range in.To {
			if anyGlob(f.Domains, extractDomain(strings.ToLower(strings.TrimSpace(to)))) {
				ok = true
				break
			}
		}
		matches = append(matches, ok)
	}
	if len(f.Subjects) > 0 {
		matches = append(matches, anyGlob(f.Subjects, subject))
	}
	if f.HasAttachments != nil {
		matches = append(matches, *f.HasAttachments == in.HasAttachments)
	}
	if len(f.AttachmentTypes) > 0 && in.HasAttachments {
		matches = append(matches, attachmentTypeMatch(f.AttachmentTypes, in.AttachmentNames))
	}
	for _, m := range matches {
		if m {
			return true
		}
	}
	return false
}

func directionFilterMatch(raw []byte, direction string, in matchInput) (matched bool, active bool) {
	mf := parseMailboxFilters(raw)
	if strings.EqualFold(direction, mailDirectionSent) {
		if !outboundFiltersActive(mf) {
			return false, false
		}
		return matchOutboundRules(mf.Outbound, in), true
	}
	if !inboundFiltersActive(mf) {
		return false, false
	}
	return matchInboundRules(mf.Inbound, in), true
}
