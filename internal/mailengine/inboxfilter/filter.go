// Package inboxfilter mirrors Cimplr Go mailbox filters so the email service can
// skip MIME download / S3 put when a message would never be ingested.
package inboxfilter

import (
	"encoding/json"
	"path"
	"strings"
)

const (
	DirectionReceived = "RECEIVED"
	DirectionSent     = "SENT"
)

// Input is metadata available before (or without) full MIME → S3 work.
type Input struct {
	From                 string
	To                   []string
	Subject              string
	HasAttachments       bool
	AttachmentNames      []string
	AttachmentNamesKnown bool // true when names come from a parsed MIME (IMAP/meta); false for Graph list
}

type filterRules struct {
	Senders         []string `json:"senders"`
	Recipients      []string `json:"recipients"`
	Domains         []string `json:"domains"`
	Subjects        []string `json:"subjects"`
	ExcludeSenders  []string `json:"exclude_senders"`
	HasAttachments  *bool    `json:"has_attachments"`
	AttachmentTypes []string `json:"attachment_types"`
}

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

func directionFilterMatch(raw []byte, direction string, in Input) (matched bool, active bool) {
	mf := parseMailboxFilters(raw)
	if strings.EqualFold(direction, DirectionSent) {
		if !filterRulesActive(mf.Outbound) {
			return false, false
		}
		return matchOutboundRules(mf.Outbound, in), true
	}
	if !filterRulesActive(mf.Inbound) {
		return false, false
	}
	return matchInboundRules(mf.Inbound, in), true
}

func matchInboundRules(f filterRules, in Input) bool {
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

func matchOutboundRules(f filterRules, in Input) bool {
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

func anyInboundCategoryMatches(f filterRules, in Input) bool {
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
	if len(f.AttachmentTypes) > 0 && in.HasAttachments && in.AttachmentNamesKnown {
		matches = append(matches, attachmentTypeMatch(f.AttachmentTypes, in.AttachmentNames))
	}
	for _, m := range matches {
		if m {
			return true
		}
	}
	return false
}

func anyOutboundCategoryMatches(f filterRules, in Input) bool {
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
	if len(f.AttachmentTypes) > 0 && in.HasAttachments && in.AttachmentNamesKnown {
		matches = append(matches, attachmentTypeMatch(f.AttachmentTypes, in.AttachmentNames))
	}
	for _, m := range matches {
		if m {
			return true
		}
	}
	return false
}

// ShouldIngest is true when the message should proceed to MIME download / S3 put.
// False means Go would skip DB insert for the same metadata (inactive filters or clear miss).
// When attachment_types could still match after MIME parse, returns true to defer.
func ShouldIngest(filtersJSON []byte, direction string, in Input) bool {
	if len(filtersJSON) == 0 {
		return false
	}
	matched, active := directionFilterMatch(filtersJSON, direction, in)
	if !active {
		return false
	}
	if matched {
		return true
	}
	if in.AttachmentNamesKnown {
		return false
	}
	mf := parseMailboxFilters(filtersJSON)
	var rules filterRules
	if strings.EqualFold(direction, DirectionSent) {
		rules = mf.Outbound
	} else {
		rules = mf.Inbound
	}
	// Unevaluable attachment extension filter — only fetch if attachments exist.
	if len(rules.AttachmentTypes) > 0 && in.HasAttachments {
		return true
	}
	return false
}

// DirectionFromSentFolder maps poll sent_folder flag to RECEIVED/SENT.
func DirectionFromSentFolder(sentFolder bool) string {
	if sentFolder {
		return DirectionSent
	}
	return DirectionReceived
}

func anyGlob(patterns []string, value string) bool {
	for _, p := range patterns {
		if globMatch(strings.ToLower(strings.TrimSpace(p)), strings.ToLower(value)) {
			return true
		}
	}
	return false
}

func globMatch(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	ok, _ := path.Match(pattern, value)
	return ok
}

func extractDomain(email string) string {
	at := strings.LastIndex(email, "@")
	if at < 0 {
		return email
	}
	return strings.ToLower(email[at+1:])
}

func attachmentTypeMatch(types []string, names []string) bool {
	for _, name := range names {
		ext := strings.TrimPrefix(strings.ToLower(path.Ext(name)), ".")
		for _, t := range types {
			t = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(t)), ".")
			if t == ext {
				return true
			}
		}
	}
	return false
}
