package emailjobs

// FilterMatchInput is the public shape for validating a message against inbox filters.
type FilterMatchInput struct {
	From              string
	To                []string
	Subject           string
	HasAttachments    bool
	AttachmentNames   []string
}

// MailboxMatchesRecipient reports whether mailbox appears in the To list (case-insensitive).
func MailboxMatchesRecipient(mailbox string, to []string) bool {
	return mailboxMatches(mailbox, to)
}

// FiltersConfigured reports whether any inbound or outbound filter rule is active.
func FiltersConfigured(filtersJSON []byte) bool {
	mf := parseMailboxFilters(filtersJSON)
	return inboundFiltersActive(mf) || outboundFiltersActive(mf)
}

// MatchInboundFilters applies received-mail filter rules (match on From).
func MatchInboundFilters(filtersJSON []byte, in FilterMatchInput) bool {
	mf := parseMailboxFilters(filtersJSON)
	if !inboundFiltersActive(mf) {
		return false
	}
	return matchInboundRules(mf.Inbound, matchInput{
		From: in.From, To: in.To, Subject: in.Subject,
		HasAttachments: in.HasAttachments, AttachmentNames: in.AttachmentNames,
	})
}

// MatchSentFilters applies sent-mail filter rules (match on To).
func MatchSentFilters(filtersJSON []byte, in FilterMatchInput) bool {
	mf := parseMailboxFilters(filtersJSON)
	if !outboundFiltersActive(mf) {
		return false
	}
	return matchOutboundRules(mf.Outbound, matchInput{
		From: in.From, To: in.To, Subject: in.Subject,
		HasAttachments: in.HasAttachments, AttachmentNames: in.AttachmentNames,
	})
}
