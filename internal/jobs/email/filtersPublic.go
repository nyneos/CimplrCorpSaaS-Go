package emailjobs

import "encoding/json"

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

// FiltersConfigured reports whether any inbound filter rule is active.
func FiltersConfigured(filtersJSON []byte) bool {
	var f filters
	_ = json.Unmarshal(filtersJSON, &f)
	return filtersActive(f)
}

// MatchInboundFilters applies received-mail filter rules from filters_json.
func MatchInboundFilters(filtersJSON []byte, in FilterMatchInput) bool {
	var f filters
	if err := json.Unmarshal(filtersJSON, &f); err != nil {
		return false
	}
	if !filtersActive(f) {
		return true
	}
	return matchFilters(f, matchInput{
		From:            in.From,
		To:              in.To,
		Subject:         in.Subject,
		HasAttachments:  in.HasAttachments,
		AttachmentNames: in.AttachmentNames,
	})
}

// MatchSentFilters applies sent-mail filter rules from filters_json.
func MatchSentFilters(filtersJSON []byte, in FilterMatchInput) bool {
	var f filters
	if err := json.Unmarshal(filtersJSON, &f); err != nil {
		return false
	}
	if !filtersActive(f) {
		return true
	}
	return matchSentFilters(f, matchInput{
		From:            in.From,
		To:              in.To,
		Subject:         in.Subject,
		HasAttachments:  in.HasAttachments,
		AttachmentNames: in.AttachmentNames,
	})
}
