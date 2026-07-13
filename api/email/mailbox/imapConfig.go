package emailmailbox

import (
	"net/mail"
	"os"
	"strings"
)

func IsValidMailboxAddress(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return false
	}
	_, err := mail.ParseAddress(addr)
	return err == nil
}

func MailboxAddressAllowed(addr, sourceType string, customCreds bool) bool {
	addr = strings.TrimSpace(addr)
	switch strings.ToUpper(strings.TrimSpace(sourceType)) {
	case "IMAP":
		return IsValidMailboxAddress(addr)
	case "OAUTH":
		return IsValidMailboxAddress(addr)
	case "OUTLOOK_GRAPH":
		if !IsValidMailboxAddress(addr) {
			return false
		}
		if customCreds {
			return true
		}
		return MailboxDomainAllowed(addr)
	case "GOOGLE_WORKSPACE":
		if !IsValidMailboxAddress(addr) {
			return false
		}
		if customCreds {
			return true
		}
		return MailboxDomainAllowed(addr)
	default:
		return MailboxDomainAllowed(addr)
	}
}

func MailboxAllowedForSource(addr, sourceType string) bool {
	return MailboxAddressAllowed(addr, sourceType, false)
}

func AllowedMailboxDomains() []string {
	raw := strings.TrimSpace(os.Getenv("EMAIL_ALLOWED_MAILBOX_DOMAINS"))
	if raw == "" {
		raw = "nyneos.com"
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.ToLower(strings.TrimSpace(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func MailboxDomainAllowed(addr string) bool {
	addr = strings.ToLower(strings.TrimSpace(addr))
	at := strings.LastIndex(addr, "@")
	if at < 0 {
		return false
	}
	domain := addr[at+1:]
	for _, d := range AllowedMailboxDomains() {
		if domain == d {
			return true
		}
	}
	return false
}
