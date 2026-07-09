package imapmail

import (
	"fmt"
	"strings"
)

// Provider presets for common IMAP hosts (Gmail personal, Workspace, Yahoo, Outlook IMAP).
const (
	ProviderGmailPersonal   = "gmail_personal"
	ProviderGoogleWorkspace = "google_workspace"
	ProviderYahoo           = "yahoo"
	ProviderOutlookIMAP     = "outlook_imap"
	ProviderZoho            = "zoho"
	ProviderICloud          = "icloud"
	ProviderAOL             = "aol"
	ProviderGeneric         = "generic"
)

// Config holds IMAP connection settings stored in inbox_config.imap_config_json.
type Config struct {
	Provider    string `json:"provider"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Username    string `json:"username"`
	Password    string `json:"password,omitempty"`
	UseTLS      bool   `json:"use_tls"`
	InboxFolder string `json:"inbox_folder"`
	SentFolder  string `json:"sent_folder"`
}

// Resolve fills host/port/folders from provider preset when not explicitly set.
func (c *Config) Resolve(mailboxAddress string) error {
	c.Provider = strings.TrimSpace(strings.ToLower(c.Provider))
	if c.Provider == "" {
		c.Provider = ProviderGeneric
	}
	mailboxAddress = strings.TrimSpace(strings.ToLower(mailboxAddress))
	if c.Username == "" {
		c.Username = mailboxAddress
	}
	if c.Port == 0 {
		c.Port = 993
	}
	if !c.UseTLS && c.Port == 993 {
		c.UseTLS = true
	}

	switch c.Provider {
	case ProviderGmailPersonal, ProviderGoogleWorkspace:
		if c.Host == "" {
			c.Host = "imap.gmail.com"
		}
		if c.InboxFolder == "" {
			c.InboxFolder = "INBOX"
		}
		if c.SentFolder == "" {
			c.SentFolder = "[Gmail]/Sent Mail"
		}
	case ProviderYahoo:
		if c.Host == "" {
			c.Host = "imap.mail.yahoo.com"
		}
		if c.InboxFolder == "" {
			c.InboxFolder = "INBOX"
		}
		if c.SentFolder == "" {
			c.SentFolder = "Sent"
		}
	case ProviderOutlookIMAP:
		if c.Host == "" {
			c.Host = "outlook.office365.com"
		}
		if c.InboxFolder == "" {
			c.InboxFolder = "INBOX"
		}
		if c.SentFolder == "" {
			c.SentFolder = "Sent Items"
		}
	case ProviderZoho:
		if c.Host == "" {
			c.Host = "imap.zoho.in"
		}
		if c.InboxFolder == "" {
			c.InboxFolder = "INBOX"
		}
		if c.SentFolder == "" {
			c.SentFolder = "Sent"
		}
	case ProviderICloud:
		if c.Host == "" {
			c.Host = "imap.mail.me.com"
		}
		if c.InboxFolder == "" {
			c.InboxFolder = "INBOX"
		}
		if c.SentFolder == "" {
			c.SentFolder = "Sent Messages"
		}
	case ProviderAOL:
		if c.Host == "" {
			c.Host = "imap.aol.com"
		}
		if c.InboxFolder == "" {
			c.InboxFolder = "INBOX"
		}
		if c.SentFolder == "" {
			c.SentFolder = "Sent"
		}
	default:
		if c.Host == "" {
			return fmt.Errorf("imap host is required for provider %q", c.Provider)
		}
		if c.InboxFolder == "" {
			c.InboxFolder = "INBOX"
		}
	}

	if c.Username == "" {
		return fmt.Errorf("imap username is required")
	}
	if c.Password == "" {
		return fmt.Errorf("imap password (app password) is required")
	}
	return nil
}

// Redacted returns a copy safe for API responses (password masked).
func (c Config) Redacted() Config {
	out := c
	if out.Password != "" {
		out.Password = "********"
	}
	return out
}
