package graphmail

import (
	"fmt"
	"strings"
)

// Config holds per-mailbox Microsoft Graph app credentials (optional).
// Empty fields fall back to server .env (AZURE_* / GRAPH_*).
type Config struct {
	Label        string `json:"label,omitempty"`
	TenantID     string `json:"tenant_id"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret,omitempty"`
}

// Resolve merges mailbox config with env defaults.
func (c *Config) Resolve(defaults Config) Config {
	out := defaults
	if c == nil {
		return out
	}
	if strings.TrimSpace(c.Label) != "" {
		out.Label = strings.TrimSpace(c.Label)
	}
	if strings.TrimSpace(c.TenantID) != "" {
		out.TenantID = strings.TrimSpace(c.TenantID)
	}
	if strings.TrimSpace(c.ClientID) != "" {
		out.ClientID = strings.TrimSpace(c.ClientID)
	}
	if strings.TrimSpace(c.ClientSecret) != "" && c.ClientSecret != "********" {
		out.ClientSecret = strings.TrimSpace(c.ClientSecret)
	}
	return out
}

// Configured reports whether all three credential fields are set.
func (c Config) Configured() bool {
	return c.TenantID != "" && c.ClientID != "" && c.ClientSecret != ""
}

// Validate returns an error if credentials are partially set.
func (c Config) Validate() error {
	hasAny := c.TenantID != "" || c.ClientID != "" || c.ClientSecret != ""
	if !hasAny {
		return nil
	}
	if !c.Configured() {
		return fmt.Errorf("graph config incomplete: tenant_id, client_id, and client_secret are all required when overriding defaults")
	}
	return nil
}

// Redacted returns a copy safe for API responses.
func (c Config) Redacted() Config {
	out := c
	if out.ClientSecret != "" {
		out.ClientSecret = "********"
	}
	return out
}
