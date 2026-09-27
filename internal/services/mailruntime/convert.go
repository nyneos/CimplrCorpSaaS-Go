package mailruntime

import (
	"os"
	"strings"

	"CimplrCorpSaas/internal/mailengine/model"
	"CimplrCorpSaas/internal/services/graphmail"
	"CimplrCorpSaas/internal/services/imapmail"
)

func fromModelParsed(p model.ParsedEmail) ParsedMessage {
	var out ParsedMessage
	out.MessageID = p.MessageID
	out.S3RawKey = p.S3RawKey
	out.S3ParsedKey = p.S3ParsedKey
	out.Envelope.From = p.Envelope.From
	out.Envelope.To = p.Envelope.To
	out.Envelope.Cc = p.Envelope.Cc
	out.Envelope.Subject = p.Envelope.Subject
	out.Envelope.Date = p.Envelope.Date
	out.Envelope.MessageIDHeader = p.Envelope.MessageIDHeader
	out.Body.TextPlain = p.Body.TextPlain
	out.Body.TextHTML = p.Body.TextHTML
	out.Body.Preferred = p.Body.Preferred
	out.Status = p.Status
	for _, a := range p.Attachments {
		out.Attachments = append(out.Attachments, struct {
			Filename    string `json:"filename"`
			ContentType string `json:"content_type"`
			SizeBytes   int64  `json:"size_bytes"`
			S3Key       string `json:"s3_key"`
			SHA256      string `json:"sha256"`
		}{
			Filename:    a.Filename,
			ContentType: a.ContentType,
			SizeBytes:   a.SizeBytes,
			S3Key:       a.S3Key,
			SHA256:      a.SHA256,
		})
	}
	return out
}

func toIMAPConfig(conn IMAPConnection, mailbox string) (imapmail.Config, error) {
	port := conn.Port
	if port <= 0 {
		port = 993
	}
	cfg := imapmail.Config{
		Provider:    conn.Provider,
		Host:        conn.Host,
		Port:        port,
		Username:    conn.Username,
		Password:    conn.Password,
		AuthMode:    conn.AuthMode,
		AccessToken: conn.AccessToken,
		UseTLS:      conn.UseTLS,
		InboxFolder: conn.InboxFolder,
		SentFolder:  conn.SentFolder,
	}
	if err := cfg.Resolve(mailbox); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func toGraphConfig(conn GraphConnection) (graphmail.Config, error) {
	cfg := graphmail.Config{
		Label:        conn.TenantLabel,
		TenantID:     conn.TenantID,
		ClientID:     conn.ClientID,
		ClientSecret: conn.ClientSecret,
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func inboundS3Prefix() string {
	prefix := strings.TrimSpace(os.Getenv("EMAIL_INBOUND_S3_PREFIX"))
	if prefix == "" {
		prefix = "email/inbound/raw/"
	}
	return prefix
}

func imapRawS3Key(mailbox, direction, imapKey string) string {
	safeMailbox := strings.ReplaceAll(strings.ToLower(mailbox), "@", "_at_")
	safeKey := strings.ReplaceAll(imapKey, ":", "_")
	dir := "received"
	if strings.EqualFold(direction, "SENT") {
		dir = "sent"
	}
	return inboundS3Prefix() + "imap/" + safeMailbox + "/" + dir + "/" + safeKey + ".eml"
}

func graphRawS3Key(mailbox string, sent bool, graphID string) string {
	safeMailbox := strings.ReplaceAll(strings.ToLower(mailbox), "@", "_at_")
	dir := "received"
	if sent {
		dir = "sent"
	}
	safeID := strings.ReplaceAll(graphID, "/", "_")
	return inboundS3Prefix() + "graph/" + safeMailbox + "/" + dir + "/" + safeID + ".eml"
}

func oauthRawS3Key(mailbox string, sent bool, provider, messageID string) string {
	safeMailbox := strings.ReplaceAll(strings.ToLower(mailbox), "@", "_at_")
	dir := "received"
	if sent {
		dir = "sent"
	}
	safeID := strings.ReplaceAll(messageID, "/", "_")
	return inboundS3Prefix() + "oauth/" + strings.ToLower(provider) + "/" + safeMailbox + "/" + dir + "/" + safeID + ".eml"
}

func gmailDWDRawS3Key(mailbox string, sent bool, messageID string) string {
	dir := "received"
	if sent {
		dir = "sent"
	}
	safeMailbox := strings.NewReplacer("@", "_at_", ".", "_").Replace(strings.ToLower(mailbox))
	return "email/inbound/raw/gmail-dwd/" + safeMailbox + "/" + dir + "/" + messageID + ".eml"
}
