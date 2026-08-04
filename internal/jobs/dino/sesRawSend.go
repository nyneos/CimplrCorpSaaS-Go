package jobs

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	sesTypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
)

// owSendViaSESRaw delivers one email with attachments using SES SendRawEmail.
// Used when the remote Notification-Service bulk endpoint cannot be trusted to
// honour the attachments[] field (older deploys ignore unknown JSON and send
// body-only via SES SendEmail — which cannot carry files).
func owSendViaSESRaw(ctx context.Context, p owSendPayload) (messageID string, err error) {
	from := strings.TrimSpace(p.From)
	if from == "" {
		from = strings.TrimSpace(os.Getenv("SENDER_EMAIL"))
	}
	if from == "" {
		from = "kanav.arora@nyneos.com"
	}
	if p.SenderName != "" && !strings.Contains(from, "<") {
		from = fmt.Sprintf("%s <%s>", p.SenderName, from)
	}

	atts := make([]owSesAttachment, 0, len(p.Attachments))
	for _, a := range p.Attachments {
		raw, decErr := base64.StdEncoding.DecodeString(strings.TrimSpace(a.DataBase64))
		if decErr != nil {
			return "", fmt.Errorf("decode attachment %q: %w", a.Filename, decErr)
		}
		if len(raw) == 0 {
			return "", fmt.Errorf("attachment %q is empty", a.Filename)
		}
		atts = append(atts, owSesAttachment{
			Filename:    a.Filename,
			ContentType: a.ContentType,
			Data:        raw,
		})
	}

	rawMIME, err := owBuildRawMIME(from, p.To, p.Cc, p.Subject, p.HTMLBody, atts)
	if err != nil {
		return "", err
	}

	region := strings.TrimSpace(os.Getenv("AWS_REGION"))
	if region == "" {
		region = "ap-south-1"
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return "", fmt.Errorf("aws config: %w", err)
	}
	client := ses.NewFromConfig(cfg)

	// Source must be a bare email for SES; display-name form is in the MIME From header.
	source := from
	if i := strings.LastIndex(from, "<"); i >= 0 {
		source = strings.TrimSuffix(strings.TrimSpace(from[i+1:]), ">")
	}

	destinations := owSplitEmails(p.To)
	destinations = append(destinations, owSplitEmails(p.Cc)...)
	if len(destinations) == 0 {
		return "", fmt.Errorf("no To/Cc destinations")
	}

	out, err := client.SendRawEmail(ctx, &ses.SendRawEmailInput{
		Source:       aws.String(source),
		Destinations: destinations,
		RawMessage:   &sesTypes.RawMessage{Data: rawMIME},
	})
	if err != nil {
		return "", err
	}
	if out.MessageId != nil {
		return *out.MessageId, nil
	}
	return "", nil
}

type owSesAttachment struct {
	Filename    string
	ContentType string
	Data        []byte
}

func owBuildRawMIME(from, to, cc, subject, htmlBody string, atts []owSesAttachment) ([]byte, error) {
	mixed := fmt.Sprintf("mixed_%d", time.Now().UnixNano())
	alt := fmt.Sprintf("alt_%d", time.Now().UnixNano()+1)
	textBody := owStripHTML(htmlBody)

	var b strings.Builder
	b.WriteString(fmt.Sprintf("From: %s\r\n", from))
	b.WriteString(fmt.Sprintf("To: %s\r\n", to))
	if strings.TrimSpace(cc) != "" {
		b.WriteString(fmt.Sprintf("Cc: %s\r\n", strings.TrimSpace(cc)))
	}
	b.WriteString(fmt.Sprintf("Subject: %s\r\n", owEncodeSubject(subject)))
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString(fmt.Sprintf("Content-Type: multipart/mixed; boundary=\"%s\"\r\n\r\n", mixed))

	b.WriteString(fmt.Sprintf("--%s\r\n", mixed))
	b.WriteString(fmt.Sprintf("Content-Type: multipart/alternative; boundary=\"%s\"\r\n\r\n", alt))

	b.WriteString(fmt.Sprintf("--%s\r\n", alt))
	b.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	b.WriteString(owChunkBase64([]byte(textBody)))
	b.WriteString("\r\n")

	b.WriteString(fmt.Sprintf("--%s\r\n", alt))
	b.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	b.WriteString(owChunkBase64([]byte(htmlBody)))
	b.WriteString("\r\n")
	b.WriteString(fmt.Sprintf("--%s--\r\n", alt))

	for _, a := range atts {
		fn := strings.TrimSpace(a.Filename)
		if fn == "" {
			fn = "attachment.bin"
		}
		ct := strings.TrimSpace(a.ContentType)
		if ct == "" {
			ct = "application/octet-stream"
		}
		// Keep Content-Type simple (type/subtype only) — params go on Disposition.
		if i := strings.Index(ct, ";"); i >= 0 {
			ct = strings.TrimSpace(ct[:i])
		}
		b.WriteString(fmt.Sprintf("--%s\r\n", mixed))
		b.WriteString(fmt.Sprintf("Content-Type: %s; name=\"%s\"\r\n", ct, fn))
		b.WriteString("Content-Transfer-Encoding: base64\r\n")
		b.WriteString(fmt.Sprintf("Content-Disposition: attachment; filename=\"%s\"\r\n\r\n", fn))
		b.WriteString(owChunkBase64(a.Data))
		b.WriteString("\r\n")
	}
	b.WriteString(fmt.Sprintf("--%s--\r\n", mixed))
	return []byte(b.String()), nil
}

func owChunkBase64(data []byte) string {
	encoded := base64.StdEncoding.EncodeToString(data)
	var b strings.Builder
	for i := 0; i < len(encoded); i += 76 {
		end := i + 76
		if end > len(encoded) {
			end = len(encoded)
		}
		b.WriteString(encoded[i:end])
		b.WriteString("\r\n")
	}
	return b.String()
}

func owEncodeSubject(s string) string {
	for _, r := range s {
		if r > 127 {
			return "=?UTF-8?B?" + base64.StdEncoding.EncodeToString([]byte(s)) + "?="
		}
	}
	return s
}

func owStripHTML(html string) string {
	var b strings.Builder
	inTag := false
	for _, r := range html {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			inTag = false
		case !inTag:
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

func owSplitEmails(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';'
	})
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, p := range parts {
		email := strings.ToLower(strings.TrimSpace(p))
		if email == "" || !strings.Contains(email, "@") {
			continue
		}
		if _, dup := seen[email]; dup {
			continue
		}
		seen[email] = struct{}{}
		out = append(out, email)
	}
	return out
}
