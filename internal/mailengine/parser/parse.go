package parser

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"CimplrCorpSaas/internal/mailengine/model"
	"CimplrCorpSaas/internal/mailengine/s3store"

	"github.com/jhillyerd/enmime"
)

func newMessageID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func ParseFromS3(ctx context.Context, s3RawKey string) (model.ParsedEmail, error) {
	raw, err := s3store.GetObjectBytes(ctx, s3RawKey)
	if err != nil {
		return model.ParsedEmail{}, fmt.Errorf("read s3: %w", err)
	}
	return ParseRaw(ctx, s3RawKey, raw)
}

func ParseRaw(ctx context.Context, s3RawKey string, raw []byte) (model.ParsedEmail, error) {
	env, err := enmime.ReadEnvelope(strings.NewReader(string(raw)))
	if err != nil {
		return model.ParsedEmail{}, fmt.Errorf("parse mime: %w", err)
	}

	msgID := StorageIDFromRawKey(s3RawKey)
	envelope := model.Envelope{
		From:            firstAddr(env.GetHeader("From")),
		To:              splitAddrs(env.GetHeader("To")),
		Cc:              splitAddrs(env.GetHeader("Cc")),
		Subject:         strings.TrimSpace(env.GetHeader("Subject")),
		Date:            strings.TrimSpace(env.GetHeader("Date")),
		MessageIDHeader: strings.TrimSpace(env.GetHeader("Message-Id")),
		InReplyTo:       strings.TrimSpace(env.GetHeader("In-Reply-To")),
		References:      strings.TrimSpace(env.GetHeader("References")),
	}

	body := model.Body{
		TextPlain: strings.TrimSpace(env.Text),
		TextHTML:  strings.TrimSpace(env.HTML),
	}
	switch {
	case body.TextHTML != "":
		body.Preferred = "html"
	case body.TextPlain != "":
		body.Preferred = "text"
	default:
		body.Preferred = "none"
	}

	var attachments []model.Attachment
	for _, part := range env.Attachments {
		filename := part.FileName
		if filename == "" {
			filename = "attachment"
		}
		ct := part.ContentType
		if ct == "" {
			ct = "application/octet-stream"
		}
		s3Key, hash, err := s3store.PutAttachmentStable(ctx, msgID, filename, part.Content, ct)
		if err != nil {
			return model.ParsedEmail{}, fmt.Errorf("upload attachment: %w", err)
		}
		attachments = append(attachments, model.Attachment{
			Filename:    filename,
			ContentType: ct,
			SizeBytes:   int64(len(part.Content)),
			S3Key:       s3Key,
			SHA256:      hash,
		})
	}

	parsed := model.ParsedEmail{
		MessageID:   msgID,
		S3RawKey:    s3RawKey,
		Envelope:    envelope,
		Body:        body,
		Attachments: attachments,
		Headers: map[string]string{
			"message-id":  envelope.MessageIDHeader,
			"in-reply-to": envelope.InReplyTo,
			"references":  envelope.References,
		},
		ParsedAt: time.Now().UTC(),
		Status:   "PARSED",
	}

	parsedKey, err := s3store.PutParsedJSON(ctx, msgID, parsed)
	if err != nil {
		return model.ParsedEmail{}, fmt.Errorf("write parsed json: %w", err)
	}
	parsed.S3ParsedKey = parsedKey
	return parsed, nil
}

func firstAddr(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	addrs, err := mail.ParseAddressList(s)
	if err != nil || len(addrs) == 0 {
		return s
	}
	return addrs[0].Address
}

func splitAddrs(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	// Outlook/Exchange often uses semicolons; net/mail expects commas.
	normalized := strings.ReplaceAll(s, ";", ",")
	addrs, err := mail.ParseAddressList(normalized)
	if err != nil {
		return parseAddrFragments(s)
	}
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a.Address != "" {
			out = append(out, a.Address)
		}
	}
	return out
}

func parseAddrFragments(s string) []string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ';'
	})
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if addrs, err := mail.ParseAddressList(p); err == nil {
			for _, a := range addrs {
				if a.Address != "" {
					out = append(out, a.Address)
				}
			}
			continue
		}
		if addr, err := mail.ParseAddress(p); err == nil && addr.Address != "" {
			out = append(out, addr.Address)
		}
	}
	if len(out) == 0 {
		return []string{s}
	}
	return out
}
