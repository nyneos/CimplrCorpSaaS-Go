package parser

import (
	"strings"

	"CimplrCorpSaas/internal/mailengine/inboxfilter"

	"github.com/jhillyerd/enmime"
)

// MetaFromRaw extracts filter metadata from raw MIME without any S3 writes.
func MetaFromRaw(raw []byte) (inboxfilter.Input, error) {
	env, err := enmime.ReadEnvelope(strings.NewReader(string(raw)))
	if err != nil {
		return inboxfilter.Input{}, err
	}
	names := make([]string, 0, len(env.Attachments))
	for _, part := range env.Attachments {
		if part.FileName != "" {
			names = append(names, part.FileName)
		}
	}
	return inboxfilter.Input{
		From:                 firstAddr(env.GetHeader("From")),
		To:                   splitAddrs(env.GetHeader("To")),
		Subject:              strings.TrimSpace(env.GetHeader("Subject")),
		HasAttachments:       len(env.Attachments) > 0,
		AttachmentNames:      names,
		AttachmentNamesKnown: true,
	}, nil
}
