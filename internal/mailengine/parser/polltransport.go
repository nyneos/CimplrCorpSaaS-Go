package parser

import (
	"CimplrCorpSaas/internal/mailengine/model"
)

const pollTransportPlainMax = 8192

// ForPollTransport returns a copy suitable for poll HTTP responses: drops HTML body
// (already in S3 parsed JSON) and caps plain text for list preview on the Go side.
func ForPollTransport(parsed model.ParsedEmail) model.ParsedEmail {
	out := parsed
	out.Body.TextHTML = ""
	if len(out.Body.TextPlain) > pollTransportPlainMax {
		out.Body.TextPlain = out.Body.TextPlain[:pollTransportPlainMax]
	}
	if out.Body.TextPlain != "" {
		out.Body.Preferred = "text"
	} else {
		out.Body.Preferred = "none"
	}
	return out
}
