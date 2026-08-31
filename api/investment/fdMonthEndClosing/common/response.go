// Package common holds cross-cutting helpers shared by every feature package
// under fdMonthEndClosing (cycle, and — as sibling agents add them — scope,
// checklist, lock, reopen, evidencePack). Mirrors api/email/common's shape:
// small, focused files split by concern rather than one grab-bag helpers.go.
package common

import (
	"net/http"

	"CimplrCorpSaas/api"
)

// respondEnvelopeError/-Success/-FailureWithData are thin same-package
// wrappers that delegate to api/envelope.go — the only correct envelope
// implementation (CLAUDE.md "API Response Envelope" rule). Mirrors
// api/fx/forwards/envelopeRespond.go's pattern; never reinvent the shape
// locally.

// RespondError writes the standard error envelope for status/message, with
// a stable error code derived from status.
func RespondError(w http.ResponseWriter, status int, message string) {
	api.RespondEnvelopeError(w, status, message, api.EnvelopeErrorCode(status))
}

// RespondSuccess writes the standard success envelope.
func RespondSuccess(w http.ResponseWriter, message string, data interface{}) {
	api.RespondEnvelopeSuccess(w, message, data)
}

// RespondFailureWithData writes the standard error envelope carrying an
// additional data payload (e.g. per-row bulk-action results).
func RespondFailureWithData(w http.ResponseWriter, status int, message string, data interface{}) {
	api.RespondEnvelopeFailureWithData(w, status, message, api.EnvelopeErrorCode(status), data)
}
