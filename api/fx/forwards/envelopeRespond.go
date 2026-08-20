package forwards

import (
	"CimplrCorpSaas/api"
	"net/http"
)

func respondEnvelopeError(w http.ResponseWriter, status int, errMsg string) {
	api.RespondEnvelopeError(w, status, errMsg, api.EnvelopeErrorCode(status))
}

func respondEnvelopeSuccess(w http.ResponseWriter, message string, data interface{}) {
	api.RespondEnvelopeSuccess(w, message, data)
}

// respondEnvelopeSuccessCompat nests fields under data and also flattens them
// onto the top-level body for legacy frontend readers (e.g. response.bookings).
func respondEnvelopeSuccessCompat(w http.ResponseWriter, message string, fields map[string]interface{}) {
	api.RespondEnvelopeSuccessCompat(w, message, fields)
}

func respondEnvelopeFailureWithData(w http.ResponseWriter, status int, message string, data interface{}) {
	api.RespondEnvelopeFailureWithData(w, status, message, api.EnvelopeErrorCode(status), data)
}
