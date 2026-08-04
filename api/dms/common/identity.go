package common

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	middlewares "CimplrCorpSaas/api/middlewares"
)

// RequestActor resolves the maker/checker actor for an audit row: session user first,
// then an explicit actor_id/checker_id from the request body, falling back to "system".
func RequestActor(r *http.Request, reqActorID string) string {
	actor := strings.TrimSpace(middlewares.GetUserIDFromContext(r.Context()))
	if actor == "" {
		actor = strings.TrimSpace(reqActorID)
	}
	if actor == "" {
		actor = "system"
	}
	return actor
}

// RequestIP resolves the client IP for audit rows from the request context.
func RequestIP(r *http.Request) string {
	return api.ClientIPFromContext(r.Context())
}
