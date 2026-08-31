package common

import (
	"context"
	"net/http"

	"CimplrCorpSaas/api"
)

// Actor is the acting user resolved from the session already loaded onto the
// request context by middlewares.SessionMiddleware. Every mutating handler in
// this module resolves the actor exactly this way — sibling agents building
// other fdMonthEndClosing sub-features (scope, checklist, lock, reopen,
// evidencePack) should call ActorFromRequest instead of re-deriving identity
// from api.GetUserEmailFromCtx/api.GetUserIDFromCtx themselves.
type Actor struct {
	// UserID is public.users.id — the FK-safe value for columns like
	// approval_instance.submitted_by. May be empty for sessions that only
	// carry an email (mirrors api.GetUserIDFromCtx's own contract).
	UserID string
	// Email is the actor's session email — used for requested_by/checker_by/
	// created_by/audit columns everywhere in this module (same convention as
	// fdBookingWorkbench/booking.go's userEmail and
	// fdInterestAndTdsWorkbench/tdsRegister.go's resolveUserEmail).
	Email string
}

// ActorFromRequest resolves the acting user from r's context. ok is false
// when there is no valid session — callers must respond 401 via
// common.RespondError(w, http.StatusUnauthorized, ...) and return.
func ActorFromRequest(r *http.Request) (Actor, bool) {
	return ActorFromContext(r.Context())
}

// ActorFromContext is ActorFromRequest's context-only variant, for code paths
// (background goroutines, post-finalize hooks) that no longer have the
// original *http.Request.
func ActorFromContext(ctx context.Context) (Actor, bool) {
	email := api.GetUserEmailFromCtx(ctx)
	if email == "" {
		return Actor{}, false
	}
	return Actor{UserID: api.GetUserIDFromCtx(ctx), Email: email}, true
}
