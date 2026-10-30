package governance

import (
	"context"
	"errors"
	"strings"

	"CimplrCorpSaas/api"
)

// ErrSelfApproval is returned when the checker is the maker of the request.
var ErrSelfApproval = errors.New("you cannot approve or reject a request you submitted")

// ErrNoChecker is returned when the checker identity is missing.
var ErrNoChecker = errors.New("checker identity is required")

// EnsureNotMaker fails when checker and maker are the same user.
func EnsureNotMaker(makerUserID, checkerUserID string) error {
	checker := strings.TrimSpace(checkerUserID)
	if checker == "" {
		return ErrNoChecker
	}
	if strings.EqualFold(strings.TrimSpace(makerUserID), checker) {
		return ErrSelfApproval
	}
	return nil
}

// IsSelfApproval reports whether the checker is also one of the makers.
func IsSelfApproval(checkerUserID string, makers ...string) bool {
	for _, m := range makers {
		if EnsureNotMaker(m, checkerUserID) == ErrSelfApproval {
			return true
		}
	}
	return false
}

// CheckerAliases returns the identifiers a maker column may hold for the caller:
// user id, email and display name from the session.
func CheckerAliases(ctx context.Context) []string {
	out := []string{}
	add := func(v string) {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	add(api.GetUserIDFromCtx(ctx))
	if s := api.GetSessionFromCtx(ctx); s != nil {
		add(s.UserID)
		add(s.Email)
		add(s.Name)
	}
	return out
}

// IsSelfApprovalCtx reports whether any maker value names the calling user.
func IsSelfApprovalCtx(ctx context.Context, makers ...string) bool {
	aliases := CheckerAliases(ctx)
	for _, m := range makers {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		for _, a := range aliases {
			if strings.EqualFold(m, a) {
				return true
			}
		}
	}
	return false
}
