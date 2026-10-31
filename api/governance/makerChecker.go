package governance

import (
	"context"
	"errors"
	"strings"

	"CimplrCorpSaas/api"
)

var ErrSelfApproval = errors.New("you cannot approve or reject a request you submitted")

var ErrNoChecker = errors.New("checker identity is required")

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

func IsSelfApproval(checkerUserID string, makers ...string) bool {
	for _, m := range makers {
		if EnsureNotMaker(m, checkerUserID) == ErrSelfApproval {
			return true
		}
	}
	return false
}

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

// DenySelfApprovalCtx is true when the checker matches a maker and admin override is not active.
func DenySelfApprovalCtx(ctx context.Context, makers ...string) bool {
	if api.IsAdminOverride(ctx) {
		return false
	}
	return IsSelfApprovalCtx(ctx, makers...)
}
