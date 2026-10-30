package api

import (
	"net/http"
	"os"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Permission actions stored in public.permissions.action.
const (
	PermView    = "hasAccess"
	PermCreate  = "showCreateButton"
	PermEdit    = "showEditButton"
	PermDelete  = "showDeleteButton"
	PermApprove = "showApproveButton"
	PermReject  = "showRejectButton"
)

// permissionMode is PERMISSION_ENFORCE: "on" blocks, "off" skips, anything else logs only.
func permissionMode() string {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("PERMISSION_ENFORCE"))) {
	case "on", "true", "enforce":
		return "on"
	case "off", "false":
		return "off"
	}
	return "log"
}

// RequirePermission allows the request when one of the session user's roles has
// the page action granted in role_permissions. It must run after the session
// middleware. Admin override users always pass.
func RequirePermission(db *pgxpool.Pool, page, action string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mode := permissionMode()
			if mode == "off" || api.IsAdminOverride(r.Context()) {
				next.ServeHTTP(w, r)
				return
			}
			userID := GetUserIDFromContext(r.Context())
			var allowed bool
			err := db.QueryRow(r.Context(), `
				SELECT EXISTS (
					SELECT 1
					FROM public.user_roles ur
					JOIN public.role_permissions rp ON rp.role_id::text = ur.role_id::text
					JOIN public.permissions p ON p.id = rp.permission_id
					WHERE ur.user_id::text = $1
					  AND COALESCE(ur.is_deleted, false) = false
					  AND p.page_name = $2
					  AND p.action = $3
					  AND rp.allowed = true
				)`, userID, page, action).Scan(&allowed)
			if err != nil {
				logger.LogError("[Permission] lookup failed user=%s page=%s action=%s path=%s: %v", userID, page, action, r.URL.Path, err)
				if mode == "on" {
					api.RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to verify permission", "PERMISSION_LOOKUP_FAILED")
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			if !allowed {
				logger.LogInfo("[Permission] denied user=%s page=%s action=%s path=%s mode=%s", userID, page, action, r.URL.Path, mode)
				if mode == "on" {
					api.RespondEnvelopeError(w, http.StatusForbidden, "You do not have permission for this action", "PERMISSION_DENIED")
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
