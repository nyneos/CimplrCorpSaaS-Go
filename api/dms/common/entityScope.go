package common

import (
	"fmt"
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/internal/ctxutil"
)

// AppendEntityScopeFilter keeps org-wide rows (NULL entity_id) plus rows whose
// entity_id is in the caller's prevalidated entity list. Admin override and an
// empty entity list (legacy "all entities") skip the filter.
func AppendEntityScopeFilter(
	query string,
	args []interface{},
	entityCol string,
	scope ctxutil.RequestScope,
) (string, []interface{}) {
	if scope.IsAdminOverride || len(scope.EntityIDs) == 0 {
		return query, args
	}
	args = append(args, scope.EntityIDs)
	n := len(args)
	query += fmt.Sprintf(
		" AND (%s IS NULL OR %s = ANY($%d))",
		entityCol, entityCol, n,
	)
	return query, args
}

// RequireEntityAccess rejects when the caller cannot see entityID.
// Empty entityID (org-wide) is always allowed.
func RequireEntityAccess(w http.ResponseWriter, scope ctxutil.RequestScope, entityID string) bool {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return true
	}
	if scope.HasEntityAccess(entityID) {
		return true
	}
	api.RespondEnvelopeError(w, http.StatusForbidden, "entity is outside your access scope", "DMS_ENTITY_FORBIDDEN")
	return false
}
