package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

// IsAdminOverride reports whether the request runs with the admin entity override.
func IsAdminOverride(ctx context.Context) bool {
	v, _ := ctx.Value("is_admin_override").(bool)
	return v
}

// EntityInScope reports whether an entity id or name belongs to the caller's scope.
// An empty entity or an empty scope is out of scope.
func EntityInScope(ctx context.Context, entity string) bool {
	if strings.TrimSpace(entity) == "" {
		return IsAdminOverride(ctx)
	}
	if IsAdminOverride(ctx) {
		return true
	}
	return IsEntityAllowed(ctx, entity)
}

// AllEntitiesInScope reports whether every entity is in the caller's scope.
func AllEntitiesInScope(ctx context.Context, entities []string) bool {
	for _, e := range entities {
		if !EntityInScope(ctx, e) {
			return false
		}
	}
	return true
}

// FilterEntitiesInScope keeps the entities that are in the caller's scope.
func FilterEntitiesInScope(ctx context.Context, entities []string) []string {
	out := make([]string, 0, len(entities))
	for _, e := range entities {
		if EntityInScope(ctx, e) {
			out = append(out, e)
		}
	}
	return out
}

// RespondEntityOutOfScope writes the standard 403 for a record outside the caller's scope.
func RespondEntityOutOfScope(w http.ResponseWriter) {
	RespondEnvelopeError(w, http.StatusForbidden, "You do not have access to this record", "ENTITY_OUT_OF_SCOPE")
}

// RowQuerier is satisfied by *pgxpool.Pool and pgx.Tx.
type RowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// RecordsInScope runs query (which must select one entity id or name per row
// for the given args) and reports whether every row is in the caller's scope.
func RecordsInScope(ctx context.Context, db RowQuerier, query string, args ...any) (bool, error) {
	if IsAdminOverride(ctx) {
		return true, nil
	}
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var entity *string
		if err := rows.Scan(&entity); err != nil {
			return false, err
		}
		if entity == nil || !EntityInScope(ctx, *entity) {
			return false, nil
		}
	}
	return true, rows.Err()
}

// RequireRecordsInScope writes a 403 (or 500 on a lookup failure) and returns
// false unless every record selected by query is in the caller's scope.
func RequireRecordsInScope(ctx context.Context, w http.ResponseWriter, db RowQuerier, query string, args ...any) bool {
	ok, err := RecordsInScope(ctx, db, query, args...)
	if err != nil {
		LogErrorForResponse(w, "entity scope lookup failed: %v", err)
		RespondEnvelopeError(w, http.StatusInternalServerError, "Failed to verify record access", "")
		return false
	}
	if !ok {
		RespondEntityOutOfScope(w)
		return false
	}
	return true
}

// RequireAdminOverride allows the request only for callers running with the
// admin entity override. It must run after the session middleware.
func RequireAdminOverride(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !IsAdminOverride(r.Context()) {
			RespondEnvelopeError(w, http.StatusForbidden, "This view is limited to administrators", "ADMIN_ONLY")
			return
		}
		next.ServeHTTP(w, r)
	})
}
