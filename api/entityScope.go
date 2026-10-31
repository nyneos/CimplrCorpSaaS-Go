package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

func IsAdminOverride(ctx context.Context) bool {
	v, _ := ctx.Value("is_admin_override").(bool)
	return v
}

func EntityInScope(ctx context.Context, entity string) bool {
	if strings.TrimSpace(entity) == "" {
		return IsAdminOverride(ctx)
	}
	if IsAdminOverride(ctx) {
		return true
	}
	return IsEntityAllowed(ctx, entity)
}

func AllEntitiesInScope(ctx context.Context, entities []string) bool {
	for _, e := range entities {
		if !EntityInScope(ctx, e) {
			return false
		}
	}
	return true
}

func FilterEntitiesInScope(ctx context.Context, entities []string) []string {
	out := make([]string, 0, len(entities))
	for _, e := range entities {
		if EntityInScope(ctx, e) {
			out = append(out, e)
		}
	}
	return out
}

func RespondEntityOutOfScope(w http.ResponseWriter) {
	RespondEnvelopeError(w, http.StatusForbidden, "You do not have access to this record", "ENTITY_OUT_OF_SCOPE")
}

type RowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

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

func RequireAdminOverride(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !IsAdminOverride(r.Context()) {
			RespondEnvelopeError(w, http.StatusForbidden, "This view is limited to administrators", "ADMIN_ONLY")
			return
		}
		next.ServeHTTP(w, r)
	})
}
