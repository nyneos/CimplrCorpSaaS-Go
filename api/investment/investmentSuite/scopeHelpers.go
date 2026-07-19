package investmentsuite

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/internal/ctxutil"

	"github.com/jackc/pgx/v5/pgxpool"
)

func suiteEntityNameRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return scope.EntityNames
}

func suiteMFSchemeRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return suiteScopeValues(scope.Schemes, "scheme_id", "scheme_name", "isin", "internal_scheme_code")
}

func suiteMFFolioRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return suiteScopeValues(scope.Folios, "folio_id", "folio_number")
}

func suiteMFDematRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return suiteScopeValues(scope.Demats, "demat_id", "demat_account_number")
}

func suiteScopeValues(rows []map[string]string, keys ...string) []string {
	seen := make(map[string]struct{})
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		for _, key := range keys {
			value := strings.TrimSpace(row[key])
			if value == "" {
				continue
			}
			lookup := strings.ToUpper(value)
			if _, ok := seen[lookup]; ok {
				continue
			}
			seen[lookup] = struct{}{}
			values = append(values, value)
		}
	}
	return values
}

// suiteLookupEntityName resolves entity_id to display name from master tables.
func suiteLookupEntityName(ctx context.Context, pool *pgxpool.Pool, entityRef string) string {
	entityRef = strings.TrimSpace(entityRef)
	if entityRef == "" || pool == nil {
		return ""
	}
	var name string
	_ = pool.QueryRow(ctx, `SELECT entity_name FROM masterentitycash WHERE entity_id::text = $1 LIMIT 1`, entityRef).Scan(&name)
	if strings.TrimSpace(name) != "" {
		return strings.TrimSpace(name)
	}
	_ = pool.QueryRow(ctx, `SELECT entity_name FROM masterentity WHERE entity_id::text = $1 LIMIT 1`, entityRef).Scan(&name)
	return strings.TrimSpace(name)
}

// suiteResolveEntityName maps entity_name or entity_id to a scoped entity name.
// Returns an error message when the caller is not allowed to access the entity.
func suiteResolveEntityName(ctx context.Context, pool *pgxpool.Pool, entityRef string) (string, string) {
	entityRef = strings.TrimSpace(entityRef)
	if entityRef == "" {
		return "", "entity_name is required"
	}

	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		if name := suiteLookupEntityName(ctx, pool, entityRef); name != "" {
			return name, ""
		}
		return entityRef, ""
	}

	if scope.HasEntityNameAccess(entityRef) {
		return entityRef, ""
	}
	if scope.HasEntityAccess(entityRef) {
		if name := suiteLookupEntityName(ctx, pool, entityRef); name != "" {
			return name, ""
		}
		return entityRef, ""
	}

	if name := suiteLookupEntityName(ctx, pool, entityRef); name != "" && scope.HasEntityNameAccess(name) {
		return name, ""
	}

	return "", fmt.Sprintf("Entity '%s' is not within your authorized access scope.", entityRef)
}
