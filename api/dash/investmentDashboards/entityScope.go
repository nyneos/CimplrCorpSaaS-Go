package investmentdashboards

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
	"CimplrCorpSaas/internal/ctxutil"
)

func normalizeEntityNameList(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	out := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		key := strings.ToLower(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

// SQLParamsEntityScope matches entity columns against params CTE (p.entity_filter, p.allowed_entities).
func SQLParamsEntityScope(entityCol string) string {
	return `(p.entity_filter IS NULL OR LOWER(TRIM(COALESCE(` + entityCol + `, ''))) = LOWER(TRIM(p.entity_filter)))
AND (p.allowed_entities IS NULL OR LOWER(TRIM(COALESCE(` + entityCol + `, ''))) = ANY(
  SELECT LOWER(TRIM(x)) FROM unnest(p.allowed_entities) AS x
))`
}

// SQLAndParamsEntityScope is the same predicate prefixed with AND for WHERE clauses.
func SQLAndParamsEntityScope(entityCol string) string {
	return `AND ` + SQLParamsEntityScope(entityCol)
}

func investmentDashboardEntityScope(ctx context.Context, requestedEntityName string) (string, []string, string) {
	requestedEntityName = strings.TrimSpace(requestedEntityName)
	scope := ctxutil.FromContext(ctx)
	allowed := normalizeEntityNameList(ctxutil.AllowedEntityNames(ctx))
	allowedIDs := api.GetEntityIDsFromCtx(ctx)
	if len(allowed) == 0 && !scope.IsAdminOverride {
		return "", nil, constants.ErrNoAccessibleBusinessUnit
	}
	if requestedEntityName != "" {
		for _, entityName := range allowed {
			if strings.EqualFold(strings.TrimSpace(entityName), requestedEntityName) {
				return requestedEntityName, allowed, ""
			}
		}
		for _, entityID := range allowedIDs {
			if strings.EqualFold(strings.TrimSpace(entityID), requestedEntityName) {
				return requestedEntityName, allowed, ""
			}
		}
		return "", nil, fmt.Sprintf("Entity '%s' is not within your authorized access scope.", requestedEntityName)
	}
	return "", allowed, ""
}

func investmentDashboardAllowedEntities(ctx context.Context) ([]string, string) {
	allowed := ctxutil.AllowedEntityNames(ctx)
	if len(allowed) == 0 && !ctxutil.FromContext(ctx).IsAdminOverride {
		return nil, constants.ErrNoAccessibleBusinessUnit
	}
	return allowed, ""
}
