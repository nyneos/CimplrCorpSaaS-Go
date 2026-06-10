package investmentdashboards

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
)

func investmentDashboardEntityScope(ctx context.Context, requestedEntityName string) (string, []string, string) {
	requestedEntityName = strings.TrimSpace(requestedEntityName)
	allowed := api.GetEntityNamesFromCtx(ctx)
	if len(allowed) == 0 {
		return "", nil, constants.ErrNoAccessibleBusinessUnit
	}
	if requestedEntityName != "" {
		for _, entityName := range allowed {
			if strings.EqualFold(strings.TrimSpace(entityName), requestedEntityName) {
				return requestedEntityName, allowed, ""
			}
		}
		return "", nil, fmt.Sprintf("Entity '%s' is not within your authorized access scope.", requestedEntityName)
	}
	return "", allowed, ""
}

func investmentDashboardAllowedEntities(ctx context.Context) ([]string, string) {
	allowed := api.GetEntityNamesFromCtx(ctx)
	if len(allowed) == 0 {
		return nil, constants.ErrNoAccessibleBusinessUnit
	}
	return allowed, ""
}
