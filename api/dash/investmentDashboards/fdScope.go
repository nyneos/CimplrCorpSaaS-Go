package investmentdashboards

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/constants"
)

func resolveFDDashboardEntity(ctx context.Context, requestedEntityID string) (string, string) {
	requestedEntityID = strings.TrimSpace(requestedEntityID)
	allowed := api.GetEntityIDsFromCtx(ctx)
	if len(allowed) == 0 {
		return "", constants.ErrNoAccessibleBusinessUnit
	}
	if requestedEntityID != "" {
		for _, entityID := range allowed {
			if strings.EqualFold(strings.TrimSpace(entityID), requestedEntityID) {
				return requestedEntityID, ""
			}
		}
		return "", fmt.Sprintf(constants.ErrEntityIDNotAuthorized, requestedEntityID)
	}
	scoped := make([]string, 0, len(allowed))
	for _, entityID := range allowed {
		if trimmed := strings.TrimSpace(entityID); trimmed != "" {
			scoped = append(scoped, trimmed)
		}
	}
	if len(scoped) == 0 {
		return "", constants.ErrNoAccessibleBusinessUnit
	}
	return strings.Join(scoped, ","), ""
}

func resolveFDDashboardSingleEntity(ctx context.Context, requestedEntityID string) (string, string) {
	requestedEntityID = strings.TrimSpace(requestedEntityID)
	allowed := api.GetEntityIDsFromCtx(ctx)
	if len(allowed) == 0 {
		return "", constants.ErrNoAccessibleBusinessUnit
	}
	if requestedEntityID != "" {
		for _, entityID := range allowed {
			if strings.EqualFold(strings.TrimSpace(entityID), requestedEntityID) {
				return requestedEntityID, ""
			}
		}
		return "", fmt.Sprintf(constants.ErrEntityIDNotAuthorized, requestedEntityID)
	}
	if len(allowed) == 1 {
		return strings.TrimSpace(allowed[0]), ""
	}
	return "", "entity_id is required when saving checklist status across multiple accessible entities."
}
