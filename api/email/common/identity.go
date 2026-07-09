package emailcommon

import (
	"net/http"
	"strings"

	"CimplrCorpSaas/api"
	middlewares "CimplrCorpSaas/api/middlewares"
)

func RequestIdentity(r *http.Request, reqUserID, reqEntityID string) (userID string, userEmail string, entityID string, entityIDs []string) {
	userID = strings.TrimSpace(reqUserID)
	if userID == "" {
		userID = middlewares.GetUserIDFromContext(r.Context())
	}
	userEmail = api.GetUserEmailFromCtx(r.Context())
	if userEmail == "" {
		userEmail = userID
	}
	entityID = strings.TrimSpace(reqEntityID)
	entityIDs = api.GetEntityIDsFromCtx(r.Context())
	if entityID == "" {
		if rootID, _ := middlewares.GetRootEntityFromContext(r.Context()); rootID != "" {
			entityID = rootID
		} else if len(entityIDs) > 0 {
			entityID = entityIDs[0]
		}
	}
	if len(entityIDs) == 0 && entityID != "" {
		entityIDs = []string{entityID}
	}
	if entityIDs == nil {
		entityIDs = []string{}
	}
	return userID, userEmail, entityID, entityIDs
}

func EntityInScope(entityID string, entityIDs []string) bool {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return true
	}
	for _, id := range entityIDs {
		if strings.TrimSpace(id) == entityID {
			return true
		}
	}
	return false
}

func NullableJSON(raw []byte) interface{} {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}
