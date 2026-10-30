package notification

import (
	"CimplrCorpSaas/api"
	"CimplrCorpSaas/api/notification/catalog"
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TriggerFX fires the notification dispatcher in a goroutine. The request context is
// detached so the send survives the response, bounded by triggerFXTimeout, and a
// panic in the dispatcher is recovered.
func TriggerFX(
	ctx context.Context,
	pool *pgxpool.Pool,
	sourceRoute string,
	correlationID string,
	payload map[string]interface{},
) {
	if pool == nil || sourceRoute == "" || correlationID == "" {
		return
	}
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				api.LogError("[FX notification] %s panic: %v", sourceRoute, rec)
			}
		}()
		sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), triggerFXTimeout)
		defer cancel()
		catalog.TriggerNotification(sendCtx, pool, sourceRoute, correlationID, payload)
	}()
}

const triggerFXTimeout = 2 * time.Minute
