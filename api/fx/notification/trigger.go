package notification

import (
	"CimplrCorpSaas/api/notification/catalog"
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TriggerFX fires the notification dispatcher in a fire-and-forget goroutine.
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
	go catalog.TriggerNotification(ctx, pool, sourceRoute, correlationID, payload)
}
