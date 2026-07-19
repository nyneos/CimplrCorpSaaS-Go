package payablerecievable

import (
	"context"
	"strings"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PayRecMasterIDMaps struct {
	entityByName       map[string]string
	counterpartyByName map[string]string
}

func LoadPayRecMasterIDMaps(ctx context.Context, pool *pgxpool.Pool) PayRecMasterIDMaps {
	maps := PayRecMasterIDMaps{
		entityByName:       make(map[string]string),
		counterpartyByName: make(map[string]string),
	}

	entityIDs := api.CtxEntityIDs(ctx)
	if names, ok := ctx.Value(api.BusinessUnitsKey).([]string); ok {
		for i, name := range names {
			key := strings.ToLower(strings.TrimSpace(name))
			if key == "" {
				continue
			}
			if i < len(entityIDs) {
				if id := strings.TrimSpace(entityIDs[i]); id != "" {
					maps.entityByName[key] = id
				}
			}
		}
	}

	if counterparties, ok := ctx.Value("ApprovedCounterparties").([]map[string]string); ok {
		for _, row := range counterparties {
			name := strings.ToLower(strings.TrimSpace(row["counterparty_name"]))
			id := strings.TrimSpace(row["counterparty_id"])
			if name != "" && id != "" {
				maps.counterpartyByName[name] = id
			}
		}
	}

	if pool == nil {
		return maps
	}

	entityRows, err := pool.Query(ctx, `
		SELECT COALESCE(entity_id::text, ''), COALESCE(entity_name, '')
		FROM masterentitycash
		WHERE COALESCE(is_deleted, false) = false`)
	if err == nil {
		defer entityRows.Close()
		for entityRows.Next() {
			var id, name string
			if entityRows.Scan(&id, &name) != nil {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(name))
			if key != "" && strings.TrimSpace(id) != "" && maps.entityByName[key] == "" {
				maps.entityByName[key] = strings.TrimSpace(id)
			}
		}
	}

	counterpartyRows, err := pool.Query(ctx, `
		SELECT COALESCE(counterparty_id::text, ''), COALESCE(counterparty_name, '')
		FROM mastercounterparty
		WHERE COALESCE(is_deleted, false) = false`)
	if err == nil {
		defer counterpartyRows.Close()
		for counterpartyRows.Next() {
			var id, name string
			if counterpartyRows.Scan(&id, &name) != nil {
				continue
			}
			key := strings.ToLower(strings.TrimSpace(name))
			if key != "" && strings.TrimSpace(id) != "" && maps.counterpartyByName[key] == "" {
				maps.counterpartyByName[key] = strings.TrimSpace(id)
			}
		}
	}

	return maps
}

func EnrichPayRecRowIDs(
	maps PayRecMasterIDMaps,
	entityName, counterpartyName, entityID, counterpartyID string,
) (string, string) {
	if strings.TrimSpace(entityID) == "" {
		entityID = maps.entityByName[strings.ToLower(strings.TrimSpace(entityName))]
	}
	if strings.TrimSpace(counterpartyID) == "" {
		counterpartyID = maps.counterpartyByName[strings.ToLower(strings.TrimSpace(counterpartyName))]
	}
	return strings.TrimSpace(entityID), strings.TrimSpace(counterpartyID)
}
