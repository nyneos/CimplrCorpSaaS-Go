package domaincatalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ResolveSubModuleAlias resolves a canonical domain_catalog.sub_module_code to
// one consumer system's alias code — e.g. sub_module_code "FD_BOOKING" +
// consumer_system "DASHBOARD" → "fdBooking" (the dashboard-builder data
// source key). Used by the DMS generation worker to turn a rule's
// domain_catalog scope into the data source FetchSourceData understands,
// without a separate DMS-local mapping to drift out of sync.
func ResolveSubModuleAlias(ctx context.Context, pool *pgxpool.Pool, subModuleCode, consumerSystem string) (string, error) {
	sm := strings.TrimSpace(subModuleCode)
	cs := normalizeConsumer(consumerSystem)
	if sm == "" || cs == "" {
		return "", fmt.Errorf("sub_module_code and consumer_system are required")
	}
	var alias string
	err := pool.QueryRow(ctx, `
		SELECT alias_code FROM domain_catalog.sub_module_alias
		WHERE sub_module_code = $1 AND consumer_system = $2 AND is_deleted = false
		LIMIT 1`, sm, cs,
	).Scan(&alias)
	if err != nil {
		return "", fmt.Errorf("no %s alias for sub_module_code %q: %w", cs, sm, err)
	}
	return alias, nil
}
