package fdMaster

import (
	"context"

	"CimplrCorpSaas/internal/schemacache"
)

// loadTableColumns returns the column set of schema.table, cached for a few minutes.
// Callers must treat the returned map as read-only.
func loadTableColumns(ctx context.Context, exec queryExecutor, schemaName string, tableName string) (map[string]bool, error) {
	return schemacache.Columns(schemaName+"."+tableName, func() (map[string]bool, error) {
		return queryTableColumns(ctx, exec, schemaName, tableName)
	})
}

func tableExistsCached(ctx context.Context, exec queryExecutor, qualified string) bool {
	return schemacache.TableExists(qualified, func() (bool, error) {
		return queryTableExists(ctx, exec, qualified)
	})
}
