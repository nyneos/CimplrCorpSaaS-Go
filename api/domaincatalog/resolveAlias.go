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

// ExpandPolicySubModuleCodes returns the given code plus POLICY consumer
// aliases / canonical counterparts. After a catalog rename (FORWARD_BOOKING →
// FX_CONFIRMATION) handlers may still pass the old alias while policies store
// the canonical code, or the reverse. Nil pool / query failure returns [code].
func ExpandPolicySubModuleCodes(ctx context.Context, pool *pgxpool.Pool, code string) []string {
	sm := strings.TrimSpace(code)
	if sm == "" {
		return nil
	}
	seen := map[string]struct{}{sm: {}}
	out := []string{sm}
	if pool == nil {
		return out
	}
	rows, err := pool.Query(ctx, `
		SELECT sub_module_code, alias_code
		FROM domain_catalog.sub_module_alias
		WHERE is_deleted = false
		  AND consumer_system = 'POLICY'
		  AND (sub_module_code = $1 OR alias_code = $1)`, sm)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var canonical, alias string
		if err := rows.Scan(&canonical, &alias); err != nil {
			continue
		}
		for _, c := range []string{canonical, alias} {
			c = strings.TrimSpace(c)
			if c == "" {
				continue
			}
			if _, ok := seen[c]; ok {
				continue
			}
			seen[c] = struct{}{}
			out = append(out, c)
		}
	}
	return out
}

// ExpandPolicySubModuleCodeList expands every code in the slice and de-dupes.
func ExpandPolicySubModuleCodeList(ctx context.Context, pool *pgxpool.Pool, codes []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(codes)+2)
	for _, code := range codes {
		for _, sm := range ExpandPolicySubModuleCodes(ctx, pool, code) {
			if _, ok := seen[sm]; ok {
				continue
			}
			seen[sm] = struct{}{}
			out = append(out, sm)
		}
	}
	return out
}
