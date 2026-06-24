package accountingworkbench

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"CimplrCorpSaas/internal/ctxutil"
)

// Global settings cache
var (
	globalSettings *SettingsCache
	settingsMux    sync.RWMutex
)

// SettingsCache holds frequently used settings
type SettingsCache struct {
	UnitsPrecision    int
	NAVPrecision      int
	CurrencyPrecision int
	RoundingMode      string
}

// LoadSettings fetches settings from the single-row accounting_setting table
func LoadSettings(ctx context.Context, pool *pgxpool.Pool) (*SettingsCache, error) {
	query := `
		SELECT units_precision, nav_precision, currency_precision, rounding_mode
		FROM investment.accounting_setting 
		LIMIT 1
	`

	var cache SettingsCache
	err := pool.QueryRow(ctx, query).Scan(
		&cache.UnitsPrecision,
		&cache.NAVPrecision,
		&cache.CurrencyPrecision,
		&cache.RoundingMode,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load settings: %w", err)
	}

	// Update global cache
	settingsMux.Lock()
	globalSettings = &cache
	settingsMux.Unlock()

	return &cache, nil
}

// GetCachedSettings returns the cached settings (or loads if not cached)
func GetCachedSettings() *SettingsCache {
	settingsMux.RLock()
	defer settingsMux.RUnlock()

	if globalSettings == nil {
		// Return default values if not loaded
		return &SettingsCache{
			UnitsPrecision:    3,
			NAVPrecision:      4,
			CurrencyPrecision: 2,
			RoundingMode:      "BANKERS",
		}
	}
	return globalSettings
}

func accountingMFSchemeRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return accountingScopeValues(scope.Schemes, "scheme_id", "scheme_name", "isin", "internal_scheme_code")
}

func accountingMFFolioRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return accountingScopeValues(scope.Folios, "folio_id", "folio_number")
}

func accountingMFDematRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return accountingScopeValues(scope.Demats, "demat_id", "demat_account_number")
}

func accountingScopeValues(rows []map[string]string, keys ...string) []string {
	seen := make(map[string]struct{})
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		for _, key := range keys {
			value := strings.TrimSpace(row[key])
			if value == "" {
				continue
			}
			lookup := strings.ToUpper(value)
			if _, ok := seen[lookup]; ok {
				continue
			}
			seen[lookup] = struct{}{}
			values = append(values, value)
		}
	}
	return values
}

// accountingEntityNamesForScope returns scoped entity names for SQL filters.
// nil = admin / no filter (all entities).
func accountingEntityNamesForScope(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	names := make([]string, 0, len(scope.EntityNames))
	seen := make(map[string]struct{}, len(scope.EntityNames))
	for _, name := range scope.EntityNames {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		key := strings.ToUpper(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		names = append(names, trimmed)
	}
	return names
}

// accountingLookupEntityName resolves entity_id to display name.
func accountingLookupEntityName(ctx context.Context, pool *pgxpool.Pool, entityRef string) string {
	entityRef = strings.TrimSpace(entityRef)
	if entityRef == "" || pool == nil {
		return ""
	}
	var name string
	_ = pool.QueryRow(ctx, `SELECT entity_name FROM masterentitycash WHERE entity_id::text = $1 LIMIT 1`, entityRef).Scan(&name)
	if strings.TrimSpace(name) != "" {
		return strings.TrimSpace(name)
	}
	_ = pool.QueryRow(ctx, `SELECT entity_name FROM masterentity WHERE entity_id::text = $1 LIMIT 1`, entityRef).Scan(&name)
	return strings.TrimSpace(name)
}

// accountingResolveEntityName maps entity_name or entity_id to a scoped entity name.
func accountingResolveEntityName(ctx context.Context, pool *pgxpool.Pool, entityRef string) (string, string) {
	entityRef = strings.TrimSpace(entityRef)
	if entityRef == "" {
		return "", "entity_name is required"
	}
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		if name := accountingLookupEntityName(ctx, pool, entityRef); name != "" {
			return name, ""
		}
		return entityRef, ""
	}
	if scope.HasEntityNameAccess(entityRef) {
		return entityRef, ""
	}
	if scope.HasEntityAccess(entityRef) {
		if name := accountingLookupEntityName(ctx, pool, entityRef); name != "" {
			return name, ""
		}
		return entityRef, ""
	}
	if name := accountingLookupEntityName(ctx, pool, entityRef); name != "" && scope.HasEntityNameAccess(name) {
		return name, ""
	}
	return "", fmt.Sprintf("Entity '%s' is not within your authorized access scope.", entityRef)
}

// accountingEntityNameSQLFilter appends AND <entityCol> = ANY($n) for scoped users.
// Returns the SQL fragment and whether a filter arg was added.
func accountingEntityNameSQLFilter(ctx context.Context, entityCol string, argPos int) (string, []interface{}) {
	names := accountingEntityNamesForScope(ctx)
	if len(names) == 0 {
		return "", nil
	}
	return fmt.Sprintf(" AND LOWER(TRIM(COALESCE(%s,''))) = ANY(SELECT LOWER(TRIM(x)) FROM unnest($%d::text[]) AS x)", entityCol, argPos), []interface{}{names}
}

// accountingFolioEntityScopeClause filters rows by masterfolio.entity_name for scoped users.
func accountingFolioEntityScopeClause(ctx context.Context, folioAlias string, argPos int) (string, []interface{}, int) {
	names := accountingEntityNamesForScope(ctx)
	if len(names) == 0 {
		return "", nil, argPos
	}
	clause := fmt.Sprintf(" AND LOWER(TRIM(COALESCE(%s.entity_name,''))) = ANY(SELECT LOWER(TRIM(x)) FROM unnest($%d::text[]) AS x)", folioAlias, argPos)
	return clause, []interface{}{names}, argPos + 1
}

// accountingEntitiesForQuery returns entity names to iterate for portfolio reads.
// Scoped users get their entities; admin/unrestricted users get all active folio entities.
func accountingEntitiesForQuery(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	if names := accountingEntityNamesForScope(ctx); len(names) > 0 {
		return names, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT TRIM(entity_name)
		FROM investment.masterfolio
		WHERE UPPER(COALESCE(status,'')) = 'ACTIVE'
		  AND COALESCE(is_deleted, false) = false
		  AND NULLIF(TRIM(entity_name), '') IS NOT NULL
		ORDER BY 1
	`)
	if err != nil {
		return nil, fmt.Errorf("list entities: %w", err)
	}
	defer rows.Close()

	entities := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		if name = strings.TrimSpace(name); name != "" {
			entities = append(entities, name)
		}
	}
	return entities, rows.Err()
}

// RoundUnits rounds units based on precision and rounding mode
func RoundUnits(value float64, precision int, roundingMode string) float64 {
	multiplier := math.Pow(10, float64(precision))
	mode := strings.ToUpper(roundingMode)

	switch mode {
	case "BANKERS":
		// Banker's rounding (round half to even)
		return math.Round(value*multiplier) / multiplier
	case "UP":
		return math.Ceil(value*multiplier) / multiplier
	case "DOWN":
		return math.Floor(value*multiplier) / multiplier
	case "FLOOR":
		return math.Floor(value*multiplier) / multiplier
	case "CEILING":
		return math.Ceil(value*multiplier) / multiplier
	default:
		// Default to banker's rounding
		return math.Round(value*multiplier) / multiplier
	}
}

// RoundAmount rounds monetary amounts
func RoundAmount(value float64, precision int) float64 {
	multiplier := math.Pow(10, float64(precision))
	return math.Round(value*multiplier) / multiplier
}
