package runtime

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NOTIFICATION field_alias rows for a sub_module (alias_key keyed by field_code).
type notifAliasRow struct {
	FieldCode string
	AliasKey  string
}

var (
	notifAliasCacheMu  sync.RWMutex
	notifAliasCache    = map[string]cachedNotifAliases{}
	notifAliasCacheTTL = 5 * time.Minute
)

type cachedNotifAliases struct {
	rows []notifAliasRow
	at   time.Time
}

// LoadSubModuleNotificationAliases returns NOTIFICATION alias_key per field_code.
// Falls back to field_code when no alias row exists (caller still gets a key).
func LoadSubModuleNotificationAliases(ctx context.Context, pool *pgxpool.Pool, subModuleCode string) ([]notifAliasRow, error) {
	sm := strings.TrimSpace(subModuleCode)
	if sm == "" || pool == nil {
		return nil, nil
	}

	notifAliasCacheMu.RLock()
	if c, ok := notifAliasCache[sm]; ok && len(c.rows) > 0 && time.Since(c.at) < notifAliasCacheTTL {
		out := c.rows
		notifAliasCacheMu.RUnlock()
		return out, nil
	}
	notifAliasCacheMu.RUnlock()

	rows, err := pool.Query(ctx, `
		SELECT f.field_code, COALESCE(NULLIF(TRIM(a.alias_key), ''), f.field_code) AS alias_key
		FROM domain_catalog.field f
		LEFT JOIN domain_catalog.field_alias a
		  ON a.field_id = f.field_id
		 AND a.consumer_system = 'NOTIFICATION'
		 AND a.is_deleted = false
		WHERE f.sub_module_code = $1
		  AND f.is_deleted = false
		ORDER BY f.sort_order, f.field_code, a.sort_order`, sm)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	seen := map[string]struct{}{}
	out := make([]notifAliasRow, 0)
	for rows.Next() {
		var code, alias string
		if err := rows.Scan(&code, &alias); err != nil {
			return nil, err
		}
		code = strings.TrimSpace(code)
		alias = strings.TrimSpace(alias)
		if code == "" || alias == "" {
			continue
		}
		// Prefer first alias_key per field_code (scalar over list duplicates).
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, notifAliasRow{FieldCode: code, AliasKey: alias})
	}

	notifAliasCacheMu.Lock()
	notifAliasCache[sm] = cachedNotifAliases{rows: out, at: time.Now()}
	notifAliasCacheMu.Unlock()
	return out, nil
}

// BuildDispatchPayload maps Fields (field_code → value) onto NOTIFICATION alias_key
// keys so template UI snippets ({{entity_id}}) resolve. extras are merged last
// (Action, PolicyCode, TraceID, …) and win on key collision.
func BuildDispatchPayload(
	ctx context.Context,
	pool *pgxpool.Pool,
	subModuleCode string,
	fields map[string]interface{},
	extras map[string]interface{},
) map[string]interface{} {
	out := map[string]interface{}{}
	if len(fields) > 0 {
		aliases, err := LoadSubModuleNotificationAliases(ctx, pool, subModuleCode)
		if err != nil || len(aliases) == 0 {
			// Fallback: emit field_code keys so templates still have something.
			for k, v := range fields {
				key := strings.TrimSpace(k)
				if key == "" || v == nil {
					continue
				}
				s := stringifyCDMValue(v)
				if s == "" {
					continue
				}
				out[key] = s
			}
		} else {
			byCode := make(map[string]interface{}, len(fields))
			for k, v := range fields {
				byCode[strings.TrimSpace(k)] = v
			}
			for _, row := range aliases {
				raw, ok := byCode[row.FieldCode]
				if !ok || raw == nil {
					continue
				}
				s := stringifyCDMValue(raw)
				if s == "" {
					continue
				}
				out[row.AliasKey] = s
			}
		}
	}
	for k, v := range extras {
		key := strings.TrimSpace(k)
		if key == "" || v == nil {
			continue
		}
		out[key] = v
	}
	return out
}
