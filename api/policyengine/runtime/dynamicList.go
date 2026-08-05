package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api"

	"github.com/jackc/pgx/v5/pgxpool"
)

// dynamicListCacheTTL is the in-process refresh window for resolved Dynamic
// list masters. BRD §15.5.31 budgets ~300ms for evaluation at 200 concurrent
// users — re-querying counterparties/banks on every check would blow that.
const dynamicListCacheTTL = 60 * time.Second

// dynamicListCacheMaxEntries caps distinct ref_code entries retained in memory.
const dynamicListCacheMaxEntries = 32

// dynamicListQuery is a vetted, code-reviewed SELECT that returns a single
// text column of membership values. No user-authored SQL is ever executed.
type dynamicListQuery struct {
	sql string
}

// dynamicListResolvers maps ref_code → vetted query. Keys must match rows
// seeded in policyengine_svc.dynamic_list_registry.
var dynamicListResolvers = map[string]dynamicListQuery{
	"APPROVED_COUNTERPARTIES": {
		sql: `
			SELECT DISTINCT TRIM(counterparty_code)
			  FROM mastercounterparty
			 WHERE COALESCE(is_deleted, false) = false
			   AND LOWER(TRIM(status)) = 'active'
			   AND NULLIF(TRIM(counterparty_code), '') IS NOT NULL
			 ORDER BY 1`,
	},
	"APPROVED_BANKS": {
		sql: `
			SELECT DISTINCT TRIM(bank_short_name)
			  FROM masterbank
			 WHERE COALESCE(is_deleted, false) = false
			   AND LOWER(TRIM(active_status)) = 'active'
			   AND NULLIF(TRIM(bank_short_name), '') IS NOT NULL
			 ORDER BY 1`,
	},
	"PERMITTED_INSTRUMENTS": {
		sql: `
			SELECT DISTINCT TRIM(internal_scheme_code)
			  FROM investment.masterscheme
			 WHERE COALESCE(is_deleted, false) = false
			   AND LOWER(TRIM(status)) = 'active'
			   AND NULLIF(TRIM(internal_scheme_code), '') IS NOT NULL
			 ORDER BY 1`,
	},
	"APPROVED_CURRENCIES": {
		sql: `
			SELECT DISTINCT TRIM(currency_code)
			  FROM mastercurrency
			 WHERE COALESCE(is_deleted, false) = false
			   AND LOWER(TRIM(status)) = 'active'
			   AND NULLIF(TRIM(currency_code), '') IS NOT NULL
			 ORDER BY 1`,
	},
	"APPROVED_ENTITIES": {
		sql: `
			SELECT DISTINCT TRIM(m.entity_name)
			  FROM masterentitycash m
			  LEFT JOIN LATERAL (
			       SELECT a.processing_status
			         FROM auditactionentity a
			        WHERE a.entity_id = m.entity_id
			          AND a.actiontype IN ('CREATE','EDIT','DELETE')
			        ORDER BY a.requested_at DESC
			        LIMIT 1
			  ) ma ON TRUE
			 WHERE COALESCE(m.is_deleted, false) = false
			   AND m.active_status = 'Active'
			   AND COALESCE(ma.processing_status, 'REJECTED') = 'APPROVED'
			   AND NULLIF(TRIM(m.entity_name), '') IS NOT NULL
			 ORDER BY 1`,
	},
	"APPROVED_ENTITY_IDS": {
		sql: `
			SELECT DISTINCT TRIM(m.entity_id::text)
			  FROM masterentitycash m
			  LEFT JOIN LATERAL (
			       SELECT a.processing_status
			         FROM auditactionentity a
			        WHERE a.entity_id = m.entity_id
			          AND a.actiontype IN ('CREATE','EDIT','DELETE')
			        ORDER BY a.requested_at DESC
			        LIMIT 1
			  ) ma ON TRUE
			 WHERE COALESCE(m.is_deleted, false) = false
			   AND m.active_status = 'Active'
			   AND COALESCE(ma.processing_status, 'REJECTED') = 'APPROVED'
			   AND NULLIF(TRIM(m.entity_id::text), '') IS NOT NULL
			 ORDER BY 1`,
	},
}

type dynamicListCacheEntry struct {
	values    []string
	expiresAt time.Time
}

var (
	dynamicListCacheMu sync.Mutex
	dynamicListCache   = make(map[string]dynamicListCacheEntry)

	// dynamicListNow is overridable in tests for TTL expiry.
	dynamicListNow = time.Now

	// dynamicListQueryCount increments on each live DB resolve (not cache hits).
	dynamicListQueryCount int64
)

// IsKnownDynamicListRef reports whether ref_code has a Go-side vetted query.
func IsKnownDynamicListRef(refCode string) bool {
	_, ok := dynamicListResolvers[strings.TrimSpace(refCode)]
	return ok
}

// KnownDynamicListRefs returns the sorted list of implemented ref codes.
func KnownDynamicListRefs() []string {
	out := make([]string, 0, len(dynamicListResolvers))
	for code := range dynamicListResolvers {
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

// resolveDynamicList materialises membership values for a Dynamic list ref.
// On any failure or zero rows it returns an empty slice and an error so the
// evaluator's existing Dynamic+empty guard produces ERROR → HardBlock.
func resolveDynamicList(ctx context.Context, pool *pgxpool.Pool, refCode string) ([]string, error) {
	ref := strings.TrimSpace(refCode)
	if ref == "" {
		api.LogError("policy dynamic list: empty ref_code")
		return []string{}, fmt.Errorf("dynamic list ref is empty")
	}

	if vals, ok := dynamicListCacheGet(ref); ok {
		return vals, nil
	}

	q, ok := dynamicListResolvers[ref]
	if !ok {
		api.LogError("policy dynamic list: unknown ref %q", ref)
		return []string{}, fmt.Errorf("unknown dynamic list ref %q", ref)
	}
	if pool == nil {
		api.LogError("policy dynamic list: nil pool for ref %q", ref)
		return []string{}, fmt.Errorf("dynamic list pool unavailable")
	}

	vals, err := queryDynamicList(ctx, pool, ref, q.sql)
	if err != nil {
		api.LogError("policy dynamic list: query failed for ref %q: %v", ref, err)
		return []string{}, err
	}
	if len(vals) == 0 {
		api.LogError("policy dynamic list: zero rows for ref %q", ref)
		return []string{}, fmt.Errorf("dynamic list %q resolved to zero rows", ref)
	}

	dynamicListCachePut(ref, vals)
	return copyStrings(vals), nil
}

func queryDynamicList(ctx context.Context, pool *pgxpool.Pool, ref, sql string) ([]string, error) {
	dynamicListCacheMu.Lock()
	dynamicListQueryCount++
	dynamicListCacheMu.Unlock()

	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_ = ref
	return out, nil
}

func dynamicListCacheGet(ref string) ([]string, bool) {
	dynamicListCacheMu.Lock()
	defer dynamicListCacheMu.Unlock()
	ent, ok := dynamicListCache[ref]
	if !ok || !dynamicListNow().Before(ent.expiresAt) {
		if ok {
			delete(dynamicListCache, ref)
		}
		return nil, false
	}
	return copyStrings(ent.values), true
}

func dynamicListCachePut(ref string, vals []string) {
	dynamicListCacheMu.Lock()
	defer dynamicListCacheMu.Unlock()
	if len(dynamicListCache) >= dynamicListCacheMaxEntries {
		// Drop one arbitrary expired-or-any entry to stay bounded.
		now := dynamicListNow()
		evicted := false
		for k, e := range dynamicListCache {
			if !now.Before(e.expiresAt) {
				delete(dynamicListCache, k)
				evicted = true
				break
			}
		}
		if !evicted {
			for k := range dynamicListCache {
				delete(dynamicListCache, k)
				break
			}
		}
	}
	dynamicListCache[ref] = dynamicListCacheEntry{
		values:    copyStrings(vals),
		expiresAt: dynamicListNow().Add(dynamicListCacheTTL),
	}
}

func copyStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func clearDynamicListCache() {
	dynamicListCacheMu.Lock()
	defer dynamicListCacheMu.Unlock()
	dynamicListCache = make(map[string]dynamicListCacheEntry)
	dynamicListQueryCount = 0
}

func dynamicListQueriesExecuted() int64 {
	dynamicListCacheMu.Lock()
	defer dynamicListCacheMu.Unlock()
	return dynamicListQueryCount
}
