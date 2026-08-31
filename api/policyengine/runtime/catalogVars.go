package runtime

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"CimplrCorpSaas/api/domaincatalog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Generic domain_catalog → CDM variable builder.
// Prefer this for new modules; FD booking still uses BuildFdBookingVariables
// (synonyms + defaults) which delegates path loading here.

type cdmFieldRow struct {
	FieldCode string
	CDMPath   string
}

var (
	cdmPathCacheMu  sync.RWMutex
	cdmPathCache    = map[string]cachedCDMPaths{}
	cdmPathCacheTTL = 5 * time.Minute
)

type cachedCDMPaths struct {
	rows []cdmFieldRow
	at   time.Time
}

// LoadSubModuleCDMPaths reads field_code → cdm_path for a domain_catalog sub_module.
// Empty cdm_path rows are skipped. Returns nil,nil when none configured (caller decides).
func LoadSubModuleCDMPaths(ctx context.Context, pool *pgxpool.Pool, subModuleCode string) ([]cdmFieldRow, error) {
	sm := strings.TrimSpace(subModuleCode)
	if sm == "" {
		return nil, fmt.Errorf("sub_module_code required")
	}
	if pool == nil {
		return nil, nil
	}

	cdmPathCacheMu.RLock()
	if c, ok := cdmPathCache[sm]; ok && len(c.rows) > 0 && time.Since(c.at) < cdmPathCacheTTL {
		out := c.rows
		cdmPathCacheMu.RUnlock()
		return out, nil
	}
	cdmPathCacheMu.RUnlock()

	codes := domaincatalog.ExpandPolicySubModuleCodes(ctx, pool, sm)
	rows, err := pool.Query(ctx, `
		SELECT field_code, cdm_path
		FROM domain_catalog.field
		WHERE sub_module_code = ANY($1::text[])
		  AND cdm_path IS NOT NULL
		  AND TRIM(cdm_path) <> ''
		  AND is_deleted = false
		ORDER BY sort_order, field_code`, codes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]cdmFieldRow, 0)
	seen := map[string]struct{}{}
	for rows.Next() {
		var code, path string
		if err := rows.Scan(&code, &path); err != nil {
			return nil, err
		}
		if _, dup := seen[code]; dup {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, cdmFieldRow{FieldCode: code, CDMPath: path})
	}

	cdmPathCacheMu.Lock()
	cdmPathCache[sm] = cachedCDMPaths{rows: out, at: time.Now()}
	cdmPathCacheMu.Unlock()
	return out, nil
}

func stringifyCDMValue(v interface{}) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32)
	case *float64:
		if t == nil {
			return ""
		}
		return strconv.FormatFloat(*t, 'g', -1, 64)
	case *float32:
		if t == nil {
			return ""
		}
		return strconv.FormatFloat(float64(*t), 'g', -1, 32)
	case *string:
		if t == nil {
			return ""
		}
		return strings.TrimSpace(*t)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case bool:
		if t {
			return "true"
		}
		return "false"
	case time.Time:
		if t.IsZero() {
			return ""
		}
		return t.UTC().Format(time.RFC3339)
	default:
		s := strings.TrimSpace(fmt.Sprint(t))
		if s == "<nil>" {
			return ""
		}
		return s
	}
}

// BuildVariablesFromCatalog maps a field map onto CDM keys using domain_catalog
// for the given sub_module. Synonyms optionally expand API/DB names onto field_codes.
func BuildVariablesFromCatalog(
	ctx context.Context,
	pool *pgxpool.Pool,
	subModuleCode string,
	fields map[string]interface{},
	synonyms map[string]string,
) (map[string]string, error) {
	norm := make(map[string]interface{}, len(fields)+8)
	for k, v := range fields {
		key := strings.TrimSpace(k)
		if key == "" || v == nil {
			continue
		}
		norm[key] = v
		if synonyms != nil {
			if canon, ok := synonyms[key]; ok {
				if _, exists := norm[canon]; !exists {
					norm[canon] = v
				}
			}
		}
	}

	paths, err := LoadSubModuleCDMPaths(ctx, pool, subModuleCode)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(paths))
	for _, row := range paths {
		raw, ok := norm[row.FieldCode]
		if !ok || raw == nil {
			continue
		}
		s := stringifyCDMValue(raw)
		if s == "" {
			continue
		}
		out[row.CDMPath] = s
	}
	return out, nil
}
