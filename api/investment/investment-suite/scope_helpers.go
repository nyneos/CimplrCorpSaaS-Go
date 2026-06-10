package investmentsuite

import (
	"context"
	"strings"

	"CimplrCorpSaas/internal/ctxutil"
)

func suiteEntityNameRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return scope.EntityNames
}

func suiteMFSchemeRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return suiteScopeValues(scope.Schemes, "scheme_id", "scheme_name", "isin", "internal_scheme_code")
}

func suiteMFFolioRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return suiteScopeValues(scope.Folios, "folio_id", "folio_number")
}

func suiteMFDematRefs(ctx context.Context) []string {
	scope := ctxutil.FromContext(ctx)
	if scope.IsAdminOverride {
		return nil
	}
	return suiteScopeValues(scope.Demats, "demat_id", "demat_account_number")
}

func suiteScopeValues(rows []map[string]string, keys ...string) []string {
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
