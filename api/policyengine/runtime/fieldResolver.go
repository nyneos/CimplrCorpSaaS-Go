package runtime

import (
	"context"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ResolveTriggerApprovalMatrix evaluates the given scope purely to recover the
// approval matrix a TriggerApproval breach would pin. It never blocks: callers
// use it at approve time to open an instance for a record that was submitted
// before the policy (or before instance creation) existed.
func ResolveTriggerApprovalMatrix(ctx context.Context, pool *pgxpool.Pool, in EnforceInput) string {
	if pool == nil || !PolicyChecksEnabled() {
		return ""
	}
	out := EnforceDetailed(ctx, nil, pool, in)
	return strings.TrimSpace(out.Result.TriggerApprovalMatrixID)
}

type RecordFieldResolver func(ctx context.Context, pool *pgxpool.Pool, recordID string) (map[string]interface{}, error)

var (
	recordFieldResolversMu sync.RWMutex
	recordFieldResolvers   = map[string]RecordFieldResolver{}
)

func RegisterRecordFieldResolver(subModuleCode string, resolver RecordFieldResolver) {
	code := strings.ToUpper(strings.TrimSpace(subModuleCode))
	if code == "" || resolver == nil {
		return
	}
	recordFieldResolversMu.Lock()
	recordFieldResolvers[code] = resolver
	recordFieldResolversMu.Unlock()
}

func lookupRecordFieldResolver(subModuleCode string) RecordFieldResolver {
	code := strings.ToUpper(strings.TrimSpace(subModuleCode))
	if code == "" {
		return nil
	}
	recordFieldResolversMu.RLock()
	resolver := recordFieldResolvers[code]
	recordFieldResolversMu.RUnlock()
	return resolver
}

// ResolveRecordVariables maps a persisted record onto CDM variables using the
// same field builder its handler uses, so a pre-flight check evaluates the
// identical variable set the handler will.
func ResolveRecordVariables(ctx context.Context, pool *pgxpool.Pool, subModuleCode, recordID string) map[string]string {
	if pool == nil || strings.TrimSpace(recordID) == "" {
		return nil
	}
	resolver := lookupRecordFieldResolver(subModuleCode)
	if resolver == nil {
		return nil
	}
	fields, err := resolver(ctx, pool, strings.TrimSpace(recordID))
	if err != nil || len(fields) == 0 {
		return nil
	}
	vars, err := BuildVariablesFromCatalog(ctx, pool, subModuleCode, fields, nil)
	if err != nil {
		return nil
	}
	return vars
}
