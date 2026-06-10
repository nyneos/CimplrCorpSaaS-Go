package fdBooking

import (
	"context"
	"fmt"
	"strings"

	"CimplrCorpSaas/internal/ctxutil"
)

type fdBookingReadScope struct {
	isAdmin     bool
	entityIDs   []string
	bankIDs     []string
	bankNamesLC []string
}

func fdBookingScopeFromContext(ctx context.Context) fdBookingReadScope {
	scope := ctxutil.FromContext(ctx)
	return fdBookingReadScope{
		isAdmin:     scope.IsAdminOverride,
		entityIDs:   trimStrings(scope.EntityIDs),
		bankIDs:     trimStrings(scope.BankIDs()),
		bankNamesLC: lowerBankNames(scope.Banks),
	}
}

func fdBookingScopeWhere(ctx context.Context, bookingAlias string, argIndex int) (string, []interface{}) {
	scope := fdBookingScopeFromContext(ctx)
	if scope.isAdmin {
		return "", nil
	}

	var (
		clauses []string
		args    []interface{}
	)
	if len(scope.entityIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf(" AND %s.entity_id = ANY($%d::text[])", bookingAlias, argIndex))
		args = append(args, scope.entityIDs)
		argIndex++
	}
	switch {
	case len(scope.bankIDs) > 0 && len(scope.bankNamesLC) > 0:
		clauses = append(clauses, fmt.Sprintf(" AND (%s.bank_id = ANY($%d::text[]) OR LOWER(TRIM(COALESCE(%s.bank_name,''))) = ANY($%d::text[]))", bookingAlias, argIndex, bookingAlias, argIndex+1))
		args = append(args, scope.bankIDs, scope.bankNamesLC)
	case len(scope.bankIDs) > 0:
		clauses = append(clauses, fmt.Sprintf(" AND %s.bank_id = ANY($%d::text[])", bookingAlias, argIndex))
		args = append(args, scope.bankIDs)
	case len(scope.bankNamesLC) > 0:
		clauses = append(clauses, fmt.Sprintf(" AND LOWER(TRIM(COALESCE(%s.bank_name,''))) = ANY($%d::text[])", bookingAlias, argIndex))
		args = append(args, scope.bankNamesLC)
	}
	return strings.Join(clauses, ""), args
}

func fdBookingEntityAllowed(ctx context.Context, entityID string) bool {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return true
	}
	scope := ctxutil.FromContext(ctx)
	return scope.HasEntityAccess(entityID)
}

func trimStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func lowerBankNames(banks []map[string]string) []string {
	out := make([]string, 0, len(banks))
	for _, bank := range banks {
		if name := strings.ToLower(strings.TrimSpace(bank["bank_name"])); name != "" {
			out = append(out, name)
		}
	}
	return out
}
